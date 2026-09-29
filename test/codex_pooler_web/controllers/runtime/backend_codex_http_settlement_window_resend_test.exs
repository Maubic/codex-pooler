defmodule CodexPoolerWeb.Runtime.BackendCodexHttpSettlementWindowResendTest do
  # The native HTTP SSE counterpart of the websocket settlement window
  # (`backend_codex_websocket/settlement_window_resend_test.exs`, findings#288).
  # A turn's settlement writes the request, its attempt, its ledger entries and
  # the turn row in one transaction; it used to complete the turn in a second
  # one, so a resend between the two commits met a terminal request behind a
  # turn still `in_progress`.
  #
  # The provider fails the turn (`response.failed` `server_error`) and the
  # settling process is held inside its settlement transaction, right after it
  # wrote the turn's completion. Nothing of the settlement is visible: the
  # request and its turn both still read `in_progress`, and the client has not
  # received the terminal either (the Pooler writes it after the settlement).
  # The client drops its connection and resends the same turn over HTTP on a new
  # one: the resend's session lookup waits on the codex session row the held
  # transaction locked (observed with `pg_blocking_pids` before the release),
  # and once the settlement commits it is served. The native HTTP turn claim
  # steps over a failed predecessor that showed no output (findings#212 row
  # 212-50), so the resend is a request of its own, not a linked successor.
  # One node, committed rows, native `POST
  # /backend-api/codex/responses` with SSE, the Pool's model forced to Full and
  # to Lite, FakeUpstream; synthetic text and identifiers.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, register_unboxed_pool_cleanup!: 1, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.SettlementTransactionHold
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @native_path "/backend-api/codex/responses"
  @detection_timeout_ms 15_000

  for mode <- ["full", "lite"] do
    @tag serving_mode: mode
    test "http sse #{mode}: a resend arriving while a provider-failed turn's settlement is open waits for it and is served", ctx do
      measured = run(ctx.serving_mode)
      CodexPooler.TestDiagnostics.puts(fn -> "http settlement window #{ctx.serving_mode}: #{inspect(measured)}" end)

      assert measured.window == {"in_progress", "in_progress"}
      assert measured.resend_waited_on == "codex_sessions"
      assert measured.resend == {200, "response.completed"}
      assert measured.requests == [{"failed", "server_error"}, {"succeeded", nil}]
      assert measured.failed_turn == {"failed", "server_error"}
      assert measured.linked? == false
      assert measured.recorded_settlements == [1, 1]
      assert measured.upstream_requests == 2
    end
  end

  defp run(mode) do
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)

    upstream =
      start_upstream(
        # provenance: observed runbook terminal-failure resend (response.failed server_error), carried over native HTTP SSE; reply events synthetic
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "POST", path: @native_path, respond: FakeUpstream.sse_stream([created_event("resp_http_window_failed"), failed_event("resp_http_window_failed")])),
          FakeUpstream.expect_request(method: "POST", path: @native_path, respond: FakeUpstream.sse_stream([created_event("resp_http_window_resend"), completed_event("resp_http_window_resend")]))
        ])
      )

    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    put_serving_mode!(setup, mode)
    watcher = SettlementTransactionHold.start_lock_watcher!()
    port = start_public_endpoint!()
    thread_id = Ecto.UUID.generate()
    payload = native_payload(setup, thread_id)
    hold = SettlementTransactionHold.inside_transaction!()

    # The provider fails the turn; the settling process stops inside its
    # settlement transaction, right after it wrote the turn's completion, and
    # the client drops the connection that carried it.
    first = Task.async(fn -> post_then_drop!(port, setup, payload, thread_id) end)
    {settler, %{backend: settler_backend}} = SettlementTransactionHold.await_held!(hold)
    [failed] = pool_requests(setup)
    window = {failed.status, Repo.get_by!(CodexTurn, request_id: failed.id).status}
    send(first.pid, :drop)
    :dropped = Task.await(first, @detection_timeout_ms)

    # The resend, on a new connection, while the settlement is still open.
    resend_task = Task.async(fn -> post_until_done!(port, setup, payload, thread_id) end)
    resend_waited_on = SettlementTransactionHold.await_session_lookup_wait!(watcher, settler_backend)
    :ok = SettlementTransactionHold.release(hold, settler)
    resend = Task.await(resend_task, @detection_timeout_ms)
    [failed | later] = requests = await_settled!(setup)
    failed_turn = Repo.get_by!(CodexTurn, request_id: failed.id)

    %{
      window: window,
      resend_waited_on: resend_waited_on,
      resend: resend,
      requests: Enum.map(requests, &{&1.status, &1.last_error_code}),
      failed_turn: {failed_turn.status, failed_turn.error_code},
      linked?: match?([successor] when is_struct(successor, Request), later) and linked_successor?(failed, hd(later)),
      recorded_settlements: Enum.map(requests, &recorded_settlements/1),
      upstream_requests: FakeUpstream.count(upstream)
    }
  end

  # The first request on its own connection, dropped by the client once the
  # test says so.
  defp post_then_drop!(port, setup, payload, thread_id) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    {:ok, conn, _ref} = request!(conn, setup, payload, thread_id)

    receive do
      :drop ->
        Mint.HTTP.close(conn)
        :dropped
    after
      @detection_timeout_ms -> Mint.HTTP.close(conn) && :never_told
    end
  end

  # One request on its own connection, read until its stream ends: the status
  # and the terminal event the client received.
  defp post_until_done!(port, setup, payload, thread_id) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    {:ok, conn, ref} = request!(conn, setup, payload, thread_id)

    try do
      receive_all(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp request!(conn, setup, payload, thread_id) do
    Mint.HTTP.request(
      conn,
      "POST",
      @native_path,
      [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread_id}, {"originator", "codex_cli_rs"}],
      CodexPooler.JSON.encode!(payload)
    )
  end

  defp receive_all(conn, ref, status, body) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @detection_timeout_ms)

    {status, body, done?} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, status}, {_status, body, done?} -> {status, body, done?}
        {:data, ^ref, data}, {status, body, done?} -> {status, body <> data, done?}
        {:done, ^ref}, {status, body, _done?} -> {status, body, true}
        _other, acc -> acc
      end)

    if done?, do: {status, terminal_of(body)}, else: receive_all(conn, ref, status, body)
  end

  defp terminal_of(body) do
    cond do
      body =~ "response.completed" -> "response.completed"
      body =~ "response.failed" -> "response.failed"
      true -> :no_terminal
    end
  end

  defp await_settled!(setup) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_settled!(setup, deadline)
  end

  defp await_settled!(setup, deadline) do
    requests = pool_requests(setup)
    turns = Repo.all(from(t in CodexTurn, where: t.request_id in ^Enum.map(requests, & &1.id), select: t.status))

    cond do
      Enum.all?(Enum.map(requests, & &1.status) ++ turns, &(&1 not in ["accepted", "in_progress"])) -> requests
      System.monotonic_time(:millisecond) >= deadline -> requests
      true -> Process.sleep(10) && await_settled!(setup, deadline)
    end
  end

  defp linked_successor?(%Request{id: failed_id}, %Request{id: successor_id} = successor) do
    successor.request_metadata["client_resend"]["predecessor_request_id"] == failed_id or
      Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^failed_id and link.successor_request_id == ^successor_id))
  end

  defp recorded_settlements(%Request{id: id}),
    do: Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^id and l.entry_kind == "settlement" and l.amount_status == "recorded"), :count)

  # The released client's native turn over HTTP: the turn metadata in the body
  # and the thread in `session-id`.
  defp native_payload(setup, thread_id) do
    %{
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("synthetic http settlement window turn"),
      "stream" => true,
      "client_metadata" => %{
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => "http-settlement-window-turn", "request_kind" => "turn"})
      }
    }
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp created_event(id), do: {"response.created", %{"type" => "response.created", "response" => %{"id" => id, "status" => "in_progress"}}}

  defp failed_event(id),
    do: {"response.failed", %{"type" => "response.failed", "response" => %{"id" => id, "status" => "failed", "error" => %{"code" => "server_error", "message" => "synthetic provider failure"}}}}

  defp completed_event(id),
    do: {"response.completed", %{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}}}

  defp put_serving_mode!(setup, mode) do
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    :ok
  end
end
