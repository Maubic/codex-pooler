defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.SettlementWindowResendTest do
  # A websocket turn's settlement writes the request, its attempt, its ledger
  # entries and the turn row in one transaction (findings#288). It used to
  # complete the turn in a second transaction, and the released client (Codex
  # 0.156.1), which resends a turn the provider failed (`response.failed`
  # `server_error`) on a new connection about 200 ms after the failure frame,
  # could land between the two commits on a slow database: the request already
  # `failed`, the turn still `in_progress`. With owner forwarding on, the
  # owner's replay preflight judged that turn orphaned and closed it
  # `failed orphaned_turn_closed`, which no resend policy admits, so every
  # resend met `409 duplicate_turn` (findings#206 row 206-609); with it off the
  # claim waited, bounded, for the open turn.
  #
  # The settling process is held inside its settlement transaction, right after
  # it wrote the turn's completion. Nothing of the settlement is visible yet:
  # the request and its turn both still read `in_progress`. The resend, on a
  # new connection, waits on the codex session row the held transaction locked
  # (observed with `pg_blocking_pids` before the release), and once the
  # settlement commits it is served as the failed request's one successor. One
  # node, committed rows, native websocket `/backend-api/codex/responses`, owner
  # forwarding on and off, the Pool's model forced to Full and to Lite,
  # FakeUpstream. Turn metadata and frame shapes are the released client's;
  # text and identifiers synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.SettlementTransactionHold
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @detection_timeout_ms 15_000

  for forwarding <- [:forwarded, :direct], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, serving_mode: mode
    test "websocket #{forwarding} #{mode}: a resend arriving while a provider-failed turn's settlement is open waits for it and is served as its one successor", ctx do
      measured = run(ctx.forwarding, ctx.serving_mode)
      CodexPooler.TestDiagnostics.puts(fn -> "settlement window #{ctx.forwarding} #{ctx.serving_mode}: #{inspect(measured)}" end)

      assert measured.window == {"in_progress", "in_progress"}
      assert measured.resend_waited_on == "codex_sessions"
      assert measured.resend == {"response.completed", nil}
      assert measured.requests == [{"failed", "server_error"}, {"succeeded", nil}]
      assert measured.failed_turn == {"failed", "server_error"}
      assert measured.linked? == true
      assert measured.recorded_settlements == [1, 1]
      assert measured.upstream_requests == 2
    end
  end

  defp run(forwarding, mode) do
    put_owner_forwarding!(forwarding)
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)

    upstream =
      start_upstream(
        # provenance: observed runbook terminal-failure resend (response.failed server_error, the released client's resend on a new connection); reply frames synthetic
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: failure_frames()),
          FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: completed_frames("resp_settlement_window_resend"))
        ])
      )

    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    watcher = SettlementTransactionHold.start_lock_watcher!()
    port = start_public_endpoint!()
    thread = "ws-settlement-window-#{System.unique_integer([:positive])}"
    frame = released_frame(setup, thread)
    hold = SettlementTransactionHold.inside_transaction!()

    # The provider fails the turn; the settling process stops inside its
    # settlement transaction, right after it wrote the turn's completion.
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
    {conn, _websocket, failure} = receive_until_terminal(conn, websocket, ref)
    assert %{"type" => "response.failed"} = failure
    {settler, %{backend: settler_backend}} = SettlementTransactionHold.await_held!(hold)
    [failed] = pool_requests(setup)
    window = {failed.status, Repo.get_by!(CodexTurn, request_id: failed.id).status}
    Mint.HTTP.close(conn)

    # The resend, on a new connection, while the settlement is still open: its
    # socket's session lookup waits on the session row the held transaction
    # locked, and the settler is released once that wait is observed.
    resend_task = Task.async(fn -> resend!(port, setup, thread, frame) end)
    resend_waited_on = SettlementTransactionHold.await_session_lookup_wait!(watcher, settler_backend)
    :ok = SettlementTransactionHold.release(hold, settler)
    resend = Task.await(resend_task, @detection_timeout_ms)
    await_settled!(setup)
    [failed | later] = requests = pool_requests(setup)
    failed_turn = Repo.get_by!(CodexTurn, request_id: failed.id)

    %{
      window: window,
      resend_waited_on: resend_waited_on,
      resend: {resend["type"], get_in(resend, ["error", "code"])},
      requests: Enum.map(requests, &{&1.status, &1.last_error_code}),
      failed_turn: {failed_turn.status, failed_turn.error_code},
      linked?: match?([successor] when is_struct(successor, Request), later) and linked_successor?(failed, hd(later)),
      recorded_settlements: Enum.map(requests, &recorded_settlements/1),
      upstream_requests: FakeUpstream.count(upstream)
    }
  end

  defp resend!(port, setup, thread, frame) do
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
      {_conn, _websocket, terminal} = receive_until_terminal(conn, websocket, ref)
      terminal
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_until_terminal(conn, websocket, ref) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal when type in ["response.completed", "response.failed", "error"] -> {conn, websocket, terminal}
      _progress -> receive_until_terminal(conn, websocket, ref)
    end
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp await_settled!(setup) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_settled!(setup, deadline)
  end

  defp await_settled!(setup, deadline) do
    turns = Repo.all(from(t in CodexTurn, join: r in Request, on: r.id == t.request_id, where: r.pool_id == ^setup.pool.id, select: t.status))
    requests = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, select: r.status))

    cond do
      Enum.all?(requests ++ turns, &(&1 not in ["accepted", "in_progress"])) -> :ok
      System.monotonic_time(:millisecond) >= deadline -> :ok
      true -> Process.sleep(10) && await_settled!(setup, deadline)
    end
  end

  defp linked_successor?(%Request{id: failed_id}, %Request{id: successor_id} = successor) do
    successor.request_metadata["client_resend"]["predecessor_request_id"] == failed_id or
      Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^failed_id and link.successor_request_id == ^successor_id))
  end

  defp recorded_settlements(%Request{id: id}),
    do: Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^id and l.entry_kind == "settlement" and l.amount_status == "recorded"), :count)

  # The released client's turn frame (`request_kind` turn).
  defp released_frame(setup, thread) do
    turn_id = Ecto.UUID.generate()
    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn_id}

    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("synthetic settlement window turn"),
      "stream" => true,
      "generate" => true,
      "client_metadata" => Map.put(metadata, "x-codex-turn-metadata", CodexPooler.JSON.encode!(Map.put(metadata, "request_kind", "turn")))
    })
  end

  defp failure_frames do
    response_id = "resp_settlement_window_failed"

    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"id" => response_id, "status" => "failed", "error" => %{"code" => "server_error", "message" => "synthetic provider failure"}}})
    ])
  end

  defp completed_frames(response_id) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}
      })
    ])
  end

  defp put_owner_forwarding!(forwarding) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding == :forwarded)
  end
end
