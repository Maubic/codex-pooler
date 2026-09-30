defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketCompactionResumeResendTest do
  # findings#225 row 225-87: the resume of a mid-turn websocket compaction is
  # admitted by redeeming the native compaction runtime proof, under a
  # capability-minted UUID correlation rather than a durable turn claim. What
  # does an identical resend of that resume meet, on the same socket, on a new
  # socket of the same thread, and over HTTP? The released client resends the
  # same turn_id and the same compacted history after a lost connection.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [released_client_connect!: 4]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Dev.NativeCompactionAuthorizationObserver
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true

  for topology <- [:direct, :forwarded], resend_via <- [:same_socket, :new_socket, :http] do
    @tag slow: "drives an anchor turn, a mid-turn compaction, its resume and a resend through the real public listener (0.3-0.5 s alone, over 1 s under partition load)"
    test "a runtime-proof-admitted resume is durably claimed for its resend (#{topology}, #{resend_via})", %{conn: conn} do
      put_owner_forwarding!(unquote(topology) == :forwarded)
      assert_resume_resend_refused(conn, unquote(resend_via))
    end
  end

  defp put_owner_forwarding!(enabled?) do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
      end
    end)
  end

  defp assert_resume_resend_refused(conn, resend_via) do
    :ok = NativeCompactionAuthorizationObserver.arm()
    on_exit(fn -> NativeCompactionAuthorizationObserver.disarm() end)

    thread = "thread-resume-resend-#{System.unique_integer([:positive])}"
    turn = "turn-resume-resend-#{System.unique_integer([:positive])}"
    item = %{"type" => "compaction", "encrypted_content" => "synthetic-resume-resend"}

    upstream =
      start_upstream(
        # provenance: synthetic, shaped after the released Codex client's remote compaction v2 request.
        # A fourth expectation receives the resend if it is (wrongly) admitted.
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames("resp_resend_anchor")),
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            json: [valid: true, equals: %{"previous_response_id" => "resp_resend_anchor", "input.0.type" => "function_call_output"}],
            respond:
              FakeUpstream.websocket_text_frames([
                CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => item}),
                CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_resend_compact", "status" => "completed", "output" => [item]}})
              ])
          ),
          FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames("resp_resend_final")),
          resend_expectation(resend_via)
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    port = start_public_endpoint!()
    client = released_client_connect!(port, setup.authorization, thread, window_id(thread, 1))

    try do
      anchor = frame(setup, [%{"type" => "message", "role" => "user", "content" => "anchor"}], nil, metadata(thread, turn, 1, :turn))
      client = send_and_receive!(client, anchor, 2)

      compact =
        frame(
          setup,
          [%{"type" => "function_call_output", "call_id" => "synthetic", "output" => "ok"}, %{"type" => "compaction_trigger"}],
          "resp_resend_anchor",
          metadata(thread, turn, 1, :compaction)
        )

      client = send_and_receive!(client, compact, 2)

      resume = frame(setup, [item], nil, metadata(thread, turn, 2, :turn))
      {client, [_created, completed]} = send_and_collect!(client, resume, 2)
      assert %{"type" => "response.completed"} = CodexPooler.JSON.decode!(completed)
      assert NativeCompactionAuthorizationObserver.captures()["counts"]["final_runtime_proof_redeemed"] == 1

      [_anchor_row, _compact_row, resume_row] = settled_pool_requests!(setup.pool.id, 3)
      dispatched = FakeUpstream.count(upstream)
      assert dispatched == 3

      outcome =
        case resend_via do
          :same_socket ->
            {_client, [created, completed]} = send_and_collect!(client, resume, 2)
            assert %{"type" => "response.completed", "response" => %{"id" => "resp_resend_admitted"}} = CodexPooler.JSON.decode!(completed)
            websocket_outcome(created)

          :new_socket ->
            Mint.HTTP.close(client.conn)
            second = released_client_connect!(port, setup.authorization, thread, window_id(thread, 2))
            {second, [created, completed]} = send_and_collect!(second, resume, 2)
            assert %{"type" => "response.completed", "response" => %{"id" => "resp_resend_admitted"}} = CodexPooler.JSON.decode!(completed)
            Mint.HTTP.close(second.conn)
            websocket_outcome(created)

          :http ->
            response =
              conn
              |> recycle()
              |> put_req_header("authorization", setup.authorization)
              |> put_req_header("session-id", thread)
              |> put_req_header("x-codex-window-id", window_id(thread, 2))
              |> put_req_header("x-codex-turn-metadata", metadata(thread, turn, 2, :turn))
              |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => [item]})

            {response.status, response |> Map.get(:resp_body) |> decode_error_code()}
        end

      CodexPooler.TestDiagnostics.puts("225-87 resend_via=#{resend_via} outcome=#{inspect(outcome)} dispatched_after=#{FakeUpstream.count(upstream)} rows=#{length(pool_requests(setup))}")

      case resend_via do
        :http ->
          # This probe omits the original frame's non-input fields, so its
          # payload witness differs and it remains refused.
          assert outcome == {409, "duplicate_turn"}
          assert FakeUpstream.count(upstream) == dispatched

        _websocket ->
          assert {:admitted, "response.created"} = outcome
          # Admission alone is lifecycle-only and remains replayable. Closing
          # before the successor's terminal arms its pre-visible replay instead
          # of settling it; terminal receipt is the completion oracle here.
          rows = settled_pool_requests!(setup.pool.id, 4)
          successor = List.last(rows)
          assert Repo.exists?(from(link in CodexPooler.Accounting.RequestClientRetryLink, where: link.predecessor_request_id == ^resume_row.id and link.successor_request_id == ^successor.id))
          assert FakeUpstream.count(upstream) == dispatched + 1
      end

      assert String.starts_with?(resume_row.correlation_id, "codex-resume:")
    after
      Mint.HTTP.close(client.conn)
    end
  end

  defp resend_expectation(:http),
    do: FakeUpstream.expect_request(method: "POST", respond: FakeUpstream.json_response(%{"id" => "resp_resend_admitted"}))

  defp resend_expectation(_websocket),
    do: FakeUpstream.expect_request(method: "WEBSOCKET", respond: completed_frames("resp_resend_admitted"))

  defp websocket_outcome(frame) do
    case CodexPooler.JSON.decode!(frame) do
      %{"type" => "error", "status" => status, "error" => %{"code" => code}} -> {status, code}
      %{"type" => type} -> {:admitted, type}
    end
  end

  defp decode_error_code(body) do
    case CodexPooler.JSON.decode(body) do
      {:ok, %{"error" => %{"code" => code}}} -> code
      _other -> :no_error
    end
  end

  defp send_and_receive!(client, payload, frames) do
    {client, _frames} = send_and_collect!(client, payload, frames)
    client
  end

  defp send_and_collect!(client, payload, count) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, payload)

    {conn, websocket, frames} =
      Enum.reduce(1..count, {conn, websocket, []}, fn _index, {conn, websocket, acc} ->
        {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, client.ref)
        {conn, websocket, acc ++ [frame]}
      end)

    {%{client | conn: conn, websocket: websocket}, frames}
  end

  # The resume's response task settles it after the terminal frame reaches the
  # client, in a transaction on the shared sandbox connection, so reading the
  # rows right after the frame can queue behind it. No completion signal
  # reaches the test, so wait, within a bounded detection budget, for the
  # resume to be settled before reading.
  @settlement_budget_ms 15_000

  defp settled_pool_requests!(pool_id, count) do
    deadline = System.monotonic_time(:millisecond) + @settlement_budget_ms
    await_settled_pool_requests(pool_id, count, deadline)
  end

  defp await_settled_pool_requests(pool_id, count, deadline) do
    rows = Repo.all(from(r in Request, where: r.pool_id == ^pool_id, order_by: [asc: r.admitted_at, asc: r.id]))

    cond do
      length(rows) == count and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"])) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("expected #{count} settled requests, got #{inspect(Enum.map(rows, & &1.status))}")

      true ->
        Process.sleep(10)
        await_settled_pool_requests(pool_id, count, deadline)
    end
  end

  defp pool_requests(setup),
    do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at, asc: r.id]))

  defp frame(setup, input, previous_response_id, metadata) do
    %{"type" => "response.create", "model" => setup.model.exposed_model_id, "stream" => true, "input" => input, "client_metadata" => %{"x-codex-turn-metadata" => metadata}}
    |> then(&if previous_response_id, do: Map.put(&1, "previous_response_id", previous_response_id), else: &1)
    |> CodexPooler.JSON.encode!()
  end

  defp window_id(thread, number), do: "#{thread}:#{number}"

  defp metadata(thread, turn, window_number, kind) do
    %{
      "turn_id" => turn,
      "thread_id" => thread,
      "window_id" => window_id(thread, window_number),
      "context_window_id" => "00000000-0000-4000-8000-00000000#{window_number}b01",
      "window_number" => window_number,
      "request_kind" => Atom.to_string(kind)
    }
    |> then(fn document ->
      if kind == :compaction,
        do:
          Map.put(document, "compaction", %{
            "trigger" => "auto",
            "reason" => "context_limit",
            "implementation" => "responses_compaction_v2",
            "phase" => "mid_turn",
            "strategy" => "memento"
          }),
        else: document
    end)
    |> CodexPooler.JSON.encode!()
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
end
