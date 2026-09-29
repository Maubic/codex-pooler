defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.SettlementTurnFaultTest do
  # A turn's settlement completes the turn inside the request's own
  # transaction (findings#288), so a fault in that write rolls the whole
  # settlement back: the request, its attempt, its turn and its reservation
  # stay as they were, `in_progress` and outstanding, and nothing shows a
  # settled request behind an open turn. The response task that raised then
  # fails its own request, attempt and turn as `owner_task_exception` with
  # usage unknown, the recovery every raising response task takes. The
  # socket pushed the provider's terminal before the task settled, so the
  # client already has the answer. Its byte-identical resend on a new
  # connection is refused without owner forwarding, where the resend policy
  # reads the delivered receipt, and is admitted as the failed request's one
  # successor with it, where the owner's retry policy admits any failed task
  # exception. The released client does not resend a turn whose completion it
  # read, though: its next request is the next turn on the same socket,
  # anchored on that answer, and that is served in both modes.
  #
  # Before, the request and attempt had already committed `succeeded` when the
  # turn write failed: the turn stayed `in_progress` behind them without owner
  # forwarding, and the owner's replay preflight completed it with forwarding
  # on; the resend was refused in both.
  #
  # The fault is a PostgreSQL trigger inside the sandbox transaction every
  # process of the test shares: it raises on the first write that completes a
  # turn as `succeeded`, once. One node, native websocket, Full, owner
  # forwarding off and on. The released client's key sets and identifiers,
  # synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [await_socket_connection_state!: 2, metadata_control_frame?: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.SettlementTransactionHold
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true

  @installation_id "00000000-0000-4000-8000-00000000c288"
  @context_window_id "00000000-0000-4000-8000-00000000c289"
  @turn_endpoint "/backend-api/codex/responses"
  @detection_timeout_ms 15_000

  for forwarding <- [:forwarded, :direct] do
    @tag forwarding: forwarding
    test "#{forwarding}: a fault completing the turn rolls the whole settlement back and the raising task's own failure closes it", %{forwarding: forwarding} do
      measured = run(forwarding)
      CodexPooler.TestDiagnostics.puts(fn -> "settlement turn fault #{forwarding}: #{inspect(measured)}" end)

      assert measured.rolled_back == %{request: {"in_progress", nil}, attempts: ["in_progress"], turn: {"in_progress", nil}, ledger: ["reservation"]}
      assert measured.first_answer == {:served, "resp_turn_fault_first"}
      assert measured.recovered == %{request: {"failed", "owner_task_exception"}, attempts: ["failed"], turn: {"failed", "owner_task_exception"}, ledger: ["release", "reservation", "settlement"]}
      assert {measured.retry, measured.rows, measured.upstream_requests} == resend_outcome(forwarding)
      assert measured.live_rows == 0
    end
  end

  # The released client does not resend a turn whose `response.completed` it
  # read: its next request is the next turn, on the same socket, anchored on
  # that answer. The raising task's failure reaches the socket after the
  # completion and is logged and recorded on the rows only: a frame pushed for
  # it would wait at the idle client and be read as the failure of that next
  # request (findings#270 row 270-325; before, the socket pushed `500
  # websocket_response_task_failed` there, row 270-290).
  for forwarding <- [:forwarded, :direct] do
    @tag forwarding: forwarding
    test "#{forwarding}: after a rolled-back delivered completion the client's next turn on the same socket is served and nothing is left open", %{forwarding: forwarding} do
      {measured, log} = with_info_log(fn -> run_continuation(forwarding) end)
      CodexPooler.TestDiagnostics.puts(fn -> "settlement turn fault continuation #{forwarding}: #{inspect(measured)}" end)

      assert measured.first_frames == [{"response.created", "resp_turn_fault_first"}, {"response.output_item.done", nil}, {"response.completed", "resp_turn_fault_first"}]
      assert measured.after_completed == []
      assert log =~ "websocket turn error not sent after its terminal"
      assert log =~ "terminal_class=response.completed error_code=websocket_response_task_failed"
      assert measured.next_frames == [{"response.created", "resp_turn_fault_next"}, {"response.output_item.done", nil}, {"response.completed", "resp_turn_fault_next"}]
      assert measured.upstream == [nil, "resp_turn_fault_first"]
      assert measured.rows == [{"failed", "owner_task_exception", "usage_unknown"}, {"succeeded", nil, "usage_known"}]
      assert measured.turns == [{"failed", "owner_task_exception"}, {"succeeded", nil}]
    end
  end

  defp resend_outcome(:direct), do: {{409, "duplicate_turn"}, [{"failed", "owner_task_exception", "usage_unknown"}], 1}

  defp resend_outcome(:forwarded),
    do: {{:served, "resp_turn_fault_resend"}, [{"failed", "owner_task_exception", "usage_unknown"}, {"succeeded", nil, "usage_known"}], 2}

  defp run(forwarding) do
    put_owner_forwarding!(forwarding)
    thread_id = Ecto.UUID.generate()

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a served turn whose settlement meets a fault in its turn write, then the released client's byte-identical resend on a new connection)
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}], respond: completed_frames("resp_turn_fault_first")),
          FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}], respond: completed_frames("resp_turn_fault_resend"))
        ])
      )

    setup = gateway_setup(upstream)
    {_server, port} = start_public_endpoint_with_server!()
    frame = frame(setup, thread_id)
    install_fault!()
    hold = SettlementTransactionHold.after_rollback!()

    # The provider serves the turn; its settlement meets the fault in the turn
    # write, rolls back, and the raising task is held right after that
    # rollback, before its own failure handling runs.
    first = Task.async(fn -> send_once!(port, setup, thread_id, frame) end)
    {settler, _facts} = SettlementTransactionHold.await_held!(hold)
    [request] = pool_requests(setup.pool.id)
    rolled_back = row_state(request.id)
    :ok = SettlementTransactionHold.release(hold, settler)
    first_answer = Task.await(first, @detection_timeout_ms)
    [_recovered] = await_settled!(setup.pool.id)
    recovered = row_state(request.id)

    retry = send_once!(port, setup, thread_id, frame)
    rows = await_settled!(setup.pool.id)

    %{
      rolled_back: rolled_back,
      first_answer: outcome(first_answer),
      recovered: recovered,
      retry: outcome(retry),
      rows: Enum.map(rows, &{&1.status, &1.last_error_code, &1.usage_status}),
      settlements: Enum.map(rows, &settlement_amounts/1),
      chained?: match?([_predecessor, _successor], rows) and chained?(List.last(rows), hd(rows)),
      live_rows: Enum.count(rows, &(&1.status in ["accepted", "in_progress"])),
      upstream_requests: FakeUpstream.count(upstream)
    }
  end

  defp run_continuation(forwarding) do
    put_owner_forwarding!(forwarding)
    thread_id = Ecto.UUID.generate()

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a served turn whose settlement meets a fault in its turn write, then the released client's next turn on the same socket, anchored on the answer it received)
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}], respond: completed_frames("resp_turn_fault_first")),
          FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}], respond: completed_frames("resp_turn_fault_next"))
        ])
      )

    setup = gateway_setup(upstream)
    {_server, port} = start_public_endpoint_with_server!()
    install_fault!()
    {conn, websocket, ref, socket} = connect!(port, setup, thread_id)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame(setup, thread_id))
      {conn, websocket, first_frames} = receive_until_terminal(conn, websocket, ref, [])
      [first] = await_settled!(setup.pool.id)
      recovered = row_state(first.id)
      # The socket handled the raising task's failure, so whatever it pushed
      # for it is written before the pong of a ping sent now.
      _state = await_socket_connection_state!(socket, &(MapSet.size(Map.get(&1, :tasks, MapSet.new())) == 0))
      {conn, websocket, after_completed} = frames_before_pong!(conn, websocket, ref)

      next = frame(setup, thread_id, turn_id: "#{thread_id}-next", input: [user_message("synthetic next prompt")], previous_response_id: "resp_turn_fault_first")
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, next)
      {_conn, _websocket, next_frames} = receive_until_terminal(conn, websocket, ref, [])
      rows = await_settled!(setup.pool.id)

      %{
        first_frames: Enum.map(first_frames, &frame_summary/1),
        after_completed: Enum.map(after_completed, &{&1["type"], &1["status"], get_in(&1, ["error", "code"])}),
        recovered: recovered,
        next_frames: Enum.map(next_frames, &frame_summary/1),
        rows: Enum.map(rows, &{&1.status, &1.last_error_code, &1.usage_status}),
        turns: Enum.map(rows, &turn_outcome/1),
        settlements: Enum.map(rows, &settlement_amounts/1),
        upstream: Enum.map(FakeUpstream.requests(upstream), &(&1.json && &1.json["previous_response_id"]))
      }
    after
      Mint.HTTP.close(conn)
    end
  end

  defp connect!(port, setup, thread_id) do
    before = WebsocketCleanupFence.listener_sockets()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"session-id", thread_id},
      {"thread-id", thread_id},
      {"x-client-request-id", thread_id},
      {"x-codex-window-id", "#{thread_id}:0"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @turn_endpoint, headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref, WebsocketCleanupFence.await_new_listener_socket!(before)}
  end

  defp receive_until_terminal(conn, websocket, ref, frames) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "error"],
      do: {conn, websocket, Enum.reverse([frame | frames])},
      else: receive_until_terminal(conn, websocket, ref, [frame | frames])
  end

  defp frame_summary(frame), do: {frame["type"], get_in(frame, ["response", "id"])}

  # Every text frame the client receives before the pong of a ping sent now,
  # the Pooler's metadata control frames aside.
  defp frames_before_pong!(conn, websocket, ref) do
    payload = "settlement-turn-fault-#{System.unique_integer([:positive])}"
    {:ok, websocket, data} = Mint.WebSocket.encode(websocket, {:ping, payload})
    {:ok, conn} = Mint.WebSocket.stream_request_body(conn, ref, data)
    frames_before_pong!(conn, websocket, ref, payload, [])
  end

  defp frames_before_pong!(conn, websocket, ref, payload, frames) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "timed out waiting for the pong")
    {:ok, conn, responses} = Mint.WebSocket.stream(conn, message)

    {websocket, decoded} =
      Enum.reduce(responses, {websocket, []}, fn
        {:data, ^ref, data}, {websocket, decoded} ->
          {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)
          {websocket, decoded ++ frames}

        _response, acc ->
          acc
      end)

    frames = frames ++ for({:text, text} = frame <- decoded, not metadata_control_frame?(frame), do: CodexPooler.JSON.decode!(text))

    if {:pong, payload} in decoded,
      do: {conn, websocket, frames},
      else: frames_before_pong!(conn, websocket, ref, payload, frames)
  end

  defp turn_outcome(%Request{id: id}) do
    turn = Repo.get_by!(CodexTurn, request_id: id)
    {turn.status, turn.error_code}
  end

  # What each request was charged: its recorded settlement's usage status,
  # tokens and cost.
  defp settlement_amounts(%Request{id: id}) do
    Repo.one(
      from(entry in LedgerEntry,
        where: entry.request_id == ^id and entry.entry_kind == "settlement" and entry.amount_status == "recorded",
        select: {entry.usage_status, entry.total_tokens, entry.estimated_cost_micros, entry.settled_cost_micros}
      )
    )
  end

  defp user_message(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp row_state(request_id) do
    request = Repo.get!(Request, request_id)
    turn = Repo.get_by!(CodexTurn, request_id: request_id)
    attempts = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^request_id, order_by: attempt.attempt_number, select: attempt.status))

    %{
      request: {request.status, request.last_error_code},
      attempts: attempts,
      turn: {turn.status, turn.error_code},
      ledger: ledger_kinds(request)
    }
  end

  # Raises on the first write that completes a turn as `succeeded`: the
  # settlement's own turn write. A sequence counts across the rollback, so the
  # resend's completion goes through.
  defp install_fault! do
    Repo.query!("CREATE SEQUENCE pg_temp.p288_turn_fault_seq")

    Repo.query!("""
    CREATE FUNCTION pg_temp.p288_turn_fault() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.status = 'succeeded' AND OLD.status = 'in_progress' THEN
        IF nextval('pg_temp.p288_turn_fault_seq') = 1 THEN
          RAISE EXCEPTION 'synthetic turn completion fault' USING ERRCODE = 'raise_exception';
        END IF;
      END IF;
      RETURN NEW;
    END $$
    """)

    Repo.query!("CREATE TRIGGER p288_turn_fault BEFORE UPDATE ON codex_turns FOR EACH ROW EXECUTE FUNCTION pg_temp.p288_turn_fault()")
  end

  # One request on its own connection, as the released client sends a retry:
  # a new connection and the same body.
  defp send_once!(port, setup, thread_id, frame) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"session-id", thread_id},
      {"thread-id", thread_id},
      {"x-client-request-id", thread_id},
      {"x-codex-window-id", "#{thread_id}:0"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @turn_endpoint, headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
      receive_terminal!(conn, websocket, ref)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => type} = terminal when type in ["response.completed", "response.failed", "error"] -> terminal
      _progress -> receive_terminal!(conn, websocket, ref)
    end
  end

  defp outcome(%{"type" => "response.completed", "response" => %{"id" => id}}), do: {:served, id}
  defp outcome(%{"type" => "error", "status" => status, "error" => %{"code" => code}}), do: {status, code}
  defp outcome(other), do: {:unexpected, other["type"]}

  defp frame(setup, thread_id, opts \\ []) do
    turn_id = Keyword.get(opts, :turn_id, "#{thread_id}-turn")

    %{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => Keyword.get(opts, :input, [user_message("synthetic settlement fault prompt")]),
      "tools" => [],
      "tool_choice" => "auto",
      "parallel_tool_calls" => true,
      "reasoning" => %{"effort" => "low"},
      "store" => false,
      "stream" => true,
      "include" => ["reasoning.encrypted_content"],
      "text" => %{"verbosity" => "low"},
      "prompt_cache_key" => thread_id,
      "client_metadata" => %{
        "session_id" => thread_id,
        "thread_id" => thread_id,
        "turn_id" => turn_id,
        "root_turn_id" => turn_id,
        "x-codex-installation-id" => @installation_id,
        "x-codex-window-id" => "#{thread_id}:0",
        "x-codex-turn-metadata" =>
          CodexPooler.JSON.encode!(%{
            "agent_name" => "/root",
            "context_window_id" => @context_window_id,
            "installation_id" => @installation_id,
            "root_turn_id" => turn_id,
            "session_id" => thread_id,
            "thread_id" => thread_id,
            "turn_id" => turn_id,
            "turn_started_at_unix_ms" => 1_790_000_000_000,
            "window_id" => "#{thread_id}:0",
            "window_number" => 0,
            "request_kind" => "turn"
          })
      }
    }
    |> then(fn body -> if anchor = Keyword.get(opts, :previous_response_id), do: Map.put(body, "previous_response_id", anchor), else: body end)
    |> CodexPooler.JSON.encode!()
  end

  defp completed_frames(response_id) do
    answer = %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}
    usage = %{"input_tokens" => 20, "output_tokens" => 2, "total_tokens" => 22}

    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => answer}),
      CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [answer], "usage" => usage}})
    ])
  end

  defp chained?(%Request{request_metadata: %{"client_resend" => %{"predecessor_request_id" => id}}}, %Request{id: id}), do: true
  defp chained?(%Request{id: successor_id}, %Request{id: predecessor_id}), do: Repo.exists?(from(link in RequestClientRetryLink, where: link.successor_request_id == ^successor_id and link.predecessor_request_id == ^predecessor_id))

  defp ledger_kinds(%Request{id: request_id}), do: ledger_kinds(request_id)
  defp ledger_kinds(request_id), do: Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request_id, order_by: entry.entry_kind, select: entry.entry_kind))

  defp pool_requests(pool_id), do: Repo.all(from(request in Request, where: request.pool_id == ^pool_id, order_by: [asc: request.admitted_at, asc: request.id]))

  # The socket writes the terminal before the task settles its rows, and a
  # refused resend claims no row; poll the rows within a detection budget until
  # none of them, their attempts or their turns is live.
  defp await_settled!(pool_id) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    Stream.repeatedly(fn -> pool_requests(pool_id) end)
    |> Enum.reduce_while(nil, fn rows, _acc ->
      cond do
        rows != [] and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"])) and no_live_turn?(rows) -> {:halt, rows}
        System.monotonic_time(:millisecond) >= deadline -> {:halt, rows}
        true -> Process.sleep(10) && {:cont, nil}
      end
    end)
  end

  defp no_live_turn?(rows) do
    ids = Enum.map(rows, & &1.id)

    not Repo.exists?(from(attempt in Attempt, where: attempt.request_id in ^ids and attempt.status in ["queued", "in_progress"])) and
      not Repo.exists?(from(turn in CodexTurn, where: turn.request_id in ^ids and turn.status == "in_progress"))
  end

  # The socket notes the failure it did not send at `info`.
  defp with_info_log(fun) do
    previous_level = Logger.level()
    on_exit(fn -> Logger.configure(level: previous_level) end)
    Logger.configure(level: :info)

    try do
      with_log([level: :info], fun)
    after
      Logger.configure(level: previous_level)
    end
  end

  defp put_owner_forwarding!(forwarding) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding == :forwarded)
  end
end
