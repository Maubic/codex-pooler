defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.DeadExecutionPreflightTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionTerminalProofs}
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @timeout_ms 15_000

  for {forwarding, mode, predecessor_kind} <- [{false, "full", :opening}, {true, "full", :opening}, {false, "lite", :opening}, {true, "lite", :opening}, {false, "full", :tool_continuation}, {true, "full", :tool_continuation}] do
    @tag forwarding: forwarding, serving_mode: mode, predecessor_kind: predecessor_kind
    @tag slow: "real visible executor death and concurrent new-session retries before scheduled cleanup"
    test "#{predecessor_kind} #{mode} forwarding=#{forwarding} recovers a visible dead executor during fresh-session preflight", %{forwarding: forwarding, serving_mode: mode, predecessor_kind: predecessor_kind} do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding)
      barrier = make_ref()

      upstream =
        start_upstream(
          FakeUpstream.strict_sequence(
            initial_expectations(predecessor_kind) ++
              [
                FakeUpstream.expect_request(
                  method: "WEBSOCKET",
                  path: "/backend-api/codex/responses",
                  respond: FakeUpstream.barrier_websocket_frames(visible_frames(), notify: self(), release_ref: barrier)
                ),
                FakeUpstream.expect_request(
                  method: "WEBSOCKET",
                  path: "/backend-api/codex/responses",
                  respond: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_recovered", "status" => "completed", "output" => if(predecessor_kind == :tool_continuation, do: [next_tool_call()], else: [])}})])
                )
              ] ++ subsequent_expectations(predecessor_kind)
          )
        )

      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, mode)
      session_id = Ecto.UUID.generate()
      thread_id = Ecto.UUID.generate()
      turn_id = Ecto.UUID.generate()

      payload = %{
        "type" => "response.create",
        "model" => setup.model.exposed_model_id,
        "input" => [],
        "stream" => true,
        "store" => false,
        "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => session_id, "thread_id" => thread_id, "turn_id" => turn_id, "request_kind" => "turn"})}
      }

      {server, port} = start_public_endpoint_with_server!()
      {conn, websocket, ref} = connect!(port, setup, session_id)
      {conn, websocket, payload, resend_payload} = start_predecessor!(predecessor_kind, conn, websocket, ref, payload)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))

      for ordinal <- 0..1 do
        assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^barrier}, @timeout_ms
        assert :ok = FakeUpstream.release_frame(upstream, barrier)
      end

      assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^barrier}, @timeout_ms
      {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, ref)
      assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(created)
      {conn, _websocket, visible} = public_websocket_receive_text!(conn, websocket, ref)
      assert %{"type" => "response.output_text.delta"} = CodexPooler.JSON.decode!(visible)

      request = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress")
      attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id)
      turn = Repo.one!(from t in CodexTurn, where: t.request_id == ^request.id)
      predecessor_count = if predecessor_kind == :opening, do: 1, else: 2
      expected_claim_prefix = if predecessor_kind == :opening, do: "codex-turn:", else: "codex-request:"
      assert String.starts_with?(request.correlation_id, expected_claim_prefix)

      if predecessor_kind == :tool_continuation do
        [opening_upstream, continuation_upstream] = FakeUpstream.requests(upstream)
        assert opening_upstream.websocket_connection_id == continuation_upstream.websocket_connection_id
        assert continuation_upstream.json["previous_response_id"] == "resp_tool_anchor"
        assert [%{"type" => "function_call_output"}] = continuation_upstream.json["input"]
      end

      assert attempt.replay_generation == 0
      assert ExecutionIdentity.status(attempt) == :alive
      task = attempt.owner_process_id |> String.to_charlist() |> :erlang.list_to_pid()
      socket = stranded_socket!(forwarding, server, turn)
      refute task == socket
      task_monitor = Process.monitor(task)

      # Prevent the original socket from consuming its DOWN and settling the
      # turn. The only permitted settlement path is the incoming retry claim;
      # this test never invokes a cleanup worker or recovery sweep.
      on_exit(fn ->
        if Process.alive?(socket), do: :erlang.resume_process(socket)
      end)

      :erlang.suspend_process(socket)

      try do
        Process.exit(task, :kill)
        assert_receive {:DOWN, ^task_monitor, :process, ^task, :killed}, @timeout_ms
        publisher = CodexPooler.ExecutionProofSupport.start_publisher!(name: :"preflight_proof_#{unquote(forwarding)}_#{unquote(mode)}")
        :ok = CodexPooler.ExecutionProofSupport.await_terminal!(attempt, publisher)
        assert ExecutionTerminalProofs.terminal?(attempt)
        assert Repo.reload!(request).status == "in_progress"
        assert Repo.reload!(attempt).status == "in_progress"

        changed = Map.put(resend_payload, "temperature", 0.5)
        assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = retry!(port, setup, changed)
        assert Repo.reload!(request).status == "in_progress"
        assert FakeUpstream.count(upstream) == predecessor_count

        anchored = Map.put(resend_payload, "previous_response_id", "resp_unrelated_anchor")
        assert %{"type" => "error"} = retry!(port, setup, anchored)
        assert Repo.reload!(request).status == "in_progress"
        assert FakeUpstream.count(upstream) == predecessor_count

        # Separate connection owners/mailboxes, a common start barrier, and
        # distinct session/window headers reproduce concurrent reconnects.
        parent = self()

        clients =
          for _ <- 1..2 do
            client =
              Task.async(fn ->
                {client_conn, ws, client_ref} = connect!(port, setup, Ecto.UUID.generate())
                send(parent, {:retry_ready, self()})

                receive do
                  :send ->
                    {client_conn, ws} = public_websocket_send_text!(client_conn, ws, client_ref, CodexPooler.JSON.encode!(resend_payload))
                    {client_conn, ws, frame} = public_websocket_receive_text!(client_conn, ws, client_ref)
                    result = CodexPooler.JSON.decode!(frame)
                    send(parent, {:retry_result, self(), result})

                    receive do
                      :close ->
                        Mint.HTTP.close(client_conn)

                      :continue ->
                        next = resend_payload |> Map.put("previous_response_id", "resp_recovered") |> Map.put("input", [%{"type" => "function_call_output", "call_id" => "call_next", "output" => "next synthetic result"}])
                        {client_conn, ws} = public_websocket_send_text!(client_conn, ws, client_ref, CodexPooler.JSON.encode!(next))
                        {client_conn, _ws, next_frame} = public_websocket_receive_text!(client_conn, ws, client_ref)
                        Mint.HTTP.close(client_conn)
                        assert %{"type" => "response.completed", "response" => %{"id" => "resp_next_completed"}} = CodexPooler.JSON.decode!(next_frame)
                    end
                end
              end)

            monitor = Process.monitor(client.pid)
            on_exit(fn -> if Process.alive?(client.pid), do: Process.exit(client.pid, :kill) end)
            {client, monitor}
          end

        for {client, _} <- clients do
          pid = client.pid
          assert_receive {:retry_ready, ^pid}, @timeout_ms
        end

        for {client, _} <- clients, do: send(client.pid, :send)

        results =
          for {client, _monitor} <- clients do
            pid = client.pid
            assert_receive {:retry_result, ^pid, result}, @timeout_ms
            result
          end

        assert Enum.count(results, &(&1["type"] == "response.completed")) == 1
        assert [%{"status" => 409, "error" => %{"code" => "duplicate_turn"}}] = Enum.filter(results, &(&1["type"] == "error"))
        assert %Request{status: "failed", last_error_code: "dead_execution_recovered"} = Repo.reload!(request)
        assert %Attempt{status: "failed", replay_generation: 0} = Repo.reload!(attempt)
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == predecessor_count + 1
        assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^request.id), :count) == 1
        link = Repo.one!(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^request.id))
        await_request_succeeded!(link.successor_request_id, System.monotonic_time(:millisecond) + @timeout_ms)
        assert %{"status" => 409, "error" => %{"code" => "duplicate_turn"}} = retry!(port, setup, resend_payload)

        assert FakeUpstream.count(upstream) == predecessor_count + 1
        assert request.id |> Accounting.list_ledger_entries_for_request() |> Enum.map(& &1.entry_kind) |> Enum.sort() == ["release", "reservation", "settlement"]

        for {{client, monitor}, result} <- Enum.zip(clients, results) do
          action = if predecessor_kind == :tool_continuation and result["type"] == "response.completed", do: :continue, else: :close
          send(client.pid, action)
          Task.await(client, @timeout_ms)
          pid = client.pid
          assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, @timeout_ms
        end

        if predecessor_kind == :tool_continuation do
          assert FakeUpstream.count(upstream) == 4
          [_, _, recovered_upstream, next_upstream] = FakeUpstream.requests(upstream)
          assert recovered_upstream.websocket_connection_id == next_upstream.websocket_connection_id
          assert next_upstream.json["previous_response_id"] == "resp_recovered"
          assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^request.id), :count) == 1
        end
      after
        :erlang.resume_process(socket)
        assert :ok = FakeUpstream.release_remaining_frames(upstream, barrier)
        socket_monitor = Process.monitor(socket)
        Mint.HTTP.close(conn)
        assert_receive {:DOWN, ^socket_monitor, :process, ^socket, _}, @timeout_ms
      end

      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  defp subsequent_expectations(:opening), do: []

  defp subsequent_expectations(:tool_continuation) do
    [FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_next_completed", "status" => "completed"}})]))]
  end

  defp next_tool_call, do: Map.merge(tool_call(), %{"id" => "fc_next", "call_id" => "call_next"})

  defp initial_expectations(:opening), do: []

  defp initial_expectations(:tool_continuation) do
    [
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        path: "/backend-api/codex/responses",
        respond: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_tool_anchor", "status" => "completed", "output" => [tool_call()]}})])
      )
    ]
  end

  defp start_predecessor!(:opening, conn, websocket, _ref, payload), do: {conn, websocket, payload, payload}

  defp start_predecessor!(:tool_continuation, conn, websocket, ref, payload) do
    message = %{"type" => "message", "role" => "user", "content" => "synthetic lookup"}
    initial = Map.merge(payload, %{"input" => [message], "tools" => [%{"type" => "function", "name" => "lookup", "parameters" => %{"type" => "object", "properties" => %{}}}]})
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(initial))
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_tool_anchor"}} = CodexPooler.JSON.decode!(frame)
    output = %{"type" => "function_call_output", "call_id" => "call_lookup", "output" => "synthetic result"}
    continuation = initial |> Map.put("input", [output]) |> Map.put("previous_response_id", "resp_tool_anchor")
    resend = continuation |> Map.delete("previous_response_id") |> Map.put("input", [message, tool_call(), output])
    {conn, websocket, continuation, resend}
  end

  defp tool_call, do: %{"id" => "fc_lookup", "type" => "function_call", "call_id" => "call_lookup", "name" => "lookup", "arguments" => "{}", "status" => "completed"}

  defp await_request_succeeded!(request_id, deadline) do
    case Repo.get!(Request, request_id) do
      %Request{status: "succeeded"} ->
        :ok

      %Request{status: status} ->
        assert System.monotonic_time(:millisecond) < deadline, "successor did not settle: #{status}"

        receive do
        after
          5 -> await_request_succeeded!(request_id, deadline)
        end
    end
  end

  defp retry!(port, setup, payload) do
    {conn, ws, ref} = connect!(port, setup, Ecto.UUID.generate())
    {conn, ws} = public_websocket_send_text!(conn, ws, ref, CodexPooler.JSON.encode!(payload))
    {conn, _ws, frame} = public_websocket_receive_text!(conn, ws, ref)
    Mint.HTTP.close(conn)
    CodexPooler.JSON.decode!(frame)
  end

  defp visible_frames do
    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => "resp_visible", "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_text.delta", "item_id" => "msg_visible", "output_index" => 0, "content_index" => 0, "delta" => "partial"}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp stranded_socket!(true, _server, turn) do
    assert {:ok, owner} = WebsocketOwnerSession.lookup(turn.codex_session_id)
    %{downstream: %{pid: socket}} = :sys.get_state(owner)
    socket
  end

  defp stranded_socket!(false, server, turn) do
    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(turn.codex_session_id)
    assert {:ok, [socket]} = ThousandIsland.connection_pids(server)
    socket
  end

  defp connect!(port, setup, session_id) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"session-id", session_id}, {"x-codex-window-id", session_id}, {"x-request-id", Ecto.UUID.generate()}, {"user-agent", "codex_cli_rs/0.154.0"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/backend-api/codex/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end
end
