defmodule CodexPoolerWeb.CodexResponsesSocketUpstreamCloseTest do
  # The native socket's handling of its upstream session's close signal
  # (findings#270), driven through the WebSock callbacks with hand-built
  # socket states: when it latches, when it keeps the socket open and why, and
  # when the latch turns into the 1001 close. The listener-level behaviour is
  # in `backend_codex_websocket/upstream_close_downstream_test.exs`.
  use CodexPooler.DataCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  alias CodexPooler.Events
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPoolerWeb.CodexResponsesSocket

  @upstream_close_detail {1001, "upstream connection closed"}
  @codex_session_id "00000000-0000-4000-8000-000000000270"

  setup do
    session = spawn_quiet_process()
    task = spawn_quiet_process()
    lifecycle_id = Ecto.UUID.generate()

    %{
      session: session,
      task: task,
      signal: {:upstream_websocket_connection_closed, session, %{cause: :peer_close_frame, lifecycle_id: lifecycle_id, generation: 3, connection_requests: 2}},
      lifecycle_id: lifecycle_id
    }
  end

  test "a native socket still settling its completed turn latches the close", ctx do
    state = settling_state(ctx)

    assert {{:ok, latched}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(ctx.signal, state) end)

    assert latched == Map.put(state, :upstream_close_pending, %{cause: :peer_close_frame, lifecycle_id: ctx.lifecycle_id, generation: 3, forwarding: :off})
    assert upstream_close_lines(log) == []
  end

  test "an idle native socket closes 1001 at once", ctx do
    state = native_state(ctx)

    assert {{:stop, :normal, @upstream_close_detail, stopped}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(ctx.signal, state) end)

    assert stopped == Map.put(state, :socket_stopped?, true)
    assert [line] = upstream_close_lines(log)
    assert line =~ "websocket downstream closed after upstream connection close reason_code=peer_close_frame lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off codex_session_id=#{@codex_session_id}"
  end

  test "the latched socket closes 1001 once its last task is gone", ctx do
    state = settling_state(ctx)
    {:ok, latched} = CodexResponsesSocket.handle_info(ctx.signal, state)
    monitor = latched.task_monitors[ctx.task]

    assert {{:stop, :normal, @upstream_close_detail, stopped}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info({:DOWN, monitor, :process, ctx.task, :normal}, latched) end)

    refute Map.has_key?(stopped, :upstream_close_pending)
    assert MapSet.size(stopped.tasks) == 0
    assert stopped.socket_stopped?
    assert [line] = upstream_close_lines(log)
    assert line =~ "websocket downstream closed after upstream connection close reason_code=peer_close_frame lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off"
  end

  # Answers the socket already owes the client ride out ahead of the close.
  test "pending answers go out ahead of the 1001 close", ctx do
    state = settling_state(ctx)
    {:ok, latched} = CodexResponsesSocket.handle_info(ctx.signal, state)
    latched = Map.put(latched, :discarded_submission_terminals, [{:text, "synthetic owed answer"}])
    monitor = latched.task_monitors[ctx.task]

    assert {:stop, :normal, @upstream_close_detail, [{:text, "synthetic owed answer"}], stopped} =
             CodexResponsesSocket.handle_info({:DOWN, monitor, :process, ctx.task, :normal}, latched)

    refute Map.has_key?(stopped, :upstream_close_pending)
  end

  for {skip_reason, change} <- [
        busy: :unaccepted_task,
        queued: :queued_frame,
        public_route: :public_route,
        handoff: :pending_handoff,
        reconnect: :active_turn_reconnect,
        no_completed_response: :no_completed_response,
        revoked: :revoked
      ] do
    @skip_reason skip_reason
    @change change

    test "a native socket keeps its connection open with skip_reason=#{skip_reason}", ctx do
      state = ctx |> settling_state() |> change_state(@change, ctx)

      assert {{:ok, unchanged}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(ctx.signal, state) end)

      assert unchanged == state
      assert [line] = upstream_close_lines(log)
      assert line =~ "websocket downstream kept open after upstream connection close reason_code=peer_close_frame skip_reason=#{@skip_reason} lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off codex_session_id=#{@codex_session_id}"
    end
  end

  test "a signal from another session, or naming a cause that ends no anchor, changes nothing", ctx do
    state = native_state(ctx)
    {:upstream_websocket_connection_closed, _session, signal} = ctx.signal

    {results, log} =
      with_info_log(fn ->
        [
          CodexResponsesSocket.handle_info({:upstream_websocket_connection_closed, spawn_quiet_process(), signal}, state),
          CodexResponsesSocket.handle_info({:upstream_websocket_connection_closed, ctx.session, %{signal | cause: :request_key_changed}}, state),
          CodexResponsesSocket.handle_info({:upstream_websocket_connection_closed, ctx.session, Map.delete(signal, :generation)}, state)
        ]
      end)

    assert results == [{:ok, state}, {:ok, state}, {:ok, state}]
    assert upstream_close_lines(log) == []
  end

  # A frame from the client after the latch shows it is not idle: the latch
  # goes, with a line, before the frame is handled (here a binary frame, which
  # the socket refuses).
  test "a client frame drops the latch", ctx do
    {:ok, latched} = CodexResponsesSocket.handle_info(ctx.signal, settling_state(ctx))

    assert {{:stop, :unsupported_binary_frame, {1003, _reason}, stopped}, log} =
             with_info_log(fn -> CodexResponsesSocket.handle_in({<<0>>, [opcode: :binary]}, latched) end)

    refute Map.has_key?(stopped, :upstream_close_pending)
    assert [line] = upstream_close_lines(log)
    assert line =~ "websocket downstream kept open after upstream connection close reason_code=peer_close_frame skip_reason=client_frame lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off"
  end

  # A key revoked after the latch: the revocation drops the latch and closes
  # 1008 once the socket is idle; the upstream close never turns into 1001.
  test "a revocation after the latch wins with 1008", ctx do
    pool_id = Ecto.UUID.generate()
    api_key_id = Ecto.UUID.generate()
    state = ctx |> settling_state() |> Map.merge(%{api_key_pool_id: pool_id, api_key_id: api_key_id, api_key_runtime_epoch: 4, api_key_revoked?: false, api_key_close_sent?: false})
    {:ok, latched} = CodexResponsesSocket.handle_info(ctx.signal, state)

    event =
      %Events.Event{
        version: 1,
        id: Ecto.UUID.generate(),
        pool_id: pool_id,
        topics: ["pools"],
        reason: "api_key_paused",
        emitted_at: DateTime.utc_now(),
        payload: %{"api_key_id" => api_key_id, "status" => "paused", "runtime_revocation_epoch" => 5}
      }

    assert {{:ok, revoked}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info({Events, event}, latched) end)
    assert revoked.api_key_revoked?
    refute Map.has_key?(revoked, :upstream_close_pending)
    assert [line] = upstream_close_lines(log)
    assert line =~ "websocket downstream kept open after upstream connection close reason_code=peer_close_frame skip_reason=revoked lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off"

    monitor = revoked.task_monitors[ctx.task]
    assert {:stop, :normal, {1008, "api key is no longer active"}, _stopped} = CodexResponsesSocket.handle_info({:DOWN, monitor, :process, ctx.task, :normal}, revoked)
  end

  # A native socket whose last pushed terminal completed a native response,
  # with no task, queue or owner work under way.
  defp native_state(ctx) do
    %{
      auth: nil,
      opts: RequestOptions.for_websocket(%{}),
      codex_session: %{id: @codex_session_id},
      upstream_websocket_session: ctx.session,
      request_response_work_started?: true,
      tasks: MapSet.new(),
      task_monitors: %{},
      queued_response_payloads: :queue.new(),
      public_response_task_pid: nil,
      websocket_owner_pending_handoff: nil,
      response_task_terminals_accepted: MapSet.new(),
      direct_cleanup_contexts: %{},
      last_completed_native_response: %{semantic_turn_key: <<1::256>>, response_digest: <<2::256>>}
    }
  end

  # The same socket while that turn's task still settles: tracked, its
  # terminal accepted.
  defp settling_state(ctx) do
    ctx
    |> native_state()
    |> Map.merge(%{
      tasks: MapSet.new([ctx.task]),
      task_monitors: %{ctx.task => make_ref()},
      response_task_terminals_accepted: MapSet.new([ctx.task])
    })
  end

  defp change_state(state, :unaccepted_task, _ctx) do
    newer = spawn_quiet_process()
    %{state | tasks: MapSet.put(state.tasks, newer), task_monitors: Map.put(state.task_monitors, newer, make_ref())}
  end

  defp change_state(state, :queued_frame, _ctx), do: %{state | queued_response_payloads: :queue.in(:synthetic_queued_frame, :queue.new())}
  defp change_state(state, :public_route, _ctx), do: %{state | opts: RequestOptions.for_websocket(%{public_openai_responses_stream: true})}
  defp change_state(state, :pending_handoff, _ctx), do: %{state | websocket_owner_pending_handoff: %{synthetic: :handoff}}
  defp change_state(state, :active_turn_reconnect, _ctx), do: Map.put(state, :websocket_owner_active_turn_reconnect?, true)
  defp change_state(state, :no_completed_response, _ctx), do: Map.delete(state, :last_completed_native_response)
  defp change_state(state, :revoked, _ctx), do: Map.merge(state, %{api_key_revoked?: true, api_key_close_sent?: false})

  # One line per decision: a signal the socket does not act on is logged once,
  # and a close once.
  defp upstream_close_lines(log), do: log |> String.split("\n") |> Enum.filter(&(&1 =~ "after upstream connection close"))

  defp spawn_quiet_process do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> send(pid, :stop) end)
    pid
  end
end
