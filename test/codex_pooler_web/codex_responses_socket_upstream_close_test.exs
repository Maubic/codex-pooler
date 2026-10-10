defmodule CodexPoolerWeb.CodexResponsesSocketUpstreamCloseTest do
  # The native socket's handling of its upstream session's close signal
  # (findings#270), driven through the WebSock callbacks with hand-built
  # socket states: when it latches, when it keeps the socket open and why, and
  # when the latch turns into the 1001 close. The listener-level behaviour is
  # in `backend_codex_websocket/upstream_close_downstream_test.exs`.
  use CodexPooler.DataCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  alias CodexPooler.Events
  alias CodexPooler.Gateway.Payloads.{NativeCodexTurnMetadata, RequestOptions}
  alias CodexPooler.Gateway.Transports.Streaming.PreparedWebsocketFrame
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

  test "actual peer authority survives a dead matching lane and preserves its reason", ctx do
    {lane, monitor} = spawn_monitor(fn -> :ok end)
    assert_receive {:DOWN, ^monitor, :process, ^lane, :normal}
    state = Map.put(native_state(ctx), :native_response_steering, lane)
    {tag, session, signal} = ctx.signal
    signal = Map.put(signal, :steering_peer_close, {lane, 1000, "synthetic peer close"})

    assert {:stop, :normal, {1000, "synthetic peer close"}, _stopped} = CodexResponsesSocket.handle_info({tag, session, signal}, state)
  end

  test "a mismatched or malformed steering close retains normal idle 1001", ctx do
    lane = spawn_quiet_process()
    state = Map.put(native_state(ctx), :native_response_steering, lane)
    {tag, session, signal} = ctx.signal

    for detail <- [{self(), 1000, ""}, {lane, 999, ""}, {lane, 1000, <<255>>}, {lane, 1000, String.duplicate("x", 124)}] do
      assert {:stop, :normal, @upstream_close_detail, _stopped} = CodexResponsesSocket.handle_info({tag, session, Map.put(signal, :steering_peer_close, detail)}, state)
    end
  end

  test "known revocation wins over both copies of the steering peer close", ctx do
    lane = spawn_quiet_process()
    state = native_state(ctx) |> Map.merge(%{native_response_steering: lane, api_key_revoked?: true, api_key_close_sent?: false})
    {tag, session, signal} = ctx.signal
    signal = Map.put(signal, :steering_peer_close, {lane, 1000, ""})

    assert {:stop, :normal, {1008, "api key is no longer active"}, _stopped} = CodexResponsesSocket.handle_info({tag, session, signal}, state)
    assert {:stop, :normal, {1008, "api key is no longer active"}, _stopped} = CodexResponsesSocket.handle_info({:native_response_steering_close, lane, 1000, ""}, state)
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

  # A request queued behind the settling turn and anchored on the closed
  # connection's response can only meet the continuation guard's refusal
  # (findings#270 row 270-302): the signal latches the close, the queue does
  # not move while it is pending, and the close takes the request's place
  # once the socket's last task is gone.
  test "a queued request anchored on the closed connection's response latches the close, which takes its place", ctx do
    anchor = "resp_upstream_close_unit_anchor"
    state = ctx |> settling_state() |> anchored_on(anchor) |> Map.put(:queued_response_payloads, :queue.from_list([queued_request(anchor)]))

    assert {{:ok, latched}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(ctx.signal, state) end)

    assert latched == Map.put(state, :upstream_close_pending, %{cause: :peer_close_frame, lifecycle_id: ctx.lifecycle_id, generation: 3, forwarding: :off})
    assert upstream_close_lines(log) == []

    monitor = latched.task_monitors[ctx.task]

    assert {{:stop, :normal, @upstream_close_detail, stopped}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info({:DOWN, monitor, :process, ctx.task, :normal}, latched) end)

    assert :queue.is_empty(stopped.queued_response_payloads)
    refute Map.has_key?(stopped, :upstream_close_pending)
    assert [line] = upstream_close_lines(log)

    assert line =~
             "websocket downstream closed after upstream connection close reason_code=peer_close_frame lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off codex_session_id=#{@codex_session_id} queued_request=closed_anchor"
  end

  # A queued request anchored elsewhere can be served on a fresh connection, so
  # it keeps the socket open, as any queued request did.
  test "a queued request anchored on another response keeps the socket open with skip_reason=queued", ctx do
    state = ctx |> settling_state() |> anchored_on("resp_upstream_close_unit_anchor") |> Map.put(:queued_response_payloads, :queue.from_list([queued_request("resp_upstream_close_unit_other")]))

    assert {{:ok, unchanged}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(ctx.signal, state) end)

    assert unchanged == state
    assert [line] = upstream_close_lines(log)
    assert line =~ "websocket downstream kept open after upstream connection close reason_code=peer_close_frame skip_reason=queued lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=off"
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

  # With owner forwarding on the websocket owner holds the upstream session
  # and relays the same facts to its attached downstream as
  # `{:websocket_owner_upstream_closed, correlation_id, epoch, signal}`; the
  # socket takes it only for its own owner binding and decides on its own
  # state exactly as with forwarding off.
  describe "the owner's word with owner forwarding on" do
    test "a socket still settling its completed turn latches it for its own binding", ctx do
      state = owner_settling_state(ctx)

      assert {{:ok, latched}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(owner_word(ctx, state), state) end)

      assert latched == Map.put(state, :upstream_close_pending, %{cause: :peer_close_frame, lifecycle_id: ctx.lifecycle_id, generation: 3, forwarding: :on})
      assert upstream_close_lines(log) == []
    end

    test "an idle socket closes 1001 at once", ctx do
      state = owner_state(ctx)

      assert {{:stop, :normal, @upstream_close_detail, stopped}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(owner_word(ctx, state), state) end)

      assert stopped == Map.put(state, :socket_stopped?, true)
      assert [line] = upstream_close_lines(log)
      assert line =~ "websocket downstream closed after upstream connection close reason_code=peer_close_frame lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=on codex_session_id=#{@codex_session_id}"
    end

    test "the latched socket closes 1001 once its last task is gone", ctx do
      state = owner_settling_state(ctx)
      {:ok, latched} = CodexResponsesSocket.handle_info(owner_word(ctx, state), state)
      monitor = latched.task_monitors[ctx.task]

      assert {{:stop, :normal, @upstream_close_detail, stopped}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info({:DOWN, monitor, :process, ctx.task, :normal}, latched) end)

      refute Map.has_key?(stopped, :upstream_close_pending)
      assert [line] = upstream_close_lines(log)
      assert line =~ "websocket downstream closed after upstream connection close reason_code=peer_close_frame lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=on"
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

      test "a socket keeps its connection open with skip_reason=#{skip_reason}", ctx do
        state = ctx |> owner_settling_state() |> change_state(@change, ctx)

        assert {{:ok, unchanged}, log} = with_info_log(fn -> CodexResponsesSocket.handle_info(owner_word(ctx, state), state) end)

        assert unchanged == state
        assert [line] = upstream_close_lines(log)
        assert line =~ "websocket downstream kept open after upstream connection close reason_code=peer_close_frame skip_reason=#{@skip_reason} lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=on codex_session_id=#{@codex_session_id}"
      end
    end

    # The owner decided for the binding it held; a socket bound to the owner
    # anew since (another epoch or correlation id) keeps its connection and
    # names why.
    test "the word for another binding keeps the socket open with skip_reason=stale_downstream", ctx do
      state = owner_state(ctx)
      %{correlation_id: correlation_id, epoch: epoch} = state.websocket_owner_downstream

      {results, log} =
        with_info_log(fn ->
          for {word_correlation_id, word_epoch} <- [{correlation_id, epoch - 1}, {correlation_id, epoch + 1}, {"corr-another-socket", epoch}] do
            CodexResponsesSocket.handle_info(owner_word(ctx, word_correlation_id, word_epoch), state)
          end
        end)

      assert results == [{:ok, state}, {:ok, state}, {:ok, state}]
      lines = upstream_close_lines(log)
      assert length(lines) == 3

      for line <- lines,
          do: assert(line =~ "websocket downstream kept open after upstream connection close reason_code=peer_close_frame skip_reason=stale_downstream lifecycle_id=#{ctx.lifecycle_id} generation=3 forwarding=on codex_session_id=#{@codex_session_id}")
    end

    test "a malformed word, or one reaching a socket without an owner, changes nothing", ctx do
      state = owner_state(ctx)
      %{correlation_id: correlation_id, epoch: epoch} = state.websocket_owner_downstream
      signal = %{cause: :peer_close_frame, lifecycle_id: ctx.lifecycle_id, generation: 3}
      forwarding_off = native_state(ctx)

      {results, log} =
        with_info_log(fn ->
          [
            CodexResponsesSocket.handle_info({:websocket_owner_upstream_closed, correlation_id, epoch, %{signal | cause: :request_key_changed}}, state),
            CodexResponsesSocket.handle_info({:websocket_owner_upstream_closed, correlation_id, epoch, Map.put(signal, :connection_requests, 2)}, state),
            CodexResponsesSocket.handle_info({:websocket_owner_upstream_closed, correlation_id, epoch, %{signal | lifecycle_id: "not-a-lifecycle"}}, state),
            CodexResponsesSocket.handle_info({:websocket_owner_upstream_closed, correlation_id, epoch, signal}, forwarding_off)
          ]
        end)

      assert results == [{:ok, state}, {:ok, state}, {:ok, state}, {:ok, forwarding_off}]
      assert upstream_close_lines(log) == []
    end

    # A socket of the earlier release has no clause for the word: this socket
    # without it (the published socket differs from it by the forwarding-off
    # latch and this clause, and routes every unknown message to the same
    # catch-all) drops the word and stays as it was, with no line and no stop.
    test "a socket of an earlier release drops the word", ctx do
      earlier_release_socket = earlier_release_socket!()
      state = owner_state(ctx)

      assert {result, log} = with_info_log(fn -> earlier_release_socket.handle_info(owner_word(ctx, state), state) end)

      assert result == {:ok, state}
      assert upstream_close_lines(log) == []
    end
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

  # The same idle socket with owner forwarding on: the owner holds the
  # upstream session, so the socket has none, and its owner binding names
  # this downstream.
  defp owner_state(ctx) do
    ctx
    |> native_state()
    |> Map.delete(:upstream_websocket_session)
    |> Map.merge(%{
      websocket_owner_lease_token: Ecto.UUID.generate(),
      websocket_owner_downstream: %{pid: self(), epoch: 2, correlation_id: "corr-upstream-close", active_turn_reconnect?: false}
    })
  end

  defp owner_settling_state(ctx) do
    ctx
    |> owner_state()
    |> Map.merge(%{
      tasks: MapSet.new([ctx.task]),
      task_monitors: %{ctx.task => make_ref()},
      response_task_terminals_accepted: MapSet.new([ctx.task])
    })
  end

  defp owner_word(ctx, %{websocket_owner_downstream: %{correlation_id: correlation_id, epoch: epoch}}), do: owner_word(ctx, correlation_id, epoch)

  defp owner_word(ctx, correlation_id, epoch),
    do: {:websocket_owner_upstream_closed, correlation_id, epoch, %{cause: :peer_close_frame, lifecycle_id: ctx.lifecycle_id, generation: 3}}

  # This socket compiled without its clause for the owner's word, under
  # another module name so the running module is never replaced.
  @earlier_release_socket CodexPoolerWeb.CodexResponsesSocketWithoutUpstreamClosedWord

  # Returns the module the load answered: the test calls it through that
  # value, since a module the test compiles at run time is unknown to the
  # compiler.
  defp earlier_release_socket! do
    {:ok, {CodexResponsesSocket, [abstract_code: {:raw_abstract_v1, forms}]}} =
      CodexResponsesSocket |> :code.which() |> :beam_lib.chunks([:abstract_code])

    {forms, dropped} = Enum.map_reduce(forms, 0, &without_upstream_closed_word/2)
    assert dropped == 1
    {:ok, module, binary} = :compile.forms(forms, [:binary, :return_errors])
    # A rerun in the same VM replaces the copy an earlier run loaded.
    _purged = :code.purge(module)
    {:module, loaded} = :code.load_binary(module, ~c"earlier_release_socket", binary)
    loaded
  end

  defp without_upstream_closed_word({:attribute, line, :module, CodexResponsesSocket}, dropped),
    do: {{:attribute, line, :module, @earlier_release_socket}, dropped}

  defp without_upstream_closed_word({:function, line, :handle_socket_info, 2, clauses}, dropped) do
    kept = Enum.reject(clauses, &upstream_closed_word_clause?/1)
    {{:function, line, :handle_socket_info, 2, kept}, dropped + length(clauses) - length(kept)}
  end

  defp without_upstream_closed_word(form, dropped), do: {form, dropped}

  defp upstream_closed_word_clause?({:clause, _line, [pattern, _state], _guards, _body}), do: upstream_closed_word_pattern?(pattern)

  defp upstream_closed_word_pattern?({:match, _line, left, right}), do: upstream_closed_word_pattern?(left) or upstream_closed_word_pattern?(right)
  defp upstream_closed_word_pattern?({:tuple, _line, [{:atom, _, :websocket_owner_upstream_closed} | _rest]}), do: true
  defp upstream_closed_word_pattern?(_pattern), do: false

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

  # The socket's last completed response, the one the released client anchors on.
  defp anchored_on(state, response_id),
    do: %{state | last_completed_native_response: %{semantic_turn_key: <<1::256>>, response_digest: NativeCodexTurnMetadata.response_id_digest(response_id)}}

  # A prepared request queued behind the settling turn, anchored on `response_id`
  # (no capability: the drop's release answers `:invalid` and is ignored).
  defp queued_request(response_id) do
    %PreparedWebsocketFrame{
      variant: :native_response_create,
      endpoint: "/backend-api/codex/responses",
      payload: %{"type" => "response.create", "previous_response_id" => response_id},
      request_options: RequestOptions.for_websocket(%{})
    }
  end

  # One line per decision: a signal the socket does not act on is logged once,
  # and a close once.
  defp upstream_close_lines(log), do: log |> String.split("\n") |> Enum.filter(&(&1 =~ "after upstream connection close"))

  defp spawn_quiet_process do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> send(pid, :stop) end)
    pid
  end
end
