defmodule CodexPoolerWeb.V1.ResponsesWebsocketForwardedPrevisibleInterruptionTest do
  # A public `/v1/responses` websocket turn whose upstream connection closes
  # after the request was sent ends with exactly one `error` event, with owner
  # forwarding off and on, and the socket serves the next turn. With
  # forwarding on the owner settles an interruption that showed the client
  # nothing without an error of its own (its output-commit probe says the
  # client saw nothing) and no retry follows once the request reached the
  # provider, so the socket answers the task's error itself; that turn used
  # to get no terminal at all (findings#272). An interruption after visible
  # output keeps the owner's error as the only terminal.
  #
  # One node, owner forwarding off and on, public `/v1/responses` websocket,
  # the Pool's default serving mode; FakeUpstream fails the first request
  # (1012 before any event, or a TCP drop after a text delta) and serves the
  # next one normally. The last test drives the socket callbacks with a
  # hand-built owner-forwarded public turn.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerContract
  alias CodexPooler.Gateway.Websocket.Adapter
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @terminal_types ["response.completed", "response.failed", "response.incomplete", "error"]

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    :ok
  end

  for forwarding <- [:off, :on] do
    @tag forwarding: forwarding
    test "an interruption before any event answers the public turn with one error with owner forwarding #{forwarding}", ctx do
      # provenance: synthetic_adversarial (a provider restart close after the request, before any event)
      first = FakeUpstream.websocket_sse_then_close([], code: 1012, reason: "synthetic restart")
      %{interrupted: interrupted} = interrupted_then_next_turn!(ctx.forwarding, first)

      assert [%{"type" => "error", "status" => 502, "error" => %{"code" => "upstream_request_failed"}}] = interrupted
    end

    @tag forwarding: forwarding
    test "an interruption after visible output answers the public turn with one error with owner forwarding #{forwarding}", ctx do
      # provenance: synthetic_adversarial (a TCP drop right after a text delta)
      first =
        FakeUpstream.websocket_text_frames_then_abrupt_close(
          Enum.map(
            [
              %{"type" => "response.created", "response" => %{"id" => "resp_previsible_interruption_visible", "status" => "in_progress"}},
              %{"type" => "response.output_text.delta", "delta" => "synthetic visible text"}
            ],
            &CodexPooler.JSON.encode!/1
          )
        )

      %{interrupted: interrupted} = interrupted_then_next_turn!(ctx.forwarding, first)

      assert ["response.created", "response.output_text.delta", "error"] = Enum.map(interrupted, & &1["type"])
      assert %{"status" => 502} = List.last(interrupted)
    end
  end

  # An owner error the client was sent stays the turn's only terminal when
  # the task then finishes with an error of its own.
  test "an owner error the client was sent is the only terminal of a public turn whose task then fails" do
    task = spawn_quiet_process()
    state = public_owner_turn_state(task)
    downstream = state.websocket_owner_downstream
    {:ok, owner_error} = WebsocketOwnerContract.safe_error_payload(:owner_forward_timeout, nil)

    assert {:push, {:text, pushed}, state} = CodexResponsesSocket.handle_info(owner_frame(downstream, task, {:error, :owner_forward_timeout, owner_error}), state)
    assert %{"type" => "error"} = CodexPooler.JSON.decode!(pushed)
    assert {:ok, state} = CodexResponsesSocket.handle_info(owner_frame(downstream, task, :complete), state)

    task_error = {:response_task_result, {:error, %{status: 502, code: "upstream_request_failed", message: "upstream request failed"}}}
    assert {:ok, state} = CodexResponsesSocket.handle_info({:codex_response_done, task, {:socket_response_result, :owner_completion_pending, task_error}}, state)
    assert is_nil(state.public_response_task_pid)
  end

  defp interrupted_then_next_turn!(forwarding, first) do
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding == :on)
    next = FakeUpstream.websocket_sse_then_close(completed_response_events("resp_previsible_interruption_next", [], 3, 2), code: 1000, reason: "synthetic close")
    upstream = start_upstream(FakeUpstream.repeat_last([first, next]))
    setup = gateway_setup(upstream)
    {_server, port} = start_public_endpoint_with_server!()
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, "", "/v1/responses")
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    create = fn text -> CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input(text), "stream" => true}) end

    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, create.("interrupted"))
    {conn, websocket, interrupted} = receive_turn_frames!(conn, websocket, ref, [])
    {conn, websocket} = await_turn_settled!(conn, websocket, ref, socket)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, create.("next"))
    {conn, websocket, next} = receive_turn_frames!(conn, websocket, ref, [])
    {conn, _websocket} = await_turn_settled!(conn, websocket, ref, socket)
    Mint.HTTP.close(conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_previsible_interruption_next"}} = List.last(next)
    refute Enum.any?(next, &(&1["type"] == "error"))
    assert FakeUpstream.count(upstream) == 2
    %{interrupted: interrupted}
  end

  # The socket settled the turn (its task is gone) and sent nothing after its
  # terminal: a frame pushed when the task finished reaches the client before
  # the barrier's pong.
  defp await_turn_settled!(conn, websocket, ref, socket) do
    _state = await_socket_connection_state!(socket, &(MapSet.size(&1.tasks) == 0))
    socket_transport_barrier!(conn, websocket, ref)
  end

  # Every frame of one turn, its terminal last.
  defp receive_turn_frames!(conn, websocket, ref, frames) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frames = frames ++ [CodexPooler.JSON.decode!(text)]

    if List.last(frames)["type"] in @terminal_types,
      do: {conn, websocket, frames},
      else: receive_turn_frames!(conn, websocket, ref, frames)
  end

  defp public_owner_turn_state(task) do
    %{
      opts: %{request_id: "ws-public-owner-error-once"} |> RequestOptions.for_websocket() |> RequestOptions.put_openai_compatibility(public_openai_responses_stream: true),
      tasks: MapSet.new([task]),
      task_monitors: %{},
      queued_response_payloads: :queue.new(),
      native_turn_output_task_pids: MapSet.new(),
      websocket_owner_downstream: %{pid: self(), epoch: 1, correlation_id: "correlation-public-owner-error-once"},
      websocket_owner_drain_observed?: false,
      websocket_owner_active_turn_reconnect?: false,
      connection_started_at_monotonic_ms: System.monotonic_time(:millisecond),
      public_response_task_pid: task,
      public_responses_websocket_state: Adapter.public_responses_turn_state(),
      public_turn_task_done?: false,
      public_turn_owner_complete?: false,
      public_turn_aborted?: false,
      public_turn_output_committed?: false
    }
  end

  defp owner_frame(downstream, owner_turn_id, payload),
    do: {:websocket_owner_frame, downstream.correlation_id, downstream.epoch, owner_turn_id, payload}

  defp spawn_quiet_process do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> send(pid, :stop) end)
    pid
  end
end
