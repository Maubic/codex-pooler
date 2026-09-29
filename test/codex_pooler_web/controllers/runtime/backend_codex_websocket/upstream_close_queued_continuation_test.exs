defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.UpstreamCloseQueuedContinuationTest do
  # The provider closes the upstream connection a native turn's response came
  # from, and the client's next request, a tool output anchored on that
  # response, already waits at the socket behind the turn's settlement
  # (findings#270 row 270-302). The socket used to drop the close at once for
  # the queued request and dispatch it afterwards into a certain refusal: the
  # continuation guard answered `previous_response_not_found` on the fresh
  # connection, the request settled failed, and the client resent the whole
  # turn on a new socket. The socket now keeps the close pending behind the
  # queued request and, when it dequeues a request anchored on the closed
  # connection's last completed response, closes 1001 instead of dispatching
  # it, as for an idle socket. The client resends the whole turn on a new
  # socket either way.
  #
  # One node, owner forwarding off and on (the session's owner on this node),
  # native websocket `/backend-api/codex/responses`, the Pool's serving mode
  # forced to Full and to Lite, FakeUpstream closing the connection after the
  # turn's terminal, both held until released, the released client's turn
  # frames (turn metadata naming thread and turn), synthetic text. The turn's
  # settlement is held so the tool output queues, as a tool faster than the
  # settlement does.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @detection_timeout_ms 15_000

  @upstream_close {:close, 1001, "upstream connection closed"}
  @turn_path "/backend-api/codex/responses"
  @tool_call %{"type" => "function_call", "call_id" => "call_queued_continuation_sample", "name" => "sample_lookup", "arguments" => "{}"}
  @tool_output %{"type" => "function_call_output", "call_id" => "call_queued_continuation_sample", "output" => "sample output"}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    :ok
  end

  for forwarding <- [:off, :on], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, serving_mode: mode
    test "forwarding #{forwarding}, #{mode}: a tool output queued when the provider closes its connection closes the socket 1001, and the whole resend is served", ctx do
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, ctx.forwarding == :on)
      hold = hold_settled_websocket_turn!()
      release_ref = make_ref()
      first_input = native_text_input("queued continuation")

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (a close 1000 after a turn that asked for a tool, the tool output queued behind the turn's settlement)
          FakeUpstream.strict_sequence([
            anchorless_request(1, FakeUpstream.websocket_terminal_then_close_barrier(completed_terminal_frame("resp_queued_continuation"), code: 1000, reason: "synthetic age limit", notify: self(), release_ref: release_ref)),
            anchorless_request(nil, completed_response_frames("resp_queued_continuation_resend", [], 4, 3))
          ])
        )

      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, ctx.serving_mode)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      {_server, port} = start_public_endpoint_with_server!()
      thread = "queued-continuation-#{System.unique_integer([:positive])}"
      client = connect!(port, setup, thread, ctx.forwarding)
      frame = released_client_frame(setup, thread)
      turn_id = Ecto.UUID.generate()

      {frames, log} =
        with_info_log(fn ->
          {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame.(first_input, turn_id, %{}))
          assert_receive {:fake_upstream_websocket_barrier, :before_terminal, handler, ^release_ref}, @detection_timeout_ms
          send(handler, {:fake_upstream_release_websocket, release_ref})
          {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, client.ref)
          assert %{"type" => "response.completed", "response" => %{"id" => "resp_queued_continuation"}} = terminal
          assert_receive {^hold, :held, task}, @detection_timeout_ms

          # The tool output, anchored on that response, waits behind the settlement.
          {conn, websocket} = public_websocket_send_text!(conn, websocket, client.ref, frame.([@tool_output], turn_id, %{"previous_response_id" => "resp_queued_continuation"}))
          _queued = await_socket_connection_state!(client.socket, &(:queue.len(Map.get(&1, :queued_response_payloads, :queue.new())) == 1))

          # Then the provider closes the connection, and the socket hears it.
          assert_receive {:fake_upstream_websocket_barrier, :before_close, ^handler, ^release_ref}, @detection_timeout_ms
          send(handler, {:fake_upstream_release_websocket, release_ref})
          :ok = await_session_disconnected!(client.session)
          :ok = await_close_signal_handled!(client)

          :ok = release_settled_websocket_turn(hold, task)
          {conn, _websocket, frames} = next_client_frames!(conn, websocket, client.ref)
          Mint.HTTP.close(conn)
          :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket, @detection_timeout_ms)
          frames
        end)

      assert frames == [@upstream_close]
      assert_upstream_close_lines!(log, [downstream_closed_line("peer_close_frame", client.lifecycle_id, 1, Atom.to_string(ctx.forwarding))])
      assert log =~ "queued_request=closed_anchor"

      # The client's whole resend on a new socket is served on a new connection.
      retry = connect!(port, setup, thread, ctx.forwarding)
      {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, frame.(first_input ++ [@tool_call, @tool_output], turn_id, %{}))
      {conn, _websocket, resend} = receive_native_terminal!(conn, websocket, retry.ref)
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_queued_continuation_resend"}} = resend
      Mint.HTTP.close(conn)

      # No refused request, no connection opened for it.
      assert [opener, served] = await_settled!(setup, 2)
      assert {opener.status, served.status} == {"succeeded", "succeeded"}
      assert FakeUpstream.websocket_connection_count(upstream) == 2
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  # Opens a native socket keyed by `thread` (its turn state) and returns the
  # client connection with the socket's connection process, the upstream
  # session that serves it (the socket's own with forwarding off, its owner's
  # with forwarding on) and that session's lifecycle id.
  defp connect!(port, setup, thread, forwarding) do
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread, @turn_path)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    state = socket_connection_state!(socket)

    {owner, session} =
      case forwarding do
        :off ->
          {nil, state.upstream_websocket_session}

        :on ->
          assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
          {owner, :sys.get_state(owner).upstream_pid}
      end

    %{lifecycle_id: lifecycle_id} = :sys.get_state(session)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket, owner: owner, session: session, lifecycle_id: lifecycle_id}
  end

  # The session's close signal is in its subscriber's mailbox once it holds no
  # connection; with forwarding on the owner relays it to the socket. Each
  # `:sys.get_state/1` returns after the process handled what came before.
  defp await_close_signal_handled!(%{owner: owner, socket: socket}) do
    if owner, do: _owner = await_owner_relay!(owner)
    _socket = :sys.get_state(socket)
    :ok
  end

  # The owner holds a close while its turn has not completed and relays it
  # after the turn's `:complete`.
  defp await_owner_relay!(owner, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    state = :sys.get_state(owner)

    cond do
      not match?(%{active_turn: %{upstream_close: _held}}, state) ->
        state

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          5 -> await_owner_relay!(owner, deadline)
        end

      true ->
        flunk("the owner kept the close")
    end
  end

  # The next frames the client reads: through a terminal event or a Close.
  defp next_client_frames!(conn, websocket, ref, frames \\ []) do
    {conn, websocket, read} = receive_websocket_frames!(conn, websocket, ref)
    frames = frames ++ Enum.map(read, &decode_frame/1)

    if Enum.any?(frames, &(match?({:close, _code, _reason}, &1) or match?(%{"type" => type} when type in ["error", "response.completed", "response.failed"], &1))),
      do: {conn, websocket, frames},
      else: next_client_frames!(conn, websocket, ref, frames)
  end

  defp decode_frame({:text, text}), do: CodexPooler.JSON.decode!(text)
  defp decode_frame(other), do: other

  defp receive_websocket_frames!(conn, websocket, ref) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "the client read nothing")
    {:ok, conn, responses} = Mint.WebSocket.stream(conn, message)

    {websocket, frames} =
      Enum.reduce(responses, {websocket, []}, fn
        {:data, ^ref, data}, {websocket, frames} ->
          {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
          {websocket, frames ++ decoded}

        _response, acc ->
          acc
      end)

    {conn, websocket, Enum.reject(frames, &(match?({:ping, _}, &1) or metadata_control_frame?(&1)))}
  end

  defp completed_terminal_frame(response_id) do
    [terminal] = response_id |> completed_response_events([@tool_call], 2, 1) |> Enum.take(-1)
    CodexPooler.JSON.encode!(terminal)
  end

  defp await_settled!(setup, count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))

    cond do
      length(requests) >= count and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          10 -> await_settled!(setup, count, deadline)
        end

      true ->
        flunk("the Pool's requests did not settle: #{inspect(Enum.map(requests, &{&1.status, &1.last_error_code}))}")
    end
  end
end
