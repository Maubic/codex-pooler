defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.RemoteOwnerLossTest do
  # An owner-forwarded socket whose session's owner runs on the other node goes
  # on without it (findings#276), as production does whenever a turn lands on
  # the web pod that does not own its session. The socket never monitored a
  # remote owner, so after that owner went away (killed, its upstream
  # connection process killed, drained, stopped) every later request on the
  # socket met `503 owner_unavailable` in a few milliseconds, with no row and
  # nothing sent upstream, until the client reconnected; a new socket on the
  # session took it over and was served. Now the socket monitors the remote
  # owner and answers its exit as a local owner's: a crash closes the idle
  # socket 1011; any other exit closes an idle native socket 1001, so the
  # client's next request goes out whole on a new socket that takes the session
  # over, and leaves a public `/v1` socket open to take the session over before
  # its next request reaches an owner.
  #
  # Two BEAM nodes: this node runs the public listener and the sockets, a
  # second VM sharing the committed database runs the session's owner and its
  # provider connection (the module boots it once). Owner forwarding on, the
  # Pool's default serving mode, FakeUpstream; the released client's native
  # frames and a public `/v1` SDK request.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [assert_quiet_close!: 1, socket_connection_state!: 1, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario

  @moduletag capture_log: true

  @drained_close {1001, "websocket owner is draining"}
  @crashed_close {1011, "websocket owner crashed"}

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    enter_peer_owner_topology!()
    :ok
  end

  for loss <- [:owner_drained, :owner_stopped] do
    @tag loss: loss
    test "native: after its remote owner #{loss}, the idle socket closes 1001 and the client's new socket takes the session over to this node", ctx do
      %{setup: setup, port: port, window: window, client: client, peer_owner: peer_owner} = remote_owner_socket!(ctx, Scenario.native_route(), "resp_remote_owner_loss")

      {:ok, log} =
        with_info_log(fn ->
          assert Scenario.lose_owner!(peer_owner.owner_pid, ctx.loss) == :normal
          Scenario.assert_closed!(client, @drained_close)
        end)

      assert Scenario.owner_exit_lines(log) == ["websocket downstream closed after owner exit reason_code=owner_drained owner=remote codex_session_id=#{peer_owner.session.id}"]
      assert_quiet_close!(log)

      Scenario.assert_new_socket_served!(port, setup, Scenario.native_route(), window, peer_owner.session.id, "resp_remote_owner_loss_next")
      assert Scenario.settled_statuses!(setup, 2) == ["succeeded", "succeeded"]
    end

    @tag loss: loss
    test "/v1: after its remote owner #{loss}, the socket stays open and takes the session over to this node for its next requests", ctx do
      %{setup: setup, client: client, peer_owner: peer_owner} = remote_owner_socket!(ctx, Scenario.public_route(), "resp_remote_owner_loss")

      {{client, lost}, log} =
        with_info_log(fn ->
          assert Scenario.lose_owner!(peer_owner.owner_pid, ctx.loss) == :normal
          Scenario.await_open_without_owner!(client)
        end)

      assert Scenario.owner_exit_lines(log) == ["websocket downstream kept open after owner exit reason_code=owner_drained skip_reason=public_route owner=remote codex_session_id=#{peer_owner.session.id}"]

      client = Scenario.assert_next_requests_served!(client, setup, lost, "resp_remote_owner_loss_next")
      Scenario.close!(client)
      assert Scenario.settled_statuses!(setup, 3) == ["succeeded", "succeeded", "succeeded"]
    end
  end

  for route <- [Scenario.native_route(), Scenario.public_route()], {loss, reason} <- [owner_killed: :killed, upstream_killed: :owner_crashed] do
    @tag route: route, loss: loss, reason: reason
    test "#{route}: a remote owner that crashed (#{loss}) closes the idle socket 1011, and the client's new socket takes the session over", ctx do
      %{setup: setup, port: port, window: window, client: client, peer_owner: peer_owner} = remote_owner_socket!(ctx, ctx.route, "resp_remote_owner_crash")

      {:ok, log} =
        with_info_log(fn ->
          assert Scenario.lose_owner!(peer_owner.owner_pid, ctx.loss) == ctx.reason
          Scenario.assert_closed!(client, @crashed_close)
        end)

      assert Scenario.owner_exit_lines(log) == ["websocket downstream closed after owner exit reason_code=owner_crashed owner=remote codex_session_id=#{peer_owner.session.id}"]

      Scenario.assert_new_socket_served!(port, setup, ctx.route, window, peer_owner.session.id, "resp_remote_owner_crash_next")
      assert Scenario.settled_statuses!(setup, 2) == ["succeeded", "succeeded"]
    end
  end

  # The session's owner on the peer, a socket on `route` on this node attached
  # to it that monitors it, and one request it served.
  defp remote_owner_socket!(ctx, route, prefix) do
    setup = gateway_setup(Scenario.upstream!(prefix))
    window = Scenario.window()
    peer_owner = start_shared_peer_window_owner!(setup, window.id, ctx.peer_node)
    {_server, port} = start_public_endpoint_with_server!()
    client = Scenario.connect!(port, setup, route, window)

    {client, one} = Scenario.turn!(client, setup, "before the owner left")
    assert Scenario.terminal(one) == {"response.completed", "#{prefix}_one"}
    state = socket_connection_state!(client.socket)
    assert state.codex_session.id == peer_owner.session.id
    assert node(peer_owner.owner_pid) == ctx.peer_node
    assert state.websocket_owner_pid == peer_owner.owner_pid

    %{setup: setup, port: port, window: window, client: client, peer_owner: peer_owner}
  end
end
