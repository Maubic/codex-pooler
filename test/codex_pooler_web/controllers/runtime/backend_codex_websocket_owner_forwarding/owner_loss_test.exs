defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.OwnerLossTest do
  # An owner-forwarded socket whose websocket owner on its own node goes away
  # between two requests (findings#276). An owner that exits without crashing
  # (drained or stopped) took every response the client could anchor on with
  # its upstream connection: the idle native socket closes 1001 and the
  # client's next request goes out whole on a new socket, which takes the
  # session over and is served; a public `/v1` socket stays open and takes the
  # session over before its next request reaches an owner. Both used to stay
  # open and answer every later request `503 owner_unavailable` until the
  # client reconnected. An owner that crashed closes the idle socket 1011, as
  # before. The remote owner's arms are `remote_owner_loss_test.exs`.
  #
  # One node: the real public listener, owner forwarding on, the session's
  # owner on this node, the Pool's default serving mode, FakeUpstream; the
  # released client's native frames and a public `/v1` SDK request.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [assert_quiet_close!: 1, socket_connection_state!: 1, with_info_log: 1]

  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario

  @moduletag capture_log: true

  @drained_close {1001, "websocket owner is draining"}
  @crashed_close {1011, "websocket owner crashed"}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for loss <- [:owner_drained, :owner_stopped] do
    @tag loss: loss
    test "native: after its owner #{loss}, the idle socket closes 1001 and the client's new socket is served", ctx do
      %{setup: setup, port: port, window: window, client: client, owner: owner, session_id: session_id} = owner_socket!(Scenario.native_route(), "resp_owner_loss")

      {:ok, log} =
        with_info_log(fn ->
          assert Scenario.lose_owner!(owner, ctx.loss) == :normal
          Scenario.assert_closed!(client, @drained_close)
        end)

      assert Scenario.owner_exit_lines(log) == ["websocket downstream closed after owner exit reason_code=owner_drained owner=local codex_session_id=#{session_id}"]
      assert_quiet_close!(log)

      Scenario.assert_new_socket_served!(port, setup, Scenario.native_route(), window, session_id, "resp_owner_loss_next")
      assert Scenario.settled_statuses!(setup, 2) == ["succeeded", "succeeded"]
    end

    @tag loss: loss
    test "/v1: after its owner #{loss}, the socket stays open and takes the session over for its next requests", ctx do
      %{setup: setup, client: client, owner: owner, session_id: session_id} = owner_socket!(Scenario.public_route(), "resp_owner_loss")

      {{client, lost}, log} =
        with_info_log(fn ->
          assert Scenario.lose_owner!(owner, ctx.loss) == :normal
          Scenario.await_open_without_owner!(client)
        end)

      assert Scenario.owner_exit_lines(log) == ["websocket downstream kept open after owner exit reason_code=owner_drained skip_reason=public_route owner=local codex_session_id=#{session_id}"]

      client = Scenario.assert_next_requests_served!(client, setup, lost, "resp_owner_loss_next")
      Scenario.close!(client)
      assert Scenario.settled_statuses!(setup, 3) == ["succeeded", "succeeded", "succeeded"]
    end
  end

  for route <- [Scenario.native_route(), Scenario.public_route()], {loss, reason} <- [owner_killed: :killed, upstream_killed: :owner_crashed] do
    @tag route: route, loss: loss, reason: reason
    test "#{route}: an owner that crashed (#{loss}) closes the idle socket 1011", ctx do
      %{setup: setup, client: client, owner: owner, session_id: session_id} = owner_socket!(ctx.route, "resp_owner_crash")

      {:ok, log} =
        with_info_log(fn ->
          assert Scenario.lose_owner!(owner, ctx.loss) == ctx.reason
          Scenario.assert_closed!(client, @crashed_close)
        end)

      assert Scenario.owner_exit_lines(log) == ["websocket downstream closed after owner exit reason_code=owner_crashed owner=local codex_session_id=#{session_id}"]
      assert Scenario.settled_statuses!(setup, 1) == ["succeeded"]
    end
  end

  # A socket on `route` whose session's owner is on this node and monitored,
  # after one request it served.
  defp owner_socket!(route, prefix) do
    setup = gateway_setup(Scenario.upstream!(prefix))
    {_server, port} = start_public_endpoint_with_server!()
    window = Scenario.window()
    client = Scenario.connect!(port, setup, route, window)

    {client, one} = Scenario.turn!(client, setup, "before the owner left")
    assert Scenario.terminal(one) == {"response.completed", "#{prefix}_one"}
    state = socket_connection_state!(client.socket)
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    assert state.websocket_owner_pid == owner

    %{setup: setup, port: port, window: window, client: client, owner: owner, session_id: state.codex_session.id}
  end
end
