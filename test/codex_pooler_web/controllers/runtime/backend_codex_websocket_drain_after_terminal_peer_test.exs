defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketDrainAfterTerminalPeerTest do
  # The arms of `backend_codex_websocket_drain_after_terminal_test.exs`
  # (findings#287) with the session's owner and its provider connection on a
  # second VM sharing the committed database, the socket on this node: the
  # socket node's drain leaves its proxy task, whose terminal went out, to
  # settle; the owner's own drain waits for the settlement of the turn it
  # forwarded. The module boots that VM once: booting it and warming its first
  # turn inside each test pushed an arm past the six-second budget on a busy
  # machine (findings#270 row 270-323). Each test starts its own session's
  # owner on it.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full), owner forwarding on, the session's owner on the peer VM.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [receive_frames_until_close!: 3]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0]

  import CodexPoolerWeb.Runtime.DrainAfterTerminalScenario

  @moduletag capture_log: true

  @drained_close {:close, 1001, "websocket owner is draining"}

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  test "owner on another VM: the socket node's drain leaves the proxy task whose terminal went out to settle as answered", %{peer_node: peer_node} do
    put_owner_forwarding!(true)
    enter_peer_owner_topology!()
    turn = relayed_turn_held!(start_turn!(peer_node: peer_node))

    assert node(turn.owner) == peer_node
    assert_task_drain_waits_for_settlement!(turn, :proxy)
    assert receive_frames_for!(turn.client, 300) == []
    drop!(turn.client)
    assert_settled_as_answered!(turn.setup)
  end

  test "owner on another VM: the cut waits for the forwarded turn's settlement, and nothing follows its terminal", %{peer_node: peer_node} do
    put_owner_forwarding!(true)
    enter_peer_owner_topology!()
    turn = relayed_turn_held!(start_turn!(peer_node: peer_node))

    assert node(turn.owner) == peer_node
    assert_owner_cut_waits_for_settlement!(turn, turn.owner)
    assert receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2) == [@drained_close]
    assert_settled_as_answered!(turn.setup)
  end
end
