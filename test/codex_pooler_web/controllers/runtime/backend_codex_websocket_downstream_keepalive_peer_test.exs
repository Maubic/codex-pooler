defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketDownstreamKeepalivePeerTest do
  # The arms of `backend_codex_websocket/downstream_keepalive_test.exs`
  # (findings#302) with the session's owner and its provider connection on a
  # second VM sharing the committed database, the socket on this node. The
  # proxy's response task waits for the owner's reply to the turn submission
  # under the remote turn budget, `max(receive timeout, websocket idle timeout)
  # + 1 s`, which was a total bound: a remote turn streaming past it was
  # abandoned `owner_forward_timeout` while its frames still reached the
  # client. The budget now counts from the latest frames the socket delivered.
  # The module boots the VM once (booting it per test pushed arms past the
  # six-second budget, findings#270 row 270-323); each test starts its own
  # session's owner on it.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full), owner forwarding on, the session's owner on the peer VM. On this
  # node the downstream idle bound is 0.5 s; the long turn's arm also takes the
  # upstream receive timeout to 0.4 s, so its remote turn budget is 1.5 s.
  use CodexPoolerWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0]

  import CodexPoolerWeb.Runtime.DownstreamKeepaliveScenario

  alias CodexPooler.FakeUpstream
  alias CodexPoolerWeb.WebsocketConnectionLogger

  @moduletag capture_log: true

  @idle_timeout_ms 500
  @receive_timeout_ms 400
  @remote_turn_budget_ms 1_500
  @frame_interval_ms 100

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup %{settings: settings} do
    :ok = put_settings!(settings, true)
    enter_peer_owner_topology!()
  end

  @tag settings: [websocket_idle_timeout_ms: @idle_timeout_ms, upstream_receive_timeout_ms: @receive_timeout_ms]
  @tag slow: "the remote turn budget is a real timer: the turn must stream past it to show its delivered frames renew it"
  test "owner on another VM: a turn streaming past the remote turn budget completes", %{peer_node: peer_node} do
    # Twenty-two frames 100 ms apart: about 2.2 s against the 1.5 s budget.
    deltas = 18
    upstream = start_upstream(FakeUpstream.delayed_sse_stream(stream_events("resp_keepalive_remote_long", deltas), interval_ms: @frame_interval_ms))
    setup = gateway_setup(upstream)

    {turn, log} = with_log(fn -> setup |> start_turn!(peer_node: peer_node) |> read_turn!(answer_pings?: true) end)

    assert node(turn.client.owner) == peer_node
    assert turn.end == {:terminal, "response.completed"}
    assert Enum.count(turn.events, &(&1["type"] == "response.output_text.delta")) == deltas
    assert turn.elapsed_ms > @remote_turn_budget_ms
    request = assert_request_settled!(setup, "succeeded", nil)
    assert %{"owner_instance_id" => owner_instance, "proxy_instance_id" => proxy_instance} = request.request_metadata["websocket_owner_forwarding"]
    assert owner_instance != proxy_instance
    refute log =~ "owner_forward_timeout"
    refute log =~ WebsocketConnectionLogger.downstream_idle_timeout_message()
    :ok = close_client!(turn.client)
  end

  # The upstream receive timeout keeps its default here: travelling with the
  # turn to the owner, a shorter one ends the stalled stream first (`502
  # server_error`), and the downstream idle bound would never be reached.
  @tag settings: [websocket_idle_timeout_ms: @idle_timeout_ms]
  @tag slow: "the idle bound is a real timer: the close lands one bound after the last pong"
  test "owner on another VM: a turn that stops delivering frames is still closed by the idle bound", %{peer_node: peer_node} do
    hold = make_ref()
    # provenance: synthetic_adversarial
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_request(FakeUpstream.barrier_websocket_frames(encoded_events("resp_keepalive_remote_stalled", 1), notify: self(), release_ref: hold))]))
    setup = gateway_setup(upstream)

    {turn, log} =
      with_log(fn ->
        client = start_turn!(setup, peer_node: peer_node)

        # The provider sends its first two frames, then stalls.
        for pushed <- 0..1 do
          assert_receive {:fake_upstream_frame_barrier, ^pushed, _handler, ^hold}, detection_timeout_ms()
          :ok = FakeUpstream.release_frame(upstream, hold)
        end

        assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^hold}, detection_timeout_ms()
        read_turn!(client, answer_pings?: true)
      end)

    assert node(turn.client.owner) == peer_node
    assert turn.end in [{:close, 1002}, :socket_closed]
    assert Enum.map(turn.events, & &1["type"]) -- ["codex.response.metadata"] == ["response.created", "response.output_item.added"]
    assert turn.pings <= 2
    assert_request_settled!(setup, "failed", "client_disconnected")
    assert log =~ "#{WebsocketConnectionLogger.downstream_idle_timeout_message()} tracked_tasks=1"
    :ok = FakeUpstream.release_remaining_frames(upstream, hold)
    :ok = close_client!(turn.client)
  end
end
