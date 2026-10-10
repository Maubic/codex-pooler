defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.DownstreamKeepaliveTest do
  # The downstream idle bound is Bandit's read timeout, which only bytes from
  # the client re-arm, and the released client sends nothing while a response
  # streams: a turn streaming longer than `websocket_idle_timeout_ms` was closed
  # in the middle (close 1002) and settled `client_disconnected` (findings#302).
  # The socket now pings the client at every third of the bound while a turn
  # delivers frames, and the client's pong re-arms the timer; a turn that stops
  # delivering gets no ping and is still closed by the bound.
  #
  # Topology: the real public listener and a Mint client, FakeUpstream pacing
  # the provider's frames on the upstream websocket, the Pool's default mode
  # (Full), owner forwarding off and on (the owner on this node). The bound is
  # one second through the test-only settings override; the behaviour is a
  # timer's, so each arm spends up to about two and a half seconds of real
  # time. The bound also covers the turn's setup before its first frame, which
  # the client's request frame starts; the peer module's half-second bound was
  # shorter than that setup on a shared CI node (Drone 1809).
  use CodexPoolerWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, start_upstream: 1]
  import CodexPoolerWeb.Runtime.DownstreamKeepaliveScenario

  alias CodexPooler.Accounting.RequestOutcome
  alias CodexPooler.FakeUpstream
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPoolerWeb.WebsocketConnectionLogger

  @moduletag capture_log: true

  @idle_timeout_ms 1_000
  @frame_interval_ms 100

  setup %{forwarding: forwarding?} do
    put_settings!([websocket_idle_timeout_ms: @idle_timeout_ms], forwarding?)
  end

  for forwarding? <- [false, true] do
    topology = if forwarding?, do: "owner forwarding on", else: "owner forwarding off"

    @tag forwarding: forwarding?
    @tag slow: "the idle bound is a real timer: the turn must stream for two bounds to show the pings keep it open"
    test "#{topology}: a turn streaming two idle bounds long completes for a client that answers pings" do
      # Twenty-two frames 100 ms apart: the turn streams for about 2.2 s
      # against a 1 s bound.
      deltas = 18
      upstream = start_upstream(FakeUpstream.delayed_sse_stream(stream_events("resp_keepalive_long", deltas), interval_ms: @frame_interval_ms))
      setup = gateway_setup(upstream)

      {turn, log} = with_log(fn -> setup |> start_turn!() |> read_turn!(answer_pings?: true) end)

      assert turn.end == {:terminal, "response.completed"}, "the turn " <> describe_turn(turn)
      assert Enum.count(turn.events, &(&1["type"] == "response.output_text.delta")) == deltas
      assert turn.pings >= 1, "the turn " <> describe_turn(turn)
      assert turn.elapsed_ms >= 2 * @idle_timeout_ms, "the turn " <> describe_turn(turn)
      request = assert_request_settled!(setup, "succeeded", nil)
      refute Map.has_key?(request.request_metadata, "downstream_interruption")
      refute log =~ WebsocketConnectionLogger.downstream_idle_timeout_message()
      assert [%{method: "WEBSOCKET", path: "/backend-api/codex/responses"}] = FakeUpstream.requests(upstream)
      :ok = close_client!(turn.client)
    end

    @tag forwarding: forwarding?
    test "#{topology}: an ordinary peer close retains client cancellation without idle attribution" do
      hold = make_ref()
      upstream = start_upstream(FakeUpstream.strict_sequence([turn_request(FakeUpstream.barrier_websocket_frames(encoded_events("resp_peer_close", 1), notify: self(), release_ref: hold))]))
      setup = gateway_setup(upstream)
      client = start_turn!(setup)
      assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^hold}, detection_timeout_ms()
      :ok = FakeUpstream.release_frame(upstream, hold)
      {conn, websocket, _frame} = BackendCodexTestSupport.public_websocket_receive_text!(client.conn, client.websocket, client.ref)
      :ok = close_client!(%{client | conn: conn, websocket: websocket})
      request = assert_request_settled!(setup, "failed", "client_disconnected")
      assert RequestOutcome.client_cancelled?(request)
      refute Map.has_key?(request.request_metadata, "downstream_interruption")
      :ok = FakeUpstream.release_remaining_frames(upstream, hold)
    end

    @tag forwarding: forwarding?
    @tag slow: "the idle bound is a real timer: the cut lands one bound after the request frame"
    test "#{topology}: a client that never answers pings is still cut at the idle bound" do
      # Twenty-two frames 100 ms apart: the stream outlasts the 1 s bound.
      upstream = start_upstream(FakeUpstream.delayed_sse_stream(stream_events("resp_keepalive_silent_client", 18), interval_ms: @frame_interval_ms))
      setup = gateway_setup(upstream)

      {turn, log} = with_log(fn -> setup |> start_turn!() |> read_turn!(answer_pings?: false) end)

      assert turn.end in [{:close, 1002}, :socket_closed], "the turn " <> describe_turn(turn)
      refute Enum.any?(turn.events, &(&1["type"] in terminal_types()))
      assert turn.pings >= 1, "the turn " <> describe_turn(turn)
      request = assert_request_settled!(setup, "failed", "client_disconnected")
      assert_idle_attribution!(request, "unknown")
      assert log =~ "#{WebsocketConnectionLogger.downstream_idle_timeout_message()} tracked_tasks=1 idle_timeout_ms=#{@idle_timeout_ms}"
      :ok = close_client!(turn.client)
    end

    @tag forwarding: forwarding?
    @tag slow: "the idle bound is a real timer: the close lands one bound after the last pong"
    test "#{topology}: a turn that stops delivering frames gets no more pings and is closed by the idle bound" do
      hold = make_ref()
      # provenance: synthetic_adversarial
      upstream = start_upstream(FakeUpstream.strict_sequence([turn_request(FakeUpstream.barrier_websocket_frames(encoded_events("resp_keepalive_stalled", 1), notify: self(), release_ref: hold))]))
      setup = gateway_setup(upstream)

      {turn, log} =
        with_log(fn ->
          client = start_turn!(setup)

          # The provider sends its first two frames, then stalls.
          for pushed <- 0..1 do
            assert_receive {:fake_upstream_frame_barrier, ^pushed, _handler, ^hold}, detection_timeout_ms()
            :ok = FakeUpstream.release_frame(upstream, hold)
          end

          assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^hold}, detection_timeout_ms()
          read_turn!(client, answer_pings?: true)
        end)

      # The client answered every ping, so the close proves the pings stopped
      # with the frames: only the ticks that saw the two frames pinged (one,
      # or two when a tick fell between them).
      assert turn.end in [{:close, 1002}, :socket_closed], "the turn " <> describe_turn(turn)
      assert Enum.map(turn.events, & &1["type"]) -- ["codex.response.metadata"] == ["response.created", "response.output_item.added"]
      assert turn.pings <= 2, "the turn " <> describe_turn(turn)
      request = assert_request_settled!(setup, "failed", "client_disconnected")
      assert_idle_attribution!(request, if(turn.pings > 0, do: "pong_observed", else: "unknown"))
      assert log =~ "#{WebsocketConnectionLogger.downstream_idle_timeout_message()} tracked_tasks=1"
      :ok = FakeUpstream.release_remaining_frames(upstream, hold)
      :ok = close_client!(turn.client)
    end
  end
end
