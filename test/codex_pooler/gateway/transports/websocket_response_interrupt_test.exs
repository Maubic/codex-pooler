defmodule CodexPooler.Gateway.Transports.WebsocketResponseInterruptTest do
  # findings#270 row 270-272: a client's `response.interrupt` is not a request.
  # It is parsed from a fixed shape, handed to the upstream session that
  # carries the running turn (through the session's owner with owner
  # forwarding on), written only while the response it names runs there, and
  # dropped everywhere else with one line naming where. These are the pieces
  # the real-path tests in
  # `test/codex_pooler_web/controllers/runtime/backend_codex_websocket_response_interrupt_test.exs`
  # cannot reach: the parser's bounds, an owner asked for another downstream's
  # or a collected turn, an idle session, and an owner node of an earlier
  # release.
  use ExUnit.Case, async: false

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  alias CodexPooler.Gateway.Transports.Websocket.ResponseInterrupt
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness

  @interrupt %{response_id: "resp_interrupt_unit_sample", mode: "discard_partial_items"}

  describe "ResponseInterrupt.parse/1" do
    test "reads the released client's frame" do
      assert {:ok, @interrupt} = ResponseInterrupt.parse(%{"type" => "response.interrupt", "response_id" => @interrupt.response_id, "mode" => @interrupt.mode})
    end

    test "a response.interrupt frame without a provider response id or a bounded mode is malformed" do
      valid = %{"type" => "response.interrupt", "response_id" => @interrupt.response_id, "mode" => @interrupt.mode}

      for frame <- [
            Map.delete(valid, "response_id"),
            Map.delete(valid, "mode"),
            %{valid | "response_id" => "msg_not_a_response"},
            %{valid | "response_id" => "resp_" <> String.duplicate("a", 1_021)},
            %{valid | "response_id" => "resp_with space"},
            %{valid | "mode" => "Discard"},
            %{valid | "mode" => String.duplicate("a", 65)},
            %{valid | "mode" => 1}
          ] do
        assert ResponseInterrupt.parse(frame) == :malformed
      end
    end

    test "every other frame is not an interrupt" do
      for frame <- [%{"type" => "response.create"}, %{"type" => "response.interrupted"}, %{}, "response.interrupt", nil] do
        assert ResponseInterrupt.parse(frame) == :not_interrupt
      end
    end

    test "the frame written upstream carries the interrupt's own fields and nothing else" do
      assert CodexPooler.JSON.decode!(ResponseInterrupt.frame(Map.put(@interrupt, :extra, "dropped"))) ==
               %{"type" => "response.interrupt", "response_id" => @interrupt.response_id, "mode" => @interrupt.mode}
    end
  end

  # The owner hands the interrupt to the upstream session of its running turn
  # only when that turn belongs to the asking downstream and is relayed as it
  # comes. `upstream_pid` here is the test process, so a relayed interrupt is
  # the message the session would receive.
  describe "WebsocketOwnerSession interrupt_turn" do
    setup do
      downstream = %{pid: spawn(fn -> Process.sleep(:infinity) end), epoch: 1, correlation_id: "interrupt-downstream"}
      on_exit(fn -> Process.exit(downstream.pid, :kill) end)
      %{downstream: downstream}
    end

    test "a relayed turn of the asking downstream gets the interrupt on its upstream session", %{downstream: downstream} do
      state = owner_state(%{downstream: Map.put(downstream, :owner_turn_id, self()), collect?: false, upstream_pid: self()})

      {reply, log} = with_info_log(fn -> call_interrupt(state, downstream) end)

      assert {:reply, :ok, ^state} = reply
      assert_received {:upstream_websocket_interrupt, @interrupt}
      refute log =~ "native websocket response interrupt"
    end

    test "another downstream's turn, a collected turn and no turn drop it with their line", %{downstream: downstream} do
      other = %{downstream | correlation_id: "another-downstream"}

      for {active_turn, outcome} <- [
            {%{downstream: other, collect?: false, upstream_pid: self()}, "owner_not_downstream"},
            {%{downstream: downstream, collect?: true, upstream_pid: self()}, "owner_turn_not_relay"},
            {nil, "no_running_turn"}
          ] do
        state = owner_state(active_turn)
        {reply, log} = with_info_log(fn -> call_interrupt(state, downstream) end)

        assert {:reply, :ok, ^state} = reply
        assert log =~ "native websocket response interrupt outcome=#{outcome} topology=owner"
        refute_received {:upstream_websocket_interrupt, _interrupt}
      end
    end
  end

  test "an upstream session with no turn in flight drops the interrupt with its line" do
    session = start_supervised!({UpstreamWebsocketSession, []})

    {_state, log} =
      with_info_log(fn ->
        assert :ok = UpstreamWebsocketSession.interrupt(session, @interrupt)
        :sys.get_state(session)
      end)

    assert log =~ "native websocket response interrupt outcome=session_idle topology=direct"
    assert Process.alive?(session)
  end

  # An owner node of an earlier release has no `remote_interrupt_turn_v1`: the
  # erpc call answers `undef`, read as the unsupported protocol, and the socket
  # drops the interrupt; the turn runs to its end as it would have there.
  test "an owner node of an earlier release answers the interrupt as unsupported" do
    earlier_release_node = :"codex_pooler@earlier-release-owner.example"
    session_id = Ecto.UUID.generate()
    downstream = %{pid: self(), epoch: 1, correlation_id: "interrupt-earlier-release"}
    args = [session_id, downstream, @interrupt]
    undef = {:error, {:exception, :undef, [{WebsocketOwnerForwarder, :remote_interrupt_turn_v1, args, []}]}}
    opts = WebsocketOwnerNodeHarness.node_client_opts([earlier_release_node], calls: %{earlier_release_node => {:return, undef}})

    assert {:error, :remote_interrupt_v1_unsupported} =
             WebsocketOwnerForwarder.interrupt_remote_turn(earlier_release_node, session_id, Map.put(downstream, :owner_turn_id, self()), @interrupt, opts)

    assert_receive {:websocket_owner_harness_node_call, %{node: ^earlier_release_node, function: :remote_interrupt_turn_v1, arity: 3}}
  end

  defp owner_state(active_turn), do: %WebsocketOwnerSession{active_turn: active_turn}

  defp call_interrupt(state, downstream),
    do: WebsocketOwnerSession.handle_call({:interrupt_turn, downstream.pid, downstream.epoch, downstream.correlation_id, @interrupt}, {self(), make_ref()}, state)
end
