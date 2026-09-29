defmodule CodexPoolerWeb.Runtime.PinnedWebsocketQuotaContinuityPeerTest do
  use CodexPoolerWeb.ConnCase, async: false
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0]
  alias CodexPoolerWeb.Runtime.PinnedWebsocketQuotaScenario
  @moduletag capture_log: true

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup do
    enter_peer_owner_topology!()
    :ok
  end

  for mode <- ["full", "lite"], sibling_state <- [:healthy, :exhausted, :unknown] do
    @tag forwarding: true, mode: mode, sibling_state: sibling_state
    test "remote anchored quota refusal preserves Pool capacity mode=#{mode} sibling=#{sibling_state}", context do
      PinnedWebsocketQuotaScenario.run(context)
    end
  end
end
