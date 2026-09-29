defmodule CodexPoolerWeb.Runtime.PinnedWebsocketQuotaContinuityTest do
  use CodexPoolerWeb.ConnCase, async: false
  alias CodexPoolerWeb.Runtime.PinnedWebsocketQuotaScenario
  @moduletag capture_log: true

  for forwarding <- [false, true], mode <- ["full", "lite"], sibling_state <- [:healthy, :exhausted, :unknown] do
    @tag forwarding: forwarding, mode: mode, sibling_state: sibling_state
    test "anchored quota refusal preserves Pool capacity forwarding=#{forwarding} mode=#{mode} sibling=#{sibling_state}", context do
      PinnedWebsocketQuotaScenario.run(context)
    end
  end

  for forwarding <- [false, true], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, mode: mode, sibling_state: :healthy, resetless: true
    test "resetless anchored quota refusal permits full-history recovery forwarding=#{forwarding} mode=#{mode}", context do
      PinnedWebsocketQuotaScenario.run(context)
    end
  end
end
