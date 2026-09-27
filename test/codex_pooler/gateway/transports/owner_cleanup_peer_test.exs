defmodule CodexPooler.Gateway.Transports.OwnerCleanupPeerTest do
  use ExUnit.Case, async: true
  alias CodexPooler.Gateway.Transports.OwnerCleanupPeer

  test "a required teardown diagnostic cannot pass on an empty capture" do
    assert_raise ExUnit.AssertionError, ~r/expected owner cleanup peer teardown diagnostic/, fn -> OwnerCleanupPeer.assert_teardown_log!("") end
    assert :ok = OwnerCleanupPeer.assert_teardown_log!("websocket owner exit persistence failed reason_class=stale_owner_cleanup")
    assert :ok = OwnerCleanupPeer.assert_teardown_log!("", required?: false)
    assert_raise ExUnit.AssertionError, fn -> OwnerCleanupPeer.assert_teardown_log!("unexpected diagnostic", required?: false) end
  end
end
