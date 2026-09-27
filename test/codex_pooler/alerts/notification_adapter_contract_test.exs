defmodule CodexPooler.Alerts.NotificationAdapterContractTest do
  use ExUnit.Case, async: false

  test "installed pg adapter missing group still delivers every local topic" do
    name = :deletion_completion_notification_adapter
    start_supervised!({Phoenix.PubSub, name: name, pool_size: 1})
    adapter = Module.concat(name, "Adapter")
    topics = ["sample:first", "sample:second"]
    for topic <- topics, do: assert(:ok = Phoenix.PubSub.subscribe(name, topic))
    groups = :persistent_term.get(adapter)
    group = elem(groups, 0)
    members = :pg.get_local_members(Phoenix.PubSub, group)
    # Owned PubSub only: remove the shard's membership, the reported trigger.
    for pid <- members, do: :pg.leave(Phoenix.PubSub, group, pid)
    assert :pg.get_members(Phoenix.PubSub, group) == []

    for topic <- topics do
      assert :ok = Phoenix.PubSub.broadcast(name, topic, {:delivered, topic})
      assert_receive {:delivered, ^topic}
    end

    assert :ok = Phoenix.PubSub.broadcast(name, "sample:unsubscribed", :no_listeners)
  end
end
