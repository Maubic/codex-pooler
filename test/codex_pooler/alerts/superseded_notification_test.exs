defmodule CodexPooler.Alerts.SupersededNotificationTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Incidents.NotificationEvents
  alias CodexPooler.Alerts.Schemas.AlertIncident
  alias CodexPooler.UnboxedFixture

  @kind "upstream_saved_reset_banked_first_seen"

  test "a newer grant invalidates the superseded target after the new incident commits" do
    %{user: owner} = committed_bootstrap_owner_fixture!()
    account_id = "acct_#{Ecto.UUID.generate()}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      ids = Repo.all(from i in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: i.chatgpt_account_id == ^account_id, select: i.id)
      Repo.delete_all(from i in AlertIncident, where: i.upstream_identity_id in ^ids)
      Repo.delete_all(from i in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: i.id in ^ids)
    end)

    {pool_a, pool_b, rule_a, rule_b, identity} =
      UnboxedFixture.run_unboxed(fn ->
        pool_a = pool_fixture(%{created_by_user_id: owner.id})
        pool_b = pool_fixture(%{created_by_user_id: owner.id})
        identity = upstream_identity_fixture(%{chatgpt_account_id: account_id})
        rule_a = alert_rule_fixture(pool_a, scope_type: "upstream_identity", rule_kind: @kind, severity: "info")
        rule_b = alert_rule_fixture(pool_b, scope_type: "upstream_identity", rule_kind: @kind, severity: "info")
        {pool_a, pool_b, rule_a, rule_b, identity}
      end)

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.delete_all(from i in AlertIncident, where: i.upstream_identity_id == ^identity.id)
    end)

    older = DateTime.add(DateTime.utc_now(), -60, :second)
    attrs = attrs(identity, [{rule_a, pool_a}, {rule_b, pool_b}], older)
    {:ok, %{incident: first}} = UnboxedFixture.run_unboxed(fn -> Alerts.record_incident_once(attrs) end)
    parent = self()

    observer =
      Task.async(fn ->
        NotificationEvents.subscribe_pool(pool_b.id)
        send(parent, :subscribed)

        receive do
          {NotificationEvents, :invalidated, _id} ->
            UnboxedFixture.run_unboxed(fn ->
              assert Repo.get!(AlertIncident, first.id).state == "resolved"
              assert Repo.aggregate(from(i in AlertIncident, where: i.upstream_identity_id == ^identity.id and i.state == "open"), :count) == 1
            end)

            :observed_committed_supersession
        after
          2_000 -> :missing_invalidation
        end
      end)

    monitor = Process.monitor(observer.pid)
    on_exit(fn -> if Process.alive?(observer.pid), do: Process.exit(observer.pid, :kill) end)
    assert_receive :subscribed
    next = attrs(identity, [{rule_a, pool_a}], DateTime.add(older, 30, :second))
    assert {:ok, _} = UnboxedFixture.run_unboxed(fn -> Alerts.record_incident_once(next) end)
    assert Task.await(observer, 5_000) == :observed_committed_supersession
    assert_receive {:DOWN, ^monitor, :process, _, :normal}
  end

  defp attrs(identity, targets, seen) do
    %{
      dedupe_key: "alerts:v2:#{@kind}:upstream_identity:#{identity.id}",
      scope_type: "upstream_identity",
      rule_kind: @kind,
      severity: "info",
      upstream_identity_id: identity.id,
      pool_id: nil,
      safe_evidence_snapshot: %{"reason_code" => "saved_reset_banked_first_seen", "available_count" => 1, "latest_reset_first_seen_at" => DateTime.to_iso8601(seen)},
      targets: Enum.map(targets, fn {rule, pool} -> %{rule_id: rule.id, pool_id: pool.id, metadata: %{}} end),
      matched_at: DateTime.utc_now()
    }
  end
end
