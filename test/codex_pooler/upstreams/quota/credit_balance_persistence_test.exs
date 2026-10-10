defmodule CodexPooler.Upstreams.Quota.CreditBalancePersistenceTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Quota.{AccountQuotaWindow, Windows}
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation

  @overflow 9_223_372_036_854_775_808

  for {label, balance, expected} <- [
        {"integer overflow", @overflow, nil},
        {"string overflow", "9223372036854775808", nil},
        {"float overflow", 1.0e20, nil},
        {"exponent overflow", "1e20", nil},
        {"integer maximum", @overflow - 1, @overflow - 1},
        {"string maximum", "9223372036854775807", @overflow - 1},
        {"zero", 0, 0},
        {"rounded float", 12.5, 13},
        {"rounded string", "12.4", 12}
      ] do
    test "#{label} persists both account windows on first insert" do
      assert_first_insert(unquote(balance), unquote(expected))
    end

    test "#{label} persists both account windows through HTTP usage reconciliation" do
      assert_usage_refresh(unquote(balance), unquote(expected))
    end
  end

  for field <- [:credits, :active_limit] do
    test "direct schema insert rejects oversized #{field} before encoding" do
      identity = upstream_identity_fixture()
      observed_at = now()
      [attrs | _] = window_attrs(100, observed_at)

      attrs =
        attrs
        |> Map.put(:upstream_identity_id, identity.id)
        |> Map.put(:created_at, observed_at)
        |> Map.put(:updated_at, observed_at)
        |> Map.put(unquote(field), @overflow)

      assert {:error, changeset} = %AccountQuotaWindow{} |> AccountQuotaWindow.changeset(attrs) |> Repo.insert()
      assert Keyword.has_key?(changeset.errors, unquote(field))
      assert stored_windows(identity) == []
    end

    test "direct evidence overflow in #{field} rolls back an earlier window insert" do
      identity = upstream_identity_fixture()
      [primary, secondary] = window_attrs(100, now())

      assert {:error, %Ecto.Changeset{} = changeset} =
               Windows.upsert_quota_windows(identity, [primary, Map.put(secondary, unquote(field), @overflow)])

      assert Keyword.has_key?(changeset.errors, unquote(field))
      assert stored_windows(identity) == []
    end
  end

  test "direct overflow rolls back an earlier update and preserves the persisted row" do
    identity = upstream_identity_fixture()
    observed_at = now()
    [primary, secondary] = window_attrs(100, observed_at)
    assert {:ok, [stored]} = Windows.upsert_quota_windows(identity, [primary])

    update = %{primary | credits: 80, used_percent: Decimal.new(30), observed_at: DateTime.add(observed_at, 1), last_sync_at: DateTime.add(observed_at, 1)}
    assert {:ok, [updated]} = Windows.upsert_quota_windows(identity, [update])
    assert updated.credits == 80
    assert Decimal.equal?(updated.used_percent, 30)

    later = %{update | credits: 70, used_percent: Decimal.new(40), observed_at: DateTime.add(observed_at, 2), last_sync_at: DateTime.add(observed_at, 2)}
    assert {:error, %Ecto.Changeset{}} = Windows.upsert_quota_windows(identity, [later, %{secondary | credits: @overflow}])
    assert [unchanged] = stored_windows(identity)
    assert unchanged.id == stored.id
    assert DateTime.compare(unchanged.observed_at, updated.observed_at) == :eq
    assert DateTime.compare(unchanged.updated_at, updated.updated_at) == :eq
    assert unchanged.credits == 80
    assert Decimal.equal?(unchanged.used_percent, 30)
  end

  test "overflow and missing provider balance retain the same existing credit baseline" do
    identity = upstream_identity_fixture()
    observed_at = now()
    assert {:ok, original} = Windows.upsert_quota_windows(identity, window_attrs(100, observed_at))

    assert {:ok, missing} = Windows.upsert_quota_windows(identity, window_attrs(nil, DateTime.add(observed_at, 1)))
    assert Enum.map(missing, &{&1.id, &1.active_limit, &1.credits}) == Enum.map(original, &{&1.id, &1.active_limit, &1.credits})

    assert {:ok, oversized} = Windows.upsert_quota_windows(identity, window_attrs(@overflow, DateTime.add(observed_at, 2)))
    assert Enum.map(oversized, &{&1.id, &1.active_limit, &1.credits}) == Enum.map(missing, &{&1.id, &1.active_limit, &1.credits})
    assert Enum.all?(stored_windows(identity), &(&1.credits == 100 and &1.active_limit == 100))
  end

  defp assert_first_insert(balance, expected) do
    identity = upstream_identity_fixture()
    observed_at = now()
    assert {:ok, windows} = Windows.upsert_quota_windows_from_codex_usage_payload(identity, usage_payload(balance, observed_at), observed_at)
    assert Enum.sort(Enum.map(windows, & &1.id)) == Enum.sort(Enum.map(stored_windows(identity), & &1.id))
    assert_stored_windows(identity, expected, observed_at)
  end

  defp assert_usage_refresh(balance, expected) do
    observed_at = now()
    name = :"credit_balance_fake_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if pid = Process.whereis(name) do
        FakeUpstream.stop(%FakeUpstream{supervisor: pid})
        refute Process.alive?(pid)
      end

      assert Process.whereis(name) == nil
    end)

    routes = Map.new(["/api/codex/usage", "/backend-api/wham/usage"], &{&1, {200, usage_payload(balance, observed_at)}})
    assert {:ok, fake} = FakeUpstream.start_link({:path_json, routes}, supervisor_name: name)
    Process.unlink(fake.supervisor)
    %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture(pool_fixture(), %{metadata: %{"usage_base_url" => FakeUpstream.url(fake)}})

    assert {:ok, refreshed, probe} = PoolReconciliation.refresh_quota_and_probe_from_usage(identity, assignment, observed_at: observed_at)
    assert refreshed.id == identity.id
    assert length(probe.windows) == 2
    assert Enum.map(FakeUpstream.requests(fake), & &1.path) == ["/backend-api/wham/usage"]
    assert_stored_windows(identity, expected, observed_at)
  end

  defp assert_stored_windows(identity, expected, observed_at) do
    assert [primary, secondary] = stored_windows(identity)
    assert {primary.window_kind, primary.window_minutes} == {"primary", 300}
    assert {secondary.window_kind, secondary.window_minutes} == {"secondary", 10_080}
    assert Decimal.equal?(primary.used_percent, 10)
    assert Decimal.equal?(secondary.used_percent, 20)

    for window <- [primary, secondary] do
      assert window.active_limit == expected
      assert window.credits == expected
      assert DateTime.compare(window.observed_at, observed_at) == :eq
      assert DateTime.compare(window.reset_at, DateTime.add(observed_at, 900)) == :eq
    end
  end

  defp stored_windows(identity), do: Repo.all(from(window in AccountQuotaWindow, where: window.upstream_identity_id == ^identity.id, order_by: window.window_kind))

  defp window_attrs(balance, observed_at) do
    {:ok, evidence} = Evidence.parse_codex_usage_payload(usage_payload(balance, observed_at), observed_at)
    Enum.map(evidence, &Evidence.to_window_attrs/1)
  end

  defp usage_payload(balance, observed_at) do
    %{
      "plan_type" => "pro",
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => balance},
      "spend_control" => %{"reached" => false},
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => usage_window(18_000, 10, observed_at),
        "secondary_window" => usage_window(604_800, 20, observed_at)
      }
    }
  end

  defp usage_window(seconds, used_percent, observed_at), do: %{"used_percent" => used_percent, "limit_window_seconds" => seconds, "reset_after_seconds" => 900, "reset_at" => observed_at |> DateTime.add(900) |> DateTime.to_unix()}
  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second)
end
