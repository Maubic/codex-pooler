defmodule CodexPooler.Upstreams.Quota.WindowDurationPersistenceTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Quotas.{CapacityFacts, Evidence}
  alias CodexPooler.Quotas.Evidence.CodexParsers
  alias CodexPooler.Upstreams.Quota.{AccountQuotaWindow, Windows}
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation

  @max_minutes 2_147_483_647
  @paths ["/backend-api/wham/usage", "/backend-api/codex/usage"]

  for {label, minutes, valid?} <- [
        {"minimum", 1, true},
        {"ordinary", 300, true},
        {"capacity maximum", 525_600, true},
        {"above capacity maximum", 525_601, true},
        {"int4 maximum", @max_minutes, true},
        {"zero", 0, false},
        {"negative", -1, false},
        {"int4 overflow", @max_minutes + 1, false},
        {"huge", 153_722_867_280_912_931, false},
        {"string overflow", "2147483648", false}
      ] do
    test "normalized evidence #{label} duration enforces the storage range" do
      assert_evidence_duration(unquote(minutes), unquote(valid?))
    end

    test "direct store #{label} duration enforces the storage range" do
      assert_store_duration(unquote(minutes), unquote(valid?))
    end

    test "schema insert #{label} duration enforces the storage range" do
      assert_schema_duration(unquote(minutes), unquote(valid?))
    end
  end

  test "overflow in a later window rolls back a preceding insert" do
    identity = upstream_identity_fixture()
    observed_at = now()
    valid = attrs(300, observed_at)
    invalid = %{attrs(@max_minutes + 1, observed_at) | window_kind: "secondary"}
    assert {:error, changeset} = Windows.upsert_quota_windows(identity, [valid, invalid])
    assert Keyword.has_key?(changeset.errors, :window_minutes)
    assert stored_windows(identity) == []
  end

  test "overflow in a later window rolls back a preceding update" do
    identity = upstream_identity_fixture()
    observed_at = now()
    original = attrs(300, observed_at)
    assert {:ok, [prior]} = Windows.upsert_quota_windows(identity, [original])
    valid_update = %{original | credits: 80, used_percent: Decimal.new(30), observed_at: DateTime.add(observed_at, 1), last_sync_at: DateTime.add(observed_at, 1)}
    assert {:ok, [updated]} = Windows.upsert_quota_windows(identity, [valid_update])
    assert updated.credits == 80
    assert Decimal.equal?(updated.used_percent, 30)

    next_update = %{valid_update | credits: 70, used_percent: Decimal.new(40), observed_at: DateTime.add(observed_at, 2), last_sync_at: DateTime.add(observed_at, 2)}
    invalid = %{attrs(@max_minutes + 1, observed_at) | window_kind: "secondary"}
    assert {:error, changeset} = Windows.upsert_quota_windows(identity, [next_update, invalid])
    assert Keyword.has_key?(changeset.errors, :window_minutes)
    assert [current] = stored_windows(identity)
    assert current.id == prior.id
    assert current.credits == 80
    assert Decimal.equal?(current.used_percent, 30)
    assert DateTime.compare(current.observed_at, updated.observed_at) == :eq
    assert DateTime.compare(current.updated_at, updated.updated_at) == :eq
  end

  test "schema update rejects overflow without changing its persisted row" do
    identity = upstream_identity_fixture()
    assert {:ok, [prior]} = Windows.upsert_quota_windows(identity, [attrs(300, now())])
    assert {:error, changeset} = prior |> AccountQuotaWindow.changeset(%{window_minutes: @max_minutes + 1}) |> Repo.update()
    assert Keyword.has_key?(changeset.errors, :window_minutes)
    assert Repo.reload!(prior).window_minutes == 300
  end

  for {seconds, count} <- [{@max_minutes * 60, 1}, {@max_minutes * 60 + 1, 0}] do
    test "strict parser duration #{seconds} seconds retains storage validity and capacity conflict separately" do
      observed_at = now()
      assert {:ok, result} = CodexParsers.parse_codex_usage_result(strict_payload(unquote(seconds), observed_at), observed_at)
      assert length(result.windows) == unquote(count)
      assert Enum.all?(result.windows, &(&1.window_minutes == @max_minutes))
      assert result.account_availability.state == :unknown
      assert result.account_availability.basis == :conflict
      refute CapacityFacts.authority_observed?(result.capacity_facts)
    end
  end

  for {label, seconds, expected_state, expected_count} <- [
        {"ordinary", 18_000, :available, 1},
        {"capacity maximum", 31_536_000, :available, 1},
        {"capacity maximum plus one", 31_536_001, :unknown, 0},
        {"int4 maximum", @max_minutes * 60, :unknown, 0},
        {"int4 overflow", @max_minutes * 60 + 1, :unknown, 0},
        {"huge", 9_223_372_036_854_775_808, :unknown, 0}
      ] do
    test "strict account HTTP #{label} duration preserves capacity classification" do
      assert_http_classification(unquote(seconds), unquote(expected_state), unquote(expected_count))
    end
  end

  defp assert_evidence_duration(minutes, valid?) do
    observed_at = now()
    raw = attrs(minutes, observed_at)
    assert {:ok, control} = Evidence.new(attrs(300, observed_at), observed_at)
    direct = Map.put(control, :window_minutes, minutes)

    if valid? do
      assert {:ok, evidence} = Evidence.new(raw, observed_at)
      assert evidence.window_minutes == minutes
      assert Evidence.validate(direct) == :ok
    else
      assert {:error, %{window_minutes: [_]}} = Evidence.new(raw, observed_at)
      assert {:error, %{window_minutes: [_]}} = Evidence.validate(direct)
    end
  end

  defp assert_store_duration(minutes, valid?) do
    identity = upstream_identity_fixture()
    result = Windows.upsert_quota_windows(identity, [attrs(minutes, now())])
    assert_persistence_result(result, identity, minutes, valid?)
  end

  defp assert_schema_duration(minutes, valid?) do
    identity = upstream_identity_fixture()
    observed_at = now()
    raw = Map.merge(attrs(minutes, observed_at), %{upstream_identity_id: identity.id, created_at: observed_at, updated_at: observed_at})

    result =
      case %AccountQuotaWindow{} |> AccountQuotaWindow.changeset(raw) |> Repo.insert() do
        {:ok, row} -> {:ok, [row]}
        error -> error
      end

    assert_persistence_result(result, identity, minutes, valid?)
  end

  defp assert_persistence_result(result, identity, minutes, true) do
    assert {:ok, [window]} = result
    assert [stored] = stored_windows(identity)
    assert stored.id == window.id
    assert stored.window_minutes == minutes
  end

  defp assert_persistence_result(result, identity, _minutes, false) do
    assert {:error, %Ecto.Changeset{} = changeset} = result
    assert Keyword.has_key?(changeset.errors, :window_minutes)
    assert stored_windows(identity) == []
  end

  defp assert_http_classification(seconds, expected_state, expected_count) do
    observed_at = now()
    name = :"normalized_duration_fake_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if pid = Process.whereis(name) do
        FakeUpstream.stop(%FakeUpstream{supervisor: pid})
        refute Process.alive?(pid)
      end

      assert Process.whereis(name) == nil
    end)

    payload = strict_payload(seconds, observed_at)
    assert {:ok, fake} = FakeUpstream.start_link({:path_json, Map.new(@paths, &{&1, {200, payload}})}, supervisor_name: name)
    Process.unlink(fake.supervisor)
    %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture(pool_fixture(), %{metadata: %{"usage_base_url" => FakeUpstream.url(fake)}})
    assert {:ok, _identity, probe} = PoolReconciliation.refresh_quota_and_probe_from_usage(identity, assignment, observed_at: observed_at)
    assert probe.account_availability.state == expected_state
    assert CapacityFacts.authority_observed?(probe.capacity_facts) == (expected_state == :available)
    assert length(probe.windows) == expected_count
    assert MapSet.size(probe.covered_descriptors) == expected_count
    assert length(stored_windows(identity)) == expected_count
    expected_paths = if seconds == 18_000, do: [hd(@paths)], else: @paths
    assert Enum.map(FakeUpstream.requests(fake), & &1.path) == expected_paths
  end

  defp strict_payload(seconds, observed_at) do
    %{
      "plan_type" => "sample_plan",
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => %{"used_percent" => 25, "limit_window_seconds" => seconds, "reset_after_seconds" => 900, "reset_at" => observed_at |> DateTime.add(900) |> DateTime.to_unix()}
      }
    }
  end

  defp attrs(minutes, observed_at) do
    %{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      window_kind: "primary",
      window_minutes: minutes,
      source: "codex_usage_api",
      source_precision: "observed",
      freshness_state: "fresh",
      active_limit: 100,
      credits: 100,
      used_percent: Decimal.new(25),
      observed_at: observed_at,
      last_sync_at: observed_at,
      reset_at: DateTime.add(observed_at, 900),
      metadata: %{}
    }
  end

  defp stored_windows(identity), do: Repo.all(from(window in AccountQuotaWindow, where: window.upstream_identity_id == ^identity.id))
  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second)
end
