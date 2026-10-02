defmodule CodexPooler.Upstreams.Quota.Windows.UsageCoherenceStoreTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Gateway.Routing.CandidateEligibility.Quota, as: CandidateQuota
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{AccountAvailabilityStore, AccountQuotaWindow, CapacityFactsStore, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Quota.Windows.EvidenceStore
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.Quota.Windows.UsageCoherence

  @key "__quota_usage_coherence_v1"

  test "two coherent usage readings supersede a fresh header exhaustion in the effective view" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    reset_at = DateTime.add(now, 900, :second) |> DateTime.truncate(:second)

    assert {:ok, _headers} =
             record!(
               identity,
               "codex_response_headers",
               "100",
               reset_at,
               DateTime.add(now, -90, :second),
               %{}
             )

    assert [%{used_percent: exhausted}] = Windows.list_quota_windows(identity, now)
    assert Decimal.equal?(exhausted, Decimal.new("100"))

    assert {:ok, first} =
             record!(
               identity,
               "codex_usage_api",
               "20",
               reset_at,
               DateTime.add(now, -60, :second),
               safe_status()
             )

    assert first.metadata[@key]["count"] == 1
    refute UsageCoherence.confirmed?(first, now)

    # One lower reading is retained beside the exhausted row, which still wins.
    assert [%{source: "codex_response_headers"}] = Windows.list_quota_windows(identity, now)

    assert {:ok, second} =
             record!(
               identity,
               "codex_usage_api",
               "20",
               reset_at,
               DateTime.add(now, -30, :second),
               safe_status()
             )

    assert second.metadata[@key]["count"] == 2
    assert UsageCoherence.confirmed?(second, now)

    assert [%{source: "codex_usage_api", used_percent: recovered}] =
             Windows.list_quota_windows(identity, now)

    assert Decimal.equal?(recovered, Decimal.new("20"))

    # A denied reading clears the confirmation and the exhausted row wins again.
    assert {:ok, denied} =
             record!(identity, "codex_usage_api", "20", reset_at, now, %{
               "rate_limit_allowed" => false,
               "rate_limit_reached" => true
             })

    refute Map.has_key?(denied.metadata, @key)
    assert [%{source: "codex_response_headers"}] = Windows.list_quota_windows(identity, now)
  end

  test "a lower usage reading without provider permission facts never confirms" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    reset_at = DateTime.add(now, 900, :second) |> DateTime.truncate(:second)

    assert {:ok, _headers} =
             record!(
               identity,
               "codex_response_headers",
               "100",
               reset_at,
               DateTime.add(now, -90, :second),
               %{}
             )

    for offset <- [-60, -30] do
      assert {:ok, row} =
               record!(
                 identity,
                 "codex_usage_api",
                 "20",
                 reset_at,
                 DateTime.add(now, offset, :second),
                 %{}
               )

      refute Map.has_key?(row.metadata, @key)
    end

    assert [%{source: "codex_response_headers"}] = Windows.list_quota_windows(identity, now)
  end

  for {label, window_minutes} <- [{"5h", 300}, {"30d", 43_200}] do
    test "repeated permitted zero-percent #{label} account evidence stays fresh" do
      %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
      t0 = DateTime.utc_now() |> DateTime.add(-20, :minute) |> DateTime.truncate(:microsecond)
      reset_at = DateTime.add(t0, unquote(window_minutes), :minute)

      assert {:ok, first} =
               record!(
                 identity,
                 "codex_usage_api",
                 "0",
                 reset_at,
                 t0,
                 safe_status(),
                 unquote(window_minutes)
               )

      t1 = DateTime.add(t0, 10, :minute)
      refreshed_reset_at = DateTime.add(reset_at, 2, :minute)

      assert {:ok, second} =
               record!(
                 identity,
                 "codex_usage_api",
                 "0",
                 refreshed_reset_at,
                 t1,
                 safe_status(),
                 unquote(window_minutes)
               )

      assert second.id == first.id
      assert Decimal.equal?(second.used_percent, Decimal.new("0"))
      assert second.active_limit == nil
      assert second.credits == nil
      assert DateTime.compare(second.observed_at, t1) == :eq
      assert DateTime.compare(second.last_sync_at, t1) == :eq

      after_original_ttl = DateTime.add(t0, 16, :minute)

      assert %{eligible?: true, routing_state: :precise, exclusions: []} =
               Routing.eligibility_from_windows([second], at: after_original_ttl)
    end
  end

  test "a reset correction without explicit provider permission cannot refresh a primary zero" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-20, :minute) |> DateTime.truncate(:microsecond)
    reset_at = DateTime.add(t0, 300, :minute)

    assert {:ok, first} =
             record!(identity, "codex_usage_api", "0", reset_at, t0, %{}, 300)

    t1 = DateTime.add(t0, 10, :minute)

    assert {:ok, retained} =
             record!(
               identity,
               "codex_usage_api",
               "0",
               DateTime.add(reset_at, 2, :minute),
               t1,
               %{},
               300
             )

    assert retained.id == first.id
    assert DateTime.compare(retained.observed_at, t0) == :eq
    assert DateTime.compare(retained.last_sync_at, t0) == :eq
  end

  test "permitted idle primary with a full-window sliding reset stays fresh beyond five minutes" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-30, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 300, :minute)

    assert {:ok, first} =
             record!(identity, "codex_usage_api", "0", reset_at, t0, safe_status(), 300)

    for minute <- 1..20 do
      observed_at = DateTime.add(t0, minute, :minute)

      assert {:ok, current} =
               record!(
                 identity,
                 "codex_usage_api",
                 "0",
                 DateTime.add(observed_at, 300, :minute),
                 observed_at,
                 safe_status(),
                 300
               )

      assert current.id == first.id
      assert DateTime.compare(current.observed_at, observed_at) == :eq
      assert DateTime.compare(current.last_sync_at, observed_at) == :eq
      assert DateTime.compare(current.reset_at, reset_at) == :eq
      assert %{eligible?: true} = Routing.eligibility_from_windows([current], at: observed_at)
    end
  end

  test "large reset drift without full-window timing cannot refresh a zero primary" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-30, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 300, :minute)
    assert {:ok, _} = record!(identity, "codex_usage_api", "0", reset_at, t0, safe_status(), 300)
    t1 = DateTime.add(t0, 6, :minute)

    assert {:ok, retained} =
             record!(
               identity,
               "codex_usage_api",
               "0",
               DateTime.add(reset_at, 12, :minute),
               t1,
               safe_status(),
               300
             )

    assert DateTime.compare(retained.observed_at, t0) == :eq
    assert DateTime.compare(retained.reset_at, reset_at) == :eq
  end

  test "full-window idle timing cannot erase positive primary consumption" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-10, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 300, :minute)
    assert {:ok, _} = record!(identity, "codex_usage_api", "22", reset_at, t0, safe_status(), 300)
    t1 = DateTime.add(t0, 6, :minute)

    assert {:ok, retained} =
             record!(
               identity,
               "codex_usage_api",
               "0",
               DateTime.add(t1, 300, :minute),
               t1,
               safe_status(),
               300
             )

    assert Decimal.equal?(retained.used_percent, 22)
    assert DateTime.compare(retained.reset_at, reset_at) == :eq
  end

  for allowed <- [true, false] do
    @tag :primary_idle_display
    test "primary idle display proof survives freshness rollover with permission #{allowed}" do
      allowed = unquote(allowed)
      pool = pool_fixture()
      %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture(pool, %{})
      model = model_fixture(pool)
      t0 = DateTime.utc_now() |> DateTime.add(-20, :minute) |> DateTime.truncate(:second)

      for offset <- [0, 60, 240, 901, 960] do
        at = DateTime.add(t0, offset, :second)
        {primary, result} = record_idle_payload!(identity, t0, at, allowed)

        if offset >= 240, do: assert(primary.metadata["reset_state"] == "floating")
        if offset < 240, do: refute(primary.metadata["reset_state"] == "floating")
        assert Decimal.equal?(primary.used_percent, Decimal.new(0))
        assert primary.metadata["rate_limit_allowed"] == allowed
        assert primary.metadata["rate_limit_reached"] == not allowed

        if offset == 240 do
          assert DateTime.compare(primary.reset_at, DateTime.add(t0, 18_000, :second)) == :eq
          assert DateTime.compare(primary.observed_at, expected_primary_observation(allowed, at, t0)) == :eq
        end

        snapshot = idle_routing_snapshot(identity, result, at)
        without_display = %{snapshot | raw_windows: Enum.map(snapshot.raw_windows, fn window -> %{window | metadata: Map.drop(window.metadata, ["reset_state", "__quota_primary_idle_display_v1"])} end)}
        context = %{model: model.exposed_model_id, upstream_model: model.upstream_model_id, serving_mode: "full", transport: "http"}
        candidate = {assignment, identity}
        routeable = CandidateQuota.quota_routable?(model, candidate, snapshot, context)
        assert routeable == allowed
        assert routeable == CandidateQuota.quota_routable?(model, candidate, without_display, context)
      end
    end
  end

  @tag :primary_idle_display
  test "replayed and out-of-order idle receipts cannot establish floating primary display" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-10, :minute) |> DateTime.truncate(:second)
    record_idle_payload!(identity, t0, t0, false)
    record_idle_payload!(identity, t0, DateTime.add(t0, 60, :second), false)

    for {observed_offset, provider_offset} <- [{240, 0}, {30, 30}, {300, 0}] do
      {primary, _} = record_idle_payload!(identity, t0, DateTime.add(t0, observed_offset, :second), false, DateTime.add(t0, provider_offset + 18_000, :second))
      refute primary.metadata["reset_state"] == "floating"
    end

    {confirmed, _} = record_idle_payload!(identity, t0, DateTime.add(t0, 360, :second), false)
    assert confirmed.metadata["reset_state"] == "floating"
  end

  @tag :primary_idle_display
  test "a fixed zero-use countdown stays anchored and accepted positive use clears floating display" do
    %{identity: anchored} = active_upstream_assignment_fixture(pool_fixture(), %{})
    %{identity: floating} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-10, :minute) |> DateTime.truncate(:second)
    reset_at = DateTime.add(t0, 18_000, :second)

    for offset <- [0, 60, 240] do
      at = DateTime.add(t0, offset, :second)
      {control, _} = record_idle_payload!(anchored, t0, at, true, reset_at, 0, 18_000 - offset)
      refute control.metadata["reset_state"] == "floating"
      record_idle_payload!(floating, t0, at, true)
    end

    assert primary_row!(floating).metadata["reset_state"] == "floating"
    {used, _} = record_idle_payload!(floating, t0, DateTime.add(t0, 300, :second), true, reset_at, 10, 17_700)
    assert Decimal.equal?(used.used_percent, Decimal.new(10))
    refute used.metadata["reset_state"] == "floating"
    refute Map.has_key?(used.metadata, "__quota_primary_idle_display_v1")
  end

  @tag :primary_idle_display
  test "accepted fixed countdown clears idle display only after canonical evidence advances" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-6, :hour) |> DateTime.truncate(:second)
    for offset <- [0, 60, 240], do: record_idle_payload!(identity, t0, DateTime.add(t0, offset, :second), true)
    assert primary_row!(identity).metadata["reset_state"] == "floating"

    # The previous canonical cycle expired; the store accepts the provider's
    # later fixed countdown. A rejected observation must not clear the proof.
    at = DateTime.add(t0, 18_061, :second)
    reset_at = DateTime.add(t0, 36_000, :second)
    {anchored, _} = record_idle_payload!(identity, t0, at, true, reset_at, 0, 17_939)
    assert DateTime.compare(anchored.observed_at, at) == :eq
    assert DateTime.compare(anchored.reset_at, reset_at) == :eq
    refute anchored.metadata["reset_state"] == "floating"
    refute Map.has_key?(anchored.metadata, "__quota_primary_idle_display_v1")
  end

  @tag :primary_idle_display
  test "idle-shaped receipts cannot replace an active positive primary with floating display" do
    %{identity: identity} = active_upstream_assignment_fixture(pool_fixture(), %{})
    t0 = DateTime.utc_now() |> DateTime.add(-10, :minute) |> DateTime.truncate(:second)
    record_idle_payload!(identity, t0, t0, true, DateTime.add(t0, 18_000, :second), 20)

    for offset <- [60, 240, 360] do
      {current, _} = record_idle_payload!(identity, t0, DateTime.add(t0, offset, :second), true)
      assert Decimal.equal?(current.used_percent, Decimal.new(20))
      refute current.metadata["reset_state"] == "floating"
      refute Map.has_key?(current.metadata, "__quota_primary_idle_display_v1")
    end
  end

  defp expected_primary_observation(true, at, _t0), do: at
  defp expected_primary_observation(false, _at, t0), do: t0

  defp record_idle_payload!(identity, t0, at, allowed, reset_at \\ nil, used \\ 0, reset_after \\ 18_000) do
    payload = %{
      "plan_type" => "team",
      "rate_limit" => %{
        "allowed" => allowed,
        "limit_reached" => not allowed,
        "primary_window" => %{"used_percent" => used, "limit_window_seconds" => 18_000, "reset_after_seconds" => reset_after, "reset_at" => DateTime.to_unix(reset_at || DateTime.add(at, 18_000, :second))},
        "secondary_window" => %{"used_percent" => 100, "limit_window_seconds" => 604_800, "reset_after_seconds" => DateTime.diff(DateTime.add(t0, 604_800, :second), at, :second), "reset_at" => DateTime.to_unix(DateTime.add(t0, 604_800, :second))}
      },
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => nil},
      "spend_control" => %{"reached" => false}
    }

    assert {:ok, result} = Evidence.CodexParsers.parse_codex_usage_result(payload, at)

    for evidence <- result.windows do
      assert {:ok, _} = EvidenceStore.record_evidence(identity, Evidence.to_window_attrs(evidence), at, at)
    end

    {primary_row!(identity), result}
  end

  defp primary_row!(identity) do
    Repo.one!(from window in AccountQuotaWindow, where: window.upstream_identity_id == ^identity.id and window.window_kind == "primary" and window.source == "codex_usage_api")
  end

  defp idle_routing_snapshot(identity, result, at) do
    epoch = CredentialFencing.credential_epoch(identity)
    metadata = identity.metadata |> AccountAvailabilityStore.transition(result.account_availability, at, epoch) |> CapacityFactsStore.record_observations([result.capacity_facts], epoch)
    windows = Repo.all(from window in AccountQuotaWindow, where: window.upstream_identity_id == ^identity.id)
    RoutingQuotaSnapshot.from_identity(%{identity | metadata: metadata}, windows, at)
  end

  defp record!(identity, source, used_percent, reset_at, observed_at, metadata),
    do: record!(identity, source, used_percent, reset_at, observed_at, metadata, 300)

  defp record!(
         identity,
         source,
         used_percent,
         reset_at,
         observed_at,
         metadata,
         window_minutes
       ) do
    EvidenceStore.record_evidence(
      identity,
      %{
        quota_key: "account",
        quota_scope: "account",
        quota_family: "account",
        window_kind: "primary",
        window_minutes: window_minutes,
        used_percent: Decimal.new(used_percent),
        reset_at: reset_at,
        observed_at: observed_at,
        last_sync_at: observed_at,
        source: source,
        source_precision: "observed",
        freshness_state: "fresh",
        metadata: Map.put(metadata, "reset_after_seconds", DateTime.diff(reset_at, observed_at, :second))
      },
      observed_at,
      observed_at
    )
  end

  defp safe_status, do: %{"rate_limit_allowed" => true, "rate_limit_reached" => false}
end
