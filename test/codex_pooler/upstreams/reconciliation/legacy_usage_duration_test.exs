defmodule CodexPooler.Upstreams.Reconciliation.LegacyUsageDurationTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Quotas.{CapacityFacts, WindowClassifier}
  alias CodexPooler.Quotas.Evidence.CodexParsers
  alias CodexPooler.Upstreams.Quota.{AccountQuotaWindow, Windows}
  alias CodexPooler.Upstreams.Reconciliation.{PoolReconciliation, UsageProbe}

  @paths ["/backend-api/wham/usage", "/backend-api/codex/usage"]

  for slot <- ["primary", "secondary"],
      {label, duration} <- [{"missing", :missing}, {"null", nil}, {"text", "bad"}, {"zero", 0}, {"negative", -1}, {"fractional", 18_000.5}, {"numeric string", "18000"}, {"maximum plus one", 31_536_001}, {"oversized", 9_223_372_036_854_775_808}] do
    test "HTTP legacy #{slot} #{label} duration creates no persisted or saved-reset window" do
      assert_unusable_duration(unquote(slot), unquote(duration))
    end
  end

  for {slot, duration, kind, minutes, descriptor} <- [
        {"primary", 18_000, "primary", 300, :primary_5h},
        {"secondary", 604_800, "secondary", 10_080, :weekly_secondary},
        {"primary", 604_800, "secondary", 10_080, :weekly_secondary},
        {"primary", 1, "primary", 1, :unknown_account_primary},
        {"primary", 61, "primary", 2, :unknown_account_primary},
        {"primary", 31_536_000, "primary", 525_600, :unknown_account_primary}
      ] do
    test "HTTP legacy #{slot} explicit #{duration} seconds persists its exact descriptor" do
      assert_explicit_duration(unquote(slot), unquote(duration), unquote(kind), unquote(minutes), unquote(descriptor))
    end
  end

  test "missing legacy duration cannot refresh or replace a previously persisted weekly window" do
    observed_at = now()
    payload = usage_payload("secondary", :missing, observed_at)
    {fake, identity, assignment} = setup_upstream(payload)
    prior_at = DateTime.add(observed_at, -60)

    assert {:ok, [prior]} = Windows.upsert_quota_windows_from_codex_usage_payload(identity, usage_payload("secondary", 604_800, prior_at), prior_at)
    result = PoolReconciliation.refresh_quota_and_probe_from_usage(identity, assignment, observed_at: observed_at)
    assert refresh_status(result) == :upstream_quota_unusable
    assert [current] = stored_windows(identity)
    assert current.id == prior.id
    assert current.window_minutes == 10_080
    assert DateTime.compare(current.observed_at, prior.observed_at) == :eq
    assert DateTime.compare(current.reset_at, prior.reset_at) == :eq
    assert DateTime.compare(current.updated_at, prior.updated_at) == :eq
    assert Enum.map(FakeUpstream.requests(fake), & &1.path) == @paths
  end

  test "HTTP complete strict duration preserves authoritative account evidence" do
    observed_at = now()

    payload =
      usage_payload("primary", 18_000, observed_at)
      |> put_in(["rate_limit", "primary_window", "reset_after_seconds"], 900)
      |> put_in(["rate_limit", "allowed"], true)
      |> put_in(["rate_limit", "limit_reached"], false)
      |> Map.put("plan_type", "sample_plan")

    {fake, identity, assignment} = setup_upstream(payload)
    assert {:ok, _identity, probe} = PoolReconciliation.refresh_quota_and_probe_from_usage(identity, assignment, observed_at: observed_at)
    assert probe.account_availability.state == :available
    assert CapacityFacts.authority_observed?(probe.capacity_facts)
    assert [window] = stored_windows(identity)
    assert {window.window_kind, window.window_minutes} == {"primary", 300}
    assert Enum.map(FakeUpstream.requests(fake), & &1.path) == [hd(@paths)]
  end

  defp assert_unusable_duration(slot, duration) do
    observed_at = now()
    payload = usage_payload(slot, duration, observed_at)
    assert {:ok, strict} = CodexParsers.parse_codex_usage_result(payload, observed_at)
    assert strict.windows == []
    assert strict.account_availability == nil
    refute CapacityFacts.authority_observed?(strict.capacity_facts)
    {fake, identity, assignment} = setup_upstream(payload)

    {status, probe} = fetch_status(UsageProbe.fetch_from_identity(identity, assignment, observed_at, []))
    result = PoolReconciliation.refresh_quota_and_probe_from_usage(identity, assignment, observed_at: observed_at)
    assert Enum.map(stored_windows(identity), &{&1.window_kind, &1.window_minutes}) == []
    assert status == :unusable
    assert probe.windows == []
    assert MapSet.size(probe.covered_descriptors) == 0
    refute probe.usable_authority?
    refute Enum.any?(probe.windows, &WindowClassifier.saved_reset_window?/1)

    assert refresh_status(result) == :upstream_quota_unusable
    assert stored_windows(identity) == []
    assert Enum.map(FakeUpstream.requests(fake), & &1.path) == @paths ++ @paths
  end

  defp assert_explicit_duration(slot, duration, kind, minutes, descriptor) do
    observed_at = now()
    payload = usage_payload(slot, duration, observed_at)
    assert {:ok, %{windows: [], account_availability: nil}} = CodexParsers.parse_codex_usage_result(payload, observed_at)
    {fake, identity, assignment} = setup_upstream(payload)

    assert {:ok, _refreshed, probe} = PoolReconciliation.refresh_quota_and_probe_from_usage(identity, assignment, observed_at: observed_at)
    assert length(probe.windows) == 1
    assert MapSet.size(probe.covered_descriptors) == 1
    assert probe.usable_authority?
    assert [window] = stored_windows(identity)
    assert {window.window_kind, window.window_minutes} == {kind, minutes}
    assert window.metadata["limit_window_seconds"] == duration
    assert WindowClassifier.classify(window) == descriptor
    assert DateTime.compare(window.observed_at, observed_at) == :eq
    assert DateTime.compare(window.reset_at, DateTime.add(observed_at, 900)) == :eq
    expected_paths = if kind == "primary" and minutes == 300, do: [hd(@paths)], else: @paths
    assert Enum.map(FakeUpstream.requests(fake), & &1.path) == expected_paths
  end

  defp fetch_status({:ok, probe}), do: {:ok, probe}
  defp fetch_status({:error, {:capacity_observation_unusable, probe, _fence}}), do: {:unusable, probe}
  defp refresh_status({:ok, _identity, _probe}), do: :ok
  defp refresh_status({:error, %{code: code}}), do: code

  defp setup_upstream(payload) do
    name = :"legacy_duration_fake_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if pid = Process.whereis(name) do
        FakeUpstream.stop(%FakeUpstream{supervisor: pid})
        refute Process.alive?(pid)
      end

      assert Process.whereis(name) == nil
    end)

    routes = Map.new(@paths, &{&1, {200, payload}})
    assert {:ok, fake} = FakeUpstream.start_link({:path_json, routes}, supervisor_name: name)
    Process.unlink(fake.supervisor)
    %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture(pool_fixture(), %{metadata: %{"usage_base_url" => FakeUpstream.url(fake)}})
    {fake, identity, assignment}
  end

  defp stored_windows(identity), do: Repo.all(from(window in AccountQuotaWindow, where: window.upstream_identity_id == ^identity.id))

  defp usage_payload(slot, duration, observed_at) do
    window = %{"used_percent" => 25, "reset_at" => observed_at |> DateTime.add(900) |> DateTime.to_unix()}
    window = if duration == :missing, do: window, else: Map.put(window, "limit_window_seconds", duration)
    %{"rate_limit" => %{"#{slot}_window" => window}}
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second)
end
