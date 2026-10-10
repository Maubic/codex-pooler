defmodule CodexPooler.Admin.UTCUsageBucketsTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.{HourlyModelUsageRollup, LedgerEntry, Reporting, Rollups}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Admin.{Stats, UpstreamCockpitMetrics}
  alias CodexPooler.Admin.Stats.Buckets

  @zones ["UTC", "Europe/Rome", "America/New_York", "Asia/Kathmandu", "Australia/Lord_Howe"]
  @dates [~D[2026-01-10], ~D[2026-03-08], ~D[2026-03-29], ~D[2025-11-02]]
  @offsets [-60, 0, 3540, 3600, 21_540, 21_600, 37_800, 84_600]

  for zone <- @zones, date <- @dates, surface <- [:pool_raw, :pool_covered, :stats, :cockpit, :model, :rebuild] do
    test "#{surface} preserves UTC buckets on #{date} in #{zone}" do
      seed = seed_usage!(unquote(Macro.escape(date)))
      use_zone!("UTC")
      prepare_surface!(unquote(surface), seed)
      baseline = assert_surface!(unquote(surface), seed)
      use_zone!(unquote(zone))
      assert assert_surface!(unquote(surface), seed) == baseline
    end
  end

  defp seed_usage!(date) do
    seed = accounting_setup()
    %{user: owner} = bootstrap_owner_fixture()
    midnight = DateTime.new!(date, ~T[00:00:00.000000], "Etc/UTC")

    entries =
      @offsets
      |> Enum.with_index(1)
      |> Enum.map(fn {offset, ordinal} ->
        timestamp = DateTime.add(midnight, offset, :second)
        request = request_fixture(seed, %{model_id: seed.model.id}) |> Ecto.Changeset.change(admitted_at: timestamp, completed_at: timestamp) |> Repo.update!()
        attempt = attempt_fixture(request, seed.assignment) |> Ecto.Changeset.change(started_at: timestamp, completed_at: timestamp, latency_ms: 100) |> Repo.update!()
        entry = ledger_entry_fixture(request, %{attempt_id: attempt.id, pool_upstream_assignment_id: seed.assignment.id, upstream_identity_id: seed.identity.id, input_tokens: ordinal * 10, output_tokens: 0, total_tokens: ordinal * 10, settled_cost_micros: ordinal * 10, occurred_at: timestamp, created_at: timestamp})
        assert :ok = Rollups.accumulate!(request, entry)
        entry
      end)

    Map.merge(seed, %{scope: Scope.for_user(owner, ["instance_owner"]), midnight: midnight, as_of: DateTime.add(midnight, 85_500, :second), entries: entries})
  end

  defp use_zone!(zone) do
    Repo.query!("SELECT set_config('TimeZone', $1, true)", [zone])
    assert Repo.query!("SHOW TimeZone").rows == [[zone]]
  end

  defp prepare_surface!(:pool_covered, seed) do
    for offset <- 1..6 do
      assert {:ok, _count} = Rollups.rebuild_for_date(Date.add(DateTime.to_date(seed.midnight), -offset))
    end
  end

  defp prepare_surface!(_surface, _seed), do: :ok

  defp assert_surface!(surface, seed) when surface in [:pool_raw, :pool_covered] do
    for window <- ["7d", "24h"] do
      result = Stats.pool_usage_by_pool_ids([seed.pool.id], as_of: seed.as_of, traffic_window: window, histogram_pool_ids: [seed.pool.id], force_raw: surface == :pool_raw)
      histogram = result.histogram_by_pool_id[seed.pool.id]
      summary = result.summary_by_pool_id[seed.pool.id]
      expected_source = if surface == :pool_covered and window == "7d", do: :daily_rollups_with_raw_tail, else: :raw_fallback
      assert result.source == expected_source
      assert summary.total_tokens == 360
      assert summary.request_count == 8
      # Pool hourly cards keep24 labels; Stats includes the partial leading hour.
      histogram_seed = if window == "24h", do: %{seed | entries: tl(seed.entries)}, else: seed
      assert nonzero(histogram.token_histogram, :bucket, :total_tokens) == expected(histogram_seed, window, :tokens)
      assert nonzero(histogram.request_histogram, :bucket, :requests) == expected(histogram_seed, window, :requests)
      result
    end
  end

  defp assert_surface!(:stats, seed) do
    for window <- ["7d", "24h"] do
      assert {:ok, dashboard} = Stats.build_dashboard(seed.scope, %{pool_id: seed.pool.id, window: window, as_of: seed.as_of})
      assert dashboard.kpis.requests.value == 8
      assert nonzero(dashboard.charts.requests, :bucket, :requests) == expected(seed, window, :requests)
      %{requests: dashboard.charts.requests, kpis: dashboard.kpis, models: dashboard.charts.model_usage}
    end
  end

  defp assert_surface!(:cockpit, seed) do
    result = UpstreamCockpitMetrics.request_health(seed.scope, seed.identity, seed.as_of)
    assert result.kpis.total_requests_7d == 8
    assert nonzero(result.items, :date, :total_count) == expected(seed, "7d", :requests)
    result
  end

  defp assert_surface!(:model, seed) do
    for {window, label} <- [{:seven_days, "7d"}, {:twenty_four_hours, "24h"}] do
      hours = if window == :seven_days, do: 168, else: 24
      result = Reporting.model_usage_buckets_for_pool_ids([seed.pool.id], window, DateTime.add(seed.as_of, -hours, :hour), seed.as_of)
      rows = Enum.map(result.rows, &Map.update!(&1, :bucket, fn bucket -> model_label(bucket, label) end))
      assert nonzero(rows, :bucket, :total_tokens) == expected(seed, label, :tokens)
      assert Enum.sum_by(rows, & &1.total_tokens) == 360
      assert length(rows) == map_size(expected(seed, label, :tokens))
      result
    end
  end

  defp assert_surface!(:rebuild, seed) do
    started_at = DateTime.add(seed.midnight, -1, :hour)
    ended_at = DateTime.add(seed.midnight, 24, :hour)
    assert {:ok, 8} = Rollups.rebuild_hourly_model_usage_rollups_for_range(started_at, ended_at)
    rows = Repo.all(from r in HourlyModelUsageRollup, where: r.pool_id == ^seed.pool.id, order_by: r.bucket_started_at)
    assert Map.new(rows, &{model_label(&1.bucket_started_at, "24h"), &1.total_tokens}) == expected(seed, "24h", :tokens)

    # The rebuild's deletion range uses the same UTC representation as its upserts.
    removed = Enum.at(seed.entries, 6)
    assert {1, _} = Repo.delete_all(from e in LedgerEntry, where: e.id == ^removed.id)
    hour = DateTime.add(seed.midnight, 10, :hour)
    assert {:ok, 0} = Rollups.rebuild_hourly_model_usage_rollups_for_range(hour, DateTime.add(hour, 1, :hour))
    remaining = Repo.all(from r in HourlyModelUsageRollup, where: r.pool_id == ^seed.pool.id, select: {r.bucket_started_at, r.total_tokens})
    assert Map.new(remaining, fn {bucket, tokens} -> {model_label(bucket, "24h"), tokens} end) == Map.delete(expected(seed, "24h", :tokens), model_label(hour, "24h"))
    Repo.insert!(removed)
    assert {:ok, 1} = Rollups.rebuild_hourly_model_usage_rollups_for_range(hour, DateTime.add(hour, 1, :hour))
    Enum.map(rows, &Map.take(&1, [:bucket_started_at, :total_tokens, :request_count]))
  end

  defp expected(seed, window, metric) do
    seed.entries
    |> Enum.group_by(&model_label(&1.occurred_at, window))
    |> Map.new(fn {bucket, entries} -> {bucket, if(metric == :tokens, do: Enum.sum_by(entries, & &1.total_tokens), else: length(entries))} end)
  end

  defp nonzero(rows, key, value), do: rows |> Enum.reject(&(Map.fetch!(&1, value) == 0)) |> Map.new(&{Map.fetch!(&1, key), Map.fetch!(&1, value)})
  defp model_label(%Date{} = date, _window), do: Date.to_iso8601(date)
  defp model_label(datetime, "7d"), do: datetime |> DateTime.to_date() |> Date.to_iso8601()
  defp model_label(datetime, "24h"), do: Buckets.label(datetime, :twenty_four_hours)
end
