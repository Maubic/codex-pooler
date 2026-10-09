defmodule CodexPooler.Admin.StatsBoundedUsageTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Admin.Stats

  @as_of ~U[2026-03-29 23:45:00.000000Z]
  @started_at DateTime.add(@as_of, -7, :day)
  @budget 25

  for count <- [48, 480, 4_800] do
    test "dashboard returns bounded traffic projections for #{count} seeded requests" do
      seed = seed!(unquote(count))
      {dashboard, queries} = collect_queries(fn -> dashboard!(seed.scope, %{}) end)
      assert_parity!(dashboard, seed, seed.pools)
      traffic = traffic_queries(queries)
      assert traffic != []
      record_measurements(seed, queries)
      assert Enum.all?(traffic, &(&1.rows <= @budget)), "traffic query row counts: #{inspect(Enum.map(traffic, & &1.rows))}"
      CodexPooler.TestDiagnostics.puts("bounded_stats requests=#{unquote(count)} query_count=#{length(queries)} traffic_rows=#{inspect(Enum.map(traffic, & &1.rows))}")
    end
  end

  test "UTC and fractional-offset scoped projections preserve independently seeded outputs" do
    seed = seed!(480)

    for zone <- ["UTC", "Asia/Kathmandu"], pools <- [seed.pools, [hd(seed.pools)]], window <- ["1h", "5h", "24h", "7d"] do
      Repo.query!("SELECT set_config('TimeZone', $1, true)", [zone])
      assert Repo.query!("SHOW TimeZone").rows == [[zone]]
      filters = if length(pools) == 1, do: %{pool_id: hd(pools).id}, else: %{}
      dashboard = dashboard!(seed.scope, Map.put(filters, :window, window))
      assert_parity!(dashboard, seed, pools, window)
    end
  end

  test "deleted-key totals across Pools retain their label and bounded leaderboard union" do
    seed = seed!(480)
    deleted_ids = seed.entries |> Enum.take(2) |> Enum.map(& &1.ledger.api_key_id)
    Repo.update_all(from(e in LedgerEntry, where: e.api_key_id in ^deleted_ids), set: [api_key_id: nil])
    entries = Enum.map(seed.entries, fn entry -> if entry.ledger.api_key_id in deleted_ids, do: put_in(entry, [:ledger, :api_key_id], nil), else: entry end)
    seed = %{seed | entries: entries}
    dashboard = dashboard!(seed.scope, %{})
    assert_parity!(dashboard, seed, seed.pools)
    assert %{pool_name: "Multiple Pools", display_name: nil} = Enum.find(dashboard.tables.top_api_keys, &is_nil(&1.api_key_id))
    assert length(dashboard.tables.top_api_keys) <= 20
  end

  test "all unknown usage and missing latency remain unpriced and unavailable respectively" do
    seed = seed!(48)
    Repo.update_all(LedgerEntry, set: [usage_status: "usage_unknown"])
    Repo.update_all(Attempt, set: [latency_ms: nil])
    entries = Enum.map(seed.entries, fn entry -> %{entry | known?: false, attempts: Enum.map(entry.attempts, &%{&1 | latency_ms: nil})} end)
    seed = %{seed | entries: entries}
    dashboard = dashboard!(seed.scope, %{})
    assert_parity!(dashboard, seed, seed.pools)
    assert dashboard.kpis.settled_cost.status == "unpriced"
    assert dashboard.kpis.average_latency_ms.value == nil
    assert dashboard.kpis.tokens_per_second.value == nil
  end

  test "equal leaderboard ranks select deterministic key ids without exceeding ten tied entries" do
    seed = seed!(480)
    Repo.update_all(LedgerEntry, set: [usage_status: "usage_known", total_tokens: 0, settled_cost_micros: Decimal.new(0), request_count: 0])
    expected = seed.entries |> Enum.map(& &1.ledger.api_key_id) |> Enum.uniq() |> Enum.sort() |> Enum.take(10)
    dashboard = dashboard!(seed.scope, %{})
    assert Enum.sort(Enum.map(dashboard.tables.top_api_keys, & &1.api_key_id)) == expected
  end

  test "numeric sums retain the original integer range instead of narrowing to bigint" do
    seed = seed!(48)
    value = 1_000_000_000_000_000_000
    Repo.update_all(LedgerEntry, set: [usage_status: "usage_known", total_tokens: value, settled_cost_micros: Decimal.new(value)])
    dashboard = dashboard!(seed.scope, %{})
    assert dashboard.kpis.tokens.total_tokens == 46 * value
    assert dashboard.kpis.settled_cost.micros == 46 * value
  end

  defp dashboard!(scope, filters) do
    assert {:ok, dashboard} = Stats.build_dashboard(scope, Map.merge(%{window: "7d", as_of: @as_of}, filters))
    dashboard
  end

  defp seed!(count) do
    %{user: owner} = bootstrap_owner_fixture()
    pools = for _ <- 1..2, do: pool_fixture()
    assignments = Enum.map(pools, &upstream_assignment_fixture/1)
    _zero_inventory = upstream_assignment_fixture(hd(pools))
    keys = for i <- 0..23, do: active_api_key_fixture(Enum.at(pools, rem(i, 2))).api_key
    entries = for i <- 0..(count - 1), do: seed_row(i, pools, keys, assignments)
    insert_rows!(Request, Enum.map(entries, & &1.request))
    insert_rows!(Attempt, Enum.flat_map(entries, & &1.attempts))
    insert_rows!(LedgerEntry, Enum.map(entries, & &1.ledger))
    %{scope: Scope.for_user(owner, ["instance_owner"]), pools: pools, entries: entries}
  end

  defp seed_row(i, pools, keys, assignments) do
    key_index = rem(i, 24)
    pool = Enum.at(pools, rem(i, 2))
    key = Enum.at(keys, key_index)
    upstream = Enum.at(assignments, rem(i, 2))
    admitted_at = timestamp(i)
    known? = rem(i, 5) != 0
    tokens = if key_index < 12, do: 2_000 - key_index * 10, else: 10 + key_index
    cost = if key_index < 12, do: Decimal.new("0.5"), else: Decimal.new(1_000 + key_index)
    latency = if rem(i, 4) == 0, do: nil, else: 100 + rem(i, 30)
    retry? = rem(i, 3) == 0
    request_id = Ecto.UUID.generate()
    request = %{id: request_id, pool_id: pool.id, api_key_id: key.id, requested_model: "sample-model", endpoint: "/v1/responses", transport: "http_sse", status: "succeeded", usage_status: if(known?, do: "usage_known", else: "usage_unknown"), correlation_id: "sample-#{request_id}", request_metadata: %{}, admitted_at: admitted_at, completed_at: admitted_at, response_status_code: 200, retry_count: if(retry?, do: 1, else: 0)}
    attempt = %{id: Ecto.UUID.generate(), request_id: request_id, attempt_number: 1, pool_upstream_assignment_id: upstream.assignment.id, upstream_identity_id: upstream.identity.id, upstream_model_id: "sample-model", transport: "http_sse", status: "succeeded", started_at: admitted_at, completed_at: admitted_at, upstream_status_code: 200, retryable: false, latency_ms: latency, usage_status: request.usage_status, response_metadata: %{}, replay_generation: 0}
    attempts = if retry?, do: [%{attempt | id: Ecto.UUID.generate(), status: "retryable_failed", upstream_status_code: 503, retryable: true}, %{attempt | attempt_number: 2}], else: [attempt]
    ledger = %{id: Ecto.UUID.generate(), request_id: request_id, attempt_id: attempt.id, pool_id: pool.id, api_key_id: key.id, pool_upstream_assignment_id: upstream.assignment.id, upstream_identity_id: upstream.identity.id, entry_kind: "settlement", amount_status: "recorded", usage_status: request.usage_status, transport: "http_sse", currency_code: "USD", input_tokens: tokens - 4, cached_input_tokens: 2, output_tokens: 4, reasoning_tokens: 1, total_tokens: tokens, request_count: 1, estimated_cost_micros: Decimal.new(0), settled_cost_micros: cost, occurred_at: admitted_at, created_at: admitted_at, details: %{}}
    %{request: request, attempts: attempts, ledger: ledger, known?: known?}
  end

  defp timestamp(i) do
    case rem(i, 48) do
      0 -> @started_at
      1 -> @as_of
      2 -> DateTime.add(@started_at, -1, :microsecond)
      3 -> DateTime.add(@as_of, 1, :microsecond)
      offset -> DateTime.add(@as_of, -offset * 300, :second)
    end
  end

  defp insert_rows!(schema, rows), do: Enum.each(Enum.chunk_every(rows, 1_000), &Repo.insert_all(schema, &1))

  defp assert_parity!(dashboard, seed, pools, window \\ "7d") do
    pool_ids = Enum.map(pools, & &1.id)
    hours = %{"1h" => 1, "5h" => 5, "24h" => 24, "7d" => 168}[window]
    started_at = DateTime.add(@as_of, -hours, :hour)
    entries = Enum.filter(seed.entries, &(&1.request.pool_id in pool_ids and DateTime.compare(&1.request.admitted_at, started_at) != :lt and DateTime.compare(&1.request.admitted_at, @as_of) != :gt))
    known = Enum.filter(entries, & &1.known?)
    tokens = Map.new([:input_tokens, :cached_input_tokens, :output_tokens, :reasoning_tokens, :total_tokens], fn field -> {field, Enum.sum_by(known, &Map.fetch!(&1.ledger, field))} end)
    latencies = entries |> Enum.flat_map(& &1.attempts) |> Enum.map(& &1.latency_ms) |> Enum.reject(&is_nil/1)
    cost = Enum.sum_by(known, &(&1.ledger.settled_cost_micros |> Decimal.round(0) |> Decimal.to_integer()))
    assert dashboard.kpis.requests.value == length(entries)
    assert dashboard.kpis.tokens == tokens
    assert dashboard.kpis.cache_rate.value == if(tokens.input_tokens > 0, do: Float.round(tokens.cached_input_tokens / tokens.input_tokens * 100, 1), else: nil)
    assert dashboard.kpis.settled_cost.micros == cost
    expected_latency = if latencies != [], do: round(Enum.sum(latencies) / length(latencies))
    expected_speed = if tokens.total_tokens > 0 and Enum.sum(latencies) > 0, do: Float.round(tokens.total_tokens / (Enum.sum(latencies) / 1000), 2)
    assert dashboard.kpis.average_latency_ms.value == expected_latency
    assert dashboard.kpis.tokens_per_second.value == expected_speed
    assert dashboard.sources.attempts == Enum.sum_by(entries, &length(&1.attempts))
    assert dashboard.sources.settlements == length(entries)
    assert Enum.sum_by(dashboard.charts.tokens, & &1.total_tokens) == tokens.total_tokens
    assert Enum.sum_by(dashboard.charts.settled_cost, & &1.settled_cost_micros) == cost
    assert Enum.any?(dashboard.tables.upstreams, &(&1.requests == 0)) == hd(seed.pools).id in pool_ids
    assert Enum.sum_by(dashboard.tables.upstreams, & &1.requests) == length(entries)
    assert Enum.sum_by(dashboard.tables.upstreams, & &1.total_tokens) == tokens.total_tokens
    assert Enum.sum_by(dashboard.tables.upstreams, & &1.settled_cost_micros) == cost
    assert_leaderboard!(dashboard.tables.top_api_keys, entries, pools)
    assert_series!(dashboard.charts, entries, window)
    assert_upstreams!(dashboard.tables.upstreams, entries)
  end

  defp totals(entries) do
    known = Enum.filter(entries, & &1.known?)
    %{requests: Enum.sum_by(entries, & &1.ledger.request_count), total_tokens: Enum.sum_by(known, & &1.ledger.total_tokens), settled_cost_micros: Enum.sum_by(known, &(&1.ledger.settled_cost_micros |> Decimal.round(0) |> Decimal.to_integer()))}
  end

  defp assert_leaderboard!(actual, entries, pools) do
    names = Map.new(pools, &{&1.id, &1.name})
    groups = Enum.group_by(entries, & &1.ledger.api_key_id)

    rows =
      Enum.map(groups, fn {id, grouped} ->
        pool_ids = grouped |> Enum.map(& &1.ledger.pool_id) |> Enum.uniq()
        name = if length(pool_ids) == 1, do: names[hd(pool_ids)], else: "Multiple Pools"
        Map.merge(totals(grouped), %{api_key_id: id, pool_name: name})
      end)

    tokens = rows |> Enum.sort_by(&{-&1.total_tokens, -&1.requests, &1.api_key_id || ""}) |> Enum.take(10)
    cost = rows |> Enum.sort_by(&{-&1.settled_cost_micros, -&1.total_tokens, &1.api_key_id || ""}) |> Enum.take(10)
    expected = Enum.uniq_by(tokens ++ cost, & &1.api_key_id)
    assert MapSet.new(Enum.map(actual, &Map.take(&1, [:api_key_id, :pool_name, :requests, :total_tokens, :settled_cost_micros]))) == MapSet.new(expected)
  end

  defp assert_series!(charts, entries, window) do
    groups = Enum.group_by(entries, &bucket_label(&1.ledger.occurred_at, window))
    assert length(charts.requests) == %{"1h" => 2, "5h" => 6, "24h" => 25, "7d" => 8}[window]
    assert length(charts.tokens) == length(charts.requests)
    assert length(charts.settled_cost) == length(charts.requests)

    for row <- charts.tokens do
      known = groups |> Map.get(row.bucket, []) |> Enum.filter(& &1.known?)
      for field <- [:input_tokens, :cached_input_tokens, :output_tokens, :reasoning_tokens, :total_tokens], do: assert(Map.fetch!(row, field) == Enum.sum_by(known, &Map.fetch!(&1.ledger, field)))
      assert row.uncached_input_tokens == max(row.input_tokens - row.cached_input_tokens, 0)
    end

    for row <- charts.settled_cost, do: assert(row.settled_cost_micros == totals(Map.get(groups, row.bucket, [])).settled_cost_micros)
  end

  defp bucket_label(timestamp, "7d"), do: timestamp |> DateTime.to_date() |> Date.to_iso8601()
  defp bucket_label(timestamp, _window), do: Date.to_iso8601(DateTime.to_date(timestamp)) <> "T" <> String.pad_leading(Integer.to_string(timestamp.hour), 2, "0") <> ":00:00Z"

  defp assert_upstreams!(actual, entries) do
    groups = Enum.group_by(entries, & &1.ledger.upstream_identity_id)
    requests = Enum.sum_by(entries, & &1.ledger.request_count)

    for row <- actual do
      expected = totals(Map.get(groups, row.upstream_identity_id, []))
      assert Map.take(row, [:requests, :total_tokens, :settled_cost_micros]) == expected
      assert row.traffic_share_percent == Float.round(expected.requests / requests * 100, 1)
    end
  end

  defp collect_queries(fun) do
    ref = make_ref()
    handler = {__MODULE__, ref}
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.query_event/4, {self(), ref})

    try do
      result = fun.()
      {result, drain_queries(ref, [])}
    after
      :telemetry.detach(handler)
    end
  end

  def query_event(_event, _measurements, metadata, {pid, ref}) do
    if self() == pid do
      rows =
        case metadata.result do
          {:ok, %{num_rows: count}} -> count
          _ -> 0
        end

      send(pid, {ref, %{query: metadata.query, rows: rows, params: metadata.params}})
    end
  end

  defp drain_queries(ref, acc) do
    receive do
      {^ref, query} -> drain_queries(ref, [query | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp traffic_queries(queries), do: Enum.filter(queries, &(String.contains?(&1.query, ["ledger_entries", "attempts"]) and not String.starts_with?(&1.query, "EXPLAIN")))

  defp record_measurements(seed, queries) do
    if System.get_env("CODEX_POOLER_TEST_DIAGNOSTICS") == "1" do
      for _ <- 1..2, do: dashboard!(seed.scope, %{})
      samples = for _ <- 1..7, do: elem(:timer.tc(fn -> dashboard!(seed.scope, %{}) end), 0)

      plans =
        Enum.map(traffic_queries(queries), fn query ->
          %{rows: [[[plan]]]} = Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> query.query, query.params)
          %{returned_rows: query.rows, plan_rows: plan["Plan"]["Actual Rows"], node_types: plan_node_types(plan["Plan"]), execution_ms: plan["Execution Time"], shared_hits: plan["Plan"]["Shared Hit Blocks"], temp_written: plan["Plan"]["Temp Written Blocks"]}
        end)

      CodexPooler.TestDiagnostics.puts("stats_measurement " <> CodexPooler.JSON.encode!(%{seed_requests: length(seed.entries), query_count: length(queries), warmups: 2, samples: 7, median_us: samples |> Enum.sort() |> Enum.at(3), queries: plans}))
    end
  end

  defp plan_node_types(plan), do: [plan["Node Type"] | Enum.flat_map(Map.get(plan, "Plans", []), &plan_node_types/1)] |> Enum.uniq()
end
