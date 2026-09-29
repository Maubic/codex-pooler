defmodule CodexPooler.Accounting.TokenWindowEdgePlanTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting.RequestLifecycle.WindowUsage
  alias CodexPooler.TestDiagnostics

  # Enough rows to distinguish bounded edge discovery (at most 50 rows) from a
  # retained-history scan, and linear terminal lookup from a quadratic join.
  # Larger fixtures mostly measure accounting trigger work during insertion.
  @retained_histories 500

  # The tables the window query reads, with their indexes.
  @tables ["ledger_entries", "api_key_usage_buckets", "requests", "attempts"]

  setup tags do
    case tags[:statistics] do
      :empty -> put_statistics!(:empty)
      # The analyzed arm starts from missing statistics too, so its ANALYZE
      # writes over this transaction's own version of each `pg_class` row and
      # its counts go with the rollback.
      statistics when statistics in [:missing, :analyzed] -> put_statistics!(:missing)
      nil -> :ok
    end

    stats("before_seed")
    :ok
  end

  # The statistics a plan is measured under are written in the test's own
  # transaction, never inherited. An ANALYZE inside an earlier test's
  # rolled-back transaction keeps its `pg_class` row and page counts, and the
  # scheduled analyze and vacuum sample and truncate what other tests left.
  # Counts of about ten rows (9 over 46 pages, as a partition run left them)
  # over a ledger heap the vacuum had truncated to a few dozen pages, with
  # indexes still grown by earlier tests' rolled-back rows, made every range
  # lookup here a sequential scan of that small heap, its cheapest plan
  # (findings#270 row 270-321).
  #
  # Missing: nobody analyzed the tables yet (a new database, or right after a
  # restore). Empty: statistics taken while the tables were empty, over
  # physical pages, so they underestimate bulk data.
  defp put_statistics!(:missing) do
    for relation <- relations(), do: Repo.query!("SELECT pg_clear_relation_stats('public', $1)", [relation])
    clear_attribute_statistics!()

    assert %{rows: [[0, true]]} =
             Repo.query!("SELECT (SELECT count(*) FROM pg_stats WHERE schemaname = 'public' AND tablename = ANY($1)), bool_and(reltuples = -1) FROM pg_class WHERE relname = ANY($2) AND relnamespace = 'public'::regnamespace", [@tables, relations()])
  end

  defp put_statistics!(:empty) do
    for relation <- relations() do
      Repo.query!("SELECT pg_restore_relation_stats('schemaname', 'public', 'relname', $1::text, 'reltuples', 0::real, 'relpages', 1::integer, 'relallvisible', 0::integer)", [relation])
    end

    clear_attribute_statistics!()

    assert %{rows: [[0, true]]} =
             Repo.query!("SELECT (SELECT count(*) FROM pg_stats WHERE schemaname = 'public' AND tablename = ANY($1)), bool_and(reltuples = 0 AND relpages > 0) FROM pg_class WHERE relname = ANY($2) AND relnamespace = 'public'::regnamespace", [@tables, relations()])
  end

  defp relations do
    %{rows: rows} = Repo.query!("SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid = ANY($1::text[]::regclass[])", [Enum.map(@tables, &("public." <> &1))])
    @tables ++ Enum.map(rows, fn [index] -> String.replace_prefix(index, "public.", "") end)
  end

  defp clear_attribute_statistics! do
    for table <- @tables do
      Repo.query!("SELECT pg_clear_attribute_stats('public', $1, attname, false) FROM pg_attribute WHERE attrelid = $1::text::regclass AND attnum > 0 AND NOT attisdropped", [table])
    end
  end

  defp stats(stage) do
    if TestDiagnostics.enabled?() do
      rows =
        Repo.query!("SELECT c.relname,c.reltuples,c.relpages,s.n_live_tup,s.n_dead_tup,s.n_mod_since_analyze,s.analyze_count,s.autoanalyze_count FROM pg_class c JOIN pg_stat_all_tables s ON s.relid=c.oid WHERE c.oid IN ('ledger_entries'::regclass,'api_key_usage_buckets'::regclass)").rows

      attributes =
        Repo.query!("SELECT tablename,attname,null_frac,n_distinct,array_length(most_common_freqs,1) FROM pg_stats WHERE schemaname='public' AND tablename IN ('ledger_entries','api_key_usage_buckets') AND attname IN ('api_key_id','request_id','entry_kind','occurred_at','bucket_started_at') ORDER BY tablename,attname").rows

      TestDiagnostics.puts(
        CodexPooler.JSON.encode!(%{
          scenario: "table_stats",
          stage: stage,
          rows: rows,
          attributes: attributes
        })
      )
    end
  end

  for statistics <- [:missing, :empty, :analyzed] do
    @tag statistics: statistics
    test "#{statistics} current-minute histories use a set projection with no per-request event function",
         %{statistics: statistics} do
      fixture = accounting_setup()
      at = ~U[2026-09-21 12:00:30.000000Z]
      key = Ecto.UUID.dump!(fixture.api_key.id)
      pool = Ecto.UUID.dump!(fixture.pool.id)

      Repo.query!(
        """
        INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
        SELECT $1,$2,'synthetic-model','/v1/responses','http_json','edge-plan-'||n,$3
        FROM generate_series(1,1000) n
        """,
        [pool, key, at]
      )

      for {kind, usage, tokens} <- [
            {"reservation", "usage_pending", 512},
            {"release", "usage_unknown", 512}
          ] do
        Repo.query!(
          """
          INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,request_count,occurred_at,transport)
          SELECT pool_id,api_key_id,id,$1,$2,$3,1,admitted_at,'http_json'
          FROM requests WHERE api_key_id=$4
          """,
          [kind, usage, tokens, key]
        )
      end

      if statistics == :analyzed, do: analyze_tables()

      handler = "edge-plan-#{System.unique_integer([:positive])}"
      on_exit(fn -> :telemetry.detach(handler) end)

      :ok =
        :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture/4, self())

      try do
        assert %{day: %{effective_request_count: 1000, effective_total_tokens: 0}} =
                 WindowUsage.window_usages(
                   fixture.api_key.id,
                   [day: DateTime.add(at, -86_400), minute: DateTime.add(at, -60)],
                   at
                 )

        assert_receive {:window_query, query, params}

        %{rows: [[[explain]]]} =
          Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> query, params)

        nodes = nodes(explain["Plan"])

        TestDiagnostics.puts(
          CodexPooler.JSON.encode!(%{
            scenario: "current_plan",
            statistics: statistics,
            plan: explain
          })
        )

        function_nodes = Enum.filter(nodes, &(&1["Function Name"] == "api_key_usage_events"))

        TestDiagnostics.puts("edge_plan histories=1000 event_function_nodes=#{length(function_nodes)} execution_ms=#{explain["Execution Time"]}")

        assert function_nodes == [],
               "edge projection must not invoke the event function once per retained request"

        comparisons = Enum.reduce(nodes, 0, &(&2 + Map.get(&1, "Rows Removed by Join Filter", 0)))
        TestDiagnostics.puts("edge_plan join_filter_comparisons=#{comparisons}")

        assert comparisons < 10_000,
               "edge projection must not compare every terminal with every reservation"

        edge_history = Enum.find(nodes, &(&1["Subplan Name"] == "CTE edge_history"))

        assert edge_history["Actual Rows"] == 0,
               "fully included current-minute histories must use their additive bucket"
      after
        :telemetry.detach(handler)
      end
    end
  end

  def capture(_event, measurements, metadata, owner) do
    if self() == owner and String.starts_with?(metadata.query, "WITH bounds") do
      TestDiagnostics.puts(CodexPooler.JSON.encode!(%{scenario: "window_query_timing", measurements: measurements}))

      send(owner, {:window_query, metadata.query, metadata.params})
    end
  end

  for boundary <- [:none, :few], statistics <- [:missing, :empty] do
    @tag boundary: boundary, statistics: statistics
    test "#{statistics} #{boundary} excluded boundaries stay bounded with retained finalized histories",
         %{
           boundary: boundary,
           statistics: statistics
         } do
      fixture = accounting_setup()
      other = accounting_setup()
      at = ~U[2026-09-21 12:00:30.000000Z]
      key = Ecto.UUID.dump!(fixture.api_key.id)

      for setup <- [fixture, other] do
        insert_retained_histories(setup, at)
      end

      handler = "retained-edge-plan-#{System.unique_integer([:positive])}"
      on_exit(fn -> :telemetry.detach(handler) end)

      :ok =
        :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture/4, self())

      if boundary == :few, do: insert_boundary_releases(fixture, at)

      observations =
        try do
          stats("after_seed")

          for analyzed <- [false, true] do
            if analyzed do
              analyze_tables()
              stats("after_analyze")
            end

            usage =
              WindowUsage.window_usages(
                fixture.api_key.id,
                [
                  day: DateTime.add(at, -86_400),
                  minute: DateTime.add(at, -60),
                  same: DateTime.add(at, -10)
                ],
                at
              )

            assert usage.day.effective_request_count ==
                     if(boundary == :few, do: @retained_histories + 1, else: @retained_histories)

            assert usage.minute.effective_request_count == @retained_histories
            assert usage.same.effective_request_count == @retained_histories
            assert usage.day.effective_total_tokens == 0
            assert Enum.all?(usage, fn {_window, values} -> values.pending_total_tokens == 0 end)
            assert_receive {:window_query, query, params}

            %{rows: [[[plan]]]} =
              Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> query, params)

            history = Enum.find(nodes(plan["Plan"]), &(&1["Subplan Name"] == "CTE edge_history"))
            pending = Enum.find(nodes(plan["Plan"]), &(&1["Subplan Name"] == "CTE pending"))
            pending_nodes = nodes(pending)
            comparisons = join_comparisons(pending_nodes)

            terminal_scans =
              pending_nodes
              |> Enum.filter(&(&1["Relation Name"] == "ledger_entries" and &1["Alias"] != "r"))
              |> scanned_rows()

            assert comparisons < @retained_histories,
                   "pending lookup must not compare every terminal with every reservation: #{comparisons} comparisons"

            assert terminal_scans <= @retained_histories * 2 + 6,
                   "pending terminal lookup must remain linear in retained histories: #{terminal_scans} scanned rows"

            scanned =
              history
              |> nodes()
              |> Enum.filter(&(&1["Relation Name"] == "ledger_entries"))
              |> Enum.map(
                &((&1["Actual Rows"] + Map.get(&1, "Rows Removed by Filter", 0)) *
                    &1["Actual Loops"])
              )
              |> Enum.sum()

            TestDiagnostics.puts(
              CodexPooler.JSON.encode!(%{
                scenario: "retained_edge_discovery",
                boundary: boundary,
                statistics: statistics,
                analyzed: analyzed,
                histories_per_key: @retained_histories,
                keys: 2,
                scanned_edge_rows: scanned,
                edge_history_rows: history["Actual Rows"],
                pending_join_comparisons: comparisons,
                pending_terminal_scans: terminal_scans,
                query_sha256: Base.encode16(:crypto.hash(:sha256, query), case: :lower),
                plan: plan
              })
            )

            {boundary, analyzed, scanned, history["Actual Rows"]}
          end
        after
          :telemetry.detach(handler)
        end

      assert Enum.all?(observations, fn {boundary, _analyzed, scanned, histories} ->
               scanned <= 50 and histories == if(boundary == :few, do: 6, else: 0)
             end),
             "excluded edges must not scan retained key history: #{inspect(observations)}"

      [[ledger_count]] =
        Repo.query!("SELECT count(*) FROM ledger_entries WHERE api_key_id=$1", [key]).rows

      assert ledger_count == @retained_histories * 2 + if(boundary == :few, do: 6, else: 0)
    end
  end

  defp analyze_tables do
    Repo.query!("ANALYZE ledger_entries")
    Repo.query!("ANALYZE api_key_usage_buckets")
  end

  defp join_comparisons(nodes) do
    Enum.reduce(
      nodes,
      0,
      &(&2 + Map.get(&1, "Rows Removed by Join Filter", 0) * &1["Actual Loops"])
    )
  end

  defp scanned_rows(nodes) do
    Enum.reduce(nodes, 0, fn node, total ->
      total +
        (node["Actual Rows"] + Map.get(node, "Rows Removed by Filter", 0)) * node["Actual Loops"]
    end)
  end

  defp insert_retained_histories(setup, at) do
    {elapsed_us, result} =
      :timer.tc(fn ->
        Repo.query!(
          """
          WITH inserted_requests AS (
            INSERT INTO requests(
              pool_id,
              api_key_id,
              requested_model,
              endpoint,
              transport,
              correlation_id,
              admitted_at
            )
            SELECT $1,$2,'synthetic-model','/v1/responses','http_json',gen_random_uuid()::text,$3
            FROM generate_series(1,$4)
            RETURNING id,pool_id,api_key_id,admitted_at
          )
          INSERT INTO ledger_entries(
            pool_id,
            api_key_id,
            request_id,
            entry_kind,
            usage_status,
            total_tokens,
            request_count,
            occurred_at,
            transport
          )
          SELECT r.pool_id,r.api_key_id,r.id,event.kind,'usage_pending',512,1,r.admitted_at,'http_json'
          FROM inserted_requests r
          CROSS JOIN (VALUES ('reservation'),('release')) AS event(kind)
          """,
          [
            Ecto.UUID.dump!(setup.pool.id),
            Ecto.UUID.dump!(setup.api_key.id),
            at,
            @retained_histories
          ]
        )
      end)

    assert result.num_rows == @retained_histories * 2

    TestDiagnostics.puts(
      CodexPooler.JSON.encode!(%{
        scenario: "retained_seed_batch",
        histories: @retained_histories,
        ledger_rows: result.num_rows,
        elapsed_us: elapsed_us
      })
    )

    assert [[@retained_histories]] =
             Repo.query!("SELECT count(*) FROM requests WHERE api_key_id=$1", [
               Ecto.UUID.dump!(setup.api_key.id)
             ]).rows
  end

  defp insert_boundary_releases(fixture, at) do
    key = Ecto.UUID.dump!(fixture.api_key.id)

    Repo.query!(
      """
      INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
      SELECT $1,$2,'synthetic-model','/v1/responses','http_json','bounded-extra-'||gen_random_uuid(),stamp
      FROM unnest($3::timestamptz[]) stamp
      """,
      [Ecto.UUID.dump!(fixture.pool.id), key, Enum.map([-86_410, -65, 10], &DateTime.add(at, &1))]
    )

    for kind <- ["reservation", "release"] do
      Repo.query!(
        """
        INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,total_tokens,request_count,occurred_at,transport)
        SELECT pool_id,api_key_id,id,$1,'usage_pending',512,1,admitted_at,'http_json'
        FROM requests WHERE api_key_id=$2 AND correlation_id LIKE 'bounded-extra-%'
        """,
        [kind, key]
      )
    end
  end

  test "boundary subtraction matches event authority across corrections and both partial edges" do
    fixture = accounting_setup()
    origin = ~U[2026-09-20 23:59:00.000000Z]
    key = Ecto.UUID.dump!(fixture.api_key.id)
    pool = Ecto.UUID.dump!(fixture.pool.id)

    for {usage, tokens, terminal_second} <- [
          {"usage_known", 0, 0},
          {"not_applicable", 0, 15},
          {"usage_unknown", 512, 30},
          {"usage_known", 4096, 45},
          {"usage_unknown", 512, 60},
          {"usage_known", 8192, 75}
        ] do
      [[request]] =
        Repo.query!(
          """
          INSERT INTO requests(pool_id,api_key_id,requested_model,endpoint,transport,correlation_id,admitted_at)
          VALUES ($1,$2,'synthetic-model','/v1/responses','http_json',$3,$4) RETURNING id
          """,
          [pool, key, Ecto.UUID.generate(), origin]
        ).rows

      for {kind, status, amount, offset, value} <- [
            {"reservation", "usage_pending", "recorded", -30, 512},
            {"settlement", usage, "voided", terminal_second, tokens},
            {"release", usage, "recorded", terminal_second, 512},
            {"settlement", usage, "recorded", terminal_second + 120, tokens}
          ] do
        Repo.query!(
          """
          INSERT INTO ledger_entries(pool_id,api_key_id,request_id,entry_kind,usage_status,
            amount_status,total_tokens,request_count,occurred_at,transport,details)
          VALUES ($1,$2,$3,$4,$5,$6,$7,1,$8,'http_json','{"estimated_from_reserve":true}')
          """,
          [pool, key, request, kind, status, amount, value, DateTime.add(origin, offset)]
        )
      end
    end

    for {start, finish} <- [{0, 60}, {15, 45}, {30, 75}, {45, 60}, {-604_800, 75}, {60, 60}] do
      since = DateTime.add(origin, start)
      as_of = DateTime.add(origin, finish)

      [[known, provisional, admissions, cost]] =
        Repo.query!(
          """
          SELECT COALESCE(SUM(v.known_total_tokens),0)::bigint,
            COALESCE(SUM(v.provisional_total_tokens),0)::bigint,
            COALESCE(SUM(v.admission_count),0)::bigint, COALESCE(SUM(v.known_cost_micros),0)
          FROM (SELECT array_agg(e) AS entries FROM ledger_entries e WHERE api_key_id=$1 GROUP BY request_id) h
          CROSS JOIN LATERAL public.api_key_usage_events(h.entries) v
          WHERE v.occurred_at >= $2 AND v.occurred_at <= $3
          """,
          [key, since, as_of]
        ).rows

      actual = WindowUsage.window_usages(fixture.api_key.id, [window: since], as_of).window
      assert actual.known_total_tokens == known
      assert actual.provisional_total_tokens == provisional
      assert actual.effective_request_count == admissions
      assert Decimal.equal?(actual.effective_cost_micros, cost)
      assert actual.pending_total_tokens == 0
    end
  end

  defp nodes(plan), do: [plan | Enum.flat_map(Map.get(plan, "Plans", []), &nodes/1)]
end
