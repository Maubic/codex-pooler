defmodule CodexPooler.Upstreams.AccountDeletionQueryPlanTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  @tag slow: "real PostgreSQL planner boundary with 100,000 unrelated ledger rows and multi-batch deletion"
  test "sparse identity and assignment tails use bounded indexed batches with generic estimates" do
    CodexPooler.TestAppEnv.restore_on_exit(:upstream_deletion_immediate_row_limit)
    Application.put_env(:codex_pooler, :upstream_deletion_immediate_row_limit, 0)
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    %{identity: identity, assignment: first} = active_upstream_assignment_fixture()
    {:ok, second} = PoolAssignments.create_pool_assignment(pool_fixture(), identity)
    for assignment <- [first, second], do: assert({:ok, _} = PoolAssignments.delete_pool_assignment(assignment.pool_id, assignment.id))
    assert {:deleting, _} = Upstreams.delete_account_for_scope(Scope.for_user(owner), identity.id)

    # A connection-local history table isolates the planner distribution from the
    # suite's retained statistics. Clone the real indexes; no fabricated index or
    # planner cost override may make this pass. Ordinary deletion tests cover the
    # real ledger triggers, FKs, amounts and accounting projections.
    Repo.query!("CREATE TEMP TABLE ledger_entries (LIKE public.ledger_entries INCLUDING ALL) ON COMMIT DROP")
    Repo.query!("ALTER TABLE pg_temp.ledger_entries ADD COLUMN plan_padding text")
    columns = "id, request_id, api_key_id, transport, pool_id, entry_kind, amount_status, occurred_at, created_at, upstream_identity_id, pool_upstream_assignment_id, plan_padding"
    other_first = active_upstream_assignment_fixture()
    other_second = active_upstream_assignment_fixture()
    unrelated = Enum.map([other_first.identity.id, other_second.identity.id, other_first.assignment.id, other_second.assignment.id], &Ecto.UUID.dump!/1)
    pool_id = Ecto.UUID.dump!(first.pool_id)

    Repo.query!(
      """
      INSERT INTO pg_temp.ledger_entries (#{columns})
      SELECT gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'http_sse', $1, 'reservation', 'recorded', now(), now(),
        CASE WHEN n % 2 = 0 THEN $2::uuid ELSE $3::uuid END,
        CASE WHEN n % 2 = 0 THEN $4::uuid ELSE $5::uuid END, repeat('x', 200)
      FROM generate_series(1, 100000) n
      """,
      [pool_id | unrelated]
    )

    Repo.query!(
      """
      INSERT INTO pg_temp.ledger_entries (#{columns})
      SELECT gen_random_uuid(), gen_random_uuid(), gen_random_uuid(), 'http_sse', $1, 'reservation', 'recorded', now(), now(), $2,
        CASE WHEN n % 2 = 0 THEN $3::uuid ELSE $4::uuid END, repeat('x', 200)
      FROM generate_series(1, 1001) n
      """,
      [pool_id, Ecto.UUID.dump!(identity.id), Ecto.UUID.dump!(first.id), Ecto.UUID.dump!(second.id)]
    )

    Repo.query!("ANALYZE pg_temp.ledger_entries")
    CodexPooler.PlannerStatistics.analyze!(["pool_upstream_assignments"])
    Repo.query!("SET LOCAL plan_cache_mode = force_generic_plan")
    Repo.query!("CREATE TEMP TABLE deletion_tail ON COMMIT DROP AS SELECT id, upstream_identity_id, pool_upstream_assignment_id FROM pg_temp.ledger_entries WHERE upstream_identity_id = $1 LIMIT 3", [Ecto.UUID.dump!(identity.id)])

    for condition <- ["upstream_identity_id = $1", "pool_upstream_assignment_id IN (SELECT id FROM pool_upstream_assignments WHERE upstream_identity_id = $1)"] do
      baseline = explain_generic!("SELECT ctid FROM ledger_entries WHERE #{condition} LIMIT 500", [Ecto.UUID.dump!(identity.id)])
      assert Enum.any?(plan_nodes(baseline["Plan"]), &(&1["Node Type"] == "Seq Scan" and &1["Relation Name"] == "ledger_entries")), inspect(baseline)
      assert local_blocks(baseline["Plan"]) > 1_000, inspect(baseline)
    end

    receiver = self()
    target_ids = Enum.map([identity.id, first.id, second.id], &Ecto.UUID.dump!/1)
    handler_id = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, {pid, target_ids} ->
          if String.starts_with?(metadata.query, "UPDATE ledger_entries SET") and Enum.any?(List.flatten(metadata.params), &(&1 in target_ids)) do
            send(pid, {:detach_query, metadata.query, metadata.params, metadata.result})
          end
        end,
        {receiver, target_ids}
      )

    assert :deleted = Upstreams.continue_account_deletion(identity.id, owner.id, System.monotonic_time(:millisecond) + 45_000)
    refute Repo.get(UpstreamIdentity, identity.id)
    queries = collect_queries([])
    assert length(queries) == 6

    assert %{rows: [[1001]]} = Repo.query!("SELECT count(*) FROM pg_temp.ledger_entries WHERE upstream_identity_id IS NULL AND pool_upstream_assignment_id IS NULL")
    Repo.query!("UPDATE pg_temp.ledger_entries e SET upstream_identity_id = t.upstream_identity_id, pool_upstream_assignment_id = t.pool_upstream_assignment_id FROM deletion_tail t WHERE e.id = t.id")

    for field <- ["upstream_identity_id", "pool_upstream_assignment_id"] do
      batches = Enum.filter(queries, fn {sql, _, _} -> String.starts_with?(sql, "UPDATE ledger_entries SET #{field} =") end)
      assert Enum.map(batches, fn {_, _, {:ok, result}} -> result.num_rows end) == [500, 500, 1]

      {sql, params, _result} = hd(batches)
      explain = explain_generic!(sql, params)
      nodes = plan_nodes(explain["Plan"])
      assert Enum.any?(nodes, &(&1["Node Type"] == "Index Scan" and &1["Index Cond"])), inspect(explain)
      refute Enum.any?(nodes, &(&1["Node Type"] == "Seq Scan")), inspect(explain)
      selector = Enum.find(nodes, &(&1["Node Type"] == "Limit"))
      assert selector["Actual Rows"] == 3, inspect(explain)
      # Include dead target tuples from the preceding batches, but exclude the
      # UPDATE's unrelated index-maintenance writes from the selector budget.
      assert local_blocks(selector) < 256, inspect(explain)
    end

    assert %{rows: [[1001]]} = Repo.query!("SELECT count(*) FROM pg_temp.ledger_entries WHERE upstream_identity_id IS NULL AND pool_upstream_assignment_id IS NULL")
    assert %{rows: [[100_000]]} = Repo.query!("SELECT count(*) FROM pg_temp.ledger_entries WHERE upstream_identity_id IS NOT NULL AND pool_upstream_assignment_id IS NOT NULL")
  end

  defp explain_generic!(sql, params) do
    name = "deletion_plan_#{System.unique_integer([:positive])}"
    Repo.query!("PREPARE #{name} AS " <> sql)

    try do
      values = Enum.map_join(params, ", ", &sql_literal/1)
      %{rows: [[[plan]]]} = Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE #{name}(#{values})")
      assert %{rows: [[1, 0]]} = Repo.query!("SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = $1", [name])
      plan
    after
      Repo.query!("DEALLOCATE #{name}")
    end
  end

  defp sql_literal(ids) when is_list(ids), do: "ARRAY[#{Enum.map_join(ids, ", ", &sql_literal/1)}]::uuid[]"
  defp sql_literal(<<_::128>> = id), do: "'#{Ecto.UUID.load!(id)}'::uuid"
  defp sql_literal(limit) when is_integer(limit), do: Integer.to_string(limit)

  defp collect_queries(acc) do
    receive do
      {:detach_query, sql, params, result} -> collect_queries([{sql, params, result} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp local_blocks(plan), do: Map.get(plan, "Local Hit Blocks", 0) + Map.get(plan, "Local Read Blocks", 0)

  defp plan_nodes(node), do: [node | Enum.flat_map(Map.get(node, "Plans", []), &plan_nodes/1)]
end
