defmodule CodexPooler.Gateway.Persistence.RuntimeCleanupBatchTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture
  import Ecto.Query

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, BridgeSessionAlias, CodexSession, RuntimeCleanup}
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Repo

  for plan_cache_mode <- ["force_custom_plan", "force_generic_plan"] do
    @tag plan_cache_mode: plan_cache_mode
    test "retirement skips a locked row and uses its bounded index with #{plan_cache_mode}", %{plan_cache_mode: plan_cache_mode} do
      slug = "retirement-lock-#{System.unique_integer([:positive])}"

      register_unboxed_cleanup!(fn ->
        ids = Repo.all(from pool in CodexPooler.Pools.Pool, where: pool.slug == ^slug, select: pool.id)
        delete_committed_pools!(ids)
        Repo.query!("ANALYZE codex_sessions, bridge_session_aliases, bridge_owner_leases, codex_turns")
      end)

      now = InstancePresence.database_now()
      cutoff = DateTime.add(now, -(OperationalSettings.current().expired_alias_ttl_seconds + 60), :second)

      {pool_id, oldest_id} =
        run_unboxed(fn ->
          pool = pool_fixture(%{slug: slug})
          %{api_key: key} = active_api_key_fixture(pool)

          rows =
            for n <- 1..10_000 do
              at = if n <= 3, do: DateTime.add(cutoff, n, :second), else: now
              %{id: Ecto.UUID.generate(), pool_id: pool.id, api_key_id: key.id, session_key: "retired-lock-#{n}", status: "active", owner_instance_id: "sample-owner", owner_lease_token: Ecto.UUID.generate(), owner_lease_expires_at: at, last_heartbeat_at: at, created_at: at, updated_at: at}
            end

          # Enough recent sessions to keep the selective ordered index cheaper than
          # a whole-table scan after earlier cases leave dead alias/index entries.
          Enum.each(Enum.chunk_every(rows, 2_000), &Repo.insert_all(CodexSession, &1))
          prime_previous_run_statistics!(pool.id, key.id, rows, now)
          {pool.id, hd(rows).id}
        end)

      blocker = start_supervised!({Postgrex, Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database])})
      Postgrex.query!(blocker, "BEGIN", [])
      Postgrex.query!(blocker, "SELECT id FROM codex_sessions WHERE id=$1 FOR UPDATE", [Ecto.UUID.dump!(oldest_id)])
      handler = {__MODULE__, make_ref()}
      parent = self()
      on_exit(fn -> :telemetry.detach(handler) end)
      :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture_retirement_query/4, parent)

      assert {:ok, %{closed_retired_sessions: 2}} =
               run_unboxed(fn ->
                 Process.put({__MODULE__, :capture}, true)

                 Repo.transaction(fn ->
                   Repo.query!("SET LOCAL lock_timeout = '300ms'")
                   assert {:ok, result} = RuntimeCleanup.cleanup_expired(now)
                   result
                 end)
               end)

      :telemetry.detach(handler)
      assert_receive {:retirement_query, query, params}
      assert run_unboxed(fn -> Repo.get!(CodexSession, oldest_id).status end) == "active"
      Postgrex.query!(blocker, "COMMIT", [])
      # The joins need current fixture statistics too: a previous test can leave
      # empty alias/lease tables estimated as hundreds of live rows and select a
      # hash-join/seqscan plan. Keep one eligible row for a non-vacuous index proof.
      plan =
        run_unboxed(fn ->
          Repo.query!("ANALYZE codex_sessions, bridge_session_aliases, bridge_owner_leases, codex_turns")
          explain_prepared_retirement!(query, params, plan_cache_mode, oldest_id)
        end)

      nodes = plan_nodes(plan["Plan"])
      index_scan = Enum.find(nodes, &(&1["Index Name"] == "codex_sessions_retirement_idx"))
      assert index_scan, "retirement index absent: #{inspect(plan_summary(nodes))}"
      assert index_scan["Actual Rows"] == 1
      assert plan["Plan"]["Node Type"] == "Limit"
      assert plan["Plan"]["Actual Rows"] == 1
      assert plan["Plan"]["Actual Rows"] <= 500
      assert {:ok, %{closed_retired_sessions: 1}} = run_unboxed(fn -> RuntimeCleanup.cleanup_expired(now) end)
      assert run_unboxed(fn -> Repo.aggregate(from(s in CodexSession, where: s.pool_id == ^pool_id and s.status == "closed"), :count) end) == 3
    end
  end

  defp explain_prepared_retirement!(query, params, plan_cache_mode, oldest_id) do
    name = "retirement_plan_#{System.unique_integer([:positive])}"

    {:ok, plan} =
      Repo.transaction(fn ->
        Repo.query!("SET LOCAL plan_cache_mode = '#{plan_cache_mode}'")
        assert %{rows: [[id]]} = Repo.query!(query, params, cache_statement: name)
        assert Ecto.UUID.load!(id) == oldest_id

        # EXPLAIN with bind parameters describes a new unnamed statement. Execute the
        # actual named statement so the regression covers production's generic-plan path.
        values = Enum.map_join(params, ", ", &sql_literal/1)
        %{rows: [[[plan]]]} = Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE #{name}(#{values})")
        %{rows: [[generic, custom]]} = Repo.query!("SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = $1", [name])
        assert {generic, custom} == if(plan_cache_mode == "force_generic_plan", do: {2, 0}, else: {0, 2})
        Repo.query!("DEALLOCATE #{name}")
        plan
      end)

    plan
  end

  defp sql_literal(%DateTime{} = value), do: "TIMESTAMPTZ '#{DateTime.to_iso8601(value)}'"
  defp sql_literal(value) when is_list(value), do: "ARRAY[#{Enum.map_join(value, ", ", &sql_literal/1)}]::text[]"
  defp sql_literal(value) when is_binary(value), do: "'#{String.replace(value, "'", "''")}'"
  defp sql_literal(value) when is_integer(value), do: Integer.to_string(value)

  # Model retained planner statistics from earlier tests without keeping any
  # of their rows: ANALYZE does not automatically follow a committed DELETE.
  defp prime_previous_run_statistics!(pool_id, key_id, sessions, now) do
    aliases =
      for row <- Enum.take(sessions, 300),
          do: %{id: Ecto.UUID.generate(), codex_session_id: row.id, pool_id: pool_id, api_key_id: key_id, alias_kind: "session_header", alias_hash: :crypto.hash(:sha256, row.id), alias_preview: "sample", status: "active", expires_at: now, last_seen_at: now, created_at: now, updated_at: now}

    Repo.insert_all(BridgeSessionAlias, aliases)

    leases =
      for row <- Enum.take(sessions, 300),
          do: %{id: Ecto.UUID.generate(), codex_session_id: row.id, pool_id: pool_id, api_key_id: key_id, owner_instance_id: "sample-owner", lease_token: Ecto.UUID.generate(), status: "active", acquired_at: now, renewed_at: now, expires_at: now, created_at: now, updated_at: now}

    Repo.insert_all(BridgeOwnerLease, leases)
    Repo.query!("ANALYZE bridge_session_aliases, bridge_owner_leases")
    Repo.delete_all(from a in BridgeSessionAlias, where: a.pool_id == ^pool_id)
    Repo.delete_all(from l in BridgeOwnerLease, where: l.pool_id == ^pool_id)
  end

  defp plan_summary(nodes), do: Enum.map(nodes, &Map.take(&1, ["Node Type", "Relation Name", "Index Name", "Actual Rows", "Plan Rows", "Rows Removed by Filter", "Shared Hit Blocks", "Shared Read Blocks", "Total Cost"]))

  defp plan_nodes(node), do: [node | Enum.flat_map(Map.get(node, "Plans", []), &plan_nodes/1)]

  def capture_retirement_query(_event, _measurements, metadata, parent) do
    if Process.get({__MODULE__, :capture}) and String.contains?(metadata.query, "SKIP LOCKED") and String.contains?(metadata.query, "codex_sessions") do
      send(parent, {:retirement_query, metadata.query, metadata.params})
    end
  end
end
