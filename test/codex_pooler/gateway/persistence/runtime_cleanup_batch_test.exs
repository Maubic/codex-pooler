defmodule CodexPooler.Gateway.Persistence.RuntimeCleanupBatchTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture
  import Ecto.Query

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.{CodexSession, RuntimeCleanup}
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Repo

  test "retirement skips a row held by another connection and remains bounded and indexable" do
    slug = "retirement-lock-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      ids = Repo.all(from pool in CodexPooler.Pools.Pool, where: pool.slug == ^slug, select: pool.id)
      delete_committed_pools!(ids)
    end)

    now = InstancePresence.database_now()
    cutoff = DateTime.add(now, -(OperationalSettings.current().expired_alias_ttl_seconds + 60), :second)

    {pool_id, oldest_id} =
      run_unboxed(fn ->
        pool = pool_fixture(%{slug: slug})
        %{api_key: key} = active_api_key_fixture(pool)

        rows =
          for n <- 1..2_000 do
            at = if n <= 3, do: DateTime.add(cutoff, n, :second), else: now
            %{id: Ecto.UUID.generate(), pool_id: pool.id, api_key_id: key.id, session_key: "retired-lock-#{n}", status: "active", owner_instance_id: "sample-owner", owner_lease_token: Ecto.UUID.generate(), owner_lease_expires_at: at, last_heartbeat_at: at, created_at: at, updated_at: at}
          end

        Repo.insert_all(CodexSession, rows)
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
    assert {:ok, %{closed_retired_sessions: 1}} = run_unboxed(fn -> RuntimeCleanup.cleanup_expired(now) end)
    assert run_unboxed(fn -> Repo.aggregate(from(s in CodexSession, where: s.pool_id == ^pool_id and s.status == "closed"), :count) end) == 3

    # A selective real plan must reach the expression index without disabling seqscan.
    plan =
      run_unboxed(fn ->
        Repo.query!("ANALYZE codex_sessions")
        %{rows: [[[explain]]]} = Repo.query!("EXPLAIN (ANALYZE, FORMAT JSON) " <> query, params)
        explain
      end)

    assert inspect(plan) =~ "codex_sessions_retirement_idx"
    assert plan["Plan"]["Actual Rows"] <= 500
  end

  def capture_retirement_query(_event, _measurements, metadata, parent) do
    if Process.get({__MODULE__, :capture}) and String.contains?(metadata.query, "SKIP LOCKED") and String.contains?(metadata.query, "codex_sessions") do
      send(parent, {:retirement_query, metadata.query, metadata.params})
    end
  end
end
