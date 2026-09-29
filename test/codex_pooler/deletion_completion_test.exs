defmodule CodexPooler.DeletionCompletionTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access.APIKeys.Deletion, as: KeyDeletion
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Jobs.{APIKeyDeletionWorker, PoolDeletionWorker}
  alias CodexPooler.Pools.{Deletion, Pool}
  alias CodexPooler.Repo

  setup do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    %{owner: owner, scope: Scope.for_user(owner, ["instance_owner"])}
  end

  test "scheduling records deletion intent once, before any destructive batch", %{owner: owner, scope: scope} do
    pool = pool_fixture(%{status: "archived"})
    %{api_key: key} = active_api_key_fixture(pool_fixture(), %{scope: scope})
    assert {:deleting, _} = Deletion.schedule(owner, pool)
    assert {:deleting, _} = Deletion.schedule(owner, pool)
    assert {:deleting, _} = KeyDeletion.schedule(scope, key)
    assert {:deleting, _} = KeyDeletion.schedule(scope, key)

    for {action, id} <- [{"pool.delete_requested", pool.id}, {"api_key.delete_requested", key.id}] do
      assert [%{actor_user_id: actor_id}] = Repo.all(from e in AuditEvent, where: e.action == ^action and e.target_id == ^id)
      assert actor_id == owner.id
    end

    assert Repo.exists?(from p in Pool, where: p.id == ^pool.id)
    refute Repo.exists?(from e in AuditEvent, where: e.action in ["pool.delete", "api_key.delete"])
  end

  test "cancelled deletion jobs are idle while discarded jobs remain failed", %{owner: owner, scope: scope} do
    pool = pool_fixture(%{status: "archived"})
    %{api_key: key} = active_api_key_fixture(pool_fixture(), %{scope: scope})
    assert {:deleting, _} = Deletion.schedule(owner, pool)
    assert {:deleting, _} = KeyDeletion.schedule(scope, key)

    for {worker, field, id, module} <- [{PoolDeletionWorker, "pool_id", pool.id, Deletion}, {APIKeyDeletionWorker, "api_key_id", key.id, KeyDeletion}] do
      [job] = all_enqueued(worker: worker, args: %{field => id})
      assert :ok = Oban.cancel_job(job)
      assert module.states([id]) == %{}
      assert module.pending?(id) == false
      Repo.update!(Ecto.Changeset.change(job, state: "discarded"))
      assert module.states([id]) == %{id => :failed}
      {:ok, newer} = Oban.insert(worker.new(%{field => id}))
      assert :ok = Oban.cancel_job(newer)
      assert module.states([id]) == %{}
    end
  end

  test "deletion state lookups use the existing JSON containment index", %{owner: owner, scope: scope} do
    pool = pool_fixture(%{status: "archived"})
    assert {:deleting, _} = Deletion.schedule(owner, pool)
    Repo.query!("INSERT INTO oban_jobs (state, queue, worker, args) SELECT 'available', 'jobs', 'CodexPooler.Jobs.PoolDeletionWorker', jsonb_build_object('pool_id', md5(g::text)) FROM generate_series(1, 20000) g")
    # The bulk insert leaves its entries in the GIN index's pending list, which
    # autovacuum merges in a live database; until then the planner prices the
    # index by that list (about 850 here against 22 once merged) and a seq scan
    # can win on table statistics alone, as it did on Drone 1709.
    Repo.query!("SELECT gin_clean_pending_list('oban_jobs_args_index')")
    CodexPooler.PlannerStatistics.analyze!(["oban_jobs"])
    capture = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(capture) end)
    :telemetry.attach(capture, [:codex_pooler, :repo, :query], &__MODULE__.capture_state_query/4, self())
    assert Deletion.states([pool.id]) == %{pool.id => :in_progress}
    :telemetry.detach(capture)
    assert_receive {:state_query, sql, params}
    assert state_lookup_plan(sql, params) =~ "oban_jobs_args_index"

    %{api_key: key} = active_api_key_fixture(pool_fixture(), %{scope: scope})
    assert {:deleting, _} = KeyDeletion.schedule(scope, key)
    :telemetry.attach(capture, [:codex_pooler, :repo, :query], &__MODULE__.capture_state_query/4, self())
    assert KeyDeletion.states([key.id]) == %{key.id => :in_progress}
    :telemetry.detach(capture)
    assert_receive {:state_query, sql, params}
    assert state_lookup_plan(sql, params) =~ "oban_jobs_args_index"
  end

  # Plans the captured lookup with sequential scans priced out, as the other plan
  # tests do: whether the lookup can use the containment index is a property of its
  # predicate, not of how the planner weighs this test's table statistics. A
  # predicate the index cannot serve still plans a sequential scan here.
  defp state_lookup_plan(sql, params) do
    Repo.query!("SET LOCAL enable_seqscan = off")
    %{rows: [[plan]]} = Repo.query!("EXPLAIN (FORMAT JSON) " <> sql, params)
    inspect(plan)
  after
    Repo.query!("SET LOCAL enable_seqscan = on")
  end

  for kind <- [:pool, :key] do
    test "#{kind} partial batches retain operator attribution when a later batch fails", %{owner: owner, scope: scope} do
      pool = pool_fixture()
      %{api_key: key} = active_api_key_fixture(pool, %{scope: scope})
      request = request_fixture(%{pool: pool, api_key: key})
      now = DateTime.utc_now()
      Repo.insert!(%CodexSession{pool_id: pool.id, api_key_id: key.id, session_key: "partial-#{pool.id}", status: "active", created_at: now, updated_at: now})
      pool = if unquote(kind) == :pool, do: Repo.update!(Ecto.Changeset.change(pool, status: "archived")), else: pool

      {worker, args, id, action} =
        if unquote(kind) == :pool do
          assert {:deleting, _} = Deletion.schedule(owner, pool)
          {PoolDeletionWorker, %{"pool_id" => pool.id, "requested_by_user_id" => owner.id}, pool.id, "pool.delete_requested"}
        else
          assert {:deleting, _} = KeyDeletion.schedule(scope, key)
          {APIKeyDeletionWorker, %{"api_key_id" => key.id, "requested_by_user_id" => owner.id}, key.id, "api_key.delete_requested"}
        end

      Repo.query!("CREATE FUNCTION pg_temp.fail_later_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'sample interruption' USING ERRCODE = 'query_canceled'; END $$")
      Repo.query!("CREATE TRIGGER fail_later_batch BEFORE DELETE ON codex_sessions FOR EACH ROW EXECUTE FUNCTION pg_temp.fail_later_batch()")
      assert {:error, _} = perform_job(worker, args)
      if unquote(kind) == :pool, do: refute(Repo.get(CodexPooler.Accounting.Request, request.id)), else: assert(is_nil(Repo.get!(CodexPooler.Accounting.Request, request.id).api_key_id))
      assert [%{actor_user_id: actor_id}] = Repo.all(from e in AuditEvent, where: e.action == ^action and e.target_id == ^id)
      assert actor_id == owner.id
      [job] = all_enqueued(worker: worker, args: args)
      Repo.update!(Ecto.Changeset.change(job, state: "discarded"))
      assert Repo.get(Pool, pool.id)
      assert Repo.exists?(from e in AuditEvent, where: e.action == ^action and e.target_id == ^id)
      Repo.query!("DROP TRIGGER fail_later_batch ON codex_sessions")
    end
  end

  test "an audit insertion failure rolls back the newly scheduled job", %{owner: owner, scope: scope} do
    pool = pool_fixture(%{status: "archived"})
    %{api_key: key} = active_api_key_fixture(pool_fixture(), %{scope: scope})
    Repo.query!("CREATE FUNCTION pg_temp.reject_deletion_intent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action IN ('pool.delete_requested', 'api_key.delete_requested') THEN RAISE EXCEPTION 'sample audit rejection' USING ERRCODE = 'query_canceled'; END IF; RETURN NEW; END $$")
    Repo.query!("CREATE TRIGGER reject_deletion_intent BEFORE INSERT ON audit_events FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_deletion_intent()")

    for action <- [fn -> Deletion.schedule(owner, pool) end, fn -> KeyDeletion.schedule(scope, key) end] do
      assert_raise Postgrex.Error, action
    end

    refute_enqueued(worker: PoolDeletionWorker, args: %{"pool_id" => pool.id})
    refute_enqueued(worker: APIKeyDeletionWorker, args: %{"api_key_id" => key.id})
    assert Repo.get!(CodexPooler.Access.APIKey, key.id).status == "revoked"
  end

  def capture_state_query(_event, _measurements, %{query: query, params: params}, owner) do
    if self() == owner and String.starts_with?(query, "SELECT") and query =~ ~s("oban_jobs"), do: send(owner, {:state_query, query, params})
  end
end
