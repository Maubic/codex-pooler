defmodule CodexPooler.Catalog.SyncRetryTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1]

  alias CodexPooler.Catalog.{Model, SyncRun}
  alias CodexPooler.{FakeUpstream, Jobs, Repo, TestDiagnostics, Upstreams}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @model "sample-scheduled-recovery"

  test "real Oban Stager retries the catalog worker after a rolled-back persistence exception" do
    log = ExUnit.CaptureLog.capture_log(fn -> assert_scheduled_retry() end)
    assert log =~ "oban job failed worker=CodexPooler.Jobs.CatalogSyncWorker"
    assert log =~ "error=Postgrex.Error"
    refute log =~ "synthetic catalog persistence failure"
  end

  defp assert_scheduled_retry do
    suffix = System.unique_integer([:positive])
    oban_name = String.to_atom("catalog_retry_oban_#{suffix}")
    repo_name = String.to_atom("catalog_retry_repo_#{suffix}")
    fixture = fixture(suffix, oban_name)
    trigger = install_failure(fixture, suffix)

    database(fn ->
      assert Repo.aggregate(Oban.Job, :count) == 0, "isolated Oban runtime must not consume an unowned job"
    end)

    {:ok, job} = database(fn -> Jobs.enqueue_catalog_sync(fixture.pool, trigger_kind: "scheduled") end)
    assert job.worker == "CodexPooler.Jobs.CatalogSyncWorker"
    assert job.queue == "jobs"

    repo_opts = Repo.config() |> Keyword.put(:name, repo_name) |> Keyword.put(:pool, DBConnection.ConnectionPool) |> Keyword.put(:pool_size, 4)
    repo = start_supervised!(Supervisor.child_spec({Repo, repo_opts}, id: repo_name))
    telemetry_id = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(telemetry_id) end)
    events = [[:oban, :peer, :election, :stop], [:oban, :plugin, :stop], [:oban, :job, :start], [:oban, :job, :exception], [:oban, :job, :stop]]
    :ok = :telemetry.attach_many(telemetry_id, events, &__MODULE__.observe_oban/4, %{name: oban_name, job_id: job.id, repo: repo, parent: self()})

    oban =
      start_supervised!(
        Supervisor.child_spec({Oban, name: oban_name, repo: Repo, get_dynamic_repo: fn -> repo end, testing: :disabled, queues: [jobs: 1], plugins: [], notifier: Oban.Notifiers.PG, peer: {Oban.Peers.Database, interval: 100}, stager: {Oban.Stager, interval: 25}, shutdown_grace_period: @budget, log: false},
          id: oban_name
        )
      )

    assert_receive {:catalog_retry_leader, true}, @budget
    assert Oban.Peer.leader?(oban_name)
    assert is_pid(Oban.Registry.whereis(oban_name, Oban.Stager))
    {first_pid, first_monitor} = release_job_start(1, job.id)
    assert_receive {:catalog_retry_finished, 1, :failure}, @budget
    assert_receive {:DOWN, ^first_monitor, :process, ^first_pid, :normal}, @budget

    first_job = database(fn -> Repo.get!(Oban.Job, job.id) end)
    assert first_job.state == "retryable"
    assert first_job.attempt == 1
    assert length(first_job.errors) == 1
    assert Enum.any?(first_job.errors, &String.contains?(&1["error"], "synthetic catalog persistence failure")), "the failing trigger must observe the model written inside the transaction"
    assert DateTime.compare(first_job.scheduled_at, first_job.attempted_at) == :gt
    [failed_run] = runs(fixture)
    assert database(fn -> Repo.aggregate(from(m in Model, where: m.pool_id == ^fixture.pool.id), :count) end) == 0
    assert database(fn -> Repo.get!(PoolUpstreamAssignment, fixture.assignment.id).last_successful_sync_at end) == nil
    drop_failure(trigger)

    # Advance only this retry's due time. The real Stager still promotes the
    # retryable row, and the real queue fetches and executes attempt two.
    database(fn ->
      assert {1, _} = Repo.update_all(from(j in Oban.Job, where: j.id == ^job.id and j.state == "retryable" and j.attempt == 1), set: [scheduled_at: DateTime.add(DateTime.utc_now(), -1, :second)])
    end)

    assert_receive :catalog_retry_staged, @budget
    {second_pid, second_monitor} = release_job_start(2, job.id)
    assert_receive {:catalog_retry_finished, 2, second_state}, @budget
    assert_receive {:DOWN, ^second_monitor, :process, ^second_pid, :normal}, @budget
    final_job = database(fn -> Repo.get!(Oban.Job, job.id) end)
    requests = length(FakeUpstream.requests(fixture.provider))

    backoff_ms = DateTime.diff(first_job.scheduled_at, first_job.attempted_at, :millisecond)
    TestDiagnostics.puts("catalog-retry first_run=#{failed_run.status} first_job=#{first_job.state} first_attempt=#{first_job.attempt} original_backoff_ms=#{backoff_ms} stager_promoted=true second_result=#{second_state} final_job=#{final_job.state} final_attempt=#{final_job.attempt} provider_requests=#{requests} scheduled_at_accelerated=true")

    assert failed_run.status == "failed"
    assert %DateTime{} = failed_run.finished_at
    assert failed_run.error_message == "model catalog sync failed unexpectedly"
    assert second_state == :success
    assert final_job.state == "completed"
    assert final_job.attempt == 2
    assert length(final_job.errors) == 1
    assert requests == 2
    assert Enum.map(runs(fixture), & &1.status) == ["failed", "succeeded"]
    assert database(fn -> Repo.one!(from m in Model, where: m.pool_id == ^fixture.pool.id).exposed_model_id end) == @model
    assert :ok = stop_supervised(oban_name)
    refute Process.alive?(oban)
    assert :ok = stop_supervised(repo_name)
    refute Process.alive?(repo)
  end

  def observe_oban(event, _measurements, metadata, config) do
    if metadata[:conf].name == config.name do
      observe_owned_event(event, metadata, config)
    end

    :ok
  end

  defp observe_owned_event([:oban, :job, :start], %{job: %{id: id} = job}, %{job_id: id} = config) do
    Repo.put_dynamic_repo(config.repo)
    ref = make_ref()
    send(config.parent, {:catalog_retry_started, job.attempt, self(), ref})

    receive do
      {:continue_catalog_job, ^ref} -> :ok
    after
      @budget -> raise "catalog retry executor was not released"
    end
  end

  defp observe_owned_event([:oban, :job, ending], %{job: %{id: id} = job, state: state}, %{job_id: id} = config) when ending in [:exception, :stop],
    do: send(config.parent, {:catalog_retry_finished, job.attempt, state})

  defp observe_owned_event([:oban, :peer, :election, :stop], metadata, config),
    do: send(config.parent, {:catalog_retry_leader, metadata.leader})

  defp observe_owned_event([:oban, :plugin, :stop], %{plugin: Oban.Stager} = metadata, config) do
    if Enum.any?(metadata.staged_jobs, &(&1.id == config.job_id)), do: send(config.parent, :catalog_retry_staged)
  end

  defp observe_owned_event(_event, _metadata, _config), do: :ok

  defp release_job_start(attempt, job_id) do
    assert_receive {:catalog_retry_started, ^attempt, pid, ref}, @budget
    monitor = Process.monitor(pid)
    stored = database(fn -> Repo.get!(Oban.Job, job_id) end)
    assert stored.state == "executing"
    assert stored.attempt == attempt
    TestDiagnostics.puts("catalog-retry executing_attempt=#{attempt} observed_state=executing")
    send(pid, {:continue_catalog_job, ref})
    {pid, monitor}
  end

  defp install_failure(fixture, suffix) do
    name = "catalog_retry_failure_#{suffix}"
    on_exit(fn -> drop_failure(name) end)

    database(fn ->
      Repo.query!("""
      CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN
        IF NEW.id = '#{fixture.assignment.id}'::uuid AND NEW.last_successful_sync_at IS DISTINCT FROM OLD.last_successful_sync_at THEN
          IF EXISTS (SELECT 1 FROM models WHERE pool_id = '#{fixture.pool.id}'::uuid) THEN
            RAISE EXCEPTION 'synthetic catalog persistence failure';
          ELSE
            RAISE EXCEPTION 'catalog fixture did not reach model publication';
          END IF;
        END IF;
        RETURN NEW;
      END $$
      """)

      Repo.query!("CREATE TRIGGER #{name} BEFORE UPDATE ON pool_upstream_assignments FOR EACH ROW EXECUTE FUNCTION #{name}()")
    end)

    name
  end

  defp drop_failure(name) do
    database(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{name} ON pool_upstream_assignments")
      Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
    end)
  end

  defp fixture(suffix, oban_name) do
    slug = "catalog-retry-#{suffix}"
    account = "sample-catalog-retry-#{suffix}"

    register_unboxed_cleanup!(fn ->
      if pool = Repo.get_by(Pool, slug: slug), do: delete_committed_pools!([pool.id])
      Repo.delete_all(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
      Repo.query!("DELETE FROM oban_peers WHERE name = $1", [inspect(oban_name)])
      refute Repo.exists?(from p in Pool, where: p.slug == ^slug)
      refute Repo.exists?(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
      TestDiagnostics.puts("catalog-retry cleanup owned_fixture_removed=true")
    end)

    name = String.to_atom("catalog_retry_provider_#{suffix}")

    on_exit(fn ->
      if pid = Process.whereis(name), do: Supervisor.stop(pid)
      assert Process.whereis(name) == nil
      TestDiagnostics.puts("catalog-retry cleanup provider_stopped=true")
    end)

    {:ok, provider} = FakeUpstream.start_link(FakeUpstream.json_response(%{"models" => [%{"id" => @model}]}), supervisor_name: name)
    Process.unlink(provider.supervisor)

    database(fn ->
      pool = pool_fixture(%{slug: slug})
      source = upstream_assignment_fixture(pool, %{chatgpt_account_id: account, assignment_metadata: %{"base_url" => FakeUpstream.url(provider)}})
      assert {:ok, _} = Upstreams.store_encrypted_secret(source.identity, %{secret_kind: "access_token", plaintext: "synthetic-catalog-retry-access"})
      %{pool: pool, assignment: source.assignment, provider: provider}
    end)
  end

  defp runs(fixture), do: database(fn -> Repo.all(from r in SyncRun, where: r.pool_id == ^fixture.pool.id, order_by: r.started_at) end)
  defp database(fun), do: Sandbox.unboxed_run(Repo, fn -> Repo.checkout(fun) end)
end
