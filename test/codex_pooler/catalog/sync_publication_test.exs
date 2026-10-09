defmodule CodexPooler.Catalog.SyncPublicationTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1]

  alias CodexPooler.Catalog.{Model, Sync, SyncRun}
  alias CodexPooler.Catalog.Sync.{Discovery, Persistence}
  alias CodexPooler.{Events, FakeUpstream, Repo, TestDiagnostics, Upstreams}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @model "sample-published"

  setup do
    supervisor = start_supervised!(Task.Supervisor)
    on_exit(fn -> refute Process.alive?(supervisor) end)
    %{supervisor: supervisor}
  end

  for outcome <- [:success, :tagged_failure, :exception] do
    test "late #{outcome} preserves the successor and old terminal receipt", context do
      assert_late_outcome(context.supervisor, unquote(outcome))
    end
  end

  test "publication locks Pool then its exact run before writes and defeats later cleanup", %{supervisor: supervisor} do
    fixture = fixture()
    input = publication_input(fixture)
    ref = install_lock_observer()

    publisher =
      actor(supervisor, fn ->
        Process.put({__MODULE__, :hold}, {self_parent(), ref, :publication})
        publish(input)
      end)

    assert_receive {^ref, :lock_held, holder, :publication}, @budget

    try do
      assert snapshot(fixture).models == []
      assert snapshot(fixture).assignment.last_successful_sync_at == nil
      assert_lock_conflict(publisher.backend, fn -> Repo.one!(from p in Pool, where: p.id == ^fixture.pool.id, lock: "FOR NO KEY UPDATE NOWAIT") end)
      assert_lock_conflict(publisher.backend, fn -> Repo.one!(from r in SyncRun, where: r.id == ^input.run.id, lock: "FOR UPDATE NOWAIT") end)
      assert_timeout(publisher.backend, fn -> Sync.cleanup_stale_sync_runs(DateTime.utc_now()) end)
    after
      send(holder, {ref, :release})
    end

    assert {:ok, result} = join(publisher)
    assert result.sync_run.status == "succeeded"
    assert {:ok, %{stale_catalog_sync_runs_failed: 0}} = database(fn -> Sync.cleanup_stale_sync_runs(DateTime.utc_now()) end)
    assert length(snapshot(fixture).models) == 2
    assert snapshot(fixture).assignment.last_successful_sync_at != nil
    TestDiagnostics.puts("catalog-publication order=publication_first backend=#{publisher.backend} pool_nowait=lock_not_available run_nowait=lock_not_available cleanup=lock_not_available publication=succeeded models=2")
  end

  test "cleanup commits first and publication cannot write from the expired claim", %{supervisor: supervisor} do
    fixture = fixture()
    input = publication_input(fixture)
    ref = install_lock_observer()

    cleanup =
      actor(supervisor, fn ->
        Process.put({__MODULE__, :hold}, {self_parent(), ref, :cleanup})
        Repo.transaction(fn -> Sync.cleanup_stale_sync_runs(DateTime.utc_now()) end)
      end)

    assert_receive {^ref, :lock_held, holder, :cleanup}, @budget

    try do
      assert_lock_conflict(cleanup.backend, fn -> Repo.one!(from r in SyncRun, where: r.id == ^input.run.id, lock: "FOR UPDATE NOWAIT") end)
      assert_timeout(cleanup.backend, fn -> publish(input) end)
      assert snapshot(fixture).models == []
      assert snapshot(fixture).assignment.last_successful_sync_at == nil
    after
      send(holder, {ref, :release})
    end

    assert {:ok, {:ok, %{stale_catalog_sync_runs_failed: 1}}} = join(cleanup)
    before = snapshot(fixture)
    assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> publish(input) end)
    assert snapshot(fixture) == before
    assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert Enum.map(snapshot(fixture).runs, & &1.status) |> Enum.sort() == ["failed", "succeeded"]
    TestDiagnostics.puts("catalog-publication order=cleanup_first backend=#{cleanup.backend} run_nowait=lock_not_available publication=lock_not_available retry=catalog_sync_superseded successor=succeeded")
  end

  test "a fresh claimant serializes behind publication while a different Pool progresses", %{supervisor: supervisor} do
    fixture = fixture()
    other = fixture()
    input = publication_input(fixture)
    database(fn -> Repo.update!(Ecto.Changeset.change(input.run, started_at: DateTime.utc_now())) end)
    ref = install_lock_observer()

    publisher =
      actor(supervisor, fn ->
        Process.put({__MODULE__, :hold}, {self_parent(), ref, :publication})
        publish(input)
      end)

    assert_receive {^ref, :lock_held, holder, :publication}, @budget

    try do
      assert_timeout(publisher.backend, fn -> Sync.sync_pool_catalog(fixture.pool) end)
      assert length(FakeUpstream.requests(fixture.provider)) == 1
      assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(other.pool) end)
      assert length(FakeUpstream.requests(other.provider)) == 1
    after
      send(holder, {ref, :release})
    end

    assert {:ok, _} = join(publisher)
    assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert length(FakeUpstream.requests(fixture.provider)) == 2
    TestDiagnostics.puts("catalog-publication fresh_claim_during_publication=lock_not_available extra_fetches=0 different_pool=succeeded later_claim=succeeded")
  end

  for status <- ["failed", "cancelled", "succeeded"] do
    test "a #{status} receipt rejects both publication and ordinary failure without writes" do
      fixture = fixture()
      input = publication_input(fixture)
      database(fn -> Repo.update!(Ecto.Changeset.change(input.run, status: unquote(status), finished_at: DateTime.utc_now(), error_message: "original receipt")) end)
      before = snapshot(fixture)
      assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> publish(input) end)
      assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> Persistence.fail_sync_run(input.run, "late failure") end)
      assert snapshot(fixture) == before
    end
  end

  test "a deleted Pool and run reject publication without stale-row exceptions" do
    fixture = fixture()
    input = publication_input(fixture)
    database(fn -> delete_committed_pools!([fixture.pool.id]) end)
    assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> publish(input) end)
    assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> Persistence.fail_sync_run(input.run, "late failure") end)
  end

  test "a deleted exact run cannot publish into its still-present Pool" do
    fixture = fixture()
    input = publication_input(fixture)
    database(fn -> Repo.delete!(input.run) end)
    before = snapshot(fixture)
    assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> publish(input) end)
    assert {:error, %{code: :catalog_sync_superseded}} = database(fn -> Persistence.fail_sync_run(input.run, "late failure") end)
    assert snapshot(fixture) == before
  end

  defp publication_input(fixture) do
    database(fn ->
      assignments = Sync.list_catalog_sync_assignments(fixture.pool)
      assert {:ok, successful, [], discovered} = Discovery.discover_models(assignments, &Discovery.fetch_models_for_assignment/1)

      run =
        %SyncRun{}
        |> SyncRun.changeset(%{pool_id: fixture.pool.id, trigger_kind: "manual", status: "running", started_at: DateTime.add(DateTime.utc_now(), -901, :second), discovered_model_count: 0, upserted_model_count: 0, stale_marked_count: 0, stats: %{}})
        |> Repo.insert!()

      %{run: run, assignments: assignments, successful: successful, discovered: discovered}
    end)
  end

  defp publish(input), do: Persistence.persist_catalog(input.run, input.assignments, input.successful, [], input.discovered)

  defp install_lock_observer do
    ref = make_ref()
    handler_id = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.observe_lock/4, nil)
    ref
  end

  def observe_lock(_event, _measurements, metadata, _config) do
    case Process.get({__MODULE__, :hold}) do
      {parent, ref, stage} ->
        lock? =
          metadata[:source] == "sync_runs" and
            ((stage == :publication and String.contains?(metadata.query, "FOR UPDATE")) or
               (stage == :cleanup and String.starts_with?(metadata.query, "UPDATE")))

        if lock? do
          Process.delete({__MODULE__, :hold})
          send(parent, {ref, :lock_held, self(), stage})

          receive do
            {^ref, :release} -> :ok
          after
            @budget -> raise "publication lock was not released"
          end
        end

      nil ->
        :ok
    end
  end

  defp assert_lock_conflict(holder_backend, fun) do
    assert {:database_error, :lock_not_available} =
             database(fn ->
               record_waiter_backend(holder_backend, :nowait)

               try do
                 Repo.transaction(fun)
               rescue
                 error in Postgrex.Error -> {:database_error, error.postgres.code}
               end
             end)
  end

  defp assert_timeout(holder_backend, fun) do
    assert {:database_error, :lock_not_available} =
             database(fn ->
               record_waiter_backend(holder_backend, :lock_timeout)
               [[previous]] = Repo.query!("SHOW lock_timeout").rows
               Repo.query!("SELECT set_config('lock_timeout', '100ms', false)")

               try do
                 fun.()
               rescue
                 error in Postgrex.Error -> {:database_error, error.postgres.code}
               after
                 Repo.query!("SELECT set_config('lock_timeout', $1, false)", [previous])
                 assert Repo.query!("SHOW lock_timeout").rows == [[previous]]
               end
             end)
  end

  defp record_waiter_backend(holder_backend, probe) do
    [[waiter_backend]] = Repo.query!("SELECT pg_backend_pid()").rows
    assert waiter_backend != holder_backend
    TestDiagnostics.puts("catalog-publication lock_probe=#{probe} holder_backend=#{holder_backend} waiter_backend=#{waiter_backend} distinct=true")
  end

  defp self_parent, do: Process.get({__MODULE__, :parent})

  defp assert_late_outcome(supervisor, outcome) do
    fixture = fixture()
    ref = make_ref()
    status = if outcome == :tagged_failure, do: 503, else: 200
    body = if outcome == :tagged_failure, do: %{}, else: catalog_body("Earlier", "sample-earlier-only")
    FakeUpstream.set_mode(fixture.provider, FakeUpstream.barrier_json_response(body, notify: self(), release_ref: ref, status: status))
    assert :ok = Events.subscribe_pool(fixture.pool, ["model_sync"])

    original =
      actor(supervisor, fn ->
        if outcome == :exception do
          try do
            Sync.sync_pool_catalog(fixture.pool,
              fetcher: fn source ->
                assert {:ok, _} = Discovery.fetch_models_for_assignment(source)
                raise ArgumentError, "synthetic late publication exception"
              end
            )
          rescue
            _error in ArgumentError -> {:caught, :error}
          end
        else
          Sync.sync_pool_catalog(fixture.pool)
        end
      end)

    assert_receive {:fake_upstream_timeout_barrier, :before_headers, handler, ^ref}, @budget
    old_run = database(fn -> Repo.one!(from r in SyncRun, where: r.pool_id == ^fixture.pool.id and r.status == "running") end)
    database(fn -> Repo.update!(Ecto.Changeset.change(old_run, started_at: DateTime.add(DateTime.utc_now(), -901, :second))) end)
    FakeUpstream.set_mode(fixture.provider, FakeUpstream.json_response(catalog_body("Current", "sample-current-only")))
    successor = actor(supervisor, fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert original.backend != successor.backend
    assert {:ok, result} = join(successor)
    assert Process.alive?(original.task.pid)
    assert database(fn -> Repo.reload!(old_run).status end) == "failed"
    assert Enum.find(result.models, &(&1.exposed_model_id == @model)).display_name == "Current"
    pool_id = fixture.pool.id
    assert_receive {Events, %{pool_id: ^pool_id, reason: "model_sync_completed"}}, @budget
    before = snapshot(fixture)
    send(handler, {:fake_upstream_release_timeout, ref})
    late_result = join(original)
    after_late = snapshot(fixture)
    late_events = model_events(pool_id)
    success_event? = Enum.any?(late_events, &(&1.reason == "model_sync_completed"))

    TestDiagnostics.puts("catalog-publication late=#{outcome} original_backend=#{original.backend} successor_backend=#{successor.backend} models_unchanged=#{after_late.models == before.models} assignment_unchanged=#{after_late.assignment == before.assignment} runs_unchanged=#{after_late.runs == before.runs} success_event=#{success_event?} outcome=#{outcome_tag(late_result)} actual_http_requests=#{length(FakeUpstream.requests(fixture.provider))}")

    assert after_late == before, "a late #{outcome} must make zero model, assignment or terminal-run writes"
    refute success_event?
    assert length(FakeUpstream.requests(fixture.provider)) == 2

    if outcome == :exception do
      assert late_result == {:caught, :error}
    else
      assert {:error, %{code: :catalog_sync_superseded}} = late_result
      assert [%{reason: "model_sync_failed", payload: %{"code" => "catalog_sync_superseded"}}] = late_events
    end
  end

  defp actor(supervisor, fun) do
    parent = self()
    ref = make_ref()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        receive do
          {:start, ^ref} ->
            database(fn ->
              Process.put({__MODULE__, :parent}, parent)
              %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
              send(parent, {ref, :backend, backend})
              fun.()
            end)
        after
          @budget -> raise "publication actor was not started"
        end
      end)

    monitor = Process.monitor(task.pid)
    send(task.pid, {:start, ref})
    assert_receive {^ref, :backend, backend}, @budget
    %{task: task, monitor: monitor, backend: backend}
  end

  defp join(%{task: task, monitor: monitor}) do
    result = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    result
  end

  defp model_events(pool_id, events \\ []) do
    receive do
      {Events, %{pool_id: ^pool_id} = event} -> model_events(pool_id, [event | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp snapshot(fixture) do
    database(fn ->
      %{
        models: Repo.all(from m in Model, where: m.pool_id == ^fixture.pool.id, order_by: m.id),
        assignment: Repo.get!(PoolUpstreamAssignment, fixture.assignment.id),
        runs: Repo.all(from r in SyncRun, where: r.pool_id == ^fixture.pool.id, order_by: r.id)
      }
    end)
  end

  defp fixture do
    suffix = System.unique_integer([:positive])
    slug = "catalog-publication-#{suffix}"
    account = "sample-catalog-publication-#{suffix}"

    register_unboxed_cleanup!(fn ->
      if pool = Repo.get_by(Pool, slug: slug), do: delete_committed_pools!([pool.id])
      Repo.delete_all(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
      refute Repo.exists?(from p in Pool, where: p.slug == ^slug)
      refute Repo.exists?(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
      TestDiagnostics.puts("catalog-publication cleanup owned_rows_removed=true")
    end)

    name = String.to_atom("catalog_publication_provider_#{suffix}")

    on_exit(fn ->
      if pid = Process.whereis(name), do: Supervisor.stop(pid)
      assert Process.whereis(name) == nil
      TestDiagnostics.puts("catalog-publication cleanup provider_stopped=true")
    end)

    {:ok, provider} = FakeUpstream.start_link(FakeUpstream.json_response(catalog_body("Current", "sample-current-only")), supervisor_name: name)
    Process.unlink(provider.supervisor)

    database(fn ->
      pool = pool_fixture(%{slug: slug})
      source = upstream_assignment_fixture(pool, %{chatgpt_account_id: account, assignment_metadata: %{"base_url" => FakeUpstream.url(provider)}})
      assert {:ok, _} = Upstreams.store_encrypted_secret(source.identity, %{secret_kind: "access_token", plaintext: "synthetic-catalog-publication-access"})
      %{pool: pool, assignment: source.assignment, provider: provider}
    end)
  end

  defp catalog_body(label, other), do: %{"models" => [%{"id" => @model, "display_name" => label}, %{"id" => other}]}
  defp database(fun), do: Sandbox.unboxed_run(Repo, fn -> Repo.checkout(fun) end)
  defp outcome_tag({:ok, _}), do: :succeeded
  defp outcome_tag({:error, %{code: code}}), do: code
  defp outcome_tag({:error, _, %{code: code}}), do: code
  defp outcome_tag({:caught, kind}), do: kind
end
