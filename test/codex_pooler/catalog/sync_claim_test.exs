defmodule CodexPooler.Catalog.SyncClaimTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1]

  alias CodexPooler.Catalog.{Model, Sync, SyncRun}
  alias CodexPooler.{FakeUpstream, Repo, TestDiagnostics, Upstreams}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @model "sample-claim-model"

  setup do
    supervisor = start_supervised!(Task.Supervisor)
    on_exit(fn -> refute Process.alive?(supervisor) end)
    %{supervisor: supervisor}
  end

  for order <- [["manual", "scheduled"], ["scheduled", "manual"]] do
    test "concurrent #{Enum.join(order, "/")} claims dispatch one catalog HTTP request", context do
      assert_concurrent_claims(context.supervisor, unquote(order))
    end
  end

  test "different Pools discover concurrently while each provider response is held", %{supervisor: supervisor} do
    first = fixture()
    second = fixture()
    first_ref = hold_provider(first)
    second_ref = hold_provider(second)
    {holder, holder_ref} = hold_pool(supervisor, first.pool)
    first_worker = worker(supervisor, :first, fn -> Sync.sync_pool_catalog(first.pool) end)
    await_pool_block(first_worker.backend, [holder.backend])
    second_worker = worker(supervisor, :second, fn -> Sync.sync_pool_catalog(second.pool) end)
    assert_receive {:fake_upstream_timeout_barrier, :before_headers, second_handler, ^second_ref}, @budget
    assert run_statuses(first) == []
    assert catalog_requests(first) == 0
    send(holder.task.pid, {:release_pool, holder_ref})
    assert {:ok, :released} = join(holder)
    assert_receive {:fake_upstream_timeout_barrier, :before_headers, first_handler, ^first_ref}, @budget

    try do
      assert run_statuses(first) == ["running"]
      assert run_statuses(second) == ["running"]
      assert_unlocked_during_http(first, first_worker.backend)
      assert_unlocked_during_http(second, second_worker.backend)
    after
      release_provider(first_handler, first_ref)
      release_provider(second_handler, second_ref)
      assert {:ok, _} = join(first_worker)
      assert {:ok, _} = join(second_worker)
    end

    assert catalog_requests(first) == 1
    assert catalog_requests(second) == 1
    TestDiagnostics.puts("catalog-claim different_pools=true second_dispatched_while_first_claim_blocked=true simultaneous_http=true requests=2")
  end

  for failure <- [:http_error, :malformed_catalog] do
    test "#{failure} finalizes the claim and permits a successful successor" do
      fixture = fixture()

      response =
        case unquote(failure) do
          :http_error -> FakeUpstream.json_response(%{}, 503)
          :malformed_catalog -> FakeUpstream.json_response(%{"models" => [nil]})
        end

      FakeUpstream.set_mode(fixture.provider, response)
      assert {:error, run, %{code: :catalog_sync_failed}} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
      assert run.status == "failed"
      assert %DateTime{} = run.finished_at
      FakeUpstream.set_mode(fixture.provider, catalog_response())
      assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
      assert run_statuses(fixture) == ["failed", "succeeded"]
      assert catalog_requests(fixture) == 2
    end
  end

  test "invalid claim and cancellation attributes leave no new run or locked Pool", %{supervisor: supervisor} do
    fixture = fixture()
    assert {:error, %Ecto.Changeset{}} = database(fn -> Sync.sync_pool_catalog(fixture.pool, trigger_kind: "invalid") end)
    assert run_statuses(fixture) == []
    assert catalog_requests(fixture) == 0
    ref = hold_provider(fixture)
    claimant = worker(supervisor, :claimant, fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert_receive {:fake_upstream_timeout_barrier, :before_headers, handler, ^ref}, @budget

    try do
      assert {:error, %Ecto.Changeset{}} = database(fn -> Sync.sync_pool_catalog(fixture.pool, trigger_kind: "invalid") end)
      assert run_statuses(fixture) == ["running"]
      assert_unlocked_during_http(fixture, claimant.backend)
      assert catalog_requests(fixture) == 1
    after
      release_provider(handler, ref)
      assert {:ok, _} = join(claimant)
    end
  end

  test "statement timeout while claiming rolls back and a successor can claim", %{supervisor: supervisor} do
    fixture = fixture()
    {holder, holder_ref} = hold_pool(supervisor, fixture.pool)

    claimant =
      worker(supervisor, :timed_claim, fn ->
        previous = Repo.query!("SHOW statement_timeout").rows |> hd() |> hd()
        Repo.query!("SELECT set_config('statement_timeout', '200ms', false)")

        result =
          try do
            Sync.sync_pool_catalog(fixture.pool)
          rescue
            error in Postgrex.Error -> {:database_error, error.postgres.code}
          after
            Repo.query!("SELECT set_config('statement_timeout', $1, false)", [previous])
          end

        assert Repo.query!("SHOW statement_timeout").rows == [[previous]]
        result
      end)

    await_pool_block(claimant.backend, [holder.backend])
    assert {:database_error, :query_canceled} = join(claimant)
    assert run_statuses(fixture) == []
    assert catalog_requests(fixture) == 0
    send(holder.task.pid, {:release_pool, holder_ref})
    assert {:ok, :released} = join(holder)
    assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert catalog_requests(fixture) == 1
    TestDiagnostics.puts("catalog-claim statement_timeout=query_canceled claimed_before_release=0 timeout_restored=true successor=succeeded")
  end

  test "stale recovery expires old runs but a fresh running claim still refuses discovery" do
    fixture = fixture()
    now = DateTime.utc_now()
    stale = database(fn -> insert_run(fixture.pool, DateTime.add(now, -901, :second)) end)
    fresh = database(fn -> insert_run(fixture.pool, now) end)
    assert {:error, %{code: :catalog_sync_in_progress}} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert database(fn -> Repo.reload!(stale).status end) == "failed"
    assert database(fn -> Repo.reload!(fresh).status end) == "running"
    assert catalog_requests(fixture) == 0
    database(fn -> Repo.update!(Ecto.Changeset.change(fresh, started_at: DateTime.add(now, -901, :second))) end)
    assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert run_statuses(fixture) == ["cancelled", "failed", "failed", "succeeded"]
    assert catalog_requests(fixture) == 1
  end

  test "Pool deletion after source discovery refuses the claim without provider work", %{supervisor: supervisor} do
    fixture = fixture()
    parent = self()
    ref = make_ref()
    handler_id = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.hold_source_snapshot/4, nil)

    claimant =
      worker(supervisor, :deleted_pool, fn ->
        Process.put({__MODULE__, :source_snapshot}, {parent, ref})
        Sync.sync_pool_catalog(fixture.pool)
      end)

    assert_receive {:source_snapshot_read, ^ref}, @budget
    database(fn -> delete_committed_pools!([fixture.pool.id]) end)
    send(claimant.task.pid, {:release_source_snapshot, ref})
    assert {:error, %{code: :pool_not_found}} = join(claimant)
    assert catalog_requests(fixture) == 0
    assert run_statuses(fixture) == []
  end

  def hold_source_snapshot(_event, _measurements, metadata, _config) do
    if metadata[:source] == "pool_upstream_assignments" do
      case Process.delete({__MODULE__, :source_snapshot}) do
        {parent, ref} ->
          send(parent, {:source_snapshot_read, ref})

          receive do
            {:release_source_snapshot, ^ref} -> :ok
          after
            @budget -> raise "source snapshot not released"
          end

        nil ->
          :ok
      end
    end
  end

  defp assert_concurrent_claims(supervisor, [first_kind, second_kind]) do
    fixture = fixture()
    provider_ref = hold_provider(fixture)
    {holder, holder_ref} = hold_pool(supervisor, fixture.pool)
    first = worker(supervisor, :first, fn -> Sync.sync_pool_catalog(fixture.pool, trigger_kind: first_kind) end)
    await_pool_block(first.backend, [holder.backend])
    second = worker(supervisor, :second, fn -> Sync.sync_pool_catalog(fixture.pool, trigger_kind: second_kind) end)
    await_pool_block(second.backend, [holder.backend, first.backend])
    assert length(Enum.uniq([holder.backend, first.backend, second.backend])) == 3
    send(holder.task.pid, {:release_pool, holder_ref})
    assert {:ok, :released} = join(holder)
    assert_receive {:fake_upstream_timeout_barrier, :before_headers, handler, ^provider_ref}, @budget
    outcome = next_claim_outcome(first, second, provider_ref)

    try do
      case outcome do
        {:refused, loser} ->
          assert run_statuses(fixture) == ["cancelled", "running"]
          winner = if loser == first.label, do: second, else: first
          loser_kind = if loser == first.label, do: first_kind, else: second_kind
          cancelled = database(fn -> Repo.one!(from r in SyncRun, where: r.pool_id == ^fixture.pool.id and r.status == "cancelled") end)
          assert cancelled.trigger_kind == loser_kind
          assert cancelled.error_message == "catalog sync already running"
          assert %DateTime{} = cancelled.finished_at
          assert_unlocked_during_http(fixture, winner.backend)

        {:duplicate, _handler} ->
          :ok
      end
    after
      release_provider(handler, provider_ref)
      if match?({:duplicate, _}, outcome), do: release_provider(elem(outcome, 1), provider_ref)
    end

    results = [join(first), join(second)]
    requests = catalog_requests(fixture)
    TestDiagnostics.puts("catalog-claim order=#{first_kind}/#{second_kind} holder_backend=#{holder.backend} first_backend=#{first.backend} second_backend=#{second.backend} distinct_backends=true blocked_on_owned_pool=2 provider_requests=#{requests} statuses=#{Enum.join(run_statuses(fixture), ",")}")
    assert requests == 1, "overlapping claims sent #{requests} catalog HTTP requests"
    assert Enum.count(results, &match?({:ok, _}, &1)) == 1
    assert Enum.count(results, &match?({:error, %{code: :catalog_sync_in_progress}}, &1)) == 1
    assert run_statuses(fixture) == ["cancelled", "succeeded"]
    assert database(fn -> Repo.aggregate(from(m in Model, where: m.pool_id == ^fixture.pool.id), :count) end) == 1
    FakeUpstream.set_mode(fixture.provider, catalog_response())
    assert {:ok, _} = database(fn -> Sync.sync_pool_catalog(fixture.pool) end)
    assert catalog_requests(fixture) == 2
  end

  defp next_claim_outcome(first, second, provider_ref) do
    first_ref = first.ref
    second_ref = second.ref

    receive do
      {ref, :result, label, {:error, %{code: :catalog_sync_in_progress}}} when ref in [first_ref, second_ref] -> {:refused, label}
      {:fake_upstream_timeout_barrier, :before_headers, handler, ^provider_ref} -> {:duplicate, handler}
    after
      @budget -> flunk("no refused claimant or duplicate HTTP request was observed")
    end
  end

  defp assert_unlocked_during_http(fixture, backend) do
    database(fn ->
      assert Repo.query!("SELECT xact_start IS NULL FROM pg_stat_activity WHERE pid = $1", [backend]).rows == [[true]]

      assert {:ok, :unlocked} =
               Repo.transaction(fn ->
                 Repo.one!(from p in Pool, where: p.id == ^fixture.pool.id, lock: "FOR NO KEY UPDATE NOWAIT")
                 :unlocked
               end)
    end)

    TestDiagnostics.puts("catalog-claim provider_http_held=true claim_transaction_open=false pool_nowait=acquired")
  end

  defp hold_pool(supervisor, pool) do
    parent = self()
    ref = make_ref()

    holder =
      worker(supervisor, :holder, fn ->
        Repo.transaction(fn ->
          Repo.one!(from p in Pool, where: p.id == ^pool.id, lock: "FOR UPDATE")
          send(parent, {:pool_held, ref})

          receive do
            {:release_pool, ^ref} -> :released
          after
            @budget -> Repo.rollback(:holder_not_released)
          end
        end)
      end)

    assert_receive {:pool_held, ^ref}, @budget
    {holder, ref}
  end

  defp worker(supervisor, label, fun) do
    parent = self()
    ref = make_ref()

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        receive do
          {:start, ^ref} ->
            database(fn ->
              %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
              send(parent, {ref, :backend, backend})
              result = fun.()
              send(parent, {ref, :result, label, result})
              result
            end)
        after
          @budget -> raise "claim worker not started"
        end
      end)

    monitor = Process.monitor(task.pid)
    send(task.pid, {:start, ref})
    assert_receive {^ref, :backend, backend}, @budget
    %{task: task, monitor: monitor, label: label, ref: ref, backend: backend}
  end

  defp join(%{task: task, monitor: monitor}) do
    result = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    result
  end

  defp await_pool_block(waiter, blockers), do: await_pool_block(waiter, blockers, System.monotonic_time(:millisecond) + @budget)

  defp await_pool_block(waiter, blockers, deadline) do
    blocked =
      database(fn ->
        Repo.query!("SELECT pg_stat_clear_snapshot()")

        Repo.query!(
          """
          SELECT EXISTS (
            SELECT 1 FROM pg_stat_activity a
            JOIN pg_locks w ON w.pid = a.pid AND w.granted AND w.locktype = 'relation'
            JOIN pg_locks b ON b.pid = ANY(pg_blocking_pids(a.pid)) AND b.granted AND b.relation = w.relation
            JOIN pg_class c ON c.oid = w.relation
            WHERE a.pid = $1 AND a.wait_event_type = 'Lock' AND c.relname = 'pools'
              AND b.pid = ANY($2::integer[])
          )
          """,
          [waiter, blockers]
        ).rows == [[true]]
      end)

    cond do
      blocked -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("claimant never blocked on an owned Pool-row holder")
      true -> await_pool_block(waiter, blockers, deadline)
    end
  end

  defp fixture do
    suffix = System.unique_integer([:positive])
    slug = "catalog-claim-#{suffix}"
    account = "sample-catalog-claim-#{suffix}"

    register_unboxed_cleanup!(fn ->
      if pool = Repo.get_by(Pool, slug: slug), do: delete_committed_pools!([pool.id])
      Repo.delete_all(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
      refute Repo.exists?(from p in Pool, where: p.slug == ^slug)
      refute Repo.exists?(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
      TestDiagnostics.puts("catalog-claim cleanup owned_pool_absent=true owned_identity_absent=true")
    end)

    name = String.to_atom("catalog_claim_provider_#{suffix}")

    on_exit(fn ->
      if pid = Process.whereis(name), do: Supervisor.stop(pid)
      assert Process.whereis(name) == nil
      TestDiagnostics.puts("catalog-claim cleanup provider_stopped=true")
    end)

    {:ok, provider} = FakeUpstream.start_link(catalog_response(), supervisor_name: name)
    Process.unlink(provider.supervisor)

    database(fn ->
      pool = pool_fixture(%{slug: slug})
      source = upstream_assignment_fixture(pool, %{chatgpt_account_id: account, assignment_metadata: %{"base_url" => FakeUpstream.url(provider)}})
      assert {:ok, _} = Upstreams.store_encrypted_secret(source.identity, %{secret_kind: "access_token", plaintext: "synthetic-catalog-claim-access"})
      %{pool: pool, provider: provider}
    end)
  end

  defp insert_run(pool, started_at) do
    %SyncRun{}
    |> SyncRun.changeset(%{pool_id: pool.id, trigger_kind: "manual", status: "running", started_at: started_at, discovered_model_count: 0, upserted_model_count: 0, stale_marked_count: 0, stats: %{}})
    |> Repo.insert!()
  end

  defp hold_provider(fixture) do
    ref = make_ref()
    FakeUpstream.set_mode(fixture.provider, FakeUpstream.barrier_json_response(catalog_body(), notify: self(), release_ref: ref))
    ref
  end

  defp release_provider(handler, ref), do: send(handler, {:fake_upstream_release_timeout, ref})
  defp catalog_body, do: %{"models" => [%{"id" => @model}]}
  defp catalog_response, do: FakeUpstream.json_response(catalog_body())
  defp database(fun), do: Sandbox.unboxed_run(Repo, fn -> Repo.checkout(fun) end)
  defp run_statuses(fixture), do: database(fn -> Repo.all(from r in SyncRun, where: r.pool_id == ^fixture.pool.id, order_by: r.status, select: r.status) end)

  defp catalog_requests(fixture) do
    requests = FakeUpstream.requests(fixture.provider)
    assert Enum.all?(requests, &(&1.method == "GET" and &1.path == "/backend-api/codex/models"))
    length(requests)
  end
end
