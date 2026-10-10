defmodule CodexPooler.Catalog.SyncRefreshingSourceTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.{Catalog, CodexCatalogShapes, FakeRefreshTokenProvider, FakeUpstream, TestDiagnostics, Upstreams}
  alias CodexPooler.Catalog.Sync
  alias CodexPooler.Gateway.Routing.CandidateEligibility
  alias CodexPooler.Upstreams.Auth.TokenRefresh

  @exclusive "sample-exclusive"
  @shared "sample-shared"
  @removed "sample-removed"
  @budget 15_000

  test "a held token refresh preserves exclusive visibility and shared sources across catalog syncs" do
    pool = pool_fixture()
    {refresh_url, ledger} = refresh_provider()
    {refreshing, catalog} = source(pool, [@exclusive, @shared], refresh_url)
    {healthy, healthy_catalog} = source(pool, [@shared, @removed])
    {public_url, authorization} = public_models_endpoint(pool)

    assert {:ok, _} = Sync.sync_pool_catalog(pool)
    assert_snapshot(pool, public_url, authorization, refreshing, "before", [@exclusive, @removed, @shared])
    prior = Catalog.get_model_by_exposed_id(pool, @exclusive)
    prior_source = Repo.reload!(refreshing.assignment)
    shared_source = Catalog.get_model_by_exposed_id(pool, @shared).metadata["source_assignment_models"][refreshing.assignment.id]
    FakeUpstream.set_mode(healthy_catalog, catalog_response([@shared]))

    with_held_refresh(refreshing.identity, fn ->
      assert Repo.reload!(refreshing.identity).status == "refreshing"
      assert Enum.map(Sync.list_catalog_sync_assignments(pool), & &1.assignment.id) == [healthy.assignment.id]

      for iteration <- 1..3 do
        assert {:ok, result} = Sync.sync_pool_catalog(pool)
        assert_snapshot(pool, public_url, authorization, refreshing, "during-#{iteration}", [@exclusive, @shared])
        assert result.partial?
        assert result.sync_run.stats["source_assignment_count"] == 2
        assert result.sync_run.stats["skipped_source_assignment_count"] == 1
        assert result.sync_run.stats["failed_source_assignment_count"] == 0
        assert result.sync_run.stats["successful_source_assignment_count"] == 1
        assert result.sync_run.stale_marked_count == if(iteration == 1, do: 1, else: 0)
        assert Repo.reload!(prior).last_seen_at == prior.last_seen_at
        assert Repo.reload!(prior).last_sync_run_id == prior.last_sync_run_id
        assert Repo.reload!(refreshing.assignment).last_successful_sync_at == prior_source.last_successful_sync_at
        shared = Catalog.get_model_by_exposed_id(pool, @shared)
        assert shared.metadata["source_assignment_ids"] == Enum.sort([refreshing.assignment.id, healthy.assignment.id])
        assert shared.metadata["source_assignment_models"][refreshing.assignment.id] == shared_source
        assert shared.metadata["source_assignment_missing_sync_run_ids"] == %{}
        assert Catalog.get_model_by_exposed_id(pool, @removed).status == "stale"
      end

      assert length(FakeUpstream.requests(catalog)) == 1
    end)

    assert Repo.reload!(refreshing.identity).status == "active"
    assert FakeRefreshTokenProvider.snapshot(ledger) == %{generation: 1, consumed: 1, requests: 1, invalidated: false}
    # Refresh completion must not need another sync to recover the model.
    assert_snapshot(pool, public_url, authorization, refreshing, "after-refresh", [@exclusive, @shared])
    assert {:ok, %{partial?: false}} = Sync.sync_pool_catalog(pool)
    assert length(FakeUpstream.requests(catalog)) == 2
    assert_snapshot(pool, public_url, authorization, refreshing, "after-sync", [@exclusive, @shared])

    FakeUpstream.set_mode(catalog, catalog_response([]))
    assert {:ok, %{sync_run: run}} = Sync.sync_pool_catalog(pool)
    assert run.stale_marked_count == 1
    assert Catalog.get_model_by_exposed_id(pool, @exclusive).status == "stale"
    assert public_model_ids(public_url, authorization) == [@shared]
    assert {:ok, _} = Sync.sync_pool_catalog(pool)
    assert Catalog.get_model_by_exposed_id(pool, @shared).metadata["source_assignment_ids"] == [healthy.assignment.id]
  end

  test "an all-refreshing pool skips discovery without changing its catalog" do
    pool = pool_fixture()
    {refresh_url, _ledger} = refresh_provider()
    {refreshing, catalog} = source(pool, [@exclusive], refresh_url)
    {public_url, authorization} = public_models_endpoint(pool)
    assert {:ok, _} = Sync.sync_pool_catalog(pool)
    prior = Catalog.get_model_by_exposed_id(pool, @exclusive)

    with_held_refresh(refreshing.identity, fn ->
      assert Sync.list_catalog_sync_assignments(pool) == []
      assert {:ok, %{skipped?: true}} = Sync.sync_pool_catalog(pool)
      assert Repo.reload!(prior) == prior
      assert length(FakeUpstream.requests(catalog)) == 1
      assert_snapshot(pool, public_url, authorization, refreshing, "all-refreshing", [@exclusive])
    end)
  end

  for policy <- [:failed_read, :paused_identity, :deleted_identity, :disabled_assignment, :deleted_assignment] do
    test "source authority policy #{policy} remains unchanged" do
      assert_source_policy(unquote(policy))
    end
  end

  defp assert_source_policy(policy) do
    pool = pool_fixture()
    {original, catalog} = source(pool, [@exclusive, @shared])
    {healthy, _catalog} = source(pool, [@shared])
    assert {:ok, _} = Sync.sync_pool_catalog(pool)

    case policy do
      :failed_read -> FakeUpstream.set_mode(catalog, FakeUpstream.json_response(%{}, 503))
      :paused_identity -> Repo.update!(Ecto.Changeset.change(original.identity, status: "paused"))
      :deleted_identity -> Repo.update!(Ecto.Changeset.change(original.identity, status: "deleted"))
      :disabled_assignment -> Repo.update!(Ecto.Changeset.change(original.assignment, status: "disabled"))
      :deleted_assignment -> Repo.delete!(original.assignment)
    end

    for _ <- 1..2 do
      assert {:ok, result} = Sync.sync_pool_catalog(pool)
      exclusive = Catalog.get_model_by_exposed_id(pool, @exclusive)
      shared = Catalog.get_model_by_exposed_id(pool, @shared)

      if policy == :failed_read do
        assert exclusive.status == "active"
        assert result.partial?
        assert result.sync_run.stats["failed_source_assignment_count"] == 1
        assert shared.metadata["source_assignment_ids"] == Enum.sort([original.assignment.id, healthy.assignment.id])
      else
        assert exclusive.status == "stale"
        refute result.partial?
        assert shared.metadata["source_assignment_ids"] == [healthy.assignment.id]
      end
    end
  end

  defp assert_snapshot(pool, url, authorization, source, phase, expected_ids) do
    model = Catalog.get_model_by_exposed_id(pool, @exclusive)
    context = CandidateEligibility.visible_model_context(pool, @exclusive)

    candidate_ids =
      case context do
        nil ->
          []

        hydration ->
          assert {:ok, candidates} = CandidateEligibility.routable_candidates(hydration, model)
          Enum.map(candidates, fn {assignment, _identity} -> assignment.id end)
      end

    visible_ids = public_model_ids(url, authorization)

    TestDiagnostics.puts(fn ->
      "catalog-refresh phase=#{phase} identity=#{Repo.reload!(source.identity).status} model=#{model.status} visible=#{@exclusive in visible_ids} routable=#{source.assignment.id in candidate_ids}"
    end)

    assert model.status == "active"
    assert context.visible_model.id == model.id
    assert candidate_ids == [source.assignment.id]
    assert visible_ids == expected_ids
  end

  defp public_model_ids(url, authorization) do
    response = Req.get!(url <> "/v1/models", headers: [{"authorization", authorization}], retry: false)
    assert response.status == 200
    Enum.sort(Enum.map(response.body["data"], & &1["id"]))
  end

  defp public_models_endpoint(pool) do
    %{authorization: authorization} = api_key_fixture(pool)
    server = start_supervised!({Bandit, plug: CodexPoolerWeb.Endpoint, port: 0, ip: {127, 0, 0, 1}, startup_log: false})

    on_exit(fn ->
      refute Process.alive?(server)
      TestDiagnostics.puts("catalog-cleanup public_listener_stopped=true")
    end)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {"http://127.0.0.1:#{port}", authorization}
  end

  defp source(pool, models, refresh_url \\ nil) do
    name = String.to_atom("catalog_source_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if pid = Process.whereis(name) do
        Supervisor.stop(pid)
        refute Process.alive?(pid)
      end

      assert Process.whereis(name) == nil
      TestDiagnostics.puts("catalog-cleanup source_listener_stopped=true")
    end)

    {:ok, catalog} = FakeUpstream.start_link(catalog_response(models), supervisor_name: name)
    Process.unlink(catalog.supervisor)

    source =
      upstream_assignment_fixture(pool, %{
        identity_metadata: %{"base_url" => refresh_url || FakeUpstream.url(catalog)},
        assignment_metadata: %{"base_url" => FakeUpstream.url(catalog)}
      })

    assert {:ok, _} = Upstreams.store_encrypted_secret(source.identity, %{secret_kind: "access_token", plaintext: "synthetic-catalog-access"})
    assert {:ok, _} = Upstreams.store_encrypted_secret(source.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-refresh-0"})
    {source, catalog}
  end

  defp catalog_response(models), do: FakeUpstream.json_response(%{"models" => Enum.map(models, &CodexCatalogShapes.synced_source/1)})

  defp refresh_provider do
    name = String.to_atom("catalog_refresh_#{System.unique_integer([:positive])}")
    server = start_supervised!({FakeRefreshTokenProvider, name: name, notify: self(), steps: [%{hold: :after_consume}]})

    on_exit(fn ->
      refute Process.alive?(server)
      assert Process.whereis(name) == nil
      TestDiagnostics.puts("catalog-cleanup refresh_provider_stopped=true")
    end)

    {FakeRefreshTokenProvider.url(server), name}
  end

  defp with_held_refresh(identity, fun) do
    supervisor = start_supervised!(Task.Supervisor)
    task = Task.Supervisor.async_nolink(supervisor, fn -> TokenRefresh.refresh_access_token(identity) end)
    monitor = Process.monitor(task.pid)
    assert_receive {:rotation_barrier, :after_consume, 1, handler, ref}, @budget

    try do
      fun.()
    after
      send(handler, {:rotation_release, ref})
      assert {:ok, %{status: :active}} = Task.await(task, @budget)
      assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    end
  end
end
