defmodule CodexPoolerWeb.V1.ModelsCaseVariantsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Catalog.{Model, Sync}
  alias CodexPooler.{CodexCatalogShapes, FakeUpstream, Repo, TestDiagnostics, Upstreams}
  alias CodexPooler.Gateway.Metadata.CodexCatalog
  alias CodexPooler.Gateway.Routing.CandidateEligibility
  alias CodexPooler.Pools

  for {exposed, canonical} <- [{"sample-model", "SAMPLE-MODEL"}, {"SAMPLE-MODEL", "sample-model"}, {"sample-model", "sample-model"}] do
    test "public listing joins exposed #{exposed} to canonical #{canonical} exactly once" do
      assert_case_join(unquote(exposed), unquote(canonical))
    end
  end

  for status <- ~w(suppressed stale retired) do
    test "case folding does not expose a #{status} catalog model" do
      setup = catalog_fixture("sample-model", "SAMPLE-MODEL")
      Repo.update!(Ecto.Changeset.change(setup.model, status: unquote(status)))
      assert native_snapshot(setup).body == %{"models" => []}
      assert public_models(setup) == []
    end
  end

  test "key allow-list filtering retains case identity without adding media or distinct ids" do
    setup = catalog_fixture("sample-model", "SAMPLE-MODEL", [CodexCatalogShapes.synced_source("sample-distinct")])
    Repo.update!(Ecto.Changeset.change(setup.api_key, allowed_model_identifiers: ["SaMpLe-MoDeL"]))
    assert [%{"slug" => "SAMPLE-MODEL"}] = native_snapshot(setup).body["models"]
    assert [%{"id" => "sample-model"}] = public_models(setup)
    Repo.update!(Ecto.Changeset.change(setup.api_key, allowed_model_identifiers: ["sample-absent"]))
    assert native_snapshot(setup).body == %{"models" => []}
    assert public_models(setup) == []
  end

  test "distinct casefolded ids retain their own selected metadata" do
    extra = CodexCatalogShapes.synced_source("sample-distinct", %{"context_window" => 400_000, "max_context_window" => 400_000})
    setup = catalog_fixture("sample-model", "SAMPLE-MODEL", [extra])
    assert Pools.allow_image_generation?(setup.pool)
    models = public_models(setup)
    assert Enum.sort(Enum.map(models, & &1["id"])) == ["sample-distinct", "sample-model"]
    assert Enum.find(models, &(&1["id"] == "sample-model"))["context_length"] == 190_000
    assert Enum.find(models, &(&1["id"] == "sample-distinct"))["context_length"] == 380_000
    refute Enum.any?(models, &String.starts_with?(&1["id"], "gpt-image"))
    refute Enum.any?(models, &String.contains?(&1["id"], "transcribe"))
  end

  test "the case-insensitive join uses the selected majority context rather than the oldest source" do
    setup = catalog_fixture("sample-model", "SAMPLE-MODEL", [], %{"context_window" => 100_000, "max_context_window" => 100_000})
    assert native_snapshot(setup).body == %{"models" => [setup.canonical_source]}
    assert [%{"id" => "sample-model", "context_length" => 190_000}] = public_models(setup)
  end

  test "an active model with no eligible source stays hidden" do
    setup = catalog_fixture("sample-model", "SAMPLE-MODEL")
    for source <- setup.sources, do: Repo.update!(Ecto.Changeset.change(source.assignment, status: "disabled"))
    assert native_snapshot(setup).body == %{"models" => []}
    assert public_models(setup) == []
  end

  for damage <- [:missing_slug, :nonbinary_slug, :wrong_slug, :nonmap_source] do
    test "#{damage} canonical source metadata stays excluded without crashing the public join" do
      setup = catalog_fixture("sample-model", "SAMPLE-MODEL")
      source = setup.canonical_source

      invalid =
        case unquote(damage) do
          :missing_slug -> Map.delete(source, "slug")
          :nonbinary_slug -> Map.put(source, "slug", 42)
          :wrong_slug -> Map.put(source, "slug", "sample-other")
          :nonmap_source -> "malformed"
        end

      metadata = Map.put(setup.model.metadata, "source_assignment_models", Map.new(setup.sources, &{&1.assignment.id, invalid}))
      Repo.update!(Ecto.Changeset.change(setup.model, metadata: metadata))
      assert native_snapshot(setup).body == %{"models" => []}
      assert public_models(setup) == []
    end
  end

  defp assert_case_join(exposed, canonical) do
    setup = catalog_fixture(exposed, canonical)
    assert Repo.aggregate(from(m in Model, where: m.pool_id == ^setup.pool.id), :count) == 1
    assert setup.model.exposed_model_id == exposed

    for spelling <- [exposed, canonical] do
      context = CandidateEligibility.visible_model_context(setup.pool, spelling)
      assert context.visible_model.id == setup.model.id
      assert {:ok, candidates} = CandidateEligibility.routable_candidates(context, setup.model)
      assert length(candidates) == 3
    end

    first_native = native_snapshot(setup)
    assert first_native.body == %{"models" => [setup.canonical_source]}
    assert first_native.etag == CodexCatalog.etag(%{"models" => [setup.canonical_source]})
    public = public_models(setup)
    second_native = native_snapshot(setup)
    assert second_native.bytes == first_native.bytes
    assert second_native.etag == first_native.etag
    assert is_binary(first_native.etag)
    assert Repo.reload!(setup.model) == setup.model
    assert Enum.all?(setup.sources, &(FakeUpstream.count(&1.provider) == 1))

    TestDiagnostics.puts("catalog-case exposed=#{exposed} native_slug=#{canonical} rows=1 routable_sources=3 source_http_reads=3 public_count=#{length(public)} native_bytes_stable=true etag_stable=true native_sha256=#{digest(first_native.bytes)} etag_sha256=#{digest(first_native.etag)}")

    assert [%{"id" => ^exposed} = entry] = public
    assert entry["context_length"] == 190_000
    assert entry["input_modalities"] == ["text", "image"]
    assert entry["display_name"] == setup.model.display_name
    assert entry["object"] == "model"
    assert entry["owned_by"] == "codex-pooler"
  end

  defp catalog_fixture(exposed, canonical, extra \\ [], anchor_overrides \\ %{}) do
    pool = pool_fixture()
    canonical_source = CodexCatalogShapes.synced_source(canonical, %{"context_window" => 200_000, "max_context_window" => 200_000})
    anchor_source = canonical_source |> Map.put("slug", exposed) |> Map.merge(anchor_overrides)

    sources =
      [{anchor_source, -180}, {canonical_source, -120}, {canonical_source, -60}]
      |> Enum.map(fn {source, offset} -> source_fixture(pool, [source | extra], offset) end)

    assert {:ok, _} = Sync.sync_pool_catalog(pool)
    model = Repo.one!(from m in Model, where: m.pool_id == ^pool.id and m.exposed_model_id == ^exposed)
    key = api_key_fixture(pool)
    server = start_supervised!({Bandit, plug: CodexPoolerWeb.Endpoint, port: 0, ip: {127, 0, 0, 1}, startup_log: false})

    on_exit(fn ->
      refute Process.alive?(server)
      TestDiagnostics.puts("catalog-case cleanup public_listener_stopped=true")
    end)

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    Map.merge(key, %{model: model, sources: sources, canonical_source: canonical_source, url: "http://127.0.0.1:#{port}"})
  end

  defp source_fixture(pool, models, offset) do
    name = String.to_atom("catalog_case_source_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if pid = Process.whereis(name), do: Supervisor.stop(pid)
      assert Process.whereis(name) == nil
      TestDiagnostics.puts("catalog-case cleanup source_listener_stopped=true")
    end)

    {:ok, provider} = FakeUpstream.start_link(FakeUpstream.json_response(%{"models" => models}), supervisor_name: name)
    Process.unlink(provider.supervisor)
    source = upstream_assignment_fixture(pool, %{assignment_metadata: %{"base_url" => FakeUpstream.url(provider)}})
    Repo.update!(Ecto.Changeset.change(source.assignment, created_at: DateTime.add(DateTime.utc_now(), offset, :second)))
    assert {:ok, _} = Upstreams.store_encrypted_secret(source.identity, %{secret_kind: "access_token", plaintext: "synthetic-catalog-case-access"})
    Map.put(source, :provider, provider)
  end

  defp native_snapshot(setup) do
    response = Req.get!(setup.url <> "/backend-api/codex/models", headers: headers(setup), retry: false, decode_body: false)
    assert response.status == 200
    assert [etag] = Req.Response.get_header(response, "etag")
    %{body: CodexPooler.JSON.decode!(response.body), bytes: response.body, etag: etag}
  end

  defp public_models(setup) do
    response = Req.get!(setup.url <> "/v1/models", headers: headers(setup), retry: false)
    assert response.status == 200
    assert %{"object" => "list", "data" => data} = response.body
    data
  end

  defp headers(setup), do: [{"authorization", setup.authorization}, {"user-agent", "sample-catalog-contract/1.0"}]
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
