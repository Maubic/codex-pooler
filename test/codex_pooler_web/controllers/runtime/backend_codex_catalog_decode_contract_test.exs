defmodule CodexPoolerWeb.Runtime.BackendCodexCatalogDecodeContractTest do
  # The released Codex client decodes the whole
  # `/models` body as one `ModelsResponse`, so one entry it cannot decode makes
  # it discard every entry and fall back to its bundled catalog. A client
  # inside the verified decode window is served the
  # catalog without that entry, the omission is logged with the model slug and
  # field names only, and its turns name the ETag of that same body. Clients
  # outside the window and `/v1/models` keep the unchecked catalog.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import ExUnit.CaptureLog

  alias CodexPooler.CodexCatalogShapes
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Metadata.CodexCatalog
  alias CodexPooler.Gateway.Metadata.CodexModelDecodeContract
  alias CodexPooler.Repo

  @broken_slug "gpt-catalog-undecodable"

  test "a client inside the verified window is served every entry but the one it cannot decode", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = catalog_setup(upstream)
    good_slug = setup.model.exposed_model_id

    for path <- ["/backend-api/codex/models", "/backend-api/codex/v1/models"],
        version <- CodexCatalogShapes.version_samples().checked do
      log =
        capture_log(fn ->
          response = conn |> recycle() |> auth(setup) |> put_req_header("user-agent", user_agent(version)) |> get(path, %{"client_version" => version})
          body = json_response(response, 200)

          assert Enum.map(body["models"], & &1["slug"]) == [good_slug], path
          assert get_resp_header(response, "etag") == [CodexCatalog.etag(body)], path
        end)

      assert log =~ "codex catalog entry left out", path
      assert log =~ "pool_id=#{setup.pool.id} model=#{@broken_slug} fields=experimental_supported_tools,support_verbosity", path
      refute log =~ "Synthetic instructions template", path
      refute log =~ "Synthetic #{@broken_slug}", path
    end

    samples = CodexCatalogShapes.version_samples()

    for version <- [samples.after, samples.after <> "-alpha.1", samples.before, "0.146.1", ""] do
      response = conn |> recycle() |> auth(setup) |> put_req_header("user-agent", user_agent(version)) |> get("/backend-api/codex/models", %{"client_version" => version})
      assert response |> json_response(200) |> Map.fetch!("models") |> Enum.map(& &1["slug"]) |> Enum.sort() == Enum.sort([good_slug, @broken_slug]), version
    end

    openai = conn |> recycle() |> auth(setup) |> get("/v1/models") |> json_response(200)
    assert @broken_slug in Enum.map(openai["data"], & &1["id"])
    assert FakeUpstream.count(upstream) == 0
  end

  test "a turn from a client inside the window names the ETag of the checked catalog", %{conn: conn} do
    upstream =
      start_upstream(
        FakeUpstream.sse_stream([
          {"response.completed",
           %{
             "type" => "response.completed",
             "response" => %{
               "id" => "resp_catalog_decode_contract_etag",
               "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}
             }
           }}
        ])
      )

    setup = catalog_setup(upstream)
    samples = CodexCatalogShapes.version_samples()

    capture_log(fn ->
      models =
        conn
        |> recycle()
        |> auth(setup)
        |> put_req_header("user-agent", user_agent(samples.current))
        |> get("/backend-api/codex/models", %{"client_version" => samples.current})

      assert [catalog_etag] = get_resp_header(models, "etag")

      unchecked =
        conn |> recycle() |> auth(setup) |> put_req_header("user-agent", user_agent(samples.after)) |> get("/backend-api/codex/models", %{"client_version" => samples.after}) |> get_resp_header("etag")

      refute unchecked == [catalog_etag]

      turn =
        conn
        |> recycle()
        |> auth(setup)
        |> put_req_header("user-agent", user_agent(samples.current))
        |> post("/backend-api/codex/responses", %{
          "model" => setup.model.exposed_model_id,
          "input" => native_text_input("synthetic catalog decode contract turn"),
          "stream" => true
        })

      assert turn.status == 200
      assert get_resp_header(turn, "x-models-etag") == [catalog_etag]
    end)
  end

  test "current stable and alpha clients get the decode-checked catalog with its optional guidance and ETag", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_catalog_guidance", "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}}]))
    setup = catalog_setup(upstream)
    source = CodexCatalogShapes.synced_source(setup.model.exposed_model_id) |> put_in(["model_messages", "content_filter_guidance"], "Synthetic guidance")
    setup.model |> Ecto.Changeset.change(metadata: %{"source_assignment_ids" => [setup.assignment.id], "source_assignment_models" => %{setup.assignment.id => source}}) |> Repo.update!()

    samples = CodexCatalogShapes.version_samples()

    for version <- [samples.current, samples.current <> "-alpha.1.2"] do
      for path <- ["/backend-api/codex/models", "/backend-api/codex/v1/models"] do
        catalog = conn |> recycle() |> auth(setup) |> put_req_header("user-agent", user_agent(version)) |> get(path, %{"client_version" => version})
        body = json_response(catalog, 200)
        entry = Enum.find(body["models"], &(&1["slug"] == setup.model.exposed_model_id))
        assert entry["model_messages"]["content_filter_guidance"] == "Synthetic guidance"
        refute Map.has_key?(entry, "base_instructions")
        refute @broken_slug in Enum.map(body["models"], & &1["slug"])
        assert [etag] = get_resp_header(catalog, "etag")
        assert etag == CodexCatalog.etag(body)

        turn = conn |> recycle() |> auth(setup) |> put_req_header("user-agent", user_agent(version)) |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic guidance representation"), "stream" => true})
        assert turn.status == 200
        assert get_resp_header(turn, "x-models-etag") == [etag]
      end
    end
  end

  # The model every `gateway_setup/2` test routes to
  # is a catalog entry the released client decodes, so a test sending an
  # in-window `User-Agent` computes its ETag from a body that still lists it.
  test "the default gateway fixture model is a decodable catalog entry for every client", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(upstream)

    log =
      capture_log(fn ->
        for version <- CodexCatalogShapes.version_samples().checked ++ ["0.146.1"] do
          body = conn |> recycle() |> auth(setup) |> put_req_header("user-agent", user_agent(version)) |> get("/backend-api/codex/models", %{"client_version" => version}) |> json_response(200)

          assert [entry] = body["models"], version
          assert entry["slug"] == setup.model.exposed_model_id, version
          assert CodexModelDecodeContract.violations(entry) == [], version
        end
      end)

    refute log =~ "codex catalog entry left out"
  end

  # The catalog fetch selects its representation from the Codex build's
  # `User-Agent`, exactly as its turns do.
  defp user_agent(""), do: "codex_cli_rs"
  defp user_agent(version), do: "codex_cli_rs/#{version} (Mac OS 26.0.0; arm64) xterm-256color"

  defp catalog_setup(upstream) do
    setup = gateway_setup(upstream)

    model =
      setup.model
      |> Ecto.Changeset.change(
        metadata: %{
          "source_assignment_ids" => [setup.assignment.id],
          "source_assignment_models" => %{
            setup.assignment.id => CodexCatalogShapes.synced_source(setup.model.exposed_model_id)
          }
        }
      )
      |> Repo.update!()

    broken_source =
      @broken_slug
      |> CodexCatalogShapes.synced_source()
      |> Map.drop(["support_verbosity", "experimental_supported_tools"])

    broken =
      model_fixture(setup.pool, %{
        exposed_model_id: @broken_slug,
        upstream_model_id: "provider-#{@broken_slug}",
        display_name: "Catalog Undecodable",
        metadata: %{
          "source_assignment_ids" => [setup.assignment.id],
          "source_assignment_models" => %{setup.assignment.id => broken_source}
        }
      })

    setup
    |> Map.put(:model, model)
    |> Map.put(:broken_model, broken)
  end
end
