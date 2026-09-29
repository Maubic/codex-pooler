defmodule CodexPoolerWeb.Runtime.ServiceTierVariantRoutingTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true

  for mode <- ["full", "lite"], surface <- [:native, :native_ws, :v1, :v1_ws], control <- [:supported, :unsupported, :context_drift, :priority, :foreign_anchor, :enforced_supported, :enforced_unsupported], forwarding <- [false, true], control != :foreign_anchor or surface == :native_ws do
    @tag mode: mode, surface: surface, control: control, forwarding: forwarding
    test "#{surface} #{mode} routes requested ultrafast to its sole advertised account #{control} forwarding=#{forwarding}", %{conn: conn, mode: mode, surface: surface, control: control, forwarding: forwarding} do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding)
      answer = FakeUpstream.json_response(%{"id" => "resp_synthetic_ultrafast", "object" => "response", "status" => "completed", "output" => [], "service_tier" => "ultrafast", "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}})
      answer = if surface in [:native_ws, :v1_ws], do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_ultrafast", "status" => "completed", "output" => [], "service_tier" => "ultrafast", "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}})]), else: answer
      ordinary = start_upstream(answer)
      capable = start_upstream(answer)
      setup = gateway_setup(ordinary)
      sibling = gateway_upstream(setup.pool, ordinary, "synthetic-sibling-token", compact?: false)
      fast = gateway_upstream(setup.pool, capable, "synthetic-fast-token", compact?: false)
      prime_routing_quota!(sibling.identity)
      prime_routing_quota!(fast.identity)
      model = put_model_source_assignments!(setup.model, [setup.assignment, sibling.assignment, fast.assignment])
      sources = model.metadata["source_assignment_models"]
      base = sources[setup.assignment.id] |> Map.put("service_tiers", [%{"id" => "priority"}]) |> Map.put("additional_speed_tiers", [])
      fast_source = Map.put(base, "service_tiers", [%{"id" => "priority"}, %{"id" => "ultrafast"}])

      fast_source =
        case control do
          control when control in [:unsupported, :enforced_unsupported] -> base
          :context_drift -> Map.put(fast_source, "context_window", 111_111)
          _ -> fast_source
        end

      metadata = Map.put(model.metadata, "source_assignment_models", %{setup.assignment.id => base, sibling.assignment.id => base, fast.assignment.id => fast_source})
      model = Repo.update!(Ecto.Changeset.change(model, metadata: metadata))
      setup = %{setup | model: model}
      set_model_serving_mode!(model_serving_scope(), setup, mode)
      catalog = build_conn() |> put_req_header("authorization", setup.authorization) |> get("/backend-api/codex/models") |> json_response(200)
      [catalog_model] = catalog["models"]
      advertised = Enum.any?(catalog_model["service_tiers"] || [], &(&1["id"] == "ultrafast"))
      assert advertised == control not in [:unsupported, :enforced_unsupported, :context_drift]

      if control in [:enforced_supported, :enforced_unsupported] do
        setup.api_key |> Ecto.Changeset.change(enforced_service_tier: "ultrafast") |> Repo.update!()
        assert Repo.get!(CodexPooler.Access.APIKey, setup.api_key.id).enforced_service_tier == "ultrafast"
      end

      requested_tier = if control == :priority, do: "priority", else: "ultrafast"
      client_tier = if control in [:enforced_supported, :enforced_unsupported], do: "priority", else: requested_tier
      payload = %{"model" => model.exposed_model_id, "input" => native_text_input("synthetic"), "service_tier" => client_tier}
      payload = if control == :foreign_anchor, do: Map.merge(payload, %{"previous_response_id" => "resp_synthetic_ultrafast", "input" => [%{"type" => "custom_tool_call_output", "call_id" => "call_synthetic", "output" => "synthetic"}]}), else: payload

      status =
        if surface in [:native_ws, :v1_ws] do
          {_server, port} = start_public_endpoint_with_server!()
          {ws_conn, websocket, ref} = public_websocket_connect!(port, setup, Ecto.UUID.generate(), if(surface == :v1_ws, do: "/v1/responses", else: "/backend-api/codex/responses"))

          {ws_conn, websocket} =
            if control == :foreign_anchor do
              opening = %{"type" => "response.create", "model" => model.exposed_model_id, "input" => native_text_input("synthetic opening"), "stream" => true}
              {ws_conn, websocket} = public_websocket_send_text!(ws_conn, websocket, ref, CodexPooler.JSON.encode!(opening))
              {ws_conn, websocket, opened} = public_websocket_receive_text!(ws_conn, websocket, ref)
              assert %{"type" => "response.completed"} = CodexPooler.JSON.decode!(opened)
              {ws_conn, websocket}
            else
              {ws_conn, websocket}
            end

          body = payload |> Map.put("type", "response.create") |> Map.put("stream", true) |> CodexPooler.JSON.encode!()
          {ws_conn, websocket} = public_websocket_send_text!(ws_conn, websocket, ref, body)
          {ws_conn, _, frame} = public_websocket_receive_text!(ws_conn, websocket, ref)
          Mint.HTTP.close(ws_conn)
          event = CodexPooler.JSON.decode!(frame)
          if event["type"] == "response.completed", do: 200, else: event["status"]
        else
          path = if surface == :native, do: "/backend-api/codex/responses", else: "/v1/responses"
          conn = conn |> put_req_header("authorization", setup.authorization) |> post(path, payload)
          conn.status
        end

      expected = if control in [:unsupported, :enforced_unsupported, :foreign_anchor] or (control == :context_drift and surface not in [:v1, :v1_ws]), do: 503, else: 200
      assert status == expected, "status=#{status} ordinary=#{FakeUpstream.count(ordinary)} capable=#{FakeUpstream.count(capable)}"

      if expected == 503 do
        assert FakeUpstream.count(ordinary) == if(control == :foreign_anchor, do: 1, else: 0)
        assert FakeUpstream.count(capable) == 0
      else
        assert FakeUpstream.count(ordinary) + FakeUpstream.count(capable) == 1
        if control != :priority, do: assert(FakeUpstream.count(ordinary) == 0)
        [captured] = FakeUpstream.requests(ordinary) ++ FakeUpstream.requests(capable)
        assert captured.json["service_tier"] == requested_tier
      end

      if surface == :native and control == :supported do
        request = Repo.get_by!(Request, pool_id: setup.pool.id, endpoint: "/backend-api/codex/responses")
        assert get_in(request.request_metadata, ["canonical_partition", "selected_count"]) == 3
        assert get_in(request.request_metadata, ["canonical_partition", "filtered_count"]) == 0
      end

      assert Repo.get!(CodexPooler.Catalog.Model, model.id).metadata == metadata
    end
  end

  test "an explicit tier does not bypass authentication or model visibility", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"output" => []}))
    setup = gateway_setup(upstream)
    assert get(conn, "/backend-api/codex/models").status == 401
    unknown = build_conn() |> put_req_header("authorization", setup.authorization) |> post("/backend-api/codex/responses", %{"model" => "unknown-synthetic-model", "input" => native_text_input("synthetic"), "service_tier" => "ultrafast"})
    assert unknown.status == 400
    setup.api_key |> Ecto.Changeset.change(allowed_model_identifiers: ["different-synthetic-model"]) |> Repo.update!()
    catalog = build_conn() |> put_req_header("authorization", setup.authorization) |> get("/backend-api/codex/models") |> json_response(200)
    assert catalog["models"] == []
    denied = build_conn() |> put_req_header("authorization", setup.authorization) |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic"), "service_tier" => "ultrafast"})
    assert denied.status in [400, 403]
    assert FakeUpstream.count(upstream) == 0
  end
end
