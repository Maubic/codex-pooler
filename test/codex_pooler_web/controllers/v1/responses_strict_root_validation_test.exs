defmodule CodexPoolerWeb.V1.ResponsesStrictRootValidationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [curl_json_request!: 4, gateway_setup: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  for mode <- ~w(full lite), transport <- [:http, :websocket], layout <- [:flat, :namespace] do
    @tag :strict_root_diagnostic
    test "#{mode} #{transport} strict #{layout} invalid roots report their path before effects" do
      upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_dispatch"}))
      setup = gateway_setup(upstream)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: unquote(mode), created_at: now, updated_at: now})
      port = start_public_endpoint!()

      for root <- [:absent, nil, true, 7, "schema", [], %{"type" => "string"}] do
        tool = %{"type" => "function", "name" => "strict_root_fixture", "strict" => true}
        tool = if root == :absent, do: tool, else: Map.put(tool, "parameters", root)
        {tools, param} = if unquote(layout) == :flat, do: {[%{"type" => "web_search"}, tool], "tools.1.parameters"}, else: {[%{"type" => "namespace", "name" => "fixture_namespace", "description" => "Synthetic namespace", "tools" => [%{"type" => "custom", "name" => "custom_fixture"}, tool]}], "tools.0.tools.1.parameters"}
        payload = %{"model" => setup.model.exposed_model_id, "input" => "synthetic", "tools" => tools}

        error =
          if unquote(transport) == :http do
            {headers, body} = curl_json_request!(port, setup.authorization, payload, "/v1/responses")
            assert String.starts_with?(headers, "HTTP/1.1 400")
            CodexPooler.JSON.decode!(body)["error"]
          else
            {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "strict-root-#{System.unique_integer([:positive])}", "/v1/responses", [{"openai-beta", "responses_websockets=2026-02-06"}])

            try do
              {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.merge(payload, %{"type" => "response.create", "stream" => true})))
              {_conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
              assert %{"type" => "error", "status" => 400, "error" => error} = CodexPooler.JSON.decode!(frame)
              error
            after
              Mint.HTTP.close(conn)
            end
          end

        assert %{"type" => "invalid_request_error", "code" => "invalid_function_parameters", "param" => ^param} = error
        assert FakeUpstream.count(upstream) == 0
        assert Repo.aggregate(Request, :count) == 0
        assert Repo.aggregate(Attempt, :count) == 0
        assert Repo.aggregate(LedgerEntry, :count) == 0
      end
    end
  end
end
