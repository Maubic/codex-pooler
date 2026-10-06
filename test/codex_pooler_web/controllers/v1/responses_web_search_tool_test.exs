defmodule CodexPoolerWeb.V1.ResponsesWebSearchToolTest do
  # The hosted `web_search` tool keys `/v1/responses` admits are the keys the provider accepts: it validates them
  # strictly, in the Full request and in the Lite manifest alike (direct probe, 2026-10-06), accepting
  # `external_web_access`, `indexed_web_access`, `filters`, `user_location`, `search_context_size` and
  # `search_content_types` and refusing every other key with `400 unknown_parameter`, the pre-0.144.0 Codex spelling
  # `index_gated_web_access` included. The Pooler used to refuse the keys released Codex serializes (a bundled Full
  # catalog entry's `search_content_types`, a configured `user_location` or `search_context_size`) and to accept the
  # spelling the provider no longer knows, which the provider then refused after dispatch with the generic error.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true

  @tools [
    {"cached search with text and image content types", %{"type" => "web_search", "external_web_access" => false, "search_content_types" => ["text", "image"]}},
    {"indexed search", %{"type" => "web_search", "external_web_access" => true, "indexed_web_access" => true}},
    {"every key released Codex serializes",
     %{
       "type" => "web_search",
       "external_web_access" => false,
       "filters" => %{"allowed_domains" => ["example.com"]},
       "user_location" => %{"type" => "approximate", "country" => "US", "region" => "California", "city" => "San Francisco", "timezone" => "America/Los_Angeles"},
       "search_context_size" => "low",
       "search_content_types" => ["text", "image"]
     }}
  ]

  for mode <- ["full", "lite"], {label, tool} <- @tools do
    test "#{mode} serving: #{label} reaches the upstream unchanged" do
      tool = unquote(Macro.escape(tool))
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_forward")]))
      setup = serving_setup(upstream, unquote(mode))

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "input" => "synthetic search request", "stream" => true, "tools" => [tool]})

      assert conn.status == 200
      assert conn.resp_body =~ "resp_web_search_forward"
      assert [captured] = FakeUpstream.requests(upstream)
      assert forwarded_tools(captured.json) == [tool]
      assert [%{status: "succeeded"}] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    end
  end

  for {label, tool} <- [
        {"the pre-0.144.0 spelling index_gated_web_access", %{"type" => "web_search", "external_web_access" => true, "index_gated_web_access" => true}},
        {"a key the provider does not know", %{"type" => "web_search", "external_web_access" => false, "zz_probe_unknown_key" => true}},
        {"a user location sub-key the provider does not know", %{"type" => "web_search", "user_location" => %{"type" => "approximate", "zz_probe_unknown_key" => "x"}}},
        {"a content type outside the provider vocabulary", %{"type" => "web_search", "search_content_types" => ["video"]}},
        {"a search context size outside the provider vocabulary", %{"type" => "web_search", "search_context_size" => "huge"}}
      ] do
    test "#{label} is refused before any upstream work" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_never")]))
      setup = gateway_setup(upstream)

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "input" => "synthetic search request", "stream" => true, "tools" => [unquote(Macro.escape(tool))]})

      assert %{"error" => %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "tools", "message" => "tool shape is not translatable"}} = json_response(conn, 400)
      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 0
    end
  end

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_responses(setup, payload), do: build_conn() |> auth(setup) |> post("/v1/responses", payload)

  # Full keeps the tool at top level; Lite moves it into the leading `additional_tools` manifest item.
  defp forwarded_tools(%{"tools" => tools}), do: tools

  defp forwarded_tools(%{"input" => input}) do
    Enum.flat_map(input, fn
      %{"type" => "additional_tools", "tools" => tools} -> tools
      _item -> []
    end)
  end

  defp completed(response_id) do
    {"response.completed",
     %{
       "type" => "response.completed",
       "response" => %{
         "id" => response_id,
         "object" => "response",
         "created_at" => 1_790_000_000,
         "model" => "provider-gpt-test-model",
         "status" => "completed",
         "output" => [%{"type" => "message", "id" => "msg_web_search_reply", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic reply", "annotations" => []}]}],
         "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
       }
     }}
  end
end
