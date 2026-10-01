defmodule CodexPoolerWeb.V1.ResponsesLiteToolChoiceTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [curl_json_request!: 4, gateway_setup: 1, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  for mode <- ~w(full lite), route <- ~w(responses chat/completions), stream <- [false, true] do
    @tag serving_mode: mode, route: route, streaming: stream
    test "#{route} preserves a forced function choice in #{mode} with stream=#{stream}", %{serving_mode: mode, route: route, streaming: stream} do
      tool = %{"type" => "function", "name" => "finish_report", "parameters" => %{"type" => "object", "properties" => %{}}}
      choice = Map.take(tool, ["type", "name"])
      item = %{"type" => "function_call", "id" => "fc_fixture", "call_id" => "call_fixture", "name" => tool["name"], "arguments" => "{}", "status" => "completed"}
      completed = %{"id" => "resp_fixture", "object" => "response", "status" => "completed", "model" => "provider-gpt-test-model", "output" => [item], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}

      upstream =
        start_upstream(
          FakeUpstream.sse_stream([
            {"response.created", %{"type" => "response.created", "response" => %{completed | "output" => [], "status" => "in_progress"}}},
            {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.put(item, "arguments", "")}},
            {"response.function_call_arguments.delta", %{"type" => "response.function_call_arguments.delta", "output_index" => 0, "item_id" => item["id"], "delta" => "{}"}},
            {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => item}},
            {"response.completed", %{"type" => "response.completed", "response" => completed}}
          ])
        )

      setup = gateway_setup(upstream)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: now, updated_at: now})

      payload = %{"model" => setup.model.exposed_model_id, "stream" => stream}

      payload =
        if route == "responses" do
          Map.merge(payload, %{"input" => "synthetic report request", "tools" => [tool], "tool_choice" => choice})
        else
          Map.merge(payload, %{"messages" => [%{"role" => "user", "content" => "synthetic report request"}], "tools" => [%{"type" => "function", "function" => Map.delete(tool, "type")}], "tool_choice" => %{"type" => "function", "function" => %{"name" => tool["name"]}}})
        end

      {headers, body} = curl_json_request!(start_public_endpoint!(), setup.authorization, payload, "/v1/" <> route)
      assert headers =~ "200 OK"
      assert body =~ "finish_report"
      refute body =~ "unsupported_parameter"

      if stream do
        assert headers =~ "text/event-stream"
        assert body =~ if(route == "responses", do: "event: response.completed", else: "[DONE]")
      else
        decoded = CodexPooler.JSON.decode!(body)

        if route == "responses" do
          assert [%{"type" => "function_call", "name" => "finish_report"}] = decoded["output"]
        else
          assert [%{"message" => %{"tool_calls" => [%{"function" => %{"name" => "finish_report"}}]}}] = decoded["choices"]
        end
      end

      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.json["tool_choice"] === choice

      if mode == "lite" do
        refute Map.has_key?(captured.json, "tools")
        assert [%{"type" => "additional_tools", "tools" => [%{"name" => "finish_report"}]} | _] = captured.json["input"]
      else
        assert [%{"name" => "finish_report"}] = captured.json["tools"]
      end

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert request.status == "succeeded"
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert attempt.status == "succeeded"
      assert request.request_metadata["routing"]["model_serving_mode"] == mode
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement" and l.amount_status == "recorded"), :count) == 1
    end
  end
end
