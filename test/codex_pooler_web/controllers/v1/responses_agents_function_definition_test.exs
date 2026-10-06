defmodule CodexPoolerWeb.V1.ResponsesAgentsFunctionDefinitionTest do
  @moduledoc """
  What the openai-node beta Agents helper `functionTool()` hands to `agent.tools` (`type`, `name`, `description`,
  `parameters`, and `defer_loading` only when the application sets it, never `strict`) is, on `POST /v1/responses`, an
  ordinary Responses function tool: the Agents calls themselves are refused (`agents_unsupported_test.exs`), but a client
  that reuses the same definition and the same JSON-text or content-part tool results with the Responses API gets them
  forwarded as such in both serving modes. Provider side, the same definitions answered 200 on the Codex backend (Full and
  Lite) and on the public API, with `defer_loading: true` refused without a `tool_search` tool in Full only.
  """

  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.FakeUpstream

  @definition %{
    "type" => "function",
    "name" => "lookup_item",
    "description" => "Look up a catalog item.",
    "parameters" => %{
      "type" => "object",
      "properties" => %{"item_id" => %{"type" => "string"}},
      "required" => ["item_id"],
      "additionalProperties" => false
    }
  }

  @not_deferred Map.put(@definition, "defer_loading", false)

  # An `input_image` part, in a tool result as anywhere else, needs a model that declares image input.
  @vision_metadata %{"input_modalities" => ["text", "image"]}

  @function_call %{
    "type" => "function_call",
    "id" => "fc_agents_definition",
    "call_id" => "call_agents_definition",
    "name" => "lookup_item",
    "arguments" => ~s({"item_id":"ITEM_A"}),
    "status" => "completed"
  }

  for mode <- ~w(full lite), stream <- [false, true] do
    @tag serving_mode: mode, streaming: stream
    test "an Agents-shaped function definition is an ordinary function tool in #{mode} with stream=#{stream}", %{
      conn: conn,
      serving_mode: mode,
      streaming: stream
    } do
      upstream = start_upstream(FakeUpstream.sse_stream(function_call_events()))
      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)

      response =
        conn
        |> auth(setup)
        |> post("/v1/responses", %{
          "model" => setup.model.exposed_model_id,
          "input" => "synthetic lookup request",
          "stream" => stream,
          "tools" => [@definition]
        })

      if stream do
        body = response(response, 200)
        assert body =~ "event: response.output_item.done"
        assert body =~ "lookup_item"
      else
        assert %{"output" => [%{"type" => "function_call", "name" => "lookup_item", "call_id" => "call_agents_definition"}]} =
                 json_response(response, 200)
      end

      assert [captured] = FakeUpstream.requests(upstream)
      assert_forwarded_tools(captured, mode, [@definition])
    end
  end

  for mode <- ~w(full lite) do
    @tag serving_mode: mode
    test "an explicit defer_loading: false stays on the forwarded function tool in #{mode}", %{conn: conn, serving_mode: mode} do
      upstream = start_upstream(FakeUpstream.sse_stream(function_call_events()))
      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)

      response =
        conn
        |> auth(setup)
        |> post("/v1/responses", %{
          "model" => setup.model.exposed_model_id,
          "input" => "synthetic lookup request",
          "tools" => [@not_deferred]
        })

      assert %{"output" => [%{"type" => "function_call", "name" => "lookup_item"}]} = json_response(response, 200)
      assert [captured] = FakeUpstream.requests(upstream)
      assert_forwarded_tools(captured, mode, [@not_deferred])
    end

    @tag serving_mode: mode
    test "a tool result as JSON text or as input_text and input_image parts is forwarded unchanged in #{mode}", %{
      conn: conn,
      serving_mode: mode
    } do
      upstream = start_upstream(FakeUpstream.sse_stream(completed_events()))
      setup = gateway_setup(upstream, model_metadata: @vision_metadata)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)

      text_result = ~s({"item_id":"ITEM_A","in_stock":true})

      part_result = [
        %{"type" => "input_text", "text" => "synthetic tool note"},
        %{"type" => "input_image", "image_url" => "https://example.com/tool-result.png"}
      ]

      for {call_id, output} <- [{"call_agents_text", text_result}, {"call_agents_parts", part_result}] do
        call = %{@function_call | "id" => "fc_" <> call_id, "call_id" => call_id}

        response =
          conn
          |> recycle()
          |> auth(setup)
          |> post("/v1/responses", %{
            "model" => setup.model.exposed_model_id,
            "tools" => [@definition],
            "input" => [
              %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic lookup request"}]},
              call,
              %{"type" => "function_call_output", "call_id" => call_id, "output" => output}
            ]
          })

        assert %{"object" => "response", "status" => "completed"} = json_response(response, 200)

        captured = List.last(FakeUpstream.requests(upstream))
        assert Enum.find(captured.json["input"], &(&1["type"] == "function_call_output")) == %{"type" => "function_call_output", "call_id" => call_id, "output" => output}
      end

      assert length(FakeUpstream.requests(upstream)) == 2
    end
  end

  # Full carries the tools at the top level; Lite relocates them into the leading developer manifest item.
  defp assert_forwarded_tools(captured, "full", tools) do
    assert captured.json["tools"] == tools
    refute Enum.any?(List.wrap(captured.json["input"]), &(is_map(&1) and &1["type"] == "additional_tools"))
  end

  defp assert_forwarded_tools(captured, "lite", tools) do
    refute Map.has_key?(captured.json, "tools")
    assert [%{"type" => "additional_tools", "role" => "developer", "tools" => ^tools} | _rest] = captured.json["input"]
  end

  defp function_call_events do
    completed = %{
      "id" => "resp_agents_definition",
      "object" => "response",
      "status" => "completed",
      "model" => "provider-gpt-test-model",
      "output" => [@function_call],
      "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}
    }

    [
      {"response.created", %{"type" => "response.created", "response" => %{completed | "output" => [], "status" => "in_progress"}}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.put(@function_call, "arguments", "")}},
      {"response.function_call_arguments.delta", %{"type" => "response.function_call_arguments.delta", "output_index" => 0, "item_id" => @function_call["id"], "delta" => @function_call["arguments"]}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => @function_call}},
      {"response.completed", %{"type" => "response.completed", "response" => completed}}
    ]
  end

  defp completed_events do
    [
      {"response.completed",
       %{
         "type" => "response.completed",
         "response" => %{
           "id" => "resp_agents_tool_result",
           "object" => "response",
           "status" => "completed",
           "output" => [],
           "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}
         }
       }}
    ]
  end
end
