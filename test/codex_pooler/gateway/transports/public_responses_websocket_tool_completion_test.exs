defmodule CodexPooler.Gateway.Transports.PublicResponsesWebsocketToolCompletionTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.PublicResponsesWebsocket

  for kind <- ["function_call", "custom_tool_call"], defect <- [:missing_done, :wrong_index, :wrong_id, :missing_index, :string_index, :one_pending] do
    @tag kind: kind
    @tag defect: defect
    test "#{kind} #{defect} fails defensively before a success terminal", %{kind: kind, defect: defect} do
      item = %{"type" => kind, "id" => "item_fixture", "call_id" => "call_fixture", "name" => "fixture"}
      added = %{"type" => "response.output_item.added", "output_index" => 0, "item" => item}
      done = %{"type" => "response.output_item.done", "output_index" => 0, "item" => item}

      events =
        case defect do
          :missing_done -> [added]
          :wrong_index -> [added, Map.put(done, "output_index", 1)]
          :wrong_id -> [added, put_in(done, ["item", "id"], "wrong_fixture")]
          :missing_index -> [Map.delete(added, "output_index"), done]
          :string_index -> [Map.put(added, "output_index", "0"), done]
          :one_pending -> [added, done, %{added | "output_index" => 1, "item" => %{item | "id" => "other_fixture", "call_id" => "other_call"}}]
        end

      state =
        Enum.reduce(events, PublicResponsesWebsocket.new_state("fixture_stream"), fn event, state ->
          assert {:push, _wire, state} = normalize(event, state)
          state
        end)

      assert {:push, wire, state} = normalize(terminal("response.completed"), state)
      event = CodexPooler.JSON.decode!(wire)
      assert event["type"] == "error"
      assert event["error"]["code"] == "server_error"
      assert event["stream_id"] == "fixture_stream"
      assert state.terminal_latched?
      assert {:drop, ^state} = normalize(terminal("response.completed"), state)
      assert {:push, next_wire, _state} = normalize(terminal("response.completed"), PublicResponsesWebsocket.new_state())
      assert CodexPooler.JSON.decode!(next_wire)["type"] == "response.completed"
    end
  end

  for kind <- ["function_call", "custom_tool_call"] do
    test "healthy #{kind} done and omitted terminal-only fields retain success" do
      item = %{"type" => unquote(kind), "id" => "item_fixture", "call_id" => "call_fixture", "name" => "fixture"}
      state = PublicResponsesWebsocket.new_state()
      assert {:push, _, state} = normalize(%{"type" => "response.output_item.added", "output_index" => 0, "item" => item}, state)
      assert {:push, _, state} = normalize(%{"type" => "response.output_item.done", "output_index" => 0, "item" => item}, state)
      assert {:push, wire, _} = normalize(terminal("response.completed"), state)
      assert CodexPooler.JSON.decode!(wire)["type"] == "response.completed"
      event = put_in(terminal("response.completed"), ["response", "output"], [Map.drop(item, ["id", "status"])])
      assert {:push, wire, _} = normalize(event, PublicResponsesWebsocket.new_state())
      assert CodexPooler.JSON.decode!(wire)["type"] == "response.completed"
    end
  end

  for type <- ["response.failed", "response.incomplete"] do
    test "pending tool does not rewrite provider #{type}" do
      item = %{"type" => "function_call", "id" => "item_fixture", "call_id" => "call_fixture"}
      assert {:push, _, state} = normalize(%{"type" => "response.output_item.added", "output_index" => 0, "item" => item}, PublicResponsesWebsocket.new_state())
      assert {:push, wire, _} = normalize(terminal(unquote(type)), state)
      assert CodexPooler.JSON.decode!(wire)["type"] == unquote(type)
    end
  end

  defp terminal(type) do
    status =
      case type do
        "response.completed" -> "completed"
        "response.failed" -> "failed"
        "response.incomplete" -> "incomplete"
      end

    %{"type" => type, "response" => %{"id" => "resp_fixture", "status" => status, "output" => [], "usage" => %{"input_tokens" => 5457, "input_tokens_details" => %{"cache_write_tokens" => 5454}, "output_tokens" => 2, "total_tokens" => 5459}}}
  end

  defp normalize(event, state), do: PublicResponsesWebsocket.normalize(CodexPooler.JSON.encode!(event), state)
end
