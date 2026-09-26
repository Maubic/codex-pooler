defmodule CodexPooler.Accounting.NativeHttpToolObservationTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.NativeHttpToolObservation, as: Observation

  for {kind, prefix, field} <- [
        {"custom_tool_call", "response.custom_tool_call_input", "input"},
        {"function_call", "response.function_call_arguments", "arguments"}
      ] do
    test "#{kind} remains incomplete through input.done but never through item.done" do
      kind = unquote(kind)
      prefix = unquote(prefix)
      started = started(kind)
      delta = %{"type" => prefix <> ".delta", "item_id" => "item_synthetic", "output_index" => 0, "delta" => "synthetic"}
      state = Observation.observe(started, delta["type"], delta)
      assert Observation.eligible_metadata?(Observation.metadata(state, true))
      done = %{"type" => prefix <> ".done", "item_id" => "item_synthetic", "output_index" => 0, unquote(field) => "synthetic"}
      state = Observation.observe(state, done["type"], done)
      assert Observation.eligible_metadata?(Observation.metadata(state, true))
      refute Observation.eligible_metadata?(Observation.metadata(state, false))

      completed = %{"type" => "response.output_item.done", "item" => %{"type" => kind}}
      state = Observation.observe(state, completed["type"], completed)
      refute Observation.eligible_metadata?(Observation.metadata(state, true))
    end
  end

  test "reasoning and known controls preserve a trailing incomplete tool at its actual index" do
    reasoning = %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_synthetic"}}
    summary = %{"type" => "response.reasoning_summary_text.delta", "item_id" => "rs_synthetic", "output_index" => 0, "summary_index" => 0, "delta" => "synthetic"}

    for kind <- ["custom_tool_call", "function_call"], index <- [1, 3] do
      tool = Map.put(added(kind), "output_index", index)
      events = [reasoning, summary, %{"type" => "codex.rate_limits"}, tool, %{"type" => "codex.response.metadata"}]
      state = Enum.reduce(events, Observation.new(), fn event, state -> Observation.observe(state, event["type"], event) end)
      assert Observation.eligible_metadata?(Observation.metadata(state, true))

      prefix = if kind == "custom_tool_call", do: "response.custom_tool_call_input", else: "response.function_call_arguments"
      delta = %{"type" => prefix <> ".delta", "item_id" => "item_synthetic", "output_index" => index, "delta" => "synthetic"}
      state = Observation.observe(state, delta["type"], delta)
      assert Observation.eligible_metadata?(Observation.metadata(state, true))
      field = if kind == "custom_tool_call", do: "input", else: "arguments"
      done = %{"type" => prefix <> ".done", "item_id" => "item_synthetic", "output_index" => index, field => "synthetic"}
      done_state = Observation.observe(state, done["type"], done)
      assert Observation.eligible_metadata?(Observation.metadata(done_state, true))
      assert done_state.input_done?

      for event <- [
            Map.put(delta, "output_index", 0),
            reasoning,
            %{"type" => "response.output_item.done", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_synthetic"}},
            %{"type" => "codex.unknown"}
          ] do
        refute state |> Observation.observe(event["type"], event) |> Observation.metadata(true) |> Observation.eligible_metadata?()
      end
    end
  end

  test "reasoning events require the announced item, matching index and complete content shape" do
    reasoning = %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_synthetic"}}
    initial = Observation.observe(Observation.new(), reasoning["type"], reasoning)
    address = %{"item_id" => "rs_synthetic", "output_index" => 0}

    for {type, content} <- [
          {"response.reasoning_summary_text.delta", %{"summary_index" => 0, "delta" => "synthetic"}},
          {"response.reasoning_summary_text.done", %{"summary_index" => 0, "text" => "synthetic"}},
          {"response.reasoning_text.delta", %{"content_index" => 0, "delta" => "synthetic"}},
          {"response.reasoning_text.done", %{"content_index" => 0, "text" => "synthetic"}},
          {"response.reasoning_summary_part.added", %{"summary_index" => 0, "part" => %{"type" => "summary_text", "text" => ""}}},
          {"response.reasoning_summary_part.done", %{"summary_index" => 0, "part" => %{"type" => "summary_text", "text" => "synthetic"}}}
        ] do
      event = address |> Map.merge(content) |> Map.put("type", type)
      refute Observation.observe(initial, type, event).poisoned?

      for invalid <- [Map.put(event, "item_id", "other"), Map.put(event, "output_index", 1), Map.drop(event, Map.keys(content))] do
        assert Observation.observe(initial, type, invalid).poisoned?
      end
    end

    assert Observation.observe(initial, reasoning["type"], reasoning).poisoned?
    assert Observation.observe(initial, "response.output_item.added", added("function_call")).poisoned?
  end

  test "malformed, unknown, terminal, mismatched and duplicate events permanently poison authority" do
    for event <- [
          %{},
          %{"type" => "response.unknown"},
          %{"type" => "response.completed"},
          %{"type" => "error"},
          %{"type" => "response.output_item.done"},
          added("custom_tool_call"),
          %{"type" => "response.custom_tool_call_input.delta", "item_id" => "other", "output_index" => 0, "delta" => "synthetic"},
          %{"type" => "response.custom_tool_call_input.delta", "item_id" => "item_synthetic", "output_index" => 1, "delta" => "synthetic"}
        ] do
      state = Observation.observe(started("custom_tool_call"), event["type"], event)
      refute Observation.eligible_metadata?(Observation.metadata(state, true))
      state = Observation.observe(state, "response.output_item.added", added("custom_tool_call"))
      refute Observation.eligible_metadata?(Observation.metadata(state, true))
    end

    for kind <- ["computer_call", "web_search_call", "unknown"] do
      refute Observation.eligible_metadata?(Observation.metadata(started(kind), true))
    end

    refute Observation.eligible_metadata?(Observation.metadata(Observation.new(), true))
    refute Observation.eligible_metadata?(%{})
    refute Observation.eligible_metadata?(nil)
    refute inspect(Observation.metadata(started("custom_tool_call"), true)) =~ "item_synthetic"
  end

  defp started(kind), do: Observation.observe(Observation.new(), "response.output_item.added", added(kind))

  defp added(kind),
    do: %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"id" => "item_synthetic", "type" => kind, "call_id" => "call_synthetic", "name" => "sample_tool", "status" => "in_progress"}}
end
