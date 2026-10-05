defmodule CodexPooler.Gateway.Transports.PublicResponsesToolCompletionTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.PublicResponsesToolCompletion, as: Completion

  test "no tools, unknown events and terminal-only snapshots impose no obligations" do
    for event <- [
          %{"type" => "response.future_event"},
          %{"type" => "response.output_item.added", "item" => %{"type" => "message"}},
          %{"type" => "response.completed", "response" => %{"output" => [%{"type" => "function_call"}]}},
          %{"type" => "response.incomplete"},
          %{"type" => "response.failed"},
          %{"type" => "error"},
          nil
        ] do
      assert Completion.completion_verdict(Completion.observe(Completion.new_state(), event)) == :ok
    end
  end

  for kind <- ["function_call", "custom_tool_call"] do
    test "#{kind} matching source identity/index closes only on item done" do
      kind = unquote(kind)
      state = observe([item("added", kind, 0, "item-a"), payload("delta", kind, 0, "item-a"), payload("done", kind, 0, "item-a")])
      assert Completion.completion_verdict(state) == {:error, :incomplete_tool_item}
      assert Completion.completion_verdict(Completion.observe(state, item("done", kind, 0, "item-a"))) == :ok
    end

    test "#{kind} call-only identity binds a later item id through same call and index" do
      kind = unquote(kind)
      state = observe([item("added", kind, 2, nil, "call-a"), item("done", kind, 2, "item-a", "call-a")])
      assert Completion.completion_verdict(state) == :ok
    end
  end

  test "two and three parallel tools complete in interleaved order" do
    for count <- [2, 3] do
      kinds = ["function_call", "custom_tool_call", "function_call"] |> Enum.take(count)
      added = Enum.with_index(kinds) |> Enum.map(fn {kind, index} -> item("added", kind, index, "item-#{index}") end)
      deltas = Enum.with_index(kinds) |> Enum.map(fn {kind, index} -> payload("delta", kind, index, "item-#{index}") end)
      done = Enum.with_index(kinds) |> Enum.reverse() |> Enum.map(fn {kind, index} -> item("done", kind, index, "item-#{index}") end)
      state = observe(added ++ deltas)
      assert Completion.completion_verdict(observe(Enum.drop(done, 1), state)) == {:error, :incomplete_tool_item}
      assert Completion.completion_verdict(observe(done, state)) == :ok
    end
  end

  for index <- [nil, -1, "0", 0.0, 9_007_199_254_740_992] do
    test "malformed output index #{inspect(index)} poisons correlation" do
      assert_invalid([item("added", "function_call", unquote(index), "item-a")])
    end
  end

  test "maximum safe output index is accepted" do
    index = 9_007_199_254_740_991
    assert Completion.completion_verdict(observe([item("added", "function_call", index, "item-a"), item("done", "function_call", index, "item-a")])) == :ok
  end

  for identity <- [nil, "", " \t ", 42, <<255>>] do
    test "invalid source identity #{inspect(identity)} never becomes generated identity" do
      assert_invalid([item("added", "function_call", 0, unquote(identity))])
    end
  end

  test "top-level item id establishes identity and must agree with nested id" do
    added = item("added", "function_call", 0, nil) |> Map.put("item_id", "item-a")
    done = item("done", "function_call", 0, "item-a") |> Map.put("item_id", "item-a")
    assert Completion.completion_verdict(observe([added, done])) == :ok
    assert_invalid([Map.put(added, "item", %{"type" => "function_call", "id" => "item-b"})])
    assert_invalid([Map.put(done, "item_id", " item-a")])
  end

  test "exact string identity is not trimmed to manufacture equality" do
    assert_invalid([item("added", "function_call", 0, " item-a "), item("done", "function_call", 0, "item-a")])
    assert Completion.completion_verdict(observe([item("added", "function_call", 0, " item-a "), item("done", "function_call", 0, " item-a ")])) == :ok
  end

  test "valid UTF-8 identities use byte bounds rather than character counts" do
    identity = String.duplicate("é", 512)
    assert byte_size(identity) == 1024
    assert Completion.completion_verdict(observe([item("added", "function_call", 0, identity), item("done", "function_call", 0, identity)])) == :ok
    assert Completion.completion_verdict(observe([item("added", "function_call", 0, identity <> "x")])) == {:error, :tool_tracking_overflow}
    assert_invalid([item("added", "function_call", 0, "\u00a0")])
  end

  test "top-level call alias correlates payloads and later done source identity" do
    added = item("added", "custom_tool_call", 0, nil, "call-a")
    delta = %{"type" => "response.custom_tool_call_input.delta", "output_index" => 0, "call_id" => "call-a"}
    done = item("done", "custom_tool_call", 0, "item-a") |> Map.put("call_id", "call-a")
    assert Completion.completion_verdict(observe([added, delta, done])) == :ok
    assert_invalid([added, Map.put(delta, "item_id", "item-a")])
    assert_invalid([added, Map.put(done, "item", %{"type" => "custom_tool_call", "id" => "item-a", "call_id" => "call-b"})])
  end

  test "failed and incomplete terminals preserve pending metadata without accepting later frames" do
    for type <- ["response.failed", "response.incomplete", "error"] do
      state = observe([item("added", "function_call", 0, "item-a"), %{"type" => type}])
      assert Completion.completion_verdict(state) == {:error, :incomplete_tool_item}
      assert Completion.observe(state, item("done", "function_call", 0, "item-a")) == state
    end
  end

  test "call aliases must be valid and agree with established bindings" do
    for call <- ["", " ", 42, <<255>>] do
      assert_invalid([item("added", "function_call", 0, "item-a", call)])
    end

    assert_invalid([item("added", "function_call", 0, "item-a", "call-a"), item("done", "function_call", 0, "item-a", "call-b")])
    assert_invalid([item("added", "function_call", 0, nil, "call-a"), item("done", "function_call", 0, "item-a", "call-b")])
    assert_invalid([item("added", "function_call", 0, nil, "call-a"), item("done", "function_call", 0, "item-a")])
  end

  test "wrong index, type and identity cannot complete another tool" do
    added = item("added", "function_call", 0, "item-a")

    for done <- [item("done", "function_call", 1, "item-a"), item("done", "function_call", 0, "item-b"), item("done", "custom_tool_call", 0, "item-a"), item("done", "message", 0, "item-a")] do
      assert_invalid([added, done])
    end
  end

  test "completed bindings detect identity, alias and index reuse" do
    complete = [item("added", "function_call", 0, "item-a", "call-a"), item("done", "function_call", 0, "item-a", "call-a")]

    for reused <- [item("added", "function_call", 1, "item-a"), item("added", "function_call", 0, "item-b"), item("added", "function_call", 1, "item-b", "call-a")] do
      assert_invalid(complete ++ [reused])
    end
  end

  for {kind, output_kind} <- [{"function_call", "function_call_output"}, {"custom_tool_call", "custom_tool_call_output"}] do
    test "#{output_kind} with distinct item and index preserves its completed call" do
      kind = unquote(kind)
      output_kind = unquote(output_kind)
      complete = observe([item("added", kind, 1, "item-a", "call-a"), item("done", kind, 1, "item-a", "call-a")])
      output = item("added", output_kind, 2, "output-a", "call-a") |> put_in(["item", "output"], "synthetic-private-output")
      state = observe([output, %{output | "type" => "response.output_item.done"}, %{"type" => "response.completed"}], complete)
      assert Completion.completion_verdict(state) == :ok
      assert state.tools == complete.tools
      assert state.aliases == complete.aliases
      refute :erlang.term_to_binary(state) =~ "synthetic-private-output"
    end

    test "#{output_kind} cannot discharge pending calls or hide collisions" do
      kind = unquote(kind)
      output_kind = unquote(output_kind)
      added = item("added", kind, 1, "item-a", "call-a")
      output = item("done", output_kind, 2, "output-a", "call-a")
      assert Completion.completion_verdict(observe([added, output])) == {:error, :incomplete_tool_item}

      for phase <- ["added", "done"],
          {index, id} <- [{1, "output-a"}, {2, "item-a"}] do
        assert_invalid([added, item("done", kind, 1, "item-a", "call-a"), item(phase, output_kind, index, id, "call-a")])
      end

      assert_invalid([added, put_in(output, ["item", "call_id"], "call-b") |> Map.put("call_id", "call-a")])
      assert_invalid([added, item("done", "message", 2, "output-a", "call-a")])
      assert_invalid([added, item("done", output_kind, -1, "output-a", "call-a")])
      assert_invalid([added, item("done", output_kind, 2, "", "call-a")])
      assert Completion.completion_verdict(observe([added, item("done", output_kind, 2, String.duplicate("x", 1025), "call-a")])) == {:error, :tool_tracking_overflow}
    end
  end

  test "duplicate add, duplicate done and orphan tool events poison" do
    added = item("added", "function_call", 0, "item-a")
    done = item("done", "function_call", 0, "item-a")

    for events <- [[added, added], [added, done, done], [done], [payload("delta", "function_call", 0, "item-a")], [payload("done", "custom_tool_call", 0, "item-a")]] do
      assert_invalid(events)
    end
  end

  test "payload family must match its open item and cannot follow item done" do
    added = item("added", "function_call", 0, "item-a")
    assert_invalid([added, payload("delta", "custom_tool_call", 0, "item-a")])
    assert_invalid([added, item("done", "function_call", 0, "item-a"), payload("delta", "function_call", 0, "item-a")])
  end

  for status <- ["in_progress", "incomplete", "failed", nil, 42, " completed "] do
    test "explicit malformed or unsuccessful done status #{inspect(status)} poisons" do
      done = item("done", "function_call", 0, "item-a") |> put_in(["item", "status"], unquote(status))
      assert_invalid([item("added", "function_call", 0, "item-a"), done])
    end
  end

  test "omitted and completed done status discharge" do
    for done <- [item("done", "function_call", 0, "item-a"), put_in(item("done", "function_call", 0, "item-a"), ["item", "status"], "completed")] do
      assert Completion.completion_verdict(observe([item("added", "function_call", 0, "item-a"), done])) == :ok
    end
  end

  test "terminal snapshot cannot discharge pending obligations and later frames are ignored" do
    for type <- ["response.completed", "response.done"] do
      state = observe([item("added", "function_call", 0, "item-a"), %{"type" => type, "response" => %{"status" => "completed", "output" => [%{"type" => "function_call", "id" => "item-a", "status" => "completed"}]}}])
      assert Completion.completion_verdict(state) == {:error, :incomplete_tool_item}
      assert Completion.observe(state, item("done", "function_call", 0, "item-a")) == state
    end
  end

  test "4096 completed tools retain their bindings and 4097 freezes growth" do
    state = Enum.reduce(0..4095, Completion.new_state(), fn index, state -> observe([item("added", "function_call", index, "item-#{index}"), item("done", "function_call", index, "item-#{index}")], state) end)
    assert Completion.completion_verdict(state) == :ok
    overflow = Completion.observe(state, item("added", "function_call", 4096, "item-next"))
    assert Completion.completion_verdict(overflow) == {:error, :tool_tracking_overflow}
    assert Completion.observe(overflow, item("added", "function_call", 4097, "item-later")) == overflow
  end

  for key <- ["id", "call_id", "item_id"] do
    test "#{key} accepts 1024 bytes and rejects 1025 bytes" do
      key = unquote(key)

      for size <- [1024, 1025] do
        value = String.duplicate("x", size)
        added = item("added", "function_call", 0, nil)
        done = item("done", "function_call", 0, nil)
        {added, done} = if key == "item_id", do: {Map.put(added, key, value), Map.put(done, key, value)}, else: {put_in(added, ["item", key], value), put_in(done, ["item", key], value)}
        assert Completion.completion_verdict(observe([added, done])) == if(size == 1024, do: :ok, else: {:error, :tool_tracking_overflow})
      end
    end
  end

  test "poison latches first reason and new state resets every binding" do
    state = observe([item("added", "function_call", -1, "item-a")])
    assert Completion.observe(state, item("added", "function_call", 0, String.duplicate("x", 1025))) == state
    assert Completion.completion_verdict(state) == {:error, :invalid_tool_correlation}
    fresh = Completion.new_state()
    assert Completion.completion_verdict(fresh) == :ok
    assert Completion.completion_verdict(observe([item("added", "function_call", 0, "item-a"), item("done", "function_call", 0, "item-a")], fresh)) == :ok
  end

  test "tracker retains no arguments input delta or arbitrary payload content" do
    sentinel = "synthetic-private-content-sentinel"
    added = item("added", "function_call", 0, "item-a") |> put_in(["item", "arguments"], sentinel)
    state = observe([added, Map.put(payload("delta", "function_call", 0, "item-a"), "delta", sentinel), Map.put(payload("done", "function_call", 0, "item-a"), "arguments", sentinel)])
    refute :erlang.term_to_binary(state) =~ sentinel
    assert Completion.completion_verdict(state) == {:error, :incomplete_tool_item}
  end

  defp observe(events, state \\ Completion.new_state()), do: Enum.reduce(events, state, &Completion.observe(&2, &1))

  defp assert_invalid(events), do: assert(Completion.completion_verdict(observe(events)) == {:error, :invalid_tool_correlation})

  defp item(phase, kind, index, id, call \\ nil) do
    item = %{"type" => kind}
    item = if is_nil(id), do: item, else: Map.put(item, "id", id)
    item = if is_nil(call), do: item, else: Map.put(item, "call_id", call)
    %{"type" => "response.output_item.#{phase}", "output_index" => index, "item" => item}
  end

  defp payload(phase, kind, index, id) do
    family = if kind == "function_call", do: "function_call_arguments", else: "custom_tool_call_input"
    %{"type" => "response.#{family}.#{phase}", "output_index" => index, "item_id" => id}
  end
end
