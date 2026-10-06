defmodule CodexPooler.Gateway.Payloads.NativeContinuationTailTest do
  # The items a Codex client records between two sampling requests of one turn
  # (findings#311): what may follow a response's completed output items in a
  # native HTTP re-sample. The controller consequences live in
  # `test/codex_pooler_web/controllers/runtime/backend_codex_http_resample_test.exs`.
  #
  # Provenance: the allowed items follow the client source (`session/turn.rs`
  # `run_turn`, `time_reminder.rs`, `context/rollout_budget.rs`,
  # `reasoning_effort.rs`, `top_level_tools.rs`); texts are synthetic.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.NativeContinuationTail

  @sentinel "sentinel_tail_value"

  describe "check/1 accepts what the turn loop records" do
    test "an empty tail" do
      assert NativeContinuationTail.check([]) == :ok
    end

    for {label, item} <- [
          {"a time reminder", %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "<current_time_reminder>\nsynthetic\n</current_time_reminder>"}]}},
          {"a rollout budget", %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "<rollout_budget>\nsynthetic\n</rollout_budget>"}]}},
          {"an unmarked developer fragment", %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "synthetic hook context"}]}},
          {"a configuration update", %{"type" => "configuration_update", "reasoning" => %{"effort" => "high"}}},
          {"a developer tool manifest", %{"type" => "additional_tools", "role" => "developer", "tools" => []}}
        ] do
      test "#{label}" do
        assert NativeContinuationTail.check([unquote(Macro.escape(item))]) == :ok
      end
    end

    test "up to the bound, and refuses one item more by its length alone" do
      fragment = developer("synthetic fragment")
      bound = NativeContinuationTail.max_items()
      assert bound == 16
      assert NativeContinuationTail.check(List.duplicate(fragment, bound)) == :ok
      assert NativeContinuationTail.check(List.duplicate(fragment, bound + 1)) == {:error, %{reason: :too_long, tail_length: bound + 1}}
    end
  end

  describe "check/1 refuses everything else at the first such item" do
    for {label, item, type, role} <- [
          {"a user message", %{"type" => "message", "role" => "user", "content" => []}, "message", "user"},
          {"an assistant message", %{"type" => "message", "role" => "assistant", "content" => []}, "message", "assistant"},
          {"a system message", %{"type" => "message", "role" => "system", "content" => []}, "message", "system"},
          {"reasoning", %{"type" => "reasoning", "summary" => []}, "reasoning", "none"},
          {"a function call", %{"type" => "function_call", "call_id" => "call_1", "name" => "synthetic_tool", "arguments" => "{}"}, "function_call", "none"},
          {"a function result", %{"type" => "function_call_output", "call_id" => "call_1", "output" => "synthetic"}, "function_call_output", "none"},
          {"a custom tool result", %{"type" => "custom_tool_call_output", "call_id" => "call_1", "output" => "synthetic"}, "custom_tool_call_output", "none"},
          {"a compaction item", %{"type" => "compaction", "encrypted_content" => "synthetic"}, "compaction", "none"},
          {"addressed mail", %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => []}, "agent_message", "none"},
          {"a tool manifest of another role", %{"type" => "additional_tools", "role" => "assistant", "tools" => []}, "additional_tools", "assistant"},
          {"a tool manifest without a role", %{"type" => "additional_tools", "tools" => []}, "additional_tools", "none"}
        ] do
      test "#{label}" do
        tail = [developer("synthetic fragment"), unquote(Macro.escape(item)), developer("synthetic later fragment")]

        assert NativeContinuationTail.check(tail) == {:error, %{reason: :item, tail_index: 1, tail_length: 3, item_type: unquote(type), item_role: unquote(role)}}
      end
    end

    test "an unknown type or role reads other, and only a fingerprint of the type is kept" do
      item = %{"type" => @sentinel, "role" => @sentinel, "content" => @sentinel}

      assert {:error, %{reason: :item, tail_index: 0, tail_length: 1, item_type: "other", item_role: "other", item_type_fingerprint: fingerprint} = refusal} = NativeContinuationTail.check([item])
      assert fingerprint =~ ~r/\A[0-9a-f]{12}\z/
      assert {:error, %{item_type_fingerprint: ^fingerprint}} = NativeContinuationTail.check([%{"type" => @sentinel}])
      assert {:error, %{item_type_fingerprint: other}} = NativeContinuationTail.check([%{"type" => "another_sentinel"}])
      assert other != fingerprint
      refute inspect(refusal) =~ @sentinel
    end

    test "an item without a string type, and a non-object item" do
      assert {:error, %{item_type: "untyped", item_role: "developer", tail_index: 0}} = NativeContinuationTail.check([%{"role" => "developer"}])
      assert {:error, %{item_type: "untyped", item_role: "none", tail_index: 0}} = NativeContinuationTail.check([%{"type" => 7}])
      assert {:error, %{item_type: "non_object", item_role: "none", tail_index: 0}} = NativeContinuationTail.check([@sentinel])
      refute NativeContinuationTail.check([@sentinel]) |> inspect() =~ @sentinel
    end
  end

  defp developer(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}
end
