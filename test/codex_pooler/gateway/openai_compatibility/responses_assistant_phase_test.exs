defmodule CodexPooler.Gateway.OpenAICompatibility.ResponsesAssistantPhaseTest do
  # An assistant history item the `/v1` input adapter forwards may carry the message `phase` of the Codex
  # Responses model: `commentary`, `partial_answer` (stable answer text that may be followed by more output or
  # tools; Codex 8b6bb1c77, in no released client yet) and `final_answer`. The adapter refuses any other value, and a
  # phase on anything but an assistant message, before dispatch (findings#306). The provider accepted
  # `partial_answer` on a stateless assistant input item in both the Full and Lite request shapes (direct probe,
  # 2026-10-06), so the adapter forwards the phase byte for byte instead of dropping or rewriting it.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.OpenAICompatibility.Responses

  @phases ["commentary", "partial_answer", "final_answer"]

  for phase <- @phases do
    test "a replayed assistant item with phase #{phase} is forwarded with that phase" do
      item = assistant(%{"phase" => unquote(phase)})

      assert {:ok, %{payload: coerced}} = coerce([user("synthetic first"), item, user("synthetic follow-up")])
      assert [_first, replayed, _follow_up] = coerced["input"]
      assert %{"type" => "message", "role" => "assistant", "phase" => unquote(phase)} = replayed
      assert replayed["content"] == [%{"type" => "output_text", "text" => "synthetic answer"}]
    end

    test "a replayed #{phase} item in the SDK's completed-item shape keeps its phase beside id, status and annotations" do
      item = %{
        "id" => "msg_synthetic_replay",
        "type" => "message",
        "role" => "assistant",
        "status" => "completed",
        "phase" => unquote(phase),
        "content" => [%{"type" => "output_text", "text" => "synthetic answer", "annotations" => [], "logprobs" => []}]
      }

      assert {:ok, %{payload: coerced}} = coerce([user("synthetic first"), item, user("synthetic follow-up")])
      assert [_first, %{"role" => "assistant", "phase" => unquote(phase)}, _follow_up] = coerced["input"]
    end

    test "a #{phase} item rides a previous_response_id tool continuation like any other assistant history" do
      payload = %{
        "model" => "gpt-fixture-text",
        "previous_response_id" => "resp_fixture_previous",
        "input" => [assistant(%{"phase" => unquote(phase)}), %{"type" => "function_call_output", "call_id" => "call_fixture", "output" => "synthetic tool output"}]
      }

      assert {:ok, %{payload: coerced}} = Responses.coerce(payload)
      assert [%{"role" => "assistant", "phase" => unquote(phase)}, %{"type" => "function_call_output"}] = coerced["input"]
    end
  end

  test "an assistant item without a phase or with a null phase is still accepted and keeps no phase value" do
    assert {:ok, %{payload: omitted}} = coerce([user("synthetic first"), assistant(%{}), user("synthetic follow-up")])
    refute Map.has_key?(Enum.at(omitted["input"], 1), "phase")

    assert {:ok, %{payload: null_phase}} = coerce([user("synthetic first"), assistant(%{"phase" => nil}), user("synthetic follow-up")])
    assert Enum.at(null_phase["input"], 1)["phase"] == nil
  end

  test "string assistant content keeps a partial_answer phase" do
    item = %{"role" => "assistant", "phase" => "partial_answer", "content" => "synthetic answer"}

    assert {:ok, %{payload: coerced}} = coerce([user("synthetic first"), item, user("synthetic follow-up")])

    assert [_first, %{"type" => "message", "role" => "assistant", "phase" => "partial_answer", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}, _follow_up] =
             coerced["input"]
  end

  for {label, phase} <- [{"a different phase vocabulary", "progress"}, {"a hyphenated spelling", "partial-answer"}, {"another case", "Partial_Answer"}, {"a prefix of the name", "partial"}, {"the empty string", ""}, {"a number", 1}, {"a boolean", true}, {"an object", %{"name" => "partial_answer"}}, {"a list", ["partial_answer"]}] do
    test "an assistant item whose phase is #{label} is refused before dispatch" do
      assert {:error, %{status: 400, code: "invalid_request", param: "input"} = error} = coerce([user("synthetic first"), assistant(%{"phase" => unquote(Macro.escape(phase))}), user("synthetic follow-up")])
      assert error.message == "input item shape is not translatable"
    end
  end

  test "a partial_answer phase on a user message is refused like any phase on a non-assistant item" do
    item = %{"role" => "user", "phase" => "partial_answer", "content" => "synthetic text"}

    assert {:error, %{status: 400, code: "invalid_request", param: "input"}} = coerce([user("synthetic first"), item])
  end

  test "a partial_answer item still obeys the assistant replay shape rules" do
    assert {:ok, _accepted} = coerce([user("synthetic first"), assistant(%{"phase" => "partial_answer"}), user("synthetic follow-up")])

    for changes <- [
          %{"status" => "failed"},
          %{"content" => [%{"type" => "input_text", "text" => "synthetic"}]},
          %{"unexpected_field" => "synthetic"},
          %{"id" => 7}
        ] do
      item = assistant(Map.put(changes, "phase", "partial_answer"))

      assert {:error, %{status: 400, code: "invalid_request", param: "input"}} = coerce([user("synthetic first"), item, user("synthetic follow-up")])
    end
  end

  defp coerce(input), do: Responses.coerce(%{"model" => "gpt-fixture-text", "store" => false, "input" => input})

  defp user(text), do: %{"role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp assistant(fields), do: Map.merge(%{"role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}, fields)
end
