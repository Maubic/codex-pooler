defmodule CodexPooler.Gateway.Payloads.WebsocketTurnIdentityPartialAnswerTest do
  # A completed assistant message tagged `partial_answer` (stable answer text that may be followed by more output or
  # tools) is recorded by the client through the same typed `ResponseItem::Message` model as a
  # commentary message and resent with exactly the fields that model keeps: `type`, `id`, `role`, `phase` and the
  # ordered `output_text` parts' `type` and `text`; serde drops every provider-only field. The completed-item
  # identity therefore projects it like commentary, so the item the provider pushed and the item the client resends
  # name the same receipt digest. The partial-answer projection is source-derived and has not been captured
  # on the wire. Items, ids and text are synthetic.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity

  test "the client's typed partial-answer projection matches the provider item with extras at both levels" do
    assert {:ok, expected} = WebsocketTurnIdentity.completed_item_digest(partial_answer())
    assert expected =~ ~r/\A[0-9a-f]{12}\z/

    for provider <- [Map.put(partial_answer(), "provider_extension", "synthetic"), update_in(partial_answer(), ["content", Access.all()], &Map.put(&1, "provider_extension", "synthetic")), provider()] do
      assert {:ok, ^expected} = WebsocketTurnIdentity.completed_item_digest(provider)
    end
  end

  test "retained identity, content order and malformed or unknown parts stay distinct" do
    assert {:ok, expected} = WebsocketTurnIdentity.completed_item_digest(partial_answer())

    for changed <- [Map.put(partial_answer(), "id", "msg_other"), Map.put(partial_answer(), "role", "user"), Map.put(partial_answer(), "phase", "commentary"), Map.put(partial_answer(), "phase", "final_answer"), Map.delete(partial_answer(), "phase"), put_in(partial_answer(), ["content", Access.at(0), "text"], "synthetic changed"), Map.update!(partial_answer(), "content", &Enum.reverse/1), Map.put(partial_answer(), "content", []), Map.put(partial_answer(), "content", nil), Map.put(partial_answer(), "content", "synthetic"), Map.put(partial_answer(), "id", 1), put_in(partial_answer(), ["content", Access.at(0), "type"], "unknown_part"), put_in(partial_answer(), ["content", Access.at(0), "text"], 1), Map.update!(partial_answer(), "content", &(&1 ++ [nil]))] do
      assert {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(changed)
      refute digest == expected
    end
  end

  test "unknown or malformed content keeps unknown fields bound on the whole item" do
    for malformed <- [Map.put(partial_answer(), "id", nil), Map.delete(partial_answer(), "id"), Map.put(partial_answer(), "content", []), put_in(partial_answer(), ["content", Access.at(0), "type"], "unknown_part"), put_in(partial_answer(), ["content", Access.at(0), "type"], "input_text"), put_in(partial_answer(), ["content", Access.at(0), "text"], nil), Map.update!(partial_answer(), "content", &(&1 ++ [7]))] do
      assert {:ok, before} = WebsocketTurnIdentity.completed_item_digest(malformed)
      assert {:ok, after_extra} = WebsocketTurnIdentity.completed_item_digest(Map.put(malformed, "provider_extension", "synthetic"))
      refute before == after_extra
    end
  end

  test "commentary, partial answers, final answers and unphased messages never share an identity" do
    digests =
      for phase <- ["commentary", "partial_answer", "final_answer", nil] do
        item = if phase, do: Map.put(partial_answer(), "phase", phase), else: Map.delete(partial_answer(), "phase")
        assert {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(item)
        digest
      end

    assert digests == Enum.uniq(digests)
  end

  test "final answers and unphased messages keep the generic identity that retains provider extras" do
    for item <- [Map.put(partial_answer(), "phase", "final_answer"), Map.delete(partial_answer(), "phase")] do
      assert {:ok, before} = WebsocketTurnIdentity.completed_item_digest(item)
      assert {:ok, after_extra} = WebsocketTurnIdentity.completed_item_digest(Map.put(item, "provider_extension", "synthetic"))
      refute before == after_extra
    end
  end

  test "the grown resend of a partial answer names the pushed item and leaves request and replay claims unchanged" do
    semantic = :crypto.hash(:sha256, "synthetic partial answer turn")
    original = %{"type" => "response.create", "model" => "synthetic-model", "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}, %{"type" => "message", "role" => "user", "content" => "synthetic second"}]}
    assert {:ok, original_digest} = WebsocketTurnIdentity.replay_claim_digest(semantic, original)
    assert {:ok, item_digest} = WebsocketTurnIdentity.completed_item_digest(provider())
    grown = Map.update!(original, "input", &(&1 ++ [partial_answer()]))
    assert {:ok, [%{items: [^item_digest], digest: ^original_digest}]} = WebsocketTurnIdentity.grown_resend_candidates(semantic, grown)

    before = Map.put(original, "input", [provider()])
    after_projection = Map.put(original, "input", [partial_answer()])
    refute WebsocketTurnIdentity.request_claim_key(semantic, before) == WebsocketTurnIdentity.request_claim_key(semantic, after_projection)
    refute WebsocketTurnIdentity.replay_claim_digest(semantic, before) == WebsocketTurnIdentity.replay_claim_digest(semantic, after_projection)
    refute WebsocketTurnIdentity.http_resume_input_digest(semantic, before["input"]) == WebsocketTurnIdentity.http_resume_input_digest(semantic, after_projection["input"])
  end

  defp partial_answer, do: %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => "partial_answer", "content" => [%{"type" => "output_text", "text" => "synthetic one"}, %{"type" => "output_text", "text" => "synthetic two"}]}
  defp provider, do: partial_answer() |> Map.merge(%{"status" => "completed", "provider_extension" => "synthetic", "internal_chat_message_metadata_passthrough" => %{"executed_tool_calls" => []}, "nullable_extra" => nil}) |> update_in(["content", Access.all()], &Map.merge(&1, %{"annotations" => [], "logprobs" => [], "provider_extension" => "synthetic", "nullable_extra" => nil}))
end
