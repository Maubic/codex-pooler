defmodule CodexPooler.Gateway.Payloads.WebsocketTurnIdentityCommentaryTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity

  test "observed released commentary projection matches provider extras at both levels" do
    assert {:ok, expected} = WebsocketTurnIdentity.completed_item_digest(commentary())
    assert expected =~ ~r/\A[0-9a-f]{12}\z/

    for provider <- [Map.put(commentary(), "provider_extension", "synthetic"), update_in(commentary(), ["content", Access.all()], &Map.put(&1, "provider_extension", "synthetic")), provider()] do
      assert {:ok, ^expected} = WebsocketTurnIdentity.completed_item_digest(provider)
    end
  end

  test "retained identity, content order and malformed or unknown parts stay distinct" do
    assert {:ok, expected} = WebsocketTurnIdentity.completed_item_digest(commentary())

    for changed <- [Map.put(commentary(), "id", "msg_other"), Map.put(commentary(), "role", "user"), Map.put(commentary(), "phase", "final_answer"), Map.delete(commentary(), "phase"), put_in(commentary(), ["content", Access.at(0), "text"], "synthetic changed"), Map.update!(commentary(), "content", &Enum.reverse/1), Map.put(commentary(), "content", []), Map.put(commentary(), "content", nil), Map.put(commentary(), "content", "synthetic"), Map.put(commentary(), "id", 1), put_in(commentary(), ["content", Access.at(0), "type"], "unknown_part"), put_in(commentary(), ["content", Access.at(0), "text"], 1), Map.update!(commentary(), "content", &(&1 ++ [nil]))] do
      assert {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(changed)
      refute digest == expected
    end
  end

  test "unknown or malformed content keeps unknown fields bound on the whole item" do
    for malformed <- [Map.put(commentary(), "id", nil), Map.delete(commentary(), "id"), Map.put(commentary(), "content", []), put_in(commentary(), ["content", Access.at(0), "type"], "unknown_part"), put_in(commentary(), ["content", Access.at(0), "type"], "input_text"), put_in(commentary(), ["content", Access.at(0), "text"], nil), Map.update!(commentary(), "content", &(&1 ++ [7]))] do
      assert {:ok, before} = WebsocketTurnIdentity.completed_item_digest(malformed)
      assert {:ok, after_extra} = WebsocketTurnIdentity.completed_item_digest(Map.put(malformed, "provider_extension", "synthetic"))
      refute before == after_extra
    end
  end

  test "ordinary message, function and tool identity retains generic extras" do
    for item <- [Map.delete(commentary(), "phase"), Map.put(commentary(), "phase", "final_answer"), %{"type" => "function_call", "id" => "fc_synthetic", "call_id" => "call_synthetic", "name" => "synthetic", "arguments" => "{}"}, %{"type" => "custom_tool_call", "id" => "ct_synthetic", "call_id" => "call_synthetic", "name" => "synthetic", "input" => "synthetic"}] do
      assert {:ok, before} = WebsocketTurnIdentity.completed_item_digest(item)
      assert {:ok, after_extra} = WebsocketTurnIdentity.completed_item_digest(Map.put(item, "provider_extension", "synthetic"))
      refute before == after_extra
    end
  end

  test "projection changes completed receipts only, not request or replay claims" do
    semantic = :crypto.hash(:sha256, "synthetic commentary turn")
    original = %{"type" => "response.create", "model" => "synthetic-model", "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}]}
    assert {:ok, original_digest} = WebsocketTurnIdentity.replay_claim_digest(semantic, original)
    assert {:ok, item_digest} = WebsocketTurnIdentity.completed_item_digest(provider())
    grown = Map.update!(original, "input", &(&1 ++ [commentary()]))
    assert {:ok, [%{items: [^item_digest], digest: ^original_digest}]} = WebsocketTurnIdentity.grown_resend_candidates(semantic, grown)
    before = Map.put(original, "input", [provider()])
    after_projection = Map.put(original, "input", [commentary()])
    refute WebsocketTurnIdentity.request_claim_key(semantic, before) == WebsocketTurnIdentity.request_claim_key(semantic, after_projection)
    refute WebsocketTurnIdentity.replay_claim_digest(semantic, before) == WebsocketTurnIdentity.replay_claim_digest(semantic, after_projection)
    refute WebsocketTurnIdentity.http_resume_input_digest(semantic, before["input"]) == WebsocketTurnIdentity.http_resume_input_digest(semantic, after_projection["input"])
  end

  defp commentary, do: %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => "commentary", "content" => [%{"type" => "output_text", "text" => "synthetic one"}, %{"type" => "output_text", "text" => "synthetic two"}]}
  defp provider, do: commentary() |> Map.merge(%{"status" => "completed", "provider_extension" => "synthetic", "internal_chat_message_metadata_passthrough" => %{"executed_tool_calls" => []}, "nullable_extra" => nil}) |> update_in(["content", Access.all()], &Map.merge(&1, %{"annotations" => [], "logprobs" => [], "provider_extension" => "synthetic", "nullable_extra" => nil}))
end
