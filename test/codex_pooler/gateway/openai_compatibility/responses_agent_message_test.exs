defmodule CodexPooler.Gateway.OpenAICompatibility.ResponsesAgentMessageTest do
  # A Codex multi-agent v2 client replays `agent_message` items in its history: the mailbox message one agent
  # delivered to another, plaintext (a subagent's `FINAL_ANSWER` handed back to its lead) or sealed (the lead's
  # `NEW_TASK` / `MESSAGE` handed to a subagent). A gateway that relays that history to `/v1/responses` used to meet
  # `400 invalid_request` on `input` ("input item shape is not translatable"). The provider reads both forms in a
  # stateless request, in the Full and the Lite request shape (direct probe, 2026-10-06), and the native backend route
  # forwards the same items, so the adapter admits exactly the shapes that route recognizes and forwards them
  # untouched: a nonempty plaintext `input_text` content, or the exact sealed two-part handoff. Everything else is
  # refused before dispatch instead of being filtered silently.
  use ExUnit.Case, async: true

  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.Gateway.OpenAICompatibility.Responses

  @author "/root/worker"
  @lead "/root"
  @envelope "Message Type: FINAL_ANSWER\nTask name: /root\nSender: /root/worker\nPayload:\nsynthetic worker answer"
  @refusal %{status: 400, code: "invalid_request", param: "input", message: "input item shape is not translatable"}

  @accepted_plaintext_variants [
    {"without an id", :no_id},
    {"with a null passthrough", :null_passthrough},
    {"with a passthrough map", :passthrough_map},
    {"with several input_text parts", :several_parts}
  ]

  @refused_plaintext_variants [
    {"an unknown top-level key", :status_key},
    {"a role beside the author", :role_key},
    {"a phase", :phase_key},
    {"a blank id", :blank_id},
    {"a null id", :null_id},
    {"a non-string id", :integer_id},
    {"a missing author", :no_author},
    {"a blank author", :blank_author},
    {"a non-string recipient", :map_recipient},
    {"a missing recipient", :no_recipient},
    {"a missing content", :no_content},
    {"an empty content list", :empty_content},
    {"a string content", :string_content},
    {"a null content", :null_content},
    {"a map content", :map_content},
    {"an output_text part", :output_text_part},
    {"a part with an extra key", :part_extra_key},
    {"a non-string text", :integer_text},
    {"a non-map part", :string_part},
    {"a non-map passthrough", :string_passthrough}
  ]

  @refused_sealed_variants [
    {"an encrypted part alone", :cipher_alone},
    {"a handoff addressed to another recipient than its envelope names", :other_recipient},
    {"a handoff whose envelope names another sender", :other_author},
    {"an author that is not an agent path", :author_not_agent_path},
    {"an arbitrary envelope text", :arbitrary_envelope},
    {"a blank cipher", :blank_cipher},
    {"the parts in the reverse order", :reversed_parts},
    {"a third part", :third_part},
    {"an extra key on the cipher part", :cipher_extra_key},
    {"an extra key on the envelope part", :envelope_extra_key},
    {"a non-string cipher", :integer_cipher}
  ]

  describe "plaintext agent_message" do
    test "reaches the upstream payload exactly as the client sent it" do
      item = plaintext()

      assert {:ok, %{payload: payload}} = coerce(history(item))
      assert payload["input"] == [Map.put(user("synthetic task"), "type", "message"), item]
    end

    test "stays an agent_message instead of being rewritten into a user message by the generic content clauses" do
      assert {:ok, %{payload: %{"input" => [_user, forwarded]}}} = coerce(history(plaintext()))

      assert forwarded["type"] == "agent_message"
      assert forwarded["content"] == [%{"type" => "input_text", "text" => @envelope}]
      refute Map.has_key?(forwarded, "role")
    end

    for {label, variant} <- @accepted_plaintext_variants do
      test "is forwarded unchanged #{label}" do
        item = mutate(unquote(variant), plaintext())

        assert {:ok, %{payload: %{"input" => [_user, forwarded]}}} = coerce(history(item))
        assert forwarded == item
      end
    end

    test "sits next to the call items of the lead's own history, in the position the client sent it" do
      call = %{"type" => "function_call", "call_id" => "call_synthetic_wait", "name" => "wait_agent", "namespace" => "collaboration", "arguments" => "{}"}
      output = %{"type" => "function_call_output", "call_id" => "call_synthetic_wait", "output" => "{\"timed_out\":false}"}
      item = plaintext()

      assert {:ok, %{payload: payload}} = coerce([user("synthetic task"), call, output, item])
      assert Enum.map(payload["input"], &(&1["type"] || "message")) == ["message", "function_call", "function_call_output", "agent_message"]
      assert List.last(payload["input"]) == item
    end
  end

  describe "sealed agent_message handoff" do
    for {type, author, recipient} <- [{"NEW_TASK", @lead, @author}, {"MESSAGE", @author, @lead}, {"NEW_TASK", "/morpheus", "/root/child_1"}] do
      test "the exact #{type} handoff from #{author} to #{recipient} is forwarded unchanged" do
        item = sealed(unquote(type), unquote(author), unquote(recipient))

        assert {:ok, %{payload: %{"input" => [_user, forwarded]}}} = coerce(history(item))
        assert forwarded == item
      end
    end

    test "is forwarded without an id and with a passthrough map" do
      item = sealed("NEW_TASK", @lead, @author) |> Map.delete("id") |> Map.put("internal_chat_message_metadata_passthrough", %{"turn_id" => "turn-synthetic-1"})

      assert {:ok, %{payload: %{"input" => [_user, forwarded]}}} = coerce(history(item))
      assert forwarded == item
    end
  end

  describe "refused agent_message shapes" do
    for {label, variant} <- @refused_plaintext_variants do
      test "refuses #{label} before dispatch" do
        assert {:error, @refusal} = coerce(history(mutate(unquote(variant), plaintext())))
      end
    end

    for {label, variant} <- @refused_sealed_variants do
      test "refuses a sealed item with #{label} instead of filtering it" do
        assert {:error, @refusal} = coerce(history(mutate(unquote(variant), sealed("NEW_TASK", @lead, @author))))
      end
    end

    test "refuses the reserved executed_tool_calls passthrough key like on every other item" do
      item = plaintext() |> Map.put("internal_chat_message_metadata_passthrough", %{"executed_tool_calls" => []})

      assert {:error, %{status: 400, code: "invalid_request", param: "input", message: "executed_tool_calls is reserved"}} = coerce(history(item))
    end

    test "still refuses an unknown item type that carries the same content list" do
      item = plaintext() |> Map.put("type", "agent_notification")

      assert {:error, @refusal} = coerce(history(item))
    end
  end

  describe "compatibility matrix" do
    test "states the contract this adapter implements" do
      contract = CompatibilityMatrix.fixture!(:responses_chat).agent_message_history

      assert contract.accepted_items == ["agent_message"]
      assert contract.shapes.sealed_handoff.message_types == ["NEW_TASK", "MESSAGE"]
      assert contract.serving_modes == ["full", "lite"]
      assert Map.delete(contract.refusal, :upstream_dispatch) == Map.take(@refusal, [:status, :code, :param])
      assert contract.refusal.upstream_dispatch == false
    end
  end

  defp coerce(input), do: Responses.coerce(%{"model" => "gpt-fixture-text", "input" => input})

  defp history(item), do: [user("synthetic task"), item]

  defp user(text), do: %{"role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp plaintext do
    %{"type" => "agent_message", "id" => "amsg_synthetic_0001", "author" => @author, "recipient" => @lead, "content" => [%{"type" => "input_text", "text" => @envelope}]}
  end

  defp sealed(type, author, recipient) do
    %{
      "type" => "agent_message",
      "id" => "amsg_synthetic_0002",
      "author" => author,
      "recipient" => recipient,
      "content" => [
        %{"type" => "input_text", "text" => "Message Type: #{type}\nTask name: #{recipient}\nSender: #{author}\nPayload:\n"},
        %{"type" => "encrypted_content", "encrypted_content" => "gAAAAA-synthetic-cipher"}
      ]
    }
  end

  defp mutate(:no_id, item), do: Map.delete(item, "id")
  defp mutate(:null_passthrough, item), do: Map.put(item, "internal_chat_message_metadata_passthrough", nil)
  defp mutate(:passthrough_map, item), do: Map.put(item, "internal_chat_message_metadata_passthrough", %{"turn_id" => "turn-synthetic-1"})
  defp mutate(:several_parts, item), do: Map.put(item, "content", [%{"type" => "input_text", "text" => "first part"}, %{"type" => "input_text", "text" => ""}])
  defp mutate(:status_key, item), do: Map.put(item, "status", "completed")
  defp mutate(:role_key, item), do: Map.put(item, "role", "assistant")
  defp mutate(:phase_key, item), do: Map.put(item, "phase", "commentary")
  defp mutate(:blank_id, item), do: Map.put(item, "id", " ")
  defp mutate(:null_id, item), do: Map.put(item, "id", nil)
  defp mutate(:integer_id, item), do: Map.put(item, "id", 7)
  defp mutate(:no_author, item), do: Map.delete(item, "author")
  defp mutate(:blank_author, item), do: Map.put(item, "author", "")
  defp mutate(:map_recipient, item), do: Map.put(item, "recipient", %{"path" => "/root"})
  defp mutate(:no_recipient, item), do: Map.delete(item, "recipient")
  defp mutate(:no_content, item), do: Map.delete(item, "content")
  defp mutate(:empty_content, item), do: Map.put(item, "content", [])
  defp mutate(:string_content, item), do: Map.put(item, "content", @envelope)
  defp mutate(:null_content, item), do: Map.put(item, "content", nil)
  defp mutate(:map_content, item), do: Map.put(item, "content", %{"type" => "input_text", "text" => @envelope})
  defp mutate(:output_text_part, item), do: Map.put(item, "content", [%{"type" => "output_text", "text" => @envelope}])
  defp mutate(:part_extra_key, item), do: Map.put(item, "content", [%{"type" => "input_text", "text" => @envelope, "annotations" => []}])
  defp mutate(:integer_text, item), do: Map.put(item, "content", [%{"type" => "input_text", "text" => 5}])
  defp mutate(:string_part, item), do: Map.put(item, "content", [@envelope])
  defp mutate(:string_passthrough, item), do: Map.put(item, "internal_chat_message_metadata_passthrough", "turn-synthetic-1")
  defp mutate(:cipher_alone, item), do: Map.put(item, "content", [%{"type" => "encrypted_content", "encrypted_content" => "synthetic-cipher"}])
  defp mutate(:other_recipient, item), do: Map.put(item, "recipient", "/root/other")
  defp mutate(:other_author, item), do: Map.put(item, "author", "/root/other")

  defp mutate(:author_not_agent_path, %{"content" => [envelope, cipher]} = item) do
    text = String.replace(envelope["text"], "Sender: /root\n", "Sender: root\n")
    %{item | "author" => "root", "content" => [%{envelope | "text" => text}, cipher]}
  end

  defp mutate(:arbitrary_envelope, %{"content" => [envelope, cipher]} = item), do: %{item | "content" => [%{envelope | "text" => "Message Type: MESSAGE\nPayload:\n"}, cipher]}
  defp mutate(:blank_cipher, %{"content" => [envelope, cipher]} = item), do: %{item | "content" => [envelope, %{cipher | "encrypted_content" => " "}]}
  defp mutate(:reversed_parts, %{"content" => parts} = item), do: %{item | "content" => Enum.reverse(parts)}
  defp mutate(:third_part, %{"content" => parts} = item), do: %{item | "content" => parts ++ [%{"type" => "input_text", "text" => "tail"}]}
  defp mutate(:cipher_extra_key, %{"content" => [envelope, cipher]} = item), do: %{item | "content" => [envelope, Map.put(cipher, "file_id", "file-synthetic")]}
  defp mutate(:envelope_extra_key, %{"content" => [envelope, cipher]} = item), do: %{item | "content" => [Map.put(envelope, "annotations", []), cipher]}
  defp mutate(:integer_cipher, %{"content" => [envelope, cipher]} = item), do: %{item | "content" => [envelope, %{cipher | "encrypted_content" => 42}]}
end
