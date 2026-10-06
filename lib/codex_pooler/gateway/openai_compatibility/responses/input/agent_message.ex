defmodule CodexPooler.Gateway.OpenAICompatibility.Responses.Input.AgentMessage do
  @moduledoc false

  # The `agent_message` input item a Codex multi-agent v2 client replays in its
  # history: a mailbox message one agent delivered to another (a subagent's
  # `FINAL_ANSWER` handed back to its lead, or the lead's sealed `NEW_TASK` /
  # `MESSAGE` handed to a subagent). Codex serializes it as `author`,
  # `recipient` and a `content` list of `input_text` and `encrypted_content`
  # parts, with an optional `amsg_` id and an optional
  # `internal_chat_message_metadata_passthrough`.
  #
  # The provider reads both forms in a stateless request (no
  # `previous_response_id`), in Full and in Lite shape, with and without the id
  # and the passthrough (direct probe on `gpt-6-luna`, 2026-10-06: a canary
  # carried in the plaintext message and the sealed task's own answer both came
  # back in the reply). The native backend route forwards the same items, so the
  # public adapter admits exactly the shapes that route recognizes and forwards
  # them untouched:
  #
  #   * plaintext: a nonempty `content` of exact `input_text` parts only;
  #   * sealed: the exact two-part handoff `ContinuityPayload.v2_encrypted_handoff?/1`
  #     recognizes (envelope `input_text` naming the recipient and author, then
  #     one nonblank `encrypted_content`), which the native websocket route also
  #     keeps while it filters every other encrypted `agent_message`.
  #
  # Any other shape is refused here instead of being filtered silently, so the
  # upstream never receives a history that differs from the one the client sent.

  alias CodexPooler.Gateway.OpenAICompatibility.Error
  alias CodexPooler.Gateway.Payloads.ContinuityPayload

  @passthrough_key "internal_chat_message_metadata_passthrough"
  @item_keys ["type", "id", "author", "recipient", "content", @passthrough_key]

  @spec validate_item(term()) :: :ok | {:error, Error.reason()}
  def validate_item(%{"type" => "agent_message"} = item) do
    with :ok <- exact_keys(item, @item_keys),
         :ok <- optional_id(item),
         :ok <- nonblank(Map.get(item, "author")),
         :ok <- nonblank(Map.get(item, "recipient")),
         :ok <- optional_passthrough(item),
         :ok <- content(item) do
      :ok
    else
      :error -> {:error, Error.invalid_request("input item shape is not translatable", "input")}
    end
  end

  def validate_item(_item), do: {:error, Error.invalid_request("input item shape is not translatable", "input")}

  defp content(%{"content" => [_part | _rest] = parts} = item) do
    if Enum.all?(parts, &plaintext_part?/1) or sealed_handoff?(item, parts), do: :ok, else: :error
  end

  defp content(_item), do: :error

  defp plaintext_part?(%{"type" => "input_text", "text" => text} = part), do: map_size(part) == 2 and is_binary(text)
  defp plaintext_part?(_part), do: false

  defp sealed_handoff?(item, [%{"type" => "input_text"} = envelope, %{"type" => "encrypted_content"} = cipher]),
    do: map_size(envelope) == 2 and map_size(cipher) == 2 and ContinuityPayload.v2_encrypted_handoff?(item)

  defp sealed_handoff?(_item, _parts), do: false

  defp exact_keys(item, allowed) do
    if Enum.all?(Map.keys(item), &(&1 in allowed)), do: :ok, else: :error
  end

  defp optional_id(%{"id" => id}), do: nonblank(id)
  defp optional_id(_item), do: :ok

  defp optional_passthrough(%{@passthrough_key => nil}), do: :ok
  defp optional_passthrough(%{@passthrough_key => metadata}) when is_map(metadata), do: :ok
  defp optional_passthrough(%{@passthrough_key => _metadata}), do: :error
  defp optional_passthrough(_item), do: :ok

  defp nonblank(value) when is_binary(value), do: if(String.trim(value) == "", do: :error, else: :ok)
  defp nonblank(_value), do: :error
end
