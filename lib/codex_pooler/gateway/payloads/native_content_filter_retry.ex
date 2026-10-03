defmodule CodexPooler.Gateway.Payloads.NativeContentFilterRetry do
  @moduledoc false

  alias CodexPooler.Accounting.ClientRetry
  alias CodexPooler.Accounting.ClientRetry.OriginalWitness
  alias CodexPooler.Gateway.Payloads.{NativeTurnContinuation, RequestOptions, WebsocketTurnIdentity}

  @max_edges 16
  @max_items 16
  @max_guidance_bytes 65_536
  @lite_marker "ws_request_header_x_openai_internal_codex_responses_lite"

  @spec attach(OriginalWitness.t(), <<_::256>>, map(), RequestOptions.t()) :: OriginalWitness.t()
  def attach(%OriginalWitness{} = witness, semantic_key, payload, options) do
    original =
      case WebsocketTurnIdentity.replay_claim_digest(semantic_key, payload) do
        {:ok, digest} -> digest
        _invalid -> nil
      end

    %{witness | content_filter_original: original, content_filter: candidates(semantic_key, payload, options)}
  end

  # Candidates are untrusted shapes. Only a finalized, delivered predecessor
  # can redeem one. Every prefix preserves the original options and input.
  defp candidates(key, %{"input" => input} = payload, options) when is_list(input) do
    with nil <- Map.get(payload, "previous_response_id"),
         "turn" <- NativeTurnContinuation.request_kind(payload, options),
         true <- guidance?(List.last(input)),
         positions when length(positions) in 1..@max_edges <- guidance_positions(input) do
      Enum.flat_map(positions, &candidates_at(key, payload, options, &1))
    else
      _invalid -> []
    end
  end

  defp candidates(_key, _payload, _options), do: []

  defp candidates_at(key, payload, options, ending) do
    prefix = Enum.take(payload["input"], ending)
    output_count = prefix |> Enum.reverse() |> Enum.take(@max_items) |> Enum.take_while(&retained_output?/1) |> length()
    Enum.flat_map(0..output_count, &candidate(key, payload, options, ending, &1))
  end

  defp candidate(key, payload, options, ending, count) do
    input = payload["input"]
    prefix_count = ending - count
    output = Enum.slice(input, prefix_count, count)

    with {:ok, items} <- item_digests(output),
         {:ok, before} <- witnesses(key, Map.put(payload, "input", Enum.take(input, prefix_count))),
         {:ok, after_guidance} <- witnesses(key, Map.put(payload, "input", Enum.take(input, ending + 1))) do
      [%{prefix: before, ending: after_guidance, current?: ending + 1 == length(input), items: items, http_progress: progress_candidates(output), serving_mode: candidate_mode(payload, options)}]
    else
      _invalid -> []
    end
  end

  defp candidate_mode(payload, options) do
    if get_in(payload, ["client_metadata", @lite_marker]) == "true", do: "lite", else: RequestOptions.model_serving_mode(options)
  end

  defp guidance_positions(input), do: input |> Enum.with_index() |> Enum.flat_map(fn {item, index} -> if guidance?(item), do: [index], else: [] end)

  defp guidance?(%{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text} = part]} = item)
       when map_size(part) == 2 and is_binary(text) and byte_size(text) <= @max_guidance_bytes do
    valid_fields = Map.drop(item, ["type", "role", "content", "id", "internal_chat_message_metadata_passthrough"]) == %{} and guidance_id?(Map.get(item, "id")) and guidance_metadata?(Map.get(item, "internal_chat_message_metadata_passthrough"))

    valid_fields and
      case text do
        "<content_filter_guidance>\n" <> rest ->
          String.ends_with?(rest, "\n</content_filter_guidance>") and
            not String.contains?(String.replace_suffix(rest, "\n</content_filter_guidance>", ""), ["<content_filter_guidance>", "</content_filter_guidance>"])

        _other ->
          false
      end
  end

  defp guidance?(_item), do: false
  defp guidance_id?(nil), do: true
  defp guidance_id?("msg_" <> rest) when byte_size(rest) in 1..256, do: Regex.match?(~r/\A[A-Za-z0-9_-]+\z/, rest)
  defp guidance_id?(_id), do: false
  defp guidance_metadata?(nil), do: true

  defp guidance_metadata?(%{} = metadata) do
    Map.drop(metadata, ["turn_id", "create_time", "content_item_kinds"]) == %{} and
      (is_nil(metadata["turn_id"]) or (is_binary(metadata["turn_id"]) and byte_size(metadata["turn_id"]) in 1..256)) and
      (is_nil(metadata["create_time"]) or is_number(metadata["create_time"])) and
      metadata["content_item_kinds"] in [nil, ["generic.content_filter_guidance"]]
  end

  defp guidance_metadata?(_metadata), do: false
  defp retained_output?(%{"type" => "reasoning"}), do: true
  defp retained_output?(%{"type" => "message", "role" => "assistant"}), do: true
  defp retained_output?(_item), do: false

  defp witnesses(key, payload) do
    frame = Map.put(payload, "type", "response.create")
    metadata = Map.get(frame, "client_metadata")
    marked = if is_map(metadata) or is_nil(metadata), do: Map.put(frame, "client_metadata", Map.put(metadata || %{}, @lite_marker, "true")), else: frame

    Enum.reduce_while(Enum.uniq([payload, frame, marked]), {:ok, []}, fn variant, {:ok, acc} ->
      case WebsocketTurnIdentity.replay_claim_digest(key, variant) do
        {:ok, digest} -> {:cont, {:ok, [digest | acc]}}
        _invalid -> {:halt, :error}
      end
    end)
  end

  defp item_digests(items) do
    Enum.reduce_while(items, {:ok, []}, fn item, {:ok, acc} ->
      case WebsocketTurnIdentity.completed_item_digest(item) do
        {:ok, digest} -> {:cont, {:ok, acc ++ [digest]}}
        _invalid -> {:halt, :error}
      end
    end)
  end

  defp progress_candidates([]), do: [ClientRetry.native_http_progress_metadata(ClientRetry.new_native_http_progress())]
  defp progress_candidates(items), do: ClientRetry.native_http_mailbox_progress_candidates(items)
end
