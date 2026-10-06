defmodule CodexPooler.Gateway.OpenAICompatibility.Responses.Input.WebSearchCall do
  @moduledoc false

  # The `web_search_call` item a Full-served model emits when it ran the hosted `web_search` tool, replayed by a
  # client in the history of its next stateless request (`id`, `status` and an `action` naming what was searched,
  # opened or looked for). `/v1` returns the item in its output, so an SDK that sends `response.output` back meets it.
  #
  # The provider reads it in a stateless request, in the Full and in the Lite request shape (direct probe on
  # `gpt-6-luna`: a canary carried in the replayed `action.query` came back in the reply). It validates the item
  # strictly, so this adapter mirrors that validation and forwards an admitted item untouched:
  #
  #   * item keys: `type`, `id`, `status`, `action`, `internal_chat_message_metadata_passthrough`; `id` and `status`
  #     are nonblank strings when present (the provider takes any status string; it wants an id that begins with
  #     `ws`, which it names itself and the adapter does not second-guess, except for the id the public stream
  #     invents for an item the upstream sent without one, which `Normalization` drops first);
  #   * `action.type` is one of `search`, `open_page`, `find_in_page`, each with its own keys: `search` takes
  #     `query`, `queries` and `sources` (a list of `{"type": "url", "url": ...}`, which the provider emits when the
  #     request includes `web_search_call.action.sources`), `open_page` takes `url`, `find_in_page` takes `url` and
  #     `pattern`; a key of another variant is refused by the provider and here;
  #   * a null is refused for every field except the passthrough: the provider accepts a null `status` and `query`,
  #     but fails the whole request with an in-stream `internal_error` ("response protection is unavailable") for a
  #     null `queries`, so no null is forwarded.

  alias CodexPooler.Gateway.OpenAICompatibility.Error

  @passthrough_key "internal_chat_message_metadata_passthrough"
  @item_keys ["type", "id", "status", "action", @passthrough_key]
  @action_keys %{
    "search" => ["type", "query", "queries", "sources"],
    "open_page" => ["type", "url"],
    "find_in_page" => ["type", "url", "pattern"]
  }
  @string_action_fields ["query", "url", "pattern"]

  @spec validate_item(term()) :: :ok | {:error, Error.reason()}
  def validate_item(%{"type" => "web_search_call"} = item) do
    with :ok <- exact_keys(item, @item_keys),
         :ok <- optional_nonblank(item, "id"),
         :ok <- optional_nonblank(item, "status"),
         :ok <- optional_action(item),
         :ok <- optional_passthrough(item) do
      :ok
    else
      :error -> {:error, Error.invalid_request("input item shape is not translatable", "input")}
    end
  end

  def validate_item(_item), do: {:error, Error.invalid_request("input item shape is not translatable", "input")}

  defp optional_action(%{"action" => %{"type" => type} = action}) when is_map_key(@action_keys, type) do
    with :ok <- exact_keys(action, Map.fetch!(@action_keys, type)) do
      action_fields(action)
    end
  end

  defp optional_action(%{"action" => _action}), do: :error
  defp optional_action(_item), do: :ok

  defp action_fields(action) do
    if Enum.all?(action, fn {key, value} -> action_field?(key, value) end), do: :ok, else: :error
  end

  defp action_field?("type", _type), do: true
  defp action_field?(key, value) when key in @string_action_fields, do: is_binary(value)
  defp action_field?("queries", value), do: is_list(value) and Enum.all?(value, &is_binary/1)
  defp action_field?("sources", value), do: is_list(value) and Enum.all?(value, &url_source?/1)
  defp action_field?(_key, _value), do: false

  defp url_source?(%{"type" => "url", "url" => url} = source), do: map_size(source) == 2 and is_binary(url)
  defp url_source?(_source), do: false

  defp exact_keys(map, allowed) do
    if Enum.all?(Map.keys(map), &(&1 in allowed)), do: :ok, else: :error
  end

  defp optional_nonblank(item, key) do
    case Map.fetch(item, key) do
      :error -> :ok
      {:ok, value} when is_binary(value) -> if String.trim(value) == "", do: :error, else: :ok
      {:ok, _value} -> :error
    end
  end

  defp optional_passthrough(%{@passthrough_key => nil}), do: :ok
  defp optional_passthrough(%{@passthrough_key => metadata}) when is_map(metadata), do: :ok
  defp optional_passthrough(%{@passthrough_key => _metadata}), do: :error
  defp optional_passthrough(_item), do: :ok
end
