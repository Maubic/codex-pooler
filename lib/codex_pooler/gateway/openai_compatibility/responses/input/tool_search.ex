defmodule CodexPooler.Gateway.OpenAICompatibility.Responses.Input.ToolSearch do
  @moduledoc false

  # The `tool_search_call` and `tool_search_output` items of a tool search (findings#313), replayed by a client in the
  # history of its next stateless request. A Full-served model running a server-executed search emits both (the call
  # names the searched `paths` in `arguments`, the output carries the loaded deferred `tools`) beside the
  # `function_call` it then makes; a client-executed search (`execution: client`) emits only the call, and the
  # client answers it with a `tool_search_output` of its own (the AI SDK sends that one without an `id`). `/v1`
  # returns the provider's items in its output, so an SDK that sends `response.output` back meets them.
  #
  # The provider reads them in a stateless request, in the Full and in the Lite request shape (direct probe on
  # `gpt-6-luna`: a canary in the function output that follows them came back, and a client-executed output made the
  # model call the loaded function). It validates them strictly, so this adapter mirrors that validation and
  # forwards an admitted item untouched:
  #
  #   * item keys: `type`, `id`, `call_id`, `execution`, `status`, plus `arguments` on the call and `tools` on the
  #     output; any other key is refused (`unknown_parameter` at the provider);
  #   * `arguments` is required and an object (a JSON string is `invalid_type`), `tools` is required and a list;
  #   * `id`, `call_id` and `status` may be absent or null; `id` and `call_id` are otherwise nonblank strings (the
  #     provider wants an id that begins with `tsc` or `tso`, which it names itself and the adapter does not
  #     second-guess), `status` is `in_progress`, `completed` or `incomplete` and `execution` is `server` or `client`
  #     (other values are `invalid_value`);
  #   * the loaded `tools` are a client's tool definitions, so they follow the rule of a client-sent `additional_tools`
  #     manifest: a remote MCP tool is refused, every other definition is left to the provider.

  alias CodexPooler.Gateway.OpenAICompatibility.Error

  @common_keys ["type", "id", "call_id", "execution", "status"]
  @item_keys %{"tool_search_call" => ["arguments" | @common_keys], "tool_search_output" => ["tools" | @common_keys]}
  @executions ["server", "client"]
  @statuses ["in_progress", "completed", "incomplete"]

  @spec validate_item(term()) :: :ok | {:error, Error.reason()}
  def validate_item(%{"type" => type} = item) when is_map_key(@item_keys, type) do
    with :ok <- exact_keys(item, Map.fetch!(@item_keys, type)),
         :ok <- optional_nonblank(item, "id"),
         :ok <- optional_nonblank(item, "call_id"),
         :ok <- optional_member(item, "execution", @executions, false),
         :ok <- optional_member(item, "status", @statuses, true) do
      payload(item)
    else
      :error -> shape_error()
    end
  end

  def validate_item(_item), do: shape_error()

  defp payload(%{"type" => "tool_search_call", "arguments" => arguments}) when is_map(arguments), do: :ok
  defp payload(%{"type" => "tool_search_output", "tools" => tools}) when is_list(tools), do: loaded_tools(tools)
  defp payload(_item), do: shape_error()

  defp loaded_tools(tools) do
    cond do
      not Enum.all?(tools, &is_map/1) -> shape_error()
      Enum.any?(tools, &(Map.get(&1, "type") == "mcp")) -> {:error, Error.invalid_request("remote MCP tools are not supported", "input")}
      true -> :ok
    end
  end

  defp exact_keys(map, allowed) do
    if Enum.all?(Map.keys(map), &(&1 in allowed)), do: :ok, else: :error
  end

  defp optional_nonblank(item, key) do
    case Map.fetch(item, key) do
      :error -> :ok
      {:ok, nil} -> :ok
      {:ok, value} when is_binary(value) -> if String.trim(value) == "", do: :error, else: :ok
      {:ok, _value} -> :error
    end
  end

  defp optional_member(item, key, allowed, nullable?) do
    case Map.fetch(item, key) do
      :error -> :ok
      {:ok, nil} -> if nullable?, do: :ok, else: :error
      {:ok, value} -> if value in allowed, do: :ok, else: :error
    end
  end

  defp shape_error, do: {:error, Error.invalid_request("input item shape is not translatable", "input")}
end
