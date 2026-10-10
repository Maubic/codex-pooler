defmodule CodexPooler.Gateway.Transports.Streaming.StreamProtocol.PublicResponsesToolCompletion do
  @moduledoc false

  @max_index 9_007_199_254_740_991
  @max_tools 4096
  @max_identity_bytes 1024
  @tool_types ["function_call", "custom_tool_call"]
  @payload_types %{
    "response.function_call_arguments.delta" => "function_call",
    "response.function_call_arguments.done" => "function_call",
    "response.custom_tool_call_input.delta" => "custom_tool_call",
    "response.custom_tool_call_input.done" => "custom_tool_call"
  }

  @type reason :: :incomplete_tool_item | :invalid_tool_correlation | :tool_tracking_overflow
  @type identity :: {:item | :call, String.t()}
  @type tool :: %{kind: String.t(), item_id: String.t() | nil, call_id: String.t() | nil, done?: boolean()}
  @type state :: %{
          tools: %{non_neg_integer() => tool()},
          aliases: %{identity() => non_neg_integer()},
          reason: reason() | nil,
          terminal_latched?: boolean()
        }

  @spec new_state() :: state()
  def new_state, do: %{tools: %{}, aliases: %{}, reason: nil, terminal_latched?: false}

  @spec observe(state(), term()) :: state()
  def observe(%{terminal_latched?: true} = state, _event), do: state
  def observe(%{reason: reason} = state, _event) when not is_nil(reason), do: state

  def observe(state, %{"type" => type} = event) do
    cond do
      type in ["response.completed", "response.done", "response.failed", "response.incomplete", "error"] ->
        %{state | terminal_latched?: true}

      type in ["response.output_item.added", "response.output_item.done"] ->
        observe_item(state, event, type)

      Map.has_key?(@payload_types, type) ->
        observe_tool(state, event, Map.fetch!(@payload_types, type), :payload)

      true ->
        state
    end
  end

  def observe(state, _event), do: state

  defp observe_item(state, event, type) do
    item = Map.get(event, "item")
    kind = if is_map(item), do: Map.get(item, "type")
    phase = if type == "response.output_item.added", do: :add, else: :done

    cond do
      kind in @tool_types ->
        observe_tool(state, event, kind, phase)

      tracked_reference?(state, event) ->
        if kind in ["function_call_output", "custom_tool_call_output"],
          do: observe_output(state, event),
          else: observe_tool(state, event, kind, phase)

      true ->
        state
    end
  end

  defp observe_output(state, event) do
    with {:ok, item_id, _call_id} <- source_identity(event),
         index when is_integer(index) and index >= 0 and index <= @max_index <- Map.get(event, "output_index"),
         false <- Map.has_key?(state.tools, index) or alias_bound?(state, item_id, nil) do
      state
    else
      {:error, reason} -> %{state | reason: reason}
      _invalid -> %{state | reason: :invalid_tool_correlation}
    end
  end

  @spec completion_verdict(state()) :: :ok | {:error, reason()}
  def completion_verdict(%{reason: reason}) when not is_nil(reason), do: {:error, reason}

  def completion_verdict(state) do
    if Enum.any?(state.tools, fn {_index, tool} -> not tool.done? end),
      do: {:error, :incomplete_tool_item},
      else: :ok
  end

  defp observe_tool(state, event, kind, phase) do
    with {:ok, item_id, call_id} <- source_identity(event),
         index when is_integer(index) and index >= 0 and index <= @max_index <- Map.get(event, "output_index"),
         true <- kind in @tool_types do
      correlate(state, event, kind, phase, index, item_id, call_id)
    else
      {:error, reason} -> %{state | reason: reason}
      _invalid -> %{state | reason: :invalid_tool_correlation}
    end
  end

  defp correlate(state, _event, kind, :add, index, item_id, call_id) do
    cond do
      Map.has_key?(state.tools, index) or alias_bound?(state, item_id, call_id) ->
        %{state | reason: :invalid_tool_correlation}

      map_size(state.tools) == @max_tools ->
        %{state | reason: :tool_tracking_overflow}

      true ->
        bind(state, index, %{kind: kind, item_id: item_id, call_id: call_id, done?: false})
    end
  end

  defp correlate(state, event, kind, phase, index, item_id, call_id) do
    case Map.get(state.tools, index) do
      %{kind: ^kind, done?: false} = tool ->
        if matching_identity?(state, tool, index, item_id, call_id, phase) and valid_done_status?(event, phase) do
          tool = %{tool | item_id: tool.item_id || item_id, call_id: tool.call_id || call_id, done?: phase == :done}
          bind(state, index, tool)
        else
          %{state | reason: :invalid_tool_correlation}
        end

      _invalid ->
        %{state | reason: :invalid_tool_correlation}
    end
  end

  defp matching_identity?(state, tool, index, item_id, call_id, phase) do
    # A new item alias is legitimate only when the existing call alias proves it.
    item_matches?(tool, item_id, call_id, phase) and compatible_alias?(tool.call_id, call_id) and known_alias?(tool, item_id, call_id) and
      Enum.all?(aliases(item_id, call_id), fn alias -> Map.get(state.aliases, alias, index) == index end)
  end

  defp item_matches?(_tool, nil, _call_id, _phase), do: true
  defp item_matches?(%{item_id: item_id}, item_id, _call_id, _phase), do: true
  defp item_matches?(%{item_id: nil, call_id: call_id}, _item_id, call_id, :done) when not is_nil(call_id), do: true
  defp item_matches?(_tool, _item_id, _call_id, _phase), do: false

  defp compatible_alias?(nil, _alias), do: true
  defp compatible_alias?(_alias, nil), do: true
  defp compatible_alias?(alias, alias), do: true
  defp compatible_alias?(_existing, _incoming), do: false

  defp known_alias?(tool, item_id, call_id),
    do: (not is_nil(item_id) and item_id == tool.item_id) or (not is_nil(call_id) and call_id == tool.call_id)

  defp valid_done_status?(_event, :payload), do: true

  # A done item completes its call only with `status` "completed" or no
  # status. Any other value leaves the call unfinished: the callers turn a
  # `response.completed` after it into the synthetic failure, while a
  # provider `response.incomplete` or `response.failed` keeps its outcome.
  # The status says only whether the model finished writing the call. The
  # client runs function and custom tools, so the provider schema gives these
  # items `in_progress`, `completed` and `incomplete`, and no `failed`. A tool
  # the client fails or cancels, such as a Codex dynamic tool settled as its
  # own failed item, reaches the provider as the next request's
  # `function_call_output` or `custom_tool_call_output`, never as a frame of
  # this stream, so no unsuccessful status is admitted (findings#338).
  defp valid_done_status?(%{"item" => item}, :done) when is_map(item),
    do: not Map.has_key?(item, "status") or Map.get(item, "status") == "completed"

  defp valid_done_status?(_event, :done), do: false

  defp bind(state, index, tool) do
    aliases = Enum.reduce(aliases(tool.item_id, tool.call_id), state.aliases, &Map.put(&2, &1, index))
    %{state | tools: Map.put(state.tools, index, tool), aliases: aliases}
  end

  defp aliases(item_id, call_id) do
    Enum.reject([if(item_id, do: {:item, item_id}), if(call_id, do: {:call, call_id})], &is_nil/1)
  end

  defp alias_bound?(state, item_id, call_id),
    do: Enum.any?(aliases(item_id, call_id), &Map.has_key?(state.aliases, &1))

  defp tracked_reference?(state, event) do
    item =
      case Map.get(event, "item") do
        %{} = item -> item
        _other -> %{}
      end

    Map.has_key?(state.tools, Map.get(event, "output_index")) or
      Enum.any?([{:item, Map.get(item, "id")}, {:item, Map.get(event, "item_id")}, {:call, Map.get(item, "call_id")}, {:call, Map.get(event, "call_id")}], &Map.has_key?(state.aliases, &1))
  end

  defp source_identity(event) do
    item =
      case Map.get(event, "item") do
        %{} = item -> item
        _other -> %{}
      end

    with {:ok, nested_id} <- identity_field(item, "id"),
         {:ok, top_id} <- identity_field(event, "item_id"),
         {:ok, nested_call} <- identity_field(item, "call_id"),
         {:ok, top_call} <- identity_field(event, "call_id"),
         true <- compatible_alias?(nested_id, top_id),
         true <- compatible_alias?(nested_call, top_call),
         item_id = nested_id || top_id,
         call_id = nested_call || top_call,
         true <- not is_nil(item_id) or not is_nil(call_id) do
      {:ok, item_id, call_id}
    else
      {:error, reason} -> {:error, reason}
      _invalid -> {:error, :invalid_tool_correlation}
    end
  end

  defp identity_field(map, key) do
    case Map.fetch(map, key) do
      :error ->
        {:ok, nil}

      {:ok, value} when is_binary(value) and byte_size(value) > @max_identity_bytes ->
        {:error, :tool_tracking_overflow}

      {:ok, value} when is_binary(value) ->
        if String.valid?(value) and String.trim(value) != "", do: {:ok, value}, else: {:error, :invalid_tool_correlation}

      _invalid ->
        {:error, :invalid_tool_correlation}
    end
  end
end
