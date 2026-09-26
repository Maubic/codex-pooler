defmodule CodexPooler.Accounting.NativeHttpToolObservation do
  @moduledoc false

  # Only a single incomplete client-side tool is admitted. Completed items,
  # provider-side tools and unrecognised events all destroy this authority.
  # Item ids and indices are transient matching state and never enter metadata.
  # Non-completing reasoning can precede the last (incomplete) tool item.
  defstruct item_id: nil, output_index: nil, reasoning_item_id: nil, reasoning_index: -1, call_type: nil, input_done?: false, poisoned?: false

  @type t :: %__MODULE__{
          item_id: String.t() | nil,
          output_index: non_neg_integer() | nil,
          reasoning_item_id: String.t() | nil,
          reasoning_index: integer(),
          call_type: String.t() | nil,
          input_done?: boolean(),
          poisoned?: boolean()
        }

  defguardp valid_item_id(id) when is_binary(id) and byte_size(id) in 1..1024

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec poison(t()) :: t()
  def poison(%__MODULE__{} = observation), do: %{observation | poisoned?: true}

  @spec observe(t(), String.t() | nil, term()) :: t()
  def observe(%__MODULE__{poisoned?: true} = observation, _label, _decoded), do: observation

  def observe(observation, label, %{"type" => type} = decoded) when label == type,
    do: observe_event(observation, type, decoded)

  def observe(observation, _label, _decoded), do: poison(observation)

  @spec metadata(t(), boolean()) :: map()
  def metadata(%__MODULE__{} = observation, parser_complete?) do
    %{
      "version" => 1,
      "parser_complete" => parser_complete?,
      "poisoned" => observation.poisoned?,
      "partial_tool" => observation.call_type,
      "input_done" => observation.input_done?
    }
  end

  @spec eligible_metadata?(term()) :: boolean()
  def eligible_metadata?(%{"version" => 1, "parser_complete" => true, "poisoned" => false, "partial_tool" => type}),
    do: type in ["custom_tool_call", "function_call"]

  def eligible_metadata?(_metadata), do: false

  defp observe_event(observation, type, %{"response" => %{} = response})
       when type in ["response.created", "response.in_progress", "response.queued"] do
    if Map.get(response, "output", []) == [] and Map.get(response, "status") in [nil, "queued", "in_progress"],
      do: observation,
      else: poison(observation)
  end

  defp observe_event(observation, type, _decoded) when type in ["codex.rate_limits", "codex.response.metadata"],
    do: observation

  defp observe_event(%__MODULE__{item_id: nil, reasoning_index: previous} = observation, "response.output_item.added", %{
         "output_index" => index,
         "item" => %{"id" => id, "type" => "reasoning"} = item
       })
       when is_integer(index) and index > previous and valid_item_id(id) do
    if Map.get(item, "status") in [nil, "in_progress"],
      do: %{observation | reasoning_item_id: id, reasoning_index: index},
      else: poison(observation)
  end

  defp observe_event(
         %__MODULE__{reasoning_item_id: id, reasoning_index: index} = observation,
         type,
         %{
           "item_id" => id,
           "output_index" => index
         } = event
       )
       when is_binary(id) and
              type in [
                "response.reasoning_summary_text.delta",
                "response.reasoning_summary_text.done",
                "response.reasoning_text.delta",
                "response.reasoning_text.done",
                "response.reasoning_summary_part.added",
                "response.reasoning_summary_part.done"
              ] do
    if valid_reasoning_event?(type, event), do: observation, else: poison(observation)
  end

  defp observe_event(%__MODULE__{item_id: nil, reasoning_index: previous} = observation, "response.output_item.added", %{
         "output_index" => index,
         "item" => %{"id" => id, "type" => type, "call_id" => call_id, "name" => name} = item
       })
       when type in ["custom_tool_call", "function_call"] and is_integer(index) and index > previous and valid_item_id(id) and
              is_binary(call_id) and byte_size(call_id) > 0 and is_binary(name) and byte_size(name) > 0 do
    if Map.get(item, "status") in [nil, "in_progress"],
      do: %{observation | item_id: id, output_index: index, call_type: type},
      else: poison(observation)
  end

  defp observe_event(%__MODULE__{item_id: id, output_index: index, call_type: call_type, input_done?: false} = observation, type, %{
         "item_id" => id,
         "output_index" => index,
         "delta" => delta
       })
       when is_binary(id) and is_binary(delta) do
    if {call_type, type} in [
         {"custom_tool_call", "response.custom_tool_call_input.delta"},
         {"function_call", "response.function_call_arguments.delta"}
       ],
       do: observation,
       else: poison(observation)
  end

  defp observe_event(%__MODULE__{item_id: id, output_index: index, call_type: "custom_tool_call", input_done?: false} = observation, "response.custom_tool_call_input.done", %{"item_id" => id, "output_index" => index, "input" => input})
       when is_binary(id) and is_binary(input),
       do: %{observation | input_done?: true}

  defp observe_event(%__MODULE__{item_id: id, output_index: index, call_type: "function_call", input_done?: false} = observation, "response.function_call_arguments.done", %{"item_id" => id, "output_index" => index, "arguments" => arguments})
       when is_binary(id) and is_binary(arguments),
       do: %{observation | input_done?: true}

  defp observe_event(observation, _type, _decoded), do: poison(observation)

  defp valid_reasoning_event?(type, event) do
    index_key = if String.starts_with?(type, "response.reasoning_summary_"), do: "summary_index", else: "content_index"
    index = Map.get(event, index_key)
    is_integer(index) and index >= 0 and valid_reasoning_content?(type, event)
  end

  defp valid_reasoning_content?(type, event) do
    cond do
      String.ends_with?(type, ".delta") -> is_binary(Map.get(event, "delta"))
      type in ["response.reasoning_summary_part.added", "response.reasoning_summary_part.done"] -> match?(%{"type" => "summary_text", "text" => text} when is_binary(text), Map.get(event, "part"))
      true -> is_binary(Map.get(event, "text"))
    end
  end
end
