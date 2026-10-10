defmodule CodexPooler.Gateway.Transports.Streaming.StreamProtocol.PublicResponsesTerminalOutput do
  @moduledoc """
  The output a public `/v1/responses` stream delivered, for a terminal the
  provider closed with an empty `output` (findings#335).

  The Codex backend often ends a stream with `response.completed` listing
  `output: []` although `response.output_item.done` carried every item
  (measured on `gpt-6-luna` over HTTP SSE, the bridged upstream websocket and
  the public websocket alike). The OpenAI Responses contract carries the full
  output in the terminal, and the SDK stream helpers take their final response
  from it: openai-node `responses.stream().finalResponse()` replaces the
  snapshot it accumulated from the events with the terminal's response, and
  openai-python `responses.stream()` keeps an empty list as the final output
  (it recovers the done items only when `output` is absent), so their final
  output, and the `output_text` they derive from it, came back empty.

  The public SSE relay and the public websocket record each item a
  `response.output_item.done` event delivered, as the client received it, and
  put them into a `response.completed` or `response.incomplete` whose
  `output` is empty or absent: ordered by `output_index`, the order the SDK
  accumulators build and openai-python's own recovery uses (a later done item
  at the same index replaces the earlier one), or in arrival order when an
  event carries no index. A non-empty terminal output is left as the provider
  sent it, and an item is never taken from `response.output_item.added`, whose
  announcement can differ from the closed item. Frames keep flowing as they
  arrive: only the decoded items are kept, bounded by the largest terminal the
  relay accepts from the provider; past that bound nothing is recorded and the
  terminal is relayed as sent.
  """

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol

  @terminal_types ["response.completed", "response.incomplete"]

  @type entry :: {non_neg_integer() | nil, map()}
  @type state :: %{
          required(:items) => [entry()],
          required(:bytes) => non_neg_integer(),
          required(:overflow?) => boolean()
        }

  @spec new_state() :: state()
  def new_state, do: %{items: [], bytes: 0, overflow?: false}

  @doc """
  Records the item of a relayed `response.output_item.done` event. `bytes` is
  the size of the event as relayed, which bounds what the state keeps.
  """
  @spec observe(state(), map(), non_neg_integer()) :: state()
  @spec observe(state(), map(), non_neg_integer(), pos_integer()) :: state()
  def observe(state, event, bytes, max_bytes \\ StreamProtocol.max_incomplete_terminal_sse_block_bytes())

  def observe(%{overflow?: true} = state, _event, _bytes, _max_bytes), do: state

  def observe(state, %{"item" => %{} = item} = event, bytes, max_bytes)
      when is_integer(bytes) and bytes >= 0 and is_integer(max_bytes) and max_bytes > 0 do
    bytes = state.bytes + bytes

    if bytes > max_bytes,
      do: %{state | items: [], bytes: bytes, overflow?: true},
      else: %{state | items: [{output_index(event), item} | state.items], bytes: bytes}
  end

  def observe(state, _event, _bytes, _max_bytes), do: state

  @doc """
  Puts the recorded items into a `response.completed` or
  `response.incomplete` event whose response `output` is empty or absent;
  returns every other event unchanged.
  """
  @spec fill(String.t() | nil, map(), state()) :: map()
  def fill(type, %{"response" => %{} = response} = event, %{overflow?: false, items: [_ | _]} = state)
      when type in @terminal_types do
    case Map.get(response, "output") do
      [_ | _] -> event
      _empty -> Map.put(event, "response", Map.put(response, "output", items(state)))
    end
  end

  def fill(_type, event, _state), do: event

  defp items(%{items: entries}) do
    entries = Enum.reverse(entries)

    if Enum.all?(entries, fn {index, _item} -> is_integer(index) end) do
      entries
      |> Enum.reduce(%{}, fn {index, item}, by_index -> Map.put(by_index, index, item) end)
      |> Enum.sort_by(fn {index, _item} -> index end)
      |> Enum.map(fn {_index, item} -> item end)
    else
      Enum.map(entries, fn {_index, item} -> item end)
    end
  end

  defp output_index(%{"output_index" => index}) when is_integer(index) and index >= 0, do: index
  defp output_index(_event), do: nil
end
