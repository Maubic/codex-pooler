defmodule CodexPooler.Gateway.Runtime.Streaming.StreamTiming do
  @moduledoc false

  # Bounded, metadata-only timing of an upstream HTTP SSE attempt, so a slow request can be placed from its row:
  # which wait was the provider's (before its headers, between its headers and its first event, before its first
  # output) and which was ours. Every value is an integer in milliseconds from the attempt start, the origin of
  # `latency_ms` (`SelectedCandidateContext.started`), or a word of a fixed vocabulary:
  #
  #   * `headers_ms`: the upstream's response headers arrived (`attach/2`, in `UpstreamAttempt.dispatch_http/2`);
  #   * `first_event_ms`: the first complete SSE event of the body arrived;
  #   * `first_visible_ms`: the first model output arrived, an output item or content part being opened, a delta or a
  #     completed item (`DeliveryReceipt.frame_class/1`): never a lifecycle event, `response.metadata` or a terminal;
  #   * `connection`: `fresh` when the request opened a connection, `reused` when it took a pooled one
  #     (`UpstreamConnectionProbe`); absent when it could not be observed.
  #
  # A mark that did not happen is absent, so `headers_ms` alone is a provider that sent its headers and then nothing.
  # The serving node is not a field: `attempts.owner_instance_id` already names it.
  #
  # `Finalization.Streaming` replaces the attempt's response metadata wholesale, so the timing lives in the relay
  # state and reaches the row as one `stream_timing` map through `Metadata.merge_stream_state_metadata/2`; a
  # first-event failure never reaches a relay state at finalization, so it carries the same map on its failure
  # (`attach_to_failure/2`). Nothing from the wire is retained beyond a bounded SSE block residue, dropped as soon as
  # the first output was seen.

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.UpstreamConnectionProbe
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt

  @metadata_key "stream_timing"
  @private_key :codex_pooler_stream_timing
  @state_key :stream_timing
  @output_frame_classes ~w(item_added part_added delta item_done)
  @mark_fields [headers_ms: "headers_ms", first_event_ms: "first_event_ms", first_visible_ms: "first_visible_ms"]

  @spec metadata_key() :: String.t()
  def metadata_key, do: @metadata_key

  @spec connections() :: [String.t()]
  def connections, do: UpstreamConnectionProbe.connections()

  @doc """
  Stamps the headers mark and the connection class on the response of an upstream HTTP stream, `started` being the
  attempt's `SelectedCandidateContext.started`. Any other response (a collected body, a websocket bridge) is returned
  as it is.
  """
  @spec attach(Req.Response.t(), integer()) :: Req.Response.t()
  def attach(%Req.Response{body: %Req.Response.Async{}} = response, started) when is_integer(started) do
    timing = %{started: started, headers_ms: elapsed_ms(started), connection: UpstreamConnectionProbe.connection(response)}
    Req.Response.put_private(response, @private_key, timing)
  end

  def attach(response, _started), do: response

  @doc "Starts the relay state's timing from what `attach/2` stamped on the response; a state without it stays without."
  @spec init_state(map(), Req.Response.t()) :: map()
  def init_state(state, %Req.Response{} = response) when is_map(state) do
    case Req.Response.get_private(response, @private_key) do
      %{started: started} = timing when is_integer(started) ->
        Map.put(state, @state_key, Map.merge(timing, %{first_event_ms: nil, first_visible_ms: nil, sse: StreamProtocol.new_sse_block_state()}))

      _unstamped ->
        state
    end
  end

  @doc """
  Reads one upstream body chunk. Until the first model output was seen it keeps the incomplete tail of the SSE stream
  (bounded) so an event split across chunks is read when it completes; afterwards it does nothing.
  """
  @spec observe_chunk(map(), term()) :: map()
  def observe_chunk(%{@state_key => %{sse: nil}} = state, _data), do: state

  def observe_chunk(%{@state_key => %{sse: sse, started: started} = timing} = state, data) when is_binary(data) do
    {blocks, sse} = StreamProtocol.complete_sse_blocks(sse, data, bounded?: true)
    %{state | @state_key => observe_blocks(%{timing | sse: sse}, blocks, elapsed_ms(started))}
  end

  def observe_chunk(state, _data), do: state

  @doc "The `stream_timing` metadata of a relay state, bounded to the fixed fields; empty when nothing was timed."
  @spec metadata(term()) :: map()
  def metadata(%{@state_key => %{} = timing}), do: fields_metadata(timing)
  def metadata(_state), do: %{}

  @doc "Carries the timing of the attempt on a first-event failure, which finalizes without the relay state."
  @spec attach_to_failure(map(), term()) :: map()
  def attach_to_failure(failure, state) when is_map(failure) do
    case metadata(state) do
      %{@metadata_key => fields} -> Map.put(failure, @state_key, fields)
      _untimed -> failure
    end
  end

  @doc "The `stream_timing` metadata a failure carries, validated again; empty when it carries none."
  @spec failure_metadata(term()) :: map()
  def failure_metadata(%{@state_key => %{} = fields}) do
    fields_metadata(%{
      headers_ms: Map.get(fields, "headers_ms"),
      first_event_ms: Map.get(fields, "first_event_ms"),
      first_visible_ms: Map.get(fields, "first_visible_ms"),
      connection: Map.get(fields, "connection")
    })
  end

  def failure_metadata(_failure), do: %{}

  defp observe_blocks(timing, [], _elapsed), do: timing

  defp observe_blocks(timing, blocks, elapsed) do
    timing = if is_nil(timing.first_event_ms), do: %{timing | first_event_ms: elapsed}, else: timing

    if Enum.any?(blocks, &model_output_block?/1),
      do: %{timing | first_visible_ms: elapsed, sse: nil},
      else: timing
  end

  defp model_output_block?(block), do: DeliveryReceipt.frame_class(block <> "\n\n") in @output_frame_classes

  defp fields_metadata(timing) do
    fields =
      for {key, name} <- @mark_fields, mark = Map.get(timing, key), is_integer(mark) and mark >= 0, into: %{}, do: {name, mark}

    fields =
      case Map.get(timing, :connection) do
        connection when is_binary(connection) -> if connection in UpstreamConnectionProbe.connections(), do: Map.put(fields, "connection", connection), else: fields
        _unobserved -> fields
      end

    if fields == %{}, do: %{}, else: %{@metadata_key => fields}
  end

  defp elapsed_ms(started), do: max(System.monotonic_time(:millisecond) - started, 0)
end
