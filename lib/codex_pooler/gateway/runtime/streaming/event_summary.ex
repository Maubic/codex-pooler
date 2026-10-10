defmodule CodexPooler.Gateway.Runtime.Streaming.EventSummary do
  @moduledoc false

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.ErrorCanonicalization
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.SSEParser

  @spec from_complete_block(binary()) :: map()
  def from_complete_block(block) when is_binary(block),
    do: ErrorCanonicalization.event_summary_from_block(block)

  @spec from_direct_candidate(binary()) :: {:ok, map()} | :incomplete
  def from_direct_candidate(buffer) when is_binary(buffer) do
    # Retry classification needs the provider data, not an incomplete label.
    # Shared EOF diagnostics retain their existing coarse event-label behavior.
    payload = SSEParser.sse_field(buffer, "data") || buffer

    case CodexPooler.JSON.decode(payload) do
      {:ok, %{} = _decoded} -> StreamProtocol.first_complete_event(buffer)
      _partial_or_invalid -> :incomplete
    end
  end
end
