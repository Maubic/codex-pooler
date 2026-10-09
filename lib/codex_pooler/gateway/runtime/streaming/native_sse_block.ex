defmodule CodexPooler.Gateway.Runtime.Streaming.NativeSSEBlock do
  @moduledoc false

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.ErrorCanonicalization
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.EventSummary

  # This value belongs to one normalization call, never to relay/parser state.
  # Classification uses the normalized label and whole-block JSON fallback;
  # delivery uses the raw SSE label and only the data field, as before.
  @type t :: %__MODULE__{
          raw: binary(),
          separator: binary(),
          event_type: String.t() | nil,
          decoded: map(),
          direct_failure?: boolean(),
          delivery_event: %{event_type: String.t() | nil, data_type: String.t() | nil}
        }
  defstruct [:raw, :event_type, :decoded, :delivery_event, separator: "\n\n", direct_failure?: false]

  @type delivery :: %{preamble: binary(), data: binary(), commits?: boolean()}

  @spec parse(binary()) :: t()
  @spec parse(binary(), binary()) :: t()
  def parse(raw, separator \\ "\n\n") do
    raw = if :binary.match(raw, "\r") == :nomatch, do: raw, else: raw |> :binary.replace("\r\n", "\n", [:global]) |> :binary.replace("\r", "\n", [:global])
    separator = if separator == "", do: "", else: "\n\n"
    label = StreamProtocol.sse_field(raw, "event")
    data = StreamProtocol.sse_field(raw, "data")
    decoded = StreamProtocol.decode_sse_data(data || raw)
    data_type = decoded_type(decoded)

    %__MODULE__{
      raw: raw,
      separator: separator,
      event_type: StreamProtocol.normalize_sse_event_label(label) || data_type,
      decoded: decoded,
      direct_failure?: is_nil(data) and EventSummary.typeless_detail_error?(decoded),
      delivery_event: %{event_type: label, data_type: if(is_binary(data), do: data_type)}
    }
  end

  @spec outcome(t()) :: {:ok, StreamProtocol.terminal_outcome()} | nil
  def outcome(%{decoded: decoded}) when map_size(decoded) == 0, do: nil
  def outcome(block), do: StreamProtocol.terminal_outcome(block.event_type, block.decoded)

  @spec normalize(t(), boolean()) :: {iodata(), t()}
  def normalize(block, private_details?) do
    {wire, changed} =
      ErrorCanonicalization.normalize_decoded_block(
        block.raw,
        block.separator,
        block.event_type,
        block.decoded,
        private_details?
      )

    output =
      if changed do
        %__MODULE__{
          raw: "",
          event_type: "response.failed",
          decoded: changed,
          delivery_event: %{event_type: "response.failed", data_type: decoded_type(changed)}
        }
      else
        block
      end

    {wire, output}
  end

  @spec delivery([{iodata(), t()}]) :: delivery()
  def delivery(outputs) do
    {preamble, data, commits?} =
      Enum.reduce(outputs, {[], [], false}, fn {wire, block}, {preamble, data, commits?} ->
        if StreamProtocol.retry_window_preamble_event?(block.delivery_event) do
          {[wire | preamble], data, commits?}
        else
          commits? =
            commits? or StreamProtocol.downstream_visible_event?(block.delivery_event) or
              not is_nil(outcome(block))

          {preamble, [wire | data], commits?}
        end
      end)

    kept =
      Enum.reject(outputs, fn {_wire, block} ->
        StreamProtocol.retry_window_preamble_event?(block.delivery_event)
      end)

    data = data |> Enum.reverse() |> IO.iodata_to_binary()

    # Preserve the aggregate direct-JSON fallback for typeless detail objects.
    # Concatenated objects fail it; additional whitespace remains acceptable.
    direct_failure? =
      not commits? and Enum.any?(kept, fn {_wire, block} -> block.direct_failure? end) and
        match?({:ok, _outcome}, StreamProtocol.terminal_outcome(data))

    %{
      preamble: preamble |> Enum.reverse() |> IO.iodata_to_binary(),
      data: data,
      commits?: commits? or direct_failure?
    }
  end

  defp decoded_type(decoded) do
    case Map.get(decoded, "type") do
      type when is_binary(type) -> type
      _other -> nil
    end
  end
end
