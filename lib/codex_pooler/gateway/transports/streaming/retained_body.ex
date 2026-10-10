defmodule CodexPooler.Gateway.Transports.Streaming.RetainedBody do
  @moduledoc false

  alias CodexPooler.Gateway.Runtime.Streaming.BufferTelemetry

  @max_bytes 65_536

  @type t :: {[binary()], non_neg_integer(), boolean()}

  @spec empty() :: t()
  def empty, do: {[], 0, false}

  @spec append(t(), iodata(), BufferTelemetry.event_opts()) :: t()
  def append(body, data, opts \\ [])

  def append(body, "", _opts), do: body

  def append({chunks, byte_count, truncated?}, data, opts)
      when is_list(chunks) and is_integer(byte_count) do
    data = IO.iodata_to_binary(data)
    retained_bytes = byte_count + byte_size(data)

    if not truncated? and retained_bytes > @max_bytes do
      BufferTelemetry.record_retained_body_truncated(
        "retained_body",
        retained_bytes,
        @max_bytes,
        opts
      )
    end

    # Compaction resets the retained count, but not this body's first-crossing state.
    truncated? = truncated? or retained_bytes > @max_bytes

    if byte_size(data) >= @max_bytes do
      retained = data |> suffix(@max_bytes) |> :binary.copy()
      {[retained], byte_size(retained), truncated?}
    else
      compact_if_needed({[own_binary(data) | chunks], retained_bytes, truncated?})
    end
  end

  @spec read(t()) :: binary()
  def read({chunks, byte_count, _truncated?}) when is_list(chunks) and is_integer(byte_count) do
    chunks
    |> Enum.reverse()
    |> IO.iodata_to_binary()
    |> suffix(@max_bytes)
    |> own_binary()
  end

  @spec max_bytes() :: pos_integer()
  def max_bytes, do: @max_bytes

  defp compact_if_needed({chunks, byte_count, truncated?}) when byte_count > @max_bytes * 2 do
    retained =
      chunks |> Enum.reverse() |> IO.iodata_to_binary() |> suffix(@max_bytes) |> own_binary()

    {[retained], byte_size(retained), truncated?}
  end

  defp compact_if_needed(body), do: body

  defp suffix(body, max_bytes) when byte_size(body) <= max_bytes, do: body

  defp suffix(body, max_bytes) do
    binary_part(body, byte_size(body) - max_bytes, max_bytes)
  end

  defp own_binary(binary) do
    if :binary.referenced_byte_size(binary) == byte_size(binary),
      do: binary,
      else: :binary.copy(binary)
  end
end
