defmodule CodexPooler.Gateway.RequestCompression.JsonMinifier do
  @moduledoc """
  Lexical JSON minification for lossless request compression.

  Only insignificant whitespace between JSON tokens is removed. Every token byte
  (strings with their original escapes, numbers, literals, and punctuation) is
  copied verbatim, so numeric precision, escape spelling, key order, and
  duplicate keys survive exactly. A decode/encode roundtrip cannot promise that:
  decimals become floats and lose digits or their spelling.

  Callers pass JSON text they have already validated.
  """

  @spec minify(binary()) :: binary()
  def minify(json) when is_binary(json) do
    json
    |> outside(json, 0, 0, [])
    |> IO.iodata_to_binary()
  end

  defp outside(<<byte, rest::binary>>, json, run_start, offset, acc) when byte in [?\s, ?\t, ?\n, ?\r] do
    skip_whitespace(rest, json, offset + 1, flush(json, run_start, offset, acc))
  end

  defp outside(<<?", rest::binary>>, json, run_start, offset, acc), do: string(rest, json, run_start, offset + 1, acc)
  defp outside(<<_byte, rest::binary>>, json, run_start, offset, acc), do: outside(rest, json, run_start, offset + 1, acc)
  defp outside(<<>>, json, run_start, offset, acc), do: flush(json, run_start, offset, acc)

  defp skip_whitespace(<<byte, rest::binary>>, json, offset, acc) when byte in [?\s, ?\t, ?\n, ?\r] do
    skip_whitespace(rest, json, offset + 1, acc)
  end

  defp skip_whitespace(rest, json, offset, acc), do: outside(rest, json, offset, offset, acc)

  defp string(<<?\\, _escaped, rest::binary>>, json, run_start, offset, acc), do: string(rest, json, run_start, offset + 2, acc)
  defp string(<<?", rest::binary>>, json, run_start, offset, acc), do: outside(rest, json, run_start, offset + 1, acc)
  defp string(<<_byte, rest::binary>>, json, run_start, offset, acc), do: string(rest, json, run_start, offset + 1, acc)
  defp string(<<>>, json, run_start, offset, acc), do: flush(json, run_start, offset, acc)

  defp flush(_json, offset, offset, acc), do: acc
  defp flush(json, run_start, offset, acc), do: [acc, binary_part(json, run_start, offset - run_start)]
end
