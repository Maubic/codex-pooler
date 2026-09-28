defmodule CodexPooler.Gateway.RequestCompression.BoundedJson do
  @moduledoc false

  alias CodexPooler.Gateway.RequestCompression.JsonStringRanges

  # Match the dispatch work allowance and outer JSON structural limits. Check
  # all text before classification: embedded JSON must not bypass these bounds
  # by looking like logs or search results before its containers are inspected.
  @max_bytes 1_048_576
  @max_depth 512
  @max_values 262_144

  @spec within_limits?(binary()) :: boolean()
  def within_limits?(text) when byte_size(text) > @max_bytes, do: false
  def within_limits?(text), do: bounded_nesting?(text, 0, false, 0)

  @spec decode(binary(), keyword()) :: {:ok, term()} | {:error, term()}
  def decode(text, opts \\ []) do
    with true <- within_limits?(text),
         {:ok, _ranges} <- JsonStringRanges.scan(text, max_values: @max_values, max_path_length: 0, record?: fn _ -> false end) do
      CodexPooler.JSON.decode(text, opts)
    else
      false -> {:error, :inner_json_limit}
      error -> error
    end
  end

  defp bounded_nesting?(_text, _depth, _quoted, values) when values > @max_values, do: false
  defp bounded_nesting?(<<>>, _depth, _quoted, _values), do: true
  defp bounded_nesting?(<<?\\, _, rest::binary>>, depth, true, values), do: bounded_nesting?(rest, depth, true, values)
  defp bounded_nesting?(<<?", rest::binary>>, depth, quoted, values), do: bounded_nesting?(rest, depth, not quoted, values)
  defp bounded_nesting?(<<_, rest::binary>>, depth, true, values), do: bounded_nesting?(rest, depth, true, values)
  defp bounded_nesting?(<<byte, _::binary>>, depth, false, _values) when byte in [?{, ?[] and depth >= @max_depth, do: false
  defp bounded_nesting?(<<byte, rest::binary>>, depth, false, values) when byte in [?{, ?[], do: bounded_nesting?(rest, depth + 1, false, values + 1)
  defp bounded_nesting?(<<byte, rest::binary>>, depth, false, values) when byte in [?}, ?]], do: bounded_nesting?(rest, max(depth - 1, 0), false, values)
  defp bounded_nesting?(<<byte, rest::binary>>, depth, false, values) when byte in [?,, ?:], do: bounded_nesting?(rest, depth, false, values + 1)
  defp bounded_nesting?(<<_, rest::binary>>, depth, false, values), do: bounded_nesting?(rest, depth, false, values)
end
