defmodule CodexPooler.Gateway.RequestCompression.Strategies.EmbeddedJsonLossless do
  @moduledoc """
  Conservative lossless compression for JSON containers embedded in ordinary text.

  The strategy preserves every byte outside validated JSON object and array spans,
  preserves duplicate object keys, and returns a rewrite only when local token
  counting proves that the complete output shrinks.
  """

  alias CodexPooler.Gateway.RequestCompression.EmbeddedJson
  alias CodexPooler.Gateway.RequestCompression.JsonStringRanges
  alias CodexPooler.Gateway.RequestCompression.Strategies
  alias CodexPooler.Gateway.RequestCompression.Strategies.JsonArrayLossless
  alias CodexPooler.Gateway.RequestCompression.Strategies.JsonDocumentLossless

  @strategy :embedded_json_lossless

  @spec compress(term(), Strategies.opts()) :: Strategies.result()
  def compress(content, opts \\ [])

  def compress(content, opts) when is_binary(content) do
    with {:ok, spans} <- EmbeddedJson.plan(content),
         {:ok, replacements, counts} <- span_replacements(content, spans, opts),
         {:ok, compressed} <- JsonStringRanges.replace_ranges(content, replacements) do
      Strategies.finalize(@strategy, content, compressed, counts, opts)
    else
      {:skip, reason} when reason in [:tokenizer_input_limit, :work_budget_exhausted] -> {:skip, reason}
      _not_rewritable -> :skip
    end
  end

  def compress(_content, _opts), do: :skip

  defp span_replacements(content, spans, opts) do
    spans
    |> Enum.reduce_while({[], [], 0}, fn span, {replacements, kinds, skips} ->
      case compress_span(content, span, opts) do
        {:ok, replacement} ->
          {:cont, {[replacement | replacements], [span.kind | kinds], skips}}

        {:skip, :tokenizer_input_limit} ->
          {:cont, {replacements, kinds, skips + 1}}

        # A spent budget stays spent: the remaining spans and the whole output
        # keep their original bytes.
        {:skip, :work_budget_exhausted} ->
          {:halt, :work_budget_exhausted}

        :skip ->
          {:cont, {replacements, kinds, skips}}
      end
    end)
    |> span_replacement_result(length(spans))
  end

  defp span_replacement_result(:work_budget_exhausted, _span_count), do: {:skip, :work_budget_exhausted}

  defp span_replacement_result({replacements, kinds, tokenizer_input_skips}, span_count) do
    replacements = Enum.reverse(replacements)

    cond do
      replacements != [] -> {:ok, replacements, counts(kinds)}
      tokenizer_input_skips == span_count -> {:skip, :tokenizer_input_limit}
      true -> :skip
    end
  end

  defp compress_span(content, span, opts) do
    span_content = binary_part(content, span.byte_start, span.byte_end - span.byte_start)
    module = strategy_module(span.kind)

    case module.compress(span_content, opts) do
      {:ok, %{content: compressed}} ->
        {:ok,
         %{
           byte_start: span.byte_start,
           byte_end: span.byte_end,
           replacement: compressed
         }}

      {:skip, reason} when reason in [:tokenizer_input_limit, :work_budget_exhausted] ->
        {:skip, reason}

      _not_compressed ->
        :skip
    end
  end

  defp strategy_module(:array), do: JsonArrayLossless
  defp strategy_module(:object), do: JsonDocumentLossless

  defp counts(kinds) do
    %{
      span_count: length(kinds),
      object_span_count: Enum.count(kinds, &(&1 == :object)),
      array_span_count: Enum.count(kinds, &(&1 == :array))
    }
  end
end
