defmodule CodexPooler.Gateway.RequestCompression.Strategies.JsonDocumentLossless do
  @moduledoc """
  Conservative JSON-object request compression.

  This strategy only minifies valid top-level JSON object text and returns a
  rewrite when local token counting proves a strict reduction. Minification is
  lexical, so every token keeps its original bytes. Arrays keep using the
  existing array strategy so row-oriented metadata stays stable.
  """

  alias CodexPooler.Gateway.RequestCompression.JsonMinifier
  alias CodexPooler.Gateway.RequestCompression.Strategies

  @strategy :json_document_lossless

  @spec compress(term(), Strategies.opts()) :: Strategies.result()
  def compress(content, opts \\ [])

  def compress(content, opts) when is_binary(content) do
    case CodexPooler.JSON.decode(content, objects: :ordered_objects) do
      {:ok, %CodexPooler.JSON.OrderedObject{} = document} ->
        Strategies.finalize(
          @strategy,
          content,
          JsonMinifier.minify(content),
          %{top_level_key_count: top_level_key_count(document)},
          opts
        )

      _skip ->
        :skip
    end
  end

  def compress(_content, _opts), do: :skip

  defp top_level_key_count(%CodexPooler.JSON.OrderedObject{values: values}), do: length(values)
end
