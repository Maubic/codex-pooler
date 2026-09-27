defmodule CodexPooler.Gateway.RequestCompression.TokenCounter do
  @moduledoc """
  Local exact token counter for request-compression decisions.

  This module intentionally performs local BPE tokenization against vendored
  tiktoken rank data. It does not call provider APIs, load model weights, or use
  native dependencies.
  """

  alias CodexPooler.Gateway.RequestCompression.TokenCounter.BPE
  alias CodexPooler.Gateway.RequestCompression.TokenCounter.Pretokenizer
  alias CodexPooler.Gateway.RequestCompression.TokenCounter.Ranks
  alias CodexPooler.Gateway.RequestCompression.WorkBudget

  @type encoding :: :cl100k_base | :o200k_base
  @type metadata :: %{
          required(:tokenizer) => String.t(),
          required(:encoding) => String.t()
        }
  @type error_reason ::
          :unsupported_model
          | :unsupported_encoding
          | :rank_file_unavailable
          | :invalid_rank_file
          | :tokenizer_input_limit
          | :work_budget_exhausted
  @type count_result :: {:ok, non_neg_integer(), metadata()} | {:error, error_reason()}
  @type lower_bound_result :: {:ok, non_neg_integer(), metadata()} | {:error, error_reason()}

  @max_input_bytes 8_192
  @max_bpe_chunk_bytes 1_024
  # Work budget surcharge per byte of a piece that takes the heap merge, on top
  # of the byte charged for reading the text: measured, such pieces cost about
  # five times as much per byte as ordinary tool output.
  @heap_surcharge_per_byte 4
  # A lower bound counts only the prefix chunks that the whole text is sure to
  # split the same way. Matching a non-space chunk reads at most four bytes past
  # its end (the optional contraction suffix), so a chunk ending this far before
  # the cut, and not followed only by whitespace, cannot change.
  @stable_chunk_margin 8

  @doc """
  Counts tokens exactly. `work_budget:` (a `WorkBudget`) is charged for the
  text before any tokenization and fails the count once it is spent.
  """
  @spec count(String.t(), String.t(), keyword()) :: count_result()
  def count(model, text, opts \\ [])

  def count(model, text, opts) when is_binary(model) and is_binary(text) do
    with :ok <- input_within_limit(text),
         {:ok, encoding} <- encoding_for_model(model),
         {:ok, ranks} <- Ranks.load(encoding),
         {:ok, chunks} <- charged_chunks(text, encoding, ranks, opts) do
      {:ok, count_chunks(chunks, ranks), metadata(encoding)}
    end
  end

  def count(_model, _text, _opts), do: {:error, :unsupported_model}

  @spec count_tokens(String.t(), String.t()) :: count_result()
  def count_tokens(model, text), do: count(model, text)

  @doc """
  Counts tokens exactly up to #{@max_input_bytes} bytes; beyond that, returns a
  count the whole text is guaranteed to reach. Tokenizing a cut-off prefix is
  not such a bound (completing a piece can merge it into fewer tokens), so only
  prefix chunks that stay identical in the whole text are counted.
  """
  @spec count_lower_bound(String.t(), String.t(), keyword()) :: lower_bound_result()
  def count_lower_bound(model, text, opts \\ [])

  def count_lower_bound(model, text, opts) when is_binary(model) and is_binary(text) do
    with {:ok, encoding} <- encoding_for_model(model),
         {:ok, ranks} <- Ranks.load(encoding),
         {:ok, bounded_text, truncated?} <- bounded_prefix(text),
         {:ok, chunks} <- charged_chunks(bounded_text, encoding, ranks, opts),
         {:ok, chunks} <- stable_chunks(chunks, byte_size(bounded_text), truncated?) do
      {:ok, count_chunks(chunks, ranks), metadata(encoding)}
    end
  end

  def count_lower_bound(_model, _text, _opts), do: {:error, :unsupported_model}

  @spec max_input_bytes() :: pos_integer()
  def max_input_bytes, do: @max_input_bytes

  @spec max_bpe_chunk_bytes() :: pos_integer()
  def max_bpe_chunk_bytes, do: @max_bpe_chunk_bytes

  @spec encoding_for_model(String.t()) :: {:ok, encoding()} | {:error, :unsupported_model}
  def encoding_for_model(model) when is_binary(model) do
    normalized = model |> String.trim() |> String.downcase()

    cond do
      normalized in ["o200k_base", "gpt-4o", "gpt-4o-mini", "gpt-6-astra", "gpt-6-sol", "gpt-6-luna"] ->
        {:ok, :o200k_base}

      String.starts_with?(normalized, [
        "gpt-4o-",
        "o1",
        "o3",
        "o4",
        "gpt-4.1",
        "gpt-5"
      ]) ->
        {:ok, :o200k_base}

      normalized in ["cl100k_base", "gpt-4", "gpt-4-turbo", "gpt-3.5-turbo"] ->
        {:ok, :cl100k_base}

      String.starts_with?(normalized, [
        "gpt-4-",
        "gpt-4-turbo-",
        "gpt-3.5-turbo-",
        "text-embedding-3-",
        "text-embedding-ada-002"
      ]) ->
        {:ok, :cl100k_base}

      true ->
        {:error, :unsupported_model}
    end
  end

  def encoding_for_model(_model), do: {:error, :unsupported_model}

  defp input_within_limit(text) do
    if byte_size(text) > @max_input_bytes do
      {:error, :tokenizer_input_limit}
    else
      :ok
    end
  end

  defp bounded_prefix(text) do
    cond do
      not String.valid?(text) ->
        {:error, :tokenizer_input_limit}

      byte_size(text) <= @max_input_bytes ->
        {:ok, text, false}

      true ->
        text
        |> binary_part(0, @max_input_bytes)
        |> trim_to_valid_prefix()
    end
  end

  defp trim_to_valid_prefix(prefix) do
    if String.valid?(prefix) do
      {:ok, prefix, true}
    else
      prefix
      |> binary_part(0, byte_size(prefix) - 1)
      |> trim_to_valid_prefix()
    end
  end

  defp stable_chunks(chunks, _prefix_size, false), do: {:ok, chunks}

  # Drop the chunk the cut runs through, then every chunk that ends within the
  # margin before the cut or is only whitespace (a whitespace run reaching the
  # cut may regroup with what follows it).
  defp stable_chunks(chunks, prefix_size, true) do
    {chunks_with_ends, covered} =
      Enum.map_reduce(chunks, 0, fn chunk, offset -> {{chunk, offset + byte_size(chunk)}, offset + byte_size(chunk)} end)

    if covered == prefix_size do
      stable =
        chunks_with_ends
        |> Enum.reverse()
        |> Enum.drop(1)
        |> Enum.drop_while(fn {chunk, chunk_end} -> chunk_end > prefix_size - @stable_chunk_margin or whitespace_only?(chunk) end)
        |> Enum.reverse()
        |> Enum.map(fn {chunk, _chunk_end} -> chunk end)

      {:ok, stable}
    else
      {:error, :tokenizer_input_limit}
    end
  end

  defp whitespace_only?(chunk), do: Regex.match?(~r/\A\s+\z/u, chunk)

  defp charged_chunks(text, encoding, ranks, opts) do
    budget = WorkBudget.from_opts(opts)

    with :ok <- WorkBudget.charge(budget, byte_size(text)),
         {:ok, chunks} <- safe_chunks(text, encoding),
         :ok <- WorkBudget.charge(budget, heap_surcharge(chunks, ranks)) do
      {:ok, chunks}
    end
  end

  defp heap_surcharge(chunks, ranks) do
    Enum.reduce(chunks, 0, fn chunk, units ->
      if byte_size(chunk) >= BPE.heap_min_bytes() and not Map.has_key?(ranks, chunk) do
        units + byte_size(chunk) * @heap_surcharge_per_byte
      else
        units
      end
    end)
  end

  defp count_chunks(chunks, ranks), do: Enum.reduce(chunks, 0, fn chunk, acc -> acc + BPE.count(chunk, ranks) end)

  defp safe_chunks(text, encoding) do
    chunks = Pretokenizer.split(text, encoding)

    if Enum.any?(chunks, &(byte_size(&1) > @max_bpe_chunk_bytes)) do
      {:error, :tokenizer_input_limit}
    else
      {:ok, chunks}
    end
  end

  defp metadata(encoding) do
    %{
      tokenizer: "codex_pooler:tiktoken",
      encoding: Atom.to_string(encoding)
    }
  end
end
