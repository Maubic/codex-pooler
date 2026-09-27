defmodule CodexPooler.Gateway.RequestCompression.TokenCounter.BPE do
  @moduledoc false

  @type ranks :: %{required(binary()) => non_neg_integer()}
  @type path :: :linear | :heap

  # Pieces this long use the heap merge; shorter ones keep the linear scan,
  # which measured cheaper below 16 bytes. tiktoken splits its two algorithms
  # the same way (`_byte_pair_merge` / `_byte_pair_merge_large`, at 100 bytes).
  @heap_min_bytes 16

  @spec count(binary(), ranks()) :: non_neg_integer()
  def count("", _ranks), do: 0

  def count(chunk, ranks) when is_binary(chunk) and is_map(ranks) do
    case Map.fetch(ranks, chunk) do
      {:ok, _rank} -> 1
      :error -> chunk |> parts(ranks, path(chunk)) |> length()
    end
  end

  @spec encode(binary(), ranks()) :: [non_neg_integer()]
  def encode(chunk, ranks) when is_binary(chunk) and is_map(ranks), do: encode(chunk, ranks, path(chunk))

  @doc false
  @spec encode(binary(), ranks(), path()) :: [non_neg_integer()]
  def encode("", _ranks, _path), do: []

  def encode(chunk, ranks, path) when is_binary(chunk) and is_map(ranks) and path in [:linear, :heap] do
    case Map.fetch(ranks, chunk) do
      {:ok, rank} -> [rank]
      :error -> chunk |> parts(ranks, path) |> Enum.map(&Map.fetch!(ranks, &1))
    end
  end

  @doc false
  @spec heap_min_bytes() :: pos_integer()
  def heap_min_bytes, do: @heap_min_bytes

  defp path(chunk) when byte_size(chunk) >= @heap_min_bytes, do: :heap
  defp path(_chunk), do: :linear

  defp parts(chunk, ranks, :linear), do: chunk |> byte_pieces() |> merge_pieces(ranks)
  defp parts(chunk, ranks, :heap), do: merge_heap(chunk, ranks)

  defp byte_pieces(chunk) do
    for <<byte <- chunk>>, do: <<byte>>
  end

  defp merge_pieces([], _ranks), do: []
  defp merge_pieces([_piece] = pieces, _ranks), do: pieces

  defp merge_pieces(pieces, ranks) do
    case lowest_ranked_pair(pieces, ranks) do
      nil -> pieces
      index -> pieces |> merge_at(index) |> merge_pieces(ranks)
    end
  end

  defp lowest_ranked_pair([first, second | rest], ranks) do
    best_rank = pair_rank(first, second, ranks)
    best_index = if is_nil(best_rank), do: nil, else: 0

    lowest_ranked_pair(rest, second, ranks, 0, best_rank, best_index)
  end

  defp lowest_ranked_pair(_pieces, _ranks), do: nil

  defp lowest_ranked_pair([], _previous, _ranks, _index, nil, best_index), do: best_index
  defp lowest_ranked_pair([], _previous, _ranks, _index, _best_rank, best_index), do: best_index

  defp lowest_ranked_pair([next | rest], previous, ranks, index, best_rank, best_index) do
    pair_index = index + 1
    rank = pair_rank(previous, next, ranks)

    {best_rank, best_index} =
      cond do
        is_nil(rank) -> {best_rank, best_index}
        is_nil(best_rank) -> {rank, pair_index}
        rank < best_rank -> {rank, pair_index}
        true -> {best_rank, best_index}
      end

    lowest_ranked_pair(rest, next, ranks, pair_index, best_rank, best_index)
  end

  defp pair_rank(first, second, ranks), do: Map.get(ranks, first <> second)

  defp merge_at(pieces, index) do
    {before_pair, [first, second | after_pair]} = Enum.split(pieces, index)
    before_pair ++ [first <> second | after_pair]
  end

  # Heap merge (tiktoken's `_byte_pair_merge_large`). Parts are addressed by
  # their start offset. Every mergeable adjacent pair waits in a pairing heap
  # keyed by `rank * size + start`, so the lowest rank merges first and equal
  # ranks keep the leftmost pair, exactly like the linear scan. A merge
  # re-ranks only the pairs on either side of the new part; heap entries left
  # behind by earlier merges no longer match `pair_ranks` and are skipped. The
  # work is O(n log n) instead of the linear scan's O(n^2).
  defp merge_heap(piece, ranks) do
    size = byte_size(piece)
    last = size - 1

    {pair_ranks, heap} =
      Enum.reduce(0..(last - 1)//1, {%{}, nil}, fn start, {pair_ranks, heap} ->
        put_pair(pair_ranks, heap, size, start, Map.get(ranks, binary_part(piece, start, 2)))
      end)

    state = %{
      piece: piece,
      size: size,
      ranks: ranks,
      ends: Map.new(0..last//1, &{&1, &1 + 1}),
      prevs: Map.new(1..last//1, &{&1, &1 - 1}),
      pair_ranks: pair_ranks
    }

    state
    |> merge_heap_loop(heap)
    |> heap_parts(0, [])
  end

  defp merge_heap_loop(state, nil), do: state

  defp merge_heap_loop(state, heap) do
    {key, heap} = heap_pop(heap)
    start = rem(key, state.size)

    if Map.get(state.pair_ranks, start) == div(key, state.size) do
      {state, heap} = merge_heap_pair(state, heap, start)
      merge_heap_loop(state, heap)
    else
      merge_heap_loop(state, heap)
    end
  end

  defp merge_heap_pair(state, heap, start) do
    %{size: size} = state
    right = Map.fetch!(state.ends, start)
    right_end = Map.fetch!(state.ends, right)
    ends = Map.put(state.ends, start, right_end)
    pair_ranks = Map.delete(state.pair_ranks, right)

    {pair_ranks, heap} =
      if right_end < size do
        next_end = Map.fetch!(ends, right_end)
        put_pair(pair_ranks, heap, size, start, rank_between(state, start, next_end))
      else
        {Map.delete(pair_ranks, start), heap}
      end

    prevs = if right_end < size, do: Map.put(state.prevs, right_end, start), else: state.prevs

    {pair_ranks, heap} =
      case Map.fetch(prevs, start) do
        {:ok, previous} -> put_pair(pair_ranks, heap, size, previous, rank_between(state, previous, right_end))
        :error -> {pair_ranks, heap}
      end

    {%{state | ends: ends, prevs: prevs, pair_ranks: pair_ranks}, heap}
  end

  defp rank_between(state, first, last), do: Map.get(state.ranks, binary_part(state.piece, first, last - first))

  defp put_pair(pair_ranks, heap, _size, start, nil), do: {Map.delete(pair_ranks, start), heap}

  defp put_pair(pair_ranks, heap, size, start, rank) do
    {Map.put(pair_ranks, start, rank), heap_insert(heap, rank * size + start)}
  end

  defp heap_parts(%{size: size}, start, parts) when start >= size, do: Enum.reverse(parts)

  defp heap_parts(state, start, parts) do
    part_end = Map.fetch!(state.ends, start)
    heap_parts(state, part_end, [binary_part(state.piece, start, part_end - start) | parts])
  end

  # Pairing heap of integer keys: `nil` or `{min_key, subheaps}`.
  defp heap_insert(nil, key), do: {key, []}
  defp heap_insert(heap, key), do: heap_meld(heap, {key, []})

  defp heap_pop({key, subheaps}), do: {key, heap_merge_pairs(subheaps, [])}

  defp heap_meld({left_key, left_subheaps} = left, {right_key, right_subheaps} = right) do
    if left_key <= right_key, do: {left_key, [right | left_subheaps]}, else: {right_key, [left | right_subheaps]}
  end

  # Two-pass pairing: meld neighbours left to right, then fold the pairs back
  # right to left.
  defp heap_merge_pairs([first, second | rest], pairs), do: heap_merge_pairs(rest, [heap_meld(first, second) | pairs])
  defp heap_merge_pairs([last], pairs), do: heap_fold([last | pairs])
  defp heap_merge_pairs([], pairs), do: heap_fold(pairs)

  defp heap_fold([]), do: nil
  defp heap_fold([heap | rest]), do: Enum.reduce(rest, heap, &heap_meld/2)
end
