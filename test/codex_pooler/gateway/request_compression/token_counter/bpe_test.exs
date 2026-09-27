defmodule CodexPooler.Gateway.RequestCompression.TokenCounter.BPETest do
  use ExUnit.Case, async: true
  alias CodexPooler.Gateway.RequestCompression.TokenCounter.BPE
  alias CodexPooler.Gateway.RequestCompression.TokenCounter.Ranks

  test "empty, single-byte and whole-token inputs preserve token identities" do
    ranks = %{"a" => 0, "b" => 1, "ab" => 2}
    assert BPE.count("", ranks) == 0
    assert BPE.encode("", ranks) == []
    assert BPE.encode("a", ranks) == [0]
    assert BPE.encode("ab", ranks) == [2]
    assert BPE.count("x", ranks) == 1
  end

  test "pair rank decides the merge and equal ranks preserve the leftmost pair" do
    base = %{"a" => 10, "b" => 11, "c" => 12, "d" => 13}

    for path <- [:linear, :heap] do
      assert BPE.encode("abcd", base, path) == [10, 11, 12, 13]
      assert BPE.encode("abc", Map.merge(base, %{"ab" => 1, "bc" => 0}), path) == [10, 0]
      assert BPE.encode("abc", Map.merge(base, %{"ab" => 0, "bc" => 0}), path) == [0, 12]
      assert BPE.encode("abc", Map.put(base, "bc", 0), path) == [10, 0]
      assert BPE.encode("aaaaa", %{"a" => 5, "aa" => 1, "aaaa" => 2}, path) == [2, 5]
    end
  end

  test "the heap merge yields the linear merge's token ids on real rank data" do
    :rand.seed(:exsss, {20_260, 927, 3})
    alphabet = String.graphemes("aaaabbbcz  \n\t'\"{}[]:,.-_/0123456789AZéü漢字🙂")

    random = for size <- [2, 3, 5, 16, 17, 31, 48, 64, 100, 129], _sample <- 1..12, do: Enum.map_join(1..size, fn _ -> Enum.random(alphabet) end)
    repetitive = for unit <- ["a", "ab", "aab", "漢", "z\n", " ", "Aa", "01", "=-"], size <- [16, 33, 200], do: String.duplicate(unit, size)

    for encoding <- [:o200k_base, :cl100k_base] do
      {:ok, ranks} = Ranks.load(encoding)

      for piece <- random ++ repetitive do
        assert BPE.encode(piece, ranks, :heap) == BPE.encode(piece, ranks, :linear), "#{encoding} diverged on #{byte_size(piece)} bytes"
      end
    end
  end

  test "work grows near-linearly with the piece length" do
    {:ok, ranks} = Ranks.load(:o200k_base)
    short = String.duplicate("a", 128)
    long = String.duplicate("a", 1_024)
    BPE.count(long, ranks)

    short_reductions = reductions(fn -> BPE.count(short, ranks) end)
    long_reductions = reductions(fn -> BPE.count(long, ranks) end)

    # Eight times the bytes: the heap merge measured about 8x the work, the
    # former all-pairs rescan about 75x (3.3 million reductions).
    assert long_reductions < 16 * short_reductions
    assert long_reductions < 500_000
  end

  defp reductions(fun) do
    {:reductions, before} = Process.info(self(), :reductions)
    fun.()
    {:reductions, later} = Process.info(self(), :reductions)
    later - before
  end
end
