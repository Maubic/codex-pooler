defmodule CodexPooler.Gateway.RequestCompression.Strategies.DiffComplexityTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.RequestCompression.Strategies.Diff
  alias CodexPooler.Gateway.RequestCompression.TokenCounter

  test "counts many small hunks without repeatedly traversing earlier hunks" do
    assert {:ok, _, _} = TokenCounter.count("gpt-4o", "warm tokenizer ranks")

    content =
      "diff --git a/sample b/sample\n--- a/sample\n+++ b/sample\n" <>
        Enum.map_join(1..16_000, "\n", fn index ->
          "@@ -#{index * 4},1 +#{index * 4},1 @@\n-old\n+new"
        end)

    assert byte_size(content) < 1_048_576
    {:reductions, before} = Process.info(self(), :reductions)
    result = Diff.compress(content, model: "gpt-4o")
    {:reductions, after_count} = Process.info(self(), :reductions)

    assert {:ok, %{metadata: metadata, content: compressed}} = result
    assert metadata.original_hunk_count == 16_000
    assert metadata.compressed_hunk_count == 8
    assert metadata.omitted_hunk_count == 15_992
    assert compressed =~ "[compressed diff output: omitted 15992 hunks]"

    # Indexing every new hunk with length(previous_hunks) took about 18M
    # reductions for this sub-MiB input, before token work could reject it.
    assert after_count - before < 12_000_000
  end
end
