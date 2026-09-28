defmodule CodexPooler.ToolOutputPreservationFixtures do
  @moduledoc false

  @spec corpus() :: [{String.t(), String.t()}]
  def corpus do
    rows = Enum.map(1..64, &%{"id" => &1, "value" => "synthetic-value"})
    object = CodexPooler.JSON.encode!(%{"rows" => rows}, pretty: true)
    array = CodexPooler.JSON.encode!(rows, pretty: true)
    exact = ~S({ "decimal": 0.12345678901234567890123456789, "negative": -0.0, "escaped": "\u0061\n\t\\", "duplicate": 1, "duplicate": 2 })

    [
      {"pretty_object", object},
      {"pretty_array", array},
      {"ndjson", Enum.join(List.duplicate(exact, 12), "\n")},
      {"concatenated_json", String.duplicate(exact, 12)},
      {"embedded_json", "report begins\n" <> object <> "\nreport ends"},
      {"precise_lexemes", exact},
      {"malformed_json", "{\n  \"rows\": [1,2,\n" <> String.duplicate("unfinished ", 80)},
      {"diagnostics", Enum.map_join(1..96, "\n", &"src/sample.c:#{&1}: error: missing synthetic value #{&1}")},
      {"search_context", Enum.map_join(1..96, "\n", &"src/sample.ex-#{&1}-surrounding context\nsrc/sample.ex:#{&1}:matched term\nsrc/sample.ex-#{&1 + 1}-following context\n--")},
      {"multi_file_diff", Enum.map_join(1..16, "\n", &"diff --git a/file_#{&1} b/file_#{&1}\n--- a/file_#{&1}\n+++ b/file_#{&1}\n@@ -1 +1 @@\n-old value\n+new value\n\\ No newline at end of file")},
      {"source_unicode", String.duplicate("def sample(value), do: {value, \"日本語 🙂 café\"}\n", 40)},
      {"over_one_mib", String.duplicate("synthetic retained line\n", 50_000)}
    ] ++ Enum.map(1..40, &{"ordered_#{&1}", "retained output #{&1}\n"})
  end

  @spec input([{String.t(), String.t()}]) :: [map()]
  def input(corpus \\ corpus()) do
    Enum.flat_map(corpus, fn {id, output} ->
      [
        %{"type" => "function_call", "call_id" => id, "name" => "synthetic_tool", "arguments" => "{}"},
        %{"type" => "function_call_output", "call_id" => id, "output" => output}
      ]
    end)
  end

  @spec fingerprint(String.t()) :: String.t()
  def fingerprint(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
