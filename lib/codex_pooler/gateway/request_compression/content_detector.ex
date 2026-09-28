defmodule CodexPooler.Gateway.RequestCompression.ContentDetector do
  @moduledoc false

  alias CodexPooler.Gateway.RequestCompression.BoundedJson
  alias CodexPooler.Gateway.RequestCompression.EmbeddedJson
  alias CodexPooler.Gateway.RequestCompression.JsonMinifier

  @type kind ::
          :json_array
          | :json_document
          | :embedded_json
          | :diff
          | :html
          | :search
          | :build
          | :source_code
          | :text
  @type strategy ::
          :json_array_lossless
          | :json_document_lossless
          | :embedded_json_lossless
          | :diff
          | :search_results
          | :log_output
  @type decision :: %{
          required(:kind) => kind(),
          required(:confidence) => float(),
          required(:compressible) => boolean(),
          required(:strategy) => strategy() | nil
        }

  @strategies %{
    json_array: :json_array_lossless,
    json_document: :json_document_lossless,
    embedded_json: :embedded_json_lossless,
    diff: :diff,
    search: :search_results,
    build: :log_output
  }

  @diff_git_regex ~r/^diff --git\s+\S+\s+\S+/m
  @diff_index_regex ~r/^index\s+[0-9a-f]+\.\.[0-9a-f]+/im
  @diff_file_header_regex ~r/^---\s+\S+.*\n\+\+\+\s+\S+/m
  @diff_hunk_regex ~r/^@@{1,2}\s+-\d+(?:,\d+)?(?:\s+-\d+(?:,\d+)?)*\s+\+\d+(?:,\d+)?\s+@@{1,2}/m
  @diff_addition_regex ~r/^(?!\+\+\+)[ +-]{0,2}\+.+/m
  @diff_deletion_regex ~r/^(?!---)[ +-]{0,2}-.+/m
  @combined_diff_hunk_regex ~r/^@@@\s+-\d+(?:,\d+)?\s+-\d+(?:,\d+)?\s+\+\d+(?:,\d+)?\s+@@@/m

  @html_root_regex ~r/<!doctype\s+html|<html(?:\s|>|$)/i
  @html_open_tag_regex ~r/<[a-z][\w:-]*(?:\s[^<>]*)?>/i
  @html_close_tag_regex ~r/<\/[a-z][\w:-]*>/i
  @html_attribute_regex ~r/<[a-z][^<>]*\s[\w:-]+=(?:"[^"]*"|'[^']*'|[^\s>]+)/i

  @search_heading_regex ~r/\b(?:search results|results for|matches for|found\s+\d+\s+(?:results|matches))\b/i
  @numbered_result_regex ~r/^\s*(?:\d+[\).]\s+|[-*]\s+).+\s-\s.+$/m
  @path_match_regex ~r/^\s*[\w.\/-]+:\d+(?::\d+)?:\s*\S/m
  @path_context_regex ~r/^\s*[\w.\/-]+-\d+(?:-\d+)?-\s*\S/m
  @nul_path_match_regex ~r/^[^\x00\r\n]+\x00\d+(?::\d+)?:\s*\S/m
  @grouped_line_match_regex ~r/^\s*\d+(?::\d+)?:\s*\S/
  @grouped_line_context_regex ~r/^\s*\d+(?:-\d+)?-\s*\S/
  @grouped_heading_path_regex ~r/^[\w.\/-]+$/u
  @grouped_heading_extension_regex ~r/(?:^|\/)[\w.-]+\.[A-Za-z0-9][A-Za-z0-9_-]*$/u
  @grouped_heading_sentence_punctuation_regex ~r/[!?;]|\.\s*$/
  @grouped_separator_regex ~r/^\s*--\s*$/
  @max_grouped_heading_bytes 240
  @url_regex ~r/https?:\/\/\S+/i
  @snippet_separator_regex ~r/\s-\s/

  @severity_regex ~r/^\s*(?:error|warning|warn|failed|failure|panic|exception|traceback)\b[:\s]/im
  @build_term_regex ~r/\b(?:build|building|compile|compiling|compiled|test|tests|failed|failure|running|finished|warning|error|npm|yarn|pnpm|mix|make|cargo|gradle|maven|pytest|rspec|exunit)\b/i
  @build_prefix_regex ~r/^\s*(?:\$ |==>|-->|Compiling|Running|Finished|warning:|error:|\[\w+\])/mi
  @stack_line_regex ~r/(?:^\s+at\s+\S+|^\s*File\s+"[^"]+",\s+line\s+\d+|^\s*\S+:\d+:\d+:)/m
  @diagnostic_path_regex ~r/^\s*\S+\.(?:c|cc|cpp|cs|css|ex|exs|go|h|hpp|html|java|js|jsx|json|kt|m|md|mjs|mm|php|py|rb|rs|scss|svelte|swift|toml|ts|tsx|vue|xml|ya?ml)(?:\(\d+,\d+\)|:\d+(?::\d+)?)[:\s].+/im
  @diagnostic_summary_regex ~r/^\s*(?:Build\s+(?:FAILED|succeeded)|BUILD\s+(?:FAILED|SUCCESSFUL)|\*\*\s+BUILD\s+(?:FAILED|SUCCEEDED)\s+\*\*|Failed!\s+-\s+Failed:|Passed!\s+-\s+Failed:|Test summary:|Found\s+\d+\s+(?:errors?|warnings?)|Executed\s+\d+\s+tests?,\s+with\s+\d+|\d+\s+(?:Warning|Error)\(s\)|\d+\s+(?:tests?|examples?)\s+(?:completed|run|passed|failed)|\d+\s+actionable tasks?:)/im
  @lint_rule_regex ~r/\b(?:lint|style|correctness|suspicious|complexity|nursery|performance|security)\/[a-z0-9_\/-]+\b/i
  # Compilers and linters print grep's `path:line[:col]:` location shape. A
  # severity word or a rule code right after the location marks a diagnostic.
  @diagnostic_location_regex ~r/^\s*[\w.\/-]+:\d+(?::\d+)?:\s*(?:(?i:(?:fatal\s+)?error|warning|note|remark|info)\b|[A-Z]{1,4}\d{2,5}\b)/m
  @compiler_summary_regex ~r/^\s*(?:\d+\s+(?:errors?|warnings?)(?:\s+and\s+\d+\s+(?:errors?|warnings?))?\s+generated\b|compilation\s+terminated\b|make(?:\[\d+\])?:\s+\*\*\*)|^\W*\d+\s+problems?\b/imu

  @source_keyword_regex ~r/^\s*(?:defmodule|defp?\s+\w+|class\s+\w+|function\s+\w+|import\s+|export\s+|const\s+\w+|let\s+\w+|var\s+\w+|pub\s+fn\s+\w+|fn\s+\w+|impl\s+\w+|module\s+\w+|alias\s+)/m
  @source_punctuation_regex ~r/[{}();]/
  @source_operator_regex ~r/(?:=>|->|::|\|>|<-|==|=)/
  @source_indentation_regex ~r/^\s{2,}\S/m
  @source_comment_regex ~r/^\s*(?:#|\/\/|\/\*|\*)/m
  @source_closing_regex ~r/^\s*(?:end|})\s*$/m

  @doc """
  Classifies a tool output. `command:` carries the provenance of the command
  that printed it (`:search`, `:other`, or `:unknown`); `:other` rules out the
  lossy search strategy even when the output has search-result shape.
  """
  @spec detect(term(), keyword()) :: decision()
  def detect(content, opts \\ [])

  def detect(content, opts) when is_binary(content) do
    trimmed = String.trim(content)

    if trimmed == "" or not BoundedJson.within_limits?(content) do
      decision(:text, 100)
    else
      case json_kind(trimmed) do
        {:ok, kind} -> decision(kind, 100)
        :error -> scored_decision(content, Keyword.get(opts, :command, :unknown))
      end
    end
  end

  def detect(_content, _opts), do: decision(:text, 100)

  @doc """
  Whether text reads as compiler or linter diagnostics: a diagnostic summary
  line, or diagnostic locations on at least half of the grep-shaped lines.
  """
  @spec diagnostic_output?(String.t()) :: boolean()
  def diagnostic_output?(content) when is_binary(content) do
    regex_match?(@compiler_summary_regex, content) or regex_match?(@diagnostic_summary_regex, content) or
      diagnostic_location_majority?(content)
  end

  defp diagnostic_location_majority?(content) do
    diagnostic_lines = scan_count(@diagnostic_location_regex, content)
    diagnostic_lines > 0 and diagnostic_lines * 2 >= scan_count(@path_match_regex, content)
  end

  @spec scored_decision(String.t(), atom()) :: decision()
  defp scored_decision(content, command) do
    cond do
      (points = diff_points(content)) >= 70 ->
        decision(:diff, points)

      (points = html_points(content)) >= 70 ->
        decision(:html, points)

      true ->
        scored_text_decision(content, lines(content), command)
    end
  end

  defp scored_text_decision(content, lines, command) do
    case search_decision(content, lines, command) do
      {:ok, decision} -> decision
      :error -> non_search_text_decision(content, lines)
    end
  end

  # Diagnostics share the search location shape, so the ambiguity is resolved
  # here, before a lossy strategy is chosen: a non-search command or diagnostic
  # evidence rules search out and lets the build score decide.
  defp search_decision(_content, _lines, :other), do: :error

  defp search_decision(content, lines, _command) do
    points = search_points(content, lines)

    if points >= 60 and not diagnostic_output?(content) do
      {:ok, decision(:search, points)}
    else
      :error
    end
  end

  defp non_search_text_decision(content, lines) do
    cond do
      (points = build_points(content, lines)) >= 50 ->
        decision(:build, points)

      (points = source_points(content, lines)) >= 50 ->
        decision(:source_code, points)

      EmbeddedJson.embedded?(content) ->
        decision(:embedded_json, 100)

      true ->
        decision(:text, 100)
    end
  end

  defp decision(kind, points) do
    strategy = Map.get(@strategies, kind)

    %{
      kind: kind,
      confidence: confidence(points),
      compressible: not is_nil(strategy),
      strategy: strategy
    }
  end

  defp confidence(points), do: min(points, 100) / 100

  @spec normalize_concatenated_json_objects(term()) :: {:ok, String.t(), pos_integer()} | :error
  def normalize_concatenated_json_objects(content) when is_binary(content) do
    with {:ok, objects} <- decode_concatenated_json_objects(content),
         row_count when row_count >= 2 <- length(objects) do
      rows = objects |> Enum.map(&JsonMinifier.minify/1) |> Enum.intersperse(",")
      {:ok, IO.iodata_to_binary(["[", rows, "]"]), row_count}
    else
      _not_concatenated -> :error
    end
  end

  def normalize_concatenated_json_objects(_content), do: :error

  defp decode_concatenated_json_objects(content) do
    if BoundedJson.within_limits?(content) do
      content |> String.trim() |> decode_object_stream([])
    else
      :error
    end
  end

  defp json_kind(content) do
    case BoundedJson.decode(content) do
      {:ok, value} when is_list(value) -> {:ok, :json_array}
      {:ok, value} when is_map(value) -> {:ok, :json_document}
      {:error, _reason} -> concatenated_json_kind(content)
      _other -> :error
    end
  end

  defp concatenated_json_kind(content) do
    case decode_concatenated_json_objects(content) do
      {:ok, [_first, _second | _rest]} -> {:ok, :json_array}
      _other -> :error
    end
  end

  defp decode_object_stream("", _objects), do: :error

  defp decode_object_stream(content, objects) do
    case decode_leading_json_object(content) do
      {:ok, object_json, rest} ->
        rest
        |> trim_leading_json_whitespace()
        |> continue_object_stream([object_json | objects])

      :error ->
        :error
    end
  end

  defp continue_object_stream({"", _separator_bytes}, [_last, _previous | _rest] = objects) do
    {:ok, Enum.reverse(objects)}
  end

  defp continue_object_stream({"", _separator_bytes}, _objects), do: :error

  defp continue_object_stream({next, separator_bytes}, objects) when separator_bytes > 0 do
    decode_object_stream(next, objects)
  end

  defp continue_object_stream({_next, _separator_bytes}, _objects), do: :error

  # Returns the validated object text itself, so normalization can keep every
  # token byte instead of re-encoding decoded values.
  defp decode_leading_json_object(<<?{, _rest::binary>> = content) do
    with {:ok, byte_end} <- json_object_byte_end(content),
         object_json = binary_part(content, 0, byte_end),
         {:ok, %CodexPooler.JSON.OrderedObject{}} <-
           BoundedJson.decode(object_json, objects: :ordered_objects) do
      rest = binary_part(content, byte_end, byte_size(content) - byte_end)
      {:ok, object_json, rest}
    else
      _invalid -> :error
    end
  end

  defp decode_leading_json_object(_content), do: :error

  defp json_object_byte_end(<<?{, rest::binary>>), do: scan_json_object(rest, 1, 1)

  defp scan_json_object(<<>>, _offset, _depth), do: :error

  defp scan_json_object(<<34, rest::binary>>, offset, depth),
    do: scan_json_string(rest, offset + 1, depth)

  defp scan_json_object(<<?{, rest::binary>>, offset, depth),
    do: scan_json_object(rest, offset + 1, depth + 1)

  defp scan_json_object(<<?}, _rest::binary>>, offset, 1), do: {:ok, offset + 1}

  defp scan_json_object(<<?}, rest::binary>>, offset, depth),
    do: scan_json_object(rest, offset + 1, depth - 1)

  defp scan_json_object(<<_byte, rest::binary>>, offset, depth),
    do: scan_json_object(rest, offset + 1, depth)

  defp scan_json_string(<<>>, _offset, _depth), do: :error

  defp scan_json_string(<<92, _escaped, rest::binary>>, offset, depth),
    do: scan_json_string(rest, offset + 2, depth)

  defp scan_json_string(<<34, rest::binary>>, offset, depth),
    do: scan_json_object(rest, offset + 1, depth)

  defp scan_json_string(<<_byte, rest::binary>>, offset, depth),
    do: scan_json_string(rest, offset + 1, depth)

  defp trim_leading_json_whitespace(content), do: trim_leading_json_whitespace(content, 0)

  defp trim_leading_json_whitespace(<<byte, rest::binary>>, count)
       when byte in [?\s, ?\n, ?\r, ?\t],
       do: trim_leading_json_whitespace(rest, count + 1)

  defp trim_leading_json_whitespace(content, count), do: {content, count}

  defp diff_points(content) do
    hunks = scan_count(@diff_hunk_regex, content)
    additions = scan_count(@diff_addition_regex, content)
    deletions = scan_count(@diff_deletion_regex, content)
    changes = additions + deletions

    if hunks == 0 or changes == 0 do
      0
    else
      diff_points(content, hunks, additions, deletions, changes)
    end
  end

  defp diff_points(content, hunks, additions, deletions, changes) do
    combined_hunks = scan_count(@combined_diff_hunk_regex, content)

    [
      cond_score(hunks, [{2, 50}, {1, 45}]),
      cond_score(changes, [{2, 30}, {1, 25}]),
      score(additions > 0 and deletions > 0, 10),
      score(regex_match?(@diff_file_header_regex, content), 15),
      score(regex_match?(@diff_git_regex, content), 10),
      score(regex_match?(@diff_index_regex, content), 5),
      score(combined_hunks > 0, 10)
    ]
    |> Enum.sum()
    |> min(100)
  end

  defp html_points(content) do
    open_tags = scan_count(@html_open_tag_regex, content)
    close_tags = scan_count(@html_close_tag_regex, content)

    [
      score(regex_match?(@html_root_regex, content), 35),
      score(open_tags >= 3, 20),
      score(close_tags >= 2, 15),
      score(regex_match?(@html_attribute_regex, content), 10),
      score(open_tags + close_tags >= 4, 10)
    ]
    |> Enum.sum()
  end

  defp search_points(content, lines) do
    numbered_results = scan_count(@numbered_result_regex, content)
    path_matches = scan_count(@path_match_regex, content)
    path_context_lines = scan_count(@path_context_regex, content)
    nul_matches = scan_count(@nul_path_match_regex, content)
    grouped_matches = grouped_search_match_count(lines)
    structural_matches = path_matches + nul_matches + grouped_matches
    urls = scan_count(@url_regex, content)
    separators = scan_count(@snippet_separator_regex, content)

    [
      score(regex_match?(@search_heading_regex, content), 30),
      cond_score(numbered_results, [{2, 30}, {1, 15}]),
      cond_score(structural_matches, [{3, 45}, {2, 35}, {1, 15}]),
      score(structural_matches >= 2, 15),
      score(nul_matches >= 2 or grouped_matches >= 2 or path_context_lines >= 2, 10),
      cond_score(urls, [{2, 20}, {1, 10}]),
      score(separators >= 2, 10)
    ]
    |> Enum.sum()
    |> min(100)
  end

  defp build_points(content, lines) do
    severities = scan_count(@severity_regex, content)
    diagnostic_paths = scan_count(@diagnostic_path_regex, content)
    diagnostic_summaries = scan_count(@diagnostic_summary_regex, content)

    [
      cond_score(severities, [{2, 30}, {1, 20}]),
      cond_score(diagnostic_paths, [{2, 35}, {1, 25}]),
      cond_score(diagnostic_summaries, [{2, 25}, {1, 15}]),
      score(regex_match?(@lint_rule_regex, content), 15),
      score(regex_match?(@build_term_regex, content), 20),
      score(regex_match?(@build_prefix_regex, content), 20),
      score(regex_match?(@stack_line_regex, content), 25),
      score(length(lines) >= 4, 10),
      score(repeated_line?(lines), 15)
    ]
    |> Enum.sum()
    |> min(100)
  end

  defp source_points(content, lines) do
    keywords = scan_count(@source_keyword_regex, content)
    punctuation = scan_count(@source_punctuation_regex, content)
    operators = scan_count(@source_operator_regex, content)
    indented_lines = scan_count(@source_indentation_regex, content)

    [
      cond_score(keywords, [{2, 30}, {1, 20}]),
      score(punctuation >= 3, 15),
      score(operators >= 2, 15),
      score(indented_lines >= 2, 10),
      score(regex_match?(@source_comment_regex, content), 10),
      score(regex_match?(@source_closing_regex, content), 10),
      score(length(lines) >= 3, 10)
    ]
    |> Enum.sum()
    |> min(100)
  end

  defp grouped_search_match_count(lines) do
    grouped_search_match_count(lines, nil, 0)
  end

  defp grouped_search_match_count([], current_matches, count) do
    count + completed_group_match_count(current_matches)
  end

  defp grouped_search_match_count([line | rest], nil, count) do
    if grouped_heading?(String.trim(line)) do
      grouped_search_match_count(rest, 0, count)
    else
      grouped_search_match_count(rest, nil, count)
    end
  end

  defp grouped_search_match_count([line | rest] = lines, current_matches, count) do
    cond do
      Regex.match?(@grouped_line_match_regex, line) ->
        grouped_search_match_count(rest, current_matches + 1, count)

      Regex.match?(@grouped_line_context_regex, line) or
          Regex.match?(@grouped_separator_regex, line) ->
        grouped_search_match_count(rest, current_matches, count)

      true ->
        grouped_search_match_count(
          lines,
          nil,
          count + completed_group_match_count(current_matches)
        )
    end
  end

  defp completed_group_match_count(matches) when is_integer(matches) and matches >= 2, do: matches
  defp completed_group_match_count(_matches), do: 0

  defp grouped_heading?(path) do
    byte_size(path) in 1..@max_grouped_heading_bytes and
      Regex.match?(@grouped_heading_path_regex, path) and
      (String.contains?(path, "/") or Regex.match?(@grouped_heading_extension_regex, path)) and
      not Regex.match?(@grouped_heading_sentence_punctuation_regex, path)
  end

  defp repeated_line?(lines) do
    lines
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.frequencies()
    |> Enum.any?(fn {line, count} -> count >= 2 and byte_size(line) >= 8 end)
  end

  defp cond_score(count, thresholds) do
    case Enum.find(thresholds, fn {minimum, _points} -> count >= minimum end) do
      {_minimum, points} -> points
      nil -> 0
    end
  end

  defp score(true, points), do: points
  defp score(false, _points), do: 0

  defp regex_match?(regex, content), do: Regex.match?(regex, content)
  defp scan_count(regex, content), do: regex |> Regex.scan(content, capture: :first) |> length()

  defp lines(content) do
    String.split(content, ["\r\n", "\n", "\r"], trim: true)
  end
end
