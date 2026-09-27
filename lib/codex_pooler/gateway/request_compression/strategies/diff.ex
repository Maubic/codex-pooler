defmodule CodexPooler.Gateway.RequestCompression.Strategies.Diff do
  @moduledoc false

  alias CodexPooler.Gateway.RequestCompression.Strategies

  @strategy :diff
  @default_min_bytes 512
  @default_min_hunks 1
  @default_max_files 16
  @default_max_hunks 32
  @default_max_hunks_per_file 8
  @default_context_lines 2
  @default_model "gpt-4o"

  # A file section starts at any `diff` invocation line (`diff --git`,
  # `diff --cc`, `diff -ru a/x b/x`, `diff -u x y`) or, after a complete hunk,
  # at a `---`/`+++` header pair. Two-way hunks end when the line counts in
  # their `@@` header are used up; lines outside hunks stay verbatim in place.
  @file_start_regex ~r/^diff\s+(?:--(?:cc|combined)\s+\S+|(?:-\S+\s+)*\S+\s+\S+)/
  @hunk_header_regex ~r/^@@{1,2}\s+-\d+(?:,\d+)?(?:\s+-\d+(?:,\d+)?)*\s+\+\d+(?:,\d+)?\s+@@{1,2}/
  @two_way_hunk_counts_regex ~r/^@@\s+-\d+(?:,(\d+))?\s+\+\d+(?:,(\d+))?\s+@@/

  @spec compress(term(), Strategies.opts()) :: Strategies.result()
  def compress(content, opts \\ [])

  def compress(content, opts) when is_binary(content) do
    min_bytes = Strategies.integer_option(opts, :min_bytes, @default_min_bytes, 0)
    min_hunks = Strategies.integer_option(opts, :min_hunks, @default_min_hunks, 1)

    # Every file section counts, including those without text hunks (mode
    # changes, binary files, renames, empty files): they are rendered verbatim
    # or counted as omitted, never dropped unseen.
    with true <- byte_size(content) >= min_bytes,
         {:ok, lines} <- Strategies.lines(content),
         files when files != [] <- parse_files(lines),
         original_hunk_count when original_hunk_count >= min_hunks <- total_hunk_count(files),
         true <- total_change_count(files) > 0,
         true <- every_hunk_changes?(files) do
      selected_files = select_files(files, opts)
      compressed_hunk_count = total_hunk_count(selected_files)

      {compressed_lines, kept_context_line_count, omitted_context_line_count} =
        render_files(files, selected_files, opts)

      omitted_anything? =
        omitted_context_line_count > 0 or compressed_hunk_count < original_hunk_count or length(selected_files) < length(files)

      if compressed_hunk_count > 0 and omitted_anything? do
        compressed = Strategies.join_lines(compressed_lines)

        finalize(
          @strategy,
          content,
          compressed,
          %{
            original_line_count: length(lines),
            compressed_line_count: length(compressed_lines),
            original_file_count: length(files),
            compressed_file_count: length(selected_files),
            omitted_file_count: length(files) - length(selected_files),
            original_hunk_count: original_hunk_count,
            compressed_hunk_count: compressed_hunk_count,
            omitted_hunk_count: original_hunk_count - compressed_hunk_count,
            addition_line_count: addition_line_count(files),
            deletion_line_count: deletion_line_count(files),
            context_line_count: context_line_count(files),
            kept_context_line_count: kept_context_line_count,
            omitted_context_line_count: omitted_context_line_count
          },
          opts
        )
      else
        :skip
      end
    else
      _not_compressible -> :skip
    end
  end

  def compress(_content, _opts), do: :skip

  defp parse_files(lines), do: parse_lines(lines, %{files: [], file: new_file(false), hunk: nil})

  defp parse_lines([], state) do
    state
    |> close_file()
    |> Map.fetch!(:files)
    |> Enum.reverse()
  end

  defp parse_lines([line | rest], state) do
    cond do
      Regex.match?(@file_start_regex, line) -> parse_lines(rest, start_file(state, line))
      Regex.match?(@hunk_header_regex, line) -> parse_lines(rest, state |> close_hunk() |> open_hunk(line))
      hunk_open?(state.hunk) or no_newline_marker?(state.hunk, line) -> parse_lines(rest, add_hunk_line(state, line))
      state.hunk == nil and state.file.hunks == [] -> parse_lines(rest, add_header_line(state, line))
      header_pair?(line, rest) -> parse_lines(rest, start_file(state, line))
      true -> parse_lines(rest, add_part_line(state, line))
    end
  end

  defp new_file(started?), do: %{started?: started?, header: [], parts: [], hunks: []}

  # Lines before the first file section (a preamble) belong to its header.
  defp start_file(%{file: %{started?: false, parts: [], hunks: []} = preamble, hunk: nil} = state, line) do
    %{state | file: %{new_file(true) | header: [line | preamble.header]}}
  end

  defp start_file(state, line) do
    state = close_file(state)
    %{state | file: %{new_file(true) | header: [line]}}
  end

  defp close_file(state) do
    %{file: file} = state = close_hunk(state)

    files =
      if file.header == [] and file.parts == [] and file.hunks == [] do
        state.files
      else
        [%{header: Enum.reverse(file.header), parts: Enum.reverse(file.parts), hunks: Enum.reverse(file.hunks)} | state.files]
      end

    %{state | files: files, file: new_file(false)}
  end

  defp open_hunk(state, header) do
    prefix_width = hunk_prefix_width(header)
    %{state | hunk: %{header: header, body: [], prefix_width: prefix_width, remaining: hunk_line_counts(header, prefix_width)}}
  end

  defp close_hunk(%{hunk: nil} = state), do: state

  defp close_hunk(%{file: file, hunk: hunk} = state) do
    hunk = %{header: hunk.header, body: Enum.reverse(hunk.body), prefix_width: hunk.prefix_width}
    file = %{file | hunks: [hunk | file.hunks], parts: [{:hunk, length(file.hunks)} | file.parts]}
    %{state | file: file, hunk: nil}
  end

  defp add_header_line(%{file: file} = state, line), do: %{state | file: %{file | header: [line | file.header]}}

  defp add_part_line(state, line) do
    %{file: file} = state = close_hunk(state)
    %{state | file: %{file | parts: [{:line, line} | file.parts]}}
  end

  defp add_hunk_line(%{hunk: hunk} = state, line) do
    %{state | hunk: %{hunk | body: [line | hunk.body], remaining: consume_hunk_line(hunk.remaining, line)}}
  end

  # Combined-diff hunks carry one line count per parent; they stay open until
  # the next hunk or file header, as before.
  defp hunk_line_counts(header, 1) do
    case Regex.run(@two_way_hunk_counts_regex, header, capture: :all_but_first) do
      nil -> :uncounted
      counts -> {line_count(Enum.at(counts, 0)), line_count(Enum.at(counts, 1))}
    end
  end

  defp hunk_line_counts(_header, _prefix_width), do: :uncounted

  defp line_count(nil), do: 1
  defp line_count(""), do: 1
  defp line_count(count), do: String.to_integer(count)

  defp consume_hunk_line(:uncounted, _line), do: :uncounted
  defp consume_hunk_line(remaining, "\\" <> _marker), do: remaining
  defp consume_hunk_line({old, new}, "+" <> _added), do: {old, new - 1}
  defp consume_hunk_line({old, new}, "-" <> _removed), do: {old - 1, new}
  defp consume_hunk_line({old, new}, _context), do: {old - 1, new - 1}

  defp hunk_open?(nil), do: false
  defp hunk_open?(%{remaining: :uncounted}), do: true
  defp hunk_open?(%{remaining: {old, new}}), do: old > 0 or new > 0

  # `\ No newline at end of file` annotates the hunk line just before it, even
  # when that line used up the hunk's counts.
  defp no_newline_marker?(nil, _line), do: false
  defp no_newline_marker?(_hunk, line), do: String.starts_with?(line, "\\")

  defp header_pair?("--- " <> _old, ["+++ " <> _new | _rest]), do: true
  defp header_pair?(_line, _rest), do: false

  defp hunk_prefix_width(header) do
    header
    |> String.split(" ", parts: 2)
    |> List.first()
    |> String.length()
    |> Kernel.-(1)
    |> max(1)
  end

  defp select_files(files, opts) do
    max_files = Strategies.integer_option(opts, :max_files, @default_max_files, 1)
    max_hunks = Strategies.integer_option(opts, :max_hunks, @default_max_hunks, 1)

    max_hunks_per_file =
      Strategies.integer_option(opts, :max_hunks_per_file, @default_max_hunks_per_file, 1)

    {selected_files, _file_count, _kept_hunks} =
      Enum.reduce_while(files, {[], 0, 0}, fn file, {selected_files, file_count, kept_hunks} ->
        cond do
          file_count >= max_files ->
            {:halt, {selected_files, file_count, kept_hunks}}

          kept_hunks >= max_hunks ->
            {:halt, {selected_files, file_count, kept_hunks}}

          true ->
            remaining_hunks = max_hunks - kept_hunks
            keep_count = min(max_hunks_per_file, remaining_hunks)
            selected_hunks = Enum.take(file.hunks, keep_count)

            selected_file = %{
              header: file.header,
              parts: file.parts,
              hunks: selected_hunks,
              omitted_hunk_count: length(file.hunks) - length(selected_hunks)
            }

            {:cont, {[selected_file | selected_files], file_count + 1, kept_hunks + length(selected_hunks)}}
        end
      end)

    Enum.reverse(selected_files)
  end

  defp render_files(files, selected_files, opts) do
    context_lines = Strategies.integer_option(opts, :context_lines, @default_context_lines, 0)

    {file_chunks, kept_context_line_count, omitted_context_line_count} =
      Enum.reduce(selected_files, {[], 0, 0}, fn file, {lines, kept_context, omitted_context} ->
        {part_lines, file_kept_context, file_omitted_context} = render_parts(file, context_lines)

        file_lines = file.header ++ part_lines ++ omitted_hunk_marker(file.omitted_hunk_count)

        {[file_lines | lines], kept_context + file_kept_context, omitted_context + file_omitted_context}
      end)

    omitted_file_count = length(files) - length(selected_files)

    file_lines =
      file_chunks
      |> Enum.reverse()
      |> Kernel.++([omitted_file_marker(omitted_file_count)])
      |> List.flatten()

    {file_lines, kept_context_line_count, omitted_context_line_count}
  end

  # Parts keep file order: kept hunks render compressed, omitted hunks drop
  # out (counted by the file's omitted-hunks marker), and lines between hunks
  # or after the last one stay verbatim.
  defp render_parts(file, context_lines) do
    kept_hunks = file.hunks |> Enum.with_index() |> Map.new(fn {hunk, index} -> {index, hunk} end)

    {chunks, kept_context, omitted_context} =
      Enum.reduce(file.parts, {[], 0, 0}, fn
        {:line, line}, {chunks, kept_context, omitted_context} ->
          {[[line] | chunks], kept_context, omitted_context}

        {:hunk, index}, {chunks, kept_context, omitted_context} = acc ->
          case Map.fetch(kept_hunks, index) do
            {:ok, hunk} ->
              {hunk_lines, hunk_kept_context, hunk_omitted_context} = render_hunk(hunk, context_lines)
              {[hunk_lines | chunks], kept_context + hunk_kept_context, omitted_context + hunk_omitted_context}

            :error ->
              acc
          end
      end)

    {chunks |> Enum.reverse() |> List.flatten(), kept_context, omitted_context}
  end

  defp render_hunk(hunk, context_lines) do
    selected_indexes = selected_hunk_indexes(hunk, context_lines)
    selected_index_set = MapSet.new(selected_indexes)

    {body_lines, omitted_context_line_count} =
      Strategies.collapse_lines(hunk.body, selected_indexes, fn count ->
        " [compressed diff output: omitted #{count} context lines]"
      end)

    kept_context_line_count =
      hunk.body
      |> Enum.with_index()
      |> Enum.count(fn {line, index} ->
        not changed_line?(line, hunk) and MapSet.member?(selected_index_set, index)
      end)

    # The header carries the hunk position, so it stays with every kept hunk.
    {[hunk.header | body_lines], kept_context_line_count, omitted_context_line_count}
  end

  defp selected_hunk_indexes(%{body: lines} = hunk, context_lines) do
    last_index = length(lines) - 1

    lines
    |> Enum.with_index()
    |> Enum.reduce(MapSet.new(), fn {line, index}, indexes ->
      if changed_line?(line, hunk) do
        Enum.reduce(
          max(index - context_lines, 0)..min(index + context_lines, last_index)//1,
          indexes,
          &MapSet.put(&2, &1)
        )
      else
        indexes
      end
    end)
    |> MapSet.to_list()
    |> Enum.sort()
  end

  defp omitted_hunk_marker(0), do: []

  defp omitted_hunk_marker(count) do
    ["[compressed diff output: omitted #{count} hunks]"]
  end

  defp omitted_file_marker(0), do: []

  defp omitted_file_marker(count) do
    ["[compressed diff output: omitted #{count} files]"]
  end

  # A hunk without changed lines means the input is not the diff it looks like;
  # there is no faithful compressed form for it.
  defp every_hunk_changes?(files) do
    Enum.all?(files, fn file -> Enum.all?(file.hunks, &(change_count(&1) > 0)) end)
  end

  defp total_hunk_count(files) do
    Enum.reduce(files, 0, &(hunk_count(&1) + &2))
  end

  defp hunk_count(file), do: length(file.hunks)

  defp total_change_count(files) do
    sum_hunks(files, &change_count/1)
  end

  defp change_count(hunk), do: addition_count(hunk) + deletion_count(hunk)

  defp addition_line_count(files) do
    sum_hunks(files, &addition_count/1)
  end

  defp addition_count(hunk) do
    Enum.count(hunk.body, fn line ->
      changed_line?(line, hunk, "+")
    end)
  end

  defp deletion_line_count(files) do
    sum_hunks(files, &deletion_count/1)
  end

  defp deletion_count(hunk) do
    Enum.count(hunk.body, fn line ->
      changed_line?(line, hunk, "-")
    end)
  end

  defp context_line_count(files) do
    sum_hunks(files, fn hunk ->
      Enum.count(hunk.body, &(not changed_line?(&1, hunk)))
    end)
  end

  defp sum_hunks(files, count_fun) do
    Enum.reduce(files, 0, fn file, total ->
      Enum.reduce(file.hunks, total, &(count_fun.(&1) + &2))
    end)
  end

  defp changed_line?(line, hunk) do
    changed_line?(line, hunk, "+") or changed_line?(line, hunk, "-")
  end

  defp changed_line?(line, %{prefix_width: prefix_width}, prefix)
       when is_binary(line) and is_integer(prefix_width) and prefix_width > 0 do
    line
    |> String.slice(0, prefix_width)
    |> String.contains?(prefix)
  end

  defp changed_line?(_line, _hunk, _prefix), do: false

  defp finalize(strategy, original, compressed, counts, opts) do
    Strategies.finalize(strategy, original, compressed, counts, default_model_opts(opts))
  end

  defp default_model_opts(opts) when is_list(opts) do
    if Keyword.has_key?(opts, :model), do: opts, else: Keyword.put(opts, :model, @default_model)
  end

  defp default_model_opts(opts) when is_map(opts) do
    if Map.has_key?(opts, :model) or Map.has_key?(opts, "model") do
      opts
    else
      Map.put(opts, :model, @default_model)
    end
  end
end
