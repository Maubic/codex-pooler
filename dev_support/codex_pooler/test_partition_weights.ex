defmodule CodexPooler.TestPartitionWeights do
  @moduledoc """
  Turns the per-file durations of CI runs into the weights `CodexPooler.TestPartitions` deals by.

  The input is any text that holds the durations `make test-fast` prints when `TEST_FAST_PRINT_FILE_DURATIONS=1` (a
  `test-fast: file durations partition p/N v1 max_cases=… run_ms=… async_ms=…` header, then one `  <sync_ms> <async_ms> <path>` line per
  test file): a saved CI step log, as the Drone API returns it (a JSON array of `{pos, out, time}` rows) or as plain text. A file that
  `CodexPooler.TestFileDurations` wrote (`# codex-pooler test file durations v1 …`, then tab-separated path, `sync_ms` and `async_ms`)
  is read too. Each header starts one sample, the run of one partition. A sample belongs to the tooling profile when most of its files are
  tooling files and to the product profile otherwise, so a file both profiles run (the two mixed LiveView files) is weighed once per profile
  from the run that ran it.

  A file's weight in a sample is its `sync_ms` plus its `async_ms` divided by the sample's `max_cases` (rounded up, at least 1 ms): a
  sync module runs alone, so its wall time is what it costs its partition, and an async module runs beside up to `max_cases` others, so
  it costs its wall time divided by that. A file measured in several samples (several builds) weighs the median of its weights.

  `refresh/3` merges the new weights into the previous ones: a measured file takes its new weight, a file that is still a test file of the
  profile but was not measured keeps its previous weight, and a file that is no longer a test file is dropped. `replay/3` compares, per
  profile and partition count, the measured run times with the weights' loads for the deal Mix makes and for the balanced deal.
  """

  alias CodexPooler.TestPartitions

  @type sample :: %{
          partition: {pos_integer(), pos_integer()} | nil,
          max_cases: pos_integer(),
          run_ms: non_neg_integer(),
          async_ms: non_neg_integer(),
          files: %{String.t() => {non_neg_integer(), non_neg_integer()}}
        }

  @type stats :: %{measured: non_neg_integer(), kept: non_neg_integer(), dropped: non_neg_integer(), unknown: non_neg_integer()}
  @type refreshed :: %{weights: %{TestPartitions.profile() => TestPartitions.weights()}, stats: %{TestPartitions.profile() => stats()}}

  @doc "Every sample in the text (a CI log, as a Drone JSON array or plain text, or an export file), in order of appearance."
  @spec parse(String.t()) :: [sample()]
  def parse(text) do
    {samples, current} =
      text
      |> unwrap_log()
      |> String.split("\n")
      |> Enum.reduce({[], nil}, &read_line/2)

    samples |> push(current) |> Enum.reverse()
  end

  @doc "The weight of every file of a sample."
  @spec weights(sample()) :: TestPartitions.weights()
  def weights(%{files: files, max_cases: max_cases}) do
    Map.new(files, fn {path, {sync_ms, async_ms}} -> {path, max(sync_ms + div(async_ms + max_cases - 1, max_cases), 1)} end)
  end

  @doc """
  The profile a sample belongs to: tooling when most of its files are among `tooling_files`, product otherwise.
  """
  @spec profile(sample(), Enumerable.t()) :: TestPartitions.profile()
  def profile(%{files: files}, tooling_files) do
    tooling = MapSet.new(tooling_files)
    paths = Map.keys(files)
    if Enum.count(paths, &MapSet.member?(tooling, &1)) * 2 > length(paths), do: :tooling, else: :product
  end

  @doc """
  Merges the samples into the previous weights of every profile; `universe` names, per profile, every file that is a test file of it now.

  Returns the new weights with, per profile, the number of files measured, kept from the previous weights, dropped from them and measured
  but no longer test files.
  """
  @spec refresh(%{TestPartitions.profile() => TestPartitions.weights()}, [sample()], %{TestPartitions.profile() => Enumerable.t()}) :: refreshed()
  def refresh(previous, samples, universe) do
    grouped = Enum.group_by(samples, &profile(&1, Map.fetch!(universe, :tooling)))

    merged =
      Map.new(TestPartitions.profiles(), fn profile ->
        {profile, merge(Map.get(previous, profile, %{}), Map.get(grouped, profile, []), MapSet.new(Map.fetch!(universe, profile)))}
      end)

    %{weights: Map.new(merged, fn {profile, {weights, _stats}} -> {profile, weights} end), stats: Map.new(merged, fn {profile, {_weights, stats}} -> {profile, stats} end)}
  end

  @doc """
  For every profile and partition count among the samples: the measured run time of each partition and the weights' load of each partition
  under the deal Mix makes and under the balanced deal, over the files the samples measured.
  """
  @spec replay([sample()], Enumerable.t(), %{TestPartitions.profile() => TestPartitions.weights()}) :: [map()]
  def replay(samples, tooling_files, weights) do
    samples
    |> Enum.filter(& &1.partition)
    |> Enum.group_by(fn %{partition: {_number, count}} = sample -> {profile(sample, tooling_files), count} end)
    |> Enum.sort()
    |> Enum.map(fn {{profile, count}, group} ->
      group = Enum.sort_by(group, fn %{partition: {number, _count}} -> number end)
      files = group |> Enum.flat_map(&Map.keys(&1.files)) |> Enum.uniq()
      weights = Map.get(weights, profile, %{})

      %{
        profile: profile,
        partitions: count,
        files: length(files),
        measured_ms: Enum.map(group, & &1.run_ms),
        round_robin_ms: files |> TestPartitions.round_robin(count) |> TestPartitions.loads(files, weights),
        balanced_ms: files |> TestPartitions.assign(count, weights) |> TestPartitions.loads(files, weights)
      }
    end)
  end

  defp merge(previous, samples, universe) do
    all = samples |> Enum.flat_map(&Map.to_list(weights(&1))) |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    {known, unknown} = Enum.split_with(all, fn {path, _weights} -> MapSet.member?(universe, path) end)
    measured = Map.new(known, fn {path, values} -> {path, TestPartitions.median(values)} end)
    current = Map.filter(previous, fn {path, _weight} -> MapSet.member?(universe, path) end)
    kept = Map.drop(current, Map.keys(measured))

    {Map.merge(kept, measured), %{measured: map_size(measured), kept: map_size(kept), dropped: map_size(previous) - map_size(current), unknown: length(unknown)}}
  end

  # The Drone API returns a step's log as `[{"pos": 0, "out": "line\n", "time": 1}, ...]`.
  defp unwrap_log(text) do
    with "[" <> _rows <- String.trim_leading(text),
         {:ok, rows} when is_list(rows) <- CodexPooler.JSON.decode(text),
         true <- Enum.all?(rows, &(is_map(&1) and is_binary(&1["out"]))) do
      Enum.map_join(rows, & &1["out"])
    else
      _plain -> text
    end
  end

  # The reducer's state is the finished samples, newest first, and the block being read as `{kind, sample}`.
  defp read_line(line, {samples, current}) do
    line = String.trim_trailing(line, "\r")

    case header(line) || row(line, current) do
      {:header, started} -> {push(samples, current), started}
      {:row, captures} -> {samples, add_row(current, captures)}
      nil -> {push(samples, current), nil}
    end
  end

  # A CI log header is `test-fast: file durations partition p/N v1 max_cases=8 run_ms=1 async_ms=2 (...)`, an export's `# codex-pooler test file durations v1 ...`.
  defp header(line) do
    cond do
      captures = Regex.run(~r/test-fast: file durations partition (\d+)\/(\d+) v(\d+) max_cases=(\d+) run_ms=(\d+) async_ms=(\d+)/, line, capture: :all_but_first) ->
        [number, count, version, max_cases, run_ms, async_ms] = Enum.map(captures, &String.to_integer/1)
        {:header, {:log, new_sample(version, {number, count}, max_cases, run_ms, async_ms)}}

      captures = Regex.run(~r/^# codex-pooler test file durations v(\d+) max_cases=(\d+) run_ms=(\d+) async_ms=(\d+)\s*$/, line, capture: :all_but_first) ->
        [version, max_cases, run_ms, async_ms] = Enum.map(captures, &String.to_integer/1)
        {:header, {:export, new_sample(version, nil, max_cases, run_ms, async_ms)}}

      true ->
        nil
    end
  end

  defp new_sample(1, partition, max_cases, run_ms, async_ms), do: %{partition: partition, max_cases: max(max_cases, 1), run_ms: run_ms, async_ms: async_ms, files: %{}}
  defp new_sample(version, _partition, _max_cases, _run_ms, _async_ms), do: Mix.raise("the test file durations are in format v#{version}, this task reads v1")

  defp row(_line, nil), do: nil

  # A CI log block line is `  <sync_ms> <async_ms> <path>`, an export line `<path>\t<sync_ms>\t<async_ms>`.
  defp row(line, {:log, _sample}) do
    with [sync_ms, async_ms, path] <- Regex.run(~r/^\s+(\d+) (\d+) (\S.*?)\s*$/, line, capture: :all_but_first), do: {:row, {path, sync_ms, async_ms}}
  end

  defp row(line, {:export, _sample}) do
    with [path, sync_ms, async_ms] <- Regex.run(~r/^([^\t#][^\t]*)\t(\d+)\t(\d+)\s*$/, line, capture: :all_but_first), do: {:row, {path, sync_ms, async_ms}}
  end

  defp add_row({kind, sample}, {path, sync_ms, async_ms}) do
    {kind, %{sample | files: Map.put(sample.files, path, {String.to_integer(sync_ms), String.to_integer(async_ms)})}}
  end

  defp push(samples, nil), do: samples
  defp push(samples, {_kind, sample}), do: [sample | samples]
end
