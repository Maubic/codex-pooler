defmodule CodexPooler.TestPartitions do
  @moduledoc """
  Deals the test files of a profile to the partitions of a partitioned `mix test.product` or `mix test.tooling` run by recorded duration.

  `mix test --partitions N` sorts the files it is given and keeps every Nth one, so a file added or removed moves every file after it to the
  next partition, and the run waits for whichever partition happened to collect the heavy files. Here every partition computes the same
  deal from the same inputs: the profile's files, its weights in `test/partition_weights.tsv` and the partition count.

  The deal puts the files in order of weight, heaviest first (path order among equals), and gives each to the partition with the lowest
  load so far (lowest number among equals). A file without a recorded weight weighs the median of the profile's recorded weights, so a new
  file lands where an ordinary one would, and stale weights, for files that moved or grew, only cost balance. With no weights at all every
  file weighs the same and the deal is the one Mix makes. Every weight is at least 1 ms, so with at least as many files as partitions every
  partition gets a file: an empty list would make `mix test` run every test file.

  A weight is the file's cost to its partition in milliseconds: its sync modules' wall time plus its async modules' wall time divided by
  ExUnit's `max_cases`. The weights file holds one line per profile and file, because the two profiles run a file differently: the product
  profile runs every test of the two mixed LiveView files, the tooling profile only their Unix-tagged tests. `mix test.partition_weights`
  writes the file from a saved CI log (see `CodexPooler.TestPartitionWeights`).

  The weights say nothing about the time a partition spends compiling its test files before ExUnit starts any sync module (roughly a sixth
  of a product partition's run): that time is set by a few files that compile for tens of seconds, is not predicted by file size and is left
  to the luck of the deal.

  `select/4` takes `--partitions N` out of the arguments of an unfocused run and returns this partition's files, because Mix would deal the
  files it is given again. A run Mix would not partition (no count, a count of 1, an unusable `MIX_TEST_PARTITION`) keeps its arguments, and
  Mix reports its own errors.
  """

  @weights_path "test/partition_weights.tsv"
  @fallback_weight_ms 1_000
  @profiles [:product, :tooling]
  @header "# codex-pooler test file partition weights v1"
  @note "# profile, test file and milliseconds: sync wall time plus async wall time divided by max_cases; written by mix test.partition_weights"

  @type profile :: :product | :tooling
  @type weights :: %{optional(String.t()) => pos_integer()}

  @spec weights_path() :: String.t()
  def weights_path, do: @weights_path

  @spec profiles() :: [profile()]
  def profiles, do: @profiles

  @doc """
  Takes `--partitions N` and this partition's files out of an unfocused run's arguments.

  Returns `{:partition, remaining_arguments, files}`, or `:unpartitioned` when Mix would not partition the run or would reject it.
  Options for tests: `:partition` (the `MIX_TEST_PARTITION` value) and `:weights` (instead of the weights file).
  """
  @spec select(profile(), [String.t()], [String.t()], keyword()) :: {:partition, [String.t()], [String.t()]} | :unpartitioned
  def select(profile, args, files, opts \\ []) do
    partition = Keyword.get_lazy(opts, :partition, fn -> System.get_env("MIX_TEST_PARTITION") end)

    with {:ok, partitions, remaining} <- partitions_option(args),
         {:ok, number} <- partition_number(partition, partitions) do
      weights = Keyword.get_lazy(opts, :weights, fn -> load_weights(profile) end)

      case files |> assign(partitions, weights) |> Enum.at(number - 1) do
        [] -> Mix.raise("partition #{number} of #{partitions} has no test files (the #{profile} profile has #{length(Enum.uniq(files))}); with no files `mix test` would run every test file")
        assigned -> {:partition, remaining, assigned}
      end
    else
      :error -> :unpartitioned
    end
  end

  @doc """
  Deals `files` to `partitions` lists, heaviest file first onto the partition with the lowest load.

  Each list is sorted by path. Lists are empty only when there are fewer files than partitions.
  """
  @spec assign([String.t()], pos_integer(), weights()) :: [[String.t()]]
  def assign(files, partitions, weights) when is_integer(partitions) and partitions > 0 do
    files = files |> Enum.uniq() |> Enum.sort()
    default = default_weight(files, weights)

    files
    |> Enum.map(fn file -> {file, max(Map.get(weights, file, default), 1)} end)
    |> Enum.sort_by(fn {file, weight} -> {-weight, file} end)
    |> Enum.reduce(List.duplicate({0, []}, partitions), &place/2)
    |> Enum.map(fn {_load, assigned} -> Enum.sort(assigned) end)
  end

  @doc "The deal Mix makes: every Nth file of the sorted list, one list per partition."
  @spec round_robin([String.t()], pos_integer()) :: [[String.t()]]
  def round_robin(files, partitions) when is_integer(partitions) and partitions > 0 do
    indexed = files |> Enum.uniq() |> Enum.sort() |> Enum.with_index()
    for partition <- 0..(partitions - 1), do: for({file, index} <- indexed, rem(index, partitions) == partition, do: file)
  end

  @doc "The weight of a file without a recorded one: the median of the recorded weights of `files`, or a fixed second when there are none."
  @spec default_weight([String.t()], weights()) :: pos_integer()
  def default_weight(files, weights) do
    case for(file <- files, Map.has_key?(weights, file), do: Map.fetch!(weights, file)) do
      [] -> @fallback_weight_ms
      known -> median(known)
    end
  end

  @doc "The load of every list of an assignment: its files' weights, with the default weight for a file without one."
  @spec loads([[String.t()]], [String.t()], weights()) :: [non_neg_integer()]
  def loads(assignment, files, weights) do
    default = default_weight(files, weights)
    Enum.map(assignment, fn assigned -> assigned |> Enum.map(&max(Map.get(weights, &1, default), 1)) |> Enum.sum() end)
  end

  @doc "The median of integers, the mean of the two middle values rounded up when there is an even number."
  @spec median([pos_integer(), ...]) :: pos_integer()
  def median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    middle = div(count, 2)

    if rem(count, 2) == 1, do: Enum.at(sorted, middle), else: div(Enum.at(sorted, middle - 1) + Enum.at(sorted, middle) + 1, 2)
  end

  @doc "The weights of one profile from the weights file. A missing file is no weights; a malformed one raises."
  @spec load_weights(profile(), Path.t()) :: weights()
  def load_weights(profile, path \\ @weights_path) when profile in @profiles, do: path |> read_weights() |> Map.get(profile, %{})

  @doc "The weights of every profile from the weights file. A missing file is no weights; a malformed one raises."
  @spec read_weights(Path.t()) :: %{profile() => weights()}
  def read_weights(path \\ @weights_path) do
    case File.read(path) do
      {:ok, text} -> decode_weights(text, path)
      {:error, :enoent} -> %{}
      {:error, reason} -> Mix.raise("cannot read #{path}: #{:file.format_error(reason)}")
    end
  end

  @doc "Parses a weights file: `#` lines and blank lines are ignored; every other line is a profile, a test file and a positive integer, tab-separated."
  @spec decode_weights(String.t(), String.t()) :: %{profile() => weights()}
  def decode_weights(text, source \\ "weights") do
    text
    |> String.split("\n")
    |> Enum.with_index(1)
    |> Enum.reduce(%{}, fn {line, number}, weights -> decode_line(String.trim_trailing(line, "\r"), weights, "#{source}:#{number}") end)
  end

  @doc "The weights file's text: a header, then one line per profile and file in order."
  @spec encode_weights(%{profile() => weights()}) :: iodata()
  def encode_weights(weights) do
    rows = for profile <- @profiles, {file, weight} <- weights |> Map.get(profile, %{}) |> Enum.sort(), do: [Atom.to_string(profile), "\t", file, "\t", Integer.to_string(weight), "\n"]
    [@header, "\n", @note, "\n" | rows]
  end

  defp decode_line("", weights, _where), do: weights
  defp decode_line("#" <> _comment, weights, _where), do: weights

  defp decode_line(line, weights, where) do
    with [name, file, value] <- String.split(line, "\t"),
         {:ok, profile} <- profile_named(name),
         {weight, ""} when weight > 0 <- Integer.parse(value),
         false <- weights |> Map.get(profile, %{}) |> Map.has_key?(file) do
      Map.update(weights, profile, %{file => weight}, &Map.put(&1, file, weight))
    else
      true -> Mix.raise("#{where}: #{inspect(line |> String.split("\t") |> Enum.at(1))} is listed twice for the #{hd(String.split(line, "\t"))} profile")
      _malformed -> Mix.raise("#{where}: expected a profile (product or tooling), a test file path and a positive number of milliseconds, tab-separated, got #{inspect(line)}")
    end
  end

  defp profile_named("product"), do: {:ok, :product}
  defp profile_named("tooling"), do: {:ok, :tooling}
  defp profile_named(_other), do: :error

  defp place({file, weight}, bins) do
    {_load, index} = bins |> Enum.with_index() |> Enum.map(fn {{load, _assigned}, index} -> {load, index} end) |> Enum.min()
    List.update_at(bins, index, fn {load, assigned} -> {load + weight, [file | assigned]} end)
  end

  # Mix reads the last `--partitions`; anything it would not partition (no count, a count below 2, a value that is no integer) is left to Mix.
  defp partitions_option(args), do: scan_partitions(args, nil, [])

  defp scan_partitions(["--partitions", value | rest], _previous, kept), do: scan_partitions(rest, value, kept)
  defp scan_partitions(["--partitions=" <> value | rest], _previous, kept), do: scan_partitions(rest, value, kept)
  defp scan_partitions([arg | rest], value, kept), do: scan_partitions(rest, value, [arg | kept])

  defp scan_partitions([], value, kept) do
    case value && Integer.parse(value) do
      {partitions, ""} when partitions > 1 -> {:ok, partitions, Enum.reverse(kept)}
      _ -> :error
    end
  end

  defp partition_number(value, partitions) do
    case value && Integer.parse(value) do
      {number, ""} when number in 1..partitions//1 -> {:ok, number}
      _ -> :error
    end
  end
end
