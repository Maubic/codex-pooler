defmodule Mix.Tasks.Test.PartitionWeights do
  @moduledoc """
  Refreshes `test/partition_weights.tsv`, the per-file weights `mix test.product` and `mix test.tooling` deal their partitions by, from the
  durations of CI runs.

      mix test.partition_weights [--dry-run] [--output PATH] LOG_OR_EXPORT...

  Each argument is a saved step log of the CI quality step (the Drone API's JSON rows or plain text) holding the `test-fast: file
  durations` blocks `make test-fast` prints, or a file `CODEX_POOLER_TEST_FILE_DURATIONS` made. Several runs are combined by the median
  of each file's weight. A file measured now takes its new weight, a test file not measured keeps its previous weight, and a file that is
  no longer a test file of its profile is dropped. `--dry-run` prints the report without writing; `--output` writes to another path (the
  previous weights are then read from it if it exists, else from the committed file).

  The report names, per profile and partition count, the measured run times of the partitions and the weights' loads for the deal Mix
  makes and for the balanced deal. A weight counts sync wall time and async wall time divided by `max_cases`, not the time a partition
  spends compiling its test files first, so a measured run time exceeds its round-robin load by that time.
  """
  @shortdoc "Refreshes the test partition weights from CI logs"
  use Mix.Task

  alias CodexPooler.{TestPartitions, TestPartitionWeights, TestProfiles}

  @impl Mix.Task
  def run(args) do
    {opts, inputs} = OptionParser.parse!(args, strict: [dry_run: :boolean, output: :string])
    if inputs == [], do: Mix.raise("usage: mix test.partition_weights [--dry-run] [--output PATH] LOG_OR_EXPORT...")

    path = Keyword.get(opts, :output, TestPartitions.weights_path())
    samples = Enum.flat_map(inputs, &read_samples/1)
    if samples == [], do: Mix.raise("no test file durations found in #{Enum.join(inputs, ", ")}: expected `test-fast: file durations` blocks or a CODEX_POOLER_TEST_FILE_DURATIONS file")

    previous = TestPartitions.read_weights(if File.exists?(path), do: path, else: TestPartitions.weights_path())
    universe = %{product: TestProfiles.files(:product), tooling: TestProfiles.files(:tooling)}
    refreshed = TestPartitionWeights.refresh(previous, samples, universe)

    report(samples, inputs, refreshed, TestPartitionWeights.replay(samples, universe.tooling, refreshed.weights))
    unless opts[:dry_run], do: File.write!(path, TestPartitions.encode_weights(refreshed.weights))
    info(if opts[:dry_run], do: "dry run: #{path} not written", else: "wrote #{refreshed.weights |> Map.values() |> Enum.map(&map_size/1) |> Enum.sum()} weights to #{path}")
    :ok
  end

  defp read_samples(path) do
    case File.read(path) do
      {:ok, text} -> TestPartitionWeights.parse(text)
      {:error, reason} -> Mix.raise("cannot read #{path}: #{:file.format_error(reason)}")
    end
  end

  defp report(samples, inputs, refreshed, replay) do
    info("test.partition_weights: #{length(samples)} samples from #{length(inputs)} inputs, #{samples |> Enum.map(&map_size(&1.files)) |> Enum.sum()} file measurements")

    for profile <- TestPartitions.profiles() do
      stats = refreshed.stats[profile]
      info("#{profile} weights: #{stats.measured} measured, #{stats.kept} kept from the previous file, #{stats.dropped} dropped (no longer test files), #{stats.unknown} measured but not test files now")
    end

    for row <- replay do
      info("#{row.profile}, #{row.partitions} partitions, #{row.files} files: measured run #{seconds(row.measured_ms)}")
      info("#{row.profile}, #{row.partitions} partitions: weights' loads, deal Mix makes #{seconds(row.round_robin_ms)}; balanced #{seconds(row.balanced_ms)}")
    end
  end

  defp seconds(milliseconds) do
    list = Enum.map_join(milliseconds, "/", &decimal(&1 / 1000))
    "#{list} s (slowest #{decimal(Enum.max(milliseconds) / 1000)}, mean #{decimal(Enum.sum(milliseconds) / length(milliseconds) / 1000)})"
  end

  defp decimal(value), do: :erlang.float_to_binary(value, decimals: 1)

  defp info(message), do: Mix.shell().info(message)
end
