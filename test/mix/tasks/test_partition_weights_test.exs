defmodule CodexPooler.TestPartitionWeightsTest do
  use ExUnit.Case, async: false

  alias CodexPooler.{TestPartitions, TestPartitionWeights, TestProfiles}
  alias Mix.Tasks.Test.PartitionWeights, as: WeightsTask

  @log """
  mix compile output
  test-fast: partition 1/2 PASS
  test-fast: partition 1/2: Finished in 1.0 seconds (0.1s async, 0.9s sync)
  test-fast: file durations partition 1/2 v1 max_cases=8 run_ms=1000 async_ms=100 (sync_ms async_ms path)
    900 0 test/a_test.exs
    0 81 test/b_test.exs
    7 9 test/dir with space/c_test.exs
  test-fast: file durations partition 2/2 v1 max_cases=4 run_ms=700 async_ms=50 (sync_ms async_ms path)
    650 3 test/d_test.exs
  test-fast: PASS (2/2 partitions)
    999 999 test/after_the_block_test.exs
  """

  describe "parse/1" do
    test "reads the blocks of a plain text CI log, one sample per header" do
      assert [first, second] = TestPartitionWeights.parse(@log)

      assert first == %{partition: {1, 2}, max_cases: 8, run_ms: 1000, async_ms: 100, files: %{"test/a_test.exs" => {900, 0}, "test/b_test.exs" => {0, 81}, "test/dir with space/c_test.exs" => {7, 9}}}
      assert second == %{partition: {2, 2}, max_cases: 4, run_ms: 700, async_ms: 50, files: %{"test/d_test.exs" => {650, 3}}}
    end

    test "reads the same blocks from the rows of a Drone step log" do
      rows =
        @log
        |> String.split("\n", trim: true)
        |> Enum.with_index()
        |> Enum.map(fn {line, index} -> %{"pos" => index, "out" => line <> "\r\n", "time" => index} end)

      assert TestPartitionWeights.parse(JSON.encode!(rows)) == TestPartitionWeights.parse(@log)
      assert TestPartitionWeights.parse("[]") == []
      assert TestPartitionWeights.parse("[1, 2]") == []
    end

    test "reads the file CodexPooler.TestFileDurations writes" do
      export = "# codex-pooler test file durations v1 max_cases=6 run_ms=25096 async_ms=1910\ntest/a_test.exs\t3237\t0\ntest/b_test.exs\t0\t717\n"

      assert TestPartitionWeights.parse(export) == [%{partition: nil, max_cases: 6, run_ms: 25_096, async_ms: 1910, files: %{"test/a_test.exs" => {3237, 0}, "test/b_test.exs" => {0, 717}}}]
    end

    test "a block ends at the first line that is not one of its rows, and text with no header holds no sample" do
      assert TestPartitionWeights.parse("  1 2 test/x_test.exs\n") == []
      assert TestPartitionWeights.parse("test/x_test.exs\t1\t2\n") == []
      assert [%{files: files}] = TestPartitionWeights.parse("test-fast: file durations partition 1/1 v1 max_cases=2 run_ms=3 async_ms=4 (sync_ms async_ms path)\n  1 2 test/x_test.exs\n\n  5 6 test/y_test.exs\n")
      assert files == %{"test/x_test.exs" => {1, 2}}
    end

    test "refuses a format version it does not know" do
      assert_raise Mix.Error, ~r/format v2, this task reads v1/, fn ->
        TestPartitionWeights.parse("test-fast: file durations partition 1/1 v2 max_cases=2 run_ms=3 async_ms=4 (sync_ms async_ms path)\n")
      end
    end
  end

  describe "weights/1" do
    test "counts sync time whole, async time divided by max_cases rounded up, and at least a millisecond" do
      sample = %{partition: nil, max_cases: 8, run_ms: 0, async_ms: 0, files: %{"sync" => {900, 0}, "async" => {0, 81}, "mixed" => {7, 9}, "tiny" => {0, 0}, "exact" => {0, 80}}}

      assert TestPartitionWeights.weights(sample) == %{"sync" => 900, "async" => 11, "mixed" => 9, "tiny" => 1, "exact" => 10}
    end
  end

  describe "refresh/3" do
    @universe %{product: ["a", "measured", "unmeasured", "new", "both"], tooling: ["measured", "both", "tool"]}

    defp sample(files), do: %{partition: nil, max_cases: 8, run_ms: 0, async_ms: 0, files: Map.new(files, fn {path, sync} -> {path, {sync, 0}} end)}

    test "a file measured by several samples weighs the median of its weights" do
      refresh = fn syncs -> TestPartitionWeights.refresh(%{}, Enum.map(syncs, &sample([{"a", &1}])), @universe).weights.product["a"] end

      assert refresh.([100]) == 100
      assert refresh.([100, 900, 300]) == 300
      assert refresh.([100, 300]) == 200
      assert refresh.([100, 101]) == 101
    end

    test "a measured file takes its new weight, an unmeasured test file keeps its weight, and a file that is gone is dropped" do
      previous = %{product: %{"measured" => 5, "unmeasured" => 70, "gone" => 9}, tooling: %{"tool" => 4, "gone" => 1}}
      samples = [sample([{"measured", 400}, {"new", 20}, {"vanished", 30}])]

      assert TestPartitionWeights.refresh(previous, samples, @universe) == %{
               weights: %{product: %{"measured" => 400, "unmeasured" => 70, "new" => 20}, tooling: %{"tool" => 4}},
               stats: %{
                 product: %{measured: 2, kept: 1, dropped: 1, unknown: 1},
                 tooling: %{measured: 0, kept: 1, dropped: 1, unknown: 0}
               }
             }
    end

    test "a file both profiles run is weighed per profile from the run that ran it" do
      product = sample([{"both", 9_000}, {"a", 100}, {"measured", 50}, {"new", 50}, {"unmeasured", 50}])
      tooling = sample([{"both", 800}, {"tool", 100}, {"measured", 50}])

      refreshed = TestPartitionWeights.refresh(%{}, [product, tooling], @universe)

      assert refreshed.weights.product["both"] == 9_000
      assert refreshed.weights.tooling["both"] == 800
      assert refreshed.weights.tooling["tool"] == 100
      refute Map.has_key?(refreshed.weights.product, "tool")
    end

    test "classifies a sample by where most of its files belong" do
      assert TestPartitionWeights.profile(sample([{"tool", 1}, {"both", 1}, {"a", 1}]), @universe.tooling) == :tooling
      assert TestPartitionWeights.profile(sample([{"tool", 1}, {"a", 1}]), @universe.tooling) == :product
      assert TestPartitionWeights.profile(sample([{"a", 1}]), @universe.tooling) == :product
    end
  end

  describe "replay/3" do
    test "compares the measured run times with the loads of the deal Mix makes and of the balanced deal, per profile and partition count" do
      sample = fn number, count, run_ms, files -> %{partition: {number, count}, max_cases: 8, run_ms: run_ms, async_ms: 0, files: files} end

      samples = [
        sample.(2, 2, 250, %{"test/f2_test.exs" => {100, 0}, "test/f4_test.exs" => {100, 0}}),
        sample.(1, 2, 1500, %{"test/f1_test.exs" => {900, 0}, "test/f3_test.exs" => {500, 0}}),
        sample.(1, 2, 90, %{"test/mix/t1_test.exs" => {80, 0}, "test/mix/t3_test.exs" => {10, 0}}),
        sample.(2, 2, 40, %{"test/mix/t2_test.exs" => {30, 0}, "test/mix/t4_test.exs" => {10, 0}}),
        %{partition: nil, max_cases: 8, run_ms: 1, async_ms: 0, files: %{"test/export_test.exs" => {1, 0}}}
      ]

      weights = TestPartitionWeights.refresh(%{}, samples, %{product: ["test/f1_test.exs", "test/f2_test.exs", "test/f3_test.exs", "test/f4_test.exs", "test/export_test.exs"], tooling: tooling_files()}).weights

      assert TestPartitionWeights.replay(samples, tooling_files(), weights) == [
               %{profile: :product, partitions: 2, files: 4, measured_ms: [1500, 250], round_robin_ms: [1400, 200], balanced_ms: [900, 700]},
               %{profile: :tooling, partitions: 2, files: 4, measured_ms: [90, 40], round_robin_ms: [90, 40], balanced_ms: [80, 50]}
             ]
    end

    defp tooling_files, do: ["test/mix/t1_test.exs", "test/mix/t2_test.exs", "test/mix/t3_test.exs", "test/mix/t4_test.exs"]
  end

  describe "the task" do
    @describetag :tmp_dir

    setup %{tmp_dir: dir} do
      previous_shell = Mix.shell()
      Mix.shell(Mix.Shell.Process)
      on_exit(fn -> Mix.shell(previous_shell) end)

      [product_a, product_b | _rest] = TestProfiles.files(:product)
      tooling = hd(TestProfiles.files(:tooling))
      output = Path.join(dir, "weights.tsv")
      File.write!(output, TestPartitions.encode_weights(%{product: %{product_a => 1, product_b => 77, "test/deleted_test.exs" => 5}, tooling: %{tooling => 3}}))

      log = """
      test-fast: file durations partition 1/2 v1 max_cases=8 run_ms=5000 async_ms=100 (sync_ms async_ms path)
        4000 0 #{product_a}
        0 81 test/vanished_test.exs
      test-fast: file durations partition 1/2 v1 max_cases=8 run_ms=300 async_ms=0 (sync_ms async_ms path)
        250 0 #{tooling}
      """

      File.write!(Path.join(dir, "step.log"), log)
      %{output: output, log: Path.join(dir, "step.log"), product_a: product_a, product_b: product_b, tooling: tooling}
    end

    test "writes the merged weights and reports what it measured", %{output: output, log: log, product_a: product_a, product_b: product_b, tooling: tooling} do
      WeightsTask.run(["--output", output, log])

      assert TestPartitions.read_weights(output) == %{product: %{product_a => 4000, product_b => 77}, tooling: %{tooling => 250}}
      assert output |> File.read!() |> String.starts_with?("# codex-pooler test file partition weights v1\n")

      lines = shell_lines()
      assert "test.partition_weights: 2 samples from 1 inputs, 3 file measurements" in lines
      assert "product weights: 1 measured, 1 kept from the previous file, 1 dropped (no longer test files), 1 measured but not test files now" in lines
      assert "tooling weights: 1 measured, 0 kept from the previous file, 0 dropped (no longer test files), 0 measured but not test files now" in lines
      assert "product, 2 partitions, 2 files: measured run 5.0 s (slowest 5.0, mean 5.0)" in lines
      assert "tooling, 2 partitions, 1 files: measured run 0.3 s (slowest 0.3, mean 0.3)" in lines
      assert List.last(lines) == "wrote 3 weights to #{output}"
    end

    test "--dry-run reports and leaves the file alone", %{output: output, log: log} do
      before = File.read!(output)

      WeightsTask.run(["--dry-run", "--output", output, log])

      assert File.read!(output) == before
      assert List.last(shell_lines()) == "dry run: #{output} not written"
    end

    test "combines several inputs by the median of each file's weight", %{tmp_dir: dir, output: output, log: log, product_a: product_a} do
      second = Path.join(dir, "second.log")
      third = Path.join(dir, "third.log")
      File.write!(second, "test-fast: file durations partition 1/2 v1 max_cases=8 run_ms=1 async_ms=0 (sync_ms async_ms path)\n  1000 0 #{product_a}\n")
      File.write!(third, "# codex-pooler test file durations v1 max_cases=8 run_ms=1 async_ms=0\n#{product_a}\t2000\t0\n")

      WeightsTask.run(["--output", output, log, second, third])

      assert TestPartitions.load_weights(:product, output)[product_a] == 2000
    end

    test "refuses to run without an input, without a sample or with an unreadable file", %{tmp_dir: dir, output: output} do
      assert_raise Mix.Error, ~r/usage: mix test.partition_weights/, fn -> WeightsTask.run(["--output", output]) end

      empty = Path.join(dir, "empty.log")
      File.write!(empty, "nothing to see\n")
      assert_raise Mix.Error, ~r/no test file durations found in .*empty\.log/, fn -> WeightsTask.run(["--output", output, empty]) end

      assert_raise Mix.Error, ~r/cannot read .*missing\.log/, fn -> WeightsTask.run(["--output", output, Path.join(dir, "missing.log")]) end
    end
  end

  defp shell_lines(lines \\ []) do
    receive do
      {:mix_shell, :info, [line]} -> shell_lines([line | lines])
    after
      0 -> Enum.reverse(lines)
    end
  end
end
