defmodule CodexPooler.TestPartitionsTest do
  use ExUnit.Case, async: false

  alias CodexPooler.{TestPartitions, TestProfiles}

  # Mix has compiled the current dev_support source; each child loads that BEAM without recompiling it or starting the application.
  @ebin TestPartitions |> :code.which() |> List.to_string() |> Path.dirname()

  describe "assign/3" do
    test "deals every file to exactly one partition, whatever the order the files come in" do
      files = for index <- 1..40, do: "test/file_#{String.pad_leading(Integer.to_string(index), 2, "0")}_test.exs"
      weights = Map.new(files, fn file -> {file, 50 + :erlang.phash2(file, 700)} end)

      assignment = TestPartitions.assign(files, 4, weights)

      assert assignment == TestPartitions.assign(Enum.shuffle(files), 4, weights)
      assert assignment == TestPartitions.assign(files ++ files, 4, weights)
      assert assignment |> List.flatten() |> Enum.sort() == Enum.sort(files)
      assert Enum.all?(assignment, &(&1 == Enum.sort(&1)))
    end

    test "balances a skewed input where the round-robin deal puts the heavy files together" do
      # a, d and g come first among the sorted names, so Mix gives all three to partition 1
      weights = %{"a" => 100, "d" => 90, "g" => 80, "b" => 10, "c" => 10, "e" => 10, "f" => 10, "h" => 10, "i" => 10, "j" => 10, "k" => 10, "l" => 10}
      files = Map.keys(weights)

      assert TestPartitions.round_robin(files, 3) |> TestPartitions.loads(files, weights) == [280, 40, 40]
      assert TestPartitions.assign(files, 3, weights) |> TestPartitions.loads(files, weights) == [120, 120, 120]

      assignment = TestPartitions.assign(files, 3, weights)
      assert ["a", "d", "g"] |> Enum.map(fn heavy -> Enum.find_index(assignment, &(heavy in &1)) end) |> Enum.sort() == [0, 1, 2]
    end

    test "gives the heaviest files to different partitions and leaves no partition empty when there are enough files" do
      files = for index <- 1..7, do: "f#{index}"
      weights = Map.new(files, &{&1, 1}) |> Map.put("f4", 1_000_000_000)

      assignment = TestPartitions.assign(files, 4, weights)

      assert Enum.all?(assignment, &(&1 != []))
      assert Enum.find(assignment, &("f4" in &1)) == ["f4"]
      assert assignment |> List.flatten() |> Enum.sort() == files
    end

    test "counts a weight below a millisecond as a millisecond, so no partition is left empty" do
      files = ["a", "b", "c", "d", "e"]

      assert files |> TestPartitions.assign(3, %{"a" => 0, "b" => -5}) |> Enum.all?(&(&1 != []))
      assert TestPartitions.assign(files, 3, Map.new(files, &{&1, 0})) == TestPartitions.round_robin(files, 3)
    end

    test "breaks every tie by path and then by partition number" do
      files = ["b", "a", "d", "c", "e"]

      assert TestPartitions.assign(files, 2, Map.new(files, &{&1, 7})) == [["a", "c", "e"], ["b", "d"]]
    end

    test "a file added to the profile moves only that file and files lighter than it, where Mix's deal moves most of them" do
      files = for index <- 1..60, do: "test/f#{String.pad_leading(Integer.to_string(index), 2, "0")}_test.exs"
      weights = Map.new(files, &{&1, 100 + :erlang.phash2(&1, 5000)})
      added = "test/f00_test.exs"
      added_weight = 2_500
      heavier = Enum.filter(files, &(weights[&1] > added_weight))
      owner = fn assignment, file -> Enum.find_index(assignment, &(file in &1)) end

      before = TestPartitions.assign(files, 4, weights)
      added_to = TestPartitions.assign([added | files], 4, Map.put(weights, added, added_weight))

      assert heavier != [] and length(heavier) < length(files)
      for file <- heavier, do: assert(owner.(before, file) == owner.(added_to, file), file)

      # f00 sorts first, so Mix's deal shifts every file after it by one partition
      mix_before = TestPartitions.round_robin(files, 4)
      mix_after = TestPartitions.round_robin([added | files], 4)
      assert Enum.count(files, &(owner.(mix_before, &1) != owner.(mix_after, &1))) == length(files)
    end

    test "deals a profile with no weights as Mix does" do
      files = for index <- 1..23, do: "test/f#{index}_test.exs"

      for partitions <- 1..4, do: assert(TestPartitions.assign(files, partitions, %{}) == TestPartitions.round_robin(files, partitions))
    end

    test "weighs a file without a recorded weight as the median of the recorded weights of the profile" do
      files = ["a", "b", "c", "d"]

      assert TestPartitions.default_weight(files, %{}) == 1_000
      assert TestPartitions.default_weight(files, %{"a" => 30, "b" => 10, "c" => 20}) == 20
      assert TestPartitions.default_weight(files, %{"a" => 30, "b" => 10, "c" => 20, "d" => 25}) == 23
      assert TestPartitions.default_weight(files, %{"a" => 30, "elsewhere" => 1}) == 30

      # d has no weight and counts as the median, 20, like c: the deal pairs a with b and c with d
      weights = %{"a" => 30, "b" => 10, "c" => 20}
      assert TestPartitions.assign(files, 2, weights) == [["a", "b"], ["c", "d"]]
      assert TestPartitions.assign(files, 2, weights) |> TestPartitions.loads(files, weights) == [40, 40]
    end

    test "ignores weights recorded for files that are not in the profile" do
      files = for index <- 1..12, do: "test/f#{index}_test.exs"
      weights = Map.new(files, &{&1, :erlang.phash2(&1, 90) + 10})

      assert TestPartitions.assign(files, 4, Map.put(weights, "test/deleted_test.exs", 1_000_000)) == TestPartitions.assign(files, 4, weights)
    end

    test "leaves a partition empty only when there are fewer files than partitions, and select refuses to hand it out" do
      assert TestPartitions.assign(["a", "b"], 4, %{}) == [["a"], ["b"], [], []]

      assert_raise Mix.Error, ~r/partition 4 of 4 has no test files \(the product profile has 2\)/, fn ->
        TestPartitions.select(:product, ["--partitions", "4"], ["a", "b"], partition: "4", weights: %{})
      end
    end

    test "sums the loads of an assignment with the default weight for unknown files" do
      assert TestPartitions.loads([["a", "x"], ["b"]], ["a", "b", "x"], %{"a" => 5, "b" => 9}) == [12, 9]
    end

    test "the median of an even count is the mean of the middle two rounded up" do
      assert TestPartitions.median([5]) == 5
      assert TestPartitions.median([9, 1, 5]) == 5
      assert TestPartitions.median([1, 2]) == 2
      assert TestPartitions.median([4, 10, 1, 2]) == 3
    end
  end

  describe "the weights file" do
    @describetag :tmp_dir

    test "round-trips by profile and path behind a header and ignores comments and blank lines", %{tmp_dir: dir} do
      weights = %{product: %{"test/b_test.exs" => 20, "test/a_test.exs" => 1_500}, tooling: %{"test/a_test.exs" => 7}}
      path = Path.join(dir, "weights.tsv")
      File.write!(path, TestPartitions.encode_weights(weights))

      assert File.read!(path) |> String.split("\n", trim: true) |> Enum.drop(2) == ["product\ttest/a_test.exs\t1500", "product\ttest/b_test.exs\t20", "tooling\ttest/a_test.exs\t7"]
      assert TestPartitions.read_weights(path) == weights
      assert TestPartitions.load_weights(:product, path) == weights.product
      assert TestPartitions.load_weights(:tooling, path) == weights.tooling
      assert TestPartitions.decode_weights("# note\n\nproduct\ttest/a_test.exs\t5\r\n\n# more\ntooling\ttest/a_test.exs\t6\n") == %{product: %{"test/a_test.exs" => 5}, tooling: %{"test/a_test.exs" => 6}}
      assert TestPartitions.encode_weights(%{product: %{"a" => 1}}) |> IO.iodata_to_binary() |> String.split("\n", trim: true) |> Enum.drop(2) == ["product\ta\t1"]
    end

    test "a missing file is no weights and a malformed one names the line", %{tmp_dir: dir} do
      path = Path.join(dir, "weights.tsv")
      assert TestPartitions.read_weights(path) == %{}
      assert TestPartitions.load_weights(:tooling, path) == %{}

      for {text, message} <- [
            {"product\ttest/a_test.exs\t5\nproduct\ttest/b_test.exs\tfast\n", ~r/weights\.tsv:2: expected a profile \(product or tooling\), a test file path and a positive number of milliseconds, tab-separated, got "product\\ttest\/b_test\.exs\\tfast"/},
            {"product\ttest/a_test.exs\t0\n", ~r/weights\.tsv:1: expected/},
            {"product\ttest/a_test.exs\t-3\n", ~r/weights\.tsv:1: expected/},
            {"test/a_test.exs\t5\n", ~r/weights\.tsv:1: expected/},
            {"unix\ttest/a_test.exs\t5\n", ~r/weights\.tsv:1: expected/},
            {"product\ttest/a_test.exs 5\n", ~r/weights\.tsv:1: expected/},
            {"product\ttest/a_test.exs\t5\textra\n", ~r/weights\.tsv:1: expected/},
            {"product\ttest/a_test.exs\t5\n\nproduct\ttest/a_test.exs\t6\n", ~r/weights\.tsv:3: "test\/a_test\.exs" is listed twice for the product profile/},
            {"<<<<<<< HEAD\n", ~r/weights\.tsv:1: expected/}
          ] do
        File.write!(path, text)
        assert_raise Mix.Error, message, fn -> TestPartitions.read_weights(path) end
      end

      # the same file in both profiles is not a duplicate: the profiles run it differently
      File.write!(path, "product\ttest/a_test.exs\t900\ntooling\ttest/a_test.exs\t8\n")
      assert TestPartitions.read_weights(path) == %{product: %{"test/a_test.exs" => 900}, tooling: %{"test/a_test.exs" => 8}}
    end

    test "the committed weights parse and hold a weight for every profile" do
      weights = TestPartitions.read_weights()

      for profile <- TestPartitions.profiles() do
        assert map_size(weights[profile]) > 0
        assert Enum.all?(weights[profile], fn {file, weight} -> String.ends_with?(file, "_test.exs") and is_integer(weight) and weight > 0 end)
      end
    end
  end

  describe "select/4" do
    test "takes --partitions out of the arguments, keeps the rest in order and names the partition's files" do
      files = for index <- 1..9, do: "test/f#{index}_test.exs"
      weights = Map.new(files, fn file -> {file, :erlang.phash2(file, 50) + 1} end)
      expected = files |> TestPartitions.assign(3, weights) |> Enum.at(1)

      assert {:partition, ["--warnings-as-errors", "--seed", "4"], ^expected} = TestPartitions.select(:product, ["--warnings-as-errors", "--partitions", "3", "--seed", "4"], files, partition: "2", weights: weights)
      assert {:partition, ["--seed", "4"], ^expected} = TestPartitions.select(:tooling, ["--partitions=3", "--seed", "4"], files, partition: "2", weights: weights)
      # Mix reads the last count
      assert {:partition, ["--seed", "4"], ^expected} = TestPartitions.select(:product, ["--partitions", "2", "--seed", "4", "--partitions", "3"], files, partition: "2", weights: weights)
    end

    test "hands every file to exactly one partition" do
      files = for index <- 1..31, do: "test/f#{index}_test.exs"
      weights = Map.new(files, fn file -> {file, :erlang.phash2(file, 900) + 1} end)

      dealt =
        for partition <- 1..4 do
          assert {:partition, [], assigned} = TestPartitions.select(:product, ["--partitions", "4"], files, partition: Integer.to_string(partition), weights: weights)
          assigned
        end

      assert dealt |> List.flatten() |> Enum.sort() == Enum.sort(files)
    end

    test "leaves a run Mix would not partition, or would reject, to Mix" do
      files = ["a", "b", "c"]

      for {args, partition} <- [
            {["--seed", "4"], "2"},
            {["--partitions", "1"], "1"},
            {["--partitions", "0"], "1"},
            {["--partitions", "two"], "1"},
            {["--partitions", "2x"], "1"},
            {["--partitions"], "1"},
            {["--partitions", "2"], nil},
            {["--partitions", "2"], ""},
            {["--partitions", "2"], "0"},
            {["--partitions", "2"], "3"},
            {["--partitions", "2"], "one"}
          ] do
        assert TestPartitions.select(:product, args, files, partition: partition, weights: %{}) == :unpartitioned, inspect({args, partition})
      end
    end
  end

  describe "TestProfiles.arguments/2" do
    setup do
      original = System.fetch_env("MIX_TEST_PARTITION")

      on_exit(fn ->
        case original do
          {:ok, value} -> System.put_env("MIX_TEST_PARTITION", value)
          :error -> System.delete_env("MIX_TEST_PARTITION")
        end
      end)

      :ok
    end

    test "a partitioned run of the product or tooling profile names only its partition's files and no --partitions" do
      for profile <- [:product, :tooling] do
        files = TestProfiles.files(profile)

        dealt =
          for partition <- 1..4 do
            System.put_env("MIX_TEST_PARTITION", Integer.to_string(partition))
            arguments = TestProfiles.arguments(profile, ["--warnings-as-errors", "--partitions", "4"])
            {options, assigned} = Enum.split_with(arguments, &(not String.ends_with?(&1, "_test.exs") or String.starts_with?(&1, "file:")))

            refute "--partitions" in options
            assert "--warnings-as-errors" in options
            assert assigned != []
            assert assigned == Enum.sort(assigned)
            assert Enum.all?(assigned, &(&1 in files))
            if profile == :tooling, do: assert("unix_integration" in options)
            assigned
          end

        assert dealt |> List.flatten() |> Enum.sort() == Enum.sort(files)
        assert dealt == TestPartitions.assign(files, 4, TestPartitions.load_weights(profile))
      end
    end

    test "focused runs, runs without partitions and the unix profile keep their arguments" do
      System.put_env("MIX_TEST_PARTITION", "2")
      selector = "test/codex_pooler/platform/readiness_test.exs:12"

      assert TestProfiles.arguments(:product, [selector, "--partitions", "4"]) == [selector, "--partitions", "4"]
      assert TestProfiles.arguments(:tooling, ["--partitions", "4", selector]) == ["--partitions", "4", selector]
      assert TestProfiles.arguments(:product, ["--seed", "4"]) == ["--seed", "4"] ++ TestProfiles.files(:product)
      assert TestProfiles.arguments(:product, ["--partitions", "1"]) == ["--partitions", "1"] ++ TestProfiles.files(:product)
      assert TestProfiles.arguments(:unix, ["--partitions", "4"]) == ["--only", "unix_integration", "--partitions", "4"] ++ TestProfiles.files(:unix)

      # Mix reports a partition number outside 1..N, or none, itself
      System.put_env("MIX_TEST_PARTITION", "5")
      assert TestProfiles.arguments(:product, ["--partitions", "4"]) == ["--partitions", "4"] ++ TestProfiles.files(:product)
      System.delete_env("MIX_TEST_PARTITION")
      assert TestProfiles.arguments(:product, ["--partitions", "4"]) == ["--partitions", "4"] ++ TestProfiles.files(:product)
    end
  end

  describe "partition VMs" do
    @tag slow: "boots four isolated BEAM VMs to compare the deal each one computes"
    test "every partition VM computes the same deal from the same inputs" do
      files = TestProfiles.files(:product)

      dealt =
        for partition <- 1..4 do
          {output, 0} =
            System.cmd("elixir", ["--erl", "+S 1:1", "-pa", @ebin, "-e", ~s|IO.puts(Enum.join(CodexPooler.TestProfiles.arguments(:product, ["--partitions", "4", "--seed", "7"]), "\\n"))|],
              env: [{"MIX_TEST_PARTITION", Integer.to_string(partition)}],
              stderr_to_stdout: true
            )

          assert ["--seed", "7" | assigned] = String.split(output, "\n", trim: true)
          assigned
        end

      assert dealt |> List.flatten() |> Enum.sort() == Enum.sort(files)
      assert dealt == TestPartitions.assign(files, 4, TestPartitions.load_weights(:product))
      assert Enum.all?(dealt, &(&1 != []))
    end
  end
end
