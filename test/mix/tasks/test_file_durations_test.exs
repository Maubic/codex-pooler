defmodule CodexPooler.TestFileDurationsTest do
  use ExUnit.Case, async: false

  alias CodexPooler.TestFileDurations

  # Mix has compiled the current test/support source; each child loads that BEAM without recompiling the formatter or starting the
  # Pooler application, and runs real ExUnit modules through ExUnit's own runner events.
  @formatter TestFileDurations |> :code.which() |> List.to_string()
  @env "CODEX_POOLER_TEST_FILE_DURATIONS"
  @probe """
  ExUnit.start(max_cases: 4)
  :ok = CodexPooler.TestFileDurations.start!()
  IO.puts("formatters=" <> inspect(ExUnit.configuration()[:formatters]))
  for file <- System.argv(), do: Code.require_file(file)
  """

  # Stand-ins for ExUnit case modules: the formatter asks the module named by a TestModule for its `async` option.
  defmodule AsyncModule do
    @moduledoc false
    def __ex_unit__(:config), do: %{async?: true, group: nil, parameterize: nil}
  end

  defmodule SyncModule do
    @moduledoc false
    def __ex_unit__(:config), do: %{async?: false, group: nil, parameterize: nil}
  end

  describe "the formatter's accounting" do
    test "a module's time goes to its file's column by the module's own async option, and a file's modules add up" do
      file = Path.expand("test/sample/mixed_test.exs")

      state =
        new_state()
        |> run_module(SyncModule, file, 6)
        |> run_module(AsyncModule, file, 6)
        |> run_module(SyncModule, file, 6)
        |> run_module(AsyncModule, Path.expand("test/sample/async_only_test.exs"), 6)

      assert %{"test/sample/mixed_test.exs" => mixed, "test/sample/async_only_test.exs" => async_only} = state.files
      assert mixed.sync >= 12_000
      assert mixed.async >= 6_000
      assert async_only.sync == 0
      assert async_only.async >= 6_000
      assert state.started == %{}
    end

    test "parameter sets of one module are timed separately and add to the same file" do
      file = Path.expand("test/sample/parameterized_test.exs")
      first = module(SyncModule, file, %{size: 1})
      second = module(SyncModule, file, %{size: 2})

      state =
        new_state()
        |> cast({:module_started, first})
        |> tap(fn _state -> pause(6) end)
        |> cast({:module_started, second})
        |> tap(fn _state -> pause(6) end)
        |> cast({:module_finished, first})
        |> cast({:module_finished, second})

      # 12 ms for the first set (it ran while the second started) and 6 ms for the second; a single start per module name would have
      # lost the first one's start and counted only the second finish.
      assert state.files["test/sample/parameterized_test.exs"].sync >= 18_000
    end

    test "a module that finishes without having started is not counted" do
      state = cast(new_state(), {:module_finished, module(SyncModule, Path.expand("test/sample/never_started_test.exs"))})

      assert state.files == %{}
    end

    test "events that carry no module leave the state alone" do
      state = cast(new_state(), {:test_finished, %ExUnit.Test{name: :sample, module: SyncModule}})

      assert state == new_state()
    end

    test "the export has one header and one path-ordered row per file, rounded to milliseconds" do
      state = %{max_cases: 8, files: %{"test/b_test.exs" => %{sync: 1_499, async: 0}, "test/a_test.exs" => %{sync: 2_500_000, async: 1_500}}}

      assert state |> TestFileDurations.render(%{run: 943_700_400, async: 92_100_000}) |> IO.iodata_to_binary() ==
               "# codex-pooler test file durations v1 max_cases=8 run_ms=943700 async_ms=92100\ntest/a_test.exs\t2500\t2\ntest/b_test.exs\t1\t0\n"

      assert state |> TestFileDurations.render(%{run: 1_000_000, async: nil}) |> IO.iodata_to_binary() =~ "run_ms=1000 async_ms=0\n"
    end
  end

  describe "a real ExUnit run in a child VM" do
    @describetag :tmp_dir
    @describetag slow: "boots an isolated BEAM VM to run real ExUnit modules through the formatter"

    test "writes each file's sync and async wall time with the run's header", %{tmp_dir: dir} do
      files = write_probe_files!(dir)
      export = Path.join(dir, "export.tsv")

      assert {output, 0} = run_probe(dir, files, export)
      assert output =~ "formatters=[ExUnit.CLIFormatter, CodexPooler.TestFileDurations]"
      assert output =~ "Result: 5 passed"

      assert [header | rows] = export |> File.read!() |> String.split("\n", trim: true)
      assert [run_ms, async_ms] = ~r/^# codex-pooler test file durations v1 max_cases=4 run_ms=(\d+) async_ms=(\d+)$/ |> Regex.run(header, capture: :all_but_first) |> Enum.map(&String.to_integer/1)
      # 240 ms of sync modules after an async phase of at least 60 ms
      assert run_ms >= 250
      assert async_ms >= 55

      parsed = Map.new(rows, &parse_row/1)
      assert Enum.map(rows, &(&1 |> String.split("\t") |> hd())) == Enum.sort(Map.keys(parsed))
      assert Enum.sort(Map.keys(parsed)) == files |> Map.values() |> Enum.map(&Path.relative_to_cwd/1) |> Enum.sort()

      # setup_all (120 ms) and the test (80 ms) of the sync module: the module's wall time
      assert {sync_ms, 0} = parsed[Path.relative_to_cwd(files.sync)]
      assert sync_ms >= 190
      # one async and one sync module in the same file
      assert {mixed_sync, mixed_async} = parsed[Path.relative_to_cwd(files.mixed)]
      assert mixed_sync >= 35 and mixed_async >= 55
      assert {0, async_only} = parsed[Path.relative_to_cwd(files.async_only)]
      assert async_only >= 55

      refute File.exists?(export <> ".partial")
    end

    test "registers nothing and writes nothing when the variable is unset", %{tmp_dir: dir} do
      files = write_probe_files!(dir)

      assert {output, 0} = run_probe(dir, files, nil)
      assert output =~ "formatters=[ExUnit.CLIFormatter]"
      assert output =~ "Result: 5 passed"
      assert Path.wildcard(Path.join(dir, "*.tsv")) == []
    end

    test "refuses a file in a directory that does not exist before any test runs", %{tmp_dir: dir} do
      files = write_probe_files!(dir)

      assert {output, exit_code} = run_probe(dir, files, Path.join([dir, "missing", "export.tsv"]))
      assert exit_code != 0
      assert output =~ "(ArgumentError) #{@env} names"
      assert output =~ "is not an existing directory"
      refute output =~ "Result:"
    end

    test "reports a file it cannot write and keeps the run's own result", %{tmp_dir: dir} do
      files = write_probe_files!(dir)
      # an existing directory cannot be replaced by the finished file
      export = Path.join(dir, "export")
      File.mkdir_p!(export)

      assert {output, 0} = run_probe(dir, files, export)
      assert output =~ "test file durations: could not write #{export}:"
      assert output =~ "Result: 5 passed"
      refute File.exists?(export <> ".partial")
    end
  end

  defp parse_row(row) do
    [path, sync, async] = String.split(row, "\t")
    {path, {String.to_integer(sync), String.to_integer(async)}}
  end

  defp new_state do
    {:ok, state} = TestFileDurations.init([{:codex_pooler_test_file_durations, %{path: "unused"}}, {:max_cases, 4}])
    state
  end

  defp cast(state, event) do
    {:noreply, state} = TestFileDurations.handle_cast(event, state)
    state
  end

  defp module(name, file, parameters \\ %{}), do: %ExUnit.TestModule{name: name, file: file, parameters: parameters}

  defp run_module(state, name, file, milliseconds) do
    test_module = module(name, file)
    state = cast(state, {:module_started, test_module})
    pause(milliseconds)
    cast(state, {:module_finished, test_module})
  end

  # A `receive` timeout never ends early, so the measured time is at least the pause.
  defp pause(milliseconds) do
    receive do
    after
      milliseconds -> :ok
    end
  end

  defp write_probe_files!(dir) do
    files = %{sync: Path.join(dir, "sync_test.exs"), mixed: Path.join(dir, "mixed_test.exs"), async_only: Path.join(dir, "async_only_test.exs")}

    File.write!(files.sync, """
    defmodule ProbeSyncCase do
      use ExUnit.Case, async: false
      setup_all do
        Process.sleep(120)
        :ok
      end
      test "sync", do: Process.sleep(80)
    end
    """)

    File.write!(files.mixed, """
    defmodule ProbeMixedAsyncCase do
      use ExUnit.Case, async: true
      test "async", do: Process.sleep(60)
    end
    defmodule ProbeMixedSyncCase do
      use ExUnit.Case, async: false
      test "sync", do: Process.sleep(40)
    end
    """)

    File.write!(files.async_only, """
    defmodule ProbeAsyncOnlyCase do
      use ExUnit.Case, async: true
      test "first", do: Process.sleep(30)
      test "second", do: Process.sleep(30)
    end
    """)

    files
  end

  defp run_probe(dir, files, export) do
    probe = Path.join(dir, "probe.exs")
    File.write!(probe, @probe)

    System.cmd("elixir", ["--erl", "+S 2:2", "-pa", Path.dirname(@formatter), probe | files |> Map.values() |> Enum.sort()],
      env: [{@env, export}],
      stderr_to_stdout: true
    )
  end
end
