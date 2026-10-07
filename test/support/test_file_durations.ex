defmodule CodexPooler.TestFileDurations do
  @moduledoc """
  Records how long each test file takes in one ExUnit run.

  With `CODEX_POOLER_TEST_FILE_DURATIONS` naming a file, this formatter times every test module from its `module_started` to its
  `module_finished` event. That is the module's wall time: `setup_all`, the tests with their `setup` blocks, every `on_exit` callback
  and the module's teardown, where ExUnit's per-test `time` leaves `setup_all` and `on_exit` out. The module's time is added to its
  file's `sync_ms` or `async_ms` column according to the module's own `async` option, so a file holding both kinds of module has both.

  When the suite finishes the formatter writes the file atomically: a `#` header line with the format version, ExUnit's `max_cases`
  and the suite's own `run_ms` and `async_ms`, then one tab-separated line per test file in path order (`path`, `sync_ms`, `async_ms`):

      # codex-pooler test file durations v1 max_cases=8 run_ms=943700 async_ms=92100
      test/codex_pooler/example_test.exs<TAB>12345<TAB>0

  An async module's time is measured while up to `max_cases` modules run beside it, so its share of the run is that time divided by
  `max_cases`; a sync module runs alone and its share is its whole time.

  `make test-fast` gives every partition its own file and, with `TEST_FAST_PRINT_FILE_DURATIONS=1` (the CI pipeline sets it), prints
  them after a passing run, so a saved CI log carries the durations of the whole suite and no local full run is needed. Without the
  variable the formatter is not registered. The variable must name a file inside an existing directory: anything else raises from
  `start!/0`, before the suite starts, instead of after a run that cannot deliver its measurement. A run that cannot write the file
  at its end reports that on stderr and keeps its own result. A nested `mix test` that must not touch the caller's file clears the
  variable for its child.
  """

  use GenServer

  @env "CODEX_POOLER_TEST_FILE_DURATIONS"
  @config_key :codex_pooler_test_file_durations
  @format_version 1

  @spec start!() :: :ok
  def start! do
    case System.get_env(@env) do
      path when is_binary(path) and path != "" -> register!(path)
      _unset -> :ok
    end
  end

  defp register!(path) do
    directory = Path.dirname(path)

    unless File.dir?(directory) do
      raise ArgumentError, "#{@env} names #{path}, but #{directory} is not an existing directory"
    end

    ExUnit.configure(
      formatters: Enum.uniq(ExUnit.configuration()[:formatters] ++ [__MODULE__]),
      codex_pooler_test_file_durations: %{path: path}
    )

    :ok
  end

  @impl true
  def init(opts) do
    %{path: path} = Keyword.fetch!(opts, @config_key)
    {:ok, %{path: path, max_cases: Keyword.fetch!(opts, :max_cases), started: %{}, files: %{}}}
  end

  @impl true
  def handle_cast({:module_started, %ExUnit.TestModule{} = module}, state) do
    {:noreply, %{state | started: Map.put(state.started, module_key(module), System.monotonic_time(:microsecond))}}
  end

  def handle_cast({:module_finished, %ExUnit.TestModule{} = module}, state) do
    finished = System.monotonic_time(:microsecond)

    case Map.pop(state.started, module_key(module)) do
      {nil, _started} ->
        {:noreply, state}

      {started, remaining} ->
        column = if async?(module), do: :async, else: :sync
        files = add_elapsed(state.files, Path.relative_to_cwd(module.file), column, finished - started)
        {:noreply, %{state | started: remaining, files: files}}
    end
  end

  def handle_cast({:suite_finished, times_us}, state) do
    write(state, times_us)
    {:noreply, state}
  end

  def handle_cast(_event, state), do: {:noreply, state}

  @doc false
  @spec render(%{max_cases: pos_integer(), files: %{String.t() => %{sync: non_neg_integer(), async: non_neg_integer()}}}, %{run: non_neg_integer(), async: non_neg_integer() | nil}) :: iodata()
  def render(state, times_us) do
    header = "# codex-pooler test file durations v#{@format_version} max_cases=#{state.max_cases} run_ms=#{milliseconds(times_us.run)} async_ms=#{milliseconds(times_us.async || 0)}\n"
    [header | state.files |> Enum.sort() |> Enum.map(fn {path, %{sync: sync, async: async}} -> "#{path}\t#{milliseconds(sync)}\t#{milliseconds(async)}\n" end)]
  end

  defp write(state, times_us) do
    temporary = state.path <> ".partial"

    with :ok <- File.write(temporary, render(state, times_us)),
         :ok <- File.rename(temporary, state.path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(temporary)
        IO.puts(:stderr, "test file durations: could not write #{state.path}: #{:file.format_error(reason)}")
    end
  end

  defp add_elapsed(files, path, column, elapsed) do
    Map.update(files, path, Map.put(%{sync: 0, async: 0}, column, elapsed), &Map.update!(&1, column, fn total -> total + elapsed end))
  end

  # Parameterized modules run the same module once per parameter set.
  defp module_key(%ExUnit.TestModule{name: name, parameters: parameters}), do: {name, parameters}

  # ExUnit registers every test module through this same call (`ExUnit.Case.__after_compile__/2`).
  defp async?(%ExUnit.TestModule{name: name}) do
    function_exported?(name, :__ex_unit__, 1) and match?(%{async?: true}, name.__ex_unit__(:config))
  end

  defp milliseconds(microseconds), do: div(microseconds + 500, 1000)
end
