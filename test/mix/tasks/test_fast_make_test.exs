defmodule CodexPooler.MixTasks.TestFastMakeTest do
  use CodexPooler.UnixIntegrationCase,
    async: false,
    tools: ~w(make ps epmd elixir /usr/bin/script /bin/sh)

  @moduletag :test_infrastructure
  @timeout_ms 15_000
  @receipt_detection_timeout_ms 15_000
  @receipt_poll_interval_ms 20

  test "test-fast starts EPMD before partition processes" do
    fixture = start_fixture!()
    epmd_port = available_tcp_port()
    epmd_port_string = Integer.to_string(epmd_port)

    on_exit(fn ->
      System.cmd("epmd", ["-kill"], env: [{"ERL_EPMD_PORT", epmd_port_string}])
    end)

    assert {output, 0} =
             run_make(fixture, 2,
               TEST_FAST_RELEASE: "1",
               TEST_FAST_REQUIRE_EPMD: "1",
               ERL_EPMD_PORT: epmd_port_string
             )

    assert output =~ "test-fast: PASS (2/2 partitions)"
  end

  test "N=4 partitions share the host scheduler budget without dropping existing ERL flags" do
    fixture = start_fixture!()

    assert {output, 0} =
             run_make(fixture, 4,
               TEST_FAST_CAPTURE_SCHEDULERS: "1",
               TEST_FAST_LOGICAL_CPUS: "12",
               TEST_FAST_RELEASE: "1",
               ERL_FLAGS: "+sbwt none"
             )

    assert output =~ "test-fast: PASS (4/4 partitions)"

    fixture.directory
    |> await_receipts!("run-", 4)
    |> Enum.each(fn receipt ->
      contents = File.read!(Path.join(fixture.directory, receipt))

      assert contents =~ "schedulers=3"
      assert contents =~ "erl_flags=+hmbs 1000000 +sbwt none +S 3:3"
      assert contents =~ ~r/candidates=\S+\/duration-[1-4]\.tsv/
    end)

    refute Enum.any?(File.ls!(fixture.directory), &String.starts_with?(&1, "confirm-"))
  end

  test "two simultaneous N=4 invocations overlap with distinct namespaces and clean exact databases" do
    fixture = start_fixture!()

    first = Task.async(fn -> run_make(fixture, 4) end)
    second = Task.async(fn -> run_make(fixture, 4) end)

    started = await_receipts!(fixture.directory, "run-", 8)
    File.touch!(fixture.release_path)

    assert {first_output, 0} = Task.await(first, @timeout_ms)
    assert {second_output, 0} = Task.await(second, @timeout_ms)
    assert first_output =~ "test-fast: PASS (4/4 partitions)"
    assert second_output =~ "test-fast: PASS (4/4 partitions)"

    assert_namespace_partitions(started, 2, 1..4)

    dropped = await_receipts!(fixture.directory, "drop-", 8)
    assert MapSet.new(dropped) == rename_receipts(started, "run-", "drop-")
  end

  test "a failing partition is attributed, propagated, and cleaned" do
    fixture = start_fixture!()

    assert {output, exit_code} =
             run_make(fixture, 2, TEST_FAST_FAIL_PARTITION: "2", TEST_FAST_RELEASE: "1")

    assert exit_code != 0
    assert output =~ "partition 2/2 FAIL (exit 17)"
    assert output =~ "test-fast: FAIL (1/2 partitions)"

    started = await_receipts!(fixture.directory, "run-", 2)
    dropped = await_receipts!(fixture.directory, "drop-", 2)
    assert MapSet.new(dropped) == rename_receipts(started, "run-", "drop-")
  end

  test "duration candidates are re-measured alone and pass when they fit" do
    fixture = start_fixture!()

    assert {output, 0} =
             run_make(fixture, 2,
               TEST_FAST_RELEASE: "1",
               TEST_FAST_LOGICAL_CPUS: "4",
               ERL_FLAGS: "",
               TEST_FAST_CANDIDATES: "test/b_test.exs:9 test/a_test.exs:3",
               TEST_FAST_CANDIDATE_PARTITION: "2"
             )

    assert output =~ "test-fast: 2 tests exceeded the duration limits beside the other partitions; re-measuring them alone"
    assert output =~ "test-fast: all 2 re-measured within the duration limits (1/3 runs)"
    assert output =~ "test-fast: PASS (2/2 partitions)"

    started = await_receipts!(fixture.directory, "run-", 2)
    [namespace] = started |> Enum.map(&(&1 |> String.split("-") |> Enum.at(1))) |> Enum.uniq()

    assert confirm_rounds(fixture, namespace) == [
             "namespace=#{namespace} partition=1 erl_flags=+hmbs 1000000 +S 2:2 candidates=set args=test/a_test.exs:3 test/b_test.exs:9"
           ]

    dropped = await_receipts!(fixture.directory, "drop-", 2)
    assert MapSet.new(dropped) == rename_receipts(started, "run-", "drop-")
  end

  test "each partition writes its file durations to a file of its own inside the invocation's temporary directory and nothing is printed by default" do
    fixture = start_fixture!()

    assert {output, 0} = run_make(fixture, 2, TEST_FAST_RELEASE: "1", TEST_FAST_WRITE_FILE_DURATIONS: "1 2")

    refute output =~ "file durations"
    assert output =~ "test-fast: PASS (2/2 partitions)"

    fixture.directory
    |> await_receipts!("run-", 2)
    |> Enum.each(fn receipt ->
      contents = File.read!(Path.join(fixture.directory, receipt))
      [_receipt, partition] = Regex.run(~r/partition=(\d)/, contents)
      assert contents =~ ~r/file_durations=\S*codex-pooler-test-fast\.\w+\/files-#{partition}\.tsv$/
    end)
  end

  test "TEST_FAST_PRINT_FILE_DURATIONS=1 prints every partition's file durations after the partition results and before the final PASS" do
    fixture = start_fixture!()

    assert {output, 0} = run_make(fixture, 2, TEST_FAST_RELEASE: "1", TEST_FAST_WRITE_FILE_DURATIONS: "1 2", TEST_FAST_PRINT_FILE_DURATIONS: "1")

    assert String.split(output, "\n", trim: true) |> Enum.drop_while(&(&1 != "test-fast: partition 2/2 PASS")) |> Enum.reject(&String.starts_with?(&1, "test-fast: partition 2/2:")) == [
             "test-fast: partition 2/2 PASS",
             "test-fast: file durations partition 1/2 v1 max_cases=8 run_ms=100 async_ms=10 (sync_ms async_ms path)",
             "  1250 0 test/p1_sync_test.exs",
             "  0 175 test/p1_async_test.exs",
             "test-fast: file durations partition 2/2 v1 max_cases=8 run_ms=200 async_ms=20 (sync_ms async_ms path)",
             "  2250 0 test/p2_sync_test.exs",
             "  0 275 test/p2_async_test.exs",
             "test-fast: PASS (2/2 partitions)"
           ]
  end

  test "the printed file durations are what the partition weights task reads back" do
    fixture = start_fixture!()

    assert {output, 0} = run_make(fixture, 2, TEST_FAST_RELEASE: "1", TEST_FAST_WRITE_FILE_DURATIONS: "1 2", TEST_FAST_PRINT_FILE_DURATIONS: "1")

    assert CodexPooler.TestPartitionWeights.parse(output) == [
             %{partition: {1, 2}, max_cases: 8, run_ms: 100, async_ms: 10, files: %{"test/p1_sync_test.exs" => {1250, 0}, "test/p1_async_test.exs" => {0, 175}}},
             %{partition: {2, 2}, max_cases: 8, run_ms: 200, async_ms: 20, files: %{"test/p2_sync_test.exs" => {2250, 0}, "test/p2_async_test.exs" => {0, 275}}}
           ]
  end

  test "a partition that wrote no export is reported when the durations are printed and does not fail the run" do
    fixture = start_fixture!()

    assert {output, 0} = run_make(fixture, 2, TEST_FAST_RELEASE: "1", TEST_FAST_WRITE_FILE_DURATIONS: "1", TEST_FAST_PRINT_FILE_DURATIONS: "1")

    assert output =~ "test-fast: file durations partition 1/2 v1 max_cases=8 run_ms=100 async_ms=10"
    assert output =~ "\ntest-fast: file durations partition 2/2 none recorded\n"
    assert output =~ "test-fast: PASS (2/2 partitions)"
  end

  test "a failing partition prints no file durations" do
    fixture = start_fixture!()

    assert {output, exit_code} =
             run_make(fixture, 2, TEST_FAST_RELEASE: "1", TEST_FAST_WRITE_FILE_DURATIONS: "1 2", TEST_FAST_PRINT_FILE_DURATIONS: "1", TEST_FAST_FAIL_PARTITION: "2")

    assert exit_code != 0
    assert output =~ "test-fast: FAIL (1/2 partitions)"
    refute output =~ "file durations"
  end

  test "the re-measurement of duration candidates does not inherit a partition's export file" do
    fixture = start_fixture!()

    assert {_output, 0} =
             run_make(fixture, 2,
               TEST_FAST_RELEASE: "1",
               TEST_FAST_WRITE_FILE_DURATIONS: "1 2",
               TEST_FAST_PRINT_FILE_DURATIONS: "1",
               TEST_FAST_CANDIDATES: "test/a_test.exs:3",
               TEST_FAST_CANDIDATE_PARTITION: "1"
             )

    started = await_receipts!(fixture.directory, "run-", 2)
    [namespace] = started |> Enum.map(&(&1 |> String.split("-") |> Enum.at(1))) |> Enum.uniq()
    assert fixture.directory |> Path.join("confirm-env-#{namespace}") |> File.read!() == "file_durations=unset\n"
  end

  for summary <- ["", "Result: 0 tests", "Result: 1/2 passed", "Result: 0 tests, 2 excluded", "Result: 2 passed, 1 invalid"] do
    test "a successful child with invalid completion #{inspect(summary)} fails its partition" do
      fixture = start_fixture!()

      assert {output, exit_code} =
               run_make(fixture, 2,
                 TEST_FAST_RELEASE: "1",
                 TEST_FAST_SUMMARY_PARTITION: "2",
                 TEST_FAST_SUMMARY: unquote(summary)
               )

      assert exit_code != 0
      assert output =~ "partition 2/2 FAIL (no successful nonempty test result)"
      refute output =~ "test-fast: PASS"

      started = await_receipts!(fixture.directory, "run-", 2)
      dropped = await_receipts!(fixture.directory, "drop-", 2)
      assert MapSet.new(dropped) == rename_receipts(started, "run-", "drop-")
    end
  end

  test "a candidate that exceeds its limits in every run on its own fails the invocation" do
    fixture = start_fixture!()

    assert {output, exit_code} =
             run_make(fixture, 2,
               TEST_FAST_RELEASE: "1",
               TEST_FAST_CANDIDATES: "test/a_test.exs:3 test/b_test.exs:9",
               TEST_FAST_CANDIDATE_PARTITION: "1",
               TEST_FAST_CONFIRM_KEEP: "test/b_test.exs:9"
             )

    assert exit_code != 0
    assert output =~ "test-fast: FAIL (duration: 1 of 2 tests exceeded the limits in 3 runs on their own)"
    assert output =~ "test/b_test.exs:9 synthetic still over its limit"
    refute output =~ "test-fast: PASS"

    started = await_receipts!(fixture.directory, "run-", 2)
    [namespace] = started |> Enum.map(&(&1 |> String.split("-") |> Enum.at(1))) |> Enum.uniq()

    assert fixture |> confirm_rounds(namespace) |> Enum.map(&(&1 |> String.split("args=") |> List.last())) == [
             "test/a_test.exs:3 test/b_test.exs:9",
             "test/b_test.exs:9",
             "test/b_test.exs:9"
           ]

    dropped = await_receipts!(fixture.directory, "drop-", 2)
    assert MapSet.new(dropped) == rename_receipts(started, "run-", "drop-")
  end

  test "a re-measurement that fails on its own fails the invocation" do
    fixture = start_fixture!()

    assert {output, exit_code} =
             run_make(fixture, 2,
               TEST_FAST_RELEASE: "1",
               TEST_FAST_CANDIDATES: "test/a_test.exs:3",
               TEST_FAST_CANDIDATE_PARTITION: "1",
               TEST_FAST_CONFIRM_EXIT: "2"
             )

    assert exit_code != 0
    assert output =~ "test-fast: FAIL (duration re-measurement 1/3 exited 2)"
    refute output =~ "test-fast: PASS"
  end

  test "the partitions' normal-limit reports merge longest first, capped at 20 lines, and never fail the run" do
    fixture = start_fixture!()

    assert {output, 0} = run_make(fixture, 2, TEST_FAST_RELEASE: "1", TEST_FAST_REPORT_COUNT: "12")

    assert [_before, report] = String.split(output, "test-fast: duration report: 24 tests over the normal limit without @tag slow beside the other partitions (not a failure), longest first:\n")
    lines = report |> String.split("\n") |> Enum.take(21)

    assert Enum.take(lines, 20) ==
             Enum.map(12..1//-1, &"  #{200 + &1}.5ms test/p2_test.exs:#{&1} ProbeTest test #{&1}") ++
               Enum.map(12..5//-1, &"  #{100 + &1}.5ms test/p1_test.exs:#{&1} ProbeTest test #{&1}")

    assert List.last(lines) == "  ... and 4 more"
    refute output =~ "outside the report block"
    assert output =~ "test-fast: PASS (2/2 partitions)"
    refute Enum.any?(File.ls!(fixture.directory), &String.starts_with?(&1, "confirm-"))
  end

  for {signal, make_exit} <- [{"INT", 130}, {"TERM", 143}] do
    test "#{signal} stops children and cleans only the interrupted invocation databases" do
      fixture = start_fixture!()
      port = open_make_port(fixture, 2, unquote(signal))

      started = await_receipts!(fixture.directory, "run-", 2)

      interrupt_port(port, unquote(signal))

      {output, exit_code} = collect_port(port)

      assert exit_code != 0
      assert output =~ "test-fast: interrupted; stopping partitions"
      assert output =~ "Error #{unquote(make_exit)}"

      dropped = await_receipts!(fixture.directory, "drop-", 2)
      assert MapSet.new(dropped) == rename_receipts(started, "run-", "drop-")
    end
  end

  defp start_fixture! do
    directory =
      Path.join(
        System.tmp_dir!(),
        "codex-pooler-test-fast-acceptance-#{System.unique_integer([:positive])}"
      )

    File.mkdir_p!(directory)
    helper_path = Path.join(directory, "test-fast-child")
    release_path = Path.join(directory, "release")

    File.write!(helper_path, """
    #!/bin/bash
    set -eu

    phase="$1"
    namespace="${CODEX_POOLER_TEST_RUN_NAMESPACE:?missing run namespace}"
    partition="${MIX_TEST_PARTITION:?missing partition}"
    receipt="${TEST_FAST_ACCEPTANCE_DIR}/${phase}-${namespace}-${partition}"

    if [ "$phase" = "drop" ]; then
      printf 'namespace=%s partition=%s\n' "$namespace" "$partition" > "$receipt"
      exit 0
    fi

    # Without --partitions this is the re-measurement of duration candidates:
    # its arguments are the candidate locations.
    if [ "${2:-}" != "--partitions" ]; then
      shift
      printf 'namespace=%s partition=%s erl_flags=%s candidates=%s args=%s\n' \
        "$namespace" "$partition" "${ERL_FLAGS:-}" "${CODEX_POOLER_TEST_DURATION_CANDIDATES:+set}" "$*" \
        >> "${TEST_FAST_ACCEPTANCE_DIR}/confirm-${namespace}"

      printf 'file_durations=%s\n' "${CODEX_POOLER_TEST_FILE_DURATIONS:-unset}" >> "${TEST_FAST_ACCEPTANCE_DIR}/confirm-env-${namespace}"

      if [ -n "${TEST_FAST_CONFIRM_KEEP:-}" ]; then
        printf '%s\tsynthetic still over its limit\n' "$TEST_FAST_CONFIRM_KEEP" > "$CODEX_POOLER_TEST_DURATION_CANDIDATES"
      else
        : > "$CODEX_POOLER_TEST_DURATION_CANDIDATES"
      fi

      exit "${TEST_FAST_CONFIRM_EXIT:-0}"
    fi

    schedulers=""

    if [ "${TEST_FAST_CAPTURE_SCHEDULERS:-}" = "1" ]; then
      schedulers="$(elixir -e 'IO.write(System.schedulers_online())')"
    fi

    printf 'namespace=%s partition=%s schedulers=%s erl_flags=%s candidates=%s file_durations=%s\n' \
      "$namespace" "$partition" "$schedulers" "${ERL_FLAGS:-}" "${CODEX_POOLER_TEST_DURATION_CANDIDATES:-}" "${CODEX_POOLER_TEST_FILE_DURATIONS:-}" > "$receipt"

    # The export CodexPooler.TestFileDurations writes at the end of a partition: a header, then path, sync_ms and async_ms
    # separated by tabs. TEST_FAST_WRITE_FILE_DURATIONS lists the partitions that write one.
    case " ${TEST_FAST_WRITE_FILE_DURATIONS:-} " in
      *" $partition "*)
        printf '# codex-pooler test file durations v1 max_cases=8 run_ms=%s00 async_ms=%s0\n' "$partition" "$partition" > "$CODEX_POOLER_TEST_FILE_DURATIONS"
        printf 'test/p%s_sync_test.exs\t%s250\t0\n' "$partition" "$partition" >> "$CODEX_POOLER_TEST_FILE_DURATIONS"
        printf 'test/p%s_async_test.exs\t0\t%s75\n' "$partition" "$partition" >> "$CODEX_POOLER_TEST_FILE_DURATIONS"
        ;;
    esac

    if [ -n "${TEST_FAST_CANDIDATES:-}" ] && [ "${TEST_FAST_CANDIDATE_PARTITION:-}" = "$partition" ]; then
      printf '%s\tsynthetic candidate\n' $TEST_FAST_CANDIDATES > "$CODEX_POOLER_TEST_DURATION_CANDIDATES"
    fi

    # The guard's report block as TestDurationGuard prints it, after other output.
    if [ -n "${TEST_FAST_REPORT_COUNT:-}" ]; then
      echo "Finished in 1.0 seconds"
      echo "test duration report: ${TEST_FAST_REPORT_COUNT} tests over 1000.0ms without @tag slow (not a failure)" >&2
      for index in $(seq 1 "$TEST_FAST_REPORT_COUNT"); do
        echo "  $((partition * 100 + index)).5ms test/p${partition}_test.exs:${index} ProbeTest test ${index}" >&2
      done
      # Once a line leaves the block, a later indented timing is not part of it.
      echo "Randomized with seed 1" >&2
      echo "  999.5ms outside the report block" >&2
    fi

    if [ "${TEST_FAST_REQUIRE_EPMD:-}" = "1" ] && ! epmd -names >/dev/null 2>&1; then
      exit 19
    fi

    if [ "${TEST_FAST_FAIL_PARTITION:-}" = "$partition" ]; then
      exit 17
    fi

    trap 'exit 0' INT TERM

    while [ ! -e "${TEST_FAST_ACCEPTANCE_DIR}/release" ] &&
          [ "${TEST_FAST_RELEASE:-}" != "1" ]; do
      sleep 0.02
    done

    if [ "${TEST_FAST_SUMMARY_PARTITION:-}" = "$partition" ]; then
      printf '%s\\n' "${TEST_FAST_SUMMARY:-}"
    else
      echo "Result: 1 passed"
    fi
    """)

    File.chmod!(helper_path, 0o700)

    on_exit(fn ->
      File.touch(release_path)
      File.rm_rf!(directory)
    end)

    %{directory: directory, helper_path: helper_path, release_path: release_path}
  end

  defp available_tcp_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, {_address, port}} = :inet.sockname(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  defp run_make(fixture, partitions, extra_env \\ []) do
    System.cmd(
      "make",
      ["--no-print-directory", "test-fast", "N=#{partitions}"],
      cd: File.cwd!(),
      env: make_env(fixture, extra_env),
      stderr_to_stdout: true
    )
  end

  defp open_make_port(fixture, partitions, "INT") do
    make = System.find_executable("make")

    Port.open(
      {:spawn_executable, "/usr/bin/script"},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: script_args(make, partitions),
        cd: File.cwd!(),
        env:
          Enum.map(make_env(fixture, []), fn
            {key, nil} -> {to_charlist(key), false}
            {key, value} -> {to_charlist(key), to_charlist(value)}
          end)
      ]
    )
  end

  defp open_make_port(fixture, partitions, "TERM") do
    Port.open(
      {:spawn_executable, System.find_executable("make")},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["--no-print-directory", "test-fast", "N=#{partitions}"],
        cd: File.cwd!(),
        env:
          Enum.map(make_env(fixture, []), fn
            {key, nil} -> {to_charlist(key), false}
            {key, value} -> {to_charlist(key), to_charlist(value)}
          end)
      ]
    )
  end

  defp script_args(make, partitions) do
    make_args = ["--no-print-directory", "test-fast", "N=#{partitions}"]

    case :os.type() do
      {:unix, :darwin} ->
        ["-q", "-e", "/dev/null", make | make_args]

      {:unix, _name} ->
        ["-q", "-e", "-c", Enum.join(["exec", make | make_args], " "), "/dev/null"]
    end
  end

  defp interrupt_port(port, "INT") do
    true = Port.command(port, <<3>>)
  end

  defp interrupt_port(port, "TERM") do
    {:os_pid, os_pid} = Port.info(port, :os_pid)
    {_output, 0} = signal_term(os_pid)
  end

  # The recipe generates its own run namespace and gives each child its own
  # partition, candidate file and file durations export. Clearing the invoking
  # test run's values keeps this acceptance identical with or without a
  # namespace, which every focused run sets, and when `make test-fast` itself
  # runs this module: a tooling partition has its own export file, and the CI
  # step sets TEST_FAST_PRINT_FILE_DURATIONS for every make it runs.
  defp make_env(fixture, extra_env) do
    extra = Enum.map(extra_env, fn {key, value} -> {Atom.to_string(key), value} end)

    cleared =
      for key <- ~w(CODEX_POOLER_TEST_RUN_NAMESPACE MIX_TEST_PARTITION CODEX_POOLER_TEST_DURATION_CANDIDATES CODEX_POOLER_TEST_FILE_DURATIONS TEST_FAST_PRINT_FILE_DURATIONS),
          not List.keymember?(extra, key, 0),
          do: {key, nil}

    cleared ++
      [
        {"TEST_FAST_ACCEPTANCE_DIR", fixture.directory},
        {"TEST_FAST_COMMAND", "#{fixture.helper_path} run"},
        {"TEST_FAST_DROP_COMMAND", "#{fixture.helper_path} drop"}
        | extra
      ]
  end

  defp signal_term(pid) when is_integer(pid) do
    System.cmd("/bin/sh", ["-c", "kill -TERM \"$1\"", "test-fast", Integer.to_string(pid)])
  end

  defp await_receipts!(directory, prefix, expected, timeout_ms \\ @receipt_detection_timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    await_receipts_until!(directory, prefix, expected, deadline)
  end

  defp await_receipts_until!(directory, prefix, expected, deadline) do
    receipts =
      directory
      |> File.ls!()
      |> Enum.filter(&String.starts_with?(&1, prefix))
      |> Enum.sort()

    if length(receipts) == expected do
      receipts
    else
      remaining_ms = deadline - System.monotonic_time(:millisecond)

      if remaining_ms > 0 do
        receive do
        after
          min(@receipt_poll_interval_ms, remaining_ms) ->
            await_receipts_until!(directory, prefix, expected, deadline)
        end
      else
        flunk("expected #{expected} #{prefix} receipts in #{directory}")
      end
    end
  end

  defp assert_namespace_partitions(receipts, namespace_count, partitions) do
    parsed =
      Enum.map(receipts, fn receipt ->
        [namespace, partition] =
          receipt
          |> String.replace_prefix("run-", "")
          |> String.split("-", parts: 2)

        assert namespace =~ ~r/^[0-9a-f]{16}$/
        {namespace, String.to_integer(partition)}
      end)

    assert parsed |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == namespace_count

    assert parsed
           |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
           |> Map.values()
           |> Enum.all?(&(Enum.sort(&1) == Enum.to_list(partitions)))
  end

  defp confirm_rounds(fixture, namespace) do
    fixture.directory |> Path.join("confirm-#{namespace}") |> File.read!() |> String.split("\n", trim: true)
  end

  defp rename_receipts(receipts, from, to) do
    receipts
    |> Enum.map(&String.replace_prefix(&1, from, to))
    |> MapSet.new()
  end

  defp collect_port(port, output \\ "") do
    receive do
      {^port, {:data, data}} ->
        collect_port(port, output <> data)

      {^port, {:exit_status, exit_code}} ->
        {output, exit_code}
    after
      @timeout_ms ->
        terminate_port(port)
        Port.close(port)
        flunk("timed out waiting for make test-fast")
    end
  end

  defp terminate_port(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, os_pid} ->
        {_output, 0} = signal_term(os_pid)
        :ok

      nil ->
        :ok
    end
  end
end
