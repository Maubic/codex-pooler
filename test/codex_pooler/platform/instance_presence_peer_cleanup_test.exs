defmodule CodexPooler.InstancePresencePeerCleanupTest do
  use ExUnit.Case, async: true

  alias CodexPooler.InstancePresencePeer

  # findings#207 row 207-47: the wave introduced two rules that R1 and R3 of the
  # same review showed are easy to get wrong -- the cleanup timeout must outlast
  # the detection budget, and a peer's backends must be ended before its rows are
  # deleted. Both now live in one helper each, and these pin the helpers' own
  # behaviour rather than restating the convention.
  test "a cleanup timeout always outlasts both detection budgets it may wait on" do
    for budget <- [1, 10, 1_000, 15_000, 120_000] do
      timeout = InstancePresencePeer.cleanup_timeout_ms(budget)

      assert timeout > budget * 2,
             "a cleanup that may wait on two #{budget}ms budgets needs more than #{budget * 2}ms"
    end

    for invalid <- [0, -1, nil, 15_000.0, "15000"] do
      assert_raise FunctionClauseError, fn ->
        InstancePresencePeer.cleanup_timeout_ms(invalid)
      end
    end
  end

  test "peer state purge proves the peer's backends ended before deleting its rows" do
    {:ok, calls} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)
    record = fn step -> Agent.update(calls, &(&1 ++ [step])) end

    assert :ok =
             InstancePresencePeer.purge_peer_state!(
               "boot-id",
               fn -> record.(:delete) end,
               budget_ms: 1,
               terminate: fn "boot-id" ->
                 record.(:terminate)
                 :ok
               end,
               await: fn "boot-id", 1 ->
                 record.(:await)
                 :ok
               end
             )

    assert Agent.get(calls, & &1) == [:terminate, :await, :delete]
  end

  test "peer state purge never deletes rows when the backends cannot be ended" do
    {:ok, calls} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)

    assert_raise MatchError, fn ->
      InstancePresencePeer.purge_peer_state!(
        "boot-id",
        fn -> Agent.update(calls, &(&1 ++ [:delete])) end,
        terminate: fn _boot_id -> :error end
      )
    end

    assert Agent.get(calls, & &1) == []
  end

  test "peer state purge never deletes rows while backend absence is unproven" do
    {:ok, calls} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(calls), do: Agent.stop(calls) end)

    assert_raise MatchError, fn ->
      InstancePresencePeer.purge_peer_state!(
        "boot-id",
        fn -> Agent.update(calls, &(&1 ++ [:delete])) end,
        terminate: fn _boot_id -> :ok end,
        await: fn _boot_id, _budget -> :error end
      )
    end

    assert Agent.get(calls, & &1) == []
  end

  test "the peer application name is the one the termination predicate matches" do
    assert InstancePresencePeer.peer_application_name("boot-id") == "execution_peer_boot-id"
  end

  test "OS absence waits past peer termination until the kernel PID disappears" do
    {:ok, samples} =
      Agent.start_link(fn -> [{"", 0}, {"", 0}, {"kill: 123: No such process\n", 1}] end)

    on_exit(fn -> if Process.alive?(samples), do: Agent.stop(samples) end)

    probe = fn "owned-pid" ->
      Agent.get_and_update(samples, fn [next | rest] -> {next, rest} end)
    end

    assert :ok = InstancePresencePeer.assert_os_process_absent!("owned-pid", probe: probe)
    assert Agent.get(samples, & &1) == []
  end

  test "a surviving PID still fails instead of accepting a clean peer exit" do
    assert_raise ExUnit.AssertionError, ~r/owned peer OS process survived/, fn ->
      InstancePresencePeer.assert_os_process_absent!("owned-pid",
        budget_ms: 0,
        probe: fn _ -> {"", 0} end
      )
    end
  end

  for result <- [
        {"kill: 123: Operation not permitted\n", 1},
        {"kill: 123: Permission denied\n", 1},
        {"/usr/bin/kill: (123): Operation not permitted\n", 1},
        {"/bin/kill: (123): Permission denied\n", 1},
        {"unknown failure", 2},
        {"", 1}
      ] do
    @tag probe_result: result
    test "probe error #{inspect(result)} never establishes absence", %{probe_result: result} do
      assert InstancePresencePeer.classify_os_process_probe(result) == :unknown

      assert_raise ExUnit.AssertionError, ~r/owned peer OS process survived/, fn ->
        InstancePresencePeer.assert_os_process_absent!("owned-pid",
          budget_ms: 0,
          probe: fn _ -> result end
        )
      end
    end
  end

  test "only explicit ESRCH diagnostics establish absence" do
    for output <- [
          "kill: 123: No such process\n",
          "kill: (123): No such process\n",
          "/usr/bin/kill: (123): No such process\n",
          "/bin/kill: (123): No such process\n",
          "123: no such process\n"
        ] do
      assert InstancePresencePeer.classify_os_process_probe({output, 1}) == :absent
    end
  end

  test "captures the exact owned process start identity" do
    snapshot =
      {:present, %{source: :proc, state: "S", parent_pid: 42, start_signature: "123456"}}

    assert %{pid: "owned-pid", source: :proc, start_signature: "123456"} =
             InstancePresencePeer.capture_os_process_identity!("owned-pid",
               probe: fn _ -> snapshot end
             )
  end

  test "an inspection-denied environment cannot certify the owned process identity" do
    assert_raise ExUnit.AssertionError, ~r/owned peer OS process identity unavailable/, fn ->
      InstancePresencePeer.capture_os_process_identity!("owned-pid", probe: fn _ -> {:error, :ps_process_unknown} end)
    end

    assert %{pid: pid, source: source, start_signature: signature} = InstancePresencePeer.capture_os_process_identity!(System.pid())
    assert pid == System.pid()
    assert source in [:ps, :proc]
    assert byte_size(signature) > 0
  end

  test "owned process stop accepts absence, PID reuse, and a same-identity zombie" do
    identity = %{pid: "owned-pid", source: :proc, start_signature: "123456"}

    for result <- [
          :absent,
          {:present, %{source: :proc, state: "S", parent_pid: 1, start_signature: "999999"}},
          {:present, %{source: :proc, state: "Z", parent_pid: 1, start_signature: "123456"}}
        ] do
      assert :ok =
               InstancePresencePeer.assert_os_process_stopped!(identity,
                 budget_ms: 0,
                 probe: fn _ -> result end
               )
    end
  end

  test "owned process stop fails closed for a live identity and unknown inspection" do
    identity = %{pid: "owned-pid", source: :proc, start_signature: "123456"}

    for result <- [
          {:present, %{source: :proc, state: "S", parent_pid: 1, start_signature: "123456"}},
          {:present, %{source: :ps, state: "S", parent_pid: 1, start_signature: "same time"}},
          {:error, :permission_denied}
        ] do
      assert_raise ExUnit.AssertionError, ~r/owned peer OS process remained/, fn ->
        InstancePresencePeer.assert_os_process_stopped!(identity,
          budget_ms: 0,
          probe: fn _ -> result end
        )
      end
    end
  end

  test "parses Linux proc stat using state, parent, and start-time fields" do
    stat =
      "123 (beam.smp worker (owned)) Z 42 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 987654 0 0\n"

    assert {:present, %{source: :proc, state: "Z", parent_pid: 42, start_signature: "987654"}} =
             InstancePresencePeer.parse_linux_process_stat(stat)

    assert {:error, :invalid_proc_stat} =
             InstancePresencePeer.parse_linux_process_stat("invalid")
  end

  test "parses portable ps output without changing the backend identity" do
    output = "S    Tue Sep 15 02:33:48 2026     42\n"

    assert {:present,
            %{
              source: :ps,
              state: "S",
              parent_pid: 42,
              start_signature: "Tue Sep 15 02:33:48 2026"
            }} = InstancePresencePeer.parse_portable_process_output(output)

    assert {:error, :invalid_ps_output} =
             InstancePresencePeer.parse_portable_process_output("invalid")
  end
end
