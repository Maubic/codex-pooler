defmodule CodexPooler.Accounting.ExecutionRecoveryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.ExecutionRecovery
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProofs}
  alias CodexPooler.Platform.InstancePresence.Identity

  @detection_timeout_ms 5_000

  test "startup recovers a committed proof missed by notifications without recovering a live executor" do
    setup = accounting_setup()
    {dead_pid, dead_request, dead_attempt} = start_attempt!(setup, :dead_executor)
    {_live_pid, live_request, live_attempt} = start_attempt!(setup, :live_executor)
    dead_monitor = Process.monitor(dead_pid)
    send(dead_pid, :finish)
    assert_receive {:DOWN, ^dead_monitor, :process, ^dead_pid, :normal}, @detection_timeout_ms
    :ok = CodexPooler.ExecutionProofSupport.publish_terminal!(dead_attempt)
    assert Repo.reload!(dead_request).status == "in_progress"
    refute ExecutionTerminalProofs.terminal?(live_attempt)

    runner = start_supervised!({ExecutionRecovery, enabled: true, name: nil, interval_ms: 60_000})
    :sys.get_state(runner)

    assert %{status: "failed", last_error_code: "dead_execution_recovered", usage_status: "usage_unknown"} = Repo.reload!(dead_request)
    assert Repo.reload!(dead_attempt).status == "failed"
    assert Repo.reload!(live_request).status == "in_progress"
    assert Repo.reload!(live_attempt).status == "in_progress"
    assert ledger_kinds(dead_request.id) == ["release", "reservation", "settlement"]
    assert ledger_kinds(live_request.id) == ["reservation"]

    send(runner, :recover)
    :sys.get_state(runner)
    assert ledger_kinds(dead_request.id) == ["release", "reservation", "settlement"]
  end

  for reason <- [:client_disconnected, :owner_drained, :websocket_terminated] do
    test "prompt proof recovery leaves a caller-stopped #{reason} attempt to its authoritative cleanup" do
      setup = accounting_setup()
      {pid, request, attempt} = start_attempt!(setup, :cancelled_executor)
      monitor = Process.monitor(pid)
      Process.exit(pid, {:shutdown, unquote(reason)})
      assert_receive {:DOWN, ^monitor, :process, ^pid, {:shutdown, unquote(reason)}}, @detection_timeout_ms
      :ok = CodexPooler.ExecutionProofSupport.publish_terminal!(attempt)

      runner = start_supervised!({ExecutionRecovery, enabled: true, name: nil, interval_ms: 60_000})
      :sys.get_state(runner)
      assert Repo.reload!(request).status == "in_progress"
      assert Repo.reload!(attempt).status == "in_progress"
      assert ledger_kinds(request.id) == ["reservation"]
    end
  end

  test "a live waiting HTTP admission is untouched and its actual exit releases it after proof publication" do
    setup = accounting_setup()
    parent = self()

    executor =
      start_supervised!(
        {Task,
         fn ->
           instance = Identity.local()
           execution = ExecutionIdentity.local() |> Map.put(:owner_instance_id, instance.node_name) |> Map.put(:owner_instance_boot_id, instance.boot_id)

           {:ok, %{request: request}} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{transport: "http_sse", admission_execution: execution})
           send(parent, {:admitted, request, execution})

           receive do
             :finish -> :ok
           end
         end}
      )

    assert_receive {:admitted, request, identity}, @detection_timeout_ms
    runner = start_supervised!({ExecutionRecovery, enabled: true, name: nil, interval_ms: 60_000})
    :sys.get_state(runner)
    assert Repo.reload!(request).status == "in_progress"
    assert ledger_kinds(request.id) == ["reservation"]

    monitor = Process.monitor(executor)
    send(executor, :finish)
    assert_receive {:DOWN, ^monitor, :process, ^executor, :normal}, @detection_timeout_ms
    assert :dead = ExecutionIdentity.status(identity)
    [proof] = Enum.filter(ExecutionRegistry.pending(10_000), &(&1.owner_execution_id == identity.owner_execution_id))
    assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
    assert :ok = ExecutionRegistry.acknowledge([identity.owner_execution_id])
    ExecutionRecovery.publish_committed([proof], runner)

    await_closed!(request.id, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    assert %{status: "failed", last_error_code: "admission_execution_recovered", usage_status: "not_applicable"} = Repo.reload!(request)
    assert ledger_kinds(request.id) == ["release", "reservation"]
    assert Repo.aggregate(from(a in Accounting.Attempt, where: a.request_id == ^request.id), :count) == 0

    send(runner, :recover)
    :sys.get_state(runner)
    assert ledger_kinds(request.id) == ["release", "reservation"]
  end

  defp start_attempt!(setup, id) do
    parent = self()

    pid =
      start_supervised!(
        {Task,
         fn ->
           {:ok, %{request: request}} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{transport: "websocket"})
           {:ok, attempt} = Accounting.create_attempt(request, setup.assignment)
           send(parent, {:attempt, id, request, attempt})

           receive do
             :finish -> :ok
           end
         end},
        id: id
      )

    assert_receive {:attempt, ^id, request, attempt}, @detection_timeout_ms
    {pid, request, attempt}
  end

  defp ledger_kinds(id), do: id |> Accounting.list_ledger_entries_for_request() |> Enum.map(& &1.entry_kind) |> Enum.sort()

  defp await_closed!(id, deadline) do
    if Repo.get!(Accounting.Request, id).status == "in_progress" do
      assert System.monotonic_time(:millisecond) < deadline, "ended admission still holds its reservation"

      receive do
      after
        10 -> await_closed!(id, deadline)
      end
    end
  end
end
