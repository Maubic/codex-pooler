defmodule CodexPooler.Platform.ProducerExecutionRegistryCoexistenceTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Platform.{ExecutionProofPublisher, ExecutionRegistry, ExecutionTerminalProof}

  @registry :producer_execution_coexistence_registry
  @budget 2_000

  test "distinct accounting and producer actors preserve subscriber and pending contracts" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    assert :ok = ExecutionRegistry.subscribe(registry)
    accounting = start_actor!(registry, :accounting)
    producer = start_actor!(registry, :producer)
    assert accounting.pid != producer.pid
    assert accounting.id != producer.id
    assert ExecutionRegistry.status(accounting.id, accounting.pid, registry) == :alive
    assert ExecutionRegistry.status(producer.id, producer.pid, registry) == :alive
    assert ExecutionRegistry.pending(10, registry) == []

    send(producer.pid, :complete)
    assert_receive {:actor_completed, producer_id, :unknown}
    assert producer_id == producer.id
    assert ExecutionRegistry.status(producer.id, producer.pid, registry) == :alive
    assert ExecutionRegistry.mark_interruption(producer.pid, "client_disconnected", registry) == :unknown
    send(accounting.pid, :complete)
    assert_receive {:actor_completed, accounting_id, :ok}
    assert accounting_id == accounting.id
    assert_receive {:publish_early, ^accounting_id}
    refute_receive {:publish_early, ^producer_id}
    assert Process.alive?(accounting.pid)
    assert [%{owner_execution_id: ^accounting_id, end_kind: "completed"}] = ExecutionRegistry.pending(10, registry)
    assert ExecutionRegistry.pending_proofs([producer_id], registry) == []

    stop_actor!(producer)
    assert ExecutionRegistry.status(producer.id, producer.pid, registry) == :dead
    assert [%{owner_execution_id: ^accounting_id}] = ExecutionRegistry.pending_proofs([producer_id, accounting_id], registry)
    refute_receive {:publish_early, ^producer_id}
    assert :ok = ExecutionRegistry.acknowledge([producer_id], registry)
    assert [%{owner_execution_id: ^accounting_id}] = ExecutionRegistry.pending(10, registry)
    assert :ok = ExecutionRegistry.acknowledge([accounting_id], registry)
    assert ExecutionRegistry.pending(10, registry) == []
  end

  @tag slow: "real publisher shutdown flush with distinct monitored accounting and producer processes"
  test "shutdown flush persists only accounting when both actor kinds ended" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    publisher = start_supervised!({ExecutionProofPublisher, enabled: true, name: nil, registry: registry, interval_ms: 60_000})
    :sys.get_state(publisher)
    accounting = start_actor!(registry, :accounting)
    producer = start_actor!(registry, :producer)
    :ok = :sys.suspend(publisher)
    on_exit(fn -> if Process.alive?(publisher), do: :sys.resume(publisher) end)
    stop_actor!(accounting)
    stop_actor!(producer)
    assert :dead = ExecutionRegistry.status(accounting.id, accounting.pid, registry)
    assert :dead = ExecutionRegistry.status(producer.id, producer.pid, registry)
    assert is_nil(Repo.get(ExecutionTerminalProof, accounting.id))
    assert is_nil(Repo.get(ExecutionTerminalProof, producer.id))
    flush = Task.async(fn -> ExecutionProofPublisher.flush(@budget, publisher) end)
    await_flush_queued!(publisher, System.monotonic_time(:millisecond) + @budget)
    :ok = :sys.resume(publisher)
    assert :ok = Task.await(flush, @budget)
    assert %ExecutionTerminalProof{execution_id: id, end_kind: "process_down"} = Repo.get!(ExecutionTerminalProof, accounting.id)
    assert id == accounting.id
    assert is_nil(Repo.get(ExecutionTerminalProof, producer.id))
    assert ExecutionRegistry.pending(10, registry) == []
    assert ExecutionRegistry.pending_proofs([producer.id], registry) == []
  end

  test "registry replacement makes both still-live actor identities unknown" do
    registry = start_supervised!({ExecutionRegistry, name: @registry})
    accounting = start_actor!(@registry, :accounting)
    producer = start_actor!(@registry, :producer)
    monitor = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^registry, :killed}
    replacement = await_replacement!(registry, System.monotonic_time(:millisecond) + @budget)
    assert replacement != registry
    assert Process.alive?(accounting.pid) and Process.alive?(producer.pid)
    assert ExecutionRegistry.status(accounting.id, accounting.pid, @registry) == :unknown
    assert ExecutionRegistry.status(producer.id, producer.pid, @registry) == :unknown
    assert ExecutionRegistry.pending(10, @registry) == []
    assert :ok = ExecutionRegistry.subscribe(@registry)
    later = start_actor!(@registry, :accounting)
    send(later.pid, :complete)
    assert_receive {:actor_completed, later_id, :ok}
    assert later_id == later.id
    assert_receive {:publish_early, ^later_id}
    assert [%{owner_execution_id: ^later_id}] = ExecutionRegistry.pending(10, @registry)
  end

  test "producer tombstone expiry stays unknown without expiring accounting proof" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    accounting = start_actor!(registry, :accounting)
    producer = start_actor!(registry, :producer)
    stop_actor!(producer)
    assert ExecutionRegistry.status(producer.id, producer.pid, registry) == :dead
    send(registry, {:expire, producer.id})
    assert ExecutionRegistry.status(producer.id, producer.pid, registry) == :unknown
    assert ExecutionRegistry.status(accounting.id, accounting.pid, registry) == :alive
    send(accounting.pid, :complete)
    assert_receive {:actor_completed, accounting_id, :ok}
    assert accounting_id == accounting.id
    assert [%{owner_execution_id: ^accounting_id}] = ExecutionRegistry.pending(10, registry)
    assert ExecutionRegistry.pending_proofs([producer.id], registry) == []
    assert :ok = ExecutionRegistry.retire_ended(registry)
    assert [%{owner_execution_id: ^accounting_id}] = ExecutionRegistry.pending(10, registry)
  end

  defp start_actor!(registry, kind) do
    parent = self()
    pid = start_supervised!(Supervisor.child_spec({Task, fn -> actor_start(parent, registry, kind) end}, id: make_ref(), restart: :temporary))
    assert_receive {:actor_registered, ^pid, ^kind, id, :ok}
    %{pid: pid, id: id}
  end

  defp actor_start(parent, registry, kind) do
    id = Ecto.UUID.generate()

    result =
      case kind do
        :accounting -> ExecutionRegistry.register(id, registry)
        :producer -> ExecutionRegistry.register_producer(id, registry)
      end

    send(parent, {:actor_registered, self(), kind, id, result})
    actor_loop(parent, registry, id)
  end

  defp actor_loop(parent, registry, id) do
    receive do
      :complete ->
        send(parent, {:actor_completed, id, ExecutionRegistry.complete(id, registry)})
        actor_loop(parent, registry, id)

      :stop ->
        :ok
    end
  end

  defp stop_actor!(%{pid: pid}) do
    ref = Process.monitor(pid)
    send(pid, :stop)
    assert_receive {:DOWN, ^ref, :process, ^pid, :normal}
  end

  defp await_flush_queued!(publisher, deadline) do
    {:messages, messages} = Process.info(publisher, :messages)

    if Enum.any?(messages, &match?({:"$gen_call", _, :flush}, &1)) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(5)
      await_flush_queued!(publisher, deadline)
    end
  end

  defp await_replacement!(previous, deadline) do
    case Process.whereis(@registry) do
      pid when is_pid(pid) and pid != previous ->
        pid

      _ ->
        assert System.monotonic_time(:millisecond) < deadline
        Process.sleep(5)
        await_replacement!(previous, deadline)
    end
  end
end
