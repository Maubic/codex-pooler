defmodule CodexPooler.Platform.ProducerExecutionRegistryTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Platform.ExecutionRegistry

  test "producer identity is immutable and cannot complete, publish or consume accounting capacity" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    assert :ok = ExecutionRegistry.subscribe(registry)
    parent = self()
    id = Ecto.UUID.generate()

    producer =
      spawn(fn ->
        send(parent, {:registered, ExecutionRegistry.register_producer(id, registry)})
        send(parent, {:reused, ExecutionRegistry.register_producer(id, registry)})
        send(parent, {:purpose_change, ExecutionRegistry.register(id, registry)})
        send(parent, {:live_complete, ExecutionRegistry.complete(id, registry)})
        receive do: (:stop -> :ok)
      end)

    on_exit(fn -> if Process.alive?(producer), do: Process.exit(producer, :kill) end)
    assert_receive {:registered, :ok}
    assert_receive {:reused, :ok}
    assert_receive {:purpose_change, :unknown}
    assert_receive {:live_complete, :unknown}
    assert ExecutionRegistry.status(id, producer, registry) == :alive
    assert ExecutionRegistry.register_producer(id, registry) == :unknown
    assert ExecutionRegistry.mark_interruption(producer, "client_disconnected", registry) == :unknown
    monitor = Process.monitor(producer)
    send(producer, :stop)
    assert_receive {:DOWN, ^monitor, :process, ^producer, :normal}
    assert ExecutionRegistry.status(id, producer, registry) == :dead
    assert ExecutionRegistry.retire_ended(registry) == :ok
    assert ExecutionRegistry.pending(100, registry) == []
    assert ExecutionRegistry.pending_proofs([id], registry) == []
    refute_receive {:publish_early, ^id}, 20

    send(registry, {:expire, id})
    assert ExecutionRegistry.status(id, producer, registry) == :unknown
    state = :sys.get_state(registry)
    refute Map.has_key?(state.purposes, id)
  end

  test "default accounting registration publishes after a producer purpose change is refused" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    assert :ok = ExecutionRegistry.subscribe(registry)
    id = Ecto.UUID.generate()
    assert ExecutionRegistry.register(id, registry) == :ok
    assert ExecutionRegistry.register_producer(id, registry) == :unknown
    assert ExecutionRegistry.mark_interruption(self(), "client_disconnected", registry) == :ok
    assert ExecutionRegistry.complete(id, registry) == :ok
    assert_receive {:publish_early, ^id}
    [proof] = ExecutionRegistry.pending(100, registry)
    assert proof.owner_execution_id == id
    assert proof.interruption_code == "client_disconnected"
    assert proof.end_kind == "completed"
    assert ExecutionRegistry.status(id, self(), registry) == :dead
    assert ExecutionRegistry.acknowledge([id], registry) == :ok
    assert ExecutionRegistry.pending(100, registry) == []
  end
end
