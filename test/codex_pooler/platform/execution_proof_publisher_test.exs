defmodule CodexPooler.Platform.ExecutionProofPublisherTest do
  # findings#283: an execution that ends `process_down`, without delivering its
  # result, is the executor a client's resend of its turn waits on, so its
  # registry asks the publisher to write its proof within 100 ms instead of
  # at the next tick. Each test pairs its own registry with its own publisher,
  # whose tick is far beyond the test: only an early publication can write a
  # proof, and the global registry other tests use is left alone.
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog

  alias CodexPooler.ExecutionProofSupport
  alias CodexPooler.Platform.{ExecutionProofPublisher, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Platform.InstancePresence.Identity

  @tick_ms 60_000
  # One fixed name: the file is synchronous, so no two tests hold it at once.
  @registry :execution_proof_publisher_test_registry
  @detection_timeout_ms 2_000

  test "an execution that ends without delivering is published without waiting for the tick" do
    {registry, _publisher} = start_pair!()
    execution = start_execution!(registry)

    Process.exit(execution.pid, :kill)

    :ok = await_published!(execution.identity)
    assert %ExecutionTerminalProof{end_kind: "process_down"} = Repo.get!(ExecutionTerminalProof, execution.identity.owner_execution_id)
  end

  # A backlog of older pending proofs (retained through a database outage, or
  # left by earlier tests in this VM, where the application's publisher is
  # off) does not hold the early publication back: it writes the execution
  # that asked for it and leaves the backlog to the tick, which writes the
  # oldest hundred at a time. It used to write the oldest hundred, so the
  # asking execution waited one tick per hundred older proofs (Drone 1707).
  test "an execution that ends without delivering is published ahead of an older backlog" do
    {registry, publisher} = start_pair!()

    for _ <- 1..250 do
      id = Ecto.UUID.generate()
      :ok = ExecutionRegistry.register(id, registry)
      :ok = ExecutionRegistry.complete(id, registry)
    end

    execution = start_execution!(registry)
    Process.exit(execution.pid, :kill)

    :ok = await_published!(execution.identity)
    assert %{early: nil, early_ids: []} = :sys.get_state(publisher)
    assert length(ExecutionRegistry.pending(10_000, registry)) == 250
  end

  test "an execution that completes waits for the tick" do
    {registry, publisher} = start_pair!()
    execution = start_execution!(registry)

    # `complete/2` returns once the registry retired the execution, so any
    # request for an early publication is already in the publisher's mailbox.
    send(execution.pid, :complete)
    assert_receive {:completed, _id}
    assert %{early: nil} = :sys.get_state(publisher)
    refute ExecutionTerminalProofs.terminal?(execution.identity)

    # The tick still publishes it.
    :ok = ExecutionProofSupport.await_terminal!(execution.identity, publisher)
    assert %ExecutionTerminalProof{end_kind: "completed"} = Repo.get!(ExecutionTerminalProof, execution.identity.owner_execution_id)
  end

  # A burst of ends shares publications: each execution here ends only after
  # the publisher has read the previous end's request, which without the
  # shared window would cost one transaction per execution. The fifty ends
  # take a few milliseconds, well inside one window.
  test "executions ending one after another share a publication" do
    {registry, publisher} = start_pair!()
    executions = for _ <- 1..50, do: start_execution!(registry)
    publications = count_publications(publisher)

    for execution <- executions do
      Process.exit(execution.pid, :kill)
      assert :dead = ExecutionRegistry.status(execution.identity.owner_execution_id, execution.pid, registry)
      :sys.get_state(publisher)
    end

    Enum.each(executions, &(:ok = await_published!(&1.identity)))
    assert publications.() in 1..2
  end

  test "an end brings no publication forward while publication is failing" do
    {registry, publisher} = start_pair!()

    # A stored proof that disagrees with a pending one fails every publication.
    poisoned = start_execution!(registry)
    {id, identity} = Map.pop(poisoned.identity, :owner_execution_id)
    Repo.insert_all(ExecutionTerminalProof, [Map.merge(identity, %{execution_id: id, end_kind: "process_down", ended_at: DateTime.add(DateTime.utc_now(), -60, :second)})])

    log =
      capture_log(fn ->
        send(poisoned.pid, :complete)
        assert_receive {:completed, _id}
        send(publisher, :publish)
        assert %{failed: true} = :sys.get_state(publisher)
      end)

    assert log =~ "execution terminal proof publication unavailable; pending proofs retained"

    execution = start_execution!(registry)
    Process.exit(execution.pid, :kill)
    assert :dead = ExecutionRegistry.status(execution.identity.owner_execution_id, execution.pid, registry)
    assert %{early: nil, failed: true} = :sys.get_state(publisher)
  end

  # The publisher renews its subscription at every publication, so a registry
  # its supervisor restarted, which knows no subscriber, asks for early
  # publications again after the publisher's next tick.
  test "a restarted registry asks for early publications again after the next tick" do
    registry = start_supervised!({ExecutionRegistry, name: @registry})
    publisher = start_supervised!({ExecutionProofPublisher, enabled: true, name: nil, registry: @registry, interval_ms: @tick_ms})
    :sys.get_state(publisher)

    ref = Process.monitor(registry)
    Process.exit(registry, :kill)
    assert_receive {:DOWN, ^ref, :process, ^registry, :killed}
    :ok = await_restarted!(registry, System.monotonic_time(:millisecond) + @detection_timeout_ms)

    send(publisher, :publish)
    :sys.get_state(publisher)
    execution = start_execution!(@registry)
    Process.exit(execution.pid, :kill)

    :ok = await_published!(execution.identity)
  end

  defp await_restarted!(previous, deadline) do
    case Process.whereis(@registry) do
      pid when is_pid(pid) and pid != previous ->
        :ok

      _absent_or_previous ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("the registry was not restarted")

        receive do
        after
          5 -> await_restarted!(previous, deadline)
        end
    end
  end

  defp start_pair! do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    publisher = start_supervised!({ExecutionProofPublisher, enabled: true, name: nil, registry: registry, interval_ms: @tick_ms})
    # Its first publication, which subscribes it to the registry, runs before
    # it reads any message.
    :sys.get_state(publisher)
    {registry, publisher}
  end

  # One execution registered from its own process, as a response task
  # registers the one it runs; `:complete` retires it as delivered.
  defp start_execution!(registry) do
    test_pid = self()
    id = Ecto.UUID.generate()

    pid =
      spawn(fn ->
        :ok = ExecutionRegistry.register(id, registry)
        send(test_pid, {:registered, id})

        receive do
          :complete ->
            :ok = ExecutionRegistry.complete(id, registry)
            send(test_pid, {:completed, id})
        end
      end)

    assert_receive {:registered, ^id}
    local = Identity.local()

    %{
      pid: pid,
      identity: %{
        owner_execution_id: id,
        owner_instance_id: local.node_name,
        owner_instance_boot_id: local.boot_id,
        owner_process_id: List.to_string(:erlang.pid_to_list(pid))
      }
    }
  end

  defp await_published!(identity),
    do: await_published(identity, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_published(identity, deadline) do
    cond do
      ExecutionTerminalProofs.terminal?(identity) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("execution terminal proof was not published before the publisher's tick")

      true ->
        receive do
        after
          5 -> await_published(identity, deadline)
        end
    end
  end

  # Publications that wrote proofs: the publisher's inserts into the table.
  defp count_publications(publisher) do
    handler = {__MODULE__, make_ref()}
    config = %{publisher: publisher, test_pid: self(), handler: handler}
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.observe_query/4, config)
    on_exit(fn -> :telemetry.detach(handler) end)
    fn -> drain_count(handler, 0) end
  end

  @doc false
  def observe_query(_event, _measurements, %{source: "execution_terminal_proofs", query: "INSERT" <> _}, %{publisher: publisher} = config)
      when self() == publisher,
      do: send(config.test_pid, {:proof_insert, config.handler})

  def observe_query(_event, _measurements, _metadata, _config), do: :ok

  defp drain_count(handler, count) do
    receive do
      {:proof_insert, ^handler} -> drain_count(handler, count + 1)
    after
      0 -> count
    end
  end
end
