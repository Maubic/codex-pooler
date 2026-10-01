defmodule CodexPooler.Platform.ExecutionTerminalProofsTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProofs}
  alias CodexPooler.Platform.InstancePresence.Identity

  test "registry completion creates an immutable exact proof before any attempt exists" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    id = Ecto.UUID.generate()
    assert :ok = ExecutionRegistry.register(id, registry)
    assert :ok = ExecutionRegistry.complete(id, registry)
    assert [proof] = ExecutionRegistry.pending(100, registry)
    assert proof.owner_execution_id == id
    assert proof.end_kind == "completed"
    assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
    assert ExecutionTerminalProofs.terminal?(proof)
    assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])

    for field <- [
          :owner_execution_id,
          :owner_instance_id,
          :owner_instance_boot_id,
          :owner_process_id
        ] do
      refute ExecutionTerminalProofs.terminal?(Map.put(proof, field, Ecto.UUID.generate()))
    end

    conflicting = %{proof | end_kind: "process_down"}

    assert {:error, :execution_terminal_proof_conflict} =
             ExecutionTerminalProofs.publish([conflicting])

    assert Repo.aggregate("execution_terminal_proofs", :count) == 1
    assert Repo.one(from p in "execution_terminal_proofs", select: p.end_kind) == "completed"
    assert :ok = ExecutionRegistry.acknowledge([id], registry)
    assert [] = ExecutionRegistry.pending(100, registry)
  end

  test "process death produces a terminal proof and another live execution stays unknown" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    parent = self()
    id = Ecto.UUID.generate()

    task =
      start_supervised!(
        {Task,
         fn ->
           :ok = ExecutionRegistry.register(id, registry)
           send(parent, :registered)

           receive do
             :finish -> :ok
           end
         end}
      )

    assert_receive :registered
    assert [] = ExecutionRegistry.pending(100, registry)
    live_id = Ecto.UUID.generate()
    :ok = ExecutionRegistry.register(live_id, registry)
    monitor = Process.monitor(task)
    send(task, :finish)
    assert_receive {:DOWN, ^monitor, :process, ^task, :normal}, 15_000
    assert :dead = ExecutionRegistry.status(id, task, registry)
    assert [proof] = ExecutionRegistry.pending(100, registry)
    assert proof.end_kind == "process_down"
    assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
    assert ExecutionTerminalProofs.terminal?(proof)
    refute ExecutionTerminalProofs.terminal?(%{proof | owner_execution_id: live_id})
  end

  test "the proof helper rejects live and unknown executions and publishes only its exact completed identity" do
    id = Ecto.UUID.generate()
    on_exit(fn -> ExecutionRegistry.acknowledge([id]) end)
    owner = Identity.local()
    assert :ok = ExecutionRegistry.register(id)

    identity = %{
      owner_execution_id: id,
      owner_process_id: List.to_string(:erlang.pid_to_list(self())),
      owner_instance_id: owner.node_name,
      owner_instance_boot_id: owner.boot_id
    }

    assert_raise ExUnit.AssertionError, fn -> CodexPooler.ExecutionProofSupport.publish_terminal!(identity) end
    refute ExecutionTerminalProofs.terminal?(identity)
    unknown = %{identity | owner_execution_id: Ecto.UUID.generate()}
    assert_raise ExUnit.AssertionError, fn -> CodexPooler.ExecutionProofSupport.publish_terminal!(unknown) end
    refute ExecutionTerminalProofs.terminal?(unknown)

    assert :ok = ExecutionRegistry.complete(id)
    assert Process.alive?(self())

    for {field, replacement} <- [owner_instance_id: "foreign-instance@localhost", owner_instance_boot_id: "foreign-boot", owner_process_id: "<0.0.0>"] do
      mismatched = Map.put(identity, field, replacement)
      assert_raise ExUnit.AssertionError, fn -> CodexPooler.ExecutionProofSupport.publish_terminal!(mismatched) end
      refute ExecutionTerminalProofs.terminal?(mismatched)
    end

    assert [retained] = ExecutionRegistry.pending_proofs([identity.owner_execution_id])
    assert retained.end_kind == "completed"
    assert :ok = CodexPooler.ExecutionProofSupport.publish_terminal!(identity)
    assert ExecutionTerminalProofs.terminal?(identity)
    assert [] = ExecutionRegistry.pending_proofs([identity.owner_execution_id])
    published = Repo.get!(CodexPooler.Platform.ExecutionTerminalProof, identity.owner_execution_id)
    assert published.end_kind == retained.end_kind
    assert published.ended_at == retained.ended_at
    assert published.owner_process_id == identity.owner_process_id
    assert published.owner_instance_id == identity.owner_instance_id
    assert published.owner_instance_boot_id == identity.owner_instance_boot_id
    assert :ok = CodexPooler.ExecutionProofSupport.publish_terminal!(identity)
    assert Repo.get!(CodexPooler.Platform.ExecutionTerminalProof, identity.owner_execution_id) == published
  end

  test "proof expiry preserves the full six-hour window and leaves stale recovery available" do
    setup = CodexPooler.AccountingTestSupport.accounting_setup()

    {:ok, reserved} =
      CodexPooler.Accounting.reserve(
        setup.auth,
        setup.model,
        %{"model" => setup.model.exposed_model_id},
        %{transport: "http_sse"}
      )

    {:ok, attempt} = CodexPooler.Accounting.create_attempt(reserved.request, setup.assignment)
    :ok = ExecutionIdentity.complete()

    proof =
      ExecutionRegistry.pending(10_000)
      |> Enum.find(&(&1.owner_execution_id == attempt.owner_execution_id))

    assert proof
    assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
    expiry = DateTime.add(proof.ended_at, 21_601, :second)
    assert {:ok, %{execution_terminal_proofs_pruned: 0}} = ExecutionTerminalProofs.prune(expiry)
    assert ExecutionTerminalProofs.terminal?(attempt)

    Repo.query!(
      "UPDATE execution_terminal_proofs SET published_at = clock_timestamp() AT TIME ZONE 'UTC' - interval '6 hours 1 second' WHERE execution_id = $1",
      [Ecto.UUID.dump!(proof.owner_execution_id)]
    )

    assert {:ok, %{execution_terminal_proofs_pruned: 1}} =
             ExecutionTerminalProofs.prune(DateTime.add(expiry, 1, :microsecond))

    refute ExecutionTerminalProofs.terminal?(attempt)

    assert {:ok, %{stale_reservations_settled: 1}} =
             CodexPooler.Accounting.recover_stale_reservations(expiry)

    assert Repo.reload!(reserved.request).last_error_code == "stale_reservation_recovered"
    :ok = ExecutionRegistry.acknowledge([proof.owner_execution_id])
  end

  test "owner clock skew cannot shorten or extend the publication retention window" do
    registry = start_supervised!({ExecutionRegistry, name: nil})

    for ended_at <- [~U[2000-01-01 00:00:00.000000Z], ~U[2100-01-01 00:00:00.000000Z]] do
      id = Ecto.UUID.generate()
      :ok = ExecutionRegistry.register(id, registry)
      :ok = ExecutionRegistry.complete(id, registry)
      [proof] = ExecutionRegistry.pending(100, registry)
      proof = %{proof | ended_at: ended_at}
      assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])

      assert {:ok, %{execution_terminal_proofs_pruned: 0}} =
               ExecutionTerminalProofs.prune(~U[2200-01-01 00:00:00Z])

      assert ExecutionTerminalProofs.terminal?(proof)
      published = Repo.get!(CodexPooler.Platform.ExecutionTerminalProof, id).published_at
      assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
      assert Repo.get!(CodexPooler.Platform.ExecutionTerminalProof, id).published_at == published

      Repo.query!(
        "UPDATE execution_terminal_proofs SET published_at = clock_timestamp() AT TIME ZONE 'UTC' - interval '5 hours 59 minutes' WHERE execution_id = $1",
        [Ecto.UUID.dump!(id)]
      )

      assert {:ok, %{execution_terminal_proofs_pruned: 0}} =
               ExecutionTerminalProofs.prune(~U[2200-01-01 00:00:00Z])

      Repo.query!(
        "UPDATE execution_terminal_proofs SET published_at = clock_timestamp() AT TIME ZONE 'UTC' - interval '6 hours 1 second' WHERE execution_id = $1",
        [Ecto.UUID.dump!(id)]
      )

      assert {:ok, %{execution_terminal_proofs_pruned: 1}} =
               ExecutionTerminalProofs.prune(~U[1900-01-01 00:00:00Z])

      refute ExecutionTerminalProofs.terminal?(proof)
      :ok = ExecutionRegistry.acknowledge([id], registry)
    end
  end

  test "unowned and malformed registrations cannot publish terminal authority" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    assert :unknown = ExecutionRegistry.register("not-an-execution-uuid", registry)
    assert [] = ExecutionRegistry.pending(100, registry)
    parent = self()
    id = Ecto.UUID.generate()
    :ok = ExecutionRegistry.register(id, registry)

    task =
      Task.async(fn ->
        send(parent, {:foreign_complete, ExecutionRegistry.complete(id, registry)})
      end)

    Task.await(task)
    assert_receive {:foreign_complete, :unknown}
    assert :alive = ExecutionRegistry.status(id, self(), registry)
    assert [] = ExecutionRegistry.pending(100, registry)
  end

  test "a bounded interruption proof cannot authorize prompt recovery or be rewritten after publication" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    id = Ecto.UUID.generate()
    assert :ok = ExecutionRegistry.register(id, registry)
    assert :ok = ExecutionRegistry.complete(id, registry)
    [proof] = ExecutionRegistry.pending(100, registry)
    proof = Map.put(proof, :interruption_code, "owner_drained")
    assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
    assert ExecutionTerminalProofs.terminal?(proof)
    refute ExecutionTerminalProofs.terminal?(proof, include_interrupted: false)
    assert {:error, :execution_terminal_proof_conflict} = ExecutionTerminalProofs.publish([%{proof | interruption_code: nil}])
    assert {:error, :invalid_execution_terminal_proof} = ExecutionTerminalProofs.publish([%{proof | interruption_code: "arbitrary_shutdown"}])
    assert Repo.get!(CodexPooler.Platform.ExecutionTerminalProof, id).interruption_code == "owner_drained"
  end

  test "an older publisher's omitted cause remains unknown rather than enabling prompt recovery" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    id = Ecto.UUID.generate()
    assert :ok = ExecutionRegistry.register(id, registry)
    assert :ok = ExecutionRegistry.complete(id, registry)
    [proof] = ExecutionRegistry.pending(100, registry)

    row = proof |> Map.drop([:owner_execution_id, :interruption_code]) |> Map.put(:execution_id, id)
    Repo.insert_all(CodexPooler.Platform.ExecutionTerminalProof, [row])

    assert ExecutionTerminalProofs.terminal?(proof)
    refute ExecutionTerminalProofs.terminal?(proof, include_interrupted: false)
    assert Repo.get!(CodexPooler.Platform.ExecutionTerminalProof, id).interruption_code == "unobserved_exit"
  end

  test "queue saturation is bounded and warns once without inventing a proof" do
    registry = start_supervised!({ExecutionRegistry, name: nil})

    for _ <- 1..10_000 do
      id = Ecto.UUID.generate()
      :ok = ExecutionRegistry.register(id, registry)
      :ok = ExecutionRegistry.complete(id, registry)
    end

    overflow = Ecto.UUID.generate()

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        for id <- [overflow, Ecto.UUID.generate()] do
          :ok = ExecutionRegistry.register(id, registry)
          :ok = ExecutionRegistry.complete(id, registry)
        end
      end)

    proofs = ExecutionRegistry.pending(10_001, registry)
    assert length(proofs) == 10_000
    refute Enum.any?(proofs, &(&1.owner_execution_id == overflow))
    assert length(Regex.scan(~r/execution terminal proof queue full/, logs)) == 1
  end

  test "unpublished proof expiry logs once and removes authority from the retry queue" do
    registry = start_supervised!({ExecutionRegistry, name: nil})
    ids = for _ <- 1..2, do: Ecto.UUID.generate()

    for id <- ids do
      :ok = ExecutionRegistry.register(id, registry)
      :ok = ExecutionRegistry.complete(id, registry)
    end

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        Enum.each(ids, &send(registry, {:expire, &1}))
        assert [] = ExecutionRegistry.pending(100, registry)
      end)

    assert length(Regex.scan(~r/execution terminal proof expired before publication/, logs)) == 1
  end
end
