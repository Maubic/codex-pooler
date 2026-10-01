defmodule CodexPooler.ExecutionProofSupport do
  @moduledoc false
  import ExUnit.Assertions
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProofs}

  @identity_fields [:owner_execution_id, :owner_instance_id, :owner_instance_boot_id, :owner_process_id]
  @terminal_readiness_timeout_ms 15_000

  @spec publish_committed_terminal!(map()) :: :ok
  def publish_committed_terminal!(identity) do
    CodexPooler.UnboxedFixture.register_unboxed_cleanup!(fn ->
      case CodexPooler.Repo.get(
             CodexPooler.Platform.ExecutionTerminalProof,
             identity.owner_execution_id
           ) do
        nil -> :ok
        proof -> CodexPooler.Repo.delete!(proof)
      end
    end)

    CodexPooler.UnboxedFixture.run_unboxed(fn -> publish_terminal!(identity) end)
  end

  @spec publish_terminal!(map()) :: :ok
  def publish_terminal!(identity) do
    assert ExecutionIdentity.status(identity) == :dead

    # A dead PID can be observed before the registry receives its own DOWN.
    # Publication needs its exact retained proof, not that liveness sample.
    proof = await_pending_proof(identity, System.monotonic_time(:millisecond) + @terminal_readiness_timeout_ms)

    if proof do
      assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
      assert :ok = ExecutionRegistry.acknowledge([proof.owner_execution_id])
    end

    assert ExecutionTerminalProofs.terminal?(identity)
    :ok
  end

  defp await_pending_proof(identity, deadline) do
    case ExecutionRegistry.pending_proofs([identity.owner_execution_id]) do
      [proof] ->
        assert Map.take(proof, @identity_fields) == Map.take(identity, @identity_fields),
               "pending terminal proof does not match the exact execution identity"

        proof

      [] ->
        if ExecutionTerminalProofs.terminal?(identity) do
          nil
        else
          assert ExecutionIdentity.status(identity) == :dead,
                 "execution became alive or unknown while awaiting its retained terminal proof"

          remaining = deadline - System.monotonic_time(:millisecond)

          assert remaining > 0,
                 "registry did not retain a terminal proof for execution #{identity.owner_execution_id} within #{@terminal_readiness_timeout_ms}ms"

          receive do
          after
            min(10, remaining) -> await_pending_proof(identity, deadline)
          end
        end

      :unknown ->
        flunk("execution registry is unavailable while awaiting the exact terminal proof")
    end
  end

  @spec await_terminal!(map(), pid() | nil) :: :ok
  def await_terminal!(identity, publisher \\ nil),
    do: await_terminal(identity, publisher, System.monotonic_time(:millisecond) + 15_000)

  defp await_terminal(identity, publisher, deadline) do
    if ExecutionTerminalProofs.terminal?(identity) do
      :ok
    else
      assert System.monotonic_time(:millisecond) < deadline,
             "terminal execution proof was not published"

      # Drive the owned production publisher on demand. Its default one-second
      # cadence otherwise charges every batch left by earlier sandbox tests to
      # this test, making serial and partitioned order produce different costs.
      if is_pid(publisher) do
        send(publisher, :publish)
        :sys.get_state(publisher)
      end

      receive do
      after
        10 -> :ok
      end

      await_terminal(identity, publisher, deadline)
    end
  end
end
