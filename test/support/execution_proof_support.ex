defmodule CodexPooler.ExecutionProofSupport do
  @moduledoc false
  import ExUnit.Assertions
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionProofPublisher, ExecutionRegistry, ExecutionTerminalProofs}

  @identity_fields [:owner_execution_id, :owner_instance_id, :owner_instance_boot_id, :owner_process_id]
  @terminal_readiness_timeout_ms 15_000
  # Detection budget for the publisher's stop: it waits for a publication in
  # flight, whose own database deadline is far shorter.
  @publisher_stop_timeout_ms 15_000

  @doc """
  Starts the production `ExecutionProofPublisher` for one test (`config/test.exs`
  disables the application's own) and stops it from an `on_exit` registered now,
  so the stop runs before the cleanups the test registered earlier: its Pool's
  owner stops, the websocket cleanup fence and the sandbox owner's stop.

  Never start it with `start_supervised!/1`. ExUnit stops supervised children
  with an exit signal before any `on_exit` runs, and the publisher does not trap
  exits: about 100 ms after an execution ends it publishes the proof inside a
  transaction on the shared sandbox connection, and a publisher stopped there
  took that connection down, so the next cleanup that read the database failed
  `DBConnection.OwnershipError ... mode :manual` (Drone 1807,
  `dead_execution_resend_test.exs`). `GenServer.stop/3` is handled between
  callbacks, after a publication in flight committed. The publisher is not
  linked to the test process, whose `:shutdown` exit would stop it the same way.
  Takes the publisher's own options (`:name`, defaulting to the module as
  `start_link/1` does, `:registry`, `:interval_ms`).
  """
  @spec start_publisher!(keyword()) :: pid()
  def start_publisher!(opts \\ []) do
    {:ok, publisher} = GenServer.start(ExecutionProofPublisher, opts, name: Keyword.get(opts, :name, ExecutionProofPublisher))
    ExUnit.Callbacks.on_exit(fn -> stop_publisher(publisher) end)
    publisher
  end

  defp stop_publisher(publisher) do
    GenServer.stop(publisher, :normal, @publisher_stop_timeout_ms)
  catch
    :exit, {:noproc, _call} -> :ok
  end

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
