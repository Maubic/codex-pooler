defmodule CodexPooler.DisconnectedExecutionPeer do
  @moduledoc false

  alias CodexPooler.Accounting
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness

  alias CodexPooler.Platform.{
    ExecutionIdentity,
    ExecutionProofPublisher,
    ExecutionRegistry,
    ExecutionTerminalProofs
  }

  alias CodexPooler.Platform.InstancePresence.Identity

  @spec bootstrap(keyword(), keyword()) :: :ok
  def bootstrap(env, config) do
    Enum.each(env, fn {key, value} -> Application.put_env(:codex_pooler, key, value) end)
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _} = Application.ensure_all_started(:ex_unit)

    {:ok, supervisor} =
      Supervisor.start_link([{Phoenix.PubSub, name: CodexPooler.PubSub}], strategy: :one_for_one)

    Process.unlink(supervisor)
    boot_id = Identity.mint_boot_id!()

    WebsocketOwnerNodeHarness.start_repo(
      Keyword.merge(config,
        pool: DBConnection.ConnectionPool,
        log: false,
        parameters: [application_name: "execution_peer_" <> boot_id]
      )
    )

    {:ok, _} = GenServer.start(ExecutionRegistry, nil, name: ExecutionRegistry)
    {:ok, publisher} = ExecutionProofPublisher.start_link(enabled: true)
    Process.unlink(publisher)
    :ok
  end

  @spec start(map()) :: {struct(), struct(), pid()}
  def start(setup) do
    caller = self()

    pid =
      spawn(fn ->
        {:ok, reserved} =
          Accounting.reserve(
            setup.auth,
            setup.model,
            %{"model" => setup.model.exposed_model_id, "max_output_tokens" => 10},
            %{transport: "http_sse", correlation_id: Ecto.UUID.generate()}
          )

        {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
        send(caller, {:started, reserved.request, attempt, self()})

        receive do
          :finish -> :ok
        end
      end)

    receive do
      {:started, request, attempt, ^pid} -> {request, attempt, pid}
    after
      15_000 -> raise "execution did not start"
    end
  end

  @spec finish(pid(), struct()) :: :ok
  def finish(pid, attempt) do
    end_process(pid, attempt)
    CodexPooler.ExecutionProofSupport.await_terminal!(attempt)
  end

  @spec finish_without_database(pid(), struct()) :: map()
  def finish_without_database(pid, attempt) do
    # The capture opens before the database goes away: the publisher warns once, at the first publication that finds no
    # Repo, and that can be its own tick rather than the `:publish` below (findings#317).
    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        Supervisor.stop(CodexPooler.Repo)
        end_process(pid, attempt)
        publisher = CodexPooler.Platform.ExecutionProofPublisher
        send(publisher, :publish)
        %{failed: true} = :sys.get_state(publisher)
      end)

    await_pending_proof(attempt.owner_execution_id, System.monotonic_time(:millisecond) + 15_000)

    %{
      queued: Enum.count(ExecutionRegistry.pending(100)),
      warned: String.contains?(logs, "publication unavailable"),
      publisher_alive: Process.alive?(Process.whereis(CodexPooler.Platform.ExecutionProofPublisher))
    }
  end

  # findings#317. The publisher warns once per outage, at its first publication that finds no Repo, and its own tick can be that
  # publication. A capture opened after the database went away missed a warning that had already been printed: opening it loads
  # modules and waits on ExUnit's capture server, a few to some tens of milliseconds against a tick every second. This holds the
  # capture's install while a publication lands, the interleaving that used to depend on load, so `warned` shows the capture
  # covers the outage from its first moment.
  @spec finish_without_database_with_capture_held(pid(), struct()) :: map()
  def finish_without_database_with_capture_held(pid, attempt) do
    publisher = ExecutionProofPublisher
    :ok = :sys.suspend(ExUnit.CaptureServer)

    try do
      finish = Task.async(fn -> finish_without_database(pid, attempt) end)
      await_capture_requested(System.monotonic_time(:millisecond) + 15_000)
      send(publisher, :publish)
      %{} = :sys.get_state(publisher)
      :ok = :sys.resume(ExUnit.CaptureServer)
      Task.await(finish, 15_000)
    after
      :sys.resume(ExUnit.CaptureServer)
    end
  end

  # The capture's install is a call to ExUnit's capture server, so while that server is suspended the request waits in its mailbox.
  defp await_capture_requested(deadline) do
    {:message_queue_len, requests} = Process.info(Process.whereis(ExUnit.CaptureServer), :message_queue_len)

    cond do
      requests > 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "log capture install was not requested"

      true ->
        receive do
        after
          10 -> :ok
        end

        await_capture_requested(deadline)
    end
  end

  @spec restore_database(struct()) :: :ok
  def restore_database(attempt) do
    WebsocketOwnerNodeHarness.start_repo(Application.fetch_env!(:codex_pooler, CodexPooler.Repo))

    send(CodexPooler.Platform.ExecutionProofPublisher, :publish)
    CodexPooler.ExecutionProofSupport.await_terminal!(attempt)
  end

  @spec finish_with_uncertain_commit(pid(), struct()) :: map()
  def finish_with_uncertain_commit(pid, attempt) do
    observer = self()
    handler = {__MODULE__, make_ref()}
    publisher = Process.whereis(CodexPooler.Platform.ExecutionProofPublisher)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        &__MODULE__.hold_commit/4,
        {publisher, observer}
      )

    try do
      end_process(pid, attempt)
      send(publisher, :publish)

      receive do
        {:proof_committed, ^publisher} -> :ok
      after
        15_000 -> raise "publisher commit was not observed"
      end

      monitor = Process.monitor(publisher)
      Process.exit(publisher, :kill)

      receive do
        {:DOWN, ^monitor, :process, ^publisher, :killed} -> :ok
      after
        15_000 -> raise "publisher did not stop"
      end

      persisted = ExecutionTerminalProofs.terminal?(attempt)
      pending = Enum.count(ExecutionRegistry.pending(100))
      {:ok, replacement} = ExecutionProofPublisher.start_link(enabled: true)
      Process.unlink(replacement)
      await_acknowledged(System.monotonic_time(:millisecond) + 15_000)

      %{
        committed_before_ack: persisted,
        pending_before_restart: pending,
        pending_after_restart: 0
      }
    after
      :telemetry.detach(handler)
    end
  end

  @doc false
  def hold_commit(_event, _measurements, metadata, {publisher, observer}) do
    if self() == publisher and metadata.query == "commit" do
      send(observer, {:proof_committed, self()})

      receive do
        :release_commit -> :ok
      after
        15_000 -> raise "publisher commit barrier not released"
      end
    end
  end

  defp await_acknowledged(deadline) do
    if ExecutionRegistry.pending(100) == [] do
      :ok
    else
      if System.monotonic_time(:millisecond) >= deadline, do: raise("proof was not acknowledged")

      receive do
      after
        10 -> :ok
      end

      await_acknowledged(deadline)
    end
  end

  # `ExecutionIdentity.status/1` can answer `:dead` before the registry has received its own DOWN for the process (the window
  # `ExecutionProofSupport.publish_terminal!` documents), and the proof is pending only from then on: wait for the registry to
  # hold this execution's proof before counting what is queued.
  defp await_pending_proof(execution_id, deadline) do
    case ExecutionRegistry.pending_proofs([execution_id]) do
      [_proof] ->
        :ok

      :unknown ->
        raise "execution registry is unavailable while awaiting the pending proof"

      [] ->
        if System.monotonic_time(:millisecond) >= deadline, do: raise("registry did not retain a pending proof for execution #{execution_id}")

        receive do
        after
          10 -> :ok
        end

        await_pending_proof(execution_id, deadline)
    end
  end

  defp end_process(pid, attempt) do
    monitor = Process.monitor(pid)
    send(pid, :finish)

    receive do
      {:DOWN, ^monitor, :process, ^pid, :normal} -> :ok
    after
      15_000 -> raise "execution did not stop"
    end

    :dead = ExecutionIdentity.status(attempt)
    :ok
  end
end
