defmodule CodexPooler.Platform.ExecutionProofPublisher do
  @moduledoc false
  use GenServer
  require Logger

  alias CodexPooler.Platform.{ExecutionRegistry, ExecutionTerminalProofs}
  @interval_ms 1_000
  @early_ms 100
  # The longest a shutdown flush waits for the database, inside the caller's
  # own budget: a stalled database must not hold the VM's exit longer.
  @flush_timeout_ms 2_000
  @flush_limit 10_000

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts) do
    configured = Application.get_env(:codex_pooler, __MODULE__, [])

    if Keyword.get(opts, :enabled, Keyword.get(configured, :enabled, true)),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__)),
      else: :ignore
  end

  @doc """
  Publishes the proof of every execution that has ended, before the VM exits.

  The shutdown's drain ends the executions it cuts `process_down`, and a
  client's resend on another node is admitted only once their proofs exist.
  The application stops this process right after its `prep_stop/1`, before
  the early publication (`@early_ms`) or the next tick: the proofs died with
  the VM (findings#270 row 270-371). Waits at most `budget_ms`, and never
  more than `@flush_timeout_ms`; with nothing pending it touches no
  database.
  """
  @spec flush(non_neg_integer(), GenServer.server()) :: :ok | :error | :timeout | :no_budget | :not_running
  def flush(budget_ms, server \\ __MODULE__)

  def flush(budget_ms, server) when is_integer(budget_ms) and budget_ms > 0 do
    GenServer.call(server, :flush, min(budget_ms, @flush_timeout_ms))
  catch
    :exit, {:timeout, _call} ->
      Logger.warning("execution terminal proof flush timed out before the VM exit; pending proofs retained")
      :timeout

    :exit, _not_running ->
      :not_running
  end

  def flush(_budget_ms, _server), do: :no_budget

  @impl true
  def init(opts) do
    state = %{
      registry: Keyword.get(opts, :registry, ExecutionRegistry),
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      timer: nil,
      early: nil,
      early_ids: [],
      failed: false
    }

    {:ok, state, {:continue, :publish}}
  end

  @impl true
  def handle_continue(:publish, state), do: {:noreply, publish(state)}

  @impl true
  def handle_call(:flush, _from, state) do
    _retired = ExecutionRegistry.retire_ended(state.registry)
    result = publish_proofs(state.registry, fn -> ExecutionRegistry.pending(@flush_limit, state.registry) end)
    {:reply, result, note_result(state, result)}
  end

  @impl true
  def handle_info(:publish, state), do: {:noreply, publish(state)}

  # The registry asks for this when an execution ends `process_down`, without
  # delivering its result. A client's resend of that execution's turn waits
  # for its proof: `ClientRetry` admits the resend of an owner-crashed turn
  # only once the proof exists, and at the one-second tick the released
  # client's first resend met `409 duplicate_turn` in about half the cases
  # (findings#283). Requests within `@early_ms` share one publication. The
  # window still writes the proof before that first resend (the executor
  # ended within about 230 ms of the owner's crash and the resend came about
  # 490 ms after it), and it bounds a node whose sockets drop one after
  # another to about ten publications a second. The early publication writes
  # exactly the executions that asked for it: the tick writes the oldest
  # hundred pending proofs, so behind a backlog (proofs retained through a
  # database outage) the asking execution waited one tick per hundred older
  # ones. None brings publication forward while it is failing, which the tick
  # keeps retrying on its own cadence.
  def handle_info({:publish_early, id}, %{failed: false} = state) do
    early = state.early || Process.send_after(self(), :publish_early, @early_ms)
    {:noreply, %{state | early: early, early_ids: [id | state.early_ids]}}
  end

  def handle_info({:publish_early, _id}, state), do: {:noreply, state}

  def handle_info(:publish_early, state) do
    result = publish_proofs(state.registry, fn -> ExecutionRegistry.pending_proofs(state.early_ids, state.registry) end)
    {:noreply, %{note_result(state, result) | early: nil, early_ids: []}}
  end

  # Subscribing on every publication brings a restarted registry's requests
  # back within one tick.
  defp publish(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    _subscribed = ExecutionRegistry.subscribe(state.registry)
    result = publish_proofs(state.registry, fn -> ExecutionRegistry.pending(100, state.registry) end)
    %{note_result(state, result) | timer: Process.send_after(self(), :publish, state.interval_ms)}
  end

  defp note_result(state, result) do
    if result == :error and not state.failed,
      do: Logger.warning("execution terminal proof publication unavailable; pending proofs retained")

    %{state | failed: result == :error}
  end

  defp publish_proofs(registry, proofs) do
    if Process.whereis(CodexPooler.Repo), do: publish_available(registry, proofs.()), else: :error
  end

  defp publish_available(_registry, []), do: :ok
  defp publish_available(_registry, :unknown), do: :error

  defp publish_available(registry, proofs) when is_list(proofs) do
    Enum.reduce_while(Enum.chunk_every(proofs, 100), :ok, fn chunk, :ok ->
      case ExecutionTerminalProofs.publish(chunk) do
        {:ok, _} ->
          _acknowledged = ExecutionRegistry.acknowledge(Enum.map(chunk, & &1.owner_execution_id), registry)
          {:cont, :ok}

        {:error, _} ->
          {:halt, :error}
      end
    end)
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> :error
  catch
    :exit, _ -> :error
  end
end
