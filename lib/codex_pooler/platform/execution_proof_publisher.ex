defmodule CodexPooler.Platform.ExecutionProofPublisher do
  @moduledoc false
  use GenServer
  require Logger

  alias CodexPooler.Accounting.ExecutionRecovery
  alias CodexPooler.Platform.{ExecutionRegistry, ExecutionTerminalProofs}
  @interval_ms 1_000
  @early_ms 100
  @retry_ms 250
  @publication_timeout_ms 500
  @pending_limit 10_000
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
    {:reply, if(result == :ok, do: :ok, else: :error), note_result(state, result)}
  end

  @impl true
  def handle_info(:publish, state), do: {:noreply, publish(state)}

  # Retain an ended execution's priority through a database outage. Both
  # delivered results and process exits can strand accounting, and dropping
  # their ids on a failed publication makes a restored owner wait behind the
  # oldest backlog. Coalesce ends without delaying the bounded retry timer.
  def handle_info({:publish_early, id}, state) do
    early =
      if state.failed,
        do: state.early,
        else: state.early || Process.send_after(self(), :publish_early, @early_ms)

    ids = [id | Enum.reject(state.early_ids, &(&1 == id))] |> Enum.take(@pending_limit)
    {:noreply, %{state | early: early, early_ids: ids}}
  end

  def handle_info(:publish_early, state) do
    result = publish_proofs(state.registry, fn -> priority_proofs(state, 100) end, state.failed)
    state = %{note_result(state, result) | early: nil, early_ids: next_priority_ids(state, result)}
    {:noreply, if(state.failed or state.early_ids != [], do: schedule_publish(state, @retry_ms), else: state)}
  end

  # Subscribing on every publication brings a restarted registry's requests
  # back within one tick.
  defp publish(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    _subscribed = ExecutionRegistry.subscribe(state.registry)
    result = publish_proofs(state.registry, fn -> pending_proofs(state) end, state.failed)
    state = %{note_result(state, result) | early_ids: next_priority_ids(state, result)}
    delay = if(state.failed or state.early_ids != [], do: @retry_ms, else: state.interval_ms)
    schedule_publish(state, delay)
  end

  defp schedule_publish(state, delay_ms) do
    if state.timer, do: Process.cancel_timer(state.timer)
    %{state | timer: Process.send_after(self(), :publish, delay_ms)}
  end

  defp pending_proofs(state) do
    with priority when is_list(priority) <- priority_proofs(state, 100),
         pending when is_list(pending) <- ExecutionRegistry.pending(100, state.registry) do
      pending = if state.failed, do: Enum.sort_by(pending, & &1.ended_at, {:desc, DateTime}), else: pending
      Enum.uniq_by(priority ++ pending, & &1.owner_execution_id) |> Enum.take(100)
    else
      _unknown -> :unknown
    end
  end

  defp priority_proofs(state, limit) do
    ids = Enum.take(state.early_ids, limit)

    case ExecutionRegistry.pending_proofs(ids, state.registry) do
      proofs when is_list(proofs) ->
        indexed = Map.new(proofs, &{&1.owner_execution_id, &1})
        Enum.flat_map(ids, &pending_priority_proof(indexed, &1))

      unknown ->
        unknown
    end
  end

  defp pending_priority_proof(indexed, id) do
    case Map.fetch(indexed, id) do
      {:ok, proof} -> [proof]
      :error -> []
    end
  end

  defp retained_priority_ids(state) do
    case ExecutionRegistry.pending_proofs(state.early_ids, state.registry) do
      proofs when is_list(proofs) ->
        pending = MapSet.new(proofs, & &1.owner_execution_id)
        Enum.filter(state.early_ids, &MapSet.member?(pending, &1))

      _unknown ->
        state.early_ids
    end
  end

  defp next_priority_ids(state, :ok), do: retained_priority_ids(state)

  defp next_priority_ids(state, {:error, failed_ids}) do
    case ExecutionRegistry.pending(@pending_limit, state.registry) do
      proofs when is_list(proofs) ->
        ids = Enum.uniq(retained_priority_ids(state) ++ Enum.map(proofs, & &1.owner_execution_id))
        {failed, remaining} = Enum.split_with(ids, &(&1 in failed_ids))
        remaining ++ failed

      _unknown ->
        state.early_ids
    end
  end

  defp note_result(state, result) do
    failed? = result != :ok

    if failed? and not state.failed, do: Logger.warning(unavailable_message(state.registry))

    %{state | failed: failed?}
  end

  # The warning claims only what the registry holds when publication first fails: a publisher whose Repo is gone while nothing is
  # pending retains nothing, and one that cannot reach its registry cannot say. It reports the count, never an id.
  defp unavailable_message(registry) do
    case ExecutionRegistry.pending(@pending_limit, registry) do
      [_ | _] = pending -> "execution terminal proof publication unavailable; pending proofs retained: #{length(pending)}"
      _none_or_unknown -> "execution terminal proof publication unavailable"
    end
  end

  defp publish_proofs(registry, proofs, isolate? \\ false) do
    opts = [timeout: @publication_timeout_ms, deadline: System.monotonic_time(:millisecond) + @publication_timeout_ms, checkout_retries: 0]
    if Process.whereis(CodexPooler.Repo), do: publish_available(registry, proofs.(), isolate?, opts), else: {:error, []}
  end

  defp publish_available(_registry, [], _isolate?, _opts), do: :ok
  defp publish_available(_registry, :unknown, _isolate?, _opts), do: {:error, []}

  # Healthy bursts still share transactions. After a failure isolate proofs:
  # one locked/conflicting older row must not hold the next executor's proof
  # behind it for the whole client retry window.
  defp publish_available(registry, proofs, isolate?, opts) when is_list(proofs) do
    Enum.reduce_while(Enum.chunk_every(proofs, if(isolate?, do: 1, else: 100)), :ok, fn chunk, :ok ->
      case publish_chunk(chunk, opts) do
        {:ok, _} ->
          _acknowledged = ExecutionRegistry.acknowledge(Enum.map(chunk, & &1.owner_execution_id), registry)
          :ok = ExecutionRecovery.publish_committed(chunk)
          {:cont, :ok}

        {:error, _} ->
          {:halt, {:error, Enum.map(chunk, & &1.owner_execution_id)}}
      end
    end)
  end

  defp publish_chunk(chunk, opts) do
    ExecutionTerminalProofs.publish(chunk, opts)
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> {:error, :publication_unavailable}
  catch
    :exit, _ -> {:error, :publication_unavailable}
  end
end
