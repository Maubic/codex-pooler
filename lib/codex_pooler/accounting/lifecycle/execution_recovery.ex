defmodule CodexPooler.Accounting.ExecutionRecovery do
  @moduledoc false
  use GenServer
  require Logger

  alias CodexPooler.Accounting.RequestLifecycle.{AdmissionExecutionRecovery, DeadExecutionRecovery}
  alias CodexPooler.Platform.ExecutionTerminalProofs

  @interval_ms 1_000
  @publication_delay_ms 25
  @batch_limit 100
  @database_timeout_ms 500

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts \\ []) do
    configured = Application.get_env(:codex_pooler, __MODULE__, [])

    if Keyword.get(opts, :enabled, Keyword.get(configured, :enabled, true)),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__)),
      else: :ignore
  end

  @spec publish_committed([ExecutionTerminalProofs.terminal()], GenServer.server()) :: :ok
  def publish_committed(proofs, server \\ __MODULE__) when is_list(proofs) do
    ids = proofs |> Enum.take(@batch_limit) |> Enum.map(& &1.owner_execution_id)
    GenServer.cast(server, {:published, ids})
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      timer: nil,
      early: nil,
      ids: MapSet.new(),
      failed: false
    }

    {:ok, state, {:continue, :recover}}
  end

  @impl true
  def handle_continue(:recover, state), do: {:noreply, recover_published(state)}

  @impl true
  def handle_cast({:published, ids}, state) do
    # This set only prioritizes new proofs. Durable proof/lifecycle joins are
    # the retry queue, including notifications lost to overflow or VM exit.
    ids = Enum.reduce(ids, state.ids, fn id, acc -> if MapSet.size(acc) < @batch_limit, do: MapSet.put(acc, id), else: acc end)
    early = state.early || Process.send_after(self(), :recover_early, @publication_delay_ms)
    {:noreply, %{state | ids: ids, early: early}}
  end

  @impl true
  def handle_info(:recover_early, state) do
    ids = MapSet.to_list(state.ids)
    result = recover(fn module, opts -> module.recover_execution_ids(ids, DateTime.utc_now(), opts) end)
    {:noreply, %{note_result(state, result) | ids: MapSet.new(), early: nil}}
  end

  def handle_info(:recover, state), do: {:noreply, recover_published(state)}

  defp recover_published(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    result = recover(fn module, opts -> module.recover_published(DateTime.utc_now(), opts) end)
    %{note_result(state, result) | timer: Process.send_after(self(), :recover, state.interval_ms)}
  end

  # Publication never waits for accounting locks. Each recovery owns its
  # transaction and rechecks exact proof, generation and replay authority.
  # A failure in one phase cannot skip the other; the durable next pass retries.
  defp recover(run) do
    opts = [limit: @batch_limit, timeout: @database_timeout_ms, checkout_retries: 0]
    results = Enum.map([AdmissionExecutionRecovery, DeadExecutionRecovery], &run_phase(&1, run, opts))
    if Enum.all?(results, &match?({:ok, _summary}, &1)), do: :ok, else: :error
  end

  defp run_phase(module, run, opts) do
    run.(module, opts)
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> :error
  catch
    :exit, _reason -> :error
  end

  defp note_result(state, result) do
    if result == :error and not state.failed,
      do: Logger.warning("execution accounting recovery unavailable; committed proofs retained")

    %{state | failed: result == :error}
  end
end
