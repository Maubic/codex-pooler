defmodule CodexPooler.Platform.ExecutionProofPublisher do
  @moduledoc false
  use GenServer
  require Logger

  alias CodexPooler.Platform.{ExecutionRegistry, ExecutionTerminalProofs}
  @interval_ms 1_000
  @early_ms 20

  @spec start_link(keyword()) :: GenServer.on_start() | :ignore
  def start_link(opts) do
    configured = Application.get_env(:codex_pooler, __MODULE__, [])

    if Keyword.get(opts, :enabled, Keyword.get(configured, :enabled, true)),
      do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__)),
      else: :ignore
  end

  @impl true
  def init(opts) do
    state = %{
      registry: Keyword.get(opts, :registry, ExecutionRegistry),
      interval_ms: Keyword.get(opts, :interval_ms, @interval_ms),
      timer: nil,
      early: nil,
      failed: false
    }

    {:ok, state, {:continue, :publish}}
  end

  @impl true
  def handle_continue(:publish, state), do: {:noreply, publish(state)}

  @impl true
  def handle_info(:publish, state), do: {:noreply, publish(state)}

  # The registry asks for this when an execution ends `process_down`, without
  # delivering its result. A client's resend of that execution's turn waits
  # for its proof: `ClientRetry` admits the resend of an owner-crashed turn
  # only once the proof exists, and at the one-second tick the released
  # client's first resend met `409 duplicate_turn` in about half the cases
  # (findings#283). Requests within `@early_ms` share one publication, and
  # none brings publication forward while it is failing, which the tick keeps
  # retrying on its own cadence.
  def handle_info(:publish_early, %{early: nil, failed: false} = state),
    do: {:noreply, %{state | early: Process.send_after(self(), :publish, @early_ms)}}

  def handle_info(:publish_early, state), do: {:noreply, state}

  # Subscribing on every publication brings a restarted registry's requests
  # back within one tick.
  defp publish(state) do
    if state.timer, do: Process.cancel_timer(state.timer)
    if state.early, do: Process.cancel_timer(state.early)
    _subscribed = ExecutionRegistry.subscribe(state.registry)
    result = publish_pending(state.registry)

    if result == :error and not state.failed,
      do: Logger.warning("execution terminal proof publication unavailable; pending proofs retained")

    %{state | timer: Process.send_after(self(), :publish, state.interval_ms), early: nil, failed: result == :error}
  end

  defp publish_pending(registry) do
    if Process.whereis(CodexPooler.Repo), do: publish_available(registry), else: :error
  end

  defp publish_available(registry) do
    case ExecutionRegistry.pending(100, registry) do
      [] ->
        :ok

      proofs when is_list(proofs) ->
        case ExecutionTerminalProofs.publish(proofs) do
          {:ok, _} ->
            ExecutionRegistry.acknowledge(Enum.map(proofs, & &1.owner_execution_id), registry)

          {:error, _} ->
            :error
        end

      :unknown ->
        :error
    end
  rescue
    _error in [DBConnection.ConnectionError, Postgrex.Error] -> :error
  catch
    :exit, _ -> :error
  end
end
