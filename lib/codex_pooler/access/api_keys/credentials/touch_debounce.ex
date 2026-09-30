defmodule CodexPooler.Access.APIKeys.TouchDebounce do
  @moduledoc """
  Debounces successful API key authentication touch writes per node.

  Each node keeps only the most recent observed touch timestamp per API key and
  flushes at most once per 60-second interval. Multiple replicas may flush the
  same key concurrently, so the database write is idempotent and monotonic: it
  only advances `last_used_at` when the stored value is nil or older than the
  flushed timestamp. Authentication status, expiry, pool visibility, and policy
  checks happen before this best-effort touch path and do not depend on the
  debounce process.

  A flush that meets a transient database failure
  (`CodexPooler.Platform.TransientDatabaseError`) keeps the keys it could not
  write and tries them again one interval later, after one warning, instead of
  crashing (findings#294). The process is a direct child of the application
  supervisor, whose restart budget every child shares, and a crash would also
  drop every pending touch. Any other exception still crashes it.
  """

  use GenServer

  import Ecto.Query

  require Logger

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Platform.TransientDatabaseError
  alias CodexPooler.Repo

  @debounce_interval_ms 60_000

  @type state :: %{
          pending: %{optional(Ecto.UUID.t()) => DateTime.t()},
          timer_ref: reference() | nil,
          debounce_interval_ms: pos_integer()
        }

  @spec debounce_interval_ms() :: pos_integer()
  def debounce_interval_ms, do: @debounce_interval_ms

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec touch(APIKey.t(), DateTime.t(), GenServer.server()) :: APIKey.t()
  def touch(%APIKey{} = api_key, %DateTime{} = touched_at \\ now(), server \\ __MODULE__) do
    if pid = GenServer.whereis(server) do
      GenServer.cast(pid, {:touch, api_key.id, touched_at})
    end

    %{api_key | last_used_at: newest(api_key.last_used_at, touched_at)}
  end

  @doc """
  Writes the pending touches now. `{:error, :deferred}` means a transient
  database failure kept some of them pending for the next interval.
  """
  @spec flush(GenServer.server()) :: :ok | {:error, :deferred}
  def flush(server \\ __MODULE__) do
    GenServer.call(server, :flush, :infinity)
  end

  @spec reset(GenServer.server()) :: :ok
  def reset(server \\ __MODULE__) do
    GenServer.call(server, :reset, :infinity)
  end

  @impl GenServer
  def init(opts) do
    state = %{
      pending: %{},
      timer_ref: nil,
      debounce_interval_ms: Keyword.get(opts, :debounce_interval_ms, @debounce_interval_ms)
    }

    {:ok, state}
  end

  @impl GenServer
  def handle_cast({:touch, api_key_id, %DateTime{} = touched_at}, state)
      when is_binary(api_key_id) do
    pending = Map.update(state.pending, api_key_id, touched_at, &newest(&1, touched_at))

    {:noreply, %{state | pending: pending} |> ensure_timer()}
  end

  @impl GenServer
  def handle_call(:flush, _from, state) do
    state = state |> cancel_timer() |> flush_pending()
    reply = if map_size(state.pending) == 0, do: :ok, else: {:error, :deferred}
    {:reply, reply, state}
  end

  def handle_call(:reset, _from, state) do
    state = cancel_timer(state)
    {:reply, :ok, %{state | pending: %{}, timer_ref: nil}}
  end

  @impl GenServer
  def handle_info(:flush, state) do
    {:noreply, flush_pending(%{state | timer_ref: nil})}
  end

  defp ensure_timer(%{timer_ref: nil} = state) do
    timer_ref = Process.send_after(self(), :flush, state.debounce_interval_ms)
    %{state | timer_ref: timer_ref}
  end

  defp ensure_timer(state), do: state

  defp cancel_timer(%{timer_ref: nil} = state), do: state

  defp cancel_timer(%{timer_ref: timer_ref} = state) do
    Process.cancel_timer(timer_ref)
    %{state | timer_ref: nil}
  end

  # Writes the pending touches in turn. At the first transient database
  # failure the keys not yet written stay pending (the write is monotonic, so
  # writing one again later is harmless) and the timer is armed again.
  defp flush_pending(%{pending: pending} = state) do
    case Enum.reduce_while(pending, pending, &write_touch/2) do
      remaining when map_size(remaining) == 0 ->
        %{state | pending: %{}}

      remaining ->
        ensure_timer(%{state | pending: remaining})
    end
  end

  defp write_touch({api_key_id, touched_at}, remaining) do
    APIKey
    |> where([key], key.id == ^api_key_id)
    |> where([key], is_nil(key.last_used_at) or key.last_used_at < ^touched_at)
    |> Repo.update_all(set: [last_used_at: touched_at])

    {:cont, Map.delete(remaining, api_key_id)}
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] ->
      if TransientDatabaseError.transient?(error) do
        Logger.warning(
          "api key touch flush deferred after a transient database failure " <>
            "pending_keys=#{map_size(remaining)} reason_class=#{TransientDatabaseError.reason_class(error)}"
        )

        {:halt, remaining}
      else
        reraise error, __STACKTRACE__
      end
  end

  defp newest(nil, %DateTime{} = right), do: right

  defp newest(%DateTime{} = left, %DateTime{} = right) do
    if DateTime.compare(left, right) == :lt, do: right, else: left
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
