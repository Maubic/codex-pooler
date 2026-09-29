defmodule CodexPooler.Platform.ExecutionRegistry do
  @moduledoc false
  use GenServer
  require Logger

  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Platform.InstancePresence.Identity

  @retention_ms :timer.hours(6)
  @pending_limit 10_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, nil, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec register(String.t(), GenServer.server()) :: :ok | :unknown
  def register(id, server \\ __MODULE__), do: call(server, {:register, id})

  @spec complete(String.t(), GenServer.server()) :: :ok | :unknown
  def complete(id, server \\ __MODULE__), do: call(server, {:complete, id})

  @spec status(String.t(), pid(), GenServer.server()) :: :alive | :dead | :unknown
  def status(id, pid, server \\ __MODULE__), do: call(server, {:status, id, pid})

  @spec pending(pos_integer(), GenServer.server()) ::
          [ExecutionTerminalProofs.terminal()] | :unknown
  def pending(limit, server \\ __MODULE__), do: call(server, {:pending, limit})

  @spec acknowledge([String.t()], GenServer.server()) :: :ok | :unknown
  def acknowledge(ids, server \\ __MODULE__), do: call(server, {:acknowledge, ids})

  # The caller (the proof publisher) is asked to publish early each time an
  # execution ends `process_down`.
  @spec subscribe(GenServer.server()) :: :ok | :unknown
  def subscribe(server \\ __MODULE__), do: call(server, :subscribe)

  # The pending proofs of `ids`, for the early publication those executions
  # asked for; one already published is no longer pending.
  @spec pending_proofs([String.t()], GenServer.server()) ::
          [ExecutionTerminalProofs.terminal()] | :unknown
  def pending_proofs(ids, server \\ __MODULE__) when is_list(ids), do: call(server, {:pending_proofs, ids})

  # Retires, as `process_down`, every execution whose process has exited
  # while its `:DOWN` still waits in this registry's mailbox. The shutdown
  # flush reads the pending proofs right after this: the drain that ended an
  # execution saw its process go down, but nothing orders that signal before
  # the one this registry receives (findings#270 row 270-371).
  @spec retire_ended(GenServer.server()) :: :ok | :unknown
  def retire_ended(server \\ __MODULE__), do: call(server, :retire_ended)

  defp call(server, request) do
    GenServer.call(server, request, 1_000)
  catch
    :exit, _ -> :unknown
  end

  @impl true
  def init(nil),
    do:
      {:ok,
       %{
         entries: %{},
         monitors: %{},
         owners: %{},
         pending: %{},
         subscribers: %{},
         overflow: false,
         expired_warning: false
       }}

  @impl true
  def handle_call({:register, id}, {pid, _}, state) do
    case Map.get(state.entries, id) do
      nil when is_binary(id) ->
        if match?({:ok, ^id}, Ecto.UUID.cast(id)) do
          ref = Process.monitor(pid)

          state = %{
            state
            | entries: Map.put(state.entries, id, {pid, ref}),
              owners: Map.put(state.owners, id, Identity.local()),
              monitors: Map.put(state.monitors, ref, id)
          }

          {:reply, :ok, state}
        else
          {:reply, :unknown, state}
        end

      {^pid, ref} when is_reference(ref) ->
        {:reply, :ok, state}

      _ ->
        {:reply, :unknown, state}
    end
  end

  def handle_call({:complete, id}, {pid, _}, state) do
    case Map.get(state.entries, id) do
      {^pid, ref} when is_reference(ref) ->
        {:reply, :ok, retire(state, id, pid, ref, "completed")}

      _ ->
        {:reply, :unknown, state}
    end
  end

  def handle_call({:status, id, pid}, _from, state) do
    case Map.get(state.entries, id) do
      {^pid, :dead} ->
        {:reply, :dead, state}

      {^pid, ref} when is_reference(ref) ->
        if Process.alive?(pid) do
          {:reply, :alive, state}
        else
          {:reply, :dead, retire(state, id, pid, ref, "process_down")}
        end

      _ ->
        {:reply, :unknown, state}
    end
  end

  def handle_call({:pending, limit}, _from, state) when is_integer(limit) and limit > 0 do
    proofs =
      state.pending
      |> Map.values()
      |> Enum.sort_by(& &1.ended_at, DateTime)
      |> Enum.take(min(limit, @pending_limit))

    {:reply, proofs, state}
  end

  def handle_call({:pending_proofs, ids}, _from, state) when is_list(ids),
    do: {:reply, state.pending |> Map.take(ids) |> Map.values(), state}

  def handle_call(:retire_ended, _from, state) do
    state =
      Enum.reduce(state.entries, state, fn
        {id, {pid, ref}}, state when is_reference(ref) ->
          if Process.alive?(pid), do: state, else: retire(state, id, pid, ref, "process_down")

        _retired, state ->
          state
      end)

    {:reply, :ok, state}
  end

  def handle_call({:acknowledge, ids}, _from, state) do
    {:reply, :ok, %{state | pending: Map.drop(state.pending, ids), overflow: false, expired_warning: false}}
  end

  def handle_call(:subscribe, {pid, _}, state) do
    if pid in Map.values(state.subscribers),
      do: {:reply, :ok, state},
      else: {:reply, :ok, %{state | subscribers: Map.put(state.subscribers, Process.monitor(pid), pid)}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    case Map.get(state.monitors, ref) do
      nil -> {:noreply, %{state | subscribers: Map.delete(state.subscribers, ref)}}
      id -> {:noreply, retire(state, id, pid, ref, "process_down")}
    end
  end

  def handle_info({:expire, id}, state) do
    expired = Map.has_key?(state.pending, id)

    if expired and not state.expired_warning,
      do: Logger.warning("execution terminal proof expired before publication; execution becomes unknown")

    {:noreply,
     %{
       state
       | entries: Map.delete(state.entries, id),
         owners: Map.delete(state.owners, id),
         pending: Map.delete(state.pending, id),
         expired_warning: state.expired_warning or expired
     }}
  end

  defp retire(state, id, pid, ref, end_kind) do
    Process.demonitor(ref, [:flush])
    Process.send_after(self(), {:expire, id}, @retention_ms)

    state = %{
      state
      | entries: Map.put(state.entries, id, {pid, :dead}),
        monitors: Map.delete(state.monitors, ref)
    }

    owner = Map.fetch!(state.owners, id)

    proof = %{
      owner_execution_id: id,
      owner_instance_id: owner.node_name,
      owner_instance_boot_id: owner.boot_id,
      owner_process_id: List.to_string(:erlang.pid_to_list(pid)),
      end_kind: end_kind,
      ended_at: DateTime.utc_now()
    }

    if map_size(state.pending) < @pending_limit do
      if end_kind == "process_down", do: request_early_publication(state.subscribers, id)
      %{state | pending: Map.put(state.pending, id, proof)}
    else
      unless state.overflow,
        do: Logger.warning("execution terminal proof queue full; unpublished executions remain unknown")

      %{state | overflow: true}
    end
  end

  # An execution that ended without delivering its result: a client's resend
  # of its turn waits for this proof (findings#283). A completed execution
  # stays on the publisher's tick. The owner-crash case gets here only because
  # its response task ends `process_down`: its socket closes 1011 before the
  # task hears that its error was delivered. Were the socket to confirm that
  # delivery first, the task would complete and its proof would wait for the
  # tick again.
  defp request_early_publication(subscribers, id),
    do: Enum.each(subscribers, fn {_ref, subscriber} -> send(subscriber, {:publish_early, id}) end)
end
