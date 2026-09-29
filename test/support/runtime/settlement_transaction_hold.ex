defmodule CodexPoolerWeb.Runtime.SettlementTransactionHold do
  @moduledoc false

  # Holds the process that settles a request at a chosen point of its
  # settlement, through a `[:codex_pooler, :repo, :query]` handler that
  # matches the settling process's own statements (findings#288). The held
  # process reports `{hold, :held, pid, facts}` to the test and waits for
  # `{hold, :release}`, or releases itself after the detection budget. Only the
  # first match is held; `await_held!/1` registers the release on exit.
  #
  # `inside_transaction!/0` holds the first process that writes a turn's
  # completion inside a transaction after it inserted a settlement ledger
  # entry, right after that write: its transaction stays open and keeps every
  # row it locked, the codex session first. The settlement completes the turn
  # inside the request's own transaction, so nothing of it is visible while it
  # is held. It used to complete the turn in a second transaction, where the
  # same match holds that second transaction with the request already
  # committed.
  #
  # `after_commit!/1` holds the first process that commits a transaction in
  # which it wrote the given request's terminal status, right after that commit
  # and outside any transaction, and reports whether the same transaction wrote
  # a turn's completion.
  #
  # `after_rollback!/0` holds the first process that rolls back a transaction
  # in which it inserted a settlement ledger entry, right after that rollback
  # and outside any transaction.

  import ExUnit.Callbacks, only: [on_exit: 1, start_supervised!: 1]
  import ExUnit.Assertions, only: [flunk: 1]

  alias CodexPooler.Repo

  @detection_timeout_ms 15_000

  @spec inside_transaction!() :: reference()
  def inside_transaction!, do: attach!(%{mode: :inside_transaction})

  @spec after_commit!(Ecto.UUID.t()) :: reference()
  def after_commit!(request_id), do: attach!(%{mode: :after_commit, request_id: Ecto.UUID.dump!(request_id)})

  @spec after_rollback!() :: reference()
  def after_rollback!, do: attach!(%{mode: :after_rollback})

  @doc "Waits for the held process: `{pid, facts}`; an `inside_transaction!/0` hold carries its PostgreSQL `backend`."
  @spec await_held!(reference()) :: {pid(), map()}
  def await_held!(hold) do
    receive do
      {^hold, :held, settler, facts} ->
        on_exit(fn -> send(settler, {hold, :release}) end)
        {settler, facts}
    after
      @detection_timeout_ms -> flunk("the settlement never reached its hold")
    end
  end

  @spec release(reference(), pid()) :: :ok
  def release(hold, settler) do
    send(settler, {hold, :release})
    :ok
  end

  defp attach!(config) do
    hold = make_ref()
    handler_id = {__MODULE__, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = Map.merge(config, %{hold: hold, test: self(), claimed: :atomics.new(1, [])})
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.handle_query/4, config)
    hold
  end

  @doc false
  def handle_query(_event, _measurements, %{query: query} = metadata, %{mode: :inside_transaction, hold: hold} = config) do
    key = {__MODULE__, hold}

    cond do
      not Repo.in_transaction?() -> :ok
      settlement_insert?(query, metadata) -> Process.put(key, :settlement)
      Process.get(key) == :settlement and turn_completion?(query) -> hold_once(key, %{backend: backend(metadata)}, config)
      true -> :ok
    end
  end

  def handle_query(_event, _measurements, %{query: query} = metadata, %{mode: :after_commit, hold: hold, request_id: request_id} = config) do
    key = {__MODULE__, hold}
    state = Process.get(key, %{})

    case after_commit_step(query, metadata, request_id) do
      :request -> Process.put(key, Map.put(state, :request_updated?, true))
      :turn -> Process.put(key, Map.put(state, :turn_completed?, true))
      :commit -> end_transaction(key, state, config)
      :rollback -> Process.delete(key)
      :other -> :ok
    end
  end

  def handle_query(_event, _measurements, %{query: query} = metadata, %{mode: :after_rollback, hold: hold} = config) do
    key = {__MODULE__, hold}

    cond do
      Repo.in_transaction?() and settlement_insert?(query, metadata) ->
        Process.put(key, :settlement)

      query == "rollback" and not Repo.in_transaction?() and Process.get(key) == :settlement ->
        Process.delete(key)
        hold_once(key, %{}, config)

      query in ["commit", "rollback"] and not Repo.in_transaction?() ->
        Process.delete(key)

      true ->
        :ok
    end
  end

  def handle_query(_event, _measurements, _metadata, _config), do: :ok

  defp after_commit_step(query, metadata, request_id) do
    in_transaction? = Repo.in_transaction?()

    cond do
      in_transaction? and settles_request?(query, metadata, request_id) -> :request
      in_transaction? and turn_completion?(query) -> :turn
      in_transaction? -> :other
      query == "commit" -> :commit
      query == "rollback" -> :rollback
      true -> :other
    end
  end

  defp end_transaction(key, state, config) do
    Process.delete(key)

    if Map.get(state, :request_updated?) == true,
      do: hold_once(key, %{turn_in_commit?: Map.get(state, :turn_completed?, false)}, config),
      else: :ok
  end

  defp hold_once(_key, facts, %{hold: hold, test: test, claimed: claimed}) do
    if :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {hold, :held, self(), facts})

      receive do
        {^hold, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  # The request's own terminal write: its id and a terminal status.
  defp settles_request?(query, metadata, request_id) do
    params = List.wrap(metadata[:params])
    String.starts_with?(query, ~s(UPDATE "requests")) and request_id in params and Enum.any?(params, &(&1 in ["succeeded", "failed"]))
  end

  defp settlement_insert?(query, metadata),
    do: String.contains?(query, ~s(INSERT INTO "ledger_entries")) and "settlement" in List.wrap(metadata[:params])

  defp turn_completion?(query), do: String.starts_with?(query, ~s(UPDATE "codex_turns")) and String.contains?(query, ~s("completed_at"))

  defp backend(%{result: {:ok, %Postgrex.Result{connection_id: backend}}}), do: backend
  defp backend(_metadata), do: nil

  @doc """
  A PostgreSQL connection of the test's own, outside the Repo pool the
  listener's processes draw from (findings#206 row 206-501).
  """
  @spec start_lock_watcher!() :: pid()
  def start_lock_watcher! do
    options = Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir])
    start_supervised!({Postgrex, options})
  end

  @doc """
  Waits until a socket's codex session lookup waits on the held settlement and
  returns the relation of the row it waits for.

  A socket looks its codex session up by alias and locks the row, which the
  held settlement locked first; the wait shows as that lookup holding a tuple
  lock while it waits for the settler. `pg_stat_activity` pairs a live wait
  with the backend's status snapshot, so a sample whose statement is not that
  lookup is not the wait yet and is sampled again (findings#206 row 206-182).
  Other writers wait on the held rows too (a closed socket's delivery receipt
  on the attempt); only the lookup is a socket's.
  """
  @spec await_session_lookup_wait!(pid(), pos_integer()) :: String.t()
  def await_session_lookup_wait!(watcher, settler_backend) when is_integer(settler_backend) do
    await_session_lookup_wait!(watcher, settler_backend, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_session_lookup_wait!(watcher, settler_backend, deadline) do
    %{rows: rows} =
      Postgrex.query!(
        watcher,
        """
        SELECT activity.query, tuple_lock.relation::regclass::text
        FROM pg_stat_activity AS activity
        LEFT JOIN pg_locks AS tuple_lock ON tuple_lock.pid = activity.pid AND tuple_lock.locktype = 'tuple'
        WHERE activity.datname = current_database() AND activity.wait_event_type = 'Lock' AND $1 = ANY(pg_blocking_pids(activity.pid))
        """,
        [settler_backend]
      )

    case Enum.find(rows, fn [query, _relation] -> session_lookup?(query) end) do
      [_query, relation] when is_binary(relation) -> relation
      _not_yet -> resample_session_lookup_wait!(watcher, settler_backend, deadline, rows)
    end
  end

  defp resample_session_lookup_wait!(watcher, settler_backend, deadline, rows) do
    if System.monotonic_time(:millisecond) >= deadline do
      flunk("no session lookup waited on the held settlement: #{inspect(Enum.map(rows, fn [query, relation] -> {String.slice(query || "", 0, 80), relation} end))}")
    else
      Process.sleep(10)
      await_session_lookup_wait!(watcher, settler_backend, deadline)
    end
  end

  defp session_lookup?(query) when is_binary(query), do: String.contains?(query, ~s(FROM "codex_sessions" AS c0 INNER JOIN "bridge_session_aliases"))
  defp session_lookup?(_query), do: false
end
