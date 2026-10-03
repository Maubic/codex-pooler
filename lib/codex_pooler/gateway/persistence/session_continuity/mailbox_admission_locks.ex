defmodule CodexPooler.Gateway.Persistence.SessionContinuity.MailboxAdmissionLocks do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Repo

  @held_sessions {__MODULE__, :held_sessions}
  @rediscover :rediscover_mailbox_sessions
  @max_restarts 2

  @type discovery :: (-> [Ecto.UUID.t() | nil])
  @type operation :: (-> term())

  # Discovery belongs to the accounting caller. This coordinator owns only
  # session lock ordering and the lifetime of the enclosing transaction.
  @spec transaction(discovery(), operation(), term()) :: {:ok, term()} | {:error, term()}
  def transaction(discover, operation, exhausted_reason)
      when is_function(discover, 0) and is_function(operation, 0) do
    if Repo.in_transaction?() do
      nested_transaction(operation, exhausted_reason)
    else
      transact(discover, operation, exhausted_reason, 0)
    end
  end

  @spec require_session!(Ecto.UUID.t() | nil) :: :ok
  def require_session!(nil), do: :ok

  def require_session!(session_id) do
    case Process.get(@held_sessions) do
      %MapSet{} = held ->
        if MapSet.member?(held, session_id), do: :ok, else: Repo.rollback(@rediscover)

      nil ->
        Repo.rollback(@rediscover)
    end
  end

  @spec require_sessions!([Ecto.UUID.t() | nil]) :: :ok
  def require_sessions!(session_ids) do
    Enum.each(session_ids, &require_session!/1)
    :ok
  end

  @spec coordinated?() :: boolean()
  def coordinated?, do: Repo.in_transaction?() and match?(%MapSet{}, Process.get(@held_sessions))

  defp nested_transaction(operation, exhausted_reason) do
    # A parent without the coordinator cannot safely discover after its locks.
    # No retry can release locks acquired by an arbitrary caller-owned outer
    # transaction. Abort that whole transaction with the public caller error;
    # private rediscovery is reserved for an enclosing coordinator to consume.
    if is_nil(Process.get(@held_sessions)), do: Repo.rollback(exhausted_reason)

    case Repo.transaction(operation) do
      {:error, @rediscover} -> Repo.rollback(@rediscover)
      result -> result
    end
  end

  defp transact(discover, operation, exhausted_reason, restarts) do
    session_ids = discover.() |> Enum.reject(&is_nil/1) |> Enum.uniq() |> Enum.sort()

    result =
      Repo.transaction(fn ->
        sessions = lock_sessions(session_ids)
        held = MapSet.new(sessions, & &1.id)
        previous = Process.put(@held_sessions, held)

        try do
          require_sessions!(session_ids)
          operation.()
        after
          restore_held_sessions(previous)
        end
      end)

    # This branch is deliberately outside Repo.transaction: all old locks
    # have been released before discovery and the next transaction begin.
    case result do
      {:error, @rediscover} when restarts < @max_restarts ->
        transact(discover, operation, exhausted_reason, restarts + 1)

      {:error, @rediscover} ->
        {:error, exhausted_reason}

      result ->
        result
    end
  end

  defp lock_sessions(session_ids) do
    Repo.all(from session in CodexSession, where: session.id in ^session_ids, order_by: [asc: session.id], lock: "FOR UPDATE")
  end

  defp restore_held_sessions(nil), do: Process.delete(@held_sessions)
  defp restore_held_sessions(previous), do: Process.put(@held_sessions, previous)
end
