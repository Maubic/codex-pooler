defmodule CodexPooler.Platform.ForwardedGenerationEnds do
  @moduledoc """
  What the owner that served a forwarded attempt knows about the generation
  it relayed (findings#290).

  With owner forwarding a turn's executor runs on its socket's node and the
  generation on the session owner's node. When the socket's node becomes
  unreachable, the owner can end the generation itself: it cancels a turn no
  resend can rejoin, cancels a `:lost` turn at its first output, or delivers
  the terminal to a socket that reattached. After any of these the executor
  can no longer settle the attempt as a success, since no terminal reached it.
  The owner records that here, and absent-instance recovery takes the row as
  the exact evidence that the execution is over, which it otherwise gets only
  from a successor incarnation under the same node name or container slot. A
  replaced pod comes back under neither, and its orphans waited for the
  six-hour sweep.

  Only the owner that served the generation writes a row, only at those ends,
  and never for a terminal delivered to the attempt's own socket, whose
  executor may still settle it. A row is not enough on its own: recovery still
  requires the executor's instance absent past the liveness window and a
  fresh observer. Ids and times only.
  """

  import Ecto.Query

  alias CodexPooler.Platform.{ExecutionTerminalProofs, ForwardedGenerationEnd, InstancePresence}
  alias CodexPooler.Repo

  @reasons ~w(unreachable_downstream_cancelled lost_turn_cancelled_at_output terminal_delivered_to_reattached)
  # Past this window the six-hour stale-reservation sweep settles the attempt
  # whatever the evidence, which is why execution terminal proofs keep it too.
  @retention_seconds ExecutionTerminalProofs.retention_seconds()
  # One lease-row sized write, with the budget the code gives such a write.
  @write_budget_ms InstancePresence.heartbeat_write_budget_ms()

  @spec reasons() :: [String.t()]
  def reasons, do: @reasons

  @spec retention_seconds() :: pos_integer()
  def retention_seconds, do: @retention_seconds

  @doc """
  Records, as this instance, that the generation of `attempt_id` ended for
  `reason`. The first end recorded for an attempt stays.
  """
  @spec record(Ecto.UUID.t(), String.t()) :: :ok | {:error, term()}
  def record(attempt_id, reason) when reason in @reasons do
    case Ecto.UUID.cast(attempt_id) do
      {:ok, attempt_id} ->
        identity = InstancePresence.local_identity()
        row = %{attempt_id: attempt_id, owner_instance_id: identity.node_name, owner_instance_boot_id: identity.boot_id, reason: reason}
        {_count, _rows} = Repo.insert_all(ForwardedGenerationEnd, [row], on_conflict: :nothing, conflict_target: :attempt_id, timeout: @write_budget_ms)
        :ok

      :error ->
        {:error, :invalid_attempt_id}
    end
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] -> {:error, error.__struct__}
  end

  def record(_attempt_id, _reason), do: {:error, :invalid_reason}

  @doc "Whether the owner that served `attempt` recorded the end of its generation."
  @spec ended?(map()) :: boolean()
  def ended?(%{id: attempt_id}) when is_binary(attempt_id),
    do: Repo.exists?(from ending in ForwardedGenerationEnd, where: ending.attempt_id == ^attempt_id)

  def ended?(_attempt), do: false

  @doc """
  Deletes the rows older than `retention_seconds/0`, oldest first, a thousand
  at a time. Age is read on the database clock, which also stamped
  `ended_at`, so no node's clock shortens or extends the window.
  """
  @spec prune(DateTime.t()) :: {:ok, %{forwarded_generation_ends_pruned: non_neg_integer()}}
  def prune(_now) do
    expired =
      from ending in ForwardedGenerationEnd,
        where: ending.ended_at < fragment("(statement_timestamp() AT TIME ZONE 'UTC') - (? * interval '1 second')", ^@retention_seconds),
        order_by: [asc: ending.ended_at, asc: ending.attempt_id],
        limit: 1_000,
        select: ending.attempt_id

    {count, _rows} = Repo.delete_all(from ending in ForwardedGenerationEnd, where: ending.attempt_id in subquery(expired))
    {:ok, %{forwarded_generation_ends_pruned: count}}
  end
end
