defmodule CodexPooler.Accounting.RequestLifecycle.AdmissionExecutionRecovery do
  @moduledoc false
  import Ecto.Query

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request, RequestLifecycle, RequestReplayEntitlement}
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Platform.{ExecutionTerminalProof, ExecutionTerminalProofs, InstancePresence, TransientDatabaseError}
  alias CodexPooler.Repo

  @request_statuses ~w(accepted in_progress)
  @recovery_code "admission_execution_recovered"
  @database_unavailable :admission_recovery_database_unavailable
  @identity_fields [:admission_instance_id, :admission_instance_boot_id, :admission_process_id, :admission_execution_id]

  @type summary :: %{
          required(:admission_only_requests_recovered) => non_neg_integer(),
          optional(:after_commit_markers) => [PreAttemptRelease.marker()]
        }
  @type result :: {:ok, summary()} | {:error, term(), summary()}

  @spec recover(DateTime.t(), keyword()) :: result()
  def recover(now, opts \\ []), do: recover_candidates(:scheduled, now, opts)

  @spec recover_published(DateTime.t(), keyword()) :: result()
  def recover_published(now, opts \\ []), do: recover_candidates(:published, now, opts)

  @spec recover_execution_ids([Ecto.UUID.t()], DateTime.t(), keyword()) :: result()
  def recover_execution_ids(ids, now, opts \\ []) when is_list(ids),
    do: recover_candidates({:execution_ids, ids}, now, opts)

  defp recover_candidates(selector, now, opts) do
    caller_owned_transaction? = Repo.in_transaction?()

    opts =
      opts
      |> Keyword.put(:include_superseded, selector == :scheduled)
      |> Keyword.put(:skip_locked, selector == :published and not caller_owned_transaction?)
      |> put_query_deadline()

    {summary, failures, markers} =
      selector
      |> candidates(opts)
      |> Enum.reduce_while({initial_summary(), [], []}, fn candidate, acc ->
        recover_candidate(candidate, acc, now, opts)
      end)

    summary = publish_committed_markers(summary, markers, caller_owned_transaction?)

    if failures == [],
      do: {:ok, summary},
      else: {:error, {:admission_execution_candidates_failed, Enum.reverse(failures)}, summary}
  rescue
    exception ->
      {:error, {:admission_execution_candidates_failed, [{nil, failure_reason(exception)}]}, initial_summary()}
  catch
    :exit, _reason ->
      {:error, {:admission_execution_candidates_failed, [{nil, @database_unavailable}]}, initial_summary()}
  end

  defp candidates(selector, opts) do
    eligible =
      from request in Request,
        as: :request,
        where:
          request.status in ^@request_statuses and is_nil(request.completed_at) and
            not is_nil(request.admission_execution_id) and not is_nil(request.admission_instance_id) and
            not is_nil(request.admission_instance_boot_id) and not is_nil(request.admission_process_id),
        where:
          exists(
            from entry in LedgerEntry,
              where:
                entry.request_id == parent_as(:request).id and entry.entry_kind == "reservation" and
                  entry.amount_status == "recorded",
              select: 1
          ),
        where:
          not exists(
            from entry in LedgerEntry,
              where: entry.request_id == parent_as(:request).id and entry.entry_kind in ["release", "settlement"],
              select: 1
          ),
        where:
          not exists(
            from attempt in Attempt,
              where: attempt.request_id == parent_as(:request).id,
              select: 1
          ),
        where:
          not exists(
            from turn in CodexTurn,
              where: turn.request_id == parent_as(:request).id,
              select: 1
          ),
        where:
          not exists(
            from replay in RequestReplayEntitlement,
              where: replay.request_id == parent_as(:request).id,
              select: 1
          ),
        order_by: [asc: fragment("COALESCE(?, ?)", request.admission_execution_checked_at, request.admitted_at), asc: request.id],
        limit: ^min(Keyword.get(opts, :limit, 100), 100)

    eligible
    |> select_authority(selector)
    |> skip_locked_candidates(opts)
    |> Repo.all(query_options(opts))
  end

  defp select_authority(query, :scheduled), do: query

  defp select_authority(query, :published), do: join_terminal_proof(query)

  defp select_authority(query, {:execution_ids, ids}) do
    query
    |> where([request], request.admission_execution_id in ^ids)
    |> join_terminal_proof()
  end

  defp skip_locked_candidates(query, opts) do
    if Keyword.fetch!(opts, :skip_locked), do: lock(query, "FOR UPDATE SKIP LOCKED"), else: query
  end

  # Publication notifications are an accelerator, not a queue: this exact-proof
  # join retains work through a publisher/runner restart and never makes live
  # waiters consume a proof-only recovery batch.
  defp join_terminal_proof(query) do
    from request in query,
      join: proof in ExecutionTerminalProof,
      on:
        proof.execution_id == request.admission_execution_id and
          proof.owner_instance_id == request.admission_instance_id and
          proof.owner_instance_boot_id == request.admission_instance_boot_id and
          proof.owner_process_id == request.admission_process_id,
      where: is_nil(proof.interruption_code),
      select: request
  end

  defp recover_candidate(candidate, {summary, failures, markers}, now, opts) do
    case recover_candidate(candidate, now, opts) do
      {:ok, :recovered, committed_markers} ->
        summary = %{summary | admission_only_requests_recovered: summary.admission_only_requests_recovered + 1}
        {:cont, {summary, failures, markers ++ committed_markers}}

      {:ok, :noop, []} ->
        {:cont, {summary, failures, markers}}

      {:error, reason} ->
        acc = {summary, [{candidate.id, reason} | failures], markers}
        if reason == @database_unavailable, do: {:halt, acc}, else: {:cont, acc}
    end
  end

  defp recover_candidate(candidate, now, opts) do
    # Durable progress advances failed/live candidates behind untouched rows,
    # even if settlement rolls back or a different replica runs the next pass.
    db_opts = query_options(opts)

    case stamp_candidate(candidate, now, opts, db_opts) do
      {0, _rows} ->
        {:ok, :noop, []}

      {1, _rows} ->
        finalize_candidate(candidate, now, opts, db_opts)
    end
  rescue
    exception -> {:error, failure_reason(exception)}
  catch
    :exit, _reason -> {:error, @database_unavailable}
  end

  defp finalize_candidate(candidate, now, opts, db_opts) do
    case Repo.transaction(fn -> recover_locked(candidate, now, opts, db_opts) end, db_opts) do
      {:ok, {:recovered, markers}} -> {:ok, :recovered, markers}
      {:ok, :noop} -> {:ok, :noop, []}
      {:error, reason} -> {:error, reason}
    end
  end

  defp stamp_candidate(candidate, now, opts, db_opts) do
    query =
      from(request in Request,
        where:
          request.id == ^candidate.id and request.status in ^@request_statuses and
            request.admission_execution_id == ^candidate.admission_execution_id and
            request.admission_instance_id == ^candidate.admission_instance_id and
            request.admission_instance_boot_id == ^candidate.admission_instance_boot_id and
            request.admission_process_id == ^candidate.admission_process_id
      )

    query =
      if Keyword.fetch!(opts, :skip_locked) do
        # A row another lifecycle transaction holds is deferred, not a database
        # outage. Keep its proof queued while this pass reaches unlocked work.
        available = from(request in query, select: request.id, lock: "FOR UPDATE SKIP LOCKED")
        from(request in Request, where: request.id in subquery(available))
      else
        query
      end

    Repo.update_all(query, [set: [admission_execution_checked_at: now]], db_opts)
  end

  defp recover_locked(candidate, now, opts, db_opts) do
    query = from(request in Request, where: request.id == ^candidate.id)
    query = if Keyword.fetch!(opts, :skip_locked), do: lock(query, "FOR UPDATE SKIP LOCKED"), else: lock(query, "FOR UPDATE")
    request = Repo.one(query, db_opts)

    # create_attempt and replay consumption serialize on this request lock.
    # Recheck everything after the wait; a stale candidate cannot release a
    # request the dispatcher or replay owner transitioned in the meantime.
    if admission_only?(request, candidate, db_opts) and recovery_authorized?(request, opts, db_opts) do
      case RequestLifecycle.finalize_reserved_request_failure(request, %{
             request_status: "failed",
             response_status_code: 499,
             last_error_code: @recovery_code,
             usage_status: "not_applicable",
             pre_attempt_phase: PreAttemptRelease.turn_interrupted(),
             now: now
           }) do
        {:ok, result} -> {:recovered, Map.get(result, :after_commit_markers, [])}
        {:error, reason} -> Repo.rollback(reason)
      end
    else
      :noop
    end
  end

  defp admission_only?(%Request{} = request, candidate, opts) do
    request.status in @request_statuses and is_nil(request.completed_at) and
      Map.take(request, @identity_fields) == Map.take(candidate, @identity_fields) and
      admission_lifecycle_unstarted?(request.id, opts) and admission_reservation_open?(request.id, opts)
  end

  defp admission_only?(_missing, _candidate, _opts), do: false

  defp admission_lifecycle_unstarted?(id, opts) do
    not Repo.exists?(from(attempt in Attempt, where: attempt.request_id == ^id), opts) and
      not Repo.exists?(from(turn in CodexTurn, where: turn.request_id == ^id), opts) and
      not Repo.exists?(from(replay in RequestReplayEntitlement, where: replay.request_id == ^id), opts)
  end

  defp admission_reservation_open?(id, opts) do
    Repo.exists?(from(entry in LedgerEntry, where: entry.request_id == ^id and entry.entry_kind == "reservation" and entry.amount_status == "recorded"), opts) and
      not Repo.exists?(from(entry in LedgerEntry, where: entry.request_id == ^id and entry.entry_kind in ["release", "settlement"]), opts)
  end

  defp recovery_authorized?(request, opts, db_opts) do
    identity = execution_identity(request)
    proof_opts = Keyword.put(db_opts, :include_interrupted, Keyword.get(opts, :include_superseded, false))

    ExecutionTerminalProofs.valid_identity?(identity) and
      (ExecutionTerminalProofs.terminal?(identity, proof_opts) or
         (Keyword.get(opts, :include_superseded, true) and superseded_instance?(identity, opts)))
  end

  defp superseded_instance?(identity, opts) do
    owner = InstancePresence.Identity.owner(identity.owner_instance_id, identity.owner_instance_boot_id)
    now = InstancePresence.database_now()

    InstancePresence.observer_fresh?(now, opts) and
      InstancePresence.absent?(owner, now, opts) and InstancePresence.superseded?(owner)
  end

  defp execution_identity(request) do
    %{
      owner_instance_id: request.admission_instance_id,
      owner_instance_boot_id: request.admission_instance_boot_id,
      owner_process_id: request.admission_process_id,
      owner_execution_id: request.admission_execution_id
    }
  end

  defp put_query_deadline(opts) do
    case Keyword.get(opts, :timeout) do
      timeout when is_integer(timeout) and timeout > 0 -> Keyword.put_new(opts, :deadline, System.monotonic_time(:millisecond) + timeout)
      _default -> opts
    end
  end

  defp query_options(opts), do: opts |> Keyword.take([:timeout, :deadline]) |> Keyword.put(:checkout_retries, 0)

  defp failure_reason(exception) do
    if TransientDatabaseError.transient?(exception), do: @database_unavailable, else: exception.__struct__
  end

  defp publish_committed_markers(summary, [], _caller_owned_transaction?), do: summary

  defp publish_committed_markers(summary, markers, true), do: Map.put(summary, :after_commit_markers, markers)

  defp publish_committed_markers(summary, markers, false) do
    Enum.each(markers, &PreAttemptRelease.emit_marker/1)
    summary
  end

  defp initial_summary, do: %{admission_only_requests_recovered: 0}
end
