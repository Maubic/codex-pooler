defmodule CodexPooler.Accounting.RequestLifecycle.DeadExecutionRecovery do
  @moduledoc false
  import Ecto.Query

  alias CodexPooler.Accounting.{
    Attempt,
    LedgerEntry,
    Request,
    RequestLifecycle,
    RequestReplayEntitlement
  }

  alias CodexPooler.Gateway.Runtime.Finalization.InterruptionOutcome
  alias CodexPooler.Platform.{ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Repo

  @type summary :: %{
          required(:dead_execution_attempts_recovered) => non_neg_integer(),
          optional(:after_commit_markers) => [map()]
        }
  @spec recover(DateTime.t(), keyword()) :: {:ok, summary()} | {:error, term(), summary()}
  def recover(now, opts \\ []) do
    cutoff = DateTime.add(now, -Keyword.get(opts, :minimum_age_seconds, 120), :second)
    limit = Keyword.get(opts, :limit, 100)
    caller_owned_transaction? = Repo.in_transaction?()

    {summary, failures, markers} =
      cutoff
      |> candidates(limit)
      |> Enum.reduce(
        {%{dead_execution_attempts_recovered: 0}, [], []},
        &recover_candidate(&1, &2, now, caller_owned_transaction?)
      )

    summary = put_after_commit_markers(summary, markers)

    if failures == [],
      do: {:ok, summary},
      else: {:error, {:dead_execution_candidates_failed, Enum.reverse(failures)}, summary}
  end

  @spec recover_execution_ids([Ecto.UUID.t()], DateTime.t(), keyword()) ::
          {:ok, summary()} | {:error, term(), summary()}
  def recover_execution_ids(ids, now, opts \\ []) when is_list(ids) do
    recover_published_candidates(Enum.uniq(ids), now, opts)
  end

  @spec recover_published(DateTime.t(), keyword()) ::
          {:ok, summary()} | {:error, term(), summary()}
  def recover_published(now, opts \\ []), do: recover_published_candidates(:all, now, opts)

  # These paths are proof-driven, not age-driven. The durable joined scan is
  # also the restart backstop for a publication whose notification was lost.
  # Candidate selection and locked settlement both match the full execution
  # identity; a UUID alone never proves an executor ended.
  defp recover_published_candidates(ids, now, opts) do
    caller_owned_transaction? = Repo.in_transaction?()
    limit = Keyword.get(opts, :limit, 100)

    query_opts =
      opts
      |> Keyword.take([:timeout, :deadline, :checkout_retries])
      |> Keyword.put_new(:deadline, System.monotonic_time(:millisecond) + Keyword.get(opts, :timeout, 500))

    skip_locked? = ids == :all and not caller_owned_transaction?

    candidates =
      nil
      |> eligible_candidates(limit)
      |> join(:inner, [a], proof in ExecutionTerminalProof,
        on:
          proof.execution_id == a.owner_execution_id and
            proof.owner_instance_id == a.owner_instance_id and
            proof.owner_instance_boot_id == a.owner_instance_boot_id and
            proof.owner_process_id == a.owner_process_id and is_nil(proof.interruption_code)
      )
      |> filter_execution_ids(ids)
      |> available_attempts(skip_locked?)
      |> candidate_rows()
      |> Repo.all(query_opts)

    recovery_opts = query_opts |> Keyword.put(:include_interrupted, false) |> Keyword.put(:skip_locked, skip_locked?)
    recover_published_rows(candidates, now, recovery_opts, caller_owned_transaction?)
  rescue
    exception -> published_scan_failed(ids, exception.__struct__)
  catch
    :exit, _reason -> published_scan_failed(ids, :execution_recovery_unavailable)
  end

  defp filter_execution_ids(query, :all), do: query
  defp filter_execution_ids(query, ids), do: where(query, [a], a.owner_execution_id in ^ids)

  defp available_attempts(query, true), do: lock(query, "FOR UPDATE SKIP LOCKED")
  defp available_attempts(query, false), do: query

  defp published_scan_failed(_ids, reason) do
    summary = %{dead_execution_attempts_recovered: 0}
    {:error, {:dead_execution_candidates_failed, [{nil, reason}]}, summary}
  end

  defp recover_published_rows(candidates, now, opts, caller_owned_transaction?) do
    initial = {%{dead_execution_attempts_recovered: 0}, [], []}

    {summary, failures, markers} =
      Enum.reduce_while(candidates, initial, fn candidate, accumulator ->
        recover_published_row(candidate, accumulator, now, opts, caller_owned_transaction?)
      end)

    summary = put_after_commit_markers(summary, markers)

    if failures == [],
      do: {:ok, summary},
      else: {:error, {:dead_execution_candidates_failed, Enum.reverse(failures)}, summary}
  end

  defp recover_published_row({request, attempt}, {summary, failures, markers}, now, opts, caller_owned_transaction?) do
    case recover_published_candidate(request, attempt, now, opts) do
      {:ok, :recovered} ->
        marker = recovery_outcome_marker(request, attempt)
        summary = %{summary | dead_execution_attempts_recovered: summary.dead_execution_attempts_recovered + 1}
        markers = committed_recovery_markers(marker, markers, caller_owned_transaction?)
        {:cont, {summary, failures, markers}}

      {:ok, :noop} ->
        {:cont, {summary, failures, markers}}

      {:error, reason} ->
        failures = [{attempt.id, reason} | failures]
        if database_unavailable?(reason), do: {:halt, {summary, failures, markers}}, else: {:cont, {summary, failures, markers}}
    end
  end

  defp committed_recovery_markers(marker, markers, true), do: [marker | markers]

  defp committed_recovery_markers(marker, markers, false) do
    emit_recovery_outcome(marker)
    markers
  end

  defp recover_published_candidate(request, attempt, now, opts) do
    query =
      from a in Attempt,
        where: a.id == ^attempt.id and a.owner_execution_id == ^attempt.owner_execution_id

    query =
      if Keyword.get(opts, :skip_locked, false) do
        available = from a in query, select: a.id, lock: "FOR UPDATE SKIP LOCKED"
        from a in Attempt, where: a.id in subquery(available)
      else
        query
      end

    case Repo.update_all(query, [set: [owner_execution_checked_at: now]], opts) do
      {0, _rows} -> {:ok, :noop}
      {1, _rows} -> RequestLifecycle.recover_dead_execution(request, attempt, now, opts)
    end
  rescue
    exception in Postgrex.Error ->
      if Keyword.get(opts, :skip_locked, false) and exception.postgres[:code] == :lock_not_available,
        do: {:ok, :noop},
        else: {:error, exception.__struct__}

    exception ->
      {:error, exception.__struct__}
  catch
    :exit, _reason -> {:error, :execution_recovery_unavailable}
  end

  defp database_unavailable?(reason),
    do: reason in [DBConnection.ConnectionError, Postgrex.Error, :execution_recovery_unavailable]

  defp candidates(cutoff, limit), do: cutoff |> eligible_candidates(limit) |> candidate_rows() |> Repo.all()

  defp eligible_candidates(cutoff, limit) do
    # Keep eligibility checks correlated: flattening them into joins makes Postgres
    # scan and sort every open attempt before applying the batch limit.
    eligible =
      from a in Attempt,
        as: :attempt,
        where:
          a.status in ["queued", "in_progress"] and a.replay_generation == 0 and
            not is_nil(a.owner_execution_id),
        where:
          fragment(
            "? IS TRUE",
            exists(
              from request in Request,
                where:
                  request.id == parent_as(:attempt).request_id and
                    request.status in ["accepted", "in_progress"],
                select: 1
            )
          ),
        where:
          fragment(
            "? IS TRUE",
            exists(
              from entry in LedgerEntry,
                where:
                  entry.request_id == parent_as(:attempt).request_id and
                    entry.entry_kind == "reservation" and entry.amount_status == "recorded",
                select: 1
            )
          ),
        where:
          not fragment(
            "? IS TRUE",
            exists(
              from entry in LedgerEntry,
                where:
                  entry.request_id == parent_as(:attempt).request_id and
                    entry.entry_kind == "release",
                select: 1
            )
          ),
        where:
          not fragment(
            "? IS TRUE",
            exists(
              from replay in RequestReplayEntitlement,
                where: replay.request_id == parent_as(:attempt).request_id,
                select: 1
            )
          ),
        order_by: [
          asc: fragment("COALESCE(?, ?)", a.owner_execution_checked_at, a.started_at),
          asc: a.id
        ],
        limit: ^limit

    if cutoff, do: where(eligible, [a], a.started_at <= ^cutoff), else: eligible
  end

  defp candidate_rows(eligible) do
    from a in subquery(eligible),
      join: r in Request,
      on: r.id == a.request_id,
      order_by: [
        asc: fragment("COALESCE(?, ?)", a.owner_execution_checked_at, a.started_at),
        asc: a.id
      ],
      select: {r, a}
  end

  # A recovered candidate's `interrupted` outcome is emitted here only when
  # this call owns no transaction. Inside a caller-owned transaction the
  # recovery has released a savepoint, not committed, so the marker is handed
  # back on the summary for the outermost commit to publish through
  # `Interruption.emit_committed_deferred_outcomes/1` — the shape
  # `AbsentInstanceRecovery` uses. It used to be dropped on that path with no
  # marker returned, which a future transactional caller would have paid for
  # as silently missing `interrupted` counts (findings#224).
  defp recover_candidate({request, attempt}, {summary, failures, markers}, now, caller_owned_transaction?) do
    case recover_candidate(request, attempt, now) do
      {:ok, :recovered, marker} ->
        summary = %{
          summary
          | dead_execution_attempts_recovered: summary.dead_execution_attempts_recovered + 1
        }

        if caller_owned_transaction? do
          {summary, failures, [marker | markers]}
        else
          emit_recovery_outcome(marker)
          {summary, failures, markers}
        end

      {:ok, :noop} ->
        {summary, failures, markers}

      {:error, reason} ->
        {summary, [{attempt.id, reason} | failures], markers}
    end
  end

  defp recover_candidate(request, attempt, now) do
    # Persist scheduling progress so another replica or a later job advances
    # past active executions instead of repeatedly selecting the oldest batch.
    from(a in Attempt,
      where: a.id == ^attempt.id and a.owner_execution_id == ^attempt.owner_execution_id
    )
    |> Repo.update_all(set: [owner_execution_checked_at: now])

    result =
      if ExecutionTerminalProofs.terminal?(attempt),
        do: RequestLifecycle.recover_dead_execution(request, attempt, now),
        else: {:ok, :noop}

    case result do
      {:ok, :recovered} -> {:ok, :recovered, recovery_outcome_marker(request, attempt)}
      other -> other
    end
  rescue
    exception -> {:error, exception.__struct__}
  catch
    :exit, _reason -> {:error, :execution_recovery_unavailable}
  end

  defp put_after_commit_markers(summary, []), do: summary

  defp put_after_commit_markers(summary, markers),
    do: Map.put(summary, :after_commit_markers, Enum.reverse(markers))

  defp recovery_outcome_marker(request, attempt) do
    %{
      kind: :stream_outcome,
      outcome: "interrupted",
      downstream_transport: bounded_transport(request.transport),
      upstream_transport: bounded_transport(attempt.transport)
    }
  end

  defp emit_recovery_outcome(marker),
    do: InterruptionOutcome.emit(marker.downstream_transport, marker.upstream_transport)

  defp bounded_transport(transport) when transport in ["http_sse", "websocket"], do: transport
  defp bounded_transport(_), do: "unknown"
end
