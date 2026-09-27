defmodule CodexPooler.Upstreams.Quota.Windows.ExpiredPruning do
  @moduledoc """
  Deletes `account_quota_windows` rows whose reset passed long ago.

  Evidence rows are keyed by identity, source and descriptor, and a writer
  only ever rewrites the row of a descriptor it observes again. A descriptor
  the provider stops reporting (a retired model window, a response-header or
  rate-limit-event shape an account no longer produces, a Usage API window a
  complete poll no longer covers) therefore keeps its last row forever, still
  carrying the `freshness_state` it was written with. Every reader already
  treats such a row as stale; this pass removes it once it can no longer
  describe any cycle, so the table only holds evidence that can still matter.

  A row is deleted when it is past retention (`Retention.past_retention?/2`):
  its `reset_at` passed more than `retention_seconds/0` ago. A row carrying the saved-reset
  automatic-confirmation marker is deleted only when that marker has lapsed
  by the same cutoff (`AutomaticConfirmation.lapsed_before?/2`: every reset
  instant it carries passed before the cutoff, or it is malformed), so no
  confirmation, approach witness or claim can still read it. A row another
  transaction holds locked is never deleted: a saved-reset claim and its
  reservation lock their proof rows `FOR UPDATE`, and those rows are skipped
  rather than waited for. Deletion runs per identity under the same identity-first locks as the
  evidence writers (`EvidenceStore.lock_evidence_identity!/1`), and the
  candidate conditions are re-evaluated under those locks, so a row a
  concurrent observation refreshed is kept. A pass handles at most
  `batch_size/0` candidate rows (or the caller's smaller batch). A durable Oban
  continuation resumes beyond that page, so retained markers cannot starve later rows. Read surfaces
  already ignore rows past retention, so the pass reclaims space and keeps SQL
  forensics honest without changing any decision.
  """

  import Ecto.Query

  require Logger

  alias CodexPooler.Jobs.ExpiredQuotaPruningWorker
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Quota.Windows.EvidenceStore
  alias CodexPooler.Upstreams.Quota.Windows.Retention
  alias CodexPooler.Upstreams.SavedResets.AutomaticConfirmation

  @batch_size 500

  @type summary :: %{expired_quota_windows_pruned: non_neg_integer()}

  @spec retention_seconds() :: pos_integer()
  def retention_seconds, do: Retention.retention_seconds()

  @spec batch_size() :: pos_integer()
  def batch_size, do: @batch_size

  @spec prune(DateTime.t(), keyword()) :: {:ok, summary()} | {:error, term()}
  def prune(%DateTime{} = now, opts \\ []) do
    cutoff = Retention.cutoff(now)
    batch_size = min(Keyword.get(opts, :batch_size, @batch_size), @batch_size)
    cursor = Keyword.get(opts, :after)

    Repo.transact(fn ->
      rows =
        cutoff
        |> candidate_query()
        |> after_cursor(cursor)
        |> order_by([window], asc: window.reset_at, asc: window.id)
        |> limit(^batch_size)
        |> select([window], {window.upstream_identity_id, window.id, window.metadata, window.reset_at})
        |> Repo.all()

      pruned =
        rows
        |> Enum.filter(fn {identity, id, metadata, _reset} -> lapsed_marker?({identity, id, metadata}, cutoff) end)
        |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
        |> Enum.sort_by(&elem(&1, 0))
        |> Enum.reduce(0, fn {identity, ids}, count -> count + prune_identity(identity, ids, cutoff) end)

      with :ok <- enqueue_continuation(rows, batch_size, now) do
        log_pruned(pruned)
        {:ok, %{expired_quota_windows_pruned: pruned}}
      end
    end)
  end

  defp log_pruned(0), do: :ok
  defp log_pruned(pruned), do: Logger.info("quota window cleanup deleted #{pruned} rows whose reset passed more than #{div(Retention.retention_seconds(), 86_400)} days ago")

  defp enqueue_continuation(rows, batch_size, now) when length(rows) == batch_size and batch_size > 0 do
    {_identity, id, _metadata, reset_at} = List.last(rows)
    args = %{"now" => DateTime.to_iso8601(now), "cursor_reset_at" => DateTime.to_iso8601(reset_at), "cursor_id" => id, "batch_size" => batch_size}

    case Oban.insert(ExpiredQuotaPruningWorker.new(args), retry: false) do
      {:ok, _job} -> :ok
      {:error, _reason} -> {:error, :quota_pruning_enqueue_failed}
    end
  end

  defp enqueue_continuation(_rows, _batch_size, _now), do: :ok

  defp after_cursor(query, nil), do: query
  defp after_cursor(query, {reset_at, id}), do: where(query, [window], window.reset_at > ^reset_at or (window.reset_at == ^reset_at and window.id > ^id))

  defp prune_identity(identity_id, window_ids, cutoff) do
    {:ok, deleted} =
      Repo.transaction(fn ->
        :ok = EvidenceStore.lock_evidence_identity!(identity_id)

        deletable_ids =
          cutoff
          |> candidate_query()
          |> where([window], window.upstream_identity_id == ^identity_id and window.id in ^window_ids)
          |> lock("FOR UPDATE SKIP LOCKED")
          |> select([window], {window.upstream_identity_id, window.id, window.metadata})
          |> Repo.all()
          |> Enum.filter(&lapsed_marker?(&1, cutoff))
          |> Enum.map(&elem(&1, 1))

        {count, _rows} =
          Repo.delete_all(from(window in AccountQuotaWindow, where: window.id in ^deletable_ids))

        count
      end)

    if deleted > 0, do: Windows.broadcast_quota_update(identity_id)

    deleted
  end

  # Durable keyset pages advance past retained markers while the identity-locked
  # recheck still protects refreshed rows.
  defp candidate_query(cutoff) do
    from(window in AccountQuotaWindow, where: window.reset_at < ^cutoff)
  end

  defp lapsed_marker?({_identity_id, _id, metadata}, cutoff),
    do: AutomaticConfirmation.lapsed_before?(metadata, cutoff)
end
