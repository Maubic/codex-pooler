defmodule CodexPooler.Jobs.ReadModel.SavedResetRequests do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Jobs.SavedResetRedemptionWorker
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment
  alias CodexPooler.Upstreams.StatusVocabulary.Assignment, as: AssignmentStatus

  @assignment_deleted AssignmentStatus.deleted_status()
  # The worker's uniqueness counts these states as an incomplete request (Oban's `:incomplete`), so a held
  # `suspended` job still reads as an open request and a new submission would join it.
  @open_states ~w(suspended available scheduled executing retryable)

  @type request_state :: :queued | :processing | :completed | :discarded | :cancelled | :stopped
  @type request :: %{
          required(:state) => request_state(),
          required(:requested_at) => DateTime.t(),
          required(:scheduled_at) => DateTime.t() | nil
        }
  @type summary :: %{required(:open) => request() | nil, required(:latest_terminal) => request() | nil}
  @type summaries :: %{optional(Ecto.UUID.t()) => summary()}
  @type options :: [pool_ids: [Ecto.UUID.t()]]

  @doc """
  Reads retained manual request facts for currently visible assignment targets.

  Only requests with both persisted nested target bindings are included; legacy unbound
  rows cannot prove their original target after an assignment changes.
  Each identity has at most one open and one terminal request. A terminal
  request carries how its job ended (`:completed`, `:discarded`, `:cancelled`,
  or `:stopped` for any other state). That is job context only, never provider
  application, spend, actor attribution or correlation with the identity's
  latest reset lifecycle.
  """
  @spec summaries(Scope.t(), [Ecto.UUID.t()], options()) :: summaries()
  def summaries(scope, identity_ids, opts \\ [])

  def summaries(%Scope{user: %{id: user_id}} = scope, identity_ids, opts) when is_list(identity_ids) and is_list(opts) do
    with {:ok, _user_id} <- Ecto.UUID.cast(user_id),
         true <- Keyword.keyword?(opts),
         [_ | _] = requested_ids <- normalize_ids(identity_ids),
         [_ | _] = pool_ids <- visible_pool_ids(scope, opts),
         [_ | _] = visible_ids <- visible_identity_ids(scope, requested_ids, pool_ids) do
      visible_ids
      |> latest_requests_query(pool_ids)
      |> Repo.all()
      |> summarize_requests(visible_ids)
    else
      _invalid_or_inaccessible -> %{}
    end
  end

  def summaries(_scope, _identity_ids, _opts), do: %{}

  defp summarize_requests(rows, identity_ids) do
    empty = Map.new(identity_ids, &{&1, %{open: nil, latest_terminal: nil}})
    Enum.reduce(rows, empty, &put_request_summary/2)
  end

  defp put_request_summary(row, summaries) do
    key = if row.state in @open_states, do: :open, else: :latest_terminal
    request = %{state: request_state(row.state), requested_at: row.requested_at, scheduled_at: row.scheduled_at}
    Map.update!(summaries, row.identity_id, &Map.put(&1, key, request))
  end

  defp visible_pool_ids(scope, opts) do
    visible_ids = scope |> Pools.list_visible_pools() |> Enum.map(& &1.id)

    case Keyword.fetch(opts, :pool_ids) do
      :error ->
        visible_ids

      {:ok, ids} when is_list(ids) ->
        selected_ids = normalize_ids(ids)
        Enum.filter(visible_ids, &(&1 in selected_ids))

      {:ok, _invalid} ->
        []
    end
  end

  defp visible_identity_ids(scope, requested_ids, pool_ids) do
    scope
    |> Upstreams.list_visible_upstream_identities(pool_ids: pool_ids, include_unassigned: false)
    |> Enum.map(& &1.id)
    |> Enum.filter(&(&1 in requested_ids))
  end

  defp latest_requests_query(identity_ids, pool_ids) do
    ranked =
      from job in Oban.Job,
        join: assignment in PoolUpstreamAssignment,
        on: fragment("?->>'pool_upstream_assignment_id'", job.args) == type(assignment.id, :string),
        where: assignment.upstream_identity_id in ^identity_ids and assignment.pool_id in ^pool_ids and assignment.status != ^@assignment_deleted,
        where: job.worker == ^worker_name() and fragment("?->>'trigger_kind'", job.args) == "admin_manual",
        where: not fragment("jsonb_exists(?, 'recovery_kind')", job.args),
        where: fragment("?->'manual_request_target'->>'upstream_identity_id'", job.args) == type(assignment.upstream_identity_id, :string),
        where: fragment("?->'manual_request_target'->>'pool_id'", job.args) == type(assignment.pool_id, :string),
        windows: [
          identity_request: [
            partition_by: [assignment.upstream_identity_id, job.state in ^@open_states],
            order_by: [desc: job.inserted_at, desc: job.id]
          ]
        ],
        select: %{
          identity_id: assignment.upstream_identity_id,
          state: job.state,
          requested_at: job.inserted_at,
          scheduled_at: job.scheduled_at,
          row_number: over(row_number(), :identity_request)
        }

    from row in subquery(ranked),
      where: row.row_number == 1,
      select: %{identity_id: row.identity_id, state: row.state, requested_at: row.requested_at, scheduled_at: row.scheduled_at}
  end

  defp normalize_ids(ids), do: normalize_ids(ids, [])
  defp normalize_ids([], acc), do: Enum.uniq(acc)

  defp normalize_ids([id | rest], acc) do
    case Ecto.UUID.cast(id) do
      {:ok, valid_id} -> normalize_ids(rest, [valid_id | acc])
      :error -> normalize_ids(rest, acc)
    end
  end

  defp normalize_ids(_improper_tail, _acc), do: []

  defp request_state("executing"), do: :processing
  defp request_state(state) when state in @open_states, do: :queued
  defp request_state("completed"), do: :completed
  defp request_state("discarded"), do: :discarded
  defp request_state("cancelled"), do: :cancelled
  defp request_state(_state), do: :stopped

  # Computed at runtime: a module attribute built from the worker module would make this read model a compile-time
  # dependent of the worker (`mix quality.xref` permits none).
  defp worker_name, do: SavedResetRedemptionWorker |> Atom.to_string() |> String.replace_prefix("Elixir.", "")
end
