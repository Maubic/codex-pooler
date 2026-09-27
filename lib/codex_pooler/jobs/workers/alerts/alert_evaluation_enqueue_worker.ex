defmodule CodexPooler.Jobs.AlertEvaluationEnqueueWorker do
  @moduledoc """
  Periodically enqueues alert rule evaluation jobs for active alert rules.
  """

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 1,
    tags: ["alert_evaluation_enqueue"],
    unique: [
      fields: [:worker, :queue, :args],
      keys: [:evaluation_window_started_at, :cursor_created_at, :cursor_id],
      states: :incomplete,
      period: :infinity
    ]

  require Logger

  alias CodexPooler.Alerts
  alias CodexPooler.Jobs

  @impl Oban.Worker
  def new(args, opts) do
    # Cron roots have no durable window identity. A dead executor must not
    # suppress future windows while Lifeline waits to rescue the old row.
    opts = if map_size(args) == 0, do: Keyword.put_new(opts, :unique, period: {5, :minutes}), else: opts
    super(args, opts)
  end

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.seconds(30)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"evaluation_window_started_at" => window, "fanout_started_at" => cutoff, "cursor_created_at" => created_at, "cursor_id" => id} = args}) when is_binary(window) and is_binary(cutoff) and is_binary(created_at) and is_binary(id) do
    with {:ok, window, _} <- DateTime.from_iso8601(window),
         {:ok, cutoff, _} <- DateTime.from_iso8601(cutoff),
         {:ok, created_at, _} <- DateTime.from_iso8601(created_at),
         {:ok, id} <- Ecto.UUID.cast(id) do
      enqueue_page(window, cutoff, {created_at, id}, Map.get(args, "trigger_kind", "scheduled"))
    else
      _ -> {:cancel, :invalid_alert_evaluation_args}
    end
  end

  def perform(%Oban.Job{args: args, scheduled_at: %DateTime{} = scheduled_at}) when map_size(args) == 0 do
    # Incidents whose rules were all deleted are resolved here, because no
    # per-rule evaluation will ever clear them (findings#260 row 260-31). A
    # failed resolution never holds back the evaluations themselves.
    case Alerts.resolve_orphaned_incidents(scheduled_at) do
      {:ok, _resolved} -> :ok
      {:error, %Ecto.Changeset{}} -> Logger.warning("alert orphaned incident resolution failed error=invalid_incident_changeset")
    end

    enqueue_page(scheduled_at, scheduled_at, nil)
  end

  def perform(%Oban.Job{}), do: {:cancel, :invalid_alert_evaluation_args}

  defp enqueue_page(window, cutoff, cursor, trigger_kind \\ "scheduled") do
    case Jobs.enqueue_alert_evaluation_page(window, cutoff, cursor, trigger_kind: trigger_kind) do
      {:ok, _result} -> :ok
      {:error, _reason} = error -> error
    end
  end
end
