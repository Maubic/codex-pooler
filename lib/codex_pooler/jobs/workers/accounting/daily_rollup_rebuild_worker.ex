defmodule CodexPooler.Jobs.DailyRollupRebuildWorker do
  @moduledoc """
  Rebuilds accounting rollups for a single UTC date.
  """

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 3,
    tags: ["daily_rollup_rebuild"],
    unique: [
      fields: [:args, :queue, :worker],
      keys: [:rollup_date],
      states: :incomplete,
      period: {7, :days}
    ]

  alias CodexPooler.Accounting

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.minutes(30)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"rollup_date" => text}}) when is_binary(text) do
    case Date.from_iso8601(text) do
      {:ok, date} ->
        case Accounting.rebuild_daily_rollups_for_date(date) do
          {:ok, _count} -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, _reason} ->
        {:cancel, :invalid_rollup_date}
    end
  end

  def perform(%Oban.Job{}), do: {:cancel, :invalid_rollup_date}
end
