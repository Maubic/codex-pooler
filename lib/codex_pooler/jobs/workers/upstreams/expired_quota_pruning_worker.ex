defmodule CodexPooler.Jobs.ExpiredQuotaPruningWorker do
  @moduledoc "Continues a bounded quota-retention scan from its durable cursor."

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 5,
    tags: ["quota_pruning"],
    unique: [fields: [:worker, :args], keys: [:now, :cursor_reset_at, :cursor_id], states: :incomplete, period: :infinity]

  alias CodexPooler.Upstreams.Quota.Windows.ExpiredPruning

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.seconds(30)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"now" => now, "cursor_reset_at" => reset_at, "cursor_id" => id, "batch_size" => size}}) when is_binary(now) and is_binary(reset_at) and is_binary(id) and is_integer(size) and size > 0 and size <= 500 do
    with {:ok, now, _} <- DateTime.from_iso8601(now),
         {:ok, reset_at, _} <- DateTime.from_iso8601(reset_at),
         {:ok, id} <- Ecto.UUID.cast(id) do
      case ExpiredPruning.prune(now, after: {reset_at, id}, batch_size: size) do
        {:ok, _summary} -> :ok
        {:error, _reason} = error -> error
      end
    else
      _ -> {:cancel, :invalid_quota_pruning_args}
    end
  end

  def perform(%Oban.Job{}), do: {:cancel, :invalid_quota_pruning_args}
end
