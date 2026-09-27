defmodule CodexPooler.Jobs.UpstreamDeletionWorker do
  @moduledoc "Permanently removes an upstream after admitted work drains, with bounded shared database batches."

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 5,
    tags: ["upstream_deletion"],
    unique: [fields: [:args, :queue, :worker], keys: [:upstream_identity_id], states: :incomplete, period: :infinity]

  alias CodexPooler.Upstreams

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.minutes(2)

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"upstream_identity_id" => identity_id} = args}) when is_binary(identity_id) do
    case Upstreams.continue_account_deletion(identity_id, Map.get(args, "requested_by_user_id"), System.monotonic_time(:millisecond) + 45_000) do
      :more -> {:snooze, 1}
      :deleted -> :ok
      :gone -> :ok
      {:cancel, reason} -> {:cancel, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  def perform(%Oban.Job{}), do: {:cancel, :upstream_deletion_target_invalid}
end
