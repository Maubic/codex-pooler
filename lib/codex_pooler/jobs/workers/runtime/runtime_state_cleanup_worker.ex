defmodule CodexPooler.Jobs.RuntimeStateCleanupWorker do
  @moduledoc false
  use Oban.Worker,
    queue: :jobs,
    max_attempts: 3,
    tags: ["runtime_state_cleanup"],
    unique: [
      fields: [:worker, :queue],
      states: :incomplete,
      period: {15, :minutes}
    ]

  alias CodexPooler.Jobs.RuntimeStateCleanup
  alias CodexPooler.Platform.InstancePresence

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.minutes(5)

  @impl Oban.Worker
  def perform(%Oban.Job{args: args}) do
    now =
      args
      |> Map.get("now")
      |> parse_now()

    case RuntimeStateCleanup.run(now) do
      {:ok, _summary} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @impl Oban.Worker
  def backoff(%Oban.Job{attempt: attempt}), do: attempt * 30

  defp parse_now(nil), do: InstancePresence.database_now()

  defp parse_now(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, timestamp, _offset} -> DateTime.truncate(timestamp, :microsecond)
      _error -> parse_now(nil)
    end
  end

  defp parse_now(_value), do: parse_now(nil)
end
