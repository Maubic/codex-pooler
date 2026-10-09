defmodule CodexPooler.Jobs.AlertDeliveryWorker do
  @moduledoc """
  Delivers one alert incident notification to one configured alert channel.
  """

  use Oban.Worker,
    queue: :jobs,
    max_attempts: 5,
    tags: ["alert_delivery"],
    unique: [
      fields: [:args, :queue, :worker],
      keys: [:alert_incident_id, :alert_channel_id],
      states: :incomplete,
      period: {7, :days}
    ]

  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Delivery.EmailDelivery
  alias CodexPooler.Alerts.Delivery.Execution
  alias CodexPooler.Alerts.Delivery.WebhookDelivery

  @type delivery_error ::
          :invalid_alert_delivery_args
          | :alert_incident_not_found
          | :alert_channel_not_found
          | :unsupported_alert_channel
          | :channel_disabled
          | :mailer_unconfigured
          | map()

  @impl Oban.Worker
  def timeout(%Oban.Job{}), do: :timer.seconds(15)

  @impl Oban.Worker
  def backoff(%Oban.Job{attempt: attempt}) do
    max(EmailDelivery.retry_delay_seconds(attempt), WebhookDelivery.retry_delay_seconds(attempt))
  end

  @impl Oban.Worker
  def perform(
        %Oban.Job{
          args: %{
            "alert_incident_id" => incident_id,
            "alert_channel_id" => channel_id
          },
          attempt: attempt
        } = job
      ) do
    execution = Execution.new(job)

    persisted_attempt_number =
      Execution.storage(execution, fn ->
        Alerts.recover_pending_deliveries(job_id: job.id)
        Alerts.next_delivery_attempt_number(incident_id, channel_id, attempt)
      end)

    case Alerts.deliver_incident_to_channel(incident_id, channel_id, persisted_attempt_number, retry_attempt: attempt, delivery_execution: execution) do
      {:ok, _attempt} -> :ok
      {:error, %{retryable: true} = reason} -> {:error, Map.take(reason, [:code, :retryable])}
      {:error, reason} -> {:cancel, sanitize_error(reason)}
    end
  end

  def perform(%Oban.Job{}), do: {:cancel, :invalid_alert_delivery_args}

  @spec sanitize_error(delivery_error()) :: atom() | map()
  defp sanitize_error(%Ecto.Changeset{}), do: %{code: "alert_delivery_receipt_invalid"}

  defp sanitize_error(reason) when is_atom(reason), do: reason

  defp sanitize_error(%{code: code}) when is_binary(code),
    do: %{code: code}
end
