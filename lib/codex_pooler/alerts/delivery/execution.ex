defmodule CodexPooler.Alerts.Delivery.Execution do
  @moduledoc false

  import Ecto.Query
  alias CodexPooler.Repo

  @worker "CodexPooler.Jobs.AlertDeliveryWorker"
  @max_job_id 9_223_372_036_854_775_807
  @max_job_attempt 2_147_483_647
  @network_ms 10_000
  @completion_ms 12_000
  @enforce_keys [:deadline, :completion_deadline]
  defstruct [:binding, :deadline, :completion_deadline]

  @type binding :: %{String.t() => integer() | String.t()}
  @type t :: %__MODULE__{binding: binding() | nil, deadline: integer(), completion_deadline: integer()}

  defguardp valid_job_id(id) when is_integer(id) and id > 0 and id <= @max_job_id
  defguardp valid_job_attempt(attempt) when is_integer(attempt) and attempt > 0 and attempt <= @max_job_attempt

  @spec new(Oban.Job.t() | nil) :: t()
  def new(job \\ nil) do
    start = System.monotonic_time(:millisecond)
    %__MODULE__{binding: job_binding(job), deadline: start + @network_ms, completion_deadline: start + @completion_ms}
  end

  defp job_binding(%Oban.Job{id: id, attempt: attempt, attempted_at: %DateTime{} = at}) when valid_job_id(id) and valid_job_attempt(attempt),
    do: %{"version" => 1, "job_id" => id, "job_attempt" => attempt, "job_attempted_at" => DateTime.to_iso8601(at)}

  defp job_binding(%Oban.Job{id: nil}), do: nil
  defp job_binding(nil), do: nil
  defp job_binding(%Oban.Job{}), do: %{}

  @spec decode(map()) :: {:ok, binding()} | :unlinked | :invalid
  def decode(metadata) do
    case Map.fetch(metadata, "delivery_execution") do
      :error -> :unlinked
      {:ok, binding} -> decode_binding(binding)
    end
  end

  defp decode_binding(%{"version" => 1, "job_id" => id, "job_attempt" => attempt, "job_attempted_at" => at} = binding)
       when map_size(binding) == 4 and valid_job_id(id) and valid_job_attempt(attempt) and is_binary(at) do
    case DateTime.from_iso8601(at) do
      {:ok, time, 0} -> if DateTime.to_iso8601(time) == at, do: {:ok, binding}, else: :invalid
      _ -> :invalid
    end
  end

  defp decode_binding(_), do: :invalid

  @spec metadata(t(), map()) :: map()
  def metadata(%__MODULE__{binding: nil}, metadata), do: metadata
  def metadata(%__MODULE__{binding: binding}, metadata), do: Map.put(metadata, "delivery_execution", binding)

  @spec lock_job(map()) :: Oban.Job.t() | nil
  def lock_job(%{"job_id" => id}) when valid_job_id(id), do: Repo.one(from j in Oban.Job, where: j.id == ^id, lock: "FOR UPDATE")
  def lock_job(_), do: nil

  @spec matches?(Oban.Job.t() | nil, binding(), String.t(), String.t()) :: boolean()
  def matches?(%Oban.Job{} = job, binding, incident_id, channel_id),
    do: related?(job, incident_id, channel_id) and job.state == "executing" and job_binding(job) == binding

  def matches?(_, _, _, _), do: false

  @spec related?(Oban.Job.t() | nil, String.t(), String.t()) :: boolean()
  def related?(%Oban.Job{worker: @worker, args: args}, incident_id, channel_id),
    do: args["alert_incident_id"] == incident_id and args["alert_channel_id"] == channel_id

  def related?(_, _, _), do: false

  @spec storage(t() | nil, (-> result)) :: result when result: term()
  def storage(nil, fun), do: fun.()
  def storage(%__MODULE__{completion_deadline: deadline}, fun), do: Repo.checkout(fun, deadline: deadline, timeout: max(deadline - System.monotonic_time(:millisecond), 1))

  @spec remaining(t()) :: non_neg_integer()
  def remaining(%__MODULE__{deadline: deadline}), do: max(deadline - System.monotonic_time(:millisecond), 0)

  @spec run(t(), (-> result)) :: result | {:error, :delivery_timeout} when result: term()
  def run(%__MODULE__{} = execution, operation) do
    if remaining(execution) == 0, do: {:error, :delivery_timeout}, else: execute(execution, operation)
  end

  defp execute(execution, operation) do
    # Linked ownership makes worker death terminate its outbound caller too.
    repo = Repo.get_dynamic_repo()

    task =
      Task.async(fn ->
        Repo.put_dynamic_repo(repo)
        operation.()
      end)

    monitor = Process.monitor(task.pid)

    try do
      case Task.yield(task, remaining(execution)) do
        {:ok, result} -> result
        nil -> {:error, :delivery_timeout}
        {:exit, reason} -> exit(reason)
      end
    after
      Process.unlink(task.pid)
      if Process.alive?(task.pid), do: Process.exit(task.pid, :kill)

      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      after
        1_000 -> exit(:alert_delivery_executor_shutdown_timeout)
      end

      Process.demonitor(task.ref, [:flush])

      receive do
        {ref, _} when ref == task.ref -> :ok
      after
        0 -> :ok
      end
    end
  end
end
