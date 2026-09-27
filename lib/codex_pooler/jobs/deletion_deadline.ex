defmodule CodexPooler.Jobs.DeletionDeadline do
  @moduledoc false

  alias CodexPooler.Repo

  @shutdown_timeout_ms 5_000

  @spec run(integer(), (-> result)) :: result | :more | {:error, term()} when result: term()
  def run(deadline, operation) when is_integer(deadline) and is_function(operation, 0) do
    if remaining(deadline) == 0 do
      :more
    else
      run_with_executor(deadline, operation)
    end
  end

  defp run_with_executor(deadline, operation) do
    repo = Repo.get_dynamic_repo()

    task =
      Task.async(fn ->
        Repo.put_dynamic_repo(repo)
        execute(deadline, operation)
      end)

    monitor = Process.monitor(task.pid)

    try do
      case Task.yield(task, remaining(deadline)) do
        {:ok, result} -> result
        {:exit, reason} -> {:error, reason}
        nil -> :more
      end
    after
      # The outer timeout covers even an ungranted checkout. A successful Task
      # result is not process death: stop and observe this exact owned executor.
      Process.unlink(task.pid)
      if Process.alive?(task.pid), do: Process.exit(task.pid, :kill)

      receive do
        {:DOWN, ^monitor, :process, _pid, _reason} -> :ok
      after
        @shutdown_timeout_ms -> exit(:deletion_executor_shutdown_timeout)
      end

      Process.demonitor(task.ref, [:flush])
      task_ref = task.ref

      receive do
        {^task_ref, _result} -> :ok
      after
        0 -> :ok
      end
    end
  end

  defp execute(deadline, operation) do
    # One checkout spans all committed batches and the final transaction. Its
    # absolute deadline never restarts between statements or retries.
    Repo.checkout(operation, deadline: deadline, timeout: :infinity)
  rescue
    error in [Postgrex.Error, DBConnection.ConnectionError] ->
      if remaining(deadline) == 0, do: :more, else: {:error, error}
  end

  @spec remaining(integer()) :: non_neg_integer()
  def remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
