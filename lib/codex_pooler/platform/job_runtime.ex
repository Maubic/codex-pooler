defmodule CodexPooler.Platform.JobRuntime do
  @moduledoc "Starts Oban work only after this image's required migrations are applied."

  alias CodexPooler.Platform.Readiness

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec(opts) do
    %{Oban.child_spec(opts) | start: {__MODULE__, :start_link, [opts]}}
  end

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    case Oban.Config.new(opts) do
      %{queues: [], plugins: [], stager: false} ->
        Oban.start_link(opts)

      _working_config ->
        case Readiness.schema_status() do
          :ok -> Oban.start_link(opts)
          {:error, family, class} -> {:error, {:job_schema_not_ready, family, class}}
        end
    end
  end
end
