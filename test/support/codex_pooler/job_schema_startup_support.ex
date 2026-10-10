defmodule CodexPooler.JobSchemaCanaryWorker do
  @moduledoc false
  use Oban.Worker, queue: :schema_canary, max_attempts: 1

  @impl Oban.Worker
  def perform(%Oban.Job{id: id}) do
    :telemetry.execute([:codex_pooler, :schema_canary, :start], %{}, %{job_id: id})
    CodexPooler.Repo.query!("SELECT probe_generation FROM routing_circuit_states LIMIT 0", [], timeout: 1_000)
    :ok
  end
end

defmodule CodexPooler.JobSchemaStartupPlugin do
  @moduledoc false
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    send(Keyword.fetch!(opts, :observer), {:schema_plugin_started, self()})
    {:ok, opts}
  end

  def validate(_opts), do: :ok
end
