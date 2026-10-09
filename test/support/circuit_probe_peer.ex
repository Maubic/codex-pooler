defmodule CodexPooler.CircuitProbePeer do
  @moduledoc false

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Routing.{CircuitState, RoutingSelection}
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, SelectedCandidateContext}
  alias CodexPooler.Gateway.Runtime.Routing.DispatchLifecycle
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.InstancePresencePeer
  alias CodexPooler.Repo

  @spec bootstrap(keyword(), OperationalSettings.t(), String.t()) :: map()
  def bootstrap(repo_config, settings, boot_id) do
    {:ok, _} = Application.ensure_all_started(:postgrex)
    {:ok, _} = Application.ensure_all_started(:ex_unit)
    Application.put_env(:codex_pooler, OperationalSettings, settings: settings)
    WebsocketOwnerNodeHarness.start_repo(Keyword.merge(repo_config, pool: DBConnection.ConnectionPool, pool_size: 2, log: false, parameters: [application_name: InstancePresencePeer.peer_application_name(boot_id)]))
    WebsocketOwnerNodeHarness.start_pubsub()
    runtime()
  end

  @spec runtime() :: map()
  def runtime do
    %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
    %{node: node(), backend: backend, os_pid: System.pid(), beam: Base.encode16(CircuitState.module_info(:md5), case: :lower)}
  end

  @spec admit(map()) :: {:ok, RoutingSelection.t()} | {:error, term()}
  def admit(fixture), do: RoutingSelection.begin_circuit(selection(fixture), fixture.auth, fixture.model)

  @spec concurrent_admit(map(), pid(), reference()) :: term()
  def concurrent_admit(fixture, parent, gate) do
    Repo.checkout(fn ->
      %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
      send(parent, {gate, :ready, self(), node(), backend})

      receive do
        {^gate, :go} -> admit(fixture)
      after
        10_000 -> raise "circuit admission barrier was not released"
      end
    end)
  end

  @spec complete(map(), RoutingSelection.t(), atom()) :: term()
  def complete(fixture, admitted, outcome) do
    context = %Context{auth: fixture.auth, model: fixture.model, route_plan: admitted.route_plan, reserved: %{request: fixture.request}}
    selected = SelectedCandidateContext.from_dispatch_context(context, admitted, false)

    case outcome do
      :success -> DispatchLifecycle.success(selected)
      :failure -> DispatchLifecycle.failure(selected, :upstream_network_error)
      :neutral -> DispatchLifecycle.neutral_completion(selected)
    end
  end

  @spec selection(map()) :: RoutingSelection.t()
  def selection(fixture) do
    %RoutingSelection{assignment: fixture.assignment, identity: fixture.identity, route_class: "proxy_websocket", route_plan: %{planned_at: DateTime.utc_now(), affinity: %{enabled?: false, key_hash: nil, pool_id: fixture.auth.pool.id, api_key_id: fixture.auth.api_key.id, model_identifier: fixture.model.exposed_model_id}}}
  end
end
