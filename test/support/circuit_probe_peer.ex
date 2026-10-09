defmodule CodexPooler.CircuitProbePeer do
  @moduledoc false

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Routing.{CircuitState, RoutingSelection}
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, SelectedCandidateContext}
  alias CodexPooler.Gateway.Runtime.Routing.DispatchLifecycle
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.InstancePresencePeer
  alias CodexPooler.Repo

  @type fixture :: %{required(:context) => Context.t(), optional(atom()) => term()}

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

  @spec admit(fixture()) :: {:ok, RoutingSelection.t()} | {:error, term()}
  def admit(fixture), do: RoutingSelection.begin_circuit(selection(fixture), fixture.context.auth, fixture.context.model)

  @spec concurrent_admit(fixture(), pid(), reference()) :: term()
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

  @spec complete(fixture(), RoutingSelection.t(), atom()) :: term()
  def complete(fixture, admitted, outcome) do
    context = %{fixture.context | route_plan: admitted.route_plan}
    selected = SelectedCandidateContext.from_dispatch_context(context, admitted, false)

    case outcome do
      :success -> DispatchLifecycle.success(selected)
      :failure -> DispatchLifecycle.failure(selected, :upstream_network_error)
      :neutral -> DispatchLifecycle.neutral_completion(selected)
    end
  end

  @spec selection(fixture()) :: RoutingSelection.t()
  def selection(fixture) do
    context = fixture.context
    [{assignment, identity}] = context.route_plan.candidates

    RoutingSelection.prepare_candidate(%{
      assignment: assignment,
      identity: identity,
      index: 0,
      route_class: context.route_class,
      route_plan: context.route_plan
    })
  end
end
