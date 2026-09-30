defmodule CodexPoolerWeb.Runtime.MailboxPrefixRaceSupport do
  @moduledoc false

  import ExUnit.Assertions
  import ExUnit.Callbacks

  alias CodexPooler.PeerRegistry
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000

  @spec hold_before_session_lock!(pid()) :: reference()
  def hold_before_session_lock!(executor) do
    hold = make_ref()
    handler_id = {__MODULE__, :before_session_lock, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{executor: executor, test: self(), hold: hold, claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.hold_first_begin/4, config)
    hold
  end

  @spec hold_first_begin([atom()], map(), map(), map()) :: :ok
  def hold_first_begin(_event, _measurements, %{query: "begin"}, config) do
    if self() == config.executor and :atomics.add_get(config.claimed, 1, 1) == 1 do
      send(config.test, {config.hold, :before_session_lock, self()})

      receive do
        {hold, :release} when hold == config.hold -> :ok
      after
        @budget -> :ok
      end
    end

    :ok
  end

  def hold_first_begin(_event, _measurements, _metadata, _config), do: :ok

  @spec start_http_peer!() :: node()
  def start_http_peer! do
    BackendCodexWebsocketOwnerForwardingSupport.ensure_test_distribution_started!()
    name = String.to_atom("mailbox_http_#{System.unique_integer([:positive])}")
    assert {:ok, controller, peer_node} = :peer.start_link(%{name: name, args: [~c"+S", ~c"2:2", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]})
    Process.unlink(controller)

    on_exit(fn ->
      if Process.alive?(controller), do: :peer.stop(controller)
      PeerRegistry.assert_peer_absent!(name, peer_node: peer_node, budget_ms: @budget)
    end)

    assert :ok = :erpc.call(peer_node, :code, :add_paths, [:code.get_path()], @budget)
    env = for {app, _, _} <- Application.started_applications(), do: {app, Application.get_all_env(app)}
    assert {:ok, relay_owner} = :erpc.call(peer_node, __MODULE__, :boot_http_runtime, [env], @budget)

    on_exit(fn ->
      if Process.alive?(controller), do: :peer.stop(controller)
      PeerRegistry.assert_peer_absent!(name, peer_node: peer_node, budget_ms: @budget)
      Sandbox.unboxed_run(Repo, fn -> Repo.query!("DELETE FROM telemetry_relay_consumers WHERE owner = $1", [relay_owner]) end)
    end)

    peer_node
  end

  @spec boot_http_runtime([{atom(), keyword()}]) :: {:ok, String.t()}
  def boot_http_runtime(env) do
    Mix.start()
    Mix.env(:test)

    for {app, config} <- env do
      Application.load(app)
      for {key, value} <- config, do: Application.put_env(app, key, value)
    end

    repo = Application.fetch_env!(:codex_pooler, Repo)
    Application.put_env(:codex_pooler, Repo, Keyword.merge(repo, pool: DBConnection.ConnectionPool, pool_size: 4, parameters: [application_name: "synthetic_mailbox_peer"]))
    {:ok, _} = Application.ensure_all_started(:codex_pooler)
    {:ok, modules} = :application.get_key(:codex_pooler, :modules)
    :ok = :code.ensure_modules_loaded(modules)
    %{rows: [[1]]} = Repo.query!("SELECT 1")
    {:ok, :sys.get_state(CodexPooler.Telemetry.RelayRuntime).owner}
  end

  @spec start_peer_listener!(node()) :: pos_integer()
  def start_peer_listener!(peer_node) do
    {server, port} = :erpc.call(peer_node, __MODULE__, :start_listener, [], @budget)

    on_exit(fn ->
      if peer_node in Node.list(:connected), do: :erpc.call(peer_node, ThousandIsland, :stop, [server], @budget)
    end)

    port
  end

  @spec start_listener() :: {pid(), pos_integer()}
  def start_listener do
    {:ok, server} = Bandit.start_link(plug: CodexPoolerWeb.Endpoint, port: 0, ip: {127, 0, 0, 1}, startup_log: false)
    Process.unlink(server)
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {server, port}
  end
end
