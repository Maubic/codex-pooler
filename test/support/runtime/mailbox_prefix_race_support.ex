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
    peer_key = {__MODULE__, name}
    relay_owner_key = {peer_key, :relay_owner}

    # Setup_all owns this peer. Cleanup must survive a failed remote app boot,
    # including a consumer heartbeat committed before boot returns its owner.
    on_exit(fn ->
      try do
        case :persistent_term.get(peer_key, nil) do
          {controller, peer_node} ->
            if Process.alive?(controller), do: :peer.stop(controller)
            PeerRegistry.assert_peer_absent!(name, peer_node: peer_node, budget_ms: @budget)

          nil ->
            PeerRegistry.assert_peer_absent!(name, budget_ms: @budget)
        end

        if relay_owner = :persistent_term.get(relay_owner_key, nil) do
          Sandbox.unboxed_run(Repo, fn -> Repo.query!("DELETE FROM telemetry_relay_consumers WHERE owner = $1", [relay_owner]) end)
        end
      after
        :persistent_term.erase(peer_key)
        :persistent_term.erase(relay_owner_key)
      end
    end)

    assert {:ok, controller, peer_node} = :peer.start_link(%{name: name, args: [~c"+S", ~c"2:2", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]})
    :persistent_term.put(peer_key, {controller, peer_node})
    Process.unlink(controller)

    assert :ok = :erpc.call(peer_node, :code, :add_paths, [:code.get_path()], @budget)
    env = for {app, _, _} <- Application.started_applications(), do: {app, Application.get_all_env(app)}
    assert {:ok, relay_owner} = :erpc.call(peer_node, __MODULE__, :boot_http_runtime, [env, {node(), relay_owner_key}], @budget)
    assert :persistent_term.get(relay_owner_key) == relay_owner

    peer_node
  end

  @spec boot_http_runtime([{atom(), keyword()}], {node(), term()}) :: {:ok, String.t()}
  def boot_http_runtime(env, relay_cleanup) do
    Mix.start()
    Mix.env(:test)

    for {app, config} <- env do
      Application.load(app)
      for {key, value} <- config, do: Application.put_env(app, key, value)
    end

    repo = Application.fetch_env!(:codex_pooler, Repo)
    Application.put_env(:codex_pooler, Repo, Keyword.merge(repo, pool: DBConnection.ConnectionPool, pool_size: 4, parameters: [application_name: "synthetic_mailbox_peer"]))
    {:ok, _} = Application.ensure_all_started(:telemetry)
    handler_id = {__MODULE__, :boot_consumer_owner, make_ref()}

    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.capture_boot_consumer_owner/4, relay_cleanup)

    try do
      {:ok, _} = Application.ensure_all_started(:codex_pooler)
      %{owner: relay_owner} = :sys.get_state(CodexPooler.Telemetry.RelayRuntime, @budget)
      # Only the real HTTP/gateway runtime is under test. The relay's drain
      # heartbeat is unrelated; termination also writes one, so delete after
      # Supervisor has acknowledged the child's complete shutdown, not DOWN.
      :ok = Supervisor.terminate_child(CodexPooler.Supervisor, CodexPooler.Telemetry.RelayRuntime)
      Repo.query!("DELETE FROM telemetry_relay_consumers WHERE owner = $1", [relay_owner])
      {:ok, modules} = :application.get_key(:codex_pooler, :modules)
      :ok = :code.ensure_modules_loaded(modules)
      %{rows: [[1]]} = Repo.query!("SELECT 1")
      {:ok, relay_owner}
    after
      :telemetry.detach(handler_id)
    end
  end

  @spec capture_boot_consumer_owner([atom()], map(), map(), {node(), term()}) :: :ok
  def capture_boot_consumer_owner(_event, _measurements, %{query: "INSERT INTO telemetry_relay_consumers" <> _, params: [owner, _quiesced]}, {origin_node, owner_key}) do
    if self() == Process.whereis(CodexPooler.Telemetry.RelayRuntime) do
      :erpc.call(origin_node, :persistent_term, :put, [owner_key, owner], @budget)
    end

    :ok
  end

  def capture_boot_consumer_owner(_event, _measurements, _metadata, _config), do: :ok

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
