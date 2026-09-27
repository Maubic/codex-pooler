defmodule CodexPooler.Gateway.Transports.Websocket.RolloutBoundaryTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Gateway.Persistence.BridgeOwnerLease
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Repo

  @budget 15_000

  defmodule RegistryFault do
    use GenServer
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)
    @impl true
    def init(opts), do: {:ok, Map.new(opts) |> Map.put(:reads, 0)}
    @impl true
    def handle_call(:drain_entries, _from, %{mode: :invalid_empty} = state), do: {:reply, :invalid_cohort, state}
    def handle_call(:drain_entries, _from, %{reads: 1, mode: :stall} = state), do: {:noreply, state}
    def handle_call(:drain_entries, _from, %{reads: 1, mode: :invalid} = state), do: {:reply, :invalid_cohort, state}

    def handle_call(:drain_entries, _from, state) do
      entries = DeferredStreamRegistry.drain_entries(name: state.registry)
      {:reply, entries, %{state | reads: state.reads + 1}}
    end

    def handle_call(message, _from, state), do: {:reply, GenServer.call(state.registry, message), state}
  end

  for mode <- [:stall, :invalid] do
    @tag mode: mode
    test "a #{mode} cohort worker reports the real two-stream cohort and releases the coordinator", %{mode: mode} do
      registry = unique(:streams)
      start_supervised!({DeferredStreamRegistry, name: registry})
      parent = self()

      streams =
        for _ <- 1..2 do
          spawn(fn ->
            token = DeferredStreamRegistry.register(%{request_id: Ecto.UUID.generate()}, name: registry)
            send(parent, {:registered, self(), token})

            receive do
              :stop -> DeferredStreamRegistry.finish(token, :completed, name: registry)
            end
          end)
        end

      on_exit(fn -> Enum.each(streams, &send(&1, :stop)) end)
      for stream <- streams, do: assert_receive({:registered, ^stream, _token}, @budget)
      fault = start_supervised!({RegistryFault, registry: registry, mode: mode})
      drain = drain_server(fault)
      {summary, logs} = ExUnit.CaptureLog.with_log(fn -> RolloutDrain.start_drain(name: drain, timeout_ms: 200, deadline_margin_ms: 0, deadline_floor_ms: 10, owner_post_deadline_call_budget_ms: 100) end)
      assert logs =~ "websocket rollout drain HTTP cohort unavailable"
      assert logs =~ "observed_entries=2"
      assert summary.result == :error
      assert summary.http_streams_seen == 2
      assert summary.http_streams_failed == 2
      assert summary.http_streams_completed == 0
      assert :sys.get_state(drain).active_drain == nil

      for stream <- streams do
        monitor = Process.monitor(stream)
        send(stream, :stop)
        assert_receive {:DOWN, ^monitor, :process, ^stream, :normal}, @budget
      end
    end
  end

  test "an unavailable empty cohort returns an error without inventing a stream" do
    registry = unique(:streams)
    start_supervised!({DeferredStreamRegistry, name: registry})
    fault = start_supervised!({RegistryFault, registry: registry, mode: :invalid_empty})
    drain = drain_server(fault)
    {summary, logs} = ExUnit.CaptureLog.with_log(fn -> RolloutDrain.start_drain(name: drain, timeout_ms: 200, deadline_margin_ms: 0, deadline_floor_ms: 10, owner_post_deadline_call_budget_ms: 100) end)
    assert logs =~ "observed_entries=0"
    assert summary.result == :error
    assert summary.http_streams_seen == 0
    assert summary.http_streams_failed == 0
    assert :sys.get_state(drain).active_drain == nil
  end

  test "an owner held in init is still shut down and releases its lease after the drain budget" do
    %{pool: pool, api_key: api_key} = CodexPooler.PoolerFixtures.active_api_key_fixture()
    {:ok, session} = Gateway.start_codex_session(%{pool: pool, api_key: api_key}, %{owner_instance_id: Atom.to_string(node()), accepted_turn_state: Ecto.UUID.generate()})
    owner_registry = unique(:owners)
    start_supervised!({Registry, keys: :unique, name: owner_registry})
    streams = unique(:streams)
    start_supervised!({DeferredStreamRegistry, name: streams})
    drain = drain_server(streams, owner_registry)
    parent = self()
    gate = make_ref()

    upstream = %{
      start: fn ->
        send(parent, {:owner_initializing, self()})
        receive do: ({:release_init, ^gate} -> :ok)
        Agent.start_link(fn -> :ready end)
      end,
      send: fn _, _, _ -> :ok end,
      close: fn pid -> Agent.stop(pid) end
    }

    starter =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        result = WebsocketOwnerSession.start_link(codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: session.owner_instance_id, registry: owner_registry, upstream: upstream, owner_renewal_ms: 60_000)
        send(parent, {:owner_started, result})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:owner_initializing, owner}, @budget

    on_exit(fn ->
      send(owner, {:release_init, gate})
      send(starter, :stop)
      if Process.alive?(owner), do: GenServer.stop(owner, :normal)
    end)

    assert [{^owner, :starting}] = Registry.lookup(owner_registry, session.id)
    monitor = Process.monitor(owner)
    summary = RolloutDrain.start_drain(name: drain, timeout_ms: 200, deadline_margin_ms: 0, deadline_floor_ms: 10, owner_post_deadline_call_budget_ms: 100)
    assert summary.owners_seen == 1
    assert summary.owners_failed == 1
    send(owner, {:release_init, gate})
    assert_receive {:owner_started, {:ok, ^owner}}, @budget
    assert_receive {:DOWN, ^monitor, :process, ^owner, _reason}, @budget
    lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: session.id, lease_token: session.owner_lease_token)
    assert lease.status == "released"
    assert %DateTime{} = lease.released_at
    send(starter, :stop)
  end

  test "a turn queued during owner initialization receives the remaining drain budget" do
    auth = CodexPooler.PoolerFixtures.active_api_key_fixture()
    {:ok, session} = Gateway.start_codex_session(auth, %{owner_instance_id: Atom.to_string(node()), accepted_turn_state: Ecto.UUID.generate()})
    owners = unique(:owners)
    start_supervised!({Registry, keys: :unique, name: owners})
    streams = unique(:streams)
    start_supervised!({DeferredStreamRegistry, name: streams})
    drain = drain_server(streams, owners)
    parent = self()
    gate = make_ref()

    upstream = %{
      start: fn ->
        send(parent, {:initializing, self()})
        receive do: ({:release_init, ^gate} -> :ok)
        Agent.start_link(fn -> :ready end)
      end,
      send: fn _, _, _ ->
        send(parent, {:accepted_turn, self()})
        receive do: ({:finish_turn, ^gate} -> :ok)
        {:ok, %{status: 200, headers: [], terminal: "response.completed", body: ""}}
      end,
      close: fn pid -> Agent.stop(pid) end
    }

    starter =
      spawn(fn ->
        Process.flag(:trap_exit, true)
        result = WebsocketOwnerSession.start_link(codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: session.owner_instance_id, registry: owners, upstream: upstream, owner_renewal_ms: 60_000)
        send(parent, {:started, result})
        receive do: (:stop -> :ok)
      end)

    assert_receive {:initializing, owner}, @budget

    on_exit(fn ->
      send(owner, {:release_init, gate})
      send(starter, :stop)
      if Process.alive?(owner), do: GenServer.stop(owner, :normal)
    end)

    attach_ref = make_ref()
    submit_ref = make_ref()
    downstream = %{pid: self(), epoch: 1, correlation_id: "initializing-drain", owner_turn_id: self(), active_turn_reconnect?: false}
    request = %CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.Request{url: "https://example.com", payload: "{}", headers: [], timeouts: %{}, websocket_delivery_mode: :collect_compaction, effective_serving_mode: "full"}
    send(owner, {:"$gen_call", {self(), attach_ref}, {:attach_downstream, self(), downstream.correlation_id, []}})
    send(owner, {:"$gen_call", {self(), submit_ref}, {:submit_upstream, downstream, request, false}})
    task = Task.async(fn -> RolloutDrain.start_drain(name: drain, timeout_ms: 5_000, deadline_margin_ms: 0) end)
    await_drain_queued(owner, System.monotonic_time(:millisecond) + @budget)
    send(owner, {:release_init, gate})
    assert_receive {:started, {:ok, ^owner}}, @budget
    assert_receive {^attach_ref, {:ok, _}}, @budget

    executor =
      receive do
        {:accepted_turn, executor} -> executor
        {^submit_ref, result} -> flunk("queued turn refused: #{inspect(result)}")
      after
        @budget -> flunk("queued turn never reached its upstream task")
      end

    on_exit(fn -> send(executor, {:finish_turn, gate}) end)
    assert {:ok, %{active_turn?: true, draining?: true}} = WebsocketOwnerSession.owner_status(owner)
    send(executor, {:finish_turn, gate})
    assert_receive {^submit_ref, {:ok, %{terminal: "response.completed"}}}, @budget
    assert %{result: :ok, turns_completed: 1, turns_aborted: 0} = Task.await(task, @budget)
    send(starter, :stop)
  end

  defp await_drain_queued(owner, deadline) do
    # Owner processes are sensitive, so their message bodies are intentionally
    # hidden. The two queued calls precede the drain cast and its status/drain call.
    {:message_queue_len, count} = Process.info(owner, :message_queue_len)

    unless count >= 4 do
      assert System.monotonic_time(:millisecond) < deadline
      Process.sleep(5)
      await_drain_queued(owner, deadline)
    end
  end

  defp drain_server(streams, owners \\ nil) do
    owners = owners || unique(:owners)
    if is_nil(Process.whereis(owners)), do: start_supervised!({Registry, keys: :unique, name: owners})
    activities = unique(:activities)
    start_supervised!({ActivityRegistry, name: activities})
    name = unique(:drain)
    start_supervised!({RolloutDrain, name: name, stream_registry: streams, owner_registry: owners, activity_registry: activities})
    name
  end

  defp unique(kind), do: String.to_atom("#{__MODULE__}-#{kind}-#{System.unique_integer([:positive])}")
end
