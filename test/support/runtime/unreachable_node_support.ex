defmodule CodexPoolerWeb.Runtime.UnreachableNodeSupport do
  @moduledoc false

  # Peers for tests of a node that becomes unreachable while a websocket turn
  # crosses it (findings#286).
  #
  #   * `boot_tcp_owner_peer!/0`: the owner-runtime peer of
  #     `BackendCodexWebsocketOwnerForwardingSupport`, booted with a TCP control
  #     connection (`:peer` `connection: 0`), so the test still inspects it with
  #     `:peer.call/4` while BEAM distribution to it is cut.
  #   * `partition!/1` and `heal!/1`: a reversible partition. This node takes
  #     another cookie for the peer and disconnects it, so every reconnect from
  #     either side fails its handshake ("Invalid challenge reply"). Healing
  #     restores the cookie and reconnects. Both nodes keep the database, and
  #     the peer keeps reaching FakeUpstream on this node over TCP.
  #   * `boot_app_peer!/0`: a peer VM running the whole application with a
  #     public listener, on the committed database; `halt!/2` stops that VM the
  #     way a crashed pod goes, without running any cleanup there.
  #   * `start_pacer!/1`: releases a FakeUpstream reply held by
  #     `FakeUpstream.barrier_websocket_frames/2` (whose `notify:` is the pacer)
  #     one frame per interval once told to, or one frame per `step!/1`,
  #     monitors the connection handler, and reports what that connection
  #     consumed.

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture

  @detection_timeout_ms 15_000
  # The applications whose configuration the application peer takes from this
  # node, loaded first so a later start does not reset it.
  @app_peer_envs [:logger, :phoenix, :postgrex, :req, :argon2_elixir, :swoosh, :phoenix_live_view, :codex_pooler]

  @spec boot_tcp_owner_peer!() :: {pid(), node()}
  def boot_tcp_owner_peer! do
    name = CodexPooler.PeerRegistry.unique_node_name("unreachable_owner")
    {:ok, peer, peer_node} = :peer.start_link(%{name: name, connection: 0, args: [~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]})
    Process.unlink(peer)
    on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
    :ok = :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])
    signing = :codex_pooler |> Application.fetch_env!(CodexPoolerWeb.Endpoint) |> Keyword.take([:secret_key_base])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, CodexPoolerWeb.Endpoint, signing])
    {:ok, _runtime} = :erpc.call(peer_node, WebsocketOwnerNodeHarness, :start_owner_runtime, [])
    repo_config = :codex_pooler |> Application.fetch_env!(Repo) |> Keyword.merge(pool: DBConnection.ConnectionPool, pool_size: 2)
    _repo = :erpc.call(peer_node, WebsocketOwnerNodeHarness, :start_repo, [repo_config])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, CodexPooler.Upstreams, Application.get_env(:codex_pooler, CodexPooler.Upstreams, [])])
    {:ok, modules} = :application.get_key(:codex_pooler, :modules)
    :ok = :erpc.call(peer_node, :code, :ensure_modules_loaded, [modules])
    assert %{rows: [[1]]} = :erpc.call(peer_node, Repo, :query!, ["SELECT 1"])
    {peer, peer_node}
  end

  # The probe lives on the owner's peer and is read over its TCP control channel,
  # never over the distribution link the scenario cuts. It records the exact
  # early-check messages and can hold the owner after its first renewal returns,
  # so the socket releases the lease before the second check can handle it.
  @spec start_lease_check_probe!(pid() | nil, pid(), keyword()) :: atom()
  def start_lease_check_probe!(peer, owner, opts \\ []) do
    name = CodexPooler.PeerRegistry.unique_node_name("unreachable_lease_probe")
    release = make_ref()

    on_exit(fn -> call_lease_probe(peer, :stop_lease_check_probe, [name, owner, release]) end)
    :ok = call_lease_probe(peer, :install_lease_check_probe, [name, owner, release, Keyword.get(opts, :hold_first, false)])
    name
  end

  defp call_lease_probe(nil, function, args), do: apply(__MODULE__, function, args)
  defp call_lease_probe(peer, function, args), do: :peer.call(peer, __MODULE__, function, args, @detection_timeout_ms)

  @doc false
  def install_lease_check_probe(name, owner, release, hold_first) do
    {:ok, _probe} = Agent.start(fn -> %{started: [], completed: [], held?: false, lost?: false, owner: owner, release: release} end, name: name)
    :sys.install(owner, {&__MODULE__.lease_check_probe_hook/3, %{probe: name, current: nil, hold_first: hold_first}})
  end

  @doc false
  def lease_check_probe_hook(probe, {:in, {:renew_owner_lease, :unreachable_downstream, check}}, _name) do
    Agent.update(probe.probe, &%{&1 | started: &1.started ++ [check]})
    %{probe | current: check}
  end

  def lease_check_probe_hook(probe, {:noreply, state}, _name) do
    check = probe.current
    lost? = match?(%{downstream: nil, active_turn: %{descriptor: %{downstream_status: :lost}}}, state)

    Agent.update(probe.probe, fn observed ->
      %{observed | completed: if(is_nil(check), do: observed.completed, else: observed.completed ++ [check]), held?: probe.hold_first and check == 1, lost?: observed.lost? or lost?}
    end)

    if probe.hold_first and check == 1 do
      %{release: release} = lease_check_probe_state(probe.probe)

      receive do
        {^release, :release} -> :ok
      after
        @detection_timeout_ms -> exit(:lease_check_probe_not_released)
      end

      Agent.update(probe.probe, &%{&1 | held?: false})
    end

    %{probe | current: nil}
  end

  def lease_check_probe_hook(probe, _event, _name), do: probe

  @spec lease_check_probe_state(atom()) :: map()
  def lease_check_probe_state(probe), do: Agent.get(probe, & &1)

  @spec release_lease_check_probe(atom()) :: :ok
  def release_lease_check_probe(probe) do
    %{owner: owner, release: release} = lease_check_probe_state(probe)
    send(owner, {release, :release})
    :ok
  end

  @doc false
  def stop_lease_check_probe(probe, owner, release) do
    send(owner, {release, :release})

    if Process.alive?(owner) do
      try do
        :sys.remove(owner, &__MODULE__.lease_check_probe_hook/3)
      catch
        :exit, _reason -> :ok
      end
    end

    if pid = Process.whereis(probe), do: Agent.stop(pid)
    :ok
  end

  @spec partition!(node()) :: :ok
  def partition!(peer_node) do
    true = :erlang.set_cookie(peer_node, :unreachable_node_partition)
    true = Node.disconnect(peer_node)
    await!(fn -> peer_node not in Node.list() end, "the peer stayed connected")
  end

  # The peer keeps dialling this node while it is cut off (every send to a
  # pid here auto-connects), and a connect that races one of those handshakes
  # answers false: it is retried until the nodes are connected again.
  @spec heal!(node()) :: :ok
  def heal!(peer_node) do
    true = :erlang.set_cookie(peer_node, :erlang.get_cookie())
    await!(fn -> Node.connect(peer_node) end, "the peer never reconnected")
  end

  @spec boot_app_peer!() :: %{peer: pid(), node: node(), port: :inet.port_number()}
  def boot_app_peer! do
    name = CodexPooler.PeerRegistry.unique_node_name("unreachable_app")
    {:ok, peer, peer_node} = :peer.start_link(%{name: name, args: [~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]})
    Process.unlink(peer)
    on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
    :ok = :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])
    # The upstream secret box's local key fallback reads `Mix.env/0` at run time.
    {:ok, _mix} = :erpc.call(peer_node, Application, :ensure_all_started, [:mix])
    :ok = :erpc.call(peer_node, Mix, :env, [:test])

    for app <- @app_peer_envs do
      _loaded = :erpc.call(peer_node, Application, :load, [app])
      :ok = :erpc.call(peer_node, Application, :put_all_env, [[{app, Application.get_all_env(app)}], [persistent: true]])
    end

    repo_config = :codex_pooler |> Application.fetch_env!(Repo) |> Keyword.merge(pool: DBConnection.ConnectionPool, pool_size: 4)
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, Repo, repo_config, [persistent: true]])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, :websocket_owner_forwarding_enabled, true, [persistent: true]])
    {:ok, _started} = :erpc.call(peer_node, Application, :ensure_all_started, [:codex_pooler], 60_000)
    # Under the application's supervisor: a listener started through erpc
    # would stop with the erpc process that started it.
    listener = %{id: :unreachable_node_listener, type: :supervisor, start: {Bandit, :start_link, [[plug: CodexPoolerWeb.Endpoint, port: 0, ip: {127, 0, 0, 1}, startup_log: false]]}}
    {:ok, server} = :erpc.call(peer_node, Supervisor, :start_child, [CodexPooler.Supervisor, listener])
    {:ok, {_ip, port}} = :erpc.call(peer_node, ThousandIsland, :listener_info, [server])
    :ok = stop_telemetry_relay!(peer_node)
    %{peer: peer, node: peer_node, port: port}
  end

  # The peer's telemetry relay, which runs on any node whose Repo is not the
  # sandbox, commits a consumer heartbeat row at start and again every drain
  # interval, and a heartbeat during a test fails the committed-write guard.
  # Nothing here needs the relay: it is stopped and its row removed.
  defp stop_telemetry_relay!(peer_node) do
    %{owner: relay_owner} = :erpc.call(peer_node, :sys, :get_state, [CodexPooler.Telemetry.RelayRuntime])
    :ok = :erpc.call(peer_node, Supervisor, :terminate_child, [CodexPooler.Supervisor, CodexPooler.Telemetry.RelayRuntime])
    _deleted = UnboxedFixture.run_unboxed(fn -> Repo.query!("DELETE FROM telemetry_relay_consumers WHERE owner = $1", [relay_owner]) end)
    :ok
  end

  # Halts the peer's VM: nothing runs there afterwards, its sockets and tasks
  # included.
  @spec halt!(pid(), node()) :: :ok
  def halt!(peer, peer_node) do
    :ok = :peer.stop(peer)
    await!(fn -> peer_node not in Node.list() end, "the halted peer stayed connected")
  end

  # --- pacer ---

  @spec start_pacer!(reference()) :: pid()
  def start_pacer!(release_ref) do
    test = self()
    pacer = spawn_link(fn -> pacer_loop(%{test: test, ref: release_ref, upstream: nil, mode: :hold, interval_ms: nil, handler: nil, waiting: nil, t0: nil, events: [], stepper: nil}) end)
    on_exit(fn -> if Process.alive?(pacer), do: Process.exit(pacer, :kill) end)
    pacer
  end

  @spec pace_upstream!(pid(), FakeUpstream.t()) :: :ok
  def pace_upstream!(pacer, upstream) do
    send(pacer, {:upstream, upstream})
    :ok
  end

  # Waits until the held reply reached barrier `ordinal` (its connection is
  # about to send frame `ordinal`).
  @spec await_frame_barrier!(pid(), non_neg_integer()) :: :ok
  def await_frame_barrier!(pacer, ordinal) do
    send(pacer, {:await, ordinal, self()})
    assert_receive {:pacer_barrier, ^ordinal}, @detection_timeout_ms
    :ok
  end

  # Releases `count` frames at once (they reach the client before anything
  # happens to the node).
  @spec release_frames!(pid(), pos_integer()) :: :ok
  def release_frames!(pacer, count) do
    send(pacer, {:release_now, count, self()})
    assert_receive {:pacer_released, ^count}, @detection_timeout_ms
    :ok
  end

  # From now on, one frame per `interval_ms`, timed from the call.
  @spec pace!(pid(), pos_integer()) :: :ok
  def pace!(pacer, interval_ms) do
    send(pacer, {:pace, interval_ms, System.monotonic_time(:millisecond)})
    :ok
  end

  # Releases the one frame the held reply waits at, counted as `pace!/2`
  # counts from the first step on, and returns once the reply reached its
  # next frame barrier or its connection went down. No frame follows until
  # the test steps again: the provider's pace follows the test's events
  # instead of a clock.
  @spec step!(pid()) :: :ok
  def step!(pacer) do
    send(pacer, {:step, System.monotonic_time(:millisecond), self()})
    assert_receive :pacer_stepped, @detection_timeout_ms
    :ok
  end

  # The frames the held connection consumed since `pace!/2` and when its
  # handler (the last one that held a reply) went down, in ms since
  # `pace!/2` (nil while it is alive).
  @spec consumed_after_pace(pid()) :: %{frames: non_neg_integer(), connection_down_at: non_neg_integer() | :before_pace | nil}
  def consumed_after_pace(pacer) do
    send(pacer, {:report, self()})
    assert_receive {:pacer_report, events, handler}, @detection_timeout_ms
    released = for {:released, _ordinal, at} <- events, is_integer(at), do: at
    down = for {:connection_down, ^handler, at} <- events, do: at
    %{frames: length(released), connection_down_at: List.first(down)}
  end

  defp pacer_loop(state) do
    receive do
      {:upstream, upstream} ->
        pacer_loop(%{state | upstream: upstream})

      # A held reply whose connection went down no longer waits at its barrier.
      {:DOWN, _monitor, :process, handler, _reason} when handler == state.handler ->
        state = %{state | waiting: nil, events: [{:connection_down, handler, since(state)} | state.events]}
        pacer_loop(answer_stepper(state))

      {:fake_upstream_frame_barrier, ordinal, handler, ref} when ref == state.ref ->
        if handler != state.handler, do: Process.monitor(handler)
        state = %{state | handler: handler, waiting: ordinal, events: [{:barrier, ordinal, since(state)} | state.events]}
        state = state |> notify_waiter(ordinal) |> answer_stepper()

        if state.mode == :pace do
          Process.send_after(self(), :release_waiting, state.interval_ms)
        end

        pacer_loop(state)

      {:await, ordinal, from} ->
        state = Map.put(state, {:waiter, ordinal}, from)
        pacer_loop(if(state.waiting == ordinal, do: notify_waiter(state, ordinal), else: state))

      {:release_now, count, from} ->
        state = release_now(state, count)
        send(from, {:pacer_released, count})
        pacer_loop(state)

      {:pace, interval_ms, t0} ->
        state = %{state | mode: :pace, interval_ms: interval_ms, t0: t0}
        if is_integer(state.waiting), do: send(self(), :release_waiting)
        pacer_loop(state)

      # A step sent before the reply reached its barrier waits in the mailbox.
      {:step, now, from} when is_integer(state.waiting) ->
        state = %{state | mode: :step, t0: state.t0 || now, stepper: from}
        pacer_loop(release_waiting(state))

      :release_waiting ->
        pacer_loop(release_waiting(state))

      {:report, from} ->
        send(from, {:pacer_report, Enum.reverse(state.events), state.handler})
        pacer_loop(state)
    end
  end

  defp notify_waiter(state, ordinal) do
    case Map.pop(state, {:waiter, ordinal}) do
      {nil, state} ->
        state

      {from, state} ->
        send(from, {:pacer_barrier, ordinal})
        state
    end
  end

  # A step waits for the next barrier or the connection's end.
  defp answer_stepper(%{stepper: from} = state) when is_pid(from) do
    send(from, :pacer_stepped)
    %{state | stepper: nil}
  end

  defp answer_stepper(state), do: state

  defp release_waiting(%{waiting: ordinal} = state) when is_integer(ordinal) do
    case FakeUpstream.release_frame(state.upstream, state.ref) do
      :ok -> %{state | waiting: nil, events: [{:released, ordinal, since(state)} | state.events]}
      {:error, _reason} -> state
    end
  end

  defp release_waiting(state), do: state

  defp release_now(state, 0), do: state

  defp release_now(state, count) do
    :ok = FakeUpstream.release_frame(state.upstream, state.ref)

    receive do
      {:fake_upstream_frame_barrier, ordinal, handler, ref} when ref == state.ref ->
        if handler != state.handler, do: Process.monitor(handler)
        release_now(%{state | handler: handler, waiting: ordinal}, count - 1)
    after
      @detection_timeout_ms -> flunk("the held reply never reached its next frame barrier")
    end
  end

  defp since(%{t0: nil}), do: :before_pace
  defp since(%{t0: t0}), do: System.monotonic_time(:millisecond) - t0

  defp await!(fun, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_until!(fun, deadline, message)
  end

  defp await_until!(fun, deadline, message) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(message)

      true ->
        receive do
        after
          5 -> await_until!(fun, deadline, message)
        end
    end
  end
end
