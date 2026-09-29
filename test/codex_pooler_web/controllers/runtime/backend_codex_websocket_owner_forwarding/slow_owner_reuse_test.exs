defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.SlowOwnerReuseTest do
  # A new socket on a session whose owner runs on its node asks that owner
  # whether it can be reused. An owner that did not answer within its call
  # budget was taken for stale and stopped (findings#285 row 270-247):
  #
  #   * A live owner lost its provider connection, and the response anchor and
  #     cache on it.
  #   * A turn it was running for another socket failed 502 `owner_crashed`.
  #   * An owner blocked inside a callback could not handle the stop in time,
  #     so the new socket's init crashed (1011 "websocket initialization
  #     unavailable"). The queued stop still ended the owner, and the live
  #     socket's next turn met 503 `owner_unavailable`.
  #
  # An owner that does not answer in time is now judged from its registry
  # value:
  #
  #   * one that started a lease renewal within the lease TTL stays in place,
  #     and the new socket closes 1011 with `owner_forward_timeout`;
  #   * one that started none for longer than the TTL is stopped. When it
  #     cannot handle the stop within the budget it is killed, and the session
  #     goes through the lease takeover;
  #   * one holding a lease the new socket took over is stopped;
  #   * one still starting stays in place.
  #
  # One node: the real public listener, owner forwarding on, the session's
  # owner on this node, the Pool's default serving mode, FakeUpstream, and the
  # released client's native frames. The owner call budget is one second. The
  # owner is suspended (`:sys.suspend/1`) or held inside a callback
  # (`:sys.replace_state/3`) past it.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [await_socket_connection_state!: 2, completed_response_frames: 4, receive_frames_until_close!: 3, receive_native_terminal!: 3, released_client_frame: 2, socket_connection_state!: 1, with_info_log: 1]

  alias CodexPooler.Access
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession}
  alias CodexPooler.Gateway.Transports.Websocket.{OwnerDefaults, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true

  @owner_call_budget_ms 1_000
  @detection_timeout_ms 15_000
  @forward_timeout_close {:close, 1011, "websocket owner forwarding timed out"}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    original_env = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(original_env, owner_call_timeout_ms: @owner_call_budget_ms))
    :ok
  end

  @tag slow: "suspends the owner past a one-second status budget while it runs another socket's turn"
  test "a live owner slower than its status budget keeps the turn it runs for another socket, and the new socket closes with the timeout" do
    release_ref = make_ref()
    live = owner_socket!([FakeUpstream.barrier_websocket_frames(turn_frames(), notify: self(), release_ref: release_ref)])
    {conn, websocket} = public_websocket_send_text!(live.client.conn, live.client.websocket, live.client.ref, turn_frame(live, "the turn the owner runs"))
    {conn, websocket} = receive_first_output!(live, release_ref, conn, websocket)
    owner_monitor = Process.monitor(live.owner)
    :ok = :sys.suspend(live.owner)

    second_frame = turn_frame(live, "a second socket's turn")
    {refused, log} = with_info_log(fn -> refused_socket!(live, second_frame) end)

    assert refused == [@forward_timeout_close]
    assert log =~ "websocket owner busy left in place codex_session_id=#{live.session_id}"
    assert log =~ "phase=init reason_class=owner_forward_timeout"
    refute log =~ "websocket owner stale replaced"
    refute_received {:DOWN, ^owner_monitor, :process, _owner, _reason}

    :sys.resume(live.owner)
    :ok = FakeUpstream.release_remaining_frames(live.upstream, release_ref)
    {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, live.client.ref)
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_reuse_turn"}} = terminal

    # The same owner, on the same lease and provider connection, serves the
    # client's retry of the refused socket.
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_reuse_next"}} = served_socket!(live, second_frame)
    assert_owner_kept!(live)
    Scenario.close!(%{live.client | conn: conn, websocket: websocket})
    assert Scenario.settled_statuses!(live.setup, 3) == ["succeeded", "succeeded", "succeeded"]
  end

  @tag slow: "holds the owner inside a callback past a one-second status budget"
  test "an owner busy inside a callback past its status budget stays in place and serves its socket's next turn" do
    live = owner_socket!([])
    owner_monitor = Process.monitor(live.owner)
    busy = hold_owner_busy!(live.owner, 2_500)

    {refused, log} = with_info_log(fn -> refused_socket!(live, turn_frame(live, "a second socket's turn")) end)

    assert refused == [@forward_timeout_close]
    assert log =~ "websocket owner busy left in place codex_session_id=#{live.session_id}"
    refute log =~ "websocket control path failed"
    assert Task.await(busy, @detection_timeout_ms) == :released
    refute_received {:DOWN, ^owner_monitor, :process, _owner, _reason}

    {client, next} = Scenario.turn!(live.client, live.setup, "the live socket's next turn")
    assert Scenario.terminal(next) == {"response.completed", "resp_slow_reuse_next"}
    assert_owner_kept!(live)
    Scenario.close!(client)
    assert Scenario.settled_statuses!(live.setup, 2) == ["succeeded", "succeeded"]
  end

  @tag slow: "suspends an owner whose last lease renewal is older than the lease TTL past a one-second status budget"
  test "an owner that started no lease renewal for longer than the lease TTL and does not answer is stopped, and a new owner serves" do
    live = owner_socket!([])
    :ok = age_last_renewal!(live)
    owner_monitor = Process.monitor(live.owner)
    :ok = :sys.suspend(live.owner)

    {terminal, log} = with_info_log(fn -> served_socket!(live, turn_frame(live, "a second socket's turn")) end)

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_reuse_next"}} = terminal
    assert_receive {:DOWN, ^owner_monitor, :process, _owner, {:shutdown, :stale_owner}}, @detection_timeout_ms
    assert log =~ "websocket owner stale replaced codex_session_id=#{live.session_id}"
    assert log =~ "reuse_reason=unresponsive"
    assert_replaced!(live)
  end

  # The killed owner's socket takes it for crashed and releases its lease.
  # That release used to reach the new owner started on the same lease: once
  # it landed after the second socket's attach, that socket's turn met 503
  # `owner_unavailable`. The live socket is held until the second socket's
  # init has finished, so the release always lands there. The session now
  # goes through the takeover, and the new owner holds a lease of its own.
  @tag slow: "holds an owner whose last lease renewal is older than the lease TTL inside a callback past two one-second budgets"
  test "an unresponsive owner blocked inside a callback is killed after its stop budget, and a new owner on a new lease serves" do
    live = owner_socket!([])
    :ok = age_last_renewal!(live)
    owner_monitor = Process.monitor(live.owner)
    busy = hold_owner_busy!(live.owner, @detection_timeout_ms)
    :ok = hold_socket!(live.client.socket)

    {terminal, log} =
      with_info_log(fn ->
        second = Scenario.connect!(live.port, live.setup, Scenario.native_route(), live.window)
        assert_receive {:DOWN, ^owner_monitor, :process, _owner, :killed}, @detection_timeout_ms
        _initialized = await_socket_connection_state!(second.socket, &(is_map(&1) and is_pid(Map.get(&1, :websocket_owner_pid))))
        :sys.resume(live.client.socket)
        :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(live.client.socket, @detection_timeout_ms)
        {conn, websocket} = public_websocket_send_text!(second.conn, second.websocket, second.ref, turn_frame(live, "a second socket's turn"))
        {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, second.ref)
        Scenario.close!(%{second | conn: conn, websocket: websocket})
        terminal
      end)

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_reuse_next"}} = terminal
    assert log =~ "websocket owner killed after its stop budget codex_session_id=#{live.session_id}"
    assert log =~ "stop_budget_ms=#{@owner_call_budget_ms}"
    assert log =~ "websocket owner takeover succeeded"
    refute log =~ "websocket control path failed"
    assert Task.await(busy, @detection_timeout_ms) == :owner_gone
    assert Repo.get!(CodexSession, live.session_id).owner_lease_token != live.lease_token
    assert_replaced!(live)
  end

  @tag slow: "suspends the owner past a one-second status budget once its lease expired"
  test "an owner that does not answer and holds a lease the new socket took over is stopped, and a new owner serves" do
    live = owner_socket!([])
    owner_monitor = Process.monitor(live.owner)
    :ok = :sys.suspend(live.owner)

    # The lease expired while the owner renewed nothing: the next socket's
    # session start takes it over under a new token.
    expired = from(l in BridgeOwnerLease, where: l.codex_session_id == ^live.session_id and l.status == "active", update: [set: [expires_at: fragment("clock_timestamp() - interval '1 second'")]])
    assert {1, _rows} = Repo.update_all(expired, [])

    {terminal, log} = with_info_log(fn -> served_socket!(live, turn_frame(live, "a second socket's turn")) end)

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_reuse_next"}} = terminal
    assert_receive {:DOWN, ^owner_monitor, :process, _owner, {:shutdown, :stale_owner}}, @detection_timeout_ms
    assert log =~ "reuse_reason=lease_replaced"
    assert Repo.get!(CodexSession, live.session_id).owner_lease_token != live.lease_token
    assert_replaced!(live)
  end

  # The regular renewal and the early lease check after an unreachable
  # downstream (findings#286) both renew the record.
  for {tick_name, tick} <- [regular: :renew_owner_lease, early_check: {:renew_owner_lease, :unreachable_downstream, 0}] do
    @tag tick: tick
    @tag slow: "suspends the owner past a one-second status budget after a renewal tick"
    test "a #{tick_name} renewal tick renews the owner's record: aged past the lease TTL, then renewed, a slow owner stays in place", ctx do
      live = owner_socket!([])
      :ok = age_last_renewal!(live)
      send(live.owner, ctx.tick)
      # The tick is handled before the owner answers the next system message.
      _state = :sys.get_state(live.owner)
      :ok = :sys.suspend(live.owner)

      {refused, log} = with_info_log(fn -> refused_socket!(live, turn_frame(live, "a second socket's turn")) end)

      assert refused == [@forward_timeout_close]
      assert log =~ "websocket owner busy left in place codex_session_id=#{live.session_id}"
      :sys.resume(live.owner)
      assert_owner_kept!(live)
      Scenario.close!(live.client)
    end
  end

  @tag slow: "waits out a one-second status budget of an owner held in its start"
  test "an owner still starting that does not answer within the budget is left to its start" do
    setup = gateway_setup(Scenario.upstream!("resp_slow_reuse"))
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, %CodexSession{} = session} = Gateway.start_codex_session(auth, %{accepted_turn_state: "starting-owner-#{System.unique_integer([:positive])}", owner_instance_id: Atom.to_string(node())})
    session = Repo.get!(CodexSession, session.id)
    on_exit(fn -> stop_owner(session.id) end)
    start_opts = [codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: Atom.to_string(node())]
    release_ref = make_ref()
    held = held_upstream(self(), release_ref)
    starter = Task.async(fn -> WebsocketOwnerSession.start_owner([{:upstream, held} | start_opts]) end)
    assert_receive {:owner_start_held, owner, ^release_ref}, @detection_timeout_ms

    {result, log} = with_info_log(fn -> WebsocketOwnerSession.start_owner([{:upstream, immediate_upstream()} | start_opts]) end)

    assert result == {:error, :owner_forward_timeout}
    assert log =~ "websocket owner busy left in place codex_session_id=#{session.id}"
    assert log =~ "owner_last_renewal_age_ms=none"
    assert Process.alive?(owner)

    send(owner, {:release_owner_start, release_ref})
    assert {:ok, ^owner} = Task.await(starter, @detection_timeout_ms)
  end

  # The session's owner on this node, started by the socket's first turn, and
  # its provider connection. Every later request is answered by `middle`, then
  # as `resp_slow_reuse_next`.
  defp owner_socket!(middle) do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a new socket on a session whose owner on the same node does not answer within its call budget)
        FakeUpstream.repeat_last([completed_response_frames("resp_slow_reuse_one", [], 3, 2)] ++ middle ++ [completed_response_frames("resp_slow_reuse_next", [], 3, 2)])
      )

    setup = gateway_setup(upstream)
    {_server, port} = start_public_endpoint_with_server!()
    window = Scenario.window()
    client = Scenario.connect!(port, setup, Scenario.native_route(), window)
    {client, one} = Scenario.turn!(client, setup, "turn one")
    assert Scenario.terminal(one) == {"response.completed", "resp_slow_reuse_one"}
    state = socket_connection_state!(client.socket)
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    assert state.websocket_owner_pid == owner
    session = Repo.get!(CodexSession, state.codex_session.id)

    %{setup: setup, port: port, window: window, client: client, owner: owner, upstream: upstream, session_id: session.id, lease_token: session.owner_lease_token}
  end

  defp turn_frame(live, text), do: released_client_frame(live.setup, live.window.thread).(native_text_input(text), Ecto.UUID.generate(), %{})

  # The provider's first two frames of the held turn reached the client.
  defp receive_first_output!(live, release_ref, conn, websocket) do
    for ordinal <- 0..1 do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @detection_timeout_ms
      :ok = FakeUpstream.release_frame(live.upstream, release_ref)
    end

    assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^release_ref}, @detection_timeout_ms
    {conn, websocket, _created} = receive_text!(conn, websocket, live.client.ref, "response.created")
    {conn, websocket, _delta} = receive_text!(conn, websocket, live.client.ref, "response.output_text.delta")
    {conn, websocket}
  end

  # A second socket on the window that sends its turn and is closed before
  # anything else.
  defp refused_socket!(live, frame) do
    second = Scenario.connect!(live.port, live.setup, Scenario.native_route(), live.window)
    {conn, websocket} = public_websocket_send_text!(second.conn, second.websocket, second.ref, frame)
    {conn, _websocket, frames} = receive_frames_until_close!(conn, websocket, second.ref)
    Mint.HTTP.close(conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(second.socket, @detection_timeout_ms)
    frames
  end

  defp served_socket!(live, frame) do
    client = Scenario.connect!(live.port, live.setup, Scenario.native_route(), live.window)
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, client.ref)
    Scenario.close!(%{client | conn: conn, websocket: websocket})
    terminal
  end

  # Blocks the owner inside a callback for `hold_ms`: the owner runs the
  # function itself and handles nothing else meanwhile.
  defp hold_owner_busy!(owner, hold_ms) do
    test = self()
    on_exit(fn -> if Process.alive?(owner), do: Process.exit(owner, :kill) end)

    hold = fn state ->
      send(test, {:owner_busy, owner})
      Process.sleep(hold_ms)
      state
    end

    busy =
      Task.async(fn ->
        try do
          _state = :sys.replace_state(owner, hold, hold_ms + @detection_timeout_ms)
          :released
        catch
          :exit, _owner_gone -> :owner_gone
        end
      end)

    assert_receive {:owner_busy, ^owner}, @detection_timeout_ms
    busy
  end

  # Suspends a socket's listener connection process, so it handles nothing
  # (its owner's exit included) until the test resumes it.
  defp hold_socket!(socket) do
    :ok = :sys.suspend(socket)

    on_exit(fn ->
      try do
        :sys.resume(socket)
      catch
        :exit, _socket_gone -> :ok
      end
    end)

    :ok
  end

  # Ages the owner's record of its last lease renewal, as if it had started
  # none for longer than the lease TTL. The update runs inside the owner,
  # because only the process a registry value names can write it.
  defp age_last_renewal!(live) do
    aged_by_ms = OperationalSettings.current().bridge_owner_lease_ttl_seconds * 1_000 + 1_000
    session_id = live.session_id

    :sys.replace_state(live.owner, fn state ->
      {{:ready, _digest, _aged}, {:ready, _same_digest, _renewed}} =
        Registry.update_value(WebsocketOwnerSession.Registry, session_id, fn {:ready, digest, last_renewal_monotonic_ms} -> {:ready, digest, last_renewal_monotonic_ms - aged_by_ms} end)

      state
    end)

    :ok
  end

  defp assert_owner_kept!(live) do
    assert WebsocketOwnerSession.lookup(live.session_id) == {:ok, live.owner}
    assert Repo.get!(CodexSession, live.session_id).owner_lease_token == live.lease_token
    assert FakeUpstream.websocket_connection_count(live.upstream) == 1
  end

  defp assert_replaced!(live) do
    assert {:ok, replacement} = WebsocketOwnerSession.lookup(live.session_id)
    assert replacement != live.owner
    Scenario.close!(live.client)
  end

  defp held_upstream(parent, release_ref) do
    %{
      start: fn ->
        send(parent, {:owner_start_held, self(), release_ref})

        receive do
          {:release_owner_start, ^release_ref} -> Agent.start_link(fn -> :ready end)
        end
      end,
      send: fn _upstream_pid, _request, _writer -> :ok end,
      close: &stop_agent/1
    }
  end

  defp immediate_upstream do
    %{start: fn -> Agent.start_link(fn -> :ready end) end, send: fn _upstream_pid, _request, _writer -> :ok end, close: &stop_agent/1}
  end

  defp stop_agent(upstream_pid) do
    if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
    :ok
  end

  defp stop_owner(codex_session_id) do
    case WebsocketOwnerSession.lookup(codex_session_id) do
      {:ok, owner} ->
        monitor = Process.monitor(owner)
        Process.exit(owner, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^owner, _reason}, @detection_timeout_ms

      {:error, :owner_unavailable} ->
        :ok
    end
  end

  defp receive_text!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => ^type} = event -> {conn, websocket, event}
      %{"type" => "codex.response.metadata"} -> receive_text!(conn, websocket, ref, type)
    end
  end

  defp turn_frames do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_slow_reuse_turn", "status" => "in_progress"}}
    deltas = for i <- 1..10, do: %{"type" => "response.output_text.delta", "delta" => "synthetic #{i} "}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_slow_reuse_turn", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => 10, "total_tokens" => 15}}}
    Enum.map([created | deltas] ++ [completed], &CodexPooler.JSON.encode!/1)
  end
end
