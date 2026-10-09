defmodule CodexPooler.Gateway.Transports.NativeSteeringDrainWitnessTest do
  use CodexPooler.DataCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [socket_connection_state!: 1, model_serving_scope: 0, set_model_serving_mode!: 3, released_client_frame: 2]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain, WebsocketOwnerSession}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @budget 15_000
  @path "/backend-api/codex/responses"
  @usage %{"input_tokens" => 11, "output_tokens" => 3, "total_tokens" => 14}

  # Warm one real peer per module; each scenario still gets its own owner and rows.
  setup_all do
    peer_node = start_shared_bridge_peer!()
    :erpc.call(peer_node, ExUnit, :start, [[autorun: false]])
    :erpc.call(peer_node, Code, :require_file, [__ENV__.file])
    %{peer_node: peer_node}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for {topology, mode} <- [{:local, "full"}, {:local, "lite"}, {:peer, "full"}, {:peer, "lite"}] do
    test "#{topology} #{mode} no-terminal native steering successor has no settled drain witness", %{peer_node: peer_node} do
      fixture = fixture!(unquote(mode), unquote(topology), peer_node)
      {client, handler} = open_successor!(fixture)
      socket_state = socket_connection_state!(client.socket)
      owner = socket_state.websocket_owner_pid
      assert node(owner) == if(fixture.peer, do: fixture.peer.node, else: node())
      lane = socket_state.native_response_steering
      identity = socket_state.native_response_steering_active.identity
      assert Repo.get!(Request, identity.request_id).request_metadata["routing"]["model_serving_mode"] == unquote(mode)
      owner_before = :sys.get_state(owner)
      assert owner_before.active_turn.descriptor.request_id == identity.request_id
      refute owner_before.active_turn.terminal_forwarded?

      for stale <- [Map.put(identity, :request_id, Ecto.UUID.generate()), Map.update!(identity, :replay_generation, &(&1 + 1))] do
        :ok = WebsocketOwnerSession.complete_steering_successor(owner, lane, stale, {:error, :stale_completion})
        assert :sys.get_state(owner).active_turn.descriptor == owner_before.active_turn.descriptor
      end

      done = hold_done!(client.socket, identity)
      cutoff = make_ref()
      {coordinator, deadline} = coordinator!(node(owner), cutoff)
      drain = Task.async(fn -> RolloutDrain.start_drain(name: coordinator, timeout_ms: 50, deadline_margin_ms: 0, deadline_floor_ms: 1, deadline: deadline) end)

      worker =
        receive do
          {^cutoff, :cutoff_held, worker} -> worker
        after
          @budget -> flunk("coordinator cutoff barrier not reached")
        end

      {duplicate, summary_probe} = join_drain!(coordinator, deadline)
      # Close the actual provider transport after response.steer created the successor.
      assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
      hold = fixture.hold
      assert_receive {:fake_upstream_frame_barrier, 4, ^handler, ^hold}, @budget
      assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
      send(handler, :fake_upstream_abrupt_close_websocket)
      assert_receive {^done, :done_held}, @budget
      state = :sys.get_state(owner)
      assert is_nil(state.active_turn)
      assert Repo.get!(Request, identity.request_id).status == "failed"
      :ok = WebsocketOwnerSession.complete_steering_successor(owner, lane, identity, {:error, :duplicate_completion})
      send(worker, {cutoff, :release})
      summary = Task.await(drain, @budget)
      assert Task.await(duplicate, @budget) == summary
      assert :sys.get_state(coordinator).active_drain == nil
      assert_receive {^summary_probe, :summary_seen}, @budget
      refute_received {^summary_probe, :summary_seen}
      send(client.socket, {done, :release})
      {client, trailing_types} = receive_close!(client)
      assert trailing_types == ["response.output_text.delta"]
      Mint.HTTP.close(client.conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
      assert Map.take(summary, [:owners_seen, :owners_drained, :turns_completed, :turns_aborted, :owners_failed]) == %{owners_seen: 1, owners_drained: 1, turns_completed: 0, turns_aborted: 1, owners_failed: 0}
      assert length(FakeUpstream.websocket_steers(fixture.upstream)) == 1
      assert FakeUpstream.physical_counts(fixture.upstream).websocket_generation == 1
      assert FakeUpstream.physical_counts(fixture.upstream).http_generation == 0
    end
  end

  for {topology, mode} <- [{:local, "full"}, {:local, "lite"}, {:peer, "full"}, {:peer, "lite"}] do
    test "#{topology} #{mode} actually forwarded successor terminal waits for settlement at cutoff", %{peer_node: peer_node} do
      fixture = fixture!(unquote(mode), unquote(topology), peer_node, true)
      {client, handler} = open_successor!(fixture)
      socket_state = socket_connection_state!(client.socket)
      owner = socket_state.websocket_owner_pid
      assert node(owner) == if(fixture.peer, do: fixture.peer.node, else: node())
      lane = socket_state.native_response_steering
      identity = socket_state.native_response_steering_active.identity
      assert Repo.get!(Request, identity.request_id).request_metadata["routing"]["model_serving_mode"] == unquote(mode)
      terminal_hold = install_probe!(lane, %{kind: :terminal, request_id: identity.request_id})
      owner_probe = install_probe!(owner, %{kind: :drain})
      hold = fixture.hold
      :ok = FakeUpstream.release_frame(fixture.upstream, hold)
      {client, terminal} = receive_frame!(client)
      assert event_type(terminal) == "response.completed"

      receive do
        {^terminal_hold, :terminal_held} -> :ok
      after
        @budget -> flunk("successor terminal settlement barrier missing")
      end

      assert Repo.get!(Request, identity.request_id).status == "in_progress"
      assert :sys.get_state(owner).active_turn.terminal_forwarded?
      cutoff = make_ref()
      {coordinator, deadline} = coordinator!(node(owner), cutoff)
      drain = Task.async(fn -> RolloutDrain.start_drain(name: coordinator, timeout_ms: 50, deadline_margin_ms: 0, deadline_floor_ms: 1, deadline: deadline) end)

      worker =
        receive do
          {^cutoff, :cutoff_held, worker} -> worker
        after
          @budget -> flunk("forwarded terminal cutoff barrier missing")
        end

      send(worker, {cutoff, :release})

      receive do
        {^owner_probe, :drain_entered} -> :ok
      after
        @budget -> flunk("owner settlement drain call missing")
      end

      assert is_map(:sys.get_state(owner).drain_settlement)
      {duplicate, summary_probe} = join_drain!(coordinator, deadline)
      assert Repo.get!(Request, identity.request_id).status == "in_progress"
      send(lane, {terminal_hold, :release})
      assert_receive {:fake_upstream_frame_barrier, 4, ^handler, ^hold}, @budget
      :ok = FakeUpstream.release_frame(fixture.upstream, hold)
      summary = Task.await(drain, @budget)
      assert Task.await(duplicate, @budget) == summary
      assert :sys.get_state(coordinator).active_drain == nil
      assert_receive {^summary_probe, :summary_seen}, @budget
      refute_received {^summary_probe, :summary_seen}
      assert Map.take(summary, [:owners_drained, :turns_completed, :turns_aborted, :owners_failed]) == %{owners_drained: 1, turns_completed: 1, turns_aborted: 0, owners_failed: 0}
      assert Repo.get!(Request, identity.request_id).status == "succeeded"
      {client, extra} = receive_close!(client)
      assert extra == []
      Mint.HTTP.close(client.conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
    end
  end

  defp fixture!(mode, topology, peer_node, terminal? \\ false) do
    if topology == :peer, do: enter_peer_owner_topology!()
    suffix = System.unique_integer([:positive])
    original = "resp_synthetic_drain_original_#{suffix}"
    successor = "resp_synthetic_drain_successor_#{suffix}"
    hold = make_ref()
    response = FakeUpstream.websocket_steerable([created(original)], notify: self(), ref: hold, response_id: original, terminal_frames: [completed(original)], successor_frames: [created(successor), if(terminal?, do: completed(successor), else: delta())], batches: :separate)
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)]))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    peer = if topology == :peer, do: start_shared_peer_window_owner!(setup, "#{thread}:0", peer_node)

    {_server, port} = start_public_endpoint_with_server!()

    on_exit(fn ->
      try do
        FakeUpstream.release_steerable(upstream, hold)
        FakeUpstream.release_remaining_frames(upstream, hold)
      catch
        :exit, _ -> :ok
      end
    end)

    %{upstream: upstream, setup: setup, port: port, hold: hold, original: original, successor: successor, thread: thread, peer: peer}
  end

  defp open_successor!(fixture) do
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref, _} = public_websocket_connect_with_request_headers!(fixture.port, fixture.setup, fixture.thread, @path, [{"session-id", fixture.thread}, {"thread-id", fixture.thread}, {"x-codex-window-id", "#{fixture.thread}:0"}])
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    client = %{conn: conn, websocket: websocket, ref: ref, socket: socket, frames: []}
    opener = released_client_frame(fixture.setup, fixture.thread).(native_text_input("synthetic drain witness"), Ecto.UUID.generate(), %{"instructions" => "synthetic instructions", "tools" => [], "store" => false})
    client = send_text!(client, opener)
    hold = fixture.hold
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @budget
    {client, frame} = receive_frame!(client)
    assert event_type(frame) == "response.created"
    client = send_text!(client, encode(%{"type" => "response.steer", "previous_response_id" => fixture.original, "input" => native_text_input("synthetic steer")}))

    client =
      Enum.reduce(0..2, client, fn ordinal, client ->
        assert_receive {:fake_upstream_frame_barrier, ^ordinal, ^handler, ^hold}, @budget
        :ok = FakeUpstream.release_frame(fixture.upstream, hold)
        {client, frame} = receive_frame!(client)
        assert event_type(frame) == Enum.at(["response.steer.accepted", "response.completed", "response.created"], ordinal)
        client
      end)

    assert_receive {:fake_upstream_frame_barrier, 3, ^handler, ^hold}, @budget
    {client, handler}
  end

  defp join_drain!(coordinator, deadline) do
    ref = install_probe!(coordinator, %{kind: :join})
    task = Task.async(fn -> RolloutDrain.start_drain(name: coordinator, timeout_ms: 50, deadline_margin_ms: 0, deadline_floor_ms: 1, deadline: deadline) end)

    receive do
      {^ref, :joined} -> :ok
    after
      @budget -> flunk("second drain caller did not reach coordinator")
    end

    assert length(:sys.get_state(coordinator).active_drain.waiters) == 2
    {task, ref}
  end

  defp coordinator!(owner_node, ref) do
    suffix = System.unique_integer([:positive])
    names = [:"steering_witness_drain_#{suffix}", :"steering_witness_stream_#{suffix}", :"steering_witness_activity_#{suffix}"]

    on_exit(fn ->
      Enum.each(names, fn name ->
        try do
          GenServer.stop({name, owner_node}, :normal, @budget)
        catch
          :exit, {:noproc, _} -> :ok
          :exit, {{:nodedown, _}, _} -> :ok
        end
      end)
    end)

    coordinator = :erpc.call(owner_node, __MODULE__, :start_coordinator, [self(), ref, names])
    parent = self()
    {coordinator, %{now_ms: fn -> cutoff_clock(parent, ref) end, schedule_wait: fn _pid, _ref, _ms -> raise "unexpected clock wait" end, cancel_wait: fn _, _ -> :ok end}}
  end

  @doc false
  def start_coordinator(parent, ref, [name, stream_name, activity_name]) do
    {:ok, activities} = ActivityRegistry.start_link(name: activity_name)
    Process.unlink(activities)
    {:ok, streams} = DeferredStreamRegistry.start_link(name: stream_name)
    Process.unlink(streams)
    deadline = %{now_ms: fn -> cutoff_clock(parent, ref) end, schedule_wait: fn _pid, _ref, _ms -> raise "unexpected clock wait" end, cancel_wait: fn _, _ -> :ok end}
    {:ok, coordinator} = RolloutDrain.start_link(name: name, activity_registry: activity_name, stream_registry: stream_name, deadline: deadline)
    Process.unlink(coordinator)
    coordinator
  end

  @doc false
  def cutoff_clock(parent, ref) do
    {:current_stacktrace, stack} = Process.info(self(), :current_stacktrace)

    if Enum.any?(stack, fn {module, function, _, _} -> module == RolloutDrain and function == :poll_active_turn end) do
      monitor = Process.monitor(parent)
      send(parent, {ref, :cutoff_held, self()})

      receive do
        {^ref, :release} ->
          Process.demonitor(monitor, [:flush])
          1_000_000

        {:DOWN, ^monitor, :process, ^parent, _reason} ->
          1_000_000
      after
        @budget -> raise "cutoff release missing"
      end
    else
      0
    end
  end

  defp install_probe!(pid, fields) do
    ref = make_ref()

    on_exit(fn ->
      send(pid, {ref, :release})

      try do
        :sys.remove(pid, ref)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok = :sys.install(pid, {ref, &__MODULE__.probe/3, Map.merge(fields, %{ref: ref, parent: self()})})
    ref
  end

  @doc false
  def probe(%{kind: :terminal, request_id: request_id, ref: ref, parent: parent} = probe, {:in, {:"$gen_call", _from, {:terminal, request_id, _finalization}}}, _name) do
    send(parent, {ref, :terminal_held})

    receive do
      {^ref, :release} -> :done
    after
      @budget -> raise "terminal settlement release missing"
    end

    probe
  end

  def probe(%{kind: :drain, ref: ref, parent: parent} = probe, {:in, {:"$gen_call", _from, :drain}}, _name) do
    send(parent, {ref, :drain_entered})
    probe
  end

  def probe(%{kind: :join, ref: ref, parent: parent} = probe, {:in, {:"$gen_call", _from, {:start_drain, _timeout, _policy}}}, _name) do
    send(parent, {ref, :joined})
    probe
  end

  def probe(%{kind: :join, ref: ref, parent: parent} = probe, {:in, {:rollout_drain_finished, _drain_ref, _summary}}, _name) do
    send(parent, {ref, :summary_seen})
    probe
  end

  def probe(probe, _event, _name), do: probe

  defp hold_done!(socket, identity) do
    ref = make_ref()

    on_exit(fn ->
      send(socket, {ref, :release})

      try do
        :sys.remove(socket, ref)
      catch
        :exit, _ -> :ok
      end
    end)

    :ok = :sys.install(socket, {ref, &__MODULE__.hold_done/3, %{ref: ref, identity: identity, parent: self()}})
    ref
  end

  @doc false
  def hold_done(%{identity: identity, ref: ref, parent: parent}, {:in, {:native_response_steering_done, _lane, identity, _result}}, _name) do
    send(parent, {ref, :done_held})

    receive do
      {^ref, :release} -> :done
    after
      @budget -> raise "socket completion barrier release missing"
    end
  end

  def hold_done(probe, _event, _name), do: probe

  defp send_text!(client, payload) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, payload)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_frame!(%{frames: [frame | rest]} = client), do: {%{client | frames: rest}, frame}

  defp receive_frame!(client) do
    message = receive_mint_socket_message!(client.conn, @budget, "native steering frame missing")

    case Mint.WebSocket.stream(client.conn, message) do
      {:ok, conn, responses} ->
        {websocket, frames} =
          Enum.reduce(responses, {client.websocket, []}, fn
            {:data, ref, data}, {websocket, frames} when ref == client.ref ->
              {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)

              {websocket,
               frames ++
                 Enum.filter(decoded, fn
                   {:text, _} = frame -> not String.starts_with?(event_type(frame) || "", "codex.")
                   {:close, _, _} -> true
                   _ -> false
                 end)}

            _, acc ->
              acc
          end)

        receive_frame!(%{client | conn: conn, websocket: websocket, frames: frames})

      :unknown ->
        receive_frame!(client)

      {:error, _conn, _reason, _responses} ->
        flunk("native steering socket closed before expected frame")
    end
  end

  defp receive_close!(client, extra \\ []) do
    {client, frame} = receive_frame!(client)

    case frame do
      {:close, _code, _reason} -> {client, Enum.reverse(extra)}
      {:text, _} -> receive_close!(client, [event_type(frame) | extra])
    end
  end

  defp event_type({:text, raw}), do: CodexPooler.JSON.decode!(raw)["type"]
  defp encode(value), do: CodexPooler.JSON.encode!(value)
  defp delta, do: encode(%{"type" => "response.output_text.delta", "item_id" => "msg_synthetic", "output_index" => 0, "content_index" => 0, "delta" => "synthetic"})
  defp created(id), do: encode(%{"type" => "response.created", "response" => %{"id" => id, "status" => "in_progress", "output" => []}})
  defp completed(id), do: encode(%{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => @usage}})
end
