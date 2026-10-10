defmodule CodexPooler.Gateway.Transports.NativeSteeringProxyLossTest do
  use CodexPooler.DataCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3, socket_connection_state!: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [ensure_test_distribution_started!: 0, enter_peer_owner_topology!: 0, stop_pool_owners_on_exit: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Websocket.OwnerCleanup
  alias CodexPooler.Platform.{ExecutionRegistry, ForwardedGenerationEnd}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.UnreachableNodeSupport

  @moduletag capture_log: true
  @detection_timeout_ms 15_000
  @path "/backend-api/codex/responses"
  @original_usage %{"input_tokens" => 41, "output_tokens" => 23, "total_tokens" => 64}
  @successor_usage %{"input_tokens" => 110, "output_tokens" => 7, "total_tokens" => 117}

  setup_all do
    ensure_test_distribution_started!()
    %{proxy: UnreachableNodeSupport.boot_app_peer!()}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    enter_peer_owner_topology!()
    :ok
  end

  @tag slow: "halts the real socket/lane VM during an admitted successor and observes surviving-owner accounting cleanup and replacement"
  test "an actual proxy-node loss retires the exact native successor without changing its settled original", %{proxy: proxy} do
    fixture = start_fixture!(proxy)
    original_client = fixture.client
    client = send_text!(original_client, steer(fixture.original_id))
    {client, accepted} = release_and_receive!(fixture, client, 0)
    {client, original_terminal} = release_and_receive!(fixture, client, 1)
    {client, successor_created} = release_and_receive!(fixture, client, 2)
    assert accepted["type"] == "response.steer.accepted"
    assert original_terminal["response"]["id"] == fixture.original_id
    assert successor_created["response"]["id"] == fixture.successor_id
    assert_receive {:fake_upstream_frame_barrier, 3, handler, hold}, @detection_timeout_ms
    assert handler == fixture.handler
    assert hold == fixture.hold

    owner_state = await!(fn -> :sys.get_state(fixture.owner) end, &match?(%{active_turn: %{descriptor: %{kind: :native_response_steering}}}, &1), "the owner never admitted the native successor")
    active = owner_state.active_turn
    lane = active.native_response_steering
    socket = owner_state.downstream.pid
    assert node(fixture.owner) == node()
    assert node(owner_state.upstream_pid) == node()
    assert node(lane) == proxy.node
    assert node(socket) == proxy.node
    assert active.task_pid == lane
    assert active.task_ref == nil
    assert active.submitter_monitor == nil
    lane_monitor = Map.get(active, :native_steering_monitor)
    downstream_monitor = owner_state.downstream_monitor
    assert is_reference(downstream_monitor)

    identity = Map.take(active.descriptor, [:request_id, :attempt_id, :replay_generation])
    successor = Repo.get!(Request, identity.request_id)
    attempt = Repo.get!(Attempt, identity.attempt_id)
    turn = Repo.get_by!(CodexTurn, request_id: successor.id)
    await!(fn -> :erpc.call(proxy.node, CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, :socket_connection_state!, [socket]) end, &(MapSet.size(&1.tasks) == 0), "the original proxy response task never completed")
    original = await!(fn -> Repo.get!(Request, fixture.original_request_id) end, &(&1.status == "succeeded"), "the original never settled")
    original_attempt = Repo.get_by!(Attempt, request_id: original.id)
    original_ledger = ledger(original.id)
    assert_complete_ledger!(original.id)
    assert original.id != successor.id
    assert original_attempt.id != attempt.id
    assert successor.status == "in_progress"
    assert attempt.status == "in_progress"
    assert turn.status == "in_progress"
    assert turn.id == active.descriptor.codex_turn_id
    assert attempt.owner_instance_id == Atom.to_string(proxy.node)
    assert :erpc.call(proxy.node, :erlang, :list_to_pid, [String.to_charlist(attempt.owner_process_id)]) == lane
    assert {:ok, _execution_id} = Ecto.UUID.cast(attempt.owner_execution_id)
    assert :alive = :erpc.call(proxy.node, ExecutionRegistry, :status, [attempt.owner_execution_id, lane])
    assert [entry] = ledger(successor.id)
    assert entry.entry_kind == "reservation"
    assert %OwnerCleanup{request_id: request_id, attempt_id: attempt_id, owner_instance_id: cleanup_owner, owner_lease_token: token, downstream_epoch: epoch} = active.cleanup_witness
    assert {request_id, attempt_id} == {successor.id, attempt.id}
    assert cleanup_owner == Atom.to_string(node())
    assert token == owner_state.owner_lease_token
    assert epoch == owner_state.downstream.epoch
    assert successor.request_metadata["websocket_owner_forwarding"]["owner_instance_id"] == cleanup_owner
    assert successor.request_metadata["websocket_owner_forwarding"]["downstream_epoch"] == epoch
    assert :erpc.call(proxy.node, :sys, :get_state, [lane]).owner == fixture.owner

    observation = install_loss_observer!(fixture.owner, lane_monitor, downstream_monitor)
    lane_down = Process.monitor(lane)
    socket_down = Process.monitor(socket)
    assert :ok = UnreachableNodeSupport.halt!(proxy.peer, proxy.node)
    assert_receive {:DOWN, ^lane_down, :process, ^lane, :noconnection}, @detection_timeout_ms
    assert_receive {:DOWN, ^socket_down, :process, ^socket, :noconnection}, @detection_timeout_ms
    assert_receive {^observation, :owner_down, ^downstream_monitor, ^socket, :noconnection}, @detection_timeout_ms

    # Both signals are genuine VM-loss monitors. The terminal is still held at
    # the provider, so neither a lane result nor a socket delivery ACK can clear
    # the owner or release this request's reservation.
    retired = await!(fn -> :sys.get_state(fixture.owner) end, &(&1.active_turn == nil and &1.downstream == nil), "the surviving owner retained the lost proxy successor")
    assert is_reference(lane_monitor)
    assert_receive {^observation, :owner_down, ^lane_monitor, ^lane, :noconnection}, @detection_timeout_ms
    assert retired.native_response_steering == nil
    assert retired.native_compaction_admission == nil
    assert retired.ordinary_success_result == nil
    assert retired.draining? == false
    assert retired.owner_lease_token == token
    assert Process.alive?(fixture.owner)
    assert {:ok, %{generation: nil}} = UpstreamWebsocketSession.live_connection(retired.upstream_pid)
    assert %{reason: "unreachable_downstream_cancelled", owner_instance_id: ending_owner} = Repo.get!(ForwardedGenerationEnd, attempt.id)
    assert ending_owner == Atom.to_string(node())
    assert %CodexSession{owner_lease_token: ^token, owner_instance_id: ^cleanup_owner} = Repo.get!(CodexSession, fixture.session_id)
    assert [%BridgeOwnerLease{status: "active", lease_token: ^token}] = leases(fixture.session_id)

    compensated = await!(fn -> Repo.get!(Request, successor.id) end, &(&1.completed_at != nil), "the surviving owner never compensated the successor")
    assert compensated.status == "failed"
    assert compensated.response_status_code == 499
    assert compensated.last_error_code == "client_disconnected"
    assert compensated.usage_status == "usage_unknown"
    assert %Attempt{id: ^attempt_id, status: "failed", completed_at: %DateTime{}} = Repo.get!(Attempt, attempt.id)
    assert %CodexTurn{id: turn_id, status: "interrupted", error_code: "client_disconnected", final_attempt_id: ^attempt_id} = Repo.get!(CodexTurn, turn.id)
    assert turn_id == turn.id
    assert_complete_ledger!(successor.id)
    assert [settlement] = Enum.filter(ledger(successor.id), &(&1.entry_kind == "settlement"))
    assert settlement.usage_status == "usage_unknown"
    assert Decimal.equal?(settlement.settled_cost_micros, Decimal.new(0))

    # Release the actual provider tail only after the owner retired it. Its
    # late terminal cannot overwrite the compensated tuple or the original.
    assert :ok = FakeUpstream.release_remaining_frames(fixture.upstream, fixture.hold)
    assert_receive {:DOWN, ref, :process, ^handler, _reason}, @detection_timeout_ms
    assert ref == fixture.handler_monitor
    assert :ok = FakeUpstream.retire_frame_barriers(fixture.upstream, fixture.hold, handler)
    Mint.HTTP.close(client.conn)
    replacement = Scenario.connect!(fixture.port, fixture.setup, @path, fixture.window)
    replacement_state = socket_connection_state!(replacement.socket)
    assert replacement_state.websocket_owner_pid == fixture.owner
    assert replacement_state.websocket_owner_downstream.epoch > epoch
    {replacement, frames} = Scenario.turn!(replacement, fixture.setup, "synthetic independent turn after proxy loss")
    assert Scenario.terminal(frames) == {"response.completed", fixture.replacement_id}
    assert socket_connection_state!(replacement.socket).websocket_owner_pid == fixture.owner
    Scenario.close!(replacement)
    [replacement_request] = Repo.all(from request in Request, where: request.pool_id == ^fixture.setup.pool.id and request.id not in ^[fixture.warm_request_id, original.id, successor.id])
    assert replacement_request.status == "succeeded"
    assert_complete_ledger!(replacement_request.id)
    assert Repo.get!(Request, successor.id) == compensated
    assert Repo.get!(Request, original.id) == original
    assert Repo.get!(Attempt, original_attempt.id) == original_attempt
    assert ledger(original.id) == original_ledger
    assert_complete_ledger!(successor.id)
    assert FakeUpstream.physical_counts(fixture.upstream).websocket_generation == 3
    assert FakeUpstream.physical_counts(fixture.upstream).http_generation == 0
    assert :ok = FakeUpstream.verify!(fixture.upstream)
  end

  defp start_fixture!(proxy) do
    suffix = System.unique_integer([:positive])
    original_id = "resp_synthetic_proxy_original_#{suffix}"
    successor_id = "resp_synthetic_proxy_successor_#{suffix}"
    replacement_id = "resp_synthetic_proxy_replacement_#{suffix}"
    hold = make_ref()
    opening = [created(original_id), encode(%{"type" => "response.output_text.delta", "item_id" => "msg_synthetic_proxy_original", "output_index" => 0, "content_index" => 0, "delta" => "synthetic visible output"})]
    response = FakeUpstream.websocket_steerable(opening, notify: self(), ref: hold, response_id: original_id, steer_id: "steer_synthetic_proxy_#{suffix}", terminal_frames: [completed(original_id, @original_usage)], successor_frames: [created(successor_id), completed(successor_id, @successor_usage)], batches: :separate)
    upstream = start_upstream(FakeUpstream.strict_sequence([expected_request(FakeUpstream.websocket_text_frames([created("resp_synthetic_proxy_warm_#{suffix}"), completed("resp_synthetic_proxy_warm_#{suffix}", @original_usage)]), 1), expected_request(response, 1), expected_request(FakeUpstream.websocket_text_frames([created(replacement_id), completed(replacement_id, @original_usage)]), 2)]))
    scope = model_serving_scope()
    slug = "native-proxy-loss-#{suffix}"
    provider_model = "synthetic-proxy-loss-provider-#{suffix}"
    register_fixture_cleanup!(slug, provider_model)
    setup = gateway_setup(upstream, pool_slug: slug, exposed_model_id: "synthetic-proxy-loss-#{suffix}", upstream_model_id: provider_model)
    stop_pool_owners_on_exit(setup.pool)
    set_model_serving_mode!(scope, setup, "full")
    port = start_public_endpoint!()
    window = Scenario.window()
    warm = Scenario.connect!(port, setup, @path, window)
    {warm, _frames} = Scenario.turn!(warm, setup, "synthetic owner bootstrap")
    warm_state = socket_connection_state!(warm.socket)
    owner = warm_state.websocket_owner_pid
    session_id = warm_state.codex_session.id
    warm_request_id = Repo.one!(from request in Request, where: request.pool_id == ^setup.pool.id, select: request.id)
    Scenario.close!(warm)
    on_exit(fn -> release_held_provider(upstream, hold) end)
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(proxy.port, setup, Ecto.UUID.generate(), @path, [{"x-codex-window-id", window.id}])
    client = %{conn: conn, websocket: websocket, ref: ref}
    turn_id = Ecto.UUID.generate()
    metadata = %{"session_id" => window.thread, "thread_id" => window.thread, "turn_id" => turn_id, "request_kind" => "turn"}
    frame = encode(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic proxy loss instructions", "tools" => [], "input" => native_text_input("synthetic proxy original"), "stream" => true, "store" => false, "client_metadata" => %{"session_id" => window.thread, "thread_id" => window.thread, "turn_id" => turn_id, "x-codex-window-id" => window.id, "x-codex-turn-metadata" => encode(metadata)}})
    client = send_text!(client, frame)
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    handler_monitor = Process.monitor(handler)
    {client, created} = receive_text!(client)
    {client, delta} = receive_text!(client)
    assert created["type"] == "response.created"
    assert delta["type"] == "response.output_text.delta"
    original_request_id = Repo.one!(from request in Request, where: request.pool_id == ^setup.pool.id and request.id != ^warm_request_id, select: request.id)
    owner_state = :sys.get_state(owner)
    assert node(owner_state.downstream.pid) == proxy.node
    assert is_pid(owner_state.native_response_steering)
    %{setup: setup, upstream: upstream, hold: hold, handler: handler, handler_monitor: handler_monitor, owner: owner, client: client, window: window, port: port, session_id: session_id, warm_request_id: warm_request_id, original_request_id: original_request_id, original_id: original_id, successor_id: successor_id, replacement_id: replacement_id}
  end

  defp register_fixture_cleanup!(slug, provider_model) do
    UnboxedFixture.register_unboxed_cleanup!(fn ->
      if pool = Repo.get_by(Pool, slug: slug) do
        pricing = Repo.get_by!(PricingSnapshot, model_identifier: provider_model)
        cleanup_unboxed_pool!(%{pool: pool, pricing: pricing})
      end
    end)
  end

  defp release_held_provider(upstream, hold) do
    FakeUpstream.release_steerable(upstream, hold)
    FakeUpstream.release_remaining_frames(upstream, hold)
  catch
    :exit, {:noproc, _call} -> :ok
    :exit, {:normal, _call} -> :ok
    :exit, {:shutdown, _call} -> :ok
  end

  defp install_loss_observer!(owner, lane_monitor, downstream_monitor) do
    ref = make_ref()

    on_exit(fn ->
      try do
        :sys.remove(owner, ref)
      catch
        :exit, {:noproc, _call} -> :ok
        :exit, {:normal, _call} -> :ok
        :exit, {:shutdown, _call} -> :ok
      end
    end)

    assert :ok = :sys.install(owner, {ref, &__MODULE__.observe_owner_down/3, %{notify: self(), ref: ref, monitors: [lane_monitor, downstream_monitor]}})
    ref
  end

  @doc false
  @spec observe_owner_down(map(), term(), term()) :: map()
  def observe_owner_down(probe, {:in, {:DOWN, monitor, :process, pid, reason}}, _name) do
    if monitor in probe.monitors, do: send(probe.notify, {probe.ref, :owner_down, monitor, pid, reason})
    probe
  end

  def observe_owner_down(probe, _event, _name), do: probe

  defp release_and_receive!(fixture, client, ordinal) do
    assert_receive {:fake_upstream_frame_barrier, ^ordinal, handler, hold}, @detection_timeout_ms
    assert handler == fixture.handler
    assert hold == fixture.hold
    assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
    receive_text!(client)
  end

  defp send_text!(client, frame) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_text!(client) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    {%{client | conn: conn, websocket: websocket}, CodexPooler.JSON.decode!(text)}
  end

  defp expected_request(response, ordinal), do: FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, websocket_connection_ordinal: ordinal, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)
  defp encode(frame), do: CodexPooler.JSON.encode!(frame)
  defp created(id), do: encode(%{"type" => "response.created", "response" => %{"id" => id, "status" => "in_progress", "output" => []}})
  defp completed(id, usage), do: encode(%{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => usage}})
  defp steer(id), do: encode(%{"type" => "response.steer", "previous_response_id" => id, "input" => native_text_input("synthetic proxy steering input")})
  defp ledger(request_id), do: Repo.all(from entry in LedgerEntry, where: entry.request_id == ^request_id, order_by: [asc: entry.id])
  defp leases(session_id), do: Repo.all(from lease in BridgeOwnerLease, where: lease.codex_session_id == ^session_id)
  defp assert_complete_ledger!(request_id), do: assert(Enum.frequencies_by(ledger(request_id), & &1.entry_kind) == %{"reservation" => 1, "settlement" => 1, "release" => 1})

  defp await!(read, predicate, message, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    value = read.()

    cond do
      predicate.(value) ->
        value

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          10 -> await!(read, predicate, message, deadline)
        end

      true ->
        flunk(message)
    end
  end
end
