defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.UnreachableDownstreamTest do
  # A websocket owner whose downstream's node becomes unreachable while the
  # turn it asked for is still generating (findings#286). The turn's executor
  # ran on that node, so nobody can receive its output or settle it:
  #
  #   * on a partition, that node's socket closes 1011, interrupts the turn
  #     `owner_crashed` and releases the owner's lease, and the client's resend
  #     is served by a new owner there once the proxy executor's end is
  #     durable;
  #   * when that node dies, nobody interrupts or releases anything, and the
  #     resend of a turn that showed output meets `409 duplicate_turn` until
  #     recovery settles the turn.
  #
  # The owner used to keep generating a turn that showed output to the end: a
  # second generation of the same turn that nobody received or recorded. On
  # DOWN `:noconnection` of its downstream it now cancels such a turn at once:
  # the upstream request's caller exits and the upstream session closes the
  # request (`request_caller_down`). A turn that showed nothing stays `:lost`
  # for the resend that can still rejoin it, and a replay under way keeps its
  # turn, as for any other downstream loss.
  #
  # Owner forwarding on, native route, the Pool's default serving mode, the
  # released client's frames. FakeUpstream on this node holds the turn at a
  # frame barrier and, once the node is cut off, releases one frame every 50 ms,
  # the provider going on generating.
  #   * Partition: the owner on a peer VM with a TCP control connection, the
  #     socket on this node; cookie swap and disconnect, healed at the end.
  #   * Node death: the socket on a peer VM running the whole application with
  #     its public listener, the owner on this node; that VM is halted, and the
  #     client's resend comes through this node's listener.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [completed_response_frames: 4, receive_frames_until_close!: 3, receive_native_terminal!: 3, released_client_frame: 2, socket_connection_state!: 1, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [ensure_test_distribution_started!: 0, start_shared_peer_window_owner!: 3]
  import CodexPoolerWeb.Runtime.UnreachableNodeSupport

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true

  @deltas 10
  @pace_ms 50
  @detection_timeout_ms 15_000
  # Frames the provider may still have sent before the owner heard of the cut
  # (at most one pacing interval and the one released at the cut).
  @frames_before_cancel 3

  setup_all do
    ensure_test_distribution_started!()
    {owner_peer, owner_node} = boot_tcp_owner_peer!()
    %{owner_peer: owner_peer, owner_node: owner_node, app_peers: %{previsible: boot_app_peer!(), visible: boot_app_peer!()}}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    assert :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok
  end

  describe "a partition cuts the socket's node off from the owner's" do
    @tag shown: :visible
    @tag slow: "cuts a peer VM's owner off mid-turn and paces the provider until the turn's connection closes"
    test "the owner cancels a turn that showed output at once, and the socket's node interrupts it", ctx do
      turn = start_turn!(ctx, :partition)

      on_exit(fn -> heal!(ctx.owner_node) end)
      partition!(ctx.owner_node)
      :ok = pace!(turn.pacer, @pace_ms)

      {conn, _websocket, frames} = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref)
      Mint.HTTP.close(conn)
      assert List.last(frames) == {:close, 1011, "websocket owner crashed"}
      assert_turn_cancelled_at_once!(turn.pacer)
      assert %{active_turn: nil} = :peer.call(ctx.owner_peer, :sys, :get_state, [turn.owner])

      # The socket's node interrupted the turn and released the cut owner's
      # lease, as before.
      assert {"failed", 499, "owner_crashed"} == request_outcome(turn.request_id)
      assert %CodexTurn{status: "interrupted", error_code: "owner_crashed"} = Repo.get_by!(CodexTurn, request_id: turn.request_id)
      assert [%BridgeOwnerLease{status: "released", owner_instance_id: cut_owner_instance}] = leases(turn.session_id)
      assert cut_owner_instance == Atom.to_string(ctx.owner_node)
      assert FakeUpstream.count(turn.upstream) == 2
    end

    @tag slow: "arms a replay, starts it on a second socket, then cuts the peer VM's owner off while the provider paces"
    test "a replay under way keeps its turn", ctx do
      retry_ref = make_ref()
      retry_pacer = start_pacer!(retry_ref)
      replay_ref = make_ref()
      pacer = start_pacer!(replay_ref)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (a client-retry turn armed for replay when its socket closed, replayed on a socket whose node is then cut off)
          FakeUpstream.repeat_last([
            completed_response_frames("resp_unreachable_one", [], 3, 2),
            FakeUpstream.websocket_terminal_failure("server_error"),
            FakeUpstream.barrier_websocket_frames(turn_frames(), notify: retry_pacer, release_ref: retry_ref),
            FakeUpstream.barrier_websocket_frames(turn_frames(), notify: pacer, release_ref: replay_ref)
          ])
        )

      :ok = pace_upstream!(retry_pacer, upstream)
      :ok = pace_upstream!(pacer, upstream)
      setup = gateway_setup(upstream)
      window = Scenario.window()
      owner = start_shared_peer_window_owner!(setup, window.id, ctx.owner_node).owner_pid
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, Scenario.native_route(), window)
      {client, _one} = Scenario.turn!(client, setup, "turn one")
      frame = released_client_frame(setup, window.thread).(native_text_input("the turn the provider failed"), Ecto.UUID.generate(), %{})
      {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
      {conn, websocket, failed} = receive_native_terminal!(conn, websocket, client.ref)
      assert %{"type" => "response.failed"} = failed

      # The client's retry, closed before it showed anything: its replay is armed.
      {conn, _websocket} = public_websocket_send_text!(conn, websocket, client.ref, frame)
      :ok = await_frame_barrier!(retry_pacer, 0)
      Scenario.close!(%{client | conn: conn})
      :ok = await_peer_state!(ctx.owner_peer, owner, &match?(%{active_turn: nil, suspended_replay: %{provisional_status: :armed}}, &1), "the replay was never armed")

      # The client's resend on another socket starts the replay, which shows output.
      replay = Scenario.connect!(port, setup, Scenario.native_route(), window)
      {conn, websocket} = public_websocket_send_text!(replay.conn, replay.websocket, replay.ref, frame)
      :ok = await_frame_barrier!(pacer, 0)
      :ok = release_frames!(pacer, 2)
      {conn, websocket, _created} = receive_text!(conn, websocket, replay.ref, "response.created")
      {conn, _websocket, _delta} = receive_text!(conn, websocket, replay.ref, "response.output_text.delta")
      assert %{active_turn: %{visible_output?: true}, suspended_replay: %{provisional_status: :started}} = :peer.call(ctx.owner_peer, :sys, :get_state, [owner])

      on_exit(fn -> heal!(ctx.owner_node) end)
      partition!(ctx.owner_node)
      :ok = pace!(pacer, @pace_ms)
      :ok = await_peer_state!(ctx.owner_peer, owner, &is_nil(&1.downstream), "the owner kept its cut-off downstream")

      # The replay's turn goes on as before.
      assert %{active_turn: %{}, suspended_replay: %{provisional_status: :started}} = :peer.call(ctx.owner_peer, :sys, :get_state, [owner])
      assert %{connection_down_at: nil} = await_frames!(pacer, @frames_before_cancel + 2)
      Mint.HTTP.close(conn)
    end
  end

  describe "the socket's node dies" do
    @tag shown: :visible
    @tag slow: "halts the peer VM running the socket mid-turn and paces the provider until the turn's connection closes"
    test "the owner cancels a turn that showed output at once and keeps its lease", ctx do
      turn = start_turn!(ctx, :death)
      lease_token = Repo.get!(CodexSession, turn.session_id).owner_lease_token

      {_cancelled, log} =
        with_info_log(fn ->
          :ok = halt!(turn.app_peer.peer, turn.app_peer.node)
          :ok = pace!(turn.pacer, @pace_ms)
          assert_turn_cancelled_at_once!(turn.pacer)
        end)

      assert log =~ "websocket owner cancelled the turn of an unreachable downstream"
      assert %{active_turn: nil} = :sys.get_state(turn.owner)
      assert WebsocketOwnerSession.lookup(turn.session_id) == {:ok, turn.owner}

      # Nobody on the dead node interrupted or released anything: the turn
      # waits for recovery, under the owner's lease.
      assert {"in_progress", nil, nil} == request_outcome(turn.request_id)
      assert %CodexTurn{status: "in_progress"} = Repo.get_by!(CodexTurn, request_id: turn.request_id)
      assert [%BridgeOwnerLease{status: "active", lease_token: ^lease_token}] = leases(turn.session_id)
      assert FakeUpstream.count(turn.upstream) == 2
    end

    @tag shown: :previsible
    @tag slow: "halts the peer VM running the socket mid-turn and streams the whole paced turn to the resend"
    test "the owner keeps a turn that showed nothing, and the client's resend reattaches to it", ctx do
      turn = start_turn!(ctx, :death)

      :ok = halt!(turn.app_peer.peer, turn.app_peer.node)
      :ok = await_owner_state!(turn.owner, &match?(%{downstream: nil, active_turn: %{descriptor: %{downstream_status: :lost}}}, &1), "the owner did not keep the turn for a resend")

      # The released client's resend reaches this node before the turn's first
      # output, and rejoins the one generation.
      retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
      {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
      :ok = await_owner_state!(turn.owner, &match?(%{active_turn: %{descriptor: %{downstream_status: :attached}}}, &1), "the resend never reattached")
      :ok = pace!(turn.pacer, @pace_ms)
      {conn, websocket, events} = receive_turn!(conn, websocket, retry.ref, [])
      assert events == [{"response.created", "resp_unreachable_turn"} | List.duplicate({"response.output_text.delta", nil}, @deltas)] ++ [{"response.completed", "resp_unreachable_turn"}]
      assert FakeUpstream.count(turn.upstream) == 2
      Scenario.close!(%{retry | conn: conn, websocket: websocket})
    end
  end

  # A socket whose turn the owner runs and FakeUpstream holds before any
  # frame (with `:visible`, after the created event and one delta the client
  # received). `:partition`: the owner on the TCP-controlled peer, the socket
  # here. `:death`: the owner here, the socket on the application peer.
  defp start_turn!(ctx, topology) do
    release_ref = make_ref()
    pacer = start_pacer!(release_ref)

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a turn whose downstream's node is cut off or dies while the provider still generates it)
        FakeUpstream.repeat_last([
          completed_response_frames("resp_unreachable_one", [], 3, 2),
          FakeUpstream.barrier_websocket_frames(turn_frames(), notify: pacer, release_ref: release_ref)
        ])
      )

    :ok = pace_upstream!(pacer, upstream)
    setup = gateway_setup(upstream)
    window = Scenario.window()
    {_server, port} = start_public_endpoint_with_server!()

    {client, owner, app_peer} =
      case topology do
        :partition ->
          # Registers the Pool's cleanup too.
          peer_owner = start_shared_peer_window_owner!(setup, window.id, ctx.owner_node)
          client = Scenario.connect!(port, setup, Scenario.native_route(), window)
          {client, _one} = Scenario.turn!(client, setup, "turn one")
          {client, peer_owner.owner_pid, nil}

        :death ->
          register_unboxed_pool_cleanup!(setup)
          first = Scenario.connect!(port, setup, Scenario.native_route(), window)
          {first, _one} = Scenario.turn!(first, setup, "turn one")
          owner = socket_connection_state!(first.socket).websocket_owner_pid
          Scenario.close!(first)
          app_peer = Map.fetch!(ctx.app_peers, ctx.shown)
          {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(app_peer.port, setup, Ecto.UUID.generate(), Scenario.native_route(), [{"x-codex-window-id", window.id}])
          {%{conn: conn, websocket: websocket, ref: ref}, owner, app_peer}
      end

    frame = released_client_frame(setup, window.thread).(native_text_input("the turn whose downstream's node goes"), Ecto.UUID.generate(), %{})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    :ok = await_frame_barrier!(pacer, 0)

    {conn, websocket} =
      if ctx.shown == :visible do
        :ok = release_frames!(pacer, 2)
        {conn, websocket, _created} = receive_text!(conn, websocket, client.ref, "response.created")
        {conn, websocket, _delta} = receive_text!(conn, websocket, client.ref, "response.output_text.delta")
        {conn, websocket}
      else
        {conn, websocket}
      end

    session_id = Repo.one!(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id))
    request_id = Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress", select: r.id))

    %{
      pacer: pacer,
      upstream: upstream,
      setup: setup,
      window: window,
      port: port,
      client: %{client | conn: conn, websocket: websocket},
      owner: owner,
      app_peer: app_peer,
      frame: frame,
      session_id: session_id,
      request_id: request_id
    }
  end

  # The owner stopped the provider's generation right after its DOWN: the held
  # connection consumed at most the frames already on their way, and closed.
  defp assert_turn_cancelled_at_once!(pacer) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    %{frames: frames, connection_down_at: down_at} = await_pacer!(pacer, &(is_integer(&1.connection_down_at) or &1.frames > @frames_before_cancel), deadline)
    assert is_integer(down_at)
    assert frames <= @frames_before_cancel, "the owner went on generating: #{frames} frames after the cut"
  end

  defp await_frames!(pacer, count) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    report = await_pacer!(pacer, &(&1.frames >= count or not is_nil(&1.connection_down_at)), deadline)
    assert report.frames >= count or not is_nil(report.connection_down_at)
    report
  end

  defp await_pacer!(pacer, done?, deadline) do
    report = consumed_after_pace(pacer)

    if done?.(report) or System.monotonic_time(:millisecond) >= deadline do
      report
    else
      Process.sleep(10)
      await_pacer!(pacer, done?, deadline)
    end
  end

  defp await_owner_state!(owner, predicate, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_state!(fn -> :sys.get_state(owner) end, predicate, message, deadline)
  end

  defp await_peer_state!(peer, owner, predicate, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_state!(fn -> :peer.call(peer, :sys, :get_state, [owner]) end, predicate, message, deadline)
  end

  defp await_state!(get_state, predicate, message, deadline) do
    cond do
      predicate.(get_state.()) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(message)

      true ->
        Process.sleep(10)
        await_state!(get_state, predicate, message, deadline)
    end
  end

  defp receive_text!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => ^type} = event -> {conn, websocket, event}
      %{"type" => "codex.response.metadata"} -> receive_text!(conn, websocket, ref, type)
    end
  end

  # The turn's events up to its terminal, as {type, response id}.
  defp receive_turn!(conn, websocket, ref, events) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => "codex.response.metadata"} ->
        receive_turn!(conn, websocket, ref, events)

      %{"type" => type} = event when type in ["response.completed", "response.failed", "error"] ->
        {conn, websocket, Enum.reverse([{type, get_in(event, ["response", "id"])} | events])}

      %{"type" => type} = event ->
        receive_turn!(conn, websocket, ref, [{type, get_in(event, ["response", "id"])} | events])
    end
  end

  defp request_outcome(request_id) do
    request = Repo.get!(Request, request_id)
    {request.status, request.response_status_code, request.last_error_code}
  end

  defp leases(session_id), do: Repo.all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id, order_by: [asc: l.created_at]))

  defp turn_frames do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_unreachable_turn", "status" => "in_progress"}}
    deltas = for i <- 1..@deltas, do: %{"type" => "response.output_text.delta", "delta" => "synthetic #{i} "}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_unreachable_turn", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => @deltas, "total_tokens" => 5 + @deltas}}}
    Enum.map([created | deltas] ++ [completed], &CodexPooler.JSON.encode!/1)
  end
end
