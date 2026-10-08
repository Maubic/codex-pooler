defmodule CodexPooler.Gateway.Transports.WebsocketResponseSteerTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [await_socket_connection_state!: 2, socket_transport_barrier!: 3, route_circuit_failures: 1, with_info_log: 1, model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Dev.NativeCompactionTrace
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.{NativeCodexTurnMetadata, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.ResponseSteer
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.EventTaxonomy
  alias CodexPooler.Gateway.Transports.Websocket.{WebsocketOwnerForwarder, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.Gateway.Websocket.Adapter
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias __MODULE__.{OwnerSteeringProbe, RawPeer}

  @detection_timeout_ms 15_000

  @response_id "resp_synthetic_steering_unit_342"
  @input [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic client steering input"}]}]

  describe "ResponseSteer.parse/1" do
    test "recognizes native steering as a connection control with its untouched input" do
      frame = %{"type" => "response.steer", "previous_response_id" => @response_id, "input" => @input}

      assert {:ok, %{previous_response_id: @response_id, input: @input, frame: ^frame}} = ResponseSteer.parse(frame)
    end

    test "keeps provider-owned extra-key validation instead of stripping stream_id" do
      frame = %{"type" => "response.steer", "previous_response_id" => @response_id, "input" => @input, "stream_id" => "synthetic_extra_stream_id"}

      assert {:ok, steer} = ResponseSteer.parse(frame)
      assert steer.frame == frame
      assert CodexPooler.JSON.decode!(ResponseSteer.frame(steer)) == frame
    end

    test "accepts the bounds of a provider response id and an empty input list" do
      for response_id <- ["resp_a", "resp_" <> String.duplicate("a", 1_020)] do
        frame = %{"type" => "response.steer", "previous_response_id" => response_id, "input" => []}
        assert {:ok, %{previous_response_id: ^response_id, input: [], frame: ^frame}} = ResponseSteer.parse(frame)
      end
    end

    test "rejects a missing or malformed target and an input that is not a list" do
      frame = %{"type" => "response.steer", "previous_response_id" => @response_id, "input" => @input}

      for malformed <- [
            Map.delete(frame, "previous_response_id"),
            Map.delete(frame, "input"),
            %{frame | "previous_response_id" => "msg_not_a_response"},
            %{frame | "previous_response_id" => "resp_"},
            %{frame | "previous_response_id" => "resp_" <> String.duplicate("a", 1_021)},
            %{frame | "previous_response_id" => "resp_with space"},
            %{frame | "previous_response_id" => "resp_with\ncontrol"},
            %{frame | "previous_response_id" => 12},
            %{frame | "input" => nil},
            %{frame | "input" => %{}},
            %{frame | "input" => "synthetic input"}
          ] do
        assert ResponseSteer.parse(malformed) == :malformed
      end
    end

    test "does not confuse an ordinary steered continuation with response.steer" do
      for frame <- [
            %{"type" => "response.create", "previous_response_id" => @response_id, "input" => @input},
            %{"type" => "response.interrupt", "response_id" => @response_id},
            %{"type" => "response.steer.accepted"},
            %{},
            "response.steer",
            nil
          ] do
        assert ResponseSteer.parse(frame) == :not_steer
      end
    end
  end

  test "accepted and failed steering acknowledgements are known nonterminal controls" do
    for type <- ["response.steer.accepted", "response.steer.failed"] do
      assert EventTaxonomy.classify(type) == {"response.steer", "response_event"}
    end

    assert EventTaxonomy.allowed_event_type?("response.steer")
    assert EventTaxonomy.classify("response.steer.unmeasured_sibling") == {"response.unknown", "response_unknown_event"}
  end

  test "only the measured native-lane error requests native steering close handling" do
    assert ResponseSteer.native_lane_error?(%{"type" => "error", "error" => %{"code" => "unsupported_native_inflight_message"}})

    for frame <- [
          %{"type" => "response.steer.failed", "error" => %{"code" => "invalid_input"}},
          %{"type" => "error", "error" => %{"code" => "invalid_request"}},
          %{"type" => "response.failed", "response" => %{"error" => %{"code" => "unsupported_native_inflight_message"}}},
          %{},
          nil
        ] do
      refute ResponseSteer.native_lane_error?(frame)
    end
  end

  test "steering diagnostics keep only bounded vocabulary and the steer id fingerprint" do
    steer_id = "steer_synthetic_private_identifier"
    fingerprint = :crypto.hash(:sha256, steer_id) |> Base.encode16(case: :lower) |> binary_part(0, 12)

    {:ok, log} = with_info_log(fn -> ResponseSteer.log(:accepted, :direct, "response.steer.accepted", steer_id) end)

    assert log =~ "frame_type=response.steer.accepted outcome=accepted topology=direct steer_id_fingerprint=#{fingerprint}"
    refute log =~ steer_id
    refute log =~ @response_id
    refute log =~ "synthetic client steering input"
    assert ResponseSteer.fingerprint(nil) == "none"
    assert ResponseSteer.fingerprint(steer_id) == fingerprint
  end

  test "an upstream session without a producing connection does not invent steering acceptance" do
    session = start_supervised!({UpstreamWebsocketSession, []})
    assert {:ok, steer} = ResponseSteer.parse(%{"type" => "response.steer", "previous_response_id" => @response_id, "input" => @input})

    {_state, log} =
      with_info_log(fn ->
        assert :ok = UpstreamWebsocketSession.steer(session, steer)
        :sys.get_state(session)
      end)

    assert log =~ "native websocket response steer"
    assert log =~ "frame_type=response.steer"
    assert log =~ "outcome=session_idle"
    assert log =~ "topology=direct"
    refute log =~ @response_id
    refute log =~ "synthetic client steering input"
    assert Process.alive?(session)
  end

  describe "owner connection control" do
    test "another downstream and a collected turn cannot steer the owner's response" do
      downstream = downstream()
      assert {:ok, steer} = parsed_steer()

      for {active_turn, outcome} <- [
            {%{downstream: %{downstream | correlation_id: "another-steering-downstream"}, collect?: false, upstream_pid: self()}, "owner_not_downstream"},
            {%{downstream: downstream, collect?: true, upstream_pid: self()}, "owner_turn_not_relay"}
          ] do
        state = owner_state(active_turn)
        {reply, log} = with_info_log(fn -> call_steer(state, downstream, steer) end)

        assert {:reply, :ok, ^state} = reply
        assert log =~ "frame_type=response.steer outcome=#{outcome} topology=owner steer_id_fingerprint=none"
        refute log =~ @response_id
        refute log =~ "synthetic client steering input"
        refute_received {:upstream_websocket_steer, _steer}
      end
    end
  end

  test "an older remote owner reports an unsupported steering protocol without creating work" do
    earlier_node = :"codex_pooler@synthetic-earlier-steering-owner.example"
    session_id = Ecto.UUID.generate()
    downstream = downstream()
    assert {:ok, steer} = parsed_steer()
    args = [session_id, downstream, steer]
    undef = {:error, {:exception, :undef, [{WebsocketOwnerForwarder, :remote_steer_turn_v1, args, []}]}}
    opts = WebsocketOwnerNodeHarness.node_client_opts([earlier_node], calls: %{earlier_node => {:return, undef}})

    assert {:error, :remote_steer_v1_unsupported} =
             WebsocketOwnerForwarder.steer_remote_turn(earlier_node, session_id, Map.put(downstream, :owner_turn_id, self()), steer, opts)

    assert_receive {:websocket_owner_harness_node_call, %{node: ^earlier_node, function: :remote_steer_turn_v1, arity: 3}}
  end

  test "a connection-bound successor rejects replacement attachment and finishes on its original socket" do
    fixture = start_owner_successor!()
    owner = fixture.owner
    lane = fixture.lane
    identity = fixture.identity
    bound = fixture.bound

    assert {:error, :owner_busy} = WebsocketOwnerSession.attach_downstream(owner, %{pid: self(), correlation_id: Ecto.UUID.generate()})

    {replacement, log} =
      with_info_log(fn ->
        replacement = connect_owner_client!(fixture.port, fixture.setup, fixture.thread)
        on_exit(fn -> Mint.HTTP.close(replacement.conn) end)
        {_conn, _websocket, code, reason} = public_websocket_receive_close!(replacement.conn, replacement.websocket, replacement.ref)
        assert {code, reason} == Adapter.close_detail(:owner_busy)
        close_owner_client!(replacement)
        replacement
      end)

    assert log =~ CodexPoolerWeb.WebsocketConnectionLogger.init_failed_message()
    assert log =~ "reason_class=owner_busy"

    try do
      assert fixture.client.socket != replacement.socket
      assert_owner_successor_binding!(owner, lane, identity, bound)
      handler = fixture.handler
      hold = fixture.hold
      assert_receive {:fake_upstream_frame_barrier, 3, ^handler, ^hold}, @detection_timeout_ms

      assert :ok = FakeUpstream.release_remaining_frames(fixture.upstream, fixture.hold)
      {client, received} = receive_owner_frames!(fixture.client, length(fixture.remaining))
      assert frame_digests(received) == frame_digests(fixture.remaining)
      assert_receive {:fake_upstream_steered, ^handler, ^hold}, @detection_timeout_ms
      final_ordinal = 3 + length(fixture.remaining)
      assert_receive {:fake_upstream_frame_barrier, ^final_ordinal, ^handler, ^hold}, @detection_timeout_ms
      assert :ok = FakeUpstream.verify!(fixture.upstream)
      assert [original, successor] = await_raw_settled_rows!(fixture.setup)
      assert original.id != successor.id
      assert successor.id == identity.request_id
      assert successor.request_metadata["native_websocket_response_steering"]["predecessor_request_id"] == original.id

      for {row, usage} <- [{original, fixture.original_usage}, {successor, fixture.successor_usage}] do
        assert_owner_steering_settlement!(row, usage)
        assert %{"outcome" => "delivered", "terminal_class" => "response.completed"} = await_raw_delivery!(row)
      end

      # This answers only after the actor consumed the real socket's delivery
      # acknowledgement, not after its fifteen-second fallback.
      actor_state = :sys.get_state(lane, @detection_timeout_ms)
      assert %{active?: not is_nil(actor_state.active), socket: actor_state.socket, owner: actor_state.owner, admission_revoked?: actor_state.admission_revoked?} == %{active?: false, socket: fixture.client.socket, owner: owner, admission_revoked?: false}
      assert_owner_successor_compaction_binding!(fixture, actor_state)
      assert_owner_steering_physical_work!(fixture)
      assert route_circuit_failures(fixture.setup.assignment.id) == []
      socket_state = await_socket_connection_state!(client.socket, &(not is_map(Map.get(&1, :native_response_steering_active)) and MapSet.size(Map.get(&1, :tasks, MapSet.new())) == 0))
      assert socket_state.websocket_owner_downstream == bound
      {conn, websocket} = socket_transport_barrier!(client.conn, client.websocket, client.ref)
      lane_monitor = Process.monitor(lane)
      close_owner_client_on_wire!(%{client | conn: conn, websocket: websocket})
      assert {:ok, %{generation: nil}} = UpstreamWebsocketSession.live_connection(fixture.upstream_session)
      assert_receive {:DOWN, ^lane_monitor, :process, ^lane, _reason}, @detection_timeout_ms
    after
      close_owner_client!(replacement)
      close_owner_client!(fixture.client)
    end
  end

  test "an actual successor frame queued behind detach is stale without crashing its owner" do
    fixture = start_owner_successor!(terminal?: false)
    {client, [item_added]} = release_owner_successor_frame!(fixture, fixture.client, 3)
    assert frame_digests([item_added]) == frame_digests([hd(fixture.remaining)])
    ref = make_ref()
    owner = fixture.owner
    lane = fixture.lane
    identity = fixture.identity

    on_exit(fn ->
      send(lane, {ref, :release})
      remove_owner_steering_probe(owner)
    end)

    # Hold the actual upstream producer's frame call before the actor can
    # relay it, so the owner's detach is complete before that same call resumes.
    assert :ok = OwnerSteeringProbe.install(lane, %{kind: :hold_frame, ref: ref, notify: self(), identity: identity, producer: fixture.upstream_session})
    assert :ok = OwnerSteeringProbe.install(owner, %{kind: :relay_reply, ref: ref, notify: self(), identity: identity, lane: lane, socket: client.socket, pending_from: nil})
    assert :ok = OwnerSteeringProbe.install(client.socket, %{kind: :socket_frames, ref: ref, notify: self(), identity: identity, lane: lane, frames: 0})
    assert_receive {:fake_upstream_frame_barrier, 4, handler, hold}, @detection_timeout_ms
    assert {handler, hold} == {fixture.handler, fixture.hold}
    assert :ok = FakeUpstream.release_frame(fixture.upstream, fixture.hold)
    assert_receive {^ref, :frame_held, ^lane}, @detection_timeout_ms

    try do
      assert :ok = WebsocketOwnerSession.detach_downstream(owner, fixture.bound)
      assert_owner_successor_binding!(owner, lane, identity, nil)
      send(lane, {ref, :release})
      assert_receive {^ref, :relay_reply, {:error, :stale_generation}}, @detection_timeout_ms
      assert_receive {^ref, :socket_frame_fence, 0}, @detection_timeout_ms
      lane_monitor = Process.monitor(lane)
      close_owner_client_on_wire!(client)
      assert_receive {:DOWN, ^lane_monitor, :process, ^lane, _reason}, @detection_timeout_ms
      # The cancellation fixture ends at this delta, so releasing its final
      # barrier acknowledges completion without writing to a closed transport.
      finish_owner_steering_peer!(fixture, 5)

      assert [original, successor] = await_owner_steering_rows!(fixture.setup, &Enum.all?(&1, fn row -> row.completed_at != nil end))
      assert_owner_steering_settlement!(original, fixture.original_usage)
      assert successor.id == identity.request_id
      assert successor.status == "failed"
      assert successor.last_error_code == "client_disconnected"
      assert successor.response_status_code == 499
      assert successor.usage_status == "usage_unknown"
      assert successor.retry_count == 0
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^successor.id))
      assert attempt.id == identity.attempt_id
      assert attempt.status == "failed"
      assert attempt.completed_at != nil
      assert attempt.attempt_number == 1
      assert attempt.usage_status == "usage_unknown"
      assert %CodexTurn{status: "interrupted", error_code: "client_disconnected", completed_at: %DateTime{}} = Repo.get_by!(CodexTurn, request_id: successor.id)
      assert %{"outcome" => "aborted", "frames_after_visible" => 2} = await_raw_delivery!(successor)
      entries = Repo.all(from(l in LedgerEntry, where: l.request_id == ^successor.id))
      assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
      assert [settlement] = Enum.filter(entries, &(&1.entry_kind == "settlement"))
      assert settlement.attempt_id == identity.attempt_id
      assert settlement.usage_status == "usage_unknown"
      assert Enum.all?(entries, &(&1.amount_status == "recorded"))
      owner_state = :sys.get_state(owner)
      assert %{downstream: owner_state.downstream, active?: not is_nil(owner_state.active_turn), ordinary_success?: not is_nil(owner_state.ordinary_success_result), compaction_admission?: not is_nil(owner_state.native_compaction_admission)} == %{downstream: nil, active?: false, ordinary_success?: false, compaction_admission?: false}
      assert route_circuit_failures(fixture.setup.assignment.id) == []
      assert WebsocketOwnerSession.lookup(:sys.get_state(owner).codex_session_id) == {:ok, owner}
      assert {:ok, replacement} = WebsocketOwnerSession.attach_downstream(owner, %{pid: self(), correlation_id: Ecto.UUID.generate()})
      assert :ok = WebsocketOwnerSession.detach_downstream(owner, replacement)
      assert_owner_steering_physical_work!(fixture)
    after
      send(lane, {ref, :release})
      close_owner_client!(client)
      remove_owner_steering_probe(owner)
    end
  end

  for ending <- [:steered, :completed] do
    @tag ending: ending
    test "an observed decoded batch retains the unsolicited successor behind a #{ending} terminal", ctx do
      assert_raw_coalesced_successor!(ctx.ending)
    end
  end

  defp assert_raw_coalesced_successor!(ending) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    terminal_type = if ending == :steered, do: "response.incomplete", else: "response.completed"
    response_id = "resp_synthetic_raw_steering_original"
    successor_id = "resp_synthetic_raw_steering_successor"
    original_usage = %{"input_tokens" => 41, "output_tokens" => 23, "total_tokens" => 64}
    successor_usage = %{"input_tokens" => 110, "output_tokens" => 7, "total_tokens" => 117}
    original = %{"id" => response_id, "status" => if(ending == :steered, do: "incomplete", else: "completed"), "output" => [], "usage" => original_usage}
    original = if ending == :steered, do: Map.put(original, "incomplete_details", %{"reason" => "steered"}), else: original
    opening = [%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}}]

    tail = [
      %{"type" => "response.steer.accepted", "sequence_number" => 1, "steer" => %{"id" => "steer_synthetic_raw_342", "previous_response_id" => response_id}},
      %{"type" => terminal_type, "response" => original},
      %{"type" => "response.created", "response" => %{"id" => successor_id, "status" => "in_progress", "output" => []}},
      %{"type" => "response.completed", "response" => %{"id" => successor_id, "status" => "completed", "output" => [], "usage" => successor_usage}}
    ]

    peer = start_supervised!({RawPeer, notify: self(), opening: Enum.map(opening, &CodexPooler.JSON.encode!/1), tail: Enum.map(tail, &CodexPooler.JSON.encode!/1)})
    peer_monitor = Process.monitor(peer)
    port = RawPeer.port(peer)
    setup = gateway_setup(%FakeUpstream{url: "http://127.0.0.1:#{port}"})
    {_server, listener_port} = start_public_endpoint_with_server!()
    sockets_before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref} = public_websocket_connect!(listener_port, setup, Ecto.UUID.generate())
    socket_pid = WebsocketCleanupFence.await_new_listener_socket!(sockets_before)
    state = await_socket_connection_state!(socket_pid, &is_pid(Map.get(&1, :upstream_websocket_session)))
    session = state.upstream_websocket_session
    trace = observe_trailing_batch!(session)
    frame = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic raw peer instructions", "tools" => [], "input" => @input, "stream" => true, "store" => false})
    sent = CodexPooler.JSON.encode!(%{"type" => "response.steer", "previous_response_id" => response_id, "input" => @input})

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
      assert_receive {:raw_steering_open, ^peer}, @detection_timeout_ms
      {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, ref)
      assert :crypto.hash(:sha256, created) == :crypto.hash(:sha256, CodexPooler.JSON.encode!(hd(opening)))
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, sent)
      assert_receive {:raw_steering_tail_written, ^peer, %{generation_count: 1, steer_count: 1, tail_frame_count: 4, write_count: 1, steer_fingerprint: sent_fingerprint}}, @detection_timeout_ms
      assert sent_fingerprint == :crypto.hash(:sha256, sent)

      {conn, websocket, received} =
        Enum.reduce(tail, {conn, websocket, []}, fn _expected, {conn, websocket, frames} ->
          {conn, websocket, raw} = public_websocket_receive_text!(conn, websocket, ref)
          {conn, websocket, frames ++ [raw]}
        end)

      assert Enum.map(received, &:crypto.hash(:sha256, &1)) == Enum.map(tail, &:crypto.hash(:sha256, CodexPooler.JSON.encode!(&1)))
      assert_receive {:steering_decoded_trailing_count, ^session, trailing_count}, @detection_timeout_ms
      assert trailing_count >= 2
      assert [original_row, successor_row] = await_raw_settled_rows!(setup)
      assert original_row.id != successor_row.id
      assert original_row.last_error_code == nil
      assert successor_row.last_error_code == nil
      assert original_row.retry_count == 0
      assert successor_row.retry_count == 0

      for {row, usage, terminal} <- [{original_row, original_usage, terminal_type}, {successor_row, successor_usage, "response.completed"}] do
        assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^row.id))
        assert attempt.status == "succeeded"
        assert attempt.usage_status == "usage_known"
        assert attempt.attempt_number == 1
        assert [settlement] = Repo.all(from(l in LedgerEntry, where: l.request_id == ^row.id and l.entry_kind == "settlement"))
        assert {settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} == {usage["input_tokens"], usage["output_tokens"], usage["total_tokens"]}
        assert %{"outcome" => "delivered", "terminal_class" => ^terminal} = await_raw_delivery!(row)
      end

      assert route_circuit_failures(setup.assignment.id) == []
      _idle = await_socket_connection_state!(socket_pid, &(MapSet.size(&1.tasks) == 0))
      {conn, _websocket} = socket_transport_barrier!(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket_pid)
      assert_receive {:DOWN, ^peer_monitor, :process, ^peer, :normal}, @detection_timeout_ms
    after
      Mint.HTTP.close(conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket_pid)
      await_raw_peer_stopped!(peer)
      stop_batch_observer!(trace, session)
    end
  end

  defp await_raw_peer_stopped!(peer) do
    monitor = Process.monitor(peer)

    receive do
      {:DOWN, ^monitor, :process, ^peer, _reason} -> :ok
    after
      @detection_timeout_ms ->
        Process.exit(peer, :kill)
        assert_receive {:DOWN, ^monitor, :process, ^peer, _reason}, @detection_timeout_ms
        flunk("the raw steering peer remained blocked after its owned listener socket cleaned up")
    end
  end

  defp observe_trailing_batch!(session) do
    root = Path.join(System.tmp_dir!(), "synthetic-steering-batch-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      NativeCompactionTrace.stop_scope()
      File.rm_rf!(root)
    end)

    File.mkdir_p!(root)
    File.chmod!(root, 0o700)
    assert {:ok, _status} = NativeCompactionTrace.start_scope("synthetic-steering-batch", mode: :full, root: root, include_modules: [ResponseSteer], pids: [upstream_session: session])
    :erlang.trace(session, false, [:all])
    parent = self()
    tracer = start_supervised!({Task, fn -> trailing_count_loop(parent) end})
    mfa = {UpstreamWebsocketSession, :finish_terminal_result, 4}
    match_spec = [{[:"$1", :"$2", :"$3", :"$4"], [{:is_list, :"$4"}], [{:message, {:length, :"$4"}}]}]

    trace_session = :trace.session_create(:"synthetic_native_steering_batch_#{System.unique_integer([:positive])}", tracer, [])
    on_exit(fn -> :trace.session_destroy(trace_session) end)
    assert 1 = :trace.function(trace_session, mfa, match_spec, [:local])
    assert 1 = :trace.process(trace_session, session, true, [:call, :arity])
    %{session: trace_session, root: root}
  end

  defp trailing_count_loop(parent) do
    receive do
      {:trace, session, :call, {UpstreamWebsocketSession, :finish_terminal_result, 4}, count} when is_integer(count) ->
        if count > 0, do: send(parent, {:steering_decoded_trailing_count, session, count})
        trailing_count_loop(parent)

      _other ->
        trailing_count_loop(parent)
    end
  end

  defp stop_batch_observer!(trace, _session) do
    :trace.session_destroy(trace.session)
    assert :ok = NativeCompactionTrace.stop_scope()
    File.rm_rf!(trace.root)
  end

  defp await_raw_settled_rows!(setup, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    rows = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

    cond do
      length(rows) == 2 and Enum.all?(rows, &(&1.status == "succeeded" and &1.completed_at != nil)) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the decoded coalesced successor did not settle as an independent request")

      true ->
        receive do
        after
          5 -> await_raw_settled_rows!(setup, deadline)
        end
    end
  end

  defp await_raw_delivery!(row, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    receipt = Repo.one(from(a in Attempt, where: a.request_id == ^row.id, select: fragment("?->'downstream_delivery'", a.response_metadata)))

    cond do
      is_map(receipt) ->
        receipt

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the raw coalesced response has no independent delivery receipt")

      true ->
        receive do
        after
          5 -> await_raw_delivery!(row, deadline)
        end
    end
  end

  defp start_owner_successor!(opts \\ []) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    hold = make_ref()
    thread = Ecto.UUID.generate()
    original_id = "resp_synthetic_owner_steer_original_#{System.unique_integer([:positive])}"
    successor_id = "resp_synthetic_owner_steer_successor_#{System.unique_integer([:positive])}"
    original_usage = %{"input_tokens" => 41, "output_tokens" => 23, "total_tokens" => 64}
    successor_usage = %{"input_tokens" => 110, "output_tokens" => 7, "total_tokens" => 117}
    opening = encode_owner_event(%{"type" => "response.created", "response" => %{"id" => original_id, "status" => "in_progress", "output" => []}})
    original_terminal = owner_completed_frame(original_id, original_usage)
    successor_created = encode_owner_event(%{"type" => "response.created", "response" => %{"id" => successor_id, "status" => "in_progress", "output" => []}})
    item = %{"type" => "message", "id" => "msg_#{successor_id}", "role" => "assistant", "status" => "in_progress", "content" => []}

    remaining = [
      encode_owner_event(%{"type" => "response.output_item.added", "output_index" => 0, "item" => item}),
      encode_owner_event(%{"type" => "response.output_text.delta", "item_id" => item["id"], "output_index" => 0, "content_index" => 0, "delta" => "synthetic held successor output"})
    ]

    remaining = if Keyword.get(opts, :terminal?, true), do: remaining ++ [owner_completed_frame(successor_id, successor_usage)], else: remaining

    response = FakeUpstream.websocket_steerable([opening], notify: self(), ref: hold, response_id: original_id, terminal_frames: [original_terminal], successor_frames: [successor_created | remaining], batches: :separate)
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)]))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    {_server, port} = start_public_endpoint_with_server!()

    on_exit(fn ->
      try do
        FakeUpstream.release_remaining_frames(upstream, hold)
      catch
        :exit, _already_stopped -> :ok
      end
    end)

    client = connect_owner_client!(port, setup, thread)
    on_exit(fn -> Mint.HTTP.close(client.conn) end)

    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn", "window_id" => "#{thread}:0", "context_window_id" => Ecto.UUID.generate(), "window_number" => 0}
    create = encode_owner_event(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic owner steering instructions", "tools" => [], "input" => @input, "stream" => true, "store" => false, "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => metadata["turn_id"], "x-codex-window-id" => "#{thread}:0", "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, create)
    client = %{client | conn: conn, websocket: websocket}
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    handler_monitor = Process.monitor(handler)
    {client, [received_opening]} = receive_owner_frames!(client, 1)
    assert frame_digests([received_opening]) == frame_digests([opening])
    steer = encode_owner_event(%{"type" => "response.steer", "previous_response_id" => original_id, "input" => @input})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, steer)
    client = %{client | conn: conn, websocket: websocket}
    fixture = %{upstream: upstream, hold: hold, handler: handler}

    {client, initial_tail} =
      Enum.reduce(0..2, {client, []}, fn ordinal, {client, frames} ->
        {client, [frame]} = release_owner_successor_frame!(fixture, client, ordinal)
        {client, frames ++ [frame]}
      end)

    assert Enum.map(initial_tail, &CodexPooler.JSON.decode!(&1)["type"]) == ["response.steer.accepted", "response.completed", "response.created"]
    assert frame_digests(Enum.drop(initial_tail, 1)) == frame_digests([original_terminal, successor_created])
    state = await_socket_connection_state!(client.socket, &is_map(Map.get(&1, :native_response_steering_active)))
    lane = state.native_response_steering
    identity = state.native_response_steering_active.identity
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    bound = state.websocket_owner_downstream
    assert_owner_successor_binding!(owner, lane, identity, bound)
    upstream_session = :sys.get_state(owner).upstream_pid
    actor_state = :sys.get_state(lane, @detection_timeout_ms)
    assert Map.take(actor_state, [:socket, :owner, :upstream, :activated?, :admission_revoked?]) == %{socket: client.socket, owner: owner, upstream: upstream_session, activated?: true, admission_revoked?: false}
    claim_scope = WebsocketTurnIdentity.claim_scope(state.codex_session, thread)
    assert {:ok, parsed_metadata} = NativeCodexTurnMetadata.parse(CodexPooler.JSON.decode!(create), claim_scope)
    assert [original, successor] = await_owner_steering_rows!(setup, fn [original, successor] -> original.status == "succeeded" and successor.status == "in_progress" end)
    assert successor.id == identity.request_id
    assert Map.take(actor_state.active.reserved.request, [:id]) == %{id: identity.request_id}
    assert Map.take(actor_state.active.attempt, [:id]) == %{id: identity.attempt_id}
    assert %{"outcome" => "delivered"} = await_raw_delivery!(original)

    Map.merge(fixture, %{setup: setup, port: port, thread: thread, client: client, owner: owner, lane: lane, identity: identity, bound: bound, upstream_session: upstream_session, handler_monitor: handler_monitor, metadata: parsed_metadata, remaining: remaining, original_usage: original_usage, successor_usage: successor_usage, successor_id: successor_id, steer: steer})
  end

  defp connect_owner_client!(port, setup, thread) do
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, thread, "/backend-api/codex/responses", [{"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}])
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket}
  end

  defp close_owner_client!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  defp close_owner_client_on_wire!(client) do
    socket = client.socket
    monitor = Process.monitor(socket)
    {:ok, websocket, data} = Mint.WebSocket.encode(client.websocket, {:close, 1000, ""})
    {:ok, conn} = Mint.WebSocket.stream_request_body(client.conn, client.ref, data)
    {conn, _websocket, code, reason} = public_websocket_receive_close!(conn, websocket, client.ref)
    assert {code, reason} == {1000, ""}
    Mint.HTTP.close(conn)
    assert :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
    assert_receive {:DOWN, ^monitor, :process, ^socket, _reason}, @detection_timeout_ms
  end

  defp assert_owner_successor_binding!(owner, lane, identity, downstream) do
    state = :sys.get_state(owner)
    assert is_map(state.active_turn), "the owner did not retain the producer-created successor"
    active = state.active_turn
    assert is_map(active.descriptor), "the active successor has no owner descriptor"
    descriptor = active.descriptor

    assert %{downstream: state.downstream, active_downstream: Map.get(active, :downstream), lane: Map.get(active, :native_response_steering), kind: Map.get(descriptor, :kind), identity: Map.take(descriptor, [:request_id, :attempt_id, :replay_generation])} == %{downstream: downstream, active_downstream: downstream, lane: lane, kind: :native_response_steering, identity: identity}
  end

  defp assert_owner_successor_compaction_binding!(fixture, actor_state) do
    acknowledgement =
      case actor_state.original_result do
        {:ok, result} when is_map(result) -> Map.get(result, :native_response_steering_acknowledgement)
        _not_successful -> nil
      end

    assert {:ok, binding, receipt} = acknowledgement
    assert {receipt.request_id, receipt.attempt_id} == {fixture.identity.request_id, fixture.identity.attempt_id}
    assert receipt.owner == fixture.upstream_session
    assert %{lifecycle_id: lifecycle_id, generation: generation} = receipt.lifecycle
    assert {:ok, %{lifecycle_id: ^lifecycle_id, generation: ^generation}} = UpstreamWebsocketSession.live_connection(fixture.upstream_session)

    assert Map.take(binding, [:semantic_turn_key, :window_digest, :context_digest, :window_number, :previous_response_digest, :serving_mode, :lifecycle_id, :generation]) == %{semantic_turn_key: fixture.metadata.semantic_turn_key, window_digest: fixture.metadata.window_id_digest, context_digest: fixture.metadata.context_window_id_digest, window_number: fixture.metadata.window_number, previous_response_digest: NativeCodexTurnMetadata.response_id_digest(fixture.successor_id), serving_mode: :full, lifecycle_id: lifecycle_id, generation: generation}
    assert %{downstream_epoch: epoch} = binding.topology
    assert epoch == fixture.bound.epoch

    owner_state = :sys.get_state(fixture.owner)
    assert %{downstream: owner_state.downstream, active?: not is_nil(owner_state.active_turn), ordinary_success?: not is_nil(owner_state.ordinary_success_result)} == %{downstream: fixture.bound, active?: false, ordinary_success?: false}
    assert %{phase: :pending_compact, binding: ^binding} = owner_state.native_compaction_admission
    assert owner_state.native_compaction_admission_downstream == Map.take(fixture.bound, [:pid, :epoch, :correlation_id])
  end

  defp receive_owner_frames!(client, count) do
    Enum.reduce(1..count, {client, []}, fn _ordinal, {client, frames} ->
      {conn, websocket, frame} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
      {%{client | conn: conn, websocket: websocket}, frames ++ [frame]}
    end)
  end

  defp release_owner_successor_frame!(fixture, client, ordinal) do
    handler = fixture.handler
    hold = fixture.hold
    assert_receive {:fake_upstream_frame_barrier, ^ordinal, ^handler, ^hold}, @detection_timeout_ms
    assert :ok = FakeUpstream.release_frame(fixture.upstream, hold)
    receive_owner_frames!(client, 1)
  end

  defp await_owner_steering_rows!(setup, ready?, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    rows = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

    cond do
      length(rows) == 2 and ready?.(rows) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("owner steering requests did not reach the required lifecycle; statuses=#{inspect(Enum.map(rows, & &1.status))}")

      true ->
        receive do
        after
          5 -> await_owner_steering_rows!(setup, ready?, deadline)
        end
    end
  end

  defp assert_owner_steering_settlement!(row, usage) do
    assert row.status == "succeeded"
    assert row.retry_count == 0
    assert row.usage_status == "usage_known"
    assert row.completed_at != nil
    assert %CodexTurn{status: "succeeded", completed_at: %DateTime{}} = Repo.get_by!(CodexTurn, request_id: row.id)
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^row.id))
    assert attempt.status == "succeeded"
    assert attempt.attempt_number == 1
    assert attempt.usage_status == "usage_known"
    assert attempt.completed_at != nil
    entries = Repo.all(from(l in LedgerEntry, where: l.request_id == ^row.id))
    assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
    assert [settlement] = Enum.filter(entries, &(&1.entry_kind == "settlement"))
    assert settlement.attempt_id == attempt.id
    assert settlement.usage_status == "usage_known"
    assert Enum.all?(entries, &(&1.amount_status == "recorded"))
    assert {settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} == {usage["input_tokens"], usage["output_tokens"], usage["total_tokens"]}
  end

  defp assert_owner_steering_physical_work!(fixture) do
    assert [%{websocket_connection_id: 1, json: %{"type" => "response.create"}}] = FakeUpstream.requests(fixture.upstream)
    assert [%{websocket_connection_id: 1, body: steer}] = FakeUpstream.websocket_steers(fixture.upstream)
    assert frame_digests([steer]) == frame_digests([fixture.steer])
    assert FakeUpstream.physical_counts(fixture.upstream).websocket_generation == 1
    assert FakeUpstream.physical_counts(fixture.upstream).http_generation == 0
  end

  defp finish_owner_steering_peer!(fixture, ordinal) do
    handler_monitor = fixture.handler_monitor
    handler = fixture.handler
    hold = fixture.hold
    assert_receive {:fake_upstream_frame_barrier, ^ordinal, ^handler, ^hold}, @detection_timeout_ms
    assert ordinal == 3 + length(fixture.remaining)
    assert :ok = FakeUpstream.release_frame(fixture.upstream, hold)
    assert :ok = FakeUpstream.verify!(fixture.upstream)
    assert_receive {:DOWN, ^handler_monitor, :process, ^handler, _reason}, @detection_timeout_ms
    assert {:ok, %{generation: nil}} = UpstreamWebsocketSession.live_connection(fixture.upstream_session)
  end

  defp owner_completed_frame(response_id, usage), do: encode_owner_event(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => usage}})
  defp encode_owner_event(frame), do: CodexPooler.JSON.encode!(frame)
  defp frame_digests(frames), do: Enum.map(frames, &:crypto.hash(:sha256, &1))

  defp remove_owner_steering_probe(server) do
    :sys.remove(server, &OwnerSteeringProbe.hook/3)
  catch
    :exit, _reason -> :ok
  end

  defmodule OwnerSteeringProbe do
    @moduledoc false
    import ExUnit.Assertions

    @spec install(pid(), map()) :: :ok
    def install(server, opts), do: :sys.install(server, {&__MODULE__.hook/3, opts})

    @spec hook(map(), term(), term()) :: map() | :done
    def hook(%{kind: :hold_frame, identity: identity, ref: ref, notify: notify, producer: producer}, {:in, {:"$gen_call", {producer, _tag}, {:frame, identity, _data, _discriminator}}}, _name) do
      send(notify, {ref, :frame_held, self()})

      receive do
        {^ref, :release} -> :done
      after
        15_000 -> flunk("the queued native successor frame was not released")
      end
    end

    def hook(%{kind: :relay_reply, identity: identity, lane: lane} = probe, {:in, {:"$gen_call", {lane, _tag} = from, {:relay_steering_frame, lane, identity, _data, _discriminator}}}, _name), do: %{probe | pending_from: from}

    def hook(%{kind: :relay_reply, pending_from: from} = probe, {:out, reply, from, _state}, _name) when not is_nil(from), do: report_relay_reply(probe, reply)
    def hook(%{kind: :relay_reply, pending_from: from} = probe, {:out, reply, from}, _name) when not is_nil(from), do: report_relay_reply(probe, reply)

    def hook(%{kind: :socket_frames, identity: identity, lane: lane} = probe, {:in, {:native_response_steering_frame, lane, identity, _data}}, _name), do: %{probe | frames: probe.frames + 1}

    def hook(%{kind: :socket_frames, ref: ref, notify: notify, frames: frames}, {:in, {:owner_steering_frame_fence, ref}}, _name) do
      send(notify, {ref, :socket_frame_fence, frames})
      :done
    end

    def hook(probe, _event, _name), do: probe

    defp report_relay_reply(probe, reply) do
      send(probe.notify, {probe.ref, :relay_reply, reply})
      # The marker follows any frame sent by this same owner to this socket;
      # its observation proves no stale frame was delivered before the reply.
      send(probe.socket, {:owner_steering_frame_fence, probe.ref})
      :done
    end
  end

  defmodule RawPeer do
    @moduledoc false
    use GenServer, restart: :temporary
    import ExUnit.Assertions

    @timeout_ms 15_000

    @spec start_link(keyword()) :: GenServer.on_start()
    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @spec port(pid()) :: :inet.port_number()
    def port(pid), do: GenServer.call(pid, :port)

    @impl true
    def init(opts) do
      {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true, ip: {127, 0, 0, 1}, nodelay: true])
      {:ok, port} = :inet.port(listen)
      {:ok, %{listen: listen, socket: nil, port: port, notify: Keyword.fetch!(opts, :notify), opening: Keyword.fetch!(opts, :opening), tail: Keyword.fetch!(opts, :tail)}}
    end

    @impl true
    def handle_call(:port, _from, state) do
      send(self(), :accept)
      {:reply, state.port, state}
    end

    @impl true
    def handle_info(:accept, state) do
      assert {:ok, socket} = :gen_tcp.accept(state.listen, @timeout_ms)
      :ok = :gen_tcp.close(state.listen)
      state = %{state | socket: socket}
      :ok = upgrade(socket)
      assert {:ok, 1, create} = read_frame(socket)
      assert %{"type" => "response.create"} = CodexPooler.JSON.decode!(create)
      :ok = :gen_tcp.send(socket, Enum.map(state.opening, &server_frame/1))
      send(state.notify, {:raw_steering_open, self()})
      assert {:ok, 1, steer} = read_frame(socket)
      assert %{"type" => "response.steer"} = CodexPooler.JSON.decode!(steer)
      # Exactly one application buffer. The test separately observes that the
      # real decoder did expose successor frames behind the first terminal;
      # it never infers one TCP packet or one read solely from this send.
      buffer = state.tail |> Enum.map(&server_frame/1) |> IO.iodata_to_binary()
      :ok = :gen_tcp.send(socket, buffer)
      send(state.notify, {:raw_steering_tail_written, self(), %{generation_count: 1, steer_count: 1, tail_frame_count: length(state.tail), write_count: 1, steer_fingerprint: :crypto.hash(:sha256, steer)}})
      :ok = wait_for_close(socket)
      {:stop, :normal, state}
    end

    @impl true
    def terminate(_reason, state) do
      :gen_tcp.close(state.listen)
      if state.socket, do: :gen_tcp.close(state.socket)
      :ok
    end

    defp upgrade(socket) do
      headers = read_headers(socket, <<>>)

      key =
        headers
        |> String.split("\r\n")
        |> Enum.find_value(&websocket_header_key/1)

      assert is_binary(key)
      accept = :crypto.hash(:sha, key <> "258EAFA5-E914-47DA-95CA-C5AB0DC85B11") |> Base.encode64()
      :gen_tcp.send(socket, ["HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: ", accept, "\r\n\r\n"])
    end

    defp websocket_header_key(line) do
      case String.split(line, ":", parts: 2) do
        [name, value] -> if String.downcase(name) == "sec-websocket-key", do: String.trim(value)
        _line -> nil
      end
    end

    defp read_headers(socket, bytes) do
      if String.ends_with?(bytes, "\r\n\r\n") do
        bytes
      else
        assert byte_size(bytes) < 32_768
        assert {:ok, byte} = :gen_tcp.recv(socket, 1, @timeout_ms)
        read_headers(socket, bytes <> byte)
      end
    end

    defp read_frame(socket) do
      with {:ok, <<first, second>>} <- :gen_tcp.recv(socket, 2, @timeout_ms),
           true <- Bitwise.band(first, 0x80) == 0x80,
           true <- Bitwise.band(second, 0x80) == 0x80,
           {:ok, length} <- payload_length(socket, Bitwise.band(second, 0x7F)),
           true <- length <= 1_048_576,
           {:ok, mask} <- :gen_tcp.recv(socket, 4, @timeout_ms),
           {:ok, payload} <- payload(socket, length) do
        repeated_mask = mask |> :binary.copy(div(length + 3, 4)) |> binary_part(0, length)
        {:ok, Bitwise.band(first, 0x0F), :crypto.exor(payload, repeated_mask)}
      end
    end

    defp payload_length(_socket, length) when length < 126, do: {:ok, length}

    defp payload_length(socket, 126) do
      case :gen_tcp.recv(socket, 2, @timeout_ms) do
        {:ok, <<length::16>>} -> {:ok, length}
        error -> error
      end
    end

    defp payload_length(socket, 127) do
      case :gen_tcp.recv(socket, 8, @timeout_ms) do
        {:ok, <<length::64>>} -> {:ok, length}
        error -> error
      end
    end

    defp payload(_socket, 0), do: {:ok, ""}
    defp payload(socket, length), do: :gen_tcp.recv(socket, length, @timeout_ms)

    defp server_frame(text), do: server_frame(1, text)

    defp server_frame(opcode, payload) do
      first = Bitwise.bor(0x80, opcode)
      length = byte_size(payload)
      if length < 126, do: <<first, length, payload::binary>>, else: <<first, 126, length::16, payload::binary>>
    end

    defp wait_for_close(socket) do
      case read_frame(socket) do
        {:error, :closed} ->
          :ok

        {:ok, 8, _payload} ->
          :ok

        {:ok, 9, payload} ->
          :ok = :gen_tcp.send(socket, server_frame(10, payload))
          wait_for_close(socket)

        other ->
          flunk("raw steering peer received an unexpected extra generation/control: #{inspect(frame_shape(other))}")
      end
    end

    defp frame_shape({:ok, opcode, payload}), do: %{opcode: opcode, byte_size: byte_size(payload)}
    defp frame_shape({:error, reason}), do: {:error, reason}
    defp frame_shape(_other), do: :malformed_frame
  end

  defp downstream, do: %{pid: self(), epoch: 1, correlation_id: "synthetic-steering-downstream"}
  defp parsed_steer, do: ResponseSteer.parse(%{"type" => "response.steer", "previous_response_id" => @response_id, "input" => @input})
  defp owner_state(active_turn), do: %WebsocketOwnerSession{active_turn: active_turn, downstream: active_turn.downstream, upstream_pid: active_turn.upstream_pid}

  defp call_steer(state, downstream, steer),
    do: WebsocketOwnerSession.handle_call({:steer_turn, downstream.pid, downstream.epoch, downstream.correlation_id, steer}, {self(), make_ref()}, state)
end
