defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.SlowOwnerAdmissionTest do
  # An owner that is alive but slower than the owner call budget while a
  # native compaction asks it for an admission (findings#270 row 270-245). The
  # owner node answered every exit of an admission control as an owner already
  # gone (`owner_unavailable`), a timeout included, while a remote owner's
  # caller whose erpc deadline passed first read `owner_forward_timeout`. The
  # timeout now keeps its own cause wherever the owner runs:
  #
  #   * the socket's reservation of an incremental compaction is refused with
  #     the retryable `503 owner_unavailable` it always got, naming the timeout;
  #   * a final turn deferred behind a running turn, whose owner still does not
  #     answer at dequeue, is refused the same way on both topologies (a remote
  #     owner's stall used to run it as the ordinary turn, against that owner);
  #   * a compaction whose accounting start the owner does not answer is
  #     refused `503 owner_unavailable` with the timeout as the recorded reason
  #     (a remote owner's stall answered a non-retryable `500`).
  #
  # The same stall at a served compaction's confirmation, or at a first
  # full-history compaction's authorization or collection, no longer answers
  # `502`: the client gets the compaction the provider served and billed, and
  # the owner applies the confirmation once it answers again (findings#270 row
  # 270-249). With owner forwarding off the socket's own upstream session
  # answers those steps within a fixed one-second budget (`:direct`).
  #
  # The slow owner keeps its session, its lease and its upstream connection,
  # and serves the client's full-history resend once it answers again. The
  # real owner is suspended (`:sys.suspend/1`), or held right after an answer
  # (`OwnerCallHold`), past a one-second owner call budget, which the test sets
  # on both nodes. Owner forwarding on (off for `:direct`), native route, the
  # Pool forced to Full (a compaction needs it), the released client's
  # compaction frames, FakeUpstream. Topologies: the owner on a second VM
  # sharing the database, or on this node.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [
      await_socket_connection_state!: 2,
      hold_settled_websocket_turn!: 0,
      receive_frames_until_close!: 3,
      receive_native_terminal!: 3,
      release_settled_websocket_turn: 2,
      socket_connection_state!: 1,
      with_info_log: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession}
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionAdmission
  alias CodexPooler.Gateway.Transports.Websocket.OwnerDefaults
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerCallHold
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario

  @moduletag capture_log: true

  @owner_call_budget_ms 1_000
  @detection_timeout_ms 15_000

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup %{peer_node: peer_node} do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    enter_peer_owner_topology!()

    original_env = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(original_env, owner_call_timeout_ms: @owner_call_budget_ms))
    peer_env = :erpc.call(peer_node, Application, :get_env, [:codex_pooler, OwnerDefaults, []])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, OwnerDefaults, Keyword.merge(peer_env, owner_call_timeout_ms: @owner_call_budget_ms)])

    CodexPooler.TestAppEnv.restore_on_exit(NativeCompactionAdmission)

    # Through `:erpc` with the standard library only: this module is compiled
    # on this node alone.
    on_exit(fn ->
      if peer_node in Node.list() do
        :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, OwnerDefaults, peer_env])
        :ok = :erpc.call(peer_node, Application, :delete_env, [:codex_pooler, NativeCompactionAdmission])
      end
    end)
  end

  for topology <- [:remote, :local] do
    @tag topology: topology
    test "#{topology}: a compaction reservation the owner answers too late is refused with the timeout as its cause, and the owner keeps its session", ctx do
      compaction = open_compaction_session!(ctx)
      :ok = :sys.suspend(compaction.owner)

      {refusal, log} =
        with_info_log(fn ->
          refusal = send_frame!(compaction, compaction_frame(compaction, "compaction", trigger(compaction), "resp_slow_owner_anchor"))
          :ok = :sys.resume(compaction.owner)
          refusal
        end)

      # The retryable refusal the client always got, now naming the timeout.
      assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_unavailable"}} = refusal
      assert log =~ "native compaction refused before dispatch reason=admission_unavailable cause=owner_forward_timeout code=owner_unavailable status=503 compaction_phase=mid_turn topology=forwarded decided_at=arrival reservation_phase=compact"
      assert_owner_kept_session!(compaction, log)
      assert FakeUpstream.count(compaction.upstream) == 1
      assert_full_history_resend_served!(compaction)
    end

    @tag topology: topology
    test "#{topology}: a final turn deferred behind a running turn is refused at dequeue while its owner answers too late", ctx do
      hold = hold_settled_websocket_turn!()
      compaction = open_compaction_session!(ctx, false)
      assert_receive {^hold, :held, anchor_task}, @detection_timeout_ms
      :ok = :sys.suspend(compaction.owner)

      {refusal, log} =
        with_info_log(fn ->
          {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, final_frame(compaction))
          # The frame waits behind the anchor's task, whose settlement is held.
          _deferred = await_socket_connection_state!(compaction.client.socket, &(:queue.len(&1.queued_response_payloads) == 1))
          :ok = release_settled_websocket_turn(hold, anchor_task)
          {_conn, _websocket, refusal} = receive_native_terminal!(conn, websocket, compaction.client.ref)
          :ok = :sys.resume(compaction.owner)
          refusal
        end)

      assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_unavailable"}} = refusal
      assert log =~ "native compaction refused before dispatch reason=admission_unavailable cause=owner_forward_timeout code=owner_unavailable status=503 compaction_phase=none topology=forwarded decided_at=dequeue reservation_phase=final"
      assert_owner_kept_session!(compaction, log)
      assert FakeUpstream.count(compaction.upstream) == 1
    end

    @tag topology: topology
    test "#{topology}: a compaction whose accounting start the owner answers too late is refused 503 with the timeout recorded", ctx do
      compaction = open_compaction_session!(ctx)
      registry = GenServer.whereis(DeferredStreamRegistry)

      {refusal, log} =
        with_info_log(fn ->
          # The compaction's task waits at the admission checkpoint right
          # before its accounting starts; the owner is then too slow for it.
          :ok = :sys.suspend(registry)
          {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, compaction_frame(compaction, "compaction", trigger(compaction), "resp_slow_owner_anchor"))
          await_mailbox!(registry, 1, System.monotonic_time(:millisecond) + @detection_timeout_ms)
          :ok = :sys.suspend(compaction.owner)
          :ok = :sys.resume(registry)
          {_conn, _websocket, refusal} = receive_native_terminal!(conn, websocket, compaction.client.ref)
          :ok = :sys.resume(compaction.owner)
          refusal
        end)

      assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_unavailable"}} = refusal
      assert_owner_kept_session!(compaction, log)

      assert [_anchor, rejected] = Repo.all(from(r in Request, where: r.pool_id == ^compaction.setup.pool.id, order_by: r.admitted_at))
      assert {rejected.status, rejected.last_error_code, rejected.response_status_code} == {"rejected", "owner_unavailable", 503}

      assert %{"denial_family" => "session_owner_lease", "internal_reason" => "owner_forward_timeout", "failure_phase" => "reservation", "operator_action" => action} =
               rejected.request_metadata["continuity_denial"]

      assert action =~ "did not answer within its call budget"
      assert FakeUpstream.count(compaction.upstream) == 1
    end
  end

  # A compaction the provider served and billed and the collector validated
  # reaches its client when its owner does not answer the confirmation within
  # its call budget (findings#270 row 270-249): the owner applies the
  # confirmation once it answers again, which arms the final against the item
  # the client received. It used to answer `502 invalid_compaction_response`:
  # the released client resent the compaction with its full history on the
  # same connection, and the late confirmation's `pending_final` refused that
  # resend's first-compact authorization after the provider had served and
  # billed it, on every retry. An owner gone between the collection and the
  # confirmation is answered the same way
  # (`RequestOptions.compact_confirmation_outcome/1`), though the socket's own
  # handling of that owner's exit usually ends the turn first.
  for topology <- [:remote, :local, :direct] do
    @tag topology: topology
    test "#{topology}: a compaction whose confirmation its owner answers too late reaches its client, and the late confirmation arms its final", ctx do
      compaction = open_compaction_session!(ctx)
      hold = hold_settled_websocket_turn!()

      {{compaction, served}, log} =
        with_info_log(fn ->
          {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, compaction_frame(compaction, "compaction", trigger(compaction), "resp_slow_owner_anchor"))
          # The provider served the compaction and its request settled; its
          # confirmation then meets an owner too slow for it.
          assert_receive {^hold, :held, task}, @detection_timeout_ms
          :ok = :sys.suspend(compaction.owner)
          :ok = release_settled_websocket_turn(hold, task)
          {conn, websocket, served} = receive_native_terminal!(conn, websocket, compaction.client.ref)
          :ok = :sys.resume(compaction.owner)
          {with_client(compaction, conn, websocket), served}
        end)

      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_compact", "output" => [item]}} = served
      assert item == compaction.item
      reason = if ctx.topology == :direct, do: "timeout", else: "owner_forward_timeout"
      assert log =~ "native compaction answered without owner confirmation step=confirm reason=#{reason} confirmation=unknown compaction_input_mode=incremental serving_mode=full"
      refute log =~ "invalid_compaction_response"

      # Answering again, the owner applied the confirmation: the final is
      # reserved against it, and its own success arms the next compaction.
      assert admission_phase(compaction.owner) == :pending_final
      final = send_frame!(compaction, final_after_compaction_frame(compaction, item))
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final"}} = final
      :ok = await_admission_phase!(compaction.owner, :pending_compact)
      assert Scenario.settled_statuses!(compaction.setup, 3) == ["succeeded", "succeeded", "succeeded"]
      assert FakeUpstream.count(compaction.upstream) == 3
      if ctx.topology != :direct, do: assert_owner_kept_session!(compaction, log)
    end
  end

  # A first full-history compaction is authorized after the provider served it
  # (authorization, then its collection, then its acknowledgement). An
  # authorization the owner answers too late reaches the client the same way;
  # the owner applies it once it answers again, and the final runs as an
  # ordinary turn. A collection the owner answers too late is still followed
  # by its acknowledgement, which the owner handles right after it: the final
  # is then reserved against the item the client received, instead of the
  # admission staying `collected_unconfirmed` with nothing to confirm it.
  for topology <- [:remote, :local, :direct], step <- [:authorize, :collect] do
    @tag topology: topology, step: step
    test "#{topology}: a first full-history compaction whose #{step} step its owner answers too late reaches its client", ctx do
      compaction = open_compaction_session!(ctx)
      hold = hold_settled_websocket_turn!()

      {{compaction, served}, log} =
        with_info_log(fn ->
          {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, compaction_frame(compaction, "compaction", compaction.history ++ trigger(compaction), nil))
          assert_receive {^hold, :held, task}, @detection_timeout_ms
          owner_hold = slow_owner_at!(compaction, ctx.step)
          :ok = release_settled_websocket_turn(hold, task)
          {conn, websocket, served} = receive_native_terminal!(conn, websocket, compaction.client.ref)
          :ok = release_slow_owner!(compaction, owner_hold)
          {with_client(compaction, conn, websocket), served}
        end)

      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_compact", "output" => [item]}} = served
      reason = if ctx.topology == :direct, do: "timeout", else: "owner_forward_timeout"
      logged_step = if ctx.step == :authorize, do: "authorize", else: "confirm"
      assert log =~ "native compaction answered without owner confirmation step=#{logged_step} reason=#{reason} confirmation=unknown compaction_input_mode=full_history serving_mode=full"
      refute log =~ "invalid_compaction_response"

      # Answering again, the owner applied what it was sent, in order: the
      # authorization alone, or the collection and its acknowledgement, which
      # arm the final.
      :ok = await_admission_phase!(compaction.owner, if(ctx.step == :authorize, do: :ordinary_success, else: :pending_final))
      final = send_frame!(compaction, final_after_compaction_frame(compaction, item))
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final"}} = final
      :ok = await_admission_phase!(compaction.owner, :pending_compact)
      assert Scenario.settled_statuses!(compaction.setup, 3) == ["succeeded", "succeeded", "succeeded"]
      assert FakeUpstream.count(compaction.upstream) == 3
      if ctx.topology != :direct, do: assert_owner_kept_session!(compaction, log)
    end
  end

  for topology <- [:remote, :local] do
    # A confirmation its owner answered with a refusal of the admission's own
    # still answers `502 invalid_compaction_response`: here a second socket of
    # the client took the owner's downstream over between the collection and
    # the confirmation, so the owner refuses it `stale_downstream`.
    @tag topology: topology
    test "#{topology}: a compaction whose confirmation its owner refuses still answers 502", ctx do
      compaction = open_compaction_session!(ctx)
      hold = hold_settled_websocket_turn!()

      {refusal, log} =
        with_info_log(fn ->
          {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, compaction_frame(compaction, "compaction", trigger(compaction), "resp_slow_owner_anchor"))
          assert_receive {^hold, :held, task}, @detection_timeout_ms
          second = Scenario.connect!(compaction.port, compaction.setup, Scenario.native_route(), compaction.window)
          _attached = await_socket_connection_state!(second.socket, &(Map.get(&1, :websocket_owner_pid) == compaction.owner))
          :ok = release_settled_websocket_turn(hold, task)
          {_conn, _websocket, refusal} = receive_native_terminal!(conn, websocket, compaction.client.ref)
          Scenario.close!(second)
          refusal
        end)

      assert %{"type" => "error", "status" => 502, "error" => %{"code" => "invalid_compaction_response"}} = refusal
      refute log =~ "native compaction answered without owner confirmation"
      assert FakeUpstream.count(compaction.upstream) == 2
    end
  end

  # An acknowledgement lost with its caller no longer wedges the admission
  # while the socket stays attached (findings#270 row 270-249): the owner
  # collected the compaction and its task died before confirming it. The
  # collection is bound like the armed phases and ends past its bound, so the
  # client's full-history resend on the same socket is authorized, served and
  # confirmed, and its final is reserved against it. It used to stay
  # `collected_unconfirmed` for as long as the socket was attached, and the
  # resend was served, billed and refused `502 invalid_compaction_response`.
  # The bound is shortened for the lost compaction only, on the owner's node.
  for topology <- [:remote, :local, :direct] do
    @tag topology: topology
    test "#{topology}: a compaction whose acknowledgement is lost does not hold the admission past its bound", ctx do
      compaction = open_compaction_session!(ctx, true, [:anchor, :compaction, :compaction, :final])
      hold = hold_settled_websocket_turn!()
      put_reservation_ttl!(ctx, 1)
      {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, compaction_frame(compaction, "compaction", trigger(compaction), "resp_slow_owner_anchor"))
      compaction = %{compaction | client: %{compaction.client | conn: conn, websocket: websocket}}
      assert_receive {^hold, :held, task}, @detection_timeout_ms

      # The owner collected the compaction; its task dies before the
      # acknowledgement, and the collection passes its one-millisecond bound.
      :ok = await_admission_phase!(compaction.owner, :collected_unconfirmed)
      Process.exit(task, :kill)
      put_reservation_ttl!(ctx, nil)
      _idle = await_socket_connection_state!(compaction.client.socket, &(MapSet.size(&1.tasks) == 0))

      {served, log} = with_info_log(fn -> send_frame!(compaction, compaction_frame(compaction, "compaction", compaction.history ++ trigger(compaction), nil)) end)
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_compact", "output" => [item]}} = served
      refute log =~ "invalid_compaction_response"
      :ok = await_admission_phase!(compaction.owner, :pending_final)

      final = send_frame!(compaction, final_after_compaction_frame(compaction, item))
      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final"}} = final
      :ok = await_admission_phase!(compaction.owner, :pending_compact)
      assert Scenario.settled_statuses!(compaction.setup, 4) == ["succeeded", "succeeded", "succeeded", "succeeded"]
      assert FakeUpstream.count(compaction.upstream) == 4
    end
  end

  # An ordinary turn whose owner does not answer its preflight within the
  # owner call budget is refused with the timeout's own retryable `504
  # owner_forward_timeout`, whichever node the owner runs on, and the socket
  # stays open for the client's resend (findings#270 row 270-250). A remote
  # owner's stall read `502 owner_crashed` (the owner call timed out inside the
  # erpc worker, whose exit fell to the crash default), and a local owner's
  # closed the socket `1011 websocket control unavailable` (the exit reached
  # the socket's control path).
  for topology <- [:remote, :local] do
    @tag topology: topology
    test "#{topology}: an ordinary turn whose owner answers its preflight too late is refused 504 with the timeout, and the socket serves the resend", ctx do
      compaction = open_compaction_session!(ctx, true, [:anchor, :final])
      frame = compaction_frame(compaction, "turn", compaction.history ++ [%{"type" => "message", "role" => "user", "content" => "synthetic next turn"}], nil)
      :ok = :sys.suspend(compaction.owner)

      {refusal, log} =
        with_info_log(fn ->
          refusal = send_frame!(compaction, frame)
          :ok = :sys.resume(compaction.owner)
          refusal
        end)

      assert %{"type" => "error", "status" => 504, "error" => %{"code" => "owner_forward_timeout"}} = refusal
      assert log =~ "phase=handoff reason_class=owner_forward_timeout reason_code=owner_forward_timeout"
      refute log =~ "owner_crashed"
      refute log =~ "websocket control path failed"
      assert_owner_kept_session!(compaction, log)

      assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final"}} = send_frame!(compaction, frame)
      assert Scenario.settled_statuses!(compaction.setup, 2) == ["succeeded", "succeeded"]
      assert FakeUpstream.count(compaction.upstream) == 2
    end
  end

  # A second socket on the session's window whose first request its remote
  # owner does not answer within the owner call budget gives up at its attach
  # and closes `1011`, while the attach still waits in the owner's mailbox.
  # The owner took it once it answered again and made the socket that gave up
  # its downstream in place of the live one, whose next turn met `409
  # stale_owner` (findings#270 row 270-248). The socket now abandons the attach
  # on the owner's node before it gives up, and the owner refuses it: the live
  # socket's next turn is served. A local owner's attach timeout is findings#270
  # row 270-284.
  @tag topology: :remote
  test "remote: a second socket's attach its owner answers too late leaves the live socket attached", ctx do
    compaction = open_compaction_session!(ctx, true, [:anchor, :final])
    :ok = :sys.suspend(compaction.owner)

    {closed, log} =
      with_info_log(fn ->
        second = Scenario.connect!(compaction.port, compaction.setup, Scenario.native_route(), compaction.window)
        {conn, websocket} = public_websocket_send_text!(second.conn, second.websocket, second.ref, next_turn_frame(compaction, "first turn of a second socket"))
        {_conn, _websocket, closed} = receive_frames_until_close!(conn, websocket, second.ref)
        :ok = :sys.resume(compaction.owner)
        closed
      end)

    assert closed == [{:close, 1011, "websocket owner forwarding timed out"}]
    assert log =~ "phase=init reason_class=owner_forward_timeout"
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final"}} = send_frame!(compaction, next_turn_frame(compaction, "next turn of the live socket"))
    assert_owner_kept_session!(compaction, log)
    assert Scenario.settled_statuses!(compaction.setup, 2) == ["succeeded", "succeeded"]
    assert FakeUpstream.count(compaction.upstream) == 2
  end

  # The same late attach took a turn the live socket was still receiving
  # after its first output (findings#270 row 270-285): the owner handed the
  # turn to the socket that gave up, whose exit then left it without a
  # downstream, so the live socket never received the rest of it while its
  # request settled succeeded. The refused attach leaves the turn with the live
  # socket, which receives its terminal. An owner that takes the attach after
  # the socket's timeout but before the abandon's record, a window of about
  # one erpc round trip, still hands it the turn (a decision).
  @tag topology: :remote
  test "remote: a second socket's attach its owner answers too late leaves the live socket its running turn", ctx do
    ref = make_ref()
    compaction = open_compaction_session!(ctx, true, [:anchor, {:held, self(), ref}])
    client = compaction.client
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, next_turn_frame(compaction, "turn the live socket is receiving"))
    {conn, websocket} = receive_first_output!(compaction, ref, conn, websocket)
    :ok = :sys.suspend(compaction.owner)

    {closed, log} =
      with_info_log(fn ->
        second = Scenario.connect!(compaction.port, compaction.setup, Scenario.native_route(), compaction.window)
        {conn, websocket} = public_websocket_send_text!(second.conn, second.websocket, second.ref, next_turn_frame(compaction, "first turn of a second socket"))
        {_conn, _websocket, closed} = receive_frames_until_close!(conn, websocket, second.ref)
        :ok = :sys.resume(compaction.owner)
        closed
      end)

    assert closed == [{:close, 1011, "websocket owner forwarding timed out"}]
    assert log =~ "phase=init reason_class=owner_forward_timeout"
    :ok = FakeUpstream.release_remaining_frames(compaction.upstream, ref)
    assert {_conn, _websocket, %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final"}}} = receive_native_terminal!(conn, websocket, client.ref)
    assert_receive {:fake_upstream_frame_barrier, 3, _handler, ^ref}, @detection_timeout_ms
    assert_owner_kept_session!(compaction, log)
    assert Scenario.settled_statuses!(compaction.setup, 2) == ["succeeded", "succeeded"]
    assert FakeUpstream.count(compaction.upstream) == 2
  end

  # A socket on the session's window with its owner (on the peer for
  # `:remote`) and one anchor turn it served, settled unless `settle?` is
  # false (its task is then held after its settlement).
  defp open_compaction_session!(ctx, settle? \\ true, answers \\ [:anchor, :compaction, :final]) do
    item = %{"type" => "compaction", "encrypted_content" => "synthetic-slow-owner-#{ctx.topology}"}

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a native compaction on a session whose owner is suspended past the owner call budget)
        FakeUpstream.repeat_last(Enum.map(answers, &upstream_answer(&1, item)))
      )

    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: "full"})
    if ctx.topology == :direct, do: Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    window = Scenario.window()
    if ctx.topology == :remote, do: start_shared_peer_window_owner!(setup, window.id, ctx.peer_node)
    {_server, port} = start_public_endpoint_with_server!()
    client = Scenario.connect!(port, setup, Scenario.native_route(), window)

    compaction = %{
      setup: setup,
      window: window,
      port: port,
      upstream: upstream,
      item: item,
      turn_id: "slow-owner-#{ctx.topology}",
      history: [%{"type" => "message", "role" => "user", "content" => "synthetic compaction anchor"}]
    }

    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, compaction_frame(compaction, "turn", compaction.history, nil))
    {conn, websocket, anchor} = receive_native_terminal!(conn, websocket, client.ref)
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_anchor"}} = anchor
    client = %{client | conn: conn, websocket: websocket}
    client = if settle?, do: Scenario.settle!(client), else: client
    state = socket_connection_state!(client.socket)
    owner = if ctx.topology == :direct, do: state.upstream_websocket_session, else: state.websocket_owner_pid
    assert node(owner) == if(ctx.topology == :remote, do: ctx.peer_node, else: node())
    Map.merge(compaction, %{client: client, owner: owner, session: Repo.get!(CodexSession, state.codex_session.id)})
  end

  # The owner does not answer the compaction's `step` within its call budget:
  # suspended before the authorization, or held right after answering it, so
  # the collection that follows meets it.
  defp slow_owner_at!(compaction, :authorize) do
    :ok = :sys.suspend(compaction.owner)
    :suspended
  end

  defp slow_owner_at!(compaction, :collect) do
    ref = make_ref()
    owner = compaction.owner

    :ok =
      if node(owner) == node(),
        do: OwnerCallHold.install(owner, ref, self()),
        else: :erpc.call(node(owner), OwnerCallHold, :install, [owner, ref, self()])

    ref
  end

  defp release_slow_owner!(compaction, :suspended), do: :sys.resume(compaction.owner)

  defp release_slow_owner!(compaction, ref) when is_reference(ref) do
    owner = compaction.owner
    assert_receive {^ref, :held, ^owner}, @detection_timeout_ms
    send(owner, {ref, :release})
    :ok
  end

  defp await_admission_phase!(owner, phase, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    cond do
      admission_phase(owner) == phase ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the owner's compaction admission never reached #{phase}: #{inspect(admission_phase(owner))}")

      true ->
        receive do
        after
          1 -> await_admission_phase!(owner, phase, deadline)
        end
    end
  end

  # The live socket's turn shows its first output: the provider's
  # `response.created` and a text delta reach the client, and the provider
  # holds the rest of the turn.
  defp receive_first_output!(compaction, ref, conn, websocket) do
    for barrier <- 0..1 do
      assert_receive {:fake_upstream_frame_barrier, ^barrier, _handler, ^ref}, @detection_timeout_ms
      :ok = FakeUpstream.release_frame(compaction.upstream, ref)
    end

    assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^ref}, @detection_timeout_ms
    {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, compaction.client.ref)
    assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(created)
    {conn, websocket, delta} = public_websocket_receive_text!(conn, websocket, compaction.client.ref)
    assert %{"type" => "response.output_text.delta"} = CodexPooler.JSON.decode!(delta)
    {conn, websocket}
  end

  defp with_client(compaction, conn, websocket), do: %{compaction | client: Scenario.settle!(%{compaction.client | conn: conn, websocket: websocket})}

  # The owner's compaction admission phase (the socket's own upstream session's
  # with owner forwarding off), read on its node.
  defp admission_phase(owner) do
    state = if node(owner) == node(), do: :sys.get_state(owner), else: :erpc.call(node(owner), :sys, :get_state, [owner])

    case Map.get(state, :native_compaction_admission) do
      %{phase: phase} -> phase
      nil -> nil
    end
  end

  defp send_frame!(compaction, frame) do
    {conn, websocket} = public_websocket_send_text!(compaction.client.conn, compaction.client.websocket, compaction.client.ref, frame)
    {_conn, _websocket, terminal} = receive_native_terminal!(conn, websocket, compaction.client.ref)
    terminal
  end

  # The slow owner keeps the session: the same process registered on its
  # node, the same lease, and nobody replaced it or took it over.
  defp assert_owner_kept_session!(compaction, log) do
    refute log =~ "websocket owner stale replaced"
    refute log =~ "websocket owner takeover"
    assert owner_lookup(compaction.owner, compaction.session.id) == {:ok, compaction.owner}
    session = Repo.get!(CodexSession, compaction.session.id)
    assert {session.owner_instance_id, session.owner_lease_token} == {compaction.session.owner_instance_id, compaction.session.owner_lease_token}
    lease_token = session.owner_lease_token
    assert [%BridgeOwnerLease{status: "active", lease_token: ^lease_token}] = Repo.all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id))
  end

  # The released client's full-history resend of the compaction, on a new
  # socket, is served by the same owner.
  defp assert_full_history_resend_served!(compaction) do
    Scenario.close!(compaction.client)
    retry = Scenario.connect!(compaction.port, compaction.setup, Scenario.native_route(), compaction.window)
    {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, compaction_frame(compaction, "compaction", compaction.history ++ trigger(compaction), nil))
    {conn, websocket, served} = receive_native_terminal!(conn, websocket, retry.ref)
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_compact"}} = served
    assert socket_connection_state!(retry.socket).websocket_owner_pid == compaction.owner
    Scenario.close!(%{retry | conn: conn, websocket: websocket})
    assert Scenario.settled_statuses!(compaction.setup, 2) == ["succeeded", "succeeded"]
    assert FakeUpstream.count(compaction.upstream) == 2
  end

  defp owner_lookup(owner, session_id) when node(owner) == node(), do: WebsocketOwnerSession.lookup(session_id)
  defp owner_lookup(owner, session_id), do: :erpc.call(node(owner), WebsocketOwnerSession, :lookup, [session_id])

  defp await_mailbox!(pid, count, deadline) do
    {:message_queue_len, queued} = Process.info(pid, :message_queue_len)

    cond do
      queued >= count ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the compaction's task never reached the admission checkpoint")

      true ->
        receive do
        after
          1 -> await_mailbox!(pid, count, deadline)
        end
    end
  end

  defp next_turn_frame(compaction, content),
    do: compaction_frame(compaction, "turn", compaction.history ++ [%{"type" => "message", "role" => "user", "content" => content}], nil)

  defp trigger(compaction),
    do: [%{"type" => "custom_tool_call_output", "call_id" => "call_#{compaction.turn_id}", "output" => "synthetic tool output"}, %{"type" => "compaction_trigger"}]

  # The turn that continues on the compacted history: its input starts with
  # the compaction item.
  defp final_frame(compaction),
    do: compaction_frame(compaction, "turn", [compaction.item, %{"type" => "message", "role" => "user", "content" => "synthetic turn after the compaction"}], nil)

  defp anchor_event,
    do: %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_anchor", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1_200, "output_tokens" => 9, "total_tokens" => 1_209}}}

  defp upstream_answer(:anchor, _item), do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(anchor_event())])
  defp upstream_answer(:compaction, item), do: FakeUpstream.websocket_text_frames(Enum.map(compaction_events(item), &CodexPooler.JSON.encode!/1))
  defp upstream_answer(:final, _item), do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(final_event())])

  # A turn whose frames the provider sends one at a time, each after the test
  # releases its barrier (`0` to `2`); barrier `3` follows the last frame.
  defp upstream_answer({:held, notify, ref}, _item) do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_slow_owner_final", "status" => "in_progress", "output" => []}}
    delta = %{"type" => "response.output_text.delta", "item_id" => "msg_slow_owner", "output_index" => 0, "content_index" => 0, "delta" => "partial"}
    FakeUpstream.barrier_websocket_frames(Enum.map([created, delta, final_event()], &CodexPooler.JSON.encode!/1), notify: notify, release_ref: ref)
  end

  # The admission bound (`NativeCompactionAdmission.reservation_ttl_ms/0`) on
  # the node that collects the compaction: the peer's owner, this node's owner,
  # or this socket's own upstream session; `nil` restores the default.
  defp put_reservation_ttl!(%{topology: :remote, peer_node: peer_node}, ttl_ms), do: :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, NativeCompactionAdmission, reservation_ttl_env(ttl_ms)])
  defp put_reservation_ttl!(_ctx, ttl_ms), do: Application.put_env(:codex_pooler, NativeCompactionAdmission, reservation_ttl_env(ttl_ms))

  defp reservation_ttl_env(nil), do: []
  defp reservation_ttl_env(ttl_ms), do: [reservation_ttl_ms: ttl_ms]

  defp final_event,
    do: %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_final", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 900, "output_tokens" => 7, "total_tokens" => 907}}}

  # The turn the released client sends after a compaction it received: the
  # compaction item first, on the next window and a new context window.
  defp final_after_compaction_frame(compaction, item) do
    metadata = %{"turn_id" => compaction.turn_id, "window_id" => "#{compaction.window.thread}:1", "context_window_id" => "00000000-0000-4000-8000-000000000246", "window_number" => 2, "request_kind" => "turn"}
    input = [item, %{"type" => "message", "role" => "user", "content" => "synthetic turn after the compaction"}]

    CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => compaction.setup.model.exposed_model_id, "input" => input, "stream" => true, "generate" => true, "client_metadata" => %{"turn_id" => compaction.turn_id, "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}})
  end

  defp compaction_events(item) do
    [
      %{"type" => "response.output_item.done", "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_compact", "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 3_000, "output_tokens" => 40, "total_tokens" => 3_040}}}
    ]
  end

  # The released client's mid-turn frames: turn metadata naming the turn, its
  # window and the request kind; a compaction's also names its compaction.
  defp compaction_frame(compaction, request_kind, input, anchor) do
    metadata =
      %{"turn_id" => compaction.turn_id, "window_id" => compaction.window.id, "context_window_id" => "00000000-0000-4000-8000-000000000245", "window_number" => 1, "request_kind" => request_kind}
      |> then(&if(request_kind == "compaction", do: Map.put(&1, "compaction", %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "mid_turn", "strategy" => "memento"}), else: &1))

    %{"type" => "response.create", "model" => compaction.setup.model.exposed_model_id, "input" => input, "stream" => true, "generate" => true, "client_metadata" => %{"turn_id" => compaction.turn_id, "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}}
    |> then(&if(anchor, do: Map.put(&1, "previous_response_id", anchor), else: &1))
    |> CodexPooler.JSON.encode!()
  end
end
