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
  # The slow owner keeps its session, its lease and its upstream connection,
  # and serves the client's full-history resend once it answers again. The
  # real owner is suspended (`:sys.suspend/1`) past a one-second owner call
  # budget, which the test sets on both nodes. Owner forwarding on, native
  # route, the Pool forced to Full (a compaction needs it), the released
  # client's compaction frames, FakeUpstream. Topologies: the owner on a second
  # VM sharing the database, or on this node.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [
      await_socket_connection_state!: 2,
      hold_settled_websocket_turn!: 0,
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
  alias CodexPooler.Gateway.Transports.Websocket.OwnerDefaults
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
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

    # Through `:erpc` with the standard library only: this module is compiled
    # on this node alone.
    on_exit(fn ->
      if peer_node in Node.list(), do: :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, OwnerDefaults, peer_env])
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

  # A socket on the session's window with its owner (on the peer for
  # `:remote`) and one anchor turn it served, settled unless `settle?` is
  # false (its task is then held after its settlement).
  defp open_compaction_session!(ctx, settle? \\ true) do
    item = %{"type" => "compaction", "encrypted_content" => "synthetic-slow-owner-#{ctx.topology}"}

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a native compaction on a session whose owner is suspended past the owner call budget)
        FakeUpstream.repeat_last([
          FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(anchor_event())]),
          FakeUpstream.websocket_text_frames(Enum.map(compaction_events(item), &CodexPooler.JSON.encode!/1))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: "full"})
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
    owner = state.websocket_owner_pid
    assert node(owner) == if(ctx.topology == :remote, do: ctx.peer_node, else: node())
    Map.merge(compaction, %{client: client, owner: owner, session: Repo.get!(CodexSession, state.codex_session.id)})
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

  defp trigger(compaction),
    do: [%{"type" => "custom_tool_call_output", "call_id" => "call_#{compaction.turn_id}", "output" => "synthetic tool output"}, %{"type" => "compaction_trigger"}]

  # The turn that continues on the compacted history: its input starts with
  # the compaction item.
  defp final_frame(compaction),
    do: compaction_frame(compaction, "turn", [compaction.item, %{"type" => "message", "role" => "user", "content" => "synthetic turn after the compaction"}], nil)

  defp anchor_event,
    do: %{"type" => "response.completed", "response" => %{"id" => "resp_slow_owner_anchor", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1_200, "output_tokens" => 9, "total_tokens" => 1_209}}}

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
