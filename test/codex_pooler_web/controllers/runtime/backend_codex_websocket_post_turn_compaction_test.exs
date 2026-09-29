defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketPostTurnCompactionTest do
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import Ecto.Query
  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [
      receive_frames_until_close!: 3,
      hold_settled_websocket_turn!: 0,
      release_settled_websocket_turn: 2,
      set_model_serving_mode!: 3,
      model_serving_scope: 0,
      kept_open_line: 5,
      with_info_log: 1,
      native_previous_response_retry_event: 0
    ]

  import CodexPooler.AccountingTestSupport, only: [key_usage_events: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionAdmission
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket

  # The released Codex client (openai/codex#46541) runs an opt-in compaction right
  # after the final answer when `model_post_turn_compact_threshold_percent` is
  # reached: on the same socket and under the same turn id as the turn it
  # follows, a `response.create` anchored on that turn's response with
  # `[compaction_trigger]` as its only input and turn metadata
  # `request_kind: compaction`, `phase: post_turn`. The frames below keep the
  # key sets and enum values the released binary sent to a loopback capture
  # server (both samples identical); identifiers and prompt text are synthetic.
  @session_id "019a0000-0000-7000-8000-00000000a001"
  @thread_id "019a0000-0000-7000-8000-00000000a002"
  @installation_id "00000000-0000-4000-8000-00000000a003"
  @context_window_id "00000000-0000-4000-8000-00000000a004"
  @turn_id "019a0000-0000-7000-8000-00000000a005"
  @window_id "#{@thread_id}:0"
  @anchor_response_id "resp_post_turn_anchor_000001"
  @compact_response_id "resp_post_turn_compact_000001"
  @resumed_turn_id "019a0000-0000-7000-8000-00000000a006"
  @resumed_window_id "#{@thread_id}:1"
  @resumed_response_id "resp_post_turn_resumed_00001"
  @compact_usage %{"input_tokens" => 3_000, "output_tokens" => 40, "total_tokens" => 3_040}
  @resumed_usage %{"input_tokens" => 500, "output_tokens" => 3, "total_tokens" => 503}
  @lifecycle_event [:codex_pooler, :gateway, :native_compaction, :lifecycle]
  @anchor_usage %{"input_tokens" => 1_200, "output_tokens" => 9, "total_tokens" => 1_209}
  @socket_messages [
    :codex_response_chunk,
    :websocket_owner_frame,
    :websocket_owner_output_commit_probe,
    :websocket_owner_cleanup_witness,
    :websocket_response_activity,
    :codex_response_done,
    :websocket_response_delivery_complete,
    :direct_request_cleanup,
    :upstream_websocket_connection_closed,
    :websocket_owner_upstream_closed
  ]

  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} socket admits the released client's post_turn compaction on the completed turn's connection",
         %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-#{topology}"}

      upstream =
        start_upstream(
          # Strict finite scenario: the ordinary turn and its post-turn compact
          # are the only sends, both on the first physical connection; the
          # compact keeps the turn's response as its anchor.
          # provenance: observed released-binary frame shape; reply frames synthetic
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [
                valid: true,
                equals: %{"type" => "response.create"},
                forbidden: ["previous_response_id"]
              ],
              respond: completed_frames(@anchor_response_id)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [
                valid: true,
                equals: %{
                  "type" => "response.create",
                  "previous_response_id" => @anchor_response_id,
                  "input.0.type" => "compaction_trigger"
                }
              ],
              respond: compaction_frames(compact_item)
            )
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      port = start_public_endpoint!()

      {conn, websocket, ref, _headers} =
        public_websocket_connect_with_request_headers!(
          port,
          setup,
          "post-turn-#{topology}",
          "/backend-api/codex/responses",
          [{"session-id", @session_id}, {"thread-id", @thread_id}, {"x-client-request-id", @thread_id}]
        )

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, turn_frame(setup))
        {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, ref)
        {conn, websocket, completed} = public_websocket_receive_text!(conn, websocket, ref)

        assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(created)

        assert %{"type" => "response.completed", "response" => %{"id" => @anchor_response_id}} =
                 CodexPooler.JSON.decode!(completed)

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, post_turn_frame(setup))
        {conn, websocket, done} = public_websocket_receive_text!(conn, websocket, ref)
        assert CodexPooler.JSON.decode!(done) == %{"type" => "response.output_item.done", "item" => compact_item}
        {_conn, _websocket, terminal} = public_websocket_receive_text!(conn, websocket, ref)

        assert %{"type" => "response.completed", "response" => %{"status" => "completed", "output" => [^compact_item]}} =
                 CodexPooler.JSON.decode!(terminal)

        assert [turn_request, compact_request] = FakeUpstream.requests(upstream)
        assert compact_request.websocket_connection_id == turn_request.websocket_connection_id
        assert compact_request.json["input"] == [%{"type" => "compaction_trigger"}]

        compact_row =
          Repo.one!(
            from(request in Request,
              where: request.pool_id == ^setup.pool.id and request.endpoint == "/backend-api/codex/responses/compact"
            )
          )

        assert compact_row.status == "succeeded"
        assert compact_row.transport == "websocket"
        assert [compact_attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^compact_row.id))
        assert compact_attempt.status == "succeeded"
        assert compact_attempt.pool_upstream_assignment_id == setup.assignment.id

        assert Repo.aggregate(
                 from(entry in LedgerEntry, where: entry.request_id == ^compact_row.id and entry.entry_kind == "settlement"),
                 :count
               ) == 1

        assert [%CodexTurn{status: "succeeded"}] = Repo.all(from(turn in CodexTurn, where: turn.request_id == ^compact_row.id))
        assert :ok = FakeUpstream.verify!(upstream)
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  # `codex exec resume --last` after a post-turn compaction: the first socket is
  # gone (the client exited after the compaction), a new socket on the same
  # thread with the rotated window prewarms, then sends the next turn carrying
  # the compaction item. The owner left `pending_final` behind on detach; the
  # resumed turn must be admitted on a new upstream connection with the
  # compaction item and without the old anchor (findings#258 row 258-26; the
  # released-client run on one node with owner forwarding was the only
  # evidence). The resumed frames keep the key sets the released client
  # binary sent; through the Pooler its upstream frame carried no
  # previous_response_id, as here.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} post-turn compaction, client exit and resume on a new socket admits the compacted turn", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-resume-#{topology}"}

      upstream =
        start_upstream(
          # provenance: observed released-binary exec + resume frame shapes; reply frames synthetic
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: completed_frames(@anchor_response_id)),
            FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create", "input.0.type" => "compaction_trigger"}], respond: compaction_frames(compact_item)),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 2,
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_frames(@resumed_response_id)
            )
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      port = start_public_endpoint!()
      {first_conn, _websocket, _ref} = post_turn_compacted_socket!(port, setup, "post-turn-resume-#{topology}")
      Mint.HTTP.close(first_conn)

      {conn, websocket, ref, _headers} =
        public_websocket_connect_with_request_headers!(
          port,
          setup,
          "post-turn-resume-second-#{topology}",
          "/backend-api/codex/responses",
          [{"session-id", @session_id}, {"thread-id", @thread_id}, {"x-client-request-id", @thread_id}, {"x-codex-window-id", @resumed_window_id}]
        )

      try do
        # Each frame is asserted as it arrives: a refusal is a terminal, so
        # waiting for a second frame first would turn it into a receive timeout
        # that hides its code (Drone 1525).
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, resume_prewarm_frame(setup))
        {conn, websocket, prewarm_created} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(prewarm_created)
        {conn, websocket, prewarm_completed} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.completed"} = CodexPooler.JSON.decode!(prewarm_completed)

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, resumed_turn_frame(setup, compact_item))
        {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(created)
        {_conn, _websocket, completed} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => @resumed_response_id}} = CodexPooler.JSON.decode!(completed)

        assert [first_turn, compact, resumed] = FakeUpstream.requests(upstream)
        assert compact.websocket_connection_id == first_turn.websocket_connection_id
        assert resumed.websocket_connection_id != first_turn.websocket_connection_id
        assert compact_item in resumed.json["input"]

        assert await_settled_rows!(setup) == [
                 {"/backend-api/codex/responses", "websocket", "succeeded"},
                 {"/backend-api/codex/responses/compact", "websocket", "succeeded"},
                 {"/backend-api/codex/responses", "websocket", "succeeded"}
               ]

        assert :ok = FakeUpstream.verify!(upstream)
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  # Drone 1525: the resumed turn above can reach the new socket while the
  # prewarm's response task is still tracked (it pushed its terminal and has not
  # reported its result yet). The turn carries a compaction item (native phase
  # `final`) and the new connection holds no admission, so the socket defers the
  # reservation behind that task. Once the task reports, the reservation still
  # finds no admission; the turn must then run as the ordinary turn it runs when
  # no task is tracked, not be refused `503 owner_unavailable`. The prewarm's
  # result is left in the mailbox until the resumed turn has been handed to the
  # socket, which is the interleaving the loaded CI run hit.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} resumed compacted turn sent before the prewarm's task reported runs as the ordinary turn", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-held-prewarm-#{topology}"}

      upstream =
        start_upstream(
          # provenance: observed released-binary resume frame shapes; reply frames synthetic
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "input.1.type" => "compaction"}, forbidden: ["previous_response_id"]],
              respond: completed_frames(@resumed_response_id)
            )
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      turn_state = "post-turn-held-prewarm-#{topology}"

      {:ok, state} =
        CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: turn_state, accepted_turn_state: turn_state, client_ip: "127.0.0.1"}})

      Process.put(:held_prewarm_socket_state, state)

      try do
        assert {:ok, state} = CodexResponsesSocket.handle_in({resume_prewarm_frame(setup), [opcode: :text]}, state)
        assert {:push, {:text, prewarm_created}, state} = receive_socket_push(state)
        assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(prewarm_created)
        assert {:push, {:text, prewarm_completed}, state} = receive_socket_push(state)
        assert %{"type" => "response.completed"} = CodexPooler.JSON.decode!(prewarm_completed)
        assert [_prewarm_task] = MapSet.to_list(state.tasks)
        Process.put(:held_prewarm_socket_state, state)

        assert {:ok, state} = CodexResponsesSocket.handle_in({resumed_turn_frame(setup, compact_item), [opcode: :text]}, state)
        assert [%{request_options: %{native_compaction_reservation: %{phase: :final}}}] = :queue.to_list(state.queued_response_payloads)
        Process.put(:held_prewarm_socket_state, state)

        {state, frames} = collect_until_idle!(state, [])
        Process.put(:held_prewarm_socket_state, state)

        assert [%{"type" => "response.created"}, %{"type" => "response.completed", "response" => %{"id" => @resumed_response_id}}] = frames
        assert [resumed] = FakeUpstream.requests(upstream)
        assert compact_item in resumed.json["input"]
        assert await_settled_rows!(setup) == [{"/backend-api/codex/responses", "websocket", "succeeded"}]
        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:held_prewarm_socket_state))
      end
    end
  end

  # After a post-turn compaction the owner keeps the native compaction
  # admission in `pending_final` for the next turn. The released client then
  # exits, and the detach clears that admission; its lifecycle observation must
  # name the detach, not `request_rejected` (findings#258 row 258-23, seen in
  # every real-client arm of the post-turn run).
  test "owner_forwarded socket close after a post-turn compaction clears the admission as a downstream detach" do
    put_owner_forwarding!(true)
    compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-detach"}

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: completed_frames(@anchor_response_id)),
          FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create", "input.0.type" => "compaction_trigger"}], respond: compaction_frames(compact_item))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    port = start_public_endpoint!()
    {conn, websocket, ref} = post_turn_compacted_socket!(port, setup, "post-turn-detach")

    [session] = Repo.all(from(session in CodexSession, where: session.pool_id == ^setup.pool.id))
    assert {:ok, owner_pid} = WebsocketOwnerSession.lookup(session.id)
    # The lifecycle observation is a debug line of the owner module; raise
    # only that module's level, restored before the next test.
    on_exit(fn -> Logger.delete_module_level(WebsocketOwnerSession) end)
    :ok = Logger.put_module_level(WebsocketOwnerSession, :debug)

    log =
      capture_log([level: :debug], fn ->
        _websocket = websocket
        _ref = ref
        Mint.HTTP.close(conn)
        await_owner_detached!(owner_pid)
      end)

    Logger.delete_module_level(WebsocketOwnerSession)

    clears = log |> String.split("\n") |> Enum.filter(&(&1 =~ "native compaction lifecycle" and &1 =~ "operation: :clear"))
    assert Enum.any?(clears, &(&1 =~ "reason: :downstream_detached" and &1 =~ "phase_from: :pending_final")), inspect(clears)
    refute Enum.any?(clears, &(&1 =~ "reason: :request_rejected"))
  end

  # The provider closes the connection a post-turn compaction ran on (its
  # connection-age limit) while the admission waits in `pending_final` for the
  # next turn (findings#270 row 270-145). The socket closes 1001 and the
  # released client sends that turn, carrying the compaction item, on a new
  # socket of the rotated window, where it runs as an ordinary turn on a new
  # upstream connection. Owner forwarding off and on end alike: whoever held
  # the admission drops it with the connection (the direct session when the
  # connection closes, the owner on its session's signal, findings#274),
  # nothing bound to the closed connection is left, and each of the three
  # requests is settled once, for its own usage.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} provider close after a post-turn compaction closes the socket 1001 and the next turn runs as an ordinary turn, each request billed once", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      attach_admission_lifecycle!()
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-upstream-close-#{topology}"}

      upstream =
        start_upstream(
          # provenance: observed released-binary exec + resume frame shapes; reply frames and usage synthetic; the provider's close of the idle connection as attributed in findings#270
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: completed_frames(@anchor_response_id)),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => @anchor_response_id, "input.0.type" => "compaction_trigger"}],
              respond: compaction_frames(compact_item, @compact_usage)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 2,
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_frames(@resumed_response_id, @resumed_usage)
            )
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      port = start_public_endpoint!()
      {conn, websocket, ref} = post_turn_compacted_socket!(port, setup, "post-turn-upstream-close-#{topology}")
      assert_receive {:admission_lifecycle, %{from: :collected_unconfirmed, to: :pending_final} = pending_final}, 15_000

      close_ref = make_ref()
      assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
      assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000

      try do
        {_conn, _websocket, frames} = receive_frames_until_close!(conn, websocket, ref)
        assert frames == [{:close, 1001, "upstream connection closed"}]
      after
        Mint.HTTP.close(conn)
      end

      assert_closed_connection_admission_cleared!(pending_final, topology)

      {conn, websocket, ref, _headers} =
        public_websocket_connect_with_request_headers!(
          port,
          setup,
          "post-turn-upstream-close-next-#{topology}",
          "/backend-api/codex/responses",
          [{"session-id", @session_id}, {"thread-id", @thread_id}, {"x-client-request-id", @thread_id}, {"x-codex-window-id", @resumed_window_id}]
        )

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, resumed_turn_frame(setup, compact_item))
        {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.created"} = CodexPooler.JSON.decode!(created)
        {_conn, _websocket, completed} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => @resumed_response_id}} = CodexPooler.JSON.decode!(completed)
      after
        Mint.HTTP.close(conn)
      end

      assert [first_turn, compact, resumed] = FakeUpstream.requests(upstream)
      assert compact.websocket_connection_id == first_turn.websocket_connection_id
      assert resumed.websocket_connection_id != first_turn.websocket_connection_id
      assert compact_item in resumed.json["input"]

      assert await_settled_rows!(setup) == [
               {"/backend-api/codex/responses", "websocket", "succeeded"},
               {"/backend-api/codex/responses/compact", "websocket", "succeeded"},
               {"/backend-api/codex/responses", "websocket", "succeeded"}
             ]

      # One attempt per request, its reservation released by one settlement
      # for its own usage: nothing billed twice, nothing left provisional.
      requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: request.admitted_at))

      for {request, total_tokens} <- Enum.zip(requests, [20_001, @compact_usage["total_tokens"], @resumed_usage["total_tokens"]]) do
        assert [%Attempt{status: "succeeded"}] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^request.id))
        assert ledger_entry_kinds(request) == ["release", "reservation", "settlement"]
        assert key_usage_events(request.id) == %{known: total_tokens, provisional: 0, admissions: 1}
      end

      # No owner still holds an admission bound to the closed connection (with
      # owner forwarding off the socket's session held it and went with it).
      refute Enum.any?(owner_admissions(setup), &bound_to?(&1, pending_final))
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  # The socket stays open when the client's next frame crosses the provider's
  # close (and with a socket of an earlier release, or whenever the owner keeps
  # the close to itself), so nothing detaches it. The admission bound to the
  # closed connection must still end with that connection, in both topologies
  # (findings#274): a mid-turn compaction's final that crosses the close runs
  # as an ordinary turn on the next connection, where it used to reserve the
  # stale admission under owner forwarding and fail 502 without reaching the
  # provider. Driven through the socket callbacks (this process is the
  # socket), so the next frame is handled before the close message.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} mid-turn compaction final crossing the provider's close runs as an ordinary turn", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      attach_admission_lifecycle!()
      turn_id = "mid-turn-crossing-turn-#{topology}"
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-mid-turn-crossing-#{topology}"}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (compaction v2-shaped mid-turn frames; the provider's close of the idle connection as attributed in findings#270)
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "input.0.type" => "message"}, forbidden: ["previous_response_id"]],
              respond: event_frames([%{"type" => "response.completed", "response" => %{"id" => "resp_mid_turn_crossing_anchor", "status" => "completed", "output" => []}}])
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => "resp_mid_turn_crossing_anchor", "input.0.type" => "custom_tool_call_output"}],
              respond:
                event_frames([
                  %{"type" => "response.output_item.done", "item" => compact_item},
                  %{"type" => "response.completed", "response" => %{"id" => "resp_mid_turn_crossing_compact", "status" => "completed", "output" => [compact_item]}}
                ])
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 2,
              json: [valid: true, equals: %{"type" => "response.create", "input.0.type" => "compaction"}, forbidden: ["previous_response_id"]],
              respond: event_frames([%{"type" => "response.completed", "response" => %{"id" => "resp_mid_turn_crossing_final", "status" => "completed", "output" => []}}])
            )
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "mid-turn-crossing-#{topology}")

      try do
        anchor = mid_turn_payload(setup, [%{"type" => "message", "role" => "user", "content" => "synthetic mid-turn anchor"}], turn_id, "turn", 1)
        state = run_turn!(state, anchor)

        compact =
          setup
          |> mid_turn_payload([%{"type" => "custom_tool_call_output", "call_id" => "call_mid_turn_crossing", "output" => "synthetic tool output"}, %{"type" => "compaction_trigger"}], turn_id, "compaction", 1)
          |> CodexPooler.JSON.decode!()
          |> Map.put("previous_response_id", "resp_mid_turn_crossing_anchor")
          |> CodexPooler.JSON.encode!()

        state = run_turn!(state, compact)
        assert_receive {:admission_lifecycle, %{from: :collected_unconfirmed, to: :pending_final} = pending_final}, 15_000

        close_message = close_connection_1!(upstream)
        final = mid_turn_payload(setup, [compact_item, %{"type" => "message", "role" => "user", "content" => "synthetic final"}], turn_id, "turn", 2)
        assert {:ok, state} = CodexResponsesSocket.handle_in({final, [opcode: :text]}, state)
        Process.put(:crossing_socket_state, state)
        assert {:ok, state} = CodexResponsesSocket.handle_info(close_message, state)
        {state, frames} = collect_until_idle!(state, [])
        Process.put(:crossing_socket_state, state)

        assert [%{"type" => "response.completed", "response" => %{"id" => "resp_mid_turn_crossing_final"}}] = frames
        assert await_settled_rows!(setup) == [{"/backend-api/codex/responses", "websocket", "succeeded"}, {"/backend-api/codex/responses/compact", "websocket", "succeeded"}, {"/backend-api/codex/responses", "websocket", "succeeded"}]
        assert_closed_connection_admission_cleared!(pending_final, topology)
        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # The released client's next turn after a post-turn compaction crosses the
  # provider's close and is served on the next connection as an ordinary turn
  # in both topologies. Under owner forwarding the owner used to keep the
  # stale `pending_final`, which blocked the ordinary success of that turn
  # from arming the next compaction, so the next post-turn compaction on the
  # same socket was refused 503 `owner_unavailable` (findings#274).
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} post-turn compaction after a next turn that crossed the provider's close is served", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      attach_admission_lifecycle!()
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-crossing-#{topology}"}
      second_item = %{"type" => "compaction", "encrypted_content" => "synthetic-post-turn-crossing-second-#{topology}"}

      upstream =
        start_upstream(
          # provenance: observed released-binary exec + resume frame shapes; reply frames synthetic; the provider's close of the idle connection as attributed in findings#270
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: completed_frames(@anchor_response_id)),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => @anchor_response_id, "input.0.type" => "compaction_trigger"}],
              respond: compaction_frames(compact_item)
            ),
            FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 2, json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: completed_frames(@resumed_response_id)),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              websocket_connection_ordinal: 2,
              json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => @resumed_response_id, "input.0.type" => "compaction_trigger"}],
              respond: compaction_frames(second_item)
            )
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "post-turn-crossing-#{topology}")

      try do
        state = run_turn!(state, turn_frame(setup))
        state = run_turn!(state, post_turn_frame(setup))
        assert_receive {:admission_lifecycle, %{from: :collected_unconfirmed, to: :pending_final} = pending_final}, 15_000

        close_message = close_connection_1!(upstream)
        assert {:ok, state} = CodexResponsesSocket.handle_in({resumed_turn_frame(setup, compact_item), [opcode: :text]}, state)
        Process.put(:crossing_socket_state, state)
        assert {:ok, state} = CodexResponsesSocket.handle_info(close_message, state)
        {state, frames} = collect_until_idle!(state, [])
        Process.put(:crossing_socket_state, state)
        assert [%{"type" => "response.created"}, %{"type" => "response.completed", "response" => %{"id" => @resumed_response_id}}] = frames

        assert {:ok, state} = CodexResponsesSocket.handle_in({resumed_post_turn_frame(setup), [opcode: :text]}, state)
        Process.put(:crossing_socket_state, state)
        {state, frames} = collect_until_idle!(state, [])
        Process.put(:crossing_socket_state, state)
        assert [%{"type" => "response.output_item.done", "item" => ^second_item}, %{"type" => "response.completed", "response" => %{"output" => [^second_item]}}] = frames

        assert await_settled_rows!(setup) == [
                 {"/backend-api/codex/responses", "websocket", "succeeded"},
                 {"/backend-api/codex/responses/compact", "websocket", "succeeded"},
                 {"/backend-api/codex/responses", "websocket", "succeeded"},
                 {"/backend-api/codex/responses/compact", "websocket", "succeeded"}
               ]

        assert_closed_connection_admission_cleared!(pending_final, topology)
        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # The provider closes the connection a native compaction ran on after its
  # result was collected and before it was confirmed (findings#275). The
  # confirmation used to fail on the closed connection: with owner forwarding
  # off the client got `502 invalid_compaction_response` for a compaction it
  # had already been billed for, with it on the compaction was delivered but
  # its final reserved an admission bound to the closed connection and failed
  # `502 upstream_request_failed` without reaching the provider. The
  # confirmation now ends the admission as `connection_closed` and succeeds, so
  # the client gets its compaction and the final runs as an ordinary turn on
  # the next connection, each request billed once. `held_confirmation` holds
  # the compaction's response task between its settlement and its
  # confirmation while the connection closes (deterministic);
  # `close_behind_terminal` has the provider send its Close right behind the
  # compaction's terminal, with nothing held. Driven through the socket
  # callbacks (this process is the socket).
  for topology <- [:direct, :owner_forwarded], mode <- ["full", "lite"] do
    @tag topology: topology, serving_mode: mode
    test "#{topology} #{mode} provider close between a compaction's collection and its confirmation delivers the compaction and serves its final", ctx do
      assert_compaction_window_close!(ctx.topology, ctx.serving_mode, :held_confirmation)
    end
  end

  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} provider Close right behind a compaction's terminal delivers the compaction and serves its final", ctx do
      assert_compaction_window_close!(ctx.topology, "full", :close_behind_terminal)
    end
  end

  # A first full-history compaction (no anchor) is collected whole, and its
  # admission is opened only when the socket authorizes the collected result
  # (findings#270 row 270-200). The provider closes the connection between the
  # collection and that authorization, while the compaction's response task is
  # held after its settlement. With owner forwarding off the close dropped the
  # collected result and the client got `502 invalid_compaction_response` for a
  # compaction it had been billed for; the session now keeps that result as
  # the one authorization it can still accept, off the connection, and nothing
  # is armed. With owner forwarding on the owner keeps its result, and the
  # confirmation check of findings#275 ends the admission. Both deliver the
  # compaction and serve the final as an ordinary turn, each request billed once.
  for topology <- [:direct, :owner_forwarded], mode <- ["full", "lite"] do
    @tag topology: topology, serving_mode: mode
    test "#{topology} #{mode} provider close between a first full-history compaction's collection and its authorization delivers the compaction and serves its final", ctx do
      assert_first_compaction_window_close!(ctx.topology, ctx.serving_mode)
    end
  end

  # Every admission transition is reported on the lifecycle event, in the same
  # order in both topologies (findings#270 row 270-201): with owner forwarding
  # on, the owner used to arm, consume and collect without an event, so a
  # forwarded compaction showed reservations and confirmations out of nothing.
  # An uninterrupted mid-turn compaction on one connection; its final's own
  # ordinary success re-arms the next compaction from `consumed_final`.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} native compaction admission reports every transition in the same order in both topologies", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      attach_admission_lifecycle!()
      turn_id = "sequence-#{topology}"
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-sequence-#{topology}"}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (compaction v2-shaped mid-turn frames on one connection)
          FakeUpstream.strict_sequence([
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_sequence_anchor", @anchor_usage)])),
            window_request(1, [equals: %{"previous_response_id" => "resp_sequence_anchor"}], event_frames(compaction_events(compact_item, "resp_sequence_compact"))),
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_sequence_final", @resumed_usage)]))
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "sequence-#{topology}")

      try do
        state = run_turn!(state, mid_turn_payload(setup, [window_message("synthetic sequence anchor")], turn_id, "turn", 1))
        state = run_turn!(state, window_compaction_frame(setup, turn_id, "resp_sequence_anchor"))
        {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
        assert [%{"type" => "response.completed", "response" => %{"id" => "resp_sequence_final"}}] = frames
        _state = state
        assert_window_accounting!(setup, [@anchor_usage, @compact_usage, @resumed_usage])

        events = for event <- drain_lifecycle([]), not (event.from == :cleared and event.to == :cleared), do: event
        assert MapSet.new(events, & &1.topology) == MapSet.new([lifecycle_topology(topology)])

        assert Enum.map(events, &{&1.operation, &1.reason, &1.from, &1.to}) == [
                 {:ordinary_success, :success, :cleared, :pending_compact},
                 {:reserve, :success, :pending_compact, :reserved_compact},
                 {:accounting, :success, :reserved_compact, :accounting_started_compact},
                 {:consume, :success, :accounting_started_compact, :consumed_compact},
                 {:collect, :success, :consumed_compact, :collected_unconfirmed},
                 {:confirm, :success, :collected_unconfirmed, :pending_final},
                 {:reserve, :success, :pending_final, :reserved_final},
                 {:accounting, :success, :reserved_final, :accounting_started_final},
                 {:consume, :success, :accounting_started_final, :consumed_final},
                 {:ordinary_success, :success, :consumed_final, :pending_compact}
               ]

        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # The same for a first full-history compaction, whose admission is opened
  # when the socket authorizes the collected result: authorize, collect and
  # confirm are reported in both topologies (findings#270 row 270-201).
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} first full-history compaction admission reports every transition in the same order in both topologies", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      attach_admission_lifecycle!()
      turn_id = "first-sequence-#{topology}"
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-first-sequence-#{topology}"}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (compaction v2-shaped full-history frames on one connection)
          FakeUpstream.strict_sequence([
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_first_sequence_anchor", @anchor_usage)])),
            window_request(1, [forbidden: ["previous_response_id"]], event_frames(compaction_events(compact_item, "resp_first_sequence_compact"))),
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_first_sequence_final", @resumed_usage)]))
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "first-sequence-#{topology}")

      try do
        history = [window_message("synthetic first sequence anchor")]
        state = run_turn!(state, mid_turn_payload(setup, history, turn_id, "turn", 1))
        state = run_turn!(state, mid_turn_payload(setup, history ++ [%{"type" => "compaction_trigger"}], turn_id, "compaction", 1))
        {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
        assert [%{"type" => "response.completed", "response" => %{"id" => "resp_first_sequence_final"}}] = frames
        _state = state
        assert_window_accounting!(setup, [@anchor_usage, @compact_usage, @resumed_usage])

        events = for event <- drain_lifecycle([]), not (event.from == :cleared and event.to == :cleared), do: event
        assert MapSet.new(events, & &1.topology) == MapSet.new([lifecycle_topology(topology)])

        assert Enum.map(events, &{&1.operation, &1.reason, &1.from, &1.to}) == [
                 {:ordinary_success, :success, :cleared, :pending_compact},
                 {:ordinary_success, :success, :pending_compact, :ordinary_success},
                 {:collect, :success, :ordinary_success, :collected_unconfirmed},
                 {:confirm, :success, :collected_unconfirmed, :pending_final},
                 {:reserve, :success, :pending_final, :reserved_final},
                 {:accounting, :success, :reserved_final, :accounting_started_final},
                 {:consume, :success, :accounting_started_final, :consumed_final},
                 {:ordinary_success, :success, :consumed_final, :pending_compact}
               ]

        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # An anchored compaction the provider fails with a retryable error
  # (`response.failed` `server_error`). The released client retries it as the
  # full history, and the resend policy admits that retry as one successor.
  # The failure used to count as a collected compaction, so the admission
  # stayed `collected_unconfirmed` and the retry, which the provider served and
  # billed, was refused its first-compact authorization: the client got `502
  # invalid_compaction_response` (findings#281).
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} full-history retry of an anchored compaction the provider failed is delivered and billed once", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      turn_id = "failed-compaction-#{topology}"
      history = [window_message("synthetic failed-compaction anchor")]
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-failed-compaction-#{topology}"}
      failure = %{"type" => "response.failed", "response" => %{"id" => "resp_failed_compaction", "status" => "failed", "error" => %{"code" => "server_error", "message" => "synthetic server error"}}}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (compaction v2-shaped mid-turn frames on one connection; a provider response.failed server_error on the anchored compaction, findings#281)
          FakeUpstream.strict_sequence([
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_failed_compaction_anchor", @anchor_usage)])),
            window_request(1, [equals: %{"previous_response_id" => "resp_failed_compaction_anchor"}], event_frames([failure])),
            window_request(1, [forbidden: ["previous_response_id"]], event_frames(compaction_events(compact_item, "resp_failed_compaction_retry"))),
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_failed_compaction_final", @resumed_usage)]))
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "failed-compaction-#{topology}")

      try do
        state = run_turn!(state, mid_turn_payload(setup, history, turn_id, "turn", 1))
        {state, frames} = run_turn_frames!(state, window_compaction_frame(setup, turn_id, "resp_failed_compaction_anchor"))
        assert [%{"type" => "response.failed", "response" => %{"error" => %{"code" => "server_error"}}}] = frames

        {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, history ++ [%{"type" => "compaction_trigger"}], turn_id, "compaction", 1))
        assert [%{"type" => "response.output_item.done", "item" => ^compact_item}, %{"type" => "response.completed", "response" => %{"id" => "resp_failed_compaction_retry", "output" => [^compact_item]}}] = frames

        {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
        assert [%{"type" => "response.completed", "response" => %{"id" => "resp_failed_compaction_final"}}] = frames
        Process.put(:crossing_socket_state, state)

        rows = [_anchor_row, failed, _retry, _final] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: request.admitted_at))

        assert Enum.map(rows, &{&1.endpoint, &1.status, &1.last_error_code, &1.usage_status}) == [
                 {"/backend-api/codex/responses", "succeeded", nil, "usage_known"},
                 {"/backend-api/codex/responses/compact", "failed", "server_error", "usage_unknown"},
                 {"/backend-api/codex/responses/compact", "succeeded", nil, "usage_known"},
                 {"/backend-api/codex/responses", "succeeded", nil, "usage_known"}
               ]

        # Each request is billed once, as its row says: the failure's unknown
        # usage keeps its reservation estimate provisional, and the retry is
        # billed its own usage and nothing more.
        [failed_estimate] = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^failed.id and entry.entry_kind == "reservation", select: entry.total_tokens))
        assert Enum.map(rows, &ledger_entry_kinds/1) == List.duplicate(["release", "reservation", "settlement"], 4)

        assert Enum.map(rows, &key_usage_events(&1.id)) == [
                 %{known: @anchor_usage["total_tokens"], provisional: 0, admissions: 1},
                 %{known: 0, provisional: failed_estimate, admissions: 1},
                 %{known: @compact_usage["total_tokens"], provisional: 0, admissions: 1},
                 %{known: @resumed_usage["total_tokens"], provisional: 0, admissions: 1}
               ]

        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # An anchored compaction admitted on its connection, whose connection the
  # provider then closes before the compaction goes out: the connection-bound
  # guard refuses the anchor on the next connection before anything is sent
  # (findings#278). The client used to get the collected compaction's 400
  # `stream_incomplete`, which the released client reads as a fatal invalid
  # request, and the turn failed. It now gets the `previous_response_not_found`
  # event an ordinary continuation gets for the same refusal, which the
  # released client retries as a full request without the anchor: here the
  # full-history compaction on the same socket, then the final. The
  # compaction's task is held right before it goes upstream (the egress
  # observation), while the connection closes.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} guard refusal of an anchored compaction reaches the client as previous_response_not_found and the full-history retry is served", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      turn_id = "guard-refusal-#{topology}"
      history = [window_message("synthetic guard anchor")]
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-guard-refusal-#{topology}"}

      # Exactly one compaction reaches the provider, the full-history retry:
      # the refused one is checked right after its refusal, and `verify!/1`
      # fails on any request this sequence does not expect.
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (compaction v2-shaped frames; the provider's close of the connection between a compaction's admission and its send, findings#278)
          FakeUpstream.strict_sequence([
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_guard_refusal_anchor", @anchor_usage)])),
            window_request(2, [forbidden: ["previous_response_id"]], event_frames(compaction_events(compact_item, "resp_guard_refusal_retry"))),
            window_request(2, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_guard_refusal_final", @resumed_usage)]))
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "guard-refusal-#{topology}")

      try do
        state = run_turn!(state, mid_turn_payload(setup, history, turn_id, "turn", 1))
        hold = hold_before_egress!()
        assert {:ok, state} = CodexResponsesSocket.handle_in({window_compaction_frame(setup, turn_id, "resp_guard_refusal_anchor"), [opcode: :text]}, state)
        Process.put(:crossing_socket_state, state)
        assert_receive {^hold, :held, task_pid}, 15_000
        close_connection_under_held_task!(upstream, hold, task_pid, setup, state)

        {state, frames} = collect_until_idle!(state, [])
        Process.put(:crossing_socket_state, state)
        assert [_anchor_request] = FakeUpstream.requests(upstream)
        assert frames == [native_previous_response_retry_event()]

        retry_frame = mid_turn_payload(setup, history ++ [%{"type" => "compaction_trigger"}], turn_id, "compaction", 1)
        {state, frames} = run_turn_frames!(state, retry_frame)
        assert [%{"type" => "response.output_item.done", "item" => ^compact_item}, %{"type" => "response.completed", "response" => %{"id" => "resp_guard_refusal_retry", "output" => [^compact_item]}}] = frames
        state = second_resend_after_retry!(topology, state, retry_frame, upstream)
        {_state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
        assert [%{"type" => "response.completed", "response" => %{"id" => "resp_guard_refusal_final"}}] = frames

        assert [anchor_row, refused, retry, final] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: request.admitted_at))

        assert Enum.map([anchor_row, refused, retry, final], &{&1.endpoint, &1.status}) == [
                 {"/backend-api/codex/responses", "succeeded"},
                 {"/backend-api/codex/responses/compact", "failed"},
                 {"/backend-api/codex/responses/compact", "succeeded"},
                 {"/backend-api/codex/responses", "succeeded"}
               ]

        # The refused compaction reached no provider: one attempt, the guard's
        # metadata, no usage and nothing provisional.
        assert [%Attempt{status: "failed", response_metadata: %{"transport_failure" => %{"termination_source" => "continuation_generation_guard"}}}] =
                 Repo.all(from(attempt in Attempt, where: attempt.request_id == ^refused.id))

        assert key_usage_events(refused.id) == %{known: 0, provisional: 0, admissions: 1}
        assert key_usage_events(retry.id) == %{known: @compact_usage["total_tokens"], provisional: 0, admissions: 1}
        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # An anchored compaction whose connection the provider closes after the
  # socket reserved its admission and before its response task starts the
  # reservation's accounting (findings#284). The task is held where it redeems
  # its runtime proof, between those two steps. With owner forwarding off the
  # session clears the admission with its connection, and the accounting start
  # used to find it cleared and answer `500 gateway_reservation_failed` with two
  # `[error]` lines; it now gets the retryable `503 owner_unavailable` the
  # reservation's own check gives a closed connection (findings#275). With
  # forwarding on the owner keeps a reserved admission across the close, and
  # the guard answers `previous_response_not_found` at send. Either way nothing
  # reaches the provider and the client's full-history retry is served.
  for topology <- [:direct, :owner_forwarded] do
    @tag topology: topology
    test "#{topology} compaction whose connection closes between its reservation and its accounting start is refused retryably", %{topology: topology} do
      put_owner_forwarding!(topology == :owner_forwarded)
      turn_id = "accounting-close-#{topology}"
      history = [window_message("synthetic accounting-close anchor")]
      compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-accounting-close-#{topology}"}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (compaction v2-shaped frames; the provider's close of the connection between a compaction's reservation and its accounting start, findings#284)
          FakeUpstream.strict_sequence([
            window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_accounting_close_anchor", @anchor_usage)])),
            window_request(2, [forbidden: ["previous_response_id"]], event_frames(compaction_events(compact_item, "resp_accounting_close_retry"))),
            window_request(2, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_accounting_close_final", @resumed_usage)]))
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      state = callback_socket!(setup, "accounting-close-#{topology}")

      try do
        state = run_turn!(state, mid_turn_payload(setup, history, turn_id, "turn", 1))
        hold = hold_at_runtime_proof!()

        {{state, frames}, log} =
          with_log(fn ->
            assert {:ok, state} = CodexResponsesSocket.handle_in({window_compaction_frame(setup, turn_id, "resp_accounting_close_anchor"), [opcode: :text]}, state)
            Process.put(:crossing_socket_state, state)
            assert_receive {^hold, :held, task_pid}, 15_000
            close_connection_under_held_task!(upstream, hold, task_pid, setup, state)
            collect_until_idle!(state, [])
          end)

        Process.put(:crossing_socket_state, state)
        refute log =~ "[error]"
        refute log =~ "reservation cleanup failed"
        assert_accounting_close_refusal!(topology, frames, log)
        assert [_anchor_request] = FakeUpstream.requests(upstream)

        {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, history ++ [%{"type" => "compaction_trigger"}], turn_id, "compaction", 1))
        assert [%{"type" => "response.output_item.done", "item" => ^compact_item}, %{"type" => "response.completed", "response" => %{"id" => "resp_accounting_close_retry", "output" => [^compact_item]}}] = frames

        {_state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
        assert [%{"type" => "response.completed", "response" => %{"id" => "resp_accounting_close_final"}}] = frames

        rows = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: request.admitted_at))
        assert Enum.map(rows, &{&1.endpoint, &1.status}) == accounting_close_rows(topology)
        assert :ok = FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
      end
    end
  end

  # With forwarding off the refusal happens before anything is reserved and
  # leaves no row; it writes the warning every route writes for a compaction
  # refused before dispatch, naming its own route (findings#270 row 270-246).
  # With forwarding on the owner kept the reservation, so the guard's refusal
  # settles the compaction's row as failed (findings#278) and that warning is
  # not written.
  defp assert_accounting_close_refusal!(:direct, frames, log) do
    assert [%{"type" => "error", "status" => 503, "error" => %{"code" => "owner_unavailable"}}] = frames

    assert log =~
             "native compaction refused before dispatch reason=admission_unavailable cause=connection_closed code=owner_unavailable status=503 compaction_phase=mid_turn topology=direct decided_at=accounting_start reservation_phase=compact codex_session_id="
  end

  defp assert_accounting_close_refusal!(:owner_forwarded, frames, log) do
    assert frames == [native_previous_response_retry_event()]
    refute log =~ "native compaction refused before dispatch"
  end

  defp accounting_close_rows(:direct),
    do: [{"/backend-api/codex/responses", "succeeded"}, {"/backend-api/codex/responses/compact", "succeeded"}, {"/backend-api/codex/responses", "succeeded"}]

  defp accounting_close_rows(:owner_forwarded),
    do: [{"/backend-api/codex/responses", "succeeded"}, {"/backend-api/codex/responses/compact", "failed"}, {"/backend-api/codex/responses/compact", "succeeded"}, {"/backend-api/codex/responses", "succeeded"}]

  # One successor only: with owner forwarding on, a second full-history
  # resend after the served retry is refused and reaches no provider. With
  # forwarding off a resend of a served compaction is read as proof that its
  # reply was lost and chains onto it (`FailedPredecessorResend`), a rule
  # findings#278 does not change.
  defp second_resend_after_retry!(:owner_forwarded, state, retry_frame, upstream) do
    assert {:push, {:text, refusal}, state} = CodexResponsesSocket.handle_in({retry_frame, [opcode: :text]}, state)
    assert %{"status" => 409, "error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(refusal)
    assert [_anchor_request, _retry_request] = FakeUpstream.requests(upstream)
    state
  end

  defp second_resend_after_retry!(:direct, state, _retry_frame, _upstream), do: state

  # With owner forwarding on, the client's final can reach the owner before
  # the owner hears that its session closed the connection the admission names
  # (findings#270 row 270-182). The owner checks the session's open connection
  # when it reserves, so the final still runs as an ordinary turn. The order is
  # made deterministic by taking the session's close signal away from the
  # owner and handing it over only after the final was served. The final
  # opened the next connection, so the late signal names a connection nothing
  # depends on any more and the owner keeps the socket open
  # (`superseded_connection`, findings#270 row 270-199).
  test "owner_forwarded final reaching the owner before its session's close signal runs as an ordinary turn" do
    put_owner_forwarding!(true)
    attach_admission_lifecycle!()
    turn_id = "late-signal-turn"
    compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-late-signal"}

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (compaction v2-shaped mid-turn frames; the provider's close of the idle connection as attributed in findings#270)
        FakeUpstream.strict_sequence([
          window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_late_signal_anchor", @anchor_usage)])),
          window_request(1, [equals: %{"previous_response_id" => "resp_late_signal_anchor"}], event_frames(compaction_events(compact_item, "resp_late_signal_compact"))),
          window_request(2, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_late_signal_final", @resumed_usage)]))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    state = callback_socket!(setup, "late-signal")

    try do
      state = run_turn!(state, mid_turn_payload(setup, [window_message("synthetic late-signal anchor")], turn_id, "turn", 1))
      state = run_turn!(state, window_compaction_frame(setup, turn_id, "resp_late_signal_anchor"))
      assert_receive {:admission_lifecycle, %{from: :collected_unconfirmed, to: :pending_final} = pending_final}, 15_000

      [owner] = live_owners(setup)
      session = :sys.get_state(owner).upstream_pid
      test_pid = self()
      :sys.replace_state(session, &Map.put(&1, :connection_close_subscriber, test_pid))
      close_ref = make_ref()
      assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
      assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000
      assert_receive {:upstream_websocket_connection_closed, ^session, signal}, 15_000

      {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
      assert [%{"type" => "response.completed", "response" => %{"id" => "resp_late_signal_final"}}] = frames

      # Reserved nowhere: the owner ended the admission when the final asked
      # for it, before the signal arrived.
      assert_receive {:admission_lifecycle, %{operation: :clear, from: :pending_final, to: :cleared} = clear}, 15_000
      assert {clear.reason, clear.lifecycle_id, clear.generation} == {:connection_closed, pending_final.lifecycle_id, pending_final.generation}
      refute Enum.any?(drain_lifecycle([]), &(&1.to == :reserved_final))

      {_owner_state, log} =
        with_info_log(fn ->
          send(owner, {:upstream_websocket_connection_closed, session, signal})
          :sys.get_state(owner)
        end)

      assert log =~ kept_open_line("peer_close_frame", "superseded_connection", signal.lifecycle_id, 1, "on")
      refute_received {:websocket_owner_upstream_closed, _correlation_id, _epoch, _signal}
      _state = state

      assert_window_accounting!(setup, [@anchor_usage, @compact_usage, @resumed_usage])
      assert [final_request] = Enum.drop(FakeUpstream.requests(upstream), 2)
      assert compact_item in final_request.json["input"]
      assert :ok = FakeUpstream.verify!(upstream)
    after
      CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
    end
  end

  # A close signal the owner hears only after its session opened the next
  # connection names a connection nothing depends on any more (findings#270
  # row 270-199): the socket's last response, and so the anchor of its next
  # request, lives on the open one. The owner keeps the socket open with
  # `skip_reason=superseded_connection`, and the next request rides its anchor
  # on the new connection with no guard refusal. The signal is made late by
  # taking it away from the owner and handing it over after the turn on the
  # next connection completed.
  test "owner_forwarded late close signal of a superseded connection keeps the socket open for an anchor on the open one" do
    put_owner_forwarding!(true)
    turn_id = "superseded-close-turn"

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (the provider's close of an idle connection as attributed in findings#270, its signal delivered after the next connection opened)
        FakeUpstream.strict_sequence([
          window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_superseded_first", @anchor_usage)])),
          window_request(2, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_superseded_second", @compact_usage)])),
          window_request(2, [equals: %{"previous_response_id" => "resp_superseded_second"}], event_frames([completed_event("resp_superseded_third", @resumed_usage)]))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    state = callback_socket!(setup, "superseded-close")

    try do
      state = run_turn!(state, mid_turn_payload(setup, [window_message("synthetic first turn")], turn_id, "turn", 1))
      [owner] = live_owners(setup)
      %{upstream_pid: session} = :sys.get_state(owner)
      test_pid = self()
      :sys.replace_state(session, &Map.put(&1, :connection_close_subscriber, test_pid))
      close_ref = make_ref()
      assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
      assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000
      assert_receive {:upstream_websocket_connection_closed, ^session, %{generation: 1} = signal}, 15_000
      :sys.replace_state(session, &Map.put(&1, :connection_close_subscriber, owner))

      # The client's next request carries its whole history and opens the
      # next connection.
      {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [window_message("synthetic first turn"), window_message("synthetic second turn")], turn_id, "turn", 1))
      assert [%{"type" => "response.completed", "response" => %{"id" => "resp_superseded_second"}}] = frames

      {_owner_state, log} =
        with_info_log(fn ->
          send(owner, {:upstream_websocket_connection_closed, session, signal})
          :sys.get_state(owner)
        end)

      assert log =~ kept_open_line("peer_close_frame", "superseded_connection", signal.lifecycle_id, 1, "on")
      refute_received {:websocket_owner_upstream_closed, _correlation_id, _epoch, _signal}

      anchored =
        setup
        |> mid_turn_payload([window_message("synthetic third turn")], turn_id, "turn", 1)
        |> CodexPooler.JSON.decode!()
        |> Map.put("previous_response_id", "resp_superseded_second")
        |> CodexPooler.JSON.encode!()

      {_state, frames} = run_turn_frames!(state, anchored)
      assert [%{"type" => "response.completed", "response" => %{"id" => "resp_superseded_third"}}] = frames
      assert Enum.all?(await_settled_rows!(setup), &match?({_endpoint, "websocket", "succeeded"}, &1))
      assert :ok = FakeUpstream.verify!(upstream)
    after
      CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
    end
  end

  # The same race for a compaction: a post-turn compaction that reaches the
  # owner before the owner hears its connection closed finds the admission
  # bound to that connection refused when it is reserved, and is answered the
  # retryable 503 before anything is claimed, billed or sent, as when the
  # signal comes first (the admission is then already gone) and as with owner
  # forwarding off. The released client retries with its full history.
  test "owner_forwarded post-turn compaction reaching the owner before its session's close signal is refused before dispatch" do
    put_owner_forwarding!(true)
    attach_admission_lifecycle!()

    upstream =
      start_upstream(
        # provenance: observed released-binary frame shape; reply frames synthetic; the provider's close of the idle connection as attributed in findings#270
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: completed_frames(@anchor_response_id))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    state = callback_socket!(setup, "late-signal-compaction")

    try do
      state = run_turn!(state, turn_frame(setup))
      [owner] = live_owners(setup)
      assert %{native_compaction_admission: %NativeCompactionAdmission{phase: :pending_compact, binding: armed}, upstream_pid: session} = :sys.get_state(owner)

      test_pid = self()
      :sys.replace_state(session, &Map.put(&1, :connection_close_subscriber, test_pid))
      close_ref = make_ref()
      assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
      assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000
      assert_receive {:upstream_websocket_connection_closed, ^session, signal}, 15_000

      assert {:push, {:text, refusal}, state} = CodexResponsesSocket.handle_in({post_turn_frame(setup), [opcode: :text]}, state)
      Process.put(:crossing_socket_state, state)
      assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_unavailable"}} = CodexPooler.JSON.decode!(refusal)

      assert_receive {:admission_lifecycle, %{operation: :clear, from: :pending_compact, to: :cleared} = clear}, 15_000
      assert {clear.reason, clear.lifecycle_id, clear.generation} == {:connection_closed, armed.lifecycle_id, armed.generation}
      assert await_settled_rows!(setup) == [{"/backend-api/codex/responses", "websocket", "succeeded"}]

      send(owner, {:upstream_websocket_connection_closed, session, signal})
      assert {:stop, :normal, {1001, "upstream connection closed"}, stopped} = CodexResponsesSocket.handle_info(receive_owner_word!(), state)
      Process.put(:crossing_socket_state, stopped)
      assert :ok = FakeUpstream.verify!(upstream)
    after
      CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
    end
  end

  # Drives the socket callbacks until no response task is tracked and nothing
  # is queued, returning every client-visible frame in push order. Every
  # producer of a frame for a task has fired before the task reports its
  # result, so the closing mailbox sweep needs no timer. An error terminal ends
  # the turn without an owner `:complete`, so the owner's reply is not awaited.
  defp collect_until_idle!(state, frames) do
    if MapSet.size(state.tasks) == 0 and :queue.is_empty(state.queued_response_payloads) do
      sweep_socket_mailbox(state, frames)
    else
      receive do
        message when is_tuple(message) and elem(message, 0) in @socket_messages ->
          {state, frames} = handle_socket_message(message, state, frames)
          collect_until_idle!(state, frames)
      after
        15_000 -> flunk("socket never went idle; frames so far: #{inspect(Enum.reverse(frames))}")
      end
    end
  end

  defp sweep_socket_mailbox(state, frames) do
    receive do
      message when is_tuple(message) and elem(message, 0) in @socket_messages ->
        {state, frames} = handle_socket_message(message, state, frames)
        sweep_socket_mailbox(state, frames)
    after
      0 -> {state, frames |> Enum.reverse() |> Enum.map(&CodexPooler.JSON.decode!/1)}
    end
  end

  defp handle_socket_message(message, state, frames) do
    case CodexResponsesSocket.handle_info(message, state) do
      {:push, {:text, frame}, state} ->
        if StreamProtocol.internal_control_event?(frame), do: {state, frames}, else: {state, [frame | frames]}

      {:ok, state} ->
        {state, frames}

      {:stop, _reason, close_detail, _state} ->
        flunk("socket closed with #{inspect(close_detail)}; frames so far: #{inspect(Enum.reverse(frames))}")
    end
  end

  defp post_turn_compacted_socket!(port, setup, turn_state) do
    {conn, websocket, ref, _headers} =
      public_websocket_connect_with_request_headers!(
        port,
        setup,
        turn_state,
        "/backend-api/codex/responses",
        [{"session-id", @session_id}, {"thread-id", @thread_id}, {"x-client-request-id", @thread_id}, {"x-codex-window-id", @window_id}]
      )

    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, turn_frame(setup))
    {conn, websocket, _created} = public_websocket_receive_text!(conn, websocket, ref)
    {conn, websocket, _completed} = public_websocket_receive_text!(conn, websocket, ref)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, post_turn_frame(setup))
    {conn, websocket, _done} = public_websocket_receive_text!(conn, websocket, ref)
    {conn, websocket, terminal} = public_websocket_receive_text!(conn, websocket, ref)
    assert %{"type" => "response.completed", "response" => %{"status" => "completed"}} = CodexPooler.JSON.decode!(terminal)
    {conn, websocket, ref}
  end

  # The socket detaches from the owner in its terminate callback; the owner's
  # downstream becomes nil once the detach call ran (authoritative state, no
  # completion signal to wait on).
  defp await_owner_detached!(owner_pid) do
    deadline = System.monotonic_time(:millisecond) + 15_000

    Stream.repeatedly(fn -> :sys.get_state(owner_pid).downstream end)
    |> Enum.reduce_while(nil, fn
      nil, _acc ->
        {:halt, :ok}

      _attached, _acc ->
        if System.monotonic_time(:millisecond) > deadline, do: flunk("owner never saw the downstream detach")
        Process.sleep(10)
        {:cont, nil}
    end)
  end

  defp assert_compaction_window_close!(topology, mode, variant) do
    put_owner_forwarding!(topology == :owner_forwarded)
    attach_admission_lifecycle!()
    label = "window-close-#{topology}-#{mode}-#{variant}"
    turn_id = "#{label}-turn"
    compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-#{label}"}
    compaction_response = window_compaction_response(variant, compaction_events(compact_item, "resp_window_close_compact"))

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (compaction v2-shaped mid-turn frames; the provider's close of the connection between a compaction's collection and its confirmation, findings#275)
        FakeUpstream.strict_sequence([
          window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_window_close_anchor", @anchor_usage)])),
          window_request(1, [equals: %{"previous_response_id" => "resp_window_close_anchor"}], compaction_response),
          window_request(2, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_window_close_final", @resumed_usage)]))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    if mode == "lite", do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    state = callback_socket!(setup, label)

    try do
      state = run_turn!(state, mid_turn_payload(setup, [window_message("synthetic window anchor")], turn_id, "turn", 1))
      hold = if variant == :held_confirmation, do: hold_settled_websocket_turn!()
      assert {:ok, state} = CodexResponsesSocket.handle_in({window_compaction_frame(setup, turn_id, "resp_window_close_anchor"), [opcode: :text]}, state)
      Process.put(:crossing_socket_state, state)
      if variant == :held_confirmation, do: close_held_compaction_connection!(upstream, hold, setup, state)

      {state, frames} = collect_until_idle!(state, [])
      Process.put(:crossing_socket_state, state)
      assert [%{"type" => "response.output_item.done", "item" => ^compact_item}, %{"type" => "response.completed", "response" => %{"output" => [^compact_item]}}] = frames

      {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
      assert [%{"type" => "response.completed", "response" => %{"id" => "resp_window_close_final"}}] = frames
      _state = state

      assert_window_accounting!(setup, [@anchor_usage, @compact_usage, @resumed_usage])
      # Lite opens each provider context with its tool manifest; the final
      # carries the compaction item to the provider in both modes.
      assert [first, _compaction_request, final_request] = FakeUpstream.requests(upstream)
      assert lite_context?(first) == (mode == "lite")
      assert compact_item in final_request.json["input"]

      assert_window_admission_ended!(variant, topology)
      assert :ok = FakeUpstream.verify!(upstream)
    after
      CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
    end
  end

  defp window_compaction_response(:held_confirmation, events), do: event_frames(events)

  defp window_compaction_response(:close_behind_terminal, events),
    do: FakeUpstream.websocket_sse_then_close(events, code: 1000, reason: "synthetic age limit")

  # The admission ended with the connection it named, as `connection_closed`;
  # no final was ever armed or reserved on it. Every lifecycle event names the
  # arm's topology.
  defp assert_window_admission_ended!(variant, topology) do
    events = drain_lifecycle([])
    assert %{lifecycle_id: lifecycle_id, generation: 1} = Enum.find(events, &(&1.to == :reserved_compact))
    assert [clear] = Enum.filter(events, &(&1.operation == :clear and &1.from not in [:cleared, :pending_compact]))
    assert {clear.reason, clear.lifecycle_id, clear.generation} == {:connection_closed, lifecycle_id, 1}
    if variant == :held_confirmation, do: assert(clear.from == :collected_unconfirmed and not Enum.any?(events, &(&1.to in [:pending_final, :reserved_final])))
    assert MapSet.new(events, & &1.topology) == MapSet.new([lifecycle_topology(topology)])
  end

  defp assert_first_compaction_window_close!(topology, mode) do
    put_owner_forwarding!(topology == :owner_forwarded)
    attach_admission_lifecycle!()
    label = "first-window-close-#{topology}-#{mode}"
    turn_id = "#{label}-turn"
    compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-#{label}"}

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (compaction v2-shaped full-history frames; the provider's close of the connection between a first compaction's collection and its authorization, findings#270 row 270-200)
        FakeUpstream.strict_sequence([
          window_request(1, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_first_window_anchor", @anchor_usage)])),
          window_request(1, [forbidden: ["previous_response_id"]], event_frames(compaction_events(compact_item, "resp_first_window_compact"))),
          window_request(2, [forbidden: ["previous_response_id"]], event_frames([completed_event("resp_first_window_final", @resumed_usage)]))
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    if mode == "lite", do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    state = callback_socket!(setup, label)

    try do
      history = [window_message("synthetic first compaction anchor")]
      state = run_turn!(state, mid_turn_payload(setup, history, turn_id, "turn", 1))
      hold = hold_settled_websocket_turn!()
      assert {:ok, state} = CodexResponsesSocket.handle_in({mid_turn_payload(setup, history ++ [%{"type" => "compaction_trigger"}], turn_id, "compaction", 1), [opcode: :text]}, state)
      Process.put(:crossing_socket_state, state)
      close_held_compaction_connection!(upstream, hold, setup, state)

      {state, frames} = collect_until_idle!(state, [])
      Process.put(:crossing_socket_state, state)
      assert [%{"type" => "response.output_item.done", "item" => ^compact_item}, %{"type" => "response.completed", "response" => %{"output" => [^compact_item]}}] = frames

      {state, frames} = run_turn_frames!(state, mid_turn_payload(setup, [compact_item, window_message("synthetic final")], turn_id, "turn", 2))
      assert [%{"type" => "response.completed", "response" => %{"id" => "resp_first_window_final"}}] = frames

      assert_window_accounting!(setup, [@anchor_usage, @compact_usage, @resumed_usage])
      assert [first, _compaction_request, final_request] = FakeUpstream.requests(upstream)
      assert lite_context?(first) == (mode == "lite")
      assert compact_item in final_request.json["input"]
      assert_first_compaction_ended!(topology, state, setup)
      assert :ok = FakeUpstream.verify!(upstream)
    after
      CodexResponsesSocket.terminate(:closed, Process.delete(:crossing_socket_state))
    end
  end

  # No final was armed or reserved for the compaction, and nothing of it is
  # left: the direct session keeps no result or admission of the closed
  # connection; the owner ended the admission it authorized on that connection
  # as `connection_closed`. Every lifecycle event names the arm's topology.
  defp assert_first_compaction_ended!(topology, state, setup) do
    events = drain_lifecycle([])
    refute Enum.any?(events, &(&1.to in [:pending_final, :reserved_final]))
    assert MapSet.new(events, & &1.topology) == MapSet.new([lifecycle_topology(topology)])

    case topology do
      :direct ->
        session_state = :sys.get_state(state.upstream_websocket_session)
        refute Map.has_key?(session_state, :closed_connection_first_compact)
        refute Map.has_key?(session_state, :closed_connection_collection)

      :owner_forwarded ->
        assert [%{reason: :connection_closed, generation: 1}] = Enum.filter(events, &(&1.operation == :clear and &1.from == :collected_unconfirmed))
        assert [%{first_compact_result: nil}] = Enum.map(live_owners(setup), &:sys.get_state/1)
    end
  end

  # Holds the next request right before it goes upstream: the egress
  # observation fires in its task after its admission and accounting, just
  # before the payload is handed to the upstream session (directly, or through
  # the owner).
  defp hold_before_egress! do
    hold = make_ref()
    CodexPooler.TestAppEnv.restore_on_exit(:permanent_full_mode_egress_observation_enabled)
    Application.put_env(:codex_pooler, :permanent_full_mode_egress_observation_enabled, true)
    handler_id = {__MODULE__, :egress_hold, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{hold: hold, test: self(), claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :gateway, :upstream, :permanent_full_mode_egress_observation], &__MODULE__.hold_egress/4, config)
    hold
  end

  @doc false
  def hold_egress(_event, _measurements, _metadata, %{hold: hold, test: test, claimed: claimed}) do
    if :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {hold, :held, self()})

      receive do
        {^hold, :release} -> :ok
      after
        15_000 -> :ok
      end
    end

    :ok
  end

  # Holds the next compaction's response task where it redeems its runtime
  # proof: after the socket reserved the compaction's admission and before the
  # task starts the reservation's accounting.
  defp hold_at_runtime_proof! do
    hold = make_ref()
    handler_id = {__MODULE__, :runtime_proof_hold, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{hold: hold, test: self(), claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :gateway, :native_compaction, :authorization_transition], &__MODULE__.hold_runtime_proof/4, config)
    hold
  end

  @doc false
  def hold_runtime_proof(event, measurements, %{transition: :compact_runtime_proof_redeemed} = metadata, config),
    do: hold_egress(event, measurements, metadata, config)

  def hold_runtime_proof(_event, _measurements, _metadata, _config), do: :ok

  # Closes connection 1 while the held request waits, waits until the session
  # holding it has closed it (and every owner has handled its signal), then
  # lets the request go.
  defp close_connection_under_held_task!(upstream, hold, task_pid, setup, state) do
    close_ref = make_ref()
    assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
    assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000

    sessions =
      case Map.get(state, :upstream_websocket_session) do
        session when is_pid(session) -> [session]
        _owned -> Enum.map(live_owners(setup), &:sys.get_state(&1).upstream_pid)
      end

    Enum.each(sessions, &await_connection_closed!(&1, System.monotonic_time(:millisecond) + 15_000))
    Enum.each(live_owners(setup), &:sys.get_state/1)
    send(task_pid, {hold, :release})
  end

  # Closes the compaction's connection while its response task is held after
  # its settlement, waits until the session holding the connection has closed
  # it (and, with owner forwarding on, the owner has handled the session's
  # signal), then lets the task confirm the compaction.
  defp close_held_compaction_connection!(upstream, hold, setup, state) do
    assert_receive {^hold, :held, task_pid}, 15_000
    close_ref = make_ref()
    assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
    assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000

    sessions =
      case Map.get(state, :upstream_websocket_session) do
        session when is_pid(session) -> [session]
        _owned -> Enum.map(live_owners(setup), &:sys.get_state(&1).upstream_pid)
      end

    Enum.each(sessions, &await_connection_closed!(&1, System.monotonic_time(:millisecond) + 15_000))
    # The session sent its signal before this read of each owner's state.
    Enum.each(live_owners(setup), &:sys.get_state/1)
    release_settled_websocket_turn(hold, task_pid)
  end

  # The session holds no connection once its close handler ran: no message
  # marks it, so its state is polled on a monotonic deadline.
  defp await_connection_closed!(session, deadline) do
    cond do
      not Map.has_key?(:sys.get_state(session), :conn) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the upstream session never closed the compaction's connection")

      true ->
        receive do
        after
          1 -> await_connection_closed!(session, deadline)
        end
    end
  end

  defp run_turn_frames!(state, frame) do
    assert {:ok, state} = CodexResponsesSocket.handle_in({frame, [opcode: :text]}, state)
    Process.put(:crossing_socket_state, state)
    {state, frames} = collect_until_idle!(state, [])
    Process.put(:crossing_socket_state, state)
    {state, frames}
  end

  defp receive_owner_word! do
    receive do
      {:websocket_owner_upstream_closed, _correlation_id, _epoch, _signal} = word -> word
    after
      15_000 -> flunk("the owner never passed the late close signal on")
    end
  end

  # Every request of the turn settled once for its own usage: one succeeded
  # attempt, its reservation released by one settlement, nothing provisional
  # and no second compaction.
  defp assert_window_accounting!(setup, usages) do
    assert await_settled_rows!(setup) == [
             {"/backend-api/codex/responses", "websocket", "succeeded"},
             {"/backend-api/codex/responses/compact", "websocket", "succeeded"},
             {"/backend-api/codex/responses", "websocket", "succeeded"}
           ]

    requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: request.admitted_at))

    for {request, usage} <- Enum.zip(requests, usages) do
      assert [%Attempt{status: "succeeded"}] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^request.id))
      assert ledger_entry_kinds(request) == ["release", "reservation", "settlement"]
      assert key_usage_events(request.id) == %{known: usage["total_tokens"], provisional: 0, admissions: 1}
    end
  end

  defp live_owners(setup) do
    for session <- Repo.all(from(session in CodexSession, where: session.pool_id == ^setup.pool.id)),
        {:ok, owner} <- [WebsocketOwnerSession.lookup(session.id)],
        do: owner
  end

  defp window_request(connection_ordinal, json, respond) do
    json = Keyword.update(Keyword.merge([valid: true], json), :equals, %{"type" => "response.create"}, &Map.put(&1, "type", "response.create"))
    FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: connection_ordinal, json: Keyword.put_new(json, :equals, %{"type" => "response.create"}), respond: respond)
  end

  defp window_message(content), do: %{"type" => "message", "role" => "user", "content" => content}

  defp lite_context?(%{json: %{"input" => [%{"type" => "additional_tools"} | _input]}}), do: true
  defp lite_context?(_request), do: false

  defp window_compaction_frame(setup, turn_id, anchor) do
    setup
    |> mid_turn_payload([%{"type" => "custom_tool_call_output", "call_id" => "call_#{turn_id}", "output" => "synthetic tool output"}, %{"type" => "compaction_trigger"}], turn_id, "compaction", 1)
    |> CodexPooler.JSON.decode!()
    |> Map.put("previous_response_id", anchor)
    |> CodexPooler.JSON.encode!()
  end

  defp completed_event(response_id, usage),
    do: %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => usage}}

  defp compaction_events(item, response_id) do
    [
      %{"type" => "response.output_item.done", "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => @compact_usage}}
    ]
  end

  # A native socket driven through its callbacks: this process is the socket.
  defp callback_socket!(setup, turn_state) do
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {:ok, state} = CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: turn_state, accepted_turn_state: turn_state, client_ip: "127.0.0.1"}})
    Process.put(:crossing_socket_state, state)
    state
  end

  # Runs one turn and requires what a served turn shows its client: no error
  # frame, and a success terminal last. It used to drop the frames, so a turn
  # that ended in an error passed as served (findings#281).
  defp run_turn!(state, frame) do
    {state, frames} = run_turn_frames!(state, frame)
    refute Enum.any?(frames, &match?(%{"type" => "error"}, &1)), "the turn pushed an error: #{inspect(frames)}"
    assert %{"type" => terminal} = List.last(frames)
    assert terminal in ["response.completed", "response.done"]
    state
  end

  # Closes the provider connection the turns so far ran on and returns the
  # message that tells the socket, still unhandled: its own session's signal
  # with owner forwarding off, the owner's word with it on.
  defp close_connection_1!(upstream) do
    close_ref = make_ref()
    assert :ok = FakeUpstream.close_websocket_connection(upstream, 1, close_ref: close_ref, notify: self(), code: 1000, reason: "synthetic age limit")
    assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 15_000

    receive do
      {:upstream_websocket_connection_closed, _session, _signal} = message -> message
      {:websocket_owner_upstream_closed, _correlation_id, _epoch, _signal} = message -> message
    after
      15_000 -> flunk("the socket was never told that the upstream connection closed")
    end
  end

  # The admission bound to the closed connection ended with that connection,
  # whoever held it: the direct session when it closed the connection, the
  # owner on its session's signal (findings#274). Every lifecycle event so
  # far names the arm's topology, the events of an owner's upstream session
  # included (findings#270 row 270-163).
  defp assert_closed_connection_admission_cleared!(pending_final, topology) do
    assert_receive {:admission_lifecycle, %{operation: :clear, from: :pending_final, to: :cleared} = clear}, 15_000
    assert {clear.reason, clear.lifecycle_id, clear.generation} == {:connection_closed, pending_final.lifecycle_id, pending_final.generation}
    assert MapSet.new([pending_final, clear | drain_lifecycle([])], & &1.topology) == MapSet.new([lifecycle_topology(topology)])
  end

  defp lifecycle_topology(:direct), do: :direct
  defp lifecycle_topology(:owner_forwarded), do: :forwarded

  defp drain_lifecycle(acc) do
    receive do
      {:admission_lifecycle, event} -> drain_lifecycle([event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp mid_turn_payload(setup, input, turn_id, request_kind, window_number) do
    metadata =
      %{"turn_id" => turn_id, "window_id" => "mid-turn-crossing-window-#{window_number}", "context_window_id" => "00000000-0000-4000-8000-00000000028#{window_number}", "window_number" => window_number, "request_kind" => request_kind}
      |> then(&if(request_kind == "compaction", do: Map.put(&1, "compaction", %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "mid_turn", "strategy" => "memento"}), else: &1))
      |> CodexPooler.JSON.encode!()

    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "input" => input,
      "stream" => true,
      "generate" => true,
      "client_metadata" => %{"turn_id" => turn_id, "x-codex-turn-metadata" => metadata}
    })
  end

  defp event_frames(events), do: FakeUpstream.websocket_text_frames(Enum.map(events, &CodexPooler.JSON.encode!/1))

  # Every admission transition of an owner or of a socket's own upstream
  # session is reported on this event, so the test waits on transitions
  # instead of on time.
  defp attach_admission_lifecycle! do
    handler_id = {__MODULE__, self(), make_ref()}
    :ok = :telemetry.attach(handler_id, @lifecycle_event, &__MODULE__.relay_admission_lifecycle/4, self())
    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  @doc false
  def relay_admission_lifecycle(_event, _measurements, metadata, test_pid) do
    send(
      test_pid,
      {:admission_lifecycle,
       %{
         operation: metadata.operation,
         reason: metadata.reason,
         from: metadata.phase_from,
         to: metadata.phase_to,
         lifecycle_id: metadata.native_lifecycle_id,
         generation: metadata.generation,
         topology: metadata.topology
       }}
    )
  end

  # The admission each live owner of the Pool's sessions holds now.
  defp owner_admissions(setup) do
    for session <- Repo.all(from(session in CodexSession, where: session.pool_id == ^setup.pool.id)),
        {:ok, owner} <- [WebsocketOwnerSession.lookup(session.id)],
        do: :sys.get_state(owner).native_compaction_admission
  end

  defp bound_to?(%NativeCompactionAdmission{binding: %{lifecycle_id: lifecycle_id, generation: generation}}, %{lifecycle_id: lifecycle_id, generation: generation}), do: true
  defp bound_to?(_admission, _connection), do: false

  defp put_owner_forwarding!(enabled?) do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)

    on_exit(fn ->
      case previous do
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
      end
    end)
  end

  defp turn_frame(setup) do
    setup
    |> frame([%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic post-turn prompt"}]}])
    |> put_in(["client_metadata", "x-codex-turn-metadata"], turn_metadata(:turn))
    |> CodexPooler.JSON.encode!()
  end

  # The shared websocket receive helper drops every non-socket message, so the
  # request events cannot be awaited; the rows are the authority (bounded poll,
  # no completion signal survives the receive loop).
  defp await_settled_rows!(setup) do
    await_settled_rows!(setup, System.monotonic_time(:millisecond) + 15_000)
  end

  defp await_settled_rows!(setup, deadline) do
    rows =
      Repo.all(
        from(request in Request,
          where: request.pool_id == ^setup.pool.id,
          order_by: request.admitted_at,
          select: {request.endpoint, request.transport, request.status}
        )
      )

    if Enum.any?(rows, &match?({_endpoint, _transport, "in_progress"}, &1)) and
         System.monotonic_time(:millisecond) <= deadline do
      Process.sleep(10)
      await_settled_rows!(setup, deadline)
    else
      rows
    end
  end

  # The resumed process's prewarm on the rotated window: no input, no
  # generation, prewarm turn metadata.
  defp resume_prewarm_frame(setup) do
    setup
    |> frame([])
    |> Map.put("generate", false)
    |> put_in(["client_metadata", "turn_id"], @resumed_turn_id)
    |> put_in(["client_metadata", "root_turn_id"], @resumed_turn_id)
    |> put_in(["client_metadata", "x-codex-window-id"], @resumed_window_id)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], turn_metadata(:resume_prewarm))
    |> CodexPooler.JSON.encode!()
  end

  # The resumed turn replays the history with the compaction item in place of
  # what it compacted, under a new turn id on the rotated window.
  defp resumed_turn_frame(setup, compact_item) do
    history = [
      %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic post-turn prompt"}]},
      compact_item,
      %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic resumed prompt"}]}
    ]

    setup
    |> frame(history)
    |> put_in(["client_metadata", "turn_id"], @resumed_turn_id)
    |> put_in(["client_metadata", "root_turn_id"], @resumed_turn_id)
    |> put_in(["client_metadata", "x-codex-window-id"], @resumed_window_id)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], turn_metadata(:resumed_turn))
    |> CodexPooler.JSON.encode!()
  end

  # The next post-turn compaction, anchored on the resumed turn's response.
  defp resumed_post_turn_frame(setup) do
    metadata =
      common_turn_metadata()
      |> Map.merge(resumed_window_metadata())
      |> Map.merge(%{
        "root_turn_id" => @resumed_turn_id,
        "request_kind" => "compaction",
        "compaction" => %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "post_turn", "strategy" => "memento"}
      })

    setup
    |> frame([%{"type" => "compaction_trigger"}])
    |> Map.put("previous_response_id", @resumed_response_id)
    |> put_in(["client_metadata", "turn_id"], @resumed_turn_id)
    |> put_in(["client_metadata", "root_turn_id"], @resumed_turn_id)
    |> put_in(["client_metadata", "x-codex-window-id"], @resumed_window_id)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], CodexPooler.JSON.encode!(metadata))
    |> CodexPooler.JSON.encode!()
  end

  defp post_turn_frame(setup) do
    setup
    |> frame([%{"type" => "compaction_trigger"}])
    |> Map.put("previous_response_id", @anchor_response_id)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], turn_metadata(:post_turn_compaction))
    |> CodexPooler.JSON.encode!()
  end

  # Top-level keys of the released client's websocket `response.create`
  # (prewarm aside, it sends no `generate`).
  defp frame(setup, input) do
    %{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => input,
      "tools" => [],
      "tool_choice" => "auto",
      "parallel_tool_calls" => true,
      "reasoning" => %{"effort" => "low"},
      "store" => false,
      "stream" => true,
      "include" => ["reasoning.encrypted_content"],
      "text" => %{"verbosity" => "low"},
      "prompt_cache_key" => @thread_id,
      "client_metadata" => %{
        "session_id" => @session_id,
        "thread_id" => @thread_id,
        "turn_id" => @turn_id,
        "root_turn_id" => @turn_id,
        "x-codex-installation-id" => @installation_id,
        "x-codex-window-id" => @window_id,
        "x-codex-ws-stream-request-start-ms" => "1790000000000"
      }
    }
  end

  defp turn_metadata(:turn) do
    common_turn_metadata()
    |> Map.merge(%{"request_kind" => "turn", "model" => "gpt-test-model", "reasoning_effort" => "low"})
    |> CodexPooler.JSON.encode!()
  end

  defp turn_metadata(:post_turn_compaction) do
    common_turn_metadata()
    |> Map.merge(%{
      "request_kind" => "compaction",
      "compaction" => %{
        "trigger" => "auto",
        "reason" => "context_limit",
        "implementation" => "responses_compaction_v2",
        "phase" => "post_turn",
        "strategy" => "memento"
      }
    })
    |> CodexPooler.JSON.encode!()
  end

  defp turn_metadata(:resume_prewarm) do
    common_turn_metadata()
    |> Map.drop(["root_turn_id", "turn_started_at_unix_ms", "turn_trigger"])
    |> Map.merge(resumed_window_metadata())
    |> Map.merge(%{"request_kind" => "prewarm", "model" => "gpt-test-model", "reasoning_effort" => "low"})
    |> CodexPooler.JSON.encode!()
  end

  defp turn_metadata(:resumed_turn) do
    common_turn_metadata()
    |> Map.merge(resumed_window_metadata())
    |> Map.merge(%{"request_kind" => "turn", "root_turn_id" => @resumed_turn_id, "model" => "gpt-test-model", "reasoning_effort" => "low"})
    |> CodexPooler.JSON.encode!()
  end

  defp resumed_window_metadata, do: %{"turn_id" => @resumed_turn_id, "window_id" => @resumed_window_id, "window_number" => 1}

  defp common_turn_metadata do
    %{
      "agent_name" => "/root",
      "analytics_enabled" => true,
      "auto_review_enabled" => false,
      "context_window_id" => @context_window_id,
      "installation_id" => @installation_id,
      "node_repl_auto_review_required" => false,
      "node_repl_disabled" => false,
      "root_turn_id" => @turn_id,
      "sandbox" => "seccomp",
      "sandbox_mode" => "read-only",
      "session_id" => @session_id,
      "thread_id" => @thread_id,
      "thread_source" => "user",
      "turn_id" => @turn_id,
      "turn_started_at_unix_ms" => 1_790_000_000_000,
      "turn_trigger" => "exec",
      "window_id" => @window_id,
      "window_number" => 0
    }
  end

  defp completed_frames(response_id, usage \\ %{"input_tokens" => 20_000, "output_tokens" => 1, "total_tokens" => 20_001}) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{
          "id" => response_id,
          "status" => "completed",
          "output" => [],
          "usage" => usage
        }
      })
    ])
  end

  defp compaction_frames(item, usage \\ nil) do
    response = %{"id" => @compact_response_id, "status" => "completed", "output" => [item]}

    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => item}),
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => if(usage, do: Map.put(response, "usage", usage), else: response)
      })
    ])
  end
end
