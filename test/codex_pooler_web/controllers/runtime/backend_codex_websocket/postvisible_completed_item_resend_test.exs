defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.PostvisibleCompletedItemResendTest do
  # A native websocket turn cut after its socket pushed the client a completed
  # item (`response.output_item.done`) and no terminal. The released Codex
  # client records that item in its history and resends the turn under the same
  # turn id as the original request with the item appended (measured with Codex
  # 0.156.1 through a recording proxy: the resend is the original's items plus
  # the completed item, re-serialized without its `status` and its content
  # parts' `annotations` and `logprobs`). A direct provider serves it; the
  # Pooler's `codex-turn:` fence refused it `409 duplicate_turn` on every
  # websocket retry and over HTTPS, and the turn failed (findings#232 row
  # 232-232). The grown resend is now admitted as one linked successor when it
  # is exactly the predecessor plus the completed items its delivery receipt
  # names; an appended item that is not one of them, or an extra item, keeps
  # the fence.
  #
  # Everything runs through the real listener and the real owner (forwarding
  # on) or direct task (forwarding off); the fake provider holds its stream at a
  # frame barrier right after the completed item, so what the socket pushed is
  # exactly the cut shape. Only the resend timing is simplified: it is sent once
  # the original settled and its receipt was recorded (the released client's
  # own retries are the real-client lanes' job).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, public_websocket_connect!: 3, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_peer_session_owner!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.CleanupProofRace

  @timeout_ms 15_000
  @poll_ms 100
  # provenance: observed findings#232 row 232-231 (the released client's Lite websocket frame carries the marker in client_metadata, its HTTPS fallback a header)
  @websocket_lite_marker "ws_request_header_x_openai_internal_codex_responses_lite"
  # With owner forwarding off the closing socket used to leave a direct task
  # that had pushed a completed item running for its whole 5 s post-cleanup
  # grace after the 250 ms drain, so with the provider held its receipt could
  # not exist before about 5.25 s, and the original generation ran beside the
  # served successor (findings#232 row 232-257); such a task is now stopped at
  # the cleanup, like the owner cancels its active turn at the detach. The
  # budget sits below that floor and far above the stop's own cost.
  @stopped_receipt_budget_ms 4_000
  # The frames up to and including the completed item, as the released client
  # received them before the cut (findings#232 row 232-232).
  @item_text "completed answer"

  # Current released Codex starts a new turn as an anchored suffix on the
  # existing provider socket, then resends full history after mailbox preemption.
  # The durable predecessor therefore contains only the suffix claim.
  for forwarding <- [false, true, :peer], provider <- [:held, :completes] do
    @tag anchored_mailbox_tail: true, forwarding: forwarding, provider: provider
    test "#{forwarding} #{provider}: an anchored new-turn suffix recovers as full history plus retained reasoning and mailbox", %{forwarding: forwarding, provider: provider} do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding != false)
      release_ref = make_ref()
      anchor = "resp_anchored_mailbox_prelude"

      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            native_request(FakeUpstream.websocket_text_frames(stream_frames(anchor))),
            FakeUpstream.expect_request(method: "WEBSOCKET", websocket_connection_ordinal: 1, json: [equals: %{"previous_response_id" => anchor}], respond: FakeUpstream.barrier_websocket_frames(reasoning_stream_frames(), notify: self(), release_ref: release_ref)),
            successor_request(:websocket)
          ])
        )

      setup = topology_setup(upstream, forwarding)
      turn_state = Ecto.UUID.generate()
      peer = if forwarding == :peer, do: start_peer_session_owner!(setup, %{accepted_turn_state: turn_state})
      payload = native_turn_payload(Ecto.UUID.generate(), setup.model.exposed_model_id) |> mailbox_resume_payload(:opening)
      prelude = anchored_turn(payload, "synthetic-prior-turn")
      tail = %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic next turn"}]}
      anchored = payload |> anchored_turn("synthetic-next-turn") |> Map.put("previous_response_id", anchor) |> Map.put("input", [tail])
      port = start_public_endpoint!()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(prelude))
      {conn, terminal} = receive_terminal!(conn, websocket, ref)
      assert terminal["type"] == "response.completed"
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(anchored))

      for ordinal <- 0..2 do
        assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @timeout_ms
        :ok = FakeUpstream.release_frame(upstream, release_ref)
      end

      assert_receive {:fake_upstream_frame_barrier, 3, _handler, ^release_ref}, @timeout_ms
      conn = receive_until!(conn, websocket, ref, "response.output_item.done")
      predecessor = List.last(pool_requests(setup.pool.id))
      assert_peer_forwarding(predecessor, peer)
      _receipt = close_and_await_receipt!(conn, predecessor.id, provider, upstream, release_ref)
      full_history = prelude["input"] ++ terminal["response"]["output"] ++ [tail]
      candidate = anchored |> Map.delete("previous_response_id") |> Map.put("input", full_history) |> resend_payload(:mailbox)

      controls = [
        Map.put(candidate, "instructions", "synthetic changed instructions"),
        update_in(candidate["input"], &List.replace_at(&1, length(full_history) - 1, Map.put(tail, "content", "synthetic changed tail"))),
        update_in(candidate["input"], &List.update_at(&1, length(full_history), fn item -> Map.put(item, "encrypted_content", "synthetic changed output") end))
      ]

      for changed <- controls, do: assert_mailbox_refused!(resend!(:websocket, port, setup, turn_state, changed))
      assert FakeUpstream.count(upstream) == 2
      assert %{"type" => "response.completed"} = resend!(:websocket, port, setup, turn_state, candidate)
      await_all_settled!(setup.pool.id, System.monotonic_time(:millisecond) + @timeout_ms)
      assert [prior, original, successor] = pool_requests(setup.pool.id)
      assert original.id == predecessor.id
      assert original.status == if(provider == :held, do: "failed", else: "succeeded")
      assert successor.status == "succeeded"
      assert_one_settlement_each!([prior.id, original.id, successor.id])
      assert Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^original.id, select: link.successor_request_id)) == [successor.id]
      assert Repo.all(from(turn in CodexTurn, where: turn.request_id in ^[prior.id, original.id, successor.id], select: turn.codex_session_id)) |> Enum.uniq() |> length() == 1
      assert [first, second, third] = FakeUpstream.requests(upstream)
      assert second.websocket_connection_id == first.websocket_connection_id
      assert second.json["input"] == [tail]
      assert third.json["input"] == candidate["input"]
      refute Map.has_key?(third.json, "previous_response_id")

      if provider == :held do
        refute FakeUpstream.websocket_connection_alive?(upstream, second.websocket_connection_id)
        :ok = FakeUpstream.acknowledge(upstream, {:frame_barrier, release_ref, 4})
      end

      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  defp anchored_turn(payload, turn_id) do
    payload
    |> put_in(["client_metadata", "turn_id"], turn_id)
    |> update_in(["client_metadata", "x-codex-turn-metadata"], fn metadata -> metadata |> CodexPooler.JSON.decode!() |> Map.put("turn_id", turn_id) |> CodexPooler.JSON.encode!() end)
  end

  for forwarding <- [true, false, :peer] do
    for transport <- [:websocket, :https], role <- [:opening, :local_summary, :remote_resume], provider <- [:held, :completes] do
      @tag forwarding: forwarding, transport: transport, role: role, provider: provider, post_compaction_mailbox: true
      @tag slow: "a Full post-compaction reasoning cut, mailbox continuation and duplicate control through the real socket"
      test "owner forwarding #{forwarding}: a #{role} mailbox continuation after provider #{provider} is served once over #{transport}", %{forwarding: forwarding, transport: transport, role: role, provider: provider} do
        contract = CompatibilityMatrix.by_slug!(:duplicate_turn_fence).duplicate_turn.mailbox_resume
        %{setup: setup, upstream: upstream, request_id: request_id, resend: resend, receipt: receipt, port: port, turn_state: turn_state, resend_payload: payload} = scenario!(forwarding, provider, :mailbox, transport, mailbox_role: role, negative_mailbox_controls: true)

        case transport do
          :websocket -> assert {resend["type"], get_in(resend, ["error", "code"])} == {"response.completed", nil}
          :https -> assert {200, _body} = resend
        end

        assert %{"completed_items" => 1, "highest_frame_class" => "item_done", "terminal_class" => "none"} = receipt
        requests = pool_requests(setup.pool.id)
        expected_count = if role == :local_summary, do: 3, else: 2
        assert length(requests) == expected_count
        assert [%Request{id: ^request_id, correlation_id: original_claim} = original, %Request{id: successor_id, status: "succeeded", correlation_id: continuation_claim}] = Enum.take(requests, -2)
        assert original.status == if(provider == :held, do: "failed", else: "succeeded")
        if provider == :held, do: assert(original.last_error_code == "client_disconnected")
        assert String.starts_with?(original_claim, if(role == :opening, do: "codex-turn:", else: contract.original_prefix))
        assert [%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^request_id)
        assert_one_settlement_each!(Enum.map(requests, & &1.id))
        assert String.starts_with?(continuation_claim, contract.successor_prefix)
        assert original_claim != continuation_claim
        assert FakeUpstream.count(upstream) == expected_count

        changed = update_in(payload["input"], fn input -> input ++ [%{"type" => "message", "role" => "assistant", "content" => "changed history"}] end)

        case resend!(transport, port, setup, turn_state, changed) do
          %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} -> :ok
          {409, body} -> assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
          _unexpected -> flunk("the changed mailbox continuation was not fenced")
        end

        assert length(pool_requests(setup.pool.id)) == expected_count
        assert FakeUpstream.count(upstream) == expected_count
      end
    end
  end

  for forwarding <- [true, :peer] do
    @tag forwarding: forwarding, mailbox_chain: true
    @tag slow: "two real mailbox item cuts through the owner followed by one completed successor and duplicate control"
    test "owner forwarding #{forwarding}: two ordinary mailbox cuts preserve the exact predecessor chain", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: first_id, resend: resend, port: port, turn_state: turn_state, resend_payload: payload} = scenario!(forwarding, :held, :mailbox, :websocket, mailbox_role: :opening, successor_cut: true)
      assert resend["type"] == "response.completed"
      assert [%Request{id: ^first_id, status: "failed"}, %Request{id: second_id, status: "failed"}, %Request{id: third_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id in ^[first_id, second_id], select: {l.predecessor_request_id, l.successor_request_id}) |> Enum.sort() == Enum.sort([{first_id, second_id}, {second_id, third_id}])
      assert_one_settlement_each!([first_id, second_id, third_id])
      assert %{"terminal_class" => "response.completed"} = await_receipt!(third_id, System.monotonic_time(:millisecond) + @timeout_ms)
      changed = Map.update!(payload, "input", &(&1 ++ [client_recorded_item("synthetic changed history")]))
      assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = resend!(:websocket, port, setup, turn_state, changed)
      assert FakeUpstream.count(upstream) == 3
      assert %{"type" => "response.completed"} = resend!(:websocket, port, setup, turn_state, payload)
      await_all_settled!(setup.pool.id, System.monotonic_time(:millisecond) + @timeout_ms)
      requests = pool_requests(setup.pool.id)
      assert length(requests) == 4
      fourth = List.last(requests)
      assert [%RequestClientRetryLink{predecessor_request_id: ^third_id, successor_request_id: fourth_id}] = Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^third_id)
      assert fourth.id == fourth_id
      assert fourth.status == "succeeded"
      assert_one_settlement_each!(Enum.map(requests, & &1.id))
      assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = resend!(:websocket, port, setup, turn_state, changed)
      assert length(pool_requests(setup.pool.id)) == 4
      assert FakeUpstream.count(upstream) == 4
    end
  end

  for forwarding <- [true, false], transport <- [:websocket, :https] do
    @tag forwarding: forwarding, transport: transport
    @tag slow: "real item-done cut followed by a second socket or HTTPS resend"
    test "an identical resend after an unread completed item is chained (#{forwarding}, #{transport})", %{forwarding: forwarding, transport: transport} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :held, :identical, transport)

      case transport do
        :websocket -> assert resend["type"] == "response.completed"
        :https -> assert {200, _body} = resend
      end

      assert [%Request{id: ^request_id}, %Request{id: successor_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert [%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink)
      assert_one_settlement_each!([request_id, successor_id])
      assert FakeUpstream.count(upstream) == 2
    end
  end

  for forwarding <- [true, false], code <- ["owner_drained", "dead_execution_recovered"] do
    @tag forwarding: forwarding, code: code
    @tag slow: "real item-done socket cut with a metadata-only terminal settlement variant and grown resend"
    test "a grown resend keeps retained items after #{code} (#{forwarding})", %{forwarding: forwarding, code: code} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :held, :grown, :websocket, settlement_variant: code)
      assert resend["type"] == "response.completed"
      assert [%Request{id: ^request_id, last_error_code: ^code}, %Request{id: successor_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert [%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink)
      assert FakeUpstream.count(upstream) == 2
    end
  end

  for forwarding <- [true, false] do
    @tag forwarding: forwarding
    @tag slow: "a real socket cut after a completed item, its owner or direct cleanup, the recorded receipt and a second socket's resend"
    test "owner forwarding #{forwarding}: the grown resend after a completed item whose provider was still generating is served as one successor", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend, receipt: receipt} = scenario!(forwarding, :held, :grown)

      assert resend["type"] == "response.completed"
      assert [%Request{id: ^request_id, status: "failed", last_error_code: "client_disconnected"}, %Request{id: successor_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert %CodexTurn{status: "interrupted", first_visible_output_at: %DateTime{}} = Repo.get_by!(CodexTurn, request_id: request_id)
      assert [%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink)
      assert_one_settlement_each!([request_id, successor_id])
      # The cut generation was stopped (the owner's detach, or with forwarding off
      # the closing socket's cleanup), so the provider released afterwards never
      # completed it beside the served successor: no late answer corrected the
      # original's settlement (findings#232 row 232-257).
      assert Repo.all(from(l in LedgerEntry, where: l.request_id == ^request_id and l.amount_status == "voided")) == []
      assert %Request{usage_status: "usage_unknown"} = Repo.get!(Request, request_id)
      # Lite rewrites what reaches the provider; the successor carries the original's items plus the completed item.
      assert [%{json: %{"input" => original_input}}, %{json: %{"input" => successor_input}}] = FakeUpstream.requests(upstream)
      assert successor_input == original_input ++ [client_recorded_item(@item_text)]
      assert %{"outcome" => "aborted", "terminal_class" => "none", "highest_frame_class" => "item_done", "completed_items" => 1, "completed_item_digests" => [digest]} = receipt
      assert digest =~ ~r/\A[0-9a-f]{12}\z/
    end

    @tag forwarding: forwarding
    @tag slow: "a real socket cut after a completed item, the provider completing after it, the recorded receipt and a second socket's resend"
    test "owner forwarding #{forwarding}: the grown resend after a completed item the provider completed afterwards is served as one successor", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :completes, :grown)

      assert resend["type"] == "response.completed"
      assert [%Request{id: ^request_id, status: "succeeded"}, %Request{id: successor_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert [%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink)
      assert_one_settlement_each!([request_id, successor_id])
      assert FakeUpstream.count(upstream) == 2
    end

    @tag forwarding: forwarding
    @tag slow: "a real socket cut after a completed item, its cleanup, the recorded receipt and the HTTPS fallback resend"
    test "owner forwarding #{forwarding}: the HTTPS fallback of the grown resend is served as one successor", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :held, :grown, :https)

      assert {200, body} = resend
      assert body =~ "response.completed"
      assert [%Request{id: ^request_id, status: "failed"}, %Request{id: successor_id, status: "succeeded", transport: "http_sse"}] = pool_requests(setup.pool.id)
      assert [%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink)
      assert_one_settlement_each!([request_id, successor_id])
      assert FakeUpstream.count(upstream) == 2
    end

    # The appended item is not the one the socket pushed (its text differs), so
    # the resend is not the grown resend of this turn.
    @tag forwarding: forwarding
    @tag slow: "a real socket cut after a completed item, its cleanup, the recorded receipt and a second socket's resend"
    test "owner forwarding #{forwarding}: a resend whose appended item is not the pushed completed item stays a duplicate", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :held, :mismatched)

      assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = resend
      assert [%Request{id: ^request_id}] = pool_requests(setup.pool.id)
      assert Repo.all(RequestClientRetryLink) == []
      assert FakeUpstream.count(upstream) == 1
    end

    @tag forwarding: forwarding
    @tag slow: "a real socket cut after a completed item, its cleanup, the recorded receipt and a second socket's resend"
    test "owner forwarding #{forwarding}: a resend that appends more than the pushed completed items stays a duplicate", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :held, :extra)

      assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = resend
      assert [%Request{id: ^request_id}] = pool_requests(setup.pool.id)
      assert Repo.all(RequestClientRetryLink) == []
      assert FakeUpstream.count(upstream) == 1
    end

    @tag forwarding: forwarding
    @tag slow: "a real socket cut after a completed item, its cleanup, the recorded receipt and the HTTPS fallback resend"
    test "owner forwarding #{forwarding}: the HTTPS fallback whose appended item is not the pushed completed item stays a duplicate", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend} = scenario!(forwarding, :held, :mismatched, :https)

      assert {409, body} = resend
      assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
      assert [%Request{id: ^request_id}] = pool_requests(setup.pool.id)
      assert FakeUpstream.count(upstream) == 1
    end
  end

  # The closing socket stops the direct task that pushed the completed item,
  # then interrupts its request `client_disconnected`. When the stopped task's
  # terminal proof lands between the two (the production publisher, a few
  # milliseconds, `CleanupProofRace`), the interrupt used to take the stop for
  # a lost executor and settle `dead_execution_recovered`. The continuation
  # and the grown resend are admitted against the disconnect and its receipt,
  # so they were refused `duplicate_turn` and the turn was lost (findings#270
  # row 270-353). The socket knows why the task ended, so the request keeps
  # its reason and the resend is served.
  for shape <- [:mailbox, :grown], transport <- [:websocket, :https] do
    @tag shape: shape, transport: transport
    @tag slow: "a real socket cut after a completed item, the stopped task's proof published before the cleanup interrupts, and the resend"
    test "owner forwarding false: the #{shape} #{transport} resend is served once when the stopped task's end is proven before the cleanup interrupts it", %{shape: shape, transport: transport} do
      %{setup: setup, upstream: upstream, request_id: request_id, resend: resend, proven_attempt_id: attempt_id} =
        scenario!(false, :held, shape, transport, proof_before_cleanup: true)

      assert ExecutionTerminalProofs.terminal?(Repo.get!(Attempt, attempt_id))

      case transport do
        :websocket -> assert resend["type"] == "response.completed"
        :https -> assert {200, _body} = resend
      end

      assert [%Request{id: ^request_id, status: "failed", response_status_code: 499, last_error_code: "client_disconnected"}, %Request{id: successor_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert %CodexTurn{status: "interrupted", error_code: "client_disconnected"} = Repo.get_by!(CodexTurn, request_id: request_id)
      if shape == :grown, do: assert([%RequestClientRetryLink{predecessor_request_id: ^request_id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink))
      assert FakeUpstream.count(upstream) == 2
    end
  end

  defp scenario!(forwarding, provider, resend_shape, resend_transport \\ :websocket, opts \\ []) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding != false)
    release_ref = make_ref()
    mailbox? = resend_shape == :mailbox
    frames = if mailbox?, do: reasoning_stream_frames(), else: stream_frames("resp_completed_item_original")
    hold_at = length(frames) - 1
    mailbox_role = Keyword.get(opts, :mailbox_role, :remote_resume)
    local_resume? = mailbox? and mailbox_role == :local_summary
    second_cut = if Keyword.get(opts, :successor_cut, false), do: make_ref()
    upstream = start_scenario_upstream!(frames, release_ref, local_resume?, second_cut, resend_shape, resend_transport)

    setup = topology_setup(upstream, forwarding)
    if not mailbox?, do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    turn_state = Ecto.UUID.generate()
    peer = if forwarding == :peer, do: start_peer_session_owner!(setup, %{accepted_turn_state: turn_state})
    payload = native_turn_payload(Ecto.UUID.generate(), setup.model.exposed_model_id)
    payload = if mailbox?, do: mailbox_resume_payload(payload, mailbox_role), else: payload
    port = start_public_endpoint!()

    prelude_id = serve_window_prelude!(local_resume?, port, setup, turn_state, payload)

    {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))

    for ordinal <- 0..(hold_at - 1) do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @timeout_ms
      :ok = FakeUpstream.release_frame(upstream, release_ref)
    end

    # The barrier notification is read before any socket frame: the socket
    # helpers consume every message they do not recognise.
    assert_receive {:fake_upstream_frame_barrier, ^hold_at, _handler, ^release_ref}, @timeout_ms
    conn = receive_until!(conn, websocket, ref, "response.output_item.done")
    assert %Request{id: request_id} = original = List.last(pool_requests(setup.pool.id))

    assert_window_rebound!(original, prelude_id)

    if mailbox?, do: assert(original.request_metadata["routing"]["model_serving_mode"] == "full")

    assert_peer_forwarding(original, peer)

    {receipt, proven_attempt_id} = CleanupProofRace.around_cut(opts, request_id, fn -> close_and_await_receipt!(conn, request_id, provider, upstream, release_ref) end)
    _settled = await_settled!(request_id, System.monotonic_time(:millisecond) + @timeout_ms)

    apply_settlement_variant!(request_id, Keyword.get(opts, :settlement_variant))
    resend_payload = resend_payload(payload, resend_shape)
    transport_context = %{transport: resend_transport, port: port, setup: setup, turn_state: turn_state, upstream: upstream, local_resume?: local_resume?}
    assert_mailbox_controls!(Keyword.get(opts, :negative_mailbox_controls, false), transport_context, payload, resend_payload)
    resend_payload = continue_after_second_cut!(second_cut, transport_context, request_id, resend_payload)
    resend = resend!(resend_transport, port, setup, turn_state, resend_payload)

    await_all_settled!(setup.pool.id, System.monotonic_time(:millisecond) + @timeout_ms)
    %{setup: setup, upstream: upstream, request_id: request_id, resend: resend, receipt: receipt, port: port, turn_state: turn_state, resend_payload: resend_payload, proven_attempt_id: proven_attempt_id}
  end

  defp start_scenario_upstream!(frames, release_ref, local_resume?, second_cut, shape, transport) do
    prelude = if local_resume?, do: [native_request(FakeUpstream.websocket_text_frames(stream_frames("resp_window_opening")))], else: []
    responses = prelude ++ [native_request(FakeUpstream.barrier_websocket_frames(frames, notify: self(), release_ref: release_ref))] ++ scenario_successor_responses(second_cut, shape, transport)
    start_upstream(FakeUpstream.strict_sequence(responses))
  end

  defp scenario_successor_responses(second_cut, _shape, transport) when is_reference(second_cut),
    do: [native_request(FakeUpstream.barrier_websocket_frames(reasoning_stream_frames(), notify: self(), release_ref: second_cut)), successor_request(transport), successor_request(transport)]

  defp scenario_successor_responses(nil, shape, transport) when shape in [:identical, :grown, :mailbox], do: [successor_request(transport)]
  defp scenario_successor_responses(nil, _shape, _transport), do: []

  defp serve_window_prelude!(false, _port, _setup, _turn_state, _payload), do: nil

  defp serve_window_prelude!(true, port, setup, turn_state, payload) do
    opening = payload |> Map.update!("input", &Enum.take(&1, 1)) |> mailbox_window(0)
    assert %{"type" => "response.completed"} = send_and_receive_terminal!(port, setup, turn_state, CodexPooler.JSON.encode!(opening))
    assert [prior] = pool_requests(setup.pool.id)
    await_all_settled!(setup.pool.id, System.monotonic_time(:millisecond) + @timeout_ms)
    prior.id
  end

  defp assert_window_rebound!(_original, nil), do: :ok

  defp assert_window_rebound!(original, prelude_id) do
    prior = Repo.get!(Request, prelude_id)
    refute original.correlation_id == prior.correlation_id
    assert original.request_metadata["codex_session_id"] == prior.request_metadata["codex_session_id"]
  end

  defp apply_settlement_variant!(_request_id, nil), do: :ok

  defp apply_settlement_variant!(request_id, code) do
    {1, _} = Repo.update_all(from(r in Request, where: r.id == ^request_id), set: [last_error_code: code])
    {1, _} = Repo.update_all(from(t in CodexTurn, where: t.request_id == ^request_id), set: [error_code: code])
    {1, _} = Repo.update_all(from(a in Attempt, where: a.request_id == ^request_id), set: [network_error_code: code])
  end

  defp assert_mailbox_controls!(false, _context, _payload, _resend_payload), do: :ok

  defp assert_mailbox_controls!(true, context, payload, resend_payload) do
    wrong_output = update_in(resend_payload["input"], fn input -> List.update_at(input, length(payload["input"]), &Map.put(&1, "encrypted_content", "synthetic-altered")) end)
    tool = %{"type" => "function_call", "id" => "fc_synthetic", "call_id" => "synthetic-call", "name" => "synthetic_tool", "arguments" => "{}"}
    call_without_output = Map.update!(resend_payload, "input", &List.insert_at(&1, length(payload["input"]) + 1, tool))
    for refused <- [wrong_output, call_without_output], do: assert_mailbox_refused!(resend!(context.transport, context.port, context.setup, context.turn_state, refused))
    assert FakeUpstream.count(context.upstream) == if(context.local_resume?, do: 2, else: 1)
  end

  defp assert_mailbox_refused!(%{"type" => "error", "error" => %{"code" => "duplicate_turn"}}), do: :ok
  defp assert_mailbox_refused!({409, body}), do: assert(%{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body))
  defp assert_mailbox_refused!(_unexpected), do: flunk("unproved mailbox continuation was not fenced")

  defp continue_after_second_cut!(nil, _context, _request_id, payload), do: payload

  defp continue_after_second_cut!(second_cut, context, request_id, payload) do
    {conn, websocket, ref} = public_websocket_connect!(context.port, context.setup, context.turn_state)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))

    for ordinal <- 0..2 do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^second_cut}, @timeout_ms
      :ok = FakeUpstream.release_frame(context.upstream, second_cut)
    end

    assert_receive {:fake_upstream_frame_barrier, 3, _handler, ^second_cut}, @timeout_ms
    conn = receive_until!(conn, websocket, ref, "response.output_item.done")
    second = List.last(pool_requests(context.setup.pool.id))
    assert second.id != request_id
    close_and_await_receipt!(conn, second.id, :held, context.upstream, second_cut)
    resend_payload(payload, :mailbox)
  end

  defp topology_setup(upstream, :peer) do
    enter_peer_owner_topology!()
    gateway_setup(upstream)
  end

  defp topology_setup(upstream, _forwarding), do: gateway_setup(upstream)

  defp assert_peer_forwarding(_original, nil), do: :ok

  defp assert_peer_forwarding(original, peer) do
    forwarding = original.request_metadata["websocket_owner_forwarding"]
    assert forwarding["owner_instance_id"] == Atom.to_string(peer.node)
    assert forwarding["owner_instance_id"] != forwarding["proxy_instance_id"]
  end

  # provenance: observed findings#232 row 232-232 (Codex 0.156.1 through a recording proxy: the resend appends the completed item as its own model re-serializes it, without `status` and the parts' `annotations` and `logprobs`, keeping the id; every other field unchanged except the restamped request-start metadata)
  defp resend_payload(payload, shape) do
    payload
    |> Map.update!("input", &(&1 ++ appended_items(shape)))
    |> put_in(["client_metadata", "x-codex-ws-stream-request-start-ms"], 2_000)
  end

  defp appended_items(:identical), do: []
  defp appended_items(:grown), do: [client_recorded_item(@item_text)]
  defp appended_items(:mismatched), do: [client_recorded_item(@item_text <> " altered")]
  defp appended_items(:extra), do: [client_recorded_item(@item_text), client_recorded_item(@item_text)]
  defp appended_items(:mailbox), do: [reasoning_item() | Enum.map(1..5, fn n -> %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic task update #{n}"}]} end)]

  defp mailbox_resume_payload(payload, role) do
    suffix =
      case role do
        :opening -> []
        :local_summary -> [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic local summary"}]}]
        :remote_resume -> [%{"type" => "compaction", "encrypted_content" => "synthetic-compaction"}]
      end

    payload
    |> Map.update!("client_metadata", &Map.delete(&1, @websocket_lite_marker))
    |> update_in(["client_metadata", "x-codex-turn-metadata"], fn metadata -> metadata |> CodexPooler.JSON.decode!() |> Map.put("agent_name", "/root") |> CodexPooler.JSON.encode!() end)
    |> mailbox_window(if(role == :opening, do: 0, else: 1))
    |> Map.update!("input", &(&1 ++ suffix))
  end

  defp mailbox_window(payload, number) do
    metadata = CodexPooler.JSON.decode!(payload["client_metadata"]["x-codex-turn-metadata"])
    window = "#{metadata["thread_id"]}:#{number}"

    payload
    |> put_in(["client_metadata", "x-codex-window-id"], window)
    |> put_in(["client_metadata", "x-codex-turn-metadata"], CodexPooler.JSON.encode!(Map.merge(metadata, %{"window_id" => window, "window_number" => number})))
  end

  defp reasoning_item, do: %{"type" => "reasoning", "id" => "rs_mailbox_original", "summary" => [%{"type" => "summary_text", "text" => "synthetic reasoning"}], "encrypted_content" => "synthetic-reasoning"}

  defp reasoning_stream_frames do
    item = reasoning_item()

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => "resp_mailbox_original", "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.put(item, "summary", [])},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => %{"id" => "resp_mailbox_original", "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp client_recorded_item(text),
    do: %{"id" => "msg_resp_completed_item_original", "type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}

  # The provider completes only once the closing socket entered `terminate`
  # and pushes nothing more; released earlier, the socket could still write the
  # terminal into the closed connection, which is a turn the client was pushed
  # a terminal of (the fence stays).
  defp close_and_await_receipt!(conn, request_id, :completes, upstream, release_ref) do
    trace_socket_terminate!()
    _closed = Mint.HTTP.close(conn)
    assert_receive {:trace, _socket, :call, {CodexResponsesSocket, :terminate, [_reason, _state]}}, @timeout_ms
    stop_socket_terminate_trace()
    :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
    _settled = await_settled!(request_id, System.monotonic_time(:millisecond) + @timeout_ms)
    await_receipt!(request_id, System.monotonic_time(:millisecond) + @timeout_ms)
  end

  # Nothing releases the provider until the original's generation was stopped
  # (the owner's detach, or the closing socket's cleanup with forwarding off)
  # and the original settled. The receipt alone is not that signal: with
  # forwarding on the socket records it before its session cleanup detaches
  # from the owner, so when that cleanup outlasted the socket's 100 ms yield
  # the provider was released into a turn nothing had stopped yet, and the
  # original completed `succeeded` (findings#206 row 206-425).
  defp close_and_await_receipt!(conn, request_id, :held, upstream, release_ref) do
    _closed = Mint.HTTP.close(conn)
    receipt = await_receipt!(request_id, System.monotonic_time(:millisecond) + @stopped_receipt_budget_ms)
    _settled = await_settled!(request_id, System.monotonic_time(:millisecond) + @timeout_ms)
    :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
    receipt
  end

  defp resend!(:websocket, port, setup, turn_state, payload), do: send_and_receive_terminal!(port, setup, turn_state, CodexPooler.JSON.encode!(payload))
  defp resend!(:https, _port, setup, turn_state, payload), do: post_https_fallback!(setup, turn_state, payload)

  defp trace_socket_terminate! do
    on_exit(&stop_socket_terminate_trace/0)
    _matched = :erlang.trace_pattern({CodexResponsesSocket, :terminate, 2}, true, [:local])
    _traced = :erlang.trace(:all, true, [:call, {:tracer, self()}])
    :ok
  end

  defp stop_socket_terminate_trace do
    _traced = :erlang.trace(:all, false, [:call])
    _matched = :erlang.trace_pattern({CodexResponsesSocket, :terminate, 2}, false, [:local])
    :ok
  end

  defp assert_one_settlement_each!(request_ids) do
    for id <- request_ids do
      assert Repo.all(from(l in LedgerEntry, where: l.request_id == ^id and l.amount_status == "recorded", select: l.entry_kind)) |> Enum.frequencies() ==
               %{"reservation" => 1, "settlement" => 1, "release" => 1}
    end
  end

  defp successor_request(:websocket), do: native_request(FakeUpstream.websocket_text_frames(stream_frames("resp_completed_item_successor")))

  defp successor_request(:https) do
    FakeUpstream.expect_request(
      method: "POST",
      path: "/backend-api/codex/responses",
      respond: FakeUpstream.sse_stream(Enum.map(stream_frames("resp_completed_item_successor"), &CodexPooler.JSON.decode!/1))
    )
  end

  # The released client's HTTPS fallback of the websocket request: the same body
  # without the frame's `type` and without the websocket-only client metadata,
  # the Lite marker sent as a header (findings#232 row 232-231).
  defp post_https_fallback!(setup, turn_state, payload) do
    body =
      payload
      |> Map.delete("type")
      |> Map.update!("client_metadata", &Map.drop(&1, ["x-codex-ws-stream-request-start-ms", @websocket_lite_marker]))

    conn =
      build_conn()
      |> put_req_header("authorization", setup.authorization)
      |> put_req_header("x-codex-turn-state", turn_state)
      |> maybe_lite_header(payload)
      |> put_req_header("content-type", "application/json")
      |> post("/backend-api/codex/responses", CodexPooler.JSON.encode!(body))

    {conn.status, conn.resp_body}
  end

  defp maybe_lite_header(conn, %{"client_metadata" => %{@websocket_lite_marker => "true"}}), do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true")
  defp maybe_lite_header(conn, _payload), do: conn

  defp native_request(respond) do
    FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", json: [valid: true, equals: %{"type" => "response.create"}], respond: respond)
  end

  defp stream_frames(response_id) do
    item_id = "msg_" <> response_id
    item = %{"id" => item_id, "type" => "message", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => @item_text, "annotations" => [], "logprobs" => []}]}

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.in_progress", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"id" => item_id, "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}},
        %{"type" => "response.content_part.added", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "part" => %{"type" => "output_text", "text" => "", "annotations" => []}},
        %{"type" => "response.output_text.delta", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "delta" => "completed "},
        %{"type" => "response.output_text.delta", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "delta" => "answer"},
        %{"type" => "response.output_text.done", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "text" => @item_text},
        %{"type" => "response.content_part.done", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "part" => %{"type" => "output_text", "text" => @item_text, "annotations" => []}},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp native_turn_payload(thread_id, model) do
    %{
      "type" => "response.create",
      "model" => model,
      "instructions" => "synthetic base instructions",
      "stream" => true,
      "store" => false,
      "client_metadata" => %{
        "session_id" => thread_id,
        "thread_id" => thread_id,
        "turn_id" => "completed-item-turn",
        "x-codex-window-id" => thread_id <> ":0",
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => "completed-item-turn", "request_kind" => "turn"}),
        "x-codex-ws-stream-request-start-ms" => 100,
        @websocket_lite_marker => "true"
      },
      "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic completed item turn"}]}]
    }
  end

  defp send_and_receive_terminal!(port, setup, turn_state, raw_payload) do
    {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, raw_payload)
    {conn, frame} = receive_terminal!(conn, websocket, ref)
    _closed = Mint.HTTP.close(conn)
    frame
  end

  defp receive_until!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    if CodexPooler.JSON.decode!(text)["type"] == type, do: conn, else: receive_until!(conn, websocket, ref, type)
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, frame},
      else: receive_terminal!(conn, websocket, ref)
  end

  defp pool_requests(pool_id), do: Repo.all(from(r in Request, where: r.pool_id == ^pool_id, order_by: [asc: r.admitted_at]))

  # The closing socket's own cleanup can hold the shared sandbox connection
  # longer than a checkout waits under load; a dropped checkout is retried, and
  # the polls are spaced so the test's own reads do not crowd the queue the
  # socket, its task and the owner settle through.
  defp await_receipt!(request_id, deadline_ms) do
    case Repo.all(from(a in Attempt, where: a.request_id == ^request_id)) do
      [%Attempt{response_metadata: %{"downstream_delivery" => %{} = receipt}}] ->
        receipt

      _pending ->
        if System.monotonic_time(:millisecond) >= deadline_ms,
          do: flunk("no delivery receipt for #{request_id} within the budget"),
          else: Process.sleep(@poll_ms) && await_receipt!(request_id, deadline_ms)
    end
  end

  defp await_settled!(request_id, deadline_ms) do
    case Repo.all(from(r in Request, where: r.id == ^request_id and r.status != "in_progress")) do
      [%Request{} = request] ->
        request

      _pending ->
        if System.monotonic_time(:millisecond) >= deadline_ms,
          do: flunk("request never settled"),
          else: Process.sleep(@poll_ms) && await_settled!(request_id, deadline_ms)
    end
  end

  defp await_all_settled!(pool_id, deadline_ms) do
    requests = Repo.all(from(r in Request, where: r.pool_id == ^pool_id))

    cond do
      requests != [] and Enum.all?(requests, &(&1.status != "in_progress")) -> :ok
      System.monotonic_time(:millisecond) >= deadline_ms -> flunk("requests never settled")
      true -> Process.sleep(@poll_ms) && await_all_settled!(pool_id, deadline_ms)
    end
  end
end
