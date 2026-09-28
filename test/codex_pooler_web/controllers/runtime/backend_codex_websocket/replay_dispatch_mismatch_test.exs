defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.ReplayDispatchMismatchTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPooler.RequestReplayFixtures, only: [set_replay_db_now!: 1]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestReplayEntitlement}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport

  @moduletag capture_log: true

  # Failure-detection budget for an expected message; a green run returns as
  # soon as it arrives.
  @detection_timeout_ms 15_000

  for fault <- [:missing_preparation, :post_consume_request_binding] do
    @tag replay_fault: fault
    test "#{fault} refuses replay without another upstream send and expiry cleanup settles once", %{replay_fault: fault} do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)

      release_ref = make_ref()

      # provenance: synthetic_adversarial
      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            strict_native_request(
              1,
              FakeUpstream.websocket_close_without_terminal_barrier(
                notify: self(),
                release_ref: release_ref,
                code: 1001,
                reason: "synthetic pre-visible downstream death"
              )
            )
          ])
        )

      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, "full")
      thread_id = Ecto.UUID.generate()
      raw_payload = CodexPooler.JSON.encode!(payload(setup.model.exposed_model_id, thread_id))
      turn_state = Ecto.UUID.generate()
      {_server, port} = start_public_endpoint_with_server!()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state)
      {_conn, _websocket} = public_websocket_send_text!(conn, websocket, ref, raw_payload)

      assert_receive {:fake_upstream_websocket_barrier, :before_close, upstream_pid, ^release_ref}, @detection_timeout_ms

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert [original_attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert [turn] = Repo.all(from(t in CodexTurn, where: t.request_id == ^request.id))
      assert {:ok, owner_pid} = WebsocketOwnerSession.lookup(turn.codex_session_id)
      assert :suspended = WebsocketOwnerSession.detach_downstream(owner_pid, :sys.get_state(owner_pid).downstream)
      send(upstream_pid, {:fake_upstream_release_websocket, release_ref})

      assert %RequestReplayEntitlement{status: "armed"} = Repo.get_by!(RequestReplayEntitlement, request_id: request.id)
      assert %Attempt{status: "retryable_failed", replay_generation: 0} = Repo.get!(Attempt, original_attempt.id)

      assert %{"native_replay_preparation" => %{"version" => 1}} = Repo.get!(Attempt, original_attempt.id).response_metadata
      barrier = make_ref()

      if fault == :missing_preparation do
        Repo.update_all(from(a in Attempt, where: a.id == ^original_attempt.id), set: [response_metadata: %{}])
      else
        CodexPooler.TestAppEnv.restore_on_exit(:request_replay_dispatch_test_barrier)
        Application.put_env(:codex_pooler, :request_replay_dispatch_test_barrier, {self(), barrier})
      end

      {retry_conn, retry_websocket, retry_ref} = public_websocket_connect!(port, setup, turn_state)

      log =
        BackendCodexWebsocketOwnerForwardingSupport.capture_info_log(fn ->
          {retry_conn, retry_websocket} = public_websocket_send_text!(retry_conn, retry_websocket, retry_ref, raw_payload)

          if fault == :post_consume_request_binding do
            assert_receive {:request_replay_dispatch_ready, dispatch_pid, ^barrier}, @detection_timeout_ms
            assert Repo.get_by!(RequestReplayEntitlement, request_id: request.id).status == "consumed"
            Repo.update_all(from(r in Request, where: r.id == ^request.id), set: [endpoint: "/backend-api/codex/responses/compact"])
            send(dispatch_pid, {:release_request_replay_dispatch, barrier})
          end

          {_retry_conn, _retry_websocket, frame} = public_websocket_receive_text!(retry_conn, retry_websocket, retry_ref)
          send(self(), {:replay_resend_frame, CodexPooler.JSON.decode!(frame)})
        end)

      assert_received {:replay_resend_frame, %{"status" => 409, "error" => %{"code" => "duplicate_turn"}}}
      assert log =~ "websocket replay rejection"
      assert FakeUpstream.count(upstream) == 1
      entitlement = Repo.get_by!(RequestReplayEntitlement, request_id: request.id)

      {deadline, final_attempt_id, error_code} =
        if fault == :missing_preparation do
          assert log =~ "stage=runtime_replay_preflight"
          assert log =~ "reason_code=invalid_replay_preparation"
          assert %RequestReplayEntitlement{status: "armed", replay_attempt_id: nil, consumed_at: nil} = entitlement
          assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
          {entitlement.expires_at, original_attempt.id, "websocket_replay_expired"}
        else
          assert log =~ "stage=native_replay_dispatch"
          assert log =~ "reason_code=replay_lifecycle_mismatch"
          assert %RequestReplayEntitlement{status: "consumed", closed_at: nil, started_at: nil} = entitlement
          assert %Attempt{replay_generation: 1, status: "in_progress", usage_status: "usage_pending"} = Repo.get!(Attempt, entitlement.replay_attempt_id)
          {entitlement.abandon_at, entitlement.replay_attempt_id, "websocket_replay_abandoned"}
        end

      assert %Request{status: "in_progress", usage_status: "usage_pending"} = Repo.get!(Request, request.id)
      assert %CodexTurn{status: "in_progress"} = Repo.get!(CodexTurn, turn.id)
      assert ledger_kinds(request.id) == ["reservation"]

      # Before the deadline the sweep leaves it alone.
      set_replay_db_now!(DateTime.add(deadline, -1, :millisecond))
      assert {:ok, %{replay_entitlements_closed: 0}} = Accounting.cleanup_request_replays()
      assert %Request{status: "in_progress"} = Repo.get!(Request, request.id)

      set_replay_db_now!(DateTime.add(deadline, 1, :millisecond))
      assert {:ok, %{replay_entitlements_closed: 1}} = Accounting.cleanup_request_replays()

      assert %Request{status: "failed", response_status_code: 499, usage_status: "usage_unknown", last_error_code: ^error_code} =
               Repo.get!(Request, request.id)

      final_status = if fault == :missing_preparation, do: "retryable_failed", else: "failed"
      assert %Attempt{status: ^final_status, completed_at: %DateTime{}} = Repo.get!(Attempt, final_attempt_id)
      assert %CodexTurn{status: "failed", error_code: ^error_code} = Repo.get!(CodexTurn, turn.id)
      assert %RequestReplayEntitlement{closed_at: %DateTime{}} = Repo.get_by!(RequestReplayEntitlement, request_id: request.id)
      assert ledger_kinds(request.id) == ["release", "reservation", "settlement"]

      assert {:ok, %{replay_entitlements_closed: 0}} = Accounting.cleanup_request_replays()
      assert ledger_kinds(request.id) == ["release", "reservation", "settlement"]

      # Settled once, with unknown usage and nothing charged.
      assert [%LedgerEntry{usage_status: "usage_unknown", settled_cost_micros: settled}] =
               Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id and entry.entry_kind == "settlement"))

      assert is_nil(settled) or Decimal.equal?(settled, 0)
    end
  end

  defp ledger_kinds(request_id) do
    Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request_id, select: entry.entry_kind))
    |> Enum.sort()
  end

  defp payload(model, thread_id) do
    %{
      "type" => "response.create",
      "model" => model,
      "client_metadata" => %{
        "x-codex-turn-metadata" =>
          CodexPooler.JSON.encode!(%{
            "session_id" => thread_id,
            "thread_id" => thread_id,
            "turn_id" => "replay-dispatch-mismatch",
            "request_kind" => "turn"
          })
      },
      "input" => [
        %{"type" => "function_call_output", "call_id" => "call_replay_mismatch", "output" => "synthetic replay output"}
      ],
      "instructions" => "synthetic replay instructions",
      "tools" => [%{"type" => "function", "name" => "sample_tool", "parameters" => %{"type" => "object", "properties" => %{}}}],
      "stream" => true,
      "generate" => true
    }
  end
end
