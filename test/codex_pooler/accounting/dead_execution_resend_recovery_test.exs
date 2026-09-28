defmodule CodexPooler.Accounting.DeadExecutionResendRecoveryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting

  alias CodexPooler.Accounting.{
    Attempt,
    ClientRetry,
    LedgerEntry,
    Request,
    RequestClientRetryLink
  }

  alias CodexPooler.Accounting.RequestLifecycle
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo

  @endpoint "/backend-api/codex/responses"
  @retry_prefix "codex-request-retry:"
  @detection_timeout_ms 15_000

  test "a recovered successor can die and be retried once again after session rotation" do
    fixture = active_dead_execution_fixture!()
    %{setup: setup, request: request, attempt: attempt, turn: turn} = fixture
    CodexPooler.ExecutionProofSupport.publish_terminal!(attempt)
    witness = ClientRetry.original_witness!(fixture.replay_claim_digest, setup.api_key.runtime_revocation_epoch)
    opts = %{endpoint: @endpoint, correlation_id: request.correlation_id, codex_session: insert_session!(setup), semantic_turn_digest: turn.semantic_turn_digest, native_client_retry_witness: witness}
    assert {:ok, %{request: first_successor}} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)
    assert {:ok, %{request: first_successor}} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "input" => []}, %{endpoint: @endpoint, transport: "websocket", correlation_id: first_successor.correlation_id, turn_claim: first_successor})
    successor_attempt = create_dead_attempt!(setup, first_successor)
    successor_turn = insert_turn!(opts.codex_session, first_successor, successor_attempt)
    successor_turn |> Ecto.Changeset.change(semantic_turn_digest: turn.semantic_turn_digest) |> Repo.update!()
    CodexPooler.ExecutionProofSupport.publish_terminal!(successor_attempt)
    opts = %{opts | codex_session: insert_session!(setup)}

    assert {:ok, %{request: second_successor}} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)
    assert second_successor.id != first_successor.id
    assert %Request{status: "failed", last_error_code: "dead_execution_recovered"} = Repo.reload!(first_successor)
    assert ledger_kinds(first_successor.id) == ["release", "reservation", "settlement"]
    assert Repo.get_by!(RequestClientRetryLink, predecessor_request_id: first_successor.id).successor_request_id == second_successor.id
    assert {:error, %{code: :duplicate_request}} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)
  end

  for recovered_before_resend <- [false, true] do
    @tag recovered_before_resend: recovered_before_resend
    test "a proven replaced instance permits one exact resend in a new session, cleanup first=#{recovered_before_resend}", %{recovered_before_resend: recovered_before_resend} do
      fixture = active_dead_execution_fixture!()
      %{setup: setup, request: request, attempt: attempt, turn: turn} = fixture
      now = db_now()
      old = Identity.new("replaced-#{System.unique_integer([:positive])}@example", "old-boot")
      newer = Identity.new(old.node_name, "new-boot")
      stale = DateTime.add(now, -180, :second)
      {:ok, _} = InstancePresence.record_heartbeat(old, stale)
      {:ok, _} = InstancePresence.record_heartbeat(newer, now)
      {:ok, _} = InstancePresence.record_heartbeat()

      attempt = attempt |> Ecto.Changeset.change(owner_instance_id: old.node_name, owner_instance_boot_id: old.boot_id) |> Repo.update!()
      turn = turn |> Ecto.Changeset.change(first_visible_output_at: now) |> Repo.update!()
      new_session = insert_session!(setup)

      if recovered_before_resend do
        assert {:ok, :recovered} = RequestLifecycle.recover_absent_execution(request, attempt, now, [])
      end

      witness = ClientRetry.original_witness!(fixture.replay_claim_digest, setup.api_key.runtime_revocation_epoch)
      opts = %{endpoint: @endpoint, correlation_id: request.correlation_id, codex_session: new_session, native_client_retry_witness: witness}

      wrong_witness = ClientRetry.original_witness!(:crypto.strong_rand_bytes(32), setup.api_key.runtime_revocation_epoch)
      assert {:error, %{code: :duplicate_request}} = Accounting.claim_websocket_turn(setup.auth, setup.model, %{opts | native_client_retry_witness: wrong_witness})

      unless recovered_before_resend do
        assert Repo.reload!(request).status == "in_progress"
        assert Repo.reload!(attempt).status == "in_progress"
        assert Repo.reload!(turn).status == "in_progress"
        assert ledger_kinds(request.id) == ["reservation"]
      end

      assert {:ok, %{request: successor}} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)
      assert successor.id != request.id
      assert %Request{status: "failed", last_error_code: "absent_instance_recovered"} = Repo.reload!(request)
      assert %Attempt{status: "failed", network_error_code: "absent_instance_recovered"} = Repo.reload!(attempt)
      assert %CodexTurn{status: "interrupted", error_code: "absent_instance_recovered"} = Repo.reload!(turn)
      assert ledger_kinds(request.id) == ["release", "reservation", "settlement"]
      assert {:error, %{code: :duplicate_request}} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)
      assert Repo.aggregate(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^request.id), :count) == 1
    end
  end

  test "an exact terminal proof recovers the live predecessor inside the released-client resend" do
    setup = accounting_setup()
    session = insert_session!(setup)

    witness =
      ClientRetry.original_witness!(
        :crypto.strong_rand_bytes(32),
        setup.api_key.runtime_revocation_epoch
      )

    claim = "codex-turn:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

    opts = %{
      endpoint: @endpoint,
      correlation_id: claim,
      codex_session: session,
      native_client_retry_witness: witness
    }

    assert {:ok, %{request: claimed}} =
             Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

    assert {:ok, %{request: request}} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: @endpoint,
                 transport: "websocket",
                 correlation_id: claim,
                 turn_claim: claimed
               }
             )

    attempt = create_dead_attempt!(setup, request)
    turn = insert_turn!(session, request, attempt)
    CodexPooler.ExecutionProofSupport.publish_terminal!(attempt)

    wrong_witness =
      ClientRetry.original_witness!(
        :crypto.strong_rand_bytes(32),
        setup.api_key.runtime_revocation_epoch
      )

    assert {:error, %{code: :duplicate_request, resend_disposition: :terminal_predecessor}} =
             Accounting.claim_websocket_turn(
               setup.auth,
               setup.model,
               %{opts | native_client_retry_witness: wrong_witness}
             )

    assert %Request{status: "in_progress", completed_at: nil} = Repo.reload!(request)
    assert %Attempt{status: "in_progress", completed_at: nil} = Repo.reload!(attempt)
    assert %CodexTurn{status: "in_progress", completed_at: nil} = Repo.reload!(turn)
    assert ledger_kinds(request.id) == ["reservation"]

    attach_outcome_handler!()

    assert {:ok,
            %{
              request: successor,
              client_resend: %{
                predecessor_request_id: predecessor_id,
                predecessor_shape: :task_exception
              }
            }} = Accounting.claim_websocket_turn(setup.auth, setup.model, opts)

    assert predecessor_id == request.id
    assert String.starts_with?(successor.correlation_id, @retry_prefix)

    assert_receive {:dead_resend_outcome,
                    %{
                      outcome: "interrupted",
                      downstream_transport: "websocket",
                      upstream_transport: "websocket"
                    }, false}

    assert_recovered!(request, attempt, turn)
    assert ledger_kinds(request.id) == ["release", "reservation", "settlement"]

    assert Repo.aggregate(
             from(link in RequestClientRetryLink,
               where: link.predecessor_request_id == ^request.id
             ),
             :count
           ) == 1

    assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
  end

  test "direct disconnect cleanup preserves exact dead-execution attribution" do
    setup = accounting_setup()
    session = insert_session!(setup)
    claim = "codex-turn:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

    witness =
      ClientRetry.original_witness!(
        :crypto.strong_rand_bytes(32),
        setup.api_key.runtime_revocation_epoch
      )

    assert {:ok, %{request: claimed}} =
             Accounting.claim_websocket_turn(setup.auth, setup.model, %{
               endpoint: @endpoint,
               correlation_id: claim,
               codex_session: session,
               native_client_retry_witness: witness
             })

    assert {:ok, %{request: request}} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: @endpoint,
                 transport: "websocket",
                 correlation_id: claim,
                 turn_claim: claimed
               }
             )

    attempt = create_dead_attempt!(setup, request)
    turn = insert_turn!(session, request, attempt)
    CodexPooler.ExecutionProofSupport.publish_terminal!(attempt)
    attach_outcome_handler!()

    assert :ok =
             Interruption.interrupt_direct_request(
               %{
                 session_id: session.id,
                 request_id: request.id,
                 correlation_id: request.correlation_id,
                 api_key_id: request.api_key_id,
                 attempt_id: attempt.id,
                 replay_generation: attempt.replay_generation
               },
               "client_disconnected"
             )

    assert_receive {:dead_resend_outcome,
                    %{
                      outcome: "interrupted",
                      downstream_transport: "websocket",
                      upstream_transport: "websocket"
                    }, false}

    assert_recovered!(request, attempt, turn)
    assert ledger_kinds(request.id) == ["release", "reservation", "settlement"]
  end

  test "owner crash interruption preserves exact dead-execution attribution" do
    fixture = active_dead_execution_fixture!()
    CodexPooler.ExecutionProofSupport.publish_terminal!(fixture.attempt)
    attach_outcome_handler!()

    assert {:ok, %{interrupted_turn_count: 1}} =
             Interruption.interrupt_codex_turn(
               fixture.session,
               RequestOptions.for_websocket(%{
                 request_id: fixture.request.correlation_id,
                 interrupt_reason: "owner_crashed",
                 reconnect_window_seconds: 300
               })
             )

    assert_receive {:dead_resend_outcome,
                    %{
                      outcome: "interrupted",
                      downstream_transport: "websocket",
                      upstream_transport: "websocket"
                    }, false}

    assert_recovered!(fixture.request, fixture.attempt, fixture.turn)
  end

  test "owner crash interruption keeps owner_crashed without a terminal proof" do
    fixture = active_dead_execution_fixture!()

    assert {:ok, %{interrupted_turn_count: 1}} =
             Interruption.interrupt_codex_turn(
               fixture.session,
               RequestOptions.for_websocket(%{
                 request_id: fixture.request.correlation_id,
                 interrupt_reason: "owner_crashed",
                 reconnect_window_seconds: 300
               })
             )

    assert %Request{status: "failed", last_error_code: "owner_crashed"} =
             Repo.reload!(fixture.request)

    assert %Attempt{status: "failed", network_error_code: "owner_crashed"} =
             Repo.reload!(fixture.attempt)

    assert %CodexTurn{status: "interrupted", error_code: "owner_crashed"} =
             Repo.reload!(fixture.turn)
  end

  test "released-client retry admits an owner crash once exact death proof arrives" do
    fixture = active_dead_execution_fixture!()

    assert {:ok, %{interrupted_turn_count: 1}} =
             Interruption.interrupt_codex_turn(
               fixture.session,
               RequestOptions.for_websocket(%{
                 request_id: fixture.request.correlation_id,
                 interrupt_reason: "owner_crashed",
                 reconnect_window_seconds: 300
               })
             )

    input = retry_input(fixture)

    assert {:error, :terminal_predecessor} =
             Accounting.client_retry_preflight_snapshot(
               fixture.session,
               fixture.setup.api_key,
               fixture.setup.model,
               input
             )

    CodexPooler.ExecutionProofSupport.publish_terminal!(fixture.attempt)

    assert {:ok,
            %{
              replay_generation: 0,
              client_retry_predecessor_request_id: predecessor_id
            }} =
             Accounting.client_retry_preflight_snapshot(
               fixture.session,
               fixture.setup.api_key,
               fixture.setup.model,
               input
             )

    assert predecessor_id == fixture.request.id

    assert {:ok, %ClientRetry.SuccessorClaim{} = successor} =
             Accounting.claim_client_retry_successor(
               fixture.setup.auth,
               fixture.setup.model,
               %{"model" => fixture.setup.model.exposed_model_id, "input" => []},
               Map.merge(input, %{
                 codex_session: fixture.session,
                 owner_idle_validated?: true,
                 owner_lease_token: fixture.session.owner_lease_token,
                 owner_instance_id: fixture.session.owner_instance_id
               })
             )

    assert successor.predecessor_request_id == fixture.request.id
    assert ClientRetry.reserved_successor_claim?(successor.correlation_id)
  end

  defp create_dead_attempt!(setup, request) do
    parent = self()

    owner_pid =
      spawn(fn ->
        assert {:ok, attempt} = Accounting.create_attempt(request, setup.assignment)
        send(parent, {:dead_resend_attempt, self(), attempt})

        receive do
          :finish -> :ok
        end
      end)

    on_exit(fn -> if Process.alive?(owner_pid), do: Process.exit(owner_pid, :kill) end)
    assert_receive {:dead_resend_attempt, ^owner_pid, %Attempt{} = attempt}, @detection_timeout_ms
    monitor = Process.monitor(owner_pid)
    Process.exit(owner_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner_pid, :killed}, @detection_timeout_ms
    attempt
  end

  defp active_dead_execution_fixture! do
    setup = accounting_setup()
    now = db_now()

    session =
      setup
      |> insert_session!()
      |> Ecto.Changeset.change(
        owner_instance_id: "owner-node@example",
        owner_instance_boot_id: "owner-boot",
        owner_lease_token: Ecto.UUID.generate(),
        owner_lease_expires_at: DateTime.add(now, 300, :second),
        last_heartbeat_at: now
      )
      |> Repo.update!()

    Repo.insert!(%BridgeOwnerLease{
      codex_session_id: session.id,
      pool_id: setup.pool.id,
      api_key_id: setup.api_key.id,
      pool_upstream_assignment_id: setup.assignment.id,
      owner_instance_id: session.owner_instance_id,
      owner_instance_boot_id: session.owner_instance_boot_id,
      lease_token: session.owner_lease_token,
      status: "active",
      acquired_at: now,
      renewed_at: now,
      expires_at: session.owner_lease_expires_at,
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    claim = "codex-turn:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)

    witness =
      ClientRetry.original_witness!(
        :crypto.strong_rand_bytes(32),
        setup.api_key.runtime_revocation_epoch
      )

    assert {:ok, %{request: claimed}} =
             Accounting.claim_websocket_turn(setup.auth, setup.model, %{
               endpoint: @endpoint,
               correlation_id: claim,
               codex_session: session,
               native_client_retry_witness: witness
             })

    assert {:ok, %{request: request}} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id, "input" => []},
               %{
                 endpoint: @endpoint,
                 transport: "websocket",
                 correlation_id: claim,
                 turn_claim: claimed
               }
             )

    attempt = create_dead_attempt!(setup, request)

    %{
      setup: setup,
      request: request,
      attempt: attempt,
      turn: insert_turn!(session, request, attempt),
      session: session,
      replay_claim_digest: witness.digest
    }
  end

  defp retry_input(fixture) do
    %{
      endpoint: @endpoint,
      requested_model: fixture.setup.model.exposed_model_id,
      runtime_revocation_epoch: fixture.setup.api_key.runtime_revocation_epoch,
      semantic_turn_digest: fixture.turn.semantic_turn_digest,
      original_request_claim: fixture.request.correlation_id,
      replay_claim_digest: fixture.replay_claim_digest,
      anchor_present?: false,
      reservation_estimate: %{
        input_tokens: 0,
        cached_input_tokens: 0,
        output_tokens: 0,
        reasoning_tokens: 0,
        total_tokens: 0,
        estimated_cost_micros: Decimal.new(0),
        strategy: "exact"
      }
    }
  end

  defp insert_session!(setup) do
    now = db_now()

    Repo.insert!(%CodexSession{
      pool_id: setup.pool.id,
      api_key_id: setup.api_key.id,
      session_key: "dead-resend-#{System.unique_integer([:positive, :monotonic])}",
      pool_upstream_assignment_id: setup.assignment.id,
      status: "active",
      created_at: now,
      updated_at: now
    })
  end

  defp insert_turn!(session, request, attempt) do
    now = db_now()

    Repo.insert!(%CodexTurn{
      codex_session_id: session.id,
      request_id: request.id,
      turn_sequence: 1,
      transport_kind: "websocket",
      semantic_turn_digest: :crypto.strong_rand_bytes(32),
      status: "in_progress",
      final_attempt_id: attempt.id,
      started_at: now,
      created_at: now,
      updated_at: now
    })
  end

  defp attach_outcome_handler! do
    id = "dead-resend-#{System.unique_integer([:positive, :monotonic])}"

    :ok =
      :telemetry.attach(
        id,
        [:codex_pooler, :gateway, :stream, :outcome],
        fn _event, _measurements, metadata, test_pid ->
          send(test_pid, {:dead_resend_outcome, metadata, Repo.in_transaction?()})
        end,
        self()
      )

    on_exit(fn -> :telemetry.detach(id) end)
  end

  defp assert_recovered!(request, attempt, turn) do
    assert %Request{status: "failed", last_error_code: "dead_execution_recovered"} =
             Repo.reload!(request)

    assert %Attempt{
             status: "failed",
             network_error_code: "dead_execution_recovered",
             usage_status: "usage_unknown"
           } = Repo.reload!(attempt)

    assert %CodexTurn{status: "interrupted", error_code: "dead_execution_recovered"} =
             Repo.reload!(turn)
  end

  defp ledger_kinds(request_id) do
    Repo.all(
      from entry in LedgerEntry,
        where: entry.request_id == ^request_id,
        order_by: [asc: entry.entry_kind],
        select: entry.entry_kind
    )
  end

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    now
  end
end
