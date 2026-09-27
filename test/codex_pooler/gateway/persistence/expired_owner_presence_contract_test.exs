defmodule CodexPooler.Gateway.Persistence.ExpiredOwnerPresenceContractTest do
  # Session expiry is independent of VM liveness: a fresh VM heartbeat cannot
  # renew an expired per-session lease or shelter that lease's active turn.
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn, RuntimeCleanup}
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo

  test "a fresh VM heartbeat does not renew an expired session lease" do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    expired_at = DateTime.add(now, -60, :second)

    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    model = model_fixture(pool, %{exposed_model_id: "gpt-cleanup-probe"})

    owner = Identity.new("codex_pooler@10.99.0.#{System.unique_integer([:positive])}", "boot-#{System.unique_integer([:positive])}")
    assert {:ok, _presence} = InstancePresence.record_heartbeat(owner)

    # The owner is still reporting: the incarnation's presence row is fresh.
    refute InstancePresence.absent?(owner, now)

    session = session_fixture(pool, api_key, assignment, owner, expired_at, now)
    expired_lease = lease_fixture(session, pool, api_key, assignment, expired_at, now)

    request =
      request_fixture(%{pool: pool, api_key: api_key}, %{
        model_id: model.id,
        requested_model: model.exposed_model_id,
        transport: "websocket",
        status: "in_progress",
        usage_status: "usage_pending",
        completed_at: nil,
        response_status_code: nil,
        request_metadata: %{"codex_session_id" => session.id}
      })

    attempt =
      attempt_fixture(request, assignment, %{
        status: "in_progress",
        completed_at: nil,
        usage_status: "usage_pending",
        response_metadata: %{}
      })

    turn = turn_fixture(session, request, attempt, now)

    request
    |> ledger_entry_fixture(%{
      attempt_id: attempt.id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: assignment.upstream_identity_id,
      entry_kind: "reservation",
      amount_status: "recorded",
      usage_status: "usage_pending",
      transport: "websocket",
      output_tokens: 8,
      total_tokens: 12,
      details: %{"source" => "test_reservation"}
    })
    |> Ecto.Changeset.change(%{source_event_id: "request:#{request.id}:reservation"})
    |> Repo.update!()

    # The guard the finding names cannot shelter this candidate: its
    # session-scoped arities require an unexpired session or lease deadline.

    refute RuntimeCleanup.active_runtime_request?(request.id, now, [])
    refute RuntimeCleanup.active_runtime_request?(request, attempt, now, [])

    assert {:ok, summary} = RuntimeCleanup.cleanup_expired_runtime_state(now)

    assert summary.expired_owner_sessions_recovered == 1
    assert Repo.get!(CodexTurn, turn.id).status == "interrupted"
    assert Repo.get!(Request, request.id).status == "failed"
    assert Repo.get!(Request, request.id).last_error_code == "owner_unavailable"
    assert Repo.get!(BridgeOwnerLease, expired_lease.id).status == "expired"
    assert Repo.get!(CodexSession, session.id).status == "interrupted"
  end

  defp session_fixture(pool, api_key, assignment, owner, expires_at, now) do
    now = usec(now)

    %CodexSession{
      pool_id: pool.id,
      api_key_id: api_key.id,
      session_key: "probe-session-#{System.unique_integer([:positive])}",
      pool_upstream_assignment_id: assignment.id,
      status: "active",
      owner_instance_id: owner.node_name,
      owner_instance_boot_id: owner.boot_id,
      owner_lease_token: Ecto.UUID.generate(),
      owner_lease_expires_at: usec(expires_at),
      last_heartbeat_at: now,
      created_at: now,
      updated_at: now
    }
    |> Repo.insert!()
  end

  defp lease_fixture(session, pool, api_key, assignment, expires_at, now) do
    now = usec(now)
    expires_at = usec(expires_at)

    %BridgeOwnerLease{}
    |> BridgeOwnerLease.changeset(%{
      codex_session_id: session.id,
      pool_id: pool.id,
      api_key_id: api_key.id,
      pool_upstream_assignment_id: assignment.id,
      owner_instance_id: session.owner_instance_id,
      owner_instance_boot_id: session.owner_instance_boot_id,
      lease_token: session.owner_lease_token,
      status: "active",
      acquired_at: now,
      renewed_at: now,
      expires_at: expires_at,
      metadata: %{},
      created_at: now,
      updated_at: now
    })
    |> Repo.insert!()
  end

  defp turn_fixture(session, request, attempt, now) do
    timestamp = now |> DateTime.add(-30, :second) |> usec()

    %CodexTurn{
      codex_session_id: session.id,
      request_id: request.id,
      turn_sequence: 1,
      transport_kind: request.transport,
      final_attempt_id: attempt.id,
      status: "in_progress",
      started_at: timestamp,
      created_at: timestamp,
      updated_at: timestamp
    }
    |> Repo.insert!()
  end

  defp usec(%DateTime{} = timestamp) do
    %{timestamp | microsecond: {elem(timestamp.microsecond, 0), 6}}
  end
end
