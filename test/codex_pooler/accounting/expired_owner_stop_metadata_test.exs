defmodule CodexPooler.Accounting.ExpiredOwnerStopMetadataTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Metadata}
  alias CodexPooler.Accounting.{LedgerEntry, Request}
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Finalization.ExpiredOwnerGenerationCleanup, as: Cleanup
  alias CodexPooler.Jobs.RuntimeStateCleanup, as: CleanupJob
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo

  test "validated locked cause survives real sanitizer and finalizer, while caller attrs cannot replace it" do
    setup = accounting_setup()
    assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{correlation_id: Ecto.UUID.generate(), transport: "websocket"})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    witness = witness(attempt, reserved.request)
    witness = Map.merge(witness, %{"phase" => "ended", "end_kind" => "serialized_connection_closed", "observed_end_at" => witness["authorized_at"]})
    assert Metadata.sanitize_metadata(witness) == witness
    assert {:ok, ^witness} = Cleanup.validate(witness, attempt)

    attempt = Repo.update!(Ecto.Changeset.change(attempt, response_metadata: %{"expired_owner_stop" => witness}))
    assert {:ok, _} = Accounting.finalize_request(reserved.request, attempt, %{request_status: "failed", attempt_status: "failed", response_status_code: 499, last_error_code: "owner_unavailable", usage: %{status: "usage_unknown"}, attempt_metadata: %{"expired_owner_stop" => Map.put(witness, "phase", "forged"), "ordinary_flag" => true}})
    stored = Repo.get!(Attempt, attempt.id)
    assert {:ok, ^witness} = Cleanup.read(stored)
    assert stored.response_metadata["ordinary_flag"] == true

    assert {:ok, _} = Accounting.finalize_success(reserved.request, stored, %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2}, %{response_status_code: 200})
    assert {:ok, ^witness} = Cleanup.read(Repo.get!(Attempt, attempt.id))
  end

  test "malformed oversized wrong incarnation and new generation cannot copy an old cause" do
    setup = accounting_setup()
    assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{correlation_id: Ecto.UUID.generate(), transport: "websocket"})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    witness = witness(attempt, reserved.request)

    for mutation <- [Map.put(witness, "version", 2), Map.put(witness, "unexpected", true), Map.put(witness, "task_address", String.duplicate("x", 4097)), Map.put(witness, "replay_generation", 1), Map.put(witness, "lease_identity_digest", "unsafe"), Map.put(witness, "executor", Map.put(witness["executor"], "owner_execution_id", Ecto.UUID.generate()))] do
      assert {:error, :invalid_expired_owner_stop} = Cleanup.validate(mutation, attempt)
    end

    assert {:error, :invalid_expired_owner_stop} = Cleanup.validate(witness, %{attempt | replay_generation: 1})
    assert Cleanup.preserve(%{"expired_owner_stop" => witness}, attempt) == %{}
    error = %{reason: :owner_unavailable, body: "", expired_owner_stop_disposition: Cleanup.disposition(witness)}
    assert Cleanup.stopped_caller?(error, reserved.request, attempt)
    refute Cleanup.stopped_caller?(error, reserved.request, %{attempt | replay_generation: 1})
    refute Map.has_key?(Cleanup.strip(error), :expired_owner_stop_disposition)
  end

  test "whole witness budget rejects valid fields beyond4096 bytes without a shape or sanitizer rejection" do
    setup = accounting_setup()
    assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{correlation_id: Ecto.UUID.generate(), transport: "websocket"})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    witness = witness(attempt, reserved.request)
    compact = Map.put(witness, "authorized_at", "2000-01-01T00:00:00.000000Z")
    assert {:ok, ^compact} = Cleanup.validate(compact, attempt)
    long_time = "2000-01-01T00:00:00." <> String.duplicate("0", 5000) <> "Z"
    assert {:ok, _timestamp, 0} = DateTime.from_iso8601(long_time)
    oversized = Map.put(compact, "authorized_at", long_time)
    assert Metadata.sanitize_metadata(oversized) == oversized
    assert byte_size(CodexPooler.JSON.encode!(oversized)) > 4096
    assert {:error, :invalid_expired_owner_stop} = Cleanup.validate(oversized, attempt)
  end

  test "actual cleanup job settles marked oldest unknown and next stale row without promoting authorization" do
    setup = accounting_setup()
    {oldest, attempt, cause} = retained_unknown_reservation!(setup, 8)
    {second, _second_attempt, _cause} = retained_unknown_reservation!(setup, 7)
    assert {:ok, summary} = CleanupJob.run(InstancePresence.database_now())
    assert summary.stale_reservations_settled == 2

    for request <- [oldest, second] do
      stored = Repo.get!(Request, request.id)
      assert stored.status == "failed"
      assert stored.response_status_code == 499
      assert stored.last_error_code == "stale_reservation_recovered"
      assert ledger_kinds(request.id) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
    end

    assert {:ok, ^cause} = Cleanup.read(Repo.get!(Attempt, attempt.id))
    assert cause["phase"] == "authorized"
    refute Map.has_key?(cause, "observed_end_at")
  end

  test "copied backstop reason has no selector authority and young marked row is not swept" do
    setup = accounting_setup()
    {oldest, attempt, _cause} = retained_unknown_reservation!(setup, 7)
    assert {:error, %{code: :owner_unavailable}} = Accounting.finalize_request(oldest, attempt, %{request_status: "failed", attempt_status: "failed", response_status_code: 499, last_error_code: "stale_reservation_recovered", usage: %{status: "usage_unknown"}})
    assert Repo.get!(Request, oldest.id).status == "in_progress"
    {young, _attempt, _cause} = retained_unknown_reservation!(setup, 0)
    assert {:ok, summary} = Accounting.recover_stale_reservations(InstancePresence.database_now())
    assert summary.stale_reservations_settled == 1
    assert Repo.get!(Request, young.id).status == "in_progress"
    assert ledger_kinds(young.id) == %{"reservation" => 1}
  end

  test "real live Session and Lease shelter protects a marked stale reservation" do
    setup = accounting_setup()
    {request, _attempt, _cause} = retained_unknown_reservation!(setup, 7)
    clock = InstancePresence.database_now()
    future = DateTime.add(clock, 3600, :second)
    token = Ecto.UUID.generate()
    owner = Identity.local()
    session = Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: "sample-live-stale-#{Ecto.UUID.generate()}", status: "active", owner_instance_id: owner.node_name, owner_instance_boot_id: owner.boot_id, owner_lease_token: token, owner_lease_expires_at: future, last_heartbeat_at: clock, created_at: clock, updated_at: clock})
    Repo.insert!(%BridgeOwnerLease{codex_session_id: session.id, pool_id: setup.pool.id, api_key_id: setup.api_key.id, lease_token: token, owner_instance_id: owner.node_name, owner_instance_boot_id: owner.boot_id, status: "active", acquired_at: clock, renewed_at: clock, expires_at: future, metadata: %{}, created_at: clock, updated_at: clock})
    Repo.insert!(%CodexTurn{codex_session_id: session.id, request_id: request.id, turn_sequence: 1, transport_kind: "websocket", status: "in_progress", started_at: clock, created_at: clock, updated_at: clock})
    assert {:ok, summary} = Accounting.recover_stale_reservations(clock)
    assert summary.stale_reservations_settled == 0
    assert Repo.get!(Request, request.id).status == "in_progress"
    assert ledger_kinds(request.id) == %{"reservation" => 1}
  end

  test "stale selector preserves a proven ended cause while malformed older metadata cannot poison the pass" do
    setup = accounting_setup()
    {oldest, attempt, cause} = retained_unknown_reservation!(setup, 8)
    malformed = Map.put(cause, "version", 999)
    Repo.update!(Ecto.Changeset.change(attempt, response_metadata: %{"expired_owner_stop" => malformed}))
    {ended_request, ended_attempt, authorized} = retained_unknown_reservation!(setup, 7)
    ended = Map.merge(authorized, %{"phase" => "ended", "end_kind" => "serialized_connection_closed", "observed_end_at" => authorized["authorized_at"]})
    Repo.update!(Ecto.Changeset.change(ended_attempt, response_metadata: %{"expired_owner_stop" => ended}))
    assert {:ok, summary} = Accounting.recover_stale_reservations(InstancePresence.database_now())
    assert summary.stale_reservations_settled == 2
    assert Repo.get!(Request, oldest.id).last_error_code == "stale_reservation_recovered"
    assert Repo.get!(Request, ended_request.id).last_error_code == "owner_unavailable"
    assert {:ok, ^ended} = Cleanup.read(Repo.get!(Attempt, ended_attempt.id))
    assert ledger_kinds(oldest.id) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
    assert ledger_kinds(ended_request.id) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
  end

  defp retained_unknown_reservation!(setup, age_hours) do
    assert {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id}, %{correlation_id: Ecto.UUID.generate(), transport: "websocket"})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    admitted = InstancePresence.database_now() |> DateTime.add(-age_hours * 3600, :second)
    request = Repo.update!(Ecto.Changeset.change(reserved.request, admitted_at: admitted))
    cause = witness(attempt, request) |> put_in(["producer", "owner_execution_id"], Ecto.UUID.generate())
    assert ExecutionIdentity.status(Map.new(cause["producer"], fn {key, value} -> {String.to_existing_atom(key), value} end)) == :unknown
    attempt = Repo.update!(Ecto.Changeset.change(attempt, response_metadata: %{"expired_owner_stop" => cause}))
    {request, attempt, cause}
  end

  defp ledger_kinds(request_id), do: Repo.all(from l in LedgerEntry, where: l.request_id == ^request_id, select: l.entry_kind) |> Enum.frequencies()

  defp witness(attempt, request) do
    now = DateTime.utc_now() |> DateTime.to_iso8601()
    producer = ExecutionIdentity.producer() |> Cleanup.identity()

    %{
      "version" => 1,
      "stop_decision_id" => Ecto.UUID.generate(),
      "phase" => "authorized",
      "session_id" => Ecto.UUID.generate(),
      "lease_id" => Ecto.UUID.generate(),
      "turn_id" => Ecto.UUID.generate(),
      "pool_id" => request.pool_id,
      "api_key_id" => request.api_key_id,
      "model_id" => request.model_id,
      "request_id" => request.id,
      "attempt_id" => attempt.id,
      "replay_generation" => attempt.replay_generation,
      "lease_identity_digest" => Cleanup.digest(Ecto.UUID.generate()),
      "session_deadline" => now,
      "lease_deadline" => now,
      "owner_instance_id" => producer["owner_instance_id"],
      "owner_instance_boot_id" => producer["owner_instance_boot_id"],
      "process_generation" => 1,
      "task_digest" => Cleanup.digest(make_ref()),
      "task_address" => producer["owner_process_id"],
      "downstream_epoch" => 1,
      "executor" => Cleanup.identity(attempt),
      "producer" => producer,
      "authorized_at" => now
    }
  end
end
