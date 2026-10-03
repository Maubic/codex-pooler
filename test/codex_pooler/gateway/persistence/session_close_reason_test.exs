defmodule CodexPooler.Gateway.Persistence.SessionCloseReasonTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import Ecto.Query

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.ExpiredSessions
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Gateway.Websocket.OwnerCleanup

  setup do
    %{api_key: key, pool: pool} = active_api_key_fixture()
    auth = %{api_key: key, pool: pool}
    opts = RequestOptions.for_websocket(%{session_header: "sample-session-#{System.unique_integer([:positive])}"})
    {:ok, session} = SessionContinuity.start_codex_session(auth, opts)
    %{session: session, auth: auth, opts: opts}
  end

  test "real expiry writer certifies closed session with database time", %{session: session} do
    expire(session)
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture_expiry_phase/4, self())
    assert %{closed_count: 1} = close_expired(session)
    closed = Repo.get!(CodexSession, session.id)
    assert closed.status == "closed"
    assert closed.close_reason == "owner_lease_expired"
    assert DateTime.compare(closed.owner_lease_expires_at, closed.closed_at) != :gt
    assert_receive {:expiry_phase, first}
    assert first == :session_lock
    assert_receive {:expiry_phase, second}
    assert second == :lease_lock
    assert_receive {:expiry_phase, third}
    assert third == :alias_lock
    assert_receive {:expiry_phase, {:clock, clock}}
    assert closed.closed_at == clock
  end

  def capture_expiry_phase(_event, _measurements, %{query: query, result: result}, parent) when parent == self() do
    cond do
      query == "SELECT clock_timestamp()" ->
        {:ok, %{rows: [[clock]]}} = result
        send(parent, {:expiry_phase, {:clock, clock}})

      String.contains?(query, "FOR UPDATE") and String.contains?(query, "\"codex_sessions\"") ->
        send(parent, {:expiry_phase, :session_lock})

      String.contains?(query, "FOR UPDATE") and String.contains?(query, "\"bridge_owner_leases\"") ->
        send(parent, {:expiry_phase, :lease_lock})

      String.contains?(query, "FOR UPDATE") and String.contains?(query, "\"bridge_session_aliases\"") ->
        send(parent, {:expiry_phase, :alias_lock})

      true ->
        :ok
    end
  end

  def capture_expiry_phase(_event, _measurements, _metadata, _parent), do: :ok

  test "legacy session without an active lease retains actual expiry qualification", %{session: session} do
    expire(session)
    Repo.delete_all(from lease in BridgeOwnerLease, where: lease.codex_session_id == ^session.id)
    assert %{closed_count: 1} = close_expired(session)
    assert Repo.get!(CodexSession, session.id).close_reason == "owner_lease_expired"
  end

  @tag :close_reason_negative
  test "live active lease prevents certification of stale session deadline", %{session: session} do
    Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [owner_lease_expires_at: DateTime.add(DateTime.utc_now(), -1)])
    assert %{closed_count: 0} = close_expired(session)
    assert Repo.get!(CodexSession, session.id).status == "active"
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  @tag :close_reason_negative
  test "conflicting active lease token prevents certification", %{session: session} do
    expire(session)
    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id), set: [lease_token: Ecto.UUID.generate()])
    assert %{closed_count: 0} = close_expired(session)
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  for {field, expression} <- [
        {:status, "'interrupted'"},
        {:closed_at, "NULL"},
        {:owner_lease_token, "NULL, owner_instance_id = NULL, owner_lease_expires_at = NULL, last_heartbeat_at = NULL"},
        {:owner_lease_expires_at, "NULL, owner_instance_id = NULL, owner_lease_token = NULL, last_heartbeat_at = NULL"},
        {:api_key_id, "NULL"},
        {:session_key, "session_key || '-changed'"}
      ] do
    @tag :close_reason_negative
    test "old SQL writer invalidates expiry authority when #{field} changes", %{session: session} do
      certify(session)
      update_sql(session, unquote("#{field} = #{expression}"))
      assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
    end
  end

  test "unrelated metadata update preserves a genuine certificate", %{session: session} do
    certify(session)
    update_sql(session, "updated_at = clock_timestamp(), owner_instance_id = 'sample-peer'")
    assert Repo.get!(CodexSession, session.id).close_reason == "owner_lease_expired"
  end

  @tag :close_reason_negative
  test "old reopen then unrelated close cannot recover stale authority", %{session: session} do
    certify(session)
    update_sql(session, "status = 'active', closed_at = NULL")
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
    update_sql(session, "status = 'closed', closed_at = clock_timestamp()")
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  @tag :close_reason_negative
  test "stale old-token finalizer cannot change reopened session or later certify unrelated close", %{session: session} do
    certify(session)
    update_sql(session, "status = 'active', closed_at = NULL, owner_lease_token = gen_random_uuid(), owner_lease_expires_at = clock_timestamp() + interval '1 minute'")
    witness = %OwnerCleanup{session_id: session.id, owner_instance_id: session.owner_instance_id, owner_lease_token: session.owner_lease_token, request_id: Ecto.UUID.generate(), attempt_id: Ecto.UUID.generate(), replay_generation: 0, downstream_epoch: 1}
    opts = RequestOptions.for_websocket(%{websocket_owner_lease_token: session.owner_lease_token})
    opts = %{opts | runtime: %{opts.runtime | owner_cleanup: witness}}
    assert {:error, :stale_owner_cleanup} = Interruption.interrupt_codex_session(session, opts)
    reopened = Repo.get!(CodexSession, session.id)
    assert reopened.status == "active"
    refute reopened.owner_lease_token == session.owner_lease_token
    assert is_nil(reopened.close_reason)
    update_sql(session, "status = 'closed', closed_at = clock_timestamp()")
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  @tag :close_reason_negative
  test "pre-lock future clock cannot certify expiry before current database time", %{session: session} do
    prelock_now = DateTime.add(DateTime.utc_now(), 3600, :second)
    {:ok, result} = Repo.transaction(fn -> ExpiredSessions.close_for_key!(session.pool_id, session.api_key_id, session.session_key, prelock_now) end)
    assert result.closed_count == 0
    assert Repo.get!(CodexSession, session.id).status == "active"
  end

  @tag :close_reason_negative
  test "already closed legacy row cannot be recertified", %{session: session} do
    update_sql(session, "status = 'closed', closed_at = clock_timestamp(), owner_lease_expires_at = clock_timestamp() - interval '1 second'")
    update_sql(session, "close_reason = 'owner_lease_expired'")
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  @tag :close_reason_negative
  test "future deadline and arbitrary reason are not certificates", %{session: session} do
    update_sql(session, "status = 'closed', closed_at = clock_timestamp(), close_reason = 'owner_lease_expired'")
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
    update_sql(session, "close_reason = 'unrelated'")
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  test "new insert and renewal never fabricate close reason", %{session: session, auth: auth, opts: opts} do
    assert is_nil(session.close_reason)
    assert {:ok, renewed} = SessionContinuity.start_codex_session(auth, opts)
    assert renewed.id == session.id
    assert is_nil(renewed.close_reason)
  end

  @tag :close_reason_negative
  test "old SQL writer changing pool invalidates expiry authority", %{session: session} do
    certify(session)
    other_pool = pool_fixture()
    Repo.query!("UPDATE codex_sessions SET pool_id = $1 WHERE id = $2", [Ecto.UUID.dump!(other_pool.id), Ecto.UUID.dump!(session.id)])
    assert is_nil(Repo.get!(CodexSession, session.id).close_reason)
  end

  @tag :close_reason_negative
  test "database constraint rejects malformed inserted provenance", %{session: session} do
    assert {:error, %Postgrex.Error{postgres: %{code: :check_violation, constraint: "codex_sessions_close_reason_check"}}} =
             Repo.query("INSERT INTO codex_sessions (id, pool_id, api_key_id, session_key, status, close_reason, created_at, updated_at) VALUES ($1, $2, $3, 'sample-invalid', 'active', 'owner_lease_expired', clock_timestamp(), clock_timestamp())", [Ecto.UUID.dump!(Ecto.UUID.generate()), Ecto.UUID.dump!(session.pool_id), Ecto.UUID.dump!(session.api_key_id)], mode: :savepoint)
  end

  defp certify(session) do
    expire(session)
    assert %{closed_count: 1} = close_expired(session)
    assert Repo.get!(CodexSession, session.id).close_reason == "owner_lease_expired"
  end

  defp expire(session) do
    deadline = DateTime.add(DateTime.utc_now(), -1, :second)
    Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [owner_lease_expires_at: deadline])
    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id), set: [expires_at: deadline])
  end

  defp close_expired(session) do
    {:ok, result} = Repo.transaction(fn -> ExpiredSessions.close_for_key!(session.pool_id, session.api_key_id, session.session_key, DateTime.utc_now()) end)
    result
  end

  defp update_sql(session, assignments) do
    Repo.query!("UPDATE codex_sessions SET #{assignments} WHERE id = $1", [Ecto.UUID.dump!(session.id)])
  end
end
