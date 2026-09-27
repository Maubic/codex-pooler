defmodule CodexPooler.Gateway.Persistence.SessionDatabaseClockTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, BridgeSessionAlias, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.Aliases
  alias CodexPooler.Repo

  setup do
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.capture_clock/4, self())
    %{auth: active_api_key_fixture(), options: RequestOptions.for_websocket(%{session_header: "database-clock-session", session_header_source: "x-codex-window-id"})}
  end

  test "session creation and lease renewal write timestamps sampled from PostgreSQL", %{auth: auth, options: options} do
    flush_clock()
    assert {:ok, session} = SessionContinuity.start_codex_session(auth, options)
    samples = clock_samples()
    assert samples != []
    assert session.created_at in samples
    assert session.last_heartbeat_at in samples
    lease = Repo.get_by!(BridgeOwnerLease, lease_token: session.owner_lease_token)
    assert lease.acquired_at in samples
    assert session.owner_lease_expires_at == lease.expires_at

    assert {:ok, renewed} = SessionContinuity.renew_owner_token(session, session.owner_lease_token, options)
    samples = clock_samples()
    assert samples != []
    lease = Repo.get_by!(BridgeOwnerLease, lease_token: renewed.owner_lease_token)
    assert lease.renewed_at in samples
    assert renewed.last_heartbeat_at == lease.renewed_at
  end

  test "alias default timestamps come from the database and strict lookup samples it", %{auth: auth, options: options} do
    {:ok, session} = SessionContinuity.start_codex_session(auth, options)
    flush_clock()
    header_hash = :crypto.hash(:sha256, "clock-header")
    assert :ok = Aliases.register_session_header_hash(session, auth, header_hash)
    assert [now] = clock_samples()
    assert Repo.get_by!(BridgeSessionAlias, alias_hash: header_hash).last_seen_at == now

    frame_hash = :crypto.hash(:sha256, "clock-frame")
    assert :created = Aliases.point_frame_window_hash(session, auth, frame_hash)
    assert [now] = clock_samples()
    assert Repo.get_by!(BridgeSessionAlias, alias_hash: frame_hash).last_seen_at == now

    assert nil == SessionContinuity.previous_response_session_id(auth, "resp_synthetic_missing")
    assert [_now] = clock_samples()
    assert nil == SessionContinuity.previous_response_resolution(auth, "resp_synthetic_missing")
    assert [_now] = clock_samples()
  end

  def capture_clock(_event, _measurements, %{query: "SELECT clock_timestamp()", result: {:ok, %{rows: [[now]]}}}, parent) when parent == self(), do: send(parent, {:database_clock, now})
  def capture_clock(_event, _measurements, _metadata, _parent), do: :ok

  defp flush_clock, do: clock_samples()

  defp clock_samples do
    receive do
      {:database_clock, now} -> [now | clock_samples()]
    after
      0 -> []
    end
  end
end
