defmodule CodexPooler.Gateway.Persistence.OwnerAcquisitionClockTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture
  import Ecto.Query

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, SessionContinuity}
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  test "acquisition samples database time after waiting for the session row" do
    slug = "owner-clock-#{System.unique_integer([:positive])}"

    register_unboxed_cleanup!(fn ->
      ids = Repo.all(from pool in CodexPooler.Pools.Pool, where: pool.slug == ^slug, select: pool.id)
      delete_committed_pools!(ids)
    end)

    fixture =
      run_unboxed(fn ->
        pool = pool_fixture(%{slug: slug})
        auth = active_api_key_fixture(pool)
        opts = RequestOptions.for_websocket(%{session_header: slug, session_header_source: "x-codex-window-id"})
        assert {:ok, session} = SessionContinuity.start_codex_session(auth, opts)
        Repo.update_all(from(lease in BridgeOwnerLease, where: lease.codex_session_id == ^session.id), set: [status: "released"])
        %{auth: auth, opts: opts, session: session}
      end)

    blocker = start_supervised!({Postgrex, Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database])})
    Postgrex.query!(blocker, "BEGIN", [])
    Postgrex.query!(blocker, "SELECT id FROM codex_sessions WHERE id = $1 FOR UPDATE", [Ecto.UUID.dump!(fixture.session.id)])
    %{rows: [[blocker_pid]]} = Postgrex.query!(blocker, "SELECT pg_backend_pid()", [])
    parent = self()

    waiter =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          %{rows: [[pid]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:waiter, pid})
          SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
        end)
      end)

    assert_receive {:waiter, waiter_pid}, 5_000
    await_blocked(blocker, waiter_pid, blocker_pid, System.monotonic_time(:millisecond) + 5_000)
    %{rows: [[released_at]]} = Postgrex.query!(blocker, "SELECT clock_timestamp()", [])
    Postgrex.query!(blocker, "COMMIT", [])
    assert {:ok, session} = Task.await(waiter, 10_000)
    lease = run_unboxed(fn -> Repo.get_by!(BridgeOwnerLease, lease_token: session.owner_lease_token) end)
    assert DateTime.compare(lease.acquired_at, released_at) in [:eq, :gt]
    assert session.owner_lease_expires_at == lease.expires_at
  end

  test "sharing an unexpired foreign lease retains its deadline and synchronous renewal updates both rows" do
    auth = active_api_key_fixture()
    first = RequestOptions.for_websocket(%{session_header: "shared-foreign", session_header_source: "x-codex-window-id", owner_instance_id: "sample-owner-a"})
    second = RequestOptions.put_continuity(first, owner_instance_id: "sample-owner-b")
    assert {:ok, original} = SessionContinuity.start_codex_session(auth, first)
    assert {:ok, shared} = SessionContinuity.start_codex_session(auth, second)
    assert shared.owner_lease_token == original.owner_lease_token
    assert shared.owner_instance_id == original.owner_instance_id
    assert shared.owner_lease_expires_at == original.owner_lease_expires_at
    before_renewal = InstancePresence.database_now()
    assert {:ok, renewed} = SessionContinuity.renew_owner_token(shared, shared.owner_lease_token, second, take_over_expired: true)
    lease = Repo.get_by!(BridgeOwnerLease, lease_token: renewed.owner_lease_token)
    assert DateTime.compare(lease.renewed_at, before_renewal) in [:eq, :gt]
    assert renewed.owner_lease_expires_at == lease.expires_at
  end

  defp await_blocked(conn, waiter, blocker, deadline) do
    %{rows: [[blocked]]} = Postgrex.query!(conn, "SELECT $1 = ANY(pg_blocking_pids($2))", [blocker, waiter])

    unless blocked do
      assert System.monotonic_time(:millisecond) < deadline, "session acquisition never waited on the owned row lock"
      Process.sleep(10)
      await_blocked(conn, waiter, blocker, deadline)
    end
  end
end
