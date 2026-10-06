defmodule CodexPooler.Gateway.Persistence.SessionAssignmentClaimTest do
  @moduledoc """
  The claim a native HTTP request makes when its account starts serving the
  client (findings#324): it pins a session that has no pin, only under the
  request's own live owner lease, and never over a pin. Two claims racing on
  separate PostgreSQL connections leave exactly the first committed one.
  """

  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import Ecto.Query

  alias CodexPooler.Gateway.Persistence.{CodexSession, SessionContinuity}
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias Ecto.Adapters.SQL
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000

  describe "claim_session_assignment/3" do
    setup do
      pool = pool_fixture()
      %{api_key: api_key} = active_api_key_fixture(pool)
      %{assignment: first} = upstream_assignment_fixture(pool)
      %{assignment: second} = upstream_assignment_fixture(pool)
      session = start_session!(%{pool: pool, api_key: api_key})
      %{session: session, first: first, second: second}
    end

    test "pins an unpinned session under its live owner token", %{session: session, first: first} do
      assert SessionContinuity.claim_session_assignment(session.id, first.id, session.owner_lease_token) == :claimed
      assert pin(session) == first.id
    end

    test "never overwrites a pin", %{session: session, first: first, second: second} do
      assert SessionContinuity.claim_session_assignment(session.id, first.id, session.owner_lease_token) == :claimed
      assert SessionContinuity.claim_session_assignment(session.id, second.id, session.owner_lease_token) == :unclaimed
      assert pin(session) == first.id
    end

    test "writes nothing for a token that no longer owns the session", %{session: session, first: first} do
      assert SessionContinuity.claim_session_assignment(session.id, first.id, Ecto.UUID.generate()) == :unclaimed
      assert pin(session) == nil
    end

    test "writes nothing once the owner lease expired", %{session: session, first: first} do
      Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [owner_lease_expires_at: DateTime.add(DateTime.utc_now(), -1, :second)])
      assert SessionContinuity.claim_session_assignment(session.id, first.id, session.owner_lease_token) == :unclaimed
      assert pin(session) == nil
    end

    test "writes nothing for a closed session", %{session: session, first: first} do
      Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [status: "closed", closed_at: DateTime.utc_now()])
      assert SessionContinuity.claim_session_assignment(session.id, first.id, session.owner_lease_token) == :unclaimed
      assert pin(session) == nil
    end
  end

  # Two requests of one session serve on different accounts at the same time:
  # the first claim holds the session row inside an open transaction, the
  # second waits on it, and once the first commits the second re-checks the
  # committed row, finds the pin and changes nothing.
  test "of two claims racing on separate connections, the second finds the first's pin and writes nothing" do
    fixture = committed_fixture!()
    parent = self()
    ref = make_ref()

    winner =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            claimed = SessionContinuity.claim_session_assignment(fixture.session.id, fixture.first.id, fixture.session.owner_lease_token)
            send(parent, {:winner_claimed, ref, claimed, backend_pid!()})

            receive do
              {:commit, ^ref} -> claimed
            after
              @budget -> Repo.rollback(:never_released)
            end
          end)
        end)
      end)

    assert_receive {:winner_claimed, ^ref, :claimed, winner_backend}, @budget
    on_exit(fn -> send(winner.pid, {:commit, ref}) end)

    loser =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          send(parent, {:loser_backend, ref, backend_pid!()})
          SessionContinuity.claim_session_assignment(fixture.session.id, fixture.second.id, fixture.session.owner_lease_token)
        end)
      end)

    assert_receive {:loser_backend, ^ref, loser_backend}, @budget
    assert await_block!(loser_backend, winner_backend) == "codex_sessions"

    send(winner.pid, {:commit, ref})
    assert {:ok, :claimed} = Task.await(winner, @budget)
    assert Task.await(loser, @budget) == :unclaimed
    assert Sandbox.unboxed_run(Repo, fn -> pin(fixture.session) end) == fixture.first.id
  end

  defp start_session!(auth) do
    assert {:ok, %CodexSession{pool_upstream_assignment_id: nil} = session} =
             Gateway.start_codex_session(auth, %{
               accepted_turn_state: "session-assignment-claim-#{System.unique_integer([:positive, :monotonic])}",
               owner_instance_id: "node-a"
             })

    session
  end

  defp pin(%CodexSession{id: id}), do: Repo.one!(from(s in CodexSession, where: s.id == ^id, select: s.pool_upstream_assignment_id))

  # The committed owner's registered removal takes the whole graph with it: its
  # Pool cascades to the key, the assignments and the session, and the
  # identities go as their only Pool's holders.
  defp committed_fixture! do
    %{user: owner} = committed_bootstrap_owner_fixture!()

    Sandbox.unboxed_run(Repo, fn ->
      pool = pool_fixture(%{created_by_user_id: owner.id})
      %{api_key: api_key} = active_api_key_fixture(pool, %{created_by_user_id: owner.id})
      %{assignment: first} = upstream_assignment_fixture(pool)
      %{assignment: second} = upstream_assignment_fixture(pool)
      %{session: start_session!(%{pool: pool, api_key: api_key}), first: first, second: second}
    end)
  end

  # `query` comes from the backend-status snapshot the sampling transaction
  # took while `pg_blocking_pids/1` is live, so a sample whose statement names
  # no relation is sampled again until the deadline (findings#206 row 206-182).
  defp await_block!(waiter_backend, blocker_backend, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @budget

    rows =
      Sandbox.unboxed_run(Repo, fn ->
        SQL.query!(Repo, "SELECT query FROM pg_stat_activity WHERE pid = $1 AND $2 = ANY(pg_blocking_pids(pid))", [waiter_backend, blocker_backend]).rows
      end)

    case rows do
      [[query] | _rest] when is_binary(query) ->
        case Regex.run(~r/UPDATE "(\w+)"/, query) do
          [_match, relation] -> relation
          nil -> resample!(waiter_backend, blocker_backend, deadline)
        end

      _not_yet ->
        resample!(waiter_backend, blocker_backend, deadline)
    end
  end

  defp resample!(waiter_backend, blocker_backend, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      flunk("the second claim never waited on the first")
    else
      Process.sleep(20)
      await_block!(waiter_backend, blocker_backend, deadline)
    end
  end

  defp backend_pid! do
    %{rows: [[backend_pid]]} = SQL.query!(Repo, "SELECT pg_backend_pid()", [])
    backend_pid
  end
end
