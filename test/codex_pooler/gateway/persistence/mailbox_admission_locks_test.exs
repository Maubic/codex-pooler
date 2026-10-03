defmodule CodexPooler.Gateway.Persistence.MailboxAdmissionLocksTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.{Request, RequestClientRetryLink}
  alias CodexPooler.Gateway.Persistence.{BridgeSessionAlias, CodexSession}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.MailboxAdmissionLocks
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @exhausted %{code: "duplicate_turn", mailbox_check: :session}

  @tag mailbox_admission_lock_order: true
  test "historical sessions are locked before the key on independent PostgreSQL backends" do
    fixture = committed_fixture()
    parent = self()
    ref = make_ref()
    [historical | _] = Enum.sort_by(fixture.sessions, & &1.id)

    blocker =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            Repo.one!(from session in CodexSession, where: session.id == ^historical.id, lock: "FOR UPDATE")
            send(parent, {:holding_session, ref, backend_pid()})
            receive do: ({:release, ^ref} -> :ok)
          end)
        end)
      end)

    on_exit(fn -> if Process.alive?(blocker.pid), do: Process.exit(blocker.pid, :kill) end)
    assert_receive {:holding_session, ^ref, blocker_backend}, @budget

    contender =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          send(parent, {:contender, ref, backend_pid()})

          MailboxAdmissionLocks.transaction(
            fn -> Enum.map(Enum.reverse(fixture.sessions), & &1.id) end,
            fn ->
              MailboxAdmissionLocks.require_sessions!(Enum.map(fixture.sessions, & &1.id))
              Repo.one!(from key in APIKey, where: key.id == ^fixture.api_key.id, lock: "FOR UPDATE")
              :admitted
            end,
            @exhausted
          )
        end)
      end)

    on_exit(fn -> if Process.alive?(contender.pid), do: Process.exit(contender.pid, :kill) end)
    assert_receive {:contender, ^ref, contender_backend}, @budget
    refute contender_backend == blocker_backend
    query = await_block(contender_backend, blocker_backend)
    assert query =~ "codex_sessions"
    assert query =~ "ORDER BY"

    newest = fixture.sessions |> Enum.sort_by(& &1.id) |> List.last()

    assert {:ok, :later_session_free} =
             Sandbox.unboxed_run(Repo, fn ->
               Repo.transaction(fn ->
                 Repo.one!(from session in CodexSession, where: session.id == ^newest.id, lock: "FOR UPDATE NOWAIT")
                 :later_session_free
               end)
             end)

    # The key remains acquirable while admission waits for the oldest session.
    assert {:ok, :key_free} =
             Sandbox.unboxed_run(Repo, fn ->
               Repo.transaction(fn ->
                 Repo.one!(from key in APIKey, where: key.id == ^fixture.api_key.id, lock: "FOR UPDATE NOWAIT")
                 :key_free
               end)
             end)

    send(blocker.pid, {:release, ref})
    assert {:ok, :ok} = Task.await(blocker, @budget)
    assert {:ok, :admitted} = Task.await(contender, @budget)
    emit_receipt(%{scenario: "sorted_session_prelocks", blocker_backend: blocker_backend, admission_backend: contender_backend, blocking_relation: "codex_sessions", key_available_before_session_lock: true, later_session_available: true})
  end

  for added_sessions <- [1, 2, 3] do
    @tag mailbox_lock_rediscovery: true
    test "nested rollback exits the outer transaction before #{added_sessions} session rediscoveries" do
      fixture = committed_fixture()
      added_sessions = unquote(added_sessions)
      original = hd(fixture.sessions)

      result =
        Sandbox.unboxed_run(Repo, fn ->
          Process.put({__MODULE__, :attempts}, 0)
          Process.put({__MODULE__, :discoveries}, 0)
          Process.put({__MODULE__, :transactions}, [])

          discover = fn ->
            refute Repo.in_transaction?()
            assert Repo.get!(CodexSession, original.id).session_key == original.session_key
            assert_no_admission_writes(fixture)
            n = Process.get({__MODULE__, :discoveries}) + 1
            Process.put({__MODULE__, :discoveries}, n)
            ids = session_ids(fixture.pool.id)

            if n <= added_sessions do
              # A committed write on a genuinely separate backend occurs after
              # the discovery snapshot and before admission locks it.
              discovering_backend = backend_pid()

              task =
                Task.async(fn ->
                  Sandbox.unboxed_run(Repo, fn ->
                    refute backend_pid() == discovering_backend
                    insert_session(fixture, "new-#{n}")
                  end)
                end)

              Task.await(task, @budget)
            end

            ids
          end

          operation = fn ->
            n = Process.get({__MODULE__, :attempts}) + 1
            Process.put({__MODULE__, :attempts}, n)
            %{rows: [[transaction_id]]} = Repo.query!("SELECT txid_current()")
            Process.put({__MODULE__, :transactions}, [transaction_id | Process.get({__MODULE__, :transactions})])
            Repo.update_all(from(s in CodexSession, where: s.id == ^original.id), set: [session_key: "rolled-back-#{n}"])
            if n <= added_sessions, do: insert_admission_writes(fixture, original)

            MailboxAdmissionLocks.transaction(
              fn -> flunk("nested admission discovered inside the old transaction") end,
              fn -> MailboxAdmissionLocks.require_sessions!(session_ids(fixture.pool.id)) end,
              @exhausted
            )
            |> case do
              {:ok, :ok} ->
                Repo.update_all(from(s in CodexSession, where: s.id == ^original.id), set: [session_key: original.session_key])
                :admitted
            end
          end

          result = MailboxAdmissionLocks.transaction(discover, operation, @exhausted)
          assert Process.get({__MODULE__, :attempts}) == min(added_sessions + 1, 3)
          assert Process.get({__MODULE__, :discoveries}) == min(added_sessions + 1, 3)
          refute Repo.in_transaction?()
          assert Repo.get!(CodexSession, original.id).session_key == original.session_key
          assert_no_admission_writes(fixture)
          transactions = Process.get({__MODULE__, :transactions}) |> Enum.reverse()
          assert length(Enum.uniq(transactions)) == length(transactions)
          emit_receipt(%{scenario: "nested_rediscovery", inserted_sessions: added_sessions, transactions: transactions, attempts: Process.get({__MODULE__, :attempts}), previous_write_rolled_back: true, alias_request_link_writes: 0, outer_transaction_exited: true, exhausted: added_sessions == 3})
          result
        end)

      if added_sessions < 3, do: assert(result == {:ok, :admitted}), else: assert(result == {:error, @exhausted})
    end
  end

  @tag mailbox_lock_rediscovery: true
  test "a transaction without prelock context fails closed and leaves no write" do
    fixture = committed_fixture()
    original = hd(fixture.sessions)

    Sandbox.unboxed_run(Repo, fn ->
      assert {:error, @exhausted} =
               Repo.transaction(fn ->
                 Repo.update_all(from(s in CodexSession, where: s.id == ^original.id), set: [session_key: "must-rollback"])
                 MailboxAdmissionLocks.transaction(fn -> flunk("late discovery") end, fn -> :admitted end, @exhausted)
               end)

      assert Repo.get!(CodexSession, original.id).session_key == original.session_key
    end)
  end

  @tag mailbox_lock_rediscovery: true
  test "a disappeared discovered row exhausts without invoking admission" do
    fixture = committed_fixture()
    missing_id = Ecto.UUID.generate()

    Sandbox.unboxed_run(Repo, fn ->
      assert {:error, @exhausted} =
               MailboxAdmissionLocks.transaction(
                 fn -> [missing_id | Enum.map(fixture.sessions, & &1.id)] end,
                 fn -> flunk("admission ran with a missing discovered session") end,
                 @exhausted
               )
    end)
  end

  @tag mailbox_lock_rediscovery: true
  test "exception and success clear transaction coordination state" do
    fixture = committed_fixture()

    Sandbox.unboxed_run(Repo, fn ->
      discover = fn -> Enum.map(fixture.sessions, & &1.id) end

      assert_raise RuntimeError, "owned admission failure", fn ->
        MailboxAdmissionLocks.transaction(discover, fn -> raise "owned admission failure" end, @exhausted)
      end

      assert {:ok, :ok} = MailboxAdmissionLocks.transaction(discover, fn -> :ok end, @exhausted)

      assert {:error, @exhausted} =
               Repo.transaction(fn ->
                 MailboxAdmissionLocks.transaction(fn -> flunk("context leaked") end, fn -> :admitted end, @exhausted)
               end)
    end)
  end

  defp committed_fixture do
    %{user: owner} = committed_bootstrap_owner_fixture!()

    Sandbox.unboxed_run(Repo, fn ->
      pool = pool_fixture(%{created_by_user_id: owner.id})
      %{api_key: key} = active_api_key_fixture(pool, %{created_by_user_id: owner.id})
      fixture = %{pool: pool, api_key: key}
      Map.put(fixture, :sessions, Enum.map(1..3, &insert_session(fixture, "initial-#{&1}")))
    end)
  end

  defp insert_session(fixture, label) do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()")
    Repo.insert!(%CodexSession{pool_id: fixture.pool.id, api_key_id: fixture.api_key.id, session_key: "mailbox-lock-#{label}-#{System.unique_integer([:positive, :monotonic])}", status: "active", created_at: now, updated_at: now})
  end

  defp session_ids(pool_id), do: Repo.all(from s in CodexSession, where: s.pool_id == ^pool_id, select: s.id)

  defp insert_admission_writes(fixture, session) do
    predecessor = request_fixture(fixture, %{status: "accepted", completed_at: nil})
    successor = request_fixture(fixture, %{status: "accepted", completed_at: nil})
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()")
    Repo.insert!(%RequestClientRetryLink{predecessor_request_id: predecessor.id, successor_request_id: successor.id, created_at: now})
    Repo.insert!(%BridgeSessionAlias{codex_session_id: session.id, pool_id: fixture.pool.id, api_key_id: fixture.api_key.id, alias_kind: "turn_state", alias_hash: :crypto.strong_rand_bytes(32), status: "active", expires_at: DateTime.add(now, 30, :second), metadata: %{}, created_at: now, updated_at: now})
  end

  defp assert_no_admission_writes(fixture) do
    refute Repo.exists?(from r in Request, where: r.pool_id == ^fixture.pool.id)
    refute Repo.exists?(from a in BridgeSessionAlias, where: a.pool_id == ^fixture.pool.id)
    refute Repo.exists?(from l in RequestClientRetryLink, join: r in Request, on: r.id == l.predecessor_request_id, where: r.pool_id == ^fixture.pool.id)
  end

  defp backend_pid do
    %{rows: [[pid]]} = Repo.query!("SELECT pg_backend_pid()")
    pid
  end

  defp emit_receipt(receipt) do
    if System.get_env("CODEX_POOLER_LOCK_TEST_DIAGNOSTICS") == "true", do: IO.puts(Jason.encode!(receipt))
  end

  defp await_block(waiter, blocker), do: await_block(waiter, blocker, System.monotonic_time(:millisecond) + @budget)

  defp await_block(waiter, blocker, deadline) do
    rows =
      Sandbox.unboxed_run(Repo, fn ->
        Repo.query!("SELECT query FROM pg_stat_activity WHERE pid=$1 AND $2=ANY(pg_blocking_pids(pid))", [waiter, blocker]).rows
      end)

    case rows do
      [[query]] ->
        query

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "expected PostgreSQL session lock wait"
        Process.sleep(20)
        await_block(waiter, blocker, deadline)
    end
  end
end
