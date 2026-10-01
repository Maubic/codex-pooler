defmodule CodexPooler.Jobs.DeletionDeadlineTest do
  use CodexPooler.DataCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog
  import CodexPooler.PoolerFixtures, only: [request_fixture: 1]

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Access.APIKeys.Deletion, as: KeyDeletion
  alias CodexPooler.Accounting.Request
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Jobs.DeletionDeadline
  alias CodexPooler.Pools
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @detection_timeout_ms 15_000

  setup do
    name = String.to_atom("deletion_deadline_repo_#{System.unique_integer([:positive])}")
    config = Repo.config() |> Keyword.put(:name, name) |> Keyword.put(:pool, DBConnection.ConnectionPool) |> Keyword.put(:pool_size, 1)
    start_supervised!({Repo, config})
    Repo.put_dynamic_repo(name)
    %{repo: name}
  end

  test "remaining clamps elapsed deadlines and an expired run never starts work" do
    now = System.monotonic_time(:millisecond)
    assert DeletionDeadline.remaining(now - 1) == 0
    assert DeletionDeadline.remaining(now + 10_000) in 1..10_000
    assert :more = DeletionDeadline.run(now - 1, fn -> flunk("expired operation ran") end)
  end

  test "a caller trapping exits receives the executor's failure result" do
    Process.flag(:trap_exit, true)
    {result, log} = with_log(fn -> DeletionDeadline.run(System.monotonic_time(:millisecond) + 5_000, fn -> exit(:sample_deletion_exit) end) end)
    assert {:error, :sample_deletion_exit} = result
    assert log =~ "sample_deletion_exit"
    assert_receive {:EXIT, _pid, :sample_deletion_exit}
  end

  for target <- [:pool, :key] do
    test "#{target} cumulative finalization cannot overrun its absolute deadline" do
      {pool, key, trigger} = fixture(unquote(target))
      table = if unquote(target) == :pool, do: "pools", else: "api_keys"
      id = if unquote(target) == :pool, do: pool.id, else: key.id
      # Final deletion is bounded even after the history batches have finished.
      Repo.query!("CREATE FUNCTION #{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF OLD.id = '#{id}'::uuid THEN PERFORM pg_sleep(0.2); END IF; RETURN OLD; END $$")
      Repo.query!("CREATE TRIGGER #{trigger}_a BEFORE DELETE ON #{table} FOR EACH ROW EXECUTE FUNCTION #{trigger}()")
      Repo.query!("CREATE TRIGGER #{trigger}_b BEFORE DELETE ON #{table} FOR EACH ROW EXECUTE FUNCTION #{trigger}()")
      started = System.monotonic_time(:millisecond)
      {result, _log} = with_log(fn -> continue(unquote(target), id, started + 300) end)
      elapsed = System.monotonic_time(:millisecond) - started
      assert result == :more
      assert elapsed < 1_000
      assert Repo.get(Pool, pool.id)
      if key, do: assert(Repo.get(APIKey, key.id))
      Repo.query!("DROP TRIGGER #{trigger}_a ON #{table}")
      Repo.query!("DROP TRIGGER #{trigger}_b ON #{table}")
      assert continue(unquote(target), id, System.monotonic_time(:millisecond) + 5_000) == :deleted
    end
  end

  for target <- [:pool, :key] do
    @tag slow: "holds the second batch's row lock until the two-second absolute deadline"
    test "#{target} deadline stops the second batch and preserves the first committed batch", %{repo: repo} do
      {pool, key, _trigger} = fixture(unquote(target))
      request = request_fixture(%{pool: pool, api_key: key})
      now = DateTime.utc_now()
      session = Repo.insert!(%CodexSession{pool_id: pool.id, api_key_id: key.id, session_key: "deadline-#{pool.id}", status: "active", created_at: now, updated_at: now})
      config = Keyword.take(Repo.config(), [:hostname, :port, :database, :username, :password, :ssl])
      holder = start_supervised!({Postgrex, config}, id: :second_batch_holder)
      observer = start_supervised!({Postgrex, config}, id: :second_batch_observer)
      supervisor = start_supervised!(Task.Supervisor)
      Postgrex.query!(holder, "BEGIN", [])
      %{rows: [[holder_backend]]} = Postgrex.query!(holder, "SELECT pg_backend_pid()", [])
      Postgrex.query!(holder, "SELECT id FROM codex_sessions WHERE id = $1 FOR UPDATE", [Ecto.UUID.dump!(session.id)])
      id = if unquote(target) == :pool, do: pool.id, else: key.id

      # A held row, not two competing sleeps, forces the deadline to cut the second batch.
      # The two-second scenario budget leaves the first commit scheduling room under N=4.
      {result, log} =
        with_log(fn ->
          runner =
            Task.Supervisor.async_nolink(supervisor, fn ->
              Repo.put_dynamic_repo(repo)
              continue(unquote(target), id, System.monotonic_time(:millisecond) + 2_000)
            end)

          monitor = Process.monitor(runner.pid)
          waiter = await_session_batch(observer, holder_backend, System.monotonic_time(:millisecond) + @detection_timeout_ms)
          refute waiter == holder_backend
          %{rows: committed_rows} = Postgrex.query!(observer, "SELECT api_key_id FROM requests WHERE id = $1", [Ecto.UUID.dump!(request.id)])
          assert committed_rows == if(unquote(target) == :pool, do: [], else: [[nil]])
          result = Task.await(runner, @detection_timeout_ms)
          assert_receive {:DOWN, ^monitor, :process, _, :normal}, @detection_timeout_ms
          result
        end)

      assert result == :more
      assert log == "" or log =~ "timed out because it queued and checked out the connection"
      if unquote(target) == :pool, do: refute(Repo.get(Request, request.id)), else: assert(is_nil(Repo.get!(Request, request.id).api_key_id))
      assert Repo.get(CodexSession, session.id)
      Postgrex.query!(holder, "ROLLBACK", [])
      assert continue(unquote(target), id, System.monotonic_time(:millisecond) + 5_000) == :deleted
    end
  end

  test "a queued checkout is cancelled by the outer budget and its executor dies", %{repo: repo} do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.put_dynamic_repo(repo)

        Repo.checkout(fn ->
          send(parent, :connection_held)

          receive do
            :release -> :ok
          end
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    on_exit(fn -> if Process.alive?(holder.pid), do: Process.exit(holder.pid, :kill) end)
    assert_receive :connection_held
    tracer = spawn_link(fn -> trace_executor(parent) end)
    tracer_monitor = Process.monitor(tracer)
    on_exit(fn -> if Process.alive?(tracer), do: Process.exit(tracer, :kill) end)
    :erlang.trace(self(), true, [:procs, {:tracer, tracer}])
    # The only connection stays held beyond this call's deadline.
    started = System.monotonic_time(:millisecond)
    assert :more = DeletionDeadline.run(started + 100, fn -> send(parent, :unexpected_checkout) end)
    :erlang.trace(self(), false, [:procs])
    assert_receive {:executor_spawned, executor}
    refute Process.alive?(executor)
    assert_receive {:DOWN, ^tracer_monitor, :process, _, :normal}
    assert Process.alive?(holder.pid)
    refute_received :unexpected_checkout
    send(holder.pid, :release)
    assert :ok = Task.await(holder)
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}
    assert Repo.query!("SELECT 1").rows == [[1]]
    refute_received :unexpected_checkout
  end

  test "terminating a run also terminates its database executor", %{repo: repo} do
    parent = self()

    runner =
      Task.async(fn ->
        Repo.put_dynamic_repo(repo)

        DeletionDeadline.run(System.monotonic_time(:millisecond) + 10_000, fn ->
          send(parent, {:executor_started, self()})

          receive do
            :run_query -> Repo.query!("SELECT pg_sleep(5)")
          end
        end)
      end)

    runner_monitor = Process.monitor(runner.pid)
    on_exit(fn -> if Process.alive?(runner.pid), do: Process.exit(runner.pid, :kill) end)
    assert_receive {:executor_started, executor}
    executor_monitor = Process.monitor(executor)
    Process.unlink(runner.pid)
    Process.exit(runner.pid, :kill)
    assert_receive {:DOWN, ^runner_monitor, :process, _, :killed}
    assert_receive {:DOWN, ^executor_monitor, :process, _, :killed}
    Process.demonitor(runner.ref, [:flush])
    assert Repo.query!("SELECT 1").rows == [[1]]
  end

  defp await_session_batch(observer, holder_backend, deadline) do
    %{rows: rows} =
      Postgrex.query!(observer, "SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND $1 = ANY(pg_blocking_pids(pid)) AND query LIKE 'DELETE FROM codex_sessions %'", [holder_backend])

    case rows do
      [[waiter]] ->
        waiter

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "deletion never reached the held second batch"
        await_session_batch(observer, holder_backend, deadline)
    end
  end

  defp trace_executor(parent) do
    receive do
      {:trace, ^parent, :spawn, pid, _mfa} -> send(parent, {:executor_spawned, pid})
      _other -> trace_executor(parent)
    end
  end

  defp continue(:pool, id, deadline), do: Pools.continue_pool_deletion(id, nil, deadline)
  defp continue(:key, id, deadline), do: KeyDeletion.continue(id, nil, deadline)

  defp fixture(target) do
    pool_id = Ecto.UUID.generate()
    key_id = Ecto.UUID.generate()
    trigger = "deletion_deadline_#{System.unique_integer([:positive])}"
    table = if target == :pool, do: "pools", else: "api_keys"

    on_exit(fn ->
      Sandbox.unboxed_run(Repo, fn ->
        Repo.query!("DROP TRIGGER IF EXISTS #{trigger}_a ON #{table}")
        Repo.query!("DROP TRIGGER IF EXISTS #{trigger}_b ON #{table}")
        Repo.query!("DROP TRIGGER IF EXISTS #{trigger}_requests ON requests")
        Repo.query!("DROP TRIGGER IF EXISTS #{trigger}_sessions ON codex_sessions")
        Repo.query!("DROP FUNCTION IF EXISTS #{trigger}()")
        Repo.delete_all(from e in CodexPooler.Audit.AuditEvent, where: e.target_id in ^[pool_id, key_id])
        Repo.delete_all(from p in Pool, where: p.id == ^pool_id)
      end)
    end)

    now = DateTime.utc_now()
    pool = Repo.insert!(%Pool{id: pool_id, name: "Deadline sample", slug: "deadline-#{pool_id}", status: "archived", created_at: now, updated_at: now})
    key = Repo.insert!(%APIKey{id: key_id, pool_id: pool.id, display_name: "Deadline key", key_prefix: "deadline-#{key_id}", key_hash: :crypto.strong_rand_bytes(32), status: "revoked", revoked_at: now, created_at: now})
    {pool, key, trigger}
  end
end
