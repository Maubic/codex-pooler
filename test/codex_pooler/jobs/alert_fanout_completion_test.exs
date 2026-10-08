defmodule CodexPooler.Jobs.AlertFanoutCompletionTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Alerts.Schemas.AlertRule
  alias CodexPooler.Jobs.{AlertEvaluationEnqueueWorker, AlertEvaluationWorker}

  test "an orphaned cron root cannot block the next five-minute window" do
    {:ok, root} = Oban.insert(AlertEvaluationEnqueueWorker.new(%{}))
    Repo.update_all(from(job in Oban.Job, where: job.id == ^root.id), set: [state: "executing", inserted_at: DateTime.add(DateTime.utc_now(), -301, :second)])
    assert {:ok, next} = Oban.insert(AlertEvaluationEnqueueWorker.new(%{}))
    refute next.conflict?
    refute next.id == root.id
    assert {:ok, duplicate} = Oban.insert(AlertEvaluationEnqueueWorker.new(%{}))
    assert duplicate.conflict?
  end

  test "a stale executing continuation remains unique without blocking a fresh root" do
    now = DateTime.utc_now() |> DateTime.to_iso8601()
    args = %{evaluation_window_started_at: now, fanout_started_at: now, cursor_created_at: now, cursor_id: Ecto.UUID.generate()}
    assert {:ok, page} = Oban.insert(AlertEvaluationEnqueueWorker.new(args))
    Repo.update_all(from(job in Oban.Job, where: job.id == ^page.id), set: [state: "executing", inserted_at: DateTime.add(DateTime.utc_now(), -301, :second)])
    assert {:ok, duplicate} = Oban.insert(AlertEvaluationEnqueueWorker.new(args))
    assert duplicate.conflict?
    assert duplicate.id == page.id
    assert {:ok, root} = Oban.insert(AlertEvaluationEnqueueWorker.new(%{}))
    refute root.conflict?
  end

  test "manual fanout persists a continuation for rules beyond its first page" do
    rule = alert_rule_fixture(pool_fixture())
    attrs = rule |> Map.from_struct() |> Map.drop([:__meta__, :id])
    Repo.insert_all(AlertRule, for(index <- 1..500, do: Map.merge(attrs, %{id: Ecto.UUID.generate(), display_name: "Manual rule #{index}"})))
    assert {:ok, %{errors: []}} = CodexPooler.Jobs.enqueue_worker_group_now(:alert_evaluation)
    drain_pages(MapSet.new())
    jobs = all_enqueued(worker: AlertEvaluationWorker)
    assert length(jobs) == 501
    assert Enum.all?(jobs, &(&1.args["trigger_kind"] == "manual"))
  end

  @tag slow: "inserts and pages 1,102 real Oban evaluations twice to prove replay deduplication"
  test "durable continuation pages reach every rule beyond 500 exactly once per window" do
    pool = pool_fixture()
    rule = alert_rule_fixture(pool)
    attrs = rule |> Map.from_struct() |> Map.drop([:__meta__, :id])

    rows =
      for index <- 1..1_101 do
        Map.merge(attrs, %{id: Ecto.UUID.generate(), display_name: "Rule #{index}"})
      end

    Repo.insert_all(AlertRule, rows)
    all_ids = MapSet.new([rule.id | Enum.map(rows, & &1.id)])
    scheduled_at = DateTime.utc_now()
    assert :ok = perform_job(AlertEvaluationEnqueueWorker, %{}, scheduled_at: scheduled_at)
    drain_pages(MapSet.new())
    jobs = all_enqueued(worker: AlertEvaluationWorker)
    assert MapSet.new(jobs, & &1.args["alert_rule_id"]) == all_ids
    assert length(jobs) == MapSet.size(all_ids)
    Repo.update_all(from(job in Oban.Job, where: job.worker == "CodexPooler.Jobs.AlertEvaluationWorker"), set: [state: "completed", completed_at: DateTime.utc_now()])
    assert :ok = perform_job(AlertEvaluationEnqueueWorker, %{}, scheduled_at: scheduled_at)
    drain_pages(MapSet.new())
    assert Repo.aggregate(from(job in Oban.Job, where: job.worker == "CodexPooler.Jobs.AlertEvaluationWorker"), :count) == MapSet.size(all_ids)
  end

  test "concurrent fanout roots on separate connections do not duplicate a window" do
    alias CodexPooler.UnboxedFixture
    alias Ecto.Adapters.SQL.Sandbox
    %{user: owner} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    rule = UnboxedFixture.run_unboxed(fn -> alert_rule_fixture(pool_fixture(%{created_by_user_id: owner.id})) end)
    scheduled_at = DateTime.add(DateTime.utc_now(), 1, :second)
    window_text = scheduled_at |> DateTime.to_unix() |> div(300) |> Kernel.*(300) |> DateTime.from_unix!() |> DateTime.to_iso8601()
    UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from j in Oban.Job, where: fragment("?->>'evaluation_window_started_at'", j.args) == ^window_text) end)
    parent = self()

    tasks =
      for _ <- 1..2 do
        task =
          Task.async(fn ->
            Sandbox.unboxed_run(Repo, fn ->
              Repo.checkout(fn ->
                %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
                send(parent, {:ready, self(), backend})

                receive do
                  :go -> perform_job(AlertEvaluationEnqueueWorker, %{}, scheduled_at: scheduled_at)
                end
              end)
            end)
          end)

        monitor = Process.monitor(task.pid)
        on_exit(fn -> if Process.alive?(task.pid), do: Process.exit(task.pid, :kill) end)
        {task, monitor}
      end

    assert_receive {:ready, first, backend1}
    assert_receive {:ready, second, backend2}
    refute backend1 == backend2
    send(first, :go)
    send(second, :go)

    for {task, monitor} <- tasks do
      assert :ok = Task.await(task, 15_000)
      assert_receive {:DOWN, ^monitor, :process, _, :normal}
    end

    assert UnboxedFixture.run_unboxed(fn -> Repo.aggregate(from(j in Oban.Job, where: fragment("?->>'alert_rule_id'", j.args) == ^rule.id), :count) end) == 1
  end

  @tag slow: "executes 501 database-backed enqueues and verifies an injected transaction rollback"
  test "failure to persist the next page rolls back this page's evaluation jobs" do
    alias CodexPooler.UnboxedFixture
    %{user: owner} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    now = DateTime.add(DateTime.utc_now(), 1, :second)
    window_text = now |> DateTime.to_unix() |> div(300) |> Kernel.*(300) |> DateTime.from_unix!() |> DateTime.to_iso8601()
    UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from j in Oban.Job, where: fragment("?->>'evaluation_window_started_at'", j.args) in ^[window_text, DateTime.to_iso8601(now)]) end)

    UnboxedFixture.run_unboxed(fn ->
      rule = alert_rule_fixture(pool_fixture(%{created_by_user_id: owner.id}))
      attrs = rule |> Map.from_struct() |> Map.drop([:__meta__, :id])
      Repo.insert_all(AlertRule, for(index <- 1..500, do: Map.merge(attrs, %{id: Ecto.UUID.generate(), display_name: "Atomic rule #{index}"})))
    end)

    UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.query!("DROP TRIGGER IF EXISTS reject_fanout_continuation ON oban_jobs") end)

    {failure, log} =
      ExUnit.CaptureLog.with_log(fn ->
        UnboxedFixture.run_unboxed(fn ->
          Repo.checkout(fn ->
            Repo.query!("CREATE FUNCTION pg_temp.reject_fanout_continuation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.worker = 'CodexPooler.Jobs.AlertEvaluationEnqueueWorker' AND NEW.args ? 'cursor_id' THEN RAISE EXCEPTION 'sample continuation failure' USING ERRCODE = 'check_violation'; END IF; RETURN NEW; END $$")
            Repo.query!("CREATE TRIGGER reject_fanout_continuation BEFORE INSERT ON oban_jobs FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_fanout_continuation()")

            try do
              CodexPooler.Jobs.enqueue_alert_evaluation_page(now, now, nil)
            rescue
              _ in [Postgrex.Error, DBConnection.ConnectionError] -> :failed
            end
          end)
        end)
      end)

    assert failure == :failed
    assert log == "" or log =~ "transaction rolling back"
    UnboxedFixture.run_unboxed(fn -> Repo.query!("DROP TRIGGER IF EXISTS reject_fanout_continuation ON oban_jobs") end)
    assert UnboxedFixture.run_unboxed(fn -> Repo.aggregate(Oban.Job, :count) end) == 0
  end

  defp drain_pages(seen) do
    case Enum.find(all_enqueued(worker: AlertEvaluationEnqueueWorker), &(not MapSet.member?(seen, &1.id))) do
      nil ->
        :ok

      job ->
        assert :ok = perform_job(AlertEvaluationEnqueueWorker, job.args, scheduled_at: job.scheduled_at)
        drain_pages(MapSet.put(seen, job.id))
    end
  end
end
