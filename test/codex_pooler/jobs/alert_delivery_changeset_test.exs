defmodule CodexPooler.Jobs.AlertDeliveryChangesetTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1]

  alias CodexPooler.Alerts.Delivery.AttemptLifecycle
  alias CodexPooler.Alerts.Schemas.{AlertChannel, AlertDeliveryAttempt}
  alias CodexPooler.Jobs.AlertDeliveryWorker
  alias CodexPooler.Mailer
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv
  alias CodexPooler.TestDiagnostics
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @moduletag capture_log: true

  setup do
    TestAppEnv.restore_on_exit(Mailer)
    :ok
  end

  for collision? <- [true, false] do
    @collision collision?
    test "real SMTP receipt collision #{@collision} reaches a terminal Oban job without another send" do
      suffix = System.unique_integer([:positive])
      queue = "receipt_collision_#{suffix}"
      oban_name = String.to_atom("receipt_oban_#{suffix}")
      repo_name = String.to_atom("receipt_repo_#{suffix}")
      fixture = fixture!(suffix, queue)
      tasks = start_supervised!(Task.Supervisor)
      {port, smtp} = smtp_receiver!(tasks)
      smtp_monitor = Process.monitor(smtp.pid)
      Application.put_env(:codex_pooler, Mailer, adapter: Swoosh.Adapters.SMTP, use_instance_settings?: false, relay: "127.0.0.1", port: port, ssl: false, tls: :never, auth: :never, retries: 0, no_mx_lookups: true)

      repo_opts = Repo.config() |> Keyword.put(:name, repo_name) |> Keyword.put(:pool, DBConnection.ConnectionPool) |> Keyword.put(:pool_size, 3)
      repo = start_supervised!(Supervisor.child_spec({Repo, repo_opts}, id: repo_name))
      oban = start_supervised!(Supervisor.child_spec({Oban, name: oban_name, repo: Repo, get_dynamic_repo: fn -> repo end, testing: :manual, queues: [], plugins: [], notifier: Oban.Notifiers.PG, peer: false, log: false}, id: oban_name))
      telemetry_id = {__MODULE__, make_ref()}
      on_exit(fn -> :telemetry.detach(telemetry_id) end)
      :ok = :telemetry.attach(telemetry_id, [:oban, :job, :stop], &__MODULE__.observe_stop/4, %{parent: self(), job_id: fixture.job.id, name: oban_name})
      parent = self()

      execution =
        Task.Supervisor.async_nolink(tasks, fn ->
          Repo.put_dynamic_repo(repo)

          Repo.checkout(fn ->
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {:executor_backend, backend})
            Oban.drain_queue(oban_name, queue: queue, with_safety: true)
          end)
        end)

      execution_monitor = Process.monitor(execution.pid)
      assert_receive {:executor_backend, executor_backend}, @budget
      assert_receive {:smtp_data_ready, handler, release}, @budget

      # SMTP has received DATA but cannot acknowledge it until an independent
      # PostgreSQL connection commits the conflicting attempt number.
      writer_backend =
        database(fn ->
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          assert Repo.get!(Oban.Job, fixture.job.id).state == "executing"
          assert Repo.aggregate(from(a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id), :count) == 0

          if @collision do
            assert {:ok, _} = AttemptLifecycle.record_failed_attempt(fixture.incident.id, fixture.channel.id, 1, DateTime.utc_now(), "email", "synthetic_competing_receipt", "competing receipt")
          end

          backend
        end)

      refute executor_backend == writer_backend
      send(handler, {:smtp_accept, release})
      result = Task.await(execution, @budget)
      assert_receive {:DOWN, ^execution_monitor, :process, _, :normal}, @budget
      assert Task.await(smtp, @budget) == :accepted_once
      assert_receive {:DOWN, ^smtp_monitor, :process, _, :normal}, @budget
      assert_receive :smtp_accepted_once, @budget
      job = database(fn -> Repo.get!(Oban.Job, fixture.job.id) end)
      expected = if @collision, do: "cancelled", else: "completed"
      TestDiagnostics.puts("receipt-collision collision=#{@collision} separate_backends=true smtp_accepted=1 job=#{job.state} attempt=#{job.attempt} drain_failure=#{result.failure}")
      assert job.state == expected
      assert job.attempt == 1
      assert result.failure == 0

      if @collision do
        assert result.cancelled == 1
        assert %DateTime{} = job.cancelled_at
        assert_receive {:receipt_job_result, {:cancel, %{code: "alert_delivery_receipt_invalid"}}}, @budget
        assert length(job.errors) == 1
        safe_error? = String.contains?(hd(job.errors)["error"], "alert_delivery_receipt_invalid") and not String.contains?(hd(job.errors)["error"], "Ecto.Changeset")
        assert safe_error?
      else
        assert result.success == 1
        assert_receive {:receipt_job_result, :ok}, @budget
      end

      second = with_repo(repo, fn -> Oban.drain_queue(oban_name, queue: queue, with_scheduled: true) end)
      assert Enum.all?(second, fn {_outcome, count} -> count == 0 end)
      assert database(fn -> Repo.get!(Oban.Job, fixture.job.id).attempt end) == 1
      assert :ok = stop_supervised(oban_name)
      refute Process.alive?(oban)
      assert :ok = stop_supervised(repo_name)
      refute Process.alive?(repo)
    end
  end

  test "the actual receipt unique constraint returns a changeset with attempt-number error" do
    fixture = fixture!(System.unique_integer([:positive]), "receipt_seam")

    database(fn ->
      assert {:ok, _} = AttemptLifecycle.record_failed_attempt(fixture.incident.id, fixture.channel.id, 1, DateTime.utc_now(), "email", "synthetic_competing_receipt", "competing receipt")
      assert {:error, %Ecto.Changeset{} = changeset} = AttemptLifecycle.insert_sent_attempt(fixture.incident, fixture.channel, 1, DateTime.utc_now(), %{})
      assert Keyword.has_key?(changeset.errors, :attempt_number)
    end)
  end

  test "invalid job arguments retain their atom cancellation" do
    assert {:cancel, :invalid_alert_delivery_args} = AlertDeliveryWorker.perform(%Oban.Job{args: %{}})
  end

  def observe_stop(_event, _measurements, %{job: %{id: id}, conf: %{name: name}, result: result}, %{job_id: id, name: name, parent: parent}), do: send(parent, {:receipt_job_result, result})
  def observe_stop(_event, _measurements, _metadata, _config), do: :ok

  defp fixture!(suffix, queue) do
    slug = "receipt-collision-#{suffix}"
    label = "receipt-channel-#{suffix}"

    register_unboxed_cleanup!(fn ->
      if pool = Repo.get_by(Pool, slug: slug), do: delete_committed_pools!([pool.id])
      Repo.delete_all(from j in Oban.Job, where: j.queue == ^queue)
      Repo.delete_all(from c in AlertChannel, where: c.display_name == ^label)
      refute Repo.exists?(from p in Pool, where: p.slug == ^slug)
      refute Repo.exists?(from c in AlertChannel, where: c.display_name == ^label)
      refute Repo.exists?(from j in Oban.Job, where: j.queue == ^queue)
      TestDiagnostics.puts("receipt-collision cleanup owned_rows_absent=true")
    end)

    database(fn ->
      pool = pool_fixture(%{slug: slug})
      channel = alert_channel_fixture(display_name: label)
      incident = alert_incident_fixture(pool: pool)
      rule = alert_rule_fixture(pool)
      alert_incident_target_fixture(incident, rule, pool)
      {:ok, job} = %{"alert_incident_id" => incident.id, "alert_channel_id" => channel.id} |> AlertDeliveryWorker.new(queue: queue) |> Oban.insert()
      %{pool: pool, channel: channel, incident: incident, job: job}
    end)
  end

  defp smtp_receiver!(tasks) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :line, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    parent = self()

    task =
      Task.Supervisor.async_nolink(tasks, fn ->
        {:ok, socket} = :gen_tcp.accept(listener, @budget)
        :gen_tcp.close(listener)

        try do
          :ok = :gen_tcp.send(socket, "220 example.com ESMTP\r\n")
          smtp_commands(socket, parent)
        after
          :gen_tcp.close(socket)
        end
      end)

    {port, task}
  end

  defp smtp_commands(socket, parent) do
    {:ok, line} = :gen_tcp.recv(socket, 0, @budget)

    cond do
      String.starts_with?(line, "EHLO") or String.starts_with?(line, "HELO") ->
        :ok = :gen_tcp.send(socket, "250 example.com\r\n")
        smtp_commands(socket, parent)

      String.starts_with?(line, "MAIL") or String.starts_with?(line, "RCPT") ->
        :ok = :gen_tcp.send(socket, "250 OK\r\n")
        smtp_commands(socket, parent)

      line == "DATA\r\n" ->
        :ok = :gen_tcp.send(socket, "354 continue\r\n")
        smtp_data(socket)
        release = make_ref()
        send(parent, {:smtp_data_ready, self(), release})

        receive do
          {:smtp_accept, ^release} -> :ok
        after
          @budget -> raise "SMTP acceptance barrier was not released"
        end

        :ok = :gen_tcp.send(socket, "250 accepted\r\n")
        send(parent, :smtp_accepted_once)
        {:ok, "QUIT\r\n"} = :gen_tcp.recv(socket, 0, @budget)
        :ok = :gen_tcp.send(socket, "221 goodbye\r\n")
        :accepted_once
    end
  end

  defp smtp_data(socket) do
    case :gen_tcp.recv(socket, 0, @budget) do
      {:ok, ".\r\n"} -> :ok
      {:ok, _line} -> smtp_data(socket)
    end
  end

  defp database(fun), do: Sandbox.unboxed_run(Repo, fn -> Repo.checkout(fun) end)

  defp with_repo(repo, fun) do
    previous = Repo.put_dynamic_repo(repo)

    try do
      fun.()
    after
      Repo.put_dynamic_repo(previous)
    end
  end
end
