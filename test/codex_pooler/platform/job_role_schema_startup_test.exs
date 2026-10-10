defmodule CodexPooler.Platform.JobRoleSchemaStartupTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.JobSchemaCanaryWorker
  alias CodexPooler.Platform.JobRuntime
  alias CodexPooler.Platform.Readiness
  alias CodexPooler.TestAppEnv

  setup do
    TestAppEnv.restore_on_exit(Readiness)
    :ok
  end

  test "application Oban start refuses a behind schema before a single-attempt canary is consumed" do
    Repo.query!("ALTER TABLE routing_circuit_states DROP COLUMN probe_generation")
    Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [20_261_009_123_611])
    job = Repo.insert!(JobSchemaCanaryWorker.new(%{}))
    name = :"schema-worker-#{System.unique_integer([:positive])}"
    handler = "schema-worker-#{name}"
    on_exit(fn -> :telemetry.detach(handler) end)
    :telemetry.attach_many(handler, [[:oban, :job, :exception], [:codex_pooler, :schema_canary, :start]], &__MODULE__.job_event/4, {self(), job.id})
    result = start_supervised(application_child(name, queues: [schema_canary: 1], plugins: false))

    if match?({:ok, _}, result) do
      assert_receive {:canary_entered, id}, 10_000
      assert id == job.id
      assert_receive {:canary_failed, :discard}, 10_000
      assert Repo.get!(Oban.Job, job.id).state == "discarded"
      flunk("Oban started on a behind schema and discarded the real single-attempt canary")
    end

    assert_schema_refusal(result)
    assert is_nil(Oban.whereis(name))
    assert Repo.get!(Oban.Job, job.id).state == "available"
    assert Repo.get!(Oban.Job, job.id).attempt == 0
    refute_received {:canary_failed, _}
    refute_received {:canary_entered, _}
  end

  for role <- [:scheduler, :all] do
    test "#{role} startup refuses missing schema before plugin initialization" do
      Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [20_261_009_123_611])
      name = :"schema-services-#{System.unique_integer([:positive])}"
      queues = if unquote(role) == :all, do: [schema_canary: 1], else: false
      result = start_supervised(application_child(name, queues: queues, plugins: [{CodexPooler.JobSchemaStartupPlugin, observer: self()}]))
      assert_schema_refusal(result)
      assert is_nil(Oban.whereis(name))
      refute_received {:schema_plugin_started, _}
    end
  end

  test "current schema starts real queues and plugins and the canary completes" do
    name = :"schema-current-#{System.unique_integer([:positive])}"
    handler = "schema-current-#{name}"
    job = Repo.insert!(JobSchemaCanaryWorker.new(%{}))
    on_exit(fn -> :telemetry.detach(handler) end)
    :telemetry.attach_many(handler, [[:oban, :job, :stop], [:codex_pooler, :schema_canary, :start]], &__MODULE__.job_event/4, {self(), job.id})
    assert {:ok, pid} = start_supervised(application_child(name, queues: [schema_canary: 1], plugins: [{CodexPooler.JobSchemaStartupPlugin, observer: self()}]))
    assert %{queue: "schema_canary"} = Oban.check_queue(name, queue: :schema_canary)
    :ok = Oban.Notifier.notify(name, :insert, %{queue: "schema_canary"})
    assert Process.alive?(pid)
    assert_receive {:schema_plugin_started, plugin}, 5_000
    assert Process.alive?(plugin)
    assert_receive {:canary_entered, id}, 10_000
    assert id == job.id
    assert_receive {:canary_failed, :success}, 10_000
    assert Repo.get!(Oban.Job, job.id).state == "completed"
  end

  test "the wrapper retains the installed Oban child supervision contract" do
    opts = [repo: Repo, name: :schema_spec]
    original = Oban.child_spec(opts)
    assert JobRuntime.child_spec(opts) == %{original | start: {JobRuntime, :start_link, [opts]}}
  end

  test "an individual Oban child restart rechecks the database" do
    name = :"schema-restart-#{System.unique_integer([:positive])}"
    spec = application_child(name, queues: false, plugins: false)
    assert {:ok, _pid} = start_supervised(spec)
    {:ok, supervisor} = ExUnit.fetch_test_supervisor()
    :ok = Supervisor.terminate_child(supervisor, name)
    Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [20_261_009_123_611])
    assert {:error, {:job_schema_not_ready, :schema, "migrations_missing"}} = Supervisor.restart_child(supervisor, name)
    assert is_nil(Oban.whereis(name))
  end

  for mode <- [:manual, :inline] do
    test "#{mode} normalized configuration starts without job work on an incomplete application schema" do
      Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [20_261_009_123_611])
      name = :"schema-testing-#{System.unique_integer([:positive])}"
      opts = [repo: Repo, name: name, testing: unquote(mode), queues: [schema_canary: 1], notifier: Oban.Notifiers.PG]
      assert {:ok, _pid} = start_supervised(JobRuntime.child_spec(opts))
      assert %{queues: [], plugins: [], stager: false} = Oban.config(name)
    end
  end

  test "a web configuration starts without job work on an incomplete application schema" do
    Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [20_261_009_123_611])
    name = :"schema-web-#{System.unique_integer([:positive])}"
    assert {:ok, _pid} = start_supervised(application_child(name, queues: false, plugins: false, stager: false))
    assert %{queues: [], plugins: [], stager: false} = Oban.config(name)
  end

  def job_event(_event, _measurements, %{job: job, state: state}, {observer, id}) do
    if job.id == id, do: send(observer, {:canary_failed, state})
  end

  def job_event([:codex_pooler, :schema_canary, :start], _measurements, %{job_id: id}, {observer, id}) do
    send(observer, {:canary_entered, id})
  end

  defp application_child(name, options) do
    {:ok, spec} = :supervisor.get_childspec(CodexPooler.Supervisor, Oban)
    {module, function, [_opts]} = spec.start
    opts = Keyword.merge([repo: Repo, name: name, testing: :disabled, peer: false, notifier: Oban.Notifiers.PG, shutdown_grace_period: 0], options)
    %{spec | id: name, start: {module, function, [opts]}}
  end

  defp assert_schema_refusal(result) do
    assert {:error, {{:job_schema_not_ready, :schema, "migrations_missing"}, _child}} = result
  end
end
