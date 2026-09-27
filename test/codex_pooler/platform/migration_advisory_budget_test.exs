defmodule CodexPooler.Platform.MigrationAdvisoryBudgetTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Release
  alias Ecto.Adapters.Postgres

  defmodule ContenderRepo do
    use Ecto.Repo, otp_app: :codex_pooler, adapter: Ecto.Adapters.Postgres

    @impl true
    def init(_type, config) do
      connection = CodexPooler.Repo.config() |> Keyword.drop([:name, :pool, :pool_size])
      {:ok, connection |> Keyword.merge(config) |> Keyword.put(:pool_size, 2) |> Release.repo_config_for_task(:migrate)}
    end
  end

  setup do
    previous = System.get_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS")
    on_exit(fn -> if previous, do: System.put_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS", previous), else: System.delete_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS") end)
    System.put_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS", "1")
    start_supervised!(ContenderRepo)
    holder = start_supervised!({Postgrex, Repo.config() |> Keyword.drop([:name, :pool, :pool_size, :pool_count])})
    lock = :erlang.phash2({:ecto, nil, ContenderRepo})
    Postgrex.query!(holder, "SELECT pg_advisory_lock($1)", [lock])
    %{holder: holder, lock: lock}
  end

  @tag slow: "waits for the configured one-second advisory retry interval"
  test "a real contender retries and proceeds once the migration owner releases", ctx do
    parent = self()
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)
    :telemetry.attach(handler, [:codex_pooler, :platform, :migration_advisory_budget_test, :contender_repo, :query], &__MODULE__.observe_try/4, parent)
    task = Task.async(fn -> Postgres.lock_for_migrations(Ecto.Adapter.lookup_meta(ContenderRepo), [], fn -> :acquired end) end)
    assert_receive :advisory_contended, 5_000
    Postgrex.query!(ctx.holder, "SELECT pg_advisory_unlock($1)", [ctx.lock])
    assert :acquired = Task.await(task, 5_000)
  end

  @tag slow: "exhausts the configured one-second advisory wait budget"
  test "configured contention expiry is finite and does not run the migration body" do
    error =
      assert_raise RuntimeError, fn ->
        Postgres.lock_for_migrations(Ecto.Adapter.lookup_meta(ContenderRepo), [], fn -> flunk("migration ran while another owner held its lock") end)
      end

    assert error.message =~ "Tried 1 times waiting 1000ms"
  end

  def observe_try(_event, _measurements, metadata, parent) do
    if String.contains?(metadata.query, "pg_try_advisory_lock") and match?({:ok, %{rows: [[false]]}}, metadata.result), do: send(parent, :advisory_contended)
  end
end
