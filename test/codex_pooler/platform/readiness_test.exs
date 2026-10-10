defmodule CodexPooler.Platform.ReadinessTest do
  use CodexPooler.DataCase, async: false

  @moduletag :tmp_dir

  alias CodexPooler.Platform.Readiness
  alias CodexPooler.Release
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(Readiness)

    :ok = Readiness.reset_state!()

    on_exit(fn ->
      :ok = Readiness.reset_state!()
    end)

    :ok
  end

  defmodule UnreachableProbe do
    def query(_repo, _statement, _params, _opts) do
      {:error, %DBConnection.ConnectionError{message: "tcp connect: connection refused"}}
    end
  end

  defmodule MissingSchemaProbe do
    def query(_repo, _statement, _params, _opts) do
      {:error, %Postgrex.Error{postgres: %{code: :undefined_table, message: "does not exist"}}}
    end
  end

  defmodule UndefinedFunctionProbe do
    def query(repo, _statement, _params, opts) do
      SQL.query(repo, "SELECT readiness_missing_function()", [], opts)
    end
  end

  defmodule RaisingEncodeProbe do
    def query(_repo, _statement, _params, _opts) do
      raise DBConnection.EncodeError, "raw value example-secret"
    end
  end

  defmodule RaisingConnectionProbe do
    def query(_repo, _statement, _params, _opts) do
      raise DBConnection.ConnectionError, "raw connection example-secret"
    end
  end

  defmodule PostgreSQLServerErrorProbe do
    def query(_repo, _statement, _params, _opts) do
      code = Process.get({__MODULE__, :code})
      {:error, %Postgrex.Error{postgres: %{code: code, message: "raw server example-secret"}}}
    end
  end

  defmodule ConcurrentProbe do
    def query(_repo, _statement, [versions], _opts) do
      {coordinator, token, outcome} = Process.get({__MODULE__, :control})
      send(coordinator, {:readiness_probe_entered, token, self()})

      receive do
        {:release_readiness_probe, ^token} -> result(outcome, length(versions))
      after
        5_000 -> raise "readiness concurrency barrier was not released"
      end
    end

    defp result(:success, count), do: {:ok, %{rows: [[count]]}}

    defp result(:connectivity, _count) do
      {:error, %DBConnection.ConnectionError{message: "synthetic connection loss"}}
    end
  end

  test "a migrated database is ready over the real query" do
    assert Readiness.check() == :ready
  end

  test "strict schema status never accepts cached connectivity grace" do
    assert Readiness.check() == :ready
    assert Readiness.check(sql_probe: UnreachableProbe) == {:ready, :degraded, "DBConnection.ConnectionError"}
    assert Readiness.schema_status(sql_probe: UnreachableProbe) == {:error, :connectivity, "DBConnection.ConnectionError"}
  end

  test "a missing middle migration fails containment even with a newer applied version" do
    %{rows: [[version]]} = Repo.query!("SELECT min(version) FROM schema_migrations")
    Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [version])
    assert Readiness.schema_status() == {:error, :schema, "migrations_missing"}
  end

  test "duplicate packaged migration versions fail strict schema status", %{tmp_dir: tmp_dir} do
    File.write!(Path.join(tmp_dir, "123_one.exs"), "")
    File.write!(Path.join(tmp_dir, "123_two.exs"), "")
    assert Readiness.schema_status(migrations_path: tmp_dir) == {:error, :schema, "migration_files_malformed"}
  end

  test "an applied schema newer than this image stays ready" do
    # A rollout migrates before it replaces pods, so the pods still serving the
    # previous release see versions they do not carry. Containment, not
    # equality, is what keeps them in the Service.
    Repo.query!("INSERT INTO schema_migrations (version, inserted_at) VALUES ($1, now())", [
      99_999_999_999_999
    ])

    assert Readiness.check() == :ready
  end

  test "an unapplied schema is not ready over the real query" do
    Repo.query!("DELETE FROM schema_migrations")

    assert Readiness.check() == {:not_ready, "migrations_missing"}
  end

  test "a database missing only the latest shipped migration is not ready over the real query" do
    %{rows: [[latest]]} = Repo.query!("SELECT max(version) FROM schema_migrations")
    Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [latest])

    assert Readiness.check() == {:not_ready, "migrations_missing"}
  end

  test "an empty packaged migration directory fails closed without consulting schema rows", %{
    tmp_dir: tmp_dir
  } do
    File.write!(Path.join(tmp_dir, ".formatter.exs"), "[]")

    assert Readiness.check(migrations_path: tmp_dir) ==
             {:not_ready, "migration_files_missing"}
  end

  test "an unreadable packaged migration directory fails closed", %{tmp_dir: tmp_dir} do
    missing = Path.join(tmp_dir, "missing")

    assert Readiness.check(migrations_path: missing) ==
             {:not_ready, "migration_directory_unreadable"}
  end

  test "a malformed packaged migration directory fails closed", %{tmp_dir: tmp_dir} do
    File.write!(Path.join(tmp_dir, "not_a_migration.exs"), "raise \"not loaded\"")

    assert Readiness.check(migrations_path: tmp_dir) ==
             {:not_ready, "migration_files_malformed"}
  end

  test "unverifiable packaged migrations never fall back to a nonempty partial schema", %{
    tmp_dir: tmp_dir
  } do
    %{rows: [[latest]]} = Repo.query!("SELECT max(version) FROM schema_migrations")
    Repo.query!("DELETE FROM schema_migrations WHERE version = $1", [latest])

    assert Readiness.check(migrations_path: Path.join(tmp_dir, "missing")) ==
             {:not_ready, "migration_directory_unreadable"}
  end

  test "a connectivity failure inside the grace window keeps a previously ready node ready" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now + Readiness.grace_ms(), sql_probe: UnreachableProbe) ==
             {:ready, :degraded, "DBConnection.ConnectionError"}
  end

  test "a raised connection exception receives the same grace as a tagged connection result" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now, sql_probe: RaisingConnectionProbe) ==
             {:ready, :degraded, "DBConnection.ConnectionError"}
  end

  test "a connectivity failure past the grace window withdraws readiness" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now + Readiness.grace_ms() + 1, sql_probe: UnreachableProbe) ==
             {:not_ready, "DBConnection.ConnectionError"}
  end

  test "a connectivity failure before any success withdraws readiness at once" do
    assert Readiness.check(sql_probe: UnreachableProbe) ==
             {:not_ready, "DBConnection.ConnectionError"}
  end

  test "a missing schema is never graced, however recently the node was ready" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now, sql_probe: MissingSchemaProbe) ==
             {:not_ready, "undefined_table"}
  end

  test "a permanent PostgreSQL server error is never graced after prior readiness" do
    now = System.monotonic_time(:millisecond)

    assert Readiness.check(now_ms: now) == :ready

    assert Readiness.check(now_ms: now, sql_probe: UndefinedFunctionProbe) ==
             {:not_ready, "undefined_function"}
  end

  test "other permanent PostgreSQL server responses fail immediately with bounded SQLSTATE classes" do
    now = System.monotonic_time(:millisecond)
    assert Readiness.check(now_ms: now) == :ready

    for code <- [:datatype_mismatch, :insufficient_privilege] do
      Process.put({PostgreSQLServerErrorProbe, :code}, code)

      assert Readiness.check(now_ms: now, sql_probe: PostgreSQLServerErrorProbe) ==
               {:not_ready, Atom.to_string(code)}
    end
  end

  test "a positively classified transient PostgreSQL shutdown receives grace" do
    now = System.monotonic_time(:millisecond)
    assert Readiness.check(now_ms: now) == :ready

    Process.put({PostgreSQLServerErrorProbe, :code}, :admin_shutdown)

    assert Readiness.check(now_ms: now, sql_probe: PostgreSQLServerErrorProbe) ==
             {:ready, :degraded, "admin_shutdown"}
  end

  test "a query encoding exception becomes an immediate bounded failure over the real Repo boundary" do
    assert Readiness.check() == :ready

    Repo.query!("ALTER TABLE schema_migrations ALTER COLUMN version TYPE text USING version::text")

    assert Readiness.check() == {:not_ready, "DBConnection.EncodeError"}
  end

  @tag timeout: 120_000
  test "simultaneous first success and failure never lose the prior-ready fact", %{
    tmp_dir: tmp_dir
  } do
    assert Process.whereis(Readiness)
    File.write!(Path.join(tmp_dir, "20260918000000_readiness_control.exs"), "")

    Enum.each(1..2_000, fn _round ->
      :ok = Readiness.reset_state!()
      token = make_ref()

      success = concurrent_check(token, :success, tmp_dir)
      failure = concurrent_check(token, :connectivity, tmp_dir)

      entered = [await_probe(token), await_probe(token)]
      Enum.each(entered, &send(&1, {:release_readiness_probe, token}))

      assert :ready in [Task.await(success), Task.await(failure)]

      assert Readiness.check(
               now_ms: 0,
               sql_probe: UnreachableProbe,
               migrations_path: tmp_dir
             ) == {:ready, :degraded, "DBConnection.ConnectionError"}
    end)
  end

  describe "release readiness check for roles without an HTTP listener" do
    test "returns :ok against a migrated database" do
      assert Release.readiness_check() == :ok
    end

    test "tolerates a connectivity blip the same way the HTTP probe does" do
      assert Readiness.check() == :ready

      Application.put_env(:codex_pooler, Readiness, sql_probe: UnreachableProbe)

      assert Release.readiness_check() == :ok
    end

    test "raises a sanitized class when the schema is not applied" do
      Repo.query!("DELETE FROM schema_migrations")

      assert_raise RuntimeError, "readiness check failed reason_class=migrations_missing", fn ->
        Release.readiness_check()
      end
    end

    test "never carries a database message into the raised reason" do
      Application.put_env(:codex_pooler, Readiness, sql_probe: MissingSchemaProbe)

      error =
        assert_raise RuntimeError, fn -> Release.readiness_check() end

      assert error.message == "readiness check failed reason_class=undefined_table"
      refute error.message =~ "does not exist"
    end

    test "raises only a bounded class for a query encoding exception" do
      Application.put_env(:codex_pooler, Readiness, sql_probe: RaisingEncodeProbe)

      error = assert_raise RuntimeError, fn -> Release.readiness_check() end

      assert error.message == "readiness check failed reason_class=DBConnection.EncodeError"
      refute error.message =~ "example-secret"
    end
  end

  defp concurrent_check(token, outcome, migrations_path) do
    parent = self()

    Task.async(fn ->
      Process.put({ConcurrentProbe, :control}, {parent, token, outcome})

      Readiness.check(
        now_ms: 0,
        sql_probe: ConcurrentProbe,
        migrations_path: migrations_path
      )
    end)
  end

  defp await_probe(token) do
    receive do
      {:readiness_probe_entered, ^token, pid} -> pid
    after
      5_000 -> raise "readiness probe did not reach the concurrency barrier"
    end
  end
end
