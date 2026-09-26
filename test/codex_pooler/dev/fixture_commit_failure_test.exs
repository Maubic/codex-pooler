defmodule CodexPooler.Dev.FixtureCommitFailureTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.UnboxedFixture

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Dev.CodexCompactionSmokeFixture, as: Fixture
  alias CodexPooler.Dev.CodexCompactionSmokeFixture.Journal
  alias CodexPooler.Dev.Seeds.RealTraffic
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  defmodule CommitLogRelay do
    @moduledoc false

    def log(%{msg: {:string, message}}, %{config: %{test_pid: test_pid}}) do
      if IO.chardata_to_string(message) =~ "fixture commit rejected", do: send(test_pid, {:commit_failure_logged, self()})
      :ok
    end

    def log(_event, _config), do: :ok
  end

  setup do
    on_exit(fn -> :logger.remove_handler(CommitLogRelay) end)
    :ok = :logger.add_handler(CommitLogRelay, CommitLogRelay, %{level: :error, config: %{test_pid: self()}})
    run_id = "commit-failure-#{System.unique_integer([:positive])}"
    root = Path.join(System.tmp_dir!(), run_id)
    on_exit(fn -> File.rm_rf!(root) end)
    File.mkdir!(root)
    %{user: owner} = committed_bootstrap_owner_fixture!()
    %{run_id: run_id, root: root, owner: owner}
  end

  test "confirmed commit rejection restores the previous real traffic key file", context do
    first = File.cd!(context.root, fn -> run_unboxed(fn -> RealTraffic.run(%{owner: context.owner}) end) end)
    path = Path.join(context.root, RealTraffic.env_path())
    original_digest = :crypto.hash(:sha256, File.read!(path))
    trigger = install_commit_failure!("api_keys", "INSERT", "NEW", first.pool.id)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        File.cd!(context.root, fn ->
          run_unboxed(fn ->
            assert_raise Postgrex.Error, ~r/fixture commit rejected/, fn -> RealTraffic.run(%{owner: context.owner}) end
          end)
        end)

        assert_receive {:commit_failure_logged, connection}, 5_000
        :sys.get_state(connection, 5_000)
      end)

    assert logs =~ "fixture commit rejected"

    assert File.read!(path) |> then(&:crypto.hash(:sha256, &1)) == original_digest
    assert File.ls!(Path.dirname(path)) == ["real-traffic.env"]

    # The same confirmed COMMIT failure must restore absence as well as bytes.
    File.rm!(path)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        File.cd!(context.root, fn ->
          run_unboxed(fn ->
            assert_raise Postgrex.Error, ~r/fixture commit rejected/, fn -> RealTraffic.run(%{owner: context.owner}) end
          end)
        end)

        assert_receive {:commit_failure_logged, connection}, 5_000
        :sys.get_state(connection, 5_000)
      end)

    assert logs =~ "fixture commit rejected"
    refute File.exists?(path)
    assert File.ls!(Path.dirname(path)) == []

    run_unboxed(fn ->
      assert Repo.get!(APIKey, first.api_key.id).status == "active"
      assert Repo.aggregate(from(key in APIKey, where: key.pool_id == ^first.pool.id), :count) == 1
      assert %{rows: [[true]]} = Repo.query!("SELECT is_called FROM #{trigger}_reached")
    end)
  end

  @tag :unix_integration
  test "confirmed auto commit rejection preserves exact journal ownership and release", context do
    register_unboxed_cleanup!(fn ->
      Repo.delete_all(from identity in UpstreamIdentity, where: identity.chatgpt_account_id == ^"codex-compaction-smoke-#{context.run_id}")
    end)

    options = [run_id: context.run_id, root: context.root, environment: :test, allow_test_database: true, upstream_base_url: "http://127.0.0.1:4567"]
    {:ok, acquired} = run_unboxed(fn -> Fixture.acquire(options) end)
    {:ok, %{serving_override_id: override_id}} = run_unboxed(fn -> Fixture.serving_override(Keyword.put(options, :mode, "full")) end)
    paths = Journal.paths(context.root, context.run_id)
    previous = File.read!(paths.journal)
    trigger = install_commit_failure!("pool_model_serving_overrides", "DELETE", "OLD", acquired.pool_id)

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:error, "fixture serving override failed"} = run_unboxed(fn -> Fixture.serving_override(Keyword.put(options, :mode, "auto")) end)
        assert_receive {:commit_failure_logged, connection}, 5_000
        :sys.get_state(connection, 5_000)
      end)

    assert logs =~ "fixture commit rejected"
    assert File.read!(paths.journal) == previous
    assert {:ok, %{"serving_override_id" => ^override_id}} = Journal.read_journal(paths, context.run_id)

    run_unboxed(fn ->
      assert %ModelServingOverride{mode: "full"} = Repo.get!(ModelServingOverride, override_id)
      assert %{rows: [[true]]} = Repo.query!("SELECT is_called FROM #{trigger}_reached")
      drop_commit_failure!("pool_model_serving_overrides", trigger)
      assert {:ok, %{status: "released"}} = Fixture.release(options)
      refute Repo.get(ModelServingOverride, override_id)
    end)
  end

  defp install_commit_failure!(table, operation, record, pool_id) do
    trigger = "fixture_commit_#{System.unique_integer([:positive])}"
    register_unboxed_cleanup!(fn -> drop_commit_failure!(table, trigger) end)

    run_unboxed(fn ->
      Repo.query!("CREATE SEQUENCE #{trigger}_reached")

      Repo.query!("""
      CREATE FUNCTION #{trigger}() RETURNS trigger AS $$
      BEGIN
        PERFORM nextval('#{trigger}_reached');
        RAISE EXCEPTION 'fixture commit rejected' USING ERRCODE = '23514';
      END;
      $$ LANGUAGE plpgsql
      """)

      Repo.query!("""
      CREATE CONSTRAINT TRIGGER #{trigger} AFTER #{operation} ON #{table}
      DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
      WHEN (#{record}.pool_id = '#{pool_id}'::uuid)
      EXECUTE FUNCTION #{trigger}()
      """)
    end)

    trigger
  end

  defp drop_commit_failure!(table, trigger) do
    Repo.query!("DROP TRIGGER IF EXISTS #{trigger} ON #{table}")
    Repo.query!("DROP FUNCTION IF EXISTS #{trigger}()")
    Repo.query!("DROP SEQUENCE IF EXISTS #{trigger}_reached")
  end
end
