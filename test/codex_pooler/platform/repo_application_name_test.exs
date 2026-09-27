defmodule CodexPooler.Platform.RepoApplicationNameTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Release

  setup do
    previous = System.get_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS")
    on_exit(fn -> if previous, do: System.put_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS", previous), else: System.delete_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS") end)
    System.delete_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS")
    :ok
  end

  test "release migration advisory waits use a finite configurable contention budget" do
    assert Release.repo_config_for_task([], :migrate)[:migration_advisory_lock_max_tries] == 600
    System.put_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS", "2")
    opts = Release.repo_config_for_task([], :migrate)
    assert opts[:migration_advisory_lock_max_tries] == 2
    assert opts[:migration_advisory_lock_retry_interval_ms] == 1_000

    for value <- ["", "0", "-1", "1s", "1.5"] do
      System.put_env("MIGRATION_ADVISORY_LOCK_WAIT_SECONDS", value)
      assert_raise ArgumentError, ~r/MIGRATION_ADVISORY_LOCK_WAIT_SECONDS/, fn -> Release.repo_config_for_task([], :migrate) end
    end
  end

  test "Repo connections carry the configured PostgreSQL application_name" do
    assert Repo.config()[:parameters][:application_name] == "codex_pooler_test"

    assert %{rows: [["codex_pooler_test"]]} =
             Repo.query!("SELECT current_setting('application_name')", [])
  end

  test "release database tasks name their connections after the task and keep other parameters" do
    repo_config = [
      url: "ecto://user:pass@example.invalid/db",
      timeout: 1_234,
      parameters: [application_name: "codex_pooler_web", search_path: "public"]
    ]

    for {task, application_name} <- [
          migrate: "codex_pooler_migrate",
          rollback: "codex_pooler_migrate",
          import_openai_pricing: "codex_pooler_pricing_import"
        ] do
      config = Release.repo_config_for_task(repo_config, task)

      assert config[:url] == repo_config[:url]
      assert config[:parameters][:application_name] == application_name
      assert config[:parameters][:search_path] == "public"
      assert Keyword.get_values(config[:parameters], :application_name) == [application_name]
      assert byte_size(application_name) <= 63

      if task in [:migrate, :rollback] do
        assert config[:timeout] == :infinity
      else
        assert config[:timeout] == 1_234
      end
    end

    assert Release.repo_config_for_task([], :migrate) ==
             [migration_advisory_lock_max_tries: 600, migration_advisory_lock_retry_interval_ms: 1_000, timeout: :infinity, parameters: [application_name: "codex_pooler_migrate"]]
  end
end
