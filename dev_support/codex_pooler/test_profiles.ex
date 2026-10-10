defmodule CodexPooler.TestProfiles do
  @moduledoc """
  File selection for application and repository-tooling test runs.

  Selection happens before Mix requires test modules. Mixed application files
  contribute only their Unix-tagged tests to the tooling profile. An unfocused
  `--partitions N` run of the product or tooling profile gets only its own
  partition's files, dealt by recorded duration (`CodexPooler.TestPartitions`)
  instead of by position.
  """

  alias CodexPooler.TestPartitions

  @tooling_patterns ~w(
    test/codex_pooler/dev/**/*_test.exs
    test/mix/**/*_test.exs
    test/support/**/*_test.exs
    test/codex_pooler/support/**/*_test.exs
    test/codex_pooler/platform/bootstrap_owner_fixture_test.exs
    test/codex_pooler/platform/committed_fixture_cleanup_test.exs
    test/codex_pooler/platform/committed_pool_cleanup_test.exs
    test/codex_pooler/platform/committed_write_guard_test.exs
    test/codex_pooler/platform/instance_presence_peer_cleanup_test.exs
    test/codex_pooler/platform/instance_presence_peer_cleanup_postgres_test.exs
    test/codex_pooler/platform/rollup_coverage_fence_test.exs
    test/codex_pooler/platform/test_duration_guard_test.exs
    test/codex_pooler/platform/test_logger_level_test.exs
    test/codex_pooler/gateway/runtime/committed_gateway_fixture_cleanup_test.exs
    test/codex_pooler/sandbox_queue_test.exs
    test/codex_pooler/test_app_env_test.exs
    test/codex_pooler/test_runtime_database_config_test.exs
    test/codex_pooler/unboxed_fixture_contract_test.exs
    test/codex_pooler_web/controllers/runtime/backend_codex_test_support_mailbox_test.exs
    test/codex_pooler_web/controllers/runtime/websocket_cleanup_fence_test.exs
    test/test_helper_test.exs
  )

  @mixed_unix_files ~w(
    test/codex_pooler_web/live/admin/pages/upstream_cockpit_live_test.exs
    test/codex_pooler_web/live/admin/pages/upstreams_live_test.exs
  )

  @type profile :: :product | :tooling | :unix

  @spec tooling_files() :: [String.t()]
  def tooling_files, do: @tooling_patterns |> Enum.flat_map(&Path.wildcard/1) |> Enum.uniq() |> Enum.sort()

  @spec files(profile()) :: [String.t()]
  def files(:product), do: Path.wildcard("test/**/*_test.exs") -- tooling_files()
  def files(profile) when profile in [:tooling, :unix], do: Enum.sort(Enum.uniq(tooling_files() ++ @mixed_unix_files))

  @spec arguments(profile(), [String.t()]) :: [String.t()]
  def arguments(profile, args) do
    focused? = Enum.any?(args, &test_selector?/1)

    if profile == :tooling and not focused? and Enum.any?(args, &custom_filter?/1) do
      Mix.raise("test.tooling owns its profile filters; use mix test.unix for Unix-only tests or mix test <path> with custom filters")
    end

    cond do
      focused? and profile == :unix -> filters(:unix) ++ args
      focused? -> args
      true -> filters(profile) ++ unfocused(profile, args)
    end
  end

  # The unix profile keeps Mix's own deal; the other two take `--partitions` out and name only this partition's files.
  defp unfocused(:unix, args), do: args ++ files(:unix)

  defp unfocused(profile, args) do
    case TestPartitions.select(profile, args, files(profile)) do
      {:partition, remaining, assigned} -> remaining ++ assigned
      :unpartitioned -> args ++ files(profile)
    end
  end

  @spec missing_unix_files([ExUnit.Test.t()]) :: [String.t()]
  def missing_unix_files(tests) do
    tests
    |> Enum.filter(&(&1.tags[:unix_integration] == true))
    |> Enum.map(&Path.relative_to_cwd(&1.tags.file))
    |> Enum.uniq()
    |> Kernel.--(files(:tooling))
    |> Enum.sort()
  end

  @spec verify_loaded_unix_files!() :: :ok
  def verify_loaded_unix_files! do
    tests =
      for {module, _file} <- :code.all_loaded(),
          function_exported?(module, :__ex_unit__, 0),
          test <- module.__ex_unit__().tests,
          do: test

    case missing_unix_files(tests) do
      [] -> :ok
      missing -> raise "Unix test files missing from the tooling profile: #{Enum.join(missing, ", ")}"
    end
  end

  defp filters(:product), do: []
  defp filters(:unix), do: ["--only", "unix_integration"]

  defp filters(:tooling) do
    ["--exclude", "test", "--include", "unix_integration"] ++
      Enum.flat_map(tooling_files(), &["--include", "file:#{Path.expand(&1)}"])
  end

  defp test_selector?(arg), do: File.dir?(arg) or Regex.match?(~r/\.exs(?::\d+)*$/, arg)

  defp custom_filter?(arg), do: Enum.any?(~w(--only --include --exclude --name-pattern -n), &(arg == &1 or String.starts_with?(arg, &1 <> "=")))
end
