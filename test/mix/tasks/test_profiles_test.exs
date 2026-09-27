defmodule CodexPooler.TestProfilesTest do
  use ExUnit.Case, async: true

  alias CodexPooler.TestProfiles

  test "product and tooling classify every test file without losing shared Unix files" do
    all = MapSet.new(Path.wildcard("test/**/*_test.exs"))
    product = MapSet.new(TestProfiles.files(:product))
    tooling = MapSet.new(TestProfiles.tooling_files())
    assert MapSet.union(product, tooling) == all
    assert MapSet.disjoint?(product, tooling)
    assert MapSet.member?(product, "test/codex_pooler/accounting/request_replay_test.exs")
    assert MapSet.member?(tooling, "test/codex_pooler/platform/committed_write_guard_test.exs")
    assert MapSet.member?(tooling, "test/mix/tasks/test_fast_make_test.exs")
    assert "test/codex_pooler_web/live/admin/pages/upstreams_live_test.exs" in TestProfiles.files(:tooling)
    assert Enum.all?(TestProfiles.files(:tooling), &File.regular?/1)
    refute Enum.any?(product, &String.starts_with?(&1, "test/codex_pooler/dev/"))
    refute Enum.any?(product, &String.starts_with?(&1, "test/mix/"))
  end

  test "tooling includes every case in tooling files but only Unix cases in mixed application files" do
    {config, _} = TestProfiles.arguments(:tooling, []) |> parse_options()
    tool = Path.expand("test/mix/tasks/test_profiles_test.exs")
    app = Path.expand("test/codex_pooler_web/live/admin/pages/upstreams_live_test.exs")
    assert evaluate(config, tool, false) == :ok
    assert evaluate(config, tool, true) == :ok
    assert evaluate(config, app, true) == :ok
    assert {:excluded, _} = evaluate(config, app, false)
  end

  test "a focused duration remeasurement does not expand back to the whole product suite" do
    selector = "test/codex_pooler/platform/readiness_test.exs:12"
    assert TestProfiles.arguments(:product, [selector, "--seed", "4"]) == [selector, "--seed", "4"]
    assert TestProfiles.arguments(:tooling, [selector, "--only", "sample"]) == [selector, "--only", "sample"]
    assert TestProfiles.arguments(:unix, [selector]) == ["--only", "unix_integration", selector]

    for filter <- ["--only", "--include", "--exclude", "--name-pattern", "-n", "--only=unix_integration"] do
      assert_raise Mix.Error, ~r/owns its profile filters/, fn -> TestProfiles.arguments(:tooling, [filter, "sample"]) end
    end
  end

  test "an unlisted Unix case is reported even when its test was excluded" do
    known = %ExUnit.Test{tags: %{file: Path.expand("test/mix/tasks/qa_phase_test.exs"), unix_integration: true}}
    missing = %ExUnit.Test{state: {:excluded, "profile"}, tags: %{file: Path.expand("test/codex_pooler/new_unix_test.exs"), unix_integration: true}}
    assert TestProfiles.missing_unix_files([known, missing]) == ["test/codex_pooler/new_unix_test.exs"]
  end

  defp parse_options(args) do
    {opts, paths, []} = OptionParser.parse(args, strict: [exclude: :keep, include: :keep])
    {Keyword.new(include: ExUnit.Filters.parse(Keyword.get_values(opts, :include)), exclude: ExUnit.Filters.parse(Keyword.get_values(opts, :exclude))), paths}
  end

  defp evaluate(config, file, unix?) do
    tags = %{test: :sample, file: file}
    tags = if unix?, do: Map.put(tags, :unix_integration, true), else: tags
    ExUnit.Filters.eval(config[:include], config[:exclude], tags, [])
  end
end
