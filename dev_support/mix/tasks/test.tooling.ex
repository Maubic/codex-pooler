defmodule Mix.Tasks.Test.Tooling do
  @moduledoc "Runs development tools, Mix tasks, test-infrastructure contracts and Unix integration tests. Accepts execution options; use test.unix or explicit file paths for custom filtering."
  @shortdoc "Runs repository tooling and Unix integration tests"
  use Mix.Task

  @impl Mix.Task
  def run(args), do: Mix.Task.run("test", CodexPooler.TestProfiles.arguments(:tooling, args))
end
