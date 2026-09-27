defmodule Mix.Tasks.Test.Unix do
  @moduledoc "Runs only Unix integration tests without loading unrelated application test files. Accepts mix test options."
  @shortdoc "Runs the Unix integration profile"
  use Mix.Task

  @impl Mix.Task
  def run(args), do: Mix.Task.run("test", CodexPooler.TestProfiles.arguments(:unix, args))
end
