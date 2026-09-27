defmodule Mix.Tasks.Test.Product do
  @moduledoc "Runs application tests without loading development/tooling test files. Accepts mix test options and focused paths."
  @shortdoc "Runs application tests"
  use Mix.Task

  @impl Mix.Task
  def run(args), do: Mix.Task.run("test", CodexPooler.TestProfiles.arguments(:product, args))
end
