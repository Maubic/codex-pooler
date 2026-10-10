defmodule Mix.Tasks.Dev.ObservatorySeed do
  @moduledoc """
  Adds sixty synthetic request logs to an existing local Observatory key.

      mix dev.observatory_seed --api-key-prefix STORED_PREFIX

  Accepts only the exact loopback `codex_pooler_dev` database. No provider
  requests are made. Reruns preserve the original rows and their timestamps.
  """

  use Mix.Task

  alias CodexPooler.Accounting.ExecutionRecovery
  alias CodexPooler.Dev.{LocalTarget, QaBackgroundWorkers}
  alias CodexPooler.Dev.Seeds.ObservatoryPreview
  alias CodexPooler.Platform.InstanceHeartbeat
  alias CodexPooler.Repo

  @requirements ["app.config"]
  @shortdoc "Add synthetic local Observatory request logs without replacing data"

  @impl Mix.Task
  def run(args) do
    unless Mix.env() == :dev, do: Mix.raise("Observatory preview seeding requires MIX_ENV=dev")
    prefix = parse_args!(args)

    case LocalTarget.validate_target_database("codex_pooler_dev", Application.fetch_env!(:codex_pooler, Repo)) do
      :ok -> :ok
      {:error, message} -> Mix.raise(message)
    end

    # This VM only adds terminal synthetic rows; it must not claim an instance
    # or recover another listener's in-flight accounting while doing so.
    Application.put_env(:codex_pooler, InstanceHeartbeat, enabled: false)
    Application.put_env(:codex_pooler, ExecutionRecovery, enabled: false)
    endpoint = Application.fetch_env!(:codex_pooler, CodexPoolerWeb.Endpoint)
    Application.put_env(:codex_pooler, CodexPoolerWeb.Endpoint, Keyword.merge(endpoint, server: false, watchers: []))
    QaBackgroundWorkers.start_application!()
    result = ObservatoryPreview.seed!(prefix)
    Mix.shell().info("Observatory preview requests: inserted=#{result.inserted} existing=#{result.existing} total=#{result.total}")
  end

  defp parse_args!(args) do
    case OptionParser.parse(args, strict: [api_key_prefix: :string]) do
      {[api_key_prefix: prefix], [], []} when byte_size(prefix) > 0 -> prefix
      _invalid -> Mix.raise("usage: mix dev.observatory_seed --api-key-prefix STORED_PREFIX")
    end
  end
end
