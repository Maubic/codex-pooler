defmodule CodexPoolerWeb.Runtime.PeerLogRelay do
  @moduledoc false

  # `capture_log` on the test node never sees what a peer VM logs, and an
  # owner on a peer logs there (findings#329 row J). `attach!/3` raises the
  # peer's level to info and adds a `:logger` handler that sends each line
  # starting with one of `prefixes` to `recipient` as
  # `{:peer_log, peer_node, line}`; both are undone when the test ends.

  import ExUnit.Callbacks, only: [on_exit: 1]

  @call_timeout_ms 15_000

  @spec attach!(node(), [String.t()], pid()) :: :ok
  def attach!(peer_node, prefixes, recipient \\ self()) when is_atom(peer_node) and is_list(prefixes) and is_pid(recipient) do
    handler_id = :"peer_log_relay_#{System.unique_integer([:positive])}"
    previous_level = :erpc.call(peer_node, Logger, :level, [], @call_timeout_ms)
    on_exit(fn -> detach(peer_node, handler_id, previous_level) end)
    :ok = :erpc.call(peer_node, Logger, :configure, [[level: :info]], @call_timeout_ms)
    config = %{level: :info, config: %{recipient: recipient, prefixes: prefixes}}
    :ok = :erpc.call(peer_node, :logger, :add_handler, [handler_id, __MODULE__, config], @call_timeout_ms)
  end

  @doc false
  # The `:logger` handler callback, run on the peer in the process that logs.
  @spec log(:logger.log_event(), :logger.handler_config()) :: :ok
  def log(%{msg: {:string, chardata}}, %{config: %{recipient: recipient, prefixes: prefixes}}) do
    line = IO.chardata_to_string(chardata)
    if Enum.any?(prefixes, &String.starts_with?(line, &1)), do: send(recipient, {:peer_log, node(), line})
    :ok
  end

  def log(_event, _config), do: :ok

  defp detach(peer_node, handler_id, previous_level) do
    if peer_node in Node.list(:connected) do
      _removed = :erpc.call(peer_node, :logger, :remove_handler, [handler_id], @call_timeout_ms)
      :ok = :erpc.call(peer_node, Logger, :configure, [[level: previous_level]], @call_timeout_ms)
    end

    :ok
  end
end
