defmodule CodexPooler.Gateway.Transports.Websocket.OwnerDefaults do
  @moduledoc false

  @forward_timeout_ms 5_000
  @owner_call_timeout_ms 5_000
  @downstream_send_timeout_ms 1_000

  # Only tests set the forward and owner call budgets, through the
  # application environment, as the upstream session's keepalive interval is
  # set: a real owner suspended past them then fits a test. Neither runtime
  # configuration nor an Instance Setting carries them.
  @spec forward_timeout_ms() :: pos_integer()
  def forward_timeout_ms, do: test_budget(:forward_timeout_ms, @forward_timeout_ms)

  @spec owner_call_timeout_ms() :: pos_integer()
  def owner_call_timeout_ms, do: test_budget(:owner_call_timeout_ms, @owner_call_timeout_ms)

  defp test_budget(key, default) do
    :codex_pooler
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(key, default)
    |> case do
      timeout_ms when is_integer(timeout_ms) and timeout_ms > 0 -> timeout_ms
      _timeout_ms -> default
    end
  end

  @spec downstream_send_timeout_ms() :: pos_integer()
  def downstream_send_timeout_ms, do: @downstream_send_timeout_ms
end
