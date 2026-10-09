defmodule CodexPoolerWeb.Mcp.Authentication do
  @moduledoc false

  import Plug.Conn, only: [get_req_header: 2]

  alias CodexPooler.MCP

  @spec authenticate(Plug.Conn.t()) :: {:ok, map()} | {:error, pos_integer(), integer(), String.t(), nil}
  def authenticate(%Plug.Conn{private: %{codex_pooler_mcp_auth: auth}}), do: {:ok, auth}

  def authenticate(conn) do
    conn
    |> bearer_token()
    |> MCP.authenticate_token()
    |> case do
      {:ok, auth} -> {:ok, auth}
      {:error, reason} -> mcp_auth_error(reason)
    end
  end

  defp bearer_token(conn) do
    with [authorization | _rest] <- get_req_header(conn, "authorization"),
         "Bearer " <> token <- authorization do
      String.trim(token)
    else
      _other -> nil
    end
  end

  defp mcp_auth_error(%{code: code, message: message})
       when code in [
              :mcp_service_disabled,
              :mcp_account_disabled,
              :mcp_operator_deleted,
              :mcp_operator_disabled,
              :mcp_operator_password_change_required
            ] do
    {:error, 403, -32_000, message, nil}
  end

  defp mcp_auth_error(_reason) do
    {:error, 401, -32_000, "MCP bearer token is required", nil}
  end
end
