defmodule CodexPoolerWeb.Admin.MalformedIdentifierLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import ExUnit.CaptureLog
  import CodexPooler.PoolerFixtures, only: [pool_fixture: 1]

  alias CodexPooler.{Access, MCP, Repo}
  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounts.User
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.MCP.OperatorMCPKey

  @moduletag capture_log: false
  setup :register_and_log_in_user

  setup %{scope: scope, user: user} do
    pool = pool_fixture(%{created_by_user_id: user.id})
    {:ok, _} = Access.create_api_key(scope, pool, %{display_name: "Boundary key"})
    {:ok, _} = MCP.create_operator_token(user, %{label: "Boundary MCP"})
    :ok
  end

  for {path, events, message} <- [
        {"/admin/operators", ~w(deactivate_operator save_operator save_temporary_password edit_operator reset_operator_password reactivate_operator), "operator was not found"},
        {"/admin/api-keys", ~w(rotate_api_key edit_api_key delete_api_key confirm_delete_api_key disable_api_key enable_api_key revoke_api_key), "api key was not found"},
        {"/admin/settings?tab=account", ~w(open_delete_mcp_key rename_mcp_key confirm_delete_mcp_key), "mcp key was not found"}
      ],
      event <- events do
    test "#{event} preserves the page and rows for malformed versus unknown ids", %{conn: conn} do
      Process.flag(:trap_exit, true)
      {:ok, view, _html} = live(conn, unquote(path))
      before_rows = snapshot_rows()

      for id <- [Ecto.UUID.generate(), "not-a-uuid", "", String.duplicate("z", 36)] do
        {result, _private_log} =
          with_log(fn ->
            try do
              html = render_hook(view, unquote(event), payload(unquote(event), id))
              {Process.alive?(view.pid), String.contains?(String.downcase(html), unquote(message))}
            rescue
              _error -> :crashed
            catch
              :exit, _reason -> :crashed
            end
          end)

        assert result == {true, true}
      end

      unchanged? = before_rows == snapshot_rows()
      assert unchanged?
    end
  end

  defp snapshot_rows, do: {Repo.all(User), Repo.all(APIKey), Repo.all(OperatorMCPKey), Repo.all(AuditEvent)}

  defp payload("save_operator", id), do: %{"operator_edit" => %{"id" => id, "email" => "missing@example.com", "display_name" => "Missing", "role" => "instance_admin"}}
  defp payload("save_temporary_password", id), do: %{"operator_reset" => %{"id" => id, "operation" => "reset", "password_mode" => "generated", "send_email" => "false"}}
  defp payload("rename_mcp_key", id), do: %{"mcp_key" => %{"id" => id, "label" => "Changed"}}
  defp payload("confirm_delete_mcp_key", id), do: %{"mcp_key_delete" => %{"id" => id}}
  defp payload("confirm_delete_api_key", id), do: %{"api_key_delete" => %{"id" => id, "confirmation_prefix" => "missing"}}
  defp payload(_event, id), do: %{"id" => id}
end
