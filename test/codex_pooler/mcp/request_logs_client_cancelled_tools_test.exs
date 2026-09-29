defmodule CodexPooler.MCP.RequestLogsClientCancelledToolsTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.InstanceSettings
  alias CodexPooler.MCP
  alias CodexPooler.MCP.{OperatorMCPKey, OperatorMCPSettings, Redaction, ToolDispatch}
  alias CodexPooler.Repo

  # `status` keeps matching the recorded status, where a client cancellation is
  # `failed`; `status: "client_cancelled"` selects the class, and every row's
  # `display_status` is the status the admin pages show (findings#292).
  setup do
    reset_bootstrap_state_fixture!()
    Repo.delete_all(OperatorMCPKey)
    Repo.delete_all(OperatorMCPSettings)
    Repo.delete_all(InstanceSettings.Settings)
    InstanceSettings.reset_cache_for_test()
    on_exit(fn -> InstanceSettings.reset_cache_for_test() end)

    %{user: user} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    user = user |> Ecto.Changeset.change(password_change_required: false) |> Repo.update!()
    settings = InstanceSettings.ensure_singleton!()
    assert {:ok, _updated} = InstanceSettings.update_system_settings(settings, %{"mcp" => %{"enabled" => true}})
    assert {:ok, _operator_settings} = MCP.set_operator_mcp_enabled(user, true)
    assert {:ok, %{raw_token: raw_token}} = MCP.create_operator_token(user, %{label: "Client cancel MCP"})
    assert {:ok, auth} = MCP.authenticate_token(raw_token)

    pool = pool_fixture(%{slug: "mcp-client-cancel-#{System.unique_integer([:positive])}", name: "MCP Client Cancel"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    context = %{pool: pool, api_key: api_key}

    rows = %{
      cancelled_websocket: request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}),
      cancelled_stream: request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"}),
      drained: request_fixture(context, %{status: "failed", last_error_code: "owner_drained", response_status_code: 499, transport: "websocket"}),
      succeeded: request_fixture(context)
    }

    %{auth: auth, pool: pool, rows: rows}
  end

  test "status client_cancelled lists the class, each row naming it beside its recorded status", %{auth: auth, pool: pool, rows: rows} do
    assert {:ok, result} = ToolDispatch.call("codex_pooler_list_request_logs", %{"pool_id" => pool.id, "status" => "client_cancelled"}, %{auth: auth})

    assert result["isError"] == false
    assert :ok = Redaction.assert_mcp_output_safe!(result)
    items = result["structuredContent"]["items"]

    assert MapSet.new(items, & &1["id"]) == MapSet.new([rows.cancelled_websocket.id, rows.cancelled_stream.id])
    assert Enum.all?(items, &(&1["status"] == "failed" and &1["display_status"] == "client_cancelled"))

    assert [%{"type" => "text", "text" => text}] = result["content"]
    assert text =~ "2 request logs returned; total 2; offset 0; statuses client_cancelled:2"
    assert text =~ "status=client_cancelled"
    refute text =~ "status=failed"
  end

  test "status failed keeps the recorded meaning and the text names the class of each row", %{auth: auth, pool: pool, rows: rows} do
    assert {:ok, result} = ToolDispatch.call("codex_pooler_list_request_logs", %{"pool_id" => pool.id, "status" => "failed"}, %{auth: auth})

    items = Map.new(result["structuredContent"]["items"], &{&1["id"], &1})

    assert MapSet.new(Map.keys(items)) == MapSet.new([rows.cancelled_websocket.id, rows.cancelled_stream.id, rows.drained.id])
    assert items[rows.drained.id]["display_status"] == "failed"
    assert items[rows.cancelled_stream.id]["display_status"] == "client_cancelled"

    assert [%{"type" => "text", "text" => text}] = result["content"]
    assert text =~ "3 request logs returned; total 3; offset 0; statuses client_cancelled:2, failed:1"
  end

  test "the detail tool carries the class too", %{auth: auth, rows: rows} do
    assert {:ok, result} = ToolDispatch.call("codex_pooler_get_request_log", %{"id" => rows.cancelled_websocket.id}, %{auth: auth})

    assert :ok = Redaction.assert_mcp_output_safe!(result)
    assert %{"status" => "ok", "item" => %{"status" => "failed", "display_status" => "client_cancelled"}} = result["structuredContent"]
    assert [%{"type" => "text", "text" => text}] = result["content"]
    assert text =~ "status=client_cancelled"
  end
end
