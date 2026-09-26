defmodule CodexPoolerWeb.Mcp.LogContractRegressionTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.{Audit, InstanceSettings, MCP, Repo}
  alias CodexPooler.MCP.Redaction

  setup do
    %{user: user} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    user = user |> Ecto.Changeset.change(password_change_required: false) |> Repo.update!()
    assert {:ok, _} = InstanceSettings.update_system_settings(InstanceSettings.ensure_singleton!(), %{"mcp" => %{"enabled" => true}})
    assert {:ok, _} = MCP.set_operator_mcp_enabled(user, true)
    assert {:ok, %{raw_token: token}} = MCP.create_operator_token(user, %{label: "Log contract"})
    %{token: token, user: user}
  end

  test "log tools reject offsets beyond their declared bounded window before querying history", %{token: token} do
    catalog = rpc(token, "tools/list", %{})["tools"]

    for name <- ["codex_pooler_list_request_logs", "codex_pooler_list_audit_logs"] do
      schema = Enum.find(catalog, &(&1["name"] == name))["inputSchema"]

      for offset <- [10_001, 10_000_000_000, 100_000_000_000_000_000_000] do
        {result, queries} = capture_history_queries(fn -> rpc(token, "tools/call", %{"name" => name, "arguments" => %{"offset" => offset}}) end)
        assert result["isError"] == true
        assert hd(result["content"])["text"] =~ "invalid_arguments:"
        assert queries == []
        assert :ok = Redaction.assert_mcp_output_safe!(result)
      end

      assert schema["properties"]["offset"]["maximum"] == 10_000
      {result, queries} = capture_history_queries(fn -> rpc(token, "tools/call", %{"name" => name, "arguments" => %{"offset" => 10_000}}) end)
      assert result["isError"] == false
      assert result["structuredContent"]["offset"] == 10_000
      assert queries != []
      assert Enum.any?(queries, fn {_query, params} -> 20_001 in params end)
    end
  end

  test "audit outcome rejects denied and finds the normalized failure", %{token: token, user: user} do
    assert {:ok, event} = Audit.record_event(%{actor_type: "user", actor_user_id: user.id, action: "sample.denial", target_type: "user", target_id: user.id, outcome: "denied"})
    assert event.outcome == "failure"
    result = rpc(token, "tools/call", %{"name" => "codex_pooler_list_audit_logs", "arguments" => %{"outcome" => "denied"}})
    assert result["isError"] == true
    assert hd(result["content"])["text"] =~ "invalid_arguments:"
    result = rpc(token, "tools/call", %{"name" => "codex_pooler_list_audit_logs", "arguments" => %{"outcome" => "failure"}})
    assert result["isError"] == false
    assert Enum.any?(result["structuredContent"]["items"], &(&1["id"] == event.id))
  end

  test "real request detail conforms to the advertised debug required fields", %{token: token} do
    %{pool: pool, api_key: key} = active_api_key_fixture()
    request = request_fixture(%{pool: pool, api_key: key})
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    attempt_fixture(request, assignment)
    tool = Enum.find(rpc(token, "tools/list", %{})["tools"], &(&1["name"] == "codex_pooler_get_request_log"))
    required = get_in(tool, ["outputSchema", "properties", "item", "properties", "debug", "required"])
    result = rpc(token, "tools/call", %{"name" => tool["name"], "arguments" => %{"id" => request.id}})
    assert result["isError"] == false
    debug = get_in(result, ["structuredContent", "item", "debug"])
    assert required -- Map.keys(debug) == []
    assert length(debug["attempts"]) == 1
    assert :ok = Redaction.assert_mcp_output_safe!(result)
  end

  defp rpc(token, method, params) do
    build_conn()
    |> put_req_header("authorization", "Bearer #{token}")
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", "application/json, text/event-stream")
    |> put_req_header("mcp-protocol-version", "2025-11-25")
    |> post("/mcp", CodexPooler.JSON.encode!(%{"jsonrpc" => "2.0", "id" => "contract", "method" => method, "params" => params}))
    |> json_response(200)
    |> Map.fetch!("result")
  end

  defp capture_history_queries(fun) do
    handler_id = {__MODULE__, make_ref()}
    owner = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.capture_query/4, owner)
    result = fun.()
    :telemetry.detach(handler_id)
    {result, collect_queries([])}
  end

  def capture_query(_event, _measurements, metadata, owner) do
    if self() == owner and String.starts_with?(metadata.query, "SELECT") and
         (metadata.query =~ ~s("requests") or metadata.query =~ ~s("audit_events")),
       do: send(owner, {:history_query, metadata.query, metadata.params})
  end

  defp collect_queries(queries) do
    receive do
      {:history_query, query, params} -> collect_queries([{query, params} | queries])
    after
      0 -> Enum.reverse(queries)
    end
  end
end
