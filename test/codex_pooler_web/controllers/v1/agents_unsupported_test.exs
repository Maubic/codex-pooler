defmodule CodexPoolerWeb.V1.AgentsUnsupportedTest do
  @moduledoc """
  The OpenAI beta Agents API (`/agents/...`, and its credential vaults under `/vaults/...`) is not a Codex Pooler surface:
  the Codex backend behind the gateway serves no such route, so nothing could be translated or relayed. A client pointed
  at a Pooler `/v1` base URL gets one documented refusal for the whole family, after the shared auth and compatibility
  gates and before any body parsing, upstream dispatch or accounting row.

  The route list is the full set of calls the openai-node beta Agents resources make (`client.beta.agents.*`, including
  the session `stream` and `create` helpers, `functionTool` tool results, hosted environment files and result artifacts),
  with fixture identifiers in place of ids. The generic `/v1/files` route those helpers upload to first is a separate,
  supported surface and keeps its own tests (`files_controller_test.exs`).
  """

  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.PoolerFixtures

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools
  alias CodexPooler.Repo

  @refusal_message "Unsupported OpenAI /v1 endpoint: the beta Agents API is not supported"

  @sdk_routes [
    {:get, "/v1/agents"},
    {:post, "/v1/agents"},
    {:get, "/v1/agents/agent_fixture"},
    {:post, "/v1/agents/agent_fixture"},
    {:delete, "/v1/agents/agent_fixture"},
    {:get, "/v1/agents/environments/env_fixture"},
    {:get, "/v1/agents/environments/env_fixture/files"},
    {:post, "/v1/agents/environments/env_fixture/files"},
    {:get, "/v1/agents/environments/templates"},
    {:post, "/v1/agents/environments/templates"},
    {:get, "/v1/agents/environments/templates/template_fixture"},
    {:post, "/v1/agents/environments/templates/template_fixture"},
    {:delete, "/v1/agents/environments/templates/template_fixture"},
    {:get, "/v1/agents/sessions"},
    {:post, "/v1/agents/sessions"},
    {:get, "/v1/agents/sessions/session_fixture"},
    {:post, "/v1/agents/sessions/session_fixture"},
    {:delete, "/v1/agents/sessions/session_fixture"},
    {:get, "/v1/agents/sessions/session_fixture/events"},
    {:post, "/v1/agents/sessions/session_fixture/events"},
    {:get, "/v1/agents/sessions/session_fixture/items"},
    {:get, "/v1/agents/sessions/session_fixture/traces"},
    {:get, "/v1/agents/sessions/session_fixture/turns"},
    {:get, "/v1/agents/sessions/session_fixture/turns/turn_fixture"},
    {:get, "/v1/agents/sessions/session_fixture/artifacts"},
    {:get, "/v1/agents/sessions/session_fixture/artifacts/artifact_fixture"},
    {:get, "/v1/agents/sessions/session_fixture/artifacts/artifact_fixture/content"},
    {:delete, "/v1/agents/sessions/session_fixture/artifacts/artifact_fixture"},
    {:get, "/v1/agents/sessions/session_fixture/subagents"},
    {:get, "/v1/agents/sessions/session_fixture/subagents/subagent_fixture"},
    {:get, "/v1/agents/sessions/session_fixture/subagents/subagent_fixture/items"},
    {:get, "/v1/agents/sessions/session_fixture/subagents/subagent_fixture/turns"},
    {:get, "/v1/agents/sessions/session_fixture/subagents/subagent_fixture/turns/turn_fixture"},
    {:get, "/v1/agents/sessions/session_fixture/subagents/subagent_fixture/turns/turn_fixture/items"},
    {:get, "/v1/vaults"},
    {:post, "/v1/vaults"},
    {:get, "/v1/vaults/vault_fixture"},
    {:delete, "/v1/vaults/vault_fixture"},
    {:get, "/v1/vaults/vault_fixture/credentials"},
    {:post, "/v1/vaults/vault_fixture/credentials"},
    {:get, "/v1/vaults/vault_fixture/credentials/credential_fixture"},
    {:post, "/v1/vaults/vault_fixture/credentials/credential_fixture"},
    {:delete, "/v1/vaults/vault_fixture/credentials/credential_fixture"}
  ]

  describe "the SDK's own Agents calls" do
    for {method, path} <- @sdk_routes do
      test "#{String.upcase(to_string(method))} #{path} answers the documented refusal", %{conn: conn} do
        setup = active_api_key_fixture()

        conn = conn |> auth(setup) |> request(unquote(method), unquote(path))

        assert_agents_refusal(conn)
        assert_no_gateway_side_effects()
      end
    end

    test "the session event stream is refused as JSON even when the client asks for an event stream", %{conn: conn} do
      setup = active_api_key_fixture()

      conn =
        conn
        |> auth(setup)
        |> put_req_header("accept", "text/event-stream")
        |> get("/v1/agents/sessions/session_fixture/events")

      assert_agents_refusal(conn)
    end

    test "a tool result submission is refused without reading its body", %{conn: conn} do
      setup = active_api_key_fixture()

      conn =
        conn
        |> auth(setup)
        |> put_req_header("content-type", "application/json")
        |> put_req_header("idempotency-key", "synthetic-idempotency-key")
        |> post("/v1/agents/sessions/session_fixture/events", "{not-json")

      assert_agents_refusal(conn)
      assert_no_gateway_side_effects()
    end

    test "no Agents call dispatches upstream, reserves quota or writes an accounting row", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.json_response(%{"id" => "must_not_dispatch"}))
      setup = gateway_setup(upstream)

      for {method, path} <- @sdk_routes do
        conn |> recycle() |> auth(setup) |> request(method, path) |> assert_agents_refusal()
      end

      assert FakeUpstream.requests(upstream) == []
      assert_no_gateway_side_effects()
      assert Repo.aggregate(LedgerEntry, :count) == 0
    end
  end

  describe "the shared /v1 gates stay ahead of the refusal" do
    test "an unauthenticated request meets the key check, whatever the body", %{conn: conn} do
      oversized_body = "{" <> String.duplicate("x", 9_000_000)

      for path <- ["/v1/agents/sessions", "/v1/vaults"] do
        conn =
          conn
          |> recycle()
          |> put_req_header("content-type", "application/json")
          |> post(path, oversized_body)

        assert %{"error" => %{"code" => "api_key_missing", "message" => "api key is required"}} =
                 json_response(conn, 401)
      end

      assert_no_gateway_side_effects()
    end

    test "a key of a disabled Pool meets the key check", %{conn: conn} do
      setup = active_api_key_fixture()

      setup.pool
      |> Ecto.Changeset.change(%{status: "disabled"})
      |> Repo.update!()

      conn = conn |> auth(setup) |> post("/v1/agents/sessions", %{})

      assert %{"error" => %{"code" => "api_key_missing"}} = json_response(conn, 401)
      assert_no_gateway_side_effects()
    end

    test "a Pool with /v1 compatibility switched off meets the compatibility check", %{conn: conn} do
      setup = active_api_key_fixture()

      setup.pool
      |> Pools.ensure_routing_settings()
      |> Ecto.Changeset.change(%{v1_compatibility_enabled: false})
      |> Repo.update!()

      for {method, path} <- [{:post, "/v1/agents/sessions"}, {:get, "/v1/vaults"}] do
        conn = conn |> recycle() |> auth(setup) |> request(method, path)

        assert %{"error" => %{"code" => "v1_compatibility_disabled", "message" => "OpenAI /v1 compatibility is disabled for this pool"}} =
                 json_response(conn, 403)
      end

      assert_no_gateway_side_effects()
    end
  end

  describe "the refused family" do
    test "includes the bare prefixes with or without a trailing slash", %{conn: conn} do
      setup = active_api_key_fixture()

      for path <- ["/v1/agents", "/v1/agents/", "/v1/vaults", "/v1/vaults/"] do
        conn = conn |> recycle() |> auth(setup) |> get(path)

        assert_agents_refusal(conn)
      end
    end

    test "stops at the segment boundary: neighbouring names stay router misses", %{conn: conn} do
      setup = active_api_key_fixture()

      for path <- ["/v1/agent", "/v1/agentsx", "/v1/agents-sessions", "/v1/vault", "/v1/vaultsx", "/v1/sessions/agents"] do
        conn = conn |> recycle() |> auth(setup) |> get(path)

        assert html_response(conn, 404) =~ "Not Found"
      end

      assert_no_gateway_side_effects()
    end

    test "leaves the generic /v1 routes the SDK helpers upload through unchanged", %{conn: conn} do
      setup = active_api_key_fixture()

      listed = conn |> auth(setup) |> get("/v1/files")

      assert %{"object" => "list", "data" => []} = json_response(listed, 200)
    end
  end

  # The official SDKs ask for JSON, which is also how a router miss would render as `{"errors": ...}`.
  defp request(conn, method, path) do
    conn = if get_req_header(conn, "accept") == [], do: put_req_header(conn, "accept", "application/json"), else: conn

    case method do
      :get -> get(conn, path)
      :post -> post(conn, path, %{})
      :delete -> delete(conn, path)
    end
  end

  defp auth(conn, setup), do: put_req_header(conn, "authorization", setup.authorization)

  defp assert_agents_refusal(conn) do
    assert json_response(conn, 404) == %{
             "error" => %{
               "message" => @refusal_message,
               "type" => "invalid_request_error",
               "code" => "unsupported_endpoint",
               "param" => nil
             }
           }

    assert [content_type] = get_resp_header(conn, "content-type")
    assert content_type =~ "application/json"
    conn
  end

  defp assert_no_gateway_side_effects do
    assert Repo.aggregate(Request, :count) == 0
    assert Repo.aggregate(Attempt, :count) == 0
  end
end
