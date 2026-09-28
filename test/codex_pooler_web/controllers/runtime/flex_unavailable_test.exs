defmodule CodexPoolerWeb.Runtime.FlexUnavailableTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, strict_native_request: 2, collect_native_turn_frames!: 1, assert_single_native_turn_terminal!: 2]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Repo

  @moduletag capture_log: true

  for mode <- ["full", "lite"], route <- ["/backend-api/codex/responses", "/v1/responses"], stream? <- [false, true] do
    @mode mode
    @route route
    @stream stream?
    test "#{mode} #{route} stream=#{stream?} preserves terminal Flex without failover or quota exhaustion", %{conn: conn} do
      upstream = start_upstream({:json_headers, 429, %{"error" => %{"code" => "flex_unavailable", "message" => "synthetic provider detail"}}, []})
      setup = gateway_setup(upstream)
      sibling = start_upstream(FakeUpstream.json_response(%{"output" => []}))
      other = gateway_upstream(setup.pool, sibling, "synthetic-flex-sibling", [])
      setup = %{setup | model: put_model_source_assignments!(setup.model, [setup.assignment, other.assignment])}
      set_model_serving_mode!(model_serving_scope(), setup, @mode)

      conn = conn |> auth(setup) |> then(&if @mode == "lite", do: put_req_header(&1, "x-openai-internal-codex-responses-lite", "true"), else: &1)
      conn = post(conn, @route, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic input"), "stream" => @stream})

      assert conn.status == 429
      assert %{"error" => %{"code" => "flex_unavailable"}} = json_response(conn, 429)
      refute conn.resp_body =~ "synthetic provider detail"
      assert get_resp_header(conn, "x-should-retry") == ["false"]
      assert FakeUpstream.count(upstream) == 1
      assert FakeUpstream.count(sibling) == 0
      assert [%Request{id: id, status: "failed", last_error_code: "flex_unavailable"}] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert [%Attempt{status: "failed"}] = Repo.all(from(a in Attempt, where: a.request_id == ^id))
      refute Repo.exists?(from(d in BridgeDemotion, where: d.pool_id == ^setup.pool.id))
      refute Repo.exists?(from(c in RoutingCircuitState, where: c.pool_id == ^setup.pool.id))
    end
  end

  for mode <- ["full", "lite"], route <- ["/backend-api/codex/responses", "/v1/responses"], kind <- ["error", "response.failed"] do
    @mode mode
    @route route
    @kind kind
    test "#{mode} #{route} SSE #{@kind} keeps Flex terminal and health neutral", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.sse_stream([flex_event(@kind)], done: false))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, @mode)
      conn = conn |> auth(setup) |> then(&if @mode == "lite", do: put_req_header(&1, "x-openai-internal-codex-responses-lite", "true"), else: &1)
      conn = post(conn, @route, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic input"), "stream" => true})
      assert conn.status == 200
      assert conn.resp_body =~ "flex_unavailable"
      assert FakeUpstream.count(upstream) == 1
      assert_settled_flex!(setup)
    end
  end

  for mode <- ["full", "lite"], owner? <- [false, true], public? <- [false, true], kind <- ["error", "response.failed"] do
    @mode mode
    @owner owner?
    @public public?
    @kind kind
    test "#{mode} websocket owner=#{owner?} public=#{public?} #{@kind} keeps Flex terminal and health neutral" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, @owner)
      upstream = start_upstream(FakeUpstream.strict_sequence([strict_native_request(1, FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(flex_event(@kind))]))]))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, @mode)
      {:ok, auth} = CodexPooler.Access.authenticate_authorization_header(setup.authorization)
      {:ok, state} = CodexPoolerWeb.CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: Ecto.UUID.generate(), accepted_turn_state: Ecto.UUID.generate(), client_ip: "127.0.0.1", public_openai_responses_stream: @public, openai_compatibility_origin: if(@public, do: {"/v1/responses", "/backend-api/codex/responses"}, else: nil)}})

      try do
        payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic input"), "stream" => true, "generate" => true})
        assert {:ok, turn_state} = CodexPoolerWeb.CodexResponsesSocket.handle_in({payload, [opcode: :text]}, state)
        {turn_state, frames} = collect_native_turn_frames!(turn_state)
        terminal = assert_single_native_turn_terminal!(frames, "response.failed")
        assert terminal["response"]["error"]["code"] == "flex_unavailable"
        assert :ok = FakeUpstream.verify!(upstream)
        assert_settled_flex!(setup)
        assert :ok = CodexPoolerWeb.CodexResponsesSocket.terminate(:closed, turn_state)
      after
        CodexPoolerWeb.CodexResponsesSocket.terminate(:closed, state)
      end
    end
  end

  defp flex_event("error"), do: %{"type" => "error", "status" => 429, "error" => %{"code" => "flex_unavailable", "message" => "Flex capacity unavailable."}}
  defp flex_event("response.failed"), do: %{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "flex_unavailable", "message" => "Flex capacity unavailable."}}}

  defp assert_settled_flex!(setup) do
    assert [%Request{id: id, status: "failed", last_error_code: "flex_unavailable"}] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [%Attempt{status: "failed"}] = Repo.all(from(a in Attempt, where: a.request_id == ^id))
    refute Repo.exists?(from(d in BridgeDemotion, where: d.pool_id == ^setup.pool.id))
    refute Repo.exists?(from(c in RoutingCircuitState, where: c.pool_id == ^setup.pool.id))
  end
end
