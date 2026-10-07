defmodule CodexPoolerWeb.Runtime.BackendCodexMalformedReasoningTest do
  # A native request whose `reasoning` is not an object (`"reasoning": "high"`). The request reservation reads the
  # payload's reasoning effort for its settings snapshot before dispatch, and that read raised on a string, so the
  # native HTTP route answered 500 and the native websocket ended the turn `websocket_response_task_failed` without
  # reaching the provider (findings#339). The native envelope always carries a `reasoning` object, so a value that is
  # not one is replaced by an empty object before dispatch: the turn now reaches the provider with no effort, the
  # provider serves it at its default effort, and the request records no effort. Full and Lite, HTTP and websocket.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, native_text_input: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [assert_single_native_turn_terminal!: 2, collect_native_turn_frames!: 1, completed_response_events: 4, completed_response_frames: 4]

  alias CodexPooler.Access
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "native HTTP #{mode}: a string reasoning is served with no effort instead of a 500", %{conn: conn, mode: mode} do
      # provenance: synthetic_adversarial (a completed turn; the request under test is the client's malformed field)
      upstream = start_upstream(FakeUpstream.sse_stream(completed_response_events("resp_malformed_reasoning_http", [], 3, 2)))
      setup = serve!(gateway_setup(upstream), mode)

      response =
        conn
        |> auth(setup)
        |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => native_text_input("malformed reasoning"), "stream" => true, "reasoning" => "high"})

      assert response.status == 200
      assert response.resp_body =~ "response.completed"
      assert [%{method: "POST", json: %{"reasoning" => reasoning}}] = FakeUpstream.requests(upstream)
      assert is_map(reasoning) and not Map.has_key?(reasoning, "effort")

      assert [request] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
      assert request.status == "succeeded"
      assert request.reasoning_effort == nil
    end

    @tag mode: mode
    test "native websocket #{mode}: a string reasoning is served with no effort instead of a failed task", %{mode: mode} do
      # provenance: synthetic_adversarial (a completed turn; the request under test is the client's malformed field)
      upstream = start_upstream(FakeUpstream.repeat_last([completed_response_frames("resp_malformed_reasoning_ws", [], 3, 2)]))
      setup = serve!(gateway_setup(upstream), mode)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, state} = CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: "native-malformed-reasoning-#{mode}", accepted_turn_state: Ecto.UUID.generate(), client_ip: "127.0.0.1"}})

      try do
        payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("malformed reasoning"), "stream" => true, "generate" => true, "reasoning" => "high"})
        assert {:ok, state} = CodexResponsesSocket.handle_in({payload, [opcode: :text]}, state)
        {state, frames} = collect_native_turn_frames!(state)

        assert %{"type" => "response.completed"} = assert_single_native_turn_terminal!(frames, "response.completed")
        assert [%{method: "WEBSOCKET"}] = FakeUpstream.requests(upstream)

        assert [request] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
        assert request.status == "succeeded"
        assert request.reasoning_effort == nil
        assert :ok = CodexResponsesSocket.terminate(:closed, state)
      after
        CodexResponsesSocket.terminate(:closed, state)
      end
    end
  end

  defp serve!(setup, mode) do
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode})
    setup
  end
end
