defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.RetainedUserHistoryTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Access
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Websocket, as: Gateway

  @moduletag capture_log: true

  # Codex 0.158.0 local compaction preserves each retained user text part,
  # including empty parts. The gateway must not flatten the rebuilt history.
  for mode <- ["full", "lite"] do
    test "#{mode} forwards retained multipart user history unchanged on a fresh session" do
      mode = unquote(mode)
      terminal = %{"type" => "response.completed", "response" => %{"id" => "resp_retained_history", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 4, "output_tokens" => 1, "total_tokens" => 5}}}
      response = if mode == "full", do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(terminal)]), else: FakeUpstream.sse_stream([terminal])
      upstream = start_upstream(response)
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, mode)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, session} = Gateway.start_codex_session(auth, %{accepted_turn_state: "retained-history-#{mode}"})

      retained = %{
        "type" => "message",
        "role" => "user",
        "content" => [
          %{"type" => "input_text", "text" => "Synthetic code example:\n```sh\ninspect sample", "annotations" => [%{"type" => "synthetic", "label" => "first-part"}]},
          %{"type" => "input_text", "text" => ""},
          %{"type" => "input_text", "text" => "```\nOnly inspect the configuration."}
        ]
      }

      history = [retained, %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "Synthetic compacted summary"}]}, %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "Synthetic follow-up"}]}]

      assert :ok = execute_websocket_response(auth, CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => history, "stream" => true, "generate" => true}), %{request_id: "retained-history-#{mode}", codex_session: session}, fn _frame -> :ok end)

      assert [request] = FakeUpstream.requests(upstream)
      assert request.method == "WEBSOCKET"
      expected = if mode == "lite", do: [%{"type" => "additional_tools", "role" => "developer", "tools" => []} | history], else: history
      assert request.json["input"] == expected
      refute Map.has_key?(request.json, "previous_response_id")
      assert [accounting] = await_succeeded_pool_requests!(setup.pool.id, 1)
      assert accounting.request_metadata["routing"]["model_serving_mode"] == mode
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end
end
