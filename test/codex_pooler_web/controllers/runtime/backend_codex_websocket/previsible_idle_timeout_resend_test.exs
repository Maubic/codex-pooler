defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.PrevisibleIdleTimeoutResendTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, public_websocket_connect!: 3, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo

  @timeout_ms 15_000

  for forwarding <- [true, false], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, mode: mode
    @tag slow: "real receive timeout followed by a new socket's identical tool-result resend"
    test "a previsible receive timeout chains its identical resend (#{forwarding}, #{mode})", %{forwarding: forwarding, mode: mode} do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding)
      previous = CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
      Application.put_env(:codex_pooler, OperationalSettings, previous |> Keyword.put(:settings, %OperationalSettings{upstream_receive_timeout_ms: 150}) |> Keyword.put(:use_instance_settings?, false))

      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              respond:
                FakeUpstream.websocket_text_frames([
                  CodexPooler.JSON.encode!(%{"type" => "codex.response.metadata"}),
                  CodexPooler.JSON.encode!(%{"type" => "codex.rate_limits"})
                ])
            ),
            FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: FakeUpstream.websocket_text_frames([completed_frame("resp_idle_resend")]))
          ])
        )

      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
      port = start_public_endpoint!()
      session = Ecto.UUID.generate()
      payload = CodexPooler.JSON.encode!(native_turn_payload(Ecto.UUID.generate(), setup.model.exposed_model_id))

      assert %{"type" => "error"} = send_and_receive_terminal!(port, setup, session, payload)
      :ok = await_all_settled!(setup.pool.id, System.monotonic_time(:millisecond) + @timeout_ms)
      assert [%Request{id: id, status: "failed", last_error_code: "stream_idle_timeout", correlation_id: claim}] = pool_requests(setup.pool.id)
      assert String.starts_with?(claim, "codex-request:")
      assert %CodexTurn{status: "failed", first_visible_output_at: nil} = Repo.get_by!(CodexTurn, request_id: id)
      attempt = Repo.get_by!(Attempt, request_id: id)
      assert %{"phase" => "receive_timeout", "termination_source" => "pooler_receive_timeout", "pre_visible_output" => true, "upstream_committed" => true, "terminal_seen" => false, "terminal_candidate_seen" => false, "text_frame_count" => 2} = attempt.response_metadata["transport_failure"]

      assert %{"type" => "response.completed"} = send_and_receive_terminal!(port, setup, session, payload)
      :ok = await_all_settled!(setup.pool.id, System.monotonic_time(:millisecond) + @timeout_ms)
      assert [%Request{id: ^id}, %Request{id: successor_id, status: "succeeded"}] = pool_requests(setup.pool.id)
      assert [%RequestClientRetryLink{predecessor_request_id: ^id, successor_request_id: ^successor_id}] = Repo.all(RequestClientRetryLink)
      assert FakeUpstream.count(upstream) == 2
    end
  end

  defp completed_frame(response_id) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.completed",
      "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}
    })
  end

  defp send_and_receive_terminal!(port, setup, turn_state, raw_payload) do
    {conn, websocket, ref} = public_websocket_connect!(port, setup, turn_state)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, raw_payload)
    {conn, frame} = receive_terminal!(conn, websocket, ref)
    _closed = Mint.HTTP.close(conn)
    frame
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, frame},
      else: receive_terminal!(conn, websocket, ref)
  end

  defp native_turn_payload(thread_id, model) do
    %{
      "type" => "response.create",
      "model" => model,
      "instructions" => "synthetic base instructions",
      "stream" => true,
      "store" => false,
      "client_metadata" => %{
        "session_id" => thread_id,
        "thread_id" => thread_id,
        "turn_id" => "undelivered-turn",
        "x-codex-window-id" => thread_id <> ":0",
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => "undelivered-turn", "request_kind" => "turn"}),
        "x-codex-ws-stream-request-start-ms" => 100
      },
      "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic turn"}]}, %{"type" => "function_call_output", "call_id" => "synthetic-call", "output" => "synthetic result"}]
    }
  end

  defp pool_requests(pool_id), do: Repo.all(from(r in Request, where: r.pool_id == ^pool_id, order_by: [asc: r.admitted_at]))

  defp await_all_settled!(pool_id, deadline_ms) do
    requests = Repo.all(from(r in Request, where: r.pool_id == ^pool_id))

    cond do
      requests != [] and Enum.all?(requests, &(&1.status != "in_progress")) -> :ok
      System.monotonic_time(:millisecond) >= deadline_ms -> flunk("requests never settled")
      true -> Process.sleep(20) && await_all_settled!(pool_id, deadline_ms)
    end
  end
end
