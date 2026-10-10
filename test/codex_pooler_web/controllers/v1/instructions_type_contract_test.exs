defmodule CodexPoolerWeb.V1.InstructionsTypeContractTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [curl_json_request!: 4, gateway_setup: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  for mode <- ~w(full lite), surface <- [:responses, :chat_messages, :chat_fallback], transport <- [:json, :sse, :websocket], transport != :websocket or surface == :responses do
    @tag :instructions_type_contract
    test "#{mode} #{surface} #{transport} refuses malformed instructions before effects" do
      terminal = %{"type" => "response.completed", "response" => %{"id" => "resp_instructions_contract", "object" => "response", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}}
      upstream_mode = if unquote(transport) == :websocket, do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(terminal)]), else: FakeUpstream.sse_stream([terminal])
      upstream = start_upstream(upstream_mode)
      setup = gateway_setup(upstream)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: unquote(mode), created_at: now, updated_at: now})
      port = start_public_endpoint!()

      for lifted? <- [false, true], instructions <- [1, 1.5, true, %{}, [], [%{"role" => "developer", "content" => "synthetic"}]] do
        input = if(lifted?, do: [%{"role" => "system", "content" => "synthetic system"}, %{"role" => "developer", "content" => "synthetic developer"}], else: []) ++ [%{"role" => "user", "content" => "synthetic user"}]
        input_field = if unquote(surface) == :chat_messages, do: "messages", else: "input"
        path = if unquote(surface) == :responses, do: "/v1/responses", else: "/v1/chat/completions"
        payload = %{"model" => setup.model.exposed_model_id, input_field => input, "instructions" => instructions, "stream" => unquote(transport) != :json}

        error =
          if unquote(transport) == :websocket do
            {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "instructions-contract-#{System.unique_integer([:positive])}", path, [{"openai-beta", "responses_websockets=2026-02-06"}])

            try do
              {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.put(payload, "type", "response.create")))
              {_conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
              assert %{"type" => "error", "status" => 400, "error" => error} = CodexPooler.JSON.decode!(frame)
              error
            after
              Mint.HTTP.close(conn)
            end
          else
            {headers, body} = curl_json_request!(port, setup.authorization, payload, path)
            assert String.starts_with?(headers, "HTTP/1.1 400")
            CodexPooler.JSON.decode!(body)["error"]
          end

        assert error == %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "instructions", "message" => "instructions must be a string or null"}
        assert FakeUpstream.count(upstream) == 0
        assert Repo.aggregate(Request, :count) == 0
        assert Repo.aggregate(Attempt, :count) == 0
        assert Repo.aggregate(LedgerEntry, :count) == 0
      end
    end
  end
end
