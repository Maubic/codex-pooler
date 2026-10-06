defmodule CodexPoolerWeb.V1.ResponsesWebsocketHistoryItemsTest do
  # The public `GET /v1/responses` websocket coerces its `response.create` frame through the same adapter as the HTTP
  # routes, so the history items `/v1` admits reach the provider over the websocket too: a multi-agent mailbox
  # `agent_message` (plaintext, and the sealed `NEW_TASK` handoff the native websocket route keeps while it filters every
  # other encrypted `agent_message`) and a replayed hosted `web_search_call`. A malformed one is answered one `error`
  # event before any upstream frame. Covered directly and through a local owner, for an Auto, a Lite and a Full Pool.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      await_public_websocket_upgrade: 2,
      decode_public_websocket_data!: 2,
      gateway_setup: 1,
      mint_websocket_new!: 4,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.FakeUpstream

  @moduletag capture_log: true
  @frame_timeout_ms 15_000
  @response_id "resp_public_ws_history_items"
  @cipher "gAAAAA-synthetic-cipher-0001"
  @query "synthetic private search query"

  for mode <- ["auto", "lite", "full"], topology <- [:direct, :local_owner] do
    @tag :v1_websocket
    @tag mode: mode, topology: topology
    test "#{mode} serving, #{topology}: agent_message and web_search_call history items reach the provider untouched", %{mode: mode, topology: topology} do
      if topology == :local_owner, do: enable_owner_forwarding!()

      upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(upstream_events(), &CodexPooler.JSON.encode!/1)))
      setup = gateway_setup(upstream)
      if mode != "auto", do: set_model_serving_mode!(model_serving_scope(), setup, mode)

      history = [user("synthetic task"), plaintext(), sealed(), web_search_call(), assistant("synthetic answer"), user("synthetic follow-up")]
      {texts, _port} = turn!(setup, topology, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => history, "stream" => true})

      events = Enum.map(texts, &CodexPooler.JSON.decode!/1)
      assert List.last(events)["type"] == "response.completed"
      refute Enum.any?(events, &(&1["type"] == "error"))

      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.method == "WEBSOCKET"
      forwarded = captured.json["input"]
      assert Enum.filter(forwarded, &(&1["type"] in ["agent_message", "web_search_call"])) == [plaintext(), sealed(), web_search_call()]
      assert Enum.map(forwarded, & &1["type"]) -- ["additional_tools"] == ["message", "agent_message", "agent_message", "web_search_call", "message", "message"]
    end
  end

  for topology <- [:direct, :local_owner] do
    @tag :v1_websocket
    @tag topology: topology
    test "#{topology}: a malformed history item is answered one error event before any upstream frame", %{topology: topology} do
      if topology == :local_owner, do: enable_owner_forwarding!()

      upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(upstream_events(), &CodexPooler.JSON.encode!/1)))
      setup = gateway_setup(upstream)

      for bad <- [Map.put(plaintext(), "status", "completed"), put_in(web_search_call(), ["action", "queries"], nil)] do
        {texts, _port} = turn!(setup, topology, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => [user("synthetic task"), bad], "stream" => true})

        assert [%{"type" => "error", "status" => 400, "error" => %{"code" => "invalid_request", "param" => "input", "message" => "input item shape is not translatable"}}] = Enum.map(texts, &CodexPooler.JSON.decode!/1)
      end

      assert FakeUpstream.count(upstream) == 0
    end
  end

  defp turn!(setup, topology, frame) do
    port = start_public_endpoint!()
    {conn, websocket, ref} = public_v1_websocket_connect!(port, setup, topology)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
      {_conn, texts} = receive_until_terminal!(conn, websocket, ref, [])
      {texts, port}
    after
      Mint.HTTP.close(conn)
    end
  end

  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp assistant(text), do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}

  defp plaintext do
    %{
      "type" => "agent_message",
      "id" => "amsg_synthetic_0001",
      "author" => "/root/worker",
      "recipient" => "/root",
      "content" => [%{"type" => "input_text", "text" => "Message Type: FINAL_ANSWER\nTask name: /root\nSender: /root/worker\nPayload:\nsynthetic worker answer"}]
    }
  end

  defp sealed do
    %{
      "type" => "agent_message",
      "id" => "amsg_synthetic_0002",
      "author" => "/root",
      "recipient" => "/root/worker",
      "content" => [
        %{"type" => "input_text", "text" => "Message Type: NEW_TASK\nTask name: /root/worker\nSender: /root\nPayload:\n"},
        %{"type" => "encrypted_content", "encrypted_content" => @cipher}
      ]
    }
  end

  defp web_search_call do
    %{"id" => "ws_synthetic_0001", "type" => "web_search_call", "status" => "completed", "action" => %{"type" => "search", "query" => @query, "queries" => [@query]}}
  end

  defp upstream_events do
    item = %{"id" => "msg_public_ws_history_items", "type" => "message", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic reply", "annotations" => []}]}

    [
      %{"type" => "response.created", "response" => response_body("in_progress", [])},
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
      %{"type" => "response.completed", "response" => response_body("completed", [item])}
    ]
  end

  defp response_body(status, output) do
    %{
      "id" => @response_id,
      "object" => "response",
      "created_at" => 1_790_000_000,
      "model" => "provider-gpt-test-model",
      "status" => status,
      "output" => output,
      "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
    }
  end

  defp receive_until_terminal!(conn, websocket, ref, texts) do
    receive do
      message ->
        case Mint.WebSocket.stream(conn, message) do
          {:ok, conn, responses} ->
            {websocket, new_texts} = decode_texts(websocket, ref, responses)
            texts = texts ++ new_texts

            if Enum.any?(new_texts, &terminal_text?/1),
              do: {conn, texts},
              else: receive_until_terminal!(conn, websocket, ref, texts)

          {:error, _conn, reason, _responses} ->
            flunk("websocket receive failed: #{inspect(reason)}")

          :unknown ->
            receive_until_terminal!(conn, websocket, ref, texts)
        end
    after
      @frame_timeout_ms -> flunk("timed out waiting for the public terminal; received #{length(texts)} frames")
    end
  end

  defp decode_texts(websocket, ref, responses) do
    Enum.reduce(responses, {websocket, []}, fn
      {:data, ^ref, data}, {websocket, acc} ->
        case decode_public_websocket_data!(websocket, data) do
          {:ok, websocket, texts} -> {websocket, acc ++ texts}
          {:cont, websocket} -> {websocket, acc}
        end

      _part, acc ->
        acc
    end)
  end

  defp terminal_text?(text) do
    match?({:ok, %{"type" => type}} when type in ["response.completed", "response.failed", "response.incomplete", "error"], CodexPooler.JSON.decode(text))
  end

  defp public_v1_websocket_connect!(port, setup, topology) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    turn_state = "public-ws-history-items-#{topology}-#{System.unique_integer([:positive])}"

    headers = [
      {"authorization", setup.authorization},
      {"x-codex-turn-state", turn_state},
      {"openai-beta", "responses_websockets=2026-02-06"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end

  defp enable_owner_forwarding! do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
      end
    end)
  end
end
