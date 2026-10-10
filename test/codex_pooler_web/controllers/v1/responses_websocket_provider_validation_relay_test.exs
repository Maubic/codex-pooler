defmodule CodexPoolerWeb.V1.ResponsesWebsocketProviderValidationRelayTest do
  # The Codex backend answers a request it refuses at validation over HTTP with
  # `400 {"error": {"type": "invalid_request_error", "code": ..., "param": ...,
  # "message": ...}}`, and on the websocket with a wrapped error frame that
  # carries the HTTP message as text and no code and no param. The text comes in
  # two wordings, chosen per connection (direct probe 2026-10-07, findings#336,
  # gpt-6-luna, Full and Lite, 3 samples; both wordings for the same fault):
  #
  #   * `Invalid response.create payload: <HTTP message>`; the provider keeps the
  #     connection open;
  #   * `[<Schema>] [<path>] [<kind>] <HTTP message>`; the provider sends a Close
  #     frame (1000) within milliseconds.
  #
  # The frames below are the captured ones (synthetic request values only), each
  # paired with the HTTP body the same fault got. The HTTP answer relays code and
  # param; the websocket relayed the redacted `upstream_status` on `/v1` and
  # `invalid_request` without a param on the native route. Each create now gets
  # the HTTP answer's error. The one fact a frame cannot carry is the field of an
  # `Invalid value: ...` message in the payload wording, which names none, so that
  # relay has code and supported values and a null param.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      auth: 2,
      await_public_websocket_upgrade: 2,
      decode_public_websocket_data!: 2,
      gateway_setup: 1,
      mint_websocket_new!: 4,
      native_text_input: 1,
      public_websocket_connect_with_request_headers!: 5,
      public_websocket_receive_text!: 3,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [assert_single_native_turn_terminal!: 2, await_socket_connection_state!: 2, collect_native_turn_frames!: 1, socket_transport_barrier!: 3]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario
  alias CodexPoolerWeb.Runtime.V1BridgedAnchorSupport
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @frame_timeout_ms 15_000
  @effort_values "Supported values are: 'none', 'minimal', 'low', 'medium', 'high', 'xhigh', and 'max'."
  @effort_message "Invalid value: 'zz_probe_effort'. " <> @effort_values
  @effort_relayed_values "none, minimal, low, medium, high, xhigh, max"

  # `ws_differs` is what the websocket relay does not share with the HTTP answer of the same fault.
  @shapes [
    %{
      name: "invalid value in the payload wording",
      mode: "full",
      wording: :payload,
      http_error: %{"message" => @effort_message, "type" => "invalid_request_error", "param" => "reasoning.effort", "code" => "invalid_value"},
      ws_message: "Invalid response.create payload: " <> @effort_message,
      http_relayed: %{
        "type" => "invalid_request_error",
        "code" => "invalid_value",
        "param" => "reasoning.effort",
        "message" => "upstream rejected parameter reasoning.effort (invalid_value); supported values: " <> @effort_relayed_values
      },
      ws_differs: %{
        "param" => nil,
        "message" => "upstream rejected the request (invalid_value); supported values: " <> @effort_relayed_values
      },
      recorded: %{class: "invalid_value", param: nil}
    },
    %{
      name: "invalid enum value in the bracket wording",
      mode: "full",
      wording: :bracket,
      http_error: %{"message" => @effort_message, "type" => "invalid_request_error", "param" => "reasoning.effort", "code" => "invalid_value"},
      ws_message: "[ReasoningEffortParam] [reasoning.effort] [invalid_enum_value] " <> @effort_message,
      http_relayed: %{
        "type" => "invalid_request_error",
        "code" => "invalid_value",
        "param" => "reasoning.effort",
        "message" => "upstream rejected parameter reasoning.effort (invalid_value); supported values: " <> @effort_relayed_values
      },
      ws_differs: %{},
      recorded: %{class: "invalid_value", param: "reasoning.effort"}
    },
    %{
      name: "invalid type in the payload wording",
      mode: "full",
      wording: :payload,
      http_error: %{
        "message" => "Invalid type for 'parallel_tool_calls': expected a boolean, but got a string instead.",
        "type" => "invalid_request_error",
        "param" => "parallel_tool_calls",
        "code" => "invalid_type"
      },
      ws_message: "Invalid response.create payload: Invalid type for 'parallel_tool_calls': expected a boolean, but got a string instead.",
      http_relayed: %{
        "type" => "invalid_request_error",
        "code" => "invalid_type",
        "param" => "parallel_tool_calls",
        "message" => "upstream rejected parameter parallel_tool_calls (invalid_type)"
      },
      ws_differs: %{},
      recorded: %{class: "invalid_type", param: "parallel_tool_calls"}
    },
    %{
      name: "unknown parameter in the bracket wording",
      mode: "full",
      wording: :bracket,
      http_error: %{
        "message" => "Unknown parameter: 'tools[0].zz_probe_unknown_key'.",
        "type" => "invalid_request_error",
        "param" => "tools[0].zz_probe_unknown_key",
        "code" => "unknown_parameter"
      },
      ws_message: "[ObjectParam] [tools[0].zz_probe_unknown_key] [unknown_parameter] Unknown parameter: 'tools[0].zz_probe_unknown_key'.",
      http_relayed: %{
        "type" => "invalid_request_error",
        "code" => "unknown_parameter",
        "param" => "tools[0].zz_probe_unknown_key",
        "message" => "upstream rejected parameter tools[0].zz_probe_unknown_key (unknown_parameter)"
      },
      ws_differs: %{},
      recorded: %{class: "unknown_parameter", param: "tools[0].zz_probe_unknown_key"}
    },
    %{
      name: "missing required parameter in the payload wording, Lite manifest",
      mode: "lite",
      wording: :payload,
      http_error: %{
        "message" => "Missing required parameter: 'input[0].tools[0].name'.",
        "type" => "invalid_request_error",
        "param" => "input[0].tools[0].name",
        "code" => "missing_required_parameter"
      },
      ws_message: "Invalid response.create payload: Missing required parameter: 'input[0].tools[0].name'.",
      # Lite puts the tool manifest in front of the client's input, so index 0 is not an item the client sent.
      http_relayed: %{
        "type" => "invalid_request_error",
        "code" => "missing_required_parameter",
        "param" => "input[].tools[0].name",
        "message" => "upstream rejected parameter input[].tools[0].name (missing_required_parameter)"
      },
      ws_differs: %{},
      recorded: %{class: "missing_required_parameter", param: "input[0].tools[0].name"}
    },
    %{
      name: "string too long in the bracket wording",
      mode: "full",
      wording: :bracket,
      http_error: %{
        "message" => "Invalid 'tools[0].name': string too long. Expected a string with maximum length 128, but got a string with length 200 instead.",
        "type" => "invalid_request_error",
        "param" => "tools[0].name",
        "code" => "string_above_max_length"
      },
      ws_message: "[StringParam] [tools[0].name] [string_above_max_length] Invalid 'tools[0].name': string too long. Expected a string with maximum length 128, but got a string with length 200 instead.",
      http_relayed: %{
        "type" => "invalid_request_error",
        "code" => "string_above_max_length",
        "param" => "tools[0].name",
        "message" => "upstream rejected parameter tools[0].name (string_above_max_length)"
      },
      ws_differs: %{},
      recorded: %{class: "string_above_max_length", param: "tools[0].name"}
    }
  ]

  for shape <- @shapes do
    test "HTTP answer of the same fault: #{shape.name}", %{conn: conn} do
      shape = unquote(Macro.escape(shape))
      assert http_answer!(conn, shape) == shape.http_relayed
    end

    for topology <- [:direct, :local_owner] do
      @tag topology: topology
      test "the #{topology} public websocket relays the refusal as the HTTP answer does: #{shape.name}", %{conn: conn, topology: topology} do
        shape = unquote(Macro.escape(shape))
        http_error = http_answer!(conn, shape)
        assert http_error == shape.http_relayed
        expected = Map.merge(http_error, shape.ws_differs)

        if topology == :local_owner, do: enable_owner_forwarding!()
        upstream = start_upstream(FakeUpstream.repeat_last([upstream_mode(shape)]))
        setup = serve!(gateway_setup(upstream), shape.mode)
        client = connect!(setup)

        try do
          client = send_create!(client, setup, "s1")
          {_client, [text]} = receive_terminals!(client, 1)

          assert %{"type" => "error", "status" => 400, "stream_id" => "s1", "error" => ^expected} = CodexPooler.JSON.decode!(text)
          assert_recorded!(setup, shape)
        after
          Mint.HTTP.close(client.conn)
        end
      end

      @tag topology: topology
      test "the #{topology} native websocket relays the refusal as the HTTP answer does: #{shape.name}", %{conn: conn, topology: topology} do
        shape = unquote(Macro.escape(shape))
        http_error = http_answer!(conn, shape)
        assert http_error == shape.http_relayed
        expected = Map.merge(http_error, shape.ws_differs)

        if topology == :local_owner, do: enable_owner_forwarding!()
        upstream = start_upstream(FakeUpstream.repeat_last([upstream_mode(shape)]))
        setup = serve!(gateway_setup(upstream), shape.mode)
        {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
        {:ok, state} = CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: "native-validation-relay-#{topology}", accepted_turn_state: Ecto.UUID.generate(), client_ip: "127.0.0.1"}})

        try do
          {state, frames} = native_turn!(state, setup, "refused native turn")

          assert assert_single_native_turn_terminal!(frames, "error") == %{"type" => "error", "status" => 400, "error" => expected}
          assert_recorded!(setup, shape)
          assert :ok = CodexResponsesSocket.terminate(:closed, state)
        after
          CodexResponsesSocket.terminate(:closed, state)
        end
      end
    end

    test "a streaming /v1 request bridged onto the upstream websocket gets the answer of the same fault over the websocket: #{shape.name}", %{conn: conn} do
      shape = unquote(Macro.escape(shape))
      http_error = http_answer!(conn, shape)
      assert http_error == shape.http_relayed
      expected = Map.merge(http_error, shape.ws_differs)

      V1BridgedAnchorSupport.enable_bridge!()
      upstream = start_upstream(FakeUpstream.repeat_last([upstream_mode(shape)]))
      setup = serve!(gateway_setup(upstream), shape.mode)

      response =
        conn
        |> recycle()
        |> auth(setup)
        |> put_req_header("x-session-id", "validation-relay-bridge-#{System.unique_integer([:positive])}")
        |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic bridged create", "stream" => true})

      assert %{"error" => ^expected} = json_response(response, 400)
      assert [%{method: "WEBSOCKET"}] = FakeUpstream.requests(upstream)
    end
  end

  # `Unsupported tool type: <type>` is a detail text the provider sends over HTTP as `{"detail": ...}` and on the
  # websocket as a frame without a code, then it drops the connection (direct probe 2026-10-07, findings#336, Full:
  # `zz_probe_tool`, `programmatic_tool_calling`, `web_search_preview` all read alike). HTTP relays nothing structured
  # for it, so the websocket relays nothing more either; the type is kept where only diagnostics read it, on the
  # attempt, under the diagnostic taxonomy (a bounded identifier in cleartext, the fingerprint otherwise).
  @tool_type_refusal "Unsupported tool type: zz_probe_tool"

  test "an unsupported tool type over HTTP is recorded by class and type and never relayed", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.provider_refusal(@tool_type_refusal)]))
    setup = serve!(gateway_setup(upstream), "full")
    response = conn |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic refused create", "stream" => true})

    # The provider's text never reaches the client, whichever serving mode shapes the body.
    assert response.status == 400
    refute response.resp_body =~ "zz_probe_tool"
    assert_tool_type_recorded!(setup, "rejection_detail_class")
  end

  for topology <- [:direct, :local_owner] do
    @tag topology: topology
    test "the #{topology} public websocket answers an unsupported tool type as before and records its class and type", %{topology: topology} do
      if topology == :local_owner, do: enable_owner_forwarding!()
      upstream = start_upstream(FakeUpstream.repeat_last([FakeUpstream.provider_refusal(@tool_type_refusal)]))
      setup = serve!(gateway_setup(upstream), "full")
      client = connect!(setup)

      try do
        client = send_create!(client, setup, "s1")
        {_client, [text]} = receive_terminals!(client, 1)

        assert %{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "code" => "upstream_status", "message" => "upstream request failed"} = error} = CodexPooler.JSON.decode!(text)
        refute Map.has_key?(error, "param")
        refute text =~ "zz_probe_tool"
        assert_tool_type_recorded!(setup, "rejection_message_class")
      after
        Mint.HTTP.close(client.conn)
      end
    end

    @tag topology: topology
    test "the #{topology} native websocket answers an unsupported tool type as before and records its class and type", %{topology: topology} do
      if topology == :local_owner, do: enable_owner_forwarding!()
      upstream = start_upstream(FakeUpstream.repeat_last([FakeUpstream.provider_refusal(@tool_type_refusal)]))
      setup = serve!(gateway_setup(upstream), "full")
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, state} = CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: "native-tool-type-#{topology}", accepted_turn_state: Ecto.UUID.generate(), client_ip: "127.0.0.1"}})

      try do
        {state, frames} = native_turn!(state, setup, "refused native turn")

        assert assert_single_native_turn_terminal!(frames, "error") == %{
                 "type" => "error",
                 "status" => 400,
                 "error" => %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => nil, "message" => "upstream rejected the request (invalid_request)"}
               }

        assert_tool_type_recorded!(setup, "rejection_message_class")
        assert :ok = CodexResponsesSocket.terminate(:closed, state)
      after
        CodexResponsesSocket.terminate(:closed, state)
      end
    end
  end

  for topology <- [:direct, :local_owner] do
    @tag topology: topology
    test "the #{topology} public socket treats a steering-reserved provider code without a steer as an ordinary failed terminal", %{topology: topology} do
      assert_public_unsteered_native_lane_refusal!(topology)
    end
  end

  defp assert_public_unsteered_native_lane_refusal!(topology) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, topology == :local_owner)
    hold = make_ref()
    # Synthetic adversarial provider: ordinary public creates have no native
    # lane even if the provider returns the native control's reserved code.
    error = %{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "code" => "unsupported_native_inflight_message", "message" => "synthetic reserved provider failure sentinel"}}
    response = FakeUpstream.websocket_terminal_then_close_barrier(error, notify: self(), release_ref: hold, code: 1000, reason: "")
    # provenance: synthetic_adversarial
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)]))
    setup = serve!(gateway_setup(upstream), "full")
    port = start_public_endpoint!()
    before = WebsocketCleanupFence.listener_sockets()
    headers = [{"openai-beta", "responses_websockets=2026-02-06"}]
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/v1/responses", headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    client = send_create!(%{conn: conn, websocket: websocket, ref: ref}, setup, "ordinary-reserved-code")

    try do
      assert_receive {:fake_upstream_websocket_barrier, :before_terminal, handler, ^hold}, @frame_timeout_ms
      handler_monitor = Process.monitor(handler)
      active = await_socket_connection_state!(socket, &is_pid(Map.get(&1, :public_response_task_pid)))
      refute is_pid(Map.get(active, :native_response_steering))
      send(handler, {:fake_upstream_release_websocket, hold})
      assert_receive {:fake_upstream_websocket_barrier, :before_close, ^handler, ^hold}, @frame_timeout_ms
      send(handler, {:fake_upstream_release_websocket, hold})
      {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
      assert %{"type" => "error", "status" => 400, "stream_id" => "ordinary-reserved-code", "error" => %{"type" => "invalid_request_error", "code" => "upstream_status", "message" => "upstream request failed"}} = CodexPooler.JSON.decode!(text)
      refute text =~ "unsupported_native_inflight_message"
      refute text =~ "synthetic reserved provider failure sentinel"
      assert [request] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
      assert request.status == "failed"
      assert request.retry_count == 0
      assert request.usage_status == "usage_unknown"
      assert request.completed_at != nil
      assert %Attempt{status: "failed", completed_at: completed_at, response_metadata: metadata} = Repo.one!(from(attempt in Attempt, where: attempt.request_id == ^request.id))
      assert completed_at != nil
      assert metadata["rejection_error_code"] == "unsupported_native_inflight_message"
      idle = await_socket_connection_state!(socket, &(is_nil(Map.get(&1, :public_response_task_pid)) and MapSet.size(&1.tasks) == 0))
      refute is_pid(Map.get(idle, :native_response_steering))
      {_conn, _websocket} = socket_transport_barrier!(conn, websocket, client.ref)
      assert_receive {:DOWN, ^handler_monitor, :process, ^handler, _reason}, @frame_timeout_ms
      assert [%{websocket_connection_id: 1, json: %{"type" => "response.create"}}] = FakeUpstream.requests(upstream)
      assert FakeUpstream.physical_counts(upstream).websocket_generation == 1
      assert FakeUpstream.physical_counts(upstream).http_generation == 0
      assert FakeUpstream.websocket_steers(upstream) == []
      assert :ok = FakeUpstream.verify!(upstream)
    after
      Mint.HTTP.close(client.conn)
      assert :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
    end
  end

  defp assert_tool_type_recorded!(setup, class_key) do
    [request] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
    assert request.status == "failed"

    metadata = Repo.one!(from(attempt in Attempt, where: attempt.request_id == ^request.id)).response_metadata
    assert metadata[class_key] == "unsupported_tool_type"
    assert metadata["rejection_message_value"] == "zz_probe_tool"
    assert metadata["rejection_message_bytes"] == byte_size(@tool_type_refusal)
    refute Map.has_key?(metadata, "rejection_error_code")
    refute Map.has_key?(metadata, "rejection_error_param")
    refute inspect(metadata) =~ @tool_type_refusal

    assert Repo.aggregate(BridgeDemotion, :count) == 0
    assert Repo.aggregate(RoutingCircuitState, :count) == 0
  end

  # The HTTP answer of the same provider refusal on `/v1/responses` (upstream HTTP, no session).
  defp http_answer!(conn, shape) do
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.json_response(%{"error" => shape.http_error}, 400)]))
    setup = serve!(gateway_setup(upstream), shape.mode)
    response = conn |> recycle() |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic refused create", "stream" => true})
    %{"error" => error} = json_response(response, 400)
    FakeUpstream.verify!(upstream)
    error
  end

  # The provider's frame as captured, and what it did with the connection: the payload wording left it open, the
  # bracket wording was followed by a Close frame (1000).
  defp upstream_mode(%{wording: :payload} = shape), do: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(frame(shape))])
  defp upstream_mode(%{wording: :bracket} = shape), do: FakeUpstream.websocket_sse_then_close([frame(shape)], code: 1000, reason: "")

  defp frame(%{ws_message: message}),
    do: %{"type" => "error", "status" => 400, "error" => %{"code" => nil, "message" => message, "param" => nil, "type" => "invalid_request_error"}}

  # The refused turn settles as the client's error: no demotion, no circuit failure, and the attempt records the
  # rejection facts the frame states (the class names the code its text reads as; the provider sent no code).
  defp assert_recorded!(setup, shape) do
    [request] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
    assert request.status == "failed"

    attempt = Repo.one!(from(attempt in Attempt, where: attempt.request_id == ^request.id))
    metadata = attempt.response_metadata
    assert metadata["rejection_error_type"] == "invalid_request_error"
    assert metadata["rejection_message_class"] == shape.recorded.class
    assert Map.get(metadata, "rejection_error_param") == shape.recorded.param
    refute Map.has_key?(metadata, "rejection_error_code")

    assert Repo.aggregate(BridgeDemotion, :count) == 0
    assert Repo.aggregate(RoutingCircuitState, :count) == 0
  end

  defp native_turn!(state, setup, text) do
    payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input(text), "stream" => true, "generate" => true})
    assert {:ok, state} = CodexResponsesSocket.handle_in({payload, [opcode: :text]}, state)
    collect_native_turn_frames!(state)
  end

  defp serve!(setup, mode) do
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode})
    setup
  end

  defp connect!(setup) do
    port = start_public_endpoint!()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"openai-beta", "responses_websockets=2026-02-06"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    %{conn: conn, websocket: websocket, ref: ref}
  end

  defp send_create!(client, setup, stream_id) do
    frame = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => "synthetic create #{stream_id}", "stream_id" => stream_id})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  # The client's frames up to `count` terminal events, in arrival order.
  defp receive_terminals!(client, count), do: receive_terminals!(client, count, [])

  defp receive_terminals!(client, 0, terminals), do: {client, Enum.reverse(terminals)}

  defp receive_terminals!(client, count, terminals) do
    receive do
      message ->
        case Mint.WebSocket.stream(client.conn, message) do
          {:ok, conn, responses} ->
            {websocket, texts} =
              Enum.reduce(responses, {client.websocket, []}, fn
                {:data, ref, data}, {websocket, acc} when ref == client.ref ->
                  case decode_public_websocket_data!(websocket, data) do
                    {:ok, websocket, decoded} -> {websocket, acc ++ decoded}
                    {:cont, websocket} -> {websocket, acc}
                  end

                _part, acc ->
                  acc
              end)

            new_terminals = Enum.filter(texts, &terminal?/1)
            receive_terminals!(%{client | conn: conn, websocket: websocket}, max(count - length(new_terminals), 0), Enum.reverse(new_terminals) ++ terminals)

          {:error, _conn, reason, _responses} ->
            flunk("public websocket receive failed: #{inspect(reason)}")

          :unknown ->
            receive_terminals!(client, count, terminals)
        end
    after
      @frame_timeout_ms -> flunk("timed out waiting for #{count} more terminal events; received #{length(terminals)}")
    end
  end

  defp terminal?(text),
    do: match?({:ok, %{"type" => type}} when type in ["response.completed", "response.failed", "response.incomplete", "error"], CodexPooler.JSON.decode(text))

  defp enable_owner_forwarding! do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
  end
end
