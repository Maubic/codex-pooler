defmodule CodexPoolerWeb.V1.ResponsesConfigurationUpdateAsyncTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, public_websocket_connect!: 4, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [pool_owner_pids: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario

  @moduletag capture_log: true
  @parameters %{"type" => "object", "properties" => %{"key" => %{"type" => "string"}}, "required" => ["key"], "additionalProperties" => false}
  @function %{"type" => "function", "name" => "synthetic_lookup", "parameters" => @parameters, "strict" => true, "async" => true}
  @custom %{"type" => "custom", "name" => "synthetic_patch", "format" => %{"type" => "text"}, "async" => false}
  @member_function %{@function | "name" => "synthetic_member_lookup", "async" => false}
  @member_custom %{@custom | "name" => "synthetic_member_patch", "async" => true}
  @namespace %{"type" => "namespace", "name" => "synthetic", "description" => "Synthetic fixture namespace", "tools" => [@member_function, @member_custom]}
  @tools [@function, @custom, @namespace]
  @calls [
    %{"type" => "function_call", "id" => "fc_async_function_true", "call_id" => "call_async_function_true", "name" => "synthetic_lookup", "arguments" => ~s({"key":"synthetic-async-function-argument"}), "status" => "completed", "async" => true},
    %{"type" => "custom_tool_call", "id" => "ctc_async_custom_false", "call_id" => "call_async_custom_false", "name" => "synthetic_patch", "input" => "synthetic-async-custom-input", "status" => "completed", "async" => false},
    %{"type" => "function_call", "id" => "fc_async_member_false", "call_id" => "call_async_member_false", "name" => "synthetic_member_lookup", "namespace" => "synthetic", "arguments" => ~s({"key":"synthetic-async-member-argument"}), "status" => "completed", "async" => false},
    %{"type" => "custom_tool_call", "id" => "ctc_async_member_true", "call_id" => "call_async_member_true", "name" => "synthetic_member_patch", "namespace" => nil, "input" => "synthetic-async-member-input", "status" => "completed", "async" => true}
  ]

  @type transport :: :http_json | :http_sse | :bridged_sse | :websocket | :owner_websocket
  @type client :: %{conn: Mint.HTTP.t(), websocket: Mint.WebSocket.t(), ref: reference()}
  @type rejection :: :consecutive | :invalid_effort | :function | :custom | :namespace_function | :namespace_custom

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    :ok
  end

  describe "measured configuration updates and async calls cross the real public boundary" do
    for transport <- [:http_json, :http_sse, :bridged_sse, :websocket, :owner_websocket], mode <- ~w(full lite) do
      @tag transport: transport, serving_mode: mode
      test "#{transport}, #{mode}: preserves updates, returns client-owned calls and accepts their following replay", %{transport: transport, serving_mode: mode} do
        assert_preservation!(transport, mode)
      end
    end
  end

  describe "instruction messages keep configuration updates nonconsecutive at the provider boundary" do
    for transport <- [:http_json, :http_sse], mode <- ~w(full lite), role <- ~w(developer system) do
      @tag transport: transport, serving_mode: mode, separator_role: role
      test "#{transport}, #{mode}, #{role}: retains the physical separator and is not refused as consecutive", %{transport: transport, serving_mode: mode, separator_role: role} do
        assert_instruction_separator_preserved!(transport, mode, role)
      end
    end
  end

  describe "consecutive updates are the provider's policy, not Pooler's" do
    for transport <- [:http_json, :http_sse, :bridged_sse, :websocket, :owner_websocket], mode <- ~w(full lite) do
      @tag transport: transport, serving_mode: mode
      test "#{transport}, #{mode}: relays the provider's indexed unsupported_value and serves the next turn", %{transport: transport, serving_mode: mode} do
        assert_provider_refusal!(transport, mode, :consecutive)
      end
    end
  end

  describe "configuration effort values are validated by the provider" do
    for transport <- [:websocket, :owner_websocket], mode <- ~w(full lite) do
      @tag transport: transport, serving_mode: mode
      test "#{transport}, #{mode}: an arbitrary string reaches the provider and relays invalid_value with its vocabulary", %{transport: transport, serving_mode: mode} do
        assert_provider_refusal!(transport, mode, :invalid_effort)
      end
    end
  end

  describe "Lite async rollout refusal is an explicit provider answer" do
    for {transport, kind} <- [
          {:http_json, :function},
          {:http_sse, :custom},
          {:bridged_sse, :namespace_function},
          {:websocket, :namespace_custom},
          {:owner_websocket, :function}
        ] do
      @tag transport: transport, rejection: kind
      test "#{transport}, #{kind}: relays unsupported_value tools and does not refuse async:false", %{transport: transport, rejection: kind} do
        assert_provider_refusal!(transport, "lite", kind)
      end
    end

    for transport <- [:http_sse, :owner_websocket] do
      @tag transport: transport
      test "#{transport}, Full: the opt-in Lite refusal does not become a gateway async policy", %{transport: transport} do
        assert_full_with_lite_refusal_enabled!(transport)
      end
    end
  end

  describe "local shape refusals keep the client's exact coordinates before dispatch" do
    for {label, kind, code, param} <- [
          {"a content-only malformed update", :sdk_content, "missing_required_parameter", "input[1].reasoning"},
          {"null namespace custom async", :namespace_async, "invalid_type", "tools[1].tools[1].async"},
          {"non-boolean replayed custom call async", :call_async, "invalid_type", "input[2].async"},
          {"async on the namespace wrapper", :wrapper_async, "unknown_parameter", "tools[1].async"}
        ],
        transport <- [:http_json, :websocket] do
      @tag transport: transport, invalid_shape: kind, code: code, param: param
      test "#{transport}: refuses #{label} without a reservation or physical upstream request", %{transport: transport, invalid_shape: kind, code: code, param: param} do
        assert_local_refusal!(transport, kind, code, param)
      end
    end
  end

  @spec assert_preservation!(transport(), String.t()) :: :ok
  defp assert_preservation!(transport, mode) do
    configure_topology!(transport)
    first_id = "resp_configuration_async_opener"
    second_id = "resp_configuration_async_followup"
    upstream = start_upstream(two_turn_mode(transport, first_id, second_id))
    setup = serving_setup!(upstream, mode)
    session = "configuration-async-#{System.unique_integer([:positive])}"
    client = connect_client!(transport, setup, session)

    try do
      input = history()
      body = %{"input" => input, "tools" => @tools}
      {client, first} = successful_exchange!(client, transport, setup, body, session)
      calls = expected_public_calls()
      assert first.response["id"] == first_id
      assert first.response["output"] == calls
      assert Enum.map(first.response["output"], & &1["async"]) == [true, false, false, true]
      assert_public_call_events!(first.events, transport, calls)

      # The turn has settled with calls still awaiting the client's results. A completed tool-call item means the
      # model finished writing the call, never that Pooler executed it or dispatched another sampling turn.
      [request] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
      assert request.status == "succeeded"
      assert [captured] = FakeUpstream.requests(upstream)
      assert_forwarded!(captured, transport, mode, input, @tools)
      refute Enum.any?(captured.json["input"], &(&1["type"] in ["function_call_output", "custom_tool_call_output"]))

      results = client_results(calls)
      replay = input ++ calls ++ results ++ [update("medium"), user("synthetic configuration follow-up")]
      expected = input ++ Enum.map(calls, &Map.delete(&1, "status")) ++ results ++ [update("medium"), user("synthetic configuration follow-up")]
      extra = if websocket_upstream?(transport), do: %{"previous_response_id" => first_id}, else: %{}
      body = Map.merge(%{"input" => replay, "tools" => @tools}, extra)
      {_client, second} = successful_exchange!(client, transport, setup, body, session)
      assert second.response["id"] == second_id
      assert second.response["output"] == []

      assert [first_capture, replay_capture] = FakeUpstream.requests(upstream)
      assert_forwarded!(replay_capture, transport, mode, expected, @tools)
      assert Enum.filter(replay_capture.json["input"], &(&1["type"] in ["function_call_output", "custom_tool_call_output"])) == results
      assert_physical_turns!(upstream, transport, [first_capture, replay_capture])

      if websocket_upstream?(transport) do
        assert first_capture.websocket_connection_id == replay_capture.websocket_connection_id
        assert FakeUpstream.websocket_connection_count(upstream) == 1
      end

      if websocket_upstream?(transport), do: assert(replay_capture.json["previous_response_id"] == first_id)
      assert_successful_settlements!(setup, transport, 2)
      assert_owner_topology!(setup, transport)
      assert :ok = FakeUpstream.verify!(upstream)
      :ok
    after
      close_client(client)
    end
  end

  @spec assert_instruction_separator_preserved!(transport(), String.t(), String.t()) :: :ok
  defp assert_instruction_separator_preserved!(transport, mode, role) do
    configure_topology!(transport)
    upstream = start_upstream(recovery_mode(transport))
    setup = serving_setup!(upstream, mode)
    session = "configuration-separator-#{System.unique_integer([:positive])}"
    separator = %{"type" => "message", "role" => role, "content" => [%{"type" => "input_text", "text" => " synthetic first separator part "}, %{"type" => "input_text", "text" => "synthetic second separator part"}]}
    input = [update("high"), separator, update("low"), user("synthetic ordered follow-up")]
    expected = List.replace_at(input, 1, Map.put(separator, "role", "developer"))
    instructions = "Synthetic top-level instructions must stay separate"
    tools = disable_async([@function])
    body = %{"input" => input, "instructions" => instructions, "tools" => tools}
    expected_prefix = if mode == "lite", do: [%{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => instructions}]}], else: []

    # The fake applies its consecutive-update rule before consuming the successful turn.
    # Lifting away this message would manufacture adjacent updates and return a coded refusal.
    {_client, result} = successful_exchange!(nil, transport, setup, body, session)
    assert result.response["id"] == "resp_configuration_async_recovery"
    assert result.response["status"] == "completed"
    assert result.response["output"] == []
    assert [captured] = FakeUpstream.requests(upstream)
    assert_forwarded!(captured, transport, mode, expected, tools, expected_prefix)

    if mode == "lite",
      do: assert(captured.json["instructions"] in [nil, ""]),
      else: assert(captured.json["instructions"] == instructions)

    counts = FakeUpstream.physical_counts(upstream)
    assert counts.http_generation == 1
    assert counts.websocket_generation == 0
    assert counts.consume == 0
    assert_successful_settlements!(setup, transport, 1)
    assert_owner_topology!(setup, transport)
    assert :ok = FakeUpstream.verify!(upstream)
    :ok
  end

  @spec assert_provider_refusal!(transport(), String.t(), rejection()) :: :ok
  defp assert_provider_refusal!(transport, mode, kind) do
    configure_topology!(transport)
    upstream = start_upstream(recovery_mode(transport))
    if kind in [:function, :custom, :namespace_function, :namespace_custom], do: assert(:ok = FakeUpstream.refuse_lite_async_tools(upstream))
    setup = serving_setup!(upstream, mode)
    session = "configuration-async-refusal-#{System.unique_integer([:positive])}"
    client = connect_client!(transport, setup, session)

    try do
      {body, client_param} = refusal_body(kind)
      client_param = client_rejection_param(client_param, transport)
      code = if kind == :invalid_effort, do: "invalid_value", else: "unsupported_value"
      {client, error} = rejected_exchange!(client, transport, setup, body, session)
      assert %{"type" => "invalid_request_error", "code" => ^code, "param" => ^client_param} = error
      assert_effort_vocabulary!(error, kind)
      [failed] = OwnerCrashAfterSendScenario.await_settled!(setup, 1)
      assert failed.status == "failed"
      assert [rejected] = FakeUpstream.requests(upstream)
      assert_forwarded!(rejected, transport, mode, body["input"], body["tools"])

      provider_param = assert_refused_provider_item!(rejected, mode, kind)

      assert_rejection_settlement!(failed, transport, code, provider_param)

      # The fake consumes no success for a coded refusal. The same client connection submits a fresh valid turn,
      # proving that neither the relay nor the fake turns a provider-owned policy into a socket-wide local rule.
      recovery = %{"input" => [update("low"), user("synthetic refusal recovery")], "tools" => disable_async(body["tools"])}
      {_client, result} = successful_exchange!(client, transport, setup, recovery, session)
      assert result.response["id"] == "resp_configuration_async_recovery"
      assert [rejected_capture, recovered_capture] = FakeUpstream.requests(upstream)
      assert_forwarded!(recovered_capture, transport, mode, recovery["input"], recovery["tools"])
      assert_physical_turns!(upstream, transport, [rejected_capture, recovered_capture])
      [failed_again, recovered] = OwnerCrashAfterSendScenario.await_settled!(setup, 2)
      assert failed_again.id == failed.id
      assert_successful_settlement!(recovered, transport)
      assert_owner_topology!(setup, transport)
      assert_health_neutral!(setup)
      assert :ok = FakeUpstream.verify!(upstream)
      :ok
    after
      close_client(client)
    end
  end

  @spec assert_full_with_lite_refusal_enabled!(transport()) :: :ok
  defp assert_full_with_lite_refusal_enabled!(transport) do
    configure_topology!(transport)
    upstream = start_upstream(recovery_mode(transport))
    assert :ok = FakeUpstream.refuse_lite_async_tools(upstream)
    setup = serving_setup!(upstream, "full")
    session = "full-configuration-async-#{System.unique_integer([:positive])}"
    client = connect_client!(transport, setup, session)
    body = %{"input" => [update("high"), user("synthetic Full async request")], "tools" => [@function]}

    try do
      {_client, result} = successful_exchange!(client, transport, setup, body, session)
      assert result.response["status"] == "completed"
      assert [captured] = FakeUpstream.requests(upstream)
      assert_forwarded!(captured, transport, "full", body["input"], body["tools"])
      assert_successful_settlements!(setup, transport, 1)
      assert_owner_topology!(setup, transport)
      assert :ok = FakeUpstream.verify!(upstream)
      :ok
    after
      close_client(client)
    end
  end

  @spec assert_local_refusal!(transport(), atom(), String.t(), String.t()) :: :ok
  defp assert_local_refusal!(transport, kind, code, param) do
    upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_never_dispatched", [])))
    setup = serving_setup!(upstream, "full")
    session = "local-configuration-async-#{System.unique_integer([:positive])}"
    client = connect_client!(transport, setup, session)

    try do
      {_client, error} = rejected_exchange!(client, transport, setup, local_refusal_body(kind), session)
      assert %{"type" => "invalid_request_error", "code" => ^code, "param" => ^param} = error
      assert FakeUpstream.count(upstream) == 0
      assert FakeUpstream.physical_counts(upstream).http_generation == 0
      assert FakeUpstream.physical_counts(upstream).websocket_generation == 0
      refute Repo.exists?(from(request in Request, where: request.pool_id == ^setup.pool.id))
      refute Repo.exists?(from(entry in LedgerEntry, where: entry.pool_id == ^setup.pool.id))
      :ok
    after
      close_client(client)
    end
  end

  @spec configure_topology!(transport()) :: :ok
  defp configure_topology!(transport) do
    if transport in [:bridged_sse, :owner_websocket], do: Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  @spec serving_setup!(FakeUpstream.t(), String.t()) :: map()
  defp serving_setup!(upstream, mode) do
    setup = gateway_setup(upstream)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  @spec connect_client!(transport(), map(), String.t()) :: client() | nil
  defp connect_client!(transport, setup, session) when transport in [:websocket, :owner_websocket] do
    port = start_public_endpoint!()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, session, "/v1/responses")
    %{conn: conn, websocket: websocket, ref: ref}
  end

  defp connect_client!(_transport, _setup, _session), do: nil

  @spec close_client(client() | nil) :: term()
  defp close_client(nil), do: :ok
  defp close_client(client), do: Mint.HTTP.close(client.conn)

  @spec successful_exchange!(client() | nil, transport(), map(), map(), String.t()) :: {client() | nil, %{response: map(), events: [map()]}}
  defp successful_exchange!(client, transport, setup, body, _session) when transport in [:websocket, :owner_websocket] do
    client = send_create!(client, setup, body)
    {client, events} = receive_until_terminal!(client, [])
    assert List.last(events)["type"] == "response.completed", "unexpected terminal types: #{inspect(Enum.map(events, & &1["type"]))}"
    {client, %{response: List.last(events)["response"], events: events}}
  end

  defp successful_exchange!(client, transport, setup, body, session) do
    conn = post_responses!(transport, setup, body, session)

    if transport == :http_json do
      {client, %{response: json_response(conn, 200), events: []}}
    else
      assert [content_type] = get_resp_header(conn, "content-type")
      assert content_type =~ "text/event-stream"
      events = sse_events(response(conn, 200))
      assert List.last(events)["type"] == "response.completed"
      {client, %{response: List.last(events)["response"], events: events}}
    end
  end

  @spec rejected_exchange!(client() | nil, transport(), map(), map(), String.t()) :: {client() | nil, map()}
  defp rejected_exchange!(client, transport, setup, body, _session) when transport in [:websocket, :owner_websocket] do
    client = send_create!(client, setup, body)
    {client, events} = receive_until_terminal!(client, [])
    assert [%{"type" => "error", "status" => 400, "error" => error}] = events
    {client, error}
  end

  defp rejected_exchange!(client, transport, setup, body, session) do
    conn = post_responses!(transport, setup, body, session)
    {client, json_response(conn, 400)["error"]}
  end

  @spec post_responses!(transport(), map(), map(), String.t()) :: Plug.Conn.t()
  defp post_responses!(transport, setup, body, session) do
    conn = build_conn() |> auth(setup)
    conn = if transport == :bridged_sse, do: put_req_header(conn, "x-session-id", session), else: conn
    post(conn, "/v1/responses", Map.merge(body, %{"model" => setup.model.exposed_model_id, "stream" => transport != :http_json}))
  end

  @spec send_create!(client(), map(), map()) :: client()
  defp send_create!(client, setup, body) do
    frame = Map.merge(body, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "stream" => true})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, CodexPooler.JSON.encode!(frame))
    %{client | conn: conn, websocket: websocket}
  end

  @spec receive_until_terminal!(client(), [map()]) :: {client(), [map()]}
  defp receive_until_terminal!(client, events) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    client = %{client | conn: conn, websocket: websocket}
    event = CodexPooler.JSON.decode!(text)
    events = [event | events]

    if event["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {client, Enum.reverse(events)},
      else: receive_until_terminal!(client, events)
  end

  @spec sse_events(binary()) :: [map()]
  defp sse_events(body) do
    for block <- String.split(body, "\n\n", trim: true), "data: " <> data <- String.split(block, "\n"), data != "[DONE]", do: CodexPooler.JSON.decode!(data)
  end

  @spec two_turn_mode(transport(), String.t(), String.t()) :: FakeUpstream.mode()
  defp two_turn_mode(transport, first_id, second_id) do
    first = expect_turn(transport, turn_mode(transport, first_id, @calls), forbidden: ["previous_response_id"])
    continuation = if websocket_upstream?(transport), do: [equals: %{"previous_response_id" => first_id}], else: [forbidden: ["previous_response_id"]]
    second = expect_turn(transport, turn_mode(transport, second_id, []), continuation)
    # provenance: synthetic_adversarial; measured #343 call/update shapes, invented ids, content and two complete turns.
    FakeUpstream.strict_sequence([first, second])
  end

  @spec recovery_mode(transport()) :: FakeUpstream.mode()
  defp recovery_mode(transport) do
    mode = turn_mode(transport, "resp_configuration_async_recovery", [])
    # provenance: synthetic_adversarial; provider refusal consumes no success, followed by one invented valid turn.
    FakeUpstream.strict_sequence([FakeUpstream.expect_request(method: if(websocket_upstream?(transport), do: "WEBSOCKET", else: "POST"), path: "/backend-api/codex/responses", json: [valid: true, forbidden: ["previous_response_id"]], respond: mode)])
  end

  @spec expect_turn(transport(), FakeUpstream.mode(), keyword()) :: FakeUpstream.mode()
  defp expect_turn(transport, mode, json) do
    opts = [method: if(websocket_upstream?(transport), do: "WEBSOCKET", else: "POST"), path: "/backend-api/codex/responses", json: Keyword.put(json, :valid, true), respond: mode]
    opts = if websocket_upstream?(transport), do: Keyword.put(opts, :websocket_connection_ordinal, 1), else: opts
    FakeUpstream.expect_request(opts)
  end

  @spec turn_mode(transport(), String.t(), [map()]) :: FakeUpstream.mode()
  defp turn_mode(transport, id, items) do
    if websocket_upstream?(transport),
      do: FakeUpstream.websocket_text_frames(Enum.map(turn_payloads(id, items), &CodexPooler.JSON.encode!/1)),
      else: FakeUpstream.sse_stream(turn_events(id, items))
  end

  @spec turn_events(String.t(), [map()]) :: [{String.t(), map()}]
  defp turn_events(id, items), do: Enum.map(turn_payloads(id, items), &{&1["type"], &1})

  @spec turn_payloads(String.t(), [map()]) :: [map()]
  defp turn_payloads(id, items) do
    response = %{"id" => id, "object" => "response", "created_at" => 1_790_000_000, "model" => "provider-gpt-test-model", "status" => "completed", "output" => items, "usage" => %{"input_tokens" => 11, "output_tokens" => 7, "total_tokens" => 18}}

    item_events =
      items
      |> Enum.with_index()
      |> Enum.flat_map(fn {item, index} ->
        [%{"type" => "response.output_item.added", "output_index" => index, "item" => item}, %{"type" => "response.output_item.done", "output_index" => index, "item" => item}]
      end)

    [%{"type" => "response.created", "response" => %{response | "status" => "in_progress", "output" => [], "usage" => nil}}] ++ item_events ++ [%{"type" => "response.completed", "response" => response}]
  end

  @spec assert_public_call_events!([map()], transport(), [map()]) :: :ok
  defp assert_public_call_events!(_events, :http_json, _calls), do: :ok

  defp assert_public_call_events!(events, _transport, calls) do
    assert for(%{"type" => "response.output_item.added", "item" => item} <- events, do: item) == calls
    assert for(%{"type" => "response.output_item.done", "item" => item} <- events, do: item) == calls
    :ok
  end

  @spec assert_forwarded!(map(), transport(), String.t(), [map()], [map()]) :: :ok
  @spec assert_forwarded!(map(), transport(), String.t(), [map()], [map()], [map()]) :: :ok
  defp assert_forwarded!(captured, transport, mode, input, tools, expected_prefix \\ []) do
    assert captured.method == if(websocket_upstream?(transport), do: "WEBSOCKET", else: "POST")
    assert captured.path == "/backend-api/codex/responses"
    expected_input = expected_prefix ++ input

    if mode == "full" do
      assert captured.json["tools"] == tools
      assert captured.json["input"] == expected_input
    else
      refute Map.has_key?(captured.json, "tools")

      if captured.json["previous_response_id"] do
        # The Lite anchor already holds the opener's manifest. Replayed async calls and updates are the delta;
        # repeating that manifest would change the provider-held conversation instead of preserving it.
        assert captured.json["input"] == expected_input
      else
        assert [%{"type" => "additional_tools", "role" => "developer", "tools" => ^tools} | ^expected_input] = captured.json["input"]
      end
    end

    marker =
      if websocket_upstream?(transport),
        do: get_in(captured.json, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"]),
        else: Map.new(captured.headers)["x-openai-internal-codex-responses-lite"]

    assert marker == if(mode == "lite", do: "true")
    :ok
  end

  @spec assert_physical_turns!(FakeUpstream.t(), transport(), [map()]) :: :ok
  defp assert_physical_turns!(upstream, transport, captures) do
    assert length(captures) == 2
    counts = FakeUpstream.physical_counts(upstream)
    assert counts.http_generation == if(websocket_upstream?(transport), do: 0, else: 2)
    assert counts.websocket_generation == if(websocket_upstream?(transport), do: 2, else: 0)
    assert counts.consume == 0

    if websocket_upstream?(transport) do
      connection_ids = Enum.map(captures, & &1.websocket_connection_id)
      assert Enum.all?(connection_ids, &(is_integer(&1) and &1 > 0))
      assert FakeUpstream.websocket_connection_count(upstream) == length(Enum.uniq(connection_ids))
    end

    :ok
  end

  @spec assert_owner_topology!(map(), transport()) :: :ok
  defp assert_owner_topology!(setup, transport) do
    if transport in [:bridged_sse, :owner_websocket],
      do: assert([_owner | _rest] = pool_owner_pids(setup.pool)),
      else: assert(pool_owner_pids(setup.pool) == [])

    :ok
  end

  @spec assert_successful_settlements!(map(), transport(), pos_integer()) :: :ok
  defp assert_successful_settlements!(setup, transport, count) do
    requests = OwnerCrashAfterSendScenario.await_settled!(setup, count)
    Enum.each(requests, &assert_successful_settlement!(&1, transport))
    :ok
  end

  @spec assert_successful_settlement!(Request.t(), transport()) :: :ok
  defp assert_successful_settlement!(request, transport) do
    assert request.status == "succeeded"
    assert request.response_status_code == 200
    assert request.retry_count == 0
    assert is_nil(request.last_error_code)
    assert request.transport == request_transport(transport)
    assert [attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^request.id))
    assert attempt.status == "succeeded"
    assert attempt.transport == attempt_transport(transport)
    assert attempt.usage_status == "usage_known"
    assert is_nil(attempt.network_error_code)
    assert [settlement] = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id and entry.entry_kind == "settlement"))
    assert {settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} == {11, 7, 18}
    assert settlement.attempt_id == attempt.id
    assert_ledger_kinds!(request)
    persistence = inspect({request.request_metadata, attempt.response_metadata, settlement.details})

    for marker <- ["synthetic configuration opener", "synthetic configuration request", "synthetic-async-function-argument", "synthetic-async-custom-input", "synthetic-async-member-input", "synthetic client result"] do
      refute persistence =~ marker
    end

    :ok
  end

  @spec assert_rejection_settlement!(Request.t(), transport(), String.t(), String.t()) :: :ok
  defp assert_rejection_settlement!(request, transport, code, provider_param) do
    # A websocket has already upgraded successfully; its coded error is a frame, not an HTTP error response.
    # The client assertion pins status 400 while the settled row retains the existing transport status.
    assert request.response_status_code == if(transport in [:websocket, :owner_websocket], do: 200, else: 400)
    assert request.retry_count == 0
    assert request.transport == request_transport(transport)
    assert [attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^request.id))
    assert attempt.status == "failed"
    assert attempt.transport == attempt_transport(transport)
    assert attempt.upstream_status_code == if(transport in [:websocket, :owner_websocket], do: 200, else: 400)
    assert attempt.response_metadata["rejection_error_code"] == code
    assert attempt.response_metadata["rejection_error_param"] == provider_param
    assert_ledger_kinds!(request)
    :ok
  end

  @spec assert_ledger_kinds!(Request.t()) :: :ok
  defp assert_ledger_kinds!(request) do
    assert Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id, order_by: [asc: entry.entry_kind], select: entry.entry_kind)) == ["release", "reservation", "settlement"]
    :ok
  end

  @spec assert_health_neutral!(map()) :: :ok
  defp assert_health_neutral!(setup) do
    refute Repo.exists?(from(demotion in BridgeDemotion, where: demotion.pool_id == ^setup.pool.id))
    refute Repo.exists?(from(circuit in RoutingCircuitState, where: circuit.pool_id == ^setup.pool.id))
    :ok
  end

  @spec request_transport(transport()) :: String.t()
  defp request_transport(transport) when transport in [:websocket, :owner_websocket], do: "websocket"
  defp request_transport(_transport), do: "http_sse"

  @spec attempt_transport(transport()) :: String.t()
  defp attempt_transport(transport), do: if(websocket_upstream?(transport), do: "websocket", else: "http_sse")

  @spec websocket_upstream?(transport()) :: boolean()
  defp websocket_upstream?(transport), do: transport in [:bridged_sse, :websocket, :owner_websocket]

  @spec expected_public_calls() :: [map()]
  defp expected_public_calls, do: List.update_at(@calls, 3, &Map.put(&1, "namespace", "synthetic"))

  @spec client_results([map()]) :: [map()]
  defp client_results(calls) do
    Enum.map(calls, fn call ->
      type = if call["type"] == "function_call", do: "function_call_output", else: "custom_tool_call_output"
      %{"type" => type, "call_id" => call["call_id"], "output" => "synthetic client result"}
    end)
  end

  @spec history() :: [map()]
  defp history, do: [user("synthetic configuration opener"), assistant("synthetic earlier answer"), update("high"), user("synthetic configuration request"), update("low"), user("synthetic configuration follow-up")]

  @spec refusal_body(rejection()) :: {map(), String.t()}
  defp refusal_body(:consecutive), do: {%{"input" => [user("synthetic opener"), update("high"), update("low"), user("synthetic request")], "tools" => [@function]}, "input[2].type"}
  defp refusal_body(:invalid_effort), do: {%{"input" => [user("synthetic request"), update("synthetic-provider-effort")], "tools" => [@function]}, "input[1].reasoning.effort"}
  defp refusal_body(:function), do: {%{"input" => [update("high"), user("synthetic request")], "tools" => [@function]}, "tools"}
  defp refusal_body(:custom), do: {%{"input" => [update("high"), user("synthetic request")], "tools" => [Map.put(@custom, "async", true)]}, "tools"}
  defp refusal_body(:namespace_function), do: {%{"input" => [update("high"), user("synthetic request")], "tools" => [%{@namespace | "tools" => [Map.put(@member_function, "async", true)]}]}, "tools"}
  defp refusal_body(:namespace_custom), do: {%{"input" => [update("high"), user("synthetic request")], "tools" => [%{@namespace | "tools" => [@member_custom]}]}, "tools"}

  @spec client_rejection_param(String.t(), transport()) :: String.t()
  defp client_rejection_param(param, transport) when transport in [:websocket, :owner_websocket], do: String.replace(param, ~r/\[\d+\]/, "[]")
  defp client_rejection_param(param, _transport), do: param

  @spec assert_effort_vocabulary!(map(), rejection()) :: :ok
  defp assert_effort_vocabulary!(error, :invalid_effort) do
    assert error["message"] =~ "none, minimal, low, medium, high, xhigh, max"
    refute error["message"] =~ "synthetic-provider-effort"
    :ok
  end

  defp assert_effort_vocabulary!(_error, _kind), do: :ok

  @spec assert_refused_provider_item!(map(), String.t(), rejection()) :: String.t()
  defp assert_refused_provider_item!(captured, mode, :consecutive) do
    index = if mode == "lite", do: 3, else: 2
    assert Enum.at(captured.json["input"], index) == update("low")
    assert Enum.at(captured.json["input"], index - 1) == update("high")
    "input[#{index}].type"
  end

  defp assert_refused_provider_item!(captured, mode, :invalid_effort) do
    index = if mode == "lite", do: 2, else: 1
    assert Enum.at(captured.json["input"], index) == update("synthetic-provider-effort")
    "input[#{index}].reasoning.effort"
  end

  defp assert_refused_provider_item!(_captured, _mode, _async), do: "tools"

  @spec disable_async([map()]) :: [map()]
  defp disable_async(tools) do
    Enum.map(tools, fn
      %{"type" => "namespace", "tools" => members} = wrapper -> %{wrapper | "tools" => disable_async(members)}
      tool -> Map.put(tool, "async", false)
    end)
  end

  @spec local_refusal_body(atom()) :: map()
  defp local_refusal_body(:sdk_content), do: %{"input" => [user("synthetic request"), %{"type" => "configuration_update", "content" => ""}], "tools" => []}
  defp local_refusal_body(:namespace_async), do: %{"input" => [user("synthetic request")], "tools" => [@function, put_in(@namespace, ["tools", Access.at(1), "async"], nil)]}
  defp local_refusal_body(:call_async), do: %{"input" => [user("synthetic request"), update("high"), List.last(@calls) |> Map.put("async", "yes")], "tools" => @tools}
  defp local_refusal_body(:wrapper_async), do: %{"input" => [user("synthetic request")], "tools" => [@function, Map.put(@namespace, "async", false)]}

  @spec update(String.t()) :: map()
  defp update(effort), do: %{"type" => "configuration_update", "reasoning" => %{"effort" => effort}}

  @spec user(String.t()) :: map()
  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  @spec assistant(String.t()) :: map()
  defp assistant(text), do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}
end
