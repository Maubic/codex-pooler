defmodule CodexPoolerWeb.V1.OutputLimitForwardingTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, native_text_input: 1, public_websocket_connect!: 4, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [await_socket_connection_state!: 2, socket_transport_barrier!: 3]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [pool_owner_pids: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Accounts.{Scope, User}
  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @native "/backend-api/codex/responses"
  @public "/v1/responses"
  @chat "/v1/chat/completions"

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    :ok
  end

  # The terminal shape follows direct provider observations in Full/Lite over HTTP/WS.
  # All ids, inputs and usage here are synthetic; FakeUpstream does not generate or count tokens.
  for route <- [@native, @public], mode <- ["full", "lite"], transport <- [:http_json, :http_sse, :websocket, :owner_websocket], cap <- [:explicit, :absent] do
    @tag route: route, mode: mode, transport: transport, cap: cap
    test "#{route} #{mode} #{transport} #{cap} forwards the client limit and settles provider usage", context do
      assert_responses_forwarding!(context)
    end
  end

  for mode <- ["full", "lite"], cap <- [:explicit, :absent] do
    @tag route: @public, mode: mode, transport: :bridged_sse, cap: cap
    test "public #{mode} bridged SSE #{cap} forwards the client limit and settles provider usage", context do
      assert_responses_forwarding!(context)
    end
  end

  defp assert_responses_forwarding!(%{route: route, mode: mode, transport: transport, cap: cap}) do
    forwarded? = transport in [:bridged_sse, :owner_websocket]
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarded?)
    response = terminal_response(cap)
    upstream = start_upstream(FakeUpstream.strict_sequence([upstream_response(route, transport, response)]))
    setup = setup!(upstream, mode)
    # A policy limit is a reservation guard, never a source of provider request fields.
    set_output_policy!(setup, 512)
    body = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic output limit fixture"), "stream" => transport != :http_json}
    body = if cap == :explicit, do: Map.put(body, "max_output_tokens", 16), else: body
    terminal = dispatch!(setup, route, transport, body)
    assert terminal["status"] == response["status"]
    assert terminal["usage"]["output_tokens"] == response["usage"]["output_tokens"]
    assert terminal["incomplete_details"] == response["incomplete_details"]
    assert_dispatch!(upstream, mode, transport, cap)
    assert_settlement!(setup, mode, response["usage"]["output_tokens"])
    if forwarded?, do: assert([_owner | _] = pool_owner_pids(setup.pool))
    assert :ok = FakeUpstream.verify!(upstream)
  end

  for mode <- ["full", "lite"], stream? <- [false, true], limits <- [%{"max_tokens" => 16}, %{"max_completion_tokens" => 16}, %{"max_tokens" => 32, "max_completion_tokens" => 16}, %{}] do
    @tag mode: mode, stream?: stream?, limits: limits
    test "Chat #{mode} stream=#{stream?} limit fields #{inspect(limits)} reach the provider with current precedence", context do
      assert_chat_forwarding!(context)
    end
  end

  defp assert_chat_forwarding!(%{mode: mode, stream?: stream?, limits: limits}) do
    cap = if map_size(limits) == 0, do: :absent, else: :explicit
    response = terminal_response(cap)
    upstream = start_upstream(FakeUpstream.strict_sequence([upstream_response(@chat, :http_sse, response)]))
    setup = setup!(upstream, mode)
    body = Map.merge(%{"model" => setup.model.exposed_model_id, "messages" => [%{"role" => "user", "content" => "synthetic Chat limit fixture"}], "stream" => stream?}, limits)
    conn = build_conn() |> auth(setup) |> post(@chat, body)
    assert conn.status == 200
    expected_finish = if cap == :explicit, do: "length", else: "stop"

    if stream? do
      assert Enum.any?(sse_events(conn.resp_body), &(get_in(&1, ["choices", Elixir.Access.at(0), "finish_reason"]) == expected_finish))
    else
      assert get_in(json_response(conn, 200), ["choices", Elixir.Access.at(0), "finish_reason"]) == expected_finish
    end

    assert_dispatch!(upstream, mode, :http_sse, cap)
    assert [captured] = FakeUpstream.requests(upstream)
    refute Map.has_key?(captured.json, "max_tokens")
    refute Map.has_key?(captured.json, "max_completion_tokens")
    assert_settlement!(setup, mode, response["usage"]["output_tokens"])
    assert :ok = FakeUpstream.verify!(upstream)
  end

  for {route, field} <- [{@public, "max_output_tokens"}, {@chat, "max_tokens"}, {@chat, "max_completion_tokens"}], value <- [nil, 0, -1, 1.5, "16", true, %{}] do
    @tag route: route, field: field, value: value
    test "#{route} rejects malformed #{field}=#{inspect(value)} before dispatch", context do
      assert_public_validation!(context)
    end
  end

  defp assert_public_validation!(%{route: route, field: field, value: value}) do
    upstream = start_upstream(FakeUpstream.sse_stream([]))
    setup = gateway_setup(upstream)
    input = if route == @chat, do: %{"messages" => [%{"role" => "user", "content" => "synthetic validation fixture"}]}, else: %{"input" => "synthetic validation fixture"}
    payload = Map.merge(input, %{"model" => setup.model.exposed_model_id, field => value})
    conn = build_conn() |> auth(setup) |> post(route, payload)
    assert %{"error" => %{"param" => ^field, "message" => message}} = json_response(conn, 400)
    assert message == field <> " must be a positive integer"
    assert FakeUpstream.requests(upstream) == []
    refute Repo.exists?(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id))
    refute Repo.exists?(from(l in LedgerEntry, where: l.api_key_id == ^setup.api_key.id))
  end

  @invalid_limits [
    {0, "integer_below_min_value", "Invalid 'max_output_tokens': integer below minimum value. Expected a value >= 16, but got 0 instead."},
    {"16", "invalid_type", "Invalid type for 'max_output_tokens': expected an integer, but got a string instead."}
  ]

  for mode <- ["full", "lite"], transport <- [:http_json, :websocket, :owner_websocket], {value, code, message} <- @invalid_limits do
    @tag mode: mode, transport: transport, value: value, code: code, message: message
    test "native #{mode} #{transport} forwards supplied #{inspect(value)} for provider validation", context do
      assert_native_validation_passthrough!(context)
    end
  end

  defp assert_native_validation_passthrough!(%{mode: mode, transport: transport, value: value, code: code, message: message}) do
    # Direct provider observations: HTTP names the code and parameter; websocket wraps the same message without them.
    # The gateway's existing error projection stays unchanged, including its generic below-minimum websocket refusal.
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, transport == :owner_websocket)
    error = %{"type" => "invalid_request_error", "code" => code, "param" => "max_output_tokens", "message" => message}

    response =
      if transport == :http_json do
        FakeUpstream.json_response(%{"error" => error}, 400)
      else
        frame = %{"type" => "error", "status" => 400, "error" => %{error | "code" => nil, "param" => nil, "message" => "Invalid response.create payload: " <> message}}
        FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(frame)])
      end

    upstream = start_upstream(FakeUpstream.strict_sequence([response]))
    setup = setup!(upstream, mode)
    body = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic validation fixture"), "max_output_tokens" => value, "stream" => transport != :http_json}

    if transport == :http_json do
      conn = build_conn() |> auth(setup) |> post(@native, body)
      assert conn.status == 400
    else
      events = websocket_events!(setup, @native, body)
      assert [%{"type" => "error", "status" => 400}] = Enum.filter(events, &(&1["type"] == "error"))
      refute Enum.any?(events, &(&1["type"] in ["response.completed", "response.incomplete"]))
    end

    assert [captured] = FakeUpstream.requests(upstream)
    assert Map.fetch(captured.json, "max_output_tokens") == {:ok, value}
    assert captured.method == if(transport == :http_json, do: "POST", else: "WEBSOCKET")
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "failed"
    # Native websocket rows keep the successful transport status; the client error frame above carries the refusal.
    assert request.response_status_code == if(transport == :http_json, do: 400, else: 200)
    assert request.retry_count == 0
    assert [_attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "compatibility matrix records client-only output limit forwarding" do
    fixture = CompatibilityMatrix.fixture!(:client_output_limit_forwarding)
    assert fixture.client_field == "max_output_tokens"
    assert fixture.absent == "absent"
    assert fixture.chat_precedence == ["max_completion_tokens", "max_tokens"]
    assert fixture.policy_injection == false
    assert fixture.compaction == "existing_whitelist_unchanged"
  end

  defp terminal_response(cap) do
    output = if cap == :explicit, do: 16, else: 83
    response = %{"id" => "resp_synthetic_output_limit", "object" => "response", "created_at" => 1_790_000_000, "status" => if(cap == :explicit, do: "incomplete", else: "completed"), "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => output, "total_tokens" => 10 + output}}
    if cap == :explicit, do: Map.put(response, "incomplete_details", %{"reason" => "max_output_tokens"}), else: response
  end

  defp upstream_response(@native, :http_json, response), do: FakeUpstream.json_response(response)

  defp upstream_response(_route, _transport, response) do
    type = "response." <> response["status"]
    FakeUpstream.sse_stream([{type, %{"type" => type, "response" => response}}])
  end

  defp setup!(upstream, mode) do
    setup = gateway_setup(upstream)
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    setup
  end

  defp set_output_policy!(setup, limit) do
    owner = Repo.get!(User, setup.api_key.created_by_user_id)
    assert {:ok, _key} = Access.update_api_key_with_policy(Scope.for_user(owner, ["instance_owner"]), setup.api_key, %{default_policy: %{max_output_tokens_per_request: limit}})
  end

  defp dispatch!(setup, route, transport, body) when transport in [:http_json, :http_sse, :bridged_sse] do
    conn = build_conn() |> auth(setup)
    conn = if transport == :bridged_sse, do: put_req_header(conn, "x-session-id", Ecto.UUID.generate()), else: conn
    conn = post(conn, route, body)
    assert conn.status == 200
    if transport == :http_json, do: json_response(conn, 200), else: terminal!(sse_events(conn.resp_body))
  end

  defp dispatch!(setup, route, _transport, body), do: setup |> websocket_events!(route, body) |> terminal!()

  defp websocket_events!(setup, route, body) do
    port = start_public_endpoint!()
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, Ecto.UUID.generate(), route)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.put(body, "type", "response.create")))
      {conn, websocket, events} = receive_terminal!(conn, websocket, ref, [])
      await_socket_connection_state!(socket, &(is_nil(Map.get(&1, :public_response_task_pid)) and MapSet.size(&1.tasks) == 0))
      socket_transport_barrier!(conn, websocket, ref)
      events
    after
      Mint.HTTP.close(conn)
      assert :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
    end
  end

  defp receive_terminal!(conn, websocket, ref, events) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    event = CodexPooler.JSON.decode!(text)
    events = events ++ [event]
    if event["type"] in ["response.completed", "response.incomplete", "response.failed", "error"], do: {conn, websocket, events}, else: receive_terminal!(conn, websocket, ref, events)
  end

  defp sse_events(body) do
    for block <- String.split(body, "\n\n", trim: true), "data: " <> data <- String.split(block, "\n"), data != "[DONE]", do: CodexPooler.JSON.decode!(data)
  end

  defp terminal!(events) do
    terminals = Enum.filter(events, &(&1["type"] in ["response.completed", "response.incomplete", "response.failed", "error"]))
    assert [terminal] = terminals
    assert terminal["type"] in ["response.completed", "response.incomplete"]
    terminal["response"]
  end

  defp assert_dispatch!(upstream, mode, transport, cap) do
    assert [captured] = FakeUpstream.requests(upstream)
    assert Map.fetch(captured.json, "max_output_tokens") == if(cap == :explicit, do: {:ok, 16}, else: :error)
    assert captured.path == @native

    if transport in [:http_json, :http_sse] do
      assert captured.method == "POST"
      assert Map.new(captured.headers)["x-openai-internal-codex-responses-lite"] == if(mode == "lite", do: "true")
    else
      assert captured.method == "WEBSOCKET"
      assert FakeUpstream.websocket_connection_count(upstream) == 1
      assert get_in(captured.json, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"]) == if(mode == "lite", do: "true")
      assert FakeUpstream.websocket_steers(upstream) == []
    end
  end

  defp assert_settlement!(setup, mode, output) do
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert {request.status, request.usage_status, request.retry_count, request.last_error_code} == {"succeeded", "usage_known", 0, nil}
    assert request.request_metadata["routing"]["model_serving_mode"] == mode
    assert [%Attempt{status: "succeeded", network_error_code: nil}] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert [settlement] = Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"))
    assert {settlement.usage_status, settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} == {"usage_known", 10, output, 10 + output}
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "reservation"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
  end
end
