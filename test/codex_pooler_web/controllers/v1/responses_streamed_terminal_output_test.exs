defmodule CodexPoolerWeb.V1.ResponsesStreamedTerminalOutputTest do
  @moduledoc """
  A streamed `/v1/responses` terminal lists the output the stream delivered (findings#335).

  The Codex backend often closes a stream with `response.completed` listing `output: []` although
  `response.output_item.done` carried every item: measured through the dev listener on `gpt-6-luna`, Lite, in 18 of 27
  turns (8 of 10 over HTTP SSE, 4 of 7 bridged onto the upstream websocket, 6 of 10 on the public websocket), with the
  done event carrying the item in all 27 and no `output_text` key in the terminal response. The OpenAI Responses contract
  carries the full output in the terminal, and the SDK stream helpers take the final response from it (openai-node
  `responses.stream().finalResponse()` replaces its accumulated snapshot with the terminal's response, openai-python
  keeps an empty list), so their final output and `output_text` came back empty.

  The public relays now put the done items into a completed or incomplete terminal whose output is empty, ordered by
  `output_index` with the ids the done events carried, on every transport of the route and in both serving modes; a
  non-empty terminal output is relayed as sent, and the native backend routes relay the provider's terminal unchanged.

  Topology: one node, the real `/v1` endpoint, FakeUpstream streaming the measured event order with synthetic items and
  text; HTTP SSE with the turn upstream over HTTP SSE (owner forwarding off), HTTP SSE bridged onto the upstream
  websocket through a local owner (owner forwarding on, `x-session-id`), the public `GET /v1/responses` websocket direct
  (forwarding off) and through a local owner (forwarding on); Full by catalog and Lite by a Pool serving override.
  """

  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, native_text_input: 1, public_websocket_connect!: 4, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [pool_owner_pids: 1]

  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  @moduletag capture_log: true

  @reply "synthetic streamed reply"
  @reasoning %{"type" => "reasoning", "id" => "rs_synthetic_streamed", "summary" => [], "encrypted_content" => "gAAAAA-synthetic-closed-reasoning"}
  @message %{"type" => "message", "id" => "msg_synthetic_streamed", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => @reply, "annotations" => []}]}
  @call_a %{"type" => "function_call", "id" => "fc_synthetic_streamed_a", "call_id" => "call_synthetic_streamed_a", "name" => "lookup_fixture", "arguments" => ~s({"key":"a"}), "status" => "completed"}
  @call_b %{"type" => "function_call", "id" => "fc_synthetic_streamed_b", "call_id" => "call_synthetic_streamed_b", "name" => "lookup_fixture", "arguments" => ~s({"key":"b"}), "status" => "completed"}
  @tool %{"type" => "function", "name" => "lookup_fixture", "description" => "synthetic fixture lookup", "parameters" => %{"type" => "object", "properties" => %{"key" => %{"type" => "string"}}, "required" => ["key"], "additionalProperties" => false}}

  @transports [:http_sse, :bridged_sse, :websocket, :owner_websocket]
  @modes ["full", "lite"]

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    :ok
  end

  describe "a completed terminal the provider sent with an empty output" do
    for transport <- @transports, mode <- @modes do
      @tag transport: transport, mode: mode
      test "lists the done items in output order with their ids (#{transport}, #{mode})", %{transport: transport, mode: mode} do
        {events, upstream} = run_turn!(transport, mode, message_turn("response.completed", []))

        assert_item_events!(events, [@reasoning, @message])
        terminal = terminal!(events, "response.completed")
        assert terminal["response"]["output"] == [@reasoning, @message]
        assert Enum.map(terminal["response"]["output"], & &1["id"]) == ["rs_synthetic_streamed", "msg_synthetic_streamed"]
        assert sdk_final_output_text(events) == @reply
        assert_topology!(transport, mode, upstream)
      end
    end
  end

  describe "a completed terminal that lists its own output" do
    # The provider's output names only the reply, although the stream closed the reasoning item too: a relay that
    # replaced a non-empty output would list both.
    for transport <- @transports, mode <- @modes do
      @tag transport: transport, mode: mode
      test "is relayed as the provider sent it (#{transport}, #{mode})", %{transport: transport, mode: mode} do
        {events, upstream} = run_turn!(transport, mode, message_turn("response.completed", [@message]))

        assert_item_events!(events, [@reasoning, @message])
        assert terminal!(events, "response.completed")["response"]["output"] == [@message]
        assert_topology!(transport, mode, upstream)
      end
    end
  end

  describe "items closed out of their output order" do
    # Two tool calls stream at once: both are announced, then the second closes before the first. The terminal lists
    # them by output index, the order the SDK accumulators fold the done events into, not in arrival order.
    for transport <- @transports, mode <- @modes do
      @tag transport: transport, mode: mode
      test "keep their output order in the terminal (#{transport}, #{mode})", %{transport: transport, mode: mode} do
        {events, upstream} = run_turn!(transport, mode, interleaved_turn())

        assert Enum.map(done_events(events), & &1["output_index"]) == [1, 0]
        terminal = terminal!(events, "response.completed")
        assert terminal["response"]["output"] == [@call_a, @call_b]
        assert terminal["response"]["output"] == folded_output(events)
        assert_topology!(transport, mode, upstream)
      end
    end
  end

  describe "an incomplete terminal the provider sent with an empty output" do
    for transport <- @transports, mode <- @modes do
      @tag transport: transport, mode: mode
      test "lists the done items (#{transport}, #{mode})", %{transport: transport, mode: mode} do
        {events, upstream} = run_turn!(transport, mode, message_turn("response.incomplete", []))

        terminal = terminal!(events, "response.incomplete")
        assert terminal["response"]["incomplete_details"] == %{"reason" => "max_output_tokens"}
        assert terminal["response"]["output"] == [@reasoning, @message]
        assert sdk_final_output_text(events) == @reply
        assert_topology!(transport, mode, upstream)
      end
    end
  end

  describe "the native backend routes" do
    test "relay the provider's empty completed output unchanged over HTTP SSE", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.sse_stream(message_turn("response.completed", [])))
      setup = gateway_setup(upstream)

      body =
        conn
        |> auth(setup)
        |> put_req_header("content-type", "application/json")
        |> post("/backend-api/codex/responses", CodexPooler.JSON.encode!(%{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic question"), "stream" => true, "store" => false}))
        |> response(200)

      events = sse_events(body)
      assert_item_events!(events, [@reasoning, @message])
      assert terminal!(events, "response.completed")["response"]["output"] == []
    end

    test "relay the provider's empty completed output unchanged over the native websocket" do
      upstream = start_upstream(FakeUpstream.sse_stream(message_turn("response.completed", [])))
      setup = gateway_setup(upstream)
      port = start_public_endpoint!()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, Ecto.UUID.generate(), "/backend-api/codex/responses")

      events =
        try do
          frame = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "input" => native_text_input("synthetic question"), "stream" => true}
          {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
          receive_until_terminal!(conn, websocket, ref, [])
        after
          Mint.HTTP.close(conn)
        end

      assert_item_events!(events, [@reasoning, @message])
      assert terminal!(events, "response.completed")["response"]["output"] == []
    end
  end

  test "the compatibility matrix names the streamed terminal output contract pinned here" do
    fixture = CompatibilityMatrix.fixture!(:v1_supported_surface)

    assert fixture.streamed_terminal_output == %{
             surfaces: [
               %{method: :post, path: "/v1/responses", transport: "http_sse", upstream: ["http_sse", "websocket_bridge"]},
               %{method: :get, path: "/v1/responses", transport: "responses_websocket", upstream: ["direct", "owner_forwarded"]}
             ],
             serving_modes: ["full", "lite"],
             terminals: ["response.completed", "response.incomplete"],
             empty_output: "output_item_done_items_by_output_index",
             nonempty_output: "relayed_as_sent",
             native_routes: "relayed_as_sent"
           }
  end

  # The measured order: a reasoning item, then the reply message with its text, then the terminal; the terminal lists
  # `output` as given.
  defp message_turn(terminal_type, output) do
    status = String.replace_prefix(terminal_type, "response.", "")
    response = response_body(status, output)
    response = if status == "incomplete", do: Map.put(response, "incomplete_details", %{"reason" => "max_output_tokens"}), else: response
    address = %{"item_id" => @message["id"], "output_index" => 1, "content_index" => 0}
    part = %{"type" => "output_text", "text" => "", "annotations" => []}

    opening() ++
      [
        {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.delete(@reasoning, "encrypted_content")}},
        {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => @reasoning}},
        {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 1, "item" => %{@message | "status" => "in_progress", "content" => []}}},
        {"response.content_part.added", Map.merge(address, %{"type" => "response.content_part.added", "part" => part})},
        {"response.output_text.delta", Map.merge(address, %{"type" => "response.output_text.delta", "delta" => @reply})},
        {"response.output_text.done", Map.merge(address, %{"type" => "response.output_text.done", "text" => @reply})},
        {"response.content_part.done", Map.merge(address, %{"type" => "response.content_part.done", "part" => %{part | "text" => @reply}})},
        {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 1, "item" => @message}},
        {terminal_type, %{"type" => terminal_type, "response" => response}}
      ]
  end

  defp interleaved_turn do
    opening() ++
      [
        {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{@call_a | "arguments" => "", "status" => "in_progress"}}},
        {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 1, "item" => %{@call_b | "arguments" => "", "status" => "in_progress"}}},
        {"response.function_call_arguments.delta", %{"type" => "response.function_call_arguments.delta", "item_id" => @call_b["id"], "output_index" => 1, "delta" => @call_b["arguments"]}},
        {"response.function_call_arguments.delta", %{"type" => "response.function_call_arguments.delta", "item_id" => @call_a["id"], "output_index" => 0, "delta" => @call_a["arguments"]}},
        {"response.function_call_arguments.done", %{"type" => "response.function_call_arguments.done", "item_id" => @call_b["id"], "output_index" => 1, "arguments" => @call_b["arguments"]}},
        {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 1, "item" => @call_b}},
        {"response.function_call_arguments.done", %{"type" => "response.function_call_arguments.done", "item_id" => @call_a["id"], "output_index" => 0, "arguments" => @call_a["arguments"]}},
        {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => @call_a}},
        {"response.completed", %{"type" => "response.completed", "response" => response_body("completed", [])}}
      ]
  end

  defp opening do
    opening = response_body("in_progress", [])

    [
      {"response.created", %{"type" => "response.created", "response" => opening}},
      {"response.in_progress", %{"type" => "response.in_progress", "response" => opening}}
    ]
  end

  defp response_body(status, output) do
    usage = if status == "in_progress", do: nil, else: %{"input_tokens" => 37, "output_tokens" => 43, "total_tokens" => 80}
    %{"id" => "resp_synthetic_streamed_terminal", "object" => "response", "created_at" => 1_790_000_000, "model" => "provider-gpt-test-model", "status" => status, "output" => output, "usage" => usage}
  end

  defp run_turn!(transport, mode, upstream_events) do
    if transport in [:bridged_sse, :owner_websocket], do: Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    upstream = start_upstream(FakeUpstream.sse_stream(upstream_events))
    setup = gateway_setup(upstream)
    if mode == "lite", do: put_serving_mode!(setup, "lite")
    body = %{"model" => setup.model.exposed_model_id, "input" => "synthetic question", "tools" => [@tool], "stream" => true}

    events =
      case transport do
        :http_sse -> setup |> post_stream!(body, []) |> sse_events()
        :bridged_sse -> setup |> post_stream!(body, [{"x-session-id", "streamed-terminal-#{System.unique_integer([:positive])}"}]) |> sse_events()
        websocket when websocket in [:websocket, :owner_websocket] -> public_websocket_turn!(setup, body)
      end

    {events, %{upstream: upstream, setup: setup}}
  end

  defp post_stream!(setup, body, headers) do
    conn = Enum.reduce(headers, build_conn() |> auth(setup), fn {name, value}, conn -> put_req_header(conn, name, value) end)
    conn = post(conn, "/v1/responses", body)
    assert conn.status == 200
    assert [content_type] = get_resp_header(conn, "content-type")
    assert content_type =~ "text/event-stream"
    conn.resp_body
  end

  defp public_websocket_turn!(setup, body) do
    port = start_public_endpoint!()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, "streamed-terminal-#{System.unique_integer([:positive])}", "/v1/responses")

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.put(body, "type", "response.create")))
      receive_until_terminal!(conn, websocket, ref, [])
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_until_terminal!(conn, websocket, ref, events) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    event = CodexPooler.JSON.decode!(text)
    events = events ++ [event]

    if event["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: events,
      else: receive_until_terminal!(conn, websocket, ref, events)
  end

  defp sse_events(body) do
    for block <- String.split(body, "\n\n", trim: true),
        "data: " <> data <- String.split(block, "\n"),
        data != "[DONE]",
        do: CodexPooler.JSON.decode!(data)
  end

  # The transport and serving mode the turn really took: the upstream request (HTTP POST, or a websocket frame on one
  # connection), the Lite marker it carried, and for the owner topologies a running owner.
  defp assert_topology!(transport, mode, %{upstream: upstream, setup: setup}) do
    assert [request] = FakeUpstream.requests(upstream)

    case transport do
      :http_sse ->
        assert request.method == "POST"
        assert Map.new(request.headers)["x-openai-internal-codex-responses-lite"] == if(mode == "lite", do: "true")

      websocket when websocket in [:bridged_sse, :websocket, :owner_websocket] ->
        assert request.method == "WEBSOCKET"
        assert FakeUpstream.websocket_connection_count(upstream) == 1
        assert get_in(request.json, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"]) == if(mode == "lite", do: "true")
    end

    if transport in [:bridged_sse, :owner_websocket], do: assert([_owner | _rest] = pool_owner_pids(setup.pool))
  end

  defp assert_item_events!(events, items) do
    assert Enum.map(done_events(events), & &1["item"]) == items
  end

  defp done_events(events), do: Enum.filter(events, &(&1["type"] == "response.output_item.done"))

  defp terminal!(events, type) do
    terminal = List.last(events)
    assert terminal["type"] == type, "expected #{type}, received #{inspect(Enum.map(events, & &1["type"]))}"
    terminal
  end

  # The items the SDK stream helpers fold from the events: appended on `added`, replaced by output index on `done`.
  defp folded_output(events) do
    events
    |> Enum.reduce(%{}, fn
      %{"type" => type, "output_index" => index, "item" => item}, acc when type in ["response.output_item.added", "response.output_item.done"] -> Map.put(acc, index, item)
      _event, acc -> acc
    end)
    |> Enum.sort_by(fn {index, _item} -> index end)
    |> Enum.map(fn {_index, item} -> item end)
  end

  # What openai-node `responses.stream().finalResponse().output_text` returns: the terminal's response replaces the
  # accumulated snapshot, and `output_text` joins the `output_text` parts of its messages.
  defp sdk_final_output_text(events) do
    for %{"type" => "message", "content" => content} <- List.last(events)["response"]["output"],
        %{"type" => "output_text", "text" => text} <- content,
        into: "",
        do: text
  end

  defp put_serving_mode!(setup, mode) do
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
  end
end
