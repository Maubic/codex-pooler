defmodule CodexPoolerWeb.V1.ResponsesToolSearchTest do
  @moduledoc """
  findings#313: `POST /v1/responses` accepts the provider's `tool_search` tool beside deferred function tools
  (`defer_loading: true`), relays the items a search turn produces, and admits their replay in a stateless history,
  exactly as the provider does.

  Provider contract, direct probe on `gpt-6-luna` (Codex backend, Full shape and Lite manifest, unknown-key controls
  refused): `tool_search` takes `type`, `execution` (`server` or `client`), `description` and `parameters`; a
  server-executed search in Full emits `tool_search_call`, `tool_search_output` and a `function_call` that carries
  `namespace`; a client-executed search emits the call and reads the client's `tool_search_output`; the replayed items
  are read in Full and Lite and validated strictly. The pairing rules (a deferred tool needs a `tool_search`, a
  `tool_search` needs a deferred tool) are the provider's own, refused in Full and not enforced in a Lite manifest, so
  the adapter does not repeat them.

  Serving modes: Full forwards the tools at the top level, where the provider runs the search; Lite moves them into the
  leading `additional_tools` manifest, where the provider validates `tool_search` and performs no search, so the model
  sees the deferred function directly and calls it without `tool_search_*` items.

  Topology: the real `/v1` endpoint, FakeUpstream answering as the measured provider did, the Pool's serving mode
  forced to Full or Lite, HTTP SSE upstream (and the public websocket), synthetic content.
  """

  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.FakeUpstream

  @moduletag capture_log: true
  @frame_timeout_ms 15_000

  @parameters %{"type" => "object", "properties" => %{"item_id" => %{"type" => "string"}}, "required" => ["item_id"], "additionalProperties" => false}
  @deferred %{"type" => "function", "name" => "lookup_item", "description" => "Look up a catalog item.", "parameters" => @parameters, "defer_loading" => true}
  @tool_search %{"type" => "tool_search"}
  @client_search %{
    "type" => "tool_search",
    "execution" => "client",
    "description" => "Search the catalog tools.",
    "parameters" => %{"type" => "object", "properties" => %{"query" => %{"type" => "string"}}, "required" => ["query"], "additionalProperties" => false}
  }

  # The items of a server-executed search turn as the provider emitted them (shape measured; ids and values synthetic).
  @search_call %{"type" => "tool_search_call", "id" => "tsc_synthetic_0001", "call_id" => nil, "execution" => "server", "status" => "completed", "arguments" => %{"paths" => ["lookup_item"]}}
  @search_output %{
    "type" => "tool_search_output",
    "id" => "tso_synthetic_0001",
    "call_id" => nil,
    "execution" => "server",
    "status" => "completed",
    "tools" => [Map.merge(@deferred, %{"strict" => true, "output_schema" => nil})]
  }
  @namespaced_call %{
    "type" => "function_call",
    "id" => "fc_synthetic_0001",
    "call_id" => "call_synthetic_0001",
    "name" => "lookup_item",
    "namespace" => "lookup_item",
    "arguments" => ~s({"item_id":"A-17"}),
    "status" => "completed"
  }
  @plain_call Map.delete(@namespaced_call, "namespace")
  @client_call %{"type" => "tool_search_call", "id" => "tsc_synthetic_0002", "call_id" => "call_synthetic_search", "execution" => "client", "status" => "completed", "arguments" => %{"query" => "catalog lookup"}}
  @client_output %{"type" => "tool_search_output", "execution" => "client", "call_id" => "call_synthetic_search", "status" => "completed", "tools" => [@deferred]}

  describe "the tool_search tool" do
    for mode <- ~w(full lite), stream <- [false, true] do
      @tag serving_mode: mode, streaming: stream
      test "#{mode}, stream=#{stream}: a tool_search beside a deferred function is forwarded and the turn's items reach the client", %{conn: conn, serving_mode: mode, streaming: stream} do
        items = turn_items(mode)
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_tool_search_turn", items)))
        setup = serving_setup(upstream, mode)

        response = post_responses(conn, setup, %{"input" => "synthetic lookup request", "stream" => stream, "tools" => [@tool_search, @deferred]})

        if stream do
          body = response(response, 200)
          for item <- items, do: assert(body =~ ~s("type":"#{item["type"]}"))
          if mode == "full", do: assert(body =~ ~s("namespace":"lookup_item"))
        else
          assert %{"status" => "completed", "output" => output} = json_response(response, 200)
          assert Enum.map(output, & &1["type"]) == Enum.map(items, & &1["type"])
          if mode == "full", do: assert(Enum.take(output, 2) == [@search_call, @search_output] and List.last(output)["namespace"] == "lookup_item")
        end

        assert [captured] = FakeUpstream.requests(upstream)
        assert_forwarded_tools(captured, mode, [@tool_search, @deferred])
      end
    end

    for mode <- ~w(full lite) do
      @tag serving_mode: mode
      test "#{mode}: a client-executed tool_search with its description and parameters is forwarded unchanged", %{conn: conn, serving_mode: mode} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_client_search_turn", [@client_call])))
        setup = serving_setup(upstream, mode)

        response = post_responses(conn, setup, %{"input" => "synthetic lookup request", "tools" => [@client_search, @deferred]})

        assert %{"output" => [@client_call]} = json_response(response, 200)
        assert [captured] = FakeUpstream.requests(upstream)
        assert_forwarded_tools(captured, mode, [@client_search, @deferred])
      end
    end

    # The provider refuses these with `unknown_parameter` or `invalid_value` on the tool's path; the adapter answers them
    # before any reservation or dispatch, as it does an unknown `web_search` key.
    for {label, tool} <- [
          {"an unknown key", Map.put(@tool_search, "zz_unknown", true)},
          {"another execution", Map.put(@tool_search, "execution", "bogus")},
          {"a null execution", Map.put(@tool_search, "execution", nil)},
          {"a description that is not a string", Map.put(@client_search, "description", 7)},
          {"parameters that are not an object", Map.put(@client_search, "parameters", "synthetic")},
          {"a namespace key", Map.put(@tool_search, "namespace", "catalog")}
        ] do
      test "a tool_search with #{label} is refused before dispatch", %{conn: conn} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_never_sent", [])))
        setup = serving_setup(upstream, "full")

        response = post_responses(conn, setup, %{"input" => "synthetic lookup request", "tools" => [unquote(Macro.escape(tool)), @deferred]})

        assert json_response(response, 400)["error"] == %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "tools", "message" => "tool shape is not translatable"}
        assert FakeUpstream.count(upstream) == 0
      end
    end

    # The pairing rules are the provider's: Full refuses them (the relay names the param), a Lite manifest does not.
    for mode <- ~w(full lite) do
      @tag serving_mode: mode
      test "#{mode}: a tool_search beside a non-deferred function and a null description are left to the provider", %{conn: conn, serving_mode: mode} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_pairing_left_to_provider", [])))
        setup = serving_setup(upstream, mode)
        plain = Map.delete(@deferred, "defer_loading")

        for tools <- [[@tool_search, plain], [Map.put(@tool_search, "description", nil), @deferred]] do
          response = conn |> recycle() |> post_responses(setup, %{"input" => "synthetic request", "tools" => tools})
          assert %{"status" => "completed"} = json_response(response, 200)
          assert_forwarded_tools(List.last(FakeUpstream.requests(upstream)), mode, tools)
        end
      end
    end

    for mode <- ~w(full lite) do
      @tag serving_mode: mode
      test "#{mode}: a tool_search in a client-sent additional_tools item is forwarded for the provider to validate", %{conn: conn, serving_mode: mode} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_manifest_tool_search", [])))
        setup = serving_setup(upstream, mode)
        manifest = %{"type" => "additional_tools", "role" => "developer", "tools" => [@tool_search, @deferred]}

        response = post_responses(conn, setup, %{"input" => [user("synthetic request"), manifest]})

        assert %{"status" => "completed"} = json_response(response, 200)
        assert [captured] = FakeUpstream.requests(upstream)
        assert manifest in captured.json["input"]
      end
    end
  end

  describe "the replayed search items" do
    for mode <- ~w(full lite) do
      @tag serving_mode: mode
      test "#{mode}: the follow-up of a server-executed search is forwarded with its items untouched", %{conn: conn, serving_mode: mode} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_search_followup", [])))
        setup = serving_setup(upstream, mode)
        output = %{"type" => "function_call_output", "call_id" => "call_synthetic_0001", "output" => "synthetic catalog answer"}

        # As emitted, with the nullable fields null, and with only the required ones. The upstream payload drops a null
        # item `id` as it does for every item; the provider reads an absent id like a null one (both measured 200).
        for {search_call, search_output} <- [
              {@search_call, @search_output},
              {%{@search_call | "id" => nil, "call_id" => nil, "status" => nil}, %{@search_output | "id" => nil, "call_id" => nil, "status" => nil}},
              {Map.take(@search_call, ["type", "arguments"]), Map.take(@search_output, ["type", "tools"])}
            ] do
          input = [user("synthetic lookup request"), search_call, search_output, @namespaced_call, output]
          response = conn |> recycle() |> post_responses(setup, %{"input" => input, "tools" => [@tool_search, @deferred]})

          assert %{"status" => "completed"} = json_response(response, 200)
          forwarded = List.last(FakeUpstream.requests(upstream)).json["input"]
          expected = for item <- [search_call, search_output], do: if(item["id"] == nil, do: Map.delete(item, "id"), else: item)
          assert Enum.filter(forwarded, &(&1["type"] in ["tool_search_call", "tool_search_output"])) == expected
          assert %{"namespace" => "lookup_item", "call_id" => "call_synthetic_0001"} = Enum.find(forwarded, &(&1["type"] == "function_call"))
        end
      end

      @tag serving_mode: mode
      test "#{mode}: a client-executed search's call and the client's output are forwarded untouched", %{conn: conn, serving_mode: mode} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_client_followup", [@plain_call])))
        setup = serving_setup(upstream, mode)

        response = post_responses(conn, setup, %{"input" => [user("synthetic lookup request"), @client_call, @client_output], "tools" => [@client_search, @deferred]})

        assert %{"output" => [%{"type" => "function_call", "name" => "lookup_item"}]} = json_response(response, 200)
        forwarded = List.last(FakeUpstream.requests(upstream)).json["input"]
        assert Enum.filter(forwarded, &(&1["type"] in ["tool_search_call", "tool_search_output"])) == [@client_call, @client_output]
      end
    end

    # The provider refuses each of these (`unknown_parameter`, `invalid_value`, `invalid_type`); MCP inside the loaded
    # tools is the adapter's own rule for a client's tool definitions, as in a client-sent manifest.
    for {label, item, message} <- [
          {"an unknown key on the call", Map.put(@search_call, "zz_unknown", true), "input item shape is not translatable"},
          {"an unknown key on the output", Map.put(@search_output, "zz_unknown", true), "input item shape is not translatable"},
          {"another status", Map.put(@search_call, "status", "bogus"), "input item shape is not translatable"},
          {"another execution", Map.put(@search_output, "execution", "bogus"), "input item shape is not translatable"},
          {"a null execution", Map.put(@search_call, "execution", nil), "input item shape is not translatable"},
          {"arguments as a JSON string", Map.put(@search_call, "arguments", ~s({"paths":["lookup_item"]})), "input item shape is not translatable"},
          {"no arguments", Map.delete(@search_call, "arguments"), "input item shape is not translatable"},
          {"no tools", Map.delete(@search_output, "tools"), "input item shape is not translatable"},
          {"a blank call id", Map.put(@search_call, "call_id", " "), "input item shape is not translatable"},
          {"a remote MCP tool among the loaded tools", Map.update!(@search_output, "tools", &(&1 ++ [%{"type" => "mcp", "server_label" => "synthetic"}])), "remote MCP tools are not supported"}
        ] do
      test "a replayed search item with #{label} is refused before dispatch", %{conn: conn} do
        upstream = start_upstream(FakeUpstream.sse_stream(turn_events("resp_never_sent", [])))
        setup = serving_setup(upstream, "full")

        response = post_responses(conn, setup, %{"input" => [user("synthetic lookup request"), unquote(Macro.escape(item))], "tools" => [@tool_search, @deferred]})

        assert json_response(response, 400)["error"] == %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "input", "message" => unquote(message)}
        assert FakeUpstream.count(upstream) == 0
      end
    end

    # The provider names the id of each item (`tsc`, `tso`) and refuses another with `invalid_value`; the adapter does not
    # second-guess it, so the refusal reaches the client from the provider with the item's path.
    test "an id the provider did not name is forwarded and the provider's refusal is relayed", %{conn: conn} do
      provider_path = "input[1].id"
      error = %{"type" => "invalid_request_error", "code" => "invalid_value", "param" => provider_path, "message" => "Invalid '#{provider_path}': 'tool_search_call_0'. Expected an ID that begins with 'tsc'."}
      upstream = start_upstream({:json_error, 400, %{"error" => error}})
      setup = serving_setup(upstream, "full")

      response = post_responses(conn, setup, %{"input" => [user("synthetic lookup request"), %{@search_call | "id" => "tool_search_call_0"}, @search_output], "tools" => [@tool_search, @deferred]})

      assert %{"code" => "invalid_value", "param" => ^provider_path} = json_response(response, 400)["error"]
      assert FakeUpstream.count(upstream) == 1
    end
  end

  describe "the public websocket" do
    for mode <- ~w(full lite) do
      @tag :v1_websocket
      @tag serving_mode: mode
      test "#{mode}: a search turn's items reach the client and their replay reaches the provider untouched", %{serving_mode: mode} do
        items = turn_items(mode)
        upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(turn_payloads("resp_ws_tool_search", items), &CodexPooler.JSON.encode!/1)))
        setup = serving_setup(upstream, mode)

        events = websocket_turn!(setup, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => [user("synthetic lookup request")], "tools" => [@tool_search, @deferred], "stream" => true})
        assert List.last(events)["type"] == "response.completed"
        assert for(%{"type" => "response.output_item.done", "item" => item} <- events, do: item["type"]) == Enum.map(items, & &1["type"])

        replay = [user("synthetic lookup request"), @search_call, @search_output, @namespaced_call, %{"type" => "function_call_output", "call_id" => "call_synthetic_0001", "output" => "synthetic catalog answer"}]
        events = websocket_turn!(setup, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => replay, "tools" => [@tool_search, @deferred], "stream" => true})
        refute Enum.any?(events, &(&1["type"] == "error"))

        forwarded = List.last(FakeUpstream.requests(upstream)).json["input"]
        assert Enum.filter(forwarded, &(&1["type"] in ["tool_search_call", "tool_search_output"])) == [@search_call, @search_output]
      end
    end
  end

  # What the provider measured per mode: Full runs the search, Lite calls the deferred function directly.
  defp turn_items("full"), do: [@search_call, @search_output, @namespaced_call]
  defp turn_items("lite"), do: [@plain_call]

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_responses(conn, setup, body), do: conn |> auth(setup) |> post("/v1/responses", Map.put(body, "model", setup.model.exposed_model_id))

  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  # Full carries the tools at the top level; Lite relocates them into the leading developer manifest item.
  defp assert_forwarded_tools(captured, "full", tools) do
    assert captured.json["tools"] == tools
    refute Enum.any?(List.wrap(captured.json["input"]), &(is_map(&1) and &1["type"] == "additional_tools"))
  end

  defp assert_forwarded_tools(captured, "lite", tools) do
    refute Map.has_key?(captured.json, "tools")
    assert [%{"type" => "additional_tools", "role" => "developer", "tools" => ^tools} | _rest] = captured.json["input"]
  end

  defp turn_events(response_id, items), do: Enum.map(turn_payloads(response_id, items), &{&1["type"], &1})

  defp turn_payloads(response_id, items) do
    response = %{"id" => response_id, "object" => "response", "created_at" => 1_790_000_000, "model" => "provider-gpt-test-model", "status" => "completed", "output" => items, "usage" => %{"input_tokens" => 5, "output_tokens" => 3, "total_tokens" => 8}}

    item_events =
      items
      |> Enum.with_index()
      |> Enum.flat_map(fn {item, index} ->
        [%{"type" => "response.output_item.added", "output_index" => index, "item" => item}, %{"type" => "response.output_item.done", "output_index" => index, "item" => item}]
      end)

    [%{"type" => "response.created", "response" => %{response | "status" => "in_progress", "output" => []}}] ++ item_events ++ [%{"type" => "response.completed", "response" => response}]
  end

  defp websocket_turn!(setup, frame) do
    port = start_public_endpoint!()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"x-codex-turn-state", "public-ws-tool-search-#{System.unique_integer([:positive])}"}, {"openai-beta", "responses_websockets=2026-02-06"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
      {_conn, texts} = receive_until_terminal!(conn, websocket, ref, [])
      Enum.map(texts, &CodexPooler.JSON.decode!/1)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_until_terminal!(conn, websocket, ref, texts) do
    receive do
      message ->
        case Mint.WebSocket.stream(conn, message) do
          {:ok, conn, responses} ->
            {websocket, new_texts} = decode_texts(websocket, ref, responses)
            texts = texts ++ new_texts
            if Enum.any?(new_texts, &terminal_text?/1), do: {conn, texts}, else: receive_until_terminal!(conn, websocket, ref, texts)

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
end
