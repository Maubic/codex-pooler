defmodule CodexPooler.Gateway.ToolOutputPreservationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import Ecto.Query

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Pools
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPooler.ToolOutputPreservationFixtures, as: Fixtures
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, as: OwnerSupport

  @usage %{"input_tokens" => 17, "input_tokens_details" => %{"cached_tokens" => 5}, "output_tokens" => 3, "total_tokens" => 20}
  @upstream_endpoint "/backend-api/codex/responses"
  @detection_timeout_ms 15_000

  test "native HTTP preserves unprotected pretty JSON output bytes", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_preserved", "status" => "completed", "output" => []}))
    setup = gateway_setup(upstream, exposed_model_id: "gpt-4o", upstream_model_id: "gpt-4o", pricing_ref: "gpt-4o")
    set_legacy_column!(setup.pool, true)
    output = CodexPooler.JSON.encode!(%{"rows" => Enum.map(1..64, &%{"id" => &1, "value" => "synthetic-value"})}, pretty: true)

    conn = conn |> auth(setup) |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => [%{"type" => "function_call", "call_id" => "sample", "name" => "sample_tool", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "sample", "output" => output}]})

    assert %{"id" => "resp_preserved"} = json_response(conn, 200)
    assert [captured] = FakeUpstream.requests(upstream)
    observed = captured.json["input"] |> List.last() |> Map.fetch!("output")
    assert :crypto.hash(:sha256, observed) == :crypto.hash(:sha256, output)
  end

  for mode <- ["full", "lite"], legacy <- [true, false], route <- ["/backend-api/codex/responses", "/backend-api/codex/v1/responses", "/v1/responses", "/v1/chat/completions", "/backend-api/codex/responses/compact"] do
    @mode mode
    @legacy legacy
    @route route
    test "#{mode} #{@route} preserves corpus with legacy column #{legacy}", %{conn: conn} do
      compact? = String.ends_with?(@route, "/compact")
      response = response()
      response = if compact?, do: Map.put(response, "object", "response.compaction"), else: response
      behavior = if compact?, do: FakeUpstream.json_response(response), else: FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => response}}])
      upstream = start_upstream(behavior)
      setup = preservation_setup(upstream, @mode, @legacy, compact?)
      corpus = Fixtures.corpus()
      payload = %{"model" => setup.model.exposed_model_id, "input" => Fixtures.input(corpus), "stream" => true}
      payload = if compact?, do: Map.delete(payload, "stream"), else: Map.put(payload, "tools", tools())
      payload = if @route == "/v1/chat/completions", do: chat_payload(payload, corpus), else: payload
      assert byte_size(CodexPooler.JSON.encode!(payload)) > 1_048_576
      assert length(corpus) > 50
      conn = conn |> auth(setup) |> post(@route, payload)
      assert conn.status == 200
      assert conn.resp_body =~ "resp_preservation"
      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.method == "POST"
      assert_corpus!(captured.json, corpus)
      unless compact?, do: assert_serving_tools!(captured.json, @mode)
      assert_lifecycle!(setup, @mode)
    end
  end

  for mode <- ["full", "lite"], public? <- [false, true] do
    @mode mode
    @public public?
    test "#{mode} #{if public?, do: "public", else: "native"} websocket preserves corpus" do
      upstream = start_upstream(FakeUpstream.json_response(response()))
      setup = preservation_setup(upstream, @mode, true, false)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, session} = Websocket.start_codex_session(auth, accepted_turn_state: "preservation-#{System.unique_integer([:positive])}")
      options = RequestOptions.for_websocket(%{request_id: "preservation-#{System.unique_integer([:positive])}", codex_session: session, client_ip: "127.0.0.1"})
      options = if @public, do: options |> RequestOptions.put_openai_compatibility(public_openai_responses_stream: true) |> RequestOptions.put_continuity(accepted_turn_state: nil) |> RequestOptions.mark_openai_compatibility_origin("/v1/responses", @upstream_endpoint), else: options
      corpus = Fixtures.corpus()
      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => Fixtures.input(corpus), "tools" => tools(), "stream" => true, "generate" => true}
      assert :ok = Gateway.execute_websocket_response(auth, CodexPooler.JSON.encode!(payload), options, fn frame -> send(self(), {:provider_frame, frame}) end)
      assert %{"id" => "resp_preservation", "usage" => @usage} = receive_provider_frame!() |> CodexPooler.JSON.decode!()
      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.method == "WEBSOCKET"
      assert_corpus!(captured.json, corpus)
      assert_serving_tools!(captured.json, @mode)
      assert_lifecycle!(setup, @mode)
    end
  end

  test "content oracle detects corruption despite a successful provider response", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.json_response(response()))
    setup = preservation_setup(upstream, "full", false, false)
    corpus = Enum.take(Fixtures.corpus(), 1)
    payload = %{"model" => setup.model.exposed_model_id, "input" => Fixtures.input(corpus)}
    conn = conn |> auth(setup) |> post(@upstream_endpoint, payload)
    assert conn.status == 200
    assert [captured] = FakeUpstream.requests(upstream)
    assert_corpus!(captured.json, corpus)
    corrupted = update_in(captured.json, ["input", Elixir.Access.at(1), "output"], &(&1 <> "changed"))
    assert_raise ExUnit.AssertionError, fn -> assert_corpus!(corrupted, corpus) end
  end

  for mode <- ["full", "lite"] do
    @mode mode
    test "#{mode} public websocket ingress preserves corpus after a real upgrade" do
      upstream = start_upstream(FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => response()}}]))
      setup = preservation_setup(upstream, @mode, true, false)
      {_server, port} = start_public_endpoint_with_server!()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, "preservation-#{System.unique_integer([:positive])}", "/v1/responses")
      corpus = Fixtures.corpus()
      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => Fixtures.input(corpus), "tools" => tools(), "stream" => true, "generate" => true}

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
        {_conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_preservation", "usage" => @usage}} = CodexPooler.JSON.decode!(frame)
        assert [captured] = FakeUpstream.requests(upstream)
        assert captured.method == "WEBSOCKET"
        assert_corpus!(captured.json, corpus)
        assert_serving_tools!(captured.json, @mode)
      after
        Mint.HTTP.close(conn)
      end
    end

    test "#{mode} HTTP to websocket bridge preserves corpus", %{conn: conn} do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
      upstream = start_upstream(FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => response()}}]))
      setup = preservation_setup(upstream, @mode, true, false)
      corpus = Fixtures.corpus()
      payload = %{"model" => setup.model.exposed_model_id, "input" => Fixtures.input(corpus), "tools" => tools(), "stream" => true}
      conn = conn |> auth(setup) |> put_req_header("x-session-id", "preservation-#{System.unique_integer([:positive])}") |> post("/v1/responses", payload)
      assert conn.status == 200
      assert conn.resp_body =~ "resp_preservation"
      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.method == "WEBSOCKET"
      assert_corpus!(captured.json, corpus)
      assert_serving_tools!(captured.json, @mode)
      assert_lifecycle!(setup, @mode)
    end

    test "#{mode} owner-forwarded native websocket preserves corpus" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
      upstream = start_upstream(FakeUpstream.json_response(response()))
      setup = preservation_setup(upstream, @mode, true, false)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, state} = CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: "preservation-#{System.unique_integer([:positive])}", accepted_turn_state: "preservation-#{System.unique_integer([:positive])}", client_ip: "127.0.0.1"}})
      corpus = Fixtures.corpus()
      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => Fixtures.input(corpus), "tools" => tools(), "stream" => true, "generate" => true}
      cleanup_key = {__MODULE__, make_ref()}
      Process.put(cleanup_key, state)

      try do
        assert {:ok, state} = CodexResponsesSocket.handle_in({CodexPooler.JSON.encode!(payload), [opcode: :text]}, state)
        Process.put(cleanup_key, state)
        assert {:push, {:text, frame}, state} = OwnerSupport.receive_owner_socket_push(state)
        Process.put(cleanup_key, state)
        assert %{"id" => "resp_preservation"} = CodexPooler.JSON.decode!(frame)
        assert {:ok, state} = receive_socket_turn_done(state)
        Process.put(cleanup_key, state)
        assert_socket_response_tasks_released!()
        assert [captured] = FakeUpstream.requests(upstream)
        assert captured.method == "WEBSOCKET"
        assert_corpus!(captured.json, corpus)
        assert_serving_tools!(captured.json, @mode)
        assert_lifecycle!(setup, @mode)
      after
        CodexResponsesSocket.terminate(:closed, Process.delete(cleanup_key))
      end
    end
  end

  defp preservation_setup(upstream, mode, legacy, compact?) do
    setup = gateway_setup(upstream, exposed_model_id: "gpt-4o", upstream_model_id: "gpt-4o", pricing_ref: "gpt-4o", compact?: compact?)
    set_legacy_column!(setup.pool, legacy)
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    setup
  end

  defp set_legacy_column!(pool, value) do
    Pools.ensure_routing_settings(pool)
    Repo.query!("UPDATE pool_routing_settings SET request_compression_enabled = $1 WHERE pool_id = $2", [value, Ecto.UUID.dump!(pool.id)])
  end

  defp response, do: %{"id" => "resp_preservation", "object" => "response", "status" => "completed", "output" => [], "usage" => @usage}

  defp tools, do: [%{"type" => "function", "name" => "synthetic_tool", "parameters" => %{"type" => "object", "properties" => %{}}}]

  defp assert_serving_tools!(payload, "full") do
    assert [%{"type" => "function", "name" => "synthetic_tool"}] = payload["tools"]
    refute Enum.any?(payload["input"], &(&1["type"] == "additional_tools"))
  end

  defp assert_serving_tools!(payload, "lite") do
    refute Map.has_key?(payload, "tools")
    assert [%{"type" => "additional_tools", "role" => "developer", "tools" => [%{"type" => "function", "name" => "synthetic_tool"}]} | _] = payload["input"]
  end

  defp chat_payload(payload, corpus) do
    messages =
      Enum.flat_map(corpus, fn {id, output} ->
        [%{"role" => "assistant", "tool_calls" => [%{"id" => id, "type" => "function", "function" => %{"name" => "synthetic_tool", "arguments" => "{}"}}]}, %{"role" => "tool", "tool_call_id" => id, "content" => output}]
      end)

    chat_tools = Enum.map(tools(), fn tool -> %{"type" => "function", "function" => Map.delete(tool, "type")} end)
    payload |> Map.delete("input") |> Map.put("messages", messages) |> Map.put("tools", chat_tools)
  end

  defp assert_corpus!(payload, corpus) do
    correlation = payload["input"] |> Enum.filter(&(&1["type"] in ["function_call", "function_call_output"])) |> Enum.map(&{&1["type"], &1["call_id"]})
    assert correlation == Enum.flat_map(corpus, fn {id, _output} -> [{"function_call", id}, {"function_call_output", id}] end)
    outputs = payload["input"] |> Enum.filter(&(&1["type"] == "function_call_output")) |> Enum.map(&{&1["call_id"], output_text(&1["output"])})
    identical_bytes? = outputs === corpus
    observed = Enum.map(outputs, fn {id, output} -> {id, Fixtures.fingerprint(output)} end)
    assert observed == Enum.map(corpus, fn {id, output} -> {id, Fixtures.fingerprint(output)} end)
    assert identical_bytes?
  end

  defp output_text(output) when is_binary(output), do: output
  defp output_text(parts) when is_list(parts), do: Enum.map_join(parts, &Map.fetch!(&1, "text"))

  defp assert_lifecycle!(setup, mode) do
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "succeeded"
    assert request.usage_status == "usage_known"
    assert get_in(request.request_metadata, ["routing", "model_serving_mode"]) == mode
    refute Map.has_key?(request.request_metadata, "payload_compression")
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.status == "succeeded"
    refute Map.has_key?(attempt.response_metadata, "payload_compression")
    settlement = Repo.get_by!(LedgerEntry, request_id: request.id, entry_kind: "settlement")
    assert {settlement.input_tokens, settlement.cached_input_tokens, settlement.output_tokens, settlement.total_tokens} == {17, 5, 3, 20}
    assert settlement.attempt_id == attempt.id
    assert Enum.sort(Repo.all(from(e in LedgerEntry, where: e.request_id == ^request.id, select: e.entry_kind))) == ["release", "reservation", "settlement"]
  end

  defp receive_provider_frame! do
    assert_receive {:provider_frame, frame}, @detection_timeout_ms
    if StreamProtocol.internal_control_event?(frame), do: receive_provider_frame!(), else: frame
  end
end
