defmodule CodexPoolerWeb.Runtime.NativeToolMetadataCompatibilityTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Access
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Websocket, as: Gateway

  @moduletag capture_log: true

  for mode <- ["full", "lite"], transport <- [:gzip, :zstd, :websocket] do
    test "#{mode} #{transport} preserves bounded executed metadata and expanded tool schemas", %{conn: conn} do
      mode = unquote(mode)
      transport = unquote(transport)
      terminal = %{"id" => "resp_metadata_compatibility", "object" => "response", "usage" => %{"input_tokens" => 4, "output_tokens" => 1, "total_tokens" => 5}}
      upstream = start_upstream(FakeUpstream.json_response(terminal))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, mode)
      {input, tools} = history()
      payload = %{"model" => setup.model.exposed_model_id, "input" => input, "tools" => tools, "stream" => false}
      encoded = CodexPooler.JSON.encode!(payload)
      assert byte_size(encoded) > 128 * 1024
      assert byte_size(encoded) < 2 * 1024 * 1024

      dispatch!(transport, conn, setup, payload, encoded)

      assert [request] = FakeUpstream.requests(upstream)
      observed = request.json["input"]

      if mode == "full" do
        assert observed == input
        assert request.json["tools"] == tools
      else
        assert [%{"type" => "additional_tools", "tools" => ^tools} | ^input] = observed
        refute Map.has_key?(request.json, "tools")
      end

      # Retained analytics can change when Codex reloads or reduces history;
      # resume equivalence ignores them but must still distinguish tool output.
      semantic = :crypto.hash(:sha256, "metadata-compatibility")
      replay = payload |> CodexPooler.JSON.encode!() |> CodexPooler.JSON.decode!()
      assert {:ok, replay_digest} = WebsocketTurnIdentity.replay_claim_digest(semantic, payload)
      assert {:ok, ^replay_digest} = WebsocketTurnIdentity.replay_claim_digest(semantic, replay)
      stripped = Enum.map(input, &Map.delete(&1, "internal_chat_message_metadata_passthrough"))
      assert {:ok, digest} = WebsocketTurnIdentity.http_resume_input_digest(semantic, input)
      assert {:ok, ^digest} = WebsocketTurnIdentity.http_resume_input_digest(semantic, stripped)
      altered = List.update_at(stripped, 1, &Map.put(&1, "output", "changed synthetic result"))
      assert {:ok, changed_digest} = WebsocketTurnIdentity.http_resume_input_digest(semantic, altered)
      refute digest == changed_digest
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  defp dispatch!(transport, conn, setup, payload, encoded) do
    case transport do
      :websocket ->
        {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
        {:ok, session} = Gateway.start_codex_session(auth, %{accepted_turn_state: "metadata-#{transport}"})
        frame = payload |> Map.put("type", "response.create") |> Map.put("stream", true) |> CodexPooler.JSON.encode!()
        assert :ok = execute_websocket_response(auth, frame, %{request_id: "metadata-#{transport}", codex_session: session}, fn _frame -> :ok end)

      encoding when encoding in [:gzip, :zstd] ->
        compressed = if encoding == :gzip, do: :zlib.gzip(encoded), else: encoded |> :zstd.compress() |> IO.iodata_to_binary()
        conn = conn |> put_req_header("authorization", setup.authorization) |> put_req_header("content-type", "application/json") |> put_req_header("content-encoding", Atom.to_string(encoding)) |> post("/backend-api/codex/responses", compressed)
        assert json_response(conn, 200)["id"] == "resp_metadata_compatibility"
    end
  end

  defp history do
    # High-entropy synthetic metadata avoids triggering the independent
    # decompression-ratio guard and exceeds the former 128 KiB client budget.
    large = for n <- 1..5000, into: "", do: Base.encode16(:crypto.hash(:sha256, Integer.to_string(n)), case: :lower)
    access = %{"resources" => [%{"uri" => "https://example.com/resource", "access" => "read"}]}

    calls = [
      %{"name" => "sample_lookup", "arguments" => %{"query" => "sample"}, "tool_result_sources" => [%{"type" => "resource", "id" => "sample-resource"}], "tool_result_metadata" => %{"openai/resource_access" => access, "synthetic_details" => large}},
      %{"name" => "sample_large_call", "arguments" => %{"_codex_executed_tool_call_truncated" => %{"original_bytes" => 9000, "max_bytes" => 8192}}, "tool_result_metadata" => "omitted_due_to_size_limit (overage_bytes=1024)"}
    ]

    metadata = %{"cell_id" => "synthetic-cell", "executed_tool_calls" => calls}

    input = [
      %{"type" => "function_call", "call_id" => "synthetic-call", "name" => "sample_lookup", "arguments" => "{}"},
      %{"type" => "function_call_output", "call_id" => "synthetic-call", "output" => "synthetic result", "internal_chat_message_metadata_passthrough" => metadata},
      %{"type" => "function_call", "call_id" => "synthetic-wait", "name" => "sample_wait", "arguments" => "{}"},
      %{"type" => "function_call_output", "call_id" => "synthetic-wait", "output" => "synthetic later wait", "internal_chat_message_metadata_passthrough" => %{"cell_id" => "synthetic-cell", "executed_tool_calls" => []}}
    ]

    properties = for n <- 1..180, into: %{}, do: {"field_#{n}", %{"type" => "string", "description" => "Synthetic parameter guidance " <> binary_part(large, n * 64, 128)}}
    tools = [%{"type" => "function", "name" => "sample_lookup", "parameters" => %{"type" => "object", "properties" => properties}}]
    {input, tools}
  end
end
