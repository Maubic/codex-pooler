defmodule CodexPoolerWeb.V1.ResponsesWebsocketProviderRefusalRelayTest do
  # The Codex backend refuses a top-level parameter it does not accept with
  # `400 {"detail": "Unsupported parameter: <name>"}` over HTTP and, on the
  # websocket, with the same text in a codeless wrapped error frame; it then
  # answers nothing more on that connection and drops it without a Close frame
  # about 3 s later (direct probe 2026-10-06, findings#333, Full and Lite).
  # The HTTP answer relayed `unsupported_parameter` with the parameter, while
  # the websocket sent the redacted `upstream_status`, and a create sent
  # behind the refusal went out on the doomed connection and failed
  # `502 upstream_request_failed` when it dropped. Each create now gets the
  # HTTP answer's error, and the refused connection is retired so the next
  # create opens a fresh one. `FakeUpstream.provider_refusal/1` drops the
  # connection when the next request frame arrives, so a create sent on it
  # fails the same way without waiting for the provider's 3 s.
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
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [assert_single_native_turn_terminal!: 2, collect_native_turn_frames!: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario

  @frame_timeout_ms 15_000
  @param "zz_probe_unknown_param"
  @refusal "Unsupported parameter: #{@param}"
  @relayed %{
    "type" => "invalid_request_error",
    "code" => "unsupported_parameter",
    "param" => @param,
    "message" => "upstream rejected parameter #{@param} (unsupported_parameter)"
  }

  for topology <- [:direct, :local_owner], mode <- ["full", "lite"] do
    @tag topology: topology, mode: mode
    test "a #{mode} create the provider refuses reaches the #{topology} public websocket with the HTTP answer's code and param",
         %{conn: conn, topology: topology, mode: mode} do
      http_error = http_answer!(conn, mode)
      assert http_error == @relayed

      if topology == :local_owner, do: enable_owner_forwarding!()
      # provenance: observed findings#333 direct websocket probe (codeless 400 `Unsupported parameter: <name>`, then the connection dropped)
      upstream = start_upstream(FakeUpstream.repeat_last([FakeUpstream.provider_refusal(@refusal)]))
      setup = serve!(gateway_setup(upstream), mode)
      client = connect!(setup)

      try do
        client = send_create!(client, setup, "s1")
        {_client, [text]} = receive_terminals!(client, 1)

        assert %{"type" => "error", "status" => 400, "stream_id" => "s1", "error" => ^http_error} = CodexPooler.JSON.decode!(text)
        assert_refused_rows!(setup, 1)
      after
        Mint.HTTP.close(client.conn)
      end
    end

    @tag topology: topology, mode: mode
    test "#{mode} creates pipelined behind a refusal each get their own refusal on a fresh connection, #{topology}", %{topology: topology, mode: mode} do
      if topology == :local_owner, do: enable_owner_forwarding!()
      # provenance: observed findings#333 direct websocket probe (the provider ignores a create sent after its refusal and drops the connection)
      upstream = start_upstream(FakeUpstream.repeat_last([FakeUpstream.provider_refusal(@refusal)]))
      setup = serve!(gateway_setup(upstream), mode)
      client = connect!(setup)

      try do
        client = Enum.reduce(["s1", "s2", "s3"], client, &send_create!(&2, setup, &1))
        {_client, texts} = receive_terminals!(client, 3)
        events = Enum.map(texts, &CodexPooler.JSON.decode!/1)

        assert Enum.map(events, &{&1["type"], &1["status"], &1["stream_id"], &1["error"]}) == [
                 {"error", 400, "s1", @relayed},
                 {"error", 400, "s2", @relayed},
                 {"error", 400, "s3", @relayed}
               ]

        # No create went out on a connection the provider had refused a request on.
        requests = FakeUpstream.requests(upstream)
        assert length(requests) == 3
        assert requests |> Enum.map(& &1.websocket_connection_id) |> Enum.uniq() |> length() == 3
        assert_refused_rows!(setup, 3)
      after
        Mint.HTTP.close(client.conn)
      end
    end
  end

  for topology <- [:direct, :local_owner] do
    @tag topology: topology
    test "a valid create sent behind a refused one completes on a fresh connection, #{topology}", %{topology: topology} do
      if topology == :local_owner, do: enable_owner_forwarding!()

      upstream =
        start_upstream(
          # provenance: observed findings#333 direct websocket probe (refusal, then the connection dropped), then a synthetic completion
          FakeUpstream.strict_sequence([
            FakeUpstream.provider_refusal(@refusal),
            FakeUpstream.websocket_text_frames([completed_frame("resp_after_refusal")])
          ])
        )

      setup = gateway_setup(upstream)
      client = connect!(setup)

      try do
        client = client |> send_create!(setup, "refused") |> send_create!(setup, "valid")
        {_client, texts} = receive_terminals!(client, 2)

        assert [%{"type" => "error", "stream_id" => "refused", "error" => @relayed}, %{"type" => "response.completed", "stream_id" => "valid"} = completed] =
                 Enum.map(texts, &CodexPooler.JSON.decode!/1)

        assert completed["response"]["id"] == "resp_after_refusal"
        assert [first, second] = FakeUpstream.requests(upstream)
        refute first.websocket_connection_id == second.websocket_connection_id
        FakeUpstream.verify!(upstream)
      after
        Mint.HTTP.close(client.conn)
      end
    end
  end

  # The native websocket had the same gap: the refusal went out as the wrapped
  # 400 with `invalid_request` and no param, and the socket's next turn reused
  # the refused connection.
  for topology <- [:direct, :local_owner] do
    @tag topology: topology
    test "the native #{topology} websocket relays the refusal with its code and param and retires the refused connection", %{topology: topology} do
      if topology == :local_owner, do: enable_owner_forwarding!()

      upstream =
        start_upstream(
          # provenance: observed findings#333 direct websocket probe (refusal, then the connection dropped), then a synthetic completion
          FakeUpstream.strict_sequence([
            FakeUpstream.provider_refusal(@refusal),
            FakeUpstream.websocket_text_frames([completed_frame("resp_native_after_refusal")])
          ])
        )

      setup = gateway_setup(upstream)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      {:ok, state} = CodexResponsesSocket.init(%{auth: auth, opts: %{request_id: "native-refusal-relay-#{topology}", accepted_turn_state: Ecto.UUID.generate(), client_ip: "127.0.0.1"}})

      try do
        {state, refused} = native_turn!(state, setup, "refused native turn")

        assert assert_single_native_turn_terminal!(refused, "error") == %{"type" => "error", "status" => 400, "error" => @relayed}

        {state, completed} = native_turn!(state, setup, "valid native turn")
        assert %{"response" => %{"id" => "resp_native_after_refusal"}} = assert_single_native_turn_terminal!(completed, "response.completed")

        assert [first, second] = FakeUpstream.requests(upstream)
        refute first.websocket_connection_id == second.websocket_connection_id
        assert :ok = CodexResponsesSocket.terminate(:closed, state)
        FakeUpstream.verify!(upstream)
      after
        CodexResponsesSocket.terminate(:closed, state)
      end
    end
  end

  defp native_turn!(state, setup, text) do
    payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input(text), "stream" => true, "generate" => true})
    assert {:ok, state} = CodexResponsesSocket.handle_in({payload, [opcode: :text]}, state)
    collect_native_turn_frames!(state)
  end

  # The HTTP answer of the same provider refusal on `/v1/responses`.
  defp http_answer!(conn, mode) do
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.provider_refusal(@refusal)]))
    setup = serve!(gateway_setup(upstream), mode)
    response = conn |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic refused create", "stream" => true})
    %{"error" => error} = json_response(response, 400)
    FakeUpstream.verify!(upstream)
    error
  end

  # Each refused turn settles as the client's error: no demotion, no circuit
  # failure, and the attempt records the rejection the HTTP answer records.
  defp assert_refused_rows!(setup, count) do
    requests = OwnerCrashAfterSendScenario.await_settled!(setup, count)
    assert Enum.all?(requests, &(&1.status == "failed"))

    attempts = Repo.all(from(attempt in Attempt, where: attempt.request_id in ^Enum.map(requests, & &1.id)))
    assert length(attempts) == count

    for attempt <- attempts do
      assert attempt.response_metadata["rejection_message_class"] == "unsupported_parameter"
      assert attempt.response_metadata["rejection_error_param"] == @param
      refute Map.has_key?(attempt.response_metadata, "rejection_error_code")
    end

    assert Repo.aggregate(BridgeDemotion, :count) == 0
    assert Repo.aggregate(RoutingCircuitState, :count) == 0
  end

  defp serve!(setup, mode) do
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode})
    setup
  end

  defp completed_frame(response_id) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.completed",
      "response" => %{"id" => response_id, "object" => "response", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}
    })
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
