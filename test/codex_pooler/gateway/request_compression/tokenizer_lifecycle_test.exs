defmodule CodexPooler.Gateway.RequestCompression.TokenizerLifecycleTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Catalog.Model
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.RequestCompression
  alias CodexPooler.Gateway.RequestCompression.TokenCounter
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, RouteState}
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.Pools
  alias CodexPooler.Pools.{ModelServingOverride, RoutingSettings}
  alias CodexPooler.Repo

  @endpoint "/backend-api/codex/responses"
  @unsupported_model "unmapped-tokenizer-model"
  @models ["gpt-6-astra", "gpt-6-sol", "gpt-6-luna", @unsupported_model]
  @usage %{
    "input_tokens" => 17,
    "input_tokens_details" => %{"cached_tokens" => 5},
    "output_tokens" => 3,
    "total_tokens" => 20
  }
  @websocket_frame_timeout 15_000

  test "verified and unavailable tokenizers preserve the compression contract in all runtime modes" do
    for model_id <- @models, mode <- ["full", "lite"], transport <- ["http_sse", "websocket"] do
      payload = payload("boundary-#{mode}-#{transport}", model_id)
      body = CodexPooler.JSON.encode!(payload)
      route_class = if transport == "websocket", do: "proxy_websocket", else: "proxy_stream"

      options =
        %{transport: transport, upstream_endpoint: @endpoint}
        |> RequestOptions.build(@endpoint, payload)
        |> RequestOptions.put_transport(route_class: route_class, upstream_endpoint: @endpoint)
        |> RequestOptions.put_model_serving_mode(%{
          configured_mode: mode,
          effective_mode: mode,
          source: "override"
        })

      model = %Model{exposed_model_id: model_id, upstream_model_id: model_id}

      context = %Context{
        endpoint: @endpoint,
        payload: payload,
        model: model,
        request_options: options,
        route_class: route_class,
        route_state: %RouteState{
          visible_model: model,
          candidates: [],
          routing_settings: %RoutingSettings{request_compression_enabled: true}
        }
      }

      assert {compressed_body, compressed_options} =
               RequestCompression.maybe_compress(body, context, options)

      assert_output!(CodexPooler.JSON.decode!(compressed_body), model_id)

      assert %{
               "enabled" => true,
               "attempted" => true,
               "route_class" => ^route_class,
               "transport" => ^transport,
               "original_bytes" => original_bytes,
               "compressed_bytes" => compressed_bytes
             } = metadata = compressed_options.runtime.payload_compression

      assert original_bytes == byte_size(body)
      assert compressed_bytes == byte_size(compressed_body)
      assert_compression!(metadata, model_id)

      if model_id == @unsupported_model, do: assert(compressed_body == body)

      assert compressed_options ==
               RequestOptions.put_runtime_context(options, payload_compression: metadata)
    end
  end

  test "GPT-6 compression and unsupported-model passthrough settle Full and Lite HTTP SSE" do
    for model <- @models, mode <- ["full", "lite"] do
      response_id = "resp_tokenizer_http_#{mode}"
      completed = completed_event(response_id)
      upstream = start_upstream(FakeUpstream.sse_stream([completed]))
      setup = tokenizer_setup(upstream, mode, model)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      request_payload = payload("http-#{mode}", model) |> Map.put("stream", true)

      request_options =
        RequestOptions.build(
          %{
            request_id: "tokenizer-http-#{mode}-#{System.unique_integer([:positive])}",
            accepted_turn_state: "tokenizer-http-turn-#{mode}-#{System.unique_integer([:positive])}",
            client_ip: "127.0.0.1"
          },
          @endpoint,
          request_payload
        )

      assert {:ok, %{stream: stream}} =
               Gateway.execute(auth, @endpoint, request_payload, request_options)

      conn = build_conn() |> put_resp_content_type("text/event-stream") |> send_chunked(200)
      assert {:ok, conn} = stream.(conn)
      assert conn.resp_body == sse_chunk(completed) <> "data: [DONE]\n\n"

      assert_lifecycle!(setup, upstream, mode, "http_sse", "proxy_stream", response_id)
    end
  end

  test "GPT-6 compression and unsupported-model passthrough settle Full and Lite native websocket" do
    for model <- @models, mode <- ["full", "lite"] do
      response_id = "resp_tokenizer_websocket_#{mode}"

      upstream =
        start_upstream(
          FakeUpstream.json_response(%{
            "id" => response_id,
            "object" => "response",
            "status" => "completed",
            "output" => [],
            "usage" => @usage
          })
        )

      setup = tokenizer_setup(upstream, mode, model)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

      {:ok, session} =
        Websocket.start_codex_session(
          auth,
          accepted_turn_state: "tokenizer-websocket-turn-#{mode}-#{System.unique_integer([:positive])}"
        )

      request_options =
        %{
          request_id: "tokenizer-websocket-#{mode}-#{System.unique_integer([:positive])}",
          client_ip: "127.0.0.1",
          codex_session: session
        }
        |> RequestOptions.for_websocket()

      raw_payload =
        payload("websocket-#{mode}", model)
        |> Map.merge(%{"type" => "response.create", "stream" => true, "generate" => true})
        |> CodexPooler.JSON.encode!()

      assert :ok =
               Gateway.execute_websocket_response(auth, raw_payload, request_options, fn frame ->
                 send(self(), {:provider_frame, frame})
               end)

      frame = receive_provider_frame!()
      assert %{"id" => ^response_id, "usage" => @usage} = CodexPooler.JSON.decode!(frame)

      assert_lifecycle!(setup, upstream, mode, "websocket", "proxy_websocket", response_id)
    end
  end

  defp tokenizer_setup(upstream, mode, model) do
    setup =
      gateway_setup(upstream,
        exposed_model_id: model,
        upstream_model_id: model,
        pricing_ref: model,
        display_name: "Tokenizer test model"
      )

    setup.pool
    |> Pools.ensure_routing_settings()
    |> Ecto.Changeset.change(request_compression_enabled: true)
    |> Repo.update!()

    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.insert!(%ModelServingOverride{
      pool_id: setup.pool.id,
      exposed_model_id: model,
      mode: mode,
      created_at: timestamp,
      updated_at: timestamp
    })

    setup
  end

  defp assert_lifecycle!(setup, upstream, mode, transport, route_class, response_id) do
    model = setup.model.upstream_model_id
    assert [captured] = FakeUpstream.requests(upstream)
    assert captured.path == @endpoint
    assert captured.json["model"] == model
    assert_output!(captured.json, model)

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "succeeded"
    assert request.usage_status == "usage_known"
    assert request.transport == transport
    assert request.endpoint == @endpoint

    assert get_in(request.request_metadata, ["routing", "model_serving_mode"]) == mode

    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.status == "succeeded"
    assert attempt.usage_status == "usage_known"
    assert attempt.upstream_model_id == model

    assert %{
             "enabled" => true,
             "attempted" => true,
             "route_class" => ^route_class,
             "transport" => ^transport,
             "compressed_bytes" => compressed_bytes
           } = metadata = attempt.response_metadata["payload_compression"]

    assert compressed_bytes == byte_size(captured.body)
    assert_compression!(metadata, model)
    refute inspect(metadata) =~ output_fixture()

    assert [turn] = Repo.all(from(t in CodexTurn, where: t.request_id == ^request.id))
    assert turn.status == "succeeded"
    assert turn.final_attempt_id == attempt.id

    assert settlement =
             Repo.get_by!(LedgerEntry, request_id: request.id, entry_kind: "settlement")

    assert settlement.usage_status == "usage_known"
    assert settlement.input_tokens == 17
    assert settlement.cached_input_tokens == 5
    assert settlement.output_tokens == 3
    assert settlement.total_tokens == 20
    assert settlement.attempt_id == attempt.id

    refute inspect({request.request_metadata, attempt.response_metadata}) =~ response_id

    assert Enum.sort(settlement_entry_kinds(request.id)) == [
             "release",
             "reservation",
             "settlement"
           ]
  end

  defp payload(label, model) do
    %{
      "model" => model,
      "input" => [
        %{
          "type" => "function_call",
          "call_id" => "call_#{label}",
          "name" => "synthetic_tool",
          "arguments" => "{}"
        },
        %{
          "type" => "function_call_output",
          "call_id" => "call_#{label}",
          "output" => output_fixture()
        }
      ]
    }
  end

  defp output_fixture do
    1..80
    |> Enum.map(&%{"enabled" => true, "id" => &1})
    |> then(&%{"rows" => &1})
    |> CodexPooler.JSON.encode!(pretty: true)
  end

  defp completed_event(response_id) do
    {"response.completed",
     %{
       "type" => "response.completed",
       "response" => %{
         "id" => response_id,
         "object" => "response",
         "status" => "completed",
         "output" => [],
         "usage" => @usage
       }
     }}
  end

  defp sse_chunk({event, payload}) do
    "event: #{event}\ndata: #{CodexPooler.JSON.encode!(payload)}\n\n"
  end

  defp settlement_entry_kinds(request_id) do
    Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request_id, select: entry.entry_kind))
  end

  defp receive_provider_frame! do
    assert_receive {:provider_frame, frame}, @websocket_frame_timeout

    if StreamProtocol.internal_control_event?(frame) do
      receive_provider_frame!()
    else
      frame
    end
  end

  defp assert_output!(payload, @unsupported_model) do
    assert payload["input"] |> List.last() |> Map.fetch!("output") == output_fixture()
  end

  defp assert_output!(payload, model) do
    output = payload["input"] |> List.last() |> Map.fetch!("output")
    assert CodexPooler.JSON.decode!(output) == CodexPooler.JSON.decode!(output_fixture())
    assert byte_size(output) < byte_size(output_fixture())
    assert {:ok, original_tokens, _} = TokenCounter.count(model, output_fixture())
    assert {:ok, compressed_tokens, _} = TokenCounter.count(model, output)
    assert compressed_tokens < original_tokens
  end

  defp assert_compression!(metadata, @unsupported_model) do
    assert %{"status" => "skipped", "reason" => "tokenizer_unavailable", "candidate_count" => 0, "compressed_count" => 0, "skipped_count" => 0} = metadata
    assert metadata["original_bytes"] == metadata["compressed_bytes"]
    refute Map.has_key?(metadata, "original_tokens")
    refute Map.has_key?(metadata, "compressed_tokens")
    refute Map.has_key?(metadata, "saved_tokens")
  end

  defp assert_compression!(metadata, _model) do
    assert %{"status" => "compressed", "candidate_count" => 1, "compressed_count" => 1, "skipped_count" => 0, "token_count_mode" => "exact", "strategies" => ["json_document_lossless"]} = metadata
    assert metadata["original_bytes"] > metadata["compressed_bytes"]
    assert metadata["original_tokens"] > metadata["compressed_tokens"]
    assert metadata["saved_tokens"] == metadata["original_tokens"] - metadata["compressed_tokens"]
  end
end
