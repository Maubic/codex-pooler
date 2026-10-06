defmodule CodexPoolerWeb.Runtime.AttemptMetadataShapeTest do
  # The persisted shape of a completed attempt, pinned on every transport and both serving modes. The key lists
  # below were measured on the unmodified tree, then extended by exactly what the end_turn class and the stream
  # timing add: a `stream_timing` map on an upstream HTTP SSE attempt (relayed native and `/v1`, and the collected
  # non-streaming `/v1` body) and an `end_turn` field on the delivery receipt of a pushed `response.completed`.
  # Websocket attempts gain no timing and no other key; no request row changes at all. A future field has to be
  # added here with its own test, which is the point of pinning the full lists.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      auth: 2,
      gateway_setup: 1,
      native_text_input: 1,
      public_websocket_connect_with_request_headers!: 5,
      public_websocket_receive_text!: 3,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true

  @http_receipt ~w(end_turn frames_after_visible outcome pushed_at terminal_class transport)
  @websocket_receipt ~w(end_turn frames_after_visible highest_frame_class outcome pushed_at terminal_class transport)
  @timing ~w(connection first_event_ms first_visible_ms headers_ms)

  @native_http_attempt ~w(content_type downstream_delivery provider_credits_admission reasoning routing status_code stream_timing usage_observation)
  @v1_sse_attempt ~w(content_type downstream_delivery provider_credits_admission public_openai_responses_stream reasoning routing status_code stream_timing usage_observation)
  @v1_json_attempt ~w(content_type provider_credits_admission reasoning routing status_code stream_timing usage_observation)
  @websocket_attempt ~w(content_type downstream_delivery provider_credits_admission reasoning routing status_code upstream_transport upstream_websocket_connection)

  @native_request ~w(api_key effective_model endpoint key_prefix pricing quota_decision request_bytes request_content_type requested_model requested_stream reservation reservation_snapshot_inputs routing transport)
  @v1_request ~w(api_key effective_model endpoint key_prefix openai_compatibility pricing quota_decision request_bytes request_content_type requested_model requested_stream reservation reservation_snapshot_inputs routing transport)
  @native_websocket_request ~w(api_key codex_session_id codex_session_key effective_model endpoint key_prefix pricing quota_decision request_bytes requested_model requested_stream reservation reservation_snapshot_inputs routing transport)
  @v1_websocket_request ~w(api_key codex_session_id codex_session_key effective_model endpoint key_prefix openai_compatibility pricing quota_decision request_bytes requested_model requested_stream reservation reservation_snapshot_inputs routing transport)

  for mode <- ["full", "lite"] do
    test "#{mode} native HTTP SSE attempt: only stream_timing and the receipt's end_turn are new" do
      upstream = start_upstream(FakeUpstream.sse_stream(events(false), done: false))
      setup = serving_setup(upstream, unquote(mode))
      conn = build_conn() |> auth(setup)
      conn = if unquote(mode) == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn

      assert post(conn, "/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic"), "stream" => true}).status == 200

      {request, attempt} = settled_rows(setup)
      assert_keys(attempt.response_metadata, @native_http_attempt)
      assert_keys(attempt.response_metadata["downstream_delivery"], @http_receipt)
      assert_keys(attempt.response_metadata["stream_timing"], @timing)
      assert_keys(request.request_metadata, @native_request)
    end

    test "#{mode} /v1 SSE attempt: only stream_timing and the receipt's end_turn are new" do
      upstream = start_upstream(FakeUpstream.sse_stream(events(false)))
      setup = serving_setup(upstream, unquote(mode))

      assert build_conn() |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic", "stream" => true}) |> Map.fetch!(:status) == 200

      {request, attempt} = settled_rows(setup)
      assert_keys(attempt.response_metadata, @v1_sse_attempt)
      assert_keys(attempt.response_metadata["downstream_delivery"], @http_receipt)
      assert_keys(attempt.response_metadata["stream_timing"], @timing)
      assert_keys(request.request_metadata, @v1_request)
    end

    test "#{mode} /v1 non-streaming attempt: only stream_timing is new and no receipt appears" do
      upstream = start_upstream(FakeUpstream.sse_stream(events(false)))
      setup = serving_setup(upstream, unquote(mode))

      assert build_conn() |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic", "stream" => false}) |> Map.fetch!(:status) == 200

      {request, attempt} = settled_rows(setup)
      assert_keys(attempt.response_metadata, @v1_json_attempt)
      assert_keys(attempt.response_metadata["stream_timing"], @timing)
      assert_keys(request.request_metadata, @v1_request)
    end

    test "#{mode} native websocket attempt: only the receipt's end_turn is new, and it has no stream timing" do
      upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(events(false), &frame/1)))
      setup = serving_setup(upstream, unquote(mode))
      port = start_public_endpoint!()
      headers = if unquote(mode) == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"}], else: []
      {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/backend-api/codex/responses", headers)
      on_exit(fn -> Mint.HTTP.close(conn) end)

      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic"), "stream" => true, "store" => false}
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {_conn, _websocket, _terminal} = receive_terminal(conn, websocket, ref)

      {request, attempt} = await_settled_rows(setup)
      assert_keys(attempt.response_metadata, @websocket_attempt)
      assert_keys(attempt.response_metadata["downstream_delivery"], @websocket_receipt)
      assert_keys(request.request_metadata, @native_websocket_request)
    end

    test "#{mode} /v1 websocket attempt: only the receipt's end_turn is new, and it has no stream timing" do
      upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(events(false), &frame/1)))
      setup = serving_setup(upstream, unquote(mode))
      port = start_public_endpoint!()
      {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "shape-#{System.unique_integer([:positive])}", "/v1/responses", [{"openai-beta", "responses_websockets=2026-02-06"}])
      on_exit(fn -> Mint.HTTP.close(conn) end)

      frame = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => "synthetic", "store" => false, "generate" => true}
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
      {_conn, _websocket, _terminal} = receive_terminal(conn, websocket, ref)

      {request, attempt} = await_settled_rows(setup)
      assert_keys(attempt.response_metadata, @websocket_attempt)
      assert_keys(attempt.response_metadata["downstream_delivery"], @websocket_receipt)
      assert_keys(request.request_metadata, @v1_websocket_request)
    end
  end

  defp assert_keys(map, expected), do: assert(Enum.sort(Map.keys(map)) == Enum.sort(expected))

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp settled_rows(setup) do
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    {request, attempt}
  end

  # A websocket receipt is merged after the turn settled.
  defp await_settled_rows(setup, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 10_000

    case Repo.all(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id, select: {r, a})) do
      [{request, %Attempt{response_metadata: %{"downstream_delivery" => %{}}} = attempt}] ->
        {request, attempt}

      _pending ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk("the websocket attempt never carried a delivery receipt")

        receive do
        after
          20 -> await_settled_rows(setup, deadline)
        end
    end
  end

  defp receive_terminal(conn, websocket, ref, seen \\ 0) when seen < 12 do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    event = CodexPooler.JSON.decode!(text)

    if event["type"] in ["response.completed", "response.failed", "error"],
      do: {conn, websocket, event},
      else: receive_terminal(conn, websocket, ref, seen + 1)
  end

  defp frame({_type, payload}), do: CodexPooler.JSON.encode!(payload)

  defp events(end_turn) do
    response = %{"id" => "resp_shape_guard", "object" => "response", "created_at" => 1_790_000_000, "model" => "provider-gpt-test-model", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}

    [
      {"response.created", %{"type" => "response.created", "response" => %{"id" => "resp_shape_guard", "status" => "in_progress"}}},
      {"response.output_text.delta", %{"type" => "response.output_text.delta", "delta" => "synthetic delta"}},
      {"response.completed", %{"type" => "response.completed", "response" => Map.put(response, "end_turn", end_turn)}}
    ]
  end
end
