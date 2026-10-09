defmodule CodexPoolerWeb.Runtime.ReasoningUsageValidationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [curl_json_request!: 4, gateway_setup: 2, native_text_input: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Events
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @detection_budget 15_000

  # One node, one synthetic assignment and a real loopback downstream and upstream.
  # HTTP JSON, HTTP SSE and websocket each exercise their own finalization path.
  for mode <- ~w(full lite), transport <- [:compact_json, :json, :sse, :websocket], reasoning <- [2, 3] do
    test "#{mode} #{transport} reasoning #{reasoning} against output 2 settles consistently" do
      assert_surface!(unquote(mode), unquote(transport), unquote(reasoning))
    end
  end

  for mode <- ~w(full lite), level <- [:root, :direct], shape <- [:null, :scalar, :array, :malformed, :valid, :absent] do
    test "#{mode} compact JSON #{level} #{shape} usage retains envelope precedence" do
      assert_precedence_surface!(unquote(mode), :compact_json, unquote(level), unquote(shape))
    end
  end

  for mode <- ~w(full lite), transport <- [:sse, :websocket], shape <- [:null, :valid, :absent] do
    test "#{mode} #{transport} #{shape} root usage keeps the stream aggregate contract" do
      assert_precedence_surface!(unquote(mode), unquote(transport), :root, unquote(shape))
    end
  end

  defp assert_precedence_surface!(mode, transport, level, shape) do
    response = precedence_response(transport, level, shape)
    {request, attempt, settlement, reservation} = run_surface!(mode, transport, response)
    known? = shape == :valid or (shape == :absent and transport == :compact_json)
    expected_status = if known?, do: "usage_known", else: "usage_unknown"
    assert {request.usage_status, attempt.usage_status, settlement.usage_status} == {expected_status, expected_status, expected_status}

    if known? do
      expected_total = if shape == :valid, do: 12, else: 36
      expected_cost = if shape == :valid, do: 140, else: 420
      assert settlement.total_tokens == expected_total
      assert Decimal.equal?(settlement.settled_cost_micros, Decimal.new(expected_cost))
      assert settlement.details["estimated_from_reserve"] == false
    else
      assert settlement.details["settled_cost_micros"] == nil
      assert settlement.details["estimated_from_reserve"] == true
      assert settlement.total_tokens == reservation.total_tokens
    end
  end

  defp precedence_response(transport, level, shape) do
    nested = %{"input_tokens" => 30, "output_tokens" => 6, "total_tokens" => 36}
    item = %{"type" => "compaction", "encrypted_content" => "synthetic-encrypted-compaction", "response" => %{"usage" => nested}}
    response = %{"id" => "resp_usage_precedence", "object" => if(transport == :compact_json, do: "response.compaction", else: "response"), "status" => "completed", "output" => [item]}
    put_precedence_usage(response, level, precedence_usage(shape, nested))
  end

  defp precedence_usage(:null, _nested), do: nil
  defp precedence_usage(:scalar, _nested), do: 42
  defp precedence_usage(:array, nested), do: [nested]
  defp precedence_usage(:malformed, _nested), do: %{"input_tokens" => -1}
  defp precedence_usage(:valid, _nested), do: %{"input_tokens" => 10, "output_tokens" => 2, "total_tokens" => 12}
  defp precedence_usage(:absent, _nested), do: :absent

  defp put_precedence_usage(response, :root, :absent), do: response
  defp put_precedence_usage(response, :direct, :absent), do: Map.put(response, "response", %{})
  defp put_precedence_usage(response, :root, usage), do: Map.put(response, "usage", usage)
  defp put_precedence_usage(response, :direct, usage), do: Map.put(response, "response", %{"usage" => usage})

  defp assert_surface!(mode, transport, reasoning) do
    response = %{"id" => "resp_reasoning_subset", "object" => "response", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 2, "total_tokens" => 12, "output_tokens_details" => %{"reasoning_tokens" => reasoning}}}
    response = if transport == :compact_json, do: Map.merge(response, %{"object" => "response.compaction", "output" => [%{"type" => "compaction", "encrypted_content" => "synthetic-encrypted-compaction"}]}), else: response
    {request, attempt, settlement, reservation} = run_surface!(mode, transport, response)

    if reasoning <= 2 do
      assert request.usage_status == "usage_known"
      assert attempt.usage_status == "usage_known"
      assert settlement.usage_status == "usage_known"
      assert {settlement.input_tokens, settlement.output_tokens, settlement.reasoning_tokens, settlement.total_tokens} == {10, 2, 2, 12}
      assert Decimal.equal?(settlement.settled_cost_micros, Decimal.new(160))
    else
      assert request.usage_status == "usage_unknown"
      assert attempt.usage_status == "usage_unknown"
      assert settlement.usage_status == "usage_unknown"
      assert settlement.details["usage_source"] == if(transport == :sse, do: "sse_usage_missing", else: "invalid_usage_tokens")
      assert settlement.details["settled_cost_micros"] == nil
      assert settlement.details["estimated_from_reserve"] == true
      assert settlement.total_tokens == reservation.total_tokens
    end
  end

  defp run_surface!(mode, transport, response) do
    terminal = %{"type" => "response.completed", "response" => response}

    upstream_mode =
      case transport do
        transport when transport in [:json, :compact_json] -> FakeUpstream.json_response(response)
        :sse -> FakeUpstream.sse_stream([{"response.completed", terminal}])
        :websocket -> FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(terminal)])
      end

    upstream = start_upstream(upstream_mode)
    setup = gateway_setup(upstream, compact?: transport == :compact_json)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    port = start_public_endpoint!()
    payload = %{"model" => setup.model.exposed_model_id, "input" => "synthetic accounting request", "stream" => transport != :json}
    pool_id = setup.pool.id
    assert :ok = Events.subscribe_pool(pool_id, ["request_logs"])
    refute_received {Events, %{pool_id: ^pool_id, reason: "request_finalized"}}
    dispatch!(transport, port, setup, payload)
    {request, attempt, settlement, reservation} = settled_rows!(pool_id)

    assert request.status == "succeeded"
    assert request.retry_count == 0
    assert attempt.response_metadata["routing"]["model_serving_mode"] == mode
    assert FakeUpstream.count(upstream) == 1
    assert attempt.transport == expected_transport(transport)

    {request, attempt, settlement, reservation}
  end

  defp expected_transport(:compact_json), do: "http_compact_json"
  defp expected_transport(:websocket), do: "websocket"
  defp expected_transport(_http), do: "http_sse"

  defp dispatch!(:compact_json, port, setup, payload) do
    payload = payload |> Map.delete("stream") |> Map.put("input", native_text_input("synthetic accounting request"))
    {headers, body} = curl_json_request!(port, setup.authorization, payload, "/backend-api/codex/responses/compact")
    assert String.starts_with?(headers, "HTTP/1.1 200")
    assert CodexPooler.JSON.decode!(body)["object"] == "response.compaction"
  end

  defp dispatch!(:websocket, port, setup, payload) do
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), "/v1/responses", [{"openai-beta", "responses_websockets=2026-02-06"}])
    on_exit(fn -> Mint.HTTP.close(conn) end)
    frame = payload |> Map.delete("stream") |> Map.put("type", "response.create")
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
    {_conn, _websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
  end

  defp dispatch!(transport, port, setup, payload) do
    {headers, body} = curl_json_request!(port, setup.authorization, payload, "/v1/responses")
    assert String.starts_with?(headers, "HTTP/1.1 200")

    if transport == :json do
      assert CodexPooler.JSON.decode!(body)["status"] == "completed"
    else
      assert String.contains?(body, "response.completed")
    end
  end

  defp settled_rows!(pool_id) do
    # RequestLifecycle publishes this only after its settlement transaction returns.
    # The terminal frame alone can arrive before that transaction completes.
    assert_receive {Events, %{pool_id: ^pool_id, reason: "request_finalized", payload: %{"request_id" => request_id, "status" => "succeeded"}}}, @detection_budget

    Repo.one!(from(r in Request, join: a in Attempt, on: a.request_id == r.id, join: e in LedgerEntry, on: e.request_id == r.id and e.entry_kind == "settlement" and e.amount_status == "recorded", join: reservation in LedgerEntry, on: reservation.request_id == r.id and reservation.entry_kind == "reservation", where: r.pool_id == ^pool_id and r.id == ^request_id, select: {r, a, e, reservation}))
  end
end
