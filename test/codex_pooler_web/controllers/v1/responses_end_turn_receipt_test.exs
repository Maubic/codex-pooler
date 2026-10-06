defmodule CodexPoolerWeb.V1.ResponsesEndTurnReceiptTest do
  # The public `/v1/responses` surface keeps `response.end_turn` on the terminal it pushes (SSE and websocket), so the
  # delivery receipt of a pushed `response.completed` names the class the client was sent (`true`, `false` or
  # `absent`) on the public routes too (findings#311). The non-streaming surface settles without a delivery receipt
  # and records no class. Provenance: the field is source-derived (codex-rs `ResponseCompleted.end_turn`); the event
  # sequences, ids and texts are synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      auth: 2,
      gateway_setup: 1,
      public_websocket_connect_with_request_headers!: 5,
      public_websocket_receive_text!: 3,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @arms [{"true", true}, {"false", false}, {"absent", :absent}]
  @delta_sentinel "v1-end-turn-delta-sentinel"
  @moduletag capture_log: true

  for mode <- ["full", "lite"], {class, value} <- [{"true", true}, {"false", false}, {"absent", :absent}] do
    test "#{mode} /v1 SSE: end_turn #{class} reaches the client and is recorded as #{class} in the delivery receipt" do
      upstream = start_upstream(FakeUpstream.sse_stream(events("resp_v1_end_turn_sse", unquote(value))))
      setup = serving_setup(upstream, unquote(mode))

      {conn, logs} = with_info_log(fn -> post_responses(setup, true) end)

      assert conn.status == 200
      assert terminal_response!(conn.resp_body) |> Map.get("end_turn", :absent) == unquote(value)

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert request.status == "succeeded"

      assert %{"end_turn" => unquote(class), "terminal_class" => "response.completed", "outcome" => "delivered", "transport" => "http_sse"} = receipt = attempt.response_metadata["downstream_delivery"]
      assert Enum.sort(Map.keys(receipt)) == ~w(end_turn frames_after_visible outcome pushed_at terminal_class transport)
      assert logs =~ ~r/http_sse downstream terminal pushed request_id=#{request.id} .*frames_after_visible=\d+ end_turn=#{unquote(class)}$/m
      refute inspect({request.request_metadata, attempt.response_metadata}) =~ @delta_sentinel
    end
  end

  test "/v1 non-streaming: the collected response settles without a delivery receipt and records no class" do
    upstream = start_upstream(FakeUpstream.sse_stream(events("resp_v1_end_turn_json", false)))
    setup = gateway_setup(upstream)

    conn = post_responses(setup, false)

    assert conn.status == 200
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    refute Map.has_key?(attempt.response_metadata, "downstream_delivery")
    refute inspect(attempt.response_metadata) =~ "end_turn"
  end

  for mode <- ["full", "lite"], owner? <- [false, true] do
    test "#{mode} /v1 websocket, owner forwarding #{owner?}: every completed turn's receipt names the class the client was sent" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(owner?))
      turns = Enum.map(@arms, fn {class, value} -> FakeUpstream.websocket_text_frames(Enum.map(events("resp_v1_end_turn_ws_#{class}", value), &frame/1)) end)
      upstream = start_upstream(FakeUpstream.strict_sequence(turns))
      setup = serving_setup(upstream, unquote(mode))
      port = start_public_endpoint!()
      headers = [{"openai-beta", "responses_websockets=2026-02-06"}]
      {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "v1-end-turn-#{System.unique_integer([:positive])}", "/v1/responses", headers)
      on_exit(fn -> Mint.HTTP.close(conn) end)

      {conn, _websocket} =
        Enum.reduce(Enum.with_index(@arms, 1), {conn, websocket}, fn {{class, value}, ordinal}, {conn, websocket} ->
          frame = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => "synthetic turn #{class}", "store" => false, "generate" => true}
          {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
          {conn, websocket, terminal} = receive_terminal(conn, websocket, ref)
          assert terminal["type"] == "response.completed"
          assert Map.get(terminal["response"], "end_turn", :absent) == value
          assert length(await_receipts(setup, ordinal)) == ordinal
          {conn, websocket}
        end)

      Mint.HTTP.close(conn)
      attempts = await_receipts(setup, length(@arms))

      assert Enum.map(attempts, & &1.response_metadata["downstream_delivery"]["end_turn"]) == Enum.map(@arms, &elem(&1, 0))

      for attempt <- attempts do
        receipt = attempt.response_metadata["downstream_delivery"]
        assert %{"outcome" => "delivered", "terminal_class" => "response.completed", "transport" => "websocket"} = receipt
        # The turn completed one item, so the websocket receipt also names it (completed_items, completed_item_digests).
        assert Enum.sort(Map.keys(receipt)) == ~w(completed_item_digests completed_items end_turn frames_after_visible highest_frame_class outcome pushed_at terminal_class transport)
        refute inspect(attempt.response_metadata) =~ @delta_sentinel
      end
    end
  end

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_responses(setup, stream?) do
    build_conn() |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic", "stream" => stream?})
  end

  defp receive_terminal(conn, websocket, ref, seen \\ 0) when seen < 12 do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    event = CodexPooler.JSON.decode!(text)

    if event["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, websocket, event},
      else: receive_terminal(conn, websocket, ref, seen + 1)
  end

  defp await_receipts(setup, expected, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + 10_000

    attempts =
      Repo.all(
        from(a in Attempt,
          join: r in Request,
          on: r.id == a.request_id,
          where: r.pool_id == ^setup.pool.id,
          order_by: [asc: a.started_at, asc: a.attempt_number]
        )
      )

    cond do
      length(attempts) == expected and Enum.all?(attempts, &is_map(&1.response_metadata["downstream_delivery"])) ->
        attempts

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("expected #{expected} attempts with a delivery receipt, got #{inspect(Enum.map(attempts, &Map.keys(&1.response_metadata || %{})))}")

      true ->
        receive do
        after
          20 -> await_receipts(setup, expected, deadline)
        end
    end
  end

  # The `response` object of the `response.completed` event in a public SSE body.
  defp terminal_response!(body) do
    body
    |> String.split("\n\n", trim: true)
    |> Enum.find_value(fn block ->
      fields = block |> String.split("\n") |> Map.new(&List.to_tuple(String.split(&1, ": ", parts: 2)))

      case fields do
        %{"event" => "response.completed", "data" => data} -> CodexPooler.JSON.decode!(data)["response"]
        _other -> nil
      end
    end) || flunk("no response.completed event in the public stream")
  end

  defp events(response_id, end_turn) do
    item = %{"id" => "msg_#{response_id}", "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}
    done_item = %{item | "status" => "completed", "content" => [%{"type" => "output_text", "text" => @delta_sentinel, "annotations" => []}]}
    address = %{"item_id" => item["id"], "output_index" => 0, "content_index" => 0}

    [
      {"response.created", %{"type" => "response.created", "response" => response_body(response_id, "in_progress", [], :absent)}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => item}},
      {"response.output_text.delta", Map.merge(address, %{"type" => "response.output_text.delta", "delta" => @delta_sentinel})},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => done_item}},
      {"response.completed", %{"type" => "response.completed", "response" => response_body(response_id, "completed", [done_item], end_turn)}}
    ]
  end

  defp response_body(response_id, status, output, end_turn) do
    body = %{
      "id" => response_id,
      "object" => "response",
      "created_at" => 1_790_000_000,
      "model" => "provider-gpt-test-model",
      "status" => status,
      "output" => output,
      "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
    }

    if end_turn == :absent, do: body, else: Map.put(body, "end_turn", end_turn)
  end

  defp frame({_type, payload}), do: CodexPooler.JSON.encode!(payload)
end
