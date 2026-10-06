defmodule CodexPoolerWeb.V1.ResponsesAnnouncedItemTest do
  @moduledoc """
  A client of `/v1/responses` gets the ciphertext of the item the provider closed, never the one it announced.

  The provider announces an item in `response.output_item.added` before it closes it in `response.output_item.done`,
  and the announcement can carry an `encrypted_content` of its own. Measured on the Codex backend (`gpt-6-luna`, Full,
  HTTP SSE): a compaction turn streams `response.created`, `response.in_progress`, `response.output_item.added`,
  `response.compaction.compacting`, `response.output_item.done` and `response.completed`; the announced compaction
  carried a ciphertext of its own (about four fifths of the closed one's length) that the provider verifies on replay
  but that holds nothing. A reasoning item's announcement can differ from its closed item the same way. The completed
  response listed the closed items in one trace and nothing at all (`output: []`) in another, so the items can arrive
  only as `added` and `done` events. The released Codex client reads a compaction only from done items, and the
  official SDK stream helpers replace an announced item with its done item.

  Every transport family of `/v1` that aggregates or relays items is pinned here, with the completed output listing the
  closed items and with it empty: the non-streaming JSON response of Responses and of Chat Completions (from the
  completed output, or from the done items when it is empty), the SSE stream and the public websocket (the announcement
  is relayed as the provider sent it, the done event closes it with the closed ciphertext, and the completed output
  lists the closed items: as the provider sent them, or from the done items when it sent none, findings#335), and the
  compaction bridge (the item the client gets, and the id derived from its ciphertext, come from the done item).

  Topology: the real `/v1` endpoint, FakeUpstream streaming the measured event order with synthetic ciphertexts of the
  measured lengths, the Pool's default serving mode, HTTP SSE upstream (and the public websocket).
  """

  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.CompactionTrigger

  @moduletag capture_log: true
  @frame_timeout_ms 15_000

  # Synthetic ciphertexts of the measured lengths: the announcement 996 bytes, the closed item 1252.
  @announced_cipher "gAAAAA-announced-" <> String.duplicate("a", 979)
  @closed_cipher "gAAAAA-closed-" <> String.duplicate("c", 1238)

  @announced_reasoning %{"type" => "reasoning", "id" => "rs_synthetic_announced", "summary" => [], "encrypted_content" => @announced_cipher}
  @closed_reasoning %{@announced_reasoning | "encrypted_content" => @closed_cipher}
  @message %{"type" => "message", "id" => "msg_synthetic_reply", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic reply", "annotations" => []}]}

  describe "a reasoning item announced with a ciphertext of its own" do
    for completed_output <- [:closed_items, :empty] do
      @tag completed_output: completed_output
      test "the JSON response carries the closed ciphertext when the completed output lists #{completed_output}", %{conn: conn, completed_output: completed_output} do
        upstream = start_upstream(FakeUpstream.sse_stream(reasoning_turn_events(completed_output)))
        setup = gateway_setup(upstream)

        response = post_responses(conn, setup, %{"input" => "synthetic question", "include" => ["reasoning.encrypted_content"]})

        assert %{"status" => "completed", "output" => [reasoning, %{"type" => "message"}]} = json_response(response, 200)
        assert reasoning["encrypted_content"] == @closed_cipher
      end
    end

    for completed_output <- [:closed_items, :empty] do
      @tag completed_output: completed_output
      test "the SSE stream closes the item with the closed ciphertext when the completed output lists #{completed_output}", %{conn: conn, completed_output: completed_output} do
        upstream = start_upstream(FakeUpstream.sse_stream(reasoning_turn_events(completed_output)))
        setup = gateway_setup(upstream)

        events = conn |> post_responses(setup, %{"input" => "synthetic question", "stream" => true, "include" => ["reasoning.encrypted_content"]}) |> response(200) |> sse_events()

        assert_closed_by_done!(events, completed_output)
      end

      @tag :v1_websocket
      @tag completed_output: completed_output
      test "the public websocket closes the item with the closed ciphertext when the completed output lists #{completed_output}", %{completed_output: completed_output} do
        upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(reasoning_turn_events(completed_output), fn {_type, event} -> CodexPooler.JSON.encode!(event) end)))
        setup = gateway_setup(upstream)

        events = websocket_turn!(setup, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => "synthetic question", "include" => ["reasoning.encrypted_content"], "stream" => true})

        assert_closed_by_done!(events, completed_output)
      end
    end

    test "the Chat Completions JSON response takes the reply from the done items when the completed output is empty", %{conn: conn} do
      upstream = start_upstream(FakeUpstream.sse_stream(reasoning_turn_events(:empty)))
      setup = gateway_setup(upstream)

      response = conn |> auth(setup) |> post("/v1/chat/completions", %{"model" => setup.model.exposed_model_id, "messages" => [%{"role" => "user", "content" => "synthetic question"}]})

      assert %{"choices" => [%{"message" => %{"content" => "synthetic reply"}}]} = json_response(response, 200)
    end
  end

  describe "a compaction announced with a ciphertext of its own" do
    for stream? <- [false, true], completed_output <- [:closed_items, :empty] do
      @tag streaming: stream?, completed_output: completed_output
      test "the compaction bridge hands the client the closed checkpoint, stream=#{stream?}, completed output #{completed_output}", %{conn: conn, streaming: stream?, completed_output: completed_output} do
        upstream = start_upstream(FakeUpstream.sse_stream(compaction_turn_events(completed_output)))
        setup = gateway_setup(upstream, compact?: true)
        closed_item = %{"type" => "compaction", "encrypted_content" => @closed_cipher, "id" => CompactionTrigger.public_compaction_item_id(@closed_cipher)}

        response =
          post_responses(conn, setup, %{
            "input" => [%{"type" => "function_call_output", "call_id" => "call_synthetic_compaction", "output" => "synthetic output"}, %{"type" => "compaction_trigger"}],
            "stream" => stream?,
            "store" => false
          })

        items =
          if stream? do
            events = response |> response(200) |> sse_events()
            [event_item(events, "response.output_item.added"), event_item(events, "response.output_item.done") | completed_output(events)]
          else
            assert %{"status" => "completed", "output" => [item]} = json_response(response, 200)
            [item]
          end

        assert Enum.all?(items, &(&1 == closed_item))
        refute Enum.any?(items, &(&1["encrypted_content"] == @announced_cipher))
      end
    end

    # An unanchored compaction asked over the public websocket is collected from the provider's HTTP stream, then
    # handed to the client as an announced and closed item of its own. (A compaction anchored on the connection is
    # collected from the provider's websocket by the same collector, pinned in `compaction_result_collector_test.exs`.)
    for completed_output <- [:closed_items, :empty] do
      @tag :v1_websocket
      @tag completed_output: completed_output
      test "the compaction bridge hands a public websocket client the closed checkpoint, completed output #{completed_output}", %{completed_output: completed_output} do
        upstream = start_upstream(FakeUpstream.sse_stream(compaction_turn_events(completed_output)))
        setup = gateway_setup(upstream, compact?: true)
        closed_item = %{"type" => "compaction", "encrypted_content" => @closed_cipher, "id" => CompactionTrigger.public_compaction_item_id(@closed_cipher)}
        input = [%{"type" => "function_call_output", "call_id" => "call_synthetic_compaction", "output" => "synthetic output"}, %{"type" => "compaction_trigger"}]

        events = websocket_turn!(setup, %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => input, "store" => false})

        assert List.last(events)["type"] == "response.completed", "the compaction turn did not complete: #{inspect(Enum.map(events, & &1["type"]))}"
        items = [event_item(events, "response.output_item.added"), event_item(events, "response.output_item.done") | completed_output(events)]
        assert Enum.all?(items, &(&1 == closed_item))
        assert [%{method: "POST"}] = FakeUpstream.requests(upstream)
      end
    end
  end

  defp post_responses(conn, setup, body), do: conn |> auth(setup) |> post("/v1/responses", Map.put(body, "model", setup.model.exposed_model_id))

  # The measured event order: the item announced with its own ciphertext, then closed with another; the completed
  # response lists the closed items, or (a variant the aggregation also covers) nothing.
  defp reasoning_turn_events(completed_output) do
    output = if completed_output == :closed_items, do: [@closed_reasoning, @message], else: []
    response = %{"id" => "resp_synthetic_announced", "object" => "response", "created_at" => 1_790_000_000, "model" => "provider-gpt-test-model", "status" => "completed", "output" => output, "usage" => %{"input_tokens" => 37, "output_tokens" => 43, "total_tokens" => 80}}
    opening = %{response | "status" => "in_progress", "output" => []}

    [
      {"response.created", %{"type" => "response.created", "response" => opening}},
      {"response.in_progress", %{"type" => "response.in_progress", "response" => opening}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => @announced_reasoning}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => @closed_reasoning}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 1, "item" => %{@message | "status" => "in_progress", "content" => []}}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 1, "item" => @message}},
      {"response.completed", %{"type" => "response.completed", "response" => response}}
    ]
  end

  defp compaction_turn_events(completed_output) do
    announced = %{"type" => "compaction", "id" => nil, "encrypted_content" => @announced_cipher}
    closed = %{announced | "encrypted_content" => @closed_cipher}
    output = if completed_output == :closed_items, do: [closed], else: []
    response = %{"id" => "resp_synthetic_compaction", "object" => "response", "status" => "completed", "output" => output, "usage" => %{"input_tokens" => 83, "output_tokens" => 47, "total_tokens" => 130}}
    opening = %{response | "status" => "in_progress", "output" => []}

    [
      {"response.created", %{"type" => "response.created", "response" => opening}},
      {"response.in_progress", %{"type" => "response.in_progress", "response" => opening}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => announced}},
      {"response.compaction.compacting", %{"type" => "response.compaction.compacting", "output_index" => 0}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => closed}},
      {"response.completed", %{"type" => "response.completed", "response" => response}}
    ]
  end

  defp sse_events(body) do
    for block <- String.split(body, "\n\n", trim: true),
        "data: " <> data <- String.split(block, "\n"),
        data != "[DONE]",
        do: CodexPooler.JSON.decode!(data)
  end

  # The announcement is relayed as the provider sent it, the done event closes the item with the closed ciphertext, and
  # the completed output lists the closed items either way: the provider's own, or the done items when the provider sent
  # it empty (findings#335), never the announcement. A client that folds the item events by output index the way the
  # SDK stream helpers do (append on `added`, replace on `done`) ends with the same closed items.
  defp assert_closed_by_done!(events, _completed_output) do
    assert reasoning_cipher(events, "response.output_item.added") == @announced_cipher
    assert reasoning_cipher(events, "response.output_item.done") == @closed_cipher
    assert [%{"encrypted_content" => @closed_cipher}, %{"type" => "message", "status" => "completed"}] = completed_output(events)

    assert [%{"encrypted_content" => @closed_cipher}, %{"type" => "message", "status" => "completed"}] = folded_output(events)
  end

  defp folded_output(events) do
    events
    |> Enum.reduce(%{}, fn
      %{"type" => "response.output_item.added", "output_index" => index, "item" => item}, acc -> Map.put(acc, index, item)
      %{"type" => "response.output_item.done", "output_index" => index, "item" => item}, acc -> Map.put(acc, index, item)
      _event, acc -> acc
    end)
    |> Enum.sort_by(fn {index, _item} -> index end)
    |> Enum.map(fn {_index, item} -> item end)
  end

  defp event_item(events, type), do: Enum.find_value(events, &(&1["type"] == type && &1["item"]))
  defp reasoning_cipher(events, type), do: Enum.find_value(events, &((&1["type"] == type and get_in(&1, ["item", "type"]) == "reasoning") && get_in(&1, ["item", "encrypted_content"])))
  defp completed_output(events), do: Enum.find_value(events, &(&1["type"] == "response.completed" && get_in(&1, ["response", "output"])))

  defp websocket_turn!(setup, frame) do
    port = start_public_endpoint!()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"x-codex-turn-state", "public-ws-announced-#{System.unique_integer([:positive])}"}, {"openai-beta", "responses_websockets=2026-02-06"}]
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
