defmodule CodexPoolerWeb.Runtime.BackendCodexAnnouncedItemTest do
  # The native `/backend-api/codex/responses` routes relay the provider's stream to the Codex client unchanged, so the
  # client sees an item announced (`response.output_item.added`) before it is closed (`response.output_item.done`). The
  # announcement can carry an `encrypted_content` of its own that holds nothing (measured on the Codex backend,
  # `gpt-6-luna`, Full: a compaction announced with 996 bytes and closed with 1252; a reasoning item announced with a
  # ciphertext that differs from its closed one), and the released client reads only the closed item. The completed
  # response listed the closed items in one trace and nothing (`output: []`) in another. What the Pooler records about
  # the items it pushed, and later matches a client's resend against, is taken from the done items either way: the
  # native HTTP resend progress digest and the native websocket delivery receipt's completed-item digests.
  #
  # Topology: one node, owner forwarding off, the Pool's default serving mode, FakeUpstream streaming the measured
  # event order with synthetic ciphertexts, native HTTP SSE and native websocket, synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, public_websocket_connect!: 3, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @timeout_ms 15_000
  @poll_ms 20

  @announced_cipher "gAAAAA-announced-" <> String.duplicate("a", 979)
  @closed_cipher "gAAAAA-closed-" <> String.duplicate("c", 1238)
  @announced %{"type" => "reasoning", "id" => "rs_synthetic_announced", "summary" => [], "encrypted_content" => @announced_cipher}
  @closed %{@announced | "encrypted_content" => @closed_cipher}
  @message %{"type" => "message", "id" => "msg_synthetic_reply", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic reply", "annotations" => [], "logprobs" => []}]}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    :ok
  end

  for completed_output <- [:closed_items, :empty] do
    @tag completed_output: completed_output
    test "native HTTP relays the announcement as sent and records its resend progress from the closed items, completed output #{completed_output}", %{conn: conn, completed_output: completed_output} do
      upstream = start_upstream(FakeUpstream.sse_stream(Enum.map(turn_events(completed_output), &{&1["type"], &1})))
      setup = gateway_setup(upstream)
      thread = Ecto.UUID.generate()
      metadata = turn_metadata(thread)

      response =
        conn
        |> put_req_header("authorization", setup.authorization)
        |> put_req_header("content-type", "application/json")
        |> put_req_header("session-id", thread)
        |> put_req_header("thread-id", thread)
        |> put_req_header("x-codex-window-id", "#{thread}:0")
        |> put_req_header("x-codex-turn-metadata", metadata)
        |> put_req_header("originator", "codex_cli_rs")
        |> post(@path, CodexPooler.JSON.encode!(%{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic question"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => metadata}}))

      events = response |> response(200) |> sse_events()
      assert reasoning_cipher(events, "response.output_item.added") == @announced_cipher
      assert reasoning_cipher(events, "response.output_item.done") == @closed_cipher

      progress = setup |> settled_attempt!() |> Map.fetch!(:response_metadata) |> Map.fetch!("native_http_resume_progress")
      assert progress["output_item_done_count"] == 2
      assert ClientRetry.native_http_progress_matches?(progress, [@closed, @message])
      refute ClientRetry.native_http_progress_matches?(progress, [@announced, @message])
    end

    @tag completed_output: completed_output
    test "the native websocket relays the announcement as sent and records its delivery receipt from the closed items, completed output #{completed_output}", %{completed_output: completed_output} do
      upstream = start_upstream(FakeUpstream.websocket_text_frames(Enum.map(turn_events(completed_output), &CodexPooler.JSON.encode!/1)))
      setup = gateway_setup(upstream)
      thread = Ecto.UUID.generate()
      port = start_public_endpoint!()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      frame = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "input" => native_text_input("synthetic question"), "stream" => true, "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate(), "x-codex-turn-metadata" => turn_metadata(thread)}}
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
      {conn, events} = receive_until_terminal!(conn, websocket, ref, [])
      _closed = Mint.HTTP.close(conn)

      assert reasoning_cipher(events, "response.output_item.added") == @announced_cipher
      assert reasoning_cipher(events, "response.output_item.done") == @closed_cipher

      receipt = setup |> settled_attempt!() |> await_receipt!()
      {:ok, closed_digest} = WebsocketTurnIdentity.completed_item_digest(@closed)
      {:ok, announced_digest} = WebsocketTurnIdentity.completed_item_digest(@announced)
      {:ok, message_digest} = WebsocketTurnIdentity.completed_item_digest(@message)
      assert %{"completed_items" => 2, "completed_item_digests" => [^closed_digest, ^message_digest]} = receipt
      refute announced_digest in receipt["completed_item_digests"]
    end
  end

  # The measured order: the reasoning item announced with its own ciphertext and closed with another, then the reply;
  # the completed response lists the closed items, or nothing.
  defp turn_events(completed_output) do
    output = if completed_output == :closed_items, do: [@closed, @message], else: []
    response = %{"id" => "resp_synthetic_native_announced", "object" => "response", "status" => "completed", "output" => output, "usage" => %{"input_tokens" => 37, "output_tokens" => 43, "total_tokens" => 80}}
    opening = %{response | "status" => "in_progress", "output" => []}

    [
      %{"type" => "response.created", "response" => opening},
      %{"type" => "response.in_progress", "response" => opening},
      %{"type" => "response.output_item.added", "output_index" => 0, "item" => @announced},
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => @closed},
      %{"type" => "response.output_item.added", "output_index" => 1, "item" => %{@message | "status" => "in_progress", "content" => []}},
      %{"type" => "response.output_item.done", "output_index" => 1, "item" => @message},
      %{"type" => "response.completed", "response" => response}
    ]
  end

  defp turn_metadata(thread),
    do: CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})

  defp sse_events(body) do
    for block <- String.split(body, "\n\n", trim: true),
        "data: " <> data <- String.split(block, "\n"),
        data != "[DONE]",
        do: CodexPooler.JSON.decode!(data)
  end

  defp reasoning_cipher(events, type),
    do: Enum.find_value(events, &((&1["type"] == type and get_in(&1, ["item", "type"]) == "reasoning") && get_in(&1, ["item", "encrypted_content"])))

  defp receive_until_terminal!(conn, websocket, ref, events) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    event = CodexPooler.JSON.decode!(text)
    events = events ++ [event]

    if event["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, events},
      else: receive_until_terminal!(conn, websocket, ref, events)
  end

  defp settled_attempt!(setup, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms

    case Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id and r.status == "succeeded")) do
      [request] ->
        Repo.get_by!(Attempt, request_id: request.id)

      _other ->
        if System.monotonic_time(:millisecond) < deadline do
          receive do
          after
            @poll_ms -> settled_attempt!(setup, deadline)
          end
        else
          flunk("the turn never settled")
        end
    end
  end

  defp await_receipt!(%Attempt{id: id} = attempt, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms

    case Repo.get!(Attempt, id).response_metadata do
      %{"downstream_delivery" => %{"completed_item_digests" => [_first | _rest]} = receipt} ->
        receipt

      _pending ->
        if System.monotonic_time(:millisecond) < deadline do
          receive do
          after
            @poll_ms -> await_receipt!(attempt, deadline)
          end
        else
          flunk("no delivery receipt with completed items")
        end
    end
  end
end
