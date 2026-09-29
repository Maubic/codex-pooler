defmodule CodexPoolerWeb.V1.ResponsesWebsocketTaskFailureAfterTerminalTest do
  # A public `GET /v1/responses` websocket turn gets one terminal. When the
  # turn's task fails after `response.completed` went out (its settlement
  # raises), the socket sends nothing more: an SDK that keeps the socket for
  # its next response would otherwise read an `error` event on it after the
  # completed turn (findings#270 row 270-346). The owner-forwarded path already
  # held the error back once the terminal was sent; the socket's local path,
  # with owner forwarding off, pushed it. Both topologies run the same arm: the
  # completion, nothing before the pong of a ping sent once the socket handled
  # the failure, then the next response on the same socket served whole.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full), one node, owner forwarding off (direct) and on (local owner). The
  # settlement fails once through the pricing lookup's test fault.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      await_public_websocket_upgrade: 2,
      decode_public_websocket_data!: 2,
      gateway_setup: 1,
      mint_websocket_new!: 4,
      public_websocket_send_text!: 4,
      receive_mint_socket_message!: 3,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [await_socket_connection_state!: 2, socket_transport_barrier!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true

  @detection_timeout_ms 15_000

  for topology <- [:direct, :local_owner] do
    @tag :v1_websocket
    @tag topology: topology
    test "#{topology}: a task that fails after the turn's response.completed sends the client nothing, and the next response on the socket is served", %{topology: topology} do
      put_owner_forwarding!(topology == :local_owner)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (two responses on one public socket; the first one's settlement raises once after its completion went out)
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}], respond: FakeUpstream.websocket_text_frames(response_events("resp_v1_fault_first"))),
            FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, equals: %{"type" => "response.create"}], respond: FakeUpstream.websocket_text_frames(response_events("resp_v1_fault_next")))
          ])
        )

      setup = gateway_setup(upstream)
      port = start_public_endpoint!()
      before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = connect!(port, setup, topology)
      socket = WebsocketCleanupFence.await_new_listener_socket!(before)

      CodexPooler.TestAppEnv.restore_on_exit(:settlement_pricing_test_fault)
      Application.put_env(:codex_pooler, :settlement_pricing_test_fault, {setup.pool.id, %DBConnection.ConnectionError{message: "synthetic pool exhaustion"}})

      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, create(setup, "synthetic first request"))
      {conn, websocket, first} = receive_until_terminal!(conn, websocket, ref, [])
      assert {"response.completed", "resp_v1_fault_first"} = List.last(first)

      # The socket handled the task's failure, so whatever it pushed for it is
      # written before it answers a ping; the barrier fails on any such frame.
      _state = await_socket_connection_state!(socket, &(MapSet.size(Map.get(&1, :tasks, MapSet.new())) == 0))
      {conn, websocket} = socket_transport_barrier!(conn, websocket, ref)

      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, create(setup, "synthetic next request"))
      {conn, _websocket, next} = receive_until_terminal!(conn, websocket, ref, [])
      Mint.HTTP.close(conn)

      assert [{"response.created", "resp_v1_fault_next"} | _] = next
      assert {"response.completed", "resp_v1_fault_next"} = List.last(next)
      refute Enum.any?(next, &match?({"error", _id}, &1))
      assert [{"failed", "owner_task_exception"}, {"succeeded", nil}] = Enum.map(await_settled!(setup.pool.id), &{&1.status, &1.last_error_code})
    end
  end

  defp create(setup, text), do: CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => text, "stream" => true})

  defp response_events(response_id) do
    item = %{"id" => "msg_#{response_id}", "type" => "message", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic answer", "annotations" => []}]}

    Enum.map(
      [
        %{"type" => "response.created", "response" => response_body(response_id, "in_progress", [])},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => response_body(response_id, "completed", [item])}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp response_body(response_id, status, output) do
    %{
      "id" => response_id,
      "object" => "response",
      "created_at" => 1_790_000_000,
      "model" => "provider-gpt-test-model",
      "status" => status,
      "output" => output,
      "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
    }
  end

  # The frames of one response, as {type, response id}, through its terminal.
  defp receive_until_terminal!(conn, websocket, ref, events) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "timed out waiting for the public terminal; received #{length(events)} frames")
    {:ok, conn, responses} = Mint.WebSocket.stream(conn, message)

    {websocket, texts} =
      Enum.reduce(responses, {websocket, []}, fn
        {:data, ^ref, data}, {websocket, acc} ->
          case decode_public_websocket_data!(websocket, data) do
            {:ok, websocket, texts} -> {websocket, acc ++ texts}
            {:cont, websocket} -> {websocket, acc}
          end

        _response, acc ->
          acc
      end)

    events = events ++ Enum.map(texts, &event_summary/1)

    if Enum.any?(events, fn {type, _id} -> type in ["response.completed", "response.failed", "response.incomplete", "error"] end),
      do: {conn, websocket, events},
      else: receive_until_terminal!(conn, websocket, ref, events)
  end

  defp event_summary(text) do
    event = CodexPooler.JSON.decode!(text)
    {event["type"], get_in(event, ["response", "id"])}
  end

  defp connect!(port, setup, topology) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"x-codex-turn-state", "public-ws-fault-#{topology}-#{System.unique_integer([:positive])}"},
      {"openai-beta", "responses_websockets=2026-02-06"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end

  defp await_settled!(pool_id) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    Stream.repeatedly(fn -> Repo.all(from(request in Request, where: request.pool_id == ^pool_id, order_by: [asc: request.admitted_at, asc: request.id])) end)
    |> Enum.find(fn rows ->
      cond do
        length(rows) == 2 and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"])) ->
          true

        System.monotonic_time(:millisecond) > deadline ->
          flunk("the responses never settled: #{inspect(Enum.map(rows, & &1.status))}")

        true ->
          Process.sleep(10)
          false
      end
    end)
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end
end
