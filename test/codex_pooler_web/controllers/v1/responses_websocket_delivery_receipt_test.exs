defmodule CodexPoolerWeb.V1.ResponsesWebsocketDeliveryReceiptTest do
  # An SDK on the public `GET /v1/responses` websocket closes its socket as soon
  # as the last turn's `response.completed` arrives, while that turn's response
  # task is still settling. The socket then acknowledges the task `aborted` (it
  # never saw the task's result), and the delivery receipt used to reuse that
  # outcome: `aborted terminal_class=response.completed` for a terminal the
  # client had received (openai-node 7.21.0 `ResponsesWS`, production, 2 of 2;
  # findings#225 row 225-240). The native route records `delivered` since
  # findings#225 row 225-130; the public route now does the same. Only the
  # receipt changes.
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

  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.Accounting.Request
  alias CodexPooler.Events
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @frame_timeout_ms 15_000
  @response_id "resp_public_ws_delivery_receipt"

  for {topology, completion_order} <- [{:direct, :immediate}, {:local_owner, :immediate}, {:local_owner, :after_cleanup}] do
    @tag :v1_websocket
    @tag topology: topology
    @tag completion_order: completion_order
    test "a #{topology} public websocket turn with #{completion_order} completion records its delivered terminal", %{topology: topology, completion_order: completion_order} do
      if topology == :local_owner, do: enable_owner_forwarding!()
      release_ref = make_ref()
      events = Enum.map(upstream_events(), &CodexPooler.JSON.encode!/1)
      upstream = start_upstream(FakeUpstream.barrier_websocket_frames(events, notify: self(), release_ref: release_ref))
      setup = gateway_setup(upstream)
      assert :ok = Events.subscribe_pool(setup.pool)
      port = start_public_endpoint!()
      sockets_before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = public_v1_websocket_connect!(port, setup, topology)

      gate = if completion_order == :after_cleanup, do: install_completion_gate!(WebsocketCleanupFence.await_new_listener_socket!(sockets_before))

      payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => "synthetic public receipt turn", "stream" => true})
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, payload)
      last = length(events) - 1

      for ordinal <- 0..(last - 1) do
        assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @frame_timeout_ms
        assert :ok = FakeUpstream.release_frame(upstream, release_ref)
      end

      assert_receive {:fake_upstream_frame_barrier, ^last, _handler, ^release_ref}, @frame_timeout_ms
      assert :ok = FakeUpstream.release_frame(upstream, release_ref)
      {conn, texts} = receive_until_terminal!(conn, websocket, ref, [])
      assert %{"type" => "response.completed"} = texts |> List.last() |> CodexPooler.JSON.decode!()

      task_pid =
        if gate do
          assert_receive {:completion_gate, task_pid}, @frame_timeout_ms
          task_pid
        end

      # The SDK closes the moment its last turn completes.
      _closed = Mint.HTTP.close(conn)

      if gate do
        # Release only after the socket's pre-cleanup drain expired and its
        # owner cleanup completed. The result must reach the post-cleanup drain.
        assert_receive {:socket_cleanup_finished, socket}, @frame_timeout_ms
        assert socket == gate.socket
        monitor = Process.monitor(task_pid)
        send(task_pid, {:release_completion, gate.ref})
        assert_receive {:DOWN, ^monitor, :process, ^task_pid, :normal}, @frame_timeout_ms
      end

      assert_receive {Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @frame_timeout_ms
      assert [attempt] = await_delivery_receipt(setup.pool.id, System.monotonic_time(:millisecond) + @frame_timeout_ms)

      assert %{"outcome" => "delivered", "terminal_class" => "response.completed"} = attempt.response_metadata["downstream_delivery"]
      # The public socket classifies what it pushed like the native one; the
      # admin request-log drawer shows the class for every websocket receipt.
      assert attempt.response_metadata["downstream_delivery"]["highest_frame_class"] == "terminal"
    end
  end

  defp install_completion_gate!(socket) do
    test = self()
    ref = make_ref()
    handler_id = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok = :telemetry.attach(handler_id, [:codex_pooler, :gateway, :websocket_control, :cleanup_finished], &__MODULE__.notify_cleanup/4, %{test: test, socket: socket})

    gate = fn ->
      monitor = Process.monitor(test)
      send(test, {:completion_gate, self()})

      try do
        receive do
          {:release_completion, ^ref} -> :ok
          {:DOWN, ^monitor, :process, ^test, _reason} -> :ok
        after
          @frame_timeout_ms -> raise "completion gate was not released"
        end
      after
        Process.demonitor(monitor, [:flush])
      end
    end

    :sys.replace_state(socket, fn {state_name, data} ->
      options = [before_local_completion_handoff: gate]
      state = Map.put(data.connection.websock_state, :response_task_start_options, options)
      {state_name, %{data | connection: %{data.connection | websock_state: state}}}
    end)

    %{socket: socket, ref: ref}
  end

  def notify_cleanup(_event, _measurements, %{caller: socket}, %{test: test, socket: socket}),
    do: send(test, {:socket_cleanup_finished, socket})

  def notify_cleanup(_event, _measurements, _metadata, _config), do: :ok

  defp await_delivery_receipt(pool_id, deadline_ms) do
    attempts =
      Repo.all(
        from attempt in Attempt,
          join: request in Request,
          on: request.id == attempt.request_id,
          where: request.pool_id == ^pool_id
      )

    cond do
      attempts != [] and Enum.all?(attempts, &is_map(&1.response_metadata["downstream_delivery"])) ->
        attempts

      System.monotonic_time(:millisecond) >= deadline_ms ->
        attempts

      true ->
        Process.sleep(20)
        await_delivery_receipt(pool_id, deadline_ms)
    end
  end

  defp upstream_events do
    item = %{"id" => "msg_public_ws_receipt", "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}
    part = %{"type" => "output_text", "text" => "", "annotations" => []}
    done_part = %{part | "text" => "synthetic receipt answer"}
    done_item = %{item | "status" => "completed", "content" => [done_part]}
    address = %{"item_id" => "msg_public_ws_receipt", "output_index" => 0, "content_index" => 0}

    [
      %{"type" => "response.created", "response" => response_body("in_progress", [])},
      %{"type" => "response.output_item.added", "output_index" => 0, "item" => item},
      Map.merge(address, %{"type" => "response.output_text.delta", "delta" => "synthetic receipt answer"}),
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => done_item},
      %{"type" => "response.completed", "response" => response_body("completed", [done_item])}
    ]
  end

  defp response_body(status, output) do
    %{
      "id" => @response_id,
      "object" => "response",
      "created_at" => 1_790_000_000,
      "model" => "provider-gpt-test-model",
      "status" => status,
      "output" => output,
      "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
    }
  end

  defp receive_until_terminal!(conn, websocket, ref, texts) do
    message = receive_mint_socket_message!(conn, @frame_timeout_ms, "timed out waiting for the public terminal; received #{length(texts)} frames")

    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        {websocket, new_texts} = decode_texts(websocket, ref, responses)
        texts = texts ++ new_texts

        if Enum.any?(new_texts, &terminal_text?/1),
          do: {conn, texts},
          else: receive_until_terminal!(conn, websocket, ref, texts)

      {:error, _conn, reason, _responses} ->
        flunk("websocket receive failed: #{inspect(reason)}")

      :unknown ->
        receive_until_terminal!(conn, websocket, ref, texts)
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

  defp public_v1_websocket_connect!(port, setup, topology) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"x-codex-turn-state", "public-ws-receipt-#{topology}-#{System.unique_integer([:positive])}"},
      {"openai-beta", "responses_websockets=2026-02-06"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end

  defp enable_owner_forwarding! do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)

    on_exit(fn ->
      case previous do
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
      end
    end)
  end
end
