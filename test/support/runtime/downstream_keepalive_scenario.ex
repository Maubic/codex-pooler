defmodule CodexPoolerWeb.Runtime.DownstreamKeepaliveScenario do
  @moduledoc false

  # A native turn that streams longer than the downstream idle bound
  # (findings#302), shared by the one-node module
  # (`backend_codex_websocket/downstream_keepalive_test.exs`) and the peer-only
  # one (`backend_codex_websocket_downstream_keepalive_peer_test.exs`, whose
  # module boots the owner's VM once). The bound is shortened through the
  # test-only settings override, the provider's frames are paced by the fake,
  # and the client reads the turn as the released client does.

  import ExUnit.Assertions
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [start_shared_peer_window_owner!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Events
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  # Detection budget for a frame, a close, a settlement or a socket's cleanup
  # the test only observes.
  @detection_timeout_ms 15_000
  @turn_path "/backend-api/codex/responses"
  @terminal_types ~w(response.completed response.failed response.incomplete error)

  def detection_timeout_ms, do: @detection_timeout_ms
  def terminal_types, do: @terminal_types

  # Overrides gateway settings for this test (the socket reads its idle bound
  # and the proxy its remote turn budget from them) and turns owner forwarding
  # on or off, both restored at exit.
  def put_settings!(overrides, forwarding?) do
    previous = CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
    settings = struct!(Keyword.get(previous, :settings, %OperationalSettings{}), overrides)

    Application.put_env(
      :codex_pooler,
      OperationalSettings,
      previous
      |> Keyword.put(:settings, settings)
      |> Keyword.put(:use_instance_settings?, false)
    )

    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding?)
    :ok
  end

  def turn_request(respond) do
    FakeUpstream.expect_request(method: "WEBSOCKET", path: @turn_path, json: [valid: true, equals: %{"type" => "response.create"}], respond: respond)
  end

  # `peer_node:` starts the session's owner on that (shared) peer VM before the
  # socket connects.
  def start_turn!(setup, opts \\ []) do
    assert :ok = Events.subscribe_pool(setup.pool)
    thread = Ecto.UUID.generate()

    owner =
      case Keyword.get(opts, :peer_node) do
        nil -> nil
        peer_node -> start_shared_peer_window_owner!(setup, "#{thread}:0", peer_node).owner_pid
      end

    {_server, port} = start_public_endpoint_with_server!()
    before = WebsocketCleanupFence.listener_sockets()
    headers = [{"session-id", thread}, {"thread-id", thread}, {"x-client-request-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"openai-beta", "responses_websockets=2026-02-06"}]
    {conn, websocket, ref, _response_headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @turn_path, headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, turn_frame(setup, thread))
    %{conn: conn, websocket: websocket, ref: ref, socket: socket, owner: owner, sent_at: System.monotonic_time(:millisecond)}
  end

  # Reads the turn as the released client does: its websocket pump answers
  # every Ping with a Pong (codex-rs `codex-api/src/endpoint/responses_websocket.rs`);
  # `answer_pings?: false` is a client that does not. Ends on a terminal event,
  # a Close or the connection's end.
  def read_turn!(client, answer_pings?: answer_pings?),
    do: read_turn!(client, answer_pings?, %{events: [], pings: 0, first_event_ms: nil})

  defp read_turn!(client, answer_pings?, acc) do
    case receive_mint_socket_message!(client.conn, @detection_timeout_ms, "timed out waiting for the turn") do
      {tag, _socket} when tag in [:tcp_closed, :ssl_closed] ->
        finish_turn(client, acc, :socket_closed)

      {tag, _socket, _reason} when tag in [:tcp_error, :ssl_error] ->
        finish_turn(client, acc, :socket_closed)

      message ->
        {:ok, conn, responses} = Mint.WebSocket.stream(client.conn, message)
        client = %{client | conn: conn}
        {websocket, frames} = decode_frames(client.websocket, client.ref, responses)

        case apply_frames(frames, %{client | websocket: websocket}, acc, answer_pings?) do
          {:cont, client, acc} -> read_turn!(client, answer_pings?, acc)
          {:halt, client, acc, ending} -> finish_turn(client, acc, ending)
        end
    end
  end

  defp decode_frames(websocket, ref, responses) do
    Enum.reduce(responses, {websocket, []}, fn
      {:data, ^ref, data}, {websocket, frames} ->
        {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
        {websocket, frames ++ decoded}

      _response, acc ->
        acc
    end)
  end

  defp apply_frames([], client, acc, _answer_pings?), do: {:cont, client, acc}

  defp apply_frames([{:ping, payload} | frames], client, acc, answer_pings?) do
    client = if answer_pings?, do: send_frame!(client, {:pong, payload}), else: client
    apply_frames(frames, client, %{acc | pings: acc.pings + 1}, answer_pings?)
  end

  defp apply_frames([{:text, text} | frames], client, acc, answer_pings?) do
    event = CodexPooler.JSON.decode!(text)
    acc = %{acc | events: acc.events ++ [event], first_event_ms: acc.first_event_ms || System.monotonic_time(:millisecond) - client.sent_at}

    if event["type"] in @terminal_types,
      do: {:halt, client, acc, {:terminal, event["type"]}},
      else: apply_frames(frames, client, acc, answer_pings?)
  end

  defp apply_frames([{:close, code, _reason} | _frames], client, acc, _answer_pings?), do: {:halt, client, acc, {:close, code}}
  defp apply_frames([_frame | frames], client, acc, answer_pings?), do: apply_frames(frames, client, acc, answer_pings?)

  defp finish_turn(client, acc, ending) do
    turn = Map.merge(acc, %{client: client, end: ending, elapsed_ms: System.monotonic_time(:millisecond) - client.sent_at})
    CodexPooler.TestDiagnostics.puts(fn -> "downstream keepalive turn " <> describe_turn(turn) end)
    turn
  end

  # How a read turn went, for assertion messages: a close before the first
  # event is a bound shorter than the turn's setup, one after it a gap.
  def describe_turn(turn),
    do: "ended #{inspect(turn.end)} after #{turn.elapsed_ms} ms with #{length(turn.events)} events (first after #{inspect(turn.first_event_ms)} ms) and #{turn.pings} pings"

  defp send_frame!(client, frame) do
    {:ok, websocket, data} = Mint.WebSocket.encode(client.websocket, frame)
    {:ok, conn} = Mint.WebSocket.stream_request_body(client.conn, client.ref, data)
    %{client | conn: conn, websocket: websocket}
  end

  def close_client!(client) do
    Mint.HTTP.close(client.conn)
    WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  def assert_request_settled!(setup, status, last_error_code) do
    assert_receive {Events, %{reason: "request_finalized", payload: %{"status" => ^status}}}, @detection_timeout_ms
    assert [%Request{status: ^status, last_error_code: ^last_error_code, transport: "websocket"} = request] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id))
    request
  end

  # A message streamed as `deltas` text deltas, as `{type, payload}` events for
  # the fake's paced stream.
  def stream_events(response_id, deltas) do
    item_id = "msg_#{response_id}"
    item = %{"type" => "message", "id" => item_id, "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => String.duplicate("x", deltas)}]}

    [
      %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
      %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{item | "status" => "in_progress", "content" => []}}
    ]
    |> Kernel.++(for _delta <- 1..deltas, do: %{"type" => "response.output_text.delta", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "delta" => "x"})
    |> Kernel.++([
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 20, "output_tokens" => deltas, "total_tokens" => 20 + deltas}}}
    ])
    |> Enum.map(&{&1["type"], &1})
  end

  # The same events as encoded native frames, for `barrier_websocket_frames/2`.
  def encoded_events(response_id, deltas),
    do: response_id |> stream_events(deltas) |> Enum.map(fn {_type, event} -> CodexPooler.JSON.encode!(event) end)

  defp turn_frame(setup, thread) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => native_text_input("synthetic prompt"),
      "tools" => [],
      "tool_choice" => "auto",
      "parallel_tool_calls" => true,
      "store" => false,
      "stream" => true,
      "prompt_cache_key" => thread,
      "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate()}
    })
  end
end
