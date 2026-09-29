defmodule CodexPoolerWeb.Runtime.NativeWebsocketRateLimitsTest do
  # The provider's upstream websocket sends `codex.rate_limits` during a turn:
  # the windows of the one account that served it. The released Codex client
  # (rust-v0.158.0, `codex-api/src/endpoint/responses_websocket.rs`) takes that
  # frame as its own rate-limit snapshot, so its TUI warned "less than 10% of
  # your 5h limit left" from one upstream account's windows while the Pool had
  # capacity elsewhere. Native HTTP never relays the provider's rate-limit
  # headers for the same reason, and the native socket now drops the frame too
  # (findings#279 point 1). The Pooler still records it as quota evidence, and
  # every other frame of the turn, the provider's `codex.response.metadata` and
  # an unknown control included, reaches the client unchanged.
  #
  # Topology: the real public listener, native websocket
  # `/backend-api/codex/responses` (and its `/backend-api/codex/v1/responses`
  # alias, forwarding off, Full), one node with owner forwarding off (the
  # socket's own upstream session) and on (the session's owner on this node),
  # the Pool's serving mode forced to Full and to Lite; and a second VM owning
  # the session (forwarding on, remote owner, the Pool's default mode, which
  # serves this model Full).
  # FakeUpstream, the released client's turn frames, synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2]

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  # Detection budget for a frame, a settlement or a row the test only observes.
  @detection_timeout_ms 15_000

  @turn_path "/backend-api/codex/responses"
  @response_id "resp_native_rate_limits_turn"
  @native_terminal_types ["response.completed", "response.failed", "response.incomplete", "error"]
  @message_item %{"id" => "msg_native_rate_limits", "type" => "message", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic answer", "annotations" => []}]}
  @window_thread "019a0000-0000-7000-8000-00000000f279"
  @window_id "#{@window_thread}:0"

  for forwarding <- [:off, :on], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, serving_mode: mode
    test "a #{mode} native socket with owner forwarding #{forwarding} keeps the served account's codex.rate_limits from the client", ctx do
      put_owner_forwarding!(ctx.forwarding == :on)
      reset_at = DateTime.utc_now() |> DateTime.add(3_600, :second) |> DateTime.truncate(:second)
      upstream = start_turn_upstream!(reset_at)
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, ctx.serving_mode)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      {_server, port} = start_public_endpoint_with_server!()
      thread = "native-rate-limits-#{ctx.serving_mode}-#{ctx.forwarding}-#{System.unique_integer([:positive])}"
      client = connect!(port, setup, thread)
      assert_one_node_topology!(client, ctx.forwarding)

      texts = turn_texts!(client, setup, thread)

      assert_relayed_without_rate_limits!(texts, provider_frames(reset_at))
      assert_served_turn!(upstream, ctx.serving_mode)
      assert_recorded_window!(setup, reset_at)
    end
  end

  # The alias route runs the same native socket.
  test "the /backend-api/codex/v1/responses alias keeps codex.rate_limits from the client the same way" do
    put_owner_forwarding!(false)
    reset_at = DateTime.utc_now() |> DateTime.add(3_600, :second) |> DateTime.truncate(:second)
    upstream = start_turn_upstream!(reset_at)
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
    {_server, port} = start_public_endpoint_with_server!()
    thread = "native-rate-limits-alias-#{System.unique_integer([:positive])}"
    client = connect!(port, setup, thread, "/backend-api/codex/v1/responses")
    assert_one_node_topology!(client, :off)

    texts = turn_texts!(client, setup, thread)

    assert_relayed_without_rate_limits!(texts, provider_frames(reset_at))
    assert_served_turn!(upstream, "full")
    assert_recorded_window!(setup, reset_at)
  end

  # The frame crosses nodes on the owner path: the owner's upstream session
  # runs on the peer, and the socket on this node still drops it. The harness
  # peer runs the owner runtime and the Repo only, without the PubSub the quota
  # evidence is published on, so its owner cannot record the evidence; the
  # one-node arms assert it.
  @tag slow: "boots a second VM that owns the session and shares the committed database"
  test "a native socket whose session owner runs on another node keeps codex.rate_limits from the client" do
    put_owner_forwarding!(true)
    enter_peer_owner_topology!()
    reset_at = DateTime.utc_now() |> DateTime.add(3_600, :second) |> DateTime.truncate(:second)
    upstream = start_turn_upstream!(reset_at)
    setup = gateway_setup(upstream)
    assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
    peer_owner = start_peer_window_owner!(setup, @window_id)
    {_server, port} = start_public_endpoint_with_server!()
    client = connect_window!(port, setup)
    assert node(peer_owner.owner_pid) == peer_owner.node
    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(socket_connection_state!(client.socket).codex_session.id)

    texts = turn_texts!(client, setup, @window_thread)

    assert_relayed_without_rate_limits!(texts, provider_frames(reset_at))
    assert_served_turn!(upstream, "full")
  end

  defp start_turn_upstream!(reset_at) do
    start_upstream(
      # provenance: observed codex rust-v0.158.0 codex-api/src/rate_limits.rs (the codex.rate_limits fields its parser reads); frame order, ids, text and the unknown control synthetic
      FakeUpstream.strict_sequence([anchorless_request(1, FakeUpstream.websocket_text_frames(provider_frames(reset_at)))])
    )
  end

  defp provider_frames(reset_at) do
    usage = %{"input_tokens" => 5, "output_tokens" => 3, "total_tokens" => 8}

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => @response_id, "status" => "in_progress"}},
        %{"type" => "codex.response.metadata", "headers" => %{"openai-model" => "provider-model-fixture"}},
        codex_rate_limits_payload(92, reset_at),
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => @message_item},
        %{"type" => "codex.future_control", "sequence" => 1},
        %{"type" => "response.completed", "response" => %{"id" => @response_id, "status" => "completed", "output" => [@message_item], "usage" => usage}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  defp connect!(port, setup, thread, path \\ @turn_path) do
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread, path)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket}
  end

  defp connect_window!(port, setup) do
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), @turn_path, [{"x-codex-window-id", @window_id}])
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket}
  end

  # Forwarding off: the socket holds its own upstream session and no owner
  # serves the session. On: the session's owner runs on this node.
  defp assert_one_node_topology!(client, :off) do
    state = socket_connection_state!(client.socket)
    assert is_pid(state.upstream_websocket_session)
    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(state.codex_session.id)
  end

  defp assert_one_node_topology!(client, :on) do
    state = socket_connection_state!(client.socket)
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    assert node(owner) == node()
  end

  defp turn_texts!(client, setup, thread) do
    frame = released_client_frame(setup, thread).(native_text_input("served account rate limits"), Ecto.UUID.generate(), %{})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    {conn, _websocket, texts} = receive_texts_until_terminal!(conn, websocket, client.ref, [])
    Mint.HTTP.close(conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
    texts
  end

  # Every text frame the Pooler sends, controls included, up to the native
  # terminal.
  defp receive_texts_until_terminal!(conn, websocket, ref, texts) do
    message = receive_mint_socket_message!(conn, @detection_timeout_ms, "timed out waiting for the native terminal")

    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        {websocket, texts} =
          Enum.reduce(responses, {websocket, texts}, fn
            {:data, ^ref, data}, {websocket, texts} ->
              assert {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)
              {websocket, texts ++ Enum.map(frames, &text_frame!/1)}

            _response, acc ->
              acc
          end)

        if Enum.any?(texts, &terminal_text?/1),
          do: {conn, websocket, texts},
          else: receive_texts_until_terminal!(conn, websocket, ref, texts)

      {:error, _conn, reason, _responses} ->
        flunk("websocket receive failed: #{inspect(reason)}")

      :unknown ->
        receive_texts_until_terminal!(conn, websocket, ref, texts)
    end
  end

  defp text_frame!({:text, text}), do: text
  defp text_frame!(frame), do: flunk("unexpected websocket frame #{inspect(elem(frame, 0))}")

  defp terminal_text?(text), do: match?({:ok, %{"type" => type}} when type in @native_terminal_types, CodexPooler.JSON.decode(text))

  # The Pooler's own metadata frame opens the turn; after it the client gets
  # the provider's frames byte for byte, the rate limits excepted.
  defp assert_relayed_without_rate_limits!(texts, provider_frames) do
    CodexPooler.TestDiagnostics.puts(fn -> "native rate limits: client frame types " <> inspect(Enum.map(texts, &CodexPooler.JSON.decode!(&1)["type"])) end)
    assert [pooler_metadata | relayed] = texts
    assert %{"type" => "codex.response.metadata", "headers" => %{"x-models-etag" => etag}} = CodexPooler.JSON.decode!(pooler_metadata)
    assert is_binary(etag) and etag != ""
    refute Enum.any?(texts, &(CodexPooler.JSON.decode!(&1)["type"] == "codex.rate_limits"))
    assert relayed == Enum.reject(provider_frames, &(CodexPooler.JSON.decode!(&1)["type"] == "codex.rate_limits"))
  end

  # One upstream request, served in `mode` (Lite opens the context with its
  # tool manifest), settled as a success.
  defp assert_served_turn!(upstream, mode) do
    assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @detection_timeout_ms
    assert [request] = FakeUpstream.requests(upstream)
    assert served_mode(request.json["input"]) == mode
    assert :ok = FakeUpstream.verify!(upstream)
  end

  # The dropped frame is still the served account's recorded window.
  defp assert_recorded_window!(setup, reset_at) do
    assert window = wait_for_rate_limit_event_window(setup.identity, "primary")
    assert window.source == "codex_rate_limit_event"
    assert Decimal.equal?(window.used_percent, Decimal.new(92))
    assert DateTime.compare(window.reset_at, reset_at) == :eq
    wait_for_rate_limit_event_tasks()
  end

  defp served_mode([%{"type" => "additional_tools"} | _input]), do: "lite"
  defp served_mode(_input), do: "full"
end
