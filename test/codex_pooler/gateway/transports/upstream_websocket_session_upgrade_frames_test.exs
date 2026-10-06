defmodule CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSessionUpgradeFramesTest do
  use CodexPooler.DataCase, async: false

  # Frames that arrive in the same TCP read as the upstream `101 Switching Protocols`
  # (icoretech/codex-pooler-findings#304). Mint hands the bytes that follow the upgrade response to its caller as a
  # data part of that response, and the websocket owns them from then on. A raw peer writes the 101 and those bytes
  # in ONE `:gen_tcp.send`, so the session reads them together. FakeUpstream cannot: Bandit writes the 101 and a frame
  # pushed from `init/1` in separate sends, so only a lagging reader would ever see them together.

  @moduletag capture_log: true

  import ExUnit.CaptureLog

  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.Request

  @timeouts %{connect_timeout_ms: 1_000, receive_timeout_ms: 1_000}

  # Detection budget for a notification the peer or the session owes the test; a green run never spends it.
  @detection_timeout_ms 15_000

  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  for mode <- ~w(full lite), tcp <- [:hold, :close] do
    test "a Close behind the 101 fails the request with the peer's code and never writes the payload (#{mode}, tcp #{tcp})" do
      raw_reason = "upgrade-close-private-sentinel-0123456789"

      peer = start_peer!([%{trailing: close_frame(1013, raw_reason), tcp: unquote(tcp), respond: nil}, served()])
      session = start_session!()
      request = request(peer, unquote(mode))
      lifecycle = lifecycle(session)

      {result, log} = with_info_log(fn -> UpstreamWebsocketSession.request(session, request) end)

      assert {:error, failure} = result
      assert failure.reason == :upstream_websocket_closed_before_terminal

      assert Map.take(failure.transport_failure, ~w(phase termination_source peer_close_code peer_close_reason_present peer_close_reason_bytes upstream_committed text_frame_count connection_use)) == %{
               "phase" => "upstream_close",
               "termination_source" => "peer_close_frame",
               "peer_close_code" => 1013,
               "peer_close_reason_present" => true,
               "peer_close_reason_bytes" => byte_size(raw_reason),
               "upstream_committed" => false,
               "text_frame_count" => 0,
               "connection_use" => "fresh"
             }

      # The connection that closed carries the generation its upgrade opened; the session holds none now.
      assert failure.upstream_websocket_connection == %{lifecycle_id: lifecycle.lifecycle_id, generation: 1, reused: false, reconnected: false}
      assert Enum.sort(Map.keys(:sys.get_state(session))) == [:generation, :lifecycle_id]

      assert log =~ "upstream websocket upgrade frames handled frames=1 ping=0 pong=0 text=0 binary=0 close=1 error=0 close_code=1013 lifecycle_id=#{lifecycle.lifecycle_id} generation=1"
      refute log =~ raw_reason
      refute inspect(failure) =~ raw_reason

      # The request frame never left. With the connection held open the peer sees it end without a request frame; with
      # the peer closing first its own end says nothing, and the failure's phase (`upstream_close`, not `send_payload`) does.
      assert_receive {:peer_closed, 1}, @detection_timeout_ms
      refute_received {:peer_request, 1, _count}
      refute_received {:upstream_websocket_frame, _frame}

      assert {:ok, %{terminal: "response.completed", status: 200}} = UpstreamWebsocketSession.request(session, request)
      assert %{generation: 2} = lifecycle(session)
      assert_received {:peer_connection, 2}
    end
  end

  test "the Close is recorded exactly as the same Close read during the request" do
    raw_reason = "upgrade-close-parity-sentinel"
    coalesced = start_peer!([%{trailing: close_frame(1013, raw_reason), tcp: :hold, respond: nil}])
    later = start_peer!([%{trailing: <<>>, tcp: :hold, respond: fn _id, _count -> close_frame(1013, raw_reason) end}])

    assert {:error, behind_the_101} = UpstreamWebsocketSession.request(start_session!(), request(coalesced, "full"))
    assert {:error, during_the_request} = UpstreamWebsocketSession.request(start_session!(), request(later, "full"))

    classification = ~w(exception reason_class reason phase termination_source peer_close_code peer_close_reason_present peer_close_reason_bytes terminal_seen pre_visible_output text_frame_count)

    assert behind_the_101.reason == during_the_request.reason
    assert Map.take(behind_the_101.transport_failure, classification) == Map.take(during_the_request.transport_failure, classification)
    assert behind_the_101.transport_failure["peer_close_code"] == 1013

    # What differs is the truth about the payload: the request that read the Close later had already written it.
    assert behind_the_101.transport_failure["upstream_committed"] == false
    assert during_the_request.transport_failure["upstream_committed"] == true
  end

  # The provider's 101 head arrives as two TLS records (usage-probe skill, upgrade handshake probe), so a frame that
  # shares the SECOND record shares the read that completes the head, not the one that carried the status line: the
  # data part then comes out of a later `Mint.WebSocket.stream/2` call than the status. The session's test-only
  # `:upgrade_clock` is read once per pass of the upgrade loop, so its third read proves the status line was
  # processed before the peer sends the rest of the head and the frame.
  test "a frame in the read that completes a split 101 head is kept" do
    raw_reason = "split-head-close-sentinel"
    peer = start_peer!([%{trailing: close_frame(1013, raw_reason), tcp: :hold, respond: nil, split_head: true}])
    session = start_session!()
    request = %{request(peer, "full") | timeouts: Map.put(@timeouts, :upgrade_clock, answered_clock(self()))}

    task = Task.async(fn -> UpstreamWebsocketSession.request(session, request) end)

    # The deadline's read, the first pass's read, then the pass that follows the status line.
    answer_clock_read!()
    answer_clock_read!()
    assert_receive {:peer_head_split, 1, peer_pid}, @detection_timeout_ms
    answer_clock_read!()
    send(peer_pid, :release_head)

    assert {:error, failure} = await_with_clock(task)
    assert failure.reason == :upstream_websocket_closed_before_terminal

    assert Map.take(failure.transport_failure, ~w(phase termination_source peer_close_code peer_close_reason_bytes)) == %{
             "phase" => "upstream_close",
             "termination_source" => "peer_close_frame",
             "peer_close_code" => 1013,
             "peer_close_reason_bytes" => byte_size(raw_reason)
           }

    assert_receive {:peer_closed, 1}, @detection_timeout_ms
    refute_received {:peer_request, 1, _count}
  end

  for mode <- ~w(full lite) do
    test "a Ping behind the 101 is answered and the request is served on the same connection (#{mode})" do
      ping_payload = "upgrade-ping-witness"
      peer = start_peer!([%{trailing: ping_frame(ping_payload), tcp: :hold, respond: &terminal_response/2}])
      session = start_session!()
      request = request(peer, unquote(mode))

      {first, log} = with_info_log(fn -> UpstreamWebsocketSession.request(session, request) end)

      assert {:ok, %{terminal: "response.completed", status: 200}} = first
      assert_receive {:peer_pong, 1, ^ping_payload}, @detection_timeout_ms
      assert log =~ "upstream websocket upgrade frames handled frames=1 ping=1 pong=0 text=0 binary=0 close=0 error=0 close_code=none"

      # Answering it neither retires nor replaces the connection.
      assert {:ok, %{terminal: "response.completed", status: 200}} = UpstreamWebsocketSession.request(session, request)
      assert %{generation: 1} = lifecycle(session)
      refute_received {:peer_connection, 2}
    end

    test "a text frame behind the 101 reaches the request ahead of its terminal (#{mode})" do
      created = CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => "resp_upgrade_created"}})
      peer = start_peer!([%{trailing: text_frame(created), tcp: :hold, respond: &terminal_response/2}])
      session = start_session!()

      {result, log} = with_info_log(fn -> UpstreamWebsocketSession.request(session, request(peer, unquote(mode))) end)

      assert {:ok, %{terminal: "response.completed", status: 200, response_id: "resp_upgrade_created"}} = result
      assert log =~ "upstream websocket upgrade frames handled frames=1 ping=0 pong=0 text=1 binary=0 close=0 error=0 close_code=none"
      refute log =~ "resp_upgrade_created"

      assert_received {:upstream_websocket_frame, first}
      assert first =~ "resp_upgrade_created"
      assert_received {:upstream_websocket_frame, second}
      assert second =~ "resp_peer_1_1"
      refute_received {:upstream_websocket_frame, _third}

      # The turn ran the ordinary request path, serving-mode bookkeeping included.
      assert %{last_successful_effective_serving_mode: unquote(mode)} = :sys.get_state(session)
    end

    test "a Ping and a text frame behind the 101 are both handled (#{mode})" do
      created = CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => "resp_upgrade_created"}})
      trailing = [ping_frame("upgrade-ping-first"), text_frame(created)]
      peer = start_peer!([%{trailing: trailing, tcp: :hold, respond: &terminal_response/2}])

      assert {:ok, %{terminal: "response.completed", response_id: "resp_upgrade_created"}} = UpstreamWebsocketSession.request(start_session!(), request(peer, unquote(mode)))
      assert_receive {:peer_pong, 1, "upgrade-ping-first"}, @detection_timeout_ms
      assert_received {:upstream_websocket_frame, first}
      assert first =~ "resp_upgrade_created"
    end

    for cut <- [1, 2, 5] do
      test "a frame cut by the 101's read completes with the next read, cut after #{cut} bytes (#{mode})" do
        terminal = terminal_frame(1, 1)
        <<head::binary-size(unquote(cut)), rest::binary>> = terminal
        peer = start_peer!([%{trailing: head, tcp: :hold, respond: fn _id, _count -> rest end}])
        session = start_session!()

        assert {:ok, %{terminal: "response.completed", status: 200}} = UpstreamWebsocketSession.request(session, request(peer, unquote(mode)))

        assert_received {:upstream_websocket_frame, text}
        assert text =~ "resp_peer_1_1"
        refute_received {:upstream_websocket_frame, _second}
      end
    end
  end

  test "a frame the decoder rejects behind the 101 fails the request before the payload is written" do
    peer = start_peer!([%{trailing: <<0x81, 1, 0xFF>>, tcp: :hold, respond: nil}, served()])
    session = start_session!()
    request = request(peer, "full")

    {result, log} = with_info_log(fn -> UpstreamWebsocketSession.request(session, request) end)

    assert {:error, failure} = result
    # Only the decoder's error class is kept: Mint's frame errors can carry the frame's own bytes.
    assert failure.reason == {:websocket_decode_failed, :invalid_utf8}

    assert Map.take(failure.transport_failure, ~w(phase termination_source upstream_committed)) == %{
             "phase" => "decode",
             "termination_source" => "websocket_decode_error",
             "upstream_committed" => false
           }

    assert log =~ "upstream websocket upgrade frames handled frames=1 ping=0 pong=0 text=0 binary=0 close=0 error=1 close_code=none"
    assert_receive {:peer_closed, 1}, @detection_timeout_ms
    refute_received {:peer_request, 1, _count}

    assert {:ok, %{terminal: "response.completed"}} = UpstreamWebsocketSession.request(session, request)
  end

  test "a frame the decoder rejects during the request fails it instead of ending the session" do
    peer = start_peer!([%{trailing: <<>>, tcp: :hold, respond: fn _id, _count -> <<0x81, 1, 0xFF>> end}])
    session = start_session!()

    assert {:error, failure} = UpstreamWebsocketSession.request(session, request(peer, "full"))

    assert failure.reason == {:websocket_decode_failed, :invalid_utf8}
    assert Process.alive?(session)
  end

  test "a binary frame behind the 101 fails the request before the payload is written" do
    peer = start_peer!([%{trailing: <<0x82, 1, 0>>, tcp: :hold, respond: nil}])

    assert {:error, %{reason: :unexpected_upstream_websocket_binary, transport_failure: %{"termination_source" => "unexpected_binary_frame", "upstream_committed" => false}}} =
             UpstreamWebsocketSession.request(start_session!(), request(peer, "full"))

    assert_receive {:peer_closed, 1}, @detection_timeout_ms
    refute_received {:peer_request, 1, _count}
  end

  # One peer script per accepted connection: the bytes written with the 101, whether the TCP connection is closed
  # right behind them, and the bytes written after each request frame. The last script serves every later connection.
  defp served, do: %{trailing: <<>>, tcp: :hold, respond: &terminal_response/2}

  defp terminal_response(id, count), do: terminal_frame(id, count)

  defp terminal_frame(id, count) do
    text_frame(CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_peer_#{id}_#{count}", "status" => "completed"}}))
  end

  defp request(peer, mode) do
    owner = self()

    %Request{
      provider_credits_context: CodexPooler.ProviderCreditsDispatchSupport.context!(),
      url: peer.url,
      headers: [{"authorization", "Bearer synthetic-upstream-token"}],
      payload:
        CodexPooler.JSON.encode!(%{
          "model" => "upstream-test-model",
          "input" => [%{"type" => "message", "role" => "user", "content" => "sample"}],
          "stream" => true
        }),
      timeouts: @timeouts,
      writer: fn text -> send(owner, {:upstream_websocket_frame, text}) end,
      message_mapper: nil,
      effective_serving_mode: mode
    }
  end

  # Supervised, so a session that crashes ends its request call with `upstream_websocket_session_unavailable` instead
  # of taking the test process down through a link.
  defp start_session! do
    start_supervised!(%{id: make_ref(), start: {UpstreamWebsocketSession, :start_link, [[]]}, restart: :temporary})
  end

  defp lifecycle(session), do: session |> :sys.get_state() |> Map.take([:lifecycle_id, :generation])

  defp with_info_log(fun) do
    previous_logger_level = Logger.level()
    Logger.configure(level: :info)
    on_exit(fn -> Logger.configure(level: previous_logger_level) end)

    try do
      with_log([level: :info], fun)
    after
      Logger.configure(level: previous_logger_level)
    end
  end

  # A clock for the session's test-only `:upgrade_clock` timeout: every read asks the test for the time and waits for
  # its answer.
  defp answered_clock(test_pid) do
    fn ->
      ref = make_ref()
      send(test_pid, {:upgrade_clock_read, self(), ref})

      receive do
        {^ref, now_ms} -> now_ms
      after
        @detection_timeout_ms -> exit(:upgrade_clock_read_unanswered)
      end
    end
  end

  defp answer_clock_read! do
    assert_receive {:upgrade_clock_read, reader, ref}, @detection_timeout_ms
    send(reader, {ref, System.monotonic_time(:millisecond)})
  end

  # `Task.await/2` would leave the clock reads of the task's session unanswered.
  defp await_with_clock(%Task{ref: task_ref} = task) do
    receive do
      {:upgrade_clock_read, reader, ref} ->
        send(reader, {ref, System.monotonic_time(:millisecond)})
        await_with_clock(task)

      {^task_ref, result} ->
        Process.demonitor(task_ref, [:flush])
        result
    after
      @detection_timeout_ms -> flunk("the request did not finish")
    end
  end

  # The websocket frames a server writes (unmasked).
  defp text_frame(payload) when byte_size(payload) < 126, do: <<0x81, byte_size(payload), payload::binary>>
  defp text_frame(payload) when byte_size(payload) <= 65_535, do: <<0x81, 126, byte_size(payload)::16, payload::binary>>
  defp ping_frame(payload) when byte_size(payload) < 126, do: <<0x89, byte_size(payload), payload::binary>>
  defp close_frame(code, reason) when byte_size(reason) <= 123, do: <<0x88, byte_size(reason) + 2, code::16, reason::binary>>

  # --- the raw peer ---------------------------------------------------------------------------------------------

  defp start_peer!(scripts) when is_list(scripts) do
    owner = self()
    ref = make_ref()

    start_supervised!(Supervisor.child_spec({Task, fn -> peer(owner, ref, scripts) end}, id: ref, restart: :temporary))

    assert_receive {^ref, :listening, port}, @detection_timeout_ms
    %{url: "http://127.0.0.1:#{port}/backend-api/codex/responses"}
  end

  defp peer(owner, ref, scripts) do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(listen)
    send(owner, {ref, :listening, port})
    accept_loop(listen, owner, scripts, 1)
  end

  defp accept_loop(listen, owner, scripts, id) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        send(owner, {:peer_connection, id})
        serve(socket, id, Enum.at(scripts, id - 1, List.last(scripts)), owner)
        :gen_tcp.close(socket)
        send(owner, {:peer_closed, id})
        accept_loop(listen, owner, scripts, id + 1)

      {:error, _reason} ->
        :ok
    end
  end

  defp serve(socket, id, script, owner) do
    with {:ok, key} <- read_upgrade_key(socket) do
      accept = Base.encode64(:crypto.hash(:sha, key <> @guid))

      head = [
        "HTTP/1.1 101 Switching Protocols\r\n",
        "upgrade: websocket\r\n",
        "connection: Upgrade\r\n",
        "sec-websocket-accept: ",
        accept,
        "\r\n\r\n"
      ]

      if Map.get(script, :split_head, false) do
        # The status line alone, then the rest of the head with the trailing bytes once the test releases it.
        [status_line | rest] = head
        :ok = :gen_tcp.send(socket, status_line)
        send(owner, {:peer_head_split, id, self()})

        receive do
          :release_head -> :ok
        after
          @detection_timeout_ms -> exit(:head_never_released)
        end

        :ok = :gen_tcp.send(socket, [rest, script.trailing])
      else
        # One `send`: the 101 and the script's trailing bytes share a TCP segment.
        :ok = :gen_tcp.send(socket, [head, script.trailing])
      end

      if script.tcp == :hold, do: serve_frames(socket, id, script, owner, 0)
    end
  end

  defp read_upgrade_key(socket, acc \\ "") do
    if String.contains?(acc, "\r\n\r\n") do
      case Regex.run(~r/sec-websocket-key: ([^\r\n]+)/i, acc) do
        [_all, key] -> {:ok, key}
        nil -> {:error, :missing_websocket_key}
      end
    else
      with {:ok, data} <- :gen_tcp.recv(socket, 0, @detection_timeout_ms), do: read_upgrade_key(socket, acc <> data)
    end
  end

  # The peer's wait for the next client frame is an idle wait, ended by the session closing its socket.
  defp serve_frames(socket, id, script, owner, requests) do
    case recv_client_frame(socket) do
      {:ok, :text, _payload} ->
        requests = requests + 1
        send(owner, {:peer_request, id, requests})
        if script.respond, do: :ok = :gen_tcp.send(socket, script.respond.(id, requests))
        serve_frames(socket, id, script, owner, requests)

      {:ok, :ping, _payload} ->
        serve_frames(socket, id, script, owner, requests)

      {:ok, :pong, payload} ->
        send(owner, {:peer_pong, id, payload})
        serve_frames(socket, id, script, owner, requests)

      {:ok, :close, _payload} ->
        :ok

      {:error, _reason} ->
        :ok
    end
  end

  defp recv_client_frame(socket) do
    with {:ok, <<first, second>>} <- :gen_tcp.recv(socket, 2, 60_000),
         {:ok, length} <- client_payload_length(socket, Bitwise.band(second, 0x7F)),
         {:ok, mask} <- :gen_tcp.recv(socket, 4, @detection_timeout_ms),
         {:ok, payload} <- recv_payload(socket, length) do
      {:ok, opcode(Bitwise.band(first, 0x0F)), unmask(payload, mask)}
    end
  end

  defp client_payload_length(_socket, length) when length < 126, do: {:ok, length}

  defp client_payload_length(socket, 126) do
    with {:ok, <<length::16>>} <- :gen_tcp.recv(socket, 2, @detection_timeout_ms), do: {:ok, length}
  end

  defp client_payload_length(socket, 127) do
    with {:ok, <<length::64>>} <- :gen_tcp.recv(socket, 8, @detection_timeout_ms), do: {:ok, length}
  end

  defp recv_payload(_socket, 0), do: {:ok, ""}
  defp recv_payload(socket, length), do: :gen_tcp.recv(socket, length, @detection_timeout_ms)

  defp unmask(payload, mask) do
    mask
    |> :binary.copy(div(byte_size(payload) + 3, 4))
    |> binary_part(0, byte_size(payload))
    |> :crypto.exor(payload)
  end

  defp opcode(0x1), do: :text
  defp opcode(0x8), do: :close
  defp opcode(0x9), do: :ping
  defp opcode(0xA), do: :pong
  defp opcode(_opcode), do: :unknown
end
