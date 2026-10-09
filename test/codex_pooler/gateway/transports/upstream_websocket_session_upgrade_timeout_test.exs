defmodule CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSessionUpgradeTimeoutTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.{ConnectionUpgrade, Request}
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.TestAppEnv

  @moduletag capture_log: true
  @budget 15_000
  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  setup do
    TestAppEnv.restore_on_exit(OutboundHTTP)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    certificate = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, [{:iPAddress, <<127, 0, 0, 1>>}]}]]})
    previous = :persistent_term.get(:pubkey_os_cacerts, :absent)
    directory = Path.join(System.tmp_dir!(), "upgrade-timeout-#{Ecto.UUID.generate()}")

    on_exit(fn ->
      if previous == :absent, do: :public_key.cacerts_clear(), else: :persistent_term.put(:pubkey_os_cacerts, previous)
      assert :persistent_term.get(:pubkey_os_cacerts, :absent) == previous
      File.rm_rf!(directory)
      refute File.exists?(directory)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "upgrade_trust_cleanup", restored: true, temporary_directory_removed: true}) end)
    end)

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    ca = Path.join(directory, "ca.pem")
    File.write!(ca, :public_key.pem_encode(Enum.map(certificate[:cacerts], &{:Certificate, &1, :not_encrypted})))
    File.chmod!(ca, 0o600)
    :ok = :public_key.cacerts_load(String.to_charlist(ca))

    Code.ensure_loaded!(ConnectionUpgrade)
    pattern = {ConnectionUpgrade, :await_upgrade, 4}
    on_exit(fn -> :erlang.trace_pattern(pattern, false, [:local]) end)
    assert :erlang.trace_pattern(pattern, [{[:"$1", :_, :_, :_], [], [{:message, {:map_get, :socket, :"$1"}}]}], [:local]) == 1
    %{certificate: certificate}
  end

  for transport <- [:ws, :wss] do
    @tag transport: transport
    test "#{transport} two upgrade deadlines release owned connections before a successful successor", context do
      peer = start_peer!(context, [:hold, :hold, :success])
      session = start_session!()
      request = request(peer)

      observations =
        for id <- 1..2 do
          task = Task.Supervisor.async_nolink(start_supervised!({Task.Supervisor, []}, id: make_ref()), fn -> UpstreamWebsocketSession.request(session, request) end)
          assert_receive {:peer_head, ref, ^id, _holder}, @budget
          assert ref == peer.ref
          socket = acquired_socket!(session)
          assert {:error, %{reason: :upstream_websocket_upgrade_timeout}} = Task.await(task, @budget)
          peer_closed = receive do: ({:peer_closed, ^ref, ^id} -> true), after: (1_000 -> false)
          state = :sys.get_state(session)
          refute Map.has_key?(state, :conn)
          assert Process.alive?(session)
          %{socket: socket, peer_closed: peer_closed, socket_closed: socket_closed?(context.transport, socket), owned_tcp_ports: tcp_port_count(session)}
        end

      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "consecutive_upgrade_deadlines", transport: context.transport, tls_verified: context.transport == :wss, peer_closed: Enum.map(observations, & &1.peer_closed), socket_closed: Enum.map(observations, & &1.socket_closed), owned_tcp_ports: Enum.map(observations, & &1.owned_tcp_ports), session_alive: Process.alive?(session)}) end)
      assert Enum.all?(observations, & &1.peer_closed)
      assert Enum.all?(observations, & &1.socket_closed)
      assert Enum.all?(observations, &(&1.owned_tcp_ports == 0))

      assert {:ok, %{terminal: "response.completed"}} = UpstreamWebsocketSession.request(session, %{request | timeouts: %{connect_timeout_ms: 5_000, receive_timeout_ms: 5_000}})
      successor = acquired_socket!(session)
      refute successor in Enum.map(observations, & &1.socket)
      before = :sys.get_state(session)

      for observation <- observations do
        send(session, {if(context.transport == :ws, do: :tcp_closed, else: :ssl_closed), observation.socket})
        send(session, {if(context.transport == :ws, do: :tcp_error, else: :ssl_error), observation.socket, :closed})
      end

      assert :sys.get_state(session).conn == before.conn
      assert {:ok, %{terminal: "response.completed", upstream_websocket_connection: %{reused: true}}} = UpstreamWebsocketSession.request(session, request)
      assert_receive {:peer_request, ref, 3, 2}, @budget
      assert ref == peer.ref
      refute_received {:peer_head, ^ref, 4, _}
      refute socket_closed?(context.transport, successor)
    end

    @tag transport: transport
    test "#{transport} refusal closes acquired connection and the next upgrade succeeds", context do
      peer = start_peer!(context, [:refuse, :success])
      session = start_session!()
      request = request(peer)
      assert {:error, %{reason: {:websocket_upgrade_failed, 503, _headers}}} = UpstreamWebsocketSession.request(session, request)
      socket = acquired_socket!(session)
      assert_receive {:peer_closed, ref, 1}, @budget
      assert ref == peer.ref
      assert socket_closed?(context.transport, socket)
      assert {:ok, %{terminal: "response.completed"}} = UpstreamWebsocketSession.request(session, request)
    end

    @tag transport: transport
    test "#{transport} caller death during held upgrade closes its acquired connection", context do
      peer = start_peer!(context, [:hold, :success])
      session = start_session!()
      request = %{request(peer) | timeouts: %{connect_timeout_ms: 10_000, receive_timeout_ms: 5_000}}
      task = Task.Supervisor.async_nolink(start_supervised!(Task.Supervisor), fn -> UpstreamWebsocketSession.request(session, request) end)
      assert_receive {:peer_head, ref, 1, _holder}, @budget
      assert ref == peer.ref
      socket = acquired_socket!(session)
      Task.shutdown(task, :brutal_kill)
      assert_receive {:peer_closed, ^ref, 1}, @budget
      assert socket_closed?(context.transport, socket)
      assert Process.alive?(session)
      assert {:ok, %{terminal: "response.completed"}} = UpstreamWebsocketSession.request(session, request)
    end
  end

  @tag transport: :wss
  test "an untrusted TLS peer is refused before websocket upgrade", context do
    wrong = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}]})
    peer = start_peer!(%{context | certificate: wrong}, [:hold])
    session = start_session!()
    assert {:error, %{reason: %Mint.TransportError{reason: {:tls_alert, _alert}}}} = UpstreamWebsocketSession.request(session, request(peer))
    assert_receive {:peer_tls_rejected, ref}, @budget
    assert ref == peer.ref
    refute_received {:peer_head, ^ref, _, _}
    assert tcp_port_count(session) == 0
    refute Map.has_key?(:sys.get_state(session), :conn)
  end

  @tag transport: :ws
  test "connect refusal leaves the live session without an acquired connection" do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(socket) end)
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    session = start_session!()
    assert {:error, %{reason: %Mint.TransportError{reason: :econnrefused}}} = UpstreamWebsocketSession.request(session, request(%{url: "http://127.0.0.1:#{port}/responses"}))
    assert tcp_port_count(session) == 0
    refute Map.has_key?(:sys.get_state(session), :conn)
    assert Process.alive?(session)
  end

  defp request(peer) do
    %Request{
      provider_credits_context: CodexPooler.ProviderCreditsDispatchSupport.context!(),
      url: peer.url,
      headers: [{"authorization", "Bearer synthetic-upstream-token"}],
      payload: CodexPooler.JSON.encode!(%{"model" => "upstream-test-model", "input" => [], "stream" => true}),
      timeouts: %{connect_timeout_ms: 1_000, receive_timeout_ms: 5_000},
      writer: fn _text -> :ok end,
      message_mapper: nil
    }
  end

  defp start_session! do
    session = start_supervised!(%{id: make_ref(), start: {UpstreamWebsocketSession, :start_link, [[]]}, restart: :temporary})
    # Only this synthetic test-owned process is observed, and the match spec
    # emits the socket handle rather than Mint headers or payloads.
    :sys.replace_state(session, fn state ->
      Process.flag(:sensitive, false)
      state
    end)

    assert :erlang.trace(session, true, [:call, :arity, {:tracer, self()}]) == 1
    session
  end

  defp acquired_socket!(session) do
    assert_receive {:trace, ^session, :call, {ConnectionUpgrade, :await_upgrade, 4}, socket}, @budget
    socket
  end

  defp tcp_port_count(session) do
    {:links, links} = Process.info(session, :links)
    Enum.count(links, &(is_port(&1) and Port.info(&1, :name) == {:name, ~c"tcp_inet"}))
  end

  defp socket_closed?(:ws, socket), do: Port.info(socket) == nil
  defp socket_closed?(:wss, socket), do: match?({:error, :closed}, :ssl.connection_information(socket))

  defp start_peer!(context, scripts) do
    parent = self()
    ref = make_ref()
    {:ok, tracker} = Agent.start(fn -> %{tasks: [], sockets: []} end)

    on_exit(fn ->
      resources = Agent.get(tracker, & &1)
      assert Enum.all?(resources.tasks, &(not Process.alive?(&1)))
      assert Enum.all?(resources.sockets, fn {transport, socket} -> socket_closed?(transport, socket) end)
      Agent.stop(tracker)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "upgrade_peer_cleanup", tasks_stopped: true, sockets_closed: true}) end)
    end)

    start_supervised!(Supervisor.child_spec({Task, fn -> peer(parent, ref, context, scripts, tracker) end}, id: ref, restart: :temporary))
    assert_receive {:peer_listening, ^ref, port}, @budget
    scheme = if context.transport == :wss, do: "https", else: "http"
    %{url: "#{scheme}://127.0.0.1:#{port}/backend-api/codex/responses", ref: ref}
  end

  defp peer(parent, ref, context, scripts, tracker) do
    peer_pid = self()
    Agent.update(tracker, &%{&1 | tasks: [peer_pid | &1.tasks]})
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true, ip: {127, 0, 0, 1}])
    Agent.update(tracker, &%{&1 | sockets: [{:ws, listener} | &1.sockets]})
    {:ok, port} = :inet.port(listener)
    send(parent, {:peer_listening, ref, port})
    accept_loop(listener, parent, ref, context, scripts, tracker, 1)
  end

  defp accept_loop(listener, parent, ref, context, scripts, tracker, id) do
    {:ok, socket} = :gen_tcp.accept(listener)

    holder =
      spawn_link(fn ->
        receive do
          :owned -> serve(socket, parent, ref, context, Enum.at(scripts, id - 1, List.last(scripts)), id, tracker)
        end
      end)

    Agent.update(tracker, &%{&1 | tasks: [holder | &1.tasks], sockets: [{:ws, socket} | &1.sockets]})
    :ok = :gen_tcp.controlling_process(socket, holder)
    send(holder, :owned)
    accept_loop(listener, parent, ref, context, scripts, tracker, id + 1)
  end

  defp serve(raw, parent, ref, context, script, id, tracker) do
    {transport, socket} =
      if context.transport == :wss do
        case :ssl.handshake(raw, [cert: context.certificate[:cert], key: context.certificate[:key], active: false, mode: :binary], @budget) do
          {:ok, socket} ->
            Agent.update(tracker, &%{&1 | sockets: [{:wss, socket} | &1.sockets]})
            {:ssl, socket}

          {:error, _reason} ->
            send(parent, {:peer_tls_rejected, ref})
            :gen_tcp.close(raw)
            exit(:normal)
        end
      else
        {:gen_tcp, raw}
      end

    {:ok, key} = read_head(transport, socket, "")
    send(parent, {:peer_head, ref, id, self()})

    case script do
      :hold ->
        :ok

      :refuse ->
        :ok = transport.send(socket, "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")

      :success ->
        accept = Base.encode64(:crypto.hash(:sha, key <> @guid))
        :ok = transport.send(socket, ["HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: ", accept, "\r\n\r\n"])
    end

    serve_frames(transport, socket, parent, ref, id, script, 0)
  end

  defp read_head(transport, socket, bytes) do
    if String.contains?(bytes, "\r\n\r\n") do
      [_, key] = Regex.run(~r/sec-websocket-key: ([^\r\n]+)/i, bytes)
      {:ok, key}
    else
      with {:ok, data} <- transport.recv(socket, 0, @budget), do: read_head(transport, socket, bytes <> data)
    end
  end

  defp serve_frames(transport, socket, parent, ref, id, script, count) do
    case transport.recv(socket, 2, @budget) do
      {:error, :closed} ->
        send(parent, {:peer_closed, ref, id})

      {:ok, <<first, second>>} when script == :success ->
        length = Bitwise.band(second, 0x7F)

        length =
          if length == 126 do
            {:ok, <<size::16>>} = transport.recv(socket, 2, @budget)
            size
          else
            length
          end

        {:ok, _mask_and_body} = transport.recv(socket, 4 + length, @budget)
        assert Bitwise.band(first, 0xF) == 1
        count = count + 1
        send(parent, {:peer_request, ref, id, count})
        data = CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_upgrade", "status" => "completed", "output" => []}})
        :ok = transport.send(socket, <<0x81, 126, byte_size(data)::16, data::binary>>)
        serve_frames(transport, socket, parent, ref, id, script, count)

      other ->
        exit({:unexpected_peer_read, elem(other, 0)})
    end
  end
end
