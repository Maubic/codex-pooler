defmodule CodexPooler.UpstreamWebsocketRetirementPeer do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks
  @budget 15_000
  @guid "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

  @spec trust!() :: %{certificate: keyword()}
  def trust! do
    certificate = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, [{:iPAddress, <<127, 0, 0, 1>>}]}]]})
    previous = :persistent_term.get(:pubkey_os_cacerts, :absent)
    directory = Path.join(System.tmp_dir!(), "retirement-#{Ecto.UUID.generate()}")

    on_exit(fn ->
      if previous == :absent, do: :public_key.cacerts_clear(), else: :persistent_term.put(:pubkey_os_cacerts, previous)
      assert :persistent_term.get(:pubkey_os_cacerts, :absent) == previous
      File.rm_rf!(directory)
      refute File.exists?(directory)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "retirement_trust_cleanup", restored: true, temporary_directory_removed: true}) end)
    end)

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    ca = Path.join(directory, "ca.pem")
    File.write!(ca, :public_key.pem_encode(Enum.map(certificate[:cacerts], &{:Certificate, &1, :not_encrypted})))
    File.chmod!(ca, 0o600)
    :ok = :public_key.cacerts_load(String.to_charlist(ca))

    %{certificate: certificate}
  end

  @spec socket_closed?(:ws | :wss, term()) :: boolean()
  def socket_closed?(:ws, socket), do: Port.info(socket) == nil
  def socket_closed?(:wss, socket), do: match?({:error, :closed}, :ssl.connection_information(socket))

  @spec start!(map(), [atom()]) :: %{url: String.t(), ref: reference()}
  def start!(context, scripts) do
    parent = self()
    ref = make_ref()
    {:ok, tracker} = Agent.start(fn -> %{tasks: [], sockets: []} end)

    on_exit(fn ->
      resources = Agent.get(tracker, & &1)
      assert Enum.all?(resources.tasks, &(not Process.alive?(&1)))
      assert Enum.all?(resources.sockets, fn {transport, socket} -> socket_closed?(transport, socket) end)
      Agent.stop(tracker)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "retirement_peer_cleanup", tasks_stopped: true, sockets_closed: true}) end)
    end)

    start_supervised!(Supervisor.child_spec({Task, fn -> peer(parent, ref, context, scripts, tracker) end}, id: ref, restart: :temporary))
    assert_receive {:peer_listening, ^ref, port}, @budget
    scheme = if context.transport == :wss, do: "https", else: "http"
    %{url: "#{scheme}://127.0.0.1:#{port}/backend-api/codex/responses", ref: ref}
  end

  defp peer(parent, ref, context, scripts, tracker) do
    peer_pid = self()
    Agent.update(tracker, &%{&1 | tasks: [peer_pid | &1.tasks]})
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true, recbuf: 4096, ip: {127, 0, 0, 1}])
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

      mode when mode in [:success, :stalled, :small_silent, :early_terminal] ->
        accept = Base.encode64(:crypto.hash(:sha, key <> @guid))
        :ok = transport.send(socket, ["HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\nconnection: Upgrade\r\nsec-websocket-accept: ", accept, "\r\n\r\n"])
    end

    send(parent, {:peer_upgraded, ref, id, self()})

    if script == :early_terminal do
      data = CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_early", "status" => "completed", "output" => []}})
      :ok = transport.send(socket, <<0x81, 126, byte_size(data)::16, data::binary>>)
    end

    if script in [:stalled, :early_terminal] do
      receive do
        :release_peer -> drain(transport, socket, parent, ref, id)
      end
    else
      serve_frames(transport, socket, parent, ref, id, script, 0)
    end
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

      {:ok, <<first, second>>} when script in [:success, :small_silent] ->
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
        if script == :success, do: :ok = transport.send(socket, <<0x81, 126, byte_size(data)::16, data::binary>>)
        serve_frames(transport, socket, parent, ref, id, script, count)

      other ->
        exit({:unexpected_peer_read, elem(other, 0)})
    end
  end

  defp drain(transport, socket, parent, ref, id) do
    case transport.recv(socket, 0, @budget) do
      {:ok, _discarded} -> drain(transport, socket, parent, ref, id)
      {:error, reason} when reason in [:closed, :econnreset] -> send(parent, {:peer_closed, ref, id})
      {:error, reason} -> exit({:peer_drain_failed, reason})
    end
  end
end
