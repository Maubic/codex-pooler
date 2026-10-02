defmodule CodexPooler.Gateway.Transports.UpstreamWebsocketProxyTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.Request
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.ProviderCreditsDispatchSupport

  @timeouts %{connect_timeout_ms: 1_000, receive_timeout_ms: 1_000}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(OutboundHTTP)
    :ok
  end

  test "HTTPS upstream websocket sends CONNECT and Basic auth through https_proxy" do
    {proxy_port, proxy_task} = start_connect_proxy(self())
    authorization = "Basic " <> Base.encode64("proxy-user:proxy-pass")

    Application.put_env(:codex_pooler, OutboundHTTP,
      proxy_config: %{
        http: [],
        https: [
          proxy: {:http, "127.0.0.1", proxy_port, []},
          proxy_headers: [{"proxy-authorization", authorization}]
        ],
        no_proxy: []
      }
    )

    context = ProviderCreditsDispatchSupport.context!()

    assert {:error, _reason} =
             UpstreamWebsocketSession.request_once(%Request{
               provider_credits_context: context,
               url: "https://unresolvable.invalid/backend-api/codex/responses",
               headers: [],
               payload: "{}",
               timeouts: @timeouts,
               writer: fn _text -> :ok end
             })

    assert_receive {:proxy_request, request}, 5_000
    assert request =~ "CONNECT unresolvable.invalid:443 HTTP/1.1\r\n"
    assert String.downcase(request) =~ "proxy-authorization: #{String.downcase(authorization)}"
    Task.await(proxy_task, 5_000)
  end

  test "authenticated CONNECT tunnel verifies TLS and exchanges websocket frames" do
    certificate =
      :public_key.pkix_test_data(%{
        root: [digest: :sha256, key: {:rsa, 2048, 65_537}],
        peer: [
          digest: :sha256,
          key: {:rsa, 2048, 65_537},
          extensions: [{:Extension, {2, 5, 29, 17}, false, [{:dNSName, ~c"upstream.invalid"}]}]
        ]
      })

    trust_test_ca!(certificate[:cacerts])

    terminal =
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => "resp_synthetic_proxy", "status" => "completed"}
      })

    # provenance: synthetic_adversarial
    {:ok, upstream} =
      FakeUpstream.start_link(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            path: "/backend-api/codex/responses",
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond: FakeUpstream.websocket_text_frames([terminal])
          )
        ]),
        scheme: :https,
        thousand_island_options: [
          transport_options: [cert: certificate[:cert], key: certificate[:key]]
        ]
      )

    on_exit(fn -> FakeUpstream.stop(upstream) end)
    upstream_port = URI.parse(FakeUpstream.url(upstream)).port
    {proxy_port, proxy_task} = start_tunnel(upstream_port)
    monitor = Process.monitor(proxy_task.pid)
    authorization = "Basic " <> Base.encode64("synthetic-user:synthetic-password")

    Application.put_env(:codex_pooler, OutboundHTTP,
      proxy_config: %{
        http: [],
        https: [
          proxy: {:http, "127.0.0.1", proxy_port, []},
          proxy_headers: [{"proxy-authorization", authorization}]
        ],
        no_proxy: []
      }
    )

    parent = self()
    context = ProviderCreditsDispatchSupport.context!()

    assert {:ok, _result} =
             UpstreamWebsocketSession.request_once(%Request{
               provider_credits_context: context,
               url: "https://upstream.invalid/backend-api/codex/responses",
               headers: [],
               payload: CodexPooler.JSON.encode!(%{"type" => "response.create"}),
               timeouts: %{connect_timeout_ms: 5_000, receive_timeout_ms: 5_000},
               writer: fn text ->
                 send(parent, {:upstream_frame, text})
                 :ok
               end
             })

    assert_receive {:tunnel_request, request}, 15_000
    assert request =~ "CONNECT upstream.invalid:443 HTTP/1.1\r\n"
    assert String.downcase(request) =~ "proxy-authorization: #{String.downcase(authorization)}"
    assert_receive {:upstream_frame, ^terminal}, 15_000
    assert :ok = Task.await(proxy_task, 15_000)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 15_000
    assert :ok = FakeUpstream.verify!(upstream)
  end

  defp trust_test_ca!(certificates) do
    # OTP's documented CA loader stores this cache; restore the exact previous
    # value (including absence) after the exclusively synchronous test.
    previous = :persistent_term.get(:pubkey_os_cacerts, :not_loaded)

    directory =
      Path.join(System.tmp_dir!(), "pooler-proxy-ca-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      if previous == :not_loaded,
        do: :public_key.cacerts_clear(),
        else: :persistent_term.put(:pubkey_os_cacerts, previous)

      File.rm_rf!(directory)
      refute File.exists?(directory)
      assert :persistent_term.get(:pubkey_os_cacerts, :not_loaded) == previous
    end)

    File.mkdir!(directory)
    File.chmod!(directory, 0o700)
    path = Path.join(directory, "ca.pem")

    File.write!(
      path,
      :public_key.pem_encode(Enum.map(certificates, &{:Certificate, &1, :not_encrypted}))
    )

    :ok = :public_key.cacerts_load(String.to_charlist(path))
  end

  defp start_tunnel(upstream_port) do
    {:ok, listener} =
      :gen_tcp.listen(0, [:binary, active: false, packet: :raw, ip: {127, 0, 0, 1}])

    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, {_, port}} = :inet.sockname(listener)
    parent = self()
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        {:ok, downstream} = :gen_tcp.accept(listener, 15_000)
        send(parent, {:tunnel_request, recv_headers(downstream, "")})

        {:ok, upstream} =
          :gen_tcp.connect({127, 0, 0, 1}, upstream_port, [:binary, active: false], 5_000)

        try do
          :ok = :gen_tcp.send(downstream, "HTTP/1.1 200 Connection Established\r\n\r\n")
          :ok = :inet.setopts(downstream, active: true)
          :ok = :inet.setopts(upstream, active: true)
          tunnel(downstream, upstream)
        after
          :gen_tcp.close(downstream)
          :gen_tcp.close(upstream)
          :gen_tcp.close(listener)
        end
      end)

    {port, task}
  end

  defp tunnel(downstream, upstream) do
    receive do
      {:tcp, ^downstream, data} ->
        :ok = :gen_tcp.send(upstream, data)
        tunnel(downstream, upstream)

      {:tcp, ^upstream, data} ->
        :ok = :gen_tcp.send(downstream, data)
        tunnel(downstream, upstream)

      {:tcp_closed, socket} when socket in [downstream, upstream] ->
        :ok
    after
      15_000 -> raise "test tunnel did not close"
    end
  end

  defp start_connect_proxy(parent) do
    {:ok, listener} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :raw,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    {:ok, {_address, port}} = :inet.sockname(listener)

    task =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        request = recv_headers(socket, "")
        send(parent, {:proxy_request, request})
        :ok = :gen_tcp.send(socket, "HTTP/1.1 502 Bad Gateway\r\ncontent-length: 0\r\n\r\n")
        :gen_tcp.close(socket)
        :gen_tcp.close(listener)
      end)

    {port, task}
  end

  defp recv_headers(socket, buffer) do
    if String.contains?(buffer, "\r\n\r\n") do
      buffer
    else
      {:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
      recv_headers(socket, buffer <> chunk)
    end
  end
end
