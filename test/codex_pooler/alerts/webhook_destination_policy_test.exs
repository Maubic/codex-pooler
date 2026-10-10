defmodule CodexPooler.Alerts.WebhookDestinationPolicyTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Delivery.{Execution, WebhookDelivery, WebhookDestination, WebhookTransport}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.TestAppEnv
  alias CodexPooler.WebhookDestinationFixture

  setup do
    TestAppEnv.restore_on_exit(OutboundHTTP)
    TestAppEnv.restore_on_exit(WebhookDestination)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    %{user: user} = bootstrap_owner_fixture()
    Map.put(WebhookDestinationFixture.setup!(), :scope, Scope.for_user(user, ["instance_owner"]))
  end

  test "canonical loopback destination is refused before a physical TLS connection", context do
    cert = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, [{:iPAddress, <<127, 0, 0, 1>>}]}]]})
    WebhookDestinationFixture.trust!(cert[:cacerts])
    {:ok, receiver} = FakeUpstream.start_link(FakeUpstream.raw_response("", status: 204), scheme: :https, thousand_island_options: [transport_options: [cert: cert[:cert], key: cert[:key]]])
    on_exit(fn -> FakeUpstream.stop(receiver) end)
    url = "https://127.0.0.1:#{URI.parse(receiver.url).port}/hooks"
    pool = pool_fixture()
    incident = alert_incident_fixture(pool: pool)
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "destination policy", endpoint_url: url, webhook_signing_secret: "synthetic-destination-secret"})
    result = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    count = FakeUpstream.count(receiver)
    CodexPooler.TestDiagnostics.puts("webhook-destination class=loopback actual_requests=#{count}")
    assert {:ok, %{status: "failed", failure_code: "alert_webhook_destination_forbidden"}} = result
    assert count == 0
  end

  test "canonical owned private HTTPS destination retains signed delivery", context do
    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    pool = pool_fixture()
    incident = alert_incident_fixture(pool: pool)
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "private destination", endpoint_url: receiver.url <> "/hooks", webhook_signing_secret: "synthetic-destination-secret"})
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert FakeUpstream.count(receiver) == 1
  end

  test "both address families and mapped forms reject only the declared classes and exact metadata exceptions" do
    forbidden = ["127.1.2.3", "169.254.1.2", "169.254.169.254", "0.0.0.0", "::", "::1", "fe80::1", "febf::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "fd00:ec2::254", "fd20:ce::254", "100.100.100.200", "::ffff:100.100.100.200"]
    allowed = ["10.1.2.3", "172.16.1.2", "192.168.1.2", "fd00::1", "fd20:ce::255", "100.64.1.2", "168.63.129.16", "::ffff:10.1.2.3"]

    for value <- forbidden do
      {:ok, tuple} = :inet.parse_address(String.to_charlist(value))
      assert WebhookDestination.forbidden?(tuple)
    end

    for value <- allowed do
      {:ok, tuple} = :inet.parse_address(String.to_charlist(value))
      refute WebhookDestination.forbidden?(tuple)
    end
  end

  test "mixed DNS and aliases fail before any Mint dial, including synthetic metadata answers", context do
    Code.ensure_loaded!(Mint.HTTP)
    :erlang.trace_pattern({Mint.HTTP, :connect, 4}, [{:_, [], []}], [:local])
    on_exit(fn -> :erlang.trace_pattern({Mint.HTTP, :connect, 4}, false, [:local]) end)
    tracer = spawn_link(fn -> forward_connect_trace(0) end)
    on_exit(fn -> if Process.alive?(tracer), do: Process.exit(tracer, :kill) end)
    :erlang.trace(self(), true, [:call, :set_on_spawn, :arity, {:tracer, tracer}])

    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    {incident, channel} = delivery!(context, receiver.url <> "/hooks")
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert observed_dials(tracer) == 1

    for text <- ["127.0.0.1", "169.254.169.254", "::1", "fe80::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "fd00:ec2::254", "fd20:ce::254", "100.100.100.200", "::ffff:100.100.100.200"] do
      {:ok, address} = :inet.parse_address(String.to_charlist(text))

      resolver = fn _host, family ->
        case {family, tuple_size(address)} do
          {:inet, 4} -> {:ok, [context.ip, address]}
          {:inet, 8} -> {:ok, [context.ip]}
          {:inet6, 8} -> {:ok, [address]}
          {:inet6, 4} -> {:ok, []}
        end
      end

      Application.put_env(:codex_pooler, WebhookDestination, resolver: resolver)
      pool = pool_fixture()
      incident = alert_incident_fixture(pool: pool)
      {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "mixed destination", endpoint_url: "https://alias.example.test/hooks", webhook_signing_secret: "synthetic-destination-secret"})
      assert {:ok, %{status: "failed", failure_code: "alert_webhook_destination_forbidden"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
      assert observed_dials(tracer) == 0
    end
  end

  test "internal hostname resolves once per family and pins numeric address with original TLS and Host", context do
    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    parent = self()

    resolver = fn host, family ->
      send(parent, {:resolved, host, family})
      if family == :inet, do: {:ok, [context.ip]}, else: {:error, :nxdomain}
    end

    Application.put_env(:codex_pooler, WebhookDestination, resolver: resolver)
    pool = pool_fixture()
    incident = alert_incident_fixture(pool: pool)
    port = URI.parse(receiver.url).port
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "internal DNS", endpoint_url: "https://webhook.example.test:#{port}/hooks", webhook_signing_secret: "synthetic-destination-secret"})
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert_receive {:resolved, ~c"webhook.example.test", :inet}
    assert_receive {:resolved, ~c"webhook.example.test", :inet6}
    refute_receive {:resolved, _, _}, 0
    [request] = FakeUpstream.requests(receiver)
    assert Enum.find_value(request.headers, fn {name, value} -> if name == "host", do: value end) == "webhook.example.test:#{port}"
  end

  test "actual UDP DNS answer is pinned and a changed second answer cannot redirect the TLS dial", context do
    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    {dns, dns_port} = dns!(context.ip)
    previous_lookup = :inet_db.res_option(:lookup)
    previous_resolv = :inet_db.res_option(:resolv_conf)
    previous_nameservers = :inet_db.res_option(:nameservers)

    on_exit(fn ->
      :ok = :inet_db.res_option(:resolv_conf, previous_resolv)
      :ok = :inet_db.res_option(:nameservers, previous_nameservers)
      :ok = :inet_db.set_lookup(previous_lookup)
    end)

    :ok = :inet_db.res_option(:resolv_conf, ~c"")
    :ok = :inet_db.res_option(:nameservers, [{{127, 0, 0, 1}, dns_port}])
    :ok = :inet_db.set_lookup([:dns])
    # Leave the production resolver at its real :inet.getaddrs default.
    Application.delete_env(:codex_pooler, WebhookDestination)

    {incident, channel} = delivery!(context, "https://webhook.example.test:#{URI.parse(receiver.url).port}/hooks")
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert FakeUpstream.count(receiver) == 1
    send(dns, {:counts, self()})
    assert_receive {:dns_counts, %{a: 1, aaaa: aaaa_queries}}, 2_000
    assert aaaa_queries in 1..2
    CodexPooler.TestDiagnostics.puts("webhook-dns A_queries=1 AAAA_queries=#{aaaa_queries} default_inet_resolver=true pinned_destination=true")
    # The same live DNS server now gives a forbidden answer; no such second
    # lookup occurred during delivery. This is an actual resolver/wire control.
    assert {:ok, [{127, 0, 0, 1}]} = :inet.getaddrs(~c"webhook.example.test", :inet)
  end

  test "CONNECT uses the validated numeric destination while TLS and Host retain the original name", context do
    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    {proxy, port} = proxy!()
    proxy_options = [proxy: {:http, "127.0.0.1", port, []}, proxy_headers: [{"proxy-authorization", "Basic synthetic"}]]
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: proxy_options, no_proxy: []})
    Application.put_env(:codex_pooler, WebhookDestination, resolver: fn _, family -> if family == :inet, do: {:ok, [context.ip]}, else: {:ok, []} end)
    {incident, channel} = delivery!(context, "https://webhook.example.test:#{URI.parse(receiver.url).port}/hooks")
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert_receive {:proxy_connect, authority, true}, 2_000
    assert authority == "#{context.host}:#{URI.parse(receiver.url).port}"
    [request] = FakeUpstream.requests(receiver)
    assert Enum.find_value(request.headers, fn {name, value} -> if name == "host", do: value end) == "webhook.example.test:#{URI.parse(receiver.url).port}"
    refute Enum.any?(request.headers, fn {name, _} -> name == "proxy-authorization" end)
    assert Task.await(proxy, 5_000) == :closed
  end

  test "original-host no_proxy bypasses the proxy and unresolved proxy-only DNS never reaches CONNECT", context do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    proxy_options = [proxy: {:http, "127.0.0.1", port, []}]
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: proxy_options, no_proxy: ["webhook.example.test"]})
    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    Application.put_env(:codex_pooler, WebhookDestination, resolver: fn _, family -> if family == :inet, do: {:ok, [context.ip]}, else: {:ok, []} end)
    {incident, channel} = delivery!(context, "https://webhook.example.test:#{URI.parse(receiver.url).port}/hooks")
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert {:error, :timeout} = :gen_tcp.accept(listener, 0)
    Application.put_env(:codex_pooler, WebhookDestination, resolver: fn _, _ -> {:error, :nxdomain} end)
    {incident, channel} = delivery!(context, "https://proxy-only.example.test/hooks")
    assert {:error, %{code: "alert_webhook_destination_unresolved", retryable: true}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
    assert {:error, :timeout} = :gen_tcp.accept(listener, 0)
  end

  defp delivery!(context, url) do
    pool = pool_fixture()
    incident = alert_incident_fixture(pool: pool)
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "destination contract", endpoint_url: url, webhook_signing_secret: "synthetic-destination-secret"})
    {incident, channel}
  end

  defp dns!(address) do
    {:ok, socket} = :gen_udp.open(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_udp.close(socket) end)
    {:ok, port} = :inet.port(socket)
    pid = spawn_link(fn -> dns_loop(socket, address, %{a: 0, aaaa: 0}) end)
    :ok = :gen_udp.controlling_process(socket, pid)
    send(pid, :ready)
    on_exit(fn -> if Process.alive?(pid), do: Process.exit(pid, :kill) end)
    {pid, port}
  end

  defp dns_loop(socket, address, counts) do
    receive do
      :ready ->
        :inet.setopts(socket, active: true)
        dns_loop(socket, address, counts)

      {:counts, caller} ->
        send(caller, {:dns_counts, counts})
        dns_loop(socket, address, counts)

      {:udp, ^socket, host, port, <<id::16, _flags::16, 1::16, _::48, question::binary>>} ->
        question_size = byte_size(question) - 4
        <<_name::binary-size(^question_size), type::16, 1::16>> = question
        family = if type == 1, do: :a, else: :aaaa
        selected = if counts.a == 0, do: address, else: {127, 0, 0, 1}
        answer = if type == 1, do: <<0xC00C::16, 1::16, 1::16, 0::32, 4::16>> <> :erlang.list_to_binary(Tuple.to_list(selected)), else: <<>>
        response = <<id::16, 0x8180::16, 1::16, if(type == 1, do: 1, else: 0)::16, 0::32>> <> question <> answer
        :ok = :gen_udp.send(socket, host, port, response)
        dns_loop(socket, address, Map.update!(counts, family, &(&1 + 1)))
    end
  end

  defp proxy! do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    parent = self()

    task =
      Task.async(fn ->
        {:ok, client} = :gen_tcp.accept(listener, 5_000)
        headers = read_headers(client, "")
        ["CONNECT", authority, _] = headers |> String.split("\r\n") |> hd() |> String.split(" ")
        send(parent, {:proxy_connect, authority, String.contains?(String.downcase(headers), "proxy-authorization: basic synthetic")})
        [host, port] = String.split(authority, ":")
        {:ok, tuple} = :inet.parse_address(String.to_charlist(host))
        {:ok, target} = :gen_tcp.connect(tuple, String.to_integer(port), [:binary, active: true], 5_000)
        :ok = :gen_tcp.send(client, "HTTP/1.1 200 Connection established\r\n\r\n")
        :ok = :inet.setopts(client, active: true)

        try do
          relay(client, target)
        after
          :gen_tcp.close(client)
          :gen_tcp.close(target)
        end

        :closed
      end)

    {task, port}
  end

  defp read_headers(socket, bytes) do
    if String.contains?(bytes, "\r\n\r\n") do
      bytes
    else
      {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)
      read_headers(socket, bytes <> data)
    end
  end

  defp relay(client, target) do
    receive do
      {:tcp, ^client, data} ->
        :ok = :gen_tcp.send(target, data)
        relay(client, target)

      {:tcp, ^target, data} ->
        :ok = :gen_tcp.send(client, data)
        relay(client, target)

      {:tcp_closed, _} ->
        :ok

      {:tcp_error, _, reason} ->
        flunk("proxy relay failed: #{inspect(reason)}")
    after
      5_000 -> flunk("proxy relay did not close")
    end
  end

  test "two deliveries use distinct TLS sockets, preserve SNI and close each before the next", context do
    {:ok, listener} = :ssl.listen(0, [:binary, active: false, ip: context.ip, cert: context.cert[:cert], key: context.cert[:key]])
    on_exit(fn -> :ssl.close(listener) end)
    {:ok, {_, port}} = :ssl.sockname(listener)
    parent = self()

    receiver =
      Task.async(fn ->
        Enum.each(1..2, fn index ->
          {:ok, transport} = :ssl.transport_accept(listener, 5_000)
          {:ok, socket} = :ssl.handshake(transport, 5_000)
          {:ok, info} = :ssl.connection_information(socket, [:sni_hostname])
          send(parent, {:tls_identity, index, info[:sni_hostname]})
          read_tls_request(socket, "")
          :ok = :ssl.send(socket, "HTTP/1.1 204 No Content\r\n\r\n")
          assert {:error, :closed} = :ssl.recv(socket, 0, 5_000)
          :ssl.close(socket)
          send(parent, {:tls_closed, index})
        end)
      end)

    Application.put_env(:codex_pooler, WebhookDestination, resolver: fn _, family -> if family == :inet, do: {:ok, [context.ip]}, else: {:ok, []} end)

    for index <- 1..2 do
      {incident, channel} = delivery!(context, "https://webhook.example.test:#{port}/hooks")
      assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1)
      assert_receive {:tls_identity, ^index, ~c"webhook.example.test"}, 2_000
      assert_receive {:tls_closed, ^index}, 2_000
    end

    assert Task.await(receiver, 5_000) == :ok
  end

  defp read_tls_request(socket, buffer) do
    case :binary.split(buffer, "\r\n\r\n") do
      [headers, body] ->
        line = Enum.find(String.split(headers, "\r\n"), &String.starts_with?(String.downcase(&1), "content-length:"))
        length = line |> String.split(":", parts: 2) |> List.last() |> String.trim() |> String.to_integer()
        if byte_size(body) >= length, do: :ok, else: read_tls_more(socket, buffer)

      _ ->
        read_tls_more(socket, buffer)
    end
  end

  defp read_tls_more(socket, buffer) do
    {:ok, bytes} = :ssl.recv(socket, 0, 5_000)
    read_tls_request(socket, buffer <> bytes)
  end

  defp observed_dials(tracer) do
    delivered = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^delivered}, 2_000
    send(tracer, {:drain, self()})
    assert_receive {:dial_count, count}, 2_000
    count
  end

  defp forward_connect_trace(count) do
    receive do
      {:trace, _, :call, {Mint.HTTP, :connect, 4}} ->
        forward_connect_trace(count + 1)

      {:drain, caller} ->
        send(caller, {:dial_count, count})
        forward_connect_trace(0)
    end
  end

  test "DNS and CONNECT stalls consume the existing deadline and release owned callers", context do
    parent = self()

    Application.put_env(:codex_pooler, WebhookDestination,
      resolver: fn _, _ ->
        send(parent, {:dns_waiting, self()})

        receive do
          :release -> {:error, :nxdomain}
        end
      end
    )

    {incident, channel} = delivery!(context, "https://webhook.example.test/hooks")
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}
    assert {:error, %{code: "alert_webhook_delivery_timeout"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1, delivery_execution: execution)
    assert_receive {:dns_waiting, caller}, 2_000
    refute Process.alive?(caller)

    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)

    receiver =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        read_headers(socket, "")
        assert {:error, :closed} = :gen_tcp.recv(socket, 0, 5_000)
        :gen_tcp.close(socket)
        :closed
      end)

    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [proxy: {:http, "127.0.0.1", port, []}], no_proxy: []})
    Application.put_env(:codex_pooler, WebhookDestination, resolver: fn _, family -> if family == :inet, do: {:ok, [context.ip]}, else: {:ok, []} end)
    {incident, channel} = delivery!(context, "https://webhook.example.test/hooks")
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}
    assert {:error, %{retryable: true}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1, delivery_execution: execution)
    assert Task.await(receiver, 5_000) == :closed
  end

  test "delegated Pool operator can create own private channel but cannot alter another operator channel", context do
    %{user: admin} = operator_fixture(context.scope)
    scope = Scope.for_user(admin)
    pool = pool_fixture()
    operator_pool_assignment_fixture(admin, pool)
    receiver = WebhookDestinationFixture.start_fake!(FakeUpstream.raw_response("", status: 204), context)
    {:ok, own} = Alerts.create_channel(scope, %{channel_type: "webhook", display_name: "delegated destination", endpoint_url: receiver.url <> "/hooks", webhook_signing_secret: "synthetic-destination-secret"})
    assert own.created_by_user_id == admin.id
    {:ok, foreign} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "owner destination", endpoint_url: receiver.url <> "/hooks", webhook_signing_secret: "synthetic-destination-secret"})
    assert {:error, %{code: :channel_not_found}} = Alerts.update_channel(scope, foreign.id, %{endpoint_url: "https://example.test/hooks"})
    assert {:ok, _} = Alerts.create_rule(scope, %{pool_id: pool.id, rule_kind: "pool_no_usable_assignments", scope_type: "pool", display_name: "delegated rule", severity: "critical", state: "active", channel_ids: [own.id]})
    incident = alert_incident_fixture(pool: pool)
    assert {:ok, %{status: "sent"}} = WebhookDelivery.deliver_incident_to_channel(incident.id, own.id, 1)
  end

  test "pinned adapter cancellation closes a private-address TLS handshake that never answers", context do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: context.ip])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    parent = self()

    receiver =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        {:ok, _hello} = :gen_tcp.recv(socket, 0, 5_000)
        send(parent, :pinned_tls_started)
        result = await_tcp_close(socket)
        :gen_tcp.close(socket)
        result
      end)

    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}
    request = Req.new(url: "https://#{context.host}:#{port}/hooks", adapter: WebhookTransport, body: "", retry: false, redirect: false, into: fn {:data, _}, acc -> {:cont, acc} end) |> Req.Request.put_private(:webhook_execution, execution)
    assert {:error, _} = Execution.run(execution, fn -> Req.post(request) end)
    assert_receive :pinned_tls_started, 2_000
    assert Task.await(receiver, 5_000) == :closed
  end

  defp await_tcp_close(socket) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, _tls_alert} -> await_tcp_close(socket)
      {:error, :closed} -> :closed
      other -> flunk("pinned handshake did not close: #{inspect(other)}")
    end
  end
end
