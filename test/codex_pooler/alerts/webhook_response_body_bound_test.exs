defmodule CodexPooler.Alerts.WebhookResponseBodyBoundTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Delivery.{Execution, WebhookDelivery}
  alias CodexPooler.Alerts.Schemas.AlertDeliveryAttempt
  alias CodexPooler.Jobs.AlertDeliveryWorker
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.TestAppEnv
  alias CodexPooler.TestDiagnostics
  alias Oban.Engines.Basic

  setup do
    TestAppEnv.restore_on_exit(OutboundHTTP)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    %{user: user} = bootstrap_owner_fixture()
    fixture = CodexPooler.WebhookDestinationFixture.setup!()
    Map.put(fixture, :scope, Scope.for_user(user, ["instance_owner"]))
  end

  test "installed functional Req collector discards real TLS bytes while preserving status and headers", context do
    for size <- [1_024, 1_048_576] do
      {url, receiver} = receiver!(context, {:length, size, 202})
      assert {:ok, baseline} = OutboundHTTP.post(url, body: "", decode_body: false, retry: false)
      assert byte_size(baseline.body) == size
      assert Task.await(receiver) == :closed
      {url, receiver} = receiver!(context, {:length, size, 202})
      parent = self()

      collector = fn {:data, data}, {request, response} = acc ->
        send(parent, {:collector_observed, byte_size(data), response.status, response.headers["x-synthetic-receiver"], byte_size(response.body)})
        assert request.method == :post
        {:cont, acc}
      end

      assert {:ok, response} = OutboundHTTP.post(url, body: "", decode_body: false, retry: false, into: collector)
      assert response.status == 202
      assert response.body == ""
      assert response.headers["x-synthetic-receiver"] == ["yes"]
      assert Task.await(receiver) == :closed
      assert collector_bytes(0) == size
      TestDiagnostics.puts("webhook-collector-probe sent_bytes=#{size} old_retained=#{byte_size(baseline.body)} discard_retained=0 status=202 headers_preserved=true")
    end
  end

  for framing <- [:length, :chunked], size <- [0, 1, 16_384, 1_048_576] do
    test "real signed worker #{framing} #{size} response bytes retain no body and record one sent receipt", context do
      fixture = fixture!(context, {unquote(framing), unquote(size), 200})
      trace_response!()
      assert :ok = Oban.Testing.perform_job(fixture.job, [])
      assert_receive {:response_retained, 200, bytes}, 2_000
      TestDiagnostics.puts("webhook-body framing=#{unquote(framing)} sent_bytes=#{unquote(size)} retained_bytes=#{bytes}")
      assert bytes == 0
      assert_receive {:signed_request, true}, 2_000
      assert Task.await(fixture.receiver) == :closed
      [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
      assert attempt.status == "sent"
      assert attempt.response_metadata["response_status_code"] == 200
      assert attempt.response_metadata["delivery_execution"]["job_id"] == fixture.job.id
      refute Map.has_key?(attempt.response_metadata, "body")
    end
  end

  for {status, expected, retryable} <- [{204, "sent", false}, {400, "failed", false}, {503, "retryable", true}] do
    test "complete status #{status} keeps classification independently of response data", context do
      size = if unquote(status) == 204, do: 0, else: 1_048_576
      fixture = fixture!(context, {:length, size, unquote(status)})
      WebhookDelivery.deliver_incident_to_channel(fixture.incident.id, fixture.channel.id, 1, delivery_execution: Execution.new(fixture.job))
      assert Task.await(fixture.receiver) == :closed
      [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
      assert attempt.status == unquote(expected)
      assert attempt.retryable == unquote(retryable)
      assert attempt.response_metadata["response_status_code"] == unquote(status)
    end
  end

  for mode <- [:truncated, :malformed, :disconnect] do
    test "#{mode} transfer preserves transport failure instead of inventing successful delivery", context do
      fixture = fixture!(context, unquote(mode))
      assert {:error, %{retryable: true}} = WebhookDelivery.deliver_incident_to_channel(fixture.incident.id, fixture.channel.id, 1, delivery_execution: Execution.new(fixture.job))
      assert Task.await(fixture.receiver) == :closed
      [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
      assert attempt.status == "retryable"
      assert String.starts_with?(attempt.failure_code, "alert_webhook_transport_")
      refute attempt.status == "sent"
    end
  end

  test "production collector retains bounded referenced binaries after one and eight MiB before transfer completion", context do
    trace_response!()

    for size <- [1_048_576, 8_388_608] do
      fixture = fixture!(context, {:held, size, 200})
      worker = Task.async(fn -> Oban.Testing.perform_job(fixture.job, []) end)
      {caller, received} = await_discarded(size, 0, nil)
      assert received == size
      assert_receive {:body_waiting, receiver}, 2_000
      assert :erlang.garbage_collect(caller)
      {:binary, binaries} = Process.info(caller, :binary)
      referenced = Enum.reduce(binaries, 0, fn {_address, bytes, _refs}, total -> total + bytes end)
      TestDiagnostics.puts("webhook-reference-sample received_bytes=#{received} referenced_bytes=#{referenced} transient_callback_chunk_finished=true")
      # This is an observed caller retention ceiling, not a response-size policy
      # or a whole-VM memory claim. It stays independent of total received bytes.
      assert referenced < 262_144
      send(receiver, :finish)
      assert :ok = Task.await(worker)
      assert_receive {:response_retained, 200, 0}, 2_000
      assert Task.await(fixture.receiver) == :closed
    end
  end

  test "a short full-delivery deadline preserves its receipt before HTTP or during a delayed body", context do
    fixture = fixture!(context, :delayed)
    receiver_monitor = Process.monitor(fixture.receiver.pid)
    assert_receive {:delayed_listener, listener}, 2_000
    execution = %{Execution.new(fixture.job) | deadline: System.monotonic_time(:millisecond) + 200}
    assert {:error, %{code: "alert_webhook_delivery_timeout", retryable: true}} = WebhookDelivery.deliver_incident_to_channel(fixture.incident.id, fixture.channel.id, 1, delivery_execution: execution)
    # Closing only the listener releases a never-connected accept. An already
    # accepted TLS socket must still observe the actual client's closure.
    :ok = :ssl.close(listener)
    phase = Task.await(fixture.receiver, 5_000)
    assert_receive {:DOWN, ^receiver_monitor, :process, _, :normal}, 2_000
    assert phase in [:not_connected, :closed_before_http, :closed_before_headers, :closed]

    if phase == :closed do
      assert_receive {:signed_request, true}, 2_000
      assert_receive :delayed_headers_sent, 2_000
      assert_receive :client_socket_closed, 2_000
    else
      refute_received :client_socket_closed
    end

    assert Execution.remaining(execution) == 0
    TestDiagnostics.puts("webhook-delayed-timeout phase=#{phase} deadline_ms=200 receiver_down=true")
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
    assert attempt.status == "retryable"
    assert attempt.response_metadata["delivery_outcome"] == "unknown"
  end

  defp await_discarded(size, received, caller) when received >= size, do: {caller, received}

  defp await_discarded(size, received, _caller) do
    receive do
      {:chunk_discarded, caller, bytes} -> await_discarded(size, received + bytes, caller)
    after
      5_000 -> flunk("streaming collector did not consume expected response bytes")
    end
  end

  for encoding <- [:valid_gzip, :invalid_gzip] do
    test "complete #{encoding} 2xx representation is discarded without validation or retry", context do
      fixture = fixture!(context, unquote(encoding))
      trace_response!()
      assert :ok = Oban.Testing.perform_job(fixture.job, [])
      assert_receive {:response_retained, 200, 0}, 2_000
      assert_receive {:signed_request, true}, 2_000
      assert Task.await(fixture.receiver) == :closed
      [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
      assert attempt.status == "sent"
      refute attempt.retryable
      assert attempt.response_metadata["response_status_code"] == 200
    end
  end

  test "the default Req collector buffers encoded bytes without validating the unused representation", context do
    {url, receiver} = receiver!(context, :invalid_gzip)
    assert {:ok, response} = OutboundHTTP.post(url, body: "", decode_body: false, retry: false)
    assert response.status == 200
    assert byte_size(response.body) == byte_size("invalid-gzip-representation")
    assert Task.await(receiver) == :closed
  end

  defp collector_bytes(total) do
    receive do
      {:collector_observed, bytes, 202, ["yes"], 0} -> collector_bytes(total + bytes)
    after
      0 -> total
    end
  end

  defp fixture!(context, mode) do
    {url, receiver} = receiver!(context, mode)
    pool = pool_fixture()
    incident = alert_incident_fixture(pool: pool)
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "response contract", endpoint_url: url, webhook_signing_secret: "synthetic-response-secret"})
    queue = "response_#{System.unique_integer([:positive])}"
    job = %{"alert_incident_id" => incident.id, "alert_channel_id" => channel.id} |> AlertDeliveryWorker.new(queue: queue) |> Repo.insert!()
    conf = Oban.config()
    {:ok, meta} = Basic.init(conf, queue: queue, limit: 1)
    {:ok, {_meta, [claimed]}} = Basic.fetch_jobs(conf, meta, %{})
    assert claimed.id == job.id
    %{incident: incident, channel: channel, job: claimed, receiver: receiver}
  end

  defp receiver!(context, mode) do
    {:ok, listener} = :ssl.listen(0, [:binary, active: false, reuseaddr: true, ip: context.ip, cert: context.cert[:cert], key: context.cert[:key], alpn_preferred_protocols: ["http/1.1"]])
    on_exit(fn -> :ssl.close(listener) end)
    {:ok, {_, port}} = :ssl.sockname(listener)
    parent = self()
    if mode == :delayed, do: send(parent, {:delayed_listener, listener})

    receiver =
      Task.async(fn ->
        if mode == :delayed do
          receive_delayed(listener, parent)
        else
          {:ok, transport} = :ssl.transport_accept(listener, 5_000)
          {:ok, socket} = :ssl.handshake(transport, 5_000)

          try do
            {headers, body} = read_request(socket, "")
            signature = header(headers, "x-codex-pooler-signature")
            if signature, do: send(parent, {:signed_request, valid_signature?(headers, signature, body)})
            send_response(socket, mode, parent)
          after
            :ssl.close(socket)
          end

          :closed
        end
      end)

    {"https://#{context.host}:#{port}/hooks", receiver}
  end

  defp receive_delayed(listener, parent) do
    case :ssl.transport_accept(listener, 5_000) do
      {:error, :closed} ->
        :not_connected

      {:ok, transport} ->
        try do
          case :ssl.handshake(transport, 5_000) do
            {:ok, socket} -> receive_delayed_request(socket, parent, "")
            {:error, reason} when reason in [:closed, :econnreset] -> :closed_before_http
          end
        after
          :ssl.close(transport)
        end
    end
  end

  defp receive_delayed_request(socket, parent, buffer) do
    case :binary.split(buffer, "\r\n\r\n") do
      [headers, body] ->
        length = headers |> header("content-length") |> String.to_integer()

        if byte_size(body) >= length do
          send(parent, {:signed_request, valid_signature?(headers, header(headers, "x-codex-pooler-signature"), binary_part(body, 0, length))})
          send_response(socket, :delayed, parent)
        else
          receive_delayed_bytes(socket, parent, buffer)
        end

      _ ->
        receive_delayed_bytes(socket, parent, buffer)
    end
  end

  defp receive_delayed_bytes(socket, parent, buffer) do
    case :ssl.recv(socket, 0, 5_000) do
      {:ok, bytes} -> receive_delayed_request(socket, parent, buffer <> bytes)
      {:error, reason} when reason in [:closed, :econnreset] -> :closed_before_http
    end
  end

  defp read_request(socket, buffer) do
    case :binary.split(buffer, "\r\n\r\n") do
      [headers, body] ->
        length = headers |> header("content-length") |> String.to_integer()
        {headers, read_body(socket, body, length)}

      _ ->
        {:ok, bytes} = :ssl.recv(socket, 0, 5_000)
        read_request(socket, buffer <> bytes)
    end
  end

  defp read_body(_socket, body, length) when byte_size(body) >= length, do: binary_part(body, 0, length)

  defp read_body(socket, body, length) do
    {:ok, bytes} = :ssl.recv(socket, 0, 5_000)
    read_body(socket, body <> bytes, length)
  end

  defp header(headers, key), do: Enum.find_value(String.split(headers, "\r\n"), &header_value(&1, key))

  defp header_value(line, key) do
    case String.split(line, ":", parts: 2) do
      [name, value] -> if String.downcase(name) == key, do: String.trim(value)
      _ -> nil
    end
  end

  defp valid_signature?(headers, signature, body) do
    event_id = header(headers, "x-codex-pooler-event-id")
    attempt_id = header(headers, "x-codex-pooler-attempt-id")
    digest = :crypto.mac(:hmac, :sha256, "synthetic-response-secret", event_id <> "." <> attempt_id <> "." <> body) |> Base.encode16(case: :lower)
    signature == "sha256=" <> digest
  end

  defp send_response(socket, encoding, _parent) when encoding in [:valid_gzip, :invalid_gzip] do
    body = if encoding == :valid_gzip, do: :zlib.gzip(:binary.copy("x", 1_048_576)), else: "invalid-gzip-representation"
    :ssl.send(socket, ["HTTP/1.1 200 OK\r\ncontent-encoding: gzip\r\ncontent-length: #{byte_size(body)}\r\nconnection: close\r\n\r\n", body])
  end

  defp send_response(socket, :delayed, parent) do
    case :ssl.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 1\r\n\r\n") do
      :ok ->
        send(parent, :delayed_headers_sent)
        assert {:error, :closed} = :ssl.recv(socket, 0, 5_000)
        send(parent, :client_socket_closed)
        :closed

      {:error, reason} when reason in [:closed, :econnreset] ->
        :closed_before_headers
    end
  end

  defp send_response(socket, {:held, size, status}, parent) do
    :ok = :ssl.send(socket, "HTTP/1.1 #{status} Result\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n")
    send_bytes(socket, :chunked, size)
    send(parent, {:body_waiting, self()})

    receive do
      :finish -> :ssl.send(socket, "0\r\n\r\n")
    after
      5_000 -> flunk("body completion barrier was not released")
    end
  end

  defp send_response(_socket, :disconnect, _parent), do: :ok
  defp send_response(socket, :truncated, _parent), do: :ssl.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 100\r\nconnection: close\r\n\r\nx")
  defp send_response(socket, :malformed, _parent), do: :ssl.send(socket, "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\ninvalid\r\nx\r\n")

  defp send_response(socket, {framing, size, status}, _parent) do
    header = if framing == :length, do: "content-length: #{size}", else: "transfer-encoding: chunked"
    :ok = :ssl.send(socket, "HTTP/1.1 #{status} Result\r\n#{header}\r\nx-synthetic-receiver: yes\r\nconnection: close\r\n\r\n")
    send_bytes(socket, framing, size)
    if framing == :chunked, do: :ssl.send(socket, "0\r\n\r\n"), else: :ok
  end

  defp send_bytes(_socket, _framing, 0), do: :ok

  defp send_bytes(socket, framing, remaining) do
    size = min(remaining, 16_384)
    chunk = :binary.copy("x", size)
    wire = if framing == :chunked, do: [Integer.to_string(size, 16), "\r\n", chunk, "\r\n"], else: chunk
    :ok = :ssl.send(socket, wire)
    send_bytes(socket, framing, remaining - size)
  end

  defp trace_response! do
    Enum.each([OutboundHTTP, WebhookDelivery], &Code.ensure_loaded!/1)
    :erlang.trace_pattern({WebhookDelivery, :discard_response_body, 2}, [{[{:data, :"$1"}, :_], [], [{:message, {:byte_size, :"$1"}}]}], [:local])
    :erlang.trace_pattern({OutboundHTTP, :post, 2}, [{:_, [], [{:return_trace}]}], [:local])
    parent = self()
    tracer = spawn_link(fn -> forward_response(parent) end)

    on_exit(fn ->
      :erlang.trace_pattern({OutboundHTTP, :post, 2}, false, [:local])
      :erlang.trace_pattern({WebhookDelivery, :discard_response_body, 2}, false, [:local])
      if Process.alive?(tracer), do: Process.exit(tracer, :kill)
    end)

    :erlang.trace(self(), true, [:call, :set_on_spawn, :arity, {:tracer, tracer}])
  end

  defp forward_response(parent) do
    receive do
      {:trace, caller, :call, {WebhookDelivery, :discard_response_body, 2}, bytes} -> send(parent, {:chunk_discarded, caller, bytes})
      {:trace, _, :return_from, {OutboundHTTP, :post, 2}, {:ok, response}} -> send(parent, {:response_retained, response.status, byte_size(response.body)})
      _ -> :ok
    end

    forward_response(parent)
  end
end
