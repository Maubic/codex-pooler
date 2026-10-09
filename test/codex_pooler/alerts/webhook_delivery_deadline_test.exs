defmodule CodexPooler.Alerts.WebhookDeliveryDeadlineTest do
  use CodexPooler.DataCase, async: false
  import Ecto.Query
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Alerts
  alias CodexPooler.Alerts.Delivery.{Execution, WebhookDelivery}
  alias CodexPooler.Alerts.Schemas.AlertDeliveryAttempt
  alias CodexPooler.Jobs.AlertDeliveryWorker
  alias CodexPooler.Platform.OutboundHTTP
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv
  alias Oban.Engines.Basic

  defmodule Receiver do
    def init(opts), do: opts

    def call(conn, {parent, mode}) do
      {:ok, _body, conn} = Plug.Conn.read_body(conn)
      send(parent, {:https_request, self()})

      case mode do
        :success ->
          Plug.Conn.send_resp(conn, 204, "")

        :drip ->
          drip(Plug.Conn.send_chunked(conn, 200), parent)

        :hold ->
          receive do
            :finish -> Plug.Conn.send_resp(conn, 200, "done")
          after
            5_000 -> raise "HTTP pool barrier not released"
          end
      end
    end

    defp drip(conn, parent) do
      ref = make_ref()
      Process.send_after(self(), ref, 50)

      receive do
        ^ref ->
          case Plug.Conn.chunk(conn, "x") do
            {:ok, conn} ->
              drip(conn, parent)

            {:error, _} ->
              send(parent, :https_connection_released)
              conn
          end
      end
    end
  end

  setup do
    TestAppEnv.restore_on_exit(OutboundHTTP)
    Application.put_env(:codex_pooler, OutboundHTTP, proxy_config: %{http: [], https: [], no_proxy: []})
    %{user: user} = bootstrap_owner_fixture()
    cert = :public_key.pkix_test_data(%{root: [digest: :sha256, key: {:rsa, 2048, 65_537}], peer: [digest: :sha256, key: {:rsa, 2048, 65_537}, extensions: [{:Extension, {2, 5, 29, 17}, false, [{:iPAddress, <<127, 0, 0, 1>>}]}]]})
    previous = :persistent_term.get(:pubkey_os_cacerts, :absent)
    dir = Path.join(System.tmp_dir!(), "delivery-deadline-#{Base.encode16(:crypto.strong_rand_bytes(8))}")

    on_exit(fn ->
      if previous == :absent, do: :public_key.cacerts_clear(), else: :persistent_term.put(:pubkey_os_cacerts, previous)
      restored? = :persistent_term.get(:pubkey_os_cacerts, :absent) == previous
      assert restored?
      File.rm_rf!(dir)
      refute File.exists?(dir)
    end)

    File.mkdir!(dir)
    File.chmod!(dir, 0o700)
    File.write!(Path.join(dir, "ca.pem"), :public_key.pem_encode(Enum.map(cert[:cacerts], &{:Certificate, &1, :not_encrypted})))
    :ok = :public_key.cacerts_load(String.to_charlist(Path.join(dir, "ca.pem")))
    %{cert: cert, scope: Scope.for_user(user, ["instance_owner"])}
  end

  test "actual worker creates one absolute default budget before preparation and carries it through HTTPS and receipt", context do
    fixture = fixture!(context, :success)
    patterns = [{Execution, :new, 1}, {Execution, :run, 2}, {Execution, :storage, 2}, {Alerts, :next_delivery_attempt_number, 3}]
    on_exit(fn -> Enum.each(patterns, &:erlang.trace_pattern(&1, false, [:local])) end)
    Enum.each([Execution, Alerts], &Code.ensure_loaded!/1)
    :erlang.trace_pattern({Execution, :new, 1}, [{:_, [], [{:return_trace}]}], [:local])
    for function <- [:run, :storage], do: :erlang.trace_pattern({Execution, function, 2}, [{[:"$1", :_], [], [{:message, {:map_get, :deadline, :"$1"}}]}], [:local])
    :erlang.trace_pattern({Alerts, :next_delivery_attempt_number, 3}, [{:_, [], [{:message, :number_selection}]}], [:local])
    # A dedicated tracer forwards only typed clock data/arity; no headers or body.
    parent = self()
    tracer = spawn_link(fn -> forward_trace(parent) end)
    on_exit(fn -> if Process.alive?(tracer), do: Process.exit(tracer, :kill) end)
    :erlang.trace(self(), true, [:call, :arity, {:tracer, tracer}])
    started = System.monotonic_time(:millisecond)
    assert :ok = Oban.Testing.perform_job(fixture.job, [])
    :erlang.trace(self(), false, [:call])
    assert_receive {:observed, {:trace, _, :return_from, {Execution, :new, 1}, %Execution{} = execution}}, 5_000
    assert execution.deadline >= started + 10_000
    assert execution.deadline < System.monotonic_time(:millisecond) + 10_000
    assert execution.completion_deadline - execution.deadline == 2_000
    assert_receive {:observed, {:trace, _, :call, {Alerts, :next_delivery_attempt_number, 3}, :number_selection}}, 5_000
    deadline = execution.deadline
    assert_receive {:observed, {:trace, _, :call, {Execution, :storage, 2}, ^deadline}}, 5_000
    assert_receive {:observed, {:trace, _, :call, {Execution, :run, 2}, ^deadline}}, 5_000
    assert_receive {:observed, {:trace, _, :call, {Execution, :storage, 2}, ^deadline}}, 5_000
    assert_receive {:https_request, _}, 5_000
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
    assert attempt.status == "sent"
    assert attempt.response_metadata["delivery_execution"]["job_id"] == fixture.job.id
  end

  test "an already near absolute deadline cancels actual private HTTPS drip and releases its connection", context do
    fixture = fixture!(context, :drip)
    execution = %{Execution.new(fixture.job) | deadline: System.monotonic_time(:millisecond) + 300}
    assert {:error, %{code: "alert_webhook_delivery_timeout", retryable: true}} = WebhookDelivery.deliver_incident_to_channel(fixture.incident.id, fixture.channel.id, 1, delivery_execution: execution)
    assert_receive {:https_request, _}, 5_000
    assert_receive :https_connection_released, 5_000
    [attempt] = Repo.all(from a in AlertDeliveryAttempt, where: a.incident_id == ^fixture.incident.id)
    assert attempt.status == "retryable"
    assert DateTime.compare(attempt.next_retry_at, attempt.completed_at) == :gt
    assert attempt.response_metadata["delivery_outcome"] == "unknown"
  end

  test "TLS negotiation that never answers is cancelled by the same absolute deadline", context do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    parent = self()
    tasks = start_supervised!(Task.Supervisor)

    peer =
      Task.Supervisor.async_nolink(tasks, fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)

        try do
          {:ok, _client_hello} = :gen_tcp.recv(socket, 0, 5_000)
          send(parent, :tls_started)
          await_tls_close(socket)
        after
          :gen_tcp.close(socket)
        end
      end)

    monitor = Process.monitor(peer.pid)
    pool = pool_fixture()
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "TLS deadline", endpoint_url: "https://127.0.0.1:#{port}/hooks", webhook_signing_secret: "synthetic-deadline-secret"})
    incident = alert_incident_fixture(pool: pool)
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 300}
    assert {:error, %{retryable: true}} = WebhookDelivery.deliver_incident_to_channel(incident.id, channel.id, 1, delivery_execution: execution)
    assert_receive :tls_started, 5_000
    assert Task.await(peer, 5_000) == {:error, :closed}
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 5_000
  end

  test "absolute executor budget cancels a real Finch checkout without consuming the held connection", context do
    fixture = fixture!(context, :hold)
    finch_name = Module.concat(__MODULE__, "Finch#{System.unique_integer([:positive])}")
    start_supervised!({Finch, name: finch_name, pools: %{default: [size: 1, count: 1, conn_opts: [transport_opts: [cacerts: context.cert[:cacerts]]]]}})
    tasks = start_supervised!(Task.Supervisor)
    first = Task.Supervisor.async_nolink(tasks, fn -> Req.get(fixture.url, finch: finch_name, retry: false) end)
    first_monitor = Process.monitor(first.pid)
    assert_receive {:https_request, held}, 5_000
    execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 200}
    assert {:error, :delivery_timeout} = Execution.run(execution, fn -> Req.get(fixture.url, finch: finch_name, retry: false) end)
    send(held, :finish)
    assert {:ok, %{status: 200}} = Task.await(first, 5_000)
    assert_receive {:DOWN, ^first_monitor, :process, _, :normal}, 5_000
    # Completed first writer plus a fresh successful checkout fences the
    # cancellation; a queued cancelled caller must not consume the next slot.
    third = Task.Supervisor.async_nolink(tasks, fn -> Req.get(fixture.url, finch: finch_name, retry: false) end)
    third_monitor = Process.monitor(third.pid)
    assert_receive {:https_request, next}, 5_000
    send(next, :finish)
    assert {:ok, %{status: 200}} = Task.await(third, 5_000)
    assert_receive {:DOWN, ^third_monitor, :process, _, :normal}, 5_000
  end

  test "absolute executor budget closes an owned TCP socket blocked sending synthetic bytes" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}, recbuf: 1024])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, port} = :inet.port(listener)
    tasks = start_supervised!(Task.Supervisor)
    parent = self()

    sender =
      Task.Supervisor.async_nolink(tasks, fn ->
        execution = %{Execution.new() | deadline: System.monotonic_time(:millisecond) + 200}

        Execution.run(execution, fn ->
          {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false, sndbuf: 1024], 5_000)
          send(parent, :send_ready)

          try do
            :gen_tcp.send(socket, :binary.copy(<<0>>, 16_777_216))
          after
            :gen_tcp.close(socket)
          end
        end)
      end)

    monitor = Process.monitor(sender.pid)
    {:ok, peer} = :gen_tcp.accept(listener, 5_000)
    on_exit(fn -> :gen_tcp.close(peer) end)
    assert_receive :send_ready, 5_000
    assert Task.await(sender, 5_000) == {:error, :delivery_timeout}
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 5_000
    bytes = drain_closed_socket(peer, 0)
    # Timeout is not proof that zero or partial bytes reached the peer. The
    # actual EOF and sender DOWN are the cancellation/ownership oracles.
    CodexPooler.TestDiagnostics.puts("alert-send-cancellation received_bytes=#{bytes} outcome=unknown socket_closed=true")
    assert bytes <= 16_777_216
  end

  defp drain_closed_socket(socket, count) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} -> drain_closed_socket(socket, count + byte_size(data))
      {:error, :closed} -> count
    end
  end

  defp await_tls_close(socket) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, _tls_alert} -> await_tls_close(socket)
      {:error, reason} -> {:error, reason}
    end
  end

  defp forward_trace(parent) do
    receive do
      message ->
        send(parent, {:observed, message})
        forward_trace(parent)
    end
  end

  defp fixture!(context, mode) do
    server = start_supervised!({Bandit, plug: {Receiver, {self(), mode}}, scheme: :https, port: 0, ip: {127, 0, 0, 1}, startup_log: false, thousand_island_options: [transport_options: [cert: context.cert[:cert], key: context.cert[:key]]]})
    {:ok, {_, port}} = ThousandIsland.listener_info(server)
    pool = pool_fixture()
    {:ok, channel} = Alerts.create_channel(context.scope, %{channel_type: "webhook", display_name: "deadline", endpoint_url: "https://127.0.0.1:#{port}/hooks", webhook_signing_secret: "synthetic-deadline-secret"})
    incident = alert_incident_fixture(pool: pool)
    queue = "deadline_#{System.unique_integer([:positive])}"
    job = %{"alert_incident_id" => incident.id, "alert_channel_id" => channel.id} |> AlertDeliveryWorker.new(queue: queue) |> Repo.insert!()
    conf = Oban.config()
    {:ok, meta} = Basic.init(conf, queue: queue, limit: 1)
    {:ok, {_meta, [claimed]}} = Basic.fetch_jobs(conf, meta, %{})
    assert claimed.id == job.id
    %{incident: incident, channel: channel, job: claimed, url: "https://127.0.0.1:#{port}/hooks"}
  end
end
