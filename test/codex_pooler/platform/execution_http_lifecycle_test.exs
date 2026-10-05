defmodule CodexPooler.Platform.ExecutionHTTPLifecycleTest do
  use CodexPooler.DataCase, async: false
  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.InstancePresence.Identity

  @detection_timeout_ms 15_000

  # Send the real HTTP bytes, then hold the adapter return before the gateway
  # helper can retire the execution. Receiving a response is not that barrier.
  defmodule AfterSendGate do
    def send_resp({adapter, payload, parent, execution_id}, status, headers, body) do
      result = adapter.send_resp(payload, status, headers, body)
      send(parent, {:response_sent, self(), execution_id})

      receive do
        {:release_response, ^execution_id} -> result
      after
        15_000 -> raise "response return gate was not released"
      end
    end
  end

  test "owned publisher drains a queued proof backlog without spending each periodic interval" do
    alias CodexPooler.Platform.ExecutionRegistry
    registry = start_supervised!({ExecutionRegistry, name: nil})

    ids =
      for _ <- 1..1_001 do
        id = Ecto.UUID.generate()
        assert :ok = ExecutionRegistry.register(id, registry)
        assert :ok = ExecutionRegistry.complete(id, registry)
        id
      end

    last_id = List.last(ids)

    proof =
      Enum.find(ExecutionRegistry.pending(2_000, registry), &(&1.owner_execution_id == last_id))

    publisher = CodexPooler.ExecutionProofSupport.start_publisher!(name: nil, registry: registry)

    assert :ok = CodexPooler.ExecutionProofSupport.await_terminal!(proof, publisher)
    assert [] = ExecutionRegistry.pending(2_000, registry)
    assert Repo.aggregate("execution_terminal_proofs", :count) == 1_001
  end

  # Real gateway boundary (findings#207): the executor identity a request
  # records is the Bandit connection process that ran it, its terminal proof is
  # published by the production publisher once the request completes, and a
  # second request on the same keep-alive connection runs in the same process
  # under a distinct execution UUID while the connection stays open. The
  # synthetic-plug cases below keep exercising the transport corners; this one
  # proves the identity the gateway actually publishes.
  test "a real gateway request publishes its executor and a keep-alive successor gets a new execution" do
    upstream =
      start_upstream(
        FakeUpstream.json_response(%{
          "id" => "resp_execution_keep_alive",
          "object" => "response",
          "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
        })
      )

    setup = gateway_setup(upstream, compact?: true)
    port = start_public_endpoint!()

    # One fixed name: the file is synchronous, so no two tests hold it at once
    # and long sessions do not mint an atom per run. The publisher drains the
    # global ExecutionRegistry, so a proof another sandboxed test left pending
    # could be acknowledged here while its write rolls back; both files that
    # start the real publisher are `async: false` for that reason.
    publisher = CodexPooler.ExecutionProofSupport.start_publisher!(name: :execution_http_lifecycle_publisher)

    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    on_exit(fn -> Mint.HTTP.close(conn) end)

    {conn, 200, first_body} = gateway_request!(conn, setup, "execution-keep-alive-first")
    assert %{"id" => "resp_execution_keep_alive"} = CodexPooler.JSON.decode!(first_body)
    assert Mint.HTTP.open?(conn)

    [first_attempt] = pool_attempts(setup)
    local = Identity.local()
    assert first_attempt.owner_instance_id == local.node_name
    assert first_attempt.owner_instance_boot_id == local.boot_id
    assert is_binary(first_attempt.owner_execution_id)
    executor = first_attempt.owner_process_id |> String.to_charlist() |> :erlang.list_to_pid()
    assert Process.alive?(executor)

    # The completed execution is retired while its connection process lives on,
    # and the publisher turns that registry tombstone into the durable proof.
    # The registry retirement happens after the response bytes reach the
    # client, so wait for the durable proof before reading the registry state.
    :ok = CodexPooler.ExecutionProofSupport.await_terminal!(first_attempt, publisher)
    assert ExecutionIdentity.status(first_attempt) == :dead

    {conn, 200, second_body} = gateway_request!(conn, setup, "execution-keep-alive-second")
    assert %{"id" => "resp_execution_keep_alive"} = CodexPooler.JSON.decode!(second_body)
    assert Mint.HTTP.open?(conn)

    assert [%Attempt{id: first_id}, second_attempt] = pool_attempts(setup)
    assert first_id == first_attempt.id
    assert second_attempt.owner_process_id == first_attempt.owner_process_id
    assert is_binary(second_attempt.owner_execution_id)
    refute second_attempt.owner_execution_id == first_attempt.owner_execution_id
    assert Process.alive?(executor)
    :ok = CodexPooler.ExecutionProofSupport.await_terminal!(second_attempt, publisher)
    assert ExecutionIdentity.status(second_attempt) == :dead

    assert FakeUpstream.count(upstream) == 2

    assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
  end

  defp pool_attempts(setup) do
    Repo.all(
      from attempt in Attempt,
        join: request in Request,
        on: request.id == attempt.request_id,
        where: request.pool_id == ^setup.pool.id,
        order_by: [asc: attempt.started_at, asc: attempt.id]
    )
  end

  defp gateway_request!(conn, setup, marker) do
    body =
      CodexPooler.JSON.encode!(%{
        "model" => setup.model.exposed_model_id,
        "input" => native_text_input(marker),
        "stream" => false
      })

    headers = [
      {"authorization", setup.authorization},
      {"content-type", "application/json"},
      {"x-request-id", marker}
    ]

    {:ok, conn, ref} =
      Mint.HTTP.request(conn, "POST", "/backend-api/codex/responses/compact", headers, body)

    await_gateway_response!(
      conn,
      ref,
      nil,
      [],
      System.monotonic_time(:millisecond) + @detection_timeout_ms
    )
  end

  defp await_gateway_response!(conn, ref, status, body, deadline) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          :unknown ->
            await_gateway_response!(conn, ref, status, body, deadline)

          {:ok, conn, responses} ->
            {status, body, done?} =
              Enum.reduce(responses, {status, body, false}, fn
                {:status, ^ref, response_status}, {_status, body, done?} ->
                  {response_status, body, done?}

                {:data, ^ref, data}, {status, body, done?} ->
                  {status, [body, data], done?}

                {:done, ^ref}, {status, body, _done?} ->
                  {status, body, true}

                _response, acc ->
                  acc
              end)

            if done?,
              do: {conn, status, IO.iodata_to_binary(body)},
              else: await_gateway_response!(conn, ref, status, body, deadline)

          {:error, _conn, reason, _responses} ->
            flunk("gateway request failed: #{inspect(reason)}")
        end
    after
      timeout -> flunk("timed out waiting for the gateway response")
    end
  end

  defmodule LifecyclePlug do
    def init(opts), do: opts

    def call(conn, parent) do
      owner = Identity.local()

      identity =
        Map.merge(ExecutionIdentity.local(), %{
          owner_instance_id: owner.node_name,
          owner_instance_boot_id: owner.boot_id
        })

      send(parent, {:execution, self(), identity})

      response =
        case conn.request_path do
          "/raise" ->
            raise DBConnection.ConnectionError, message: "synthetic database outage"

          "/stream" ->
            CodexPoolerWeb.GatewayControllerHelpers.send_gateway_result(conn, %{
              status: 200,
              stream: fn _ ->
                send(parent, {:stream_liveness, ExecutionIdentity.status(identity)})
                {:error, :closed}
              end
            })

          "/public" ->
            {adapter, payload} = conn.adapter
            conn = %{conn | adapter: {AfterSendGate, {adapter, payload, parent, identity.owner_execution_id}}}

            response =
              CodexPoolerWeb.PublicGatewayResult.send(
                conn,
                {:ok, %{status: 200, raw_body: "{}"}},
                &Function.identity/1
              )

            {AfterSendGate, payload} = response.adapter
            %{response | adapter: {adapter, payload}}
        end

      send(parent, {:execution_finished, self(), identity.owner_execution_id})
      response
    end
  end

  test "returned stream error retires execution while real HTTP keep-alive reuses its PID" do
    {socket, _server} = start_connection()

    logs =
      capture_log(fn ->
        :ok = :gen_tcp.send(socket, "GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert_receive {:execution, pid, first}, 15_000
        response = receive_response(socket)
        assert response =~ "200 OK"
        assert_receive {:stream_liveness, :alive}
        first_id = first.owner_execution_id
        assert_receive {:execution_finished, ^pid, ^first_id}, @detection_timeout_ms
        assert Process.alive?(pid)
        assert ExecutionIdentity.status(first) == :dead
        CodexPooler.ExecutionProofSupport.publish_terminal!(first)
        :ok = :gen_tcp.send(socket, "GET /public HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert_receive {:execution, ^pid, second}, 15_000
        second_id = second.owner_execution_id

        try do
          assert receive_response(socket) =~ "200 OK"
          assert_receive {:response_sent, ^pid, ^second_id}, @detection_timeout_ms
          assert second_id != first_id
          assert ExecutionIdentity.status(second) == :alive
        after
          send(pid, {:release_response, second_id})
        end

        assert_receive {:execution_finished, ^pid, ^second_id}, @detection_timeout_ms
        assert Process.alive?(pid)
        assert ExecutionIdentity.status(second) == :dead
        CodexPooler.ExecutionProofSupport.publish_terminal!(second)
      end)

    assert logs =~ "late gateway stream failed"
  end

  test "raised DBConnection error terminates actual Bandit connection and execution" do
    {socket, _server} = start_connection()

    logs =
      capture_log(fn ->
        :ok = :gen_tcp.send(socket, "GET /raise HTTP/1.1\r\nHost: localhost\r\n\r\n")
        assert_receive {:execution, pid, identity}, 15_000
        monitor = Process.monitor(pid)
        assert_receive {:DOWN, ^monitor, :process, ^pid, _}, 15_000
        assert ExecutionIdentity.status(identity) == :dead
        CodexPooler.ExecutionProofSupport.publish_terminal!(identity)
      end)

    assert logs =~ "synthetic database outage"
  end

  defp start_connection do
    server =
      start_supervised!({Bandit, plug: {LifecyclePlug, self()}, port: 0, ip: {127, 0, 0, 1}, startup_log: false})

    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)
    {:ok, socket} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(socket) end)
    {socket, server}
  end

  defp receive_response(socket, buffer \\ "") do
    {:ok, data} = :gen_tcp.recv(socket, 0, 15_000)
    buffer = buffer <> data

    if String.ends_with?(buffer, "0\r\n\r\n") or String.ends_with?(buffer, "\r\n\r\n{}"),
      do: buffer,
      else: receive_response(socket, buffer)
  end
end
