defmodule CodexPoolerWeb.Runtime.VisibleOutputMarkTransientDatabaseTest do
  # The relay stamps a Codex turn visible (`first_visible_output_at`) before
  # it writes the turn's first visible output: the stamp is what the resend
  # and replay fences read, and the same transaction refuses output from a
  # superseded replay generation. A transient database failure on that stamp
  # used to end the connection process mid-stream (findings#294).
  #
  # The failure is injected where it happens: a deferred constraint trigger on
  # this test's own turns makes the stamp's COMMIT wait on an advisory lock the
  # test holds, and the test cancels the waiting backend. A sequence the
  # trigger advances before it waits counts the stamp's COMMITs across their
  # rollbacks.
  #
  # Topology: one node, committed rows, the real listener, Mint as the client,
  # native `POST /backend-api/codex/responses` over HTTP SSE with the released
  # client's session headers and turn metadata (so the request has a turn),
  # FakeUpstream, the Pool's default serving mode (Full), owner forwarding
  # irrelevant (HTTP).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, register_unboxed_pool_cleanup!: 1, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport
  alias CodexPooler.Accounting.{LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias Ecto.Adapters.SQL.Sandbox

  @native_path "/backend-api/codex/responses"
  @detection_timeout_ms 15_000
  @quiet_read_ms 300
  @response_id "resp_visible_output_mark_transient"

  setup do
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    CodexPooler.TestAppEnv.restore_on_exit(SettlementRetry)
    Application.put_env(:codex_pooler, SettlementRetry, initial_backoff_ms: 10, max_backoff_ms: 50)
    :ok
  end

  test "native http sse: a visible-output stamp whose COMMIT is cancelled is retried before the output is written, and the stream completes" do
    {setup, observer, gate, holder} = fixture!()
    client = post!(setup)

    {client, logs} =
      with_info_log(fn ->
        waiter = await_gate_waiter!(observer, gate, holder, 1)
        client = read_quietly(client)
        refute client.body =~ "response.output_text.delta"
        assert cancel_backend!(observer, waiter)

        # The retried stamp waits at its own COMMIT: the first one rolled back,
        # the client still has no output, and the connection is still open.
        _retried = await_gate_waiter!(observer, gate, holder, 2)
        client = read_quietly(client)
        assert client.outcome == nil
        refute client.body =~ "response.output_text.delta"
        assert %CodexTurn{first_visible_output_at: nil} = turn!(setup)

        release_gate!(holder)
        read_to_end(client)
      end)

    assert {client.status, client.outcome} == {200, :done}
    assert client.body =~ "synthetic stamped text"
    assert "response.completed" in event_types(client.body)

    request = settled_request!(setup)
    assert {request.status, request.last_error_code, request.usage_status} == {"succeeded", nil, "usage_known"}
    assert recorded_settlements(request) == [{"usage_known", 5}]
    assert %CodexTurn{status: "succeeded", first_visible_output_at: %DateTime{}} = turn!(setup)
    assert gate_passes(observer, gate) == 2

    assert logs =~ "gateway visible output mark met a transient database failure; retrying stage=visible_output request_id=#{request.id}"
    assert logs =~ "reason_class=postgres_query_canceled"
    assert logs =~ "gateway visible output mark completed after a transient database failure stage=visible_output request_id=#{request.id}"
    assert logs =~ "settlement_tries=2"
  end

  test "native http sse: when the retry window closes output stays withheld and the connection ends through failure finalization" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)
    {setup, observer, gate, holder} = fixture!()
    client = post!(setup)

    {client, logs} =
      with_log([level: :warning], fn ->
        waiter = await_gate_waiter!(observer, gate, holder, 1)
        assert cancel_backend!(observer, waiter)
        client = read_to_end(client)
        release_gate!(holder)
        client
      end)

    assert {client.status, client.outcome} == {200, :done}
    refute client.body =~ "synthetic stamped text"
    refute "response.completed" in event_types(client.body)
    assert "error" in event_types(client.body)
    assert client.body =~ "gateway_accounting_failed"

    request = settled_request!(setup)
    assert request.status == "failed"
    assert %CodexTurn{first_visible_output_at: nil} = turn!(setup)
    assert gate_passes(observer, gate) == 1

    assert [line] = Regex.scan(~r/gateway visible output mark abandoned after transient database failures[^\n]*/, logs) |> List.flatten()
    assert line =~ "stage=visible_output request_id=#{request.id}"
    assert line =~ "reason_class=postgres_query_canceled fallback=withheld_output"
  end

  test "collected response: visibility exhaustion returns the accounting error instead of parsing withheld output" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)
    %{user: owner} = CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()
    fixture = CodexPooler.RequestReplayFixtures.replay_fixture(reservation?: true, owner: owner)
    gate = install_stamp_gate!(fixture.pool.id)
    observer = observer!()
    holder = hold_gate!(gate)
    opts = CodexPooler.Gateway.Payloads.RequestOptions.for_websocket(%{})
    context = %CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext{auth: fixture.auth, endpoint: fixture.request.endpoint, payload: %{}, model: fixture.model, reserved: %{request: fixture.request}, request_options: opts, assignment: fixture.assignment, identity: fixture.identity, index: 0, retry_count: 0, allow_retry?: false, routing_attempt_metadata: %{}, route_class: opts.transport.route_class, attempt: fixture.attempt, started: System.monotonic_time(:millisecond)}
    upstream = start_upstream(FakeUpstream.sse_stream([created_event(), delta_event(), completed_event()], done: false))
    task = Task.async(fn ->
      response = Req.get!(FakeUpstream.url(upstream), into: :self, retry: false)
      CodexPooler.Gateway.Runtime.Streaming.OpenAIStreamCollector.collect_response(response, context, %{register_continuity: fn _, _, _ -> :ok end, stream_result: fn _, _ -> :ok end})
    end)
    waiter = await_gate_waiter!(observer, gate, holder, 1)
    assert cancel_backend!(observer, waiter)
    assert {:error, %{status: 500, code: "gateway_accounting_failed"}} = Task.await(task, 15_000)
    release_gate!(holder)
    assert %CodexTurn{first_visible_output_at: nil} = Repo.get_by!(CodexTurn, request_id: fixture.request.id)
  end
  test "public Responses JSON returns an explicit accounting error on visibility exhaustion" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)
    {setup, observer, gate, holder} = fixture!()
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    body = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic public collector"), "stream" => false}
    client = Task.async(fn -> Req.post!("http://127.0.0.1:#{port}/v1/responses", json: body, headers: [{"authorization", setup.authorization}, {"session-id", thread}], retry: false, receive_timeout: 15_000) end)
    waiter = await_gate_waiter!(observer, gate, holder, 1)
    assert cancel_backend!(observer, waiter)
    response = Task.await(client, 15_000)
    release_gate!(holder)
    assert response.status == 500
    assert response.body["error"]["code"] == "gateway_accounting_failed"
  end


  @tag slow: "boots a real peer and cancels owner lifecycle authorization"
  test "remote lifecycle authorization failure ends once without retrying delivery or crashing its owner" do
    ensure_test_distribution_started!()
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    release = make_ref()
    frames = [created_event(), delta_event(), completed_event()] |> Enum.map(fn {_kind, event} -> CodexPooler.JSON.encode!(event) end)
    upstream = start_upstream(FakeUpstream.websocket_init_barrier(FakeUpstream.websocket_text_frames(frames), notify: self(), release_ref: release))
    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    {:ok, auth} = CodexPooler.Access.authenticate_authorization_header(setup.authorization)
    peer = start_bridge_peer!(:current, setup.identity, repo: :real)
    thread = Ecto.UUID.generate()
    {session, owner} = start_remote_bridge_owner!(auth, thread, peer, :real)
    {:ok, socket} = owner_socket(auth, "synthetic-lifecycle-fault", Ecto.UUID.generate(), session_header: thread, session_header_source: "x-session-id", websocket_owner_forwarder_opts: [node_client: CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder.ERPCNodeClient, app_node_names: [Atom.to_string(peer)]])
    payload = websocket_input_payload(setup, native_text_input("synthetic lifecycle fault"))
    assert {:ok, socket} = CodexPoolerWeb.CodexResponsesSocket.handle_in({payload, [opcode: :text]}, socket)
    assert_receive {:fake_upstream_websocket_barrier, :before_init, server, ^release}, 15_000
    holder = start_supervised!({Postgrex, connection_options()}, id: :lifecycle_row_holder)
    observer = observer!()
    Postgrex.query!(holder, "BEGIN", [])
    %{rows: [[holder_pid]]} = Postgrex.query!(holder, "SELECT pg_backend_pid()", [])
    Postgrex.query!(holder, "SELECT id FROM codex_sessions WHERE id=$1 FOR UPDATE", [Ecto.UUID.dump!(session.id)])
    send(server, {:fake_upstream_release_websocket, release})
    backend = await_owner_query_wait!(observer, holder_pid, System.monotonic_time(:millisecond) + 15_000)
    assert cancel_backend!(observer, backend)
    Postgrex.query!(holder, "COMMIT", [])
    assert {:push, {:text, frame}, socket} = receive_owner_socket_raw_push(socket)
    assert %{"type" => "error"} = CodexPooler.JSON.decode!(frame)
    assert {:ok, _socket} = receive_owner_socket_complete(socket)
    assert :erpc.call(peer, Process, :alive?, [owner])
    assert FakeUpstream.count(upstream) == 1
    request = settled_request!(setup)
    assert request.status == "failed"
    assert %CodexTurn{first_visible_output_at: nil} = Repo.get_by!(CodexTurn, request_id: request.id)
  end

  defp await_owner_query_wait!(observer, holder, deadline) do
    case Postgrex.query!(observer, "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [holder]).rows do
      [[backend] | _] -> backend
      [] ->
        assert System.monotonic_time(:millisecond) < deadline
        Process.sleep(5)
        await_owner_query_wait!(observer, holder, deadline)
    end
  end

  @tag slow: "boots a real peer owner and cancels its first-visible COMMIT"
  test "remote native websocket: a cancelled visible mark retries outside the responsive owner and settles once" do
    ensure_test_distribution_started!()
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    frames = [created_event(), delta_event(), completed_event()] |> Enum.map(fn {_type, event} -> CodexPooler.JSON.encode!(event) end)
    setup = gateway_setup(start_upstream(FakeUpstream.websocket_text_frames(frames)))
    register_unboxed_pool_cleanup!(setup)
    {:ok, auth} = CodexPooler.Access.authenticate_authorization_header(setup.authorization)
    peer = start_bridge_peer!(:current, setup.identity, repo: :real)
    thread = Ecto.UUID.generate()
    {session, owner} = start_remote_bridge_owner!(auth, thread, peer, :real)
    {:ok, socket} = owner_socket(auth, "synthetic-visible-peer", Ecto.UUID.generate(), session_header: thread, session_header_source: "x-session-id", websocket_owner_forwarder_opts: [node_client: CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder.ERPCNodeClient, app_node_names: [Atom.to_string(peer)]])
    gate = install_stamp_gate!(setup.pool.id)
    observer = observer!()
    holder = hold_gate!(gate)
    payload = websocket_input_payload(setup, native_text_input("synthetic peer visible mark"), %{"client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn"})}})
    assert {:ok, socket} = CodexPoolerWeb.CodexResponsesSocket.handle_in({payload, [opcode: :text]}, socket)
    waiter = await_gate_waiter!(observer, gate, holder, 1)
    assert %{active_turn: %{visible_output?: false} = active} = :erpc.call(peer, :sys, :get_state, [owner, 1_000])
    authority = Map.take(active.descriptor, [:request_id, :attempt_id, :replay_generation])
    discriminator = %CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.TerminalDiscriminator{}
    forged = CodexPooler.JSON.encode!(%{"type" => "response.output_text.delta", "delta" => "synthetic forged output"})
    send(owner, {:websocket_owner_authorized_frame, active.ref, make_ref(), authority, forged, discriminator, :committed})
    # Barrier from this sender ensures the malformed capability was handled.
    assert :ok = GenServer.call(owner, {:writer_lifecycle_barrier, active.ref})
    assert %{active_turn: %{visible_output?: false}} = :erpc.call(peer, :sys, :get_state, [owner, 1_000])
    refute_receive {:websocket_owner_frame, _, _, _, {:data, ^forged}}, 0
    assert cancel_backend!(observer, waiter)
    _retried = await_gate_waiter!(observer, gate, holder, 2)
    assert %{active_turn: %{visible_output?: false}} = :erpc.call(peer, :sys, :get_state, [owner, 1_000])
    release_gate!(holder)
    assert {:ok, socket} = receive_owner_socket_complete(socket)
    request = settled_request!(setup)
    assert {request.status, request.usage_status} == {"succeeded", "usage_known"}
    assert recorded_settlements(request) == [{"usage_known", 5}]
    assert %CodexTurn{first_visible_output_at: %DateTime{}} = Repo.get_by!(CodexTurn, request_id: request.id)
    assert session.id == socket.codex_session.id
  end

  # --- fixture -------------------------------------------------------------

  defp fixture! do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (one ordinary native Responses stream with usage; the Pooler's visible-output stamp is what fails)
        FakeUpstream.sse_stream([created_event(), delta_event(), completed_event()], done: false, headers: [{"content-type", "text/event-stream"}])
      )

    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    observer = observer!()
    gate = install_stamp_gate!(setup.pool.id)
    holder = hold_gate!(gate)
    {setup, observer, gate, holder}
  end

  # A deferred constraint trigger runs at COMMIT. It fires only on the update
  # that stamps one of this Pool's turns visible, counts the COMMIT on a
  # sequence (sequences are not transactional, so the count survives the
  # rollback), then waits on the test's advisory lock.
  defp install_stamp_gate!(pool_id) do
    suffix = System.unique_integer([:positive])
    name = "visible_stamp_gate_#{suffix}"
    key = 294_000_000 + suffix

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{name} ON codex_turns")
      Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
      Repo.query!("DROP SEQUENCE IF EXISTS #{name}_passes")
    end)

    UnboxedFixture.run_unboxed(fn ->
      Repo.query!("CREATE SEQUENCE #{name}_passes")

      Repo.query!(
        "CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " <>
          "IF EXISTS (SELECT 1 FROM requests r WHERE r.id = NEW.request_id AND r.pool_id = '#{pool_id}'::uuid) THEN " <>
          "PERFORM nextval('#{name}_passes'); PERFORM pg_advisory_xact_lock(#{key}); END IF; RETURN NULL; END $$"
      )

      Repo.query!(
        "CREATE CONSTRAINT TRIGGER #{name} AFTER UPDATE ON codex_turns DEFERRABLE INITIALLY DEFERRED FOR EACH ROW " <>
          "WHEN (OLD.first_visible_output_at IS NULL AND NEW.first_visible_output_at IS NOT NULL) EXECUTE FUNCTION #{name}()"
      )
    end)

    %{name: name, key: key}
  end

  defp hold_gate!(%{key: key}) do
    holder = start_supervised!({Postgrex, connection_options()}, id: {:visible_stamp_gate_holder, key})
    %{rows: [[backend]]} = Postgrex.query!(holder, "SELECT pg_backend_pid()", [])
    %{rows: [[_void]]} = Postgrex.query!(holder, "SELECT pg_advisory_lock($1)", [key])
    %{conn: holder, backend: backend, key: key}
  end

  defp release_gate!(%{conn: holder, key: key}) do
    assert %{rows: [[true]]} = Postgrex.query!(holder, "SELECT pg_advisory_unlock($1)", [key])
    :ok
  end

  # A PostgreSQL connection outside the Repo pool the listener draws from.
  defp observer!, do: start_supervised!({Postgrex, connection_options()}, id: {:visible_stamp_gate_observer, System.unique_integer([:positive])})

  defp connection_options, do: Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir])

  # The stamp has reached its `passes`-th COMMIT and waits on the gate;
  # `pg_stat_activity` and the sequence are sampled again until they agree.
  defp await_gate_waiter!(observer, gate, holder, passes) do
    await_gate_waiter!(observer, gate, holder, passes, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_gate_waiter!(observer, gate, holder, passes, deadline) do
    %{rows: waiters} = Postgrex.query!(observer, "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [holder.backend])
    seen = gate_passes(observer, gate)

    cond do
      seen == passes and match?([[_pid]], waiters) ->
        [[pid]] = waiters
        pid

      seen > passes ->
        flunk("the stamp passed its COMMIT gate #{seen} times, expected #{passes}")

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("nothing waited at COMMIT pass #{passes} (passes #{seen}, waiters #{inspect(waiters)})")

      true ->
        Process.sleep(10)
        await_gate_waiter!(observer, gate, holder, passes, deadline)
    end
  end

  defp gate_passes(observer, %{name: name}) do
    %{rows: [[value, called?]]} = Postgrex.query!(observer, "SELECT last_value, is_called FROM #{name}_passes", [])
    if called?, do: value, else: 0
  end

  defp cancel_backend!(observer, backend) do
    %{rows: [[cancelled?]]} = Postgrex.query!(observer, "SELECT pg_cancel_backend($1)", [backend])
    cancelled?
  end

  # --- client --------------------------------------------------------------

  # The released client's native turn over HTTP: the thread in `session-id`
  # and the turn metadata in the body.
  defp post!(setup) do
    port = start_public_endpoint!()
    thread_id = Ecto.UUID.generate()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"content-type", "application/json"},
      {"accept", "text/event-stream"},
      {"session-id", thread_id},
      {"originator", "codex_cli_rs"}
    ]

    payload = %{
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("synthetic visible stamp turn"),
      "stream" => true,
      "client_metadata" => %{
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => "visible-stamp-turn", "request_kind" => "turn"})
      }
    }

    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @native_path, headers, CodexPooler.JSON.encode!(payload))
    on_exit(fn -> Mint.HTTP.close(conn) end)
    %{conn: conn, ref: ref, status: nil, body: "", outcome: nil}
  end

  # Collects what arrives within a short quiet period without requiring more.
  defp read_quietly(%{outcome: nil} = client) do
    case recv(client, @quiet_read_ms) do
      {:timeout, client} -> client
      {:ok, client} -> read_quietly(client)
    end
  end

  defp read_quietly(client), do: client

  defp read_to_end(%{outcome: nil} = client), do: client |> recv!() |> read_to_end()
  defp read_to_end(client), do: client

  defp recv!(client) do
    {_result, client} = recv(client, @detection_timeout_ms)
    client
  end

  defp recv(%{conn: conn, ref: ref} = client, timeout_ms) do
    case Mint.HTTP.recv(conn, 0, timeout_ms) do
      {:ok, conn, responses} ->
        {:ok, Enum.reduce(responses, %{client | conn: conn}, &apply_response(&1, &2, ref))}

      {:error, conn, %Mint.TransportError{reason: :timeout}, responses} when timeout_ms == @quiet_read_ms ->
        {:timeout, Enum.reduce(responses, %{client | conn: conn}, &apply_response(&1, &2, ref))}

      {:error, conn, reason, responses} ->
        client = Enum.reduce(responses, %{client | conn: conn}, &apply_response(&1, &2, ref))
        {:ok, %{client | outcome: {:error, reason}}}
    end
  end

  defp apply_response({:status, ref, status}, client, ref), do: %{client | status: status}
  defp apply_response({:data, ref, data}, client, ref), do: %{client | body: client.body <> data}
  defp apply_response({:done, ref}, client, ref), do: %{client | outcome: :done}
  defp apply_response(_other, client, _ref), do: client

  defp with_info_log(fun) do
    previous = Logger.level()
    on_exit(fn -> Logger.configure(level: previous) end)
    Logger.configure(level: :info)

    try do
      with_log([level: :info], fun)
    after
      Logger.configure(level: previous)
    end
  end

  defp event_types(body) do
    body
    |> String.split("\n", trim: true)
    |> Enum.flat_map(&data_event_type/1)
  end

  defp data_event_type("data: " <> json) do
    case CodexPooler.JSON.decode(json) do
      {:ok, %{"type" => type}} -> [type]
      _other -> []
    end
  end

  defp data_event_type(_line), do: []

  # --- rows ----------------------------------------------------------------

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))

  defp turn!(setup) do
    [request] = pool_requests(setup)
    Repo.get_by!(CodexTurn, request_id: request.id)
  end

  defp settled_request!(setup), do: await_settled!(setup, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_settled!(setup, deadline) do
    case pool_requests(setup) do
      [%Request{status: status} = request] when status not in ["accepted", "in_progress"] ->
        request

      requests ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: flunk("the request never settled: #{inspect(Enum.map(requests, & &1.status))}"),
          else: Process.sleep(10) && await_settled!(setup, deadline)
    end
  end

  defp recorded_settlements(request) do
    Repo.all(
      from(entry in LedgerEntry,
        where: entry.request_id == ^request.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded",
        select: {entry.usage_status, entry.total_tokens}
      )
    )
  end

  # --- events --------------------------------------------------------------

  defp created_event, do: {"response.created", %{"type" => "response.created", "response" => %{"id" => @response_id, "status" => "in_progress"}}}

  defp delta_event,
    do: {"response.output_text.delta", %{"type" => "response.output_text.delta", "response_id" => @response_id, "output_index" => 0, "content_index" => 0, "delta" => "synthetic stamped text"}}

  defp completed_event do
    {"response.completed",
     %{
       "type" => "response.completed",
       "response" => %{"id" => @response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 3, "total_tokens" => 5}}
     }}
  end
end
