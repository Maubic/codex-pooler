defmodule CodexPooler.Gateway.Persistence.SameKeyExpiredOwnerRecoveryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn, RuntimeCleanup, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.Aliases
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Pools.Pool
  alias CodexPooler.UnboxedFixture
  alias CodexPoolerWeb.Runtime.{BackendCodexWebsocketOwnerForwardingSupport, MailboxPrefixRaceSupport}
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @budget 15_000
  @path "/backend-api/codex/responses"

  setup_all do
    cleanup_key = {__MODULE__, make_ref()}

    on_exit(fn ->
      try do
        if identity = :persistent_term.get(cleanup_key, nil), do: CodexPooler.InstancePresencePeer.assert_os_process_stopped!(identity)
        CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "expired_replacement_peer_cleanup", peer_os_stopped: true}) end)
      after
        :persistent_term.erase(cleanup_key)
      end
    end)

    peer = BackendCodexWebsocketOwnerForwardingSupport.start_shared_bridge_peer!()
    :persistent_term.put(cleanup_key, CodexPooler.InstancePresencePeer.capture_os_process_identity!(:erpc.call(peer, System, :pid, [])))
    assert {:ok, _registry} = :erpc.call(peer, GenServer, :start, [CodexPooler.Platform.ExecutionRegistry, nil, [name: CodexPooler.Platform.ExecutionRegistry]])
    modules = [CodexPooler.Gateway.Persistence.SessionContinuity.ExpiredSessions, RuntimeCleanup, CodexPooler.Gateway.Runtime.Finalization.Interruption]

    hashes =
      Map.new(modules, fn module ->
        digest = module.module_info(:md5)
        assert digest == :erpc.call(peer, module, :module_info, [:md5])
        {inspect(module), Base.encode16(digest, case: :lower)}
      end)

    CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "expired_replacement_runtime", distinct_nodes: peer != node(), equal_beam: true, beam_md5: hashes}) end)

    %{peer: peer}
  end

  setup context do
    if context[:wire] || context[:independent] do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      Sandbox.mode(Repo, :auto)
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
      CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
      settings = %{OperationalSettings.current() | bridge_owner_lease_ttl_seconds: 300, bridge_owner_lease_renewal_seconds: 60}
      Application.put_env(:codex_pooler, OperationalSettings, settings: settings)
      previous = :erpc.call(context.peer, Application, :fetch_env, [:codex_pooler, OperationalSettings])
      on_exit(fn -> restore_peer_settings(context.peer, previous) end)
      :erpc.call(context.peer, Application, :put_env, [:codex_pooler, OperationalSettings, [settings: settings]])
    end

    :ok
  end

  for mode <- ["full", "lite"], owner <- [:local, :remote], replacement <- [true, false] do
    @tag wire: true, mode: mode, owner: owner, replacement: replacement
    test "#{mode} #{owner} real websocket replacement=#{replacement} preserves recovery and resend policy", context do
      fixture = open_wire!(context)
      expire!(fixture.session.id)

      successor_transport =
        if context.replacement do
          {conn, ws, ref, _} = connect_wire!(fixture)
          {conn, ws} = public_websocket_send_text!(conn, ws, ref, CodexPooler.JSON.encode!(fixture.payload))
          {conn, ws, refused} = receive_terminal!(conn, ws, ref)
          assert CodexPooler.JSON.decode!(refused)["error"]["code"] == "duplicate_turn"
          successor = Repo.one!(from s in CodexSession, where: s.pool_id == ^fixture.setup.pool.id and s.status == "active")
          refute successor.id == fixture.session.id
          assert Repo.reload!(fixture.session).status == "closed"
          {conn, ws, ref, successor}
        end

      assert {:ok, summary} = RuntimeCleanup.cleanup_expired_runtime_state(now())
      assert summary.expired_owner_sessions_recovered == 1
      assert_receive {:DOWN, ref, :process, actor, _reason} when ref == fixture.provider_monitor and actor == fixture.provider, @budget
      request = Repo.get!(Request, fixture.request.id)
      assert request.status == "failed"
      assert request.last_error_code == "owner_unavailable"
      assert Repo.get_by!(CodexTurn, request_id: request.id).status == "interrupted"
      assert ledger_entry_kinds(request) == ["release", "reservation", "settlement"]
      if successor_transport, do: assert(Repo.reload!(elem(successor_transport, 3)).status == "active")
      assert {:ok, repeated} = RuntimeCleanup.cleanup_expired_runtime_state(now())
      assert repeated.expired_owner_sessions_recovered == 0

      {successor_conn, successor_ws, successor_ref, _} = successor_transport || connect_wire!(fixture)
      {successor_conn, successor_ws} = public_websocket_send_text!(successor_conn, successor_ws, successor_ref, CodexPooler.JSON.encode!(fixture.payload))
      {_conn, _ws, text} = receive_terminal!(successor_conn, successor_ws, successor_ref)
      assert CodexPooler.JSON.decode!(text)["error"]["code"] == "duplicate_turn"
      assert FakeUpstream.count(fixture.upstream) == 1
      {next_conn, next_ws, next_ref, _} = connect_wire!(fixture)
      new_payload = put_in(fixture.payload, ["client_metadata", "x-codex-turn-metadata"], CodexPooler.JSON.encode!(%{"thread_id" => fixture.thread, "session_id" => fixture.thread, "turn_id" => "synthetic-successor", "request_kind" => "turn", "agent_name" => "/root"})) |> Map.put("input", native_text_input("synthetic next turn"))
      {next_conn, next_ws} = public_websocket_send_text!(next_conn, next_ws, next_ref, CodexPooler.JSON.encode!(new_payload))
      {_conn, _ws, text} = receive_terminal!(next_conn, next_ws, next_ref)
      assert CodexPooler.JSON.decode!(text)["type"] == "response.completed"
      assert FakeUpstream.count(fixture.upstream) == 2
      assert :ok = FakeUpstream.verify!(fixture.upstream)
      CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "same_key_expired_websocket", mode: context.mode, replacement: context.replacement, remote_owner: node(fixture.owner) != node(), distinct_backends: fixture.owner_backend != fixture.local_backend, owner_incarnation_verified: true, process_generation: fixture.process_generation, recovered: summary.expired_owner_sessions_recovered, predecessor: request.status, successor_usable: true, identical_resend: "duplicate_turn", physical_sends: 2, predecessor_ledger: ledger_entry_kinds(request)}) end)
    end
  end

  defp open_wire!(context) do
    slug = "expired-replacement-#{Ecto.UUID.generate()}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      delete_committed_pools!(Repo.all(from p in Pool, where: p.slug == ^slug, select: p.id))
      Repo.delete_all(from p in PricingSnapshot, where: p.model_identifier == ^slug)
    end)

    gate = make_ref()
    terminal = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_success", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
    first = FakeUpstream.delayed_terminal_sse_stream([], terminal, notify: self(), release_ref: gate)
    # provenance: synthetic_adversarial; identical resend is refused, then a new permitted turn succeeds.
    upstream = start_upstream(FakeUpstream.strict_sequence([first, FakeUpstream.sse_stream([{"response.completed", terminal}])]))
    setup = gateway_setup(upstream, pool_slug: slug, upstream_model_id: slug)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, context.mode)
    thread = Ecto.UUID.generate()
    if context.owner == :remote, do: start_remote_owner!(setup, thread, context.peer)
    port = start_public_endpoint!()
    payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic recovery"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic-recovery", "request_kind" => "turn", "agent_name" => "/root"})}}
    fixture = %{setup: setup, mode: context.mode, port: port, thread: thread, payload: payload}
    {conn, ws, ref, _} = connect_wire!(fixture)
    {_conn, _ws} = public_websocket_send_text!(conn, ws, ref, CodexPooler.JSON.encode!(payload))
    assert_receive {:fake_upstream_timeout_barrier, :before_terminal, handler, ^gate}, @budget
    on_exit(fn -> send(handler, {:fake_upstream_release_timeout, gate}) end)
    [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    turn = Repo.get_by!(CodexTurn, request_id: request.id)
    session = Repo.get!(CodexSession, turn.codex_session_id)
    owner_node = if context.owner == :remote, do: context.peer, else: node()
    assert {:ok, owner} = :erpc.call(owner_node, WebsocketOwnerSession, :lookup, [session.id])
    MailboxPrefixRaceSupport.suppress_owned_periodic_renewal!(owner, :owner)
    assert node(owner) == owner_node
    assert session.owner_instance_boot_id == :erpc.call(owner_node, CodexPooler.Platform.InstancePresence.Identity, :boot_id, [])
    state = :sys.get_state(owner, @budget)
    assert state.owner_lease_token == session.owner_lease_token
    assert state.process_generation > 0
    [[local_backend]] = Repo.query!("SELECT pg_backend_pid()").rows
    %{rows: [[owner_backend]]} = :erpc.call(owner_node, Repo, :query!, ["SELECT pg_backend_pid()"])
    if context.owner == :remote, do: refute(local_backend == owner_backend)
    Map.merge(fixture, %{owner_backend: owner_backend, local_backend: local_backend, process_generation: state.process_generation, upstream: upstream, request: request, session: session, owner: owner, provider: handler, provider_monitor: Process.monitor(handler)})
  end

  defp start_remote_owner!(setup, window, peer) do
    assert {:ok, auth} = CodexPooler.Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, session} = :erpc.call(peer, CodexPooler.Gateway.Websocket, :start_codex_session, [auth, %{session_header: window, session_header_source: "x-codex-window-id"}])

    on_exit(fn ->
      case :erpc.call(peer, WebsocketOwnerSession, :lookup, [session.id]) do
        {:ok, owner} ->
          try do
            :erpc.call(peer, GenServer, :stop, [owner, :normal, @budget])
          catch
            :exit, _already_stopped -> :ok
          end

        _gone ->
          :ok
      end
    end)

    assert session.owner_instance_boot_id == :erpc.call(peer, CodexPooler.Platform.InstancePresence.Identity, :boot_id, [])
    persistence = :erpc.call(peer, CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness, :real_persistence_boundary, [])
    assert {:ok, _owner} = :erpc.call(peer, WebsocketOwnerSession, :start_owner, [[codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: session.owner_instance_id, owner_renewal_ms: 60_000, persistence: persistence]])
  end

  defp connect_wire!(fixture) do
    headers = if fixture.mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"}], else: []
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", fixture.port, protocols: [:http1])
    on_exit(fn -> Mint.HTTP.close(conn) end)
    headers = [{"authorization", fixture.setup.authorization}, {"x-codex-window-id", fixture.thread}] ++ headers
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @path, headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, ws} = mint_websocket_new!(conn, ref, status, response_headers)
    result = {conn, ws, ref, response_headers}
    on_exit(fn -> Mint.HTTP.close(conn) end)
    result
  end

  defp receive_terminal!(conn, ws, ref) do
    {conn, ws, text} = public_websocket_receive_text!(conn, ws, ref)
    if CodexPooler.JSON.decode!(text)["type"] in ["response.completed", "response.failed", "error"], do: {conn, ws, text}, else: receive_terminal!(conn, ws, ref)
  end

  defp restore_peer_settings(peer, {:ok, previous}), do: :erpc.call(peer, Application, :put_env, [:codex_pooler, OperationalSettings, previous])
  defp restore_peer_settings(peer, :error), do: :erpc.call(peer, Application, :delete_env, [:codex_pooler, OperationalSettings])

  defp expire!(session_id) do
    deadline = DateTime.add(now(), -1, :second)

    Repo.transaction(fn ->
      Repo.one!(from s in CodexSession, where: s.id == ^session_id, lock: "FOR UPDATE")
      Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id and l.status == "active"), set: [expires_at: deadline])
      Repo.update_all(from(s in CodexSession, where: s.id == ^session_id), set: [owner_lease_expires_at: deadline])
    end)
  end

  describe "transport websocket" do
    test "control: the expired-owner sweep interrupts an in-progress turn of a lapsed session" do
      fixture = lapsed_session_with_turn!()

      assert {:ok, summary} = RuntimeCleanup.cleanup_expired_runtime_state(now())
      assert summary.expired_owner_sessions_recovered == 1
      assert Repo.reload!(fixture.turn).status == "interrupted"
    end

    test "same-key replacement keeps the predecessor recoverable by one ownership sweep" do
      fixture = lapsed_session_with_turn!()
      assert {:ok, successor} = SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
      refute successor.id == fixture.session.id

      assert {:ok, summary} = RuntimeCleanup.cleanup_expired_runtime_state(now())
      assert summary.expired_owner_sessions_recovered == 1

      assert Repo.reload!(fixture.turn).status == "interrupted"
      assert Repo.reload!(fixture.request).status == "failed"
      assert Repo.reload!(successor).status == "active"
      assert {:ok, repeated} = RuntimeCleanup.cleanup_expired_runtime_state(now())
      assert repeated.expired_owner_sessions_recovered == 0
    end
  end

  test "window alias replacement retains exact old authority until settlement" do
    fixture = lapsed_session_with_turn!()
    alias_opts = RequestOptions.for_websocket(%{session_header: Ecto.UUID.generate(), session_header_source: "x-codex-window-id"})
    assert :ok = Aliases.register!(fixture.session, fixture.auth, alias_opts, now())
    assert {:ok, successor} = SessionContinuity.start_codex_session(fixture.auth, alias_opts)
    refute successor.id == fixture.session.id
    closed = Repo.reload!(fixture.session)
    assert closed.status == "closed"
    assert {:error, :owner_unavailable} = SessionContinuity.renew_owner_token(closed, closed.owner_lease_token, fixture.opts)
    assert {:error, :owner_unavailable} = SessionContinuity.renew_owner_token(closed, closed.owner_lease_token, fixture.opts, take_over_expired: true)
    assert {:ok, %{expired_owner_leases: 0}} = RuntimeCleanup.cleanup_expired(now())
    assert Repo.get_by!(BridgeOwnerLease, codex_session_id: closed.id).status == "active"
    assert {:ok, %{expired_owner_sessions_recovered: 1}} = RuntimeCleanup.cleanup_expired_runtime_state(now())
    assert Repo.reload!(closed) == closed
    assert Repo.reload!(successor).status == "active"
    assert ledger_entry_kinds(fixture.request) == ["release", "reservation", "settlement"]
  end

  test "same-key replacement does not interrupt an HTTP-served turn" do
    fixture = lapsed_session_with_turn!(transport: "http_sse")
    assert {:ok, _successor} = SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
    before = {Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), Repo.reload!(fixture.turn)}
    assert {:ok, %{expired_owner_sessions_recovered: 0}} = RuntimeCleanup.cleanup_expired_runtime_state(now())
    assert before == {Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), Repo.reload!(fixture.turn)}
    assert ledger_entry_kinds(fixture.request) == ["reservation"]
  end

  test "another key or Pool cannot close the predecessor" do
    fixture = lapsed_session_with_turn!()
    other_key = active_api_key_fixture(fixture.auth.pool)
    other_pool = active_api_key_fixture()
    before = Repo.reload!(fixture.session)

    for auth <- [other_key, other_pool] do
      assert {:ok, successor} = SessionContinuity.start_codex_session(auth, fixture.opts)
      refute successor.id == before.id
      assert Repo.reload!(before) == before
      assert ledger_entry_kinds(fixture.request) == ["reservation"]
    end
  end

  for winner <- [:replacement, :takeover, :completion] do
    @tag independent: true
    test "#{winner} commits after selection and before the expired snapshot is applied" do
      fixture = lapsed_session_with_turn!(committed: true)
      {cleanup, ref} = hold_cleanup_selection!(fixture)
      [[observer]] = Repo.query!("SELECT pg_backend_pid()").rows

      {winner_backend, successor} =
        independent!(fn ->
          [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows

          result =
            case unquote(winner) do
              :replacement ->
                assert {:ok, successor} = SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
                successor

              :takeover ->
                assert {:ok, successor} = SessionContinuity.renew_owner_token(fixture.session, fixture.session.owner_lease_token, fixture.opts, take_over_expired: true)
                refute successor.owner_lease_token == fixture.session.owner_lease_token
                successor

              :completion ->
                complete_fixture!(fixture)
                Repo.reload!(fixture.session)
            end

          {backend, result}
        end)

      assert winner_backend != observer
      snapshot = {Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), Repo.reload!(fixture.turn)}
      send(cleanup.pid, {:release_runtime_cleanup_owner_candidates, ref})
      assert {:ok, _summary} = Task.await(cleanup, @budget)

      case unquote(winner) do
        :replacement ->
          assert Repo.reload!(fixture.turn).status == "interrupted"
          assert Repo.reload!(successor).status == "active"
          assert ledger_entry_kinds(fixture.request) == ["release", "reservation", "settlement"]

        _other ->
          assert snapshot == {Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), Repo.reload!(fixture.turn)}
      end
    end
  end

  @tag independent: true
  test "replacement waits for real renewal commit and cannot retire the new token" do
    fixture = lapsed_session_with_turn!(committed: true)
    supervisor = start_supervised!(Task.Supervisor)
    parent = self()
    ref = make_ref()

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Repo.transaction(fn ->
          assert {:ok, renewed} = SessionContinuity.renew_owner_token(fixture.session, fixture.session.owner_lease_token, fixture.opts, take_over_expired: true)
          [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
          send(parent, {:renewal_held, ref, backend, renewed})
          receive do: ({:release_renewal, ^ref} -> renewed)
        end)
      end)

    on_exit(fn -> send(holder.pid, {:release_renewal, ref}) end)
    assert_receive {:renewal_held, ^ref, backend, renewed}, @budget
    handler = {__MODULE__, :replacement_lock, ref}
    on_exit(fn -> :telemetry.detach(handler) end)
    assert :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.observe_replacement_lock/4, {parent, ref})

    replacement =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Process.put({__MODULE__, :replacement_lock}, ref)

        try do
          Repo.transaction(fn ->
            Repo.query!("SET LOCAL lock_timeout = '100ms'")
            [[waiter]] = Repo.query!("SELECT pg_backend_pid()").rows
            send(parent, {:replacement_backend, ref, waiter})
            SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
          end)
        rescue
          error in Postgrex.Error -> {:lock_error, error.postgres.code}
        after
          Process.delete({__MODULE__, :replacement_lock})
        end
      end)

    assert_receive {:replacement_backend, ^ref, waiter}, @budget
    refute waiter == backend
    assert_receive {:replacement_lock_error, ^ref, :lock_not_available, true}, @budget
    assert {:lock_error, :lock_not_available} = Task.await(replacement, @budget)
    assert Repo.reload!(fixture.session).owner_lease_token == fixture.session.owner_lease_token
    send(holder.pid, {:release_renewal, ref})
    assert {:ok, _} = Task.await(holder, @budget)
    assert {:ok, session} = independent!(fn -> SessionContinuity.start_codex_session(fixture.auth, fixture.opts) end)
    assert session.id == fixture.session.id
    assert session.owner_lease_token == renewed.owner_lease_token
    assert {:ok, %{expired_owner_sessions_recovered: 0}} = RuntimeCleanup.cleanup_expired_runtime_state(now())
    assert Repo.reload!(fixture.turn).status == "in_progress"
    CodexPooler.TestDiagnostics.puts(fn -> CodexPooler.JSON.encode!(%{scenario: "replacement_waits_for_renewal", distinct_backends: true, session_lock_observed: true, lock_error: "55P03", query_telemetry_observed: true, renewed_token_preserved: true}) end)
  end

  test "completion before replacement leaves terminal accounting unchanged" do
    fixture = lapsed_session_with_turn!()
    complete_fixture!(fixture)
    before = {Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), Repo.reload!(fixture.turn)}
    assert {:ok, _successor} = SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
    assert {:ok, %{expired_owner_sessions_recovered: 0}} = RuntimeCleanup.cleanup_expired_runtime_state(now())
    assert before == {Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), Repo.reload!(fixture.turn)}
    assert ledger_entry_kinds(fixture.request) == ["release", "reservation", "settlement"]
  end

  @tag independent: true
  test "two selected sweeps consume the retained authority only once" do
    fixture = lapsed_session_with_turn!(committed: true)
    assert {:ok, successor} = SessionContinuity.start_codex_session(fixture.auth, fixture.opts)
    {first, ref} = hold_cleanup_selection!(fixture)
    second = Task.Supervisor.async_nolink(start_supervised!({Task.Supervisor, []}, id: make_ref()), fn -> RuntimeCleanup.cleanup_expired_runtime_state(now()) end)
    on_exit(fn -> send(second.pid, {:release_runtime_cleanup_owner_candidates, ref}) end)
    assert_receive {:runtime_cleanup_owner_candidates_selected, second_pid, ^ref, candidates}, @budget
    assert second_pid == second.pid
    assert Enum.any?(candidates, &(&1.session_id == fixture.session.id))
    send(first.pid, {:release_runtime_cleanup_owner_candidates, ref})
    send(second.pid, {:release_runtime_cleanup_owner_candidates, ref})
    assert {:ok, _} = Task.await(first, @budget)
    assert {:ok, _} = Task.await(second, @budget)
    assert ledger_entry_kinds(fixture.request) == ["release", "reservation", "settlement"]
    assert Repo.reload!(fixture.turn).status == "interrupted"
    assert Repo.reload!(successor).status == "active"
  end

  for field <- [:owner_instance_boot_id, :owner_instance_id, :lease_token, :api_key_id, :pool_id] do
    test "a mismatched lease #{field} is not expired-owner authority" do
      fixture = lapsed_session_with_turn!()
      lease = Repo.get_by!(BridgeOwnerLease, codex_session_id: fixture.session.id)
      other = active_api_key_fixture()

      value =
        case unquote(field) do
          :api_key_id -> other.api_key.id
          :pool_id -> other.pool.id
          _identity -> Ecto.UUID.generate()
        end

      Repo.update!(Ecto.Changeset.change(lease, %{unquote(field) => value}))
      assert {:ok, %{expired_owner_sessions_recovered: 0}} = RuntimeCleanup.cleanup_expired_runtime_state(now())
      assert Repo.reload!(fixture.turn).status == "in_progress"
      assert ledger_entry_kinds(fixture.request) == ["reservation"]
    end
  end

  defp complete_fixture!(fixture) do
    assert {:ok, result} = CodexPooler.Accounting.finalize_request(fixture.request, fixture.attempt, %{request_status: "succeeded", attempt_status: "succeeded", response_status_code: 200, usage: %{status: "usage_known", input_tokens: 3, output_tokens: 2, total_tokens: 5}})
    assert {:ok, _} = SessionContinuity.complete_codex_turn({:ok, result}, "succeeded", nil, fixture.attempt)
  end

  defp hold_cleanup_selection!(fixture) do
    ref = make_ref()
    CodexPooler.TestAppEnv.restore_on_exit(:runtime_cleanup_owner_candidate_test_barrier)
    Application.put_env(:codex_pooler, :runtime_cleanup_owner_candidate_test_barrier, {self(), ref})
    supervisor = start_supervised!(Task.Supervisor)
    task = Task.Supervisor.async_nolink(supervisor, fn -> RuntimeCleanup.cleanup_expired_runtime_state(now()) end)
    on_exit(fn -> send(task.pid, {:release_runtime_cleanup_owner_candidates, ref}) end)
    assert_receive {:runtime_cleanup_owner_candidates_selected, pid, ^ref, candidates}, @budget
    assert pid == task.pid
    assert Enum.any?(candidates, &(&1.session_id == fixture.session.id))
    {task, ref}
  end

  defp independent!(operation) do
    Task.Supervisor.async_nolink(start_supervised!({Task.Supervisor, []}, id: make_ref()), operation) |> Task.await(@budget)
  end

  @doc false
  def observe_replacement_lock(_event, _measurements, metadata, {parent, ref}) do
    if Process.get({__MODULE__, :replacement_lock}) == ref do
      case metadata.result do
        {:error, %Postgrex.Error{postgres: %{code: code}}} ->
          session_lock? = String.contains?(metadata.query, "codex_sessions") and String.contains?(metadata.query, "FOR UPDATE")
          send(parent, {:replacement_lock_error, ref, code, session_lock?})

        _other ->
          :ok
      end
    end

    :ok
  end

  # A websocket session started through the real start path, whose owner lease
  # then lapses in the database, with one in-progress turn and a reservation
  # recorded on its request.
  defp lapsed_session_with_turn!(options \\ []) do
    slug = "expired-fixture-#{Ecto.UUID.generate()}"

    if options[:committed] do
      UnboxedFixture.register_unboxed_cleanup!(fn ->
        delete_committed_pools!(Repo.all(from p in Pool, where: p.slug == ^slug, select: p.id))
        Repo.delete_all(from i in CodexPooler.Upstreams.Schemas.UpstreamIdentity, where: i.account_label == ^slug)
      end)
    end

    pool = pool_fixture(%{slug: slug})
    %{api_key: api_key} = active_api_key_fixture(pool)
    transport = Keyword.get(options, :transport, "websocket")
    auth = %{pool: pool, api_key: api_key}
    %{assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: slug})
    model = model_fixture(pool, %{exposed_model_id: "sample-recovery-model"})
    opts = RequestOptions.for_websocket(%{session_header: Ecto.UUID.generate()})

    assert {:ok, session} = SessionContinuity.start_codex_session(auth, opts)

    deadline = DateTime.add(now(), -1, :second)

    Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [owner_lease_expires_at: deadline])

    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id), set: [expires_at: deadline])

    request =
      request_fixture(auth, %{
        model_id: model.id,
        requested_model: model.exposed_model_id,
        transport: transport,
        status: "in_progress",
        usage_status: "usage_pending",
        completed_at: nil,
        response_status_code: nil,
        request_metadata: %{"codex_session_id" => session.id}
      })

    attempt =
      attempt_fixture(request, assignment, %{
        status: "in_progress",
        completed_at: nil,
        usage_status: "usage_pending",
        response_metadata: %{}
      })

    turn = insert_turn!(session, request, attempt)

    request
    |> ledger_entry_fixture(%{
      attempt_id: attempt.id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: assignment.upstream_identity_id,
      entry_kind: "reservation",
      amount_status: "recorded",
      usage_status: "usage_pending",
      transport: transport,
      output_tokens: 8,
      total_tokens: 12,
      details: %{"source" => "test_reservation"}
    })
    |> Ecto.Changeset.change(%{source_event_id: "request:#{request.id}:reservation"})
    |> Repo.update!()

    %{auth: auth, opts: opts, turn: turn, session: session, request: request, attempt: attempt}
  end

  defp insert_turn!(session, request, attempt) do
    timestamp = DateTime.add(now(), -30, :second)

    %CodexTurn{
      codex_session_id: session.id,
      request_id: request.id,
      turn_sequence: 1,
      transport_kind: request.transport,
      final_attempt_id: attempt.id,
      status: "in_progress",
      started_at: timestamp,
      created_at: timestamp,
      updated_at: timestamp
    }
    |> Repo.insert!()
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
