defmodule CodexPoolerWeb.Runtime.BackendCodexMailboxDiagnosticsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  alias CodexPooler.{Access, Accounting}
  alias CodexPooler.Accounting.{Attempt, ClientRetry, LedgerEntry, Request, RequestClientRetryLink, RequestReplayEntitlement}
  alias CodexPooler.Accounting.RequestLifecycle.FailedPredecessorResend
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.{NativeMailboxContinuation, RequestOptions}
  alias CodexPooler.Gateway.Persistence.{BridgeSessionAlias, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true
  @stages [:no_candidate, :settlement, :authorization, :session, :witness, :ending, :output_prefix, :verified]

  setup context do
    if context[:committed] do
      :ok = Sandbox.mode(Repo, :auto)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    end

    :ok
  end

  for shape <- [:mailbox, :ordinary, :generation] do
    @tag mailbox_diagnostic_negative: true
    test "execution resolver and reservation carry the original #{shape} refusal without writes" do
      fixture = fixture!()
      payload = if unquote(shape) == :ordinary, do: Map.put(fixture.payload, "instructions", "synthetic changed instructions"), else: Map.update!(fixture.payload, "input", &(&1 ++ [fixture.output, mailbox()]))
      if unquote(shape) == :generation, do: Repo.update!(Ecto.Changeset.change(fixture.attempt, replay_generation: 1))
      witness = mailbox_witness(fixture, payload)
      "codex-turn:" <> encoded = fixture.request.correlation_id
      semantic = Base.url_decode64!(encoded, padding: false)
      scope = %{pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, model_id: fixture.setup.model.id, endpoint: @path, codex_session_id: fixture.turn.codex_session_id, semantic_turn_digest: semantic, native_client_retry_witness: witness}
      before = counts(fixture)
      assert {:ok, {:error, refusal}} = Repo.transaction(fn -> FailedPredecessorResend.resolve_execution(fixture.request.correlation_id, scope, fixture.request.id) end)

      expected =
        case unquote(shape) do
          :mailbox -> %{disposition: :terminal_predecessor, mailbox_check: :verified}
          :generation -> %{disposition: :terminal_predecessor, mailbox_check: :settlement}
          :ordinary -> :terminal_predecessor
        end

      assert refusal == expected

      {:ok, auth} = Access.authenticate_authorization_header(fixture.setup.authorization)
      options = %{endpoint: @path, transport: "websocket", correlation_id: fixture.request.correlation_id, codex_session: Repo.get!(CodexSession, fixture.turn.codex_session_id), semantic_turn_digest: semantic, native_client_retry_witness: witness, execution_recovery_request_id: fixture.request.id}
      assert {:error, %{code: :duplicate_request, resend_disposition: :terminal_predecessor} = error} = Accounting.claim_websocket_turn(auth, fixture.setup.model, options)
      if unquote(shape) == :ordinary, do: refute(Map.has_key?(error, :mailbox_check)), else: assert(error.mailbox_check == expected.mailbox_check)
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
    end
  end

  for transport <- [:http, :websocket], guard <- [:entitlement, :live_successor, :settled_successor_without_turn] do
    @tag mailbox_diagnostic_negative: true
    test "#{transport} preserves the actual mailbox proof when blocked by #{guard}" do
      fixture = fixture!()
      payload = Map.update!(fixture.payload, "input", &(&1 ++ [fixture.output, mailbox()]))
      assert current_mailbox_stage(fixture, payload) == :verified

      case unquote(guard) do
        :entitlement -> insert_entitlement!(fixture)
        :live_successor -> insert_successor!(fixture, payload, false)
        :settled_successor_without_turn -> insert_successor!(fixture, payload, true)
      end

      before = counts(fixture)
      {result, logs} = with_info_log(fn -> send_continuation(fixture, payload, unquote(transport)) end)
      assert_refusal(result, unquote(transport))
      expected = if unquote(guard) == :settled_successor_without_turn, do: :settlement, else: :verified
      assert logs =~ "mailbox_check=#{expected}"
      if unquote(guard) == :live_successor, do: assert(logs =~ "resend_disposition=active_predecessor")
      if unquote(guard) == :entitlement, do: assert(logs =~ "resend_disposition=entitlement_present")
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
    end
  end

  for transport <- [:http, :websocket] do
    @tag mailbox_diagnostic_negative: true
    test "#{transport} ordinary refusal keeps its atom and has no mailbox stage" do
      fixture = fixture!()
      payload = Map.put(fixture.payload, "instructions", "synthetic changed instructions")
      before = counts(fixture)
      scope = %{pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, model_id: fixture.setup.model.id, endpoint: @path, codex_session_id: fixture.turn.codex_session_id, native_client_retry_witness: mailbox_witness(fixture, payload), mailbox_check: %{stage: :verified}}
      assert {:ok, {:error, :terminal_predecessor}} = Repo.transaction(fn -> FailedPredecessorResend.resolve(fixture.request.correlation_id, scope) end)
      {result, logs} = with_info_log(fn -> send_continuation(fixture, payload, unquote(transport)) end)
      assert_refusal(result, unquote(transport))
      refute logs =~ "mailbox_check="
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
    end
  end

  for transport <- [:http, :websocket], stage <- @stages do
    @tag mailbox_diagnostic_negative: true
    test "#{transport} logs admission-time #{stage} with unchanged refusal and ledger" do
      fixture = fixture!()
      continuation = stage_fixture!(fixture, unquote(stage))
      before = counts(fixture)
      telemetry = attach_refusal_telemetry!()
      {result, logs} = with_info_log(fn -> send_continuation(fixture, continuation, unquote(transport)) end)

      assert_refusal(result, unquote(transport))
      label = if unquote(transport) == :http, do: "native http", else: "websocket"
      claim_stage = if unquote(transport) == :http, do: "native_http_turn_claim", else: "websocket_turn_claim"
      assert logs =~ "#{label} replay rejection stage=#{claim_stage}"
      assert logs =~ "mailbox_check=#{unquote(stage)}"
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
      assert_receive {^telemetry, %{count: 1}, %{stage: ^claim_stage, transport: telemetry_transport}}
      assert telemetry_transport == if(unquote(transport) == :http, do: "http", else: "websocket")
      refute_received {^telemetry, _, _}

      for row <- Repo.all(from r in Request, where: r.pool_id == ^fixture.setup.pool.id) do
        refute Map.has_key?(row.request_metadata, "mailbox_check")
        refute Map.has_key?(row.request_metadata, "mailbox_intent")
      end
    end
  end

  for transport <- [:http, :websocket], mutation <- [:witness, :receipt] do
    @tag committed: true, mailbox_diagnostic_negative: true
    test "#{transport} retains verified after a separate backend changes #{mutation} following rollback" do
      fixture = fixture!(true)
      payload = stage_fixture!(fixture, :verified)
      assert current_mailbox_stage(fixture, payload) == :verified
      before = counts(fixture)
      owner = self()
      id = "mailbox-after-rollback-#{System.unique_integer([:positive])}"
      claim_stage = if unquote(transport) == :http, do: "native_http_turn_claim", else: "websocket_turn_claim"
      on_exit(fn -> :telemetry.detach(id) end)

      :ok =
        :telemetry.attach(
          id,
          [:codex_pooler, :gateway, :duplicate_turn, :refused],
          fn _event, _measurements, %{stage: stage}, _config ->
            if stage == claim_stage do
              in_transaction? = Repo.in_transaction?()

              {reader_pid, writer_pid} =
                Repo.checkout(fn ->
                  reader_pid = backend_pid!()

                  writer =
                    Task.async(fn ->
                      Sandbox.unboxed_run(Repo, fn ->
                        {:ok, writer_pid} =
                          Repo.transaction(fn ->
                            writer_pid = backend_pid!()

                            case unquote(mutation) do
                              :witness -> Repo.update_all(from(r in Request, where: r.id == ^fixture.request.id), inc: [native_client_retry_auth_epoch: 1])
                              :receipt -> Repo.update_all(from(a in Attempt, where: a.id == ^fixture.attempt.id), set: [response_metadata: %{}])
                            end

                            writer_pid
                          end)

                        writer_pid
                      end)
                    end)

                  {reader_pid, Task.await(writer, 15_000)}
                end)

              send(owner, {id, in_transaction?, reader_pid, writer_pid})
            end
          end,
          nil
        )

      {result, logs} = with_info_log(fn -> send_continuation(fixture, payload, unquote(transport)) end)
      assert_refusal(result, unquote(transport))
      assert_receive {^id, false, reader_pid, writer_pid}
      assert reader_pid != writer_pid
      assert logs =~ "stage=#{claim_stage}"
      assert logs =~ "mailbox_check=verified"
      expected = if unquote(mutation) == :witness, do: :authorization, else: :output_prefix
      assert current_mailbox_stage(fixture, payload) == expected
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
    end
  end

  for inserted_edges <- [1, 2, 3] do
    @tag committed: true
    @tag mailbox_admission_lock_order: true
    @tag mailbox_lock_rediscovery: true
    test "nested HTTP admission fully exits after #{inserted_edges} historical graph insertions" do
      n = unquote(inserted_edges)
      fixture = fixture!(true)
      payload = Map.update!(fixture.payload, "input", &(&1 ++ [fixture.output, mailbox()]))
      current = Repo.get!(CodexSession, fixture.turn.codex_session_id)
      {:ok, current_binary} = Ecto.UUID.dump(current.id)
      current_integer = :binary.decode_unsigned(current_binary)

      sessions = [
        current
        | Enum.map(1..3, fn offset ->
            {:ok, id} = Ecto.UUID.load(<<current_integer + offset::128>>)
            now = db_clock!()
            Repo.insert!(%CodexSession{id: id, pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, session_key: "nested-mailbox-lock-#{System.unique_integer([:positive, :monotonic])}", status: "active", created_at: now, updated_at: now})
          end)
      ]

      before = counts(fixture)
      alias_query = from a in BridgeSessionAlias, where: a.pool_id == ^fixture.setup.pool.id
      aliases_before = Repo.aggregate(alias_query, :count)
      parent = self()
      ref = make_ref()
      telemetry = "nested-mailbox-lock-#{System.unique_integer([:positive, :monotonic])}"
      on_exit(fn -> :telemetry.detach(telemetry) end)

      :ok =
        :telemetry.attach(
          telemetry,
          [:codex_pooler, :gateway, :duplicate_turn, :refused],
          fn _event, _measurements, _metadata, _config ->
            in_transaction? = Repo.in_transaction?()
            # A real query at publication proves the connection is usable after
            # rollback; an aborted enclosing transaction cannot execute it.
            query_succeeded? =
              try do
                Repo.query!("SELECT 1").rows == [[1]]
              rescue
                _error -> false
              end

            send(parent, {:refusal_transaction, ref, in_transaction?, query_succeeded?})
          end,
          nil
        )

      {{result, admission_backend, blocker_backends}, logs} =
        with_info_log(fn ->
          admission =
            Task.async(fn ->
              Sandbox.unboxed_run(Repo, fn ->
                Process.put({Service, :runtime_authorization_barrier}, {parent, ref, {:reservation_lock, :before}})
                send(parent, {:admission_backend, ref, backend_pid!()})
                send_continuation(fixture, payload, :http)
              end)
            end)

          stop_lock_actor_on_exit(admission)
          assert_receive {:admission_backend, ^ref, admission_backend}, 15_000
          assert_receive {:runtime_authorization_barrier, ^ref, :reservation_lock, :before, admission_pid}, 15_000

          blockers =
            Enum.map(Enum.take(sessions, n), fn session ->
              task =
                Task.async(fn ->
                  Sandbox.unboxed_run(Repo, fn ->
                    Repo.transaction(fn ->
                      Repo.one!(from s in CodexSession, where: s.id == ^session.id, lock: "FOR NO KEY UPDATE")
                      send(parent, {:nested_held, ref, session.id, backend_pid!()})

                      receive do
                        {:append_nested, ^ref, predecessor, next_session} -> insert_nested_chain_edge!(fixture, payload, predecessor, next_session)
                      end
                    end)
                  end)
                end)

              stop_lock_actor_on_exit(task)
              assert_receive {:nested_held, ^ref, id, backend}, 15_000
              assert id == session.id
              refute backend == admission_backend
              %{task: task, backend: backend}
            end)

          send(admission_pid, {:runtime_authorization_release, ref})

          Enum.reduce(Enum.with_index(blockers), fixture.request, fn {blocker, index}, predecessor ->
            query = await_nested_session_wait!(admission_backend, blocker.backend)
            assert query =~ "codex_sessions"

            assert {:ok, :prior_locks_released} =
                     Sandbox.unboxed_run(Repo, fn ->
                       Repo.transaction(fn ->
                         Repo.one!(from r in Request, where: r.id == ^fixture.request.id, lock: "FOR UPDATE NOWAIT")
                         Repo.one!(from k in CodexPooler.Access.APIKey, where: k.id == ^fixture.setup.api_key.id, lock: "FOR UPDATE NOWAIT")
                         :prior_locks_released
                       end)
                     end)

            send(blocker.task.pid, {:append_nested, ref, predecessor, Enum.at(sessions, index + 1)})
            assert {:ok, successor} = Task.await(blocker.task, 15_000)
            successor
          end)

          result = Task.await(admission, 15_000)
          {result, admission_backend, Enum.map(blockers, & &1.backend)}
        end)

      assert_refusal(result, :http)
      assert_receive {:refusal_transaction, ^ref, false, true}, 15_000
      refute_receive {:refusal_transaction, ^ref, _, _}, 50
      assert logs =~ "mailbox_check=session"
      assert logs =~ "stage=native_http_turn_claim"
      assert counts(fixture) == %{before | requests: before.requests + n, attempts: before.attempts + n, turns: before.turns + n, links: before.links + n}
      assert Repo.aggregate(alias_query, :count) == aliases_before
      assert FakeUpstream.count(fixture.upstream) == 1
      if System.get_env("CODEX_POOLER_LOCK_TEST_DIAGNOSTICS") == "true", do: IO.puts(Jason.encode!(%{scenario: "nested_http_chain_insertion", inserted_edges: n, admission_backend: admission_backend, blocker_backends: blocker_backends, refusal_after_outer_transaction: true, public_status: 409, new_aliases: 0, new_reservations: 0, provider_dispatch_count: 1, mailbox_check: "session"}))
    end
  end

  defp fixture!(committed? \\ false) do
    output = reasoning("first")
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_diagnostic", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.sse_stream([{"response.output_item.done", %{"type" => "response.output_item.done", "item" => output}}, {"response.completed", completed}])]))
    setup = gateway_setup(upstream, compact?: true)
    if committed?, do: register_unboxed_pool_cleanup!(setup)
    thread = Ecto.UUID.generate()
    document = CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => document}}
    fixture = %{setup: setup, upstream: upstream, thread: thread, payload: payload, output: output}
    assert send_continuation(fixture, payload, :http).status == 200
    request = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id)
    attempt = Repo.get_by!(Attempt, request_id: request.id)
    turn = Repo.get_by!(CodexTurn, request_id: request.id)
    assert {request.status, attempt.status, turn.status} == {"succeeded", "succeeded", "succeeded"}
    assert attempt.response_metadata["native_http_mailbox_prefix"]["output_item_done_count"] == 1
    Map.merge(fixture, %{request: request, attempt: attempt, turn: turn})
  end

  defp current_mailbox_stage(fixture, payload) do
    witness = mailbox_witness(fixture, payload)
    ClientRetry.mailbox_check(Repo.reload!(fixture.turn), Repo.reload!(fixture.request), Repo.reload!(fixture.attempt), witness, nil, :same_session).stage
  end

  defp mailbox_witness(fixture, payload) do
    "codex-turn:" <> encoded = fixture.request.correlation_id
    semantic = Base.url_decode64!(encoded, padding: false)

    ClientRetry.original_witness!(:crypto.hash(:sha256, "synthetic diagnostic witness"), fixture.request.native_client_retry_auth_epoch)
    |> NativeMailboxContinuation.attach(semantic, payload, RequestOptions.build(%{}, @path, %{}))
  end

  defp insert_successor!(fixture, payload, settled?) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    [candidate] = mailbox_witness(fixture, payload).mailbox
    {:ok, claim} = ClientRetry.deterministic_failed_predecessor_claim(fixture.request.correlation_id, fixture.request.id)
    successor = Repo.insert!(%Request{pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, model_id: fixture.setup.model.id, requested_model: fixture.setup.model.exposed_model_id, endpoint: @path, transport: "http_sse", status: if(settled?, do: "failed", else: "accepted"), completed_at: if(settled?, do: now), last_error_code: if(settled?, do: "client_disconnected"), usage_status: "usage_pending", correlation_id: claim, admitted_at: now, native_client_retry_version: 1, native_client_retry_digest: hd(candidate.ending.websocket), native_client_retry_auth_epoch: fixture.request.native_client_retry_auth_epoch, request_metadata: %{"native_http_claim_arm" => "opening", "client_resend" => %{"predecessor_request_id" => fixture.request.id}}})
    ClientRetry.insert_link!(fixture.request, successor, now)
  end

  defp insert_entitlement!(fixture) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    "codex-turn:" <> encoded = fixture.request.correlation_id
    semantic = Base.url_decode64!(encoded, padding: false)
    turn = Repo.update!(Ecto.Changeset.change(fixture.turn, semantic_turn_digest: semantic))

    %RequestReplayEntitlement{}
    |> RequestReplayEntitlement.changeset(%{request_id: fixture.request.id, codex_turn_id: turn.id, eligible_attempt_id: fixture.attempt.id, api_key_id: fixture.setup.api_key.id, api_key_runtime_epoch: fixture.request.native_client_retry_auth_epoch, pool_id: fixture.setup.pool.id, model_id: fixture.setup.model.id, model_identifier: fixture.setup.model.exposed_model_id, semantic_turn_digest: semantic, replay_claim_digest: :crypto.hash(:sha256, "synthetic entitlement"), replay_generation: 1, owner_lease_digest: <<1::256>>, owner_lease_key_version: "test-v1", predecessor_epoch: 1, status: "armed", armed_at: now, expires_at: DateTime.add(now, 30, :second)})
    |> Repo.insert!()
  end

  defp backend_pid! do
    %{rows: [[pid]]} = Repo.query!("SELECT pg_backend_pid()", [])
    pid
  end

  defp stage_fixture!(fixture, stage) do
    payload = Map.update!(fixture.payload, "input", &(&1 ++ [fixture.output, mailbox()]))

    case stage do
      :no_candidate ->
        update_in(payload["input"], &List.replace_at(&1, -1, Map.put(mailbox(), "recipient", "/root/other")))

      :settlement ->
        Repo.update!(Ecto.Changeset.change(fixture.attempt, replay_generation: 1))
        payload

      :authorization ->
        Repo.update!(Ecto.Changeset.change(fixture.request, native_client_retry_auth_epoch: fixture.request.native_client_retry_auth_epoch + 1))
        payload

      :session ->
        original = Repo.get!(CodexSession, fixture.turn.codex_session_id)
        other = Repo.insert!(%CodexSession{pool_id: original.pool_id, api_key_id: original.api_key_id, session_key: Ecto.UUID.generate(), status: "active", created_at: original.created_at, updated_at: original.updated_at})
        Repo.update!(Ecto.Changeset.change(fixture.turn, codex_session_id: other.id))
        payload

      :witness ->
        Map.put(payload, "instructions", "synthetic changed instructions")

      :ending ->
        Map.update!(payload, "input", &(&1 ++ [reasoning("later")]))

      :output_prefix ->
        update_in(payload["input"], &List.replace_at(&1, -2, reasoning("changed")))

      :verified ->
        Repo.update!(Ecto.Changeset.change(fixture.request, completed_at: DateTime.add(DateTime.utc_now(), -31, :second)))
        payload
    end
  end

  defp send_continuation(fixture, payload, :http) do
    build_conn()
    |> auth(fixture.setup)
    |> put_req_header("session-id", fixture.thread)
    |> put_req_header("thread-id", fixture.thread)
    |> put_req_header("x-codex-window-id", "#{fixture.thread}:0")
    |> put_req_header("x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"])
    |> post(@path, payload)
  end

  defp send_continuation(fixture, payload, :websocket) do
    port = start_public_endpoint!()
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, fixture.setup, fixture.thread, @path, [{"session-id", fixture.thread}, {"thread-id", fixture.thread}, {"x-codex-window-id", "#{fixture.thread}:0"}])
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(Map.put(payload, "type", "response.create")))
    {conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
    Mint.HTTP.close(conn)
    CodexPooler.JSON.decode!(frame)
  end

  defp assert_refusal(conn, :http) do
    assert json_response(conn, 409) == %{"error" => %{"code" => "duplicate_turn", "message" => "duplicate Codex turn was already recorded for this session", "param" => "request_id", "type" => "invalid_request_error"}}
  end

  defp assert_refusal(frame, :websocket) do
    assert frame["type"] == "error"
    assert frame["error"] == %{"code" => "duplicate_turn", "message" => "duplicate Codex turn was already recorded for this session", "param" => "request_id", "type" => "invalid_request_error"}
    refute Map.has_key?(frame, "mailbox_check")
  end

  defp insert_nested_chain_edge!(fixture, payload, predecessor, session) do
    now = db_clock!()
    [candidate] = mailbox_witness(fixture, payload).mailbox
    {:ok, claim} = ClientRetry.deterministic_failed_predecessor_claim(predecessor.correlation_id, predecessor.id)
    request = Repo.insert!(%Request{pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, model_id: fixture.setup.model.id, requested_model: fixture.setup.model.exposed_model_id, endpoint: @path, transport: "http_sse", status: "failed", completed_at: now, last_error_code: "client_disconnected", usage_status: "usage_unknown", correlation_id: claim, admitted_at: now, native_client_retry_version: 1, native_client_retry_digest: hd(candidate.ending.websocket), native_client_retry_auth_epoch: fixture.request.native_client_retry_auth_epoch, request_metadata: %{"native_http_claim_arm" => "opening", "client_resend" => %{"predecessor_request_id" => predecessor.id}}})
    %Attempt{} = original_attempt = fixture.attempt
    attempt = Repo.insert!(%Attempt{original_attempt | id: nil, request_id: request.id, status: "failed", completed_at: now, network_error_code: "client_disconnected"})
    Repo.insert!(%CodexTurn{codex_session_id: session.id, request_id: request.id, turn_sequence: 1, transport_kind: "http_sse", semantic_turn_digest: fixture.turn.semantic_turn_digest, status: "interrupted", error_code: "client_disconnected", final_attempt_id: attempt.id, started_at: now, completed_at: now, created_at: now, updated_at: now})
    ClientRetry.insert_link!(predecessor, request, now)
    request
  end

  defp await_nested_session_wait!(waiter, blocker), do: await_nested_session_wait!(waiter, blocker, System.monotonic_time(:millisecond) + 15_000)

  defp await_nested_session_wait!(waiter, blocker, deadline) do
    rows = Sandbox.unboxed_run(Repo, fn -> Repo.query!("SELECT query FROM pg_stat_activity WHERE pid=$1 AND $2=ANY(pg_blocking_pids(pid))", [waiter, blocker]).rows end)

    case rows do
      [[query]] ->
        query

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "nested admission never reached sorted session locks"
        Process.sleep(20)
        await_nested_session_wait!(waiter, blocker, deadline)
    end
  end

  defp db_clock!, do: Repo.query!("SELECT clock_timestamp()").rows |> hd() |> hd()
  defp stop_lock_actor_on_exit(task), do: on_exit(fn -> if Process.alive?(task.pid), do: Process.exit(task.pid, :kill) end)

  defp counts(fixture) do
    requests = from r in Request, where: r.pool_id == ^fixture.setup.pool.id, select: r.id
    %{requests: Repo.aggregate(requests, :count), attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in subquery(requests)), :count), turns: Repo.aggregate(from(t in CodexTurn, where: t.request_id in subquery(requests)), :count), links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in subquery(requests)), :count), ledger: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in subquery(requests)), :count)}
  end

  defp attach_refusal_telemetry! do
    owner = self()
    id = "mailbox-refusal-#{System.unique_integer([:positive])}"
    on_exit(fn -> :telemetry.detach(id) end)
    :ok = :telemetry.attach(id, [:codex_pooler, :gateway, :duplicate_turn, :refused], fn _event, measurements, metadata, _config -> send(owner, {id, measurements, metadata}) end, nil)
    id
  end

  defp reasoning(id), do: %{"type" => "reasoning", "id" => "rs_" <> id, "summary" => [], "encrypted_content" => "synthetic_reasoning_" <> id}
  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
end
