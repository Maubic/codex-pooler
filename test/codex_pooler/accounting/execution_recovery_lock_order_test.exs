defmodule CodexPooler.Accounting.ExecutionRecoveryLockOrderTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{ClientRetry, Request, RequestClientRetryLink, RequestLifecycle}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.UnboxedFixture
  alias Ecto.Adapters.SQL.Sandbox

  @endpoint "/backend-api/codex/responses"
  @timeout_ms 15_000

  test "new-session execution recovery waits for the original session before locking its request" do
    fixture = committed_fixture!()
    attempt = dead_attempt!(fixture)
    CodexPooler.ExecutionProofSupport.publish_committed_terminal!(attempt)

    turn =
      UnboxedFixture.run_unboxed(fn ->
        now = db_now()

        Repo.insert!(%CodexTurn{
          codex_session_id: fixture.original_session.id,
          request_id: fixture.request.id,
          turn_sequence: 1,
          transport_kind: "websocket",
          semantic_turn_digest: fixture.digest,
          status: "in_progress",
          final_attempt_id: attempt.id,
          first_visible_output_at: now,
          started_at: now,
          created_at: now,
          updated_at: now
        })
      end)

    parent = self()

    blocker =
      start_supervised!(
        {Task,
         fn ->
           result =
             Sandbox.unboxed_run(Repo, fn ->
               safely(fn ->
                 Repo.transaction(fn ->
                   backend = backend_pid!()
                   Repo.one!(from session in CodexSession, where: session.id == ^fixture.original_session.id, lock: "FOR UPDATE")
                   send(parent, {:original_session_locked, backend})

                   receive do
                     :recover ->
                       # This must succeed immediately. Before the fix the reconnect
                       # owned this row while waiting on the session we already own.
                       Repo.one!(from request in Request, where: request.id == ^fixture.request.id, lock: "FOR UPDATE NOWAIT")
                       send(parent, :predecessor_request_lock_available)
                       RequestLifecycle.recover_dead_execution(fixture.request, attempt, db_now())
                   end
                 end)
               end)
             end)

           send(parent, {:blocker_result, result})
         end},
        id: :original_session_cleanup
      )

    blocker_monitor = Process.monitor(blocker)
    assert_receive {:original_session_locked, blocker_backend}, @timeout_ms

    reconnect =
      start_supervised!(
        {Task,
         fn ->
           result =
             Sandbox.unboxed_run(Repo, fn ->
               Repo.checkout(fn ->
                 send(parent, {:reconnect_backend, backend_pid!()})
                 safely(fn -> Accounting.claim_websocket_turn(fixture.auth, fixture.model, fixture.retry_opts) end)
               end)
             end)

           send(parent, {:reconnect_result, result})
         end},
        id: :new_session_reconnect
      )

    reconnect_monitor = Process.monitor(reconnect)
    assert_receive {:reconnect_backend, reconnect_backend}, @timeout_ms
    refute reconnect_backend == blocker_backend

    blocked_relation = await_blocked_relation!(reconnect_backend, blocker_backend, System.monotonic_time(:millisecond) + @timeout_ms)
    assert blocked_relation == "codex_sessions"
    send(blocker, :recover)
    assert_receive :predecessor_request_lock_available, @timeout_ms
    assert_receive {:blocker_result, {:ok, {:ok, {:ok, :recovered}}}}, @timeout_ms
    assert_receive {:reconnect_result, {:ok, {:ok, %{request: successor}}}}, @timeout_ms
    assert_receive {:DOWN, ^blocker_monitor, :process, ^blocker, :normal}, @timeout_ms
    assert_receive {:DOWN, ^reconnect_monitor, :process, ^reconnect, :normal}, @timeout_ms

    CodexPooler.TestDiagnostics.puts("execution recovery lock order: distinct PostgreSQL backends=#{blocker_backend},#{reconnect_backend}; pg_blocking_pids confirmed reconnect waiting on #{blocked_relation}; cleanup request NOWAIT succeeded; cleanup and reconnect committed")

    UnboxedFixture.run_unboxed(fn ->
      assert Repo.reload!(fixture.request).last_error_code == "dead_execution_recovered"
      assert Repo.reload!(attempt).status == "failed"
      assert Repo.reload!(turn).status == "interrupted"
      assert successor.id != fixture.request.id
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from link in RequestClientRetryLink, where: link.predecessor_request_id == ^fixture.request.id)
      assert successor_id == successor.id
      assert fixture.request.id |> Accounting.list_ledger_entries_for_request() |> Enum.map(& &1.entry_kind) |> Enum.sort() == ["release", "reservation", "settlement"]
      assert Repo.aggregate(from(request in Request, where: request.pool_id == ^fixture.auth.pool.id), :count) == 2
    end)
  end

  defp committed_fixture! do
    %{user: owner} = committed_bootstrap_owner_fixture!()

    UnboxedFixture.run_unboxed(fn ->
      pool = pool_fixture(%{created_by_user_id: owner.id})
      %{api_key: key} = active_api_key_fixture(pool, %{created_by_user_id: owner.id})
      model = model_fixture(pool)
      %{assignment: assignment} = upstream_assignment_fixture(pool)
      auth = %{pool: pool, api_key: key}
      original_session = session!(pool, key, assignment)
      new_session = session!(pool, key, assignment)
      witness = ClientRetry.original_witness!(:crypto.strong_rand_bytes(32), key.runtime_revocation_epoch)
      digest = :crypto.strong_rand_bytes(32)
      claim = "codex-request:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
      opts = %{endpoint: @endpoint, correlation_id: claim, codex_session: original_session, native_client_retry_witness: witness}
      {:ok, %{request: claimed}} = Accounting.claim_websocket_turn(auth, model, opts)
      {:ok, %{request: request}} = Accounting.reserve(auth, model, %{"model" => model.exposed_model_id, "input" => []}, %{endpoint: @endpoint, transport: "websocket", correlation_id: claim, turn_claim: claimed})
      retry_opts = Map.merge(opts, %{codex_session: new_session, semantic_turn_digest: digest, correlation_id: "codex-request:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false), anchor_present?: false})
      %{auth: auth, model: model, assignment: assignment, original_session: original_session, request: request, digest: digest, retry_opts: retry_opts}
    end)
  end

  defp dead_attempt!(fixture) do
    parent = self()

    owner =
      start_supervised!(
        {Task,
         fn ->
           {:ok, attempt} = Sandbox.unboxed_run(Repo, fn -> Accounting.create_attempt(fixture.request, fixture.assignment) end)
           send(parent, {:attempt_created, attempt})

           receive do
             :stop -> :ok
           end
         end},
        id: :execution_owner
      )

    monitor = Process.monitor(owner)
    assert_receive {:attempt_created, attempt}, @timeout_ms
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, @timeout_ms
    attempt
  end

  defp session!(pool, key, assignment) do
    now = db_now()
    Repo.insert!(%CodexSession{pool_id: pool.id, api_key_id: key.id, session_key: "execution-lock-#{Ecto.UUID.generate()}", pool_upstream_assignment_id: assignment.id, status: "active", created_at: now, updated_at: now})
  end

  defp await_blocked_relation!(waiter, blocker, deadline) do
    rows =
      UnboxedFixture.run_unboxed(fn ->
        Repo.query!("SELECT query FROM pg_stat_activity WHERE pid = $1 AND $2 = ANY(pg_blocking_pids(pid))", [waiter, blocker]).rows
      end)

    case rows do
      [[query]] ->
        case Regex.run(~r/FROM "(\w+)"/, query) do
          [_, relation] -> relation
          _ -> poll_blocked_relation!(waiter, blocker, deadline)
        end

      _ ->
        poll_blocked_relation!(waiter, blocker, deadline)
    end
  end

  defp poll_blocked_relation!(waiter, blocker, deadline) do
    assert System.monotonic_time(:millisecond) < deadline, "reconnect never waited on original session holder"

    receive do
    after
      5 -> await_blocked_relation!(waiter, blocker, deadline)
    end
  end

  defp db_now do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()", [])
    now
  end

  defp backend_pid! do
    %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()", [])
    backend
  end

  defp safely(fun) do
    {:ok, fun.()}
  rescue
    error in Postgrex.Error -> {:error, error.postgres.code}
  end
end
