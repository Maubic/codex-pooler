defmodule CodexPooler.Accounting.MailboxAdmissionChainLocksTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{ClientRetry, Request, RequestClientRetryLink}
  alias CodexPooler.Accounting.RequestLifecycle.Reservation
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeSessionAlias, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Gateway.Transports.Streaming.WebsocketCodec
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @endpoint "/backend-api/codex/responses"
  @barrier {Reservation, :runtime_authorization_barrier}

  for inserted_edges <- [1, 2, 3] do
    @tag mailbox_admission_lock_order: true
    @tag mailbox_lock_rediscovery: true
    test "actual websocket claim rediscovery after #{inserted_edges} committed historical edges" do
      n = unquote(inserted_edges)
      fixture = committed_fixture()
      parent = self()
      ref = make_ref()

      claimant =
        Task.async(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            Process.put(@barrier, {parent, ref, {:claim, :before}})
            send(parent, {:claim_backend, ref, backend_pid()})
            result = Accounting.claim_websocket_turn(fixture.auth, fixture.model, fixture.opts)
            send(parent, {:claim_finished, ref, result})
            result
          end)
        end)

      stop_on_exit(claimant)
      assert_receive {:claim_backend, ^ref, claim_backend}, @budget

      # Let the fresh claim encounter the original correlation constraint;
      # the second signal is the actual failed-predecessor resend entry.
      assert_receive {:runtime_authorization_barrier, ^ref, :claim, :before, claim_pid}, @budget
      send(claim_pid, {:runtime_authorization_release, ref})
      assert_receive {:runtime_authorization_barrier, ^ref, :claim, :before, ^claim_pid}, @budget

      blockers =
        Enum.map(Enum.take(fixture.sessions, n), fn session ->
          task =
            Task.async(fn ->
              Sandbox.unboxed_run(Repo, fn ->
                Repo.transaction(fn ->
                  Repo.one!(from s in CodexSession, where: s.id == ^session.id, lock: "FOR NO KEY UPDATE")
                  send(parent, {:held, ref, session.id, backend_pid()})

                  receive do
                    {:append, ^ref, predecessor, next_session} -> append_edge(fixture, predecessor, next_session)
                  end
                end)
              end)
            end)

          stop_on_exit(task)
          assert_receive {:held, ^ref, session_id, blocker_backend}, @budget
          assert session_id == session.id
          refute blocker_backend == claim_backend
          %{task: task, backend: blocker_backend}
        end)

      assert blockers |> Enum.map(& &1.backend) |> Enum.uniq() |> length() == n

      send(claim_pid, {:runtime_authorization_release, ref})

      {tail, observations} =
        Enum.reduce(Enum.with_index(blockers), {fixture.original, []}, fn {blocker, index}, {predecessor, observations} ->
          observation = await_block(claim_backend, blocker.backend)
          assert observation.query =~ "codex_sessions"
          assert_previous_admission_locks_released(fixture)
          next_session = Enum.at(fixture.sessions, index + 1)
          send(blocker.task.pid, {:append, ref, predecessor, next_session})
          assert {:ok, next} = Task.await(blocker.task, @budget)
          {next, [Map.drop(observation, [:query]) | observations]}
        end)

      assert_receive {:claim_finished, ^ref, result}, @budget
      assert Task.await(claimant, @budget) == result
      starts = Enum.map(observations, & &1.transaction_start)
      assert length(Enum.uniq(starts)) == n

      Sandbox.unboxed_run(Repo, fn ->
        persisted = Repo.all(from r in Request, where: r.pool_id == ^fixture.auth.pool.id)
        expected = if n == 3, do: n + 1, else: n + 2
        assert length(persisted) == expected
        refute Repo.exists?(from a in BridgeSessionAlias, where: a.pool_id == ^fixture.auth.pool.id)
        assert Repo.aggregate(from(l in RequestClientRetryLink, join: r in Request, on: r.id == l.predecessor_request_id, where: r.pool_id == ^fixture.auth.pool.id), :count) == n

        case n do
          3 ->
            assert {:error, %{code: :duplicate_request, mailbox_check: :session}} = result
            refute Enum.any?(persisted, &(&1.status == "accepted"))

          _green ->
            assert {:ok, %{request: request, client_resend: %{predecessor_request_id: predecessor_id}}} = result
            assert predecessor_id == tail.id
            assert Enum.count(persisted, &(&1.status == "accepted")) == 1
            assert request.id != tail.id
        end

        emit_receipt(%{scenario: "actual_claim_chain_insertion", inserted_edges: n, admission_backend: claim_backend, lock_waits: Enum.reverse(observations), request_count: length(persisted), expected_request_count: expected, alias_count: 0, exhaustion: n == 3})
      end)
    end
  end

  @tag mailbox_admission_lock_order: true
  @tag mailbox_lock_rediscovery: true
  @tag mailbox_fresh_exhaustion: true
  test "fresh claim exhaustion does not start another failed-predecessor retry budget" do
    fixture = committed_fixture()
    parent = self()
    ref = make_ref()

    blockers =
      Enum.map(Enum.take(fixture.sessions, 3), fn session ->
        task =
          Task.async(fn ->
            Sandbox.unboxed_run(Repo, fn ->
              Repo.transaction(fn ->
                Repo.one!(from s in CodexSession, where: s.id == ^session.id, lock: "FOR NO KEY UPDATE")
                send(parent, {:fresh_held, ref, session.id, backend_pid()})

                receive do
                  {:fresh_append, ^ref, predecessor, next_session} -> append_edge(fixture, predecessor, next_session)
                end
              end)
            end)
          end)

        stop_on_exit(task)
        assert_receive {:fresh_held, ^ref, id, backend}, @budget
        assert id == session.id
        %{task: task, backend: backend}
      end)

    actor =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          send(parent, {:fresh_actor, ref, backend_pid()})
          Accounting.claim_websocket_turn(fixture.auth, fixture.model, fixture.opts)
        end)
      end)

    stop_on_exit(actor)
    assert_receive {:fresh_actor, ^ref, actor_backend}, @budget

    waits =
      Enum.reduce(Enum.with_index(blockers), {fixture.original, []}, fn {blocker, index}, {predecessor, observations} ->
        observation = await_block(actor_backend, blocker.backend)
        assert observation.query =~ "codex_sessions"
        send(blocker.task.pid, {:fresh_append, ref, predecessor, Enum.at(fixture.sessions, index + 1)})
        assert {:ok, successor} = Task.await(blocker.task, @budget)
        {successor, [Map.drop(observation, [:query]) | observations]}
      end)
      |> elem(1)

    assert {:error, %{code: :duplicate_request, mailbox_check: :session, resend_disposition: :chain_exhausted}} = Task.await(actor, @budget)

    Sandbox.unboxed_run(Repo, fn ->
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^fixture.auth.pool.id), :count) == 4
      refute Repo.exists?(from r in Request, where: r.pool_id == ^fixture.auth.pool.id and r.status == "accepted")
    end)

    emit_receipt(%{scenario: "fresh_claim_exhaustion", admission_backend: actor_backend, lock_waits: Enum.reverse(waits), transactions: 3, accepted_requests: 0, second_retry_budget_started: false})
  end

  for entry <- [:retry_successor, :owner_preflight], inserted_edges <- [1, 2, 3] do
    @tag mailbox_admission_lock_order: true
    @tag mailbox_lock_rediscovery: true
    test "#{entry} exits before #{inserted_edges} graph rediscoveries" do
      entry = unquote(entry)
      n = unquote(inserted_edges)
      fixture = committed_fixture()
      {fixture, prepared, opts} = retry_entry_fixture(fixture)
      parent = self()
      ref = make_ref()

      blockers =
        Enum.map(Enum.take(fixture.sessions, n), fn session ->
          task =
            Task.async(fn ->
              Sandbox.unboxed_run(Repo, fn ->
                Repo.transaction(fn ->
                  Repo.one!(from s in CodexSession, where: s.id == ^session.id, lock: "FOR NO KEY UPDATE")
                  send(parent, {:entry_held, ref, session.id, backend_pid()})

                  receive do
                    {:append_entry, ^ref, predecessor, next_session} -> append_client_retry_edge(fixture, predecessor, next_session)
                  end
                end)
              end)
            end)

          stop_on_exit(task)
          assert_receive {:entry_held, ^ref, id, backend}, @budget
          assert id == session.id
          %{task: task, backend: backend}
        end)

      actor =
        Task.async(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            send(parent, {:entry_backend, ref, backend_pid()})
            execute_retry_entry(entry, fixture, prepared, opts)
          end)
        end)

      stop_on_exit(actor)
      assert_receive {:entry_backend, ^ref, actor_backend}, @budget

      observations =
        Enum.reduce(Enum.with_index(blockers), {fixture.original, []}, fn {blocker, index}, {predecessor, observations} ->
          refute actor_backend == blocker.backend
          observation = await_block(actor_backend, blocker.backend)
          assert observation.query =~ "codex_sessions"
          assert_previous_admission_locks_released(fixture)
          send(blocker.task.pid, {:append_entry, ref, predecessor, Enum.at(fixture.sessions, index + 1)})
          assert {:ok, successor} = Task.await(blocker.task, @budget)
          {successor, [Map.drop(observation, [:query]) | observations]}
        end)
        |> elem(1)

      result = Task.await(actor, @budget)
      assert_retry_entry_result(entry, n, result)

      Sandbox.unboxed_run(Repo, fn ->
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^fixture.auth.pool.id), :count) == n + 1
        refute Repo.exists?(from a in BridgeSessionAlias, where: a.pool_id == ^fixture.auth.pool.id)
        refute Repo.exists?(from l in CodexPooler.Accounting.LedgerEntry, join: r in Request, on: r.id == l.request_id, where: r.pool_id == ^fixture.auth.pool.id)
      end)

      emit_receipt(%{scenario: entry, inserted_edges: n, admission_backend: actor_backend, lock_waits: Enum.reverse(observations), new_reservations: 0, new_aliases: 0})
    end
  end

  @tag mailbox_admission_lock_order: true
  test "a foreign claim holder retains authorization_changed without locking its session" do
    fixture = committed_fixture()

    foreign =
      Sandbox.unboxed_run(Repo, fn ->
        pool = pool_fixture(%{created_by_user_id: fixture.owner.id})
        %{api_key: key} = active_api_key_fixture(pool, %{created_by_user_id: fixture.owner.id})
        model = model_fixture(pool)
        session = insert_session(%{pool: pool, api_key: key}, Ecto.UUID.generate())
        claim = "codex-request:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
        request = request_fixture(%{pool: pool, api_key: key}, %{model_id: model.id, correlation_id: claim, transport: "websocket"})
        Repo.insert!(%CodexTurn{codex_session_id: session.id, request_id: request.id, turn_sequence: 1, status: "succeeded", transport_kind: "websocket", created_at: db_now(), updated_at: db_now()})
        %{session: session, claim: claim}
      end)

    parent = self()
    ref = make_ref()

    holder =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            Repo.one!(from s in CodexSession, where: s.id == ^foreign.session.id, lock: "FOR UPDATE")
            send(parent, {:foreign_held, ref})
            receive do: ({:release, ^ref} -> :ok)
          end)
        end)
      end)

    stop_on_exit(holder)
    assert_receive {:foreign_held, ^ref}, @budget

    claimant =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          opts = %{fixture.opts | correlation_id: foreign.claim}
          Accounting.claim_websocket_turn(fixture.auth, fixture.model, opts)
        end)
      end)

    stop_on_exit(claimant)
    assert {:error, %{resend_disposition: :authorization_changed}} = Task.await(claimant, @budget)
    # Admission completed while the foreign session is still locked.
    send(holder.pid, {:release, ref})
    assert {:ok, :ok} = Task.await(holder, @budget)
  end

  @tag mailbox_admission_lock_order: true
  @tag mailbox_lock_rediscovery: true
  @tag mailbox_execution_guard: true
  test "execution recovery revalidates the session after waiting for its turn lock" do
    fixture = committed_fixture()
    parent = self()
    ref = make_ref()
    target = Enum.at(fixture.sessions, 1)
    turn = Sandbox.unboxed_run(Repo, fn -> Repo.get_by!(CodexTurn, request_id: fixture.original.id) end)

    holder =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            Repo.one!(from t in CodexTurn, where: t.id == ^turn.id, lock: "FOR UPDATE")
            send(parent, {:execution_turn_held, ref, backend_pid()})

            receive do
              {:release, ^ref} ->
                Repo.update_all(from(t in CodexTurn, where: t.id == ^turn.id), set: [codex_session_id: target.id])
                :updated
            end
          end)
        end)
      end)

    stop_on_exit(holder)
    assert_receive {:execution_turn_held, ^ref, turn_backend}, @budget

    session_holder =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            Repo.one!(from s in CodexSession, where: s.id == ^target.id, lock: "FOR NO KEY UPDATE")
            send(parent, {:execution_session_held, ref, backend_pid()})
            receive do: ({:release, ^ref} -> :ok)
          end)
        end)
      end)

    stop_on_exit(session_holder)
    assert_receive {:execution_session_held, ^ref, session_backend}, @budget

    actor =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          send(parent, {:execution_actor, ref, backend_pid()})
          result = Accounting.claim_websocket_turn(fixture.auth, fixture.model, Map.merge(fixture.opts, %{semantic_turn_digest: turn.semantic_turn_digest, execution_recovery_request_id: fixture.original.id}))
          send(parent, {:execution_result, ref, result})
          result
        end)
      end)

    stop_on_exit(actor)
    assert_receive {:execution_actor, ^ref, actor_backend}, @budget
    turn_wait = await_block(actor_backend, turn_backend)
    assert turn_wait.query =~ "codex_turns"
    send(holder.pid, {:release, ref})
    assert {:ok, :updated} = Task.await(holder, @budget)
    session_wait = await_execution_session_wait(actor_backend, session_backend, ref, System.monotonic_time(:millisecond) + @budget)
    assert session_wait =~ "codex_sessions"
    assert_previous_admission_locks_released(fixture)
    send(session_holder.pid, {:release, ref})
    assert {:ok, :ok} = Task.await(session_holder, @budget)
    assert {:error, %{resend_disposition: :terminal_predecessor}} = Task.await(actor, @budget)
    emit_receipt(%{scenario: "execution_turn_membership_after_lock", admission_backend: actor_backend, turn_blocker_backend: turn_backend, session_blocker_backend: session_backend, rediscovered_owned_session: true, disposition: "terminal_predecessor"})
  end

  @tag mailbox_admission_lock_order: true
  @tag mailbox_current_root_discovery: true
  test "a newer original elsewhere cannot hide the current session's actual retry root" do
    fixture = committed_fixture()
    {fixture, _prepared, opts} = retry_entry_fixture(fixture)

    Sandbox.unboxed_run(Repo, fn ->
      append_client_retry_edge(fixture, fixture.original, Enum.at(fixture.sessions, 1))
      newer = failed_request(fixture, Enum.at(fixture.sessions, 2), Ecto.UUID.generate(), 1)
      Repo.update_all(from(t in CodexTurn, where: t.request_id == ^newer.id), set: [semantic_turn_digest: fixture.semantic_digest])
      opts = Map.delete(opts, :original_request_claim)
      ids = Reservation.mailbox_admission_session_ids(fixture.auth, fixture.model, opts)
      assert Enum.at(fixture.sessions, 1).id in ids
      assert Enum.at(fixture.sessions, 2).id in ids
      assert {:error, :successor_claimed} = Accounting.claim_client_retry_successor(fixture.auth, fixture.model, %{"model" => fixture.model.exposed_model_id, "input" => []}, opts)
    end)
  end

  @tag mailbox_admission_lock_order: true
  @tag mailbox_lock_rediscovery: true
  test "owner rediscovery refreshes policy reads without upgrading the captured key epoch" do
    fixture = committed_fixture()
    {fixture, prepared, _opts} = retry_entry_fixture(fixture)
    parent = self()
    ref = make_ref()

    holder =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            Repo.one!(from s in CodexSession, where: s.id == ^fixture.session.id, lock: "FOR NO KEY UPDATE")
            send(parent, {:epoch_held, ref, backend_pid()})

            receive do
              {:release, ^ref} ->
                append_client_retry_edge(fixture, fixture.original, Enum.at(fixture.sessions, 1))
                Repo.update_all(from(k in CodexPooler.Access.APIKey, where: k.id == ^fixture.auth.api_key.id), inc: [runtime_revocation_epoch: 1])
                :updated
            end
          end)
        end)
      end)

    stop_on_exit(holder)
    assert_receive {:epoch_held, ^ref, holder_backend}, @budget

    actor =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          send(parent, {:epoch_actor, ref, backend_pid()})
          Service.prepare_replay_intent(fixture.auth, prepared)
        end)
      end)

    stop_on_exit(actor)
    assert_receive {:epoch_actor, ^ref, actor_backend}, @budget
    observation = await_block(actor_backend, holder_backend)
    assert observation.query =~ "codex_sessions"
    send(holder.pid, {:release, ref})
    assert {:ok, :updated} = Task.await(holder, @budget)
    assert {:error, %{code: :api_key_runtime_epoch_stale, disabling_epoch: 1}} = Task.await(actor, @budget)
    assert prepared.request_options.runtime.api_key_runtime_epoch == 0

    Sandbox.unboxed_run(Repo, fn ->
      assert Repo.get!(CodexPooler.Access.APIKey, fixture.auth.api_key.id).runtime_revocation_epoch == 1
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^fixture.auth.pool.id), :count) == 2
    end)

    emit_receipt(%{scenario: "captured_epoch_after_rediscovery", admission_backend: actor_backend, blocker_backend: holder_backend, captured_epoch: 0, persisted_epoch: 1, disposition: "api_key_runtime_epoch_stale"})
  end

  @tag mailbox_admission_lock_order: true
  @tag mailbox_final_attempt_clock: true
  @tag slow: "crosses the actual retry deadline while the final attempt row is locked"
  test "the retry clock is sampled again after the final attempt lock wait" do
    fixture = committed_fixture()
    parent = self()
    ref = make_ref()

    deadline =
      Sandbox.unboxed_run(Repo, fn ->
        expires = DateTime.add(db_now(), 1_000, :millisecond)
        completed = DateTime.add(expires, -30, :second)
        Repo.update_all(from(r in Request, where: r.id == ^fixture.original.id), set: [completed_at: completed])
        Repo.update_all(from(a in CodexPooler.Accounting.Attempt, where: a.request_id == ^fixture.original.id), set: [completed_at: completed])
        Repo.update_all(from(t in CodexTurn, where: t.request_id == ^fixture.original.id), set: [completed_at: completed])
        expires
      end)

    holder =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            Repo.one!(from a in CodexPooler.Accounting.Attempt, where: a.request_id == ^fixture.original.id, lock: "FOR UPDATE")
            send(parent, {:attempt_held, ref, backend_pid()})
            receive do: ({:release, ^ref} -> :ok)
          end)
        end)
      end)

    stop_on_exit(holder)
    assert_receive {:attempt_held, ^ref, holder_backend}, @budget

    handler = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler) end)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _, _, metadata, _ ->
          if Process.get({__MODULE__, :clock_actor}) == ref and metadata.query == "SELECT clock_timestamp()" do
            {:ok, %{rows: [[sample]]}} = metadata.result
            send(parent, {:admission_clock, ref, sample})
          end
        end,
        nil
      )

    actor =
      Task.async(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Process.put({__MODULE__, :clock_actor}, ref)
          send(parent, {:clock_backend, ref, backend_pid()})
          Accounting.claim_websocket_turn(fixture.auth, fixture.model, fixture.opts)
        end)
      end)

    stop_on_exit(actor)
    assert_receive {:clock_backend, ^ref, actor_backend}, @budget
    assert_receive {:admission_clock, ^ref, before_wait_clock}, @budget
    assert DateTime.compare(before_wait_clock, deadline) == :lt
    observation = await_block(actor_backend, holder_backend)
    assert observation.query =~ "attempts"
    wait_for_database_deadline(deadline)
    send(holder.pid, {:release, ref})
    assert {:ok, :ok} = Task.await(holder, @budget)
    assert {:error, %{resend_disposition: :retry_expired}} = Task.await(actor, @budget)
    assert_receive {:admission_clock, ^ref, after_wait_clock}, @budget
    assert DateTime.compare(after_wait_clock, deadline) != :lt
    emit_receipt(%{scenario: "fresh_final_attempt_clock", admission_backend: actor_backend, blocker_backend: holder_backend, before_wait_clock: before_wait_clock, after_wait_clock: after_wait_clock, deadline: deadline, disposition: "retry_expired"})
  end

  @tag mailbox_admission_lock_order: true
  test "incoming historical roots are discovered without semantic digest and wrong claims are excluded" do
    fixture = committed_fixture()

    Sandbox.unboxed_run(Repo, fn ->
      next = append_edge(fixture, fixture.original, Enum.at(fixture.sessions, 1))
      newest = append_edge(fixture, next, Enum.at(fixture.sessions, 2))
      opts = %{fixture.opts | correlation_id: newest.correlation_id}
      expected = Enum.take(fixture.sessions, 3) |> Enum.map(& &1.id) |> Enum.sort()
      assert Reservation.mailbox_admission_session_ids(fixture.auth, fixture.model, opts) |> Enum.uniq() |> Enum.sort() == expected

      Repo.update_all(from(r in Request, where: r.id == ^newest.id), set: [correlation_id: "client-retry-v1:wrong-owned-claim"])
      opts = %{fixture.opts | correlation_id: "client-retry-v1:wrong-owned-claim"}
      ids = Reservation.mailbox_admission_session_ids(fixture.auth, fixture.model, opts)
      refute Enum.at(fixture.sessions, 1).id in ids
      assert fixture.session.id in ids
      assert {:error, %{code: :duplicate_request}} = Accounting.claim_websocket_turn(fixture.auth, fixture.model, opts)
    end)
  end

  @tag mailbox_admission_query_cost: true
  test "sixteen failed-claim edges discover every session with bounded reads" do
    fixture = committed_fixture()

    Sandbox.unboxed_run(Repo, fn ->
      sessions = Enum.map(1..16, fn _ -> insert_session(fixture.auth, Ecto.UUID.generate()) end)
      tail = Enum.reduce(sessions, fixture.original, &append_edge(fixture, &2, &1))
      opts = %{fixture.opts | correlation_id: tail.correlation_id}
      expected = MapSet.new([fixture.session.id | Enum.map(sessions, & &1.id)])
      counter = :atomics.new(1, [])
      handler = {__MODULE__, :discovery_reads, make_ref()}
      actor = self()
      on_exit(fn -> :telemetry.detach(handler) end)

      :ok =
        :telemetry.attach(
          handler,
          [:codex_pooler, :repo, :query],
          fn _event, _measurements, metadata, {actor, counter} ->
            if self() == actor and metadata[:repo] == Repo and String.starts_with?(metadata[:query] || "", "SELECT"), do: :atomics.add(counter, 1, 1)
          end,
          {actor, counter}
        )

      try do
        ids = Reservation.mailbox_admission_session_ids(fixture.auth, fixture.model, opts)
        assert MapSet.new(ids) == expected
        reads = :atomics.get(counter, 1)
        assert reads > 0
        # The bound allows both fresh discovery walks and their exact session
        # sets, without a query for every ancestor's turn and session per hop.
        assert reads <= 48
        emit_receipt(%{scenario: "sixteen_edge_discovery_cost", distinct_sessions: MapSet.size(expected), select_queries: reads})
      after
        :telemetry.detach(handler)
      end
    end)
  end

  defp committed_fixture do
    %{user: owner} = committed_bootstrap_owner_fixture!()

    Sandbox.unboxed_run(Repo, fn ->
      pool = pool_fixture(%{created_by_user_id: owner.id})
      %{api_key: key} = active_api_key_fixture(pool, %{created_by_user_id: owner.id})
      model = model_fixture(pool)
      %{assignment: assignment} = upstream_assignment_fixture(pool)
      auth = %{pool: pool, api_key: key}
      sessions = Enum.map(1..4, fn _ -> Ecto.UUID.generate() end) |> Enum.sort() |> Enum.map(&insert_session(auth, &1))
      session = hd(sessions)
      claim = "codex-request:" <> Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
      fixture = %{owner: owner, auth: auth, model: model, assignment: assignment, sessions: sessions, session: session}
      original = failed_request(fixture, session, claim, 1)
      opts = %{endpoint: @endpoint, correlation_id: claim, codex_session: session, request_metadata: %{}}
      Map.merge(fixture, %{original: original, opts: opts})
    end)
  end

  defp append_edge(fixture, predecessor, session) do
    {:ok, claim} = ClientRetry.deterministic_failed_predecessor_claim(predecessor.correlation_id, predecessor.id)
    request = failed_request(fixture, session, claim, 1)
    Repo.insert!(%RequestClientRetryLink{predecessor_request_id: predecessor.id, successor_request_id: request.id, created_at: db_now()})
    request
  end

  defp retry_entry_fixture(fixture) do
    payload = %{"type" => "response.create", "model" => fixture.model.exposed_model_id, "input" => [%{"role" => "user", "content" => "synthetic"}], "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"turn_id" => "synthetic-#{System.unique_integer([:positive])}", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "synthetic-window", "window_number" => 1})}}
    options = RequestOptions.build(%{codex_session: fixture.session, transport: "websocket"}, @endpoint, payload) |> RequestOptions.capture_api_key_runtime_epoch(fixture.auth)
    {:ok, prepared} = WebsocketCodec.prepare_frame(CodexPooler.JSON.encode!(payload), options, fn _ -> :ok end)
    continuity = prepared.request_options.continuity

    original =
      Sandbox.unboxed_run(Repo, fn ->
        Repo.update_all(from(t in CodexTurn, where: t.request_id == ^fixture.original.id), set: [semantic_turn_digest: continuity.semantic_turn_key])
        fixture.original |> Ecto.Changeset.change(Map.put(ClientRetry.request_attrs(prepared.native_client_retry_witness), :correlation_id, continuity.request_claim_key)) |> Repo.update!()
      end)

    opts = %{endpoint: @endpoint, requested_model: fixture.model.exposed_model_id, runtime_revocation_epoch: fixture.auth.api_key.runtime_revocation_epoch, codex_session: fixture.session, semantic_turn_digest: continuity.semantic_turn_key, original_request_claim: continuity.request_claim_key, replay_claim_digest: continuity.replay_claim_digest}
    {Map.merge(fixture, %{original: original, semantic_digest: continuity.semantic_turn_key}), prepared, opts}
  end

  defp append_client_retry_edge(fixture, predecessor, session) do
    {:ok, claim} = ClientRetry.deterministic_successor_claim(fixture.original, predecessor.id)
    request = failed_request(fixture, session, claim, 1)
    Repo.update_all(from(t in CodexTurn, where: t.request_id == ^request.id), set: [semantic_turn_digest: fixture.semantic_digest])
    Repo.insert!(%RequestClientRetryLink{predecessor_request_id: predecessor.id, successor_request_id: request.id, created_at: db_now()})
    request
  end

  defp execute_retry_entry(:retry_successor, fixture, _prepared, opts), do: Accounting.claim_client_retry_successor(fixture.auth, fixture.model, %{"model" => fixture.model.exposed_model_id, "input" => []}, opts)
  defp execute_retry_entry(:owner_preflight, fixture, prepared, _opts), do: Service.prepare_replay_intent(fixture.auth, prepared)

  defp assert_retry_entry_result(:retry_successor, 3, result), do: assert({:error, %{code: :duplicate_request, mailbox_check: :session}} = result)
  defp assert_retry_entry_result(:retry_successor, _n, result), do: assert({:error, :successor_claimed} = result)
  defp assert_retry_entry_result(:owner_preflight, _n, result), do: assert({:error, %{status: 409, code: "duplicate_turn"}} = result)

  defp failed_request(fixture, session, claim, sequence) do
    now = db_now()
    request = request_fixture(fixture.auth, %{model_id: fixture.model.id, correlation_id: claim, transport: "websocket", status: "failed", usage_status: "usage_unknown", completed_at: now, last_error_code: "server_error"})
    attempt = attempt_fixture(request, fixture.assignment, %{status: "failed", completed_at: now, network_error_code: "server_error", transport: "websocket", usage_status: "usage_unknown", response_metadata: %{"stream_terminal_type" => "response.failed", "error_kind" => "server_error"}})
    Repo.insert!(%CodexTurn{codex_session_id: session.id, request_id: request.id, turn_sequence: sequence, transport_kind: "websocket", semantic_turn_digest: :crypto.strong_rand_bytes(32), status: "failed", error_code: "server_error", final_attempt_id: attempt.id, started_at: now, completed_at: now, created_at: now, updated_at: now})
    request
  end

  defp insert_session(auth, id) do
    now = db_now()
    Repo.insert!(%CodexSession{id: id, pool_id: auth.pool.id, api_key_id: auth.api_key.id, session_key: "mailbox-chain-lock-#{System.unique_integer([:positive, :monotonic])}", status: "active", created_at: now, updated_at: now})
  end

  defp await_block(waiter, blocker), do: await_block(waiter, blocker, System.monotonic_time(:millisecond) + @budget)

  defp await_block(waiter, blocker, deadline) do
    rows = Sandbox.unboxed_run(Repo, fn -> Repo.query!("SELECT query, backend_xid::text, xact_start::text FROM pg_stat_activity WHERE pid=$1 AND $2=ANY(pg_blocking_pids(pid))", [waiter, blocker]).rows end)

    case rows do
      [[query, xid, started_at]] ->
        %{query: query, transaction_id: xid, transaction_start: started_at, blocker_backend: blocker}

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "admission did not block on the expected session"
        Process.sleep(20)
        await_block(waiter, blocker, deadline)
    end
  end

  defp await_execution_session_wait(waiter, blocker, ref, deadline) do
    receive do
      {:execution_result, ^ref, _result} -> flunk("execution_turn_session_membership_skipped: admission returned before locking the newly owned session")
    after
      0 -> :ok
    end

    rows = Sandbox.unboxed_run(Repo, fn -> Repo.query!("SELECT query FROM pg_stat_activity WHERE pid=$1 AND $2=ANY(pg_blocking_pids(pid))", [waiter, blocker]).rows end)

    case rows do
      [[query]] ->
        query

      [] ->
        assert System.monotonic_time(:millisecond) < deadline, "execution rediscovery did not reach its session lock"
        Process.sleep(20)
        await_execution_session_wait(waiter, blocker, ref, deadline)
    end
  end

  defp assert_previous_admission_locks_released(fixture) do
    Sandbox.unboxed_run(Repo, fn ->
      assert {:ok, :previous_admission_locks_released} =
               Repo.transaction(fn ->
                 Repo.one!(from r in Request, where: r.id == ^fixture.original.id, lock: "FOR UPDATE NOWAIT")
                 Repo.one!(from k in CodexPooler.Access.APIKey, where: k.id == ^fixture.auth.api_key.id, lock: "FOR UPDATE NOWAIT")
                 :previous_admission_locks_released
               end)
    end)
  end

  defp wait_for_database_deadline(deadline), do: wait_for_database_deadline(deadline, System.monotonic_time(:millisecond) + @budget)

  defp wait_for_database_deadline(deadline, detection_deadline) do
    now = Sandbox.unboxed_run(Repo, &db_now/0)

    if DateTime.compare(now, deadline) != :gt do
      assert System.monotonic_time(:millisecond) < detection_deadline, "database retry deadline was not reached"
      Process.sleep(20)
      wait_for_database_deadline(deadline, detection_deadline)
    end
  end

  defp stop_on_exit(task), do: on_exit(fn -> if Process.alive?(task.pid), do: Process.exit(task.pid, :kill) end)
  defp db_now, do: Repo.query!("SELECT clock_timestamp()").rows |> hd() |> hd()
  defp backend_pid, do: Repo.query!("SELECT pg_backend_pid()").rows |> hd() |> hd()
  defp emit_receipt(receipt), do: if(System.get_env("CODEX_POOLER_LOCK_TEST_DIAGNOSTICS") == "true", do: IO.puts(Jason.encode!(receipt)))
end
