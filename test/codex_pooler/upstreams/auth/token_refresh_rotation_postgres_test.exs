defmodule CodexPooler.Upstreams.Auth.TokenRefreshRotationPostgresTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard
  import Ecto.Query
  import CodexPooler.AccountsFixtures, only: [committed_bootstrap_owner_fixture!: 1]
  import CodexPooler.PoolerFixtures, only: [delete_committed_pools!: 1]
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1, run_unboxed: 1]
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.FakeRefreshTokenProvider
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Auth.{RefreshTokenRecovery, TokenRefresh}
  alias CodexPooler.Upstreams.Schemas.{EncryptedSecret, UpstreamIdentity}
  alias CodexPooler.Upstreams.Secrets
  alias CodexPooler.Upstreams.TokenLinking
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000
  @moduletag :refresh_rotation_postgres

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(Upstreams)
    Application.put_env(:codex_pooler, Upstreams, upstream_secret_key: Base.encode64(:crypto.hash(:sha256, "synthetic-rotation-root")), upstream_secret_key_version: "test-v1")
    :ok
  end

  for operation <- [:import, :relink, :pause, :delete, :permanent_delete] do
    for order <- [:lifecycle_first, :retention_first] do
      test "#{operation} #{order} serializes against late rotation" do
        fixture = fixture()

        if unquote(operation) == :permanent_delete do
          run_unboxed(fn ->
            for assignment <- Repo.all(from a in CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, where: a.upstream_identity_id == ^fixture.identity.id) do
              assert {:ok, _} = PoolAssignments.delete_pool_assignment(fixture.pool, assignment)
            end
          end)
        end

        {task, handler, ref, attempt} = held_refresh(fixture)
        lifecycle = fn -> mutate(unquote(operation), fixture) end
        retain = fn -> retain(fixture.identity, attempt) end

        if unquote(order) == :lifecycle_first do
          {_lifecycle, {:ok, {:ok, :ignored, _}}} = serialize(lifecycle, retain)
        else
          {{:ok, {:ok, :retained, _}}, lifecycle_result} = serialize(retain, lifecycle)
          assert match?({:ok, _}, lifecycle_result)
        end

        before = snapshot(fixture.identity.id)
        send(handler, {:rotation_release, ref})
        await_refresh(task)
        assert snapshot(fixture.identity.id) == before
      end
    end
  end

  test "R4 newer real successful successor wins over delayed original finalization" do
    fixture = fixture()
    {task, handler, ref, attempt} = held_refresh(fixture)
    assert {:ok, {:ok, :retained, _}} = run_unboxed(fn -> retain(fixture.identity, attempt) end)
    age(fixture.identity)
    assert {:ok, %{status: :active}} = run_unboxed(fn -> TokenRefresh.refresh_access_token(fixture.identity) end)
    before = snapshot(fixture.identity.id)
    send(handler, {:rotation_release, ref})
    assert {:ok, %{status: :noop}} = await_refresh(task)
    assert snapshot(fixture.identity.id) == before
    assert FakeRefreshTokenProvider.snapshot(fixture.ledger).consumed == 2
    assert run_unboxed(fn -> refresh_generation?(fixture.identity, 2) end)
  end

  test "R7 a replacement secret row without epoch advance refuses recovery" do
    fixture = fixture()
    {task, handler, ref, attempt} = held_refresh(fixture)
    run_unboxed(fn -> Secrets.store_encrypted_secret(fixture.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-direct-replacement"}) end)
    before = snapshot(fixture.identity.id)
    assert {:ok, {:ok, :ignored, _}} = run_unboxed(fn -> retain(fixture.identity, attempt) end)
    assert snapshot(fixture.identity.id) == before
    # Make A superseded without changing the epoch, so its ordinary success is not applied.
    run_unboxed(fn ->
      identity = Repo.reload!(fixture.identity)
      metadata = update_in(identity.metadata, ["token_refresh", "generation"], &(&1 + 1))
      Repo.update!(Ecto.Changeset.change(identity, metadata: metadata))
    end)

    send(handler, {:rotation_release, ref})
    await_refresh(task)
    refute run_unboxed(fn -> refresh_generation?(fixture.identity, 1) end)
  end

  test "R8 duplicate completion has one encrypted replacement across independent writers" do
    fixture = fixture()
    {task, handler, ref, attempt} = held_refresh(fixture)
    assert {{:ok, {:ok, :retained, _}}, {:ok, {:ok, :ignored, _}}} = serialize(fn -> retain(fixture.identity, attempt) end, fn -> retain(fixture.identity, attempt) end)
    before = snapshot(fixture.identity.id)
    assert {:ok, {:ok, :ignored, _}} = run_unboxed(fn -> retain(fixture.identity, attempt) end)
    assert snapshot(fixture.identity.id) == before
    assert run_unboxed(fn -> Repo.aggregate(from(s in EncryptedSecret, where: s.upstream_identity_id == ^fixture.identity.id and s.secret_kind == "refresh_token"), :count) end) == 2
    # Finalize original through a newer generation to exercise duplicate salvage too.
    age(fixture.identity)
    assert {:ok, %{status: :active}} = run_unboxed(fn -> TokenRefresh.refresh_access_token(fixture.identity) end)
    send(handler, {:rotation_release, ref})
    await_refresh(task)
  end

  test "missing identity cannot be recreated by a delayed success" do
    fixture = fixture()
    {task, handler, ref, attempt} = held_refresh(fixture)
    run_unboxed(fn -> Repo.delete!(Repo.reload!(fixture.identity)) end)
    assert {:ok, {:ok, :ignored, nil}} = run_unboxed(fn -> retain(fixture.identity, attempt) end)
    send(handler, {:rotation_release, ref})
    assert {:error, %{code: :upstream_identity_not_found}} = await_refresh(task)
    assert run_unboxed(fn -> Repo.get(UpstreamIdentity, fixture.identity.id) end) == nil
  end

  @tag :rotation_publication
  test "retention publication survives permanent deletion after the committed rotation" do
    fixture = fixture([%{hold: :after_consume}, %{hold: :after_consume}])
    parent = self()
    barrier = make_ref()
    handler_id = {__MODULE__, barrier}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.hold_second_commit/4, nil)
    supervisor = start_supervised!(Task.Supervisor)

    first =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Process.put({__MODULE__, :commit_barrier}, {parent, barrier, 0})

          try do
            TokenRefresh.refresh_access_token(fixture.identity)
          rescue
            exception -> {:raised, exception.__struct__}
          end
        end)
      end)

    first_monitor = Process.monitor(first.pid)
    assert_receive {:rotation_barrier, :after_consume, 1, first_handler, first_ref}, @budget
    age(fixture.identity)

    second =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn -> TokenRefresh.refresh_access_token(fixture.identity) end)
      end)

    second_monitor = Process.monitor(second.pid)
    assert_receive {:rotation_barrier, :after_consume, 2, second_handler, second_ref}, @budget
    send(first_handler, {:rotation_release, first_ref})
    assert_receive {^barrier, :committed, publisher}, @budget

    # A different backend sees the committed rotation, then deletes the identity
    # while A has not yet executed its publication code. No database call is mocked.
    run_unboxed(fn ->
      identity = Repo.reload!(fixture.identity)
      assert identity.metadata["refresh_token_recovery"]["reason"] == "superseded_attempt"
      assert refresh_generation?(identity, 1)

      for assignment <- Repo.all(from a in CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, where: a.upstream_identity_id == ^identity.id) do
        assert {:ok, _} = PoolAssignments.delete_pool_assignment(fixture.pool, assignment)
      end

      assert {:ok, _} = Upstreams.delete_account_for_scope(fixture.scope, identity.id)
      assert Repo.get(UpstreamIdentity, identity.id) == nil
    end)

    send(publisher, {barrier, :publish})
    first_result = Task.await(first, @budget)
    assert_receive {:DOWN, ^first_monitor, :process, _, :normal}, @budget
    send(second_handler, {:rotation_release, second_ref})
    assert {:error, %{code: :upstream_identity_not_found}} = Task.await(second, @budget)
    assert_receive {:DOWN, ^second_monitor, :process, _, :normal}, @budget
    assert match?({:error, :refresh_in_progress, _}, first_result), "post-commit publication outcome: #{inspect(first_result)}"
    assert FakeRefreshTokenProvider.snapshot(fixture.ledger).consumed == 1
  end

  def hold_second_commit(_event, _measurements, %{query: "commit"}, _config) do
    case Process.get({__MODULE__, :commit_barrier}) do
      {parent, ref, 1} ->
        Process.delete({__MODULE__, :commit_barrier})
        send(parent, {ref, :committed, self()})

        receive do
          {^ref, :publish} -> :ok
        after
          @budget -> raise "publication release missing"
        end

      {parent, ref, count} ->
        Process.put({__MODULE__, :commit_barrier}, {parent, ref, count + 1})

      _unrelated ->
        :ok
    end
  end

  def hold_second_commit(_event, _measurements, _metadata, _config), do: :ok

  defp await_refresh(task) do
    monitor = Process.delete({__MODULE__, :refresh_monitor, task.ref})
    result = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    result
  end

  defp fixture(steps \\ [%{hold: :after_consume}]) do
    suffix = System.unique_integer([:positive])
    %{user: user} = committed_bootstrap_owner_fixture!(%{"email" => "rotation-#{suffix}@example.com"})
    account = "sample-rotation-#{suffix}"
    slug = "rotation-#{suffix}"

    register_unboxed_cleanup!(fn ->
      Repo.delete_all(from j in Oban.Job, where: j.worker == "CodexPooler.Jobs.UpstreamDeletionWorker" and fragment("?->>'requested_by_user_id' = ?", j.args, ^user.id))
      if pool = Repo.get_by(CodexPooler.Pools.Pool, slug: slug), do: delete_committed_pools!([pool.id])
      Repo.delete_all(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
    end)

    ledger = String.to_atom("rotation_pg_#{suffix}")
    provider = start_supervised!({FakeRefreshTokenProvider, name: ledger, notify: self(), steps: steps})
    url = FakeRefreshTokenProvider.url(provider)

    run_unboxed(fn ->
      scope = Scope.for_user(user)
      {:ok, pool} = Pools.create_pool(scope, %{name: "Sample", slug: slug})
      attrs = %{credential_provenance: "codex_chatgpt_oauth", chatgpt_account_id: account, account_label: "Sample", token: "synthetic-access-original", refresh_token: "synthetic-refresh-0"}
      {:ok, %{identity: identity}} = Upstreams.import_trusted_account(scope, pool, attrs)
      identity = Repo.update!(Ecto.Changeset.change(identity, metadata: Map.put(identity.metadata, "base_url", url)))
      %{identity: identity, scope: scope, pool: pool, attrs: attrs, ledger: ledger}
    end)
  end

  defp held_refresh(fixture) do
    supervisor = start_supervised!({Task.Supervisor, []})
    task = Task.Supervisor.async_nolink(supervisor, fn -> Sandbox.unboxed_run(Repo, fn -> TokenRefresh.refresh_access_token(fixture.identity) end) end)
    Process.put({__MODULE__, :refresh_monitor, task.ref}, Process.monitor(task.pid))
    assert_receive {:rotation_barrier, :after_consume, 1, handler, ref}, @budget

    attempt =
      run_unboxed(fn ->
        current = Repo.reload!(fixture.identity).metadata["token_refresh"]
        %{attempt_id: current["attempt_id"], generation: current["generation"], source_secret_id: current["source_secret_id"], credential_epoch: current["credential_epoch"]}
      end)

    {task, handler, ref, attempt}
  end

  defp retain(identity, attempt), do: Repo.transaction(fn -> RefreshTokenRecovery.retain(identity, attempt, %{refresh_token: "synthetic-refresh-1"}, "superseded_attempt") end)
  defp mutate(:import, f), do: Upstreams.import_trusted_account(f.scope, f.pool, %{f.attrs | token: "synthetic-imported-access", refresh_token: "synthetic-imported-refresh"})
  defp mutate(:relink, f), do: TokenLinking.link_tokens(f.scope, f.pool, %{f.attrs | token: "synthetic-relinked-access", refresh_token: "synthetic-relinked-refresh"}, target_identity_id: f.identity.id, credential_provenance: :codex_chatgpt)
  defp mutate(:pause, f), do: Upstreams.pause_account_for_scope(f.scope, f.identity.id, %{})
  defp mutate(:delete, f), do: Upstreams.soft_delete_account_for_scope(f.scope, f.identity.id, %{})
  defp mutate(:permanent_delete, f), do: Upstreams.delete_account_for_scope(f.scope, f.identity.id, %{})

  defp age(identity) do
    run_unboxed(fn ->
      current = Repo.reload!(identity)
      metadata = put_in(current.metadata, ["token_refresh", "started_at"], DateTime.to_iso8601(DateTime.add(DateTime.utc_now(), -120)))
      Repo.update!(Ecto.Changeset.change(current, metadata: metadata))
    end)
  end

  defp snapshot(id) do
    run_unboxed(fn ->
      {Repo.get(UpstreamIdentity, id), Repo.all(from s in EncryptedSecret, where: s.upstream_identity_id == ^id, order_by: s.id)}
    end)
  end

  defp refresh_generation?(identity, generation) do
    {:ok, token} = Secrets.decrypt_active_secret(identity, "refresh_token")
    token == "synthetic-refresh-#{generation}"
  end

  # Telemetry occurs in the exact query caller after PostgreSQL has executed the
  # lock. Hold only the first identity row-lock query, never an unrelated writer.
  def observe_lock(_event, _measurements, metadata, _config) do
    if String.contains?(metadata.query, "upstream_identities") and (String.contains?(metadata.query, "FOR UPDATE") or String.starts_with?(metadata.query, "UPDATE")) do
      case Process.get({__MODULE__, :lock}) do
        {:holder, parent, ref, backend} ->
          Process.delete({__MODULE__, :lock})
          send(parent, {ref, :held, self(), backend})

          receive do
            {^ref, :release} -> :ok
          after
            @budget -> raise "rotation writer release missing"
          end

        :waiter ->
          Process.put({__MODULE__, :lock_seen}, true)

          record_lock_error(metadata.result)

        _ ->
          :ok
      end
    end
  end

  defp record_lock_error({:error, %Postgrex.Error{postgres: %{code: code}}}), do: Process.put({__MODULE__, :lock_error}, code)
  defp record_lock_error(_result), do: :ok

  defp unboxed_checkout(fun), do: Sandbox.unboxed_run(Repo, fn -> Repo.checkout(fun) end)

  defp lock_proof(waiter_fun) do
    Repo.query!("SET lock_timeout = '200ms'")

    try do
      waiter_fun.()

      case Process.get({__MODULE__, :lock_error}) do
        nil -> :did_not_wait
        code -> {code, Process.get({__MODULE__, :lock_seen}, false)}
      end
    rescue
      error in Postgrex.Error -> {error.postgres.code, Process.get({__MODULE__, :lock_seen}, false)}
    after
      Repo.query!("SET lock_timeout = '0'")
    end
  end

  defp serialize(holder_fun, waiter_fun) do
    ref = make_ref()
    parent = self()
    handler_id = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.observe_lock/4, nil)
    supervisor = start_supervised!({Task.Supervisor, []}, id: ref)

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        unboxed_checkout(fn ->
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          Process.put({__MODULE__, :lock}, {:holder, parent, ref, backend})
          holder_fun.()
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    assert_receive {^ref, :held, holder_pid, holder_backend}, @budget

    waiter =
      Task.Supervisor.async_nolink(supervisor, fn ->
        unboxed_checkout(fn ->
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          Process.put({__MODULE__, :lock}, :waiter)
          proof = lock_proof(waiter_fun)
          send(parent, {ref, :proof, backend, proof})

          receive do
            {^ref, :retry} -> waiter_fun.()
          after
            @budget -> raise "rotation retry release missing"
          end
        end)
      end)

    waiter_monitor = Process.monitor(waiter.pid)
    assert_receive {^ref, :proof, waiter_backend, proof}, @budget
    send(holder_pid, {ref, :release})
    holder_result = Task.await(holder, @budget)
    send(waiter.pid, {ref, :retry})
    waiter_result = Task.await(waiter, @budget)
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @budget
    assert_receive {:DOWN, ^waiter_monitor, :process, _, :normal}, @budget
    assert waiter_backend != holder_backend
    assert proof == {:lock_not_available, true}
    {holder_result, waiter_result}
  end
end
