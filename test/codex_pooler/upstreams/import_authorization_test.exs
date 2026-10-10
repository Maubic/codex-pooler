defmodule CodexPooler.Upstreams.ImportAuthorizationTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import Ecto.Query
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.ImportBatchPlanner
  alias CodexPooler.Upstreams.Schemas.{EncryptedSecret, PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPooler.Upstreams.TokenLinking

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(Upstreams)
    Application.put_env(:codex_pooler, Upstreams, upstream_secret_key: Base.encode64(:crypto.hash(:sha256, "synthetic-import-root")), upstream_secret_key_version: "test-v1")
    %{user: owner} = bootstrap_owner_fixture()
    %{user: admin} = operator_fixture(owner)
    a = pool_fixture()
    b = pool_fixture()
    operator_pool_assignment_fixture(admin, b, created_by_user_id: owner.id)
    account = "sample-#{System.unique_integer([:positive])}"
    owner_scope = Scope.for_user(owner)
    assert {:ok, %{identity: identity}} = Upstreams.import_codex_auth_json(owner_scope, a, auth(account))
    assert {:ok, assignment} = PoolAssignments.create_pool_assignment(b, identity)
    assert {:ok, _} = PoolAssignments.activate_pool_assignment(assignment)
    %{owner: owner, admin: admin, scope: Scope.for_user(admin), owner_scope: owner_scope, a: a, b: b, identity: identity, account: account}
  end

  test "hidden served Pool denies actual auth.json replacement without local side effects", f do
    assert {:error, %{code: :capability_denied}} = Upstreams.pause_account_for_scope(f.scope, f.identity.id, %{})
    :ok = CodexPooler.Events.subscribe_pool(f.b, "upstreams")
    before = snapshot()

    task =
      Task.async(fn ->
        result = Upstreams.import_codex_auth_json(f.scope, f.b, auth(f.account))
        CodexPooler.Events.broadcast_upstreams(f.b, "authorization_test_barrier")
        result
      end)

    result = Task.await(task)
    assert match?({:error, %{code: :capability_denied}}, result), "shared replacement was not denied"
    assert_receive {CodexPooler.Events, %{reason: "authorization_test_barrier"}}
    refute_received {CodexPooler.Events, _event}
    assert snapshot() == before
  end

  test "all-Pool admin and owner preserve legitimate shared imports", f do
    operator_pool_assignment_fixture(f.admin, f.a, created_by_user_id: f.owner.id)

    for scope <- [Scope.for_user(f.admin), f.owner_scope] do
      epoch = Repo.reload!(f.identity).metadata["credential_epoch"]
      assert {:ok, %{status: :existing, identity: identity}} = Upstreams.import_codex_auth_json(scope, f.b, auth(f.account))
      assert identity.id == f.identity.id
      assert identity.metadata["credential_epoch"] == epoch + 1
      assert Repo.aggregate(PoolUpstreamAssignment, :count) == 2
    end
  end

  for status <- ~w(active pending paused disabled deleted) do
    test "served assignment #{status} follows nondeleted authorization", f do
      assignment = Repo.get_by!(PoolUpstreamAssignment, pool_id: f.a.id, upstream_identity_id: f.identity.id)
      Repo.update!(Ecto.Changeset.change(assignment, status: unquote(status)))
      before = snapshot()
      result = Upstreams.import_codex_auth_json(f.scope, f.b, auth(f.account))

      if unquote(status) == "deleted" do
        assert match?({:ok, %{status: :existing}}, result)
      else
        assert match?({:error, %{code: :capability_denied}}, result), "nondeleted served Pool bypassed authorization"
        assert snapshot() == before
      end
    end
  end

  test "targeted relink and trusted import cannot bypass served-Pool guard", f do
    attrs = %{chatgpt_account_id: f.account, chatgpt_user_id: "sample-user", account_label: "Sample", token: jwt(%{"exp" => DateTime.to_unix(DateTime.add(DateTime.utc_now(), 3600))}), refresh_token: "synthetic-relink", credential_provenance: "codex_chatgpt_oauth"}
    before = snapshot()
    assert {:error, %{code: :capability_denied}} = TokenLinking.link_tokens(f.scope, f.b, attrs, target_identity_id: f.identity.id, credential_provenance: :codex_chatgpt)
    assert {:error, %{code: :capability_denied}} = Upstreams.import_trusted_account(f.scope, f.b, attrs)
    assert snapshot() == before
  end

  test "batch dry-run and persistence reject hidden target atomically", f do
    assert {:ok, hidden} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, auth(f.account))
    assert {:ok, fresh} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, auth("sample-new-#{System.unique_integer([:positive])}"))
    before = snapshot()

    for operation <- [&ImportBatchPlanner.validate_prepared_batch_in_transaction/3, &ImportBatchPlanner.diagnose_prepared_batch_in_transaction/3, &TokenLinking.link_prepared_batch_in_transaction/3] do
      assert {:ok, {:error, %{code: :capability_denied}}} = Repo.transaction(fn -> operation.(f.scope, f.b, [fresh, hidden]) end)
      assert snapshot() == before
    end
  end

  test "prepared import rechecks revoked served-Pool capability", f do
    grant = operator_pool_assignment_fixture(f.admin, f.a, created_by_user_id: f.owner.id)
    assert {:ok, prepared} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, auth(f.account))
    Repo.update!(Ecto.Changeset.change(grant, status: "revoked"))
    before = snapshot()
    assert {:error, %{code: :capability_denied}} = TokenLinking.link_prepared(f.scope, f.b, prepared, [])
    assert snapshot() == before
  end

  test "new and unassigned identities remain importable into authorized target", f do
    assert {:ok, %{status: :created}} = Upstreams.import_codex_auth_json(f.scope, f.b, auth("sample-fresh-#{System.unique_integer([:positive])}"))
    Repo.delete_all(from a in PoolUpstreamAssignment, where: a.upstream_identity_id == ^f.identity.id)
    assert {:ok, %{identity: identity}} = Upstreams.import_codex_auth_json(f.scope, f.b, auth(f.account))
    assert identity.id == f.identity.id
    before = snapshot()
    assert {:error, %{code: :capability_denied}} = Upstreams.import_codex_auth_json(f.scope, f.a, auth("sample-denied-#{System.unique_integer([:positive])}"))
    assert snapshot() == before
  end

  test "batch authorized repeated identity keeps dry-run real parity", f do
    operator_pool_assignment_fixture(f.admin, f.a, created_by_user_id: f.owner.id)
    assert {:ok, first} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, auth(f.account))
    assert {:ok, second} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, auth(f.account))
    before = snapshot()
    assert {:ok, {:ok, 2}} = Repo.transaction(fn -> ImportBatchPlanner.validate_prepared_batch_in_transaction(f.scope, f.b, [first, second]) end)
    assert snapshot() == before
    assert {:ok, {:ok, results}} = Repo.transaction(fn -> TokenLinking.link_prepared_batch_in_transaction(f.scope, f.b, [first, second]) end)
    assert Enum.map(results, & &1.identity.id) == [f.identity.id, f.identity.id]
  end

  test "prepared import sees a newly served hidden Pool without an epoch change", f do
    hidden = Repo.get_by!(PoolUpstreamAssignment, pool_id: f.a.id, upstream_identity_id: f.identity.id)
    Repo.delete!(hidden)
    assert {:ok, prepared} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, auth(f.account))
    assert {:ok, _} = PoolAssignments.create_pool_assignment(f.a, f.identity)
    before = snapshot()
    assert {:error, %{code: :capability_denied}} = TokenLinking.link_prepared(f.scope, f.b, prepared, [])
    assert snapshot() == before
  end

  test "batch authorization ignores unrelated locked account siblings", f do
    content = auth(f.account, "other-sample-user")
    assert {:ok, %{identity: sibling}} = Upstreams.import_codex_auth_json(f.owner_scope, f.b, content)
    assert sibling.id != f.identity.id
    assert {:ok, prepared} = Upstreams.prepare_codex_auth_json_account(f.scope, f.b, content)
    assert {:ok, {:ok, 1}} = Repo.transaction(fn -> ImportBatchPlanner.validate_prepared_batch_in_transaction(f.scope, f.b, [prepared]) end)
    assert {:ok, {:ok, [%{identity: imported}]}} = Repo.transaction(fn -> TokenLinking.link_prepared_batch_in_transaction(f.scope, f.b, [prepared]) end)
    assert imported.id == sibling.id
  end

  defp snapshot do
    {Repo.all(from i in UpstreamIdentity, order_by: i.id), Repo.all(from s in EncryptedSecret, order_by: s.id), Repo.all(from a in PoolUpstreamAssignment, order_by: a.id), Repo.aggregate(AuditEvent, :count), Repo.aggregate(Oban.Job, :count)}
  end

  defp auth(account, user \\ "sample-user") do
    CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"account_id" => account, "id_token" => jwt(%{"https://api.openai.com/auth" => %{"chatgpt_account_id" => account, "chatgpt_user_id" => user, "chatgpt_plan_type" => "pro"}}), "access_token" => jwt(%{"exp" => DateTime.to_unix(DateTime.add(DateTime.utc_now(), 3600))}), "refresh_token" => "synthetic-import-#{System.unique_integer([:positive])}"}})
  end

  defp jwt(payload), do: "e30." <> Base.url_encode64(CodexPooler.JSON.encode!(payload), padding: false) <> ".synthetic"
end

defmodule CodexPooler.Upstreams.ImportAuthorizationPostgresTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard
  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures, only: [operator_pool_assignment_fixture: 3, delete_committed_pools!: 1]
  import CodexPooler.UnboxedFixture, only: [run_unboxed: 1, register_unboxed_cleanup!: 1]
  import Ecto.Query
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox

  @budget 15_000

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(Upstreams)
    Application.put_env(:codex_pooler, Upstreams, upstream_secret_key: Base.encode64(:crypto.hash(:sha256, "synthetic-import-root")), upstream_secret_key_version: "test-v1")
    %{user: owner} = committed_bootstrap_owner_fixture!()
    suffix = System.unique_integer([:positive])
    slug = "authorization-#{suffix}"
    account = "sample-auth-#{suffix}"

    register_unboxed_cleanup!(fn ->
      ids = Repo.all(from p in CodexPooler.Pools.Pool, where: p.slug in ^[slug <> "-a", slug <> "-b"], select: p.id)
      delete_committed_pools!(ids)
      Repo.delete_all(from i in UpstreamIdentity, where: i.chatgpt_account_id == ^account)
    end)

    fixture =
      run_unboxed(fn ->
        %{user: admin} = operator_fixture(owner)
        scope = Scope.for_user(owner)
        {:ok, a} = Pools.create_pool(scope, %{name: "Sample A", slug: slug <> "-a"})
        {:ok, b} = Pools.create_pool(scope, %{name: "Sample B", slug: slug <> "-b"})
        operator_pool_assignment_fixture(admin, b, created_by_user_id: owner.id)
        content = auth(account)
        assert {:ok, %{identity: identity}} = Upstreams.import_codex_auth_json(scope, b, content)
        %{a: a, b: b, identity: identity, scope: Scope.for_user(admin), content: content}
      end)

    {:ok, fixture}
  end

  for order <- [:attachment_first, :import_first] do
    test "#{order} defines shared-credential authorization at the canonical lock", f do
      attach = fn -> PoolAssignments.create_pool_assignment(f.a, f.identity) end
      import = fn -> Upstreams.import_codex_auth_json(f.scope, f.b, f.content) end

      {holder_fun, waiter_fun, holder_lock, waiter_lock} =
        case unquote(order) do
          :attachment_first -> {attach, import, "FOR KEY SHARE", "FOR UPDATE"}
          :import_first -> {import, attach, "FOR UPDATE", "FOR KEY SHARE"}
        end

      before_epoch = f.identity.metadata["credential_epoch"]
      {holder_result, waiter_result} = serialize(holder_fun, waiter_fun, holder_lock, waiter_lock)
      assert match?({:ok, _}, holder_result)

      if unquote(order) == :attachment_first do
        assert {:error, %{code: :capability_denied}} = waiter_result
        assert run_unboxed(fn -> Repo.reload!(f.identity).metadata["credential_epoch"] end) == before_epoch
      else
        assert match?({:ok, _}, waiter_result)
        assert run_unboxed(fn -> Repo.reload!(f.identity).metadata["credential_epoch"] end) == before_epoch + 1
        assert {:error, %{code: :capability_denied}} = run_unboxed(import)
      end
    end
  end

  def observe_lock(_event, _measurements, metadata, _config) do
    case Process.get({__MODULE__, :probe}) do
      {role, parent, ref, backend, lock} ->
        if String.contains?(metadata.query, "upstream_identities") and String.contains?(metadata.query, lock) do
          record_lock(role, parent, ref, backend)
        end

      _ ->
        :ok
    end
  end

  defp record_lock(:holder, parent, ref, backend) do
    Process.delete({__MODULE__, :probe})
    send(parent, {ref, :held, self(), backend})

    receive do
      {^ref, :release} -> :ok
    after
      @budget -> raise "import authorization holder release missing"
    end
  end

  defp record_lock(:waiter, _parent, _ref, _backend), do: Process.put({__MODULE__, :seen}, true)

  defp serialize(holder_fun, waiter_fun, holder_lock, waiter_lock) do
    parent = self()
    ref = make_ref()
    id = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(id) end)
    :ok = :telemetry.attach(id, [:codex_pooler, :repo, :query], &__MODULE__.observe_lock/4, nil)
    supervisor = start_supervised!(Task.Supervisor)

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        checkout(fn ->
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          Process.put({__MODULE__, :probe}, {:holder, parent, ref, backend, holder_lock})
          holder_fun.()
        end)
      end)

    monitor_h = Process.monitor(holder.pid)
    assert_receive {^ref, :held, holder_pid, backend_h}, @budget

    waiter =
      Task.Supervisor.async_nolink(supervisor, fn ->
        checkout(fn ->
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          Process.put({__MODULE__, :probe}, {:waiter, parent, ref, backend, waiter_lock})
          proof = lock_proof(waiter_fun)
          send(parent, {ref, :proof, backend, proof})

          receive do
            {^ref, :retry} -> waiter_fun.()
          after
            @budget -> raise "import authorization retry missing"
          end
        end)
      end)

    monitor_w = Process.monitor(waiter.pid)
    assert_receive {^ref, :proof, backend_w, proof}, @budget
    send(holder_pid, {ref, :release})
    result_h = Task.await(holder, @budget)
    send(waiter.pid, {ref, :retry})
    result_w = Task.await(waiter, @budget)
    assert_receive {:DOWN, ^monitor_h, :process, _, :normal}, @budget
    assert_receive {:DOWN, ^monitor_w, :process, _, :normal}, @budget
    assert backend_h != backend_w
    assert proof == {:lock_not_available, true}
    {result_h, result_w}
  end

  defp checkout(fun), do: Sandbox.unboxed_run(Repo, fn -> Repo.checkout(fun) end)

  defp lock_proof(fun) do
    Repo.query!("SET lock_timeout = '200ms'")

    try do
      fun.()
      :did_not_wait
    rescue
      error in Postgrex.Error -> {error.postgres.code, Process.get({__MODULE__, :seen}, false)}
    after
      Repo.query!("SET lock_timeout = '0'")
    end
  end

  defp auth(account) do
    jwt = fn payload -> "e30." <> Base.url_encode64(CodexPooler.JSON.encode!(payload), padding: false) <> ".synthetic" end
    CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"account_id" => account, "id_token" => jwt.(%{"https://api.openai.com/auth" => %{"chatgpt_account_id" => account, "chatgpt_user_id" => "sample-user", "chatgpt_plan_type" => "pro"}}), "access_token" => jwt.(%{"exp" => DateTime.to_unix(DateTime.add(DateTime.utc_now(), 3600))}), "refresh_token" => "synthetic-import"}})
  end
end
