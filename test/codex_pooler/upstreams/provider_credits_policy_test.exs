defmodule CodexPooler.Upstreams.ProviderCreditsPolicyTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1, run_unboxed: 1]

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.{Events, FakeUpstream, Repo, Upstreams}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Auth.TokenRefresh
  alias CodexPooler.Upstreams.Lifecycle.{CredentialFencing, IdentityLifecycle}
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPooler.Upstreams.{Secrets, TokenLinking}
  alias Ecto.Adapters.SQL.Sandbox

  @action "upstream_account.provider_credits_policy_update"
  @reason "upstream_account_provider_credits_policy_updated"
  @fence "synthetic_provider_credits_policy_fence"
  @detection_timeout_ms 15_000

  setup do
    settings = CodexPooler.TestAppEnv.restore_on_exit(Upstreams, [])

    Application.put_env(
      :codex_pooler,
      Upstreams,
      Keyword.merge(settings,
        upstream_secret_key: Base.encode64(:crypto.hash(:sha256, "synthetic-provider-credits-policy-key")),
        upstream_secret_key_version: "test-provider-credits-v1"
      )
    )

    :ok
  end

  test "new identities default to enabled without accepting generic policy overrides" do
    suffix = unique_suffix()

    assert {:ok, identity} =
             IdentityLifecycle.create_upstream_identity(%{
               chatgpt_account_id: "acct_provider_credits_default_#{suffix}",
               account_label: "Provider credits default #{suffix}",
               onboarding_method: "import",
               allow_provider_credits: false
             })

    assert identity.allow_provider_credits
    assert Repo.reload!(identity).allow_provider_credits

    assert {:ok, identity} =
             identity
             |> UpstreamIdentity.changeset(%{allow_provider_credits: false})
             |> Repo.update()

    assert identity.allow_provider_credits
  end

  test "database default applies when the new control is omitted and null is rejected" do
    identity_id = Ecto.UUID.generate()
    dumped_id = Ecto.UUID.dump!(identity_id)
    timestamp = DateTime.utc_now()

    assert %{rows: [[true]]} =
             Repo.query!(
               """
               INSERT INTO upstream_identities
                 (id, account_label, onboarding_method, status, headers_profile_version, created_at, updated_at, metadata)
               VALUES ($1, $2, 'import', 'pending', 1, $3, $3, '{}'::jsonb)
               RETURNING allow_provider_credits
               """,
               [dumped_id, "Synthetic persisted default #{unique_suffix()}", timestamp]
             )

    assert Repo.get!(UpstreamIdentity, identity_id).allow_provider_credits

    assert {:error, %Postgrex.Error{postgres: %{code: :not_null_violation, column: "allow_provider_credits"}}} =
             Repo.query(
               "UPDATE upstream_identities SET allow_provider_credits = NULL WHERE id = $1",
               [dumped_id],
               mode: :savepoint
             )

    assert Repo.get!(UpstreamIdentity, identity_id).allow_provider_credits
  end

  test "new imports default on and repeated auth JSON imports preserve an explicit opt-out" do
    fixture = scoped_fixture!()
    account = import_account()
    first_access = jwt_token(%{"exp" => future_unix()})
    second_access = jwt_token(%{"exp" => future_unix(), "synthetic_rotation" => 2})

    assert {:ok, %{status: :created, identity: identity}} =
             Upstreams.import_codex_auth_json(fixture.scope, hd(fixture.pools), auth_json(account, first_access))

    assert identity.allow_provider_credits
    assert {:ok, %{identity: opted_out}} = update_policy(fixture.scope, identity, false)
    epoch = opted_out.metadata["credential_epoch"]

    assert {:ok, %{status: :existing, identity: imported}} =
             Upstreams.import_codex_auth_json(fixture.scope, hd(fixture.pools), auth_json(account, second_access))

    assert imported.id == identity.id
    refute imported.allow_provider_credits
    assert imported.metadata["credential_epoch"] == epoch + 1
    assert {:ok, ^second_access} = Secrets.decrypt_active_secret(imported, "access_token")
    assert {:ok, account.refresh_token} == Secrets.decrypt_active_secret(imported, "refresh_token")
    refute inspect(imported.metadata) =~ second_access
    refute inspect(audit_events(identity.id)) =~ account.refresh_token
  end

  test "targeted credential relinking preserves a persisted opt-out" do
    fixture = scoped_fixture!()
    identity = fixture.identity
    assert {:ok, %{identity: opted_out}} = update_policy(fixture.scope, identity, false)
    access = "synthetic-targeted-relink-access-#{unique_suffix()}"
    refresh = "synthetic-targeted-relink-refresh-#{unique_suffix()}"

    assert {:ok, %{identity: relinked, status: :existing}} =
             TokenLinking.link_tokens(
               fixture.scope,
               hd(fixture.pools),
               %{
                 chatgpt_account_id: identity.chatgpt_account_id,
                 account_label: identity.account_label,
                 token: access,
                 refresh_token: refresh
               },
               target_identity_id: identity.id,
               credential_provenance: :codex_chatgpt
             )

    refute relinked.allow_provider_credits
    assert relinked.id == identity.id
    assert relinked.metadata["credential_epoch"] == CredentialFencing.credential_epoch(opted_out) + 1
    assert {:ok, ^access} = Secrets.decrypt_active_secret(relinked, "access_token")
  end

  test "token refresh reloads and preserves false even when passed a default-on snapshot" do
    fixture = scoped_fixture!()
    access = "synthetic-refreshed-access-#{unique_suffix()}"
    refresh = "synthetic-refresh-#{unique_suffix()}"

    # provenance: synthetic_adversarial; invented OAuth success only proves local policy preservation.
    mode =
      FakeUpstream.strict_sequence([
        FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: {:json, 200, %{"access_token" => access, "expires_in" => 3600}})
      ])

    with_fake_upstream(mode, fn upstream ->
      identity =
        fixture.identity
        |> change(metadata: %{"base_url" => FakeUpstream.url(upstream)})
        |> Repo.update!()

      assert {:ok, _secret} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "refresh_token", plaintext: refresh})
      assert {:ok, _result} = update_policy(fixture.scope, identity, false)

      assert {:ok, %{status: :active, identity: refreshed, secret_status: :present}} =
               TokenRefresh.refresh_access_token(identity, trigger_kind: "provider_credits_preservation")

      refute refreshed.allow_provider_credits
      refute Repo.reload!(identity).allow_provider_credits
      assert {:ok, ^access} = Secrets.decrypt_active_secret(refreshed, "access_token")
      assert {:ok, ^refresh} = Secrets.decrypt_active_secret(refreshed, "refresh_token")
      assert FakeUpstream.verify!(upstream) == :ok
    end)
  end

  test "usage reconciliation refreshes provider quota without resetting the credit policy" do
    fixture = scoped_fixture!()
    now = DateTime.utc_now()

    payload = %{
      "plan_type" => "pro",
      "credits" => %{"balance" => 25, "has_credits" => true, "unlimited" => false},
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "secondary_window" => %{
          "used_percent" => 20,
          "limit_window_seconds" => 604_800,
          "reset_after_seconds" => 86_400,
          "reset_at" => DateTime.to_unix(DateTime.add(now, 86_400))
        }
      }
    }

    # provenance: synthetic_adversarial; invented included quota observations carry no provider qualification.
    mode =
      FakeUpstream.strict_sequence([
        FakeUpstream.expect_request(method: "GET", path: "/backend-api/wham/usage", respond: {:json, 200, payload}),
        FakeUpstream.expect_request(method: "GET", path: "/backend-api/codex/usage", respond: {:json, 200, payload})
      ])

    with_fake_upstream(mode, fn upstream ->
      identity =
        fixture.identity
        |> change(metadata: %{"usage_base_url" => FakeUpstream.url(upstream)})
        |> Repo.update!()

      assert {:ok, _result} = update_policy(fixture.scope, identity, false)

      assert {:ok, refreshed} =
               PoolReconciliation.refresh_quota_from_usage(identity, hd(fixture.assignments))

      refute refreshed.allow_provider_credits
      refute Repo.reload!(identity).allow_provider_credits
      assert [%{window_minutes: 10_080, used_percent: percent}] = Windows.list_quota_windows(refreshed)
      assert Decimal.equal?(percent, Decimal.new(20))
      assert FakeUpstream.verify!(upstream) == :ok
    end)
  end

  for {name, input, expected} <- [
        {"boolean false", false, false},
        {"boolean true", true, true},
        {"web false", "false", false},
        {"web true", "true", true},
        {"web zero", "0", false},
        {"web one", "1", true}
      ] do
    test "accepts #{name} as an explicit provider credits control" do
      fixture = scoped_fixture!()
      before = fixture.identity |> change(allow_provider_credits: not unquote(expected)) |> Repo.update!()

      assert {:ok, %{status: :provider_credits_policy_updated, identity: updated}} =
               Upstreams.update_provider_credits_policy_for_scope(fixture.scope, before.id, %{"allow_provider_credits" => unquote(input)})

      assert updated.allow_provider_credits == unquote(expected)
      assert Repo.reload!(before).allow_provider_credits == unquote(expected)
      assert [event] = audit_events(before.id)
      assert event.details["previous_allow_provider_credits"] == not unquote(expected)
      assert event.details["allow_provider_credits"] == unquote(expected)
    end
  end

  for {name, value} <- [
        {"null", nil},
        {"empty", ""},
        {"junk", "enabled"},
        {"numeric zero", 0},
        {"numeric one", 1},
        {"checkbox on", "on"},
        {"whitespace", " false "},
        {"list", []},
        {"map", %{}}
      ] do
    @tag credits_negative: true
    test "rejects #{name} without policy audit reset jobs or publication" do
      fixture = scoped_fixture!()
      subscribe!(fixture.pools)
      before = Repo.reload!(fixture.identity)

      assert {:error, %Ecto.Changeset{} = changeset} =
               run_with_publication_fence(
                 fn ->
                   update_policy(fixture.scope, before, unquote(Macro.escape(value)))
                 end,
                 fixture.pools
               )

      assert %{allow_provider_credits: [_error]} = errors_on(changeset)
      assert Repo.reload!(before) == before
      assert_no_policy_side_effects(before.id)
    end
  end

  for {name, attrs} <- [
        {"missing control", %{}},
        {"hidden Pool ownership", %{"allow_provider_credits" => false, "pool_id" => "hidden"}},
        {"hidden identity ownership", %{"allow_provider_credits" => false, "upstream_identity_id" => "hidden"}},
        {"hidden creator ownership", %{allow_provider_credits: false, created_by_user_id: "hidden"}},
        {"provider metadata", %{allow_provider_credits: false, metadata: %{}}},
        {"duplicate control keys", %{"allow_provider_credits" => true, allow_provider_credits: false}}
      ] do
    @tag credits_negative: true
    test "rejects #{name} instead of silently dropping untrusted fields" do
      fixture = scoped_fixture!()
      subscribe!(fixture.pools)
      before = Repo.reload!(fixture.identity)

      assert {:error, %Ecto.Changeset{} = changeset} =
               run_with_publication_fence(
                 fn ->
                   Upstreams.update_provider_credits_policy_for_scope(fixture.scope, before.id, unquote(Macro.escape(attrs)))
                 end,
                 fixture.pools
               )

      assert %{allow_provider_credits: [_error]} = errors_on(changeset)
      assert Repo.reload!(before) == before
      assert_no_policy_side_effects(before.id)
    end
  end

  @tag credits_negative: true
  test "a one-Pool operator cannot change an identity shared with a hidden Pool" do
    fixture = scoped_fixture!(2)
    %{user: operator} = operator_fixture(fixture.scope.user)
    [visible_pool, hidden_pool] = fixture.pools
    operator_pool_assignment_fixture(operator, visible_pool, created_by_user_id: fixture.scope.user.id)
    scope = Scope.for_user(operator)
    subscribe!(fixture.pools)
    before = Repo.reload!(fixture.identity)

    assert {:error, %{code: :capability_denied}} =
             run_with_publication_fence(fn -> update_policy(scope, before, false) end, fixture.pools)

    assert Repo.reload!(before) == before
    assert_no_policy_side_effects(before.id)

    operator_pool_assignment_fixture(operator, hidden_pool, created_by_user_id: fixture.scope.user.id)
    scope = Scope.for_user(operator)
    assert {:ok, %{identity: updated}} = update_policy(scope, before, false)
    refute updated.allow_provider_credits
    assert Enum.sort(Enum.map(audit_events(before.id), & &1.pool_id)) == Enum.sort(Enum.map(fixture.pools, & &1.id))
  end

  @tag credits_negative: true
  test "missing identities and unassigned identities retain lifecycle denials" do
    fixture = scoped_fixture!()
    unassigned = active_upstream_identity_fixture()

    assert {:error, %{code: :upstream_identity_not_found}} = update_policy(fixture.scope, Ecto.UUID.generate(), false)
    assert {:error, %{code: :upstream_identity_not_found}} = update_policy(fixture.scope, "invalid-identity", false)
    assert {:error, %{code: :pool_assignment_not_found}} = update_policy(fixture.scope, unassigned, false)
    assert Repo.reload!(unassigned).allow_provider_credits
    assert audit_events(unassigned.id) == []
  end

  @tag credits_negative: true
  test "a scope without an actor and non-map requests cannot bypass strict audit" do
    fixture = scoped_fixture!()
    subscribe!(fixture.pools)
    before = Repo.reload!(fixture.identity)

    for {scope, attrs} <- [{%Scope{}, %{allow_provider_credits: false}}, {fixture.scope, nil}] do
      assert {:error, %{code: :invalid_request}} =
               run_with_publication_fence(
                 fn ->
                   Upstreams.update_provider_credits_policy_for_scope(scope, before.id, attrs)
                 end,
                 fixture.pools
               )
    end

    assert Repo.reload!(before) == before
    assert_no_policy_side_effects(before.id)
  end

  @tag credits_negative: true
  test "a second-Pool audit failure rolls back policy and the first audit before publication" do
    fixture = scoped_fixture!(2)
    subscribe!(fixture.pools)
    before = Repo.reload!(fixture.identity)
    install_second_audit_failure!(before.id)

    exception =
      run_with_publication_fence(
        fn ->
          assert_raise Ecto.ConstraintError, fn -> update_policy(fixture.scope, before, false) end
        end,
        fixture.pools
      )

    assert exception.type == :check
    assert exception.constraint == "provider_credits_policy_audit_fault"
    assert %{rows: [[2, true]]} = Repo.query!("SELECT last_value, is_called FROM pg_temp.provider_credits_audit_attempt")
    assert Repo.reload!(before) == before
    assert_no_policy_side_effects(before.id)
  end

  @tag credits_negative: true
  test "caller-owned transactions cannot publish a policy that is later rolled back" do
    fixture = scoped_fixture!()
    subscribe!(fixture.pools)
    before = Repo.reload!(fixture.identity)

    assert {:error, :synthetic_rollback} =
             run_with_publication_fence(
               fn ->
                 Repo.transact(fn ->
                   assert {:error, %{code: :transaction_not_allowed}} = update_policy(fixture.scope, before, false)
                   {:error, :synthetic_rollback}
                 end)
               end,
               fixture.pools
             )

    assert Repo.reload!(before) == before
    assert_no_policy_side_effects(before.id)
  end

  test "same-value saves reread persisted policy and leave timestamps audit and PubSub unchanged" do
    fixture = scoped_fixture!(2)
    assert {:ok, _result} = update_policy(fixture.scope, fixture.identity, false)
    before = Repo.reload!(fixture.identity)
    assignments = Enum.map(fixture.assignments, &Repo.reload!/1)
    audits = audit_events(before.id)
    subscribe!(fixture.pools)

    assert {:ok, %{status: :provider_credits_policy_unchanged, identity: unchanged}} =
             run_with_publication_fence(fn -> update_policy(fixture.scope, fixture.identity, false) end, fixture.pools)

    assert unchanged == before
    assert Repo.reload!(before) == before
    assert Enum.map(fixture.assignments, &Repo.reload!/1) == assignments
    assert audit_events(before.id) == audits
    assert reset_jobs(before.id) == []
  end

  test "a shared policy commits its old/new audits before both Pool subscribers observe the change" do
    fixture = committed_fixture!()
    observer = start_committed_observer!(fixture)
    barrier = make_ref()
    handler = {__MODULE__, barrier}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_first_audit/4, nil)
    parent = self()

    {writer, writer_ref, writer_monitor} =
      start_operation!(fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Process.put({__MODULE__, :audit_barrier}, {parent, barrier})
          update_policy(fixture.scope, fixture.identity, false)
        end)
      end)

    assert_receive {^barrier, :audit_pending, ^writer}, @detection_timeout_ms
    assert run_unboxed(fn -> Repo.reload!(fixture.identity).allow_provider_credits end)
    assert run_unboxed(fn -> audit_events(fixture.identity.id) end) == []
    refute_received {^observer, :committed_policy_observed, _event, _snapshot}
    send(writer, {barrier, :release})

    assert {:ok, %{status: :provider_credits_policy_updated, identity: updated}} =
             await_operation!(writer, writer_ref, writer_monitor)

    refute updated.allow_provider_credits

    observations =
      Enum.map(fixture.pools, fn _pool ->
        assert_receive {^observer, :committed_policy_observed, event, snapshot}, @detection_timeout_ms
        assert event.reason == @reason
        assert event.payload == %{"upstream_identity_id" => fixture.identity.id}
        assert snapshot.policy == false
        assert Enum.sort(Enum.map(snapshot.audits, & &1.pool_id)) == Enum.sort(Enum.map(fixture.pools, & &1.id))

        for audit <- snapshot.audits do
          assert audit.actor_user_id == fixture.scope.user.id
          assert audit.details["previous_allow_provider_credits"] == true
          assert audit.details["allow_provider_credits"] == false
        end

        event.pool_id
      end)

    assert Enum.sort(observations) == Enum.sort(Enum.map(fixture.pools, & &1.id))
    assert run_unboxed(fn -> reset_jobs(fixture.identity.id) end) == []
    send(observer, :stop)
    :telemetry.detach(handler)
  end

  @doc false
  def hold_first_audit(_event, _measurements, metadata, _config) do
    case Process.get({__MODULE__, :audit_barrier}) do
      {parent, barrier} ->
        if metadata[:source] == "audit_events" and String.starts_with?(metadata.query, "INSERT") do
          Process.delete({__MODULE__, :audit_barrier})
          send(parent, {barrier, :audit_pending, self()})

          receive do
            {^barrier, :release} -> :ok
          after
            @detection_timeout_ms -> raise "provider credits audit barrier was not released"
          end
        end

      nil ->
        :ok
    end
  end

  defp scoped_fixture!(pool_count \\ 1) do
    %{user: owner} = bootstrap_owner_fixture()
    scope = Scope.for_user(owner)
    pools = Enum.map(1..pool_count, fn _ -> pool_fixture() end)
    identity = active_upstream_identity_fixture()
    assert {:ok, _secret} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "access_token", plaintext: "synthetic-policy-access-#{unique_suffix()}"})
    assignments = Enum.map(pools, &assign!(&1, identity))
    %{scope: scope, pools: pools, identity: Repo.reload!(identity), assignments: assignments}
  end

  defp assign!(pool, identity) do
    assert {:ok, assignment} = PoolAssignments.create_pool_assignment(pool, identity)
    assert {:ok, assignment} = PoolAssignments.activate_pool_assignment(assignment)
    assignment
  end

  defp committed_fixture! do
    suffix = unique_suffix()
    account_id = "acct_provider_credits_commit_#{suffix}"
    slugs = ["provider-credits-commit-a-#{suffix}", "provider-credits-commit-b-#{suffix}"]
    %{user: owner} = committed_bootstrap_owner_fixture!()

    register_unboxed_cleanup!(fn ->
      pool_ids = Repo.all(from pool in Pool, where: pool.slug in ^slugs, select: pool.id)
      delete_committed_pools!(pool_ids)
      Repo.delete_all(from identity in UpstreamIdentity, where: identity.chatgpt_account_id == ^account_id)
    end)

    run_unboxed(fn ->
      scope = Scope.for_user(owner)
      pools = Enum.map(slugs, &pool_fixture(%{slug: &1, created_by_user_id: owner.id}))
      identity = active_upstream_identity_fixture(%{chatgpt_account_id: account_id})
      assignments = Enum.map(pools, &assign!(&1, identity))
      %{scope: scope, pools: pools, identity: identity, assignments: assignments}
    end)
  end

  defp start_committed_observer!(fixture) do
    parent = self()
    ref = make_ref()

    observer =
      start_supervised!(
        {Task,
         fn ->
           Enum.each(fixture.pools, fn pool -> :ok = Events.subscribe_pool(pool.id, "upstreams") end)
           send(parent, {ref, :observer_ready, self()})
           observe_committed_policy(parent, fixture.identity)
         end},
        id: ref
      )

    assert_receive {^ref, :observer_ready, ^observer}, @detection_timeout_ms
    observer
  end

  defp observe_committed_policy(parent, identity) do
    receive do
      {Events, %{reason: @reason} = event} ->
        snapshot = Sandbox.unboxed_run(Repo, fn -> %{policy: Repo.reload!(identity).allow_provider_credits, audits: audit_events(identity.id)} end)
        send(parent, {self(), :committed_policy_observed, event, snapshot})
        observe_committed_policy(parent, identity)

      {Events, _other_event} ->
        observe_committed_policy(parent, identity)

      :stop ->
        :ok
    end
  end

  defp start_operation!(fun) do
    parent = self()
    ref = make_ref()

    pid =
      start_supervised!(
        {Task,
         fn ->
           receive do
             {^ref, :run} -> send(parent, {ref, :operation_result, operation_outcome(fun)})
           end
         end},
        id: ref
      )

    monitor = Process.monitor(pid)
    send(pid, {ref, :run})
    {pid, ref, monitor}
  end

  defp await_operation!(pid, ref, monitor) do
    receive do
      {^ref, :operation_result, outcome} ->
        assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}, @detection_timeout_ms

        case outcome do
          {:ok, result} -> result
          {:raised, kind, reason, stacktrace} -> :erlang.raise(kind, reason, stacktrace)
        end

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        flunk("provider credits operation exited before its result: #{Exception.format_exit(reason)}")
    after
      @detection_timeout_ms -> flunk("provider credits operation did not finish")
    end
  end

  defp operation_outcome(fun) do
    {:ok, fun.()}
  catch
    kind, reason -> {:raised, kind, reason, __STACKTRACE__}
  end

  defp run_with_publication_fence(fun, pools) do
    {pid, ref, monitor} =
      start_operation!(fn ->
        result = fun.()
        Enum.each(pools, fn pool -> {:ok, _event} = Events.broadcast_upstreams(pool.id, @fence) end)
        result
      end)

    result = await_operation!(pid, ref, monitor)

    for pool <- pools do
      pool_id = pool.id
      assert_receive {Events, %{pool_id: ^pool_id, reason: @fence}}, @detection_timeout_ms
    end

    refute_received {Events, %{reason: @reason}}
    result
  end

  defp subscribe!(pools), do: Enum.each(pools, fn pool -> :ok = Events.subscribe_pool(pool.id, "upstreams") end)
  defp update_policy(scope, identity, value), do: Upstreams.update_provider_credits_policy_for_scope(scope, identity, %{allow_provider_credits: value})

  defp audit_events(identity_id) do
    Repo.all(from event in AuditEvent, where: event.action == @action and event.target_id == ^identity_id, order_by: [asc: event.pool_id])
  end

  defp reset_jobs(identity_id) do
    Repo.all(from job in Oban.Job, where: job.worker == "CodexPooler.Jobs.SavedResetRedemptionWorker" and fragment("?->>'upstream_identity_id' = ?", job.args, ^identity_id))
  end

  defp assert_no_policy_side_effects(identity_id) do
    assert audit_events(identity_id) == []
    assert reset_jobs(identity_id) == []
  end

  defp install_second_audit_failure!(identity_id) do
    Repo.query!("CREATE SEQUENCE pg_temp.provider_credits_audit_attempt")

    Repo.query!("""
    CREATE FUNCTION pg_temp.reject_second_provider_credits_audit() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.action = '#{@action}' AND NEW.target_id = '#{identity_id}'::uuid THEN
        IF nextval('pg_temp.provider_credits_audit_attempt') = 2 THEN
          RAISE EXCEPTION 'synthetic provider credits audit failure'
            USING ERRCODE = '23514', CONSTRAINT = 'provider_credits_policy_audit_fault';
        END IF;
      END IF;
      RETURN NEW;
    END
    $$
    """)

    Repo.query!("""
    CREATE TRIGGER reject_second_provider_credits_audit
    BEFORE INSERT ON audit_events
    FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_second_provider_credits_audit()
    """)
  end

  defp with_fake_upstream(mode, fun) do
    parent = self()
    ref = make_ref()

    owner =
      start_supervised!(
        {Task,
         fn ->
           receive do
             {^ref, :start} ->
               {:ok, upstream} = FakeUpstream.start_link(mode)
               send(parent, {ref, :fake_ready, upstream})

               receive do
                 {^ref, :stop} -> FakeUpstream.stop(upstream)
               end
           end
         end},
        id: ref
      )

    monitor = Process.monitor(owner)
    send(owner, {ref, :start})
    assert_receive {^ref, :fake_ready, upstream}, @detection_timeout_ms

    try do
      fun.(upstream)
    after
      send(owner, {ref, :stop})
      assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @detection_timeout_ms
    end
  end

  defp import_account do
    suffix = unique_suffix()
    %{account_id: "acct_provider_credits_import_#{suffix}", user_id: "user_provider_credits_import_#{suffix}", email: "provider-credits-#{suffix}@example.com", refresh_token: "synthetic-import-refresh-#{suffix}"}
  end

  defp auth_json(account, access_token) do
    id_token = jwt_token(%{"email" => account.email, "https://api.openai.com/auth" => %{"chatgpt_account_id" => account.account_id, "chatgpt_user_id" => account.user_id, "chatgpt_plan_type" => "pro"}})
    CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"id_token" => id_token, "access_token" => access_token, "refresh_token" => account.refresh_token, "account_id" => account.account_id}})
  end

  defp jwt_token(payload) do
    encode = &Base.url_encode64(CodexPooler.JSON.encode!(&1), padding: false)
    Enum.join([encode.(%{"alg" => "none", "typ" => "JWT"}), encode.(payload), encode.("synthetic-signature")], ".")
  end

  defp future_unix, do: DateTime.utc_now() |> DateTime.add(3600) |> DateTime.to_unix()
  defp unique_suffix, do: System.unique_integer([:positive, :monotonic])
end
