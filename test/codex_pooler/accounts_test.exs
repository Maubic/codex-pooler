defmodule CodexPooler.AccountsTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounts
  alias CodexPooler.Accounts.AuditLog
  alias CodexPooler.Accounts.{MFA, RecoveryCode, Session, TOTPSetting, User}
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.Pools.Membership
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  import CodexPooler.AccountsFixtures

  setup do
    reset_bootstrap_state_fixture!()
    :ok
  end

  describe "bootstrap_owner/2" do
    test "creates exactly one instance owner, membership, session, and audit row" do
      assert Accounts.bootstrap_status() == "pending"

      assert {:ok, %{user: %User{} = user, token: token}} =
               Accounts.bootstrap_owner(
                 valid_bootstrap_attributes(%{"email" => "Owner@Example.com"}),
                 %{
                   ip_address: "203.0.113.10",
                   user_agent: "test-agent"
                 }
               )

      assert user.email == "owner@example.com"
      assert Accounts.bootstrap_status() == "completed"

      assert Accounts.get_user_by_email_and_password("OWNER@example.com", valid_user_password()).id ==
               user.id

      assert Repo.get_by(Membership, user_id: user.id, role: "instance_owner", status: "active")
      assert Repo.one(from s in Session, where: s.user_id == ^user.id and s.status == "active")
      assert Accounts.get_user_by_session_token(token)
      assert Repo.get_by(AuditEvent, action: "auth.bootstrap", actor_user_id: user.id)

      assert {:error, :bootstrap_already_completed} =
               Accounts.bootstrap_owner(valid_bootstrap_attributes(%{"email" => "second@example.com"}))
    end

    test "serializes concurrent bootstrap attempts through the singleton lock" do
      parent = self()

      tasks =
        for idx <- 1..2 do
          Task.async(fn ->
            Sandbox.allow(Repo, parent, self())

            Accounts.bootstrap_owner(valid_bootstrap_attributes(%{"email" => "owner-#{idx}@example.com"}))
          end)
        end

      results = Task.await_many(tasks, 15_000)

      assert Enum.count(results, &match?({:ok, _}, &1)) == 1
      assert Enum.count(results, &(&1 == {:error, :bootstrap_already_completed})) == 1
      assert Repo.aggregate(Membership, :count) == 1
    end
  end

  describe "enable_totp_for_user/1" do
    test "refuses active enrollment without replacing the factor or recovery codes" do
      %{user: user} = bootstrap_owner_fixture()
      {:ok, setup} = Accounts.enable_totp_for_user(user)
      before_codes = Repo.all(from c in RecoveryCode, where: c.user_id == ^user.id, order_by: c.id)

      outcome =
        case Accounts.enable_totp_for_user(user) do
          {:ok, _setup} -> :enrolled
          {:error, reason} -> reason
        end

      assert outcome == :totp_already_enabled
      unchanged_setting? = Repo.reload!(setup.setting) == setup.setting
      unchanged_codes? = Repo.all(from c in RecoveryCode, where: c.user_id == ^user.id, order_by: c.id) == before_codes
      assert unchanged_setting?
      assert unchanged_codes?
      assert :ok = MFA.verify_second_factor(user, Accounts.current_totp_code(setup.secret), nil, %{})
      assert :ok = MFA.verify_second_factor(user, nil, hd(setup.recovery_codes), %{})
    end
  end

  describe "TOTP replay protection" do
    for offset <- [-1, 0, 1] do
      test "consumes the matched #{offset} drift step and rejects its replay" do
        %{user: user} = bootstrap_owner_fixture()
        {:ok, setup} = Accounts.enable_totp_for_user(user)
        at = totp_test_time(setup.secret)
        step = div(DateTime.to_unix(at), 30) + unquote(offset)
        code = totp_for_step(setup.secret, step)
        clock = fn -> at end
        assert :ok = MFA.verify_second_factor(user, code, nil, %{}, clock: clock)
        assert Repo.reload!(setup.setting).last_used_step == step
        result = MFA.verify_second_factor(user, code, nil, %{}, clock: clock)
        assert result == {:error, :invalid_totp_code}
        assert Repo.reload!(setup.setting).last_used_step == step
      end
    end

    test "advances only for newer in-window codes and leaves invalid and rolled-back attempts unconsumed" do
      %{user: user} = bootstrap_owner_fixture()
      {:ok, setup} = Accounts.enable_totp_for_user(user)
      at = totp_test_time(setup.secret)
      step = div(DateTime.to_unix(at), 30)
      clock = fn -> at end
      invalid = MFA.verify_second_factor(user, "not-a-code", nil, %{}, clock: clock)
      assert invalid == {:error, :totp_required}
      assert is_nil(Repo.reload!(setup.setting).last_used_step)
      outside = MFA.verify_second_factor(user, totp_for_step(setup.secret, step - 2), nil, %{}, clock: clock)
      assert outside == {:error, :invalid_totp_code}
      assert is_nil(Repo.reload!(setup.setting).last_used_step)

      assert {:error, :cancelled} =
               Repo.transaction(fn ->
                 :ok = MFA.verify_second_factor(user, totp_for_step(setup.secret, step), nil, %{}, clock: clock)
                 Repo.rollback(:cancelled)
               end)

      assert is_nil(Repo.reload!(setup.setting).last_used_step)
      assert :ok = MFA.verify_second_factor(user, totp_for_step(setup.secret, step), nil, %{}, clock: clock)
      previous = MFA.verify_second_factor(user, totp_for_step(setup.secret, step - 1), nil, %{}, clock: clock)
      assert previous == {:error, :invalid_totp_code}
      assert :ok = MFA.verify_second_factor(user, totp_for_step(setup.secret, step + 1), nil, %{}, clock: clock)
      assert Repo.reload!(setup.setting).last_used_step == step + 1
    end

    for state <- [:disabled, :deleted] do
      test "#{state} operator cannot consume a factor through a stale user struct" do
        %{user: user} = bootstrap_owner_fixture()
        {:ok, setup} = Accounts.enable_totp_for_user(user)

        changes =
          case unquote(state) do
            :disabled -> [status: "disabled"]
            :deleted -> [deleted_at: DateTime.utc_now()]
          end

        user |> change(changes) |> Repo.update!()
        result = MFA.verify_second_factor(user, Accounts.current_totp_code(setup.secret), nil, %{})
        assert result == {:error, :invalid_credentials}
        assert is_nil(Repo.reload!(setup.setting).last_used_step)
      end
    end

    test "the verifier refuses a successfully consumed code" do
      %{user: user} = bootstrap_owner_fixture()
      {:ok, setup} = Accounts.enable_totp_for_user(user)
      code = Accounts.current_totp_code(setup.secret)
      assert :ok = MFA.verify_second_factor(user, code, nil, %{})
      result = MFA.verify_second_factor(user, code, nil, %{})
      assert result == {:error, :invalid_totp_code}
    end

    for path <- [:password, :pending] do
      test "#{path} login issues only one session for the same code" do
        %{user: user} = bootstrap_owner_fixture()
        {:ok, setup} = Accounts.enable_totp_for_user(user)
        code = Accounts.current_totp_code(setup.secret)
        attrs = %{"email" => user.email, "password" => valid_user_password(), "totp_code" => code}

        login = fn ->
          result =
            case unquote(path) do
              :password -> Accounts.login_user(attrs)
              :pending -> Accounts.complete_second_factor_login(user.id, attrs)
            end

          case result do
            {:ok, _} -> :session_issued
            {:error, reason} -> reason
          end
        end

        before_count = Repo.aggregate(from(s in Session, where: s.user_id == ^user.id), :count)
        assert login.() == :session_issued
        assert login.() == :invalid_totp_code
        assert Repo.aggregate(from(s in Session, where: s.user_id == ^user.id), :count) == before_count + 1
      end
    end
  end

  describe "login_user/2" do
    test "rejects invalid credentials safely and creates sessions for valid credentials" do
      %{user: user} = bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:error, :invalid_credentials} =
               Accounts.login_user(%{"email" => user.email, "password" => "wrong-password"})

      assert {:ok, %{user: logged_in, token: token}} =
               Accounts.login_user(%{
                 "email" => "OWNER@example.com",
                 "password" => valid_user_password()
               })

      assert logged_in.id == user.id
      assert Accounts.get_user_by_session_token(token)
      assert Repo.get_by(AuditEvent, action: "auth.login", actor_user_id: user.id)
    end

    test "requires TOTP and consumes recovery codes only once" do
      %{user: user} = bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      {:ok, %{secret: secret, recovery_codes: [recovery_code | _]}} =
        Accounts.enable_totp_for_user(user)

      setting = Repo.get_by!(TOTPSetting, user_id: user.id)
      assert setting.status == "active"
      refute setting.secret_ciphertext == secret
      assert Repo.aggregate(from(c in RecoveryCode, where: c.user_id == ^user.id), :count) == 10

      assert {:error, :totp_required} =
               Accounts.login_user(%{"email" => user.email, "password" => valid_user_password()})

      assert {:ok, %{token: recovery_token}} =
               Accounts.complete_second_factor_login(user.id, %{"recovery_code" => recovery_code})

      assert Accounts.get_user_by_session_token(recovery_token)

      assert {:error, :invalid_recovery_code} =
               Accounts.login_user(%{
                 "email" => user.email,
                 "password" => valid_user_password(),
                 "recovery_code" => recovery_code
               })

      assert {:ok, %{token: totp_token}} =
               Accounts.complete_second_factor_login(user.id, %{
                 "totp_code" => Accounts.current_totp_code(secret)
               })

      assert Accounts.get_user_by_session_token(totp_token)
      assert Repo.get_by(AuditEvent, action: "auth.recovery_code_used", actor_user_id: user.id)
    end
  end

  describe "delete_user_session_token/1" do
    test "get_user_by_session_token reads without touching session state" do
      %{user: user, token: token} = bootstrap_owner_fixture()

      session =
        Repo.one!(from s in Session, where: s.user_id == ^user.id and s.status == "active")

      assert is_nil(session.last_seen_at)

      assert Accounts.get_user_by_session_token(token)
      assert is_nil(Repo.reload!(session).last_seen_at)

      assert Accounts.authenticate_session_token(token)
      assert %DateTime{} = Repo.reload!(session).last_seen_at
    end

    test "revokes a stored operator session" do
      %{token: token} = bootstrap_owner_fixture()

      assert Accounts.get_user_by_session_token(token)
      assert Accounts.delete_user_session_token(token) == :ok
      refute Accounts.get_user_by_session_token(token)
    end

    test "lists active browser sessions and revokes other sessions" do
      %{user: user, token: current_token} =
        bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:ok, %{token: other_token}} =
               Accounts.login_user(
                 %{
                   "email" => user.email,
                   "password" => valid_user_password()
                 },
                 %{user_agent: "Parallel Browser", ip_address: "203.0.113.44"}
               )

      sessions = Accounts.list_user_sessions(user, current_token)

      assert Enum.count(sessions) == 2
      assert [current_session] = Enum.filter(sessions, & &1.current?)
      assert current_session.user_agent in [nil, ""]

      assert [other_session] = Enum.reject(sessions, & &1.current?)
      assert other_session.user_agent == "Parallel Browser"
      assert other_session.ip_address == "203.0.113.44"
      refute Map.has_key?(other_session, :session_token_hash)

      assert {:ok, 1} =
               Accounts.revoke_other_user_sessions(user, current_token, %{
                 request_id: "revoke-other-sessions"
               })

      assert Accounts.get_user_by_session_token(current_token)
      refute Accounts.get_user_by_session_token(other_token)
      assert Repo.get_by(AuditEvent, action: "auth.sessions_revoked", actor_user_id: user.id)
    end

    test "revokes one active browser session by id" do
      %{user: user, token: current_token} =
        bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:ok, %{token: other_token}} =
               Accounts.login_user(
                 %{"email" => user.email, "password" => valid_user_password()},
                 %{user_agent: "Other Browser", ip_address: "198.51.100.44"}
               )

      [other_session] =
        user
        |> Accounts.list_user_sessions(current_token)
        |> Enum.reject(& &1.current?)

      assert {:ok, %{current?: false, revoked_count: 1}} =
               Accounts.revoke_user_session(user, other_session.id, current_token, %{
                 request_id: "revoke-one-session"
               })

      assert Accounts.get_user_by_session_token(current_token)
      refute Accounts.get_user_by_session_token(other_token)
      assert Repo.get_by(AuditEvent, action: "auth.session_revoked", actor_user_id: user.id)
    end
  end

  describe "change_user_password/3" do
    test "updates the password, rotates sessions, and audits the change" do
      %{user: user, token: bootstrap_token} =
        bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:ok, %{token: login_token}} =
               Accounts.login_user(%{
                 "email" => user.email,
                 "password" => valid_user_password()
               })

      assert Accounts.get_user_by_session_token(bootstrap_token)
      assert Accounts.get_user_by_session_token(login_token)

      assert {:ok, %{user: changed_user, token: rotated_token}} =
               Accounts.change_user_password(
                 user,
                 %{"new_password" => "new-bootstrap-pass-456"},
                 %{ip_address: "203.0.113.20", user_agent: "test-agent"}
               )

      assert changed_user.id == user.id
      refute rotated_token in [bootstrap_token, login_token]
      refute Accounts.get_user_by_session_token(bootstrap_token)
      refute Accounts.get_user_by_session_token(login_token)
      assert Accounts.get_user_by_session_token(rotated_token)

      refute Accounts.get_user_by_email_and_password(user.email, valid_user_password())

      assert Accounts.get_user_by_email_and_password(user.email, "new-bootstrap-pass-456").id ==
               user.id

      assert Repo.get_by(AuditEvent, action: "auth.password_change", actor_user_id: user.id)

      assert Repo.aggregate(
               from(s in Session, where: s.user_id == ^user.id and s.status == "active"),
               :count
             ) == 1
    end

    test "rejects invalid new passwords without mutating the stored hash" do
      %{user: user, token: token} = bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:error, %Ecto.Changeset{} = changeset} =
               Accounts.change_user_password(user, %{"new_password" => "short"})

      assert %{password: ["should be at least 8 character(s)"]} = errors_on(changeset)

      assert Accounts.get_user_by_email_and_password(user.email, valid_user_password())
      assert Accounts.get_user_by_session_token(token)
      refute Repo.get_by(AuditEvent, action: "auth.password_change", actor_user_id: user.id)
    end
  end

  describe "audit log wrapper" do
    test "records attrs-map events and normalizes request metadata" do
      %{user: user} = bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:ok, event} =
               AuditLog.record_user_event(user, %{
                 action: "auth.logout",
                 target_type: "session",
                 metadata: %{request_id: "audit-wrapper-request", ip_address: "203.0.113.42"},
                 details: %{reason: "manual"}
               })

      event = Repo.reload!(event)
      assert event.actor_user_id == user.id
      assert event.correlation_id == "audit-wrapper-request"
      assert event.ip_address == "203.0.113.42"
      assert event.details == %{"reason" => "manual"}
    end

    test "retains only normalized bounded ingress peer provenance" do
      %{user: user} = bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:ok, event} =
               AuditLog.record_user_event(user, %{
                 action: "auth.logout",
                 target_type: "session",
                 metadata: %{
                   ingress_peer_provenance: %{
                     immediate_peer_ip: "2001:db8::42",
                     client_ip_source: :x_forwarded_for,
                     inspected_hops: 500,
                     raw_headers: %{"x-forwarded-for" => "must-not-persist"},
                     token: "must-not-persist"
                   }
                 },
                 details: %{reason: "manual"}
               })

      assert Repo.reload!(event).details == %{
               "ingress_peer_provenance" => %{
                 "client_ip_source" => "x_forwarded_for",
                 "immediate_peer_ip" => "2001:db8::42",
                 "inspected_hops" => 32
               },
               "reason" => "manual"
             }
    end

    test "omits malformed ingress peer provenance without raising" do
      %{user: user} = bootstrap_owner_fixture(%{"email" => "owner@example.com"})

      assert {:ok, event} =
               AuditLog.record_user_event(user, %{
                 action: "auth.logout",
                 target_type: "session",
                 metadata: %{
                   ingress_peer_provenance: %{
                     immediate_peer_ip: <<255, 0, 44>>,
                     client_ip_source: "x_forwarded_for",
                     inspected_hops: 2
                   }
                 },
                 details: %{reason: "manual"}
               })

      assert Repo.reload!(event).details == %{"reason" => "manual"}
    end

    test "ignores invalid actors without writing an audit row" do
      assert {:ok, nil} =
               AuditLog.record_user_event(:not_a_user, %{
                 action: "auth.logout",
                 target_type: "session"
               })

      refute Repo.get_by(AuditEvent, action: "auth.logout")
    end
  end

  describe "operator schema" do
    @tag :operator_schema
    test "persists password_change_required with a database default of false" do
      default_user =
        %User{}
        |> User.bootstrap_changeset(valid_bootstrap_attributes(%{"email" => unique_user_email()}))
        |> Repo.insert!()

      assert Repo.reload!(default_user).password_change_required == false

      required_user =
        %User{}
        |> User.bootstrap_changeset(valid_bootstrap_attributes(%{"email" => unique_user_email()}))
        |> Ecto.Changeset.put_change(:password_change_required, true)
        |> Repo.insert!()

      assert Repo.reload!(required_user).password_change_required == true
    end
  end

  defp totp_test_time(secret) do
    Enum.find_value(0..100, fn offset ->
      at = DateTime.add(~U[2060-01-01 00:00:15.000000Z], offset * 150, :second)
      step = div(DateTime.to_unix(at), 30)
      codes = Enum.map(-2..1, &totp_for_step(secret, step + &1))
      if length(Enum.uniq(codes)) == 4, do: at
    end)
  end

  defp totp_for_step(secret, step) do
    hmac = :crypto.mac(:hmac, :sha, Base.decode32!(secret, padding: false), <<step::64>>)
    offset = Bitwise.band(:binary.last(hmac), 15)
    <<value::32>> = binary_part(hmac, offset, 4)
    value |> Bitwise.band(0x7FFFFFFF) |> rem(1_000_000) |> Integer.to_string() |> String.pad_leading(6, "0")
  end
end

defmodule CodexPooler.AccountsTOTPEnrollmentConcurrencyTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import CodexPooler.AccountsFixtures
  import CodexPooler.UnboxedFixture, only: [run_unboxed: 1]
  import Ecto.Query

  alias CodexPooler.Accounts
  alias CodexPooler.Accounts.{RecoveryCode, Session, TOTPSetting}
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @budget 10_000

  test "first enrollments serialize on the user before a TOTP row is visible" do
    %{user: user} = committed_bootstrap_owner_fixture!()
    parent = self()
    barrier = make_ref()
    handler_id = {__MODULE__, barrier}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.observe_enrollment_lock/4, barrier)
    supervisor = start_supervised!({Task.Supervisor, []})

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            {:ok, setup} = Accounts.enable_totp_for_user(user)
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {barrier, :holder, backend, setup.setting.id})

            code_ids = Repo.all(from c in RecoveryCode, where: c.totp_setting_id == ^setup.setting.id, order_by: c.id, select: c.id)

            receive do
              {^barrier, :release} -> {setup.setting, code_ids}
            after
              @budget -> raise "enrollment commit release missing"
            end
          end)
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    assert_receive {^barrier, :holder, holder_backend, setting_id}, @budget
    refute run_unboxed(fn -> Repo.exists?(from s in TOTPSetting, where: s.user_id == ^user.id) end)

    waiter =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.checkout(fn ->
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {barrier, :waiter, backend})

            Process.put({__MODULE__, :enrollment_probe}, barrier)

            lock_proof =
              try do
                Repo.transaction(fn ->
                  Repo.query!("SET LOCAL lock_timeout = '200ms'")
                  Accounts.enable_totp_for_user(user)
                end)

                :did_not_wait
              rescue
                error in Postgrex.Error ->
                  {error.postgres.code, Process.get({__MODULE__, :user_lock_query}, false)}
              end

            send(parent, {barrier, :lock_proof, lock_proof})

            receive do
              {^barrier, :retry} -> Accounts.enable_totp_for_user(user)
            after
              @budget -> raise "enrollment retry release missing"
            end
          end)
        end)
      end)

    waiter_monitor = Process.monitor(waiter.pid)
    assert_receive {^barrier, :waiter, waiter_backend}, @budget
    assert waiter_backend != holder_backend
    assert_receive {^barrier, :lock_proof, lock_proof}, @budget
    send(holder.pid, {barrier, :release})
    assert {:ok, {setting, original_code_ids}} = Task.await(holder, @budget)
    send(waiter.pid, {barrier, :retry})

    outcome =
      case Task.await(waiter, @budget) do
        {:ok, _setup} -> :enrolled
        {:error, reason} -> reason
      end

    assert outcome == :totp_already_enabled
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @budget
    assert_receive {:DOWN, ^waiter_monitor, :process, _, :normal}, @budget
    assert lock_proof == {:lock_not_available, true}

    {unchanged?, code_ids, generation} =
      run_unboxed(fn ->
        current = Repo.get_by!(TOTPSetting, user_id: user.id)
        {current == setting, Repo.all(from c in RecoveryCode, where: c.totp_setting_id == ^setting_id and c.status == "active", order_by: c.id, select: c.id), current.recovery_generation}
      end)

    assert unchanged?
    assert length(code_ids) == 10
    assert code_ids == original_code_ids
    assert generation == 1
  end

  test "independent concurrent logins consume one step and issue exactly one session" do
    %{user: user} = committed_bootstrap_owner_fixture!()
    {:ok, setup} = run_unboxed(fn -> Accounts.enable_totp_for_user(user) end)
    code = Accounts.current_totp_code(setup.secret)
    parent = self()
    barrier = make_ref()
    handler_id = {__MODULE__, barrier}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.observe_enrollment_lock/4, barrier)
    supervisor = start_supervised!({Task.Supervisor, []})
    count_sessions = fn -> Repo.aggregate(from(s in Session, where: s.user_id == ^user.id), :count) end
    before_count = run_unboxed(count_sessions)

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            {:ok, _login} = Accounts.complete_second_factor_login(user.id, %{"totp_code" => code})
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {barrier, :holder, backend})

            receive do
              {^barrier, :release} -> :consumed
            after
              @budget -> raise "factor commit release missing"
            end
          end)
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    assert_receive {^barrier, :holder, holder_backend}, @budget
    assert run_unboxed(fn -> is_nil(Repo.reload!(setup.setting).last_used_step) end)

    waiter =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.checkout(fn ->
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            Process.put({__MODULE__, :enrollment_probe}, barrier)

            proof =
              try do
                Repo.transaction(fn ->
                  Repo.query!("SET LOCAL lock_timeout = '200ms'")
                  Accounts.complete_second_factor_login(user.id, %{"totp_code" => code})
                end)

                :did_not_wait
              rescue
                error in Postgrex.Error -> {error.postgres.code, Process.get({__MODULE__, :user_lock_query}, false)}
              end

            send(parent, {barrier, :proof, backend, proof})

            receive do
              {^barrier, :retry} ->
                case Accounts.complete_second_factor_login(user.id, %{"totp_code" => code}) do
                  {:ok, _login} -> :session_issued
                  {:error, reason} -> reason
                end
            after
              @budget -> raise "factor retry release missing"
            end
          end)
        end)
      end)

    waiter_monitor = Process.monitor(waiter.pid)
    assert_receive {^barrier, :proof, waiter_backend, proof}, @budget
    send(holder.pid, {barrier, :release})
    assert {:ok, :consumed} = Task.await(holder, @budget)
    send(waiter.pid, {barrier, :retry})
    outcome = Task.await(waiter, @budget)
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @budget
    assert_receive {:DOWN, ^waiter_monitor, :process, _, :normal}, @budget
    assert waiter_backend != holder_backend
    assert proof == {:lock_not_available, true}
    assert outcome == :invalid_totp_code
    assert run_unboxed(count_sessions) == before_count + 1

    run_unboxed(fn ->
      current = Repo.reload!(setup.setting)
      unchanged_secret? = current.secret_ciphertext == setup.setting.secret_ciphertext
      assert unchanged_secret?
      assert is_integer(current.last_used_step)
      assert Repo.aggregate(from(c in RecoveryCode, where: c.user_id == ^user.id and c.status == "active"), :count) == 10
    end)
  end

  @doc false
  def observe_enrollment_lock(_event, _measurements, metadata, barrier) do
    if Process.get({__MODULE__, :enrollment_probe}) == barrier and match?({:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}}, metadata.result) do
      query = metadata.query
      user_lock? = String.starts_with?(query, "SELECT") and String.contains?(query, ~s(FROM "users")) and String.ends_with?(query, "FOR UPDATE")
      Process.put({__MODULE__, :user_lock_query}, user_lock?)
    end
  end
end
