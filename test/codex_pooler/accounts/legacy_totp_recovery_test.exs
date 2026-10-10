defmodule CodexPooler.Accounts.LegacyTOTPRecoveryTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounts
  alias CodexPooler.Accounts.{LegacyTOTPRecovery, MFA, RecoveryCode, TOTPSetting, User}

  setup do
    schema = "totp_recovery_#{System.unique_integer([:positive])}"
    connection = Repo.config() |> Keyword.drop([:name, :pool, :pool_size, :pool_count])
    {:ok, admin} = Postgrex.start_link(connection)
    Process.unlink(admin)

    on_exit(fn ->
      Postgrex.query!(admin, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [])
      GenServer.stop(admin)
    end)

    Postgrex.query!(admin, "CREATE SCHEMA #{schema}", [])

    for table <- ~w(users totp_settings recovery_codes audit_events) do
      Postgrex.query!(admin, "CREATE TABLE #{schema}.#{table} (LIKE public.#{table} INCLUDING ALL)", [])
    end

    dynamic = start_supervised!({Repo, name: nil, pool: DBConnection.ConnectionPool, pool_size: 2, parameters: [search_path: schema]})
    previous = Repo.put_dynamic_repo(dynamic)
    on_exit(fn -> Repo.put_dynamic_repo(previous) end)
    key = :crypto.strong_rand_bytes(32)
    %{key: key, schema: schema, admin: admin}
  end

  test "all statuses preserve identity, replay and recovery rows while changing only encryption", %{key: key} do
    fixtures = Enum.map(~w(active pending disabled), &legacy_fixture/1)
    before_settings = Repo.all(from s in TOTPSetting, order_by: s.id)
    before_codes = Repo.all(from c in RecoveryCode, order_by: c.id)
    assert {:ok, %{rows: 3}} = LegacyTOTPRecovery.reencrypt(Base.encode64(key), "private-v2", "all-writers-stopped")
    after_settings = Repo.all(from s in TOTPSetting, order_by: s.id)

    Enum.zip(before_settings, after_settings)
    |> Enum.each(fn {before_setting, after_setting} ->
      preserved? = Map.drop(Map.from_struct(before_setting), [:secret_ciphertext, :secret_key_version]) == Map.drop(Map.from_struct(after_setting), [:secret_ciphertext, :secret_key_version])
      assert preserved?
      assert before_setting.secret_ciphertext != after_setting.secret_ciphertext
      assert after_setting.secret_key_version == "private-v2"
    end)

    recovery_preserved? = Repo.all(from c in RecoveryCode, order_by: c.id) == before_codes
    assert recovery_preserved?
    assert {:ok, %{rows: 3}} = LegacyTOTPRecovery.verify(key)
    previous = CodexPooler.TestAppEnv.restore_on_exit(Accounts)
    Application.put_env(:codex_pooler, Accounts, Keyword.put(previous || [], :totp_encryption_key, key))
    {user, setting, secret, recovery} = hd(fixtures)
    code = MFA.current_totp_code(secret)
    assert {:error, :invalid_totp_code} = MFA.verify_second_factor(user, code, nil, %{})
    assert Repo.get!(TOTPSetting, setting.id).last_used_step == setting.last_used_step
    next = DateTime.add(DateTime.utc_now(), 90, :second)
    assert :ok = MFA.verify_second_factor(user, code_at(secret, next), nil, %{}, clock: fn -> next end)
    assert :ok = MFA.verify_second_factor(user, nil, recovery, %{})
    assert {:error, :invalid_recovery_code} = MFA.verify_second_factor(user, nil, recovery, %{})
    assert {:error, :ciphertext_not_recoverable} = LegacyTOTPRecovery.reencrypt(key, "private-v2", "all-writers-stopped")
    assert {:ok, %{rows: 3}} = LegacyTOTPRecovery.verify(key)
  end

  for bad <- [:short, :wrong_key, :invalid_plaintext, :corrupt_tag] do
    test "#{bad} final row aborts all writes", %{key: key} do
      legacy_fixture("active")
      {_user, setting, _secret, _recovery} = legacy_fixture("disabled")

      encrypted =
        case unquote(bad) do
          :short ->
            <<1, 2>>

          :wrong_key ->
            encrypt(key, secret())

          :invalid_plaintext ->
            encrypt(legacy_key(), "invalid")

          :corrupt_tag ->
            <<nonce::binary-size(12), first, rest::binary>> = encrypt(legacy_key(), secret())
            nonce <> <<Bitwise.bxor(first, 1)>> <> rest
        end

      setting |> Ecto.Changeset.change(secret_ciphertext: encrypted) |> Repo.update!()
      before = snapshot()
      assert {:error, :ciphertext_not_recoverable} = LegacyTOTPRecovery.reencrypt(key, "private-v2", "all-writers-stopped")
      unchanged? = snapshot() == before
      assert unchanged?
    end
  end

  test "a real constraint failure after one update rolls back that update", %{key: key} do
    legacy_fixture("active")
    legacy_fixture("disabled")
    [first, second] = Repo.all(from s in TOTPSetting, order_by: s.id)
    before = snapshot()
    Repo.query!("ALTER TABLE totp_settings ADD CONSTRAINT reject_second CHECK (id <> '#{second.id}'::uuid OR secret_key_version <> 'private-v2')")
    assert first.id < second.id
    assert {:error, :database_failed} = LegacyTOTPRecovery.reencrypt(key, "private-v2", "all-writers-stopped")
    unchanged? = snapshot() == before
    assert unchanged?
  end

  test "another real connection holding a table lock causes immediate refusal", %{key: key, schema: schema, admin: admin} do
    legacy_fixture("active")
    Postgrex.query!(admin, "BEGIN", [])
    Postgrex.query!(admin, "LOCK TABLE #{schema}.totp_settings IN ACCESS SHARE MODE", [])

    try do
      assert {:error, :writers_active} = LegacyTOTPRecovery.reencrypt(key, "private-v2", "all-writers-stopped")
    after
      Postgrex.query!(admin, "ROLLBACK", [])
    end

    assert {:ok, %{rows: 1}} = LegacyTOTPRecovery.reencrypt(key, "private-v2", "all-writers-stopped")
  end

  test "stable-column operation supports the old schema without replay counter", %{key: key} do
    legacy_fixture("active")
    Repo.query!("ALTER TABLE totp_settings DROP COLUMN last_used_step")
    assert {:ok, %{rows: 1}} = LegacyTOTPRecovery.reencrypt(key, "private-v2", "all-writers-stopped")
    assert {:ok, %{rows: 1}} = LegacyTOTPRecovery.verify(key)
  end

  test "explicit acknowledgement, private destination and version are mandatory", %{key: key} do
    assert {:error, :offline_acknowledgement_required} = LegacyTOTPRecovery.reencrypt(key, "v2", nil)

    for invalid <- [nil, "", "not-base64", Base.encode64(:crypto.strong_rand_bytes(48))] do
      assert {:error, :invalid_destination_key} = LegacyTOTPRecovery.reencrypt(invalid, "v2", "all-writers-stopped")
    end

    assert {:error, :public_destination_forbidden} = LegacyTOTPRecovery.reencrypt(legacy_key(), "v2", "all-writers-stopped")
    assert {:error, :public_destination_forbidden} = LegacyTOTPRecovery.verify(Base.encode64(legacy_key()))
    assert {:error, :invalid_key_version} = LegacyTOTPRecovery.reencrypt(key, "  ", "all-writers-stopped")
    assert {:ok, %{rows: 0}} = LegacyTOTPRecovery.reencrypt(key, "v2", "all-writers-stopped")
    assert {:ok, %{rows: 0}} = LegacyTOTPRecovery.verify(key)
  end

  test "release mutation refuses the actual running application" do
    assert List.keymember?(Application.started_applications(), :codex_pooler, 0)

    assert_raise RuntimeError, "totp_maintenance failed reason=serving_application_started", fn ->
      CodexPooler.Release.reencrypt_legacy_totp()
    end
  end

  test "release verification on a running app prints only a count", %{key: key} do
    legacy_fixture("active")
    assert {:ok, %{rows: 1}} = LegacyTOTPRecovery.reencrypt(key, "v2", "all-writers-stopped")
    previous = CodexPooler.TestAppEnv.restore_on_exit(Accounts)
    Application.put_env(:codex_pooler, Accounts, Keyword.put(previous || [], :totp_encryption_key, key))
    output = ExUnit.CaptureIO.capture_io(fn -> assert :ok = CodexPooler.Release.verify_totp_encryption() end)
    assert output == "totp_maintenance operation=verify rows=1 result=ok\n"
  end

  test "release verification sanitizes unexpected configuration exceptions" do
    CodexPooler.TestAppEnv.restore_on_exit(Accounts)
    Application.put_env(:codex_pooler, Accounts, "synthetic-private-configuration")

    assert_raise RuntimeError, "totp_maintenance failed reason=maintenance_failed", fn ->
      CodexPooler.Release.verify_totp_encryption()
    end
  end

  defp legacy_fixture(status) do
    now = DateTime.utc_now()
    user = Repo.insert!(%User{email: "recovery-#{System.unique_integer([:positive])}@example.com", display_name: "Example", password_hash: "synthetic", status: "active", created_at: now, updated_at: now})
    secret = secret()
    setting = Repo.insert!(%TOTPSetting{user_id: user.id, secret_ciphertext: encrypt(legacy_key(), secret), secret_key_version: "v1", recovery_generation: 2, status: status, last_used_step: div(DateTime.to_unix(now), 30) + 1, created_at: now, updated_at: now})
    recovery = "EXAMPLE#{System.unique_integer([:positive])}"
    Repo.insert!(%RecoveryCode{user_id: user.id, totp_setting_id: setting.id, code_hash: :crypto.hash(:sha256, recovery), status: "active", created_at: now})

    for code_status <- ["used", "revoked"] do
      Repo.insert!(%RecoveryCode{user_id: user.id, totp_setting_id: setting.id, code_hash: :crypto.hash(:sha256, recovery <> code_status), status: code_status, created_at: now})
    end

    {user, setting, secret, recovery}
  end

  defp snapshot, do: Repo.query!("SELECT * FROM totp_settings ORDER BY id", [], log: false).rows

  defp code_at(secret, at) do
    key = Base.decode32!(secret, padding: false)
    digest = :crypto.mac(:hmac, :sha, key, <<div(DateTime.to_unix(at), 30)::unsigned-big-64>>)
    offset = Bitwise.band(:binary.last(digest), 15)
    <<_::binary-size(^offset), value::unsigned-big-32, _::binary>> = digest
    value |> Bitwise.band(0x7FFFFFFF) |> rem(1_000_000) |> Integer.to_string() |> String.pad_leading(6, "0")
  end

  defp secret, do: Base.encode32(:crypto.strong_rand_bytes(20), padding: false)
  defp legacy_key, do: :crypto.hash(:sha256, "codex-pooler-local-totp-key")

  defp encrypt(key, secret) do
    nonce = :crypto.strong_rand_bytes(12)
    {encrypted, tag} = :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, secret, "totp", true)
    nonce <> tag <> encrypted
  end
end
