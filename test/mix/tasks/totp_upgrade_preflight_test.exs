defmodule CodexPooler.TOTPUpgradePreflightTest do
  use CodexPooler.UnixIntegrationCase, async: false, tools: ~w(sh elixir)

  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag :tmp_dir
  @script "scripts/self-host/totp-upgrade-preflight.sh"
  @legacy_key :crypto.hash(:sha256, "codex-pooler-local-totp-key")
  @key String.duplicate("k", 32)
  @secret Base.encode32(:binary.copy(<<7>>, 20), padding: false)

  setup %{tmp_dir: dir} do
    on_exit(fn -> File.rm_rf!(dir) end)
    File.chmod!(dir, 0o700)
    schema = "totp_preflight_#{System.unique_integer([:positive])}"
    acquired = :atomics.new(1, [])
    on_exit(fn -> if :atomics.get(acquired, 1) == 1, do: sql!("DROP SCHEMA #{schema} CASCADE") end)
    sql!("CREATE SCHEMA #{schema}")
    :atomics.put(acquired, 1, 1)
    sql!("CREATE TABLE #{schema}.totp_settings (status text, secret_key_version text, secret_ciphertext bytea)")

    boot = Path.join(dir, "old runtime fixture.exs")

    File.write!(boot, """
    :ok = :code.add_paths(Enum.map(Path.wildcard(System.fetch_env!("TOTP_TEST_EBIN") <> "/*/ebin"), &String.to_charlist/1))
    ["argument with spaces", "rpc", code] = System.argv()
    data = System.fetch_env!("TOTP_TEST_CONFIG") |> File.read!() |> Jason.decode!()
    key = if data["key"], do: Base.decode64!(data["key"]), else: nil
    Application.put_env(:codex_pooler, CodexPooler.Accounts, totp_encryption_key: key, totp_key_version: "v1")
    {:ok, _} = Application.ensure_all_started(:ecto_sql)
    {:ok, _} = Application.ensure_all_started(:postgrex)
    options = [hostname: data["hostname"], port: data["port"], username: data["username"], password: data["password"], database: data["database"], pool: DBConnection.ConnectionPool, pool_size: 1, parameters: [search_path: data["schema"]]]
    {:ok, _} = CodexPooler.Repo.start_link(options)
    Code.eval_string(code)
    """)

    transport = Path.join(dir, "old release transport")
    File.write!(transport, "#!/bin/sh\nexec \"$TOTP_TEST_ELIXIR\" \"$TOTP_TEST_BOOT\" \"$@\"\n")
    File.chmod!(transport, 0o700)
    %{dir: dir, schema: schema, boot: boot, transport: transport}
  end

  test "actual CLI authenticates every pending active and disabled row without changing bytes", context do
    for {status, version} <- [{"pending", "v1"}, {"active", "v1"}, {"disabled", "older"}], do: insert!(context, ciphertext(@key), status, version)
    before = snapshot(context)
    assert {%{"disposition" => "ready", "decrypt_ok" => 3, "checked" => 3, "versions" => %{"current" => 2, "other" => 1}}, 0} = run(context, @key)
    assert snapshot(context) == before
  end

  test "encoded valid key has the same effective bytes and a valid empty inventory is ready", context do
    insert!(context, ciphertext(@key))
    assert {%{"disposition" => "ready", "key_source" => "configured_base64", "decoded_bytes" => 32}, 0} = run(context, Base.encode64(@key))
    sql!("DELETE FROM #{context.schema}.totp_settings")
    assert {%{"disposition" => "ready", "total" => 0}, 0} = run(context, Base.encode64(@key))
  end

  for shape <- [:missing, :empty, :malformed, :oversized] do
    test "#{shape} configuration with no enrolled rows requires explicit key replacement", context do
      key =
        case unquote(shape) do
          :missing -> nil
          :empty -> ""
          :malformed -> "synthetic-not-base64!"
          :oversized -> Base.encode64(:binary.copy(<<3>>, 48))
        end

      assert {%{"disposition" => "replace_totp_key", "total" => 0}, 2} = run(context, key)
    end
  end

  for representation <- [:missing, :raw, :base64] do
    test "#{representation} public fallback never becomes upgrade-ready", context do
      insert!(context, ciphertext(@legacy_key))

      key =
        case unquote(representation) do
          :missing -> nil
          :raw -> @legacy_key
          :base64 -> Base.encode64(@legacy_key)
        end

      assert {%{"disposition" => "offline_reencryption_required", "known_legacy_key" => true, "decrypt_ok" => 1}, 2} = run(context, key)
    end
  end

  test "wrong key and malformed ciphertext refuse without exposing values", context do
    insert!(context, ciphertext(String.duplicate("z", 32)))
    assert {%{"disposition" => "restore_key_or_investigate", "decrypt_failed" => 1}, 2} = run(context, @key)
    insert!(context, <<1, 2, 3>>)
    assert {%{"disposition" => "restore_key_or_investigate", "malformed" => 1}, 2} = run(context, @key)
  end

  test "authenticated invalid plaintext is refused before any recovery disposition", context do
    for key <- [@key, @legacy_key] do
      sql!("DELETE FROM #{context.schema}.totp_settings")
      insert!(context, ciphertext(key, String.duplicate("!", 32)))
      assert {%{"disposition" => "restore_key_or_investigate", "invalid_plaintext" => 1, "decrypt_ok" => 0}, 2} = run(context, key)
    end
  end

  test "invalid configured key with ciphertext never silently falls back", context do
    insert!(context, ciphertext(@legacy_key))
    assert {%{"disposition" => "restore_key_or_investigate", "valid_key_shape" => false, "decrypt_ok" => 0}, 2} = run(context, Base.encode64(:binary.copy(<<3>>, 48)))
  end

  test "row cap refuses complete certification instead of inspecting a partial inventory", context do
    sql!("INSERT INTO #{context.schema}.totp_settings SELECT 'active', 'v1', decode('010203','hex') FROM generate_series(1, 10001)")
    assert {%{"disposition" => "inventory_limit_exceeded", "total" => 10_001, "checked" => 0, "inventory_complete" => false}, 2} = run(context, @key)
  end

  test "real read-only transaction rejects an injected write and preserves the row", context do
    insert!(context, ciphertext(@key))
    before = snapshot(context)
    copy = copy_scripts(context.dir)
    code = Path.join(copy, "totp-upgrade-preflight.exs")
    File.write!(code, String.replace(File.read!(code), "SELECT count(*) FROM totp_settings", "WITH changed AS (DELETE FROM totp_settings RETURNING 1) SELECT count(*) FROM changed"))
    assert {%{"disposition" => "preflight_failed"}, 3} = run(context, @key, Path.join(copy, "totp-upgrade-preflight.sh"))
    assert snapshot(context) == before
  end

  test "transport errors are fixed metadata and no shell evaluation occurs", context do
    File.write!(context.transport, "#!/bin/sh\nprintf '%s\\n' 'synthetic-sensitive-error' >&2\nexit 42\n")
    assert {%{"disposition" => "transport_failed"}, 3} = run(context, @key)
    assert {help, 0} = System.cmd("sh", [@script, "--help"])
    assert help =~ "-- RELEASE_COMMAND"
    assert {_usage, 3} = System.cmd("sh", [@script], stderr_to_stdout: true)
  end

  test "release packaging copies the executable and its old-runtime program", context do
    workflow = File.read!(".github/workflows/build.yml")
    [_, assets] = String.split(workflow, "- name: Prepare release assets", parts: 2)
    copies = assets |> String.split("\n") |> Enum.filter(&String.starts_with?(String.trim(&1), "cp ")) |> Enum.join("\n")
    directory = Path.join(context.dir, "release asset")
    File.mkdir_p!(Path.join(directory, "scripts/self-host"))
    assert {_, 0} = System.cmd("sh", ["-c", "asset_dir=$1\n" <> copies, "sh", directory], stderr_to_stdout: true)
    script = Path.join(directory, @script)
    assert File.regular?(script)
    assert File.regular?(Path.rootname(script) <> ".exs")
    assert {%{"disposition" => "ready", "total" => 0}, 0} = run(context, @key, script)
  end

  defp run(context, key, script \\ @script) do
    config = Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database]) |> Map.new() |> Map.merge(%{schema: context.schema, key: if(key, do: Base.encode64(key), else: nil)})
    file = Path.join(context.dir, "private-config.json")
    File.touch!(file)
    File.chmod!(file, 0o600)
    File.write!(file, Jason.encode!(config))
    {output, code} = System.cmd("sh", [script, "--", context.transport, "argument with spaces"], env: [{"TOTP_TEST_ELIXIR", System.find_executable("elixir")}, {"TOTP_TEST_BOOT", context.boot}, {"TOTP_TEST_CONFIG", file}, {"TOTP_TEST_EBIN", Path.expand(Path.join(Mix.Project.build_path(), "lib"))}], stderr_to_stdout: true)
    refute output =~ @secret
    refute output =~ "synthetic-sensitive-error"
    if is_binary(key) and key != "", do: refute(output =~ key)
    {Jason.decode!(output), code}
  end

  defp ciphertext(key, secret \\ @secret) do
    nonce = :crypto.strong_rand_bytes(12)
    {encrypted, tag} = :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, secret, "totp", true)
    nonce <> tag <> encrypted
  end

  defp insert!(context, ciphertext, status \\ "active", version \\ "v1"), do: sql!("INSERT INTO #{context.schema}.totp_settings VALUES ($1,$2,$3)", [status, version, ciphertext])
  defp snapshot(context), do: sql!("SELECT * FROM #{context.schema}.totp_settings ORDER BY status,secret_key_version").rows
  defp sql!(statement, params \\ []), do: Sandbox.unboxed_run(Repo, fn -> Repo.query!(statement, params) end)

  defp copy_scripts(dir) do
    copy = Path.join(dir, "script copy")
    File.mkdir!(copy)
    for suffix <- ["sh", "exs"], do: File.cp!("scripts/self-host/totp-upgrade-preflight.#{suffix}", Path.join(copy, "totp-upgrade-preflight.#{suffix}"))
    copy
  end
end
