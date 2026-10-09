defmodule CodexPooler.RuntimeConfigTest do
  use ExUnit.Case, async: false

  @required_env %{
    "DATABASE_URL" => "ecto://user:pass@example.invalid/db",
    "SECRET_KEY_BASE" => String.duplicate("a", 64),
    "PHX_SERVER" => "false",
    "PORT" => "4101",
    "CODEX_POOLER_TOTP_ENCRYPTION_KEY" => Base.encode64(:binary.copy(<<1>>, 32)),
    "CODEX_POOLER_UPSTREAM_SECRET_KEY" => String.duplicate("r", 32)
  }

  @tag :totp_key_contract
  test "production rejects absent empty malformed and wrong-size TOTP encryption keys without exposing values" do
    for {shape, value} <- [missing: nil, empty: "", malformed: "synthetic-invalid-key", short: Base.encode64(:crypto.strong_rand_bytes(31)), long: Base.encode64(:crypto.strong_rand_bytes(33))] do
      observed =
        with_env(Map.put(@required_env, "CODEX_POOLER_TOTP_ENCRYPTION_KEY", value), fn ->
          try do
            Config.Reader.read!("config/runtime.exs", env: :prod)
            %{shape: shape, refused: false, safe_message: false}
          rescue
            error -> %{shape: shape, refused: true, safe_message: Exception.message(error) == "CODEX_POOLER_TOTP_ENCRYPTION_KEY must be 32 raw bytes or base64-encoded 32 bytes"}
          end
        end)

      assert observed == %{shape: shape, refused: true, safe_message: true}
    end
  end

  @tag :totp_key_contract
  test "production retains a valid encoded TOTP key and its version unchanged" do
    key = Base.encode64(:crypto.strong_rand_bytes(32))

    with_env(Map.merge(@required_env, %{"CODEX_POOLER_TOTP_ENCRYPTION_KEY" => key, "CODEX_POOLER_TOTP_KEY_VERSION" => "synthetic-v2"}), fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)
      accounts = config[:codex_pooler][CodexPooler.Accounts]
      same_key? = accounts[:totp_encryption_key] == key
      assert same_key?
      assert accounts[:totp_key_version] == "synthetic-v2"
    end)
  end

  @tag :totp_key_contract
  test "production retains a legacy raw 32-byte TOTP key unchanged" do
    key = :crypto.strong_rand_bytes(32)
    # Environment values cannot contain NUL; a printable raw key exercises the existing branch.
    key = Base.encode16(key) |> binary_part(0, 32)

    with_env(Map.put(@required_env, "CODEX_POOLER_TOTP_ENCRYPTION_KEY", key), fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)
      unchanged? = config[:codex_pooler][CodexPooler.Accounts][:totp_encryption_key] == key
      assert unchanged?
    end)
  end

  @tag :totp_key_contract
  test "development and test runtime configuration keep their local TOTP fallback" do
    with_env(Map.put(@required_env, "CODEX_POOLER_TOTP_ENCRYPTION_KEY", nil), fn ->
      for environment <- [:dev, :test] do
        config = Config.Reader.read!("config/runtime.exs", env: environment)
        refute Keyword.has_key?(config[:codex_pooler], CodexPooler.Accounts)
      end
    end)
  end

  test "instance slot is explicit, bounded and never inferred from hostname" do
    for value <- [nil, "", "00000000-0000-0000-0000-000000000001/app"] do
      with_env(Map.merge(@required_env, %{"CODEX_POOLER_INSTANCE_SLOT_ID" => value, "HOSTNAME" => "reused-pod-name"}), fn ->
        config = Config.Reader.read!("config/runtime.exs", env: :prod)
        assert config[:codex_pooler][:instance_slot_id] == if(value == "", do: nil, else: value)
      end)
    end

    for value <- ["space invalid", "line\nbreak", String.duplicate("a", 201)] do
      with_env(Map.put(@required_env, "CODEX_POOLER_INSTANCE_SLOT_ID", value), fn ->
        assert_raise RuntimeError, ~r/CODEX_POOLER_INSTANCE_SLOT_ID must be/, fn ->
          Config.Reader.read!("config/runtime.exs", env: :prod)
        end
      end)
    end
  end

  test "prod endpoint http config keeps the configured port and binds IPv4" do
    with_env(@required_env, fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)
      endpoint_config = config[:codex_pooler][CodexPoolerWeb.Endpoint]

      assert endpoint_config[:http][:port] == 4101
      assert endpoint_config[:http][:ip] == {0, 0, 0, 0}
    end)
  end

  test "PHX_SERVER must be explicitly truthy to start the endpoint" do
    with_env(Map.put(@required_env, "PHX_SERVER", "false"), fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)
      endpoint_config = config[:codex_pooler][CodexPoolerWeb.Endpoint]

      refute endpoint_config[:server]
    end)

    with_env(Map.put(@required_env, "PHX_SERVER", "true"), fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)
      endpoint_config = config[:codex_pooler][CodexPoolerWeb.Endpoint]

      assert endpoint_config[:server]
    end)
  end

  test "production native compaction tracing remains off for every environment value" do
    for value <- ["off", "safe", "1", "true", "full", "unexpected"] do
      env = Map.put(@required_env, "CODEX_POOLER_NATIVE_COMPACTION_TRACE", value)

      with_env(env, fn ->
        config = Config.Reader.read!("config/runtime.exs", env: :prod)

        assert config[:codex_pooler][
                 CodexPooler.Gateway.Transports.Websocket.NativeCompactionTrace
               ][:mode] == :off
      end)
    end
  end

  test "prod runtime config rejects invalid upstream secret keys safely" do
    invalid_key = "too-short"
    env = Map.put(@required_env, "CODEX_POOLER_UPSTREAM_SECRET_KEY", invalid_key)

    with_env(env, fn ->
      error =
        assert_raise RuntimeError, fn ->
          Config.Reader.read!("config/runtime.exs", env: :prod)
        end

      assert Exception.message(error) ==
               "CODEX_POOLER_UPSTREAM_SECRET_KEY must be 32 raw bytes or base64-encoded 32 bytes"

      refute Exception.message(error) =~ invalid_key
    end)
  end

  test "scheduler release keeps the leader Stager active while queues stay disabled" do
    for {mode, queues?, stager?, services?} <- [
          {"web", false, false, false},
          {"unknown", false, false, false},
          {"worker", true, true, false},
          {"scheduler", false, true, true},
          {"all", true, true, true}
        ] do
      env = Map.put(@required_env, "OBAN_MODE", mode)

      with_env(env, fn ->
        config = Config.Reader.read!("config/runtime.exs", env: :prod)
        oban_config = Oban.Config.new(config[:codex_pooler][Oban])
        plugin_modules = MapSet.new(oban_config.plugins, &elem(&1, 0))

        assert oban_config.queues != [] == queues?
        assert oban_config.stager != false == stager?

        if services? do
          {Oban.Cron, cron_opts} = Enum.find(oban_config.plugins, &match?({Oban.Cron, _}, &1))
          crontab = Keyword.fetch!(cron_opts, :crontab)

          assert {"*/5 * * * *", CodexPooler.Jobs.OpenAIStatusSyncWorker} in crontab
          assert {"0 0 * * *", CodexPooler.Jobs.OpenAIStatusCleanupWorker} in crontab
        end

        for service <- [Oban.Cron, Oban.Lifeline, Oban.Pruner] do
          assert MapSet.member?(plugin_modules, service) == services?
        end
      end)
    end
  end

  test "the Repo names its PostgreSQL backends after the release role" do
    for {mode, application_name} <- [
          {"web", "codex_pooler_web"},
          {"worker", "codex_pooler_worker"},
          {"scheduler", "codex_pooler_scheduler"},
          {"all", "codex_pooler_all"},
          {"unexpected-role-value", "codex_pooler_web"}
        ] do
      with_env(Map.put(@required_env, "OBAN_MODE", mode), fn ->
        config = Config.Reader.read!("config/runtime.exs", env: :prod)
        repo_config = config[:codex_pooler][CodexPooler.Repo]

        assert repo_config[:parameters] == [application_name: application_name]
        assert byte_size(application_name) <= 63
      end)
    end
  end

  test "the upstream connection idle bound is an instance setting, not release env" do
    env = Map.put(@required_env, "CODEX_POOLER_UPSTREAM_CONN_MAX_IDLE_TIME_MS", "30s")

    with_env(env, fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)

      refute Keyword.has_key?(config[:codex_pooler], :upstream_conn_max_idle_time_ms)
    end)
  end

  test "standard proxy variables load at boot with sanitized invalid configuration errors" do
    env =
      @required_env
      |> Map.put("http_proxy", "http://http-proxy.example.com:8080")
      |> Map.put("HTTP_PROXY", "http://ignored.example.com:8081")
      |> Map.put("https_proxy", "http://user:p%40ss@proxy.example.com:3128")
      |> Map.put("no_proxy", "localhost,.example.com")

    with_env(env, fn ->
      config = Config.Reader.read!("config/runtime.exs", env: :prod)

      assert config[:codex_pooler][CodexPooler.Platform.OutboundHTTP][:proxy_config] == %{
               http: [proxy: {:http, "http-proxy.example.com", 8080, []}],
               https: [
                 proxy: {:http, "proxy.example.com", 3128, []},
                 proxy_headers: [
                   {"proxy-authorization", "Basic " <> Base.encode64("user:p@ss")}
                 ]
               ],
               no_proxy: ["localhost", ".example.com"]
             }
    end)

    invalid_proxy = "https://secret:password@proxy.example.com"

    with_env(Map.put(@required_env, "https_proxy", invalid_proxy), fn ->
      error =
        assert_raise ArgumentError, fn ->
          Config.Reader.read!("config/runtime.exs", env: :prod)
        end

      refute Exception.message(error) =~ invalid_proxy
      refute Exception.message(error) =~ "secret"
      refute Exception.message(error) =~ "password"
    end)
  end

  defp with_env(env, fun) do
    proxy_names = ~w(http_proxy https_proxy no_proxy HTTP_PROXY HTTPS_PROXY NO_PROXY)
    managed_names = Enum.uniq(Map.keys(env) ++ proxy_names)
    previous = Map.new(managed_names, &{&1, System.get_env(&1)})

    restore = fn ->
      Enum.each(previous, fn
        {key, nil} -> System.delete_env(key)
        {key, value} -> System.put_env(key, value)
      end)
    end

    # Also on_exit: the ExUnit timeout or a linked crash kills the test before `after` runs.
    on_exit(restore)

    Enum.each(proxy_names, &System.delete_env/1)

    Enum.each(env, fn
      {key, nil} -> System.delete_env(key)
      {key, value} -> System.put_env(key, value)
    end)

    try do
      fun.()
    after
      restore.()
    end
  end
end
