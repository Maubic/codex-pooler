defmodule CodexPoolerWeb.Runtime.CodexUsageChatgptDatabaseUnavailableTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [cleanup_unboxed_pool!: 1, start_upstream: 1]
  import ExUnit.CaptureLog

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Platform.TransientDatabaseError
  alias CodexPooler.Repo
  alias CodexPooler.UnavailableRepo
  alias CodexPooler.UnboxedFixture
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Quota.Windows
  alias Ecto.Adapters.SQL.Sandbox

  @paths ["/api/codex/usage", "/wham/usage", "/backend-api/wham/usage"]
  @budget 15_000
  @token "synthetic-usage-access-token"
  @unavailable %{"error" => %{"type" => "server_error", "code" => "service_unavailable", "param" => nil, "message" => "Codex Pooler is temporarily unavailable; retry the request"}}

  defmodule IsolatedEndpoint do
    @moduledoc false
    @behaviour Plug
    def init(repo), do: repo

    def call(conn, repo) do
      previous = CodexPooler.Repo.put_dynamic_repo(repo)

      try do
        CodexPoolerWeb.Endpoint.call(conn, CodexPoolerWeb.Endpoint.init([]))
      after
        CodexPooler.Repo.put_dynamic_repo(previous)
      end
    end
  end

  setup do
    upstream = start_upstream(FakeUpstream.json_response(%{}))
    fixture = committed_fixture!(FakeUpstream.url(upstream))
    counter = :counters.new(1, [])
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)
    events = for kind <- [:accepted, :enqueued, :rejected], do: [:codex_pooler, :gateway, :admission, kind]
    :ok = :telemetry.attach_many(handler, events, &__MODULE__.observe_admission/4, counter)
    %{fixture: fixture, upstream: upstream, counter: counter}
  end

  for path <- @paths, carrier <- [:controller, :http], bearer <- [:chatgpt, :api_key] do
    test "#{carrier} #{path} #{bearer} preserves an unavailable authentication pool", context do
      pool_failure!(context, unquote(path), unquote(carrier), unquote(bearer))
    end
  end

  for path <- @paths, carrier <- [:controller, :http] do
    test "#{carrier} #{path} contains the actual secret SELECT cancellation after identity lookup", context do
      secret_failure!(context, unquote(path), unquote(carrier))
    end
  end

  for path <- @paths do
    test "HTTP #{path} keeps healthy missing/invalid 401 and valid ChatGPT token 200", context do
      repo = reader_repo!()
      port = listener!(repo)

      for token <- [nil, "synthetic-invalid-token"] do
        response = request!(:http, repo, port, unquote(path), headers(context.fixture, token))
        assert response.status == 401
        assert CodexPooler.JSON.decode!(response.resp_body)["error"]["code"] == "invalid_authorization"
      end

      assert_no_effects!(context)
      response = request!(:http, repo, port, unquote(path), headers(context.fixture, @token))
      assert response.status == 200
      assert is_map(CodexPooler.JSON.decode!(response.resp_body)["rate_limit"])
      assert :counters.get(context.counter, 1) == 1
      assert FakeUpstream.count(context.upstream) == 0
    end

    test "#{path} does not turn a nontransient missing relation into a retryable outage", context do
      repo = reader_repo!(parameters: [search_path: "pg_catalog"])

      error =
        assert_raise Postgrex.Error, fn ->
          with_repo(repo, fn -> raw_controller_request(unquote(path), headers(context.fixture, @token)) end)
        end

      assert error.postgres.code == :undefined_table
      assert_no_effects!(context)
    end
  end

  defp pool_failure!(context, path, carrier, bearer) do
    unavailable = UnavailableRepo.hold!()
    on_exit(fn -> refute Process.alive?(unavailable.repo) end)
    port = if carrier == :http, do: listener!(unavailable.repo)
    token = if bearer == :chatgpt, do: @token, else: context.fixture.raw_key

    {response, log} =
      UnavailableRepo.run(unavailable, fn ->
        with_log(fn -> request!(carrier, unavailable.repo, port, path, headers(context.fixture, token)) end)
      end)

    refute Process.alive?(unavailable.holder.pid)
    assert_refused!(response, log, "DBConnection.ConnectionError")
    assert_no_effects!(context)
  end

  defp secret_failure!(context, path, carrier) do
    repo = reader_repo!()
    port = if carrier == :http, do: listener!(repo)
    {reader_backend, original_timeout} = with_repo(repo, fn -> {backend!(), setting!("statement_timeout")} end)
    {locker, ref, holder_backend} = hold_secret_table!()
    assert reader_backend != holder_backend
    assert [["AccessExclusiveLock", true]] = with_repo(repo, fn -> Repo.query!("SELECT l.mode, l.granted FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE l.pid = $1 AND c.relname = 'encrypted_secrets'", [holder_backend]).rows end)
    events = observe_reads!(repo)

    {response, log} =
      try do
        with_repo(repo, fn -> Repo.query!("SELECT set_config('statement_timeout', '100ms', false)") end)
        with_log(fn -> request!(carrier, repo, port, path, headers(context.fixture, @token)) end)
      after
        send(locker.pid, {:release_secret_lock, ref})
        assert {:ok, :released} = Task.await(locker, @budget)

        with_repo(repo, fn ->
          Repo.query!("SELECT set_config('statement_timeout', $1, false)", [original_timeout])
          assert setting!("statement_timeout") == original_timeout
          assert backend!() == reader_backend
        end)
      end

    assert_receive {^events, :identity_read, 1}, @budget
    assert_receive {^events, :secret_error, :query_canceled}, @budget
    assert_refused!(response, log, "postgres_query_canceled")
    assert_no_effects!(context)
    assert request!(carrier, repo, port, path, headers(context.fixture, @token)).status == 200
    assert :counters.get(context.counter, 1) == 1
    assert FakeUpstream.count(context.upstream) == 0
    CodexPooler.TestDiagnostics.puts("usage secret read carrier=#{carrier} path=#{path} reader_backend=#{reader_backend} holder_backend=#{holder_backend} identity_rows=1 secret_error=query_canceled statement_timeout_restored=true healthy_recovery=200")
  end

  defp hold_secret_table! do
    parent = self()
    ref = make_ref()
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn -> secret_lock_transaction(parent, ref) end)
      end)

    assert_receive {:secret_table_locked, ^ref, backend}, @budget
    {task, ref, backend}
  end

  defp secret_lock_transaction(parent, ref) do
    Repo.transaction(fn ->
      Repo.query!("LOCK TABLE encrypted_secrets IN ACCESS EXCLUSIVE MODE")
      send(parent, {:secret_table_locked, ref, backend!()})

      receive do
        {:release_secret_lock, ^ref} -> :released
      after
        @budget -> Repo.rollback(:lock_release_missing)
      end
    end)
  end

  defp observe_reads!(repo) do
    ref = make_ref()
    handler = {__MODULE__, ref}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.observe_read/4, {self(), ref, repo})
    ref
  end

  def observe_read(_event, _measurements, metadata, {observer, ref, repo}) do
    if Repo.get_dynamic_repo() == repo do
      case {metadata[:source], metadata[:result]} do
        {"upstream_identities", {:ok, %{num_rows: count}}} -> send(observer, {ref, :identity_read, count})
        {"encrypted_secrets", {:error, %Postgrex.Error{postgres: %{code: code}}}} -> send(observer, {ref, :secret_error, code})
        _other -> :ok
      end
    end
  end

  def observe_admission(_event, _measurements, %{endpoint: path}, counter) when path in @paths, do: :counters.add(counter, 1, 1)
  def observe_admission(_event, _measurements, _metadata, _counter), do: :ok

  defp reader_repo!(opts \\ []) do
    repo = start_supervised!({Repo, Keyword.merge([name: nil, pool: DBConnection.ConnectionPool, pool_size: 1], opts)})
    on_exit(fn -> refute Process.alive?(repo) end)
    repo
  end

  defp listener!(repo) do
    server = start_supervised!({Bandit, plug: {IsolatedEndpoint, repo}, port: 0, ip: {127, 0, 0, 1}, startup_log: false})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(server)

    on_exit(fn ->
      refute Process.alive?(server)
      CodexPooler.TestDiagnostics.puts("usage owned_listener port=#{port} stopped=true")
    end)

    port
  end

  defp request!(:http, _repo, port, path, headers) do
    response = Req.get!("http://127.0.0.1:#{port}#{path}", headers: headers, retry: false, decode_body: false)
    %{status: response.status, resp_body: response.body}
  end

  defp request!(:controller, repo, _port, path, headers) do
    with_repo(repo, fn -> raw_controller_request(path, headers) end)
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] -> %{status: :raised, resp_body: "", reason_class: TransientDatabaseError.reason_class(error)}
  end

  defp raw_controller_request(path, headers) do
    headers |> Enum.reduce(build_conn(), fn {name, value}, conn -> put_req_header(conn, name, value) end) |> get(path)
  end

  defp headers(fixture, token) do
    account = [{"chatgpt-account-id", fixture.account_id}]
    if token, do: [{"authorization", "Bearer " <> token} | account], else: account
  end

  defp with_repo(repo, fun) do
    previous = Repo.put_dynamic_repo(repo)

    try do
      fun.()
    after
      Repo.put_dynamic_repo(previous)
    end
  end

  defp backend!, do: Repo.query!("SELECT pg_backend_pid()").rows |> hd() |> hd()
  defp setting!("statement_timeout"), do: Repo.query!("SHOW statement_timeout").rows |> hd() |> hd()

  defp assert_refused!(response, log, reason_class) do
    assert response.status == 503
    assert CodexPooler.JSON.decode!(response.resp_body) == @unavailable
    assert log =~ "stage=authentication reason_class=#{reason_class}"
    refute log =~ @token
    refute log =~ "statement timeout"
    refute log =~ "encrypted_secrets"
  end

  defp assert_no_effects!(context) do
    assert :counters.get(context.counter, 1) == 0
    assert Repo.aggregate(Request, :count) == 0
    assert Repo.aggregate(Attempt, :count) == 0
    assert Repo.aggregate(LedgerEntry, :count) == 0
    assert FakeUpstream.count(context.upstream) == 0
  end

  defp committed_fixture!(base_url) do
    {:ok, holder} = Agent.start(fn -> nil end)

    on_exit(fn ->
      try do
        if fixture = Agent.get(holder, & &1), do: UnboxedFixture.cleanup_unboxed!(fn -> cleanup_unboxed_pool!(fixture) end)
      after
        Agent.stop(holder)
      end
    end)

    {:ok, fixture} = UnboxedFixture.run_unboxed(fn -> Repo.transaction(fn -> create_fixture!(holder, base_url) end) end)
    fixture
  end

  defp create_fixture!(holder, base_url) do
    fixture = accounting_setup()
    Agent.update(holder, fn _ -> fixture end)
    account_id = "sample-usage-account-#{System.unique_integer([:positive])}"
    identity = Repo.update!(Ecto.Changeset.change(fixture.identity, chatgpt_account_id: account_id, metadata: %{"base_url" => base_url, "usage_path" => "/api/codex/usage"}))
    assert {:ok, _secret} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "access_token", plaintext: @token})
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    assert {:ok, _} = Windows.upsert_quota_windows(identity, [%{window_kind: "primary", window_minutes: 300, used_percent: Decimal.new(10), reset_at: DateTime.add(now, 1, :hour), source: "test", freshness_state: "fresh", observed_at: now, last_sync_at: now}])
    Map.merge(fixture, %{identity: identity, account_id: account_id})
  end
end
