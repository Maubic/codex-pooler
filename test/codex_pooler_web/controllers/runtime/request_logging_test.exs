defmodule CodexPoolerWeb.Runtime.RequestLoggingTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPooler.AccountsFixtures, only: [bootstrap_owner_fixture: 0, reset_bootstrap_state_fixture!: 0]

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Request
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Repo
  alias CodexPoolerWeb.RequestLogger

  setup do
    reset_bootstrap_state_fixture!()

    previous_level = Logger.level()

    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)

    on_exit(fn ->
      Logger.configure(level: previous_level)
    end)

    Logger.configure(level: :info)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    RequestLogger.attach()

    :ok
  end

  @tag :invite_log_redaction
  @tag capture_log: false
  test "live invite completion logs redact active expired revoked and unknown credentials with encoded route spellings" do
    tokens = invite_log_tokens!()
    port = start_invite_logging_endpoint!()

    observations =
      for {state, token} <- tokens,
          {spelling, request_path} <- invite_log_paths(token) do
        {status, log} =
          with_log([level: :info], fn ->
            status = invite_http_status(port, request_path)
            assert_receive {:invite_http_observed, ^status, true}
            status
          end)

        lines = completion_lines(log)

        %{
          state: state,
          spelling: spelling,
          status: status,
          completions: length(lines),
          token_matches: Enum.count([token, percent_encode(token)], &String.contains?(log, &1)),
          placeholder_lines: Enum.count(lines, &String.contains?(&1, "path=/onboarding/invites/:invite_token ")),
          standard_metadata: Enum.all?(lines, &complete_request_metadata?/1)
        }
      end

    assert Enum.all?(observations, fn observed ->
             observed.status == 200 and observed.completions == 1 and observed.token_matches == 0 and
               observed.placeholder_lines == 1 and observed.standard_metadata
           end),
           inspect(observations)

    {_state, token} = hd(tokens)

    {status, log} =
      with_log([level: :info], fn ->
        status = invite_http_status(port, "/login?token=" <> token)
        assert_receive {:invite_http_observed, ^status, false}
        status
      end)

    ordinary = %{
      status: status,
      completions: length(completion_lines(log)),
      ordinary_path_lines: Enum.count(completion_lines(log), &String.contains?(&1, "path=/login ")),
      token_matches: Enum.count(completion_lines(log), &String.contains?(&1, token)),
      standard_metadata: Enum.all?(completion_lines(log), &complete_request_metadata?/1)
    }

    assert ordinary == %{status: 200, completions: 1, ordinary_path_lines: 1, token_matches: 0, standard_metadata: true}
  end

  @tag :invite_log_redaction
  @tag capture_log: false
  test "invite path redaction also covers unmatched suffixes while ordinary paths retain their metadata" do
    token = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
    duration = System.convert_time_unit(15, :millisecond, :native)

    for {_spelling, path} <- invite_log_paths(token) do
      conn = Plug.Test.conn(:get, path <> "/unmatched/" <> token) |> Map.put(:status, 404)
      line = RequestLogger.request_log_line(conn, duration)

      observed = %{
        raw_token_present: String.contains?(line, token),
        encoded_token_present: String.contains?(line, percent_encode(token)),
        placeholder: String.contains?(line, "path=/onboarding/invites/:invite_token "),
        status_retained: String.contains?(line, "status=404"),
        duration_retained: String.contains?(line, "duration_ms=15")
      }

      assert observed == %{raw_token_present: false, encoded_token_present: false, placeholder: true, status_retained: true, duration_retained: true}
    end

    for path <- ["/admin/invites", "/onboarding/invites", "/onboarding/invites-summary/sample", "/v1/models"] do
      conn = Plug.Test.conn(:get, path) |> Map.put(:status, 200)
      line = RequestLogger.request_log_line(conn, duration)
      assert String.contains?(line, "path=#{path} ")
    end
  end

  test "runtime request logging is one-line metadata-only and includes production fields", %{
    conn: conn
  } do
    log =
      capture_log([level: :info], fn ->
        conn
        |> put_req_header("user-agent", "Codex CLI/1.2.3")
        |> get(~p"/backend-api/codex/models")
        |> response(401)
      end)

    lines =
      log
      |> String.split("\n", trim: true)
      |> Enum.filter(&String.contains?(&1, "request_completed"))

    assert [line] = lines
    assert line =~ "request_completed"
    assert line =~ "method=GET"
    assert line =~ "path=/backend-api/codex/models"
    assert line =~ "status=401"
    assert line =~ "duration_ms="
    assert line =~ "remote_ip="
    assert line =~ ~s(user_agent="Codex CLI/1.2.3")
    assert log =~ "request_id="
    assert length(Regex.scan(~r/request_id=/, line)) == 1
    refute log =~ "GET /backend-api/codex/models"
    refute log =~ "Sent 401"
  end

  test "runtime request logging sanitizes multiline control user agents and ignores untrusted forwarded IP",
       %{conn: conn} do
    malicious_user_agent = "Codex\nInjected-Header: secret-token\r\nsecond-line\ttrail"

    log =
      capture_log([level: :info], fn ->
        conn
        |> Map.put(:remote_ip, {198, 51, 100, 20})
        |> put_req_header("x-forwarded-for", "203.0.113.55")
        |> put_req_header("user-agent", malicious_user_agent)
        |> get(~p"/backend-api/codex/models")
        |> response(401)
      end)

    assert [line] =
             log
             |> String.split("\n", trim: true)
             |> Enum.filter(&String.contains?(&1, "request_completed"))

    assert line =~ "remote_ip=198.51.100.20"
    assert line =~ ~s(user_agent="Codex Injected-Header: secret-token second-line trail")
    refute line =~ "203.0.113.55"
    refute line =~ "\n"
    refute line =~ "\r"
    refute log =~ "Injected-Header: secret-token\n"
  end

  test "request logging uses forwarded IPs from trusted proxies on browser routes", %{conn: conn} do
    setup_trusted_proxies(["10.42.0.0/16"])

    log =
      capture_log([level: :info], fn ->
        conn
        |> Map.put(:remote_ip, {10, 42, 0, 50})
        |> put_req_header("x-forwarded-for", "203.0.113.55, 10.42.0.50")
        |> get(~p"/login")
        |> response(302)
      end)

    assert [line] =
             log
             |> String.split("\n", trim: true)
             |> Enum.filter(&String.contains?(&1, "request_completed"))

    assert line =~ "path=/login"
    assert line =~ "remote_ip=203.0.113.55"
    assert line =~ "immediate_peer_ip=10.42.0.50"
    assert line =~ "client_ip_source=x_forwarded_for"
    assert line =~ "inspected_hops=2"
  end

  test "request logging omits peer provenance from untrusted forwarding input", %{conn: conn} do
    setup_trusted_proxies(["10.42.0.0/16"])

    log =
      capture_log([level: :info], fn ->
        conn
        |> Map.put(:remote_ip, {198, 51, 100, 20})
        |> put_req_header("x-forwarded-for", "203.0.113.55")
        |> get(~p"/login")
        |> response(302)
      end)

    assert [line] =
             log
             |> String.split("\n", trim: true)
             |> Enum.filter(&String.contains?(&1, "request_completed"))

    assert line =~ "remote_ip=198.51.100.20"
    refute line =~ "immediate_peer_ip="
    refute line =~ "client_ip_source="
    refute line =~ "inspected_hops="
    refute line =~ "203.0.113.55"
  end

  test "healthy backend response coalesces routing request metadata writes", %{conn: conn} do
    input_text = "metadata coalescing input #{System.unique_integer([:positive])}"

    input = [
      %{
        "type" => "message",
        "role" => "user",
        "content" => [%{"type" => "input_text", "text" => input_text}]
      }
    ]

    upstream =
      start_upstream(
        FakeUpstream.json_response(%{
          "id" => "resp_metadata_coalesced",
          "object" => "response",
          "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}
        })
      )

    setup = gateway_setup(upstream)

    {{conn, query_events}, _log} =
      with_log([level: :info], fn ->
        collect_repo_query_events(fn ->
          conn
          |> put_req_header("x-request-id", Ecto.UUID.generate())
          |> auth(setup)
          |> post("/backend-api/codex/responses", %{
            "model" => setup.model.exposed_model_id,
            "input" => input
          })
        end)
      end)

    assert %{"id" => "resp_metadata_coalesced"} = json_response(conn, 200)

    assert [request] =
             Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id))

    routing = request.request_metadata["routing"]
    assert routing["strategy"]
    assert routing["selected_bridge_candidate_id"] == setup.assignment.id
    assert routing["selected_bridge_candidate_rank"] == 1

    assert request_update_count(query_events) <= 3

    metadata_text = inspect(request.request_metadata)
    refute metadata_text =~ input_text
    refute metadata_text =~ setup.authorization
  end

  defp invite_log_tokens! do
    %{user: owner} = bootstrap_owner_fixture()
    scope = Scope.for_user(owner, ["instance_owner"])
    pool = CodexPooler.PoolerFixtures.pool_fixture()
    {:ok, %{token: active}} = Access.create_invite(scope, pool, %{invited_email: "active@example.com"})
    {:ok, %{token: expired}} = Access.create_invite(scope, pool, %{invited_email: "expired@example.com", expires_at: DateTime.add(DateTime.utc_now(), -60, :second)})
    {:ok, %{token: revoked, invite: invite}} = Access.create_invite(scope, pool, %{invited_email: "revoked@example.com"})
    {:ok, _revoked} = Access.revoke_invite(scope, invite)
    unknown = Base.url_encode64(:crypto.strong_rand_bytes(32), padding: false)
    [active: active, expired: expired, revoked: revoked, unknown: unknown]
  end

  defp invite_log_paths(token) do
    [
      canonical: "/onboarding/invites/" <> token,
      encoded_prefix: "/%6Fnboarding/%69nvites/" <> token,
      encoded_token: "/onboarding/invites/" <> percent_encode(token),
      encoded_both: "/%6fnboarding/%69nvites/" <> percent_encode(token)
    ]
  end

  defp percent_encode(value), do: for(<<byte <- value>>, into: "", do: "%" <> Base.encode16(<<byte>>))

  defp start_invite_logging_endpoint! do
    observer = self()

    probe = fn conn, _opts ->
      result = @endpoint.call(conn, @endpoint.init([]))
      send(observer, {:invite_http_observed, result.status, Map.has_key?(result.path_params, "invite_token")})
      result
    end

    refute @endpoint.config(:debug_errors, false)
    listener = start_supervised!({Bandit, plug: probe, port: 0, ip: {127, 0, 0, 1}, startup_log: false})
    {:ok, {_ip, port}} = ThousandIsland.listener_info(listener)

    on_exit(fn ->
      refute Process.alive?(listener)
      assert {:error, :econnrefused} = :gen_tcp.connect({127, 0, 0, 1}, port, [], 1_000)
    end)

    port
  end

  defp invite_http_status(port, request_path) do
    case Req.get("http://127.0.0.1:#{port}" <> request_path, headers: [{"user-agent", "sample-client/1.0"}], decode_body: false, redirect: false, retry: false) do
      {:ok, response} -> response.status
      {:error, _reason} -> :request_failed
    end
  end

  defp completion_lines(log) do
    log |> String.split("\n", trim: true) |> Enum.filter(&String.contains?(&1, "request_completed"))
  end

  defp complete_request_metadata?(line) do
    Enum.all?(["method=GET", "status=200", "duration_ms=", "remote_ip=", ~s(user_agent="sample-client/1.0")], &String.contains?(line, &1)) and length(Regex.scan(~r/request_id=/, line)) == 1
  end

  defp setup_trusted_proxies(trusted_proxies) do
    previous = CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)

    Application.put_env(
      :codex_pooler,
      OperationalSettings,
      previous
      |> Keyword.put(:settings, %OperationalSettings{trusted_proxies: trusted_proxies})
      |> Keyword.put(:use_instance_settings?, false)
    )
  end

  defp collect_repo_query_events(fun) when is_function(fun, 0) do
    handler_id = {__MODULE__, self(), System.unique_integer([:positive])}

    # Also on_exit: a linked crash or the ExUnit timeout kills the test before `after` runs.
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        &__MODULE__.handle_repo_query_event/4,
        {handler_id, self()}
      )

    try do
      result = fun.()
      {result, drain_repo_query_events(handler_id, [])}
    after
      :telemetry.detach(handler_id)
    end
  end

  def handle_repo_query_event(_event, _measurements, metadata, {handler_id, test_pid}) do
    if metadata[:repo] == Repo do
      send(test_pid, {handler_id, metadata[:source], query_command(metadata[:query])})
    end
  end

  defp drain_repo_query_events(handler_id, events) do
    receive do
      {^handler_id, source, command} ->
        drain_repo_query_events(handler_id, [{source, command} | events])
    after
      0 -> Enum.reverse(events)
    end
  end

  defp request_update_count(events) do
    Enum.count(events, fn {source, command} -> source == "requests" and command == "UPDATE" end)
  end

  defp query_command(query) when is_binary(query) do
    query
    |> String.trim_leading()
    |> String.split(~r/\s+/, parts: 2)
    |> List.first()
    |> String.upcase()
  end

  defp query_command(_query), do: "UNKNOWN"
end
