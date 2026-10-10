defmodule CodexPoolerWeb.Runtime.BackendCodexProviderCreditsRefreshTest do
  # An account serving on provider credits (its included weekly window spent,
  # credits left) whose dispatch meets a provider 401 refreshes its access token
  # and retries on the same assignment, where the final provider-credits
  # admission reads the account's quota evidence again. The refresh renews the
  # credential of the same provider account, so that evidence (the provider's
  # availability, the capacity facts and the credit balance) moves to the new
  # credential epoch with it and the retry is served on provider credits. It
  # used to stay at the old epoch, unverified, and the retry was refused 429
  # `provider_credit_capacity_unverified` until the next usage poll
  # (findings#334). A replaced credential still leaves it behind.
  #
  # Topology: one node, the real public listener, FakeUpstream, Full and Lite,
  # HTTP SSE and websocket with owner forwarding off and on, the account's
  # evidence written by the real usage parser and stores, synthetic text.
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, native_text_input: 1, public_websocket_connect!: 3, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, register_unboxed_pool_cleanup!: 1, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, stop_websocket_owner_session: 1]
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @budget 15_000
  @evidence ~w(quota_account_availability quota_capacity_facts quota_credit_balance)

  setup context do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, Map.get(context, :transport) == :websocket_owner_true)
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    :ok
  end

  for transport <- [:http, :websocket_owner_false, :websocket_owner_true], mode <- ["full", "lite"] do
    @tag transport: transport, mode: mode
    test "#{mode} #{transport} request on provider credits after a 401 and its account's token refresh is served there", context do
      # provenance: synthetic_adversarial
      upstream = start_upstream(FakeUpstream.strict_sequence([unauthorized(context.transport), FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: FakeUpstream.json_response(%{"access_token" => "synthetic-refreshed-token", "expires_in" => 3600}))] ++ [completed_reply(context.transport)]))
      setup = credit_backed_setup!(upstream, context)
      before = Repo.get!(UpstreamIdentity, setup.identity.id).metadata
      epoch = before["credential_epoch"] || 1
      port = start_public_endpoint!()

      outcome = turn!(context.transport, port, setup, Ecto.UUID.generate(), context.mode)
      calls = Enum.map(FakeUpstream.requests(upstream), &{&1.method, &1.path})

      # The refused websocket handshake is not a recorded request.
      assert {outcome, calls} == {:completed, if(context.transport == :http, do: [{"POST", @path}, {"POST", "/oauth/token"}, {"POST", @path}], else: [{"POST", "/oauth/token"}, {"WEBSOCKET", @path}])}
      assert %Request{status: "succeeded"} = served = await_settled!(setup)
      assert %Attempt{status: "succeeded", pool_upstream_assignment_id: assignment_id, response_metadata: %{"provider_credits_admission" => %{"capacity_basis" => "provider_credits"}}} = final_attempt!(served)
      assert assignment_id == setup.assignment.id
      refreshed = Repo.get!(UpstreamIdentity, setup.identity.id).metadata
      assert refreshed["credential_epoch"] == epoch + 1

      # The evidence moved to the new epoch and nothing else about it changed.
      for key <- @evidence do
        assert refreshed[key]["credential_epoch"] == epoch + 1
        assert Map.delete(refreshed[key], "credential_epoch") == Map.delete(before[key], "credential_epoch")
      end
    end
  end

  # An operator's re-import of the account's credential is a replacement: the
  # evidence stays at the old epoch, and the account serves on provider credits
  # again only once a usage poll observed the new credential.
  test "a request on provider credits after a re-import of its account's credential is refused until the next usage poll", context do
    # provenance: synthetic_adversarial
    upstream = start_upstream(FakeUpstream.strict_sequence([completed_reply(:http)]))
    setup = credit_backed_setup!(upstream, context |> Map.put(:transport, :http) |> Map.put(:mode, "full"))
    before = Repo.get!(UpstreamIdentity, setup.identity.id)
    reimport!(setup)
    replaced = Repo.get!(UpstreamIdentity, setup.identity.id)
    assert replaced.metadata["credential_epoch"] == (before.metadata["credential_epoch"] || 1) + 1
    assert replaced.metadata["quota_credit_balance"] == before.metadata["quota_credit_balance"]
    port = start_public_endpoint!()

    assert {:refused, 429, "quota_exhausted"} = turn!(:http, port, setup, Ecto.UUID.generate(), "full")
    assert FakeUpstream.count(upstream) == 0
    assert %Request{status: "rejected", response_status_code: 429, last_error_code: "quota_exhausted", request_metadata: %{"candidate_exclusions" => [exclusion]}} = await_settled!(setup)
    assert %{"upstream_identity_id" => identity_id, "reasons" => [%{"provider_credits_reason_codes" => ["exhausted", "provider_credit_capacity_unverified", "provider_credit_evidence_not_current"]}]} = exclusion
    assert identity_id == setup.identity.id

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    _polled = ProviderCreditsFixtures.persist_usage!(replaced, ProviderCreditsFixtures.usage_payload(:weekly_credit_only, now: now), now)
    assert :completed = turn!(:http, port, setup, Ecto.UUID.generate(), "full")
    assert FakeUpstream.count(upstream) == 1
  end

  # The account: its included weekly window spent, 25 credits left, provider
  # credits allowed, all of it observed by the real usage parser and stores at
  # the credential epoch before the request.
  defp credit_backed_setup!(upstream, context) do
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    if context.transport == :websocket_owner_true, do: stop_owner_sessions_on_exit(setup)
    assert {:ok, _secret} = Upstreams.store_encrypted_secret(setup.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-refresh-token"})
    identity = setup.identity |> Ecto.Changeset.change(allow_provider_credits: true) |> Repo.update!()
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    _identity = ProviderCreditsFixtures.persist_usage!(identity, ProviderCreditsFixtures.usage_payload(:weekly_credit_only, now: now), now)
    set_model_serving_mode!(model_serving_scope(), setup, context.mode)
    Map.put(setup, :serving_mode, context.mode)
  end

  # With owner forwarding on, the socket hands its turns to an owner session that outlives the test; stop the Pool's owners before its committed rows go.
  defp stop_owner_sessions_on_exit(setup), do: on_exit(fn -> for id <- Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id)), do: stop_websocket_owner_session(id) end)

  defp unauthorized(:http), do: FakeUpstream.expect_request(method: "POST", path: @path, respond: FakeUpstream.json_response(%{"error" => %{"code" => "invalid_api_key", "message" => "synthetic", "type" => "invalid_request_error"}}, 401))
  defp unauthorized(_websocket), do: FakeUpstream.expect_request(method: "GET", respond: FakeUpstream.websocket_upgrade_error(%{"error" => %{"code" => "invalid_api_key"}}, status: 401, headers: [{"x-openai-authorization-error", "invalid_api_key"}]))

  defp completed_reply(transport) do
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_credits_refresh", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}

    if transport == :http,
      do: FakeUpstream.raw_response("event: response.completed\ndata: " <> CodexPooler.JSON.encode!(completed) <> "\n\n", headers: [{"content-type", "text/event-stream"}]),
      else: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])
  end

  # An operator's Codex `auth.json` import of the same provider account into its identity.
  defp reimport!(setup) do
    %{user: owner} = CodexPooler.AccountsFixtures.bootstrap_owner_fixture()
    account_id = Repo.get!(UpstreamIdentity, setup.identity.id).chatgpt_account_id
    jwt = fn claims -> Enum.map_join([%{"alg" => "none", "typ" => "JWT"}, claims, "synthetic-signature"], ".", &Base.url_encode64(CodexPooler.JSON.encode!(&1), padding: false)) end
    id_token = jwt.(%{"email" => "synthetic@example.com", "https://api.openai.com/auth" => %{"chatgpt_account_id" => account_id, "chatgpt_user_id" => "user_synthetic", "chatgpt_plan_type" => "pro"}})
    access_token = jwt.(%{"exp" => DateTime.utc_now() |> DateTime.add(3600) |> DateTime.to_unix()})
    auth = CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"id_token" => id_token, "access_token" => access_token, "refresh_token" => "synthetic-reimported-refresh-token", "account_id" => account_id}})
    assert {:ok, %{status: :existing, identity: %{id: identity_id}}} = Upstreams.import_codex_auth_json(Scope.for_user(owner), setup.pool, auth)
    assert identity_id == setup.identity.id
  end

  # One request over the transport under test, answered by its terminal: `:completed`, or `{:refused, status, code}`.
  defp turn!(:http, port, setup, thread, mode) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    metadata = turn_metadata(thread)
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload(setup, thread, metadata)))
    {status, body} = collect(conn, ref, nil, "")
    Mint.HTTP.close(conn)

    if status == 200 and body =~ "response.completed",
      do: :completed,
      else: {:refused, status, CodexPooler.JSON.decode!(body)["error"]["code"]}
  end

  defp turn!(_websocket, port, setup, thread, mode) do
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    frame = payload(setup, thread, turn_metadata(thread)) |> Map.put("type", "response.create")
    frame = if mode == "lite", do: put_in(frame, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: frame
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
    {conn, _websocket, terminal} = receive_terminal!(conn, websocket, ref)
    Mint.HTTP.close(conn)

    case terminal do
      %{"type" => "response.completed"} -> :completed
      %{"type" => "error", "status" => status, "error" => %{"code" => code}} -> {:refused, status, code}
    end
  end

  defp turn_metadata(thread), do: CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})

  defp payload(setup, thread, metadata),
    do: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic provider credits #{thread}"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => metadata}}

  defp collect(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _, acc -> acc
      end)

    if done, do: {status, body}, else: collect(conn, ref, status, body)
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => type} = terminal when type in ["response.completed", "response.incomplete", "response.failed", "error"] -> {conn, websocket, terminal}
      _progress -> receive_terminal!(conn, websocket, ref)
    end
  end

  # The Pool's latest request once it settled.
  defp await_settled!(setup, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @budget
    row = Repo.one(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)

    cond do
      row && row.completed_at -> row
      System.monotonic_time(:millisecond) > deadline -> flunk("the request never settled")
      true -> Process.sleep(10) && await_settled!(setup, deadline)
    end
  end

  defp final_attempt!(%Request{id: request_id}), do: Repo.one!(from(a in Attempt, where: a.request_id == ^request_id, order_by: [desc: a.attempt_number], limit: 1))
end
