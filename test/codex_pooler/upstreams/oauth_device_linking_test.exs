defmodule CodexPooler.Upstreams.OAuthDeviceLinkingTest do
  use CodexPooler.DataCase, async: false

  import Ecto.Query

  alias CodexPooler.Access
  alias CodexPooler.Access.InviteOnboarding
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.Events
  alias CodexPooler.FakeOpenAIAuthProvider
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Jobs.AccountReconciliationWorker
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Auth.CodexAuth

  alias CodexPooler.Upstreams.Schemas.{
    EncryptedSecret,
    OAuthFlow,
    PoolUpstreamAssignment,
    UpstreamIdentity
  }

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  setup do
    configure_upstream_secret_key!()
    restore_codex_auth_config!()
    :ok
  end

  test "device start reports unavailable provider safely without creating a flow" do
    scope = fixture_owner_scope()
    pool = pool_fixture()
    raw_provider_value = "raw-device-provider-body-must-not-leak"

    start_provider!(%{
      "/api/accounts/deviceauth/usercode" =>
        {404,
         %{
           "error" => "not_found",
           "error_description" => raw_provider_value,
           "device_auth_id" => raw_provider_value
         }}
    })

    assert {:error, %{code: :codex_auth_unavailable, message: message} = error} =
             Upstreams.start_device_oauth(scope, pool)

    assert message == "Codex device authorization is unavailable"
    refute inspect(error) =~ raw_provider_value
    assert Repo.aggregate(OAuthFlow, :count) == 0
  end

  test "pending device poll updates retry timing without linking anything" do
    scope = fixture_owner_scope()
    pool = pool_fixture()

    provider =
      start_provider!(
        device_routes(%{
          "/api/accounts/deviceauth/token" => {400, %{"error" => "authorization_pending"}}
        })
      )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    before_poll_after_at = flow.poll_after_at

    assert {:ok, %{status: :pending, flow: polled}} = Upstreams.poll_device_oauth(scope, flow.id)

    assert polled.status == "pending"
    assert polled.interval_seconds == 5
    assert DateTime.compare(polled.poll_after_at, before_poll_after_at) in [:gt, :eq]
    assert %DateTime{} = polled.last_polled_at
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
    assert Repo.aggregate(PoolUpstreamAssignment, :count) == 0
    assert Repo.aggregate(EncryptedSecret, :count) == 0

    assert [_start_request, poll_request] = FakeOpenAIAuthProvider.requests(provider)
    assert poll_request.path == "/api/accounts/deviceauth/token"

    assert poll_request.json == %{
             "device_auth_id" => "device-auth-linking",
             "user_code" => "CODE-LINK"
           }
  end

  test "nested provider pending response keeps the device flow polling" do
    scope = fixture_owner_scope()
    pool = pool_fixture()

    start_provider!(
      device_routes(%{
        "/api/accounts/deviceauth/token" =>
          {403,
           %{
             "error" => %{
               "message" => "Device authorization is pending. Please try again.",
               "type" => "invalid_request_error",
               "code" => "deviceauth_authorization_pending"
             }
           }}
      })
    )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    assert flow.interval_seconds == 5

    assert {:ok, %{status: :pending, flow: pending}} =
             Upstreams.poll_device_oauth(scope, flow.id)

    assert pending.status == "pending"
    assert pending.interval_seconds == 5
    assert DateTime.diff(pending.poll_after_at, pending.last_polled_at, :second) in 4..5
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
    assert Repo.aggregate(PoolUpstreamAssignment, :count) == 0
    assert Repo.aggregate(EncryptedSecret, :count) == 0
  end

  test "slow_down device poll increases interval and schedules the next poll later" do
    scope = fixture_owner_scope()
    pool = pool_fixture()

    start_provider!(
      device_routes(%{
        "/api/accounts/deviceauth/token" => {400, %{"error" => "slow_down"}}
      })
    )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)

    assert {:ok, %{status: :pending, flow: slowed}} = Upstreams.poll_device_oauth(scope, flow.id)

    assert slowed.status == "pending"
    assert slowed.interval_seconds == 10
    assert DateTime.diff(slowed.poll_after_at, slowed.last_polled_at, :second) in 9..10
  end

  test "transient HTTP polling failure keeps the persisted device flow pending until recovery" do
    scope = fixture_owner_scope()
    provider = start_provider!(device_routes(%{"/api/accounts/deviceauth/token" => {503, %{"error" => "temporarily_unavailable"}}}))
    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool_fixture())
    assert {:error, %{code: :codex_auth_transient}} = CodexAuth.poll_device_authorization(device_poll_state())
    assert_transient_flow_pending(scope, flow)
    FakeUpstream.set_mode(provider, {:path_json, successful_device_routes("acct_device_http_recovery")})
    assert_flow_recovers(scope, flow)
    assert Enum.map(FakeOpenAIAuthProvider.requests(provider), & &1.path) == ["/api/accounts/deviceauth/usercode", "/api/accounts/deviceauth/token", "/api/accounts/deviceauth/token", "/api/accounts/deviceauth/token", "/oauth/token"]
  end

  test "connection failure during polling keeps the same device flow pending until recovery" do
    scope = fixture_owner_scope()
    provider = start_provider!(device_routes(%{"/api/accounts/deviceauth/token" => {400, %{"error" => "authorization_pending"}}}))
    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool_fixture())
    FakeOpenAIAuthProvider.stop(provider)
    refute Process.alive?(provider.supervisor)
    assert {:error, %{code: :codex_auth_transient}} = CodexAuth.poll_device_authorization(device_poll_state())
    assert_transient_flow_pending(scope, flow)
    recovered = start_provider!(successful_device_routes("acct_device_connection_recovery"))
    assert_flow_recovers(scope, flow)
    assert Enum.map(FakeOpenAIAuthProvider.requests(recovered), & &1.path) == ["/api/accounts/deviceauth/token", "/oauth/token"]
  end

  test "transient polling cannot extend the device flow deadline or poll beyond it" do
    scope = fixture_owner_scope()
    provider = start_provider!(device_routes(%{"/api/accounts/deviceauth/token" => {503, %{"error" => "temporarily_unavailable"}}}))
    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool_fixture())
    assert_transient_flow_pending(scope, flow)
    expired_at = DateTime.utc_now() |> DateTime.add(-1, :second)
    flow |> Ecto.Changeset.change(expires_at: expired_at) |> Repo.update!()

    assert {:error, %{code: :expired_flow}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert {:error, %{code: :expired_flow}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert Enum.map(FakeOpenAIAuthProvider.requests(provider), & &1.path) == ["/api/accounts/deviceauth/usercode", "/api/accounts/deviceauth/token"]
    assert DateTime.compare(Repo.reload!(flow).expires_at, expired_at) == :eq
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
  end

  test "post-authorization token exchange transient failure remains terminal" do
    scope = fixture_owner_scope()

    routes =
      successful_device_routes("acct_device_exchange_failure")
      |> Map.put("/oauth/token", {503, %{"error" => "temporarily_unavailable"}})

    provider = start_provider!(routes)
    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool_fixture())
    assert {:error, %{code: :token_exchange_failed}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert Repo.reload!(flow).status == "failed"
    assert {:error, %{code: :flow_not_pending}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert Enum.map(FakeOpenAIAuthProvider.requests(provider), & &1.path) == ["/api/accounts/deviceauth/usercode", "/api/accounts/deviceauth/token", "/oauth/token"]
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
    assert Repo.aggregate(EncryptedSecret, :count) == 0
  end

  test "malformed polling error remains terminal rather than transient" do
    scope = fixture_owner_scope()
    provider = start_provider!(device_routes(%{"/api/accounts/deviceauth/token" => {400, %{"error" => "invalid_request"}}}))
    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool_fixture())
    assert {:error, %{code: :codex_auth_malformed}} = CodexAuth.poll_device_authorization(device_poll_state())
    assert {:error, %{code: :token_exchange_failed}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert Repo.reload!(flow).status == "failed"
    assert {:error, %{code: :flow_not_pending}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert length(FakeOpenAIAuthProvider.requests(provider)) == 3
  end

  test "invite polling retains its pending account across a real transient response and completes later" do
    scope = fixture_owner_scope()
    pool = pool_fixture(%{created_by_user_id: scope.user.id})
    assert {:ok, %{token: token}} = Access.create_invite(scope, pool, %{invited_email: "device-acct_device_invite_recovery@example.com"})
    provider = start_provider!(device_routes(%{"/api/accounts/deviceauth/token" => {503, %{"error" => "temporarily_unavailable"}}}))
    assert {:ok, started} = InviteOnboarding.start_device(token)
    identity = started.account.identity
    assert {:error, %{code: :codex_auth_transient}} = InviteOnboarding.poll_device(token, identity.id)
    assert Repo.reload!(identity).status == "pending"
    assert Repo.reload!(started.account.assignment).status == "pending"
    assert active_secret_count("access_token") == 0
    assert active_secret_count("refresh_token") == 0
    FakeUpstream.set_mode(provider, {:path_json, successful_device_routes("acct_device_invite_recovery")})
    assert {:ok, result} = InviteOnboarding.poll_device(token, identity.id)
    assert result.identity.id == identity.id
    assert Repo.reload!(identity).status == "active"
    assert Enum.map(FakeOpenAIAuthProvider.requests(provider), & &1.path) == ["/api/accounts/deviceauth/usercode", "/api/accounts/deviceauth/token", "/api/accounts/deviceauth/token", "/oauth/token"]
  end

  @tag :subject_plumbing
  test "successful device poll exchanges provider authorization and links tokens atomically" do
    scope = fixture_owner_scope()
    pool = pool_fixture()
    id_token = device_id_token("acct_device_success")
    access_token = "device-access-token-must-be-encrypted"
    refresh_token = "device-refresh-token-must-be-encrypted"

    provider =
      start_provider!(
        device_routes(%{
          "/api/accounts/deviceauth/token" =>
            {200,
             FakeOpenAIAuthProvider.authorization_code_response(
               authorization_code: "device-authorization-code-success",
               code_verifier: "device-code-verifier-success"
             )},
          "/oauth/token" =>
            {200,
             FakeOpenAIAuthProvider.token_response(
               access_token: access_token,
               refresh_token: refresh_token,
               id_token: id_token
             )}
        })
      )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    observer = start_event_observer(pool.id)

    assert {:ok,
            %{
              status: :completed,
              link_status: :created,
              flow: completed_flow,
              identity: identity,
              assignment: assignment,
              secret_status: :present
            } = result} = Upstreams.poll_device_oauth(scope, flow.id)

    assert completed_flow.status == "completed"
    assert completed_flow.result_upstream_identity_id == identity.id
    assert %DateTime{} = completed_flow.completed_at
    assert identity.chatgpt_account_id == "acct_device_success"
    assert identity.chatgpt_user_id == "user_acct_device_success"
    assert identity.account_email == "device-acct_device_success@example.com"
    assert identity.onboarding_method == "device"
    assert identity.credential_provenance == "codex_chatgpt_oauth"
    assert identity.workspace_id == "workspace-device"
    assert identity.plan_family == "team"
    assert assignment.pool_id == pool.id
    assert assignment.upstream_identity_id == identity.id
    assert active_secret_count("access_token") == 1
    assert active_secret_count("refresh_token") == 1

    matching_jobs =
      all_enqueued(worker: AccountReconciliationWorker)
      |> Enum.filter(&(&1.args["pool_upstream_assignment_id"] == assignment.id))

    assert [job] = matching_jobs
    assert job.args["pool_id"] == pool.id
    assert job.args["trigger_kind"] == "account_link"

    assert 1 ==
             Repo.aggregate(
               from(event in AuditEvent,
                 where:
                   event.pool_id == ^pool.id and
                     event.action == "upstream_account.oauth_device_link" and
                     event.target_id == ^identity.id
               ),
               :count
             )

    events = event_snapshot(observer)
    assert Enum.count(events, &(&1.reason == "upstream_account_oauth_linked")) == 1

    assert [_start_request, poll_request, token_request] =
             FakeOpenAIAuthProvider.requests(provider)

    assert poll_request.json == %{
             "device_auth_id" => "device-auth-linking",
             "user_code" => "CODE-LINK"
           }

    form = FakeOpenAIAuthProvider.decode_form_request(token_request)
    assert form["code"] == "device-authorization-code-success"
    assert form["code_verifier"] == "device-code-verifier-success"

    assert form["redirect_uri"] ==
             FakeOpenAIAuthProvider.url(provider) <> "/deviceauth/callback"

    refute_result_contains(result, [
      "device-auth-linking",
      "device-authorization-code-success",
      "device-code-verifier-success",
      id_token,
      access_token,
      refresh_token
    ])
  end

  test "provider expired device code marks the flow expired without linking anything" do
    scope = fixture_owner_scope()
    pool = pool_fixture()

    start_provider!(
      device_routes(%{
        "/api/accounts/deviceauth/token" => {400, %{"error" => "expired_token"}}
      })
    )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    observer = start_event_observer(pool.id)

    assert {:error, %{code: :expired_flow}} = Upstreams.poll_device_oauth(scope, flow.id)

    reloaded = Repo.get!(OAuthFlow, flow.id)
    assert reloaded.status == "expired"
    assert reloaded.error_code == "expired_flow"
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
    assert Repo.aggregate(PoolUpstreamAssignment, :count) == 0
    assert Repo.aggregate(EncryptedSecret, :count) == 0

    assert Repo.aggregate(from(event in AuditEvent, where: event.pool_id == ^pool.id), :count) ==
             0

    assert [] ==
             all_enqueued(worker: AccountReconciliationWorker)
             |> Enum.filter(&(&1.args["pool_id"] == pool.id))

    assert event_snapshot(observer) == []
  end

  test "provider denied device poll stores only safe failure fields" do
    scope = fixture_owner_scope()
    pool = pool_fixture()
    raw_provider_value = "raw-device-denial-token-must-not-leak"

    start_provider!(
      device_routes(%{
        "/api/accounts/deviceauth/token" =>
          {400,
           %{
             "error" => "access_denied",
             "error_description" => raw_provider_value,
             "device_auth_id" => raw_provider_value
           }}
      })
    )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)

    assert {:error, %{code: :provider_denied, message: "OpenAI denied the OAuth request"} = error} =
             Upstreams.poll_device_oauth(scope, flow.id)

    reloaded = Repo.get!(OAuthFlow, flow.id)
    assert reloaded.status == "failed"
    assert reloaded.error_code == "provider_denied"
    assert reloaded.error_message == "OpenAI denied the OAuth request"
    refute inspect(error) =~ raw_provider_value
    refute inspect(reloaded) =~ raw_provider_value
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
    assert Repo.aggregate(PoolUpstreamAssignment, :count) == 0
    assert Repo.aggregate(EncryptedSecret, :count) == 0
  end

  test "cancelled device flow does not poll provider again" do
    scope = fixture_owner_scope()
    pool = pool_fixture()

    provider =
      start_provider!(
        device_routes(%{
          "/api/accounts/deviceauth/token" => {400, %{"error" => "authorization_pending"}}
        })
      )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    assert {:ok, cancelled} = Upstreams.cancel_oauth_flow(scope, flow.id)
    assert cancelled.status == "cancelled"

    assert {:error, %{code: :stale_flow}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert length(FakeOpenAIAuthProvider.requests(provider)) == 1
  end

  test "pending device flow can be resumed from database and completed by a later poll" do
    scope = fixture_owner_scope()
    pool = pool_fixture()

    provider =
      start_provider!(
        device_routes(%{
          "/api/accounts/deviceauth/token" => {400, %{"error" => "authorization_pending"}}
        })
      )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    assert {:ok, %{status: :pending, flow: pending}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert pending.status == "pending"

    FakeUpstream.set_mode(
      provider,
      {:path_json,
       device_routes(%{
         "/api/accounts/deviceauth/token" =>
           {200,
            FakeOpenAIAuthProvider.authorization_code_response(
              authorization_code: "device-authorization-code-resumed",
              code_verifier: "device-code-verifier-resumed"
            )},
         "/oauth/token" => {200, FakeOpenAIAuthProvider.token_response(id_token: device_id_token("acct_device_resumed"))}
       })}
    )

    assert {:ok, %{status: :completed, identity: identity, flow: completed}} =
             Upstreams.poll_device_oauth(scope, flow.id)

    assert completed.status == "completed"
    assert identity.chatgpt_account_id == "acct_device_resumed"
  end

  test "successful device linking does not persist raw device auth id authorization or token values" do
    scope = fixture_owner_scope()
    pool = pool_fixture()
    access_token = "raw-device-access-token-must-not-leak"
    refresh_token = "raw-device-refresh-token-must-not-leak"
    id_token = device_id_token("acct_device_secret_boundary")

    start_provider!(
      device_routes(%{
        "/api/accounts/deviceauth/token" =>
          {200,
           FakeOpenAIAuthProvider.authorization_code_response(
             authorization_code: "raw-device-authorization-code-must-not-leak",
             code_verifier: "raw-device-code-verifier-must-not-leak"
           )},
        "/oauth/token" =>
          {200,
           FakeOpenAIAuthProvider.token_response(
             access_token: access_token,
             refresh_token: refresh_token,
             id_token: id_token
           )}
      })
    )

    assert {:ok, %{flow: flow}} = Upstreams.start_device_oauth(scope, pool)
    assert {:ok, %{flow: completed_flow}} = Upstreams.poll_device_oauth(scope, flow.id)

    persisted_flow = Repo.get!(OAuthFlow, completed_flow.id)
    secrets = Repo.all(EncryptedSecret)

    for raw_value <- [
          "device-auth-linking",
          "raw-device-authorization-code-must-not-leak",
          "raw-device-code-verifier-must-not-leak",
          id_token,
          access_token,
          refresh_token
        ] do
      refute inspect(persisted_flow.metadata) =~ raw_value
      refute inspect(persisted_flow) =~ raw_value
      refute inspect(secrets) =~ raw_value
    end
  end

  defp fixture_owner_scope do
    %{user: user} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    Scope.for_user(user, ["instance_owner"])
  end

  defp start_event_observer(pool_id) do
    parent = self()

    observer =
      spawn_link(fn ->
        :ok = Events.subscribe_pool(pool_id, "upstreams")
        send(parent, {:event_observer_ready, self()})
        observe_events([])
      end)

    assert_receive {:event_observer_ready, ^observer}
    on_exit(fn -> send(observer, :stop) end)
    observer
  end

  defp observe_events(events) do
    receive do
      {Events, event} ->
        observe_events([event | events])

      {:snapshot, caller, ref} ->
        send(caller, {ref, Enum.reverse(events)})
        observe_events(events)

      :stop ->
        :ok
    end
  end

  defp event_snapshot(observer) do
    ref = make_ref()
    send(observer, {:snapshot, self(), ref})
    assert_receive {^ref, events}
    events
  end

  defp start_provider!(routes) do
    name = :"device_linking_provider_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      if pid = Process.whereis(name) do
        FakeOpenAIAuthProvider.stop(%FakeUpstream{supervisor: pid})
        refute Process.alive?(pid)
      end

      assert Process.whereis(name) == nil
    end)

    {:ok, provider} = FakeUpstream.start_link({:path_json, routes}, supervisor_name: name)
    Process.unlink(provider.supervisor)
    Application.put_env(:codex_pooler, CodexAuth, issuer: FakeOpenAIAuthProvider.url(provider))
    provider
  end

  defp assert_transient_flow_pending(scope, flow) do
    result = Upstreams.poll_device_oauth(scope, flow.id)
    stored = Repo.reload!(flow)
    assert stored.status == "pending"
    assert {:ok, %{status: :pending, flow: pending}} = result
    assert stored.id == pending.id
    assert stored.error_code == nil
    assert stored.error_message == nil
    assert stored.interval_seconds == flow.interval_seconds
    assert DateTime.compare(stored.expires_at, flow.expires_at) == :eq
    assert DateTime.diff(stored.poll_after_at, stored.last_polled_at, :second) == flow.interval_seconds
    assert Repo.aggregate(UpstreamIdentity, :count) == 0
    assert Repo.aggregate(PoolUpstreamAssignment, :count) == 0
    assert Repo.aggregate(EncryptedSecret, :count) == 0
  end

  defp assert_flow_recovers(scope, flow) do
    assert {:ok, %{status: :completed, flow: completed, identity: identity}} = Upstreams.poll_device_oauth(scope, flow.id)
    assert completed.id == flow.id
    assert Repo.reload!(flow).status == "completed"
    assert Repo.reload!(identity).status == "active"
    assert active_secret_count("access_token") == 1
    assert active_secret_count("refresh_token") == 1
  end

  defp device_poll_state, do: %{"device_auth_id" => "device-auth-linking", "user_code" => "CODE-LINK", "poll_interval_seconds" => 5}

  defp successful_device_routes(account_id) do
    device_routes(%{
      "/api/accounts/deviceauth/token" => {200, FakeOpenAIAuthProvider.authorization_code_response()},
      "/oauth/token" => {200, FakeOpenAIAuthProvider.token_response(id_token: device_id_token(account_id))}
    })
  end

  defp device_routes(extra_routes) do
    Map.merge(
      %{
        "/api/accounts/deviceauth/usercode" =>
          {200,
           FakeOpenAIAuthProvider.device_code_response(
             device_auth_id: "device-auth-linking",
             user_code: "CODE-LINK",
             interval: 5,
             expires_at: DateTime.add(DateTime.utc_now(), 600, :second) |> DateTime.to_iso8601()
           )}
      },
      extra_routes
    )
  end

  defp device_id_token(account_id) do
    FakeOpenAIAuthProvider.id_token(%{
      "email" => "device-#{account_id}@example.com",
      "https://api.openai.com/auth" => %{
        "chatgpt_account_id" => account_id,
        "chatgpt_user_id" => "user_#{account_id}",
        "chatgpt_plan_type" => "team",
        "workspace_id" => "workspace-device",
        "workspace_label" => "Device Workspace",
        "seat_type" => "team-seat"
      }
    })
  end

  defp active_secret_count(secret_kind) do
    Repo.aggregate(
      from(secret in EncryptedSecret,
        where: secret.secret_kind == ^secret_kind and secret.status == "active"
      ),
      :count
    )
  end

  defp refute_result_contains(result, raw_values) do
    result_text = inspect(result)

    Enum.each(raw_values, fn raw_value ->
      refute result_text =~ raw_value
    end)
  end

  defp restore_codex_auth_config! do
    previous = Application.get_env(:codex_pooler, CodexAuth)

    on_exit(fn ->
      if previous do
        Application.put_env(:codex_pooler, CodexAuth, previous)
      else
        Application.delete_env(:codex_pooler, CodexAuth)
      end
    end)
  end

  defp configure_upstream_secret_key! do
    previous = Application.get_env(:codex_pooler, CodexPooler.Upstreams)

    Application.put_env(:codex_pooler, CodexPooler.Upstreams,
      upstream_secret_key: Base.encode64(:crypto.hash(:sha256, "test-upstream-secret-key")),
      upstream_secret_key_version: "test-v1"
    )

    on_exit(fn ->
      if previous do
        Application.put_env(:codex_pooler, CodexPooler.Upstreams, previous)
      else
        Application.delete_env(:codex_pooler, CodexPooler.Upstreams)
      end
    end)
  end
end
