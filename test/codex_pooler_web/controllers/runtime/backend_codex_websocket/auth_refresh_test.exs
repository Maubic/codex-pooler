defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.AuthRefreshTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, Request, RequestReplay}
  alias CodexPooler.FakeUpstream

  alias CodexPooler.Gateway.Persistence.{
    BridgeDemotion,
    CodexSession,
    CodexTurn,
    RoutingCircuitState
  }

  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Lifecycle.IdentityLifecycle
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPoolerWeb.CodexResponsesSocket
  alias Ecto.Adapters.SQL.Sandbox

  # Failure-detection budget for an expected message: a green run returns as
  # soon as the message arrives, so only a missing one spends it.
  @detection_timeout_ms 15_000

  @large_websocket_frame_timeout 5_000
  # Detection budget for a server-side connection teardown the test only
  # observes, never a scenario timeout.
  @connection_shutdown_timeout_ms 15_000

  @tag :feature_websocket_terminal_auth_refresh
  test "websocket handshake 401 refreshes once and retries the same assignment" do
    initial_residency = "ws-initial-region-#{System.unique_integer([:positive])}"
    refreshed_residency = "ws-refreshed-region-#{System.unique_integer([:positive])}"
    initial_access_token = synthetic_access_token(initial_residency)
    refreshed_access_token = synthetic_access_token(refreshed_residency)

    # Strict: a 401 handshake, one provider token refresh, then the retried
    # handshake succeeds and the turn lands on the first accepted connection.
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "GET",
            respond:
              FakeUpstream.websocket_upgrade_error(
                %{"error" => %{"code" => "invalid_api_key"}},
                status: 401,
                headers: [{"x-openai-authorization-error", "invalid_api_key"}]
              )
          ),
          FakeUpstream.expect_request(
            method: "POST",
            path: "/oauth/token",
            respond: FakeUpstream.json_response(%{"access_token" => refreshed_access_token}, 200)
          ),
          strict_native_response_payload(websocket_auth_retry_success_payload("handshake_401"), 1)
        ])
      )

    setup = gateway_setup(upstream)

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "access_token",
               plaintext: initial_access_token
             })

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "refresh_token",
               plaintext: "refresh-token-ws-handshake-do-not-leak"
             })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    logs =
      capture_log(fn ->
        capture_stream_outcome_telemetry(fn ->
          assert :ok =
                   execute_websocket_response(
                     auth,
                     websocket_auth_refresh_payload(setup, "handshake-401"),
                     %{request_id: "ws-auth-handshake-401"},
                     fn frame -> send(self(), {:websocket_frame, frame}) end
                   )

          assert_receive {:stream_outcome, telemetry_metadata}
          refute inspect(telemetry_metadata) =~ initial_residency
          refute inspect(telemetry_metadata) =~ refreshed_residency
          refute inspect(telemetry_metadata) =~ initial_access_token
          refute inspect(telemetry_metadata) =~ refreshed_access_token
        end)
      end)

    assert_received {:websocket_frame, frame}
    assert %{"id" => "resp_ws_auth_retry_handshake_401"} = CodexPooler.JSON.decode!(frame)

    [refresh_request, retried_request] = FakeUpstream.requests(upstream)
    assert refresh_request.path == "/oauth/token"
    assert retried_request.method == "WEBSOCKET"
    assert retried_request.path == "/backend-api/codex/responses"
    assert Map.new(retried_request.headers)["authorization"] == "Bearer #{refreshed_access_token}"

    assert header_values(retried_request.headers, "x-openai-internal-codex-residency") == [
             refreshed_residency
           ]

    refute initial_residency in header_values(
             retried_request.headers,
             "x-openai-internal-codex-residency"
           )

    assert header_values(retried_request.headers, "chatgpt-account-id") == [
             setup.identity.chatgpt_account_id
           ]

    assert FakeUpstream.websocket_connection_count(upstream) == 1

    assert [first_attempt, second_attempt] =
             Repo.all(from(a in Attempt, order_by: [asc: a.attempt_number]))

    assert first_attempt.pool_upstream_assignment_id == setup.assignment.id
    assert first_attempt.status == "retryable_failed"
    assert first_attempt.network_error_code == "upstream_unauthorized"

    assert second_attempt.pool_upstream_assignment_id == setup.assignment.id
    assert second_attempt.status == "succeeded"

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "succeeded"
    assert request.retry_count == 1
    assert request.last_error_code == nil
    assert request.request_metadata["auth_refresh"]["status"] == "succeeded"

    metadata_text = inspect({request.request_metadata, first_attempt.response_metadata})
    refute metadata_text =~ setup.authorization
    refute metadata_text =~ "refresh-token-ws-handshake-do-not-leak"
    refute metadata_text =~ initial_residency
    refute metadata_text =~ refreshed_residency
    refute metadata_text =~ initial_access_token
    refute metadata_text =~ refreshed_access_token

    assert_websocket_values_not_persisted!(
      setup,
      [initial_residency, refreshed_residency, initial_access_token, refreshed_access_token],
      logs
    )
  end

  @tag :replay_generation_race
  @tag :replay_race
  test "stale generation handshake auth failure exits without refresh retry or downstream frame" do
    release_ref = make_ref()

    upstream =
      start_upstream(
        FakeUpstream.websocket_upgrade_error(
          %{"error" => %{"code" => "invalid_api_key"}},
          status: 401,
          headers: [{"x-openai-authorization-error", "invalid_api_key"}],
          notify: self(),
          release_ref: release_ref
        )
      )

    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    parent = self()

    client =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())

        execute_websocket_response(
          auth,
          websocket_auth_refresh_payload(setup, "stale-replay-generation"),
          %{
            request_id: "ws-auth-stale-replay-generation",
            accepted_turn_state: Ecto.UUID.generate()
          },
          fn frame -> send(parent, {:stale_auth_websocket_frame, frame}) end
        )
      end)

    assert_receive {:fake_upstream_timeout_barrier, :before_headers, upstream_pid, ^release_ref},
                   @large_websocket_frame_timeout

    assert [request] = Repo.all(from request in Request, where: request.pool_id == ^setup.pool.id)
    assert [attempt] = Repo.all(from attempt in Attempt, where: attempt.request_id == ^request.id)
    assert turn = Repo.get_by!(CodexTurn, request_id: request.id)

    turn
    |> Ecto.Changeset.change(%{semantic_turn_digest: <<1::256>>})
    |> Repo.update!()

    session = Repo.get!(CodexSession, turn.codex_session_id)

    assert {:ok, _armed} =
             RequestReplay.arm(%{
               api_key_id: auth.api_key.id,
               pool_id: auth.pool.id,
               codex_session_id: session.id,
               request_id: request.id,
               codex_turn_id: turn.id,
               eligible_attempt_id: attempt.id,
               api_key_runtime_epoch: auth.api_key.runtime_revocation_epoch,
               model_id: setup.model.id,
               model_identifier: setup.model.exposed_model_id,
               endpoint: request.endpoint,
               semantic_turn_digest: <<1::256>>,
               replay_claim_digest: <<2::256>>,
               owner_instance_id: session.owner_instance_id,
               owner_lease_token: session.owner_lease_token,
               predecessor_epoch: 1,
               failure_reason: :client_disconnected,
               pre_visible_output: true
             })

    send(upstream_pid, {:fake_upstream_release_timeout, release_ref})

    assert :ok = Task.await(client, @connection_shutdown_timeout_ms)
    refute_received {:stale_auth_websocket_frame, _frame}
    refute Enum.any?(FakeUpstream.requests(upstream), &(&1.path == "/oauth/token"))
    assert FakeUpstream.websocket_connection_count(upstream) == 0
    assert Repo.reload!(request).status == "in_progress"
    assert Repo.reload!(attempt).status == "retryable_failed"
  end

  @tag :feature_websocket_terminal_auth_refresh
  test "websocket auth failure under a replaced credential epoch skips the provider refresh" do
    release_ref = make_ref()

    # No /oauth/token entry in the sequence: a provider refresh would consume
    # the retry success payload and fail the test loudly.
    # Strict: a held 401 handshake, then the retried handshake succeeds
    # without any provider refresh in between.
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "GET",
            respond:
              FakeUpstream.websocket_upgrade_error(
                %{"error" => %{"code" => "invalid_api_key"}},
                status: 401,
                headers: [{"x-openai-authorization-error", "invalid_api_key"}],
                notify: self(),
                release_ref: release_ref
              )
          ),
          strict_native_response_payload(websocket_auth_retry_success_payload("stale_epoch"), 1)
        ])
      )

    setup = gateway_setup(upstream)
    original_epoch = CredentialFencing.credential_epoch(setup.identity)

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    parent = self()

    client =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())

        execute_websocket_response(
          auth,
          websocket_auth_refresh_payload(setup, "stale-epoch"),
          %{request_id: "ws-auth-stale-epoch"},
          fn frame -> send(parent, {:websocket_frame, frame}) end
        )
      end)

    # The dispatch has connected with the original credentials; rotate them
    # before the 401 is delivered, as a concurrent refresh would.
    assert_receive {:fake_upstream_timeout_barrier, :before_headers, upstream_pid, ^release_ref},
                   5_000

    identity = Repo.get!(UpstreamIdentity, setup.identity.id)

    identity
    |> Ecto.Changeset.change(%{metadata: CredentialFencing.advance_credential_epoch(identity)})
    |> Repo.update!()

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(identity, %{
               secret_kind: "access_token",
               plaintext: "rotated-ws-token-do-not-leak"
             })

    send(upstream_pid, {:fake_upstream_release_timeout, release_ref})

    assert :ok = Task.await(client, 5_000)

    assert_received {:websocket_frame, frame}
    assert %{"id" => "resp_ws_auth_retry_stale_epoch"} = CodexPooler.JSON.decode!(frame)

    # The stale 401 never reached the provider: no OAuth request, and the
    # retry ran with the rotated token stored by the concurrent refresh.
    # The rejected upgrade never records a request row, so the sole entry is
    # the retried connection.
    requests = FakeUpstream.requests(upstream)
    refute Enum.any?(requests, &(&1.path == "/oauth/token"))

    assert [retried] = requests
    assert retried.method == "WEBSOCKET"
    assert Map.new(retried.headers)["authorization"] == "Bearer rotated-ws-token-do-not-leak"

    persisted = Repo.get!(UpstreamIdentity, setup.identity.id)
    assert CredentialFencing.credential_epoch(persisted) == original_epoch + 1

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "succeeded"
    assert request.retry_count == 1
    assert request.request_metadata["auth_refresh"]["status"] == "succeeded"

    metadata_text = inspect(request.request_metadata)
    refute metadata_text =~ "rotated-ws-token-do-not-leak"
  end

  for auth_code <- ["invalid_api_key", "invalid_authentication"] do
    @auth_code auth_code
    @tag :feature_websocket_terminal_auth_refresh
    test "websocket pre-visible terminal auth #{auth_code} refreshes once and retries the same assignment" do
      auth_code = @auth_code

      upstream =
        start_upstream(
          # Strict finite scenario: one terminal auth failure, exactly one
          # provider refresh, then one retry on a replacement connection.
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([
            strict_native_request(1, websocket_terminal_auth_failure(auth_code)),
            strict_oauth_refresh(FakeUpstream.json_response(%{"access_token" => "upstream-token-refreshed"}, 200)),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 2,
              json: [valid: true, equals: %{"type" => "response.create"}],
              headers: [required: %{"authorization" => "Bearer upstream-token-refreshed"}],
              respond:
                FakeUpstream.websocket_text_frames([
                  CodexPooler.JSON.encode!(websocket_auth_retry_success_payload(auth_code))
                ])
            )
          ])
        )

      setup = gateway_setup(upstream)

      assert {:ok, _secret} =
               Upstreams.store_encrypted_secret(setup.identity, %{
                 secret_kind: "refresh_token",
                 plaintext: "refresh-token-ws-terminal-do-not-leak"
               })

      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

      assert :ok =
               execute_websocket_response(
                 auth,
                 websocket_auth_refresh_payload(setup, auth_code),
                 %{request_id: "ws-auth-terminal-#{auth_code}"},
                 fn frame -> send(self(), {:websocket_frame, frame}) end
               )

      expected_response_id = "resp_ws_auth_retry_#{auth_code}"
      assert_received {:websocket_frame, frame}
      assert %{"id" => ^expected_response_id} = CodexPooler.JSON.decode!(frame)
      refute_received {:websocket_frame, _unexpected}

      [first_request, refresh_request, retried_request] = FakeUpstream.requests(upstream)
      assert first_request.method == "WEBSOCKET"
      assert refresh_request.path == "/oauth/token"
      assert retried_request.method == "WEBSOCKET"

      assert Map.new(retried_request.headers)["authorization"] ==
               "Bearer upstream-token-refreshed"

      assert FakeUpstream.websocket_connection_count(upstream) == 2

      assert [first_attempt, second_attempt] =
               Repo.all(from(a in Attempt, order_by: [asc: a.attempt_number]))

      assert first_attempt.pool_upstream_assignment_id == setup.assignment.id
      assert first_attempt.status == "retryable_failed"
      assert first_attempt.network_error_code == "upstream_unauthorized"
      assert first_attempt.response_metadata["stream_failure_stage"] == "first_event"
      assert first_attempt.response_metadata["stream_error_code"] == auth_code
      assert first_attempt.response_metadata["upstream_error_param"] == "reasoning.effort"

      assert second_attempt.pool_upstream_assignment_id == setup.assignment.id
      assert second_attempt.status == "succeeded"

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert request.status == "succeeded"
      assert request.retry_count == 1
      assert request.last_error_code == nil
      assert request.request_metadata["auth_refresh"]["status"] == "succeeded"

      assert Repo.all(from(d in BridgeDemotion)) == []
      assert Repo.all(from(c in RoutingCircuitState)) == []

      metadata_text = inspect({request.request_metadata, first_attempt.response_metadata})
      refute metadata_text =~ setup.authorization
      refute metadata_text =~ "refresh-token-ws-terminal-do-not-leak"
      refute metadata_text =~ "upstream-token-refreshed"
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  @tag :feature_websocket_terminal_auth_refresh_failures
  test "websocket terminal auth preserves original failure when refresh is already in progress" do
    release_ref = make_ref()

    upstream =
      start_upstream(
        # Strict finite scenario: the terminal auth failure is held behind a
        # native barrier so the identity can be marked refreshing first; no
        # /oauth/token entry and no retry entry exist, so either request fails
        # the fixture as an unexpected extra request.
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          strict_native_request(
            1,
            FakeUpstream.websocket_terminal_then_close_barrier(
              %{
                "type" => "response.failed",
                "response" => %{
                  "id" => "resp_ws_auth_refresh_in_progress",
                  "error" => %{"code" => "invalid_api_key"},
                  "usage" => %{"input_tokens" => 4, "output_tokens" => 0, "total_tokens" => 4}
                }
              },
              notify: self(),
              release_ref: release_ref
            )
          )
        ])
      )

    setup = gateway_setup(upstream)

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "refresh_token",
               plaintext: "refresh-token-ws-in-progress-do-not-leak"
             })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    parent = self()

    task =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())

        execute_websocket_response(
          auth,
          websocket_auth_refresh_payload(setup, "refresh-in-progress"),
          %{request_id: "ws-auth-refresh-in-progress"},
          fn frame -> send(parent, {:websocket_frame, frame}) end
        )
      end)

    assert_receive {:fake_upstream_websocket_barrier, :before_terminal, upstream_pid, ^release_ref},
                   @detection_timeout_ms

    metadata = active_token_refresh_metadata()

    assert {:ok, _identity} =
             IdentityLifecycle.update_upstream_identity(setup.identity, %{
               status: "refreshing",
               metadata: Map.put(setup.identity.metadata || %{}, "token_refresh", metadata)
             })

    send(upstream_pid, {:fake_upstream_release_websocket, release_ref})
    assert :ok = Task.await(task, 2_000)

    # The native barrier holds the post-terminal close as well; release it so
    # the fake connection can retire cleanly after the failure has been
    # observed.
    assert_receive {:fake_upstream_websocket_barrier, :before_close, ^upstream_pid, ^release_ref},
                   @detection_timeout_ms

    send(upstream_pid, {:fake_upstream_release_websocket, release_ref})

    assert_received {:websocket_frame, frame}

    assert %{
             "type" => "response.failed",
             "response" => %{"error" => %{"code" => "invalid_api_key"}}
           } =
             CodexPooler.JSON.decode!(frame)

    assert [first_request] = FakeUpstream.requests(upstream)
    assert first_request.method == "WEBSOCKET"
    assert FakeUpstream.websocket_connection_count(upstream) == 1

    assert [attempt] = Repo.all(from(a in Attempt))
    assert attempt.status == "failed"
    assert attempt.network_error_code == "invalid_api_key"

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "failed"
    assert request.retry_count == 0
    assert request.last_error_code == "invalid_api_key"

    assert request.request_metadata["auth_refresh"] == %{
             "status" => "refresh_in_progress",
             "attempt_id" => metadata["attempt_id"],
             "generation" => metadata["generation"],
             "started_at" => metadata["started_at"],
             "stale_after_ms" => metadata["stale_after_ms"],
             "trigger_kind" => "websocket_terminal_auth_failure"
           }

    metadata_text = inspect({request.request_metadata, attempt.response_metadata})
    refute metadata_text =~ setup.authorization
    refute metadata_text =~ "refresh-token-ws-in-progress-do-not-leak"
    assert :ok = FakeUpstream.verify!(upstream)
  end

  for {refresh_status, refresh_response_status, refresh_response_body} <- [
        {"reauth_required", 400, %{"error" => "invalid_grant"}},
        {"refresh_failed", 503, %{"error" => "temporary"}}
      ] do
    @refresh_status refresh_status
    @refresh_response_status refresh_response_status
    @refresh_response_body refresh_response_body
    @tag :feature_websocket_terminal_auth_refresh_failures
    test "websocket terminal auth preserves original failure when refresh marks #{@refresh_status}" do
      refresh_status = @refresh_status

      upstream =
        start_upstream(
          # Strict finite scenario: one terminal auth failure and one failed
          # provider refresh; there is no retry entry, so a redispatch fails
          # the fixture as an unexpected extra request.
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([
            strict_native_request(1, websocket_terminal_auth_failure("invalid_authentication")),
            strict_oauth_refresh(FakeUpstream.json_response(@refresh_response_body, @refresh_response_status))
          ])
        )

      setup = gateway_setup(upstream)

      assert {:ok, _secret} =
               Upstreams.store_encrypted_secret(setup.identity, %{
                 secret_kind: "refresh_token",
                 plaintext: "refresh-token-ws-#{refresh_status}-do-not-leak"
               })

      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

      assert :ok =
               execute_websocket_response(
                 auth,
                 websocket_auth_refresh_payload(setup, refresh_status),
                 %{request_id: "ws-auth-refresh-#{refresh_status}"},
                 fn frame -> send(self(), {:websocket_frame, frame}) end
               )

      assert_received {:websocket_frame, frame}

      assert %{
               "type" => "response.failed",
               "response" => %{"error" => %{"code" => "invalid_authentication"}}
             } = CodexPooler.JSON.decode!(frame)

      assert [first_request, refresh_request] = FakeUpstream.requests(upstream)
      assert first_request.method == "WEBSOCKET"
      assert refresh_request.path == "/oauth/token"
      assert FakeUpstream.websocket_connection_count(upstream) == 1

      assert [attempt] = Repo.all(from(a in Attempt))
      assert attempt.status == "failed"
      assert attempt.network_error_code == "invalid_authentication"

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert request.status == "failed"
      assert request.retry_count == 0
      assert request.last_error_code == "invalid_authentication"
      assert request.request_metadata["auth_refresh"]["status"] == refresh_status

      assert request.request_metadata["auth_refresh"]["trigger_kind"] ==
               "websocket_terminal_auth_failure"

      metadata_text = inspect({request.request_metadata, attempt.response_metadata})
      refute metadata_text =~ setup.authorization
      refute metadata_text =~ "refresh-token-ws-#{refresh_status}-do-not-leak"
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  # A handshake 401 whose refresh cannot retry, with another eligible
  # candidate. The first attempt is recorded retryable before the refresh
  # runs, so the exhausted path fails over without recording it again; the
  # second record raised `attempt_already_finalized` and left the request open,
  # its reservation held, until the six-hour stale-reservation sweep
  # (findings#325). The failed assignment's route health records the auth
  # failure, except under a refresh another caller is running, which completes
  # neutrally as it does on HTTP.
  for mode <- ["full", "lite"],
      {refresh_status, refresh} <- [
        {"reauth_required", {:provider, 400, %{"error" => "invalid_grant"}}},
        {"refresh_failed", {:provider, 503, %{"error" => "temporary"}}},
        {"noop", :identity_paused},
        {"refresh_in_progress", :identity_refreshing}
      ] do
    @mode mode
    @refresh_status refresh_status
    @refresh refresh
    @tag :feature_websocket_terminal_auth_refresh_failures
    test "#{mode}: websocket handshake 401 whose refresh returns #{refresh_status} fails over to the next candidate" do
      refresh_status = @refresh_status
      release_ref = make_ref()

      # Strict: the preferred candidate's 401 handshake, held so the identity
      # can change after routing, then its provider refresh when the refresh
      # reaches the provider. No retry entry: a redispatch to this candidate
      # fails the fixture as an unexpected extra request.
      first_upstream =
        start_upstream(
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([held_websocket_handshake_401(self(), release_ref) | provider_refresh(@refresh)])
        )

      # Strict: the next candidate serves the turn on its first connection.
      second_upstream =
        start_upstream(
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([
            strict_native_response_payload(websocket_auth_retry_success_payload("failover_#{refresh_status}"), 1)
          ])
        )

      {setup, second} = websocket_failover_candidates!(first_upstream, second_upstream, @mode)

      assert {:ok, _secret} =
               Upstreams.store_encrypted_secret(setup.identity, %{
                 secret_kind: "refresh_token",
                 plaintext: "refresh-token-ws-failover-do-not-leak"
               })

      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      request_id = seed_preferring_assignment([setup.assignment.id, second.assignment.id], setup.assignment.id)
      parent = self()

      client =
        Task.async(fn ->
          Sandbox.allow(Repo, parent, self())

          execute_websocket_response(
            auth,
            websocket_auth_refresh_payload(setup, "failover-#{refresh_status}"),
            %{request_id: request_id},
            fn frame -> send(parent, {:websocket_frame, frame}) end
          )
        end)

      assert_receive {:fake_upstream_timeout_barrier, :before_headers, upstream_pid, ^release_ref}, @detection_timeout_ms
      :ok = put_refresh_state!(setup.identity, @refresh)
      send(upstream_pid, {:fake_upstream_release_timeout, release_ref})

      assert :ok = Task.await(client, @detection_timeout_ms)
      assert_received {:websocket_frame, frame}
      assert CodexPooler.JSON.decode!(frame)["id"] == "resp_ws_auth_retry_failover_#{refresh_status}"
      refute_received {:websocket_frame, _frame}

      assert [first_attempt, second_attempt] = Repo.all(from(a in Attempt, order_by: [asc: a.attempt_number]))
      assert {first_attempt.pool_upstream_assignment_id, first_attempt.status, first_attempt.network_error_code} == {setup.assignment.id, "retryable_failed", "upstream_unauthorized"}
      assert {second_attempt.pool_upstream_assignment_id, second_attempt.status} == {second.assignment.id, "succeeded"}

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert {request.status, request.last_error_code} == {"succeeded", nil}
      assert request.request_metadata["routing"]["model_serving_mode"] == @mode
      assert request.request_metadata["auth_refresh"]["status"] == refresh_status
      assert request.request_metadata["auth_refresh"]["trigger_kind"] == "websocket_terminal_auth_failure"
      assert route_circuit_failures(setup.assignment.id) == expected_route_failures(refresh_status)
      assert length(account_reconciliation_jobs(setup.identity.id)) == expected_reconciliations(refresh_status)

      assert :ok = FakeUpstream.verify!(first_upstream)
      assert :ok = FakeUpstream.verify!(second_upstream)
      refute inspect({request.request_metadata, first_attempt.response_metadata}) =~ "refresh-token-ws-failover-do-not-leak"
    end
  end

  # The same handshake 401 on the last candidate has nowhere to fail over: the
  # settlement replaces the attempt's retryable record and releases the
  # reservation, so nothing is left for the stale-reservation sweep
  # (findings#325). It settles as HTTP's exhausted auth does (row 325-7):
  # `503 upstream_unauthorized`, the route failure and an account
  # reconciliation, or, under another caller's refresh, a neutral completion
  # and no reconciliation.
  for mode <- ["full", "lite"],
      {refresh_status, refresh} <- [
        {"reauth_required", {:provider, 400, %{"error" => "invalid_grant"}}},
        {"refresh_failed", {:provider, 503, %{"error" => "temporary"}}},
        {"noop", :identity_paused},
        {"refresh_in_progress", :identity_refreshing}
      ] do
    @mode mode
    @refresh_status refresh_status
    @refresh refresh
    @tag :feature_websocket_terminal_auth_refresh_failures
    test "#{mode}: websocket handshake 401 whose refresh returns #{refresh_status} on the last candidate settles the request" do
      refresh_status = @refresh_status
      release_ref = make_ref()

      # Strict: the only candidate's held 401 handshake, then its provider
      # refresh when one is made, and nothing after it.
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([held_websocket_handshake_401(self(), release_ref) | provider_refresh(@refresh)])
        )

      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, @mode)

      assert {:ok, _secret} =
               Upstreams.store_encrypted_secret(setup.identity, %{
                 secret_kind: "refresh_token",
                 plaintext: "refresh-token-ws-last-candidate-do-not-leak"
               })

      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      parent = self()

      client =
        Task.async(fn ->
          Sandbox.allow(Repo, parent, self())

          execute_websocket_response(
            auth,
            websocket_auth_refresh_payload(setup, "last-candidate-#{refresh_status}"),
            %{request_id: "ws-auth-last-candidate-#{@mode}-#{refresh_status}"},
            fn frame -> send(parent, {:websocket_frame, frame}) end
          )
        end)

      assert_receive {:fake_upstream_timeout_barrier, :before_headers, upstream_pid, ^release_ref}, @detection_timeout_ms
      :ok = put_refresh_state!(setup.identity, @refresh)
      send(upstream_pid, {:fake_upstream_release_timeout, release_ref})

      assert {:error, %{status: 503, code: "upstream_unauthorized", message: "upstream authentication failed; retry the request"}} = Task.await(client, @detection_timeout_ms)

      assert [attempt] = Repo.all(from(a in Attempt))
      assert {attempt.status, attempt.network_error_code} == {"failed", "upstream_unauthorized"}

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert {request.status, request.last_error_code, request.response_status_code} == {"failed", "upstream_unauthorized", 503}
      assert request.completed_at
      assert request.request_metadata["routing"]["model_serving_mode"] == @mode
      assert request.request_metadata["auth_refresh"]["status"] == refresh_status
      assert_exhausted_auth_health!(setup, refresh_status)
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  @tag :feature_websocket_terminal_auth_refresh_failures
  test "websocket disconnect during terminal auth refresh drains the response task without DB noise" do
    release_ref = make_ref()

    upstream =
      start_upstream(
        # Strict finite scenario: one terminal auth failure, one held provider
        # refresh, then exactly one retry drained after the client disconnect.
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          strict_native_request(1, websocket_terminal_auth_failure("invalid_api_key")),
          strict_oauth_refresh(
            FakeUpstream.barrier_json_response(
              %{"access_token" => "upstream-token-refreshed"},
              notify: self(),
              release_ref: release_ref
            )
          ),
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            path: "/backend-api/codex/responses",
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond:
              FakeUpstream.websocket_text_frames([
                CodexPooler.JSON.encode!(websocket_auth_retry_success_payload("disconnect_refresh"))
              ])
          )
        ])
      )

    setup = gateway_setup(upstream)

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "refresh_token",
               plaintext: "refresh-token-ws-disconnect-do-not-leak"
             })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    {:ok, state} =
      CodexResponsesSocket.init(%{
        auth: auth,
        opts: %{
          request_id: "ws-auth-refresh-disconnect",
          accepted_turn_state: "stable-ws-auth-refresh-disconnect",
          client_ip: "127.0.0.1"
        }
      })

    assert {:ok, state} =
             CodexResponsesSocket.handle_in(
               {websocket_auth_refresh_payload(setup, "disconnect-refresh"), [opcode: :text]},
               state
             )

    {state, refresh_pid} = receive_socket_upstream_barrier!(state, {:fake_upstream_timeout_barrier, :before_headers, release_ref}, @detection_timeout_ms)

    log =
      capture_log(fn ->
        socket = self()

        tracer =
          Task.async(fn ->
            1 = :erlang.trace_pattern({CodexResponsesSocket, :terminate, 2}, true, [:local])
            1 = :erlang.trace(socket, true, [:call, :arity, {:tracer, self()}])
            send(socket, {:auth_refresh_drain_trace_ready, self()})

            try do
              receive do
                {:trace, ^socket, :call, {CodexResponsesSocket, :terminate, 2}} ->
                  send(refresh_pid, {:fake_upstream_release_timeout, release_ref})
                  :ok
              after
                @detection_timeout_ms -> flunk("the actual socket never entered its auth-refresh termination drain")
              end
            after
              :erlang.trace_pattern({CodexResponsesSocket, :terminate, 2}, false, [:local])
              :erlang.trace(socket, false, [:call, :arity])
            end
          end)

        assert_receive {:auth_refresh_drain_trace_ready, tracer_pid}, @detection_timeout_ms
        assert tracer_pid == tracer.pid
        assert :ok = CodexResponsesSocket.terminate(:closed, state)
        assert :ok = Task.await(tracer, @connection_shutdown_timeout_ms)
      end)

    assert [first_request, refresh_request, retried_request] = FakeUpstream.requests(upstream)
    assert first_request.method == "WEBSOCKET"
    assert refresh_request.path == "/oauth/token"
    assert retried_request.method == "WEBSOCKET"

    assert [first_attempt, second_attempt] =
             Repo.all(from(a in Attempt, order_by: [asc: a.attempt_number]))

    assert first_attempt.status == "retryable_failed"
    assert first_attempt.network_error_code == "upstream_unauthorized"
    assert second_attempt.status == "succeeded"

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "succeeded"
    assert request.retry_count == 1
    assert request.last_error_code == nil
    assert request.request_metadata["auth_refresh"]["status"] == "succeeded"

    assert [turn] = Repo.all(from(t in CodexTurn, where: t.request_id == ^request.id))
    assert turn.status == "succeeded"
    assert Repo.get!(CodexSession, state.codex_session.id).status == "active"
    assert request.response_status_code == 200
    assert turn.error_code == nil

    refute log =~ "Postgrex.Protocol"
    refute log =~ "DBConnection"
    refute log =~ "client "
    refute log =~ " exited"

    metadata_text = inspect({request.request_metadata, first_attempt.response_metadata})
    refute metadata_text =~ setup.authorization
    refute metadata_text =~ "refresh-token-ws-disconnect-do-not-leak"
    refute metadata_text =~ "upstream-token-refreshed"
  end

  @tag :feature_websocket_terminal_auth_refresh
  test "websocket pre-visible terminal non-auth failure does not refresh or retry" do
    upstream =
      start_upstream(
        FakeUpstream.sse_stream(
          [
            {"response.failed",
             %{
               "type" => "response.failed",
               "response" => %{
                 "id" => "resp_ws_non_auth_terminal",
                 "error" => %{"code" => "upstream_terminal_failure"},
                 "usage" => %{"input_tokens" => 4, "output_tokens" => 0, "total_tokens" => 4}
               }
             }}
          ],
          done: false
        )
      )

    setup = gateway_setup(upstream)

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "refresh_token",
               plaintext: "refresh-token-ws-non-auth-do-not-leak"
             })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    assert :ok =
             execute_websocket_response(
               auth,
               websocket_auth_refresh_payload(setup, "non-auth"),
               %{request_id: "ws-terminal-non-auth-no-refresh"},
               fn frame -> send(self(), {:websocket_frame, frame}) end
             )

    assert_received {:websocket_frame, frame}
    assert %{"type" => "response.failed"} = CodexPooler.JSON.decode!(frame)

    assert [first_request] = FakeUpstream.requests(upstream)
    assert first_request.method == "WEBSOCKET"
    assert FakeUpstream.websocket_connection_count(upstream) == 1

    assert [attempt] = Repo.all(from(a in Attempt))
    assert attempt.status == "failed"
    assert attempt.network_error_code == "upstream_terminal_failure"
    assert attempt.transport == "websocket"

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "failed"
    assert request.retry_count == 0
    assert request.last_error_code == "upstream_terminal_failure"
    refute Map.has_key?(request.request_metadata || %{}, "auth_refresh")

    metadata_text = inspect({request.request_metadata, attempt.response_metadata})
    refute metadata_text =~ "refresh-token-ws-non-auth-do-not-leak"
  end

  @tag :feature_websocket_terminal_auth_refresh
  test "websocket terminal auth after partial output does not refresh or retry" do
    upstream =
      start_upstream(
        FakeUpstream.sse_stream(
          [
            {"response.output_text.delta", %{"type" => "response.output_text.delta", "delta" => "partial"}},
            {"response.failed",
             %{
               "type" => "response.failed",
               "response" => %{
                 "id" => "resp_ws_partial_auth_terminal",
                 "error" => %{"code" => "invalid_api_key"},
                 "usage" => %{"input_tokens" => 4, "output_tokens" => 1, "total_tokens" => 5}
               }
             }}
          ],
          done: false
        )
      )

    setup = gateway_setup(upstream)

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "refresh_token",
               plaintext: "refresh-token-ws-partial-do-not-leak"
             })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {:ok, session} = Gateway.start_codex_session(auth, %{accepted_turn_state: "partial-auth"})

    assert :ok =
             execute_websocket_response(
               auth,
               websocket_auth_refresh_payload(setup, "partial-auth"),
               %{request_id: "ws-terminal-auth-after-partial", codex_session: session},
               fn frame -> send(self(), {:websocket_frame, frame}) end
             )

    frames =
      receive_websocket_frames_by_type(["response.output_text.delta", "response.failed"], 1_000)

    assert frames["response.output_text.delta"]["delta"] == "partial"
    assert frames["response.failed"]["response"]["error"]["code"] == "invalid_api_key"

    assert [first_request] = FakeUpstream.requests(upstream)
    assert first_request.method == "WEBSOCKET"
    assert FakeUpstream.websocket_connection_count(upstream) == 1

    assert [attempt] = Repo.all(from(a in Attempt))
    assert attempt.status == "failed"
    assert attempt.network_error_code == "invalid_api_key"

    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "failed"
    assert request.retry_count == 0
    assert request.last_error_code == "invalid_api_key"
    refute Map.has_key?(request.request_metadata || %{}, "auth_refresh")

    assert [turn] = Repo.all(from(t in CodexTurn, where: t.request_id == ^request.id))
    assert turn.first_visible_output_at
    assert turn.status == "failed"
    assert turn.error_code == "invalid_api_key"

    metadata_text = inspect({request.request_metadata, attempt.response_metadata})
    refute metadata_text =~ "refresh-token-ws-partial-do-not-leak"
  end

  # findings#238: the handshake `x-openai-authorization-error` header is
  # promoted to the first attempt's stream_error_code, so it takes the
  # websocket diagnostic code bound: cleartext for an identifier, a
  # fingerprint otherwise, and the `unauthorized` fallback when blank.
  test "websocket handshake 401 keeps an identifier x-openai-authorization-error in cleartext" do
    first_attempt = bounded_handshake_first_attempt!("invalid_api_key", "bounded_clear")

    assert first_attempt.response_metadata["stream_error_code"] == "invalid_api_key"
  end

  test "websocket handshake 401 fingerprints a non-identifier x-openai-authorization-error" do
    hostile_code = "Token expired; sign in again (session " <> String.duplicate("s", 60) <> ")"
    first_attempt = bounded_handshake_first_attempt!(hostile_code, "bounded_hostile")

    assert first_attempt.response_metadata["stream_error_code"] == fingerprint(hostile_code)
    refute inspect(first_attempt.response_metadata) =~ "sign in again"
  end

  test "websocket handshake 401 with a blank x-openai-authorization-error falls back to unauthorized" do
    first_attempt = bounded_handshake_first_attempt!("", "bounded_blank")

    assert first_attempt.response_metadata["stream_error_code"] == "unauthorized"
  end

  defp bounded_handshake_first_attempt!(header_value, marker) do
    initial_access_token = synthetic_access_token("ws-#{marker}-initial-region")
    refreshed_access_token = synthetic_access_token("ws-#{marker}-refreshed-region")

    # Strict: a 401 handshake carrying the header under test, one provider
    # token refresh, then the retried handshake succeeds.
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "GET",
            respond:
              FakeUpstream.websocket_upgrade_error(
                %{"error" => %{"code" => "invalid_api_key"}},
                status: 401,
                headers: [{"x-openai-authorization-error", header_value}]
              )
          ),
          FakeUpstream.expect_request(
            method: "POST",
            path: "/oauth/token",
            respond: FakeUpstream.json_response(%{"access_token" => refreshed_access_token}, 200)
          ),
          strict_native_response_payload(websocket_auth_retry_success_payload(marker), 1)
        ])
      )

    setup = gateway_setup(upstream)

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "access_token",
               plaintext: initial_access_token
             })

    assert {:ok, _secret} =
             Upstreams.store_encrypted_secret(setup.identity, %{
               secret_kind: "refresh_token",
               plaintext: "refresh-token-ws-#{marker}-do-not-leak"
             })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    capture_log(fn ->
      assert :ok =
               execute_websocket_response(
                 auth,
                 websocket_auth_refresh_payload(setup, marker),
                 %{request_id: "ws-auth-#{marker}"},
                 fn frame -> send(self(), {:websocket_frame, frame}) end
               )
    end)

    assert_received {:websocket_frame, frame}
    assert %{"id" => "resp_ws_auth_retry_" <> received_marker} = CodexPooler.JSON.decode!(frame)
    assert received_marker == marker
    assert :ok = FakeUpstream.verify!(upstream)

    assert [first_attempt, second_attempt] =
             Repo.all(from(a in Attempt, order_by: [asc: a.attempt_number]))

    assert first_attempt.status == "retryable_failed"
    assert first_attempt.network_error_code == "upstream_unauthorized"
    assert second_attempt.status == "succeeded"

    refute inspect(first_attempt.response_metadata) =~ "refresh-token-ws-#{marker}-do-not-leak"

    first_attempt
  end

  defp fingerprint(value) do
    "sha256_" <>
      (:crypto.hash(:sha256, value)
       |> Base.encode16(case: :lower)
       |> String.slice(0, 12))
  end

  defp strict_native_response_payload(payload, connection_ordinal) when is_map(payload) do
    strict_native_request(
      connection_ordinal,
      FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(payload)])
    )
  end

  defp websocket_auth_retry_success_payload(marker) do
    %{
      "id" => "resp_ws_auth_retry_#{marker}",
      "object" => "response",
      "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}
    }
  end

  defp websocket_terminal_auth_failure(code) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{
        "type" => "response.failed",
        "response" => %{
          "id" => "resp_ws_terminal_auth_#{code}",
          "error" => %{"code" => code, "param" => "reasoning.effort"},
          "usage" => %{"input_tokens" => 4, "output_tokens" => 0, "total_tokens" => 4}
        }
      })
    ])
  end

  defp strict_oauth_refresh(respond) do
    FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: respond)
  end

  defp expected_route_failures("refresh_in_progress"), do: []
  defp expected_route_failures(_refresh_status), do: [{"upstream_unauthorized", 1}]

  # Another caller's refresh is not the account's fault: no reconciliation, as
  # HTTP's refresh follower does none.
  defp expected_reconciliations("refresh_in_progress"), do: 0
  defp expected_reconciliations(_refresh_status), do: 1

  defp assert_exhausted_auth_health!(setup, refresh_status) do
    assert route_circuit_failures(setup.assignment.id) == expected_route_failures(refresh_status)
    assert length(account_reconciliation_jobs(setup.identity.id)) == expected_reconciliations(refresh_status)
  end
end
