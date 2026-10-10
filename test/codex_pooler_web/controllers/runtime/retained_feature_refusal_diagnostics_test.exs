defmodule CodexPoolerWeb.Runtime.RetainedFeatureRefusalDiagnosticsTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, stop_websocket_owner_session: 1]
  import Phoenix.LiveViewTest, only: [render_component: 2]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Routing.CandidateEligibility.Quota
  alias CodexPooler.MCP.Tools.LogMetadata.RequestLogPresenter
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Quotas.Evidence.CodexParsers
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{AccountAvailabilityStore, CapacityFactsStore, CreditBalanceStore, Evidence, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows.{AccountDenial, EvidenceStore, Routing}
  alias CodexPoolerWeb.Admin.RequestLogsDisplay.Errors
  alias CodexPoolerWeb.Admin.RequestLogsPresentation.Issues

  @at ~U[2026-10-09 03:00:00Z]
  @observed ~U[2026-10-07 00:30:33Z]
  @reset ~U[2026-10-09 04:10:08Z]
  @path "/backend-api/codex/responses"
  @context %{model: "gpt-test-model", upstream_model: "upstream-test-model", serving_mode: :lite, transport: :bridged_websocket}

  test "reported timeline separates retained feature authority from cleared account denial and releases at reset" do
    fake = start_upstream(FakeUpstream.json_response(%{}))
    setup = gateway_setup(fake, quota?: false)
    put_feature!(setup.identity, @observed, @reset, "usage_limit_reached")
    identity = persist_usage!(setup.identity, usage_payload(@at), DateTime.add(@at, -120))
    snapshot = snapshot(identity, @at)
    result = record("reported_before_reset", snapshot)
    refute result.eligible
    assert result.reasons == ["not_fresh", "provider_credit_capacity_unverified", "provider_credit_permission_unavailable"]
    assert result.account_denial == false
    assert_diagnostic(hd(result.exclusions), @observed, @reset)
    assert [%{reason_codes: ["not_fresh"], quota_scope: "feature"}] = result.exclusions

    without_permission = record("same_rows_without_account_permission", %{snapshot | availability: nil})
    refute without_permission.eligible
    assert without_permission.account_denial
    assert "provider_denied" in without_permission.reasons

    before_reset = DateTime.add(@reset, -1)
    identity = persist_usage!(Repo.reload!(identity), usage_payload(@at, before_reset), before_reset)
    still_blocked = record("one_second_before_feature_reset", snapshot(identity, before_reset))
    refute still_blocked.eligible
    assert still_blocked.reasons == ["not_fresh", "provider_credit_capacity_unverified", "provider_credit_permission_unavailable"]
    assert_diagnostic(hd(still_blocked.exclusions), @observed, @reset)

    boundary = @reset
    identity = persist_usage!(Repo.reload!(identity), usage_payload(@at, boundary), boundary)
    at_reset = record("exact_feature_reset_with_fresh_account_poll", snapshot(identity, boundary))
    assert at_reset.eligible
    assert at_reset.feature_rows == 1
    assert at_reset.feature_selected == 0
  end

  test "new same-feature header clears marker through real evidence merge" do
    fake = start_upstream(FakeUpstream.json_response(%{}))
    setup = gateway_setup(fake, quota?: false)
    put_feature!(setup.identity, @observed, @reset, "usage_limit_reached")
    identity = persist_usage!(setup.identity, usage_payload(@at), DateTime.add(@at, -120))
    refute record("before_same_feature_refresh", snapshot(identity, @at)).eligible
    put_feature!(identity, DateTime.add(@at, -30), @reset, nil)
    refreshed = record("after_same_feature_refresh", snapshot(identity, @at))
    assert refreshed.eligible
    assert refreshed.feature_refusals == 0
    assert refreshed.exclusions == []
  end

  test "without a refusal nonexhausted header-only stale evidence is ignored" do
    fake = start_upstream(FakeUpstream.json_response(%{}))
    setup = gateway_setup(fake, quota?: false)
    put_feature!(setup.identity, @observed, @reset, nil)
    identity = persist_usage!(setup.identity, usage_payload(@at), DateTime.add(@at, -120))
    assert record("stale_nonrefusal_control", snapshot(identity, @at)).eligible
  end

  test "real Lite HTTP and native websocket refuse before generation on the retained feature row" do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    reset_at = DateTime.add(now, 4208)
    usage = usage_payload(now)
    fake = start_upstream({:path_json, ProviderCreditsFixtures.usage_routes(usage)})
    setup = gateway_setup(fake, quota?: false)
    scope = model_serving_scope()
    set_model_serving_mode!(scope, setup, "lite")
    put_feature!(setup.identity, DateTime.add(now, -181_767), reset_at, "usage_limit_reached")
    _identity = persist_usage!(setup.identity, usage, DateTime.add(now, -120))
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-openai-internal-codex-responses-lite", "true"}]
    response = Req.post!("http://127.0.0.1:#{port}" <> @path, headers: headers, json: payload(setup), retry: false)
    code = response.body["error"]["code"]
    assert response.status == 503
    assert code == "quota_evidence_unavailable"
    assert generation_count(fake) == 0

    {conn, websocket, ref} = public_websocket_connect!(port, setup, Ecto.UUID.generate())
    on_exit(fn -> Mint.HTTP.close(conn) end)
    frame = payload(setup) |> Map.put("type", "response.create") |> Map.put("client_metadata", %{"ws_request_header_x_openai_internal_codex_responses_lite" => "true"})
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
    {conn, _websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    terminal = CodexPooler.JSON.decode!(text)
    Mint.HTTP.close(conn)
    assert terminal["type"] == "error"
    assert terminal["error"]["code"] == "quota_evidence_unavailable"
    assert generation_count(fake) == 0
    rows = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, select: %{status: r.status, endpoint: r.endpoint, code: r.last_error_code})
    assert length(rows) == 2

    for request <- Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id) do
      assert_operator_projection(scope, request, DateTime.add(now, -181_767), reset_at)
    end
  end

  test "a Lite HTTP-to-websocket candidate persists diagnostics after a proven bridge control" do
    TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    reset_at = DateTime.add(now, 4208)
    usage = usage_payload(now)
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_feature_control", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
    fake = start_upstream(FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)]))
    setup = gateway_setup(fake, quota?: false)
    scope = model_serving_scope()
    set_model_serving_mode!(scope, setup, "lite")
    _identity = persist_usage!(setup.identity, usage, DateTime.add(now, -120))

    on_exit(fn ->
      for id <- Repo.all(from s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id), do: stop_websocket_owner_session(id)
    end)

    request = fn ->
      build_conn()
      |> put_req_header("authorization", setup.authorization)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("x-session-id", Ecto.UUID.generate())
      |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic", "stream" => true})
    end

    assert request.().status == 200
    assert FakeUpstream.physical_counts(fake).websocket_generation == 1
    assert FakeUpstream.physical_counts(fake).http_generation == 0
    assert [%Attempt{response_metadata: %{"upstream_websocket_bridge" => true}}] = Repo.all(Attempt)
    put_feature!(setup.identity, DateTime.add(now, -181_767), reset_at, "usage_limit_reached")
    FakeUpstream.set_mode(fake, {:path_json, ProviderCreditsFixtures.usage_routes(usage)})
    blocked = request.()
    assert blocked.status == 503
    assert json_response(blocked, 503)["error"]["code"] == "quota_evidence_unavailable"
    assert FakeUpstream.physical_counts(fake).websocket_generation == 1
    assert FakeUpstream.physical_counts(fake).http_generation == 0
    [refused] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.status != "succeeded")
    assert_operator_projection(scope, refused, DateTime.add(now, -181_767), reset_at)
  end

  test "unknown markers and refreshable or fresh feature groups do not gain a retained-refusal explanation" do
    fake = start_upstream(FakeUpstream.json_response(%{}))
    setup = gateway_setup(fake, quota?: false)
    put_feature!(setup.identity, @observed, @reset, "usage_limit_reached")
    identity = persist_usage!(setup.identity, usage_payload(@at), DateTime.add(@at, -120))
    baseline = snapshot(identity, @at)
    [feature] = Enum.filter(baseline.raw_windows, &(&1.quota_scope == "feature"))
    account = Enum.reject(baseline.raw_windows, &(&1.quota_scope == "feature"))

    for marker <- [nil, "unsupported_refusal", 12, %{}, String.duplicate("x", 1000)] do
      changed = %{feature | metadata: Map.put(feature.metadata, "rate_limit_error_code", marker)}
      result = record("unknown_marker", %{baseline | raw_windows: account ++ [changed]})
      refute result.eligible
      assert result.reasons == ["not_fresh", "provider_credit_capacity_unverified", "provider_credit_permission_unavailable"]
      refute Map.has_key?(hd(result.exclusions), :retained_refusal_code)
    end

    for rows <- [
          [feature, %{feature | source: "codex_usage_api"}],
          [%{feature | observed_at: DateTime.add(@at, -60), last_sync_at: DateTime.add(@at, -60)}],
          [%{feature | reset_at: nil}],
          [%{feature | quota_scope: "model", model: "gpt-test-model"}]
        ] do
      result = record("outside_diagnostic_scope", %{baseline | raw_windows: account ++ rows})
      refute Enum.any?(result.exclusions, &Map.has_key?(&1, :retained_refusal_code))
    end
  end

  test "credential-invalid account permission does not acquire authority from diagnostics" do
    fake = start_upstream(FakeUpstream.json_response(%{}))
    setup = gateway_setup(fake, quota?: false)
    put_feature!(setup.identity, @observed, @reset, "usage_limit_exceeded")
    identity = persist_usage!(setup.identity, usage_payload(@at), DateTime.add(@at, -120))
    baseline = snapshot(identity, @at)
    changed = %{baseline | availability: %{baseline.availability | credential_epoch: baseline.credential_epoch + 1}}
    result = record("wrong_epoch_permission", changed)
    refute result.eligible
    assert result.account_denial
    assert result.reasons == ["provider_denied", "provider_credit_capacity_unverified", "provider_credit_permission_unavailable"]
    stale_permission = %{baseline | availability: %{baseline.availability | observed_at: DateTime.add(@at, -901)}}
    stale_result = record("stale_permission", stale_permission)
    refute stale_result.eligible
    assert stale_result.account_denial
    assert stale_result.reasons == ["provider_denied", "provider_credit_capacity_unverified", "provider_credit_permission_unavailable"]
    assert_diagnostic(hd(result.exclusions), @observed, @reset, "usage_limit_exceeded")
  end

  test "candidate exclusion allowlist retains only the bounded diagnostic fields" do
    fake = start_upstream(FakeUpstream.json_response(%{}))
    setup = gateway_setup(fake, quota?: false)
    put_feature!(setup.identity, @observed, @reset, "usage_limit_reached")
    identity = persist_usage!(setup.identity, usage_payload(@at), DateTime.add(@at, -120))
    result = record("candidate_projection", snapshot(identity, @at))
    exclusion = hd(result.exclusions) |> Map.put(:unrelated_note, "unrelated diagnostic material")
    projected = Quota.sanitize_quota_exclusion(exclusion)
    assert projected["retained_refusal_code"] == "usage_limit_reached"
    assert projected["retained_refusal_observed_at"] == exclusion.retained_refusal_observed_at
    assert projected["retained_refusal_reset_at"] == exclusion.retained_refusal_reset_at
    assert projected["reason_codes"] == ["not_fresh"]
    refute Map.has_key?(projected, "unrelated_note")
    refute Map.has_key?(projected, "metadata")
  end

  test "operator formatting rejects malformed retained-refusal fields instead of echoing them" do
    valid = %{code: "quota_window_unusable", retained_refusal_code: "usage_limit_reached", retained_refusal_observed_at: DateTime.to_iso8601(@observed), retained_refusal_reset_at: DateTime.to_iso8601(@reset)}

    for {key, value} <- [
          {:retained_refusal_code, "untrusted diagnostic text"},
          {:retained_refusal_observed_at, "untrusted diagnostic text"},
          {:retained_refusal_reset_at, "untrusted diagnostic text"},
          {:retained_refusal_observed_at, DateTime.to_iso8601(DateTime.add(@reset, 1))},
          {:retained_refusal_reset_at, String.duplicate("x", 1000)},
          {:retained_refusal_observed_at, %{unexpected: true}}
        ] do
      assert Errors.format_errors(%{errors: [Map.put(valid, key, value)]}, datetime_preferences()) == ["quota_window_unusable"]
    end
  end

  defp assert_diagnostic(reason, observed, reset, code \\ "usage_limit_reached") do
    assert reason.retained_refusal_code == code
    assert {:ok, actual_observed, 0} = DateTime.from_iso8601(reason.retained_refusal_observed_at)
    assert {:ok, actual_reset, 0} = DateTime.from_iso8601(reason.retained_refusal_reset_at)
    assert DateTime.compare(actual_observed, observed) == :eq
    assert DateTime.compare(actual_reset, reset) == :eq
    assert reason.message == "a provider usage-limit refusal is retained until this feature window resets"
    assert reason.reason_codes == ["not_fresh"]
    refute Map.has_key?(reason, :metadata)
    refute inspect(reason) =~ "unrelated diagnostic material"
  end

  defp assert_operator_projection(scope, request, observed, reset) do
    attempts = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    exclusions = (request.request_metadata["candidate_exclusions"] || []) ++ Enum.flat_map(attempts, &(&1.response_metadata["candidate_exclusions"] || []))
    reasons = Enum.flat_map(exclusions, & &1["reasons"])
    [reason] = Enum.filter(reasons, &(&1["quota_scope"] == "feature"))
    assert reason["retained_refusal_code"] == "usage_limit_reached"
    assert {:ok, actual_observed, 0} = DateTime.from_iso8601(reason["retained_refusal_observed_at"])
    assert {:ok, actual_reset, 0} = DateTime.from_iso8601(reason["retained_refusal_reset_at"])
    assert DateTime.compare(actual_observed, observed) == :eq
    assert DateTime.compare(actual_reset, reset) == :eq
    assert reason["reason_codes"] == ["not_fresh"]
    log = Accounting.get_request_log_for_scope(scope, request.id, surface: :admin)
    assert log
    refute inspect(log.errors) =~ "unrelated diagnostic material"
    summaries = Enum.filter(log.errors, &(&1[:retained_refusal_code] == "usage_limit_reached"))
    assert length(summaries) == 1, inspect(log.errors)
    [summary] = summaries
    assert summary[:retained_refusal_observed_at] == reason["retained_refusal_observed_at"]
    assert summary[:retained_refusal_reset_at] == reason["retained_refusal_reset_at"]
    html = render_component(&Issues.request_log_issues_cell/1, request_log: log, datetime_preferences: datetime_preferences(), prefix: "retained-refusal")
    assert html =~ "feature refusal retained"
    assert html =~ "usage_limit_reached"
    assert html =~ "observed"
    assert html =~ "resets"
    presented = RequestLogPresenter.detail_item(log)
    [projected] = Enum.filter(presented["errors"], &(&1["retained_refusal_code"] == "usage_limit_reached"))
    assert projected["retained_refusal_observed_at"] == reason["retained_refusal_observed_at"]
    assert projected["retained_refusal_reset_at"] == reason["retained_refusal_reset_at"]
    refute Map.has_key?(projected, "message")
    refute inspect(presented) =~ "unrelated diagnostic material"
  end

  defp datetime_preferences, do: %{timezone: "Etc/UTC", datetime_format: "iso8601"}

  defp persist_usage!(identity, payload, at) do
    assert {:ok, parsed} = CodexParsers.parse_codex_usage_result(payload, at)
    assert length(parsed.windows) == 2

    for window <- parsed.windows do
      assert {:ok, _} = EvidenceStore.record_evidence(identity, Map.from_struct(window), at, at)
    end

    identity = Repo.reload!(identity)
    epoch = CredentialFencing.credential_epoch(identity)
    metadata = identity.metadata |> AccountAvailabilityStore.transition(parsed.account_availability, at, epoch) |> CreditBalanceStore.transition(payload, at, epoch) |> CapacityFactsStore.transition(parsed.capacity_facts, epoch)
    Repo.update!(Ecto.Changeset.change(identity, metadata: metadata))
  end

  defp usage_payload(origin, observed_at \\ nil) do
    observed_at = observed_at || DateTime.add(origin, -120)

    %{
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => %{"used_percent" => 0, "limit_window_seconds" => 18_000, "reset_after_seconds" => DateTime.diff(DateTime.add(origin, 17_880), observed_at), "reset_at" => DateTime.to_unix(DateTime.add(origin, 17_880))},
        "secondary_window" => %{"used_percent" => 0, "limit_window_seconds" => 604_800, "reset_after_seconds" => DateTime.diff(DateTime.add(origin, 604_680), observed_at), "reset_at" => DateTime.to_unix(DateTime.add(origin, 604_680))}
      },
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => nil}
    }
  end

  defp put_feature!(identity, at, reset_at, code) do
    headers = [{"x-base-model-inference-secondary-used-percent", "9"}, {"x-base-model-inference-secondary-window-minutes", "10080"}, {"x-base-model-inference-secondary-reset-at", DateTime.to_iso8601(reset_at)}]
    [attrs] = Evidence.codex_header_windows(headers, at, "gpt-test-model", code)
    assert attrs.quota_scope == "feature"
    assert attrs.quota_key == "base_model_inference"
    attrs = Map.update!(attrs, :metadata, &Map.put(&1, "unrelated_note", "unrelated diagnostic material"))
    assert {:ok, _} = EvidenceStore.record_evidence(identity, attrs, at, at)
  end

  defp snapshot(identity, at), do: RoutingQuotaSnapshot.load_by_identity_ids([identity.id], at)[identity.id]

  defp record(label, snapshot) do
    assert Enum.count(snapshot.raw_windows, &(&1.quota_scope == "account")) == 2
    decision = Upstreams.provider_credits_decision(snapshot, @context)
    features = Enum.filter(snapshot.raw_windows, &(&1.quota_scope == "feature"))
    selected = Routing.selection_data_from_windows(snapshot.raw_windows, at: snapshot.as_of)

    result = %{
      scenario: label,
      at: DateTime.to_iso8601(snapshot.as_of),
      eligible: decision.eligible?,
      basis: decision.capacity_basis,
      reasons: decision.reason_codes,
      account_denial: not is_nil(AccountDenial.active(snapshot)),
      feature_rows: length(features),
      feature_refusals: Enum.count(features, &Map.has_key?(&1.metadata, "rate_limit_error_code")),
      feature_selected: Enum.count(selected.routing_windows, &(&1.quota_scope == "feature")),
      exclusions: decision.eligibility.exclusions
    }

    result
  end

  defp generation_count(fake), do: Enum.count(FakeUpstream.requests(fake), &(&1.path == @path))
  defp payload(setup), do: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic"), "stream" => true, "store" => false}
end
