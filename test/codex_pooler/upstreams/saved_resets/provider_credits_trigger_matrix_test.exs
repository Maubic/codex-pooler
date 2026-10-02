defmodule CodexPooler.Upstreams.SavedResets.ProviderCreditsTriggerMatrixTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, gateway_upstream: 4, response_affinity_file_fixture: 4]
  import ExUnit.CaptureLog

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.RouteFiltering
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Gateway.Transports.ProviderCreditsAdmission
  alias CodexPooler.Gateway.Transports.UpstreamDispatch
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{CapacityAssessment, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation
  alias CodexPooler.Upstreams.SavedResetRedemption
  alias CodexPooler.Upstreams.SavedResets.{Convergence, ProbeLease, RedemptionLifecycle}
  alias CodexPooler.Upstreams.Schemas.EncryptedSecret

  @consume "/api/codex/rate-limit-reset-credits/consume"
  @endpoint "/backend-api/codex/responses"

  for policy <- [true, false], credits <- [:full, :none, :unknown] do
    test "gateway exhausted recovery follows usable credits with #{credits} credits and policy #{policy}" do
      %{fake: fake, identity: identity, input: input} = arrangement(:exhausted, unquote(policy), unquote(credits))
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")
      restored = ProviderCreditsFixtures.usage_payload(:included, credits: unquote(credits))
      restored = if unquote(credits) == :unknown, do: Map.delete(restored, "credits"), else: restored
      FakeUpstream.set_mode(fake, routes(restored))

      if unquote(policy) and unquote(credits) == :full do
        assert {:ok, [{_assignment, chosen}], options, _state} = filter(input)
        assert chosen.id == identity.id
        assert options.routing.quota_decision["capacity_basis"] == "provider_credits"
        assert FakeUpstream.physical_counts(fake).consume == 0
        refute Repo.reload!(identity).metadata["saved_reset_redemption"]
      else
        capture_log(fn -> filter(input) end)
        first_phase = Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"]
        assert first_phase == if(unquote(credits) == :full, do: "confirmed_by_quota", else: "consumed_pending_probe")
        assert {:ok, refreshed} = PoolReconciliation.refresh_quota_from_usage(Repo.reload!(identity), hd(input.candidates) |> elem(0))
        Convergence.converge(refreshed)
        assert {:ok, [{_assignment, chosen}], options, _state} = filter(FilterInput.put_candidates(input, [{hd(input.candidates) |> elem(0), Repo.reload!(identity)}]))
        assert chosen.id == identity.id
        assert options.routing.quota_decision["capacity_basis"] == "recovered_included"
        assert FakeUpstream.physical_counts(fake).consume == 1
        assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
      end

      assert FakeUpstream.physical_counts(fake).http_generation == 0
    end

    @tag credits_negative: true
    test "operator credit policy alone creates no trigger with #{credits} credits and policy #{policy}" do
      %{fake: fake, input: input} = arrangement(:included, unquote(policy), unquote(credits))
      assert {:ok, _candidates, _options, _state} = filter(input)
      assert FakeUpstream.physical_counts(fake).consume == 0
      assert FakeUpstream.physical_counts(fake).http_generation == 0
    end
  end

  for trigger <- [:threshold, :last_call, :exhausted], policy <- [true, false], fence <- [:none, :disabled, :reserve, :expiration, :cooldown], credits <- [:full, :none, :unknown] do
    @tag credits_negative: true
    test "R9 scheduled #{trigger} preserves #{fence} fence with #{credits} credits and policy #{policy}" do
      %{fake: fake, identity: identity, assignment: assignment, now: now} = arrangement(unquote(trigger), unquote(policy), unquote(credits))
      identity = apply_fence(identity, unquote(fence))
      if unquote(trigger) == :exhausted, do: SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")

      capture_log(fn ->
        assert {:ok, result} = SavedResetRedemption.redeem_scheduled_expiry(assignment, identity.id, started_at: now)
        assert result.applied? == (unquote(fence) == :none)
      end)

      assert FakeUpstream.physical_counts(fake).consume == if(unquote(fence) == :none, do: 1, else: 0)
    end
  end

  for policy <- [true, false], credits <- [:full, :none, :unknown], fence <- [:none, :compatible_sibling, :incompatible_sibling, :durable_pin, :target_circuit, :sibling_circuit, :natural_reset, :freshness, :corroboration, :reserve, :cooldown] do
    @tag credits_negative: true
    test "R9 request-driven threshold preserves #{fence} with #{credits} credits policy #{policy}" do
      fixture = gateway_threshold_arrangement(unquote(policy), unquote(credits), unquote(fence))
      %{fake: fake, setup: setup} = fixture
      original = Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
      before = FakeUpstream.physical_counts(fake).consume
      result = capture_log_result(fn -> runtime_execute(setup, fixture.payload) end)
      assert_threshold_result!(result, fixture, unquote(fence))
      applied? = unquote(fence) in [:none, :incompatible_sibling, :durable_pin]
      assert FakeUpstream.physical_counts(fake).consume - before == if(applied?, do: 1, else: 0)
      if fixture.sibling, do: assert(FakeUpstream.physical_counts(fixture.sibling.fake).consume == 0)

      if applied? do
        redemption = Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
        assert redemption["trigger_kind"] == "gateway_auto"
        assert redemption["trigger_detail"] == "threshold"
        assert redemption["phase"] == "confirmed_by_quota"
        assert FakeUpstream.physical_counts(fake).http_generation == 1
        assert [consume] = Enum.filter(FakeUpstream.physical_receipts(fake), &(&1.kind == :consume))
        assert [generation] = Enum.filter(FakeUpstream.physical_receipts(fake), &(&1.kind == :generation))
        assert confirmation = Enum.find(FakeUpstream.physical_receipts(fake), &(&1.kind == :usage and &1.ordinal > consume.ordinal))
        assert consume.ordinal < confirmation.ordinal and confirmation.ordinal < generation.ordinal
      else
        assert Repo.reload!(setup.identity).metadata["saved_reset_redemption"] == original
      end

      assert Repo.reload!(setup.identity).allow_provider_credits == unquote(policy)
    end
  end

  for policy <- [true, false], credits <- [:full, :none, :unknown], phase <- [:failed, :reblocked, :expired] do
    @tag credits_negative: true
    test "R8 terminal #{phase} recovery retains its latch through selection and final admission with #{credits} credits policy #{policy}" do
      fixture = terminal_recovery_arrangement(unquote(policy), unquote(credits), unquote(phase))
      %{fake: fake, setup: setup} = fixture
      redemption = Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
      latch = RedemptionLifecycle.gateway_auto_latch(redemption, DateTime.utc_now())
      serves? = unquote(policy) and unquote(credits) == :full
      assert_terminal_credit_result!(setup, fixture.payload, fake, serves?)
      assert Repo.reload!(setup.identity).metadata["saved_reset_redemption"] == redemption
      assert RedemptionLifecycle.gateway_auto_latch(redemption, DateTime.utc_now()) == latch
      assert FakeUpstream.physical_counts(fake).consume == terminal_consume_count(unquote(phase))
      assert FakeUpstream.physical_counts(fake).http_generation == if(serves?, do: 3, else: 0)
      assert Repo.aggregate(Attempt, :count) == if(serves?, do: 2, else: 0)
    end
  end

  for policy <- [true, false], credits <- [:full, :unknown] do
    @tag credits_negative: true
    test "R4 R5 pending reset with #{credits} credits never claims a confirmation probe with policy #{policy}" do
      %{fake: fake, identity: identity, input: input, now: now} = arrangement(:exhausted, unquote(policy), unquote(credits))
      identity = pending_identity(identity, now)
      input = FilterInput.put_candidates(input, [{hd(input.candidates) |> elem(0), identity}])
      capture_log(fn -> assert {:error, _error} = filter(input) end)
      assert FakeUpstream.physical_counts(fake).consume == 0
      assert FakeUpstream.physical_counts(fake).http_generation == 0
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([identity.id], now)[identity.id]
      refute CapacityAssessment.guarded_probe_permitted?(snapshot)
      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "consumed_pending_probe"
      assert RedemptionLifecycle.probe_holder(Repo.reload!(identity).metadata["saved_reset_redemption"]) == nil
    end
  end

  for policy <- [true, false] do
    test "R6 explicitly unavailable credits retain exactly one guarded lease with policy #{policy}" do
      %{identity: identity, assignment: assignment, input: input, now: now} = arrangement(:exhausted, unquote(policy), :none)
      identity = pending_identity(identity, now)
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([identity.id], now)[identity.id]
      assert CapacityAssessment.guarded_probe_permitted?(snapshot)
      first = ResetProbe.bind(input.request_options.routing.reset_probe, assignment.id, identity.id, input.model.exposed_model_id, input.route_class) |> elem(1)
      second = ResetProbe.new() |> ResetProbe.bind(assignment.id, identity.id, input.model.exposed_model_id, input.route_class) |> elem(1)
      redemption = identity.metadata["saved_reset_redemption"]
      assert {:ok, :claimed} = ProbeLease.claim(identity, redemption["generation"], redemption["attempt_id"], first, now)
      assert {:error, :unavailable} = ProbeLease.claim(identity, redemption["generation"], redemption["attempt_id"], second, now)
    end

    test "R7 definitive no-consume outcome cannot repeat consume with policy #{policy}" do
      %{fake: fake, identity: identity, assignment: assignment, input: input} = arrangement(:exhausted, unquote(policy), :none)
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")
      FakeUpstream.set_mode(fake, {:path_json, Map.put(ProviderCreditsFixtures.usage_routes(ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :none)), @consume, {200, %{"code" => "no_credit"}})})

      capture_log(fn ->
        assert {:error, error} = filter(input)
        assert error.non_credit_recovery_outcome == "not_applied"
      end)

      capture_log(fn -> assert {:error, _error} = filter(FilterInput.put_candidates(input, [{assignment, Repo.reload!(identity)}])) end)

      assert FakeUpstream.physical_counts(fake).consume == 1
      assert FakeUpstream.physical_counts(fake).http_generation == 0
      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["result"]["applied"] == false
    end

    test "R8 credit availability does not clear expired recovery latch with policy #{policy}" do
      %{fake: fake, identity: identity, input: input, now: now} = arrangement(:exhausted, unquote(policy), :full)
      identity = pending_identity(identity, DateTime.add(now, -3600, :second))
      identity = identity |> Ecto.Changeset.change(metadata: put_in(identity.metadata, ["saved_reset_redemption", "phase"], "expired")) |> Repo.update!()
      input = FilterInput.put_candidates(input, [{hd(input.candidates) |> elem(0), identity}])
      redemption = identity.metadata["saved_reset_redemption"]

      if unquote(policy) do
        assert {:ok, _candidates, options, _state} = filter(input)
        assert options.routing.quota_decision["capacity_basis"] == "provider_credits"
      else
        assert {:error, _error} = filter(input)
      end

      assert FakeUpstream.physical_counts(fake).consume == 0
      assert Repo.reload!(identity).metadata["saved_reset_redemption"] == redemption
    end

    test "R4 credit-capable rounded full permission cannot confirm a reset with policy #{policy}" do
      %{identity: identity, now: now} = arrangement(:exhausted, unquote(policy), :full)
      identity = pending_identity(identity, DateTime.add(now, -1, :second))
      payload = ProviderCreditsFixtures.usage_payload(:included, now: now, credits: :full) |> put_in(["rate_limit", "secondary_window", "used_percent"], 100)
      ProviderCreditsFixtures.persist_usage!(identity, payload, now)
      assert {:ok, :unchanged} = Convergence.converge(identity, now)
      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "consumed_pending_probe"
    end
  end

  for policy <- [true, false], restored_shape <- [:included, :windowless_included] do
    test "C24 matching reset descriptor distinguishes #{restored_shape} restoration with policy #{policy}" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      %{identity: identity} = arrangement(:exhausted, unquote(policy), :none, now: DateTime.add(now, -2, :second))
      identity = pending_identity(identity, DateTime.add(now, -1, :second))
      identity = identity |> Ecto.Changeset.change(metadata: put_in(identity.metadata, ["saved_reset_redemption", "included_window_descriptors"], [%{"window_kind" => "secondary", "window_minutes" => 10_080}])) |> Repo.update!()
      payload = ProviderCreditsFixtures.usage_payload(unquote(restored_shape), now: now, credits: :none)
      ProviderCreditsFixtures.persist_usage!(identity, payload, now)
      proof_at = DateTime.add(now, 1, :microsecond)
      ProviderCreditsFixtures.persist_usage!(Repo.reload!(identity), ProviderCreditsFixtures.usage_payload(unquote(restored_shape), now: proof_at, credits: :none), proof_at)

      if unquote(restored_shape) == :included do
        assert {:ok, :confirmed_by_quota} = Convergence.converge(identity, proof_at)
        assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
      else
        assert {:ok, :unchanged} = Convergence.converge(identity, proof_at)
        assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "consumed_pending_probe"
      end
    end
  end

  defp gateway_threshold_arrangement(policy, credits, fence) do
    %{fake: fake, setup: setup, payload: payload} = runtime_arrangement(:included, policy, credits, reset_after: if(fence == :natural_reset, do: 600, else: 7_200))
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    initial = ProviderCreditsFixtures.usage_payload(:included, now: now, credits: credits, reset_after: if(fence == :natural_reset, do: 600, else: 7_200)) |> put_in(["rate_limit", "secondary_window", "used_percent"], 97)
    identity = ProviderCreditsFixtures.persist_usage!(Repo.reload!(setup.identity), initial, now)
    identity = Repo.update!(Ecto.Changeset.change(identity, saved_reset_auto_redeem_trigger_mode: "threshold", saved_reset_auto_redeem_quota_threshold_percent: 95))
    setup = %{setup | identity: identity}
    observations = if fence == :corroboration, do: 1, else: 2
    proof_at = if fence == :freshness, do: DateTime.add(now, -901, :second), else: now
    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, observations: observations, observed_at: proof_at, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")
    setup = %{setup | identity: Repo.reload!(identity)}
    setup = apply_runtime_fence(setup, fence)
    {setup, sibling, payload} = threshold_sibling(setup, payload, policy, fence)
    restored = if fence == :natural_reset, do: initial, else: ProviderCreditsFixtures.usage_payload(:included, credits: credits, reset_after: 14_400) |> put_in(["rate_limit", "secondary_window", "used_percent"], 98)
    FakeUpstream.set_mode(fake, runtime_routes(restored))
    %{fake: fake, setup: setup, sibling: sibling, payload: payload}
  end

  defp runtime_arrangement(state, policy, credits, opts \\ []) do
    payload = ProviderCreditsFixtures.usage_payload(state, Keyword.put(opts, :credits, credits))
    {:ok, fake} = FakeUpstream.start_link(runtime_routes(payload))
    on_exit(fn -> FakeUpstream.stop(fake) end)
    setup = gateway_setup(fake, quota?: false, exposed_model_id: "synthetic-gateway-reset-#{System.unique_integer([:positive])}")
    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(setup.identity, setup.assignment)
    now = DateTime.utc_now()
    bank = %{"status" => "reported", "available_count" => 2, "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => DateTime.to_iso8601(now), "usage_path" => "/api/codex/usage", "reason" => nil}
    identity = Repo.update!(Ecto.Changeset.change(identity, metadata: Map.put(identity.metadata, "saved_resets", bank), allow_provider_credits: policy, saved_reset_auto_redeem_enabled: true, saved_reset_auto_redeem_keep_credits: 0, saved_reset_auto_redeem_min_blocked_minutes: 60))
    %{fake: fake, setup: %{setup | identity: identity}, payload: %{"model" => setup.model.exposed_model_id, "input" => []}}
  end

  defp apply_runtime_fence(setup, :reserve), do: %{setup | identity: apply_fence(setup.identity, :reserve)}
  defp apply_runtime_fence(setup, :cooldown), do: %{setup | identity: apply_fence(setup.identity, :cooldown)}

  defp apply_runtime_fence(setup, :target_circuit) do
    open_runtime_circuit!(setup, setup.assignment)
    setup
  end

  defp apply_runtime_fence(setup, _fence), do: setup

  defp threshold_sibling(setup, payload, policy, fence) when fence in [:compatible_sibling, :incompatible_sibling, :durable_pin, :sibling_circuit] do
    {:ok, fake} = FakeUpstream.start_link(runtime_routes(ProviderCreditsFixtures.usage_payload(:included, credits: :none)))
    on_exit(fn -> FakeUpstream.stop(fake) end)
    sibling = gateway_upstream(setup.pool, fake, "synthetic-threshold-sibling", [])
    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(sibling.identity, sibling.assignment)
    identity = Repo.update!(Ecto.Changeset.change(identity, allow_provider_credits: policy))
    source = setup.model.metadata["source_assignment_models"][setup.assignment.id]
    sibling_source = if fence == :incompatible_sibling, do: Map.put(source, "capabilities", %{"responses" => false}), else: source
    metadata = setup.model.metadata |> Map.put("source_assignment_ids", [setup.assignment.id, sibling.assignment.id]) |> put_in(["source_assignment_models", sibling.assignment.id], sibling_source)
    setup = %{setup | model: Repo.update!(Ecto.Changeset.change(setup.model, metadata: metadata))}
    if fence == :sibling_circuit, do: open_runtime_circuit!(setup, sibling.assignment)
    payload = if fence == :durable_pin, do: pinned_file_payload(setup), else: payload
    {setup, Map.merge(sibling, %{fake: fake, identity: identity}), payload}
  end

  defp threshold_sibling(setup, payload, _policy, _fence), do: {setup, nil, payload}

  defp pinned_file_payload(setup) do
    file = response_affinity_file_fixture(setup, setup.assignment, setup.identity, file_id: "file_threshold_#{System.unique_integer([:positive])}", status: "uploaded", finalize_status: "succeeded")
    %{"model" => setup.model.exposed_model_id, "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_file", "file_id" => file.file_id}]}]}
  end

  defp open_runtime_circuit!(setup, assignment) do
    now = DateTime.utc_now()
    %RoutingCircuitState{} |> RoutingCircuitState.changeset(%{pool_id: setup.pool.id, pool_upstream_assignment_id: assignment.id, upstream_identity_id: assignment.upstream_identity_id, model_identifier: setup.model.exposed_model_id, route_class: "proxy_http", status: "open", reason_code: "synthetic_threshold_fence", failure_count: 3, success_count: 0, opened_at: now, next_probe_at: DateTime.add(now, 60, :second), metadata: %{}, created_at: now, updated_at: now}) |> Repo.insert!()
  end

  defp assert_threshold_result!({:error, _}, %{fake: fake}, :target_circuit), do: assert(FakeUpstream.physical_counts(fake).http_generation == 0)

  defp assert_threshold_result!({:ok, %{status: 200}}, fixture, fence) do
    [attempt] = Repo.all(Attempt)

    if fence == :compatible_sibling do
      assert attempt.upstream_identity_id in [fixture.setup.identity.id, fixture.sibling.identity.id]
      assert FakeUpstream.physical_counts(fixture.fake).http_generation + FakeUpstream.physical_counts(fixture.sibling.fake).http_generation == 1
    else
      assert attempt.upstream_identity_id == fixture.setup.identity.id
      assert FakeUpstream.physical_counts(fixture.fake).http_generation == 1
      if fixture.sibling, do: assert(FakeUpstream.physical_counts(fixture.sibling.fake).http_generation == 0)
    end
  end

  defp terminal_recovery_arrangement(policy, credits, phase) do
    %{fake: fake, setup: setup} = fixture = runtime_arrangement(:weekly_credit_only, policy, credits)
    establish_terminal_recovery!(fixture, phase)
    setup = terminal_transition!(setup, phase, credits)
    payload = ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: credits)
    identity = ProviderCreditsFixtures.persist_usage!(Repo.reload!(setup.identity), payload, DateTime.utc_now())
    identity = if phase == :failed, do: Repo.update!(Ecto.Changeset.change(identity, metadata: put_in(identity.metadata, ["saved_resets", "available_count"], 0))), else: identity
    setup = %{setup | identity: identity}
    FakeUpstream.set_mode(fake, runtime_routes(payload))
    %{fixture | setup: setup}
  end

  defp establish_terminal_recovery!(%{setup: setup}, :failed) do
    secret = Repo.one!(from secret in EncryptedSecret, where: secret.upstream_identity_id == ^setup.identity.id and secret.secret_kind == "access_token" and secret.status == "active")
    Repo.update!(Ecto.Changeset.change(secret, status: "revoked"))
    assert {:ok, %{status: :failed, applied?: false, code: "missing_access_token"}} = SavedResetRedemption.redeem(setup.assignment)
    Repo.update!(Ecto.Changeset.change(Repo.reload!(secret), status: "active"))
  end

  defp establish_terminal_recovery!(%{fake: fake, setup: setup}, phase) when phase in [:reblocked, :expired] do
    omitted = %{"plan_type" => "synthetic", "rate_limit_reset_credits" => %{"available_count" => 0}}
    FakeUpstream.set_mode(fake, {:path_json, Map.put(ProviderCreditsFixtures.usage_routes(omitted), @consume, {200, %{"code" => "reset"}})})
    assert {:ok, %{applied?: true}} = SavedResetRedemption.redeem(setup.assignment)
  end

  defp terminal_consume_count(:failed), do: 0
  defp terminal_consume_count(_phase), do: 1

  defp terminal_transition!(setup, :failed, _credits) do
    identity = Repo.reload!(setup.identity)
    assert identity.metadata["saved_reset_redemption"]["status"] == "failed"
    assert identity.metadata["saved_reset_redemption"]["result"]["applied"] == false
    %{setup | identity: identity}
  end

  defp terminal_transition!(setup, phase, credits) do
    identity = Repo.reload!(setup.identity)
    redemption = identity.metadata["saved_reset_redemption"]
    assert redemption["phase"] == "consumed_pending_probe"
    {:ok, consumed_at, 0} = DateTime.from_iso8601(redemption["consumed_at"])
    decision_at = if phase == :expired, do: RedemptionLifecycle.deadline_at(consumed_at), else: DateTime.utc_now()

    if phase == :reblocked do
      payload = ProviderCreditsFixtures.usage_payload(:weekly_credit_only, now: decision_at, credits: credits)
      ProviderCreditsFixtures.persist_usage!(identity, payload, decision_at)
    end

    assert {:ok, target} = Convergence.converge(identity, decision_at)
    assert target == phase
    identity = Repo.reload!(identity)
    %{setup | identity: identity}
  end

  defp runtime_execute(setup, payload) do
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    Service.execute(auth, @endpoint, payload, RequestOptions.build(%{}, @endpoint, payload))
  end

  defp assert_terminal_credit_result!(setup, payload, fake, true) do
    for _ <- 1..2 do
      assert {:ok, %{status: 200}} = capture_log_result(fn -> runtime_execute(setup, payload) end)
    end

    assert {:ok, _response} = final_dispatch(setup, fake)
    assert Enum.all?(Repo.all(Attempt), &(&1.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"))
  end

  defp assert_terminal_credit_result!(setup, payload, fake, false) do
    for _ <- 1..2 do
      assert {:error, _} = capture_log_result(fn -> runtime_execute(setup, payload) end)
    end

    assert {:error, %{reason: :provider_credits_policy_denied, started: false}} = final_dispatch(setup, fake)
  end

  defp capture_log_result(fun) do
    {result, _log} = with_log(fun)
    result
  end

  defp runtime_routes(payload), do: {:path_json, Map.merge(ProviderCreditsFixtures.usage_routes(Map.put(payload, "rate_limit_reset_credits", %{"available_count" => 1})), %{@consume => {200, %{"code" => "reset"}}, @endpoint => {200, %{"id" => "resp_synthetic_gateway_reset", "object" => "response", "status" => "completed", "output" => []}}})}

  defp final_dispatch(setup, fake) do
    options = RequestOptions.build(%{transport: "http_json"}, @endpoint, %{})
    context = %ProviderCreditsAdmission.Context{version: 1, pool_id: setup.pool.id, pool_upstream_assignment_id: setup.assignment.id, upstream_identity_id: setup.identity.id, credential_epoch: CredentialFencing.credential_epoch(setup.identity), model: setup.model.exposed_model_id, upstream_model: setup.model.upstream_model_id, serving_mode: :full, transport: :http_json, route_class: options.transport.route_class, request_id: nil, attempt_id: nil, reset_probe: nil, redemption_generation: nil, redemption_attempt_id: nil}
    UpstreamDispatch.http_request(%UpstreamDispatch.Request{url: FakeUpstream.url(fake) <> @endpoint, token: "synthetic", upstream_payload: CodexPooler.JSON.encode!(%{"model" => setup.model.upstream_model_id}), original_payload: %{}, identity: setup.identity, provider_credits_context: context, request_options: options, routing_hint_authorized?: false})
  end

  defp arrangement(trigger, policy, credits, opts \\ []) do
    now = Keyword.get_lazy(opts, :now, fn -> DateTime.utc_now() |> DateTime.truncate(:microsecond) end)
    included? = trigger in [:included, :threshold, :last_call]
    payload = ProviderCreditsFixtures.usage_payload(if(included?, do: :included, else: :weekly_credit_only), now: now, credits: credits)
    payload = if trigger == :threshold, do: put_in(payload, ["rate_limit", "secondary_window", "used_percent"], 97), else: payload
    payload = if credits == :unknown, do: Map.delete(payload, "credits"), else: payload
    {:ok, fake} = FakeUpstream.start_link(routes(ProviderCreditsFixtures.usage_payload(:included, now: now, credits: :none)))
    on_exit(fn -> FakeUpstream.stop(fake) end)
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture(pool, %{metadata: %{"base_url" => FakeUpstream.url(fake), "usage_base_url" => FakeUpstream.url(fake)}})
    identity = ProviderCreditsFixtures.persist_usage!(identity, payload, now)
    expires_at = DateTime.add(now, if(trigger == :last_call, do: 3600, else: 14_400), :second) |> DateTime.to_iso8601()
    observed_at = DateTime.to_iso8601(now)
    bank = %{"status" => "reported", "available_count" => 2, "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => observed_at, "usage_path" => "/api/codex/usage", "available_expires_at" => [expires_at], "available_expirations" => [%{"expires_at" => expires_at, "first_seen_at" => observed_at}], "next_expires_at" => expires_at, "expires_observed_at" => observed_at, "expires_refresh_attempted_at" => observed_at, "reason" => nil}
    identity = identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_resets", bank), allow_provider_credits: policy, saved_reset_auto_redeem_enabled: true, saved_reset_auto_redeem_keep_credits: 0, saved_reset_auto_redeem_min_blocked_minutes: 60, saved_reset_auto_redeem_trigger_mode: if(trigger == :threshold, do: "threshold", else: "blocked")) |> Repo.update!()
    model = model_fixture(pool, %{exposed_model_id: "synthetic-trigger-#{System.unique_integer([:positive])}", metadata: %{"source_assignment_ids" => [assignment.id]}})
    body = %{"model" => model.exposed_model_id, "input" => []}
    options = RequestOptions.build(%{}, @endpoint, body) |> RequestOptions.put_routing(reset_probe: ResetProbe.new())
    input = FilterInput.new(%{auth: %{pool: pool, api_key: api_key}, model: model, endpoint: @endpoint, payload: body, request_options: options, candidates: [{assignment, identity}]})
    %{fake: fake, identity: identity, assignment: assignment, input: input, now: now}
  end

  defp filter(input) do
    state = RouteState.new(%{visible_model: input.model, candidates: input.candidates}) |> RouteState.preload_routing_snapshots(input.auth, input.model, input.request_options)
    RouteFiltering.filter_candidates_with_route_state(input, state)
  end

  defp routes(payload), do: {:path_json, Map.merge(ProviderCreditsFixtures.usage_routes(Map.put(payload, "rate_limit_reset_credits", %{"available_count" => 1})), %{@consume => {200, %{"code" => "reset"}}})}

  defp pending_identity(identity, now) do
    redemption = %{"phase" => "consumed_pending_probe", "status" => "redeeming", "attempt_id" => Ecto.UUID.generate(), "generation" => 1, "trigger_kind" => "gateway_auto", "started_at" => DateTime.to_iso8601(now), "consumed_at" => DateTime.to_iso8601(now), "deadline_at" => DateTime.to_iso8601(RedemptionLifecycle.deadline_at(now)), "result" => %{"code" => "reset", "applied" => true}}
    identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption)) |> Repo.update!()
  end

  defp apply_fence(identity, :none), do: identity
  defp apply_fence(identity, :disabled), do: identity |> Ecto.Changeset.change(saved_reset_auto_redeem_enabled: false) |> Repo.update!()
  defp apply_fence(identity, :reserve), do: identity |> Ecto.Changeset.change(saved_reset_auto_redeem_keep_credits: 2) |> Repo.update!()
  defp apply_fence(identity, :expiration), do: identity |> Ecto.Changeset.change(metadata: Map.drop(identity.metadata, ["saved_resets"]) |> Map.put("saved_resets", Map.drop(identity.metadata["saved_resets"], ["next_expires_at", "available_expires_at", "available_expirations"]))) |> Repo.update!()

  defp apply_fence(identity, :cooldown) do
    now = DateTime.utc_now()
    metadata = Map.put(identity.metadata, "saved_reset_redemption", %{"phase" => "confirmed_by_quota", "status" => "succeeded", "trigger_kind" => "gateway_auto", "generation" => 1, "started_at" => DateTime.to_iso8601(now), "consumed_at" => DateTime.to_iso8601(now), "result" => %{"applied" => true}})
    identity |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()
  end
end
