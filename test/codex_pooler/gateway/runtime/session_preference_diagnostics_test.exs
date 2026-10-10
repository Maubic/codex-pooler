defmodule CodexPooler.Gateway.Runtime.SessionPreferenceDiagnosticsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, gateway_upstream: 4, start_upstream: 1]

  alias CodexPooler.{Access, Accounting, FakeUpstream, ProviderCreditsFixtures, Repo}
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Persistence.SessionContinuity, as: ContinuityStore
  alias CodexPooler.Gateway.Routing.{BridgeRing, RouteFiltering, RoutePlanInput}
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, RouteState}
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.MCP.{MetadataSanitizer, Redaction}
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation

  @endpoint "/backend-api/codex/responses"

  test "full-band exclusion records operator credit opt-out instead of inventing a provider failure" do
    assert_fallback(:pinned, :weekly_credit_only, :full, "quota_unavailable", "provider_credits_disabled", false)
  end

  test "included-band exclusion keeps observed unknown credits separate from the causal stage" do
    assert_fallback(:pinned, :included, :unknown, "non_credit_band_excluded", nil)
  end

  for preference <- [:pinned, :recreated, :previous_window] do
    test "#{preference} unavailable credit preference records the full-band refusal on successful fallback" do
      assert_fallback(unquote(preference), :weekly_credit_only, :unknown, "quota_unavailable", "provider_credit_permission_unavailable")
    end

    for enabled <- [true, false] do
      test "#{preference} included capacity wins before credits with policy #{enabled}" do
        assert_fallback(unquote(preference), :included, :full, "non_credit_band_excluded", nil, unquote(enabled))
      end
    end
  end

  test "a usable preference and a sessionless request omit unavailable diagnostics" do
    {setup, preferred_fake, _sibling_fake} = arrangement(:weekly_credit_only, :full, true)
    assert {:ok, %{status: 200}} = execute(setup, :pinned)
    routing = latest_request().request_metadata["routing"]
    assert routing["session_preference_status"] == "applied"
    refute Map.has_key?(routing, "session_preference_diagnostics")
    refute Map.has_key?(routing["session_preference_diagnostics"] || %{}, "observed_capacity_reason_codes")
    assert FakeUpstream.physical_counts(preferred_fake).http_generation == 1

    assert {:ok, %{status: 200}} = execute(setup, nil)
    routing = latest_request().request_metadata["routing"]
    refute Map.has_key?(routing, "session_preference_kind")
    refute Map.has_key?(routing, "session_preference_diagnostics")
    refute Map.has_key?(routing["session_preference_diagnostics"] || %{}, "observed_capacity_reason_codes")
  end

  test "circuit exclusion uses captured stage and a later filtering clears it" do
    {setup, _preferred_fake, _sibling_fake} = arrangement(:included, :full, true)
    {input, state} = filtering_input(setup)
    state = RouteState.put_circuit_snapshots(state, %{setup.assignment.id => false, setup.sibling.assignment.id => true})
    assert {:ok, candidates, options, filtered} = RouteFiltering.filter_candidates_with_route_state(input, state)
    routing = plan(input, candidates, options, filtered).request_metadata
    assert routing["session_preference_diagnostics"]["unavailable_reason"] == "circuit_unavailable"
    assert routing["session_preference_diagnostics"]["unavailable_reason_codes"] == []
    refute Map.has_key?(routing["session_preference_diagnostics"] || %{}, "observed_capacity_reason_codes")

    # The next retry no longer assesses the old account. It must not repeat
    # the previous filter's circuit explanation for an unassessed candidate.
    input = FilterInput.put_candidates(input, candidates)
    assert {:ok, candidates, options, filtered} = RouteFiltering.filter_candidates_with_route_state(input, filtered)
    routing = plan(input, candidates, options, filtered).request_metadata
    assert routing["session_preference_diagnostics"]["unavailable_reason"] == "candidate_unavailable"
    assert routing["session_preference_diagnostics"]["unavailable_reason_codes"] == []
    refute Map.has_key?(routing["session_preference_diagnostics"] || %{}, "observed_capacity_reason_codes")
  end

  test "quota explanation uses captured snapshot even after durable credit permission changes" do
    {setup, preferred_fake, _sibling_fake} = arrangement(:weekly_credit_only, :unknown, true)
    {input, state} = filtering_input(setup)
    FakeUpstream.set_mode(preferred_fake, routes(:weekly_credit_only, :full))
    reconcile(setup, preferred_fake)
    before = FakeUpstream.physical_counts(preferred_fake)
    assert {:ok, candidates, options, filtered} = RouteFiltering.filter_candidates_with_route_state(input, state)
    routing = plan(input, candidates, options, filtered).request_metadata
    assert routing["session_preference_diagnostics"]["unavailable_reason"] == "quota_unavailable"
    assert "provider_credit_permission_unavailable" in routing["session_preference_diagnostics"]["unavailable_reason_codes"]
    assert FakeUpstream.physical_counts(preferred_fake) == before
    assert filtered.quota_snapshots == state.quota_snapshots
  end

  test "same-request replanning clears the persisted old explanation and keeps the former plan immutable" do
    {setup, preferred_fake, _sibling_fake} = arrangement(:weekly_credit_only, :unknown, true)
    {input, state} = filtering_input(setup)
    assert {:ok, candidates, options, filtered} = RouteFiltering.filter_candidates_with_route_state(input, state)
    request = CodexPooler.PoolerFixtures.request_fixture(input.auth, %{model_id: setup.model.id, requested_model: setup.model.exposed_model_id})
    context_input = %{auth: input.auth, model: setup.model, endpoint: @endpoint, payload: input.payload, reserved: %{request: request}, candidates: candidates, request_options: options, route_state: filtered}
    assert {:ok, first} = Context.new(context_input)
    assert {:ok, persisted} = Accounting.persist_request_metadata(first.reserved.request)
    assert persisted.request_metadata["routing"]["session_preference_diagnostics"]["unavailable_reason"] == "quota_unavailable"

    # Restore provider credit authority and pass the actual refreshed filtering
    # result to the same-request partition-fallback replan boundary.
    FakeUpstream.set_mode(preferred_fake, routes(:weekly_credit_only, :full))
    reconcile(setup, preferred_fake)
    assert {:ok, recovered, recovered_options, recovered_state} = RouteFiltering.filter_candidates_with_route_state(input, RouteState.refresh_quota_snapshots(filtered))
    assert Enum.any?(recovered, fn {assignment, _identity} -> assignment.id == setup.assignment.id end)
    assert {:ok, next} = Context.new(%{context_input | reserved: %{request: persisted}, candidates: recovered, request_options: recovered_options, route_state: recovered_state})
    assert {:ok, persisted} = Accounting.persist_request_metadata(next.reserved.request)
    assert persisted.request_metadata["routing"]["session_preference_status"] == "applied"
    refute Map.has_key?(persisted.request_metadata["routing"], "session_preference_diagnostics")
    assert first.route_plan.request_metadata["session_preference_diagnostics"]["unavailable_reason"] == "quota_unavailable"

    no_preference = RequestOptions.put_continuity(options, codex_session: nil)
    assert {:ok, next} = Context.new(%{context_input | reserved: %{request: persisted}, candidates: recovered, request_options: no_preference, route_state: recovered_state})
    assert {:ok, persisted} = Accounting.persist_request_metadata(next.reserved.request)
    refute Map.has_key?(persisted.request_metadata["routing"], "session_preference_kind")
    refute Map.has_key?(persisted.request_metadata["routing"], "session_preference_diagnostics")
  end

  defp filtering_input(setup) do
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = %{"model" => setup.model.exposed_model_id, "input" => []}
    options = RequestOptions.build(%{model_serving_mode: "full", model_serving_mode_configured: "full", model_serving_mode_source: "override", codex_session: %CodexSession{pool_upstream_assignment_id: setup.assignment.id}}, @endpoint, payload)
    candidates = [{setup.assignment, setup.identity}, {setup.sibling.assignment, setup.sibling.identity}]
    input = FilterInput.new(%{auth: auth, model: setup.model, endpoint: @endpoint, payload: payload, request_options: options, candidates: candidates})
    state = RouteState.new(%{visible_model: setup.model, candidates: candidates}) |> RouteState.preload_routing_snapshots(auth, setup.model, options)
    {input, state}
  end

  defp plan(input, candidates, options, state) do
    request = CodexPooler.PoolerFixtures.request_fixture(input.auth, %{model_id: input.model.id, requested_model: input.model.exposed_model_id})
    BridgeRing.plan_route(%{auth: input.auth, model: input.model, candidates: candidates, request_options: options, route_state: state, route_plan_input: RoutePlanInput.from_reserved(%{request: request})})
  end

  defp assert_fallback(preference, sibling_capacity, preferred_credits, reason, credit_code, policy \\ true) do
    {setup, preferred_fake, sibling_fake} = arrangement(sibling_capacity, preferred_credits, policy)
    before_preferred = FakeUpstream.physical_counts(preferred_fake)
    before_sibling = FakeUpstream.physical_counts(sibling_fake)
    assert {:ok, %{status: 200}} = execute(setup, preference)
    request = latest_request()
    assert request.status == "succeeded"
    routing = request.request_metadata["routing"]
    assert routing["session_preference_kind"] == Atom.to_string(preference)
    assert routing["session_preference_status"] == "candidate_unavailable"
    assert routing["session_preference_diagnostics"]["unavailable_reason"] == reason
    codes = routing["session_preference_diagnostics"]["unavailable_reason_codes"]
    if credit_code, do: assert(credit_code in codes), else: assert(codes == [])
    observed_codes = routing["session_preference_diagnostics"]["observed_capacity_reason_codes"]

    cond do
      preferred_credits == :unknown -> assert "provider_credit_permission_unavailable" in observed_codes
      not policy -> assert observed_codes == ["provider_credits_disabled"]
      true -> assert observed_codes == []
    end

    refute inspect(request.request_metadata) =~ "route_filter_exclusions"
    attempt = Repo.one!(from attempt in Attempt, where: attempt.request_id == ^request.id)
    assert attempt.pool_upstream_assignment_id == setup.sibling.assignment.id
    refute inspect(attempt.response_metadata) =~ "route_filter_exclusions"
    assert FakeUpstream.physical_counts(preferred_fake).http_generation == 0
    assert FakeUpstream.physical_counts(sibling_fake).http_generation == 1
    assert FakeUpstream.physical_counts(preferred_fake).usage == before_preferred.usage
    assert FakeUpstream.physical_counts(sibling_fake).usage == before_sibling.usage
    assert FakeUpstream.physical_counts(preferred_fake).consume == 0
    safe = MetadataSanitizer.safe_metadata(%{"routing" => Map.put(routing, "prompt", "synthetic forbidden content")})
    assert map_size(safe["routing"]) <= 20
    refute Map.has_key?(safe["routing"], "prompt")
    assert safe["routing"]["session_preference_diagnostics"]["unavailable_reason"] == reason
    assert safe["routing"]["session_preference_diagnostics"]["unavailable_reason_codes"] == codes
    assert safe["routing"]["session_preference_diagnostics"]["observed_capacity_reason_codes"] == observed_codes
    assert :ok = Redaction.assert_mcp_output_safe!(safe)
  end

  defp arrangement(sibling_capacity, preferred_credits, policy) do
    preferred_fake = start_upstream(routes(:weekly_credit_only, preferred_credits))
    setup = gateway_setup(preferred_fake, quota?: false, exposed_model_id: "synthetic-preference-#{System.unique_integer([:positive])}", upstream_model_id: "synthetic-preference") |> reconcile(preferred_fake)
    identity = Repo.update!(Ecto.Changeset.change(setup.identity, allow_provider_credits: policy))
    sibling_fake = start_upstream(routes(sibling_capacity, :full))
    sibling = gateway_upstream(setup.pool, sibling_fake, "synthetic-sibling", []) |> reconcile(sibling_fake)
    sibling_identity = Repo.update!(Ecto.Changeset.change(sibling.identity, allow_provider_credits: true))
    sibling = %{sibling | identity: sibling_identity}
    metadata = setup.model.metadata
    source = metadata["source_assignment_models"][setup.assignment.id]
    metadata = metadata |> Map.put("source_assignment_ids", [setup.assignment.id, sibling.assignment.id]) |> put_in(["source_assignment_models", sibling.assignment.id], source)
    model = Repo.update!(Ecto.Changeset.change(setup.model, metadata: metadata))
    {Map.merge(setup, %{identity: identity, model: model, sibling: sibling}), preferred_fake, sibling_fake}
  end

  defp reconcile(setup, fake) do
    identity = Repo.update!(Ecto.Changeset.change(setup.identity, metadata: Map.put(setup.identity.metadata, "usage_base_url", FakeUpstream.url(fake))))
    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(identity, setup.assignment)
    %{setup | identity: identity}
  end

  defp routes(capacity, credits), do: {:path_json, Map.put(ProviderCreditsFixtures.usage_routes(ProviderCreditsFixtures.usage_payload(capacity, credits: credits)), @endpoint, {200, %{"id" => "resp_synthetic_preference", "object" => "response", "output" => []}})}

  defp execute(setup, preference) do
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)
    payload = %{"model" => setup.model.exposed_model_id, "input" => []}
    options = RequestOptions.build(%{api_key_policy: policy, codex_session: session(setup, preference)}, @endpoint, payload)
    Service.execute(auth, @endpoint, payload, options)
  end

  defp session(_setup, nil), do: nil

  defp session(setup, preference) do
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    options = RequestOptions.build(%{session_header: "synthetic-preference-#{System.unique_integer([:positive])}", session_header_source: "session-id"}, @endpoint, %{})
    assert {:ok, session} = ContinuityStore.start_codex_session(auth, options)
    session = if preference == :pinned, do: Repo.update!(Ecto.Changeset.change(session, pool_upstream_assignment_id: setup.assignment.id)), else: session

    case preference do
      :pinned -> session
      :recreated -> %{session | recreated_from_assignment_id: setup.assignment.id}
      :previous_window -> %{session | previous_window_assignment_id: setup.assignment.id}
    end
  end

  defp latest_request, do: Repo.one!(from request in Request, order_by: [desc: request.admitted_at], limit: 1)
end
