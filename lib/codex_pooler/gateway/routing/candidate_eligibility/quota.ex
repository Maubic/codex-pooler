defmodule CodexPooler.Gateway.Routing.CandidateEligibility.Quota do
  @moduledoc false

  alias CodexPooler.Catalog.Model
  alias CodexPooler.Gateway.Contracts
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Routing.CandidateEligibility
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.CandidateEligibility.UsageLimit
  alias CodexPooler.Gateway.Routing.ProviderCredits
  alias CodexPooler.Gateway.Routing.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.CapacityAssessment
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.SavedResets.RedemptionLifecycle

  @spec filter_quota_eligible_candidates(FilterInput.t()) ::
          CodexPooler.Gateway.Routing.CandidateEligibility.quota_filter_result()
  def filter_quota_eligible_candidates(%FilterInput{} = input) do
    %{model: model, candidates: candidates} = input

    case classify_quota_candidates(model, candidates, nil, input.request_options) do
      {:ok, candidates, decision} ->
        {:ok, candidates, decision}

      {:error, exclusions, refreshable_candidates} ->
        {:refreshable_quota,
         %{
           filter_input: input,
           candidate_exclusions: exclusions,
           refreshable_candidates: refreshable_candidates
         }}
    end
  end

  @spec filter_quota_eligible_candidates(FilterInput.t(), RouteState.t()) ::
          CodexPooler.Gateway.Routing.CandidateEligibility.quota_filter_result()
  def filter_quota_eligible_candidates(%FilterInput{} = input, %RouteState{} = route_state) do
    %{model: model, candidates: candidates} = input

    case classify_quota_candidates(model, candidates, route_state, input.request_options) do
      {:ok, candidates, decision} ->
        {:ok, candidates, decision}

      {:error, exclusions, refreshable_candidates} ->
        {:refreshable_quota,
         %{
           filter_input: input,
           route_state: route_state,
           candidate_exclusions: exclusions,
           refreshable_candidates: refreshable_candidates
         }}
    end
  end

  @doc "Classifies the genuine included-only cohort before any deferred credit/legacy fallback."
  @spec filter_non_credit_candidates(FilterInput.t(), RouteState.t()) :: CandidateEligibility.quota_filter_result()
  def filter_non_credit_candidates(%FilterInput{} = input, %RouteState{} = route_state) do
    case classify_quota_candidates(input.model, input.candidates, route_state, input.request_options, :non_credit) do
      {:ok, candidates, decision} ->
        {:ok, candidates, decision}

      {:error, exclusions, refreshable} ->
        {:refreshable_quota, %{filter_input: input, route_state: route_state, candidate_exclusions: exclusions, refreshable_candidates: refreshable, capacity_band: :non_credit}}
    end
  end

  @spec quota_unavailable_error([map()], boolean()) ::
          {:error, CodexPooler.Gateway.Routing.CandidateEligibility.gateway_error()}
  def quota_unavailable_error(exclusions, refresh_attempted?) when is_list(exclusions) do
    error_details = quota_unavailable_error_details(exclusions)

    generic_quota_unavailable_error(error_details, exclusions, refresh_attempted?)
  end

  @spec quota_unavailable_error(FilterInput.t(), [map()], boolean()) ::
          {:error, CodexPooler.Gateway.Routing.CandidateEligibility.gateway_error()}
  def quota_unavailable_error(
        %FilterInput{} = filter_input,
        exclusions,
        refresh_attempted?
      )
      when is_list(exclusions) do
    error_details = quota_unavailable_error_details(exclusions)

    case hard_pinned_quota_continuity_metadata(filter_input, exclusions, error_details.code) do
      nil ->
        generic_quota_unavailable_error(error_details, exclusions, refresh_attempted?)

      continuity_metadata ->
        {:error,
         Contracts.pinned_continuation_unavailable_error(continuity_metadata)
         |> Map.put(:candidate_exclusions, exclusions)
         |> Map.put(:quota_refresh_attempted, refresh_attempted?)}
    end
  end

  @doc """
  The quota exclusions of `candidates` under the same current request scope
  and classification the filter applies; a candidate that would be kept
  contributes none.
  """
  @spec candidate_exclusions(Model.t(), [CodexPooler.Gateway.Routing.CandidateEligibility.candidate()], RouteState.t(), RequestOptions.t()) :: [map()]
  def candidate_exclusions(%Model{} = model, candidates, %RouteState{} = route_state, %RequestOptions{} = request_options) when is_list(candidates) do
    case classify_quota_candidates(model, candidates, route_state, request_options) do
      {:error, exclusions, _refreshable} -> exclusions
      {:ok, _candidates, _decision} -> []
    end
  end

  # Every candidate exhausted with a known reset answers the provider's own
  # terminal `429 usage_limit_reached` with the earliest reset; any unknown
  # return time keeps the retryable `503` (findings#206 row 206-508).
  defp generic_quota_unavailable_error(error_details, exclusions, refresh_attempted?) do
    metadata = %{candidate_exclusions: exclusions, quota_refresh_attempted: refresh_attempted?}

    case usage_limit(error_details.code, exclusions) do
      {:ok, usage_limit} ->
        {:error, error(429, error_details.code, error_details.message, "model", Map.put(metadata, :usage_limit, usage_limit))}

      :unknown ->
        {:error, error(503, error_details.code, error_details.message, "model", metadata)}
    end
  end

  defp usage_limit("quota_exhausted", exclusions), do: UsageLimit.earliest_reset(exclusions, DateTime.utc_now())
  defp usage_limit(_code, _exclusions), do: :unknown

  defp hard_pinned_quota_continuity_metadata(
         %FilterInput{request_options: request_options, model: model},
         exclusions,
         internal_reason
       ) do
    with %{} = pin_metadata <- SessionContinuity.hard_pin_metadata(request_options, model),
         %{} = target <- first_quota_exclusion_target(exclusions) do
      Map.merge(pin_metadata, %{
        "denial_family" => "pinned_continuation_unavailable",
        "continuity_family" => "pinned_codex_session",
        "internal_reason" => internal_reason,
        "pool_upstream_assignment_id" => Map.get(target, :pool_upstream_assignment_id),
        "upstream_identity_id" => Map.get(target, :upstream_identity_id)
      })
    else
      _missing -> nil
    end
  end

  defp first_quota_exclusion_target(exclusions) do
    Enum.find(exclusions, fn exclusion ->
      present?(Map.get(exclusion, :pool_upstream_assignment_id)) and
        present?(Map.get(exclusion, :upstream_identity_id))
    end)
  end

  @doc """
  Whether a candidate would survive quota classification right now.

  Canonical partition selection uses this to answer "can this partition still
  serve a turn?" before dispatch narrows the candidate list. It reuses the same
  eligibility and post-reset lifecycle predicates the real filter applies, so a
  partition is never judged unroutable on rules routing would not have enforced.
  Circuit state is deliberately not consulted: it is per route class and
  short-lived, and letting it move the selected partition would make the
  advertised catalog flap.
  """
  @spec quota_routable?(
          Model.t(),
          CodexPooler.Gateway.Routing.CandidateEligibility.candidate(),
          RoutingQuotaSnapshot.t(),
          map()
        ) :: boolean()
  def quota_routable?(
        %Model{},
        {assignment, identity},
        %RoutingQuotaSnapshot{} = snapshot,
        request_context
      ) do
    context = put_candidate_scope(request_context, assignment, identity)
    eligibility = ProviderCredits.eligibility(snapshot, context, :all)

    not claimed_pending_snapshot?(snapshot) and
      (eligibility.eligible? or reset_probe_snapshot_routeable?(snapshot, eligibility.provider_credits_decision.eligibility.exclusions, context))
  end

  @doc "Whether this candidate is admitted specifically by its confirmed reset lifecycle."
  @spec reset_probe_candidate?(Model.t(), CandidateEligibility.candidate(), RouteState.t()) :: boolean()
  def reset_probe_candidate?(%Model{} = model, {_assignment, identity}, %RouteState{} = route_state) do
    case routing_quota_eligibility(identity, model, route_state) do
      %{routing_state: :reset_probe} -> true
      _other -> false
    end
  end

  @spec windowless_candidate?(
          Model.t(),
          CodexPooler.Gateway.Routing.CandidateEligibility.candidate(),
          RouteState.t()
        ) :: boolean()
  def windowless_candidate?(
        %Model{} = model,
        {_assignment, identity},
        %RouteState{} = route_state
      ) do
    Map.has_key?(route_state.quota_snapshots, identity.id) and
      not claimed_pending_snapshot?(route_state.quota_snapshots[identity.id]) and
      match?(
        %{routing_state: state}
        when state in [:windowless_provider_available, :provider_available],
        routing_quota_eligibility(identity, model, route_state)
      )
  end

  @spec provider_permission_current?(
          Model.t(),
          CandidateEligibility.candidate(),
          RouteState.t() | nil
        ) ::
          boolean()
  def provider_permission_current?(_model, _candidate, nil), do: true

  def provider_permission_current?(model, {assignment, identity} = candidate, route_state) do
    if windowless_candidate?(model, candidate, route_state) do
      with {:ok, candidates} <- CandidateEligibility.routable_candidates(model),
           {current_assignment, current_identity} <-
             Enum.find(candidates, fn {current, _identity} -> current.id == assignment.id end),
           true <-
             CredentialFencing.credential_epoch(current_identity) ==
               CredentialFencing.credential_epoch(identity) do
        as_of = DateTime.utc_now()

        snapshot = RoutingQuotaSnapshot.load_by_identity_ids([identity.id], as_of)[identity.id]

        current_permission_snapshot?(
          snapshot,
          model,
          {current_assignment, current_identity},
          CredentialFencing.credential_epoch(identity)
        )
      else
        _unavailable -> false
      end
    else
      true
    end
  end

  defp current_permission_snapshot?(
         %RoutingQuotaSnapshot{credential_epoch: epoch} = snapshot,
         model,
         candidate,
         epoch
       ) do
    quota_routable?(model, candidate, snapshot, ProviderCredits.request_context(model))
  end

  defp current_permission_snapshot?(_snapshot, _model, _candidate, _epoch), do: false

  defp classify_quota_candidates(%Model{} = model, candidates, route_state, request_options, band \\ :all) do
    {{precise_candidates, credit_backed_probe_candidates, weekly_probe_candidates, reset_probe_candidates, windowless_candidates, exclusions, refreshable_candidates}, assessments} =
      Enum.reduce(candidates, {{[], [], [], [], [], [], []}, %{}}, fn {assignment, identity} = candidate, {acc, assessments} ->
        eligibility = routing_quota_eligibility(identity, model, route_state, request_options, band, assignment)
        {add_classified_quota_candidate(eligibility, candidate, assignment, acc), Map.put(assessments, assignment.id, eligibility)}
      end)

    precise_candidates = Enum.reverse(precise_candidates)
    credit_backed_probe_candidates = Enum.reverse(credit_backed_probe_candidates)
    weekly_probe_candidates = Enum.reverse(weekly_probe_candidates)
    reset_probe_candidates = Enum.reverse(reset_probe_candidates)

    {provider_candidates, windowless_candidates} =
      windowless_candidates
      |> Enum.reverse()
      |> Enum.split_with(fn {state, _candidate} -> state == :provider_available end)

    provider_candidates = Enum.map(provider_candidates, &elem(&1, 1))
    windowless_candidates = Enum.map(windowless_candidates, &elem(&1, 1))
    {non_credit_windowless, legacy_windowless} = Enum.split_with(windowless_candidates, fn {assignment, _identity} -> assessments[assignment.id].capacity_basis != :unknown_legacy end)

    candidates =
      precise_candidates ++
        weekly_probe_candidates ++
        reset_probe_candidates ++
        provider_candidates ++
        non_credit_windowless ++ credit_backed_probe_candidates ++ legacy_windowless

    case candidates do
      [] ->
        {:error, Enum.reverse(exclusions), Enum.reverse(refreshable_candidates)}

      candidates ->
        {:ok, candidates, quota_decision(candidates, assessments)}
    end
  end

  defp routing_quota_eligibility(identity, %Model{} = model, route_state, request_options \\ nil, band \\ :all, assignment \\ nil) do
    snapshot =
      case route_state do
        %RouteState{} -> RouteState.quota_snapshot_for_identity(route_state, identity)
        nil -> RoutingQuotaSnapshot.load_by_identity_ids([identity.id], DateTime.utc_now())[identity.id]
      end

    context = ProviderCredits.request_context(model, request_options, assignment && assignment.id) |> put_candidate_scope(assignment, identity)
    eligibility = ProviderCredits.eligibility(snapshot, context, band)

    cond do
      claimed_pending_snapshot?(snapshot) ->
        eligibility

      eligibility.eligible? ->
        eligibility

      reset_probe_snapshot_routeable?(snapshot, eligibility.provider_credits_decision.eligibility.exclusions, context) ->
        Map.merge(eligibility, %{eligible?: true, routing_state: :reset_probe, capacity_basis: :recovered_included, exclusions: []})

      true ->
        eligibility
    end
  end

  defp put_candidate_scope(context, %{id: assignment_id}, %{id: identity_id}),
    do: Map.merge(context, %{pool_upstream_assignment_id: assignment_id, upstream_identity_id: identity_id})

  defp put_candidate_scope(context, _assignment, _identity), do: context

  defp claimed_pending_snapshot?(snapshot),
    do: RedemptionLifecycle.phase(snapshot.redemption) == RedemptionLifecycle.consumed_pending_probe() and is_binary(RedemptionLifecycle.probe_holder(snapshot.redemption))

  defp reset_probe_snapshot_routeable?(snapshot, reasons, context) do
    CapacityAssessment.guarded_probe_exclusions?(reasons) and
      RedemptionLifecycle.phase(snapshot.redemption) == RedemptionLifecycle.confirmed_by_quota() and
      RedemptionLifecycle.routeable?(snapshot.redemption, snapshot.as_of) and
      CapacityAssessment.guarded_probe_permitted?(snapshot, context)
  end

  defp add_classified_quota_candidate(
         %{routing_state: :precise},
         candidate,
         _assignment,
         {precise, credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}
       ) do
    {[candidate | precise], credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}
  end

  defp add_classified_quota_candidate(
         %{routing_state: :credit_backed_probe},
         candidate,
         _assignment,
         {precise, credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}
       ) do
    {precise, [candidate | credit_backed], weekly_probes, reset_probes, windowless, excluded, refreshable}
  end

  defp add_classified_quota_candidate(
         %{routing_state: :weekly_only_probe},
         candidate,
         _assignment,
         {precise, credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}
       ) do
    {precise, credit_backed, [candidate | weekly_probes], reset_probes, windowless, excluded, refreshable}
  end

  defp add_classified_quota_candidate(
         %{routing_state: state},
         candidate,
         _assignment,
         {precise, credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}
       )
       when state in [:windowless_provider_available, :provider_available] do
    {precise, credit_backed, weekly_probes, reset_probes, [{state, candidate} | windowless], excluded, refreshable}
  end

  defp add_classified_quota_candidate(%{routing_state: :reset_probe}, candidate, _assignment, {precise, credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}),
    do: {precise, credit_backed, weekly_probes, [candidate | reset_probes], windowless, excluded, refreshable}

  defp add_classified_quota_candidate(
         %{exclusions: reasons},
         {_, identity} = candidate,
         assignment,
         {precise, credit_backed, weekly_probes, reset_probes, windowless, excluded, refreshable}
       ) do
    exclusion = quota_candidate_exclusion(assignment, identity, reasons)
    refreshable = maybe_add_refreshable_quota_candidate(refreshable, candidate, reasons)

    {precise, credit_backed, weekly_probes, reset_probes, windowless, [exclusion | excluded], refreshable}
  end

  defp maybe_add_refreshable_quota_candidate(refreshable, candidate, reasons) do
    if stale_quota_refreshable?(reasons), do: [candidate | refreshable], else: refreshable
  end

  defp stale_quota_refreshable?(reasons) when is_list(reasons) do
    reasons != [] and Enum.all?(reasons, &stale_quota_refreshable_reason?/1)
  end

  defp stale_quota_refreshable_reason?(%{code: "quota_window_unusable"} = reason),
    do: stale_quota_refreshable_reason_codes?(Map.get(reason, :reason_codes))

  defp stale_quota_refreshable_reason?(%{"code" => "quota_window_unusable"} = reason),
    do: stale_quota_refreshable_reason_codes?(Map.get(reason, "reason_codes"))

  defp stale_quota_refreshable_reason?(_reason), do: false

  defp stale_quota_refreshable_reason_codes?(reason_codes) when is_list(reason_codes) do
    "not_fresh" in reason_codes and
      not Enum.any?(reason_codes, &(&1 in ["reset_missing", "exhausted"]))
  end

  defp stale_quota_refreshable_reason_codes?(_reason_codes), do: false

  defp quota_candidate_exclusion(assignment, identity, reasons) do
    %{
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: identity.id,
      reasons: Enum.map(reasons, &sanitize_quota_exclusion/1)
    }
  end

  defp quota_unavailable_error_details(exclusions) do
    reasons = Enum.flat_map(exclusions, &Map.get(&1, :reasons, []))

    if Enum.any?(reasons, &quota_exhaustion_reason?/1) do
      %{
        code: "quota_exhausted",
        message: "upstream quota is exhausted until its reset time"
      }
    else
      %{
        code: "quota_evidence_unavailable",
        message: "no upstream account has fresh reset-bearing quota evidence for this model"
      }
    end
  end

  defp quota_exhaustion_reason?(%{code: code}) when code in ["quota_weekly_exhausted"], do: true

  defp quota_exhaustion_reason?(%{"code" => code}) when code in ["quota_weekly_exhausted"],
    do: true

  defp quota_exhaustion_reason?(%{reason_codes: reason_codes}) when is_list(reason_codes),
    do: "exhausted" in reason_codes

  defp quota_exhaustion_reason?(%{"reason_codes" => reason_codes}) when is_list(reason_codes),
    do: "exhausted" in reason_codes

  defp quota_exhaustion_reason?(_reason), do: false

  defp quota_decision([{first_assignment, _identity} | _] = candidates, assessments) do
    first = assessments[first_assignment.id]

    counts =
      Enum.reduce(candidates, %{}, fn {assignment, _identity}, counts ->
        Map.update(counts, assessments[assignment.id].routing_state, 1, &(&1 + 1))
      end)

    %{
      "allowed" => true,
      "summary" => capacity_summary(first.capacity_basis, first.routing_state),
      "routing_state" => Atom.to_string(first.routing_state),
      "precise_candidate_count" => Map.get(counts, :precise, 0),
      "credit_backed_probe_candidate_count" => Map.get(counts, :credit_backed_probe, 0),
      "weekly_probe_candidate_count" => Map.get(counts, :weekly_only_probe, 0),
      "reset_probe_candidate_count" => Map.get(counts, :reset_probe, 0),
      "provider_available_candidate_count" => Map.get(counts, :provider_available, 0),
      "windowless_provider_available_candidate_count" => Map.get(counts, :windowless_provider_available, 0),
      "eligible_candidate_count" => length(candidates)
    }
    |> put_capacity_decisions(candidates, assessments)
  end

  defp capacity_summary(:provider_credits, _state), do: "allowed by current provider credit permission before blocked-request reset recovery"
  defp capacity_summary(:unknown_legacy, _state), do: "allowed by legacy provider attestation with unknown capacity basis"
  defp capacity_summary(:model_allowance, _state), do: "allowed by existing exact-model allowance"
  defp capacity_summary(:ordinary_provider_permission, _state), do: "allowed by current ordinary provider permission"
  defp capacity_summary(:windowless_provider_permission, _state), do: "allowed by current windowless provider permission"
  defp capacity_summary(:recovered_included, _state), do: "allowed by saved reset included recovery"
  defp capacity_summary(_basis, :weekly_only_probe), do: "allowed by weekly quota evidence"
  defp capacity_summary(_basis, _state), do: "allowed by fresh included quota"

  defp put_capacity_decisions(decision, [{first_assignment, _identity} | _] = candidates, assessments) do
    first = assessments[first_assignment.id]

    per_assignment =
      Map.new(candidates, fn {assignment, _identity} ->
        eligibility = assessments[assignment.id]
        {assignment.id, %{"capacity_basis" => Atom.to_string(eligibility.capacity_basis), "routing_state" => Atom.to_string(eligibility.routing_state)}}
      end)

    Map.merge(decision, %{"capacity_basis" => Atom.to_string(first.capacity_basis), "routing_state" => Atom.to_string(first.routing_state), "candidate_capacity" => per_assignment})
  end

  @doc false
  @spec sanitize_quota_exclusion(map()) :: map()
  def sanitize_quota_exclusion(%{} = exclusion) do
    exclusion
    |> Map.take([
      :code,
      :message,
      :reason_codes,
      :provider_credits_reason_codes,
      :quota_key,
      :window_kind,
      :quota_scope,
      :quota_family,
      :model,
      :upstream_model,
      :source,
      :source_precision,
      :freshness_state,
      :reset_at,
      :hint_reset_at,
      :retained_refusal_code,
      :retained_refusal_observed_at,
      :retained_refusal_reset_at
    ])
    |> Map.new(fn {key, value} -> {to_string(key), value} end)
  end

  defp present?(value) when is_binary(value), do: String.trim(value) != ""
  defp present?(_value), do: false

  defp error(status, code, message, param, metadata),
    do: Map.merge(%{status: status, code: code, message: message, param: param}, metadata)
end
