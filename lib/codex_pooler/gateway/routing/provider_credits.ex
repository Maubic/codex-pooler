defmodule CodexPooler.Gateway.Routing.ProviderCredits do
  @moduledoc false

  alias CodexPooler.Catalog.Model
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.Transport
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Quota.{CapacityAssessment, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows.AccountDenial

  @type band :: :all | :non_credit

  @spec request_context(Model.t()) :: map()
  @spec request_context(Model.t(), RequestOptions.t() | nil) :: map()
  @spec request_context(Model.t(), RequestOptions.t() | nil, Ecto.UUID.t() | nil) :: map()
  def request_context(%Model{} = model, request_options \\ nil, assignment_id \\ nil) do
    %{model: model.exposed_model_id, requested_model: model.exposed_model_id, upstream_model: model.upstream_model_id, upstream_model_id: model.upstream_model_id, serving_mode: serving_mode(request_options), transport: transport(request_options, assignment_id), route_class: route_class(request_options)}
  end

  @spec decision(RoutingQuotaSnapshot.t(), map()) :: CodexPooler.Upstreams.ProviderCreditsPolicy.decision()
  def decision(%RoutingQuotaSnapshot{} = snapshot, context), do: Upstreams.provider_credits_decision(snapshot, context)

  @spec eligibility(RoutingQuotaSnapshot.t(), map(), band()) :: map()
  def eligibility(%RoutingQuotaSnapshot{} = snapshot, context, band \\ :all) do
    decision = decision(snapshot, context)
    observed_capacity_reason_codes = decision.reason_codes
    decision = band_decision(decision, snapshot, context, band)
    physical = decision.eligibility
    denial = runtime_account_denial(snapshot, band, decision)

    cond do
      not is_nil(denial) ->
        Map.merge(physical, %{eligible?: false, routing_state: :blocked, exclusions: [runtime_denial_exclusion(denial)], capacity_basis: :none, provider_credits_decision: decision})

      decision.eligible? and (band == :all or non_credit_basis?(decision.capacity_basis)) ->
        state = eligible_routing_state(decision, physical)
        Map.merge(physical, %{eligible?: true, routing_state: state, exclusions: [], capacity_basis: decision.capacity_basis, provider_credits_decision: decision})

      band == :non_credit and not non_credit_basis?(decision.capacity_basis) ->
        physical_exclusion(physical, decision)

      true ->
        reasons = denied_exclusions(physical, decision)

        Map.merge(physical, %{eligible?: false, routing_state: :blocked, exclusions: reasons, capacity_basis: decision.capacity_basis, provider_credits_decision: decision})
    end
    |> Map.put(:observed_capacity_reason_codes, observed_capacity_reason_codes)
  end

  defp band_decision(decision, snapshot, context, :non_credit) do
    if CapacityAssessment.recovery_pending?(snapshot), do: decision, else: Map.merge(decision, CapacityAssessment.physical_non_credit_assessment(snapshot, context))
  end

  defp band_decision(decision, _snapshot, _context, :all), do: decision
  defp runtime_account_denial(snapshot, :all, %{capacity_basis: :provider_credits}), do: AccountDenial.active_for_credits(snapshot)
  defp runtime_account_denial(snapshot, :all, _decision), do: AccountDenial.active(snapshot)
  defp runtime_account_denial(_snapshot, :non_credit, _decision), do: nil

  defp denied_exclusions(physical, decision) do
    reasons = if physical.eligible?, do: [], else: physical.exclusions

    if reasons != [] and not Enum.any?(decision.reason_codes, &(&1 in ["saved_reset_probe_pending", "saved_reset_recovery_unavailable"])) do
      Enum.map(reasons, &Map.put(&1, :provider_credits_reason_codes, decision.reason_codes))
    else
      reasons ++ [%{code: "quota_window_unusable", message: "provider credit admission is unavailable", reason_codes: decision.reason_codes, capacity_basis: Atom.to_string(decision.capacity_basis)}]
    end
  end

  defp eligible_routing_state(%{capacity_basis: :provider_credits}, _physical), do: :credit_backed_probe
  defp eligible_routing_state(%{capacity_basis: :recovered_included}, %{eligible?: false}), do: :reset_probe
  defp eligible_routing_state(_decision, physical), do: physical.routing_state

  @spec non_credit_basis?(CapacityAssessment.capacity_basis()) :: boolean()
  def non_credit_basis?(basis), do: basis in [:included_window, :ordinary_provider_permission, :model_allowance, :windowless_provider_permission, :recovered_included]

  @spec tier(map()) :: non_neg_integer()
  def tier(%{capacity_basis: :provider_credits}), do: 5
  def tier(%{capacity_basis: :unknown_legacy}), do: 6
  def tier(%{routing_state: :weekly_only_probe}), do: 1
  def tier(%{routing_state: :reset_probe}), do: 2
  def tier(%{capacity_basis: basis}) when basis in [:ordinary_provider_permission, :model_allowance], do: 3
  def tier(%{capacity_basis: :windowless_provider_permission}), do: 4
  def tier(_decision), do: 0

  defp physical_exclusion(physical, decision) do
    reasons =
      if physical.eligible? or physical.exclusions == [],
        do: [%{code: "quota_window_unusable", reason_codes: ["non_credit_capacity_unverified"]}],
        else: physical.exclusions

    Map.merge(physical, %{eligible?: false, routing_state: :blocked, exclusions: reasons, capacity_basis: decision.capacity_basis, provider_credits_decision: decision})
  end

  defp runtime_denial_exclusion(denial),
    do: %{code: "quota_window_unusable", reason_codes: ["exhausted", "provider_denied"], quota_key: "account", quota_scope: "account", quota_family: "account", source: denial.source, rate_limit_reached_type: denial.reached_type, reset_at: if(denial.reset_at, do: DateTime.to_iso8601(denial.reset_at)), hint_reset_at: if(denial.hint_reset_at, do: DateTime.to_iso8601(denial.hint_reset_at))}

  defp serving_mode(%RequestOptions{} = options), do: RequestOptions.model_serving_mode(options)
  defp serving_mode(_options), do: nil

  defp route_class(%RequestOptions{} = options), do: options.transport.route_class
  defp route_class(_options), do: nil

  defp transport(%RequestOptions{transport: transport}, assignment_id), do: Transport.upstream_transport(transport, assignment_id)
  defp transport(_options, _assignment_id), do: nil
end
