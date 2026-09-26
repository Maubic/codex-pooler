defmodule CodexPooler.Gateway.Routing.CandidateEligibility.AccountDenial do
  @moduledoc """
  Removes candidates whose account the provider refused at workspace level.

  A `429` carrying `x-codex-rate-limit-reached-type` with a `workspace_*`
  value refuses the account for every model, and every Pool that shares the
  identity, until the reset the provider reported (findings#206 row 206-509).
  The windows on that response can sit well below 100%, so quota eligibility
  keeps calling the account routable; this filter reads the denial from the
  same routing snapshot and excludes the candidate before reservation and
  dispatch.

  It runs after quota eligibility and after the saved-reset decisions, so it
  never changes when an automatic redemption fires or which candidate a
  guarded reset probe claims: only the candidate bound to that probe or
  admitted by its confirmed reset lifecycle is exempt. Siblings are still filtered.
  The exclusion carries `reason_codes`
  `["exhausted", "provider_denied"]`, so the public answer is the ordinary
  `quota_exhausted` one with the provider's reset instant, while the
  saved-reset scans, which require exhaustion-only account reasons, can never
  read it as a weekly exhaustion.
  """

  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.CandidateEligibility.Quota
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows

  @reason_codes ["exhausted", "provider_denied"]

  @spec filter_candidates(FilterInput.t(), [FilterInput.candidate()], map() | nil, RouteState.t()) ::
          {:ok, [FilterInput.candidate()]} | {:error, map()}
  def filter_candidates(%FilterInput{} = input, candidates, _quota_decision, %RouteState{} = route_state)
      when is_list(candidates) do
    {kept, exclusions} = Enum.reduce(candidates, {[], []}, &classify_candidate(&1, &2, input.model, route_state))

    case {Enum.reverse(kept), Enum.reverse(exclusions)} do
      {[], [_ | _] = exclusions} -> Quota.quota_unavailable_error(input, quota_dropped_exclusions(input, candidates, route_state) ++ exclusions, false)
      {kept, _exclusions} -> {:ok, kept}
    end
  end

  @doc """
  The exclusion this filter would record for `candidate` under
  `route_state`'s quota snapshot, or `nil` when its account is not denied.
  """
  @spec candidate_exclusion(FilterInput.candidate(), RouteState.t()) :: map() | nil
  def candidate_exclusion({assignment, identity}, %RouteState{} = route_state) do
    case QuotaWindows.routing_account_denial(Map.get(route_state.quota_snapshots, identity.id)) do
      nil -> nil
      denial -> exclusion(assignment, identity, denial)
    end
  end

  # The refusal answers for the whole Pool, so it also names the candidates
  # quota eligibility already dropped: their resets bound the Pool's earliest
  # return as much as the denied ones do, for example a sibling exhausted
  # until a reset earlier than the denied account's (findings#206 row 206-508).
  defp quota_dropped_exclusions(%FilterInput{candidates: before_quota, model: model}, candidates, route_state) do
    kept_ids = MapSet.new(candidates, fn {assignment, _identity} -> assignment.id end)
    dropped = Enum.reject(before_quota, fn {assignment, _identity} -> MapSet.member?(kept_ids, assignment.id) end)
    Quota.candidate_exclusions(model, dropped, route_state)
  end

  defp classify_candidate({assignment, identity} = candidate, {kept, exclusions}, model, %RouteState{} = route_state) do
    case QuotaWindows.routing_account_denial(Map.get(route_state.quota_snapshots, identity.id)) do
      nil ->
        {[candidate | kept], exclusions}

      denial ->
        if bound_probe_candidate?(candidate, route_state.reset_probe) or Quota.reset_probe_candidate?(model, candidate, route_state) do
          {[candidate | kept], exclusions}
        else
          {kept, [exclusion(assignment, identity, denial) | exclusions]}
        end
    end
  end

  defp bound_probe_candidate?({assignment, identity}, %ResetProbe{} = probe),
    do: ResetProbe.bound?(probe) and probe.pool_upstream_assignment_id == assignment.id and probe.upstream_identity_id == identity.id

  defp bound_probe_candidate?(_candidate, _probe), do: false

  defp exclusion(assignment, identity, denial) do
    %{
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: identity.id,
      reasons: [
        %{
          "code" => "quota_window_unusable",
          "message" => "the provider refused this account until its reset time",
          "reason_codes" => @reason_codes,
          "quota_key" => "account",
          "quota_scope" => "account",
          "quota_family" => "account",
          "source" => denial.source,
          "rate_limit_reached_type" => denial.reached_type,
          "reset_at" => iso8601_or_nil(denial.reset_at),
          "hint_reset_at" => iso8601_or_nil(Map.get(denial, :hint_reset_at))
        }
      ]
    }
  end

  defp iso8601_or_nil(%DateTime{} = datetime), do: DateTime.to_iso8601(datetime)
  defp iso8601_or_nil(_datetime), do: nil
end
