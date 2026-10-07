defmodule CodexPooler.Upstreams.Quota.CapacityAssessment do
  @moduledoc """
  Physical capacity assessment, independent of the operator credit policy.

  Each authority keeps its provenance. In particular, a balance, catalog entry,
  successful response or reset lifecycle is not included quota evidence.
  """

  alias CodexPooler.Quotas.CapacityFacts
  alias CodexPooler.Upstreams.Quota.{CapacityFactsStore, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows.AccountDenial
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.SavedResets.ProbeLease
  alias CodexPooler.Upstreams.SavedResets.RedemptionLifecycle

  @pinned_reset_tolerance_seconds 5

  @type capacity_basis :: :included_window | :ordinary_provider_permission | :model_allowance | :windowless_provider_permission | :recovered_included | :provider_credits | :unknown_legacy | :none
  @type request_context :: keyword() | map()
  @type assessment :: %{
          eligible?: boolean(),
          capacity_basis: capacity_basis(),
          reason_codes: [String.t()],
          eligibility: map(),
          credit_available?: boolean(),
          credit_ambiguous?: boolean()
        }

  @spec assess(RoutingQuotaSnapshot.t(), request_context()) :: assessment()
  def assess(%RoutingQuotaSnapshot{} = snapshot, context \\ []) do
    opts = quota_options(context)
    included = Routing.included_only_eligibility_from_snapshot(snapshot, opts)
    raw = Routing.eligibility_from_snapshot(snapshot, opts)

    {eligible?, basis, reasons} = runtime_assessment(snapshot, physical_assessment(snapshot, included, raw, opts, context))

    %{eligible?: eligible?, capacity_basis: basis, reason_codes: reasons, eligibility: included, credit_available?: credit_usable?(snapshot, context), credit_ambiguous?: credit_ambiguous?(snapshot)}
  end

  defp physical_assessment(snapshot, included, raw, opts, context) do
    cond do
      hard_denial?(snapshot) -> {false, :none, ["provider_denied"]}
      verified_recovery_grace?(snapshot, included, opts, context) -> {true, :recovered_included, []}
      included.eligible? -> {true, eligible_basis(snapshot, included, opts), []}
      legacy_windowless?(snapshot, raw) -> {true, :unknown_legacy, []}
      true -> {false, :none, reason_codes(included)}
    end
  end

  defp runtime_assessment(snapshot, physical) do
    if is_nil(AccountDenial.active(snapshot)), do: physical, else: {false, :none, ["provider_denied"]}
  end

  @doc "Physical included capacity for reset-pressure consumers, without applying runtime denial policy."
  @spec physical_non_credit_assessment(RoutingQuotaSnapshot.t(), request_context()) :: assessment()
  def physical_non_credit_assessment(snapshot, context \\ []) do
    opts = quota_options(context)
    included = Routing.included_only_eligibility_from_snapshot(reset_capacity_snapshot(snapshot), opts)
    raw = Routing.eligibility_from_snapshot(snapshot, opts)
    {eligible?, basis, reasons} = physical_assessment(snapshot, included, raw, opts, context)
    %{eligible?: eligible?, capacity_basis: basis, reason_codes: reasons, eligibility: included, credit_available?: false, credit_ambiguous?: credit_ambiguous?(snapshot)}
  end

  defp reset_capacity_snapshot(snapshot),
    do: %{
      snapshot
      | raw_windows:
          Enum.map(snapshot.raw_windows, fn window ->
            metadata = Map.drop(window.metadata || %{}, ["rate_limit_reached_type", "rate_limit_error_code"])
            %{window | metadata: metadata}
          end)
    }

  defp verified_recovery_grace?(snapshot, included, opts, context),
    do:
      guarded_probe_exclusions?(included.exclusions) and weekly_probe_selection?(included.selection, snapshot.as_of) and
        is_nil(AccountDenial.active(snapshot)) and not retained_credit_blocker?(snapshot) and not later_scoped_denial?(snapshot, opts) and
        ProbeLease.verified_grace?(snapshot, context)

  defp eligible_basis(snapshot, %{routing_state: :windowless_provider_available}, _opts),
    do: if(fresh_included?(snapshot), do: :windowless_provider_permission, else: :unknown_legacy)

  defp eligible_basis(snapshot, %{routing_state: :provider_available} = included, opts),
    do: if(model_allowance?(snapshot, included, opts), do: :model_allowance, else: :ordinary_provider_permission)

  defp eligible_basis(snapshot, _included, _opts), do: included_basis(snapshot)

  defp legacy_windowless?(snapshot, raw),
    do:
      is_nil(snapshot.capacity_facts) and not snapshot.capacity_facts_reported? and not snapshot.capacity_blocker_reported? and
        raw.eligible? and raw.routing_state == :windowless_provider_available

  @spec non_credit_usable?(RoutingQuotaSnapshot.t(), request_context()) :: boolean()
  def non_credit_usable?(%RoutingQuotaSnapshot{} = snapshot, context \\ []) do
    assessment = physical_non_credit_assessment(snapshot, context)
    assessment.eligible? and assessment.capacity_basis not in [:unknown_legacy, :none]
  end

  @spec credit_usable?(RoutingQuotaSnapshot.t(), request_context()) :: boolean()
  def credit_usable?(%RoutingQuotaSnapshot{} = snapshot, context \\ []) do
    facts = snapshot.capacity_facts
    opts = quota_options(context)

    with %CapacityFacts{credit_permission: :available, denial_category: category} <- facts,
         true <- category in [:none, :included_limit],
         true <- CapacityFactsStore.fresh?(facts, snapshot.credential_epoch, snapshot.as_of),
         true <- is_nil(AccountDenial.active_for_credits(snapshot)),
         false <- retained_credit_blocker?(snapshot),
         false <- later_scoped_denial?(snapshot, opts),
         true <- explained_account_windows?(snapshot, facts, opts) do
      true
    else
      _denied -> false
    end
  end

  @spec credit_ambiguous?(RoutingQuotaSnapshot.t()) :: boolean()
  def credit_ambiguous?(%RoutingQuotaSnapshot{} = snapshot) do
    not (CapacityFactsStore.fresh?(snapshot.capacity_facts, snapshot.credential_epoch, snapshot.as_of) and
           match?(%CapacityFacts{credit_permission: :unavailable}, snapshot.capacity_facts))
  end

  @doc "A guarded reset probe must be incapable of succeeding through provider credits, regardless of the local toggle."
  @spec guarded_probe_permitted?(RoutingQuotaSnapshot.t(), request_context()) :: boolean()
  def guarded_probe_permitted?(%RoutingQuotaSnapshot{} = snapshot, context \\ []) do
    assessment = assess(snapshot, context)

    not credit_ambiguous?(snapshot) and not hard_denial?(snapshot) and not retained_credit_blocker?(snapshot) and
      not later_scoped_denial?(snapshot, quota_options(context)) and
      (non_credit_assessment?(assessment) or
         (guarded_probe_exclusions?(assessment.eligibility.exclusions) and
            weekly_probe_selection?(assessment.eligibility.selection, snapshot.as_of)))
  end

  @doc "A pending guarded probe may override only the existing account-weekly exhaustion shape."
  @spec guarded_probe_exclusions?([map()]) :: boolean()
  def guarded_probe_exclusions?([_ | _] = reasons) do
    Enum.all?(reasons, fn reason ->
      is_map(reason) and reason_field(reason, :quota_key) == "account" and
        reason_field(reason, :quota_scope) == "account" and
        reason_field(reason, :quota_family) == "account" and
        ((reason_field(reason, :window_kind) == "secondary" and
            reason_field(reason, :code) == "quota_weekly_exhausted") or
           (is_nil(reason_field(reason, :window_kind)) and
              reason_field(reason, :code) == "quota_window_unusable" and
              reason_field(reason, :reason_codes) == ["exhausted"]))
    end)
  end

  def guarded_probe_exclusions?(_reasons), do: false

  defp non_credit_assessment?(assessment),
    do:
      assessment.eligible? and assessment.capacity_basis in [:included_window, :ordinary_provider_permission, :recovered_included] and
        Enum.any?(assessment.eligibility.selection.routing_windows, &(&1.quota_scope == "account" and &1.window_kind == "secondary" and &1.window_minutes == 10_080))

  defp weekly_probe_selection?(selection, as_of) do
    selection.blocked_windows != [] and
      Enum.all?(selection.blocked_windows, fn window ->
        window.quota_key == "account" and window.quota_scope == "account" and
          window.quota_family == "account" and window.window_kind == "secondary" and
          window.window_minutes == 10_080 and Routing.window_reason_codes(window, as_of) == ["exhausted"]
      end)
  end

  defp reason_field(reason, key), do: Map.get(reason, key) || Map.get(reason, Atom.to_string(key))

  @spec fresh_included?(RoutingQuotaSnapshot.t()) :: boolean()
  def fresh_included?(%RoutingQuotaSnapshot{} = snapshot),
    do:
      CapacityFactsStore.fresh?(snapshot.capacity_facts, snapshot.credential_epoch, snapshot.as_of) and
        match?(%CapacityFacts{included_permission: :available}, snapshot.capacity_facts)

  @spec hard_denial?(RoutingQuotaSnapshot.t()) :: boolean()
  def hard_denial?(%RoutingQuotaSnapshot{} = snapshot) do
    snapshot.capacity_blockers_overflowed? or
      Enum.any?([snapshot.capacity_facts | snapshot.capacity_blockers], fn
        %CapacityFacts{denial_category: category} = facts when category in [:workspace_limit, :model_limit] ->
          CapacityFactsStore.current?(facts, snapshot.credential_epoch, snapshot.as_of) and not CapacityFactsStore.hard_denial_lapsed?(facts, snapshot.as_of)

        _other ->
          false
      end)
  end

  defp retained_credit_blocker?(snapshot),
    do: snapshot.capacity_blockers_overflowed? or Enum.any?(snapshot.capacity_blockers, &CapacityFactsStore.current?(&1, snapshot.credential_epoch, snapshot.as_of))

  @spec quota_options(request_context()) :: keyword()
  def quota_options(context) when is_list(context), do: context
  def quota_options(context) when is_map(context), do: Map.to_list(Map.take(context, [:model, :requested_model, :upstream_model, :upstream_model_id, :account_only]))

  @doc "The exact affected account descriptors used for contract qualification; no default weekly shape is invented."
  @spec credit_window_shape(RoutingQuotaSnapshot.t(), request_context()) :: [{String.t(), pos_integer()}]
  def credit_window_shape(%RoutingQuotaSnapshot{} = snapshot, context) do
    snapshot
    |> RoutingQuotaSnapshot.time_visible_raw_windows()
    |> Routing.included_only_windows()
    |> Routing.selection_data_from_windows(Keyword.put(quota_options(context), :at, snapshot.as_of))
    |> Map.fetch!(:blocked_windows)
    |> Enum.filter(&(&1.quota_scope == "account"))
    |> Enum.map(&{&1.window_kind, &1.window_minutes})
    |> Enum.uniq()
    |> Enum.sort()
  end

  @spec recovery_pending?(RoutingQuotaSnapshot.t()) :: boolean()
  def recovery_pending?(%RoutingQuotaSnapshot{redemption: redemption}),
    do: RedemptionLifecycle.phase(redemption) in [RedemptionLifecycle.consuming(), RedemptionLifecycle.consumed_pending_probe()]

  defp included_basis(%RoutingQuotaSnapshot{redemption: %{"phase" => "confirmed_by_quota"}}), do: :recovered_included
  defp included_basis(_snapshot), do: :included_window

  defp model_allowance?(snapshot, eligibility, opts) do
    requested = Keyword.get(opts, :upstream_model) || Keyword.get(opts, :upstream_model_id) || Keyword.get(opts, :model)

    requested == "gpt-5.3-codex-spark" and not is_nil(snapshot.availability) and
      snapshot.availability.state == :blocked and
      Enum.any?(eligibility.selection.routing_windows, &(&1.quota_scope != "account"))
  end

  defp later_scoped_denial?(snapshot, opts) do
    selected = Routing.selection_data_from_windows(RoutingQuotaSnapshot.time_visible_raw_windows(snapshot), Keyword.put(opts, :at, snapshot.as_of))
    facts = snapshot.capacity_facts

    Enum.any?(selected.routing_windows, fn window ->
      metadata = window.metadata || %{}
      model_blocked? = window.quota_scope != "account" and window in selected.blocked_windows
      later? = is_nil(facts) or DateTime.compare(window.observed_at, facts.observed_at) != :lt

      model_blocked? or
        (later? and
           explicit_window_denial?(window, metadata) and
           window.source != "codex_usage_api")
    end)
  end

  defp explicit_window_denial?(window, metadata),
    do:
      window.source == "codex_rate_limit_error" or not is_nil(metadata["rate_limit_reached_type"]) or
        metadata["rate_limit_allowed"] == false or metadata["rate_limit_reached"] == true

  defp explained_account_windows?(snapshot, facts, opts) do
    selected = snapshot |> RoutingQuotaSnapshot.time_visible_raw_windows() |> Routing.included_only_windows() |> Routing.selection_data_from_windows(Keyword.put(opts, :at, snapshot.as_of))

    Enum.all?(selected.blocked_windows, fn window ->
      window.quota_scope == "account" and
        Routing.window_reason_codes(%{window | credits: nil}, snapshot.as_of) == ["exhausted"] and
        Enum.any?(facts.account_windows, fn descriptor ->
          descriptor.window_kind == window.window_kind and descriptor.window_minutes == window.window_minutes and
            matching_credit_reset?(window, descriptor, facts)
        end)
    end)
  end

  defp matching_credit_reset?(window, descriptor, facts) do
    bounded? = abs(DateTime.diff(window.reset_at, descriptor.reset_at, :microsecond)) <= @pinned_reset_tolerance_seconds * 1_000_000
    same_receipt? = window.source == "codex_usage_api" and DateTime.compare(window.observed_at, facts.observed_at) == :eq and window.metadata["credential_epoch"] == facts.credential_epoch
    ordinary_runtime? = window.source in ["codex_response_headers", "codex_rate_limit_event"] and not explicit_window_denial?(window, window.metadata || %{})
    DateTime.compare(descriptor.reset_at, window.reset_at) == :eq or (bounded? and (same_receipt? or ordinary_runtime?))
  end

  defp reason_codes(eligibility),
    do: eligibility.exclusions |> Enum.flat_map(&Map.get(&1, :reason_codes, [])) |> Enum.uniq()
end
