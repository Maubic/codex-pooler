defmodule CodexPooler.Admin.UpstreamQuotaReadiness do
  @moduledoc """
  Shared admin projection for account-level upstream quota readiness.
  """

  alias CodexPooler.Quotas.WindowClassifier
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Quota
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows

  @account_quota_key "account"
  @account_quota_scope "account"

  @type window :: Quota.AccountQuotaWindow.t()
  @type tone :: :success | :warning | :error
  @type t :: %{
          required(:state) => String.t(),
          required(:label) => String.t(),
          required(:tone) => tone(),
          required(:routing_ready_now?) => boolean(),
          required(:reason_codes) => [String.t()],
          required(:primary_window) => window() | nil,
          required(:primary_30d_window) => window() | nil,
          required(:weekly_window) => window() | nil,
          optional(:capacity_basis) => CodexPooler.Upstreams.Quota.CapacityAssessment.capacity_basis(),
          optional(:qualification) => :established | :provider_attested | :supported | :unverified | :legacy_attested | :not_applicable,
          optional(:conditional?) => boolean(),
          optional(:included_quota_state) => String.t()
        }

  @spec from_windows([window()], DateTime.t()) :: t()
  def from_windows(windows, %DateTime{} = as_of) when is_list(windows) do
    account_windows = Enum.filter(windows, &account_window?/1)
    routing_account_windows = Enum.reject(account_windows, &usage_zero_capacity_primary_window?/1)

    eligibility =
      QuotaWindows.routing_quota_eligibility_from_windows(routing_account_windows, at: as_of)

    primary_window = get_in(eligibility, [:selection, :primary])
    primary_30d_window = primary_30d_window(primary_window)
    weekly_window = get_in(eligibility, [:selection, :secondary])
    reason_codes = reason_codes(eligibility, account_windows, as_of)

    state =
      readiness_state(
        routing_account_windows,
        eligibility,
        [primary_window, weekly_window],
        as_of
      )

    state
    |> state_projection()
    |> Map.merge(%{
      reason_codes: reason_codes,
      primary_window: primary_window,
      primary_30d_window: primary_30d_window,
      weekly_window: weekly_window
    })
  end

  @spec from_snapshot(RoutingQuotaSnapshot.t()) :: t()
  def from_snapshot(%RoutingQuotaSnapshot{} = snapshot) do
    decision = Upstreams.provider_credits_decision(snapshot, %{account_only: true})

    snapshot
    |> RoutingQuotaSnapshot.effective_windows()
    |> project_readiness(readiness_eligibility(snapshot, decision.eligibility), snapshot.as_of)
    |> with_capacity_decision(decision)
  end

  defp readiness_eligibility(snapshot, eligibility) do
    windows = RoutingQuotaSnapshot.effective_windows(snapshot)

    if Enum.any?(windows, &usage_zero_capacity_primary_window?/1) do
      windows |> Enum.filter(&account_window?/1) |> Enum.reject(&usage_zero_capacity_primary_window?/1) |> QuotaWindows.routing_quota_eligibility_from_windows(at: snapshot.as_of)
    else
      eligibility
    end
  end

  defp with_capacity_decision(readiness, decision) do
    projection = capacity_projection(readiness, decision)

    readiness
    |> Map.merge(projection)
    |> Map.merge(%{capacity_basis: decision.capacity_basis, qualification: decision.qualification.status, reason_codes: decision.reason_codes, conditional?: decision.capacity_basis == :unknown_legacy, included_quota_state: readiness.state})
  end

  defp capacity_projection(_readiness, %{eligible?: true, capacity_basis: :unknown_legacy}),
    do: %{state: "capacity_basis_unknown", label: "Conditional provider availability", tone: :warning, routing_ready_now?: true}

  defp capacity_projection(_readiness, %{eligible?: true, capacity_basis: :provider_credits}),
    do: %{state: "provider_credits_ready", label: "Routing ready via credits", tone: :success, routing_ready_now?: true}

  defp capacity_projection(readiness, %{eligible?: true}),
    do: Map.take(readiness, [:state, :label, :tone]) |> Map.put(:routing_ready_now?, true)

  defp capacity_projection(_readiness, decision) do
    cond do
      "saved_reset_probe_pending" in decision.reason_codes ->
        %{state: "saved_reset_probe_pending", label: "Banked-reset recovery pending", tone: :warning, routing_ready_now?: false}

      "provider_credits_disabled" in decision.reason_codes ->
        %{state: "provider_credits_disabled", label: "Provider credits disabled", tone: :warning, routing_ready_now?: false}

      decision.capacity_basis == :provider_credits ->
        %{state: "provider_credit_capacity_unverified", label: "Provider credit capacity unverified", tone: :warning, routing_ready_now?: false}

      true ->
        %{routing_ready_now?: false}
    end
  end

  defp project_readiness(windows, eligibility, as_of) do
    account_windows = Enum.filter(windows, &account_window?/1)
    routing_account_windows = Enum.reject(account_windows, &usage_zero_capacity_primary_window?/1)
    primary_window = get_in(eligibility, [:selection, :primary])
    primary_30d_window = primary_30d_window(primary_window)
    weekly_window = get_in(eligibility, [:selection, :secondary])
    reason_codes = reason_codes(eligibility, account_windows, as_of)

    state =
      readiness_state(
        routing_account_windows,
        eligibility,
        [primary_window, weekly_window],
        as_of
      )

    state
    |> state_projection()
    |> Map.merge(%{
      reason_codes: reason_codes,
      primary_window: primary_window,
      primary_30d_window: primary_30d_window,
      weekly_window: weekly_window
    })
  end

  @spec primary_30d_window(window() | nil) :: window() | nil
  defp primary_30d_window(%Quota.AccountQuotaWindow{} = window) do
    if WindowClassifier.monthly_primary?(window), do: window, else: nil
  end

  defp primary_30d_window(_window), do: nil

  @spec readiness_state([window()], map(), [window() | nil], DateTime.t()) :: String.t()
  defp readiness_state(
         [],
         %{routing_state: :windowless_provider_available},
         _selected_windows,
         _as_of
       ),
       do: "provider_available_no_windows"

  defp readiness_state([], %{exclusions: exclusions}, _selected_windows, _as_of) do
    if Enum.any?(exclusions, &("exhausted" in Map.get(&1, :reason_codes, []))),
      do: "blocked",
      else: "missing_evidence"
  end

  defp readiness_state(_account_windows, %{routing_state: state}, _selected_windows, _as_of)
       when state in [:precise, :provider_available],
       do: "ready"

  defp readiness_state(
         _account_windows,
         %{routing_state: :weekly_only_probe},
         _selected_windows,
         _as_of
       ),
       do: "weekly_only_probe"

  defp readiness_state(account_windows, eligibility, selected_windows, as_of) do
    cond do
      exhausted_quota?(account_windows, eligibility, as_of) ->
        "exhausted"

      stale_selected_window?(selected_windows, as_of) ->
        "stale"

      missing_evidence?(account_windows, eligibility, as_of) ->
        "missing_evidence"

      true ->
        "blocked"
    end
  end

  @spec state_projection(String.t()) :: t()
  defp state_projection("ready") do
    %{
      state: "ready",
      label: "Quota ready",
      tone: :success,
      routing_ready_now?: true,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  defp state_projection("weekly_only_probe") do
    %{
      state: "weekly_only_probe",
      label: "Weekly quota probe",
      tone: :warning,
      routing_ready_now?: true,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  defp state_projection("provider_available_no_windows") do
    %{
      state: "provider_available_no_windows",
      label: "Provider available",
      tone: :warning,
      routing_ready_now?: true,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  defp state_projection("exhausted") do
    %{
      state: "exhausted",
      label: "Quota exhausted",
      tone: :error,
      routing_ready_now?: false,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  defp state_projection("stale") do
    %{
      state: "stale",
      label: "Quota refresh needed",
      tone: :warning,
      routing_ready_now?: false,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  defp state_projection("missing_evidence") do
    %{
      state: "missing_evidence",
      label: "Quota missing",
      tone: :warning,
      routing_ready_now?: false,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  defp state_projection("blocked") do
    %{
      state: "blocked",
      label: "Quota blocked",
      tone: :warning,
      routing_ready_now?: false,
      reason_codes: [],
      primary_window: nil,
      primary_30d_window: nil,
      weekly_window: nil
    }
  end

  @spec exhausted_quota?([window()], map(), DateTime.t()) :: boolean()
  defp exhausted_quota?(account_windows, eligibility, as_of) do
    exclusion_code?(eligibility, "quota_weekly_exhausted") or
      account_window_reason?(account_windows, as_of, "exhausted")
  end

  @spec stale_selected_window?([window() | nil], DateTime.t()) :: boolean()
  defp stale_selected_window?(selected_windows, as_of) do
    selected_windows
    |> Enum.reject(&is_nil/1)
    |> Enum.any?(fn window ->
      reasons = QuotaWindows.routing_window_reason_codes(window, as_of)
      "not_fresh" in reasons or "expired" in reasons
    end)
  end

  @spec missing_evidence?([window()], map(), DateTime.t()) :: boolean()
  defp missing_evidence?(account_windows, eligibility, as_of) do
    exclusion_code?(eligibility, "quota_account_primary_missing") or
      exclusion_code?(eligibility, "quota_evidence_missing") or
      account_window_reason?(account_windows, as_of, "reset_missing")
  end

  @spec reason_codes(map(), [window()], DateTime.t()) :: [String.t()]
  defp reason_codes(%{eligible?: true, routing_state: :provider_available}, _windows, _as_of),
    do: []

  defp reason_codes(eligibility, account_windows, as_of) do
    exclusions = Map.get(eligibility, :exclusions, [])
    warnings = Map.get(eligibility, :warnings, [])

    exclusion_codes = Enum.map(exclusions, &Map.get(&1, :code))
    warning_codes = Enum.map(warnings, &Map.get(&1, :code))

    exclusion_reason_codes = Enum.flat_map(exclusions, &Map.get(&1, :reason_codes, []))

    window_reason_codes =
      account_windows
      |> Enum.flat_map(&QuotaWindows.routing_window_reason_codes(&1, as_of))
      |> Enum.reject(&(&1 == "unknown_unusable"))

    [exclusion_codes, exclusion_reason_codes, warning_codes, window_reason_codes]
    |> List.flatten()
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
  end

  @spec exclusion_code?(map(), String.t()) :: boolean()
  defp exclusion_code?(eligibility, code) do
    eligibility
    |> Map.get(:exclusions, [])
    |> Enum.any?(&(Map.get(&1, :code) == code))
  end

  @spec account_window_reason?([window()], DateTime.t(), String.t()) :: boolean()
  defp account_window_reason?(account_windows, as_of, reason) do
    Enum.any?(account_windows, fn window ->
      reason in QuotaWindows.routing_window_reason_codes(window, as_of)
    end)
  end

  @spec account_window?(term()) :: boolean()
  defp account_window?(%{} = window) do
    Map.get(window, :quota_key) == @account_quota_key and
      Map.get(window, :quota_scope) == @account_quota_scope
  end

  defp account_window?(_window), do: false

  defp usage_zero_capacity_primary_window?(
         %Quota.AccountQuotaWindow{
           source: "codex_usage_api",
           active_limit: active_limit,
           credits: credits,
           used_percent: %Decimal{} = used_percent
         } = window
       )
       when active_limit in [nil, 0] and credits in [nil, 0] do
    (WindowClassifier.primary_5h?(window) or WindowClassifier.monthly_primary?(window)) and
      Decimal.equal?(used_percent, Decimal.new(0)) and
      not match?(%{"rate_limit_allowed" => true, "rate_limit_reached" => false}, window.metadata)
  end

  defp usage_zero_capacity_primary_window?(%Quota.AccountQuotaWindow{}), do: false
end
