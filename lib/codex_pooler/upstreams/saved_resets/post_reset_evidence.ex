defmodule CodexPooler.Upstreams.SavedResets.PostResetEvidence do
  @moduledoc """
  Decides whether fresh provider quota evidence confirms, reblocks, or leaves
  pending a redemption that already consumed a credit.

  This is the evidence gate for the self-healing convergence: a consumed reset
  stays `consumed_pending_probe` until the provider supplies *fresh, explicitly
  covered, parse-safe account evidence observed at or after the consume time*.

    * `:confirmed` — a fresh post-consume account window is usable; the identity
      recovered and normal evidence-based routing resumes.
    * `:reblocked` — a fresh post-consume account window is genuinely exhausted;
      the reset did not clear the block.
    * `:pending` — no qualifying fresh account evidence (the provider omitted or
      nulled the account window, or only stale/inferred evidence exists). Nothing
      transitions; the old exhausted row is preserved untouched.

  Because an omitted account descriptor leaves the previously stored window with
  its *old* `observed_at`, the `observed_at >= consumed_at` filter alone keeps
  that stale evidence from confirming — no fabricated quota, fail-closed.

  Classification evaluates the canonical effective window view
  (`Windows.effective_quota_windows/2`), the same fold routing reads: obsolete
  rows from a different source describing the same logical window cannot veto a
  newer usable observation, while genuinely distinct current account windows
  keep their fail-closed routing semantics. Only the *temporal* filter runs
  before the fold — a still-fresh pre-consume row can never win the
  logical-window ranking and eclipse the post-consume evidence it would then
  be filtered away from — while the account and parse-safety checks run after
  it, so the fold ranks exactly the rows routing ranks and an unparseable
  winner fails closed instead of being folded away.

  Pure: it never touches the repo and reuses the routing window classifiers so
  window measurement semantics stay aligned. Production callers supply the
  locked identity to `classify/4` so a current provider permission can attest
  usable capacity even when the measured percentage is 100%.
  """

  alias CodexPooler.Upstreams.Quota.AccountAvailabilityStore
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Quota.CapacityAssessment
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  @account_quota_key "account"
  # A window carrying "unknown" precision was not parsed into a trustworthy
  # descriptor; everything else (observed/authoritative/inferred) is explicit
  # enough — the descriptor was present in the provider payload.
  @unparseable_precision "unknown"

  @type classification :: :confirmed | :reblocked | :pending

  @doc """
  Classifies the account-level evidence for an identity that consumed a credit
  at `consumed_at`. `windows` are that identity's stored account quota windows.

  Fail-closed: a single fresh exhausted account window reblocks (it would still
  exclude the identity from routing), and confirmation requires every fresh
  account window to be usable.
  """
  @spec classify([AccountQuotaWindow.t()], DateTime.t(), DateTime.t()) :: classification()
  def classify(windows, %DateTime{} = consumed_at, %DateTime{} = now) when is_list(windows) do
    fresh_account_windows =
      windows
      |> Routing.included_only_windows()
      |> Enum.filter(&observed_at_or_after?(&1, consumed_at))
      |> Windows.effective_quota_windows(now)
      |> Enum.filter(fn window -> account_window?(window) and parse_safe?(window) end)

    cond do
      fresh_account_windows == [] -> :pending
      Enum.any?(fresh_account_windows, &exhausted?(&1, now)) -> :reblocked
      Enum.all?(fresh_account_windows, &Windows.usable_window?(&1, now)) -> :confirmed
      true -> :pending
    end
  end

  @doc "Classifies post-consume windows with the locked identity's provider permission."
  @spec classify(UpstreamIdentity.t(), [AccountQuotaWindow.t()], DateTime.t(), DateTime.t()) ::
          classification()
  def classify(%UpstreamIdentity{} = identity, windows, consumed_at, now) do
    fresh_windows =
      Enum.filter(windows, &(account_window?(&1) and observed_at_or_after?(&1, consumed_at)))

    ordinary = classify(fresh_windows, consumed_at, now)

    snapshot = RoutingQuotaSnapshot.from_identity(identity, fresh_windows, now)
    eligibility = Routing.included_only_eligibility_from_snapshot(snapshot)

    cond do
      ordinary == :pending or not matching_reset_resources?(identity, fresh_windows, now) ->
        :pending

      AccountAvailabilityStore.blocked?(snapshot.availability, snapshot.credential_epoch, now) ->
        :reblocked

      true ->
        classify_permission(ordinary, snapshot, eligibility, consumed_at)
    end
  end

  defp classify_permission(:reblocked, snapshot, %{routing_state: :provider_available}, consumed_at),
    do: if(included_permission_confirms?(snapshot, consumed_at), do: :confirmed, else: :pending)

  defp classify_permission(ordinary, _snapshot, _eligibility, _consumed_at), do: ordinary

  defp included_permission_confirms?(snapshot, consumed_at),
    do:
      CapacityAssessment.fresh_included?(snapshot) and not CapacityAssessment.credit_ambiguous?(snapshot) and
        DateTime.compare(snapshot.capacity_facts.observed_at, consumed_at) != :lt

  defp matching_reset_resources?(identity, windows, now) do
    expected = get_in(identity.metadata || %{}, ["saved_reset_redemption", "included_window_descriptors"])
    effective = Windows.effective_quota_windows(windows, now)

    case expected do
      [_ | _] = descriptors when length(descriptors) <= 2 ->
        Enum.all?(descriptors, &matching_reset_descriptor?(&1, effective))

      nil ->
        true

      _invalid ->
        false
    end
  end

  @spec matching_reset_descriptor?(term(), [AccountQuotaWindow.t()]) :: boolean()
  defp matching_reset_descriptor?(%{"window_kind" => kind, "window_minutes" => minutes} = descriptor, windows) do
    map_size(descriptor) == 2 and kind in ["primary", "secondary"] and minutes in [10_080, 43_200] and
      Enum.any?(windows, fn window ->
        window.window_kind == kind and window.window_minutes == minutes and
          window.quota_key == @account_quota_key and window.quota_scope == "account" and window.quota_family == "account" and parse_safe?(window)
      end)
  end

  defp matching_reset_descriptor?(_invalid, _windows), do: false

  defp account_window?(%AccountQuotaWindow{quota_key: @account_quota_key}), do: true
  defp account_window?(_window), do: false

  defp parse_safe?(%AccountQuotaWindow{source_precision: @unparseable_precision}), do: false
  defp parse_safe?(%AccountQuotaWindow{}), do: true

  defp observed_at_or_after?(
         %AccountQuotaWindow{observed_at: %DateTime{} = observed_at},
         consumed_at
       ),
       do: DateTime.compare(observed_at, consumed_at) != :lt

  defp observed_at_or_after?(_window, _consumed_at), do: false

  defp exhausted?(%AccountQuotaWindow{} = window, now),
    do: "exhausted" in Windows.routing_window_reason_codes(window, now)
end
