defmodule CodexPooler.Upstreams.Quota.Windows.AccountDenial do
  @moduledoc """
  Reads an account-level provider denial out of a routing quota snapshot.

  The provider names why it refused a request in `x-codex-rate-limit-reached-type`,
  which the header parser keeps on every window it records from that response.
  The four `workspace_*` values refuse the whole account, whatever model the
  request named and however far the reported windows are from 100%: the
  workspace ran out of credits or hit its own usage limit. `rate_limit_reached`
  alone is left out on purpose, because it also names an ordinary per-window
  limit whose percentage already says whether it is spent; but a window
  recorded from a provider usage-limit refusal (`rate_limit_error_code`
  `usage_limit_reached`/`usage_limit_exceeded`, set by the header parser when
  the refusing `429` or wrapped error named it) on an account or feature window
  denies the account too, whatever its reached type: the provider refused the account until its reset, and an
  exhausted-window reading alone could lose to a fresh `allowed` Usage API
  reading while the provider kept refusing (findings#206 row 206-594).

  Model-scoped usage refusals do not deny unrelated models. Explicit workspace
  markers still deny the account regardless of the window carrying them.

  A denial is in force from the observation that carried it until the earliest
  reset that observation reported, and it ends earlier when a later
  provider-attested `available` reading for the identity's current credential
  epoch is still fresh (the next usage poll after the workspace regains credits).
  Once that reading expires it no longer overrides an unexpired denial. An
  observation without a reset instant holds only while it is fresh.

  `active/1` retains the non-credit/reset denial contract. Credit-channel
  consumers use `active_for_credits/1`: newer current-epoch finite weekly WHAM
  authority can supersede an older ordinary weekly refusal, but never a workspace
  marker or a same-clock/newer refusal. Policy qualification still decides whether
  that physical credit channel can serve the exact request.
  """

  alias CodexPooler.Quotas.CapacityFacts
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Quota.AccountAvailabilityStore
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Quota.CapacityFactsStore
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot

  @usage_limit_refusal_codes ~w(usage_limit_reached usage_limit_exceeded)
  @account_denial_types ~w(
    workspace_owner_credits_depleted
    workspace_member_credits_depleted
    workspace_owner_usage_limit_reached
    workspace_member_usage_limit_reached
  )

  @type t :: %{
          reached_type: String.t(),
          observed_at: DateTime.t(),
          reset_at: DateTime.t() | nil,
          hint_reset_at: DateTime.t() | nil,
          source: String.t() | nil
        }

  @spec account_denial_types() :: [String.t()]
  def account_denial_types, do: @account_denial_types

  @spec active(RoutingQuotaSnapshot.t() | nil) :: t() | nil
  def active(%RoutingQuotaSnapshot{as_of: %DateTime{} = as_of} = snapshot) do
    windows = RoutingQuotaSnapshot.time_visible_raw_windows(snapshot)

    windows
    |> Enum.filter(&account_denial_window?/1)
    |> Enum.reject(&superseded?(&1, snapshot))
    |> latest_observation()
    |> in_force(as_of)
    |> put_hint_reset_at(windows, as_of)
  end

  def active(_snapshot), do: nil

  @doc "Credit-channel denial: only newer same-epoch WHAM weekly authority can supersede an older ordinary included-limit refusal."
  @spec active_for_credits(RoutingQuotaSnapshot.t() | nil) :: t() | nil
  def active_for_credits(%RoutingQuotaSnapshot{as_of: %DateTime{} = as_of} = snapshot) do
    windows = RoutingQuotaSnapshot.time_visible_raw_windows(snapshot)

    windows
    |> Enum.filter(&account_denial_window?/1)
    |> Enum.reject(&(superseded?(&1, snapshot) or ordinary_weekly_credit_superseded?(&1, snapshot)))
    |> latest_observation()
    |> in_force(as_of)
    |> put_hint_reset_at(windows, as_of)
  end

  def active_for_credits(_snapshot), do: nil

  defp ordinary_weekly_credit_superseded?(%AccountQuotaWindow{quota_scope: "account", quota_family: "account", window_kind: "secondary", window_minutes: 10_080, metadata: metadata, observed_at: denied_at, reset_at: %DateTime{} = reset_at}, %RoutingQuotaSnapshot{capacity_facts: %CapacityFacts{} = facts} = snapshot) do
    ordinary_refusal?(metadata) and observed_weekly_credits?(facts) and
      CapacityFactsStore.fresh?(facts, snapshot.credential_epoch, snapshot.as_of) and DateTime.compare(facts.observed_at, denied_at) == :gt and
      Enum.any?(facts.account_windows, &(&1.window_kind == "secondary" and &1.window_minutes == 10_080 and DateTime.compare(&1.reset_at, reset_at) == :eq))
  end

  defp ordinary_weekly_credit_superseded?(_window, _snapshot), do: false

  defp ordinary_refusal?(metadata), do: metadata["rate_limit_error_code"] in @usage_limit_refusal_codes and metadata["rate_limit_reached_type"] in [nil, "rate_limit_reached"]

  defp observed_weekly_credits?(facts),
    do: facts.source_kind == :wham_usage and facts.included_permission == :exhausted and facts.credit_permission == :available and facts.denial_category == :included_limit and facts.has_credits == true and facts.unlimited == false and CapacityFacts.positive_balance?(facts)

  defp account_denial_window?(%AccountQuotaWindow{quota_scope: scope, metadata: %{"rate_limit_error_code" => code}, observed_at: %DateTime{}})
       when scope in ["account", "feature"] and code in @usage_limit_refusal_codes,
       do: true

  defp account_denial_window?(%AccountQuotaWindow{metadata: %{"rate_limit_reached_type" => type}, observed_at: %DateTime{}}),
    do: type in @account_denial_types

  defp account_denial_window?(%AccountQuotaWindow{}), do: false

  # A later provider-attested `available` reading for the current credential
  # epoch is newer evidence about the same account while it remains fresh.
  defp superseded?(%AccountQuotaWindow{observed_at: denied_at}, %RoutingQuotaSnapshot{
         availability: %AccountAvailabilityStore.Snapshot{observed_at: available_at} = availability,
         credential_epoch: epoch,
         as_of: as_of
       }) do
    AccountAvailabilityStore.available?(availability, epoch, as_of) and
      DateTime.compare(available_at, denied_at) == :gt and DateTime.compare(available_at, as_of) != :gt
  end

  defp superseded?(%AccountQuotaWindow{}, %RoutingQuotaSnapshot{}), do: false

  defp latest_observation([]), do: []

  defp latest_observation(windows) do
    latest_at = windows |> Enum.map(& &1.observed_at) |> Enum.max(DateTime)
    Enum.filter(windows, &(DateTime.compare(&1.observed_at, latest_at) == :eq))
  end

  defp in_force([], _as_of), do: nil

  defp in_force([first | _rest] = windows, as_of) do
    reset_at =
      windows
      |> Enum.map(& &1.reset_at)
      |> Enum.filter(&match?(%DateTime{}, &1))
      |> Enum.min(DateTime, fn -> nil end)

    if in_force?(reset_at, first.observed_at, as_of) do
      %{
        reached_type: first.metadata["rate_limit_reached_type"],
        observed_at: first.observed_at,
        reset_at: reset_at,
        source: first.source
      }
    end
  end

  # The retry advice for a client refused because of the denial
  # (findings#206 rows 206-508, 206-522). The marker rides whichever rows the
  # refusing response refreshed: a refusal that refreshed only the weekly row
  # (96% used, carrying the workspace marker) names the next day as the
  # denial's own reset, while the block can clear with the 5-hour window
  # about an hour later. Which window the workspace block follows is not on
  # the wire, so the advice is the earliest future reset among the account's
  # fresh windows, never later than the denial's own: a hint too early costs
  # the client one more refused request, one too late keeps it away for
  # hours. A resetless denial gives no advice.
  defp put_hint_reset_at(nil, _windows, _as_of), do: nil
  defp put_hint_reset_at(%{reset_at: nil} = denial, _windows, _as_of), do: Map.put(denial, :hint_reset_at, nil)

  defp put_hint_reset_at(%{reset_at: %DateTime{} = reset_at} = denial, windows, as_of) do
    hint_reset_at =
      windows
      |> Enum.filter(&fresh_account_reset_ahead?(&1, as_of))
      |> Enum.map(& &1.reset_at)
      |> Enum.min(DateTime, fn -> reset_at end)
      |> then(&Enum.min([&1, reset_at], DateTime))

    Map.put(denial, :hint_reset_at, hint_reset_at)
  end

  defp fresh_account_reset_ahead?(%AccountQuotaWindow{quota_scope: "account", reset_at: %DateTime{} = reset_at} = window, as_of),
    do: DateTime.compare(reset_at, as_of) == :gt and Evidence.current_freshness_state(window, as_of) == "fresh"

  defp fresh_account_reset_ahead?(%AccountQuotaWindow{}, _as_of), do: false

  defp in_force?(%DateTime{} = reset_at, _observed_at, as_of), do: DateTime.compare(reset_at, as_of) == :gt

  defp in_force?(nil, observed_at, as_of),
    do: DateTime.diff(as_of, observed_at, :second) <= Evidence.freshness_ttl_seconds()
end
