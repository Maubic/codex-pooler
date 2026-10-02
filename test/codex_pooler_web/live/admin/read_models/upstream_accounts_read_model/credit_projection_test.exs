defmodule CodexPoolerWeb.Admin.UpstreamAccountsReadModel.CreditProjectionTest do
  use ExUnit.Case, async: true

  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Quotas.Evidence.CodexParsers
  alias CodexPooler.Upstreams.Quota.{AccountQuotaWindow, CreditBalanceStore, RoutingQuotaSnapshot, WindowSelector}
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.QuotaProjection
  alias CodexPoolerWeb.DateTimeDisplay

  @now ~U[2026-09-01 12:00:00Z]

  test "fresh observed balance survives a source switch without changing included percentage" do
    usage = window("codex_usage_api", 29, DateTime.add(@now, -6), 0)
    event = window("codex_rate_limit_event", 30, @now, nil)
    assert [^event] = WindowSelector.logical_windows([usage, event], @now)

    for windows <- [[usage], [event], [event, usage]] do
      weekly = weekly_row(windows)
      assert weekly.count_label == nil
      assert weekly.percent_label == if(windows == [usage], do: "71%", else: "70%")
      assert QuotaProjection.provider_credits_summary(snapshot(windows, 0)).balance_label == "0"
    end
  end

  @tag credits_negative: true
  test "missing fresh balance cannot be reconstructed from a quota row" do
    for freshness <- ["fresh", "unknown", "stale"] do
      usage = %{window("codex_usage_api", 100, @now, 50) | active_limit: 100, freshness_state: freshness}
      weekly = weekly_row([usage])
      assert weekly.count_label == nil
      assert weekly.count_title == nil
      assert weekly.percent_label == "0%"
      assert weekly.percent_value == 0
      assert QuotaProjection.provider_credits_summary(snapshot([usage], nil)).balance_state == :unknown
    end
  end

  test "included exhaustion and observed finite baseline percentages remain separate" do
    for {balance, baseline, label} <- [
          {12_497, 12_500, "99.976"},
          {500, 1_000, "50.000"},
          {1_000, 1_000, "100.000"}
        ] do
      usage = %{window("codex_usage_api", 100, @now, balance) | active_limit: baseline}
      weekly = weekly_row([usage])
      summary = QuotaProjection.provider_credits_summary(snapshot([usage], balance))
      assert weekly.percent_label == "0%"
      assert weekly.percent_value == 0
      assert Decimal.to_string(summary.observed_percent, :normal) == label
      assert summary.observed_baseline_label
      assert summary.availability == :unknown
    end
  end

  test "opt-out retains observations while independent included quota keeps its percentage" do
    usage = %{window("codex_usage_api", 29, @now, 500) | active_limit: 1_000}
    summary = snapshot([usage], 500) |> Map.put(:allow_provider_credits, false) |> QuotaProjection.provider_credits_summary()
    assert summary.balance_label == "500"
    assert Decimal.equal?(summary.observed_percent, Decimal.new("50.000"))
    assert summary.availability == :disabled
    assert weekly_row([usage]).percent_label == "71%"
  end

  test "fractional exact balance takes precedence over its rounded display store" do
    payload = ProviderCreditsFixtures.usage_payload(:windowless_credit_only, now: @now, credits: :fractional)
    {:ok, parsed} = CodexParsers.parse_codex_usage_result(payload, @now)
    facts = %{parsed.capacity_facts | credential_epoch: 2}
    summary = snapshot([], 0) |> Map.put(:capacity_facts, facts) |> Map.put(:capacity_facts_reported?, true) |> QuotaProjection.provider_credits_summary()
    assert summary.balance_state == :finite
    assert summary.balance_label == "<1"
    assert summary.display_row?
    assert summary.observed_percent == nil
    assert summary.observed_baseline_label == nil
    assert summary.availability == :available
    assert summary.qualification == :provider_attested
    assert summary.reason_codes == []
    assert summary.availability_detail =~ "does not establish a credit debit"
  end

  test "balances truncate to integers without a unit suffix while the exact observation remains available" do
    for {balance, compact, exact} <- [
          {"62485.24098765", "62,485", "62,485.24098765"},
          {"0.00098765", "<1", "0.00098765"},
          {"0", "0", "0"}
        ] do
      payload = ProviderCreditsFixtures.usage_payload(:windowless_credit_only, now: @now, credits: :fractional)
      payload = put_in(payload, ["credits", "balance"], balance)
      {:ok, parsed} = CodexParsers.parse_codex_usage_result(payload, @now)
      facts = %{parsed.capacity_facts | credential_epoch: 2}
      summary = snapshot([], 0) |> Map.put(:capacity_facts, facts) |> Map.put(:capacity_facts_reported?, true) |> QuotaProjection.provider_credits_summary()

      assert summary.balance_label == compact
      assert summary.balance_exact_label == exact
      assert summary.observed_percent == nil
      assert summary.display_row? == (balance != "0")
    end
  end

  @tag credits_negative: true
  test "unlimited and unknown windowless balances invent neither percent nor baseline" do
    for credits <- [:unlimited, :unknown] do
      payload = ProviderCreditsFixtures.usage_payload(:windowless_credit_only, now: @now, credits: credits)
      {:ok, parsed} = CodexParsers.parse_codex_usage_result(payload, @now)
      facts = %{parsed.capacity_facts | credential_epoch: 2}
      summary = snapshot([], nil) |> Map.put(:capacity_facts, facts) |> Map.put(:capacity_facts_reported?, true) |> QuotaProjection.provider_credits_summary()
      assert summary.balance_state == if(credits == :unlimited, do: :unlimited, else: :unknown)
      assert summary.observed_percent == nil
      assert summary.observed_baseline_label == nil
      assert summary.display_row? == (credits == :unlimited)
    end
  end

  @tag credits_negative: true
  test "a newer omitted credit observation revokes authority but retains a fresh separate balance" do
    payload = ProviderCreditsFixtures.usage_payload(:windowless_unknown, now: @now)
    {:ok, parsed} = CodexParsers.parse_codex_usage_result(payload, @now)
    facts = %{parsed.capacity_facts | credential_epoch: 2}
    summary = snapshot([], 0) |> Map.put(:capacity_facts, facts) |> Map.put(:capacity_facts_reported?, true) |> QuotaProjection.provider_credits_summary()
    assert summary.balance_label == "0"
    assert summary.availability == :unknown
    assert summary.observed_percent == nil
  end

  defp weekly_row(windows) do
    QuotaProjection.quota_limit_rows(windows, DateTimeDisplay.preferences_for_user(nil), @now)
    |> Enum.find(&(&1.key == :weekly))
  end

  defp snapshot(windows, balance) do
    metadata = if is_nil(balance), do: %{}, else: CreditBalanceStore.transition(%{}, %{"credits" => %{"balance" => balance}}, @now, 2)
    metadata = Map.put(metadata, "credential_epoch", 2)
    RoutingQuotaSnapshot.from_identity(%UpstreamIdentity{id: Ecto.UUID.generate(), metadata: metadata}, windows, @now)
  end

  defp window(source, used, observed_at, credits) do
    %AccountQuotaWindow{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      source: source,
      source_precision: "observed",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new(used),
      credits: credits,
      observed_at: observed_at,
      last_sync_at: observed_at,
      freshness_state: "fresh",
      reset_at: DateTime.add(@now, 86_400),
      merge_precedence: 60,
      metadata: %{"credential_epoch" => 2}
    }
  end
end
