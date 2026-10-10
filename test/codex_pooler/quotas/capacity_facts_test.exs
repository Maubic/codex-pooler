defmodule CodexPooler.Quotas.CapacityFactsTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.CapacityFacts
  alias CodexPooler.Quotas.Evidence.CodexParsers

  @now ~U[2026-10-01 12:00:00Z]

  test "one complete receipt distinguishes exhausted included permission from fractional credits" do
    facts = parse(%{"rate_limit" => %{"allowed" => false, "limit_reached" => true}, "credits" => %{"balance" => "0.12500", "has_credits" => true, "unlimited" => false}, "spend_control" => %{"reached" => false}})
    assert facts.included_permission == :exhausted
    assert facts.credit_permission == :available
    assert facts.balance == "0.125"
    assert CapacityFacts.positive_balance?(facts)
    assert facts.account_windows == []
  end

  test "spend limit preserves independent included permission" do
    facts = parse(%{"rate_limit" => %{"allowed" => true, "limit_reached" => false}, "credits" => %{"balance" => 5, "has_credits" => true, "unlimited" => false}, "spend_control" => %{"reached" => true}})
    assert facts.included_permission == :available
    assert facts.credit_permission == :unavailable
    assert facts.denial_category == :spend_limit
  end

  @tag credits_negative: true
  test "absent spend differs from explicit false and malformed spend grants nothing" do
    base = %{"rate_limit" => %{"allowed" => true, "limit_reached" => false}, "credits" => %{"balance" => "0.125", "has_credits" => true, "unlimited" => false}}
    assert parse(base).credit_permission == :unknown
    assert parse(Map.put(base, "spend_control", %{"reached" => false})).credit_permission == :available
    assert parse(Map.put(base, "spend_control", nil)) == parse(base)

    for spend <- [%{"reached" => "false"}, %{}] do
      facts = parse(Map.put(base, "spend_control", spend))
      assert facts.included_permission == :unknown
      assert facts.credit_permission == :unknown
      assert facts.denial_category == :malformed
    end
  end

  @tag credits_negative: true
  test "invalid amounts and contradictory flags never authorize provider credits" do
    for balance <- [-1, "-0.125", "NaN", "Infinity", "1e1000000", String.duplicate("9", 129), nil] do
      facts = parse(%{"credits" => %{"balance" => balance, "has_credits" => true, "unlimited" => false}, "spend_control" => %{"reached" => false}})
      refute facts.credit_permission == :available
      refute CapacityFacts.positive_balance?(facts)
    end

    facts = parse(%{"credits" => %{"balance" => 0, "has_credits" => true, "unlimited" => false}, "spend_control" => %{"reached" => false}})
    assert facts.denial_category == :malformed
  end

  test "unlimited is a distinct observation without a percentage or numeric balance" do
    facts = parse(%{"credits" => %{"has_credits" => false, "unlimited" => true}, "spend_control" => %{"reached" => false}})
    assert facts.credit_permission == :available
    assert facts.unlimited
    assert facts.balance == nil
    refute CapacityFacts.positive_balance?(facts)
  end

  @tag credits_negative: true
  test "workspace denial wins over positive resources and catalog descriptions grant nothing" do
    facts = parse(%{"rate_limit" => %{"allowed" => true, "limit_reached" => false}, "credits" => %{"balance" => 10, "has_credits" => true, "unlimited" => false}, "spend_control" => %{"reached" => false}, "rate_limit_reached_type" => %{"type" => "workspace_owner_usage_limit_reached"}})
    assert facts.denial_category == :workspace_limit
    refute facts.included_permission == :available
    refute facts.credit_permission == :available
    unknown = parse(%{"plan_type" => "future-plan", "normal_model_slug" => "gpt-reserve"})
    assert unknown.included_permission == :unknown
    assert unknown.credit_permission == :unknown
    refute CapacityFacts.authority_observed?(unknown)
  end

  @tag credits_negative: true
  test "the maximum included-window duration retains its exact descriptor" do
    facts = parse(window_payload(31_536_000))
    assert facts.included_permission == :available
    assert facts.credit_permission == :available
    assert facts.account_windows == [%{window_kind: "primary", window_minutes: 525_600, reset_at: DateTime.add(@now, 3_600), used_percent: "25"}]
  end

  for seconds <- [31_536_001, 63_072_000] do
    @tag credits_negative: true
    test "an external included-window duration of #{seconds} seconds revokes the whole receipt" do
      facts = parse(window_payload(unquote(seconds)))
      assert facts.included_permission == :unknown
      assert facts.credit_permission == :unknown
      assert facts.denial_category == :malformed
      assert facts.account_windows == []
      assert facts.balance == "0.125"
      refute CapacityFacts.authority_observed?(facts)
    end
  end

  @tag credits_negative: true
  test "an invalid account descriptor cannot demote an explicit workspace denial" do
    facts = window_payload(31_536_001) |> Map.put("rate_limit_reached_type", %{"type" => "workspace_owner_usage_limit_reached"}) |> parse()
    assert facts.included_permission == :unknown
    assert facts.credit_permission == :unavailable
    assert facts.denial_category == :workspace_limit
    assert facts.account_windows == []
  end

  for spend_reached <- [false, true] do
    @tag credits_negative: true
    test "ordinary allowed permission conflicts with an included reached type even when spend reached is #{spend_reached}" do
      facts = parse(%{"rate_limit" => %{"allowed" => true, "limit_reached" => false}, "rate_limit_reached_type" => %{"type" => "rate_limit_reached"}, "credits" => %{"balance" => "0.125", "has_credits" => true, "unlimited" => false}, "spend_control" => %{"reached" => unquote(spend_reached)}})
      assert facts.included_permission == :unknown
      assert facts.credit_permission == :unknown
      assert facts.denial_category == :malformed
      assert facts.balance == "0.125"
    end
  end

  for seconds <- [18_000, 604_800, 2_592_000] do
    @tag credits_negative: true
    test "raw second account window #{seconds} remains outside sole weekly credit authority" do
      payload = window_payload(604_800) |> put_in(["rate_limit", "allowed"], false) |> put_in(["rate_limit", "limit_reached"], true)
      extra = payload["rate_limit"]["primary_window"] |> Map.put("limit_window_seconds", unquote(seconds))
      payload = put_in(payload, ["rate_limit", "secondary_window"], extra)
      facts = parse(payload)
      refute facts.credit_permission == :available
      refute facts.included_permission == :available
    end
  end

  # The provider's current usage receipt key for key (synthetic values): a live
  # receipt carries every one of these keys, and the provider schema makes
  # `credits`, `credits.balance` and `spend_control` nullable.
  @captured_receipt %{
    "user_id" => "user-synthetic-0001",
    "account_id" => "00000000-0000-4000-8000-000000000001",
    "email" => "member@example.test",
    "plan_type" => "go",
    "rate_limit" => %{
      "allowed" => true,
      "limit_reached" => false,
      "primary_window" => %{"used_percent" => 37, "limit_window_seconds" => 2_592_000, "reset_after_seconds" => 3_600, "reset_at" => DateTime.to_unix(~U[2026-10-01 13:00:00Z])},
      "secondary_window" => nil
    },
    "code_review_rate_limit" => nil,
    "additional_rate_limits" => nil,
    "credits" => %{"has_credits" => true, "unlimited" => false, "overage_limit_reached" => false, "balance" => "125.5", "approx_local_messages" => [10, 20], "approx_cloud_messages" => [1, 2]},
    "spend_control" => %{"reached" => false, "individual_limit" => nil},
    "rate_limit_reached_type" => nil,
    "rate_limit_reset_credits" => %{"available_count" => 0, "applicable_available_count" => 0},
    "model_usage" => %{},
    "promo" => nil
  }

  test "a captured allowed receipt keeps its included permission when the provider nulls a nullable credit or spend field" do
    facts = parse(@captured_receipt)
    assert {facts.denial_category, facts.included_permission, facts.credit_permission} == {:none, :available, :available}
    assert [%{window_kind: "primary", window_minutes: 43_200, used_percent: "37"}] = facts.account_windows

    member = put_in(@captured_receipt, ["credits"], %{@captured_receipt["credits"] | "has_credits" => false, "balance" => nil})

    for receipt <- [member, Map.put(@captured_receipt, "spend_control", nil), Map.put(@captured_receipt, "credits", nil)] do
      facts = parse(receipt)
      assert {facts.denial_category, facts.included_permission, facts.credit_permission} == {:none, :available, :unknown}
      assert [%{window_kind: "primary", window_minutes: 43_200}] = facts.account_windows
    end

    for unlimited <- [false, true] do
      credits = %{member["credits"] | "unlimited" => unlimited}
      assert parse(put_in(member, ["credits"], credits)) == parse(put_in(member, ["credits"], Map.delete(credits, "balance")))
    end

    denied = member |> put_in(["rate_limit", "allowed"], false) |> put_in(["rate_limit", "limit_reached"], true) |> Map.put("rate_limit_reached_type", %{"type" => "workspace_member_usage_limit_reached"})
    facts = parse(denied)
    assert {facts.denial_category, facts.included_permission, facts.credit_permission} == {:workspace_limit, :unknown, :unavailable}
  end

  defp window_payload(seconds) do
    %{
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => %{"used_percent" => 25, "limit_window_seconds" => seconds, "reset_after_seconds" => 3_600, "reset_at" => DateTime.to_unix(DateTime.add(@now, 3_600))}
      },
      "credits" => %{"balance" => "0.125", "has_credits" => true, "unlimited" => false},
      "spend_control" => %{"reached" => false}
    }
  end

  defp parse(payload) do
    assert {:ok, %{capacity_facts: facts}} = CodexParsers.parse_codex_usage_result(payload, @now)
    facts
  end
end
