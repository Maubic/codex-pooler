defmodule CodexPooler.Quotas.DiagnosticReasonCodesTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.DiagnosticReasonCodes

  test "keeps recognized reasons in first-occurrence order and drops unknown or malformed values" do
    reasons = ["provider_credit_window_mismatch", "private-provider-value", "exhausted", :exhausted, nil, %{"reason" => "exhausted"}, ["exhausted"], "provider_credit_window_mismatch", "not_fresh"]

    assert DiagnosticReasonCodes.sanitize(reasons) == ["provider_credit_window_mismatch", "exhausted", "not_fresh"]
  end

  test "caps distinct recognized reasons after filtering without spending the bound on duplicates" do
    first_twelve = ~w(provider_credit_window_mismatch exhausted not_fresh expired reset_missing unknown_unusable provider_denied provider_credits_disabled provider_credit_capacity_unverified provider_credit_permission_unavailable provider_credit_permission_denied provider_credit_evidence_not_current)
    reasons = ["private-provider-value" | List.duplicate("provider_credit_window_mismatch", 20)] ++ first_twelve ++ ["provider_credit_account_denied"]

    assert DiagnosticReasonCodes.sanitize(reasons) == first_twelve
  end

  test "non-list containers cannot supply diagnostic reasons" do
    for reasons <- [nil, "exhausted", :exhausted, %{"reason" => "exhausted"}, {"exhausted"}] do
      assert DiagnosticReasonCodes.sanitize(reasons) == []
    end
  end
end
