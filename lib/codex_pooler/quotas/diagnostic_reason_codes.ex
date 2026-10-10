defmodule CodexPooler.Quotas.DiagnosticReasonCodes do
  @moduledoc """
  Bounded capacity diagnostic reasons shared by routing and operator projections.
  """

  @allowed ~w(exhausted not_fresh expired reset_missing unknown_unusable provider_denied provider_credits_disabled provider_credit_capacity_unverified provider_credit_permission_unavailable provider_credit_permission_denied provider_credit_evidence_not_current provider_credit_account_denied provider_credit_blocker_retained provider_credit_scoped_denial provider_credit_window_mismatch capacity_basis_unknown non_credit_capacity_unverified saved_reset_probe_pending saved_reset_recovery_unavailable)

  @spec sanitize(term()) :: [String.t()]
  def sanitize(reasons) when is_list(reasons) do
    reasons |> Enum.filter(&(&1 in @allowed)) |> Enum.uniq() |> Enum.take(12)
  end

  def sanitize(_reasons), do: []
end
