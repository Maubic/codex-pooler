defmodule CodexPooler.Gateway.Routing.SavedResetAutoRedeem.ExpiryPriority do
  @moduledoc false

  alias CodexPooler.Upstreams.SavedResets
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}

  @type candidate :: {PoolUpstreamAssignment.t() | map(), UpstreamIdentity.t() | map()}

  @spec order([{candidate(), non_neg_integer()}], DateTime.t()) :: [candidate()]
  def order(indexed_candidates, %DateTime{} = timestamp) do
    indexed_candidates
    |> Enum.sort_by(fn {{_assignment, identity}, index} ->
      case SavedResets.expiration_priority_hint(identity, timestamp) do
        {:known, expiry} -> {0, DateTime.to_unix(expiry, :microsecond), index}
        :unknown -> {1, 0, index}
      end
    end)
    |> Enum.map(fn {candidate, _index} -> candidate end)
  end
end
