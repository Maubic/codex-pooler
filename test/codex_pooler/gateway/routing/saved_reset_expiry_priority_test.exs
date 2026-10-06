defmodule CodexPooler.Gateway.Routing.SavedResetExpiryPriorityTest do
  use ExUnit.Case, async: true
  alias CodexPooler.Gateway.Routing.SavedResetAutoRedeem.ExpiryPriority
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  test "orders only the supplied eligible exact pairs and preserves equal and unknown indices" do
    now = ~U[2026-10-05 10:00:00Z]
    later = pair(:later, "2026-10-06T10:00:00Z")
    earlier = pair(:earlier, "2026-10-05T11:00:00.000001Z")
    equal = pair(:equal, "2026-10-05T13:00:00.000001+02:00")
    unknown = {%{id: :unknown}, %UpstreamIdentity{metadata: %{}}}
    other_assignment = {%{id: :other_assignment}, elem(unknown, 1)}
    supplied = [{unknown, 0}, {later, 2}, {earlier, 3}, {equal, 4}, {other_assignment, 5}]
    assert [earlier, equal, later, unknown, other_assignment] == ExpiryPriority.order(supplied, now)
    assert [] == ExpiryPriority.order([], now)
  end

  defp pair(id, expiry) do
    {%{id: id},
     %UpstreamIdentity{
       metadata: %{
         "saved_resets" => %{
           "status" => "reported",
           "available_count" => 1,
           "expires_detail_status" => "authoritative_rows",
           "expires_observed_at" => "2026-10-05T10:00:00Z",
           "available_expires_at" => [expiry]
         }
       }
     }}
  end
end
