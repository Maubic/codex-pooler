defmodule CodexPooler.Accounting.PricingResolutionSettingsSnapshotTest do
  # The settings snapshot is taken from the client's payload when the request is reserved, before dispatch, so a
  # malformed `reasoning` must not raise there: the provider refuses it at validation and the client gets that 400
  # (findings#339). Only an object `reasoning` with a text effort, or a top-level text `reasoning_effort`, states one.
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.PricingResolution

  test "an object reasoning states its text effort" do
    assert %{reasoning_effort: "high"} = snapshot(%{"reasoning" => %{"effort" => " high "}})
    assert %{reasoning_effort: "low"} = snapshot(%{reasoning: %{effort: "low"}})
    assert %{reasoning_effort: "medium"} = snapshot(%{"reasoning_effort" => "medium"})
  end

  test "any other reasoning shape states no effort and does not raise" do
    for payload <- [
          %{"reasoning" => "high"},
          %{"reasoning" => ["high"]},
          %{"reasoning" => 7},
          %{"reasoning" => %{"effort" => %{"level" => "high"}}},
          %{"reasoning" => %{"effort" => ["high"]}},
          %{"reasoning_effort" => %{"level" => "high"}},
          %{"reasoning" => %{"summary" => "auto"}},
          %{}
        ] do
      assert %{reasoning_effort: nil} = snapshot(payload), inspect(payload)
    end
  end

  @pricing %{requested_service_tier: nil, actual_service_tier: nil, service_tier: nil}

  defp snapshot(payload), do: PricingResolution.request_settings_snapshot(payload, %{}, @pricing)
end
