defmodule CodexPoolerWeb.Observatory.PresentationClientCancelledTest do
  use ExUnit.Case, async: true

  alias CodexPoolerWeb.Observatory.Presentation

  # findings#292: a request the client cancelled is neither a success nor a
  # failure, so the holder's success rate leaves it out of its base and names it.
  test "the success rate is taken over the requests the client did not cancel" do
    model = Presentation.build(projection(%{total: 12, succeeded: 9, failed: 1, in_progress: 0, client_cancelled: 2}))

    assert model.overview.success_rate.percent == 90.0
    assert model.overview.success_rate.measure == %{value: "90.0", unit: "%"}
    assert model.overview.success_rate.detail == "9 succeeded · 1 failed · 2 client cancelled"
    assert model.overview.tokens.detail == "12 requests"
  end

  test "a window where the client cancelled everything keeps the counts instead of scoring 0%" do
    model = Presentation.build(projection(%{total: 3, succeeded: 0, failed: 0, in_progress: 0, client_cancelled: 3}))

    assert model.overview.success_rate.percent == nil
    assert model.overview.success_rate.measure == %{value: "not available", unit: nil}
    assert model.overview.success_rate.detail == "0 succeeded · 0 failed · 3 client cancelled"
  end

  test "the class is a warning chip that never carries a failure reason" do
    for code <- [nil, "service_unavailable", "request_failed"] do
      assert [outcome] = Presentation.build(projection(%{total: 1}, [outcome("client_cancelled", code)])).outcomes
      assert outcome.status == %{data_status: "warn", tone: :warning, label: "Client cancelled"}
    end
  end

  test "the recorded cancelled status, which nothing writes, has no chip of its own" do
    assert [outcome] = Presentation.build(projection(%{total: 1}, [outcome("cancelled", nil)])).outcomes
    assert outcome.status == %{data_status: "neutral", tone: :neutral, label: "Unknown"}
  end

  defp projection(requests, outcomes \\ []) do
    %{totals: %{requests: requests}, accounting: %{status: "partial"}, outcomes: outcomes}
  end

  defp outcome(status, code) do
    %{timestamp: ~U[2026-07-17 11:59:00Z], model: "safe-model", endpoint_class: "responses", status: status, code: code}
  end
end
