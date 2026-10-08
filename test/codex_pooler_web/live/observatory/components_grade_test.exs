defmodule CodexPoolerWeb.Observatory.ComponentsGradeTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Observatory.Components.Telemetry

  test "the rate cards carry a tiered grade badge and the others do not" do
    fragment =
      %{
        success_rate: rate("99.7", %{label: "Excellent", tone: :success}),
        cache_rate: rate("20.0", %{label: "Low", tone: :neutral})
      }
      |> render_overview()

    assert LazyHTML.query(fragment, "#observatory-fact-success dt #observatory-success-grade[data-tone='success']") |> LazyHTML.text() =~ "Excellent"
    assert LazyHTML.query(fragment, "#observatory-fact-cache dt #observatory-cache-grade[data-tone='neutral']") |> LazyHTML.text() =~ "Low"
    assert LazyHTML.query(fragment, "#observatory-fact-cost [data-role='observatory-grade']") |> Enum.empty?()
    assert LazyHTML.query(fragment, "#observatory-fact-tokens [data-role='observatory-grade']") |> Enum.empty?()
  end

  test "an unavailable rate shows no grade" do
    fragment = render_overview(%{success_rate: rate("not available", nil), cache_rate: rate("not available", nil)})

    assert LazyHTML.query(fragment, "[data-role='observatory-grade']") |> Enum.empty?()
  end

  defp render_overview(rates) do
    html =
      render_component(&Telemetry.overview_strip/1, %{
        overview: Map.merge(%{cost: %{}, tokens: %{}}, rates)
      })

    LazyHTML.from_fragment(html)
  end

  defp rate(value, grade) do
    %{
      measure: %{value: value, unit: "%"},
      detail: "detail",
      trend: %{label: "not available", tone: :neutral, direction: :unavailable},
      grade: grade
    }
  end
end
