defmodule CodexPoolerWeb.Observatory.TelemetryModelDetailsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Observatory.Components.Telemetry
  alias CodexPoolerWeb.Observatory.Presentation

  test "overview keeps all four facts and expands to one row at the large breakpoint" do
    overview = Presentation.build(%{}).overview
    fragment = render_component(&Telemetry.overview_strip/1, overview: overview) |> LazyHTML.from_fragment()

    assert LazyHTML.query(fragment, "#observatory-overview-facts[class~='sm:grid-cols-2'][class~='lg:grid-cols-4']") |> Enum.count() == 1
    assert LazyHTML.query(fragment, "#observatory-overview-facts > .observatory-kpi") |> Enum.count() == 4

    for fact <- ["success", "cache", "cost", "tokens"] do
      assert LazyHTML.query(fragment, "#observatory-fact-#{fact} > dt") |> Enum.count() == 1
      assert LazyHTML.query(fragment, "#observatory-fact-#{fact} > .observatory-kpi-value-row") |> Enum.count() == 1
    end
  end

  test "all ranked model details use body ink while their bars retain the chart palette" do
    models =
      Presentation.build(%{
        models: Enum.map(1..6, &%{label: "model-#{&1}", total_tokens: 1_000, share_percent: 10, cost_micros: 10_000, request_count: 1})
      }).models

    fragment = render_component(&Telemetry.model_distribution/1, models: models) |> LazyHTML.from_fragment()

    for rank <- 1..6 do
      row = LazyHTML.query(fragment, "#observatory-model-#{rank}")

      assert LazyHTML.query(row, ".observatory-metric.text-base-content") |> Enum.count() == 2
      assert LazyHTML.query(row, ".observatory-metric[style]") |> Enum.empty?()
      assert LazyHTML.query(row, ".text-xs.text-base-content.tabular-nums") |> Enum.count() == 1
      assert LazyHTML.query(row, ".font-mono") |> Enum.empty?()
      assert LazyHTML.query(row, ".saved-reset-life-fill[style*='#{Enum.at(models, rank - 1).color}']") |> Enum.count() == 1
    end
  end
end
