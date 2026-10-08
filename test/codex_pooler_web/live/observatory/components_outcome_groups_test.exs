defmodule CodexPoolerWeb.Observatory.ComponentsOutcomeGroupsTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Observatory.Components.{Activity, Telemetry}

  test "consecutive requests without a model and without tokens fold into one row" do
    outcomes = [
      outcome("Unknown model", "other", 0),
      outcome("Unknown model", "other", 0),
      outcome("Unknown model", "other", 0),
      outcome("alpha-model", "responses", 10),
      outcome("Unknown model", "other", 0)
    ]

    fragment = render_outcomes(outcomes)
    rows = LazyHTML.query(fragment, "[data-role='observatory-outcome-row']")

    assert Enum.count(rows) == 3
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> Enum.count() == 1
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> LazyHTML.text() =~ "3 requests"
    assert rows |> Enum.at(1) |> LazyHTML.text() =~ "alpha-model"
    assert rows |> Enum.at(2) |> LazyHTML.text() =~ "Unknown model"
  end

  test "requests with a different endpoint or with tokens are not folded" do
    outcomes = [
      outcome("Unknown model", "other", 0),
      outcome("Unknown model", "files", 0),
      outcome("Unknown model", "files", 5)
    ]

    fragment = render_outcomes(outcomes)

    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-row']") |> Enum.count() == 3
    assert LazyHTML.query(fragment, "[data-role='observatory-outcome-group']") |> Enum.empty?()
  end

  test "an unavailable trend renders no trend element" do
    html =
      render_component(&Telemetry.telemetry/1, %{
        overview: %{
          success_rate: %{
            measure: %{value: "90.0", unit: "%"},
            trend: %{label: "not available", tone: :neutral, direction: :unavailable}
          },
          cache_rate: %{
            measure: %{value: "25.0", unit: "%"},
            trend: %{label: "+10.0 pp", tone: :success, direction: :up}
          }
        },
        models: []
      })

    fragment = LazyHTML.from_fragment(html)

    assert LazyHTML.query(fragment, "#observatory-success-trend") |> Enum.empty?()
    assert LazyHTML.query(fragment, "#observatory-cache-trend[data-direction='up']") != []
    refute html =~ "not available"
  end

  defp render_outcomes(outcomes) do
    html =
      render_component(&Activity.activity/1, %{
        traffic: %{
          total_label: "0 tokens",
          chart: %{
            categories: "[]",
            series: "[]",
            units: "[]",
            value_kinds: "[]",
            yaxis: "[]",
            colors: "[]"
          },
          fallback: %{rows: [], total_label: "0 tokens"}
        },
        outcomes: outcomes,
        window: "7d"
      })

    LazyHTML.from_fragment(html)
  end

  defp outcome(model, endpoint, tokens) do
    %{
      timestamp: "Oct 02, 07:38:56",
      model: model,
      endpoint: endpoint,
      status: %{label: "Succeeded", tone: :success, data_status: "ok"},
      tokens: %{total: tokens, label: Integer.to_string(tokens)},
      cost: %{label: "-"}
    }
  end
end
