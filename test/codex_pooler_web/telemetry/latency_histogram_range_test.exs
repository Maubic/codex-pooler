defmodule CodexPoolerWeb.Telemetry.LatencyHistogramRangeTest do
  use ExUnit.Case, async: false

  alias CodexPoolerWeb.Telemetry, as: WebTelemetry

  @cases [
    {"phoenix.endpoint.stop.duration.seconds", :duration, :native, 300},
    {"phoenix.router_dispatch.stop.duration.seconds", :duration, :native, 300},
    {"codex_pooler.gateway.admission.dequeued_time.seconds", :queued_ms, :millisecond, 60},
    {"codex_pooler.gateway.admission.timeout_time.seconds", :queued_ms, :millisecond, 60},
    {"codex_pooler.admin.request_logs.reload.duration.seconds", :duration, :native, 60},
    {"codex_pooler.admin.stats.dashboard.build.duration.seconds", :duration, :native, 60},
    {"codex_pooler.repo.query.total_time.seconds", :total_time, :native, 60},
    {"codex_pooler.repo.query.query_time.seconds", :query_time, :native, 60},
    {"codex_pooler.repo.query.queue_time.seconds", :queue_time, :native, 60},
    {"codex_pooler.repo.query.decode_time.seconds", :decode_time, :native, 60}
  ]

  for {name, measurement, source_unit, ceiling} <- @cases do
    test "#{name} preserves slow observations and distinguishes its finite boundary from overflow" do
      metric = Enum.find(WebTelemetry.prometheus_metrics(), &(Enum.join(&1.name, ".") == unquote(name)))
      assert %Telemetry.Metrics.Distribution{unit: :second, keep: nil} = metric
      # The real definitions and conversions are used; only unrelated emitters
      # are excluded from this test's independent registry.
      owner = self()
      metric = %{metric | keep: fn _metadata -> self() == owner end}
      registry = :latency_histogram_range_test
      start_supervised!({TelemetryMetricsPrometheus.Core, metrics: [metric], name: registry, start_async: false})
      conn = %{Plug.Test.conn(:get, "/sample") | status: 200}
      metadata = %{conn: conn, route: "/sample", route_class: "proxy_http", transport: "http", source: "requests", query: "SELECT 1", stage: "executed", scope: "selected_pool", outcome: "ok", window: "1h"}
      values = [0.001, 5, 10, 30, unquote(ceiling), unquote(ceiling) + 1]

      converted =
        for seconds <- values do
          raw = System.convert_time_unit(round(seconds * 1_000_000), :microsecond, unquote(source_unit))
          measurements = %{unquote(measurement) => raw}
          :telemetry.execute(metric.event_name, measurements, metadata)
          metric.measurement.(measurements)
        end

      body = TelemetryMetricsPrometheus.Core.scrape(registry)
      prefix = String.replace(unquote(name), ".", "_")
      buckets = buckets(body, prefix)
      assert buckets["5"] == 2
      assert buckets["10"] == 3
      # Native time conversion can place nominal 30/60 seconds slightly above
      # the boundary. Preserve the real converted measurement without rounding.
      assert buckets["30"] == Enum.count(converted, &(&1 <= 30))
      assert buckets[Integer.to_string(unquote(ceiling))] == Enum.count(converted, &(&1 <= unquote(ceiling)))
      assert buckets["+Inf"] == 6
      assert sample(body, prefix <> "_count") == 6
      assert_in_delta sample(body, prefix <> "_sum"), Enum.sum(values), 0.00001
      assert TelemetryMetricsPrometheus.Core.scrape(registry) == body
    end
  end

  defp buckets(body, prefix) do
    body
    |> String.split("\n")
    |> Enum.filter(&String.starts_with?(&1, prefix <> "_bucket{"))
    |> Map.new(fn line ->
      [_, bound] = Regex.run(~r/le="([^"]+)"/, line)
      {bound, line |> String.split() |> List.last() |> number()}
    end)
  end

  defp sample(body, name) do
    line = Enum.find(String.split(body, "\n"), &(String.starts_with?(&1, name <> "{") or String.starts_with?(&1, name <> " ")))
    line |> String.split() |> List.last() |> number()
  end

  defp number(value) do
    {number, ""} = Float.parse(value)
    number
  end
end
