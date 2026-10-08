defmodule CodexPoolerWeb.Observatory.Components.Activity do
  @moduledoc false

  use CodexPoolerWeb, :html

  alias CodexPoolerWeb.Admin.BadgeComponents, as: AdminBadges
  alias CodexPoolerWeb.Observatory.Components.Section

  @max_outcomes 12

  attr :traffic, :map, required: true
  attr :outcomes, :list, required: true
  attr :window, :string, default: nil
  attr :traffic_mode, :atom, default: :interval, values: [:interval, :cumulative]

  def activity(assigns) do
    ~H"""
    <div id="observatory-activity" class="grid min-w-0 gap-8">
      <.traffic_panel traffic={@traffic} window={@window} traffic_mode={@traffic_mode} />
      <.outcomes_panel outcomes={@outcomes} />
    </div>
    """
  end

  attr :traffic, :map, required: true
  attr :window, :string, default: nil
  attr :traffic_mode, :atom, default: :interval, values: [:interval, :cumulative]

  def traffic_panel(assigns) do
    ~H"""
    <section
      id="observatory-traffic"
      class="grid min-w-0 gap-4"
      aria-labelledby="observatory-traffic-heading"
    >
      <Section.divider id="observatory-traffic-heading" label="Traffic" suffix={@window} />
      <div class="flex justify-end">
        <div
          id="observatory-traffic-mode-control"
          class="observatory-segmented-control"
          role="group"
          aria-label="Traffic chart mode"
        >
          <button
            :for={{label, mode} <- [{"Interval", "interval"}, {"Cumulative", "cumulative"}]}
            id={"observatory-traffic-mode-#{mode}"}
            type="button"
            class="observatory-segmented-button"
            data-chart-mode={mode}
            aria-controls="observatory-traffic-plot"
            aria-pressed={to_string(@traffic_mode == String.to_existing_atom(mode))}
            phx-click={
              JS.dispatch("chart:set-mode",
                to: "#observatory-traffic-plot",
                detail: %{mode: mode}
              )
            }
          >
            {label}
          </button>
        </div>
      </div>

      <div
        id="observatory-traffic-scroll"
        class="observatory-chart-scroll min-w-0 overflow-x-auto overscroll-x-contain"
        data-role="chart-scroll-region"
      >
        <div
          id="observatory-traffic-plot"
          class="observatory-chart admin-apex-bar-chart w-full"
          phx-hook="ApexTimeSeriesChart"
          phx-update="ignore"
          role="img"
          aria-labelledby="observatory-traffic-title"
          aria-describedby="observatory-traffic-desc observatory-traffic-mode-description"
          data-chart-categories={@traffic.chart.categories}
          data-chart-series={@traffic.chart.series}
          data-chart-unit="tokens"
          data-chart-units={@traffic.chart.units}
          data-chart-value-kinds={@traffic.chart.value_kinds}
          data-chart-yaxis={@traffic.chart.yaxis}
          data-chart-height="264"
          data-chart-colors={@traffic.chart.colors}
          data-chart-legend="always"
          data-chart-safe-tooltip="true"
          data-chart-stacked="true"
          data-chart-bar-radius="0"
          data-chart-zoom="false"
          data-chart-wheel-scroll="page"
          data-chart-mode-control="observatory-traffic-mode-control"
          data-chart-mode-description="observatory-traffic-mode-description"
        >
        </div>
      </div>

      <p id="observatory-traffic-title" class="sr-only">Traffic over time</p>
      <p id="observatory-traffic-desc" class="sr-only">
        Token activity by model, with cost, shown for each time bucket.
      </p>
      <p id="observatory-traffic-mode-description" class="sr-only" aria-live="polite">
        {if @traffic_mode == :cumulative do
          "Showing cumulative running totals through each time bucket."
        else
          "Showing interval values for each time bucket."
        end}
      </p>
      <ul
        id="observatory-traffic-interval-values"
        class="sr-only"
        data-chart-source="interval"
        aria-label="Underlying interval values for Traffic over time"
      >
        <li :for={row <- @traffic.fallback.rows}>
          {row.label}: {row.total_label}, {row.cost_label}
        </li>
      </ul>
      <div id="observatory-traffic-table-fallback" class="sr-only">
        <table>
          <caption>Traffic by time bucket</caption>
          <thead>
            <tr>
              <th scope="col">Time</th>
              <th scope="col">Tokens</th>
              <th scope="col">Cost</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={row <- @traffic.fallback.rows}>
              <th scope="row">{row.label}</th>
              <td>{row.total_label}</td>
              <td>{row.cost_label}</td>
            </tr>
          </tbody>
        </table>
        <p id="observatory-traffic-fallback-total">Total: {@traffic.fallback.total_label}</p>
      </div>
    </section>
    """
  end

  attr :outcomes, :list, required: true

  def outcomes_panel(assigns) do
    assigns = assign(assigns, :visible_outcomes, assigns.outcomes |> List.wrap() |> group_unattributed() |> Enum.take(@max_outcomes))

    ~H"""
    <section
      id="observatory-outcomes"
      class="grid min-w-0 gap-4"
      aria-labelledby="observatory-outcomes-heading"
    >
      <Section.divider id="observatory-outcomes-heading" label="Recent outcomes" />
      <div id="observatory-outcomes-scroll" class="overflow-x-auto">
        <table id="observatory-outcomes-table" class="table table-sm min-w-160">
          <caption class="sr-only">Recent request outcomes</caption>
          <thead>
            <tr>
              <th scope="col">Time</th>
              <th scope="col">Model</th>
              <th scope="col">Endpoint</th>
              <th scope="col" class="text-center">Status</th>
              <th scope="col" class="text-right">Tokens</th>
              <th scope="col" class="text-right">Cost</th>
            </tr>
          </thead>
          <tbody>
            <tr
              :for={outcome <- @visible_outcomes}
              data-role="observatory-outcome-row"
              data-status={status_data_status(outcome.status.data_status)}
              class="align-middle"
            >
              <td
                data-label="Time"
                class="whitespace-nowrap text-xs tabular-nums text-base-content/55"
              >
                {outcome.timestamp}
              </td>
              <th
                :if={!Map.has_key?(outcome, :count)}
                scope="row"
                data-label="Model"
                class="max-w-48 font-normal"
              >
                <div class="grid min-w-0 gap-1">
                  <span class="flex min-w-0 items-baseline gap-1 text-sm leading-4">
                    <span data-role="outcome-model" class="min-w-0 truncate font-semibold">{outcome.model}</span>
                    <span
                      :if={Map.get(outcome, :effort)}
                      data-role="outcome-effort"
                      class="shrink-0 text-xs text-base-content/55"
                    >· {outcome.effort}</span>
                  </span>
                  <span
                    :if={Map.get(outcome, :speed_level)}
                    data-role="outcome-speed"
                    data-speed-level={outcome.speed_level}
                    class="inline-flex items-center gap-px text-warning"
                    title={speed_title(outcome.speed_level)}
                  >
                    <.icon
                      :for={n <- 1..3}
                      name={if n <= outcome.speed_level, do: "hero-bolt-solid", else: "hero-bolt"}
                      class={["size-3", n > outcome.speed_level && "opacity-30"]}
                    />
                    <span class="sr-only">{speed_title(outcome.speed_level)}</span>
                  </span>
                </div>
              </th>
              <th
                :if={Map.has_key?(outcome, :count)}
                scope="row"
                data-label="Model"
                data-role="observatory-outcome-group"
                class="max-w-56 truncate font-normal text-base-content/55"
              >
                — no model · {outcome.count} requests
              </th>
              <td data-label="Endpoint" class="text-base-content/60">{outcome.endpoint}</td>
              <td data-label="Status" class="text-center">
                <span
                  class={[
                    AdminBadges.status_chip_class(status_for_tone(outcome.status.tone)),
                    "observatory-metadata-chip whitespace-nowrap !px-2 !py-0.5"
                  ]}
                  data-role="outcome-status"
                  data-status={status_data_status(outcome.status.data_status)}
                  role="status"
                >
                  {outcome.status.label}
                </span>
              </td>
              <td data-label="Tokens" class="whitespace-nowrap text-right tabular-nums">
                <div class="grid justify-items-end gap-1">
                  <span class="flex items-center justify-end gap-2 text-sm leading-4">
                    <span
                      :if={composition(outcome)}
                      data-role="outcome-token-bar"
                      role="img"
                      aria-label={composition_title(outcome)}
                      title={composition_title(outcome)}
                      class="hidden h-2 w-20 overflow-hidden rounded-xs lg:flex"
                    >
                      <span
                        :for={segment <- composition(outcome)}
                        data-role={segment_role(segment.key)}
                        data-token-count={segment.count}
                        aria-hidden="true"
                        class={["h-full min-w-0 basis-0", segment_color(segment.key)]}
                        style={"flex-grow: #{segment.count}"}
                      ></span>
                    </span>
                    <span data-role="outcome-tokens" class="min-w-12 text-right">{outcome.tokens.label}</span>
                  </span>
                  <span
                    :if={Map.get(outcome.tokens, :cached_label)}
                    data-role="outcome-cached"
                    class="text-[11px] leading-4 text-base-content/55"
                  >
                    {outcome.tokens.cached_label}
                  </span>
                </div>
              </td>
              <td data-label="Cost" class="whitespace-nowrap text-right text-sm tabular-nums">
                <span data-role="outcome-cost">{outcome.cost.label}</span>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
    """
  end

  defp status_data_status("ok"), do: "ok"
  defp status_data_status("warn"), do: "warn"
  defp status_data_status("err"), do: "err"
  defp status_data_status(:ok), do: "ok"
  defp status_data_status(:warn), do: "warn"
  defp status_data_status(:err), do: "err"
  defp status_data_status(_status), do: "unknown"

  defp status_for_tone(:success), do: :succeeded
  defp status_for_tone(:warning), do: :disabled
  defp status_for_tone(:error), do: :failed
  defp status_for_tone(_tone), do: :unknown
  defp speed_title(3), do: "Ultrafast"
  defp speed_title(2), do: "Fast (priority tier)"
  defp speed_title(_level), do: "Normal speed"

  defp composition(outcome), do: Map.get(outcome.tokens, :composition)

  defp composition_title(outcome) do
    outcome
    |> composition()
    |> Enum.map_join("; ", &"#{&1.label}: #{&1.count}")
    |> then(&"#{outcome.tokens.label} total tokens — #{&1}. Output includes reasoning tokens.")
  end

  defp segment_role(:cached_input), do: "cached-token-bar"
  defp segment_role(:uncached_input), do: "uncached-token-bar"
  defp segment_role(:output), do: "output-token-bar"

  defp segment_color(:cached_input), do: "bg-info"
  defp segment_color(:uncached_input), do: "bg-info/25"
  defp segment_color(:output), do: "bg-success"

  # Requests that name no model and move no tokens (model listings and the
  # like) would otherwise fill the table; consecutive identical ones become
  # one row that shows the newest time and how many were folded in.
  defp group_unattributed(outcomes) do
    outcomes
    |> Enum.chunk_while(
      nil,
      fn outcome, acc ->
        cond do
          not unattributed?(outcome) -> flush(acc, [outcome])
          acc && same_group?(acc, outcome) -> {:cont, %{acc | count: acc.count + 1}}
          true -> flush(acc, [], start_group(outcome))
        end
      end,
      fn acc -> {:cont, List.wrap(acc), nil} end
    )
    |> List.flatten()
    |> Enum.map(fn
      %{count: 1} = outcome -> Map.delete(outcome, :count)
      outcome -> outcome
    end)
  end

  defp flush(nil, rows), do: {:cont, rows, nil}
  defp flush(acc, rows), do: {:cont, [acc | rows], nil}
  defp flush(nil, rows, next), do: {:cont, rows, next}
  defp flush(acc, rows, next), do: {:cont, [acc | rows], next}

  defp unattributed?(outcome), do: outcome.model == "Unknown model" and get_in(outcome, [:tokens, :total]) == 0

  defp same_group?(group, outcome), do: group.endpoint == outcome.endpoint and group.status == outcome.status

  defp start_group(outcome), do: Map.put(outcome, :count, 1)
end
