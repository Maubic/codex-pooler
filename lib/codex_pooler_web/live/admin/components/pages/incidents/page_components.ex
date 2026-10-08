defmodule CodexPoolerWeb.Admin.IncidentsPageComponents do
  @moduledoc false

  use CodexPoolerWeb, :html

  alias CodexPoolerWeb.Admin.BadgeComponents, as: AdminBadges
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.Formatting, as: RelativeTime
  alias CodexPoolerWeb.DateTimeDisplay

  attr :page, :map, required: true

  def feed_chip(assigns) do
    ~H"""
    <span
      id="admin-incidents-feed-state"
      data-state={feed_state(@page)}
      title={feed_title(@page)}
      class={["inline-flex h-6 items-center gap-1.5 rounded-full border px-2.5 text-xs font-medium", feed_chip_class(feed_state(@page))]}
    >
      <span aria-hidden="true" class={["size-1.5 rounded-full", feed_dot_class(feed_state(@page))]}></span>
      {feed_state_label(@page)}<span :if={@page.last_success_at && feed_state(@page) in ["current", "stale", "error"]} class="font-normal text-base-content/55"> · {RelativeTime.relative_time_label(@page.last_success_at)}</span>
    </span>
    """
  end

  attr :page, :map, required: true
  attr :datetime_preferences, :map, required: true

  def incidents_content(assigns) do
    ~H"""
    <.notice
      :if={@page.polling_enabled? && @page.available? && @page.stale?}
      id="admin-incidents-stale"
      tone={:warning}
    >
      {stale_copy(@page.last_success_at, @datetime_preferences)}
    </.notice>
    <.notice
      :if={@page.polling_enabled? && @page.available? && @page.last_error_code}
      id="admin-incidents-feed-error"
      tone={:error}
    >
      The latest OpenAI status fetch failed. Showing the last known incident data.
    </.notice>
    <.notice :if={@page.polling_enabled? && !@page.available?} id="admin-incidents-feed-unavailable" tone={:warning}>
      The status feed is not available yet. The first successful refresh is still pending. The feed is checked automatically every five minutes.
    </.notice>
    <.notice :if={!@page.polling_enabled?} id="admin-incidents-feed-disabled" tone={:neutral}>
      Status polling is disabled in System settings. Retained incidents show the last known state.
    </.notice>

    <.incident_section
      id="admin-incidents-active-section"
      title="Active incidents"
      subtitle="Reported on the OpenAI status page and not yet resolved"
      count_id="admin-incidents-active-count"
      count={length(@page.active)}
      table_id="admin-incidents-active-table"
      rows={@page.active}
      datetime_preferences={@datetime_preferences}
      surface="active"
    >
      <:empty>
        <.empty_row
          id="admin-incidents-active-empty"
          icon={if @page.available?, do: "hero-check-circle", else: "hero-question-mark-circle"}
          tone={if @page.available?, do: :success, else: :neutral}
          title={if @page.available?, do: "No active incidents", else: "No incident data yet"}
          description={
            if @page.available?,
              do: "No active incidents in the last successful status refresh.",
              else: "A successful status refresh is needed before incident availability is known."
          }
        />
      </:empty>
    </.incident_section>

    <.incident_section
      id="admin-incidents-history-section"
      title="Incident history"
      subtitle="Resolved and retired incidents, newest first"
      count_id="admin-incidents-history-count"
      count={@page.history_total}
      table_id="admin-incidents-history-table"
      rows={@page.history}
      datetime_preferences={@datetime_preferences}
      surface="history"
    >
      <:empty>
        <.empty_row
          id="admin-incidents-history-empty"
          icon="hero-clock"
          tone={:neutral}
          title="No incident history"
          description="Resolved and retired incidents will appear here."
        />
      </:empty>
      <:footer :if={@page.history_overflow > 0}>
        <p id="admin-incidents-history-overflow" class="border-t border-base-300/60 px-4 py-2.5 text-xs text-base-content/60">
          +{@page.history_overflow} more historical incidents are retained.
        </p>
      </:footer>
    </.incident_section>
    """
  end

  attr :id, :string, required: true
  attr :tone, :atom, required: true
  slot :inner_block, required: true

  defp notice(assigns) do
    ~H"""
    <div id={@id} role="status" class={["flex items-start gap-3 rounded-box px-4 py-3 text-sm leading-5 text-base-content", notice_class(@tone)]}>
      <.icon name="hero-exclamation-triangle" class={["mt-0.5 size-4 shrink-0", notice_icon_class(@tone)]} />
      <p class="min-w-0">{render_slot(@inner_block)}</p>
    </div>
    """
  end

  defp notice_class(:warning), do: "bg-warning/5"
  defp notice_class(:error), do: "bg-error/5"
  defp notice_class(_tone), do: "bg-base-200"

  defp notice_icon_class(:warning), do: "text-warning"
  defp notice_icon_class(:error), do: "text-error"
  defp notice_icon_class(_tone), do: "text-base-content/50"

  attr :id, :string, required: true
  attr :title, :string, required: true
  attr :subtitle, :string, required: true
  attr :count_id, :string, required: true
  attr :count, :integer, required: true
  attr :table_id, :string, required: true
  attr :rows, :list, required: true
  attr :datetime_preferences, :map, required: true
  attr :surface, :string, required: true
  slot :empty, required: true
  slot :footer

  defp incident_section(assigns) do
    ~H"""
    <section id={@id} class="min-w-0 overflow-hidden rounded-box border border-base-300 bg-base-100">
      <header class="flex items-center justify-between gap-3 border-b border-base-300 bg-base-200/35 px-4 py-3">
        <div class="grid min-w-0 gap-0.5">
          <h2 class="text-base font-semibold leading-5 text-base-content">{@title}</h2>
          <p class="text-xs leading-5 text-base-content/60">{@subtitle}</p>
        </div>
        <span id={@count_id} class={AdminBadges.count_chip_class()}>{@count}</span>
      </header>
      <div :if={@rows == []}>{render_slot(@empty)}</div>
      <div :if={@rows != []} class="lg:overflow-x-auto">
        <table id={@table_id} class="admin-ledger-table table table-sm admin-log-table lg:min-w-[52rem]">
          <caption class="sr-only">{@title}</caption>
          <colgroup>
            <col />
            <col style="width: 8.5rem;" />
            <col style="width: 13rem;" />
            <col style="width: 6.5rem;" />
          </colgroup>
          <thead>
            <tr>
              <th class="whitespace-nowrap">Incident</th>
              <th class="whitespace-nowrap">Status</th>
              <th class="whitespace-nowrap">Last update</th>
              <th class="whitespace-nowrap">Source</th>
            </tr>
          </thead>
          <tbody>
            <.incident_row :for={row <- @rows} id={@table_id} row={row} surface={@surface} datetime_preferences={@datetime_preferences} />
          </tbody>
        </table>
      </div>
      {render_slot(@footer)}
    </section>
    """
  end

  attr :id, :string, required: true
  attr :icon, :string, required: true
  attr :tone, :atom, required: true
  attr :title, :string, required: true
  attr :description, :string, required: true

  defp empty_row(assigns) do
    ~H"""
    <div id={@id} class="flex items-center gap-3 px-4 py-4">
      <span aria-hidden="true" class={["grid size-8 shrink-0 place-items-center rounded-lg", if(@tone == :success, do: "bg-success/15 text-success", else: "bg-base-200 text-base-content/50")]}>
        <.icon name={@icon} class="size-4.5" />
      </span>
      <div class="grid min-w-0 gap-0.5">
        <p class="text-sm font-semibold leading-5 text-base-content">{@title}</p>
        <p class="text-xs leading-5 text-base-content/60">{@description}</p>
      </div>
    </div>
    """
  end

  attr :id, :string, required: true
  attr :row, :map, required: true
  attr :surface, :string, required: true
  attr :datetime_preferences, :map, required: true

  defp incident_row(assigns) do
    assigns =
      assign(assigns,
        summary: clean_summary(assigns.row.summary),
        components: component_chips(assigns.row.component)
      )

    ~H"""
    <tr id={"#{@id}-row-#{@row.id}"} data-role="openai-incident-row" data-incident-id={@row.id}>
      <td class="min-w-0 align-middle max-lg:col-span-2 max-lg:col-start-1 max-lg:row-start-1">
        <div class="grid min-w-0 gap-1">
          <p data-role="incident-title" class="line-clamp-2 text-[0.82rem] font-semibold leading-tight text-base-content">{@row.title}</p>
          <p :if={@summary} data-role="incident-summary" class="line-clamp-2 text-xs leading-4 text-base-content/55">{@summary}</p>
          <div :if={@components != []} data-role="incident-components" class="flex flex-wrap gap-1">
            <span :for={{name, state} <- @components} data-role="incident-component" class="inline-flex h-4.5 items-center whitespace-nowrap rounded-full border border-base-300 px-2 text-[10px] font-medium leading-none text-base-content/70">
              {name}<span :if={state} class="text-warning">&nbsp;· {state}</span>
            </span>
          </div>
        </div>
      </td>
      <td class="align-middle max-lg:col-start-3 max-lg:row-start-1 max-lg:justify-self-end">
        <.status_badge row={@row} surface={@surface} />
      </td>
      <td class="align-middle text-xs leading-5 text-base-content/70 max-lg:col-span-2 max-lg:col-start-1 max-lg:row-start-2 max-lg:mt-1">
        <div class="grid">
          <span data-role="incident-published-at" class="whitespace-nowrap" title={"First seen #{format_datetime(@row.first_seen_at, @datetime_preferences)}"}>
            {format_datetime(@row.published_at, @datetime_preferences)}
          </span>
          <span :if={@row.resolved_at} data-role="incident-resolved-at" class="whitespace-nowrap text-base-content/50">
            Resolved {format_datetime(@row.resolved_at, @datetime_preferences)}
          </span>
          <span :if={@row.retired_at} data-role="incident-retired-at" class="whitespace-nowrap text-base-content/50">
            Retired {format_datetime(@row.retired_at, @datetime_preferences)}
          </span>
        </div>
      </td>
      <td class="align-middle text-xs max-lg:col-start-3 max-lg:row-start-2 max-lg:mt-1 max-lg:justify-self-end">
        <a
          :if={@row.link}
          id={"#{@id}-source-#{@row.id}"}
          data-role="incident-source-link"
          href={@row.link}
          target="_blank"
          rel="noopener noreferrer"
          class="inline-flex items-center gap-1 whitespace-nowrap font-medium text-primary hover:underline focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-primary"
        >
          View <.icon name="hero-arrow-top-right-on-square" class="size-3.5" />
          <span class="sr-only"> source for {@row.title}</span>
        </a>
        <span :if={!@row.link} class="text-base-content/50">Unavailable</span>
      </td>
    </tr>
    """
  end

  attr :row, :map, required: true
  attr :surface, :string, required: true

  defp status_badge(assigns) do
    ~H"""
    <span
      id={"openai-incident-status-#{@surface}-#{@row.id}"}
      data-role="incident-status"
      data-status={@row.status_key}
      class={["inline-flex h-4.5 items-center gap-1 whitespace-nowrap rounded-full px-2 text-[10px] font-semibold uppercase leading-none tracking-[0.04em]", status_class(@row.status_key)]}
    >
      <.icon name={status_icon(@row.status_key)} class="size-3 shrink-0" />{@row.status}
    </span>
    """
  end

  defp status_icon(:investigating), do: "hero-clock"
  defp status_icon(:identified), do: "hero-exclamation-circle"
  defp status_icon(:monitoring), do: "hero-eye"
  defp status_icon(:resolved), do: "hero-check-circle"
  defp status_icon(:retired), do: "hero-archive-box"
  defp status_icon(_status), do: "hero-question-mark-circle"

  defp status_class(:resolved), do: "bg-success/15 text-success"
  defp status_class(:monitoring), do: "bg-info/15 text-info"
  defp status_class(:identified), do: "bg-warning/15 text-warning"
  defp status_class(:investigating), do: "bg-error/15 text-error"
  defp status_class(_status), do: "bg-base-300 text-base-content/70"

  # The feed's summary repeats what the row already shows: a "Status: <state>"
  # prefix (the badge) and an "Affected components ..." tail (the chips).
  defp clean_summary(nil), do: nil

  defp clean_summary(summary) do
    summary
    |> String.replace(~r/^Status:\s*(Resolved|Investigating|Identified|Monitoring|Unknown)\s*/i, "")
    |> String.replace(~r/\s*Affected components\b.*$/s, "")
    |> String.trim()
    |> case do
      "" -> nil
      text -> text
    end
  end

  # The feed joins components as "Name (State) Name (State)". An operational
  # component is only a name; any other state stays visible next to it.
  defp component_chips(component) when is_binary(component) do
    case Regex.scan(~r/\s*(.+?)\s*\(([^()]+)\)(?=\s|$)/, component) do
      [] ->
        [{component, nil}]

      matches ->
        for [_whole, name, state] <- matches do
          {String.trim(name), if(String.downcase(String.trim(state)) == "operational", do: nil, else: String.trim(state))}
        end
    end
  end

  defp component_chips(_component), do: []

  defp feed_state(%{polling_enabled?: false}), do: "disabled"
  defp feed_state(%{available?: false}), do: "unavailable"
  defp feed_state(%{last_error_code: code}) when is_binary(code), do: "error"
  defp feed_state(%{stale?: true}), do: "stale"
  defp feed_state(_), do: "current"

  defp feed_state_label(%{polling_enabled?: false}), do: "Polling disabled"
  defp feed_state_label(%{available?: false}), do: "Feed unavailable"
  defp feed_state_label(%{last_error_code: code}) when is_binary(code), do: "Last fetch failed"
  defp feed_state_label(%{stale?: true}), do: "Feed stale"
  defp feed_state_label(_), do: "Feed current"

  defp feed_title(%{last_success_at: %DateTime{} = timestamp}), do: "Last successful fetch: #{DateTime.to_iso8601(timestamp)}"
  defp feed_title(_page), do: "No successful fetch recorded yet"

  defp feed_chip_class("current"), do: "border-success/25 bg-success/5 text-base-content"
  defp feed_chip_class("stale"), do: "border-warning/30 bg-warning/5 text-base-content"
  defp feed_chip_class("error"), do: "border-error/30 bg-error/5 text-base-content"
  defp feed_chip_class(_state), do: "border-base-300 bg-base-200 text-base-content/70"

  defp feed_dot_class("current"), do: "bg-success"
  defp feed_dot_class("stale"), do: "bg-warning"
  defp feed_dot_class("error"), do: "bg-error"
  defp feed_dot_class(_state), do: "bg-base-content/35"

  defp format_datetime(nil, _preferences), do: "not recorded"

  defp format_datetime(datetime, preferences), do: DateTimeDisplay.format_datetime(datetime, preferences)

  defp stale_copy(nil, _preferences), do: "Status data may be out of date. No successful refresh has been recorded yet."

  defp stale_copy(timestamp, preferences), do: "Status data may be out of date. The last successful fetch was #{format_datetime(timestamp, preferences)}."
end
