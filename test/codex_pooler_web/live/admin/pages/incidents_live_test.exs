defmodule CodexPoolerWeb.Admin.IncidentsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  alias CodexPooler.OpenAIStatus
  alias CodexPooler.Repo
  alias CodexPooler.Status.Events
  alias CodexPooler.Status.Schemas.Incident
  alias CodexPooler.Status.Sync

  setup :register_and_log_in_user

  test "polling disabled is distinct from unavailable and stale", %{conn: conn} do
    settings = CodexPooler.InstanceSettings.ensure_singleton!()

    assert {:ok, _} =
             CodexPooler.InstanceSettings.update_system_settings(settings, %{
               "operator" => %{"openai_status_polling_enabled" => false}
             })

    {:ok, view, _} = live(conn, ~p"/admin/incidents")
    assert has_element?(view, "#admin-incidents-page-header #admin-incidents-feed-state[data-state='disabled']", "Polling disabled")
    assert has_element?(view, "#admin-incidents-feed-disabled[role='status']")
    refute has_element?(view, "#admin-incidents-feed-unavailable")
    refute has_element?(view, "#admin-incidents-stale")
    refute has_element?(view, "#admin-incidents-feed-error")
  end

  test "mount registers exactly one status subscription", %{conn: conn} do
    {:ok, view, _} = live(conn, ~p"/admin/incidents")

    entries =
      Registry.lookup(CodexPooler.PubSub, Events.topic())
      |> Enum.filter(fn {pid, _} -> pid == view.pid end)

    assert length(entries) == 1
  end

  test "real sync history is newest first beyond fifty rows", %{conn: conn} do
    now = ~U[2026-09-10 10:00:00.000000Z]

    items =
      for n <- 1..55 do
        %{
          guid: "history-sync-#{n}",
          title: "Historical incident #{n}",
          status: "Resolved",
          summary: "Service recovered",
          component: nil,
          link: "https://status.openai.com/incidents/sample-#{n}",
          published_at: DateTime.add(now, -n * 60, :second)
        }
      end

    assert {:ok, _} =
             Sync.sync(
               fetcher: fn _, _ -> {:ok, %{items: Enum.reverse(items)}} end,
               now: now
             )

    {:ok, view, _} = live(conn, ~p"/admin/incidents")

    rows =
      render(view)
      |> LazyHTML.from_fragment()
      |> LazyHTML.query("#admin-incidents-history-table tbody tr")

    assert Enum.count(rows) == 50
    assert rows |> Enum.at(0) |> LazyHTML.text() =~ "Historical incident 1"
    assert rows |> Enum.at(49) |> LazyHTML.text() =~ "Historical incident 50"
    assert has_element?(view, "#admin-incidents-history-overflow", "+5 more")
    assert has_element?(view, "#admin-incidents-history-table [data-role='incident-component']", "Unspecified")
  end

  test "a failed first poll remains unavailable and never claims no active incidents", %{
    conn: conn
  } do
    assert {:error, _} =
             Sync.sync(fetcher: fn _, _ -> {:error, %{code: :network_error}} end)

    {:ok, view, _} = live(conn, ~p"/admin/incidents")
    assert has_element?(view, "#admin-incidents-page-header #admin-incidents-feed-state[data-state='unavailable']", "Feed unavailable")
    assert has_element?(view, "#admin-incidents-feed-unavailable[role='status']")
    refute has_element?(view, "#admin-incidents-active-empty", "No active incidents")
  end

  test "redirects unauthenticated operators to login" do
    assert {:error, {:redirect, %{to: "/login"}}} = live(build_conn(), ~p"/admin/incidents")
  end

  test "renders an empty read-only incidents page for authenticated operators", %{conn: conn} do
    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-page")

    assert has_element?(
             view,
             "#admin-nav-incidents[href='/admin/incidents'][aria-current='page']"
           )

    assert has_element?(view, "#admin-incidents-active-section")
    assert has_element?(view, "#admin-incidents-history-section")
    assert has_element?(view, "#admin-incidents-active-empty")
    assert has_element?(view, "#admin-incidents-history-empty")
    refute has_element?(view, "#admin-incidents-active-table")
    refute has_element?(view, "#admin-incidents-history-table")
    assert has_element?(view, "#admin-incidents-feed-unavailable")

    assert has_element?(
             view,
             "#admin-incidents-feed-unavailable",
             "first successful refresh is still pending"
           )

    refute html =~ "last successful fetch was ."
    refute html =~ "Acknowledge"
    refute html =~ "Resolve incident"
    refute html =~ "Delete incident"
  end

  test "renders active and terminal incidents on one ledger table per list", %{conn: conn} do
    now = ~U[2026-09-10 12:00:00Z]
    active = incident_fixture("active-guid", "Investigating outage", "Investigating", now)

    resolved =
      incident_fixture(
        "resolved-guid",
        "Resolved outage",
        "Resolved",
        DateTime.add(now, -3600, :second)
      )

    retired =
      incident_fixture(
        "retired-guid",
        "Retired outage",
        "Monitoring",
        DateTime.add(now, -7200, :second)
      )

    Repo.update!(Incident.changeset(retired, %{retired_at: now, updated_at: now}))

    OpenAIStatus.upsert_feed_state(%{
      last_success_at: now,
      last_attempt_at: now,
      active_count: 1,
      aggregate_revision: 3,
      updated_at: now
    })

    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-active-table")
    assert has_element?(view, "#admin-incidents-history-table")
    refute has_element?(view, "#admin-incidents-active-empty")
    refute has_element?(view, "#admin-incidents-history-empty")

    for removed <- ["#admin-incidents-active-desktop", "#admin-incidents-active-mobile", "#admin-incidents-history-desktop", "#admin-incidents-history-mobile", "[data-role='openai-incident-card']"] do
      refute has_element?(view, removed)
    end

    assert has_element?(view, "#admin-incidents-active-count", "1")
    assert has_element?(view, "#admin-incidents-history-count", "2")
    assert has_element?(view, "#admin-incidents-active-table tbody tr", "Investigating outage")
    assert row_ids(view, "#admin-incidents-active-table tbody tr") == [active.id]
    assert Enum.sort(row_ids(view, "#admin-incidents-history-table tbody tr")) == Enum.sort([resolved.id, retired.id])

    assert has_element?(view, "#admin-incidents-active-table-row-#{active.id}[data-role='openai-incident-row'][data-incident-id='#{active.id}']")
    assert has_element?(view, "#admin-incidents-history-table-row-#{resolved.id}[data-role='openai-incident-row'] [data-role='incident-title']", "Resolved outage")
    assert has_element?(view, "#admin-incidents-history-table-row-#{retired.id}[data-role='openai-incident-row'] [data-role='incident-title']", "Retired outage")

    assert has_element?(view, "#admin-incidents-active-table-source-#{active.id}[data-role='incident-source-link'][href='https://status.openai.com/incidents/active-guid']")
    assert has_element?(view, "#admin-incidents-history-table-source-#{resolved.id}[data-role='incident-source-link'][href='https://status.openai.com/incidents/resolved-guid']")
    assert has_element?(view, "#admin-incidents-history-table-source-#{retired.id}[data-role='incident-source-link'][href='https://status.openai.com/incidents/retired-guid']")

    assert has_element?(view, "#openai-incident-status-active-#{active.id}[data-role='incident-status'][data-status='investigating']", "Investigating")
    assert has_element?(view, "#openai-incident-status-history-#{resolved.id}[data-role='incident-status'][data-status='resolved']", "Resolved")
    assert has_element?(view, "#openai-incident-status-history-#{retired.id}[data-role='incident-status'][data-status='retired']", "Retired")

    assert has_element?(view, "#admin-incidents-active-table-row-#{active.id} [data-role='incident-published-at']")
    refute has_element?(view, "#admin-incidents-active-table-row-#{active.id} [data-role='incident-resolved-at']")
    refute has_element?(view, "#admin-incidents-active-table-row-#{active.id} [data-role='incident-retired-at']")
    assert has_element?(view, "#admin-incidents-history-table-row-#{resolved.id} [data-role='incident-resolved-at']", "Resolved")
    refute has_element?(view, "#admin-incidents-history-table-row-#{resolved.id} [data-role='incident-retired-at']")
    assert has_element?(view, "#admin-incidents-history-table-row-#{retired.id} [data-role='incident-retired-at']", "Retired")

    assert has_element?(view, "#admin-incidents-active-table-row-#{active.id} [data-role='incident-summary']", "safe incident summary")
    assert has_element?(view, "#admin-incidents-active-table-row-#{active.id} [data-role='incident-component']", "Responses API")

    refute html =~ "admin-alerts-incident"
    refute html =~ "phx-click=\"acknowledge"
    refute html =~ "phx-click=\"resolve"
  end

  test "strips the feed's status prefix and affected components tail from the summary", %{conn: conn} do
    now = ~U[2026-09-10 12:00:00Z]

    incident =
      incident_fixture("clean-summary-guid", "Elevated errors", "Investigating", now,
        summary: "Status: Investigating We are investigating elevated error rates. Affected components Responses (Degraded performance) GPTs (Operational)",
        component: "Responses (Degraded performance) GPTs (Operational)"
      )

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    summary = "#admin-incidents-active-table-row-#{incident.id} [data-role='incident-summary']"
    assert has_element?(view, summary)
    assert element_text(view, summary) == "We are investigating elevated error rates."
    refute has_element?(view, summary, "Status:")
    refute has_element?(view, summary, "Affected components")
    refute has_element?(view, "#admin-incidents-active-table-row-#{incident.id}", "Status:")
    refute has_element?(view, "#admin-incidents-active-table-row-#{incident.id}", "Affected components")
  end

  test "renders no summary when the feed text is only status and component boilerplate", %{conn: conn} do
    now = ~U[2026-09-10 12:00:00Z]

    incident =
      incident_fixture("boilerplate-summary-guid", "Boilerplate only", "Resolved", now,
        summary: "Status: Resolved Affected components Responses (Operational) Codex (Operational)",
        component: "Responses (Operational) Codex (Operational)"
      )

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    row = "#admin-incidents-history-table-row-#{incident.id}"
    assert has_element?(view, row, "Boilerplate only")
    refute has_element?(view, "#{row} [data-role='incident-summary']")
    refute has_element?(view, row, "Affected components")
    assert component_chips(view, row) == ["Responses", "Codex"]
  end

  test "shows one chip per component and keeps a non-operational state next to its name", %{conn: conn} do
    now = ~U[2026-09-10 12:00:00Z]

    incident =
      incident_fixture("component-chips-guid", "Degraded responses", "Identified", now,
        summary: "Status: Identified We identified the cause. Affected components Responses (Degraded performance) GPTs (Operational)",
        component: "Responses (Degraded performance) GPTs (Operational)"
      )

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    row = "#admin-incidents-active-table-row-#{incident.id}"
    chips = component_chips(view, row)
    assert length(chips) == 2
    assert Enum.at(chips, 0) =~ "Responses"
    assert Enum.at(chips, 0) =~ "Degraded performance"
    assert Enum.at(chips, 1) == "GPTs"
    refute has_element?(view, "#{row} [data-role='incident-component']", "Operational")
    refute has_element?(view, "#{row} [data-role='incident-component']", "(")
  end

  test "renders no table or header for an empty history while active incidents exist", %{conn: conn} do
    now = ~U[2026-09-10 12:00:00Z]
    active = incident_fixture("only-active-guid", "Only active", "Investigating", now)

    OpenAIStatus.upsert_feed_state(%{last_success_at: now, last_attempt_at: now, active_count: 1, aggregate_revision: 1, updated_at: now})

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-active-table")
    assert has_element?(view, "#admin-incidents-active-table thead th")
    assert row_ids(view, "#admin-incidents-active-table tbody tr") == [active.id]
    assert has_element?(view, "#admin-incidents-active-count", "1")

    assert has_element?(view, "#admin-incidents-history-section #admin-incidents-history-empty", "No incident history")
    assert has_element?(view, "#admin-incidents-history-count", "0")
    refute has_element?(view, "#admin-incidents-history-table")
    refute has_element?(view, "#admin-incidents-history-section table")
    refute has_element?(view, "#admin-incidents-history-section thead")
    refute has_element?(view, "#admin-incidents-history-section th")
    refute has_element?(view, "#admin-incidents-history-section [data-role='openai-incident-row']")
    refute has_element?(view, "#admin-incidents-active-empty")
  end

  test "renders no table or header for empty active incidents while history exists", %{conn: conn} do
    now = ~U[2026-09-10 12:00:00Z]
    resolved = incident_fixture("only-history-guid", "Only history", "Resolved", now)

    OpenAIStatus.upsert_feed_state(%{last_success_at: now, last_attempt_at: now, active_count: 0, aggregate_revision: 1, updated_at: now})

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-history-table")
    assert has_element?(view, "#admin-incidents-history-table thead th")
    assert row_ids(view, "#admin-incidents-history-table tbody tr") == [resolved.id]
    assert has_element?(view, "#admin-incidents-history-count", "1")

    assert has_element?(view, "#admin-incidents-active-section #admin-incidents-active-empty", "No active incidents")
    assert has_element?(view, "#admin-incidents-active-count", "0")
    refute has_element?(view, "#admin-incidents-active-table")
    refute has_element?(view, "#admin-incidents-active-section table")
    refute has_element?(view, "#admin-incidents-active-section thead")
    refute has_element?(view, "#admin-incidents-active-section th")
    refute has_element?(view, "#admin-incidents-active-section [data-role='openai-incident-row']")
    refute has_element?(view, "#admin-incidents-history-empty")
  end

  test "the header chip reads a current feed without any notice", %{conn: conn} do
    now = DateTime.utc_now()

    OpenAIStatus.upsert_feed_state(%{last_success_at: now, last_attempt_at: now, active_count: 0, aggregate_revision: 1, updated_at: now})

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-page-header #admin-incidents-feed-state[data-state='current']", "Feed current")
    assert has_element?(view, "#admin-incidents-feed-state", "just now")

    for notice <- ["#admin-incidents-stale", "#admin-incidents-feed-error", "#admin-incidents-feed-unavailable", "#admin-incidents-feed-disabled"] do
      refute has_element?(view, notice)
    end

    assert has_element?(view, "#admin-incidents-active-empty", "No active incidents")
  end

  test "shows stale and failed feed state while preserving incident metadata", %{conn: conn} do
    now = DateTime.utc_now()
    stale = DateTime.add(now, -1_800, :second)
    incident_fixture("stale-guid", "Stale outage", "Unknown", stale)

    assert {:ok, _state} =
             OpenAIStatus.upsert_feed_state(%{
               last_success_at: stale,
               last_attempt_at: now,
               last_error_at: now,
               last_error_code: "network_error",
               active_count: 1,
               aggregate_revision: 1,
               updated_at: now
             })

    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-stale[role='status']")
    refute has_element?(view, "#admin-incidents-stale", "was .")
    assert has_element?(view, "#admin-incidents-feed-error[role='status']")
    assert has_element?(view, "#admin-incidents-page-header #admin-incidents-feed-state[data-state='error']", "Last fetch failed")
    refute has_element?(view, "#admin-incidents-feed-unavailable")
    refute has_element?(view, "#admin-incidents-feed-disabled")
    assert has_element?(view, "#admin-incidents-active-table", "Stale outage")
    refute html =~ "<rss"
    refute html =~ "<item"
    refute html =~ "network_error"
  end

  test "shows unavailable when no successful fetch exists", %{conn: conn} do
    now = DateTime.utc_now()

    assert {:ok, _state} =
             OpenAIStatus.upsert_feed_state(%{
               last_success_at: nil,
               last_attempt_at: now,
               active_count: 0,
               aggregate_revision: 1,
               updated_at: now
             })

    {:ok, view, html} = live(conn, ~p"/admin/incidents")

    assert has_element?(
             view,
             "#admin-incidents-feed-unavailable",
             "first successful refresh is still pending"
           )

    assert has_element?(view, "#admin-incidents-feed-state[data-state='unavailable']", "Feed unavailable")
    refute html =~ "last successful fetch was ."
  end

  test "caps visible history and reports overflow", %{conn: conn} do
    now = DateTime.utc_now()

    for index <- 1..51 do
      incident_fixture(
        "history-#{index}",
        "History #{index}",
        "Resolved",
        DateTime.add(now, -index, :second)
      )
    end

    OpenAIStatus.upsert_feed_state(%{
      last_success_at: now,
      active_count: 0,
      aggregate_revision: 1,
      updated_at: now
    })

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")

    assert has_element?(view, "#admin-incidents-history-overflow", "+1 more")

    assert 50 ==
             render(view)
             |> LazyHTML.from_fragment()
             |> LazyHTML.query("#admin-incidents-history-table tbody tr")
             |> Enum.count()
  end

  test "refreshes the page projection after a newer status event", %{conn: conn} do
    now = DateTime.utc_now()

    OpenAIStatus.upsert_feed_state(%{
      last_success_at: now,
      active_count: 0,
      aggregate_revision: 1,
      updated_at: now
    })

    {:ok, view, _html} = live(conn, ~p"/admin/incidents")
    refute has_element?(view, "[data-role='openai-incident-row']")

    incident_fixture("event-guid", "Event outage", "Investigating", now)

    assert :ok =
             Events.broadcast(%{
               event_version: 1,
               changed_count: 1,
               active_count: 1,
               aggregate_revision: 2,
               emitted_at: now
             })

    event = %{
      event_version: 1,
      changed_count: 1,
      active_count: 1,
      aggregate_revision: 2,
      emitted_at: now
    }

    assert {:ok, decoded_event} = Events.decode(event)
    send(view.pid, {:openai_status_updated, decoded_event})
    _ = render(view)
    assert has_element?(view, "[data-role='openai-incident-row']", "Event outage")
  end

  defp incident_fixture(guid, title, status, timestamp, opts \\ []) do
    {:ok, incident} =
      OpenAIStatus.upsert_incident(
        %{
          guid: guid,
          title: title,
          status: status,
          summary: Keyword.get(opts, :summary, "safe incident summary"),
          component: Keyword.get(opts, :component, "Responses API"),
          link: "https://status.openai.com/incidents/#{guid}",
          published_at: timestamp,
          content_hash: "hash-#{guid}"
        },
        timestamp
      )

    incident
  end

  defp row_ids(view, selector) do
    view
    |> render()
    |> LazyHTML.from_fragment()
    |> LazyHTML.query(selector)
    |> Enum.flat_map(&LazyHTML.attribute(&1, "data-incident-id"))
  end

  defp element_text(view, selector) do
    view |> element(selector) |> render() |> LazyHTML.from_fragment() |> LazyHTML.text() |> String.trim()
  end

  defp component_chips(view, row_selector) do
    view
    |> render()
    |> LazyHTML.from_fragment()
    |> LazyHTML.query("#{row_selector} [data-role='incident-component']")
    |> Enum.map(fn chip -> chip |> LazyHTML.text() |> String.replace("\u00A0", " ") |> String.trim() end)
  end
end
