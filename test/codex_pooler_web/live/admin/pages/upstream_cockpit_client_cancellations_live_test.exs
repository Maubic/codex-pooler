defmodule CodexPoolerWeb.Admin.UpstreamCockpitClientCancellationsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools
  alias CodexPooler.Repo

  setup :register_and_log_in_user

  # A client cancellation is recorded `failed` with `client_disconnected`; the
  # cockpit shows it as its own count and keeps it out of the failures, the
  # rate, the error breakdown and the recent activity (findings#292).
  test "request health counts client cancellations apart and the verdict names them", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "cockpit-client-cancel-live-#{System.unique_integer([:positive])}", name: "Cockpit Client Cancel"})
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: "Client cancel cockpit"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    fixture = %{pool: pool, api_key: api_key, assignment: assignment}
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    for offset <- 1..20, do: insert_request!(fixture, %{status: "succeeded", admitted_at: DateTime.add(now, -offset, :minute)})

    for {offset, transport, status_code} <- [{30, "websocket", 499}, {31, "websocket", 499}, {32, "http_sse", 200}] do
      insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -offset, :second), transport: transport, response_status_code: status_code, last_error_code: "client_disconnected"})
    end

    insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -40, :minute), transport: "websocket", response_status_code: 499, last_error_code: "owner_drained"})

    {:ok, view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    _ = render_async(view, 5_000)

    assert has_element?(view, "#request-health-client-cancelled", "24h client cancelled")
    assert has_element?(view, "#request-health-client-cancelled[title*='not counted as failures']", "3")
    assert has_element?(view, "#request-health-chart", "4.8%")
    assert has_element?(view, "#request-health-chart-summary", "1 failed in the last 24h; failure rate 4.8%; 3 cancelled by the client, not counted as failures.")
    assert has_element?(view, "#request-health-chart-plot[data-chart-series*='Client cancelled']")

    assert has_element?(
             view,
             "#upstream-routing-request-note",
             "1 of 21 requests failed in the last 24h (4.8%), within the expected range for upstream calls; 3 cancelled by the client not counted"
           )

    assert has_element?(view, "#request-health-error-breakdown", "owner_drained")
    refute has_element?(view, "#request-health-error-breakdown", "client_disconnected")

    assert has_element?(view, "#upstream-event-summary-rows [data-role='recent-event-title']", "owner_drained")
    refute has_element?(view, "#upstream-event-summary-rows", "client_disconnected")
  end

  # `cancelled` is a status the database permits and nothing writes, so it is no
  # failure: such a request reaches the recent activity only for its retry.
  test "recent activity shows a retried request recorded with the cancelled status nothing writes as retried, not failed", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "cockpit-cancelled-status-live-#{System.unique_integer([:positive])}", name: "Cockpit Cancelled Status"})
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: "Cancelled status cockpit"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    fixture = %{pool: pool, api_key: api_key, assignment: assignment}
    admitted_at = DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.add(-5, :minute)

    fixture
    |> insert_request!(%{status: "cancelled", admitted_at: admitted_at, response_status_code: nil})
    |> attempt_fixture(assignment, %{attempt_number: 2, status: "failed"})
    |> Ecto.Changeset.change(%{started_at: DateTime.add(admitted_at, 2, :second)})
    |> Repo.update!()

    {:ok, view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    _ = render_async(view, 5_000)

    assert has_element?(view, "#upstream-event-summary-rows [data-role='recent-event-title']", "Request retried")
    refute has_element?(view, "#upstream-event-summary-rows", "Request failed")
    refute has_element?(view, "#upstream-event-summary-rows", "after retry")
  end

  defp insert_request!(%{pool: pool, api_key: api_key, assignment: assignment}, attrs) do
    admitted_at = Map.fetch!(attrs, :admitted_at)
    completed_at = DateTime.add(admitted_at, 1, :second)
    status = Map.fetch!(attrs, :status)

    request =
      %{pool: pool, api_key: api_key}
      |> request_fixture(%{
        status: status,
        transport: Map.get(attrs, :transport, "http_json"),
        response_status_code: Map.get(attrs, :response_status_code, 200),
        last_error_code: Map.get(attrs, :last_error_code)
      })
      |> Ecto.Changeset.change(%{admitted_at: admitted_at, completed_at: completed_at})
      |> Repo.update!()

    attempt_status = if status == "succeeded", do: "succeeded", else: "failed"

    request
    |> attempt_fixture(assignment, %{status: attempt_status, network_error_code: Map.get(attrs, :last_error_code)})
    |> Ecto.Changeset.change(%{started_at: admitted_at, completed_at: completed_at})
    |> Repo.update!()

    request
  end
end
