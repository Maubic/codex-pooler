defmodule CodexPooler.Admin.UpstreamCockpitClientCancellationsTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.RequestOutcome
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Admin.UpstreamCockpitMetrics
  alias CodexPooler.Pools
  alias CodexPooler.Repo

  # A client cancellation is recorded `failed` with `client_disconnected`
  # (499 on a websocket, the 200 an HTTP stream had already sent). The cockpit
  # counts it apart: it is neither a failure of the account nor part of the
  # failure rate's base (findings#292).
  setup do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    scope = Scope.for_user(owner)
    {:ok, pool} = Pools.create_pool(scope, %{slug: "cockpit-client-cancel-#{System.unique_integer([:positive])}", name: "Client Cancel"})
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool)
    %{api_key: api_key} = active_api_key_fixture(pool)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %{scope: scope, identity: identity, fixture: %{pool: pool, api_key: api_key, assignment: assignment}, now: now}
  end

  test "client cancellations are counted apart and leave the failure rate, its base and the degraded state",
       %{scope: scope, identity: identity, fixture: fixture, now: now} do
    admitted_at = DateTime.add(now, -1, :hour)

    for _ordinal <- 1..20, do: insert_request!(fixture, %{status: "succeeded", admitted_at: admitted_at})

    for {transport, status_code} <- [{"websocket", 499}, {"websocket", 499}, {"http_sse", 200}] do
      insert_request!(fixture, %{status: "failed", admitted_at: admitted_at, transport: transport, response_status_code: status_code, last_error_code: "client_disconnected"})
    end

    insert_request!(fixture, %{status: "failed", admitted_at: admitted_at, transport: "websocket", response_status_code: 499, last_error_code: "owner_drained"})

    request_health = UpstreamCockpitMetrics.request_health(scope, identity, now)

    # Counted as failures, the three cancellations made 4 of 24 (16.7%) and
    # degraded an account whose one real failure in 21 is 4.8%.
    assert request_health.kpis.total_requests_24h == 24
    assert request_health.kpis.failed_requests_24h == 1
    assert request_health.kpis.client_cancelled_requests_24h == 3
    assert request_health.kpis.failure_rate_24h == 4.8
    assert request_health.state == "healthy"
    refute request_health.degraded?

    bucket = Enum.find(request_health.items, &(&1.date == Date.to_iso8601(DateTime.to_date(admitted_at))))
    assert %{success_count: 20, failure_count: 1, client_cancelled_count: 3, total_count: 24} = bucket

    assert request_health.kpis.error_breakdown_24h == [%{status_code: 499, error_code: "owner_drained", count: 1}]
  end

  test "499 cuts on the Pooler's side stay failures, and an account whose own outcomes all failed is failed whatever its clients cancelled",
       %{scope: scope, identity: identity, fixture: fixture, now: now} do
    admitted_at = DateTime.add(now, -2, :hour)

    for code <- ~w(owner_drained dead_execution_recovered) do
      insert_request!(fixture, %{status: "failed", admitted_at: admitted_at, transport: "websocket", response_status_code: 499, last_error_code: code})
    end

    for _ordinal <- 1..2 do
      insert_request!(fixture, %{status: "failed", admitted_at: admitted_at, transport: "websocket", response_status_code: 499, last_error_code: "client_disconnected"})
    end

    request_health = UpstreamCockpitMetrics.request_health(scope, identity, now)

    assert request_health.kpis.failed_requests_24h == 2
    assert request_health.kpis.client_cancelled_requests_24h == 2
    assert request_health.kpis.failure_rate_24h == 100.0
    assert request_health.state == "failed"
    assert request_health.degraded?
    assert request_health.kpis.error_breakdown_24h |> Enum.map(& &1.error_code) |> Enum.sort() == ~w(dead_execution_recovered owner_drained)
  end

  test "an account whose only non-successes are client cancellations stays healthy, with no success at all",
       %{scope: scope, identity: identity, fixture: fixture, now: now} do
    for _ordinal <- 1..2 do
      insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -3, :hour), transport: "http_sse", response_status_code: 200, last_error_code: "client_disconnected"})
    end

    request_health = UpstreamCockpitMetrics.request_health(scope, identity, now)

    # Counted as failures, 2 of 2 requests "failed" and so did the account.
    assert request_health.kpis.total_requests_24h == 2
    assert request_health.kpis.failed_requests_24h == 0
    assert request_health.kpis.client_cancelled_requests_24h == 2
    assert request_health.kpis.failure_rate_24h == 0.0
    assert request_health.state == "healthy"
    assert request_health.kpis.error_breakdown_24h == []
  end

  # `cancelled` is a status the database permits and nothing writes: no request
  # ever carried it, so the cockpit neither counts it nor lists it as a failure.
  test "a request recorded with the cancelled status nothing writes is not part of the request health",
       %{scope: scope, identity: identity, fixture: fixture, now: now} do
    admitted_at = DateTime.add(now, -1, :hour)

    insert_request!(fixture, %{status: "succeeded", admitted_at: admitted_at})
    insert_request!(fixture, %{status: "cancelled", admitted_at: admitted_at, response_status_code: 499})

    request_health = UpstreamCockpitMetrics.request_health(scope, identity, now)

    assert request_health.kpis.total_requests_24h == 1
    assert request_health.kpis.failed_requests_24h == 0
    assert request_health.state == "healthy"
    assert Enum.find(request_health.items, &(&1.date == Date.to_iso8601(DateTime.to_date(admitted_at)))).failure_count == 0
    assert %{rows: []} = UpstreamCockpitMetrics.recent_request_events(scope, identity, 10)
  end

  test "recent events keep a client cancellation only when it was retried", %{scope: scope, identity: identity, fixture: fixture, now: now} do
    plain = insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -30, :second), transport: "websocket", response_status_code: 499, last_error_code: "client_disconnected"})
    drained = insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -1, :minute), transport: "websocket", response_status_code: 499, last_error_code: "owner_drained"})
    retried = insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -2, :minute), transport: "http_sse", response_status_code: 200, last_error_code: "client_disconnected"})

    retried
    |> attempt_fixture(fixture.assignment, %{attempt_number: 2, status: "failed", network_error_code: "client_disconnected"})
    |> Ecto.Changeset.change(%{started_at: DateTime.add(now, -110, :second)})
    |> Repo.update!()

    assert %{rows: rows, searched_attempt_limit: nil} = UpstreamCockpitMetrics.recent_request_events(scope, identity, 10)
    assert Enum.map(rows, & &1.id) == [drained.id, retried.id]
    refute plain.id in Enum.map(rows, & &1.id)
  end

  test "idle cutoffs remain failed in aggregate health and recent events", %{scope: scope, identity: identity, fixture: fixture, now: now} do
    metadata = %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "unknown"}}
    timeout = insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -30, :second), transport: "websocket", response_status_code: 499, last_error_code: "client_disconnected", request_metadata: metadata})
    insert_request!(fixture, %{status: "failed", admitted_at: DateTime.add(now, -30, :second), transport: "websocket", response_status_code: 499, last_error_code: "client_disconnected"})
    health = UpstreamCockpitMetrics.request_health(scope, identity, now)
    assert health.kpis.failed_requests_24h == 1
    assert health.kpis.client_cancelled_requests_24h == 1
    assert health.kpis.failure_rate_24h == 100.0
    assert %{rows: [row]} = UpstreamCockpitMetrics.recent_request_events(scope, identity, 10)
    assert row.id == timeout.id
    refute RequestOutcome.client_cancelled?(row)
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
        request_metadata: Map.get(attrs, :request_metadata, %{}),
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
