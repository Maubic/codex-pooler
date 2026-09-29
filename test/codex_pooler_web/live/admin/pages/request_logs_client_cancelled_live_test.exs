defmodule CodexPoolerWeb.Admin.RequestLogsClientCancelledLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools

  # A client cancellation is recorded `failed` with `client_disconnected`: 499
  # on a websocket, the 200 an HTTP stream had already sent. The request log
  # shows it as its own class and filters it on its own, while the Pooler-side
  # cuts that share the 499 stay failures (findings#292).
  @detection_timeout_ms 15_000

  setup :register_and_log_in_user

  setup %{scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "client-cancel-logs-#{System.unique_integer([:positive])}", name: "Client Cancel Logs"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    %{assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: "Client cancel upstream"})
    context = %{pool: pool, api_key: api_key, assignment: assignment}

    rows = %{
      cancelled_websocket: log_row!(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}),
      cancelled_stream: log_row!(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"}),
      drained: log_row!(context, %{status: "failed", last_error_code: "owner_drained", response_status_code: 499, transport: "websocket"}),
      recovered: log_row!(context, %{status: "failed", last_error_code: "dead_execution_recovered", response_status_code: 499, transport: "http_sse"}),
      succeeded: log_row!(context, %{status: "succeeded"})
    }

    %{pool: pool, rows: rows}
  end

  test "a client cancellation shows its own label and warning tone while 499 cuts on the Pooler's side stay failed", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    for key <- [:cancelled_websocket, :cancelled_stream] do
      row = "#request-log-row-#{rows[key].id}"
      assert has_element?(view, "#{row}[data-status='failed'][data-display-status='client_cancelled']")
      assert has_element?(view, "#{row} [data-role='status-text'].text-warning", "Client cancelled")
      assert has_element?(view, "#{row} [data-role='status-icon'][data-status='client_cancelled'] .hero-stop-circle")
      assert has_element?(view, "#request-log-#{rows[key].id}-errors.text-warning", "client_disconnected")
      assert has_element?(view, "#request-log-#{rows[key].id}-errors .hero-exclamation-triangle.text-warning")
      refute has_element?(view, "#{row} [data-role='status-text'].text-error")
    end

    for key <- [:drained, :recovered] do
      row = "#request-log-row-#{rows[key].id}"
      assert has_element?(view, "#{row}[data-status='failed'][data-display-status='failed']")
      assert has_element?(view, "#{row} [data-role='status-text'].text-error", "Failed")
      assert has_element?(view, "#{row} [data-role='status-icon'][data-status='failed'] .hero-x-circle")
      assert has_element?(view, "#request-log-#{rows[key].id}-errors.text-error")
    end
  end

  test "the status filter lists client cancellations on their own and says that Failed leaves them out", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled']", "Client cancelled")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled'] .hero-stop-circle")
    # The class is not the recorded `cancelled` status, whose option stays as it was.
    refute has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled']", "Cancelled")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='cancelled'] .hero-no-symbol")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-option'][data-status='failed'] [data-role='status-filter-option-detail']", "excl. client cancelled")

    view
    |> element("#request-log-status-filter [data-role='status-filter-option'][data-status='client_cancelled']")
    |> render_click()

    assert_patch(view, ~p"/admin/request-logs?pool_id=#{pool.id}&status=client_cancelled")
    _ = await_request_logs(view)

    assert has_element?(view, "#filters_status[type='hidden'][value='client_cancelled']")
    assert listed(view, rows) == [:cancelled_stream, :cancelled_websocket]
    refute has_element?(view, "#request-log-filter-errors")

    view
    |> element("#request-log-status-filter [data-role='status-filter-option'][data-status='failed']")
    |> render_click()

    assert_patch(view, ~p"/admin/request-logs?pool_id=#{pool.id}&status=failed")
    _ = await_request_logs(view)

    assert listed(view, rows) == [:drained, :recovered]
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-trigger'][title*='not counting requests the client cancelled']", "Failed")
    assert has_element?(view, "#request-log-status-filter [data-role='status-filter-detail']", "excl. client cancelled")

    {:ok, any_view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    assert listed(any_view, rows) == [:cancelled_stream, :cancelled_websocket, :drained, :recovered, :succeeded]
    refute has_element?(any_view, "#request-log-status-filter [data-role='status-filter-detail']")
  end

  test "the detail drawer names the class and the status it was recorded with", %{conn: conn, pool: pool, rows: rows} do
    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}&status=client_cancelled")

    render_click(element(view, "#request-log-#{rows.cancelled_stream.id}-open-details"))
    _ = assert_patch(view)

    assert has_element?(view, "#request-log-detail-sidebar header span.text-warning", "Client cancelled")
    assert has_element?(view, "#request-log-detail-status", "Client cancelled (recorded as failed)")
    assert has_element?(view, "#request-log-detail-error-code", "client_disconnected")
    assert has_element?(view, "#request-log-detail-response-status", "200")
  end

  defp listed(view, rows) do
    rows
    |> Enum.filter(fn {_key, request} -> has_element?(view, "#request-log-row-#{request.id}") end)
    |> Enum.map(fn {key, _request} -> key end)
    |> Enum.sort()
  end

  defp log_row!(%{pool: pool, api_key: api_key, assignment: assignment}, attrs) do
    status = Map.fetch!(attrs, :status)

    request =
      request_fixture(%{pool: pool, api_key: api_key}, %{
        status: status,
        transport: Map.get(attrs, :transport, "http_json"),
        response_status_code: Map.get(attrs, :response_status_code, 200),
        last_error_code: Map.get(attrs, :last_error_code),
        correlation_id: "client-cancel-log-#{System.unique_integer([:positive])}"
      })

    attempt_fixture(request, assignment, %{
      status: if(status == "succeeded", do: "succeeded", else: "failed"),
      network_error_code: Map.get(attrs, :last_error_code),
      upstream_status_code: Map.get(attrs, :response_status_code, 200)
    })

    request
  end

  defp live_request_logs(conn, path) do
    with {:ok, view, html} <- live(conn, path) do
      _ = await_request_logs(view)
      {:ok, view, html}
    end
  end

  defp await_request_logs(view),
    do: await_request_logs(view, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_request_logs(view, deadline) do
    _ = render_async(view, 5_000)
    state = :sys.get_state(view.pid)

    if state.socket.assigns.request_logs_loading? or state.socket.assigns.request_logs_running? do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("request logs did not finish loading")

      receive do
      after
        1 -> await_request_logs(view, deadline)
      end
    else
      state
    end
  end
end
