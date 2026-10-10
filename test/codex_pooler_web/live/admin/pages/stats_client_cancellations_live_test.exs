defmodule CodexPoolerWeb.Admin.StatsClientCancellationsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools

  @detection_timeout_ms 15_000

  setup :register_and_log_in_user

  # The request and success-rate cards name client cancellations instead of
  # counting them as failures (findings#292).
  test "the request cards count client cancellations apart and say the success rate leaves them out", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "stats-client-cancel-live-#{System.unique_integer([:positive])}", name: "Stats Client Cancel"})
    %{api_key: api_key} = active_api_key_fixture(pool)
    context = %{pool: pool, api_key: api_key}

    request_fixture(context, %{status: "succeeded"})
    request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"})
    request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket", request_metadata: %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout"}}})

    {:ok, view, _html} = live(conn, ~p"/admin/stats?pool_id=#{pool.id}")
    _ = await_stats_dashboard(view, System.monotonic_time(:millisecond) + @detection_timeout_ms)

    assert has_element?(view, "#stats-kpi-requests", "3")
    assert has_element?(view, "#stats-kpi-requests", "1 succeeded · 1 failed · 1 client cancelled")
    assert has_element?(view, "#stats-kpi-success-rate", "50.0%")
    assert has_element?(view, "#stats-kpi-success-rate", "Completed; excludes client cancellations")
  end

  defp await_stats_dashboard(view, deadline) do
    _ = render_async(view, 5_000)
    state = :sys.get_state(view.pid)

    if Map.get(state.socket.assigns, :dashboard_loading?, false) or Map.get(state.socket.assigns, :stats_dashboard_running?, false) do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("stats dashboard did not finish loading")

      receive do
      after
        1 -> await_stats_dashboard(view, deadline)
      end
    else
      state
    end
  end
end
