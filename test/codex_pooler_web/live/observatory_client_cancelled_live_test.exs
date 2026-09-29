defmodule CodexPoolerWeb.ObservatoryClientCancelledLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Repo
  alias CodexPoolerWeb.ObservatoryControllerTestHelpers, as: Helpers

  @observatory_path "/observatory"

  # findings#292: the holder of a key sees a request their own client cancelled
  # as that, not as "Failed · Service unavailable" (the failure code of a
  # connection that dropped matched the service-unavailable family), and the
  # success rate is taken over the requests the client did not cancel.
  test "a holder sees their own cancellations apart from the failures, and the success rate leaves them out", %{conn: conn} do
    pool = pool_fixture()
    %{api_key: api_key, raw_key: raw_key} = active_api_key_fixture(pool)
    Helpers.enable_dashboard_access!(api_key)
    model = model_fixture(pool, %{exposed_model_id: "safe-cancelled-model", display_name: "safe-cancelled-model"})
    context = %{pool: pool, api_key: api_key}
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    for {seconds_ago, attrs} <- [
          {6, %{status: "succeeded"}},
          {5, %{status: "succeeded"}},
          {4, %{status: "succeeded"}},
          {3, %{status: "failed", last_error_code: "upstream_unavailable", response_status_code: 502}},
          {2, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}},
          {1, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"}}
        ] do
      admitted_at = DateTime.add(now, -seconds_ago, :second)

      context
      |> request_fixture(Map.merge(attrs, %{model_id: model.id, requested_model: "safe-cancelled-model"}))
      |> Ecto.Changeset.change(%{admitted_at: admitted_at, completed_at: admitted_at})
      |> Repo.update!()
    end

    conn = authenticated_conn(conn, raw_key)
    {:ok, view, _html} = live(conn, @observatory_path)
    render_hook(view, "observatory-refresh", %{"reason" => "initial"})
    Helpers.await_async(view)
    html = render(view)

    assert has_element?(view, "#observatory-fact-success", "75.0")
    assert has_element?(view, "#observatory-fact-success", "3 succeeded · 1 failed · 2 client cancelled")

    assert outcome_labels(html) == [
             "Client cancelled",
             "Client cancelled",
             "Failed · Service unavailable",
             "Succeeded",
             "Succeeded",
             "Succeeded"
           ]

    assert has_element?(view, "[data-role='outcome-status'][data-status='warn']", "Client cancelled")
    refute html =~ "client_disconnected"
  end

  defp outcome_labels(html) do
    html
    |> LazyHTML.from_fragment()
    |> LazyHTML.query("[data-role='outcome-status']")
    |> Enum.map(&(&1 |> LazyHTML.text() |> String.trim()))
  end

  defp authenticated_conn(conn, raw_key) do
    conn = get(conn, "/observatory/login")

    post(conn, "/observatory/login", %{
      "observatory" => %{"api_key" => raw_key},
      "_csrf_token" => Helpers.csrf_token_from(conn.resp_body)
    })
  end
end
