defmodule CodexPoolerWeb.Admin.OperatorDatetimeFormsTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access
  alias CodexPooler.Access.APIKey
  alias CodexPooler.Audit
  alias CodexPooler.Pools
  alias CodexPooler.Repo

  setup :register_and_log_in_user

  setup %{scope: scope, user: user} do
    user |> Ecto.Changeset.change(timezone: "Europe/Rome") |> Repo.update!()
    {:ok, pool} = Pools.create_pool(scope, %{slug: "operator-datetime", name: "Operator datetime"})
    {:ok, %{api_key: api_key, raw_key: raw_key}} = Access.create_api_key(scope, pool, %{display_name: "Scheduled key"})
    %{pool: pool, api_key: api_key, raw_key: raw_key}
  end

  test "editing loads local time and an unchanged save preserves the exact instant", %{conn: conn, api_key: api_key} do
    expiry = ~U[2099-07-15 07:54:38.123456Z]
    api_key |> Ecto.Changeset.change(expires_at: expiry) |> Repo.update!()
    {:ok, view, _html} = live(conn, ~p"/admin/api-keys")
    view |> element("#edit-api-key-#{api_key.id}") |> render_click()

    assert view |> element("#api_key_expires_at") |> render() |> LazyHTML.from_fragment() |> LazyHTML.query("input") |> LazyHTML.attribute("value") == ["2099-07-15T09:54"]
    view |> element("#api-key-form") |> render_submit(%{"api_key" => %{"expires_at" => "2099-07-15T09:54", "display_name" => "Renamed key"}})
    assert Repo.get!(APIKey, api_key.id).expires_at == expiry
    assert Repo.get!(APIKey, api_key.id).display_name == "Renamed key"
  end

  test "saving the current local minute expires the key instead of extending it", %{conn: conn, api_key: api_key, raw_key: raw_key} do
    now = DateTime.utc_now() |> DateTime.shift_zone!("Europe/Rome", Tz.TimeZoneDatabase)
    local_now = Calendar.strftime(now, "%Y-%m-%dT%H:%M")
    {:ok, view, _html} = live(conn, ~p"/admin/api-keys")
    view |> element("#edit-api-key-#{api_key.id}") |> render_click()
    view |> element("#api-key-form") |> render_submit(%{"api_key" => %{"expires_at" => local_now}})

    assert {:error, %{code: :api_key_expired}} = Access.authenticate_api_key(raw_key)
  end

  test "new keys save the local deadline as UTC and name its timezone", %{conn: conn, pool: pool} do
    {:ok, view, _html} = live(conn, ~p"/admin/api-keys")
    view |> element("#api-key-page-create-action") |> render_click()
    assert has_element?(view, "label", "Expires at - Europe/Rome")

    view
    |> element("#api-key-form")
    |> render_change(%{"api_key" => %{"display_name" => "Local deadline", "pool_id" => pool.id, "expires_at" => "2099-07-15T09:54"}})

    assert has_element?(view, "#api-key-expiry-summary", "2099-07-15 09:54 Europe/Rome (UTC+02:00)")
    view |> element("#api-key-tab-review") |> render_click()
    assert has_element?(view, "#api-key-review-summary", "Europe/Rome (UTC+02:00)")
    view |> element("#api-key-form") |> render_submit()
    assert Repo.get_by!(APIKey, pool_id: pool.id, display_name: "Local deadline").expires_at == ~U[2099-07-15 07:54:00.000000Z]
  end

  test "ambiguous and nonexistent local deadlines are rejected without changing the key", %{conn: conn, api_key: api_key} do
    {:ok, view, _html} = live(conn, ~p"/admin/api-keys")
    view |> element("#edit-api-key-#{api_key.id}") |> render_click()

    for {value, error} <- [{"2026-03-29T02:30", "does not exist"}, {"2026-10-25T02:30", "occurs twice"}] do
      view |> element("#api-key-form") |> render_submit(%{"api_key" => %{"expires_at" => value}})
      assert has_element?(view, "#api-key-review-errors", error)
      refute has_element?(view, "#api-key-expiry-summary")
      assert Repo.get!(APIKey, api_key.id).expires_at == nil
    end
  end

  test "editing expiry after another field reveals its validation error", %{conn: conn, api_key: api_key} do
    {:ok, view, _html} = live(conn, ~p"/admin/api-keys")
    view |> element("#edit-api-key-#{api_key.id}") |> render_click()
    view |> element("#api-key-form") |> render_change(%{"api_key" => %{"display_name" => "Typed name", "_unused_expires_at" => ""}})
    view |> element("#api-key-form") |> render_change(%{"api_key" => %{"expires_at" => "2026-03-29T02:30"}})
    assert has_element?(view, "#api-key-step-basics-panel", "does not exist in Europe/Rome")
    refute has_element?(view, "#api-key-expiry-summary")
  end

  test "request date filters include the local day and exclude adjacent dates", %{conn: conn, pool: pool, api_key: api_key} do
    fixture = %{pool: pool, api_key: api_key}
    before_day = request_at(fixture, ~U[2026-05-26 21:59:59.999999Z])
    first = request_at(fixture, ~U[2026-05-26 22:00:00.000000Z])
    last = request_at(fixture, ~U[2026-05-27 21:59:59.999999Z])
    after_day = request_at(fixture, ~U[2026-05-27 22:00:00.000000Z])

    {:ok, view, _html} = live(conn, ~p"/admin/request-logs?#{%{pool_id: pool.id, date_from: "2026-05-27", date_to: "2026-05-27"}}")
    render_async(view, 15_000)
    assert has_element?(view, "#filters_date_from-button[title='Date from (Europe/Rome)']")
    refute has_element?(view, "#filters_date_from-timezone")
    assert has_element?(view, "#request-log-row-#{first.id}")
    assert has_element?(view, "#request-log-row-#{last.id}")
    refute has_element?(view, "#request-log-row-#{before_day.id}")
    refute has_element?(view, "#request-log-row-#{after_day.id}")
  end

  test "audit date filters use the same local day as the displayed timestamps", %{conn: conn, user: user, pool: pool} do
    events =
      for at <- [~U[2026-05-26 21:59:59.999999Z], ~U[2026-05-26 22:00:00.000000Z], ~U[2026-05-27 21:59:59.999999Z], ~U[2026-05-27 22:00:00.000000Z]] do
        {:ok, event} = Audit.record_user_event(user, %{action: "operator.update", target_type: "user", target_id: user.id, pool_id: pool.id})
        event |> Ecto.Changeset.change(occurred_at: at) |> Repo.update!()
      end

    [before_day, first, last, after_day] = events
    {:ok, view, _html} = live(conn, ~p"/admin/audit-logs?#{%{pool_id: pool.id, date_from: "2026-05-27", date_to: "2026-05-27"}}")
    assert has_element?(view, "#filters_date_to-button[title='Date to (Europe/Rome)']")
    refute has_element?(view, "#filters_date_to-timezone")
    ids = :sys.get_state(view.pid).socket.assigns.audit_logs.items |> Enum.map(& &1.id)
    assert first.id in ids
    assert last.id in ids
    refute before_day.id in ids
    refute after_day.id in ids
  end

  defp request_at(fixture, at),
    do: fixture |> request_fixture() |> Ecto.Changeset.change(admitted_at: at, completed_at: at) |> Repo.update!()
end
