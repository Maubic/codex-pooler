defmodule CodexPoolerWeb.Admin.SavedResetClientReconnectLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  setup :register_and_log_in_user

  test "bank draft recovery follows the current scoped read and keeps explicit pause", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "reconnect-draft", name: "Reconnect draft"})
    %{identity: identity} = upstream_assignment_fixture(pool)
    policy = %{"auto_redeem_enabled" => "true", "keep_credits" => "4", "min_blocked_minutes" => "23"}
    conn = put_connect_params(conn, %{"live_updates_paused" => true, "saved_reset_policy_recovery" => %{"id" => identity.id, "policy" => policy}})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    assert has_element?(view, "#saved-reset-policy-dialog[open]")
    assert has_element?(view, "#saved-reset-policy-keep-credits[value='4']")
    assert has_element?(view, "#saved-reset-policy-min-blocked-minutes[value='23']")
    assert :sys.get_state(view.pid).socket.assigns.live_updates_paused?
    assert :sys.get_state(view.pid).socket.assigns.editing_saved_reset_policy.identity.id == identity.id
    assert Repo.get!(UpstreamIdentity, identity.id).metadata == identity.metadata
    filters = :sys.get_state(view.pid).socket.assigns.filter_values
    view |> element("#upstream-filter-form") |> render_change(%{"filters" => filters})
    assert has_element?(view, "#saved-reset-policy-dialog[open]")
    assert has_element?(view, "#saved-reset-policy-keep-credits[value='4']")
    assert has_element?(view, "#saved-reset-redemption-action[data-saved-reset-action='open-redemption'][data-server-disabled]")
    assert has_element?(view, "#saved-reset-policy-submit[data-saved-reset-action='save-policy']")
    assert has_element?(view, "#saved-reset-connection-bank[data-saved-reset-connection-notice][hidden]")
  end

  test "an actual filter change retains navigation and closes the old bank", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "reconnect-filter", name: "Reconnect filter"})
    %{identity: identity} = upstream_assignment_fixture(pool)
    conn = put_connect_params(conn, %{"saved_reset_policy_recovery" => %{"id" => identity.id, "policy" => %{"keep_credits" => "4"}}})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    assert has_element?(view, "#saved-reset-policy-dialog[open]")
    view |> element("#upstream-filter-form") |> render_change(%{"filters" => %{"query" => "no-matching-account"}})
    assert_patch(view, ~p"/admin/upstreams?query=no-matching-account")
    refute has_element?(view, "#saved-reset-policy-dialog")
    assert :sys.get_state(view.pid).socket.assigns.filter_values["query"] == "no-matching-account"
  end

  test "unavailable target cannot recover a bank draft or controls", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "reconnect-removed", name: "Reconnect removed"})
    %{identity: identity} = upstream_assignment_fixture(pool)
    Repo.delete!(identity)
    conn = put_connect_params(conn, %{"saved_reset_policy_recovery" => %{"id" => identity.id, "policy" => %{"keep_credits" => "4"}}})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    refute has_element?(view, "#saved-reset-policy-dialog")
    refute has_element?(view, "#saved-reset-redemption-confirm")
  end

  test "the disconnect notices say the reset continues only while the page shows one still open", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "reconnect-notice", name: "Reconnect notice"})
    %{identity: identity} = upstream_assignment_fixture(pool)
    continues = "The reset continues."

    # Nothing open: every notice says only what holds on any page.
    assert_notices(conn, identity, fn view, selector -> assert has_element?(view, selector, "Live updates disconnected. Reconnect to see the latest status before acting.") end)
    assert_notices(conn, identity, fn view, selector -> refute has_element?(view, selector, continues) end)

    # A reset under verification is still open, so every surface that shows it says it continues.
    persist_receipt!(identity, "consumed_pending_probe")
    assert_notices(conn, identity, fn view, selector -> assert has_element?(view, selector, continues) end)

    # A finished receipt leaves nothing open.
    persist_receipt!(identity, "confirmed_by_quota")
    assert_notices(conn, identity, fn view, selector -> refute has_element?(view, selector, continues) end)
  end

  test "malformed reconnect draft is ignored and both page roots expose native connection hook", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "reconnect-malformed", name: "Reconnect malformed"})
    %{identity: identity} = upstream_assignment_fixture(pool)
    conn = put_connect_params(conn, %{"saved_reset_policy_recovery" => %{"id" => identity.id, "policy" => "invalid"}})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    refute has_element?(view, "#saved-reset-policy-dialog")
    assert has_element?(view, "#admin-upstreams-live[phx-hook='SavedResetConnection']")
    {:ok, cockpit, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    assert has_element?(cockpit, "#upstream-cockpit[phx-hook='SavedResetConnection']")
    assert has_element?(cockpit, "[data-saved-reset-action='open-redemption'][data-server-disabled]")
    assert has_element?(cockpit, "#saved-reset-policy-form[data-saved-reset-form]")
  end

  # List, bank and cockpit each render their own hidden notice; the hook shows it while the socket is down.
  defp assert_notices(conn, identity, check) do
    {:ok, list, _html} = live(conn, ~p"/admin/upstreams")
    check.(list, "#saved-reset-connection-list[data-saved-reset-connection-notice][hidden]")
    render_click(list, "open_saved_reset_policy", %{"id" => identity.id})
    check.(list, "#saved-reset-connection-bank[data-saved-reset-connection-notice][hidden]")
    {:ok, cockpit, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    check.(cockpit, "#saved-reset-connection-cockpit[data-saved-reset-connection-notice][hidden]")
  end

  defp persist_receipt!(identity, phase) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    record = %{"phase" => phase, "status" => "succeeded", "generation" => 2, "started_at" => DateTime.to_iso8601(DateTime.add(now, -30, :second)), "consumed_at" => DateTime.to_iso8601(DateTime.add(now, -20, :second)), "deadline_at" => DateTime.to_iso8601(DateTime.add(now, 180, :second)), "result" => %{"applied" => true, "code" => "reset"}}
    current = Repo.get!(UpstreamIdentity, identity.id)
    current |> Ecto.Changeset.change(metadata: Map.put(current.metadata, "saved_reset_redemption", record)) |> Repo.update!()
  end
end
