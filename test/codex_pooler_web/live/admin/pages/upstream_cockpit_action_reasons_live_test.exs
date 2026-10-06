defmodule CodexPoolerWeb.Admin.UpstreamCockpitActionReasonsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools

  setup :register_and_log_in_user

  # A disabled action states its reason in a tooltip, which a touch screen cannot reach, so below `sm` the same reason also
  # shows as text under the action (https://github.com/icoretech/codex-pooler-findings/issues/326, row 326-1).
  test "a disabled action keeps its tooltip and gets the same reason as a line under it", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "rail-reasons", name: "Rail reasons"})
    %{identity: identity} = upstream_assignment_fixture(pool, %{account_label: "Rail reasons sample"})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")

    disabled = [
      "#cockpit-reactivate-upstream-account-#{identity.id}",
      "#cockpit-replace-auth-json-upstream-account-#{identity.id}",
      "#cockpit-redeem-saved-reset-upstream-account-#{identity.id}",
      "#cockpit-download-reset-calendar-#{identity.id}"
    ]

    for selector <- disabled do
      reason = view |> element(selector) |> render() |> LazyHTML.from_fragment() |> LazyHTML.query("button") |> LazyHTML.attribute("title") |> List.first()
      assert is_binary(reason) and reason != "", "#{selector} carries its reason in the title"
      reason_id = String.trim_leading(selector, "#") <> "-reason"

      assert has_element?(view, ~s(#{selector}[disabled][title="#{reason}"][aria-describedby="#{reason_id}"]))
      assert has_element?(view, "#{selector} span", "unavailable")
      # The line directly follows its action and is hidden from `sm` up, where the tooltip stays the only reason.
      assert has_element?(view, ~s(#{selector} + p##{reason_id}[data-role="cockpit-action-reason"][class~="sm:hidden"]), reason)
    end
  end

  test "an available action has no reason line and nothing dangles from aria-describedby", %{conn: conn, scope: scope} do
    {:ok, pool} = Pools.create_pool(scope, %{slug: "rail-reasons-available", name: "Rail reasons available"})
    %{identity: identity} = upstream_assignment_fixture(pool, %{account_label: "Rail reasons available sample"})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")

    available = "#cockpit-pause-upstream-account-#{identity.id}"
    assert has_element?(view, "#{available}:not([disabled])")
    refute has_element?(view, "#{available}[aria-describedby]")
    refute has_element?(view, "#{available}-reason")

    described = view |> render() |> LazyHTML.from_fragment() |> LazyHTML.query("#upstream-actions [aria-describedby]") |> LazyHTML.attribute("aria-describedby")
    assert described != []

    for id <- described do
      assert has_element?(view, "#upstream-actions p##{id}[data-role='cockpit-action-reason']"), "#{id} is rendered"
    end
  end
end
