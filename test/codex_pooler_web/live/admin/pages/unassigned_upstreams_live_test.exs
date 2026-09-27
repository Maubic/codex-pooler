defmodule CodexPoolerWeb.Admin.UnassignedUpstreamsLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import Phoenix.LiveViewTest

  alias CodexPooler.Accounts
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Pools
  alias CodexPooler.Upstreams

  setup :register_and_log_in_user

  test "owner sees an unassigned account in the list and detail views", %{
    conn: conn,
    scope: scope
  } do
    identity = active_upstream_identity_fixture(%{account_label: "Detached account"})
    reason = "Assign this account to a Pool before using account actions."

    assert Pools.list_visible_pools(scope) == []

    {:ok, list_view, _html} = live(conn, ~p"/admin/upstreams")
    assert has_element?(list_view, "#upstream-account-#{identity.id}", "Detached account")

    pool_count = "#upstream-account-#{identity.id} [data-role='upstream-pool-count-cell']"
    assert has_element?(list_view, "#{pool_count} .text-warning", "No Pools")

    list_view
    |> element("#upstream-account-#{identity.id}-pools-panel-trigger")
    |> render_click()

    assert has_element?(list_view, "#{pool_count} .text-warning", "No Pools")

    assert has_element?(
             list_view,
             "#upstream-account-actions-menu-#{identity.id}:not([title])"
           )

    assert has_element?(
             list_view,
             "#rename-upstream-account-#{identity.id}[disabled][title='#{reason}']"
           )

    {:ok, detail_view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    assert has_element?(detail_view, "#upstream-cockpit-title", "Detached account")
    assert has_element?(detail_view, "#upstream-assignments-empty", "No Pool assignments")

    assert has_element?(
             detail_view,
             "#cockpit-rename-upstream-account-#{identity.id}[disabled][title='#{reason}']"
           )

    assert has_element?(detail_view, "#saved-reset-policy-submit[disabled][title='#{reason}']")
  end

  test "Pool filtering and assigned-admin scope never expose unassigned accounts", %{
    conn: conn,
    scope: owner_scope
  } do
    visible_pool = pool_fixture(%{name: "Visible Pool"})
    hidden_pool = pool_fixture(%{name: "Hidden Pool"})
    %{identity: visible_identity} = upstream_assignment_fixture(visible_pool)
    %{identity: hidden_identity} = upstream_assignment_fixture(hidden_pool)
    unassigned_identity = active_upstream_identity_fixture(%{account_label: "Unassigned"})

    {:ok, owner_view, _html} =
      live(conn, ~p"/admin/upstreams?pool_id=#{visible_pool.id}")

    assert has_element?(owner_view, "#upstream-account-#{visible_identity.id}")

    refute has_element?(
             owner_view,
             "#upstream-account-#{visible_identity.id} [data-role='upstream-pool-count-cell'] .text-warning"
           )

    refute has_element?(owner_view, "#upstream-account-#{hidden_identity.id}")
    refute has_element?(owner_view, "#upstream-account-#{unassigned_identity.id}")

    %{user: admin, temporary_password: password} =
      operator_fixture(owner_scope, %{"password_change_required" => "false"})

    operator_pool_assignment_fixture(admin, visible_pool, created_by_user_id: owner_scope.user.id)

    assert {:ok, %{token: token}} =
             Accounts.login_user(%{"email" => admin.email, "password" => password})

    admin_conn = log_in_user(build_conn(), admin, token)
    {:ok, admin_view, _html} = live(admin_conn, ~p"/admin/upstreams")
    assert has_element?(admin_view, "#upstream-account-#{visible_identity.id}")
    refute has_element?(admin_view, "#upstream-account-#{hidden_identity.id}")
    refute has_element?(admin_view, "#upstream-account-#{unassigned_identity.id}")
  end

  test "reattaching a paused account preserves its paused identity state", %{scope: scope} do
    pool = pool_fixture()
    identity = active_upstream_identity_fixture(%{account_label: "Paused detached account"})

    identity
    |> Ecto.Changeset.change(%{status: "paused"})
    |> CodexPooler.Repo.update!()

    assert :ok =
             Upstreams.sync_pool_assignments_for_pool_edit(pool, [identity.id], select_by: :upstream_identity_id)

    assert [%{status: "active", upstream_identity_id: identity_id}] =
             Upstreams.list_pool_assignments(pool)

    assert identity_id == identity.id
    assert CodexPooler.Repo.reload!(identity).status == "paused"
    assert Upstreams.list_eligible_pool_assignments(pool) == []
    assert [visible] = Upstreams.list_visible_upstream_identities(Scope.for_user(scope.user))
    assert visible.id == identity.id
  end
end
