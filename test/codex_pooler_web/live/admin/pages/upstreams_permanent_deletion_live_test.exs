defmodule CodexPoolerWeb.Admin.UpstreamsPermanentDeletionLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import Phoenix.LiveViewTest

  alias CodexPooler.Accounts
  alias CodexPooler.Jobs.UpstreamDeletionWorker
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}

  setup :register_and_log_in_user

  test "failed enqueue keeps both confirmation dialogs alive and reports retry guidance", %{conn: conn} do
    identity = active_upstream_identity_fixture(%{account_label: "Sample delete failure"})
    Repo.query!("CREATE FUNCTION pg_temp.reject_ui_deletion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.worker = 'CodexPooler.Jobs.UpstreamDeletionWorker' THEN RAISE EXCEPTION 'sample insert failure' USING ERRCODE = 'object_not_in_prerequisite_state'; END IF; RETURN NEW; END $$")
    Repo.query!("CREATE TRIGGER reject_ui_deletion BEFORE INSERT ON oban_jobs FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_ui_deletion()")

    for {route, trigger, form} <- [
          {~p"/admin/upstreams", "delete-upstream-account-#{identity.id}", "delete-upstream-account-form"},
          {~p"/admin/upstreams/#{identity.id}", "cockpit-delete-upstream-account-#{identity.id}", "cockpit-delete-upstream-account-form"}
        ] do
      {:ok, view, _} = live(conn, route)
      view |> element("##{trigger}") |> render_click()
      view |> element("##{form}") |> render_submit(%{"upstream_delete" => %{"id" => identity.id, "confirmation_label" => identity.account_label}})
      assert Process.alive?(view.pid)
      assert has_element?(view, "##{form}")
      assert has_element?(view, "#flash-error", "upstream deletion could not be queued; retry Delete")
      assert Repo.get!(UpstreamIdentity, identity.id).status == "active"
      refute Repo.get!(UpstreamIdentity, identity.id).metadata["permanent_deletion_requested_at"]
    end
  end

  test "failed deletion labels preserve the Delete action name in card and cockpit", %{conn: conn} do
    %{identity: identity} = legacy_account(pool_fixture())
    identity |> Ecto.Changeset.change(metadata: %{"permanent_deletion_requested_at" => DateTime.to_iso8601(DateTime.utc_now())}) |> Repo.update!()
    {:ok, job} = Oban.insert(UpstreamDeletionWorker.new(%{"upstream_identity_id" => identity.id}))
    job |> Ecto.Changeset.change(state: "discarded") |> Repo.update!()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams")
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion failed - retry Delete")
    {:ok, cockpit, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    assert has_element?(cockpit, "#upstream-cockpit-status", "Deletion failed - retry Delete")
  end

  test "historical accounts retain usable reset calendar downloads", %{conn: conn} do
    %{identity: identity} = legacy_account(pool_fixture())
    expiration = DateTime.add(DateTime.utc_now(), 3600, :second) |> DateTime.to_iso8601()
    metadata = %{"saved_resets" => %{"status" => "reported", "available_count" => 1, "available_expires_at" => [expiration], "available_expirations" => [%{"expires_at" => expiration}], "next_expires_at" => expiration}}
    identity |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()
    {:ok, view, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    assert has_element?(view, "#cockpit-download-reset-calendar-#{identity.id}[href='/admin/upstreams/#{identity.id}/saved-reset-expirations.ics']")
    assert has_element?(view, "#cockpit-saved-reset-expiration-time-left-0[href='/admin/upstreams/#{identity.id}/saved-reset-expirations.ics']")
  end

  test "owner can reach and delete historical accounts retained only in inactive Pools", %{conn: conn} do
    for status <- ["archived", "disabled"] do
      pool = pool_fixture()
      %{identity: identity} = legacy_account(pool)
      pool |> Ecto.Changeset.change(status: status) |> Repo.update!()
      {:ok, view, _} = live(conn, ~p"/admin/upstreams?status=deleted")
      assert has_element?(view, "#delete-upstream-account-#{identity.id}:not([disabled])")
      {:ok, cockpit, _} = live(conn, ~p"/admin/upstreams/#{identity.id}")
      assert has_element?(cockpit, "#cockpit-delete-upstream-account-#{identity.id}:not([disabled])")
      view |> element("#delete-upstream-account-#{identity.id}") |> render_click()
      submit_delete(view, identity, identity.account_label)
      refute Repo.get(UpstreamIdentity, identity.id)
    end
  end

  test "Any status and Deleted surface historical accounts with only Delete available", %{conn: conn} do
    pool = pool_fixture()
    %{identity: current} = upstream_assignment_fixture(pool, %{account_label: "Current account"})
    %{identity: legacy, assignment: assignment} = legacy_account(pool)

    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    assert has_element?(view, "#upstream-account-#{current.id}")
    assert has_element?(view, "#upstream-account-#{legacy.id}", "Historical account")
    assert has_element?(view, "#delete-upstream-account-#{legacy.id}:not([disabled])", "Delete")
    assert has_element?(view, "#upstream-account-actions-menu-#{legacy.id}:not([title])")

    for action <- ~w(rename pause reactivate refresh saved-reset-policy replace-auth-json oauth-relink reinvite) do
      refute has_element?(view, "##{action}-upstream-account-#{legacy.id}")
    end

    render_click(view, "select_status_filter", %{"status" => "deleted"})
    assert_patch(view, ~p"/admin/upstreams?status=deleted")
    assert has_element?(view, "#filters_status[value='deleted']")
    assert has_element?(view, "#upstream-account-#{legacy.id}")
    refute has_element?(view, "#upstream-account-#{current.id}")

    view |> element("#delete-upstream-account-#{legacy.id}") |> render_click()
    assert has_element?(view, "#delete-upstream-account-dialog", "permanently removes the account")
    assert has_element?(view, "#delete-upstream-account-dialog", "Shared request accounting remains without an account association")

    submit_delete(view, legacy, "wrong label")
    assert has_element?(view, "#delete-upstream-account-form", "type the account label exactly")
    assert Repo.get(UpstreamIdentity, legacy.id)

    submit_delete(view, legacy, legacy.account_label)
    refute Repo.get(UpstreamIdentity, legacy.id)
    refute Repo.get(PoolUpstreamAssignment, assignment.id)
    refute has_element?(view, "#upstream-account-#{legacy.id}")
    refute has_element?(view, "#delete-upstream-account-dialog")
  end

  test "historical cockpit exposes Delete and rejects operational actions", %{conn: conn} do
    %{identity: identity} = legacy_account(pool_fixture())
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")

    assert has_element?(view, "#cockpit-delete-upstream-account-#{identity.id}:not([disabled])")

    for action <- ~w(rename pause reactivate refresh oauth-relink replace-auth-json redeem-saved-reset) do
      assert has_element?(view, "#cockpit-#{action}-upstream-account-#{identity.id}[disabled]")
    end

    assert has_element?(view, "#cockpit-download-reset-calendar-#{identity.id}[disabled]")

    render_click(view, "pause_account", %{"id" => identity.id})
    assert Repo.get!(UpstreamIdentity, identity.id).status == "deleted"

    view |> element("#cockpit-delete-upstream-account-#{identity.id}") |> render_click()

    view
    |> element("#cockpit-delete-upstream-account-form")
    |> render_submit(%{"upstream_delete" => %{"id" => identity.id, "confirmation_label" => identity.account_label}})

    assert_redirect(view, ~p"/admin/upstreams")
    refute Repo.get(UpstreamIdentity, identity.id)
  end

  test "historical Pool visibility does not grant deletion across inaccessible Pools", %{scope: owner_scope} do
    visible_pool = pool_fixture()
    hidden_pool = pool_fixture()
    %{identity: shared, assignment: assignment} = legacy_account(visible_pool)
    %{identity: hidden} = legacy_account(hidden_pool)

    %PoolUpstreamAssignment{
      pool_id: hidden_pool.id,
      upstream_identity_id: shared.id,
      assignment_label: "Historical assignment",
      status: "deleted",
      health_status: "active",
      eligibility_status: "ineligible",
      created_at: assignment.created_at,
      updated_at: assignment.updated_at
    }
    |> Repo.insert!()

    %{user: admin, temporary_password: password} =
      operator_fixture(owner_scope, %{"password_change_required" => "false"})

    operator_pool_assignment_fixture(admin, visible_pool, created_by_user_id: owner_scope.user.id)
    assert {:ok, %{token: token}} = Accounts.login_user(%{"email" => admin.email, "password" => password})
    conn = log_in_user(build_conn(), admin, token)
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams?status=deleted")

    assert has_element?(view, "#upstream-account-#{shared.id}")
    refute has_element?(view, "#upstream-account-#{hidden.id}")
    assert has_element?(view, "#delete-upstream-account-#{shared.id}[disabled]")
    render_click(view, "open_delete_account", %{"id" => shared.id})
    refute has_element?(view, "#delete-upstream-account-dialog")
    assert Repo.get(UpstreamIdentity, shared.id)
  end

  test "an account removed while confirmation is open closes the stale dialog", %{conn: conn} do
    identity = active_upstream_identity_fixture(%{account_label: "Detached account"})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    view |> element("#delete-upstream-account-#{identity.id}") |> render_click()
    Repo.delete!(identity)
    submit_delete(view, identity, identity.account_label)
    refute has_element?(view, "#delete-upstream-account-dialog")
    refute has_element?(view, "#upstream-account-#{identity.id}")
  end

  test "deletion waits for live requests and disables Delete until cleanup completes", %{conn: conn} do
    pool = pool_fixture()
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool, %{account_label: "Busy account"})
    %{api_key: api_key} = api_key_fixture(pool)
    request = request_fixture(%{pool: pool, api_key: api_key}, %{status: "in_progress", completed_at: nil})
    attempt_fixture(request, assignment, %{status: "in_progress", completed_at: nil})
    assert {:ok, _result} = PoolAssignments.delete_pool_assignment(pool, assignment)

    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    view |> element("#delete-upstream-account-#{identity.id}") |> render_click()
    submit_delete(view, identity, identity.account_label)

    assert has_element?(view, "#flash-info", "Upstream account deletion queued")
    refute has_element?(view, "#delete-upstream-account-dialog")
    assert has_element?(view, "#upstream-account-#{identity.id}", "Deletion in progress")
    assert has_element?(view, "#delete-upstream-account-#{identity.id}[disabled]")
    assert Repo.get(UpstreamIdentity, identity.id)

    {:ok, cockpit, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    assert has_element?(cockpit, "#upstream-cockpit-status", "Deletion in progress")
    assert has_element?(cockpit, "#cockpit-delete-upstream-account-#{identity.id}[disabled]")
  end

  test "a changed account label is checked again on submission", %{conn: conn} do
    identity = active_upstream_identity_fixture(%{account_label: "Original account"})
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    view |> element("#delete-upstream-account-#{identity.id}") |> render_click()
    identity |> Ecto.Changeset.change(account_label: "Renamed account") |> Repo.update!()
    submit_delete(view, identity, identity.account_label)

    assert has_element?(view, "#delete-upstream-account-dialog")
    assert Repo.get!(UpstreamIdentity, identity.id).account_label == "Renamed account"
  end

  test "Delete requires removal from every Pool, including disabled assignments", %{conn: conn} do
    first_pool = pool_fixture()
    second_pool = pool_fixture()
    %{identity: identity, assignment: first_assignment} = upstream_assignment_fixture(first_pool)

    assert {:ok, second_assignment} =
             PoolAssignments.create_pool_assignment(second_pool, identity, %{status: "disabled"})

    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    reason = "Remove this account from all Pools before deleting it."
    assert has_element?(view, "#delete-upstream-account-#{identity.id}[disabled][title='#{reason}']")
    render_click(view, "open_delete_account", %{"id" => identity.id})
    refute has_element?(view, "#delete-upstream-account-dialog")

    assert {:ok, _result} = PoolAssignments.delete_pool_assignment(first_pool, first_assignment)
    {:ok, cockpit, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
    assert has_element?(cockpit, "#cockpit-delete-upstream-account-#{identity.id}[disabled][title='#{reason}']")

    assert {:ok, _result} = PoolAssignments.delete_pool_assignment(second_pool, second_assignment)
    {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
    assert has_element?(view, "#delete-upstream-account-#{identity.id}:not([disabled])")
    view |> element("#delete-upstream-account-#{identity.id}") |> render_click()
    submit_delete(view, identity, identity.account_label)
    refute Repo.get(UpstreamIdentity, identity.id)
  end

  defp legacy_account(pool) do
    upstream_assignment_fixture(pool, %{
      account_label: "Historical account",
      identity_status: "deleted",
      assignment_status: "deleted",
      eligibility_status: "ineligible"
    })
  end

  defp submit_delete(view, identity, label) do
    view
    |> element("#delete-upstream-account-form")
    |> render_submit(%{"upstream_delete" => %{"id" => identity.id, "confirmation_label" => label}})
  end
end
