defmodule CodexPoolerWeb.Admin.SavedResetCockpitReceiptTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Admin.UpstreamCockpitComponents.Charts
  alias CodexPoolerWeb.Admin.UpstreamCockpitLive.SavedResetWorkflow
  alias CodexPoolerWeb.Admin.UpstreamCockpitReadModel
  alias CodexPoolerWeb.Admin.UpstreamPageComponents.AccountCard.SavedResetMeter
  alias CodexPoolerWeb.DateTimeDisplay

  setup :register_and_log_in_user

  setup %{scope: scope} do
    now = DateTime.utc_now()
    pool = pool_fixture(%{created_by_user_id: scope.user.id})
    {:ok, fake} = FakeUpstream.start_link({:json_response, 500, %{"error" => "sample-private-provider-value"}})
    on_exit(fn -> FakeUpstream.stop(fake) end)

    %{identity: identity} =
      upstream_assignment_fixture(pool, %{
        account_label: "Sample reset receipt account",
        identity_metadata: %{
          "usage_base_url" => FakeUpstream.url(fake),
          "base_url" => FakeUpstream.url(fake),
          "saved_resets" => %{
            "status" => "reported",
            "available_count" => 2,
            "observed_at" => DateTime.to_iso8601(now),
            "source" => "codex_usage_api",
            "available_expirations" => [%{"expires_at" => DateTime.to_iso8601(DateTime.add(now, 7, :day)), "first_seen_at" => DateTime.to_iso8601(now)}]
          }
        }
      })

    %{identity: identity, now: now, fake: fake}
  end

  for {state, outcome, verification, headline} <- [
        {:consuming, :unknown, :not_started, "Reset request in progress"},
        {:ambiguous, :unknown, :not_started, "Reset outcome not confirmed"},
        {:applied_pending, :applied, :pending, "Reset applied — verifying quota"},
        {:confirmed, :applied, :quota_confirmed, "Quota confirmed"},
        {:provisional, :applied, :request_verified, "Recovery verified by a request"},
        {:legacy, :unknown, :pending, "Reset outcome not confirmed"}
      ] do
    @state state
    @outcome outcome
    @verification verification
    @headline headline

    test "cockpit #{@state} uses the modern receipt and retains bank facts", context do
      %{identity: identity, now: now, conn: conn} = context
      metadata = Map.put(identity.metadata, "saved_reset_redemption", redemption(@state, now))
      identity |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()
      {:ok, view, _html} = live(conn, ~p"/admin/upstreams/#{identity.id}")
      html = render_async(view)
      capture_html!(@state, html)
      operation = "#saved-reset-operation-cockpit-#{identity.id}"

      assert has_element?(view, "#{operation}[data-provider-outcome='#{@outcome}'][data-verification-state='#{@verification}']")
      # The headline is read from the disclosure summary that labels the receipt; inside the section it only repeats in the screen-reader live region.
      assert has_element?(view, "#saved-reset-operation-heading-cockpit-#{identity.id}", @headline)
      assert has_element?(view, "#upstream-quota-saved-reset-meter-bar[aria-label='2 saved resets'][aria-valuenow='2']")
      refute has_element?(view, "#upstream-quota-saved-reset-meter-confirmation")
      refute has_element?(view, "#upstream-quota-saved-reset-meter [data-role='upstream-saved-reset-consumed-at']")
      refute has_element?(view, "#upstream-quota-saved-reset-meter [data-role='upstream-saved-reset-deadline']")
      assert has_element?(view, "#cockpit-saved-reset-expiration-row-0")
      assert has_element?(view, "#saved-reset-policy-form[data-saved-reset-form]")
      assert has_element?(view, "#saved-reset-policy-submit[data-saved-reset-action='save-policy']")

      if @outcome == :unknown do
        refute has_element?(view, "#{operation} [data-role='saved-reset-consumed-at']")
        refute has_element?(view, "#{operation} [data-role='saved-reset-deadline-at']")
      else
        assert has_element?(view, "#{operation} [data-role='saved-reset-consumed-at']")
        assert has_element?(view, "#{operation} [data-role='saved-reset-deadline-at']")
      end

      refute html =~ "sample-private-provider-value"
      record_evidence!(@state, context.fake)
    end
  end

  test "legacy-only quota component retains its bounded confirmation and inventory", context do
    %{identity: identity, now: now, scope: scope} = context
    identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption(:applied_pending, now))) |> Repo.update!()
    assert {:ok, cockpit} = UpstreamCockpitReadModel.load_visible_without_request_metrics(scope, identity.id)
    html = render_component(&Charts.quota_section/1, %{cockpit: Map.delete(cockpit, :saved_reset_operation), saved_reset_policy_form: SavedResetWorkflow.policy_form(cockpit.saved_reset_policy), datetime_preferences: DateTimeDisplay.preferences_for_user(nil)})
    document = LazyHTML.from_fragment(html)
    capture_html!(:legacy_component, html)
    assert Enum.count(LazyHTML.query(document, "#upstream-quota-saved-reset-meter-confirmation[data-confirmation-state='awaiting_confirmation']")) == 1
    assert Enum.count(LazyHTML.query(document, "#upstream-quota-saved-reset-meter-bar[aria-valuenow='2']")) == 1
    assert Enum.count(LazyHTML.query(document, "#cockpit-saved-reset-expiration-row-0")) == 1
    record_evidence!(:legacy_component, context.fake)
  end

  for state <- [:consuming, :ambiguous] do
    @state state

    test "list #{@state} meter accessibility excludes hidden legacy confirmation", context do
      %{identity: identity, now: now, conn: conn} = context
      identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption(@state, now))) |> Repo.update!()
      {:ok, view, _html} = live(conn, ~p"/admin/upstreams")
      html = render_async(view)
      capture_html!("list_#{@state}", html)
      meter = "#upstream-account-#{identity.id}-saved-reset-meter"
      operation = "#saved-reset-operation-list-#{identity.id}"

      assert has_element?(view, "#{meter}-bar[aria-label='2 saved resets'][aria-valuenow='2']")
      refute has_element?(view, "#{meter}-confirmation")
      refute has_element?(view, "#{meter} [data-role='upstream-saved-reset-consumed-at'], #{meter} [data-role='upstream-saved-reset-deadline']")
      assert has_element?(view, "#{operation}[data-provider-outcome='unknown'][data-presentation='compact']")
      assert has_element?(view, "#{operation} [data-role='saved-reset-view-status']", "View status")
      refute render(element(view, meter)) =~ "Not reported"
      refute render(element(view, meter)) =~ "Reset consumed"

      view |> element("#{operation} [data-role='saved-reset-view-status']") |> render_click()
      assert has_element?(view, "#saved-reset-operation-bank-#{identity.id}[data-provider-outcome='unknown']")
      assert has_element?(view, "#saved-reset-expiration-row-0")
      assert has_element?(view, "#saved-reset-policy-form[data-saved-reset-form]")
      refute has_element?(view, "#saved-reset-operation-bank-#{identity.id} [data-role='saved-reset-consumed-at']")
      refute html =~ "sample-private-provider-value"
      capture_html!("list_#{@state}_bank", render(view))
      record_evidence!("list_#{@state}", context.fake)
    end
  end

  test "meter show_confirmation=false omits all legacy copy while true retains it", context do
    %{identity: identity, now: now, scope: scope} = context
    identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption(:applied_pending, now))) |> Repo.update!()
    assert {:ok, account} = UpstreamCockpitReadModel.load_visible_without_request_metrics(scope, identity.id)
    attrs = %{id: "sample-reset-meter", saved_resets: account.saved_resets, saved_reset_policy: account.saved_reset_policy, saved_reset_confirmation: account.saved_reset_confirmation}
    hidden = render_component(&SavedResetMeter.saved_reset_meter/1, Map.put(attrs, :show_confirmation, false))
    visible = render_component(&SavedResetMeter.saved_reset_meter/1, Map.put(attrs, :show_confirmation, true))
    capture_html!(:meter_confirmation_hidden, hidden)
    capture_html!(:meter_confirmation_visible, visible)
    hidden_document = LazyHTML.from_fragment(hidden)
    visible_document = LazyHTML.from_fragment(visible)

    assert Enum.count(LazyHTML.query(hidden_document, "#sample-reset-meter-bar[aria-label='2 saved resets'][aria-valuenow='2']")) == 1
    assert Enum.empty?(LazyHTML.query(hidden_document, "#sample-reset-meter-confirmation, [data-role='upstream-saved-reset-consumed-at']"))
    refute hidden =~ "Reset consumed"
    refute hidden =~ "Awaiting confirmation"
    assert Enum.count(LazyHTML.query(visible_document, "#sample-reset-meter-confirmation[data-confirmation-state='awaiting_confirmation']")) == 1
    assert Enum.count(LazyHTML.query(visible_document, "#sample-reset-meter-bar[aria-label*='Awaiting confirmation'][aria-valuenow='2']")) == 1
    record_evidence!(:meter_confirmation_switch, context.fake)
  end

  defp redemption(:consuming, now), do: %{"status" => "redeeming", "phase" => "consuming", "started_at" => DateTime.to_iso8601(now)}

  defp redemption(:ambiguous, now) do
    %{"status" => "redeeming", "phase" => "consuming", "started_at" => DateTime.to_iso8601(DateTime.add(now, -5, :minute)), "result" => %{"applied" => true, "code" => "sample-private-provider-value"}}
  end

  defp redemption(:legacy, now), do: %{"phase" => "consumed_pending_probe", "started_at" => DateTime.to_iso8601(DateTime.add(now, -3, :minute))}

  defp redemption(state, now) do
    phase = %{applied_pending: "consumed_pending_probe", confirmed: "confirmed_by_quota", provisional: "confirmed_by_upstream"}[state]
    %{"status" => "succeeded", "phase" => phase, "started_at" => DateTime.to_iso8601(DateTime.add(now, -3, :minute)), "consumed_at" => DateTime.to_iso8601(DateTime.add(now, -2, :minute)), "deadline_at" => DateTime.to_iso8601(DateTime.add(now, 13, :minute)), "result" => %{"applied" => true, "code" => "reset"}}
  end

  defp record_evidence!(state, fake) do
    counts = FakeUpstream.physical_counts(fake)
    assert Enum.all?(Map.values(counts), &(&1 == 0))
    assert FakeUpstream.requests(fake) == []
    assert :ok = FakeUpstream.stop(fake)
    refute Process.alive?(fake.pid)
    refute Process.alive?(fake.server)
    refute Process.alive?(fake.supervisor)

    case System.get_env("SAVED_RESET_COCKPIT_RECEIPT_EVIDENCE_DIR") do
      nil ->
        :ok

      directory ->
        File.mkdir_p!(directory)
        File.write!(Path.join(directory, "#{state}.json"), Jason.encode!(%{scenario: state, physical_counts: counts, owned_fake_stopped?: true}, pretty: true))
    end
  end

  defp capture_html!(state, html) do
    case System.get_env("SAVED_RESET_COCKPIT_RECEIPT_EVIDENCE_DIR") do
      nil ->
        :ok

      directory ->
        File.mkdir_p!(directory)
        File.write!(Path.join(directory, "#{state}.html"), html)
    end
  end
end
