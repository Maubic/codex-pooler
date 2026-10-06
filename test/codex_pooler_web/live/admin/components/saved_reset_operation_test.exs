defmodule CodexPoolerWeb.Admin.SavedResetOperationTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Admin.Components
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.SavedResetOperationProjection, as: Projection
  alias CodexPoolerWeb.Admin.UpstreamPageComponents.SavedResetOperation
  alias CodexPoolerWeb.DateTimeDisplay

  @identity "00000000-0000-4000-8000-000000000001"
  @now ~U[2026-10-05 12:00:00Z]

  @tag :receipt_state_rendering
  test "renders actual applied, candidate, confirmed and provisional receipt states" do
    for {phase, confirmation, expected} <- [
          {"consumed_pending_probe", nil, :pending},
          {"consumed_pending_probe", %{challenged_evidence_state: :candidate_progressing}, :candidate},
          {"confirmed_by_quota", nil, :quota_confirmed},
          {"confirmed_by_upstream", nil, :request_verified}
        ] do
      operation = project(%{redemption: applied(phase), confirmation: confirmation})
      html = receipt(operation)
      record_html("#{expected}", html)
      document = LazyHTML.from_fragment(html)
      refute Enum.empty?(LazyHTML.query(document, "section[data-provider-outcome='applied'][data-verification-state='#{expected}']"))
      refute Enum.empty?(LazyHTML.query(document, "h3#saved-reset-operation-heading-bank-#{@identity}"))
      assert LazyHTML.text(document) =~ "Latest account reset"
      refute Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-consumed-at']"))
      refute Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-deadline-at']"))
      refute Enum.empty?(LazyHTML.query(document, "button#saved-reset-status-refresh-bank-#{@identity}[phx-click='refresh_saved_reset_status'][phx-value-id='#{@identity}']"))
      refute Enum.empty?(LazyHTML.query(document, "[role='status'][aria-live='polite'][aria-atomic='true']"))
      assert Enum.empty?(LazyHTML.query(document, "[role='status'] dl"))
      refute html =~ "progressbar"
      refute html =~ "alert-warning"
    end
  end

  @tag :receipt_state_rendering
  test "a newer queued request and older confirmed account result remain separate" do
    operation = project(%{redemption: applied("confirmed_by_quota"), request_summary: %{open: %{state: :queued, requested_at: @now}, latest_terminal: nil}})
    html = receipt(operation)
    record_html("queued-old-confirmed", html)
    document = LazyHTML.from_fragment(html)
    assert LazyHTML.query(document, "[data-role='saved-reset-request']") |> LazyHTML.text() =~ "Request accepted"
    assert LazyHTML.query(document, "[data-role='saved-reset-latest']") |> LazyHTML.text() =~ "Quota confirmed"
    assert Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-request'] [data-role='saved-reset-consumed-at']"))
    assert Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-latest'] [data-role='saved-reset-requested-at']"))
  end

  @tag :receipt_state_rendering
  test "list and open bank for one identity have unique ids and local aria references" do
    operation = project(%{redemption: applied("consumed_pending_probe")})
    html = receipt(operation, :list) <> receipt(operation, :bank)
    record_html("list-bank", html)
    document = LazyHTML.from_fragment(html)
    assert Enum.count(LazyHTML.query(document, "section[data-role='saved-reset-operation']")) == 2
    ids = document |> LazyHTML.query("[id]") |> Enum.flat_map(&LazyHTML.attribute(&1, "id"))
    assert ids == Enum.uniq(ids)

    for surface <- [:list, :bank], attribute <- ["aria-labelledby", "aria-describedby"] do
      node = LazyHTML.query(document, "#saved-reset-operation-#{surface}-#{@identity}")

      references = LazyHTML.attribute(node, attribute)
      assert length(references) == 1

      for id <- references do
        refute Enum.empty?(LazyHTML.query(node, "##{id}"))
      end
    end
  end

  @tag :receipt_state_rendering
  test "queued and processing manual requests carry no provider success claim" do
    for state <- [:queued, :processing] do
      operation = project(%{request_summary: %{open: %{state: state, requested_at: @now, scheduled_at: DateTime.add(@now, 30)}, latest_terminal: nil}})
      html = receipt(operation)
      document = LazyHTML.from_fragment(html)
      assert LazyHTML.text(document) =~ "Request accepted"
      assert LazyHTML.text(document) =~ "Provider application has not been confirmed yet."
      assert Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-latest']"))
      assert Enum.count(LazyHTML.query(document, "[data-role='saved-reset-requested-at'], [data-role='saved-reset-scheduled-at']")) == 2
      refute html =~ "The reset was applied"
      record_html("#{state}", html)
    end
  end

  @tag :receipt_ambiguity_and_no_fake_success
  test "unexpected states and synthetic private metadata cannot enter status attributes" do
    operation = project(%{request_summary: :unavailable}) |> Map.merge(%{provider_outcome: :synthetic_private_outcome, verification: :synthetic_private_verification, attempt_id: "synthetic-private-attempt", job_args: "synthetic-private-job", raw_error: "synthetic-private-error"})
    html = receipt(operation)
    document = LazyHTML.from_fragment(html)
    refute Enum.empty?(LazyHTML.query(document, "[data-provider-outcome='unknown'][data-verification-state='unknown']"))
    for private <- ["synthetic_private_outcome", "synthetic_private_verification", "synthetic-private-attempt", "synthetic-private-job", "synthetic-private-error"], do: refute(html =~ private)
    assert Enum.empty?(LazyHTML.query(document, "[data-attempt-id], [data-job-args], [data-refresh-cursor], .animate-spin, [role='progressbar']"))
  end

  @tag :receipt_ambiguity_and_no_fake_success
  test "every uncertain, no-op, paused, stopped and legacy state stays truthful" do
    cases = [
      {%{redemption: %{"phase" => "consuming", "started_at" => DateTime.to_iso8601(DateTime.add(@now, -1))}}, "Reset request in progress"},
      {%{redemption: %{"phase" => "consuming", "started_at" => "2020-01-01T00:00:00Z"}}, "Reset outcome not confirmed"},
      {%{redemption: %{"phase" => "consuming", "result" => %{"applied" => true, "code" => "synthetic-private-code"}}}, "Reset outcome not confirmed"},
      {%{redemption: %{"phase" => "consume_not_applied", "result" => %{"applied" => false, "code" => "no_credit"}}}, "No saved reset was available"},
      {%{redemption: %{"phase" => "consume_not_applied", "result" => %{"applied" => false, "code" => "nothing_to_reset"}}}, "No eligible quota needed resetting"},
      {%{redemption: %{"phase" => "consume_not_applied", "result" => %{"applied" => false, "code" => "consume_not_applied"}, "provider_replay" => %{"version" => 1, "provider_dispatches" => 0}}}, "Reset was not applied"},
      {%{redemption: applied("reblocked")}, "Quota is still unavailable"},
      {%{redemption: applied("expired")}, "Quota confirmation timed out"},
      {%{redemption: Map.put(applied("consumed_pending_probe"), "deadline_at", DateTime.to_iso8601(DateTime.add(@now, -1)))}, "Confirmation deadline passed"},
      {%{redemption: %{"phase" => "confirmed_by_quota"}}, "Quota confirmed"},
      {%{redemption: applied("consumed_pending_probe"), usage_poll_pause: %{paused_until: DateTime.add(@now, 60)}}, "Quota checks are delayed"},
      {%{redemption: applied("consumed_pending_probe"), usage_poll_pause: :unavailable}, "Quota checks are delayed"},
      {%{redemption: applied("consumed_pending_probe"), view_paused?: true}, "Live updates are paused"},
      {%{redemption: applied("consumed_pending_probe"), view_connected?: false}, "Status updates are disconnected"},
      {%{request_summary: %{open: nil, latest_terminal: %{state: :stopped, requested_at: @now}}}, "Request stopped"},
      {%{request_summary: :unavailable}, "Request status unavailable"}
    ]

    for {context, headline} <- cases do
      html = context |> project() |> receipt()
      record_html("failure-#{Enum.find_index(cases, &(&1 == {context, headline}))}", html)
      document = LazyHTML.from_fragment(html)
      assert LazyHTML.text(document) =~ headline
      refute html =~ "synthetic-private-code"
      refute html =~ "Not reported"
      refute html =~ "Routing ready"
      refute html =~ "redeem_saved_reset"
      refute html =~ "Retry"
      assert Enum.empty?(LazyHTML.query(document, "[role='status'] [data-role='saved-reset-pause-until']"))
    end
  end

  @tag :receipt_ambiguity_and_no_fake_success
  test "queued-only request shows pause and disconnect observations without an account receipt" do
    for observation <- [%{view_paused?: true}, %{view_connected?: false}, %{usage_poll_pause: :unavailable}] do
      operation = project(Map.put(observation, :request_summary, %{open: %{state: :queued, requested_at: @now}, latest_terminal: nil}))
      html = receipt(operation)
      document = LazyHTML.from_fragment(html)
      assert LazyHTML.text(document) =~ operation.headline
      assert Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-latest']"))
      record_html("queued-#{operation.headline}", html)
    end
  end

  @tag :receipt_ambiguity_and_no_fake_success
  test "timestamps, live announcement and private projection fields remain separate" do
    operation = project(%{redemption: applied("consumed_pending_probe"), last_checked_at: @now}) |> Map.put(:saved_reset_refresh_cursor, "synthetic-private-cursor")
    changed_clock = Map.put(operation, :last_checked_at, "October 5, 2026 at 12:01 UTC")
    before_document = operation |> receipt() |> LazyHTML.from_fragment()
    after_document = changed_clock |> receipt() |> LazyHTML.from_fragment()
    assert LazyHTML.query(before_document, "[role='status']") |> LazyHTML.text() |> String.trim() == LazyHTML.query(after_document, "[role='status']") |> LazyHTML.text()
    refute receipt(operation) =~ "synthetic-private-cursor"
    refute Enum.empty?(LazyHTML.query(before_document, "[data-role='saved-reset-last-checked-at']"))
    assert Enum.empty?(LazyHTML.query(before_document, "[role='status'] [data-role='saved-reset-last-checked-at']"))
  end

  @tag :receipt_ambiguity_and_no_fake_success
  test "empty and consuming receipts omit absent future facts and require a closed surface" do
    empty = project(%{}) |> receipt()
    assert Enum.empty?(LazyHTML.query(LazyHTML.from_fragment(empty), "section"))
    consuming = project(%{redemption: %{"phase" => "consuming", "started_at" => DateTime.to_iso8601(@now)}}) |> receipt()
    assert Enum.empty?(LazyHTML.query(LazyHTML.from_fragment(consuming), "[data-role='saved-reset-consumed-at'], [data-role='saved-reset-deadline-at']"))
    assert_raise ArgumentError, fn -> receipt(project(%{}), :invalid_surface) end
  end

  @tag :receipt_state_rendering
  test "confirmation uses approved copy and distinct existing controls without owning editable form fields" do
    html = render_component(&Components.saved_reset_confirmation/1, %{identity_id: @identity, surface: :bank, id: "saved-reset-redemption-confirmation", confirm_id: "saved-reset-redemption-confirm", cancel_id: "saved-reset-redemption-cancel"})
    document = LazyHTML.from_fragment(html)
    record_html("confirmation", html)
    assert LazyHTML.text(document) =~ "Redeem one saved reset for this account? The provider may apply it before quota checks finish. Verification can take a few minutes. You can leave this view and return to check the status."
    assert LazyHTML.query(document, "#saved-reset-redemption-confirm[phx-click='redeem_saved_reset'][type='button']") |> LazyHTML.text() |> String.trim() == "Redeem one reset"
    assert LazyHTML.query(document, "#saved-reset-redemption-cancel[phx-click='cancel_saved_reset_redemption'][type='button']") |> LazyHTML.text() |> String.trim() == "Keep resets in bank"
    assert Enum.empty?(LazyHTML.query(document, "form, input, select"))
  end

  @tag :receipt_state_rendering
  test "renders current conditional readiness without recalculating it" do
    readiness = %{label: "Conditional availability", reason: "Availability depends on the model and transport.", routing_ready_now?: true}
    html = project(%{redemption: applied("confirmed_by_upstream"), serving_readiness: readiness}) |> receipt()
    document = LazyHTML.from_fragment(html)
    assert LazyHTML.query(document, "[data-role='saved-reset-serving-readiness']") |> LazyHTML.text() =~ readiness.reason
    refute html =~ "Routing ready"
    refute html =~ "routing_ready_now"
  end

  @tag :compact_list_presentation
  test "normal historical receipts add no list status even when updates are paused" do
    readiness = %{label: "Ready", reason: "Current quota is available.", routing_ready_now?: true}

    for context <- [
          %{},
          %{view_paused?: true},
          %{redemption: applied("confirmed_by_quota")},
          %{redemption: applied("confirmed_by_quota"), view_paused?: true},
          %{redemption: applied("confirmed_by_quota"), view_connected?: false},
          %{redemption: %{"phase" => "confirmed_by_quota"}, view_paused?: true},
          %{redemption: applied("confirmed_by_upstream"), view_paused?: true},
          %{request_summary: :unavailable, view_paused?: true},
          %{request_summary: %{open: nil, latest_terminal: %{state: :stopped, requested_at: @now}}, redemption: applied("confirmed_by_quota"), view_paused?: true},
          %{redemption: %{"phase" => "consume_not_applied", "result" => %{"applied" => false, "code" => "no_credit"}}}
        ] do
      operation = context |> Map.put(:serving_readiness, readiness) |> project()
      html = receipt(operation, :list)
      assert Enum.empty?(LazyHTML.query(LazyHTML.from_fragment(html), "[data-role='saved-reset-operation']"))
      record_html("normal-#{length(Map.keys(context))}-#{operation.headline}", "<main data-evidence-host>#{html}</main>")

      if operation.show_latest_receipt? do
        detail = receipt(operation, :bank)
        document = LazyHTML.from_fragment(detail)
        assert Enum.count(LazyHTML.query(document, "details:not([open]) [data-role='saved-reset-latest']")) == 1
        assert LazyHTML.text(document) =~ operation.detail
        record_html("historical-bank-#{operation.headline}", detail)
      end
    end
  end

  @tag :compact_list_presentation
  test "active and unresolved list status is a single compact row with the safe dialog action" do
    contexts = [
      %{request_summary: %{open: %{state: :queued, requested_at: @now}, latest_terminal: nil}},
      %{request_summary: %{open: %{state: :processing, requested_at: @now}, latest_terminal: nil}, redemption: applied("confirmed_by_quota")},
      %{redemption: applied("consumed_pending_probe")},
      %{redemption: applied("consumed_pending_probe"), view_paused?: true},
      %{redemption: applied("confirmed_by_upstream")},
      %{redemption: %{"phase" => "legacy_unknown"}},
      %{redemption: %{"phase" => "consuming", "started_at" => "2020-01-01T00:00:00Z"}}
    ]

    for {context, index} <- Enum.with_index(contexts) do
      operation = project(context)
      html = receipt(operation, :list)
      document = LazyHTML.from_fragment(html)
      assert Enum.count(LazyHTML.query(document, "[data-role='saved-reset-operation'][data-presentation='compact']")) == 1
      assert Enum.count(LazyHTML.query(document, "[data-role='saved-reset-headline']")) == 1
      assert Enum.count(LazyHTML.query(document, "button[data-role='saved-reset-view-status'][type='button'][phx-value-id='#{@identity}']")) == 1
      assert LazyHTML.query(document, "[data-role='saved-reset-view-status']") |> LazyHTML.text() |> String.trim() == "View status"
      assert Enum.empty?(LazyHTML.query(document, "dl, [data-role='saved-reset-serving-readiness'], [data-role='saved-reset-latest'], [data-role='saved-reset-observation']"))
      refute html =~ "border-base-300"
      refute html =~ "Refresh reads"
      record_html("compact-list-#{index}", html)
    end
  end

  @tag :compact_list_presentation
  test "terminal recovery attention follows current readiness and all detail surfaces keep their own disclosure" do
    for phase <- ["reblocked", "expired"], ready? <- [true, false] do
      operation = project(%{redemption: applied(phase), serving_readiness: %{label: "Current readiness", reason: "Existing readiness fact", routing_ready_now?: ready?}})
      document = operation |> receipt(:list) |> LazyHTML.from_fragment()
      assert Enum.empty?(LazyHTML.query(document, "[data-role='saved-reset-operation']")) == ready?
      record_html("terminal-list-#{phase}-#{ready?}", "<main data-evidence-host>#{receipt(operation, :list)}</main>")
    end

    for surface <- [:bank, :cockpit] do
      operation = project(%{redemption: applied("consumed_pending_probe")})
      document = operation |> receipt(surface) |> LazyHTML.from_fragment()
      assert Enum.count(LazyHTML.query(document, "details#saved-reset-operation-#{surface}-#{@identity}-details[open][data-preserve-open]")) == 1
      assert Enum.count(LazyHTML.query(document, "#saved-reset-status-refresh-#{surface}-#{@identity}[data-saved-reset-action='status-refresh'][data-server-disabled='false']")) == 1
      record_html("active-detail-#{surface}", receipt(operation, surface))
    end
  end

  defp record_html(name, html) do
    if directory = System.get_env("SAVED_RESET_COMPONENT_EVIDENCE_DIR") do
      File.mkdir_p!(directory)
      filename = name |> String.downcase() |> String.replace(~r/[^a-z0-9-]+/, "-")
      File.write!(Path.join(directory, "#{filename}.html"), html)
    end
  end

  defp receipt(operation, surface \\ :bank), do: render_component(&SavedResetOperation.saved_reset_operation/1, %{operation: operation, identity_id: @identity, surface: surface})
  defp project(context), do: Projection.project(Map.merge(%{snapshot_at: @now, datetime_preferences: DateTimeDisplay.preferences_for_user(nil)}, context))
  defp applied(phase), do: %{"phase" => phase, "started_at" => DateTime.to_iso8601(DateTime.add(@now, -20)), "consumed_at" => DateTime.to_iso8601(DateTime.add(@now, -10)), "deadline_at" => DateTime.to_iso8601(DateTime.add(@now, 180)), "result" => %{"applied" => true, "code" => "reset"}}
end
