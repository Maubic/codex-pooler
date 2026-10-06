defmodule CodexPoolerWeb.Admin.UpstreamAccountsReadModel.SavedResetProjectionTest do
  use ExUnit.Case, async: true

  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.{SavedResetOperationProjection, SavedResetProjection}
  alias CodexPoolerWeb.DateTimeDisplay

  @prefs DateTimeDisplay.preferences_for_user(nil)

  test "open manual request presentation refuses another submission without changing domain facts" do
    account = %{
      identity: %{status: "active"},
      reauth_required?: false,
      refresh_status: "imported",
      secret_status: :present,
      assignments: [%{id: "trusted-assignment"}],
      saved_resets: %{reported?: true, available?: true, in_progress?: false}
    }

    assert SavedResetProjection.redemption_action(account) == %{available?: true, reason: nil}

    for state <- [:queued, :processing] do
      projected = Map.put(account, :saved_reset_operation, %{request: %{state: state}})
      assert SavedResetProjection.redemption_action(projected) == %{available?: false, reason: "saved reset request is already accepted"}
      assert projected.saved_resets == account.saved_resets
    end

    for state <- [:none, :completed, :discarded, :cancelled, :stopped] do
      projected = Map.put(account, :saved_reset_operation, %{request: %{state: state}})
      assert SavedResetProjection.redemption_action(projected).available?
    end

    unreadable = Map.put(account, :saved_reset_operation, %{request: %{state: :unavailable}})
    assert SavedResetProjection.redemption_action(unreadable) == %{available?: false, reason: "saved reset request status is unavailable"}
  end

  # One rule for the bank, the cockpit and both workflows: the recorded status holds the action back with its
  # reason, so a disabled Redeem always says why.
  test "the recorded status holds back another redemption with its reason" do
    now = ~U[2026-07-14 03:30:00.000000Z]

    account = fn record ->
      operation = SavedResetOperationProjection.project(%{snapshot_at: now, datetime_preferences: @prefs, redemption: record})
      saved_resets = SavedResetProjection.snapshot(%{"saved_reset_redemption" => record, "saved_resets" => %{"status" => "reported", "available_count" => 1}}, @prefs)
      %{identity: %{status: "active"}, reauth_required?: false, refresh_status: "imported", secret_status: :present, assignments: [%{id: "trusted-assignment"}], saved_resets: saved_resets, saved_reset_operation: operation}
    end

    unresolved = "the last saved reset is unresolved; another redemption waits until it resolves"
    malformed_replay = %{"phase" => "consume_not_applied", "result" => %{"code" => "consume_not_applied", "applied" => false}, "provider_replay" => %{"version" => 1, "provider_dispatches" => 1}}

    for {record, reason} <- [
          {%{"phase" => "confirmed_by_quota"}, unresolved},
          {malformed_replay, unresolved},
          {metadata("confirmed_by_upstream", %{"status" => "succeeded"})["saved_reset_redemption"], "the last saved reset is still in progress"}
        ] do
      assert SavedResetProjection.redemption_action(account.(record)) == %{available?: false, reason: reason}, inspect(record["phase"])
      assert SavedResetProjection.status_hold(account.(record).saved_reset_operation) == reason
    end

    # A record without a lifecycle phase never resolves and the claim ignores it, so it keeps the action.
    for record <- [%{"status" => "failed", "result" => %{"code" => "transport_error", "applied" => false}}, %{"status" => "completed"}] do
      assert SavedResetProjection.redemption_action(account.(record)) == %{available?: true, reason: nil}
      assert SavedResetProjection.status_hold(account.(record).saved_reset_operation) == nil
    end
  end

  # The action follows the claim's own guard (`RedemptionLifecycle.blocks_new_redemption?/2`) through the
  # domain snapshot, so the bank never offers a redemption the claim refuses before the provider.
  test "a redemption the claim would refuse is not offered" do
    account = fn redemption_metadata ->
      %{
        identity: %{status: "active"},
        reauth_required?: false,
        refresh_status: "imported",
        secret_status: :present,
        assignments: [%{id: "trusted-assignment"}],
        saved_resets: SavedResetProjection.snapshot(Map.put(redemption_metadata, "saved_resets", %{"status" => "reported", "available_count" => 1}), @prefs)
      }
    end

    assert %{available?: false, reason: "the last saved reset was applied and quota is still blocked; another redemption waits until a usage report shows quota recovered"} =
             SavedResetProjection.redemption_action(account.(metadata("reblocked")))

    assert %{available?: false, reason: "the last saved reset was not confirmed in time; another redemption waits until a usage report shows quota recovered"} =
             SavedResetProjection.redemption_action(account.(metadata("expired")))

    assert %{available?: false, reason: "the last saved reset is unresolved; another redemption waits until it resolves"} =
             SavedResetProjection.redemption_action(account.(metadata("phase-from-a-newer-release")))

    # A reblock that consumed nothing, a confirmed reset and a legacy record without a phase stay redeemable.
    not_applied = metadata("reblocked", %{"result" => %{"code" => "no_credit", "applied" => false}, "consumed_at" => nil})
    assert SavedResetProjection.redemption_action(account.(not_applied)) == %{available?: true, reason: nil}
    assert SavedResetProjection.redemption_action(account.(metadata("confirmed_by_quota"))) == %{available?: true, reason: nil}
    assert SavedResetProjection.redemption_action(account.(%{"saved_reset_redemption" => %{"status" => "completed"}})) == %{available?: true, reason: nil}
  end

  defp metadata(phase, extra \\ %{}) do
    consumed_at = ~U[2026-07-14 03:20:00.000000Z]

    redemption =
      Map.merge(
        %{
          "status" => "redeeming",
          "phase" => phase,
          "attempt_id" => "attempt-1",
          "generation" => 3,
          "consumed_at" => DateTime.to_iso8601(consumed_at),
          "deadline_at" => consumed_at |> DateTime.add(15, :minute) |> DateTime.to_iso8601(),
          "result" => %{"code" => "reset", "applied" => true}
        },
        extra
      )

    %{"saved_reset_redemption" => redemption}
  end

  # The receipt owns every operator-facing lifecycle fact; the snapshot keeps only the phase the action reads.
  test "keeps only the recognized lifecycle phase" do
    for phase <- ["consuming", "consumed_pending_probe", "confirmed_by_upstream", "confirmed_by_quota", "reblocked", "expired", "consume_not_applied"] do
      assert SavedResetProjection.snapshot(metadata(phase), @prefs).reset_lifecycle == %{phase: phase}
    end

    assert SavedResetProjection.snapshot(metadata("phase-from-a-newer-release"), @prefs).reset_lifecycle == nil
  end

  test "renders an applied reblock recovered by quota as confirmed" do
    snapshot =
      SavedResetProjection.snapshot(
        metadata("confirmed_by_quota", %{
          "status" => "succeeded",
          "terminal_reason" => "converged_confirmed_by_quota"
        }),
        @prefs
      )

    assert snapshot.reset_lifecycle == %{phase: "confirmed_by_quota"}
  end

  @tag :saved_reset_redemption_cause
  test "projects only the five recognized automatic redemption causes" do
    causes = %{
      {"gateway_auto", "exhausted"} => "Request · long-window quota exhausted",
      {"gateway_auto", "threshold"} => "Request · quota threshold",
      {"scheduled_expiry_rescue", "exhausted"} => "Scheduled · long-window quota exhausted",
      {"scheduled_expiry_rescue", "threshold"} => "Scheduled · quota threshold",
      {"scheduled_expiry_rescue", "last_call"} => "Scheduled · last call"
    }

    for {{trigger_kind, trigger_detail}, label} <- causes do
      snapshot =
        SavedResetProjection.snapshot(
          metadata("confirmed_by_upstream", %{
            "trigger_kind" => trigger_kind,
            "trigger_detail" => trigger_detail
          }),
          @prefs
        )

      assert snapshot.last_auto_redemption_cause == %{label: label}
    end
  end

  @tag :saved_reset_redemption_cause
  test "fails closed without rendering redemption metadata" do
    sensitive_sentinel = "saved-reset-projection-sensitive-sentinel"

    for redemption <- [
          %{"trigger_kind" => "admin_manual", "trigger_detail" => "exhausted"},
          %{"trigger_kind" => "gateway_auto", "trigger_detail" => "unrecognized"},
          %{"trigger_kind" => "scheduled_expiry_rescue"},
          %{"trigger_detail" => "last_call"},
          %{"status" => "succeeded"}
        ] do
      snapshot =
        SavedResetProjection.snapshot(
          %{
            "saved_reset_redemption" =>
              Map.merge(redemption, %{
                "probe" => %{"token" => sensitive_sentinel},
                "result" => %{"body" => sensitive_sentinel},
                "credit_id" => sensitive_sentinel,
                "arbitrary_metadata" => sensitive_sentinel
              })
          },
          @prefs
        )

      assert snapshot.last_auto_redemption_cause == nil
      refute inspect(snapshot) =~ sensitive_sentinel
    end
  end

  @tag :saved_reset_redemption_cause
  test "never leaks the probe correlation token to operators" do
    meta = metadata("confirmed_by_upstream", %{"probe" => %{"token" => "secret-probe-token"}})

    snapshot = SavedResetProjection.snapshot(meta, @prefs)

    refute Map.has_key?(snapshot, :last_redemption)
    refute inspect(snapshot) =~ "secret-probe-token"
  end

  @tag :saved_reset_redemption_cause
  test "does not project a raw terminal reason" do
    terminal_reason_sentinel = "saved-reset-terminal-reason-sensitive-sentinel"

    snapshot =
      SavedResetProjection.snapshot(
        metadata("expired", %{"terminal_reason" => terminal_reason_sentinel}),
        @prefs
      )

    refute Map.has_key?(snapshot.reset_lifecycle, :terminal_reason)
    refute inspect(snapshot) =~ terminal_reason_sentinel
  end

  @tag :saved_reset_redemption_cause
  test "has no lifecycle for legacy records without a phase" do
    meta = %{"saved_reset_redemption" => %{"status" => "succeeded"}}

    snapshot = SavedResetProjection.snapshot(meta, @prefs)

    assert snapshot.reset_lifecycle == nil
    assert snapshot.last_auto_redemption_cause == nil
  end

  test "projects a sanitized granted date from current saved-reset expiration rows" do
    expires_at = "2026-08-20T03:20:00Z"
    first_seen_at = "2026-07-20T03:20:00Z"
    granted_at = "2026-07-18T03:20:00-04:00"

    snapshot =
      SavedResetProjection.snapshot(
        %{
          "saved_resets" => %{
            "status" => "reported",
            "available_count" => 1,
            "available_expirations" => [
              %{
                "expires_at" => expires_at,
                "first_seen_at" => first_seen_at,
                "granted_at" => granted_at
              }
            ]
          }
        },
        @prefs
      )

    assert snapshot.available_expirations == [
             %{
               expires_at: expires_at,
               first_seen_at: first_seen_at,
               granted_at: "2026-07-18T07:20:00Z"
             }
           ]
  end

  test "keeps missing, nil, and malformed grant dates unavailable without estimating them" do
    expires_at = "2026-08-20T03:20:00Z"
    first_seen_at = "2026-07-20T03:20:00Z"

    snapshot =
      SavedResetProjection.snapshot(
        %{
          "saved_resets" => %{
            "status" => "reported",
            "available_count" => 3,
            "available_expirations" => [
              %{"expires_at" => expires_at, "first_seen_at" => first_seen_at},
              %{
                "expires_at" => "2026-08-21T03:20:00Z",
                "first_seen_at" => first_seen_at,
                "granted_at" => nil
              },
              %{
                "expires_at" => "2026-08-22T03:20:00Z",
                "first_seen_at" => first_seen_at,
                "granted_at" => "not-a-date"
              }
            ]
          }
        },
        @prefs
      )

    assert Enum.map(snapshot.available_expirations, & &1.granted_at) == [nil, nil, nil]
    refute Enum.any?(snapshot.available_expirations, &(&1.granted_at == "2026-07-21T03:20:00Z"))
  end
end
