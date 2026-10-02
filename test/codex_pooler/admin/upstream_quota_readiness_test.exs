defmodule CodexPooler.Admin.UpstreamQuotaReadinessTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Admin.UpstreamQuotaReadiness
  alias CodexPooler.Admin.UpstreamRoutingReadiness
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Quota.{AccountAvailabilityStore, AccountQuotaWindow}
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.Quota.Windows

  @as_of ~U[2026-05-30 12:00:00Z]
  @future_reset ~U[2026-05-30 12:15:00Z]
  @weekly_reset ~U[2026-06-06 12:00:00Z]
  @monthly_reset ~U[2026-06-29 12:00:00Z]

  describe "from_windows/2" do
    test "uses the supplied snapshot for freshness and reset expiry boundaries" do
      ttl = Evidence.freshness_ttl_seconds()

      fresh =
        account_primary_window(
          observed_at: DateTime.add(@as_of, -ttl + 1, :second),
          last_sync_at: DateTime.add(@as_of, -ttl + 1, :second)
        )

      ttl_equal =
        account_primary_window(
          observed_at: DateTime.add(@as_of, -ttl, :second),
          last_sync_at: DateTime.add(@as_of, -ttl, :second)
        )

      stale =
        account_primary_window(
          observed_at: DateTime.add(@as_of, -ttl - 1, :second),
          last_sync_at: DateTime.add(@as_of, -ttl - 1, :second)
        )

      reset_expired = account_primary_window(reset_at: @as_of)

      assert UpstreamQuotaReadiness.from_windows([fresh], @as_of).state == "ready"
      assert UpstreamQuotaReadiness.from_windows([ttl_equal], @as_of).state == "ready"
      assert UpstreamQuotaReadiness.from_windows([stale], @as_of).state == "stale"

      assert %{
               state: "stale",
               reason_codes: reason_codes
             } = UpstreamQuotaReadiness.from_windows([reset_expired], @as_of)

      assert "expired" in reason_codes
    end

    test "maps precise account quota eligibility to ready" do
      primary = account_primary_window()

      assert %{
               state: "ready",
               label: "Quota ready",
               tone: :success,
               routing_ready_now?: true,
               reason_codes: [],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: nil
             } = UpstreamQuotaReadiness.from_windows([primary], @as_of)
    end

    test "maps fresh reset-bearing monthly account primary evidence to ready" do
      monthly = account_monthly_primary_window()

      assert %{
               state: "ready",
               label: "Quota ready",
               tone: :success,
               routing_ready_now?: true,
               reason_codes: [],
               primary_window: ^monthly,
               primary_30d_window: ^monthly,
               weekly_window: nil
             } = UpstreamQuotaReadiness.from_windows([monthly], @as_of)
    end

    test "maps weekly-only probe eligibility to warning readiness that can still route" do
      weekly = account_weekly_window()

      assert %{
               state: "weekly_only_probe",
               label: "Weekly quota probe",
               tone: :warning,
               routing_ready_now?: true,
               reason_codes: ["quota_account_primary_unknown"],
               primary_window: nil,
               primary_30d_window: nil,
               weekly_window: ^weekly
             } = UpstreamQuotaReadiness.from_windows([weekly], @as_of)
    end

    test "maps exhausted account primary evidence to exhausted" do
      primary = account_primary_window(used_percent: Decimal.new("100"))

      assert %{
               state: "exhausted",
               label: "Quota exhausted",
               tone: :error,
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "exhausted"],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: nil
             } = UpstreamQuotaReadiness.from_windows([primary], @as_of)
    end

    test "maps unusable monthly primary evidence to blocked states without false readiness" do
      exhausted = account_monthly_primary_window(used_percent: Decimal.new("100"))
      stale = account_monthly_primary_window(freshness_state: "stale")
      resetless = account_monthly_primary_window(reset_at: nil)

      assert %{
               state: "exhausted",
               label: "Quota exhausted",
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "exhausted"],
               primary_window: ^exhausted,
               primary_30d_window: ^exhausted
             } = UpstreamQuotaReadiness.from_windows([exhausted], @as_of)

      assert %{
               state: "stale",
               label: "Quota refresh needed",
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "not_fresh"],
               primary_window: ^stale,
               primary_30d_window: ^stale
             } = UpstreamQuotaReadiness.from_windows([stale], @as_of)

      assert %{
               state: "missing_evidence",
               label: "Quota missing",
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "reset_missing"],
               primary_window: ^resetless,
               primary_30d_window: ^resetless
             } = UpstreamQuotaReadiness.from_windows([resetless], @as_of)
    end

    test "maps stale selected account evidence to stale" do
      primary = account_primary_window(freshness_state: "stale")

      assert %{
               state: "stale",
               label: "Quota refresh needed",
               tone: :warning,
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "not_fresh"],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: nil
             } = UpstreamQuotaReadiness.from_windows([primary], @as_of)
    end

    test "maps missing account-level windows to missing evidence" do
      projection = UpstreamQuotaReadiness.from_windows([], @as_of)

      assert %{
               state: "missing_evidence",
               label: "Quota missing",
               tone: :warning,
               routing_ready_now?: false,
               reason_codes: ["quota_evidence_missing"],
               primary_window: nil,
               primary_30d_window: nil,
               weekly_window: nil
             } = projection
    end

    test "maps account reset-missing evidence to missing evidence" do
      primary = account_primary_window(reset_at: nil)

      assert %{
               state: "missing_evidence",
               label: "Quota missing",
               tone: :warning,
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "reset_missing"],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: nil
             } = UpstreamQuotaReadiness.from_windows([primary], @as_of)
    end

    test "maps unclassified account-level blockers to blocked" do
      primary = account_primary_window()

      auxiliary_blocker =
        account_primary_window(
          window_minutes: 60,
          quota_family: "auxiliary",
          freshness_state: "stale"
        )

      assert %{
               state: "blocked",
               label: "Quota blocked",
               tone: :warning,
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "not_fresh"],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: nil
             } = UpstreamQuotaReadiness.from_windows([primary, auxiliary_blocker], @as_of)
    end

    test "ignores model-scoped and upstream-model-scoped windows for top-level readiness" do
      primary = account_primary_window()

      projection =
        UpstreamQuotaReadiness.from_windows(
          [
            primary,
            model_window(used_percent: Decimal.new("100")),
            upstream_model_window(used_percent: Decimal.new("100"))
          ],
          @as_of
        )

      assert %{
               state: "ready",
               reason_codes: [],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: nil
             } = projection
    end

    test "reports missing evidence when only non-account windows are present" do
      projection =
        UpstreamQuotaReadiness.from_windows(
          [
            model_window(),
            upstream_model_window()
          ],
          @as_of
        )

      assert %{
               state: "missing_evidence",
               label: "Quota missing",
               routing_ready_now?: false,
               reason_codes: ["quota_evidence_missing"],
               primary_window: nil,
               primary_30d_window: nil,
               weekly_window: nil
             } = projection
    end

    test "weekly exhaustion wins after eligibility blocks" do
      primary = account_primary_window()
      weekly = account_weekly_window(used_percent: Decimal.new("100"))

      assert %{
               state: "exhausted",
               label: "Quota exhausted",
               tone: :error,
               routing_ready_now?: false,
               reason_codes: ["quota_window_unusable", "exhausted"],
               primary_window: ^primary,
               primary_30d_window: nil,
               weekly_window: ^weekly
             } = UpstreamQuotaReadiness.from_windows([primary, weekly], @as_of)
    end

    test "selects measured account primary evidence over a zero-capacity usage outlier" do
      outlier =
        account_primary_window(
          active_limit: 0,
          credits: 0,
          used_percent: Decimal.new("0"),
          reset_at: DateTime.add(@as_of, 5, :hour),
          observed_at: DateTime.add(@as_of, 60, :second)
        )

      measured =
        account_primary_window(
          active_limit: 0,
          credits: 0,
          used_percent: Decimal.new("6"),
          reset_at: DateTime.add(@as_of, 2, :hour)
        )

      assert %{
               state: "ready",
               routing_ready_now?: true,
               primary_window: ^measured
             } = UpstreamQuotaReadiness.from_windows([outlier, measured], @as_of)
    end

    test "weekly-only exhaustion uses the runtime weekly exhaustion exclusion" do
      weekly = account_weekly_window(used_percent: Decimal.new("100"))

      assert %{
               state: "exhausted",
               label: "Quota exhausted",
               tone: :error,
               routing_ready_now?: false,
               reason_codes: ["quota_weekly_exhausted", "exhausted"],
               primary_window: nil,
               primary_30d_window: nil,
               weekly_window: ^weekly
             } = UpstreamQuotaReadiness.from_windows([weekly], @as_of)
    end
  end

  describe "from_snapshot/1" do
    for minutes <- [300, 43_200] do
      test "permitted zero-use #{minutes}-minute primary agrees with runtime eligibility" do
        %{identity: identity} = upstream_assignment_fixture(pool_fixture())

        primary =
          account_primary_window(
            window_minutes: unquote(minutes),
            active_limit: nil,
            credits: nil,
            used_percent: Decimal.new(0),
            metadata: %{"rate_limit_allowed" => true, "rate_limit_reached" => false}
          )

        snapshot = RoutingQuotaSnapshot.from_identity(identity, [primary], @as_of)

        assert %{eligible?: true} =
                 Windows.routing_quota_eligibility_from_snapshot(snapshot)

        assert %{state: "ready", routing_ready_now?: true, primary_window: ^primary} =
                 UpstreamQuotaReadiness.from_snapshot(snapshot)

        assert %{state: "ready", primary_window: ^primary} =
                 UpstreamQuotaReadiness.from_windows([primary], @as_of)
      end
    end

    test "affirmative permission with exhausted percentage remains ready" do
      pool = pool_fixture()

      %{identity: identity} =
        upstream_assignment_fixture(pool, %{
          identity_metadata: %{
            "credential_epoch" => 1,
            AccountAvailabilityStore.metadata_key() => AccountAvailabilityStore.encode!(:available, @as_of, 1)
          }
        })

      window =
        account_primary_window(
          source: "codex_usage_api",
          used_percent: Decimal.new(100),
          observed_at: @as_of,
          metadata: %{"rate_limit_allowed" => true, "rate_limit_reached" => false}
        )

      snapshot = RoutingQuotaSnapshot.from_identity(identity, [window], @as_of)
      projection = UpstreamQuotaReadiness.from_snapshot(snapshot)
      assert projection.state == "ready"
      assert projection.routing_ready_now?
      assert projection.reason_codes == []
      assert Decimal.equal?(projection.primary_window.used_percent, 100)

      routing =
        UpstreamRoutingReadiness.from_inputs(
          identity,
          %{status: "active", health_status: "active", eligibility_status: "eligible"},
          projection
        )

      assert routing.routing_ready_now?
    end

    @tag credits_negative: true
    test "attested legacy windowless availability stays conditional on and is denied off" do
      %{identity: identity} = upstream_assignment_fixture(pool_fixture(), %{identity_metadata: %{"credential_epoch" => 1, AccountAvailabilityStore.metadata_key() => AccountAvailabilityStore.encode!(:available, @as_of, 1)}})
      snapshot = RoutingQuotaSnapshot.from_identity(identity, [], @as_of)
      projection = UpstreamQuotaReadiness.from_snapshot(snapshot)
      assert projection.routing_ready_now?
      assert projection.conditional?
      assert projection.capacity_basis == :unknown_legacy
      assert projection.qualification == :legacy_attested
      assert projection.primary_window == nil
      assert projection.weekly_window == nil

      routing = UpstreamRoutingReadiness.from_inputs(identity, %{status: "active", health_status: "active", eligibility_status: "eligible"}, projection)
      assert routing.routing_ready_now?
      assert routing.state == "capacity_basis_unknown"

      disabled = UpstreamQuotaReadiness.from_snapshot(%{snapshot | allow_provider_credits: false})
      refute disabled.routing_ready_now?
      assert disabled.capacity_basis == :unknown_legacy
      assert disabled.reason_codes == ["provider_credits_disabled", "capacity_basis_unknown"]
    end

    test "keeps blocked unknown expired and credential-mismatched snapshots fail closed" do
      for {state, observed_at, epoch, expected_state} <- [
            {:blocked, @as_of, 1, "blocked"},
            {:unknown, @as_of, 1, "missing_evidence"},
            {:available, DateTime.add(@as_of, -(Evidence.freshness_ttl_seconds() + 1), :second), 1, "missing_evidence"},
            {:available, @as_of, 2, "missing_evidence"}
          ] do
        pool = pool_fixture()

        %{identity: identity} =
          upstream_assignment_fixture(pool, %{
            identity_metadata: %{
              "credential_epoch" => 1,
              AccountAvailabilityStore.metadata_key() => AccountAvailabilityStore.encode!(state, observed_at, epoch)
            }
          })

        projection =
          identity
          |> RoutingQuotaSnapshot.from_identity([], @as_of)
          |> UpstreamQuotaReadiness.from_snapshot()

        assert projection.state == expected_state
        refute projection.routing_ready_now?
      end
    end

    test "account-only readiness ignores unrelated model exhaustion without hiding it from snapshot" do
      pool = pool_fixture()

      %{identity: identity} =
        upstream_assignment_fixture(pool, %{
          identity_metadata: %{
            "credential_epoch" => 1,
            AccountAvailabilityStore.metadata_key() => AccountAvailabilityStore.encode!(:available, @as_of, 1)
          }
        })

      model_blocker = model_window(used_percent: Decimal.new("100"))
      snapshot = RoutingQuotaSnapshot.from_identity(identity, [model_blocker], @as_of)
      projection = UpstreamQuotaReadiness.from_snapshot(snapshot)

      assert projection.capacity_basis == :unknown_legacy
      assert projection.conditional?
      assert projection.routing_ready_now?
      assert snapshot.raw_windows == [model_blocker]
    end
  end

  test "ready credit presentation retains identity, assignment and circuit guards" do
    quota = %{state: "provider_credits_ready", label: "Routing ready via credits", tone: :success, routing_ready_now?: true, capacity_basis: :provider_credits, conditional?: false, reason_codes: [], primary_window: nil, primary_30d_window: nil, weekly_window: nil}
    assignment = %{status: "active", health_status: "active", eligibility_status: "eligible"}
    ready = UpstreamRoutingReadiness.from_inputs("active", assignment, quota)

    assert ready.routing_ready_now?
    assert ready.tone == :success
    assert ready.label == "Routing ready via credits"

    for status <- ["reauth_required", "disabled", "paused"] do
      refute UpstreamRoutingReadiness.from_inputs(status, assignment, quota).routing_ready_now?
    end

    refute UpstreamRoutingReadiness.from_inputs("active", %{assignment | status: "disabled"}, quota).routing_ready_now?
    refute UpstreamRoutingReadiness.from_inputs("active", assignment, %{quota | routing_ready_now?: false, reason_codes: ["provider_credits_disabled"]}).routing_ready_now?
    circuit = UpstreamRoutingReadiness.with_circuit_visibility(ready, %{state: :blocked})
    assert circuit.tone == :error
    assert circuit.state == "circuit_protection_active"
  end

  defp account_primary_window(attrs \\ []) do
    window(
      Keyword.merge(
        [
          quota_key: "account",
          window_kind: "primary",
          window_minutes: 300,
          used_percent: Decimal.new("12"),
          reset_at: @future_reset,
          source: "codex_usage_api",
          source_precision: "observed",
          quota_scope: "account",
          quota_family: "account",
          freshness_state: "fresh",
          observed_at: @as_of,
          last_sync_at: @as_of
        ],
        attrs
      )
    )
  end

  defp account_weekly_window(attrs \\ []) do
    window(
      Keyword.merge(
        [
          quota_key: "account",
          window_kind: "secondary",
          window_minutes: 10_080,
          used_percent: Decimal.new("12"),
          reset_at: @weekly_reset,
          source: "codex_usage_api",
          source_precision: "observed",
          quota_scope: "account",
          quota_family: "account",
          freshness_state: "fresh",
          observed_at: @as_of,
          last_sync_at: @as_of
        ],
        attrs
      )
    )
  end

  defp account_monthly_primary_window(attrs \\ []) do
    account_primary_window(
      Keyword.merge(
        [
          window_minutes: 43_200,
          used_percent: Decimal.new("42.5"),
          reset_at: @monthly_reset
        ],
        attrs
      )
    )
  end

  defp model_window(attrs \\ []) do
    window(
      Keyword.merge(
        [
          quota_key: "sample_model",
          window_kind: "primary",
          window_minutes: 300,
          used_percent: Decimal.new("12"),
          reset_at: @future_reset,
          source: "codex_usage_api",
          source_precision: "observed",
          quota_scope: "model",
          quota_family: "codex_model",
          model: "sample-model",
          upstream_model: "sample-upstream-model",
          freshness_state: "fresh",
          observed_at: @as_of,
          last_sync_at: @as_of
        ],
        attrs
      )
    )
  end

  defp upstream_model_window(attrs \\ []) do
    window(
      Keyword.merge(
        [
          quota_key: "sample_upstream_model",
          window_kind: "primary",
          window_minutes: 300,
          used_percent: Decimal.new("12"),
          reset_at: @future_reset,
          source: "codex_usage_api",
          source_precision: "observed",
          quota_scope: "upstream_model",
          quota_family: "codex_model",
          upstream_model: "sample-upstream-model",
          freshness_state: "fresh",
          observed_at: @as_of,
          last_sync_at: @as_of
        ],
        attrs
      )
    )
  end

  defp window(attrs), do: struct!(AccountQuotaWindow, attrs)
end
