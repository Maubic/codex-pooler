defmodule CodexPooler.Accounting.RequestLogsUsageTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Access.APIKeyPolicyBinding
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.LedgerEntry
  alias CodexPooler.Accounting.Reporting
  alias CodexPooler.Accounting.Rollups
  alias CodexPooler.Accounting.UsageResponses
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.AccountsFixtures
  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow

  import CodexPooler.PoolerFixtures

  test "local Codex usage reset countdown uses the requested snapshot time" do
    as_of = ~U[2026-09-01 12:00:00.000000Z]

    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    ensure_default_policy!(api_key)

    assert {:ok, usage} = Accounting.build_codex_usage_for_api_key(pool, api_key, as_of: as_of)
    assert usage.plan_type == "api_key"

    assert usage.rate_limit.primary_window == %{
             used_percent: 0,
             limit_window_seconds: 86_400,
             reset_after_seconds: 43_200,
             reset_at: DateTime.to_unix(~U[2026-09-02 00:00:00Z])
           }

    assert usage.rate_limit.secondary_window == nil

    assert {:ok, later_usage} =
             Accounting.build_codex_usage_for_api_key(pool, api_key, as_of: DateTime.add(as_of, 61, :second))

    assert later_usage.rate_limit.primary_window == %{usage.rate_limit.primary_window | reset_after_seconds: 43_139}
  end

  test "pre-attempt failure retains admission without provisional or measured request-log spend" do
    setup = CodexPooler.AccountingTestSupport.accounting_setup()
    as_of = ~U[2026-09-20 12:00:00.000000Z]

    assert {:ok, before_usage} =
             Accounting.build_api_key_self_usage(setup.pool, setup.api_key, as_of: as_of)

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id},
               %{
                 now: as_of
               }
             )

    assert Accounting.reservation_outstanding?(reserved.request)

    assert {:ok, _failed} =
             Accounting.finalize_reservation_failure(reserved.request, %{
               last_error_code: "no_available_upstream",
               pre_attempt_phase: "routing_rejected",
               now: as_of
             })

    refute Accounting.reservation_outstanding?(reserved.request)

    assert Repo.aggregate(
             from(a in CodexPooler.Accounting.Attempt,
               where: a.request_id == ^reserved.request.id
             ),
             :count
           ) == 0

    assert Repo.all(
             from(e in LedgerEntry,
               where: e.request_id == ^reserved.request.id,
               order_by: e.entry_kind,
               select: e.entry_kind
             )
           ) == ["release", "reservation"]

    assert %{items: [log], total: 1} = Accounting.list_request_logs(setup.pool)
    assert log.id == reserved.request.id
    assert log.status == "failed"
    assert log.token_counts.total_tokens == nil
    assert log.token_counts.input_tokens == nil
    assert log.token_counts.output_tokens == nil
    assert log.cost.usd == nil
    refute log.cost.status == "priced"

    assert {:ok, after_usage} =
             Accounting.build_api_key_self_usage(setup.pool, setup.api_key, as_of: as_of)

    assert after_usage.total_tokens == before_usage.total_tokens
    assert after_usage.total_cost_usd == before_usage.total_cost_usd
    assert after_usage.total_cost_status == before_usage.total_cost_status

    assert %{current_value: 1, remaining_value: 59} =
             usage_limit(after_usage.limits, "request_count", "minute")

    assert after_usage.budget_usage.daily == %{
             known_total_tokens: 0,
             provisional_total_tokens: 0,
             pending_total_tokens: 0,
             effective_total_tokens: 0,
             admission_count: 1
           }

    assert after_usage.budget_usage.daily == after_usage.budget_usage.weekly
    pricing_boundary_receipt("pre_attempt", log, after_usage)
  end

  test "deleted-key history stays reportable without entering another key's budget windows" do
    owner = AccountsFixtures.bootstrap_owner_fixture()
    scope = Scope.for_user(owner.user, ["instance_owner"])
    setup = active_api_key_fixture()
    as_of = ~U[2026-09-20 12:00:00.000000Z]
    request = request_fixture(setup)
    entry = ledger_entry_fixture(request, %{total_tokens: 123, occurred_at: as_of})
    Rollups.accumulate!(request, entry)
    assert {:ok, _} = CodexPooler.Access.delete_api_key(scope, setup.api_key)
    assert Repo.get!(LedgerEntry, entry.id).api_key_id == nil

    assert Reporting.token_totals_by_pool_ids(
             [setup.pool.id],
             DateTime.add(as_of, -1),
             DateTime.add(as_of, 1)
           ) == %{setup.pool.id => 123}

    other = active_api_key_fixture(setup.pool)

    assert {:ok, usage} =
             Accounting.build_api_key_self_usage(setup.pool, other.api_key, as_of: as_of)

    assert usage.total_tokens == 0
    assert usage.total_cost_usd == nil
    assert usage.limits == []
    assert usage.budget_usage.daily.effective_total_tokens == 0
    assert usage.budget_usage.weekly.effective_total_tokens == 0

    refute Repo.exists?(
             from r in CodexPooler.Accounting.DailyRollup,
               where: r.api_key_id == ^setup.api_key.id and r.dimension_kind == "api_key"
           )
  end

  test "missing and null usage counters stay provisional while measured zero is known" do
    setup = CodexPooler.AccountingTestSupport.accounting_setup()
    as_of = ~U[2026-09-20 12:00:00.000000Z]

    for usage <- [
          %{status: "usage_unknown"},
          %{status: "usage_unknown", total_tokens: nil},
          %{status: "usage_known", input_tokens: 0, output_tokens: 0, total_tokens: 0}
        ] do
      assert {:ok, reserved} =
               Accounting.reserve(
                 setup.auth,
                 setup.model,
                 %{"model" => setup.model.exposed_model_id},
                 %{now: as_of}
               )

      assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

      assert {:ok, _} =
               Accounting.finalize_failure(reserved.request, attempt, %{
                 usage: Map.put(usage, :recorded_at, as_of),
                 now: as_of
               })
    end

    assert {:ok, usage} =
             Accounting.build_api_key_self_usage(setup.pool, setup.api_key, as_of: as_of)

    assert usage.total_tokens == 0

    assert %{
             known_total_tokens: 0,
             provisional_total_tokens: 1_024,
             pending_total_tokens: 0,
             effective_total_tokens: 1_024,
             admission_count: 3
           } = usage.budget_usage.daily
  end

  test "budget windows separate measured, provisional and old pending usage at one as_of" do
    setup = CodexPooler.AccountingTestSupport.accounting_setup()
    as_of = ~U[2026-09-20 12:00:00.000000Z]

    CodexPooler.AccountingTestSupport.update_default_policy!(setup.api_key, %{
      max_tokens_per_day: 2_000,
      max_tokens_per_week: 4_000
    })

    known = request_fixture(setup)

    settlement =
      ledger_entry_fixture(known, %{
        total_tokens: 100,
        settled_cost_micros: 250_000,
        details: %{"settled_cost_micros" => "250000"},
        occurred_at: as_of
      })

    Rollups.accumulate!(known, settlement)

    pending = request_fixture(setup, %{status: "in_progress", completed_at: nil})

    ledger_entry_fixture(pending, %{
      entry_kind: "reservation",
      usage_status: "usage_unknown",
      total_tokens: 512,
      occurred_at: DateTime.add(as_of, -8, :day)
    })

    unknown = request_fixture(setup, %{usage_status: "usage_unknown"})

    unknown_entry =
      ledger_entry_fixture(unknown, %{
        usage_status: "usage_unknown",
        total_tokens: 512,
        details: %{"estimated_from_reserve" => true},
        occurred_at: as_of
      })

    Rollups.accumulate!(unknown, unknown_entry)
    future = request_fixture(setup)
    ledger_entry_fixture(future, %{total_tokens: 9_999, occurred_at: DateTime.add(as_of, 1)})
    other = active_api_key_fixture(setup.pool)
    ledger_entry_fixture(request_fixture(other), %{total_tokens: 8_888, occurred_at: as_of})

    assert {:ok, usage} =
             Accounting.build_v1_usage_for_api_key(setup.pool, setup.api_key, as_of: as_of)

    assert usage.total_tokens == 100
    assert usage.total_cost_usd == 0.25

    for window <- [:daily, :weekly] do
      assert usage.budget_usage[window] == %{
               known_total_tokens: 100,
               provisional_total_tokens: 512,
               pending_total_tokens: 512,
               effective_total_tokens: 1_124,
               admission_count: 0
             }
    end

    assert %{current_value: 1_124, remaining_value: 876} =
             usage_limit(usage.limits, "total_tokens", "daily")

    assert %{current_value: 1_124, remaining_value: 2_876} =
             usage_limit(usage.limits, "total_tokens", "weekly")
  end

  test "late measured correction replaces provisional pressure and pricing once at original time" do
    setup = CodexPooler.AccountingTestSupport.accounting_setup()
    as_of = ~U[2026-09-20 12:00:00.000000Z]

    setup.pricing
    |> Ecto.Changeset.change(effective_at: DateTime.add(as_of, -60))
    |> Repo.update!()

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id},
               %{now: as_of}
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)

    assert {:ok, failed} =
             Accounting.finalize_failure(reserved.request, attempt, %{
               last_error_code: "owner_drained",
               usage: %{status: "usage_unknown", recorded_at: as_of},
               now: as_of
             })

    assert {:ok, before_usage} =
             Accounting.build_api_key_self_usage(setup.pool, setup.api_key, as_of: as_of)

    assert before_usage.total_tokens == 0
    assert before_usage.total_cost_usd == nil
    assert before_usage.total_cost_status == "unpriced"
    refute Accounting.reservation_outstanding?(failed.request)
    assert %{items: [unknown_log], total: 1} = Accounting.list_request_logs(setup.pool)
    assert unknown_log.id == failed.request.id
    assert unknown_log.status == "failed"
    assert unknown_log.usage_status == "usage_unknown"
    assert unknown_log.token_counts.total_tokens == nil
    assert unknown_log.token_counts.input_tokens == nil
    assert unknown_log.token_counts.output_tokens == nil
    assert unknown_log.cost.usd == nil
    assert unknown_log.cost.status == "unpriced"
    pricing_boundary_receipt("unknown_post_attempt", unknown_log, before_usage)

    assert %{
             provisional_total_tokens: 512,
             known_total_tokens: 0,
             pending_total_tokens: 0,
             admission_count: 1
           } = before_usage.budget_usage.daily

    correction = %{
      status: "usage_known",
      source: "late_owner_completion",
      input_tokens: 10,
      output_tokens: 20,
      total_tokens: 30,
      recorded_at: DateTime.add(as_of, 1, :day)
    }

    for disposition <- [:replaced, :reused] do
      assert {:ok, %{finalization_disposition: ^disposition}} =
               Accounting.finalize_success_with_disposition(
                 failed.request,
                 failed.attempt,
                 correction,
                 %{now: DateTime.add(as_of, 1, :day)}
               )

      assert {:ok, corrected} =
               Accounting.build_api_key_self_usage(setup.pool, setup.api_key, as_of: as_of)

      assert corrected.total_tokens == 30
      assert Decimal.equal?(corrected.total_cost_usd, Decimal.new("0.000500"))

      assert %{
               provisional_total_tokens: 0,
               known_total_tokens: 30,
               pending_total_tokens: 0,
               effective_total_tokens: 30,
               admission_count: 1
             } = corrected.budget_usage.daily

      assert corrected.budget_usage.daily == corrected.budget_usage.weekly
      refute Accounting.reservation_outstanding?(failed.request)
      assert %{items: [known_log], total: 1} = Accounting.list_request_logs(setup.pool)
      assert known_log.id == failed.request.id
      assert known_log.token_counts.total_tokens == 30
      assert known_log.token_counts.input_tokens == 10
      assert known_log.token_counts.output_tokens == 20
      assert known_log.cost.status == "priced"
      assert Decimal.equal?(known_log.cost.usd, Decimal.new("0.000500"))

      assert Repo.aggregate(
               from(e in LedgerEntry,
                 where:
                   e.request_id == ^failed.request.id and e.entry_kind == "settlement" and
                     e.amount_status == "recorded"
               ),
               :count
             ) == 1

      assert Repo.aggregate(
               from(e in LedgerEntry,
                 where:
                   e.request_id == ^failed.request.id and e.entry_kind == "settlement" and
                     e.amount_status == "voided"
               ),
               :count
             ) == 1

      pricing_boundary_receipt("known_#{disposition}", known_log, corrected)
    end
  end

  defp pricing_boundary_receipt(phase, log, usage) do
    CodexPooler.TestDiagnostics.puts(
      "pricing_boundary #{phase} " <>
        inspect(%{
          log_tokens: log.token_counts && log.token_counts.total_tokens,
          log_cost: log.cost,
          measured_tokens: usage.total_tokens,
          measured_cost: usage.total_cost_usd,
          budget: usage.budget_usage.daily
        })
    )
  end

  test "request log entries are metadata-only and usage shape is v1-compatible" do
    %{pool: pool, api_key: api_key} =
      active_api_key_fixture(pool_fixture(), %{
        default_policy: %{max_tokens_per_day: 1000, max_requests_per_minute: 60}
      })

    ensure_default_policy!(api_key)

    model =
      model_fixture(pool, %{
        exposed_model_id: "gpt-log-mini",
        upstream_model_id: "provider-gpt-log-mini",
        pricing_ref: "provider-gpt-log-mini"
      })

    %{assignment: assignment} = upstream_assignment_fixture(pool)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %PricingSnapshot{
      model_identifier: "provider-gpt-log-mini",
      price_version: "test-v1",
      currency_code: "USD",
      billing_unit: "token",
      input_token_micros: Decimal.new(100),
      cached_input_token_micros: Decimal.new(0),
      output_token_micros: Decimal.new(200),
      reasoning_token_micros: Decimal.new(0),
      request_base_micros: Decimal.new(0),
      effective_at: DateTime.add(now, -60, :second),
      captured_at: now,
      config: %{
        "service_tier" => "standard",
        "price_bucket" => "default",
        "pricing_type" => "per_1m_tokens"
      }
    }
    |> Repo.insert!()

    auth = %{pool: pool, api_key: api_key, key_prefix: api_key.key_prefix}

    assert {:ok, reserved} =
             Accounting.reserve(
               auth,
               model,
               %{"model" => "gpt-log-mini", "input" => "raw input text"},
               %{
                 correlation_id: "corr-request-log",
                 user_agent: "Codex CLI/1.2.3",
                 request_metadata: %{
                   "body" => %{"input" => "raw input text"},
                   "safe_id" => "req_123"
                 }
               }
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, assignment)

    assert {:ok, _result} =
             Accounting.finalize_success(
               reserved.request,
               attempt,
               %{status: "usage_known", input_tokens: 2, output_tokens: 3, total_tokens: 5},
               %{response_status_code: 200}
             )

    assert %{items: [log], total: 1, limit: 50, offset: 0} = Accounting.list_request_logs(pool)
    assert log.pool_name == pool.name
    assert log.pool_slug == pool.slug
    assert log.api_key_prefix == api_key.key_prefix
    assert log.requested_model == "gpt-log-mini"
    assert log.status == "succeeded"
    assert log.user_agent == "Codex CLI/1.2.3"
    assert log.pool_upstream_assignment_id == assignment.id
    assert log.token_counts.total_tokens == 5
    assert log.cost.status == "priced"
    assert Decimal.equal?(log.cost.usd, Decimal.new("0.000800"))
    assert log.metadata["body"] == "[REDACTED]"
    assert log.metadata["safe_id"] == "req_123"
    refute inspect(log) =~ "raw input text"

    assert {:ok, usage} =
             Accounting.build_api_key_self_usage(
               pool,
               api_key,
               as_of: DateTime.add(now, 60, :second)
             )

    assert usage.request_count == 1
    assert usage.total_tokens == 5
    assert Decimal.equal?(usage.total_cost_usd, Decimal.new("0.000800"))
    assert Enum.any?(usage.limits, &(&1.limit_type == "credits" and &1.limit_window == "daily"))

    assert {:ok, codex_usage} = Accounting.build_codex_usage_for_api_key(pool, api_key)
    assert codex_usage.plan_type == "api_key"
    assert codex_usage.rate_limit.allowed in [true, false]
  end

  test "local usage read models keep request counts but exclude unknown reserve usage" do
    %{pool: pool, api_key: api_key} =
      active_api_key_fixture(pool_fixture(), %{
        default_policy: %{max_tokens_per_day: 1_000, max_requests_per_minute: 60}
      })

    %{api_key: unknown_only_key} = active_api_key_fixture(pool)
    ensure_default_policy!(api_key)
    ensure_default_policy!(unknown_only_key)

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    known_request =
      request_fixture(%{pool: pool, api_key: api_key}, %{
        status: "succeeded",
        correlation_id: "corr-known-read-model"
      })

    known_settlement =
      ledger_entry_fixture(known_request, %{
        input_tokens: 18,
        cached_input_tokens: 4,
        output_tokens: 8,
        reasoning_tokens: 4,
        total_tokens: 30,
        settled_cost_micros: 250_000,
        details: %{"settled_cost_micros" => "250000"},
        occurred_at: now
      })

    unknown_request =
      request_fixture(%{pool: pool, api_key: api_key}, %{
        status: "failed",
        usage_status: "usage_unknown",
        response_status_code: 502,
        correlation_id: "corr-unknown-read-model"
      })

    unknown_settlement =
      ledger_entry_fixture(unknown_request, %{
        usage_status: "usage_unknown",
        input_tokens: 8_000,
        cached_input_tokens: 500,
        output_tokens: 1_500,
        reasoning_tokens: 499,
        total_tokens: 9_999,
        estimated_cost_micros: 1_000_000,
        settled_cost_micros: 9_999_999,
        details: %{"estimated_from_reserve" => true},
        occurred_at: now
      })

    unknown_only_request =
      request_fixture(%{pool: pool, api_key: unknown_only_key}, %{
        status: "failed",
        usage_status: "usage_unknown",
        response_status_code: 502,
        correlation_id: "corr-unknown-only-read-model"
      })

    unknown_only_settlement =
      ledger_entry_fixture(unknown_only_request, %{
        usage_status: "usage_unknown",
        input_tokens: 7_000,
        output_tokens: 2_000,
        reasoning_tokens: 100,
        total_tokens: 9_100,
        estimated_cost_micros: 2_000_000,
        settled_cost_micros: 8_000_000,
        details: %{"estimated_from_reserve" => true},
        occurred_at: now
      })

    assert :ok = Rollups.accumulate!(known_request, known_settlement)
    assert :ok = Rollups.accumulate!(unknown_request, unknown_settlement)
    assert :ok = Rollups.accumulate!(unknown_only_request, unknown_only_settlement)

    assert {:ok, self_usage} =
             Accounting.build_api_key_self_usage(pool, api_key, as_of: DateTime.add(now, 60))

    assert self_usage.request_count == 2
    assert self_usage.total_tokens == 30
    assert self_usage.cached_input_tokens == 4
    assert self_usage.total_cost_status == "priced"
    assert Decimal.equal?(self_usage.total_cost_usd, Decimal.new("0.250000"))

    assert %{current_value: 1_000, remaining_value: 0} =
             usage_limit(self_usage.limits, "total_tokens", "daily")

    assert %{current_value: 1_000, remaining_value: 0} =
             usage_limit(self_usage.limits, "credits", "daily")

    assert %{current_value: 0, remaining_value: 60} =
             usage_limit(self_usage.limits, "request_count", "minute")

    assert {:ok, v1_usage} =
             Accounting.build_v1_usage_for_api_key(pool, api_key, as_of: DateTime.add(now, 60))

    assert v1_usage.request_count == 2
    assert v1_usage.total_tokens == 30
    assert v1_usage.cached_input_tokens == 4
    assert v1_usage.total_cost_status == "priced"
    assert v1_usage.total_cost_usd == 0.25

    assert %{current_value: 1_000, remaining_value: 0} =
             usage_limit(v1_usage.limits, "total_tokens", "daily")

    assert {:ok, codex_usage} =
             Accounting.build_codex_usage_for_api_key(pool, api_key, as_of: DateTime.add(now, 60))

    assert codex_usage.plan_type == "api_key"
    assert codex_usage.credits.balance == "0"
    assert codex_usage.rate_limit.primary_window.used_percent == 100

    assert {:ok, unknown_usage} = Accounting.build_api_key_self_usage(pool, unknown_only_key)
    assert unknown_usage.total_tokens == 0
    assert unknown_usage.total_cost_usd == nil
    assert unknown_usage.total_cost_status == "unpriced"
    assert unknown_usage.budget_usage.daily.provisional_total_tokens == 9_100
  end

  test "model-scoped additional quota stays outside request settlement and account credits" do
    %{pool: pool, api_key: api_key} =
      active_api_key_fixture(pool_fixture(), %{
        default_policy: %{max_tokens_per_day: 1_000, max_requests_per_minute: 60}
      })

    ensure_default_policy!(api_key)

    model =
      model_fixture(pool, %{
        exposed_model_id: "gpt-isolation-mini",
        upstream_model_id: "provider-gpt-isolation-mini"
      })

    %{assignment: assignment} = upstream_assignment_fixture(pool)
    auth = %{pool: pool, api_key: api_key, key_prefix: api_key.key_prefix}
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    account_window = %AccountQuotaWindow{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      window_kind: "secondary",
      window_minutes: 10_080,
      active_limit: 1_000,
      credits: 640,
      used_percent: Decimal.new("36"),
      reset_at: DateTime.add(now, 5, :day),
      observed_at: now,
      last_sync_at: now,
      source: "codex_usage_api",
      source_precision: "observed",
      freshness_state: "fresh"
    }

    additional_window = model_additional_window(now)

    {primary, secondary} =
      UsageResponses.account_usage_windows([account_window, additional_window], now)

    assert primary == nil
    assert UsageResponses.codex_credits(primary, secondary).balance == "640"

    assert [%{quota_key: "synthetic_model_weekly", metered_feature: "synthetic_model_meter"}] =
             UsageResponses.additional_codex_rate_limits([account_window, additional_window], now)

    assert {:ok, reserved} =
             Accounting.reserve(
               auth,
               model,
               %{"model" => "gpt-isolation-mini", "input" => "synthetic"},
               %{
                 correlation_id: "corr-additional-quota-isolation"
               }
             )

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, assignment)

    assert {:ok, _result} =
             Accounting.finalize_success(
               reserved.request,
               attempt,
               %{status: "usage_known", input_tokens: 4, output_tokens: 6, total_tokens: 10},
               %{response_status_code: 200}
             )

    assert Repo.all(
             from(entry in LedgerEntry,
               where: entry.request_id == ^reserved.request.id,
               order_by: entry.entry_kind,
               select: entry.entry_kind
             )
           ) == ["release", "reservation", "settlement"]

    assert {:ok, self_usage} = Accounting.build_api_key_self_usage(pool, api_key)
    assert self_usage.request_count == 1
    assert self_usage.total_tokens == 10
    assert usage_limit(self_usage.limits, "credits", "daily").remaining_value == 990
  end

  defp ensure_default_policy!(api_key) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    case Repo.one(
           from b in APIKeyPolicyBinding,
             where: b.api_key_id == ^api_key.id and b.binding_scope == "default",
             limit: 1
         ) do
      %APIKeyPolicyBinding{} = binding ->
        binding
        |> Ecto.Changeset.change(%{
          max_tokens_per_day: 1000,
          max_requests_per_minute: 60,
          updated_at: now
        })
        |> Repo.update!()

      nil ->
        %APIKeyPolicyBinding{
          api_key_id: api_key.id,
          binding_scope: "default",
          status: "active",
          max_tokens_per_day: 1000,
          max_requests_per_minute: 60,
          created_at: now,
          updated_at: now
        }
        |> Repo.insert!()
    end
  end

  defp usage_limit(limits, limit_type, limit_window) do
    Enum.find(limits, &(&1.limit_type == limit_type and &1.limit_window == limit_window))
  end

  defp model_additional_window(now) do
    %AccountQuotaWindow{
      quota_key: "synthetic_model_weekly",
      quota_scope: "model",
      quota_family: "codex_model",
      model: "synthetic-model",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new("0"),
      display_label: "Synthetic model weekly",
      limit_name: "synthetic-model-weekly",
      metered_feature: "synthetic_model_meter",
      raw_metered_feature: "synthetic_model_meter",
      reset_at: DateTime.add(now, 7, :day),
      observed_at: now,
      last_sync_at: now,
      source: "codex_usage_api",
      source_precision: "observed",
      freshness_state: "fresh"
    }
  end
end
