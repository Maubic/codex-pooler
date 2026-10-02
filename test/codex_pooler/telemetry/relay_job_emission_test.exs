defmodule CodexPooler.Telemetry.RelayJobEmissionTest do
  @moduledoc """
  Real Oban workers, not emitter functions, for the four relayed families.

  `test/codex_pooler/telemetry/relay_regression_test.exs` proves the relay
  carries what it is handed, but it hands the relay a call to the emitter
  itself. That cannot see the thing this file exists for: whether a
  `perform/1` still reaches the emission at all, and whether it reaches it
  outside its own transaction. Three of the four emitters decide whether to
  count by asking `Repo.in_transaction?/0`, so a future wrapping transaction
  would silently delete the series while every emitter-level test stayed
  green.

  Every test here therefore starts at `perform_job(Worker, args)` with a
  fixture the worker itself resolves, and the round-trip tests compare the
  reporter's scrape output for the relayed sample against the in-process one.

  The `interrupted` slice's own emission semantics (after commit, once per
  recovered turn, nothing on a stale owner or a rollback) are pinned by
  `test/codex_pooler/gateway/runtime/finalization/interruption_telemetry_test.exs`
  and are not repeated; what is pinned here is that slice's relay round trip.
  """

  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.PoolerFixtures
  import Ecto.Query

  alias CodexPooler.Accounting
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, SessionContinuity}
  alias CodexPooler.Gateway.Websocket, as: Gateway

  alias CodexPooler.Jobs.{
    AccountReconciliationWorker,
    AlertEvaluationWorker,
    RuntimeStateCleanupWorker,
    SavedResetRedemptionWorker
  }

  alias CodexPooler.Repo
  alias CodexPooler.Telemetry.{RelayEvent, RelayRuntime}
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox
  alias TelemetryMetricsPrometheus.Core

  @convergence [:codex_pooler, :saved_reset, :convergence]
  @pre_attempt_release [:codex_pooler, :accounting, :reservation, :pre_attempt_release]
  @quota_decision [:codex_pooler, :quota, :cycle, :decision]

  describe "stale_sweep pre-attempt release from the runtime cleanup worker" do
    test "the real worker counts one committed release outside any transaction, once" do
      %{request: request, now: now} = stale_reservation_fixture!("relay-stale-sweep")
      events = capture!(@pre_attempt_release, :release)

      assert perform_job(RuntimeStateCleanupWorker, %{"now" => DateTime.to_iso8601(now)}) == :ok

      assert [{%{count: 1}, metadata, in_transaction?}] = drain(events)

      assert metadata == %{
               phase: "stale_sweep",
               transport: "http_sse",
               release_reason: "stale_reservation_recovered"
             }

      # The whole point of the family: the job commits its own release and
      # counts it afterwards. An enclosing transaction here would mean the
      # count describes a write that may still roll back.
      refute in_transaction?
      assert Repo.reload!(request).status == "failed"

      # One abandonment, one sample: the sweep is idempotent and a second pass
      # over the same released reservation must not count a second release.
      assert perform_job(RuntimeStateCleanupWorker, %{"now" => DateTime.to_iso8601(now)}) == :ok
      assert drain(events) == []
    end

    test "a caller rollback suppresses the uncommitted release count" do
      # The finalizer now hands its marker to an enclosing transaction instead
      # of emitting at savepoint release. The worker is bare in production; this
      # wrapper is the negative control proving a caller rollback cannot leave a
      # sample for a release row that never committed.
      %{request: request, now: now} = stale_reservation_fixture!("relay-stale-sweep-rollback")
      events = capture!(@pre_attempt_release, :release)

      assert Repo.transaction(fn ->
               assert perform_job(RuntimeStateCleanupWorker, %{
                        "now" => DateTime.to_iso8601(now)
                      }) == :ok

               Repo.rollback(:caller_rollback)
             end) == {:error, :caller_rollback}

      assert drain(events) == []
      assert Repo.reload!(request).status == "in_progress"
      assert release_entries(request) == []
    end
  end

  describe "saved-reset convergence from the reconciliation worker" do
    test "the real worker counts one committed transition outside any transaction, once" do
      fixture = reconciliation_convergence_fixture!()
      events = capture!(@convergence, :convergence)

      assert perform_job(AccountReconciliationWorker, reconciliation_args(fixture)) == :ok

      # One transition, one sample: the open question this pins is whether a
      # single saved-reset transition can be counted twice. The reconciliation
      # pass converges once per refreshed identity, and the settled lifecycle
      # then fails the compare-and-set guard, so a repeated pass counts nothing.
      assert [{measurements, metadata, in_transaction?}] = drain(events)
      assert measurements.count == 1
      assert metadata == %{source: "reconciliation", outcome: "confirmed_by_quota"}
      refute in_transaction?
      assert converged_phase(fixture.identity) == "confirmed_by_quota"

      assert perform_job(AccountReconciliationWorker, reconciliation_args(fixture)) == :ok
      assert drain(events) == []
    end

    test "an outer transaction around the real worker suppresses the count" do
      fixture = reconciliation_convergence_fixture!()
      events = capture!(@convergence, :convergence)

      assert {:ok, :ok} =
               Repo.transaction(fn ->
                 perform_job(AccountReconciliationWorker, reconciliation_args(fixture))
               end)

      # `Convergence.converge/3` counts only when it owns the outermost
      # transaction. A future wrapping transaction therefore deletes the
      # series silently; this is the assertion that makes that visible.
      assert drain(events) == []
      assert converged_phase(fixture.identity) == "confirmed_by_quota"
    end
  end

  describe "saved-reset convergence from the redemption worker" do
    test "the real worker counts one committed transition outside any transaction, once" do
      fixture = redemption_convergence_fixture!()
      events = capture!(@convergence, :convergence)

      assert perform_job(SavedResetRedemptionWorker, scheduled_redemption_args(fixture)) == :ok

      assert [{measurements, metadata, in_transaction?}] = drain(events)
      assert measurements.count == 1
      assert metadata == %{source: "finalizer", outcome: "confirmed_by_quota"}
      refute in_transaction?
      assert converged_phase(fixture.identity) == "confirmed_by_quota"

      # The finalizer and the quota refresh that follows it both reach a
      # convergence site on this path; the settled lifecycle keeps the second
      # one silent, so the transition stays one sample.
      assert drain(events) == []
    end

    test "an outer transaction around the real worker suppresses the count" do
      fixture = redemption_convergence_fixture!()
      events = capture!(@convergence, :convergence)

      assert {:ok, :ok} =
               Repo.transaction(fn ->
                 perform_job(SavedResetRedemptionWorker, scheduled_redemption_args(fixture))
               end)

      assert drain(events) == []
      assert converged_phase(fixture.identity) == "confirmed_by_quota"
    end
  end

  describe "quota cycle decisions from every declared job entrypoint" do
    test "the account reconciliation worker decides a cycle while persisting evidence" do
      fixture = weekly_restart_fixture!()
      events = capture!(@quota_decision, :quota)

      assert perform_job(AccountReconciliationWorker, reconciliation_args(fixture)) == :ok

      assert [{%{count: 1}, metadata, in_transaction?}] = drain(events)
      assert metadata == %{scope: "account", decision: :candidate, source: "provider_usage"}
      # This family is the one emitted from inside the evidence transaction:
      # a rolled-back evidence write takes its decision count with it.
      assert in_transaction?
    end

    test "the alert evaluation worker decides a cycle through the routing filter" do
      fixture = superseded_primary_fixture!()
      rule = alert_rule_fixture(fixture.pool)
      events = capture!(@quota_decision, :quota)

      assert perform_job(AlertEvaluationWorker, alert_args(rule, fixture.as_of), attempted_at: fixture.as_of) == :ok

      decisions = drain(events)
      assert decisions != []

      for {measurements, metadata, in_transaction?} <- decisions do
        assert measurements == %{count: 1}

        assert metadata == %{
                 scope: "account",
                 decision: :superseded_primary_rejected,
                 source: "provider_usage"
               }

        refute in_transaction?
      end
    end

    test "the saved-reset redemption worker decides a cycle while classifying evidence" do
      fixture = redemption_superseded_fixture!()
      events = capture!(@quota_decision, :quota)

      assert perform_job(SavedResetRedemptionWorker, scheduled_redemption_args(fixture)) == :ok

      decisions = drain(events)
      assert decisions != []

      assert Enum.any?(decisions, fn {_measurements, metadata, _in_transaction?} ->
               metadata.decision == :superseded_primary_rejected
             end)

      for {measurements, _metadata, _in_transaction?} <- decisions,
          do: assert(measurements == %{count: 1})
    end
  end

  describe "quota cycle decision multiplicity" do
    test "one filter call counts each superseded window exactly once" do
      as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      stale = stale_primary_window(as_of)
      current = fresh_secondary_window(as_of)
      events = capture!(@quota_decision, :quota)

      assert Routing.reject_superseded_primary_windows([stale, current], as_of) == [current]

      # Exactly one per rejected window per call: the filter rejects and counts
      # in the same `Enum.reject/2` pass, so a second predicate evaluation
      # would be a second sample.
      assert [{%{count: 1}, %{decision: :superseded_primary_rejected}, _in_transaction?}] =
               drain(events)

      second = %{stale | window_minutes: 60}

      assert Routing.reject_superseded_primary_windows([stale, second, current], as_of) == [
               current
             ]

      assert length(drain(events)) == 2

      # A window nothing supersedes is not counted at all.
      assert Routing.reject_superseded_primary_windows([current], as_of) == [current]
      assert drain(events) == []
    end

    test "one superseded window is counted once per evaluation, not once per window" do
      # The counter measures filter calls, not distinct transitions: the alert
      # projection folds the same identity's effective windows more than once
      # per evaluation, so a single superseded primary produces several
      # identical samples. That is the shape an operator's `sum` sees, and it
      # is pinned here so a projection change that alters it is visible rather
      # than silently rescaling the panel.
      fixture = superseded_primary_fixture!()
      rule = alert_rule_fixture(fixture.pool)
      events = capture!(@quota_decision, :quota)

      assert perform_job(AlertEvaluationWorker, alert_args(rule, fixture.as_of), attempted_at: fixture.as_of) == :ok

      assert length(drain(events)) == 3
    end
  end

  describe "real-worker relay round trip" do
    test "stale_sweep reaches the reporter through PostgreSQL with identical samples", context do
      %{now: now} = stale_reservation_fixture!("relay-stale-sweep-round-trip")

      round_trip(context, "pre_attempt_release", "codex_pooler_accounting_reservation", fn ->
        assert perform_job(RuntimeStateCleanupWorker, %{"now" => DateTime.to_iso8601(now)}) == :ok
      end)
    end

    test "interrupted stream outcomes reach the reporter with identical samples", context do
      expired_owner_fixture!()

      round_trip(context, "stream_outcome", "codex_pooler_gateway_stream_outcome", fn ->
        assert perform_job(RuntimeStateCleanupWorker, %{}) == :ok
      end)
    end

    test "saved-reset convergence reaches the reporter with identical samples", context do
      fixture = reconciliation_convergence_fixture!()

      round_trip(context, "saved_reset_convergence", "codex_pooler_saved_reset_convergence", fn ->
        assert perform_job(AccountReconciliationWorker, reconciliation_args(fixture)) == :ok
      end)
    end

    test "quota cycle decisions reach the reporter with identical samples", context do
      fixture = weekly_restart_fixture!()

      round_trip(context, "quota_cycle_decision", "codex_pooler_quota_cycle_decision", fn ->
        assert perform_job(AccountReconciliationWorker, reconciliation_args(fixture)) == :ok
      end)
    end
  end

  # A producer-role runtime captures what the job emits, a reporter counts the
  # same emission in process, and a reporter-role runtime replays the claimed
  # rows into that same reporter. The two shares must then render as the same
  # scrape lines once the `via` label that distinguishes them is normalized:
  # equal label sets, equal values, equal histogram buckets.
  defp round_trip(context, relay_event, metric_prefix, drive) do
    producer = relay_runtime(context, role: "worker")
    registry = reporter()

    drive.()

    sync(producer, :flush)
    # The expired-owner fixture settles its reservation on the way out, so a
    # pass can buffer more than one family; what this asserts is that this
    # family's rows left the producer, and the scrape comparison below is
    # scoped to this family's metric names.
    assert relay_event in Enum.map(Repo.all(RelayEvent), & &1.event)
    stop_runtime(producer)

    consumer = relay_runtime(context, role: "web")
    sync(consumer, :drain)

    in_process = normalized_lines(registry, metric_prefix, "in_process")
    relayed = normalized_lines(registry, metric_prefix, "job_relay")

    assert in_process != []
    assert relayed == in_process
  end

  defp normalized_lines(registry, metric_prefix, via) do
    registry
    |> Core.scrape()
    |> String.split("\n")
    |> Enum.filter(&(String.starts_with?(&1, metric_prefix) and String.contains?(&1, ~s(via="#{via}"))))
    |> Enum.map(&String.replace(&1, ~s(via="#{via}"), ~s(via="normalized")))
    |> Enum.sort()
  end

  defp relay_runtime(context, opts) do
    id = make_ref()

    pid =
      start_supervised!(%{
        id: id,
        start:
          {RelayRuntime, :start_link,
           [
             Keyword.merge(
               [enabled: true, start_paused: true, name: nil, flush_ms: 60_000, drain_ms: 60_000],
               opts
             )
           ]}
      })

    Sandbox.allow(Repo, context.sandbox_owner, pid)
    Process.put({:relay_runtime_child, pid}, id)
    GenServer.call(pid, :activate)
    pid
  end

  defp stop_runtime(pid) when is_pid(pid),
    do: ExUnit.Callbacks.stop_supervised!(Process.get({:relay_runtime_child, pid}))

  defp reporter do
    registry = Module.concat(__MODULE__, "Registry#{System.unique_integer([:positive])}")

    start_supervised!({Core, metrics: CodexPoolerWeb.Telemetry.prometheus_metrics(), name: registry, start_async: false})

    registry
  end

  defp sync(pid, message) do
    send(pid, message)
    :sys.get_state(pid)
  end

  # Registered before attachment, filtered to the owned event, and carrying the
  # emitting process's transaction state: whether the count happened inside the
  # caller's transaction is the property three of these four families decide on.
  defp capture!(event, tag) do
    parent = self()
    handler_id = {__MODULE__, tag, System.unique_integer([:positive, :monotonic])}
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        event,
        fn _event, measurements, metadata, _config ->
          send(parent, {handler_id, measurements, metadata, Repo.in_transaction?()})
        end,
        nil
      )

    handler_id
  end

  defp drain(handler_id, acc \\ []) do
    receive do
      {^handler_id, measurements, metadata, in_transaction?} ->
        drain(handler_id, [{measurements, metadata, in_transaction?} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp stale_reservation_fixture!(correlation_id) do
    setup = accounting_setup()
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{
                 "model" => setup.model.exposed_model_id,
                 "max_output_tokens" => 10,
                 "stream" => true
               },
               %{correlation_id: correlation_id, now: DateTime.add(now, -7, :hour)}
             )

    assert reserved.request.transport == "http_sse"
    %{setup: setup, request: reserved.request, now: now}
  end

  defp release_entries(request) do
    Repo.all(
      from entry in CodexPooler.Accounting.LedgerEntry,
        where: entry.request_id == ^request.id and entry.entry_kind == "release"
    )
  end

  defp expired_owner_fixture! do
    unique = System.unique_integer([:positive, :monotonic])
    setup = accounting_setup(%{account_label: "Relay job emission #{unique}"})

    assert {:ok, session} =
             Gateway.start_codex_session(setup.auth, %{
               accepted_turn_state: "relay-job-emission-#{unique}"
             })

    assert {:ok, reserved} =
             Accounting.reserve(
               setup.auth,
               setup.model,
               %{"model" => setup.model.exposed_model_id},
               %{
                 endpoint: "/backend-api/codex/responses",
                 transport: "websocket",
                 correlation_id: "relay-job-emission-#{unique}",
                 request_metadata: %{"codex_session_id" => session.id}
               }
             )

    request_options =
      RequestOptions.for_websocket(%{
        request_id: "relay-job-emission-#{unique}",
        interrupt_reason: "client_disconnected",
        reconnect_window_seconds: 300
      })

    assert {:ok, turn} =
             SessionContinuity.start_codex_turn(session, reserved.request, request_options)

    past = DateTime.add(DateTime.utc_now(), -60, :second)

    Repo.update_all(from(s in CodexSession, where: s.id == ^session.id),
      set: [owner_lease_expires_at: past]
    )

    Repo.update_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id),
      set: [expires_at: past]
    )

    %{setup: setup, session: session, request: reserved.request, turn: turn}
  end

  defp reconciliation_convergence_fixture! do
    as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    consumed_at = DateTime.add(as_of, -120, :second)
    upstream = fake_upstream!(%{"/backend-api/wham/usage" => {200, usage_payload(as_of, 25)}})

    fixture =
      assignment_fixture!(%{
        "base_url" => FakeUpstream.url(upstream),
        "access_token_expires_at" => DateTime.utc_now() |> DateTime.add(10, :day) |> DateTime.to_iso8601(),
        "saved_reset_redemption" => pending_redemption(consumed_at)
      })

    Map.put(fixture, :as_of, as_of)
  end

  defp redemption_convergence_fixture! do
    as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    upstream =
      fake_upstream!(%{
        "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
        "/api/codex/usage" => {200, put_in(usage_payload(as_of, 25), ["rate_limit_reset_credits", "available_count"], 0)}
      })

    fixture =
      assignment_fixture!(%{
        "usage_base_url" => FakeUpstream.url(upstream),
        "saved_resets" => banked_saved_resets(as_of)
      })

    enable_scheduled_expiry_policy!(fixture.identity)
    persist_windows!(fixture.identity, [fresh_secondary_window_attrs(as_of, "25")])
    Map.put(fixture, :as_of, as_of)
  end

  defp redemption_superseded_fixture! do
    fixture = redemption_convergence_fixture!()

    persist_windows!(fixture.identity, [
      stale_primary_window_attrs(fixture.as_of),
      fresh_secondary_window_attrs(fixture.as_of, "25")
    ])

    fixture
  end

  defp weekly_restart_fixture! do
    as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    upstream = fake_upstream!(%{"/backend-api/wham/usage" => {200, usage_payload(as_of, 0)}})

    fixture =
      assignment_fixture!(%{
        "base_url" => FakeUpstream.url(upstream),
        "access_token_expires_at" => DateTime.utc_now() |> DateTime.add(10, :day) |> DateTime.to_iso8601()
      })

    # A weekly window already carrying real usage: the incoming zero is the
    # unconfirmed restart the evidence store has to decide about.
    persist_windows!(fixture.identity, [
      fresh_secondary_window_attrs(DateTime.add(as_of, -600, :second), "25")
    ])

    Map.put(fixture, :as_of, as_of)
  end

  defp superseded_primary_fixture! do
    as_of = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    fixture = assignment_fixture!(%{})

    persist_windows!(fixture.identity, [
      stale_primary_window_attrs(as_of),
      fresh_secondary_window_attrs(as_of, "25")
    ])

    Map.put(fixture, :as_of, as_of)
  end

  defp assignment_fixture!(metadata) do
    pool = pool_fixture()

    %{identity: identity, assignment: assignment} =
      active_upstream_assignment_fixture(pool, %{metadata: metadata})

    %{pool: pool, identity: identity, assignment: assignment}
  end

  defp fake_upstream!(paths) do
    {:ok, upstream} = FakeUpstream.start_link({:path_json, paths})
    on_exit(fn -> FakeUpstream.stop(upstream) end)
    upstream
  end

  defp enable_scheduled_expiry_policy!(identity) do
    identity
    |> UpstreamIdentity.changeset(%{
      saved_reset_auto_redeem_enabled: true,
      saved_reset_auto_redeem_min_blocked_minutes: 60,
      saved_reset_auto_redeem_keep_credits: 0
    })
    |> Repo.update!()
  end

  defp persist_windows!(identity, attrs) do
    assert {:ok, windows} = Windows.upsert_quota_windows(identity, attrs)
    windows
  end

  defp converged_phase(identity),
    do: Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"]

  defp reconciliation_args(%{pool: pool, assignment: assignment}) do
    %{
      "pool_id" => pool.id,
      "pool_upstream_assignment_id" => assignment.id,
      "trigger_kind" => "scheduled"
    }
  end

  defp scheduled_redemption_args(%{identity: identity, assignment: assignment}) do
    %{
      "pool_upstream_assignment_id" => assignment.id,
      "upstream_identity_id" => identity.id,
      "target_kind" => "upstream_identity",
      "trigger_kind" => "scheduled_expiry_rescue"
    }
  end

  defp alert_args(rule, as_of) do
    %{
      "alert_rule_id" => rule.id,
      "evaluation_window_started_at" => DateTime.to_iso8601(as_of),
      "trigger_kind" => "test"
    }
  end

  defp pending_redemption(consumed_at) do
    %{
      "status" => "redeeming",
      "phase" => "consumed_pending_probe",
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 3,
      "trigger_kind" => "gateway_auto",
      "started_at" => DateTime.to_iso8601(consumed_at),
      "consumed_at" => DateTime.to_iso8601(consumed_at),
      "deadline_at" => consumed_at |> DateTime.add(15, :minute) |> DateTime.to_iso8601(),
      "finished_at" => nil,
      "result" => %{"code" => "reset", "applied" => true}
    }
  end

  defp banked_saved_resets(as_of) do
    observed_at = DateTime.to_iso8601(as_of)
    expires_at = as_of |> DateTime.add(1, :hour) |> DateTime.to_iso8601()

    %{
      "status" => "reported",
      "available_count" => 1,
      "source" => "codex_usage_api",
      "path_style" => "codex_api",
      "observed_at" => observed_at,
      "usage_path" => "/api/codex/usage",
      "available_expires_at" => [expires_at],
      "available_expirations" => [%{"expires_at" => expires_at, "first_seen_at" => observed_at}],
      "next_expires_at" => expires_at,
      "expires_observed_at" => observed_at,
      "expires_refresh_attempted_at" => observed_at,
      "reason" => nil
    }
  end

  defp usage_payload(as_of, used_percent) do
    %{
      "plan_type" => "pro",
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
      "spend_control" => %{"reached" => false},
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "secondary_window" => %{
          "used_percent" => used_percent,
          "limit_window_seconds" => 604_800,
          "reset_after_seconds" => 7_200,
          "reset_at" => as_of |> DateTime.add(7_200, :second) |> DateTime.to_unix()
        }
      },
      "rate_limit_reset_credits" => %{"available_count" => 1}
    }
  end

  defp stale_primary_window_attrs(as_of) do
    frozen_at = DateTime.add(as_of, -7_200, :second)

    %{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      window_kind: "primary",
      window_minutes: 300,
      used_percent: Decimal.new("10"),
      reset_at: DateTime.add(as_of, -3_600, :second),
      observed_at: frozen_at,
      last_sync_at: frozen_at,
      source: "codex_usage_api",
      source_precision: "observed",
      freshness_state: "fresh"
    }
  end

  defp fresh_secondary_window_attrs(as_of, used_percent) do
    %{
      quota_key: "account",
      quota_scope: "account",
      quota_family: "account",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new(used_percent),
      reset_at: DateTime.add(as_of, 7_200, :second),
      observed_at: as_of,
      last_sync_at: as_of,
      source: "codex_usage_api",
      source_precision: "observed",
      freshness_state: "fresh"
    }
  end

  defp stale_primary_window(as_of),
    do: struct!(CodexPooler.Upstreams.Quota.AccountQuotaWindow, stale_primary_window_attrs(as_of))

  defp fresh_secondary_window(as_of),
    do:
      struct!(
        CodexPooler.Upstreams.Quota.AccountQuotaWindow,
        fresh_secondary_window_attrs(as_of, "25")
      )
end
