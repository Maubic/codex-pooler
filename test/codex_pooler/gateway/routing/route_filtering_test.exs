defmodule CodexPooler.Gateway.Routing.RouteFilteringTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 2, start_upstream: 1]

  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.Catalog
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Payloads.RequestOptions.Transport
  alias CodexPooler.Gateway.Persistence.BridgeSessionAlias
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Gateway.Persistence.SessionContinuity, as: ContinuityStore
  alias CodexPooler.Gateway.Routing.CandidateEligibility
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.SavedResetAutoRedeem
  alias CodexPooler.Gateway.Routing.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Repo
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.TestDiagnostics
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.AccountAvailabilityStore
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation
  alias CodexPooler.Upstreams.SavedResets.AutoEligibility
  alias CodexPooler.Upstreams.SavedResets.AutomaticConfirmation
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  defmodule RouteFiltering do
    alias CodexPooler.Gateway.Routing.RouteFiltering, as: ProductionRouteFiltering

    defdelegate filter_candidates_with_route_state(filter_input, route_state),
      to: ProductionRouteFiltering

    defdelegate filter_candidates_with_route_state(filter_input, route_state, opts),
      to: ProductionRouteFiltering

    def filter_candidates(filter_input, opts \\ []) do
      route_state =
        RouteState.new(%{
          visible_model: filter_input.model,
          candidates: filter_input.candidates
        })
        |> RouteState.preload_routing_snapshots(
          filter_input.auth,
          filter_input.model,
          filter_input.request_options
        )

      case ProductionRouteFiltering.filter_candidates_with_route_state(
             filter_input,
             route_state,
             opts
           ) do
        {:ok, candidates, request_options, _route_state} ->
          {:ok, candidates, request_options}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  describe "filter_candidates/2" do
    test "allows missing quota evidence when the route marks quota optional" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()
      first = upstream_assignment_fixture(pool)
      second = upstream_assignment_fixture(pool)

      model =
        model_fixture(pool, %{
          exposed_model_id: "gpt-route-filtering-#{System.unique_integer([:positive])}",
          metadata: %{
            "source_assignment_ids" => [first.assignment.id, second.assignment.id]
          }
        })

      payload = %{"model" => model.exposed_model_id, "input" => "route filtering"}
      request_options = RequestOptions.build(%{}, "/backend-api/codex/responses", payload)
      candidates = [{first.assignment, first.identity}, {second.assignment, second.identity}]

      filter_input =
        FilterInput.new(%{
          auth: %{pool: pool, api_key: api_key},
          model: model,
          endpoint: "/backend-api/codex/responses",
          payload: payload,
          request_options: request_options,
          candidates: candidates
        })

      assert {:ok, filtered_candidates, filtered_options} =
               RouteFiltering.filter_candidates(filter_input, quota_mode: :optional)

      assert Enum.map(filtered_candidates, fn {assignment, _identity} -> assignment.id end) == [
               first.assignment.id,
               second.assignment.id
             ]

      assert filtered_options.routing.quota_decision == nil
    end

    test "keeps missing quota evidence blocking when quota is required" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()
      upstream = upstream_assignment_fixture(pool)

      model =
        model_fixture(pool, %{
          exposed_model_id: "gpt-route-filtering-required-#{System.unique_integer([:positive])}",
          metadata: %{"source_assignment_ids" => [upstream.assignment.id]}
        })

      payload = %{"model" => model.exposed_model_id, "input" => "route filtering"}
      request_options = RequestOptions.build(%{}, "/backend-api/codex/responses", payload)

      filter_input =
        FilterInput.new(%{
          auth: %{pool: pool, api_key: api_key},
          model: model,
          endpoint: "/backend-api/codex/responses",
          payload: payload,
          request_options: request_options,
          candidates: [{upstream.assignment, upstream.identity}]
        })

      assert {:error,
              %{
                code: "quota_evidence_unavailable",
                quota_refresh_attempted: false
              }} = RouteFiltering.filter_candidates(filter_input)
    end

    test "a current provider blocker keeps required and optional routes exhausted" do
      upstream =
        start_upstream(
          FakeUpstream.json_response(%{
            "plan_type" => "pro",
            "rate_limit" => %{"allowed" => false, "limit_reached" => true},
            "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
            "spend_control" => %{"reached" => false}
          })
        )

      %{pool: pool, api_key: api_key, assignment: assignment, identity: identity} = gateway_setup(upstream, quota?: false)
      assert {:ok, blocked_identity} = PoolReconciliation.refresh_quota_from_usage(identity, assignment)
      snapshot_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      filter_input =
        filter_input(pool, api_key, assignment, blocked_identity, "provider-blocked-optional")

      route_state =
        RouteState.new(%{
          visible_model: filter_input.model,
          candidates: filter_input.candidates,
          circuit_snapshots: %{assignment.id => true}
        })
        |> put_test_quota_snapshots(%{blocked_identity.id => []}, snapshot_at)

      for opts <- [[], [quota_mode: :optional]] do
        assert {:error,
                %{
                  status: 503,
                  code: "quota_exhausted",
                  quota_refresh_attempted: false,
                  candidate_exclusions: [
                    %{
                      reasons: [%{"reason_codes" => ["exhausted"], "quota_key" => "account", "quota_scope" => "account", "quota_family" => "account"}]
                    }
                  ]
                }} =
                 RouteFiltering.filter_candidates_with_route_state(
                   filter_input,
                   route_state,
                   opts
                 )
      end
    end

    test "stale availability refreshed to blocked keeps required and optional routes exhausted" do
      upstream =
        start_upstream(
          FakeUpstream.json_response(%{
            "plan_type" => "plus",
            "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
            "spend_control" => %{"reached" => false},
            "rate_limit" => %{
              "allowed" => false,
              "limit_reached" => true,
              "primary_window" => nil,
              "secondary_window" => nil
            }
          })
        )

      setup = gateway_setup(upstream, quota?: false)
      snapshot_at = DateTime.utc_now() |> DateTime.truncate(:second)
      stale_at = DateTime.add(snapshot_at, -Evidence.freshness_ttl_seconds() - 1, :second)

      identity =
        setup.identity
        |> Ecto.Changeset.change(
          metadata:
            Map.put(
              setup.identity.metadata,
              AccountAvailabilityStore.metadata_key(),
              AccountAvailabilityStore.encode!(:available, stale_at, CredentialFencing.credential_epoch(setup.identity))
            )
        )
        |> Repo.update!()

      filter_input =
        filter_input(
          setup.pool,
          setup.api_key,
          setup.assignment,
          identity,
          "stale-provider-blocker"
        )

      route_state =
        RouteState.new(%{
          visible_model: filter_input.model,
          candidates: filter_input.candidates,
          circuit_snapshots: %{setup.assignment.id => true}
        })
        |> RouteState.put_quota_snapshots(QuotaWindows.load_routing_quota_snapshots([identity.id], snapshot_at))

      for opts <- [[], [quota_mode: :optional]] do
        assert {:error,
                %{
                  status: 503,
                  code: "quota_exhausted",
                  quota_refresh_attempted: true,
                  candidate_exclusions: [
                    %{
                      reasons: [%{"reason_codes" => ["exhausted"], "quota_key" => "account", "quota_scope" => "account", "quota_family" => "account"}]
                    }
                  ]
                }} =
                 RouteFiltering.filter_candidates_with_route_state(
                   filter_input,
                   route_state,
                   opts
                 )
      end

      assert Repo.all(Attempt) == []
      assert FakeUpstream.count(upstream) == 4

      assert Enum.all?(FakeUpstream.requests(upstream), fn request ->
               request.path in ["/backend-api/wham/usage", "/backend-api/codex/usage"]
             end)
    end

    test "route-state filtering excludes a post-snapshot observation until the snapshot advances" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()
      %{assignment: assignment, identity: identity} = upstream_assignment_fixture(pool)
      filter_input = filter_input(pool, api_key, assignment, identity, "snapshot-boundary")
      snapshot_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      refreshed_at = DateTime.add(snapshot_at, 1, :microsecond)

      snapshots = %{
        identity.id => [
          account_window_at(Decimal.new("15"), snapshot_at),
          account_window_at(Decimal.new("100"), refreshed_at)
        ]
      }

      route_state =
        RouteState.new(%{
          visible_model: filter_input.model,
          candidates: filter_input.candidates,
          circuit_snapshots: %{assignment.id => true}
        })
        |> put_test_quota_snapshots(snapshots, snapshot_at)

      assert {:ok, [{^assignment, ^identity}], request_options, returned_route_state} =
               RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)

      assert request_options.routing.quota_decision["routing_state"] == "precise"

      assert RouteState.quota_snapshot_for_identity(returned_route_state, identity).as_of ==
               snapshot_at

      refreshed_route_state =
        put_test_quota_snapshots(route_state, snapshots, refreshed_at)

      assert {:error, %{code: "quota_exhausted"}} =
               RouteFiltering.filter_candidates_with_route_state(
                 filter_input,
                 refreshed_route_state
               )
    end

    test "routes to preserved catalog source when another same-pool source has exhausted quota" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()
      source_a = active_upstream_assignment_fixture(pool, %{account_label: "Synthetic source A"})
      source_b = active_upstream_assignment_fixture(pool, %{account_label: "Synthetic source B"})
      model_id = "gpt-preserved-runtime-#{System.unique_integer([:positive])}"

      assert {:ok, %{models: [_model]}} =
               sync_catalog_step(pool, %{
                 source_a.assignment.id => [
                   runtime_sync_model(model_id, %{"source_marker" => "a"})
                 ],
                 source_b.assignment.id => [
                   runtime_sync_model(model_id, %{"source_marker" => "b"})
                 ]
               })

      assert {:ok, %{models: [_model]}} =
               sync_catalog_step(pool, %{
                 source_a.assignment.id => [],
                 source_b.assignment.id => [
                   runtime_sync_model(model_id, %{"source_marker" => "b-current"})
                 ]
               })

      context = CandidateEligibility.visible_model_context(pool, model_id)
      assert context.visible_model.exposed_model_id == model_id

      assert candidate_ids(context.candidate_snapshots) == [
               source_a.assignment.id,
               source_b.assignment.id
             ]

      assert get_in(context.visible_model.metadata, [
               "source_assignment_models",
               source_a.assignment.id,
               "source_marker"
             ]) == "a"

      assert get_in(context.visible_model.metadata, [
               "source_assignment_models",
               source_b.assignment.id,
               "source_marker"
             ]) == "b-current"

      upsert_primary_quota!(source_a.identity, Decimal.new("100"))
      upsert_primary_quota!(source_b.identity, Decimal.new("15"))

      filter_input =
        filter_input(pool, api_key, context.visible_model, context.candidate_snapshots)

      assert {:ok, filtered_candidates, filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert candidate_ids(filtered_candidates) == [source_b.assignment.id]
      assert filtered_options.routing.quota_decision["routing_state"] == "precise"

      upsert_primary_quota!(source_b.identity, Decimal.new("100"))

      assert {:error, %{code: "quota_exhausted"}} =
               RouteFiltering.filter_candidates(filter_input)
    end

    test "does not redeem saved reset when auto policy is disabled by default" do
      {:ok, upstream} =
        FakeUpstream.start_link({:path_json, %{"/api/codex/usage" => {200, usage_payload(0)}}})

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "auto-disabled")

      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(filter_input)
      assert [] = FakeUpstream.requests(upstream)
    end

    test "a legacy confirmed phase without non-credit proof cannot route an exhausted account" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()
      observed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      payload = usage_payload(0, observed_at: observed_at, used_percent: 100, reset_at: DateTime.add(observed_at, 2, :hour))
      payload = put_in(payload, ["rate_limit", "allowed"], false) |> put_in(["rate_limit", "limit_reached"], true)
      {:ok, upstream} = FakeUpstream.start_link({:path_json, %{"/api/codex/usage" => {200, payload}}})

      consumed_at =
        DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata: Map.put(reset_probe_redemption("confirmed_by_upstream", consumed_at), "usage_path", "/api/codex/usage")
        })

      observe_provider!(identity, assignment, upstream, observed_at)
      identity = Repo.reload!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "reset-probe-confirmed")

      assert {:error, %{code: "quota_exhausted", candidate_exclusions: exclusions}} =
               RouteFiltering.filter_candidates(filter_input)

      assert [%{pool_upstream_assignment_id: assignment_id}] = exclusions
      assert assignment_id == assignment.id
      refute get_in(Repo.reload!(identity).metadata, ["saved_reset_redemption", "non_credit_confirmation"])
    end

    test "a legacy confirmed phase cannot bypass an independent model-scoped weekly block" do
      # An account reset phase never supplies authority for a model's own quota.
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      consumed_at =
        DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata: reset_probe_redemption("confirmed_by_upstream", consumed_at)
        })

      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      assert {:ok, [_window]} =
               QuotaWindows.upsert_quota_windows(identity, [
                 %{
                   quota_key: "codex_spark",
                   window_kind: "secondary",
                   window_minutes: 10_080,
                   used_percent: Decimal.new("100"),
                   reset_at: DateTime.add(now, 2, :hour),
                   observed_at: now,
                   last_sync_at: now,
                   source: "codex_usage_api",
                   source_precision: "observed",
                   quota_scope: "model",
                   quota_family: "codex_model",
                   model: "gpt-5.3-codex-spark",
                   freshness_state: "fresh"
                 }
               ])

      filter_input = filter_input(pool, api_key, assignment, identity, "spark-blocked")

      assert {:error, %{code: code}} = RouteFiltering.filter_candidates(filter_input)
      assert code in ["quota_exhausted", "quota_evidence_unavailable"]
    end

    test "does not route an exhausted account that is only pending probe confirmation" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      consumed_at =
        DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata: reset_probe_redemption("consumed_pending_probe", consumed_at)
        })

      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "reset-probe-pending")

      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(filter_input)
    end

    test "does not ordinarily route a claimed pending reset probe with otherwise usable quota" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      consumed_at =
        DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)

      redemption =
        "consumed_pending_probe"
        |> reset_probe_redemption(consumed_at)
        |> put_in(
          ["saved_reset_redemption", "probe"],
          %{"token" => Ecto.UUID.generate(), "claimed_at" => DateTime.to_iso8601(consumed_at)}
        )

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: redemption})

      upsert_primary_quota!(identity, Decimal.new("15"))
      filter_input = filter_input(pool, api_key, assignment, identity, "reset-probe-claimed")

      assert {:error, %{code: "quota_evidence_unavailable", candidate_exclusions: exclusions}} =
               RouteFiltering.filter_candidates(filter_input)

      assert [%{reasons: reasons}] = exclusions
      assert Enum.any?(reasons, &("saved_reset_probe_pending" in Map.get(&1, "reason_codes", [])))
    end

    @tag :route_filtering_regression
    test "usage accounting does not override recovering circuit and pending reset routing facts" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      consumed_at =
        DateTime.utc_now() |> DateTime.add(-60, :second) |> DateTime.truncate(:microsecond)

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata:
            "consumed_pending_probe"
            |> reset_probe_redemption(consumed_at)
            |> put_in(
              ["saved_reset_redemption", "probe"],
              %{
                "token" => Ecto.UUID.generate(),
                "claimed_at" => DateTime.to_iso8601(consumed_at)
              }
            )
        })

      upsert_primary_quota!(identity, Decimal.new("15"))

      filter_input =
        filter_input(pool, api_key, assignment, identity, "route-filtering-independent-facts")

      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      circuit =
        %RoutingCircuitState{}
        |> RoutingCircuitState.changeset(%{
          pool_id: pool.id,
          pool_upstream_assignment_id: assignment.id,
          upstream_identity_id: identity.id,
          model_identifier: filter_input.model.exposed_model_id,
          route_class: filter_input.route_class,
          status: "open",
          reason_code: "route-filtering-recovering",
          failure_count: 3,
          success_count: 0,
          opened_at: DateTime.add(now, -30, :second),
          next_probe_at: DateTime.add(now, -1, :second),
          metadata: %{"probe_in_flight_count" => 0},
          created_at: DateTime.add(now, -30, :second),
          updated_at: now
        })
        |> Repo.insert!()

      request = request_fixture(%{pool: pool, api_key: api_key})

      ledger_entry =
        ledger_entry_fixture(request, %{
          pool_upstream_assignment_id: assignment.id,
          upstream_identity_id: identity.id,
          occurred_at: now,
          usage_status: "usage_unknown",
          total_tokens: 999_999,
          settled_cost_micros: 999_999
        })

      route_state = route_state(filter_input)
      assert route_state.circuit_snapshots[assignment.id].eligible? == true

      assert {:error,
              %{
                code: "quota_evidence_unavailable",
                candidate_exclusions: [%{reasons: reasons}]
              }} =
               RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)

      assert Enum.any?(reasons, &("saved_reset_probe_pending" in Map.get(&1, "reason_codes", [])))

      assert Repo.reload!(ledger_entry).usage_status == "usage_unknown"
      assert Repo.reload!(circuit).status == "open"

      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] ==
               "consumed_pending_probe"
    end

    test "does not route an exhausted account whose reset-probe window has elapsed" do
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      consumed_at =
        DateTime.utc_now() |> DateTime.add(-30, :minute) |> DateTime.truncate(:microsecond)

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata: reset_probe_redemption("confirmed_by_upstream", consumed_at)
        })

      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "reset-probe-expired")

      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(filter_input)
    end

    test "auto redemption ignores stale in-progress redemption until manual recovery" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, %{"plan_type" => "pro", "rate_limit_reset_credits" => %{"available_count" => 0}}}
           }}
        )

      started_at = DateTime.utc_now() |> DateTime.add(-5, :minute) |> DateTime.to_iso8601()
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata:
            upstream
            |> saved_reset_metadata(1)
            |> Map.put("saved_reset_redemption", %{
              "status" => "redeeming",
              "attempt_id" => Ecto.UUID.generate(),
              "generation" => 1,
              "trigger_kind" => "gateway_auto",
              "started_at" => started_at,
              "finished_at" => nil,
              "result" => nil
            })
        })

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "auto-stale-redemption")

      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(filter_input)
      assert [] = FakeUpstream.requests(upstream)
    end

    @tag :saved_reset_redemption_cause
    test "auto redeems saved reset and refilters when weekly account quota is exhausted" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "auto-enabled")

      {{:ok, [{%{id: assignment_id}, %{id: identity_id}}], filtered_options}, log} =
        with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      assert assignment_id == assignment.id
      assert identity_id == identity.id
      assert filtered_options.routing.quota_decision["routing_state"] == "reset_probe"
      assert ResetProbe.bound?(filtered_options.routing.reset_probe)

      [consume_request, usage_request] = assert_auto_redeem_usage_requests(upstream)
      assert consume_request.method == "POST"
      assert consume_request.path == "/api/codex/rate-limit-reset-credits/consume"
      assert is_binary(consume_request.json["redeem_request_id"])
      assert usage_request.path == "/api/codex/usage"

      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "consumed_pending_probe"
      assert {:ok, _} = PoolReconciliation.reconcile_pool_account(pool.id, assignment.id)
      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
      assert consume_count(upstream) == 1
      assert {:ok, [{confirmed_assignment, _identity}], confirmed_options} = RouteFiltering.filter_candidates(filter_input)
      assert confirmed_assignment.id == assignment.id
      refute ResetProbe.bound?(confirmed_options.routing.reset_probe)

      persisted = Repo.reload!(identity)
      assert get_in(persisted.metadata, ["saved_reset_redemption", "result", "code"]) == "reset"

      assert get_in(persisted.metadata, ["saved_reset_redemption", "trigger_detail"]) ==
               "exhausted"

      assert log =~ "trigger_kind=gateway_auto trigger_detail=exhausted"
      metadata_json = CodexPooler.JSON.encode!(persisted.metadata)
      refute metadata_json =~ consume_request.json["redeem_request_id"]
      refute metadata_json =~ "credit_id"
    end

    for window_kind <- [:weekly, :monthly], serving_mode <- ["full", "lite"], carrier <- [:http_sse, :native_websocket, :bridged_websocket] do
      @tag :saved_reset_expiry_priority
      test "blocked #{window_kind} recovery ranks qualified expiry in #{serving_mode} #{carrier} context" do
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        payload = usage_payload(1, window_minutes: if(unquote(window_kind) == :monthly, do: 43_200, else: 10_080))
        mode = {:path_json, %{"/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}}, "/api/codex/usage" => {200, payload}}}
        {:ok, later_fake} = FakeUpstream.start_link(mode)
        {:ok, earlier_fake} = FakeUpstream.start_link(mode)

        on_exit(fn ->
          FakeUpstream.stop(later_fake)
          FakeUpstream.stop(earlier_fake)
        end)

        %{pool: pool, api_key: api_key} = active_api_key_fixture()

        candidates =
          for {fake, seconds} <- [{later_fake, 7200}, {earlier_fake, 3600}] do
            expiration = saved_reset_expiration_attrs(now, seconds) |> Map.put("expires_detail_status", "authoritative_rows")
            %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake, 2, expiration)})
            identity = enable_saved_reset_auto_redeem!(identity)

            if unquote(window_kind) == :weekly do
              upsert_weekly_exhausted_quota!(identity)
            else
              attrs = primary_quota_attrs(Decimal.new("100")) |> Map.merge(%{window_minutes: 43_200, reset_at: DateTime.add(now, 20, :day)})
              assert {:ok, [window]} = QuotaWindows.upsert_quota_windows(identity, [attrs])
              SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, windows: [window])
            end

            {assignment, identity}
          end

        [later, {earlier_assignment, earlier_identity}] = candidates
        input = filter_input(pool, api_key, candidates, "expiry-priority")

        options =
          RequestOptions.put_transport(input.request_options,
            transport: if(unquote(carrier) == :native_websocket, do: "websocket", else: "http_sse"),
            upstream_websocket_bridge?: unquote(carrier) == :bridged_websocket
          )

        input = %{input | request_options: options}
        assert Transport.upstream_transport(options.transport) == unquote(carrier)
        options = RequestOptions.put_model_serving_mode(options, configured_mode: unquote(serving_mode), effective_mode: unquote(serving_mode), source: "override")
        input = %{input | request_options: options}
        assert input.request_options.routing.model_serving_mode == unquote(serving_mode)

        {result, _log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
        assert consume_count(later_fake) == 0
        assert consume_count(earlier_fake) == 1
        assert Repo.reload!(earlier_identity).metadata["saved_reset_redemption"]["phase"] == "consumed_pending_probe"
        refute Repo.reload!(elem(later, 1)).metadata["saved_reset_redemption"]
        assert Repo.reload!(earlier_identity).metadata["saved_resets"]["available_count"] == 1
        assert priority_generation_count(earlier_fake) == 0
        assert priority_generation_count(later_fake) == 0
        [consume] = Enum.filter(FakeUpstream.requests(earlier_fake), &(&1.method == "POST"))
        refute Map.has_key?(consume.json, "credit_id")

        if unquote(window_kind) == :weekly do
          assert {:ok, [{assignment, identity}], returned_options} = result
          assert {assignment.id, identity.id} == {earlier_assignment.id, earlier_identity.id}
          assert ResetProbe.bound?(returned_options.routing.reset_probe)
        else
          assert {:error, %{code: "quota_exhausted", non_credit_recovery_outcome: "pending"}} = result
        end

        with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
        assert consume_count(earlier_fake) == 1
        assert consume_count(later_fake) == 0
        TestDiagnostics.puts("EXPIRY_RECEIPT " <> CodexPooler.JSON.encode!(%{window: unquote(window_kind), mode: unquote(serving_mode), carrier: unquote(carrier), selected: "earlier", earlier_consumes: consume_count(earlier_fake), later_consumes: consume_count(later_fake), generation_sends: priority_generation_count(earlier_fake) + priority_generation_count(later_fake)}))
      end
    end

    for blocker <- [:disabled, :primary_and_weekly, :model_and_weekly, :additional_and_weekly, :equal, :unknown] do
      @tag :saved_reset_expiry_priority
      test "expiry preference retains exact eligibility and fallback for #{blocker}" do
        blocker = Enum.at([unquote(blocker)], 0)
        sibling_block = if blocker in [:disabled, :equal, :unknown], do: :missing, else: blocker
        %{upstream: fake, target: target, sibling: sibling, input: input} = mixed_exclusion_arrangement(sibling_block, "blocked")
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        target_hint = saved_reset_expiration_attrs(now, 7200) |> Map.put("expires_detail_status", "authoritative_rows")
        sibling_hint = saved_reset_expiration_attrs(now, if(blocker == :equal, do: 7200, else: 3600)) |> Map.put("expires_detail_status", "authoritative_rows")

        for {identity, hint} <- [{target.identity, target_hint}, {sibling.identity, sibling_hint}] do
          identity |> Ecto.Changeset.change(metadata: Map.update!(identity.metadata, "saved_resets", &Map.merge(&1, hint))) |> Repo.update!()
        end

        if blocker in [:disabled, :equal, :unknown], do: upsert_weekly_exhausted_quota!(sibling.identity)
        if blocker == :disabled, do: sibling.identity |> Ecto.Changeset.change(saved_reset_auto_redeem_enabled: false) |> Repo.update!()

        if blocker == :unknown do
          for identity <- [target.identity, sibling.identity] do
            persisted = Repo.reload!(identity)
            persisted |> Ecto.Changeset.change(metadata: Map.update!(persisted.metadata, "saved_resets", &Map.put(&1, "expires_detail_status", "incomplete"))) |> Repo.update!()
          end
        end

        candidates = Enum.map(input.candidates, fn {assignment, identity} -> {assignment, Repo.reload!(identity)} end)
        input = FilterInput.put_candidates(input, candidates)
        {{:ok, [{selected_assignment, selected_identity}], _options}, _log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
        expected = if blocker in [:equal, :unknown], do: sibling, else: target
        assert {selected_assignment.id, selected_identity.id} == {expected.assignment.id, expected.identity.id}
        assert consume_count(fake) == 1
        assert priority_generation_count(fake) == 0
        other = if expected == target, do: sibling, else: target
        refute Repo.reload!(other.identity).metadata["saved_reset_redemption"]
        with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
        assert consume_count(fake) == 1
        TestDiagnostics.puts("EXPIRY_FENCE_RECEIPT " <> CodexPooler.JSON.encode!(%{scenario: blocker, consumes: consume_count(fake), generation_sends: priority_generation_count(fake), selected: if(expected == target, do: "eligible_target", else: "original_first")}))
      end
    end

    @tag :saved_reset_expiry_priority
    test "a ranked target losing locked policy authority never consumes the next account" do
      %{upstream: fake, target: target, sibling: sibling, input: input} = mixed_exclusion_arrangement(:missing, "blocked")
      upsert_weekly_exhausted_quota!(sibling.identity)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      for {identity, seconds} <- [{target.identity, 7200}, {sibling.identity, 3600}] do
        hint = saved_reset_expiration_attrs(now, seconds) |> Map.put("expires_detail_status", "authoritative_rows")
        identity |> Ecto.Changeset.change(metadata: Map.update!(identity.metadata, "saved_resets", &Map.merge(&1, hint))) |> Repo.update!()
      end

      candidates = Enum.map(input.candidates, fn {assignment, identity} -> {assignment, Repo.reload!(identity)} end)
      input = FilterInput.put_candidates(input, candidates)
      # The scan owns an enabled snapshot; the shared locked row has lost authorization.
      Repo.reload!(sibling.identity) |> Ecto.Changeset.change(saved_reset_auto_redeem_enabled: false) |> Repo.update!()
      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
      assert {:error, %{code: "quota_exhausted"}} = result
      assert log =~ "result_code=gateway_auto_policy_disabled"
      assert consume_count(fake) == 0
      assert priority_generation_count(fake) == 0
      refute Repo.reload!(target.identity).metadata["saved_reset_redemption"]
      refute Repo.reload!(sibling.identity).metadata["saved_reset_redemption"]
      TestDiagnostics.puts("EXPIRY_FENCE_RECEIPT " <> CodexPooler.JSON.encode!(%{scenario: "ranked_locked_policy_loss", consumes: consume_count(fake), generation_sends: priority_generation_count(fake), selected: "refused_without_fallthrough"}))
    end

    for sibling_block <- [
          :missing,
          :primary,
          :model,
          :additional,
          :primary_and_weekly,
          :model_and_weekly,
          :additional_and_weekly
        ],
        mode <- ["blocked", "threshold"] do
      @tag :saved_reset_mixed_exclusions
      test "redeems weekly target in #{mode} mode with #{sibling_block} sibling exclusion" do
        %{upstream: upstream, target: target, input: input, sibling: sibling} =
          mixed_exclusion_arrangement(unquote(sibling_block), unquote(mode))

        {{:ok, [{assignment, identity}], options}, _log} =
          with_info_log(fn -> RouteFiltering.filter_candidates(input) end)

        assert assignment.id == target.assignment.id
        assert identity.id == target.identity.id
        assert ResetProbe.bound?(options.routing.reset_probe)
        assert options.routing.reset_probe.pool_upstream_assignment_id == target.assignment.id
        assert options.routing.reset_probe.upstream_identity_id == target.identity.id
        assert consume_count(upstream) == 1
        assert Repo.reload!(target.identity).metadata["saved_resets"]["available_count"] == 1
        assert {:ok, _} = PoolReconciliation.reconcile_pool_account(input.auth.pool.id, target.assignment.id)
        assert Repo.reload!(target.identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
        refute Repo.reload!(sibling.identity).metadata["saved_reset_redemption"]

        # Restore corroborated pressure with a remaining credit: cooldown/latch,
        # not an empty bank or newly available quota, prevents a second consume.
        upsert_weekly_exhausted_quota!(Repo.reload!(target.identity))
        with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
        assert consume_count(upstream) == 1
      end
    end

    @tag :saved_reset_mixed_exclusions
    test "mixed exclusions do not authorize a target with an independent blocker" do
      for blocker <- [:primary, :model, :additional, :uncorroborated, :missing, :disabled] do
        %{upstream: upstream, target: target, input: input} =
          mixed_exclusion_arrangement(:primary, "threshold", blocker)

        assert {:error, _error} = RouteFiltering.filter_candidates(input)
        assert consume_count(upstream) == 0
        refute Repo.reload!(target.identity).metadata["saved_reset_redemption"]
        assert Repo.reload!(target.identity).metadata["saved_resets"]["available_count"] == 2
      end
    end

    @tag :saved_reset_mixed_exclusions
    test "usable sibling still prevents a mixed-exclusion weekly target from spending" do
      %{upstream: upstream, input: input, sibling: sibling} =
        mixed_exclusion_arrangement(:usable, "threshold")

      assert {:ok, [{assignment, _identity}], _options} = RouteFiltering.filter_candidates(input)
      assert assignment.id == sibling.assignment.id
      assert consume_count(upstream) == 0
    end

    for blocker <- [:primary, :monthly, :model, :additional, nil] do
      @tag :saved_reset_mixed_exclusions
      test "provider blocked account preserves #{inspect(blocker)} target window eligibility" do
        %{upstream: upstream, input: input, target: target} =
          mixed_exclusion_arrangement(:missing, "blocked", unquote(blocker))

        identity = Repo.reload!(target.identity)

        metadata =
          Map.put(
            identity.metadata,
            AccountAvailabilityStore.metadata_key(),
            AccountAvailabilityStore.encode!(:blocked, DateTime.utc_now(), CredentialFencing.credential_epoch(identity))
          )

        identity = identity |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()

        candidates =
          Enum.map(input.candidates, fn
            {assignment, %{id: id}} when id == identity.id -> {assignment, identity}
            candidate -> candidate
          end)

        input = FilterInput.put_candidates(input, candidates)

        # The same raw-window veto must run at claim and immediately before
        # dispatch even if the route's earlier exclusion was availability-only.
        plan = %{filter_input: input}

        {:ok, context} =
          plan
          |> SavedResetAutoRedeem.gateway_auto_context(
            target.assignment,
            identity,
            :blocked_weekly_exhaustion
          )
          |> AutoEligibility.normalize_context()

        timestamp = DateTime.utc_now()

        expected =
          if unquote(blocker) == nil, do: :ok, else: {:noop, "gateway_auto_trigger_not_current"}

        assert AutoEligibility.validate_locked_gateway_auto(
                 identity,
                 target.assignment,
                 context,
                 timestamp
               ) == expected

        assert AutoEligibility.validate_reserved_gateway_auto(
                 identity,
                 target.assignment,
                 context,
                 timestamp
               ) == expected

        {result, _log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)

        if unquote(blocker) == nil do
          assert consume_count(upstream) == 1
          assert {:ok, _, _} = result
        else
          assert consume_count(upstream) == 0
          assert {:error, _error} = result
        end
      end
    end

    @tag :saved_reset_expiry_priority
    test "expired model evidence releases weekly recovery only at the newer usage reading TTL boundary" do
      %{upstream: upstream, input: input, target: target} = mixed_exclusion_arrangement(:missing, "blocked", :model_and_weekly)
      timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      ttl = Evidence.freshness_ttl_seconds()
      # The supported weekly-only account shape retains the separate model meter.
      Repo.delete_all(from(window in AccountQuotaWindow, where: window.upstream_identity_id == ^target.identity.id and window.quota_scope == "account" and window.window_kind == "primary"))

      for {gap, expected} <- [{ttl - 1, {:noop, "gateway_auto_trigger_not_current"}}, {ttl, :ok}] do
        # Each arm owns its entire expired meter group: retention preserves an
        # in-cycle sibling, and evidence upsert refuses an older observation.
        Repo.delete_all(from(window in AccountQuotaWindow, where: window.upstream_identity_id == ^target.identity.id and window.quota_scope == "model"))
        observed_at = DateTime.add(timestamp, -gap, :second)

        attrs =
          weekly_exhausted_quota_attrs()
          |> Map.merge(%{
            quota_key: "sample_model",
            quota_scope: "model",
            quota_family: "codex_model",
            model: input.model.exposed_model_id,
            reset_at: DateTime.add(timestamp, -1, :second),
            observed_at: observed_at,
            last_sync_at: observed_at
          })

        assert {:ok, [_window]} = QuotaWindows.upsert_quota_windows(target.identity, [attrs])

        assert {:ok, [_window]} =
                 QuotaWindows.upsert_quota_windows(target.identity, [
                   weekly_exhausted_quota_attrs() |> Map.merge(%{observed_at: timestamp, last_sync_at: timestamp})
                 ])

        identity = SavedResetConfirmationFixtures.confirm_automatic_pressure!(target.identity, observed_at: timestamp)

        {:ok, context} =
          %{filter_input: input}
          |> SavedResetAutoRedeem.gateway_auto_context(target.assignment, identity, :blocked_weekly_exhaustion)
          |> AutoEligibility.normalize_context()

        assert AutoEligibility.target_windows_resettable?(identity, context.quota_scope, timestamp) == (gap == ttl)
        assert AutoEligibility.validate_locked_gateway_auto(identity, target.assignment, context, timestamp) == expected
        assert AutoEligibility.validate_reserved_gateway_auto(identity, target.assignment, context, timestamp) == expected
        assert consume_count(upstream) == 0
      end

      candidates = Enum.map(input.candidates, fn {assignment, identity} -> {assignment, Repo.reload!(identity)} end)
      input = FilterInput.put_candidates(input, candidates)
      {{:ok, [{assignment, _identity}], _options}, _log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
      assert assignment.id == target.assignment.id
      assert consume_count(upstream) == 1
      assert priority_generation_count(upstream) == 0
    end

    @tag :saved_reset_mixed_exclusions
    test "monthly-only exhaustion without corroboration does not authorize a reset" do
      %{upstream: upstream, input: input, target: target} =
        mixed_exclusion_arrangement(:missing, "blocked", :missing)

      put_mixed_quota_block!(target.identity, :monthly, input.model)
      assert {:error, _error} = RouteFiltering.filter_candidates(input)
      assert consume_count(upstream) == 0
    end

    for {mode, percent} <- [{"blocked", "100"}, {"threshold", "96"}] do
      @tag :monthly_saved_reset
      test "monthly account #{mode} pressure redeems once and confirms from provider quota" do
        %{upstream: upstream, input: input, target: target} =
          mixed_exclusion_arrangement(:missing, unquote(mode), :missing)

        attrs =
          primary_quota_attrs(Decimal.new(unquote(percent)))
          |> Map.merge(%{
            window_minutes: 43_200,
            reset_at: DateTime.add(DateTime.utc_now(), 20, :day)
          })

        assert {:ok, [window]} = QuotaWindows.upsert_quota_windows(target.identity, [attrs])

        SavedResetConfirmationFixtures.confirm_automatic_pressure!(target.identity,
          windows: [window]
        )

        monthly_payload = usage_payload(1, window_minutes: 43_200, used_percent: 0, reset_at: DateTime.add(window.reset_at, 10, :day))

        FakeUpstream.set_mode(
          upstream,
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, monthly_payload}
           }}
        )

        # Threshold policy still needs every dispatch participant at pressure.
        input = FilterInput.put_candidates(input, [{target.assignment, target.identity}])
        {result, _log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
        assert consume_count(upstream) == 1

        if unquote(mode) == "blocked" do
          assert {:error, %{status: 503, code: "quota_exhausted", non_credit_recovery_outcome: "pending"}} = result
        else
          assert {:ok, [{assignment, _identity}], _options} = result
          assert assignment.id == target.assignment.id
        end

        refute get_in(Repo.reload!(target.identity).metadata, ["saved_reset_redemption", "probe"])

        assert Repo.reload!(target.identity).metadata["saved_reset_redemption"]["phase"] ==
                 "consumed_pending_probe"

        # The monthly window has no weekly guarded-probe escape hatch. Two
        # strictly newer matching positive receipts confirm the weak lower value.
        monthly_payload = usage_payload(1, window_minutes: 43_200, used_percent: 1, reset_at: window.reset_at)
        FakeUpstream.set_mode(upstream, {:path_json, %{"/api/codex/usage" => {200, monthly_payload}}})

        for expected_phase <- ["consumed_pending_probe", "confirmed_by_quota"] do
          assert {:ok, _} = PoolReconciliation.reconcile_pool_account(input.auth.pool.id, target.assignment.id)
          assert Repo.reload!(target.identity).metadata["saved_reset_redemption"]["phase"] == expected_phase
        end

        assert {:ok, [{assignment, _identity}], _options} = RouteFiltering.filter_candidates(input)
        assert assignment.id == target.assignment.id
        assert consume_count(upstream) == 1
      end
    end

    @tag :monthly_saved_reset
    test "monthly threshold confirmation comes from two real provider receipts" do
      %{upstream: upstream, target: target, input: input} =
        mixed_exclusion_arrangement(:missing, "threshold", :missing)

      now = DateTime.utc_now() |> DateTime.truncate(:second)
      reset_at = DateTime.add(now, 20, :day)

      # Both receipts must advance the snapshot, regardless of the wall-clock second
      # in which the fixture was created.
      target.identity
      |> Ecto.Changeset.change(
        metadata:
          put_in(
            target.identity.metadata,
            ["saved_resets", "observed_at"],
            DateTime.to_iso8601(DateTime.add(now, -120, :second))
          )
      )
      |> Repo.update!()

      for age <- [60, 0] do
        observed_at = DateTime.add(now, -age, :second)

        payload = %{
          "rate_limit_reset_credits" => %{"available_count" => 2},
          "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
          "spend_control" => %{"reached" => false},
          "rate_limit" => %{
            "allowed" => true,
            "limit_reached" => false,
            "primary_window" => %{
              "used_percent" => 96,
              "limit_window_seconds" => 2_592_000,
              "reset_at" => DateTime.to_unix(reset_at),
              "reset_after_seconds" => DateTime.diff(reset_at, observed_at)
            }
          }
        }

        FakeUpstream.set_mode(upstream, {:path_json, %{"/api/codex/usage" => {200, payload}}})
        observe_provider!(target.identity, target.assignment, upstream, observed_at)
        [window] = QuotaWindows.list_evidence(target.identity)

        assert SavedResetConfirmationFixtures.marker_state(window) ==
                 if(age == 0, do: "confirmed", else: "candidate")
      end

      identity = Repo.reload!(target.identity)

      assert [_] =
               AutoEligibility.confirmation_refs(
                 :threshold_pressure,
                 identity,
                 [identity.id],
                 DateTime.utc_now()
               )

      FakeUpstream.set_mode(
        upstream,
        {:path_json,
         %{
           "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
           "/api/codex/usage" => {200, usage_payload(1, window_minutes: 43_200, used_percent: 96, reset_at: reset_at)}
         }}
      )

      input = FilterInput.put_candidates(input, [{target.assignment, identity}])
      {result, _log} = with_info_log(fn -> RouteFiltering.filter_candidates(input) end)
      assert {:ok, _, _} = result
      assert consume_count(upstream) == 1
    end

    @tag :monthly_saved_reset
    test "usable primary selection cannot hide conflicting weekly and monthly reset descriptors" do
      %{input: input, target: target} = mixed_exclusion_arrangement(:missing, "threshold")
      put_mixed_quota_block!(target.identity, :monthly, input.model)
      upsert_primary_quota!(target.identity, Decimal.new("10"))
      refute AutoEligibility.target_windows_resettable?(target.identity, nil, DateTime.utc_now())
    end

    @tag :saved_reset_mixed_exclusions
    test "weekly target eligibility requires all exact-pair exclusion records" do
      %{upstream: upstream, input: input, target: target, sibling: sibling} =
        mixed_exclusion_arrangement(:primary, "blocked")

      {:refreshable_quota, plan} = CandidateEligibility.filter_quota_eligible_candidates(input)

      {:error, error} =
        CandidateEligibility.quota_unavailable_error(input, plan.candidate_exclusions, false)

      target_exclusion =
        Enum.find(error.candidate_exclusions, &(&1.upstream_identity_id == target.identity.id))

      sibling_exclusion =
        Enum.find(error.candidate_exclusions, &(&1.upstream_identity_id == sibling.identity.id))

      blocked_target = %{target_exclusion | reasons: sibling_exclusion.reasons}

      for exclusions <- [
            [target_exclusion],
            [sibling_exclusion],
            [target_exclusion, sibling_exclusion, blocked_target],
            [blocked_target, sibling_exclusion, target_exclusion],
            [nil | error.candidate_exclusions],
            [%{target_exclusion | upstream_identity_id: sibling.identity.id}, sibling_exclusion],
            [%{target_exclusion | reasons: []}, sibling_exclusion],
            [%{target_exclusion | reasons: nil}, sibling_exclusion]
          ] do
        result = {:error, %{error | candidate_exclusions: exclusions}}

        assert {:error, %{code: "quota_exhausted", candidate_exclusions: returned_exclusions}} =
                 SavedResetAutoRedeem.maybe_redeem_after_quota_exhaustion(result, plan, :required)

        assert returned_exclusions == exclusions

        assert consume_count(upstream) == 0
      end
    end

    @tag :saved_reset_redemption_cause
    test "logs bounded gateway causes and preserves them on provider noop and failure" do
      provider_code_sentinel = "providersentinel7c91"

      for {trigger, detail, consume_response} <- [
            {:blocked_weekly_exhaustion, "exhausted", {200, %{"code" => "nothing_to_reset"}}},
            {:threshold_pressure, "threshold",
             {502,
              %{
                "code" => provider_code_sentinel,
                "message" => "provider-sensitive-body"
              }}}
          ] do
        {:ok, upstream} =
          FakeUpstream.start_link(
            {:path_json,
             %{
               "/api/codex/rate-limit-reset-credits/consume" => consume_response,
               "/api/codex/usage" => {200, usage_payload(0)}
             }}
          )

        %{pool: pool, api_key: api_key} = active_api_key_fixture()

        %{identity: identity, assignment: assignment} =
          active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

        policy =
          if trigger == :threshold_pressure,
            do: %{
              saved_reset_auto_redeem_trigger_mode: "threshold",
              saved_reset_auto_redeem_quota_threshold_percent: 95
            },
            else: %{}

        identity = enable_saved_reset_auto_redeem!(identity, policy)

        if trigger == :threshold_pressure do
          upsert_weekly_pressure_quota!(identity, Decimal.new("96"))
        else
          upsert_weekly_exhausted_quota!(identity)
        end

        filter_input = filter_input(pool, api_key, assignment, identity, "cause-#{detail}")

        {_result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

        redemption = Repo.reload!(identity).metadata["saved_reset_redemption"]
        assert redemption["trigger_detail"] == detail
        assert log =~ "trigger_kind=gateway_auto trigger_detail=#{detail}"
        refute CodexPooler.JSON.encode!(redemption) =~ provider_code_sentinel
        refute log =~ provider_code_sentinel
        refute log =~ "provider-sensitive-body"

        if trigger == :threshold_pressure do
          assert redemption["status"] == "redeeming"
          assert redemption["phase"] == "consuming"
          assert redemption["result"] == nil
          assert redemption["provider_replay"]["provider_dispatches"] == 1
          assert redemption["provider_replay"]["last_code"] == "provider_failed"
          assert log =~ "result_code=saved_reset_consume_outcome_ambiguous"
        else
          assert redemption["status"] == "noop"
        end
      end
    end

    test "a first-turn session does not bypass threshold sibling usable capacity" do
      %{
        upstream: upstream,
        filter_input: filter_input,
        sibling_identity: sibling_identity,
        target_identity: target_identity
      } = first_turn_capacity_arrangement("first-turn-capacity")

      before_target = Repo.reload!(target_identity).metadata
      before_sibling = Repo.reload!(sibling_identity).metadata

      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      # The successful threshold routing result is preserved; the burn is vetoed.
      assert {:ok, [_sibling_candidate, _target_candidate], request_options} = result

      assert log =~ "result_code=gateway_auto_sibling_usable_capacity"
      assert log =~ "applied=false"
      refute log =~ "result_code=reset"

      assert request_options.routing.reset_probe ==
               filter_input.request_options.routing.reset_probe

      refute ResetProbe.bound?(request_options.routing.reset_probe)

      assert FakeUpstream.requests(upstream) == []
      assert Repo.reload!(target_identity).metadata == before_target
      assert Repo.reload!(sibling_identity).metadata == before_sibling
    end

    test "a hard-pinned continuation retains its threshold capacity bypass" do
      %{
        upstream: upstream,
        unattached_filter_input: filter_input,
        target_identity: target_identity,
        pool: pool,
        api_key: api_key,
        session: session
      } = first_turn_capacity_arrangement("hard-pin-capacity")

      # The anchor resolves through an alias that existed before this request:
      # a genuinely pinned continuation, attached through the real seam.
      previous_response_id = "resp_hard_pin_capacity_baseline"

      register_session_alias!(
        pool,
        api_key,
        session,
        "previous_response_id",
        previous_response_id
      )

      assert {:ok, request_options} =
               SessionContinuity.attach_codex_session(
                 %{pool: pool, api_key: api_key},
                 %{"previous_response_id" => previous_response_id},
                 filter_input.request_options
               )

      assert request_options.continuity.codex_session.id == session.id

      filter_input = %{filter_input | request_options: request_options}

      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      assert {:ok, _candidates, _request_options} = result
      assert log =~ "result_code=reset"
      assert log =~ "applied=true"

      consume_paths =
        upstream
        |> FakeUpstream.requests()
        |> Enum.map(& &1.path)
        |> Enum.filter(&(&1 == "/api/codex/rate-limit-reset-credits/consume"))

      assert consume_paths == ["/api/codex/rate-limit-reset-credits/consume"]

      redemption = Repo.reload!(target_identity).metadata["saved_reset_redemption"]
      assert redemption["result"]["applied"] == true
      assert redemption["trigger_detail"] == "threshold"
    end

    test "a file-affinity hard pin retains its threshold capacity bypass" do
      %{
        upstream: upstream,
        filter_input: filter_input,
        target_assignment: target_assignment,
        target_identity: target_identity
      } = first_turn_capacity_arrangement("file-affinity-capacity")

      request_options =
        RequestOptions.put_routing(filter_input.request_options,
          file_affinity_assignment_id: target_assignment.id
        )

      assert SessionContinuity.hard_pinned_continuity?(
               request_options,
               filter_input.model
             )

      {result, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates(%{filter_input | request_options: request_options})
        end)

      assert {:ok, _candidates, _request_options} = result
      assert log =~ "result_code=reset"
      assert log =~ "applied=true"

      assert Enum.count(
               FakeUpstream.requests(upstream),
               &(&1.path == "/api/codex/rate-limit-reset-credits/consume")
             ) == 1

      redemption = Repo.reload!(target_identity).metadata["saved_reset_redemption"]
      assert redemption["result"]["applied"] == true
      assert redemption["trigger_detail"] == "threshold"
    end

    test "an unresolved previous response anchor does not bypass the capacity veto" do
      %{
        upstream: upstream,
        unattached_filter_input: filter_input,
        sibling_identity: sibling_identity,
        target_identity: target_identity,
        pool: pool,
        api_key: api_key,
        session: session
      } = first_turn_capacity_arrangement("unresolved-hard-pin")

      # The real attach seam: the unknown anchor does not resolve, the session
      # resolves through its soft session-header alias, and the attach then
      # registers the unknown anchor onto that session — a self-created alias
      # that must not count as a hard pin for the capacity bypass.
      session_header = "sess-unresolved-anchor-#{System.unique_integer([:positive])}"
      register_session_alias!(pool, api_key, session, "session_header", session_header)

      request_options =
        RequestOptions.put_continuity(filter_input.request_options,
          session_header: session_header,
          session_header_source: "x-session-id"
        )

      assert {:ok, request_options} =
               SessionContinuity.attach_codex_session(
                 %{pool: pool, api_key: api_key},
                 %{"previous_response_id" => "resp_unresolved_anchor"},
                 request_options
               )

      assert request_options.continuity.codex_session.id == session.id

      filter_input = %{filter_input | request_options: request_options}

      before_target = Repo.reload!(target_identity).metadata
      before_sibling = Repo.reload!(sibling_identity).metadata

      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      assert {:ok, [_sibling_candidate, _target_candidate], _request_options} = result

      assert log =~ "result_code=gateway_auto_sibling_usable_capacity"
      assert log =~ "applied=false"
      refute log =~ "result_code=reset"

      assert FakeUpstream.requests(upstream) == []
      assert Repo.reload!(target_identity).metadata == before_target
      assert Repo.reload!(sibling_identity).metadata == before_sibling
    end

    test "an anchor proven against a different assignment than the attached session does not bypass the capacity veto" do
      %{
        upstream: upstream,
        unattached_filter_input: filter_input,
        sibling_identity: sibling_identity,
        sibling_assignment: sibling_assignment,
        target_identity: target_identity,
        pool: pool,
        api_key: api_key,
        session: session
      } = first_turn_capacity_arrangement("cross-assignment-anchor")

      # The anchor's alias pre-exists and strictly resolves to a session pinned
      # on the sibling assignment. The proof and the anchor attach read the same
      # validity rules, so they can only disagree across a race — the anchor
      # session retargeting or its alias expiring between the read-only proof
      # and the locking attach — which is why the raced interleaving is composed
      # here from its two halves, each driven through the production seam: the
      # proof from the real strict lookup, the attached session from the real
      # session-header attach. A proof that names one assignment while the
      # attached session pins another must fail the comparison and keep the
      # capacity veto.
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      anchor_session =
        Repo.insert!(%CodexSession{
          pool_id: pool.id,
          api_key_id: api_key.id,
          session_key: "sess-cross-anchor-#{System.unique_integer([:positive])}",
          pool_upstream_assignment_id: sibling_assignment.id,
          status: "active",
          owner_instance_id: "route-filtering-test",
          owner_lease_token: Ecto.UUID.generate(),
          owner_lease_expires_at: DateTime.add(now, 1, :hour),
          last_heartbeat_at: now,
          created_at: DateTime.add(now, -4, :second),
          updated_at: DateTime.add(now, -4, :second)
        })

      previous_response_id = "resp_cross_assignment_anchor"

      register_session_alias!(
        pool,
        api_key,
        anchor_session,
        "previous_response_id",
        previous_response_id
      )

      resolved_assignment_id =
        ContinuityStore.previous_response_assignment_id(
          %{pool: pool, api_key: api_key},
          previous_response_id,
          now
        )

      assert resolved_assignment_id == sibling_assignment.id

      session_header = "sess-cross-anchor-header-#{System.unique_integer([:positive])}"
      register_session_alias!(pool, api_key, session, "session_header", session_header)

      request_options =
        RequestOptions.put_continuity(filter_input.request_options,
          session_header: session_header,
          session_header_source: "x-session-id"
        )

      assert {:ok, request_options} =
               SessionContinuity.attach_codex_session(
                 %{pool: pool, api_key: api_key},
                 %{},
                 request_options
               )

      assert request_options.continuity.codex_session.id == session.id

      request_options =
        RequestOptions.put_continuity(request_options,
          previous_response_id: previous_response_id,
          resolved_previous_response_assignment_id: resolved_assignment_id
        )

      refute SessionContinuity.hard_pinned_continuity?(request_options, filter_input.model)

      filter_input = %{filter_input | request_options: request_options}

      before_target = Repo.reload!(target_identity).metadata
      before_sibling = Repo.reload!(sibling_identity).metadata

      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      assert {:ok, [_sibling_candidate, _target_candidate], _request_options} = result

      assert log =~ "result_code=gateway_auto_sibling_usable_capacity"
      assert log =~ "applied=false"
      refute log =~ "result_code=reset"

      assert FakeUpstream.requests(upstream) == []
      assert Repo.reload!(target_identity).metadata == before_target
      assert Repo.reload!(sibling_identity).metadata == before_sibling
    end

    test "a live-websocket hard pin retains its non-bypass threshold policy" do
      %{
        upstream: upstream,
        filter_input: filter_input,
        sibling_identity: sibling_identity,
        target_identity: target_identity,
        session: session
      } = first_turn_capacity_arrangement("websocket-pin-capacity")

      # A websocket pin is a node-local process claim: the owner traps upstream
      # exits and its lease outlives it, so no shared state can prove the
      # upstream websocket is alive at the burn decision, and bound probes
      # suppress websocket recovery. The pin therefore never authorizes the
      # irreversible threshold burn — not with a dead upstream websocket pid
      # (the incident shape below), and not with a live one either.
      {dead_pid, dead_ref} = spawn_monitor(fn -> :ok end)
      assert_receive {:DOWN, ^dead_ref, :process, ^dead_pid, _reason}

      request_options = %{
        filter_input.request_options
        | transport: %{
            filter_input.request_options.transport
            | upstream_websocket_session: dead_pid
          }
      }

      refute SessionContinuity.hard_pinned_continuity?(request_options, filter_input.model)

      live_pinned_options = %{
        request_options
        | transport: %{request_options.transport | upstream_websocket_session: self()}
      }

      refute SessionContinuity.hard_pinned_continuity?(live_pinned_options, filter_input.model)

      owner_forwarded_options = %{
        request_options
        | transport: %{
            request_options.transport
            | upstream_websocket_session: nil,
              websocket_owner: %{
                request_options.transport.websocket_owner
                | enabled?: true,
                  session: session,
                  lease_token: "owner-lease-token",
                  downstream: %{pid: self(), correlation_id: "corr-websocket-pin"}
              }
          }
      }

      refute SessionContinuity.hard_pinned_continuity?(
               owner_forwarded_options,
               filter_input.model
             )

      filter_input = %{filter_input | request_options: request_options}

      before_target = Repo.reload!(target_identity).metadata
      before_sibling = Repo.reload!(sibling_identity).metadata

      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      assert {:ok, _candidates, returned_options} = result

      assert log =~ "result_code=gateway_auto_sibling_usable_capacity"
      assert log =~ "applied=false"
      refute log =~ "result_code=reset"
      assert returned_options.routing.reset_probe == request_options.routing.reset_probe
      refute ResetProbe.bound?(returned_options.routing.reset_probe)

      assert FakeUpstream.requests(upstream) == []
      assert Repo.reload!(target_identity).metadata == before_target
      assert Repo.reload!(sibling_identity).metadata == before_sibling
    end

    test "an owner-forwarded websocket pin retains its non-bypass threshold policy" do
      %{
        upstream: upstream,
        filter_input: filter_input,
        sibling_identity: sibling_identity,
        target_identity: target_identity,
        session: session
      } = first_turn_capacity_arrangement("owner-forwarded-capacity")

      request_options = %{
        filter_input.request_options
        | transport: %{
            filter_input.request_options.transport
            | websocket_owner: %{
                filter_input.request_options.transport.websocket_owner
                | enabled?: true,
                  session: session,
                  lease_token: "synthetic-owner-lease",
                  downstream: %{pid: self(), correlation_id: "synthetic-owner-correlation"}
              }
          }
      }

      refute SessionContinuity.hard_pinned_continuity?(request_options, filter_input.model)

      before_target = Repo.reload!(target_identity).metadata
      before_sibling = Repo.reload!(sibling_identity).metadata

      {result, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates(%{filter_input | request_options: request_options})
        end)

      assert {:ok, _candidates, returned_options} = result
      assert log =~ "result_code=gateway_auto_sibling_usable_capacity"
      assert log =~ "applied=false"
      refute log =~ "result_code=reset"
      refute ResetProbe.bound?(returned_options.routing.reset_probe)
      assert FakeUpstream.requests(upstream) == []
      assert Repo.reload!(target_identity).metadata == before_target
      assert Repo.reload!(sibling_identity).metadata == before_sibling
    end

    test "normal redemption refilters from a newer persisted snapshot and preserves route state" do
      historical_scan_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      expiration = DateTime.add(historical_scan_at, 1, :hour)
      natural_reset_at = DateTime.add(historical_scan_at, 2, :hour)

      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0, used_percent: 100, reset_at: natural_reset_at)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata:
            saved_reset_metadata(upstream, 1, %{
              "observed_at" => DateTime.to_iso8601(historical_scan_at),
              "available_expires_at" => [DateTime.to_iso8601(expiration)],
              "next_expires_at" => DateTime.to_iso8601(expiration),
              "expires_observed_at" => DateTime.to_iso8601(historical_scan_at),
              "expires_refresh_attempted_at" => DateTime.to_iso8601(historical_scan_at)
            })
        })

      identity = enable_saved_reset_auto_redeem!(identity)

      upsert_weekly_pressure_quota!(identity, Decimal.new("100"),
        observed_at: historical_scan_at,
        last_sync_at: historical_scan_at,
        reset_at: natural_reset_at
      )

      filter_input = filter_input(pool, api_key, assignment, identity, "newer-refilter")
      circuit_snapshot = %{eligible?: true, marker: "preserved"}
      visible_model_context = %{visible_model: filter_input.model, marker: "preserved"}
      parent = self()

      route_state =
        RouteState.new(%{
          visible_model: filter_input.model,
          visible_model_context: visible_model_context,
          candidates: filter_input.candidates,
          circuit_snapshots: %{assignment.id => circuit_snapshot}
        })
        |> put_test_quota_snapshots(
          %{identity.id => QuotaWindows.list_quota_windows(identity, historical_scan_at)},
          historical_scan_at
        )

      refilter_clock = fn ->
        persisted_weekly =
          identity
          |> QuotaWindows.list_evidence()
          |> Enum.find(&(&1.window_kind == "secondary" and &1.window_minutes == 10_080))

        assert %AccountQuotaWindow{} = persisted_weekly
        assert DateTime.compare(persisted_weekly.observed_at, historical_scan_at) == :gt
        assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
        send(parent, {:saved_reset_refilter_clock, persisted_weekly.id, persisted_weekly.observed_at})
        persisted_weekly.observed_at
      end

      assert {:ok, [{routed_assignment, routed_identity}], request_options, refreshed_route_state} =
               RouteFiltering.filter_candidates_with_route_state(
                 filter_input,
                 route_state,
                 saved_reset_scan_at: historical_scan_at,
                 saved_reset_refilter_clock: refilter_clock
               )

      assert routed_assignment.id == assignment.id
      assert routed_identity.id == identity.id
      assert_receive {:saved_reset_refilter_clock, persisted_weekly_id, refreshed_at}
      refute_received {:saved_reset_refilter_clock, _other_weekly_id, _other_at}
      refute ResetProbe.bound?(request_options.routing.reset_probe)
      assert RouteState.quota_snapshot_for_identity(refreshed_route_state, identity).as_of == refreshed_at
      assert DateTime.compare(refreshed_at, historical_scan_at) == :gt

      assert refreshed_route_state.visible_model_context == visible_model_context
      assert refreshed_route_state.circuit_snapshots[assignment.id] == circuit_snapshot
      assert refreshed_route_state.saved_reset_auto_cohort == route_state.saved_reset_auto_cohort

      assert RouteState.quota_snapshot_for_identity(route_state, identity).as_of ==
               historical_scan_at

      assert route_state.visible_model_context == visible_model_context
      assert route_state.circuit_snapshots[assignment.id] == circuit_snapshot

      old_rows = QuotaWindows.list_quota_windows(identity, historical_scan_at)
      refute Enum.any?(old_rows, &(&1.window_kind == "secondary"))

      assert Enum.any?(
               RouteState.quota_windows_for_identity(refreshed_route_state, identity),
               &(&1.id == persisted_weekly_id and &1.window_kind == "secondary" and
                   Decimal.equal?(&1.used_percent, Decimal.new(100)))
             )
    end

    test "refuses a guarded probe when usage omits both account capacity and credit authority" do
      # A consumed reset does not prove that the provider cannot charge credits.
      # Omitted authority leaves the bank spent but the request fail-closed.
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, %{"plan_type" => "pro", "rate_limit_reset_credits" => %{"available_count" => 0}}}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      %{identity: sibling_identity, assignment: sibling_assignment} =
        active_upstream_assignment_fixture(pool)

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_weekly_exhausted_quota!(identity)
      upsert_weekly_exhausted_quota!(sibling_identity)

      filter_input =
        filter_input(
          pool,
          api_key,
          [{assignment, identity}, {sibling_assignment, sibling_identity}],
          "auto-probe"
        )

      assert {:error, %{code: "quota_exhausted", candidate_exclusions: exclusions}} =
               RouteFiltering.filter_candidates(filter_input)

      assert Enum.sort(Enum.map(exclusions, & &1.pool_upstream_assignment_id)) == Enum.sort([assignment.id, sibling_assignment.id])
      redemption = Repo.reload!(identity).metadata["saved_reset_redemption"]
      assert redemption["phase"] == "consumed_pending_probe"
      assert redemption["result"]["applied"] == true
      assert consume_count(upstream) == 1
      refute redemption["probe"]
      refute Repo.reload!(sibling_identity).metadata["saved_reset_redemption"]
    end

    test "a recent latched candidate prevents a sibling auto-redeem" do
      {:ok, latched_upstream} = auto_redeem_fake()
      {:ok, sibling_upstream} = auto_redeem_fake()

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      latched_metadata =
        latched_upstream
        |> saved_reset_metadata(1)
        |> Map.put("saved_reset_redemption", applied_auto_redemption("reblocked", 5))

      %{identity: latched_identity, assignment: latched_assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: latched_metadata})

      %{identity: sibling_identity, assignment: sibling_assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata: saved_reset_metadata(sibling_upstream, 1)
        })

      latched_identity = enable_saved_reset_auto_redeem!(latched_identity)
      sibling_identity = enable_saved_reset_auto_redeem!(sibling_identity)
      upsert_weekly_exhausted_quota!(latched_identity)
      upsert_weekly_exhausted_quota!(sibling_identity)

      filter_input =
        filter_input(
          pool,
          api_key,
          [{latched_assignment, latched_identity}, {sibling_assignment, sibling_identity}],
          "latched-skip"
        )

      assert {:error, %{code: "quota_exhausted"}} =
               RouteFiltering.filter_candidates(filter_input)

      assert [] = FakeUpstream.requests(latched_upstream)
      assert [] = FakeUpstream.requests(sibling_upstream)

      persisted = Repo.reload!(latched_identity)
      assert get_in(persisted.metadata, ["saved_reset_redemption", "phase"]) == "reblocked"

      sibling_persisted = Repo.reload!(sibling_identity)
      refute get_in(sibling_persisted.metadata, ["saved_reset_redemption"])
    end

    test "a latched candidate's stale pressure cannot arm a threshold consume on a sibling" do
      {:ok, latched_upstream} = auto_redeem_fake()
      {:ok, sibling_upstream} = auto_redeem_fake()

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      latched_metadata =
        latched_upstream
        |> saved_reset_metadata(1)
        |> Map.put(
          "saved_reset_redemption",
          applied_auto_redemption("confirmed_by_quota", 5)
        )

      %{identity: latched_identity, assignment: latched_assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: latched_metadata})

      %{identity: sibling_identity, assignment: sibling_assignment} =
        active_upstream_assignment_fixture(pool, %{
          metadata: saved_reset_metadata(sibling_upstream, 1)
        })

      threshold_policy = %{
        saved_reset_auto_redeem_trigger_mode: "threshold",
        saved_reset_auto_redeem_quota_threshold_percent: 60
      }

      latched_identity = enable_saved_reset_auto_redeem!(latched_identity, threshold_policy)
      sibling_identity = enable_saved_reset_auto_redeem!(sibling_identity, threshold_policy)
      upsert_weekly_pressure_quota!(latched_identity, Decimal.new("95"))
      upsert_weekly_pressure_quota!(sibling_identity, Decimal.new("30"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [{latched_assignment, latched_identity}, {sibling_assignment, sibling_identity}],
          "latched-threshold"
        )

      assert {:ok, [_ | _], _filtered_options} = RouteFiltering.filter_candidates(filter_input)

      # No credit is spent anywhere: the latched identity's stale pressure is
      # excluded from the trigger computation, and the sibling's own genuine
      # pressure sits below the threshold, so nothing arms. A sibling at
      # genuine threshold pressure may still redeem on its own evidence.
      assert [] = FakeUpstream.requests(latched_upstream)
      assert [] = FakeUpstream.requests(sibling_upstream)
    end

    test "second stale auto attempt does not consume after current count was refreshed" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      stale_identity = enable_saved_reset_auto_redeem!(identity)
      upsert_weekly_exhausted_quota!(stale_identity)
      filter_input = filter_input(pool, api_key, assignment, stale_identity, "auto-stale-repeat")

      assert {:ok, [{%{id: assignment_id}, %{id: identity_id}}], filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert assignment_id == assignment.id
      assert identity_id == identity.id
      assert ResetProbe.bound?(filtered_options.routing.reset_probe)
      assert consume_count(upstream) == 1
      assert get_in(Repo.reload!(identity).metadata, ["saved_resets", "available_count"]) == 0

      # Confirm the quarantined weekly lower reading with the provider's second
      # distinct receipt; the stale input cannot reuse the claimed first probe.
      assert {:ok, _} = PoolReconciliation.reconcile_pool_account(pool.id, assignment.id)
      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
      requests_before_repeat = FakeUpstream.requests(upstream)

      assert {:ok, _filtered_candidates, _filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert consume_count(upstream) == 1
      assert FakeUpstream.requests(upstream) == requests_before_repeat
    end

    test "does not redeem saved reset for a circuit-open candidate" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "circuit-open-no-spend")
      open_circuit!(pool, api_key, filter_input.model, assignment)

      assert {:error,
              %{
                code: "no_eligible_backend",
                candidate_exclusions: [
                  %{
                    pool_upstream_assignment_id: assignment_id,
                    upstream_identity_id: identity_id,
                    reasons: [%{"code" => "routing_circuit_open"}]
                  }
                ]
              }} = RouteFiltering.filter_candidates(filter_input)

      assert assignment_id == assignment.id
      assert identity_id == identity.id
      assert [] = FakeUpstream.requests(upstream)
    end

    test "route-state filtering does not redeem saved reset for a circuit-open candidate" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_weekly_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "route-state-circuit-open")
      open_circuit!(pool, api_key, filter_input.model, assignment)
      route_state = route_state(filter_input)

      assert {:error,
              %{
                code: "no_eligible_backend",
                candidate_exclusions: [
                  %{
                    pool_upstream_assignment_id: assignment_id,
                    upstream_identity_id: identity_id,
                    reasons: [%{"code" => "routing_circuit_open"}]
                  }
                ]
              }} = RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)

      assert assignment_id == assignment.id
      assert identity_id == identity.id
      assert [] = FakeUpstream.requests(upstream)
    end

    test "does not redeem saved reset for a circuit-open threshold candidate when another candidate can route" do
      {:ok, circuit_open_upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      circuit_open =
        active_upstream_assignment_fixture(pool, %{
          metadata: saved_reset_metadata(circuit_open_upstream, 1)
        })

      routable = active_upstream_assignment_fixture(pool)

      circuit_open_identity =
        enable_saved_reset_auto_redeem!(circuit_open.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(circuit_open_identity, Decimal.new("96"))
      upsert_weekly_pressure_quota!(routable.identity, Decimal.new("97"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [
            {circuit_open.assignment, circuit_open_identity},
            {routable.assignment, routable.identity}
          ],
          "threshold-circuit-open-no-spend"
        )

      open_circuit!(pool, api_key, filter_input.model, circuit_open.assignment)

      assert {:ok, filtered_candidates, filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert candidate_ids(filtered_candidates) == [routable.assignment.id]
      assert filtered_options.routing.quota_decision["allowed"] == true
      assert filtered_options.routing.quota_decision["eligible_candidate_count"] == 1
      assert [] = FakeUpstream.requests(circuit_open_upstream)
    end

    test "route-state filtering keeps only circuit survivors before threshold saved-reset redemption" do
      {:ok, circuit_open_upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      circuit_open =
        active_upstream_assignment_fixture(pool, %{
          metadata: saved_reset_metadata(circuit_open_upstream, 1)
        })

      routable = active_upstream_assignment_fixture(pool)

      circuit_open_identity =
        enable_saved_reset_auto_redeem!(circuit_open.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(circuit_open_identity, Decimal.new("96"))
      upsert_weekly_pressure_quota!(routable.identity, Decimal.new("97"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [
            {circuit_open.assignment, circuit_open_identity},
            {routable.assignment, routable.identity}
          ],
          "route-state-threshold-circuit-open"
        )

      open_circuit!(pool, api_key, filter_input.model, circuit_open.assignment)
      route_state = route_state(filter_input)

      assert {:ok, filtered_candidates, filtered_options, filtered_route_state} =
               RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)

      assert candidate_ids(filtered_candidates) == [routable.assignment.id]
      assert candidate_ids(filtered_route_state.candidates) == [routable.assignment.id]
      assert filtered_options.routing.quota_decision["allowed"] == true
      assert filtered_options.routing.quota_decision["eligible_candidate_count"] == 1
      assert [] = FakeUpstream.requests(circuit_open_upstream)
    end

    @tag :saved_reset_expiry_ownership
    @tag :route_filtering_regression
    test "threshold redemption waits for a circuit-excluded usable sibling recovery" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      redeeming =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

      circuit_open = active_upstream_assignment_fixture(pool)
      routable = active_upstream_assignment_fixture(pool)
      _outside_model = active_upstream_assignment_fixture(pool)

      redeeming_identity =
        enable_saved_reset_auto_redeem!(redeeming.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      routable_identity =
        enable_saved_reset_auto_redeem!(routable.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(redeeming_identity, Decimal.new("96"))
      upsert_weekly_exhausted_quota!(routable_identity)
      upsert_weekly_pressure_quota!(circuit_open.identity, Decimal.new("20"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [
            {redeeming.assignment, redeeming_identity},
            {circuit_open.assignment, circuit_open.identity},
            {routable.assignment, routable_identity}
          ],
          "threshold-current-candidates"
        )

      circuit = open_circuit!(pool, api_key, filter_input.model, circuit_open.assignment)
      route_state = route_state(filter_input)
      before_metadata = Repo.reload!(redeeming_identity).metadata
      reset_probe = filter_input.request_options.routing.reset_probe

      {{:ok, filtered_candidates, filtered_options, filtered_route_state}, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)
        end)

      assert candidate_ids(filtered_candidates) == [redeeming.assignment.id]
      assert candidate_ids(filtered_route_state.candidates) == [redeeming.assignment.id]

      assert candidate_ids(filtered_route_state.saved_reset_auto_cohort) == [
               redeeming.assignment.id,
               circuit_open.assignment.id,
               routable.assignment.id
             ]

      assert FakeUpstream.requests(upstream) == []
      assert filtered_options.routing.reset_probe == reset_probe
      refute ResetProbe.bound?(filtered_options.routing.reset_probe)
      assert Repo.reload!(redeeming_identity).metadata == before_metadata
      assert log =~ "trigger_kind=gateway_auto trigger_detail=threshold"
      assert log =~ "result_code=gateway_auto_sibling_transient_exclusion applied=false"

      for raw_context_value <- [
            circuit.id,
            circuit_open.assignment.id,
            circuit_open.identity.id,
            filter_input.model.exposed_model_id,
            filter_input.route_class
          ] do
        refute log =~ raw_context_value
      end
    end

    @tag :route_filtering_regression
    test "blocked exhaustion waits when a circuit-excluded sibling has usable quota" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      redeeming =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      exhausted = active_upstream_assignment_fixture(pool)
      circuit_open = active_upstream_assignment_fixture(pool)

      redeeming_identity = enable_saved_reset_auto_redeem!(redeeming.identity)
      upsert_weekly_exhausted_quota!(redeeming_identity)
      upsert_weekly_exhausted_quota!(exhausted.identity)
      upsert_weekly_pressure_quota!(circuit_open.identity, Decimal.new("20"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [
            {redeeming.assignment, redeeming_identity},
            {exhausted.assignment, exhausted.identity},
            {circuit_open.assignment, circuit_open.identity}
          ],
          "blocked-circuit-usable-sibling"
        )

      circuit = open_circuit!(pool, api_key, filter_input.model, circuit_open.assignment)
      route_state = route_state(filter_input)
      before_metadata = Repo.reload!(redeeming_identity).metadata
      reset_probe = filter_input.request_options.routing.reset_probe

      {result, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)
        end)

      result_summary =
        case result do
          {:error, %{code: code}} -> {:error, code}
          {:ok, _candidates, _request_options, _route_state} -> {:ok, :routed}
        end

      assert result_summary == {:error, "quota_exhausted"}

      assert FakeUpstream.requests(upstream) == []
      assert filter_input.request_options.routing.reset_probe == reset_probe
      refute ResetProbe.bound?(filter_input.request_options.routing.reset_probe)
      assert Repo.reload!(redeeming_identity).metadata == before_metadata
      assert log =~ "trigger_kind=gateway_auto trigger_detail=exhausted"
      assert log =~ "result_code=gateway_auto_sibling_transient_exclusion applied=false"

      for raw_context_value <- [
            circuit.id,
            circuit_open.assignment.id,
            circuit_open.identity.id,
            filter_input.model.exposed_model_id,
            filter_input.route_class
          ] do
        refute log =~ raw_context_value
      end
    end

    test "blocked exhaustion carries circuit-excluded siblings without widening candidates" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "nothing_to_reset"}}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      redeeming =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      exhausted = active_upstream_assignment_fixture(pool)
      circuit_open = active_upstream_assignment_fixture(pool)

      redeeming_identity = enable_saved_reset_auto_redeem!(redeeming.identity)
      upsert_weekly_exhausted_quota!(redeeming_identity)
      upsert_weekly_exhausted_quota!(exhausted.identity)
      upsert_weekly_pressure_quota!(circuit_open.identity, Decimal.new("20"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [
            {redeeming.assignment, redeeming_identity},
            {exhausted.assignment, exhausted.identity},
            {circuit_open.assignment, circuit_open.identity}
          ],
          "blocked-circuit-context"
        )

      circuit = open_circuit!(pool, api_key, filter_input.model, circuit_open.assignment)
      route_state = route_state(filter_input)

      assert {:ok, circuit_survivors} =
               CandidateEligibility.filter_circuit_eligible_candidates(filter_input, route_state)

      filtered_input = FilterInput.put_candidates(filter_input, circuit_survivors)
      filtered_route_state = RouteState.put_candidates(route_state, circuit_survivors)

      context =
        SavedResetAutoRedeem.gateway_auto_context(
          %{filter_input: filtered_input, route_state: filtered_route_state},
          redeeming.assignment,
          redeeming_identity,
          :blocked_weekly_exhaustion
        )

      assert {:ok, normalized_context} = AutoEligibility.normalize_context(context)
      assert context.routable_identity_ids == context.candidate_identity_ids

      assert MapSet.new(normalized_context.routable_identity_ids) ==
               MapSet.new(normalized_context.candidate_identity_ids)

      assert [exclusion] = normalized_context.transient_circuit_exclusions
      assert exclusion.upstream_identity_id == circuit_open.identity.id
      assert exclusion.pool_upstream_assignment_id == circuit_open.assignment.id
      assert exclusion.routing_circuit_state_id == circuit.id
      assert exclusion.model_identifier == filter_input.model.exposed_model_id
      assert exclusion.route_class == filter_input.route_class
      refute exclusion.upstream_identity_id in normalized_context.candidate_identity_ids
      assert exclusion.upstream_identity_id in normalized_context.cohort_identity_ids
      assert filtered_route_state.candidates == circuit_survivors
      assert FakeUpstream.requests(upstream) == []
    end

    @tag :saved_reset_redemption_cause
    test "threshold redemption preserves routing when a sibling has usable capacity" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      first =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

      second = active_upstream_assignment_fixture(pool)

      first_identity =
        enable_saved_reset_auto_redeem!(first.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      second_identity =
        enable_saved_reset_auto_redeem!(second.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(first_identity, Decimal.new("96"))
      upsert_weekly_pressure_quota!(second_identity, Decimal.new("97"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [{first.assignment, first_identity}, {second.assignment, second_identity}],
          "threshold-enabled"
        )

      {{:ok, filtered_candidates, filtered_options}, log} =
        with_info_log(fn -> RouteFiltering.filter_candidates(filter_input) end)

      assert Enum.map(filtered_candidates, fn {assignment, _identity} -> assignment.id end) == [
               first.assignment.id,
               second.assignment.id
             ]

      assert filtered_options.routing.quota_decision["allowed"] == true
      assert filtered_options.routing.quota_decision["eligible_candidate_count"] == 2
      assert [] = FakeUpstream.requests(upstream)
      refute Map.has_key?(Repo.reload!(first_identity).metadata, "saved_reset_redemption")
      assert log =~ "trigger_kind=gateway_auto trigger_detail=threshold"
      assert log =~ "result_code=gateway_auto_sibling_usable_capacity applied=false"
    end

    @tag :saved_reset_redemption_cause
    test "threshold redemption evaluates every candidate against its own policy" do
      {:ok, upstream} = auto_redeem_fake()
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      first =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      second = active_upstream_assignment_fixture(pool)

      first_identity =
        enable_saved_reset_auto_redeem!(first.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      second_identity =
        enable_saved_reset_auto_redeem!(second.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 99
        })

      upsert_weekly_pressure_quota!(first_identity, Decimal.new("96"))
      upsert_weekly_pressure_quota!(second_identity, Decimal.new("97"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [{first.assignment, first_identity}, {second.assignment, second_identity}],
          "threshold-per-candidate-policy"
        )

      assert {:ok, filtered_candidates, filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert candidate_ids(filtered_candidates) == [first.assignment.id, second.assignment.id]
      assert filtered_options.routing.quota_decision["allowed"] == true
      assert [] = FakeUpstream.requests(upstream)
      refute Map.has_key?(Repo.reload!(first_identity).metadata, "saved_reset_redemption")
    end

    @tag :saved_reset_redemption_cause
    test "threshold sibling barrier logs a noop and preserves the chosen route" do
      scan_at = DateTime.utc_now() |> DateTime.add(1, :second) |> DateTime.truncate(:microsecond)
      {:ok, upstream} = auto_redeem_fake()
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      redeeming =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      sibling = active_upstream_assignment_fixture(pool)

      redeeming_identity =
        enable_saved_reset_auto_redeem!(redeeming.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      sibling_identity =
        put_saved_reset_redemption!(
          sibling.identity,
          resolved_redemption("confirmed_by_quota", DateTime.add(scan_at, -5, :minute), true)
        )

      upsert_weekly_pressure_quota!(redeeming_identity, Decimal.new("96"))

      filter_input =
        filter_input(
          pool,
          api_key,
          redeeming.assignment,
          redeeming_identity,
          "threshold-sibling-barrier"
        )

      route_state =
        filter_input
        |> route_state()
        |> RouteState.put_saved_reset_auto_cohort([
          {redeeming.assignment, redeeming_identity},
          {sibling.assignment, sibling_identity}
        ])

      {{:ok, [{routed_assignment, routed_identity}], filtered_options, returned_route_state}, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates_with_route_state(
            filter_input,
            route_state,
            saved_reset_scan_at: scan_at
          )
        end)

      assert routed_assignment.id == redeeming.assignment.id
      assert routed_identity.id == redeeming_identity.id
      assert filtered_options.routing.quota_decision["allowed"] == true
      assert returned_route_state.saved_reset_auto_cohort == route_state.saved_reset_auto_cohort
      assert [] = FakeUpstream.requests(upstream)
      refute Map.has_key?(Repo.reload!(redeeming_identity).metadata, "saved_reset_redemption")
      assert log =~ "trigger_kind=gateway_auto trigger_detail=threshold"
      assert log =~ "result_code=gateway_auto_sibling_consume_barrier applied=false"
      refute log =~ sibling_identity.account_label
      refute log =~ sibling_identity.chatgpt_account_id
    end

    @tag :saved_reset_redemption_cause
    test "hard exhaustion sibling barrier logs a noop and preserves the quota error" do
      scan_at = DateTime.utc_now() |> DateTime.add(1, :second) |> DateTime.truncate(:microsecond)
      {:ok, upstream} = auto_redeem_fake()
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      redeeming =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      sibling = active_upstream_assignment_fixture(pool)
      redeeming_identity = enable_saved_reset_auto_redeem!(redeeming.identity)

      sibling_identity =
        put_saved_reset_redemption!(
          sibling.identity,
          resolved_redemption("reblocked", DateTime.add(scan_at, -5, :minute), true)
        )

      upsert_weekly_exhausted_quota!(redeeming_identity)

      filter_input =
        filter_input(
          pool,
          api_key,
          redeeming.assignment,
          redeeming_identity,
          "exhausted-sibling-barrier"
        )

      route_state =
        filter_input
        |> route_state()
        |> RouteState.put_saved_reset_auto_cohort([
          {redeeming.assignment, redeeming_identity},
          {sibling.assignment, sibling_identity}
        ])

      {{:error, %{code: "quota_exhausted"} = original_error}, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates_with_route_state(
            filter_input,
            route_state,
            saved_reset_scan_at: scan_at
          )
        end)

      assert original_error.candidate_exclusions != []
      assert [] = FakeUpstream.requests(upstream)
      refute Map.has_key?(Repo.reload!(redeeming_identity).metadata, "saved_reset_redemption")
      assert log =~ "trigger_kind=gateway_auto trigger_detail=exhausted"
      assert log =~ "result_code=gateway_auto_sibling_consume_barrier applied=false"
    end

    test "resolved and definitively unspent siblings release without gateway recovery work" do
      scan_at = DateTime.utc_now() |> DateTime.add(1, :second) |> DateTime.truncate(:microsecond)

      for {phase, applied?, consumed_at} <- [
            {"confirmed_by_quota", true, DateTime.add(scan_at, -30, :minute)},
            {"consume_not_applied", false, DateTime.add(scan_at, -5, :minute)}
          ] do
        {:ok, upstream} = auto_redeem_fake()
        %{pool: pool, api_key: api_key} = active_api_key_fixture()
        jobs_before = gateway_recovery_jobs()

        redeeming =
          active_upstream_assignment_fixture(pool, %{
            metadata: saved_reset_metadata(upstream, 1)
          })

        sibling = active_upstream_assignment_fixture(pool)

        redeeming_identity =
          enable_saved_reset_auto_redeem!(redeeming.identity, %{
            saved_reset_auto_redeem_trigger_mode: "threshold",
            saved_reset_auto_redeem_quota_threshold_percent: 95
          })

        sibling_identity =
          put_saved_reset_redemption!(
            sibling.identity,
            resolved_redemption(phase, consumed_at, applied?)
          )

        upsert_weekly_pressure_quota!(redeeming_identity, Decimal.new("96"))

        filter_input =
          filter_input(
            pool,
            api_key,
            redeeming.assignment,
            redeeming_identity,
            "released-sibling-#{phase}"
          )

        route_state =
          filter_input
          |> route_state()
          |> RouteState.put_saved_reset_auto_cohort([
            {redeeming.assignment, redeeming_identity},
            {sibling.assignment, sibling_identity}
          ])

        assert {:ok, [{routed_assignment, _identity}], filtered_options, returned_route_state} =
                 RouteFiltering.filter_candidates_with_route_state(
                   filter_input,
                   route_state,
                   saved_reset_scan_at: scan_at
                 )

        assert routed_assignment.id == redeeming.assignment.id
        assert filtered_options.routing.quota_decision["allowed"] == true
        assert returned_route_state.saved_reset_auto_cohort == route_state.saved_reset_auto_cohort
        requests = FakeUpstream.requests(upstream)

        assert Enum.count(
                 requests,
                 &(&1.path == "/api/codex/rate-limit-reset-credits/consume")
               ) == 1

        assert Enum.any?(requests, &(&1.path == "/api/codex/usage"))
        assert gateway_recovery_jobs() == jobs_before
      end
    end

    test "replay and observe-only siblings stay fenced without gateway recovery work" do
      scan_at = DateTime.utc_now() |> DateTime.add(1, :second) |> DateTime.truncate(:microsecond)

      for mode <- ["replay", "observe_only"] do
        {:ok, upstream} = auto_redeem_fake()
        %{pool: pool, api_key: api_key} = active_api_key_fixture()
        jobs_before = gateway_recovery_jobs()

        redeeming =
          active_upstream_assignment_fixture(pool, %{
            metadata: saved_reset_metadata(upstream, 1)
          })

        sibling = active_upstream_assignment_fixture(pool)
        redeeming_identity = enable_saved_reset_auto_redeem!(redeeming.identity)

        sibling_redemption =
          "consuming"
          |> resolved_redemption(DateTime.add(scan_at, -40, :minute), false)
          |> Map.put("status", "redeeming")
          |> Map.put("finished_at", nil)
          |> Map.put("result", nil)
          |> Map.put("provider_replay", %{
            "version" => 1,
            "mode" => mode,
            "provider_dispatches" => 1
          })

        sibling_identity = put_saved_reset_redemption!(sibling.identity, sibling_redemption)
        upsert_weekly_exhausted_quota!(redeeming_identity)

        filter_input =
          filter_input(
            pool,
            api_key,
            redeeming.assignment,
            redeeming_identity,
            "#{mode}-sibling-barrier"
          )

        route_state =
          filter_input
          |> route_state()
          |> RouteState.put_saved_reset_auto_cohort([
            {redeeming.assignment, redeeming_identity},
            {sibling.assignment, sibling_identity}
          ])

        assert {:error, %{code: "quota_exhausted"}} =
                 RouteFiltering.filter_candidates_with_route_state(
                   filter_input,
                   route_state,
                   saved_reset_scan_at: scan_at
                 )

        assert [] = FakeUpstream.requests(upstream)

        assert Repo.reload!(sibling_identity).metadata["saved_reset_redemption"] ==
                 sibling_redemption

        assert gateway_recovery_jobs() == jobs_before
      end
    end

    @tag :saved_reset_expiry_ownership
    test "threshold auto redemption waits when natural weekly reset is inside the blocked buffer" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      upstream_assignment =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

      identity =
        enable_saved_reset_auto_redeem!(upstream_assignment.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95,
          saved_reset_auto_redeem_min_blocked_minutes: 60
        })

      reset_at =
        DateTime.utc_now() |> DateTime.add(10, :minute) |> DateTime.truncate(:microsecond)

      upsert_weekly_pressure_quota!(identity, Decimal.new("96"), reset_at: reset_at)

      filter_input =
        filter_input(pool, api_key, upstream_assignment.assignment, identity, "threshold-buffer")

      assert {:ok, filtered_candidates, filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert candidate_ids(filtered_candidates) == [upstream_assignment.assignment.id]
      assert filtered_options.routing.quota_decision["allowed"] == true
      assert filtered_options.routing.quota_decision["eligible_candidate_count"] == 1
      assert [] = FakeUpstream.requests(upstream)
    end

    @tag :saved_reset_expiry_ownership
    test "request traffic never redeems solely because a saved reset is nearing expiration" do
      scan_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      for {scenario, expires_in_seconds} <- [
            remaining_23h59: 23 * 60 * 60 + 59 * 60,
            inside_final_90_minutes: 60 * 60
          ] do
        {:ok, upstream} = auto_redeem_fake()
        %{pool: pool, api_key: api_key} = active_api_key_fixture()

        %{identity: identity, assignment: assignment} =
          active_upstream_assignment_fixture(pool, %{
            metadata:
              saved_reset_metadata(
                upstream,
                1,
                saved_reset_expiration_attrs(scan_at, expires_in_seconds)
              )
          })

        identity = enable_saved_reset_auto_redeem!(identity)
        upsert_weekly_pressure_quota!(identity, Decimal.new("25"))
        filter_input = filter_input(pool, api_key, assignment, identity, "expiry-#{scenario}")

        assert {:ok, [{%{id: assignment_id}, %{id: identity_id}}], _filtered_options} =
                 RouteFiltering.filter_candidates(filter_input, saved_reset_scan_at: scan_at)

        assert assignment_id == assignment.id
        assert identity_id == identity.id
        assert [] = FakeUpstream.requests(upstream), "scenario=#{scenario}"

        persisted = Repo.reload!(identity)

        refute Map.has_key?(persisted.metadata || %{}, "saved_reset_redemption"),
               "scenario=#{scenario}"
      end
    end

    test "route-state saved reset probe narrows an otherwise eligible sibling to the claimed lane" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      redeeming =
        active_upstream_assignment_fixture(pool, %{
          metadata: saved_reset_metadata(upstream, 1)
        })

      sibling = active_upstream_assignment_fixture(pool)

      redeeming_identity =
        enable_saved_reset_auto_redeem!(redeeming.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      sibling_identity =
        enable_saved_reset_auto_redeem!(sibling.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(redeeming_identity, Decimal.new("96"))
      upsert_weekly_exhausted_quota!(sibling_identity)

      filter_input =
        filter_input(
          pool,
          api_key,
          [
            {redeeming.assignment, redeeming_identity},
            {sibling.assignment, sibling_identity}
          ],
          "route-state-expiring-reset-singleton"
        )

      route_state = route_state(filter_input)

      assert {:ok, [claimed_candidate], decision, filtered_route_state} =
               RouteFiltering.filter_candidates_with_route_state(filter_input, route_state)

      assert {redeeming.assignment.id, redeeming_identity.id} ==
               candidate_ids_pair(claimed_candidate)

      assert filtered_route_state.candidates == [claimed_candidate]

      assert candidate_ids(filtered_route_state.saved_reset_auto_cohort) == [
               redeeming.assignment.id,
               sibling.assignment.id
             ]

      assert decision.routing.quota_decision["routing_state"] == "reset_probe"
      assert decision.routing.quota_decision["reset_probe_candidate_count"] == 1
      assert %ResetProbe{} = probe = decision.routing.reset_probe
      assert probe == filtered_route_state.reset_probe
      assert probe.pool_upstream_assignment_id == redeeming.assignment.id
      assert probe.upstream_identity_id == redeeming_identity.id
      assert probe.effective_model == filter_input.model.exposed_model_id
      assert probe.route_class == "proxy_http"
      refute sibling.assignment.id in candidate_ids(filtered_route_state.candidates)

      assert Enum.count(
               FakeUpstream.requests(upstream),
               &(&1.path == "/api/codex/rate-limit-reset-credits/consume")
             ) == 1

      assert Enum.any?(FakeUpstream.requests(upstream), &(&1.path == "/api/codex/usage"))
    end

    @tag :saved_reset_expiry_ownership
    test "early auto redemption waits when another candidate is not near the weekly limit" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      first =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

      second = active_upstream_assignment_fixture(pool)

      first_identity =
        enable_saved_reset_auto_redeem!(first.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(first_identity, Decimal.new("96"))
      upsert_weekly_pressure_quota!(second.identity, Decimal.new("80"))

      filter_input =
        filter_input(
          pool,
          api_key,
          [{first.assignment, first_identity}, {second.assignment, second.identity}],
          "threshold-waits-for-pool"
        )

      assert {:ok, _filtered_candidates, _filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert [] = FakeUpstream.requests(upstream)
    end

    test "early auto redemption ignores stale weekly quota pressure" do
      {:ok, upstream} =
        FakeUpstream.start_link({:path_json, %{"/api/codex/usage" => {200, usage_payload(2)}}})

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      upstream_assignment =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

      identity =
        enable_saved_reset_auto_redeem!(upstream_assignment.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(identity, Decimal.new("96"), freshness_state: "stale")

      filter_input =
        filter_input(pool, api_key, upstream_assignment.assignment, identity, "threshold-stale")

      assert {:ok, _filtered_candidates, _filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert [] = FakeUpstream.requests(upstream)
    end

    test "early auto redemption ignores inferred weekly quota pressure" do
      {:ok, upstream} =
        FakeUpstream.start_link({:path_json, %{"/api/codex/usage" => {200, usage_payload(2)}}})

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      upstream_assignment =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

      identity =
        enable_saved_reset_auto_redeem!(upstream_assignment.identity, %{
          saved_reset_auto_redeem_trigger_mode: "threshold",
          saved_reset_auto_redeem_quota_threshold_percent: 95
        })

      upsert_weekly_pressure_quota!(identity, Decimal.new("96"), source_precision: "inferred")

      filter_input =
        filter_input(
          pool,
          api_key,
          upstream_assignment.assignment,
          identity,
          "threshold-inferred"
        )

      assert {:ok, _filtered_candidates, _filtered_options} =
               RouteFiltering.filter_candidates(filter_input)

      assert [] = FakeUpstream.requests(upstream)
    end

    @tag :saved_reset_expiry_ownership
    test "auto redemption requires weekly-account-only quota exhaustion" do
      {:ok, upstream} =
        FakeUpstream.start_link(
          {:path_json,
           %{
             "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
             "/api/codex/usage" => {200, usage_payload(0)}
           }}
        )

      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_primary_exhausted_quota!(identity)
      filter_input = filter_input(pool, api_key, assignment, identity, "primary-exhausted")

      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(filter_input)
      assert [] = FakeUpstream.requests(upstream)
    end
  end

  describe "automatic saved-reset corroboration" do
    @tag :saved_reset_redemption_cause
    test "one uncorroborated exhausted observation cannot reach the consume seam" do
      {:ok, upstream} = auto_redeem_fake()
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)
      upsert_uncorroborated_weekly_exhausted_quota!(identity)
      [window] = SavedResetConfirmationFixtures.weekly_provider_windows(identity.id)
      assert SavedResetConfirmationFixtures.marker_state(window) == "candidate"

      filter_input = filter_input(pool, api_key, assignment, identity, "uncorroborated")

      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(filter_input)
      assert [] = FakeUpstream.requests(upstream)
      refute Repo.reload!(identity).metadata["saved_reset_redemption"]
      assert Repo.reload!(identity).metadata["saved_resets"]["available_count"] == 1
    end

    @tag :saved_reset_redemption_cause
    test "an unexplained jump to blocked spends nothing until the policy blocked span elapses" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      reset_at = DateTime.add(now, 3, :hour)
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      # Provider receipts, each from its own fake upstream so the wire payload
      # and the consume endpoint are attributable per observation. The account
      # was last seen allowed at 32%, so every later blocked receipt is an
      # unexplained jump that must persist for min_blocked_minutes (60).
      receipts =
        for {kind, offset} <- [blocked: -180, allowed: -120, blocked: -60, blocked: 0] do
          observed_at = DateTime.add(now, offset, :second)
          {:ok, fake} = corroboration_fake(kind, reset_at, observed_at, 1)
          on_exit(fn -> FakeUpstream.stop(fake) end)
          {observed_at, fake}
        end

      [{at1, fake1}, {at2, fake2}, {at3, fake3}, {at4, fake4}] = receipts
      fakes = Enum.map(receipts, &elem(&1, 1))

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake1, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)

      window_state = fn ->
        identity.id
        |> SavedResetConfirmationFixtures.weekly_provider_windows()
        |> Enum.map(&SavedResetConfirmationFixtures.marker_state/1)
      end

      total_consumes = fn -> Enum.sum(Enum.map(fakes, &consume_count/1)) end

      # 1. blocked 100%: one receipt is only a candidate; routing spends nothing
      observe_provider!(identity, assignment, fake1, at1)
      assert window_state.() == ["candidate"]
      route_input = filter_input(pool, api_key, assignment, identity, "flap-1")
      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(route_input)
      assert total_consumes.() == 0

      # 2. allowed 32%: the provider flap clears the candidate and records the
      # approach witness. Whether the evidence store already trusts the lower
      # same-cycle percent for routing is a separate quota decision; the
      # automatic seam spends nothing either way.
      observe_provider!(identity, assignment, fake2, at2)
      assert window_state.() == ["approach"]
      route_input = filter_input(pool, api_key, assignment, identity, "flap-2")
      _ = RouteFiltering.filter_candidates(route_input)
      assert total_consumes.() == 0
      refute Repo.reload!(identity).metadata["saved_reset_redemption"]

      # 3. blocked 100% again: a fresh candidate after the flap, still no spend
      observe_provider!(identity, assignment, fake3, at3)
      assert window_state.() == ["candidate"]
      route_input = filter_input(pool, api_key, assignment, identity, "flap-3")
      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(route_input)
      assert total_consumes.() == 0

      # 4. blocked 100% from a strictly newer receipt corroborates the blocked
      # state, but the jump from 32% is unexplained and one minute of blocked
      # evidence is far below the 60-minute policy span: still no spend.
      observe_provider!(identity, assignment, fake4, at4)
      assert window_state.() == ["confirmed"]
      route_input = filter_input(pool, api_key, assignment, identity, "flap-4")

      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(route_input) end)
      assert {:error, %{code: "quota_exhausted"}} = result
      refute log =~ "trigger_kind=gateway_auto"
      assert total_consumes.() == 0
      refute Repo.reload!(identity).metadata["saved_reset_redemption"]

      [window] = SavedResetConfirmationFixtures.weekly_provider_windows(identity.id)

      assert {:span_pending, _remaining} =
               AutomaticConfirmation.blocked_readiness(window.metadata,
                 explained_percent: 95,
                 min_blocked_seconds: 3600
               )
    end

    @tag :saved_reset_redemption_cause
    test "the September 9 incident shape corroborated twice within minutes spends nothing" do
      # Sanitized shape of the provider receipts observed on 2026-09-09 during
      # the OpenAI usage-limit incident: allowed=false, limit_reached=true, the
      # weekly window served in the primary slot with a null secondary window,
      # rate_limit_reached_type default, one banked reset available.
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      reset_at = DateTime.add(now, 77, :hour)
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      receipts =
        for offset <- [-120, -60, 0] do
          observed_at = DateTime.add(now, offset, :second)
          {:ok, fake} = incident_fake(reset_at, observed_at)
          on_exit(fn -> FakeUpstream.stop(fake) end)
          {observed_at, fake}
        end

      [{_at1, fake1} | _] = receipts

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake1, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)

      for {at, fake} <- receipts do
        observe_provider!(identity, assignment, fake, at)
        route_input = filter_input(pool, api_key, assignment, identity, "incident-#{at}")

        assert {:error, %{code: "quota_exhausted"}} =
                 RouteFiltering.filter_candidates(route_input)
      end

      assert Enum.all?(receipts, fn {_at, fake} -> consume_count(fake) == 0 end)
      refute Repo.reload!(identity).metadata["saved_reset_redemption"]
      assert Repo.reload!(identity).metadata["saved_resets"]["available_count"] == 1

      [window] = SavedResetConfirmationFixtures.weekly_provider_windows(identity.id)
      assert SavedResetConfirmationFixtures.marker_state(window) == "confirmed"

      assert {:span_pending, _remaining} =
               AutomaticConfirmation.blocked_readiness(window.metadata,
                 explained_percent: 95,
                 min_blocked_seconds: 3600
               )
    end

    @tag :saved_reset_redemption_cause
    test "an explained exhaustion consumes exactly once on the second blocked receipt" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      reset_at = DateTime.add(now, 3, :hour)
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      receipts =
        for {kind, offset} <- [approaching: -120, blocked: -60, blocked: 0] do
          observed_at = DateTime.add(now, offset, :second)
          {:ok, fake} = corroboration_fake(kind, reset_at, observed_at, 1)
          on_exit(fn -> FakeUpstream.stop(fake) end)
          {observed_at, fake}
        end

      [{at1, fake1}, {at2, fake2}, {at3, fake3}] = receipts

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake1, 1)})

      identity = enable_saved_reset_auto_redeem!(identity)

      # allowed at 96%: the account is seen approaching the limit
      observe_provider!(identity, assignment, fake1, at1)
      route_input = filter_input(pool, api_key, assignment, identity, "explained-1")
      _ = RouteFiltering.filter_candidates(route_input)
      assert consume_count(fake1) == 0

      # first blocked receipt: candidate only
      observe_provider!(identity, assignment, fake2, at2)
      route_input = filter_input(pool, api_key, assignment, identity, "explained-2")
      assert {:error, %{code: "quota_exhausted"}} = RouteFiltering.filter_candidates(route_input)
      assert consume_count(fake2) == 0

      # second blocked receipt: explained exhaustion, spend exactly once
      observe_provider!(identity, assignment, fake3, at3)
      route_input = filter_input(pool, api_key, assignment, identity, "explained-3")
      {result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(route_input) end)
      assert log =~ "trigger_kind=gateway_auto trigger_detail=exhausted"
      assert log =~ "result_code=reset applied=true"

      assert {:ok, [{routed_assignment, routed_identity}], options} = result
      assert routed_assignment.id == assignment.id
      assert routed_identity.id == identity.id
      assert ResetProbe.bound?(options.routing.reset_probe)

      assert consume_count(fake3) == 1
      redemption = Repo.reload!(identity).metadata["saved_reset_redemption"]
      assert redemption["trigger_kind"] == "gateway_auto"
      assert redemption["result"]["applied"] == true

      # the post-consume latch and the spent bank keep a second request from consuming again
      route_input = filter_input(pool, api_key, assignment, identity, "explained-4")
      _ = RouteFiltering.filter_candidates(route_input)
      assert consume_count(fake1) + consume_count(fake2) + consume_count(fake3) == 1
    end

    @tag :saved_reset_redemption_cause
    test "an unexplained exhaustion consumes once after the policy blocked span" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      reset_at = DateTime.add(now, 3, :hour)
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      # Both receipts must stay inside the evidence freshness window, so the
      # identity policy lowers the minimum blocked span to five minutes and the
      # receipts are six minutes apart.
      receipts =
        for offset <- [-6 * 60, 0] do
          observed_at = DateTime.add(now, offset, :second)
          {:ok, fake} = corroboration_fake(:blocked, reset_at, observed_at, 1)
          on_exit(fn -> FakeUpstream.stop(fake) end)
          {observed_at, fake}
        end

      [{at1, fake1}, {at2, fake2}] = receipts

      %{identity: identity, assignment: assignment} =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake1, 1)})

      identity =
        enable_saved_reset_auto_redeem!(identity, %{
          saved_reset_auto_redeem_min_blocked_minutes: 5
        })

      observe_provider!(identity, assignment, fake1, at1)
      observe_provider!(identity, assignment, fake2, at2)
      [window] = SavedResetConfirmationFixtures.weekly_provider_windows(identity.id)

      assert AutomaticConfirmation.blocked_readiness(window.metadata,
               explained_percent: 95,
               min_blocked_seconds: 300
             ) == {:confirmed, :span}

      route_input = filter_input(pool, api_key, assignment, identity, "span")
      {_result, log} = with_info_log(fn -> RouteFiltering.filter_candidates(route_input) end)
      assert log =~ "result_code=reset applied=true"
      assert consume_count(fake2) == 1
    end

    @tag :saved_reset_redemption_cause
    test "threshold pressure needs every participant window corroborated before spending" do
      {:ok, upstream} = auto_redeem_fake()
      %{pool: pool, api_key: api_key} = active_api_key_fixture()

      target =
        active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

      sibling = active_upstream_assignment_fixture(pool)

      threshold = %{
        saved_reset_auto_redeem_trigger_mode: "threshold",
        saved_reset_auto_redeem_quota_threshold_percent: 95
      }

      target_identity = enable_saved_reset_auto_redeem!(target.identity, threshold)
      sibling_identity = enable_saved_reset_auto_redeem!(sibling.identity, threshold)

      assert {:ok, [_]} =
               QuotaWindows.upsert_quota_windows(target_identity, [
                 weekly_pressure_quota_attrs(Decimal.new("96"), [])
               ])

      assert {:ok, [_]} =
               QuotaWindows.upsert_quota_windows(sibling_identity, [
                 weekly_exhausted_quota_attrs()
               ])

      candidates = [{target.assignment, target_identity}, {sibling.assignment, sibling_identity}]

      # target corroborated, sibling single-receipt: closed set not ready
      earlier =
        DateTime.utc_now() |> DateTime.add(-120, :second) |> DateTime.truncate(:microsecond)

      SavedResetConfirmationFixtures.confirm_automatic_pressure!(target_identity)

      SavedResetConfirmationFixtures.confirm_automatic_pressure!(sibling_identity,
        observations: 1,
        observed_at: earlier
      )

      {_result, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates(filter_input(pool, api_key, candidates, "threshold-1"))
        end)

      refute log =~ "trigger_kind=gateway_auto"
      assert [] = FakeUpstream.requests(upstream)

      # sibling receives a strictly newer blocked receipt (no new allowed receipt,
      # which would clear its proof): every current pressure window is corroborated
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(sibling_identity,
        observations: 1,
        approach: false
      )

      {_result, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates(filter_input(pool, api_key, candidates, "threshold-2"))
        end)

      assert log =~ "trigger_kind=gateway_auto trigger_detail=threshold"
      assert log =~ "result_code=reset applied=true"
      assert consume_count(upstream) == 1
    end

    @tag :saved_reset_redemption_cause
    test "two installations sharing one account spend at most once through the identity policy" do
      # Metadata-only two-installation fixture: two identities for the same
      # provider account, one with the existing automatic policy enabled and one
      # disabled. Only the enabled side may reach the automatic seam.
      # One shared fake provider bank stands in for the account; each side is a
      # separate identity row because separate installations never share rows.
      {:ok, upstream} = auto_redeem_fake()
      %{pool: enabled_pool, api_key: enabled_key} = active_api_key_fixture()
      %{pool: disabled_pool, api_key: disabled_key} = active_api_key_fixture()

      enabled =
        active_upstream_assignment_fixture(enabled_pool, %{
          metadata: saved_reset_metadata(upstream, 1)
        })

      disabled =
        active_upstream_assignment_fixture(disabled_pool, %{
          metadata: saved_reset_metadata(upstream, 1)
        })

      enabled_identity = enable_saved_reset_auto_redeem!(enabled.identity)
      disabled_identity = disabled.identity
      refute disabled_identity.saved_reset_auto_redeem_enabled

      upsert_weekly_exhausted_quota!(enabled_identity)
      upsert_weekly_exhausted_quota!(disabled_identity)

      assert Enum.map(
               SavedResetConfirmationFixtures.weekly_provider_windows(enabled_identity.id),
               &SavedResetConfirmationFixtures.marker_state/1
             ) == ["confirmed"]

      assert Enum.map(
               SavedResetConfirmationFixtures.weekly_provider_windows(disabled_identity.id),
               &SavedResetConfirmationFixtures.marker_state/1
             ) == [nil]

      assert {:error, %{code: "quota_exhausted"}} =
               RouteFiltering.filter_candidates(
                 filter_input(
                   disabled_pool,
                   disabled_key,
                   disabled.assignment,
                   disabled_identity,
                   "disabled"
                 )
               )

      assert consume_count(upstream) == 0
      refute Repo.reload!(disabled_identity).metadata["saved_reset_redemption"]

      {_result, log} =
        with_info_log(fn ->
          RouteFiltering.filter_candidates(
            filter_input(
              enabled_pool,
              enabled_key,
              enabled.assignment,
              enabled_identity,
              "enabled"
            )
          )
        end)

      assert log =~ "trigger_kind=gateway_auto trigger_detail=exhausted"
      assert consume_count(upstream) == 1

      _ =
        RouteFiltering.filter_candidates(
          filter_input(
            disabled_pool,
            disabled_key,
            disabled.assignment,
            disabled_identity,
            "disabled-2"
          )
        )

      assert consume_count(upstream) == 1
      refute Repo.reload!(disabled_identity).metadata["saved_reset_redemption"]
    end
  end

  # One fake upstream per provider receipt: the usage payload carries the
  # coherent permission tuple, the exact weekly window with its countdown, and
  # the saved-reset bank the corroboration binding must witness together.
  defp corroboration_fake(kind, reset_at, observed_at, available_count) do
    {allowed, reached, used_percent} =
      case kind do
        :blocked -> {false, true, 100}
        :allowed -> {true, false, 32}
        :approaching -> {true, false, 96}
      end

    # Weekly-only shape: the exhausted automatic trigger engages on the
    # weekly-account-only exclusion, the same routing precondition the
    # existing single-window fixtures rely on.
    payload = %{
      "plan_type" => "pro",
      "rate_limit_reset_credits" => %{"available_count" => available_count},
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
      "spend_control" => %{"reached" => false},
      "rate_limit" => %{
        "allowed" => allowed,
        "limit_reached" => reached,
        "secondary_window" => %{
          "used_percent" => used_percent,
          "limit_window_seconds" => 604_800,
          "reset_after_seconds" => DateTime.diff(reset_at, observed_at, :second),
          "reset_at" => DateTime.to_unix(reset_at)
        }
      }
    }

    FakeUpstream.start_link(
      {:path_json,
       %{
         "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
         "/api/codex/usage" => {200, payload}
       }}
    )
  end

  # The exact sanitized wire shape captured during the provider's usage-limit
  # incident from an affected account: weekly window in the primary slot, null
  # secondary, blocked permission, one available banked reset, an unrelated
  # Spark meter at 0%.
  defp incident_fake(reset_at, observed_at) do
    payload = %{
      "plan_type" => "pro",
      "rate_limit" => %{
        "allowed" => false,
        "limit_reached" => true,
        "primary_window" => %{
          "used_percent" => 100,
          "limit_window_seconds" => 604_800,
          "reset_after_seconds" => DateTime.diff(reset_at, observed_at, :second),
          "reset_at" => DateTime.to_unix(reset_at)
        },
        "secondary_window" => nil
      },
      "rate_limit_reset_credits" => %{"available_count" => 1, "applicable_available_count" => 1},
      "credits" => %{
        "has_credits" => false,
        "unlimited" => false,
        "overage_limit_reached" => false,
        "balance" => "0"
      },
      "spend_control" => %{"reached" => false, "individual_limit" => nil},
      "rate_limit_reached_type" => %{"type" => "rate_limit_reached", "details" => "default"},
      "additional_rate_limits" => [
        %{
          "limit_name" => "GPT-5.3-Codex-Spark",
          "metered_feature" => "codex_bengalfox",
          "rate_limit" => %{
            "allowed" => true,
            "limit_reached" => false,
            "primary_window" => %{
              "used_percent" => 0,
              "limit_window_seconds" => 18_000,
              "reset_after_seconds" => 18_000,
              "reset_at" => DateTime.to_unix(DateTime.add(observed_at, 18_000, :second))
            },
            "secondary_window" => %{
              "used_percent" => 0,
              "limit_window_seconds" => 604_800,
              "reset_after_seconds" => 401_764,
              "reset_at" => DateTime.to_unix(DateTime.add(observed_at, 401_764, :second))
            }
          },
          "normal_model_slug" => nil
        }
      ]
    }

    FakeUpstream.start_link(
      {:path_json,
       %{
         "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
         "/api/codex/usage" => {200, payload}
       }}
    )
  end

  defp observe_provider!(identity, assignment, fake, observed_at) do
    point_upstream!(identity, fake)
    point_upstream!(assignment, fake)

    assert {:ok, %UpstreamIdentity{}} =
             PoolReconciliation.refresh_quota_from_usage(
               Repo.reload!(identity),
               Repo.reload!(assignment),
               observed_at: observed_at
             )
  end

  defp point_upstream!(%UpstreamIdentity{} = identity, fake) do
    identity = Repo.reload!(identity)

    identity
    |> Ecto.Changeset.change(metadata: Map.put(identity.metadata || %{}, "usage_base_url", FakeUpstream.url(fake)))
    |> Repo.update!()
  end

  defp point_upstream!(%PoolUpstreamAssignment{} = assignment, fake) do
    assignment = Repo.reload!(assignment)

    assignment
    |> Ecto.Changeset.change(metadata: Map.put(assignment.metadata || %{}, "usage_base_url", FakeUpstream.url(fake)))
    |> Repo.update!()
  end

  defp consume_count(fake) do
    Enum.count(
      FakeUpstream.requests(fake),
      &(&1.path == "/api/codex/rate-limit-reset-credits/consume")
    )
  end

  defp mixed_exclusion_arrangement(sibling_block, mode, target_block \\ nil) do
    {:ok, upstream} =
      FakeUpstream.start_link(
        {:path_json,
         %{
           "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
           "/api/codex/usage" => {200, usage_payload(1)}
         }}
      )

    on_exit(fn -> FakeUpstream.stop(upstream) end)
    %{pool: pool, api_key: api_key} = active_api_key_fixture()

    target =
      active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 2)})

    sibling =
      active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

    target = %{
      target
      | identity:
          enable_saved_reset_auto_redeem!(target.identity, %{
            saved_reset_auto_redeem_trigger_mode: mode
          })
    }

    sibling = %{
      sibling
      | identity:
          enable_saved_reset_auto_redeem!(sibling.identity, %{
            saved_reset_auto_redeem_trigger_mode: mode
          })
    }

    input =
      filter_input(
        pool,
        api_key,
        [{sibling.assignment, sibling.identity}, {target.assignment, target.identity}],
        "mixed-exclusions"
      )

    case target_block do
      :missing -> :ok
      :uncorroborated -> upsert_uncorroborated_weekly_exhausted_quota!(target.identity)
      _other -> upsert_weekly_exhausted_quota!(target.identity)
    end

    if target_block == :disabled do
      target.identity
      |> Ecto.Changeset.change(saved_reset_auto_redeem_enabled: false)
      |> Repo.update!()
    else
      put_mixed_quota_block!(target.identity, target_block, input.model)
    end

    put_mixed_quota_block!(sibling.identity, sibling_block, input.model)
    %{upstream: upstream, target: target, sibling: sibling, input: input}
  end

  defp put_mixed_quota_block!(identity, :primary, _model),
    do: upsert_primary_exhausted_quota!(identity)

  defp put_mixed_quota_block!(identity, :monthly, _model) do
    attrs = Map.put(primary_quota_attrs(Decimal.new("100")), :window_minutes, 43_200)
    assert {:ok, [_window]} = QuotaWindows.upsert_quota_windows(identity, [attrs])
  end

  defp put_mixed_quota_block!(identity, :primary_and_weekly, _model) do
    upsert_weekly_exhausted_quota!(identity)
    upsert_primary_exhausted_quota!(identity)
  end

  defp put_mixed_quota_block!(identity, block, model)
       when block in [:model_and_weekly, :additional_and_weekly] do
    upsert_weekly_exhausted_quota!(identity)
    scope = if block == :model_and_weekly, do: :model, else: :additional
    put_mixed_quota_block!(identity, scope, model)
  end

  defp put_mixed_quota_block!(identity, :usable, _model),
    do: upsert_primary_quota!(identity, Decimal.new("10"))

  defp put_mixed_quota_block!(identity, scope, model) when scope in [:model, :additional] do
    upsert_primary_quota!(identity, Decimal.new("10"))

    attrs =
      weekly_exhausted_quota_attrs()
      |> Map.merge(%{
        quota_key: "sample_#{scope}",
        quota_scope: "model",
        quota_family: if(scope == :model, do: "codex_model", else: "additional"),
        model: model.exposed_model_id
      })

    assert {:ok, [_window]} = QuotaWindows.upsert_quota_windows(identity, [attrs])
  end

  defp put_mixed_quota_block!(_identity, _block, _model), do: :ok

  defp filter_input(pool, api_key, assignment, identity, suffix) do
    filter_input(pool, api_key, [{assignment, identity}], suffix)
  end

  # The production first-turn shape: a threshold-pressured target, a routable
  # sibling whose applied consume is still converging (`reblocked`, so it is
  # excluded from the threshold scan but has current usable quota), and a
  # Codex session created moments ago with no hard continuity anchor.
  defp first_turn_capacity_arrangement(suffix) do
    {:ok, upstream} =
      FakeUpstream.start_link(
        {:path_json,
         %{
           "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
           "/api/codex/usage" => {200, usage_payload(0)}
         }}
      )

    %{pool: pool, api_key: api_key} = active_api_key_fixture()

    %{identity: sibling_identity, assignment: sibling_assignment} =
      active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

    %{identity: target_identity, assignment: target_assignment} =
      active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(upstream, 1)})

    sibling_identity =
      sibling_identity
      |> enable_saved_reset_auto_redeem!()
      |> put_applied_reblocked_redemption!(40)

    upsert_weekly_pressure_quota!(sibling_identity, Decimal.new("26"))

    target_identity =
      enable_saved_reset_auto_redeem!(target_identity, %{
        saved_reset_auto_redeem_trigger_mode: "threshold",
        saved_reset_auto_redeem_quota_threshold_percent: 95
      })

    upsert_weekly_pressure_quota!(target_identity, Decimal.new("96"))

    filter_input =
      filter_input(
        pool,
        api_key,
        [{sibling_assignment, sibling_identity}, {target_assignment, target_identity}],
        suffix
      )

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    session =
      Repo.insert!(%CodexSession{
        pool_id: pool.id,
        api_key_id: api_key.id,
        session_key: "sess-#{suffix}-#{System.unique_integer([:positive])}",
        pool_upstream_assignment_id: target_assignment.id,
        status: "active",
        owner_instance_id: "route-filtering-test",
        owner_lease_token: Ecto.UUID.generate(),
        owner_lease_expires_at: DateTime.add(now, 1, :hour),
        last_heartbeat_at: now,
        created_at: DateTime.add(now, -4, :second),
        updated_at: DateTime.add(now, -4, :second)
      })

    request_options =
      RequestOptions.put_continuity(filter_input.request_options, codex_session: session)

    %{
      upstream: upstream,
      filter_input: %{filter_input | request_options: request_options},
      unattached_filter_input: filter_input,
      pool: pool,
      api_key: api_key,
      session: session,
      sibling_identity: sibling_identity,
      sibling_assignment: sibling_assignment,
      target_identity: target_identity,
      target_assignment: target_assignment
    }
  end

  defp register_session_alias!(pool, api_key, session, alias_kind, alias_value) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.insert!(%BridgeSessionAlias{
      codex_session_id: session.id,
      pool_id: pool.id,
      api_key_id: api_key.id,
      alias_kind: alias_kind,
      alias_hash: :crypto.hash(:sha256, alias_value),
      status: "active",
      expires_at: DateTime.add(now, 1, :hour),
      last_seen_at: now,
      created_at: now,
      updated_at: now
    })
  end

  defp put_applied_reblocked_redemption!(%UpstreamIdentity{} = identity, consumed_minutes_ago) do
    consumed_at =
      DateTime.utc_now()
      |> DateTime.add(-consumed_minutes_ago, :minute)
      |> DateTime.truncate(:microsecond)

    redemption = %{
      "status" => "failed",
      "phase" => "reblocked",
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 1,
      "trigger_kind" => "gateway_auto",
      "trigger_detail" => "exhausted",
      "started_at" => DateTime.to_iso8601(DateTime.add(consumed_at, -1, :minute)),
      "consumed_at" => DateTime.to_iso8601(consumed_at),
      "deadline_at" => consumed_at |> DateTime.add(15, :minute) |> DateTime.to_iso8601(),
      "finished_at" => DateTime.to_iso8601(consumed_at),
      "result" => %{"code" => "reset", "applied" => true}
    }

    persisted = Repo.reload!(identity)

    persisted
    |> UpstreamIdentity.changeset(%{
      metadata: Map.put(persisted.metadata || %{}, "saved_reset_redemption", redemption)
    })
    |> Repo.update!()
  end

  defp route_state(%FilterInput{} = filter_input) do
    %{auth: auth, model: model, request_options: request_options, candidates: candidates} =
      filter_input

    %{visible_model: model, candidates: candidates}
    |> RouteState.new()
    |> RouteState.preload_routing_snapshots(auth, model, request_options)
  end

  defp account_window_at(used_percent, observed_at) do
    %AccountQuotaWindow{
      quota_key: "account",
      window_kind: "primary",
      window_minutes: 300,
      used_percent: used_percent,
      reset_at: DateTime.add(observed_at, 300, :second),
      source: "codex_usage_api",
      source_precision: "observed",
      quota_scope: "account",
      quota_family: "account",
      freshness_state: "fresh",
      observed_at: observed_at
    }
  end

  defp candidate_ids(candidates),
    do: Enum.map(candidates, fn {assignment, _identity} -> assignment.id end)

  defp candidate_ids_pair({assignment, identity}), do: {assignment.id, identity.id}

  defp filter_input(pool, api_key, model, candidates) when is_list(candidates) do
    payload = %{"model" => model.exposed_model_id, "input" => "route filtering"}
    request_options = request_options(payload)

    FilterInput.new(%{
      auth: %{pool: pool, api_key: api_key},
      model: model,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      request_options: request_options,
      candidates: candidates
    })
  end

  defp filter_input(pool, api_key, candidates, suffix) when is_list(candidates) do
    model =
      model_fixture(pool, %{
        exposed_model_id: "gpt-route-filtering-#{suffix}-#{System.unique_integer([:positive])}",
        metadata: %{
          "source_assignment_ids" => Enum.map(candidates, fn {assignment, _identity} -> assignment.id end)
        }
      })

    payload = %{"model" => model.exposed_model_id, "input" => "route filtering"}
    request_options = request_options(payload)

    FilterInput.new(%{
      auth: %{pool: pool, api_key: api_key},
      model: model,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      request_options: request_options,
      candidates: candidates
    })
  end

  defp request_options(payload) do
    %{}
    |> RequestOptions.build("/backend-api/codex/responses", payload)
    |> RequestOptions.put_routing(reset_probe: ResetProbe.new())
  end

  defp sync_catalog_step(pool, assignment_models) when is_map(assignment_models) do
    Catalog.sync_pool_catalog(pool,
      fetcher: fn %{assignment: assignment} ->
        {:ok, Map.fetch!(assignment_models, assignment.id)}
      end
    )
  end

  defp runtime_sync_model(model_id, attrs) when is_binary(model_id) and is_map(attrs) do
    Map.merge(
      %{
        "id" => model_id,
        "display_name" => "Synthetic Preserved Runtime",
        "owned_by" => "synthetic",
        "capabilities" => %{"responses" => true, "streaming" => true}
      },
      attrs
    )
  end

  defp priority_generation_count(fake) do
    Enum.count(FakeUpstream.requests(fake), fn request ->
      not String.ends_with?(request.path, "/usage") and not String.contains?(request.path, "/rate-limit-reset-credits")
    end)
  end

  defp auto_redeem_fake do
    FakeUpstream.start_link(
      {:path_json,
       %{
         "/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}},
         "/api/codex/usage" => {200, usage_payload(0)}
       }}
    )
  end

  defp applied_auto_redemption(phase, consumed_minutes_ago) do
    consumed_at =
      DateTime.utc_now()
      |> DateTime.add(-consumed_minutes_ago, :minute)
      |> DateTime.truncate(:microsecond)

    %{
      "status" => if(phase == "reblocked", do: "failed", else: "succeeded"),
      "phase" => phase,
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 3,
      "trigger_kind" => "gateway_auto",
      "started_at" => DateTime.to_iso8601(consumed_at),
      "consumed_at" => DateTime.to_iso8601(consumed_at),
      "deadline_at" => consumed_at |> DateTime.add(15, :minute) |> DateTime.to_iso8601(),
      "finished_at" => DateTime.to_iso8601(consumed_at),
      "result" => %{"code" => "reset", "applied" => true}
    }
  end

  defp resolved_redemption(phase, %DateTime{} = consumed_at, applied?) do
    %{
      "status" => if(phase == "reblocked", do: "failed", else: "succeeded"),
      "phase" => phase,
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 4,
      "trigger_kind" => "gateway_auto",
      "started_at" => DateTime.to_iso8601(consumed_at),
      "consumed_at" => DateTime.to_iso8601(consumed_at),
      "finished_at" => DateTime.to_iso8601(consumed_at),
      "result" => %{"code" => phase, "applied" => applied?}
    }
  end

  defp put_saved_reset_redemption!(%UpstreamIdentity{} = identity, redemption) do
    identity
    |> UpstreamIdentity.changeset(%{
      metadata: Map.put(identity.metadata || %{}, "saved_reset_redemption", redemption),
      updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
    })
    |> Repo.update!()
  end

  defp saved_reset_metadata(upstream, available_count, saved_reset_attrs \\ %{}) do
    observed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

    saved_resets =
      Map.merge(
        %{
          "status" => "reported",
          "available_count" => available_count,
          "source" => "codex_usage_api",
          "path_style" => "codex_api",
          "observed_at" => observed_at,
          "usage_path" => "/api/codex/usage",
          "reason" => nil
        },
        saved_reset_attrs
      )

    %{
      "usage_base_url" => FakeUpstream.url(upstream),
      "saved_resets" => saved_resets
    }
  end

  defp reset_probe_redemption(phase, %DateTime{} = consumed_at) do
    %{
      "saved_reset_redemption" => %{
        "status" => "succeeded",
        "phase" => phase,
        "attempt_id" => Ecto.UUID.generate(),
        "generation" => 2,
        "trigger_kind" => "gateway_auto",
        "consumed_at" => DateTime.to_iso8601(consumed_at),
        "deadline_at" => consumed_at |> DateTime.add(15, :minute) |> DateTime.to_iso8601(),
        "result" => %{"code" => "reset", "applied" => true}
      }
    }
  end

  defp saved_reset_expiration_attrs(timestamp, expires_in_seconds) do
    expires_at = timestamp |> DateTime.add(expires_in_seconds, :second) |> DateTime.to_iso8601()
    observed_at = DateTime.to_iso8601(timestamp)

    %{
      "available_expires_at" => [expires_at],
      "next_expires_at" => expires_at,
      "expires_observed_at" => observed_at,
      "expires_refresh_attempted_at" => observed_at
    }
  end

  defp enable_saved_reset_auto_redeem!(%UpstreamIdentity{} = identity, attrs \\ %{}) do
    identity
    |> UpstreamIdentity.changeset(
      Map.merge(
        %{
          saved_reset_auto_redeem_enabled: true,
          saved_reset_auto_redeem_min_blocked_minutes: 60,
          saved_reset_auto_redeem_keep_credits: 0,
          updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
        },
        attrs
      )
    )
    |> Repo.update!()
  end

  defp upsert_weekly_exhausted_quota!(identity) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(identity, [weekly_exhausted_quota_attrs()])

    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity)
  end

  defp upsert_uncorroborated_weekly_exhausted_quota!(identity) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(identity, [weekly_exhausted_quota_attrs()])

    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, observations: 1)
  end

  defp upsert_primary_exhausted_quota!(identity) do
    upsert_primary_quota!(identity, Decimal.new("100"))
  end

  defp upsert_primary_quota!(identity, used_percent) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(identity, [primary_quota_attrs(used_percent)])
  end

  defp upsert_weekly_pressure_quota!(identity, used_percent, attrs \\ []) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(identity, [
               weekly_pressure_quota_attrs(used_percent, attrs)
             ])

    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity)
  end

  defp weekly_exhausted_quota_attrs do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %{
      quota_key: "account",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new("100"),
      reset_at: DateTime.add(now, 2, :hour),
      observed_at: now,
      last_sync_at: now,
      source: "codex_usage_api",
      source_precision: "observed",
      quota_scope: "account",
      quota_family: "account",
      freshness_state: "fresh"
    }
  end

  defp primary_quota_attrs(used_percent) do
    weekly_exhausted_quota_attrs()
    |> Map.merge(%{
      window_kind: "primary",
      window_minutes: 300,
      used_percent: used_percent
    })
  end

  defp weekly_pressure_quota_attrs(used_percent, attrs) do
    weekly_exhausted_quota_attrs()
    |> Map.merge(%{used_percent: used_percent})
    |> Map.merge(Map.new(attrs))
  end

  defp open_circuit!(pool, _api_key, model, assignment) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %RoutingCircuitState{}
    |> RoutingCircuitState.changeset(%{
      pool_id: pool.id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: assignment.upstream_identity_id,
      model_identifier: model.exposed_model_id,
      route_class: "proxy_http",
      status: "open",
      reason_code: "test_circuit_open",
      failure_count: 3,
      success_count: 0,
      opened_at: now,
      next_probe_at: DateTime.add(now, 60, :second),
      metadata: %{"source" => "route_filtering_test"},
      created_at: now,
      updated_at: now
    })
    |> Repo.insert!()
  end

  defp assert_auto_redeem_usage_requests(upstream) do
    assert [consume_request | usage_requests] = FakeUpstream.requests(upstream)
    assert consume_count(upstream) == 1
    assert Enum.all?(usage_requests, &(&1.method == "GET" and &1.path in ["/api/codex/usage", "/backend-api/codex/usage", "/backend-api/wham/usage"]))
    assert %{path: "/api/codex/usage"} = usage_request = Enum.find(usage_requests, &(&1.path == "/api/codex/usage"))
    [consume_request, usage_request]
  end

  defp gateway_recovery_jobs do
    CodexPooler.Jobs.SavedResetRedemptionWorker
    |> then(&all_enqueued(worker: &1))
    |> Enum.filter(&(&1.args["recovery_kind"] == "stale_consuming"))
    |> Enum.map(&{&1.id, &1.args})
    |> Enum.sort()
  end

  defp with_info_log(fun) when is_function(fun, 0) do
    previous_level = Logger.level()
    # Also on_exit: a linked crash or the ExUnit timeout kills the test before `after` runs.
    on_exit(fn -> Logger.configure(level: previous_level) end)
    Logger.configure(level: :info)

    try do
      with_log([level: :info], fun)
    after
      Logger.configure(level: previous_level)
    end
  end

  defp usage_payload(available_count, opts \\ []) do
    observed_at = Keyword.get_lazy(opts, :observed_at, &DateTime.utc_now/0)
    window_minutes = Keyword.get(opts, :window_minutes, 10_080)
    reset_at = Keyword.get(opts, :reset_at, DateTime.add(observed_at, 900, :second))

    %{
      "plan_type" => "pro",
      "rate_limit_reset_credits" => %{"available_count" => available_count},
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
      "spend_control" => %{"reached" => false},
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => %{
          "used_percent" => Keyword.get(opts, :used_percent, 10),
          "limit_window_seconds" => window_minutes * 60,
          "reset_after_seconds" => DateTime.diff(reset_at, observed_at, :second),
          "reset_at" => DateTime.to_unix(reset_at)
        }
      }
    }
  end

  defp put_test_quota_snapshots(route_state, windows_by_identity_id, as_of) do
    identities =
      Map.new(route_state.candidates, fn {_assignment, identity} -> {identity.id, identity} end)

    snapshots =
      Map.new(windows_by_identity_id, fn {identity_id, windows} ->
        {identity_id, RoutingQuotaSnapshot.from_identity(Map.fetch!(identities, identity_id), windows, as_of)}
      end)

    RouteState.put_quota_snapshots(route_state, snapshots)
  end
end
