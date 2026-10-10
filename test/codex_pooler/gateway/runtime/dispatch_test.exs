defmodule CodexPooler.Gateway.Runtime.DispatchTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, prime_routing_quota!: 1, start_upstream: 1]

  import CodexPooler.PoolerFixtures, only: [active_upstream_assignment_fixture: 2]

  alias CodexPooler.Access
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request, RequestReplay, RequestReplayEntitlement}
  alias CodexPooler.Accounting.FailureResponse
  alias CodexPooler.AccountingTestSupport
  alias CodexPooler.AccountsFixtures
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.CompactionTrigger
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.CompactionProjectionContext
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Persistence.{CodexTurn, RoutingCircuitState, SessionContinuity}
  alias CodexPooler.Gateway.Routing.{BridgeRing, CircuitState, RoutePlanInput}
  alias CodexPooler.Gateway.Runtime.Dispatch
  alias CodexPooler.Gateway.Runtime.Dispatch.AccountingReservation
  alias CodexPooler.Gateway.Runtime.Dispatch.CandidateDispatch
  alias CodexPooler.Gateway.Runtime.Dispatch.Context
  alias CodexPooler.Gateway.Runtime.Dispatch.PreDispatch
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry
  alias CodexPooler.Gateway.Runtime.Routing.DispatchLifecycle
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Websocket
  alias CodexPooler.PoolerFixtures
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPooler.RequestReplayFixtures
  alias CodexPooler.UnboxedFixture
  alias CodexPooler.Upstreams.SavedResets.ProbeLease
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPooler.Upstreams.Secrets
  alias Ecto.Adapters.SQL.Sandbox

  @endpoint_path "/backend-api/codex/responses"

  setup context do
    if context[:committed_cleanup] do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
    end

    :ok
  end

  test "compaction projection merge cleanup applies the total settlement precedence table" do
    merge_reason = :merge_failed
    settlement_error = %{status: 500, code: "settlement_failed", message: "settlement failed"}
    neutral_error = %{status: 500, code: "neutral_failed", message: "neutral failed"}

    cases = [
      {{:ok, :settled}, :ok, {:accounting_failure, :merge_compaction_projection_metadata, :merge_failed}},
      {{:stale_generation, :newer_generation}, :ok, {:accounting_failure, :merge_compaction_projection_metadata, :merge_failed}},
      {{:error, settlement_error}, :ok, {:error, settlement_error}},
      {{:ok, :settled}, {:error, neutral_error}, {:error, neutral_error}},
      {{:stale_generation, :newer_generation}, {:error, neutral_error}, {:error, neutral_error}},
      {{:error, settlement_error}, {:error, neutral_error}, {:accounting_failure, :merge_compaction_projection_cleanup, {settlement_error, neutral_error}}}
    ]

    for {settlement_result, neutral_result, expected} <- cases do
      test_pid = self()

      assert expected ==
               CandidateDispatch.run_compaction_projection_cleanup(
                 fn ->
                   send(test_pid, :settlement_cleanup)
                   settlement_result
                 end,
                 fn ->
                   send(test_pid, :neutral_cleanup)
                   neutral_result
                 end,
                 merge_reason
               )

      assert_received :settlement_cleanup
      assert_received :neutral_cleanup
      refute_received :settlement_cleanup
      refute_received :neutral_cleanup
    end
  end

  for neutral <- [:ok, :error] do
    test "candidate compaction merge failure contains a stale settlement with neutral #{neutral}" do
      fixture = projection_merge_failure_fixture(unquote(neutral))
      parent = self()
      neutral_error = %{status: 500, code: "neutral_failed", message: "neutral cleanup failed"}

      operations = %{
        merge_request_metadata: fn _request, _metadata -> {:error, :synthetic_merge_failure} end,
        finalize_failure: fn _request, _attempt, _attrs ->
          send(parent, :stale_settlement_called)
          {:stale_generation, %{}}
        end,
        neutral_completion: fn context ->
          send(parent, :neutral_called)
          assert :ok = DispatchLifecycle.neutral_completion(context)
          if unquote(neutral) == :ok, do: :ok, else: {:error, neutral_error}
        end
      }

      expected_code = if unquote(neutral) == :ok, do: "gateway_accounting_failed", else: "neutral_failed"

      capture_log(fn ->
        assert {:error, %{code: ^expected_code}} = CandidateDispatch.dispatch_with_operations(fixture.context, fn _prepared -> flunk("merge failure dispatched upstream") end, operations)
      end)

      assert_received :stale_settlement_called
      assert_received :neutral_called
      refute_received :stale_settlement_called
      refute_received :neutral_called
      assert FakeUpstream.count(fixture.first_upstream) == 0
      assert FakeUpstream.count(fixture.second_upstream) == 0
      assert Repo.get!(Request, fixture.request.id).status == "in_progress"
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^fixture.request.id and l.entry_kind != "reservation"), :count) == 0
    end
  end

  # Compaction bridges are excluded from replay preparation. This composes the
  # shared cleanup boundary with a real ordinary-turn authority transition; it
  # does not manufacture a compact replay or claim a production compaction race.
  for newer <- [:in_progress, :succeeded], neutral <- [:ok, :error] do
    @tag :committed_cleanup
    test "real generation transfer leaves #{newer} replay untouched with neutral #{neutral}" do
      assert_generation_cleanup!(unquote(newer), unquote(neutral))
    end
  end

  @tag :committed_cleanup
  test "current-owner HTTP compact cleanup rolls back request attempt ledger and turn together on settlement failure" do
    fixture = committed_cleanup_fixture!(:compact)
    context = cleanup_selection!(fixture)
    before = replay_rows(fixture)
    parent = self()
    sequence = install_deferred_cleanup_failure!(fixture.request.id)
    CodexPooler.TestAppEnv.restore_on_exit(SettlementRetry)
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)

    log =
      capture_log(fn ->
        assert {:error, %{code: "gateway_accounting_failed"}} =
                 CandidateDispatch.run_compaction_projection_cleanup(
                   fn ->
                     send(parent, :current_settlement)
                     AttemptSettlement.finalize_failure(fixture.request, fixture.attempt, %{last_error_code: "gateway_accounting_failed", response_status_code: 500})
                   end,
                   fn ->
                     send(parent, :current_neutral)
                     DispatchLifecycle.neutral_completion(context)
                   end,
                   :synthetic_merge_failure
                 )
      end)

    assert log =~ "settlement abandoned"
    assert [[1, true]] = Repo.query!("SELECT last_value, is_called FROM #{sequence}").rows
    assert replay_rows(fixture) == before
    assert_received :current_settlement
    assert_received :current_neutral
    refute_received :current_settlement
    refute_received :current_neutral
    assert Repo.get!(RoutingCircuitState, context.routing_circuit_state.id).probe_admission_ids == []
    CodexPooler.TestDiagnostics.puts("current_owner_cleanup deferred_commit_graph=1 rollback_preserved_all_rows=true neutral_calls=1")
  end

  test "candidate dispatch merge failure runs complete fail-closed cleanup precedence" do
    settlement_error = %{
      status: 500,
      code: "forced_settlement_failure",
      message: "forced settlement failure"
    }

    neutral_error = %{
      status: 500,
      code: "forced_neutral_failure",
      message: "forced neutral failure"
    }

    scenarios = [
      %{
        name: :settlement_ok_neutral_ok,
        settlement: :real,
        neutral: :real,
        expected_error: "gateway_accounting_failed",
        expected_operation: :merge_compaction_projection_metadata,
        expected_request_status: "failed",
        expected_attempt_status: "failed",
        expected_probe_count: 0
      },
      %{
        name: :settlement_error_neutral_ok,
        settlement: {:error, settlement_error},
        neutral: :real,
        expected_error: "forced_settlement_failure",
        expected_operation: nil,
        expected_request_status: "in_progress",
        expected_attempt_status: "in_progress",
        expected_probe_count: 0
      },
      %{
        name: :settlement_ok_neutral_error,
        settlement: :real,
        neutral: {:error, neutral_error},
        expected_error: "forced_neutral_failure",
        expected_operation: nil,
        expected_request_status: "failed",
        expected_attempt_status: "failed",
        expected_probe_count: 1
      },
      %{
        name: :settlement_error_neutral_error,
        settlement: {:error, settlement_error},
        neutral: {:error, neutral_error},
        expected_error: "gateway_accounting_failed",
        expected_operation: :merge_compaction_projection_cleanup,
        expected_request_status: "in_progress",
        expected_attempt_status: "in_progress",
        expected_probe_count: 1
      }
    ]

    for scenario <- scenarios do
      fixture = projection_merge_failure_fixture(scenario.name)
      parent = self()
      scenario_name = scenario.name

      operations = %{
        merge_request_metadata: fn request, metadata ->
          send(parent, {scenario_name, :merge, request.id, metadata})
          {:error, :forced_projection_merge_failure}
        end,
        decrypt_active_secret: fn _identity, _kind ->
          send(parent, {scenario_name, :decrypt})
          {:error, :unexpected_decrypt}
        end,
        upstream_url: fn _identity, _assignment, _endpoint ->
          send(parent, {scenario_name, :url})
          {:error, :unexpected_url}
        end,
        finalize_failure: fn request, attempt, attrs ->
          send(parent, {scenario_name, :settlement, request.id, attempt.id, attrs})

          case scenario.settlement do
            :real -> AttemptSettlement.finalize_failure(request, attempt, attrs)
            forced_result -> forced_result
          end
        end,
        neutral_completion: fn context ->
          send(parent, {scenario_name, :neutral, context.assignment.id})

          case scenario.neutral do
            :real -> DispatchLifecycle.neutral_completion(context)
            forced_result -> forced_result
          end
        end,
        accounting_failure: fn operation, request, attempt, reason ->
          send(parent, {scenario_name, :accounting_failure, operation, reason})

          FailureResponse.accounting_failure(
            operation,
            request,
            attempt,
            reason
          )
        end
      }

      log =
        capture_log(fn ->
          assert {:error, %{code: expected_error}} =
                   CandidateDispatch.dispatch_with_operations(
                     fixture.context,
                     fn _prepared ->
                       send(parent, {scenario_name, :transport})
                       {:ok, %{status: 200}}
                     end,
                     operations
                   )

          assert expected_error == scenario.expected_error
        end)

      request_id = fixture.request.id

      assert_receive {^scenario_name, :merge, ^request_id, %{"compaction_projection" => projection}}

      assert projection["action"] == "preserved"

      assert_receive {^scenario_name, :settlement, ^request_id, attempt_id, attrs}
      assert attrs.last_error_code == "gateway_accounting_failed"
      assert attrs.retry_count == 0
      first_assignment_id = fixture.first_assignment.id
      assert_receive {^scenario_name, :neutral, ^first_assignment_id}

      if scenario.expected_operation do
        assert_receive {^scenario_name, :accounting_failure, operation, reason}
        assert operation == scenario.expected_operation

        if operation == :merge_compaction_projection_metadata do
          assert reason == :forced_projection_merge_failure
        else
          assert {%{code: "forced_settlement_failure"}, %{code: "forced_neutral_failure"}} =
                   reason
        end

        assert log =~ "operation=#{scenario.expected_operation}"
      else
        refute_received {^scenario_name, :accounting_failure, _, _}
      end

      refute_received {^scenario_name, :decrypt}
      refute_received {^scenario_name, :url}
      refute_received {^scenario_name, :transport}
      refute_received {^scenario_name, :merge, _, _}
      refute_received {^scenario_name, :settlement, _, _, _}
      refute_received {^scenario_name, :neutral, _}

      assert FakeUpstream.count(fixture.first_upstream) == 0
      assert FakeUpstream.count(fixture.second_upstream) == 0

      attempts =
        Repo.all(
          from attempt in Attempt,
            where: attempt.request_id == ^fixture.request.id,
            order_by: [asc: attempt.attempt_number]
        )

      assert [%Attempt{id: ^attempt_id, pool_upstream_assignment_id: attempt_assignment_id}] =
               attempts

      assert attempt_assignment_id == fixture.first_assignment.id

      request = Repo.get!(Request, fixture.request.id)
      attempt = Repo.get!(Attempt, attempt_id)
      assert request.status == scenario.expected_request_status
      assert request.retry_count == 0
      assert attempt.status == scenario.expected_attempt_status
      assert get_in(request.request_metadata, ["routing", "demotion_reason"]) == nil
      assert get_in(request.request_metadata, ["routing", "circuit_failure"]) == nil

      circuit = Repo.get!(RoutingCircuitState, fixture.circuit.id)
      assert circuit.status == "half_open"
      assert circuit.failure_count == fixture.circuit.failure_count
      assert circuit.reason_code == fixture.circuit.reason_code
      assert circuit.metadata["probe_in_flight_count"] == scenario.expected_probe_count

      refute Repo.exists?(
               from circuit in RoutingCircuitState,
                 where:
                   circuit.pool_upstream_assignment_id == ^fixture.second_assignment.id and
                     circuit.route_class == "proxy_compact"
             )
    end
  end

  test "compact provenance persists before decrypt failure and never reaches upstream" do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

    downstream = %{
      "model" => setup.model.exposed_model_id,
      "previous_response_id" => "resp_dispatch_projection_anchor",
      "input" => [
        %{"type" => "custom_tool_call_output", "output" => "raw-dispatch-output"},
        %{"type" => "compaction_trigger"}
      ]
    }

    compact = CompactionTrigger.project_responses_payload(downstream)

    request_options =
      %{
        request_id: "dispatch-projection-#{System.unique_integer([:positive])}",
        upstream_endpoint: @endpoint_path,
        compaction_trigger_bridge?: true,
        compaction_projection_context: CompactionProjectionContext.new(downstream, compact)
      }
      |> RequestOptions.build("/backend-api/codex/responses/compact", compact)
      |> RequestOptions.put_transport(route_class: "proxy_compact")

    assert {:ok, reserved} =
             Accounting.reserve(auth, setup.model, compact, %{
               endpoint: "/backend-api/codex/responses/compact",
               transport: "http_compact_json",
               correlation_id: "dispatch-projection-#{System.unique_integer([:positive])}",
               request_metadata: %{
                 "compaction_bridge" => %{"applied" => true, "result_transport" => "buffered"}
               }
             })

    candidates = [{setup.assignment, setup.identity}]

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: "/backend-api/codex/responses/compact",
               payload: compact,
               model: setup.model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates})
             })

    {_count, _} =
      Secrets.revoke_active_secrets(
        setup.identity.id,
        DateTime.utc_now() |> DateTime.truncate(:microsecond)
      )

    {result, log} =
      with_log([level: :warning], fn ->
        CandidateDispatch.dispatch(
          context,
          fn _prepared ->
            flunk("transport must not run after decrypt failure")
          end
        )
      end)

    assert {:error, %{code: "upstream_request_failed"}} = result
    assert event_count(log, "compact terminal decision") == 1
    assert log =~ "source_stage=local_preflight"
    assert log =~ "code=upstream_request_failed"
    assert log =~ "status=502"
    assert log =~ "terminal_type=dispatch_error"
    assert log =~ "param_state=absent"
    assert log =~ "request_id=#{reserved.request.id}"
    assert log =~ "elapsed_ms="

    projection =
      Repo.get!(Request, reserved.request.id).request_metadata["compaction_projection"]

    assert projection["action"] == "preserved"
    assert projection["downstream_frame"]["state"] == "valid"
    refute inspect(projection) =~ "resp_dispatch_projection_anchor"
    refute inspect(projection) =~ "raw-dispatch-output"
    assert FakeUpstream.count(upstream) == 0
  end

  defp event_count(log, message), do: length(String.split(log, message)) - 1

  test "compact provenance persists before upstream URL resolution failure" do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(upstream)

    assignment =
      setup.assignment
      |> Ecto.Changeset.change(%{metadata: %{"base_url" => ""}})
      |> Repo.update!()

    identity =
      setup.identity
      |> Ecto.Changeset.change(%{metadata: %{"base_url" => ""}})
      |> Repo.update!()

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    downstream = compact_downstream(setup, "resp_dispatch_url_anchor")
    compact = CompactionTrigger.project_responses_payload(downstream)
    request_options = compact_request_options(compact, downstream)

    assert {:ok, reserved} = reserve_compact(auth, setup, compact)
    candidates = [{assignment, identity}]

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: "/backend-api/codex/responses/compact",
               payload: compact,
               model: setup.model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates})
             })

    assert {:error, %{code: "upstream_request_failed"}} =
             CandidateDispatch.dispatch(
               context,
               fn _prepared ->
                 flunk("transport must not run after URL resolution failure")
               end
             )

    projection = Repo.get!(Request, reserved.request.id).request_metadata["compaction_projection"]
    assert projection["action"] == "preserved"
    refute inspect(projection) =~ "resp_dispatch_url_anchor"
    assert FakeUpstream.count(upstream) == 0
  end

  test "connection-bound compact disables candidate retry while ordinary dispatch reaches fallback" do
    first_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    second_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(first_upstream)

    %{assignment: second_assignment, identity: second_identity} =
      active_upstream_assignment_fixture(setup.pool, %{
        account_label: "Connection-bound retry control",
        base_url: FakeUpstream.url(second_upstream)
      })

    prime_routing_quota!(second_identity)

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    candidates = [{setup.assignment, setup.identity}, {second_assignment, second_identity}]

    for connection_bound? <- [true, false] do
      downstream =
        compact_downstream(
          setup,
          "resp_dispatch_retry_policy_#{connection_bound?}_#{System.unique_integer([:positive])}"
        )

      compact = CompactionTrigger.project_responses_payload(downstream)

      request_options =
        compact
        |> compact_request_options(downstream)
        |> maybe_connection_bound_compact(connection_bound?)

      transport = if connection_bound?, do: "websocket", else: "http_compact_json"

      assert {:ok, reserved} =
               Accounting.reserve(auth, setup.model, compact, %{
                 endpoint: "/backend-api/codex/responses/compact",
                 transport: transport,
                 correlation_id: "dispatch-retry-policy-#{connection_bound?}-#{System.unique_integer([:positive])}",
                 request_metadata: %{
                   "compaction_bridge" => %{
                     "applied" => true,
                     "result_transport" => "buffered"
                   }
                 }
               })

      assert {:ok, context} =
               Context.new(%{
                 auth: auth,
                 endpoint: "/backend-api/codex/responses/compact",
                 payload: compact,
                 model: setup.model,
                 reserved: reserved,
                 candidates: candidates,
                 request_options: request_options,
                 route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates})
               })

      planned_assignment_ids = Enum.map(context.route_plan.candidates, &elem(&1, 0).id)
      parent = self()

      result =
        Dispatch.dispatch(context, fn selected_context ->
          send(parent, {
            :retry_policy_candidate,
            connection_bound?,
            selected_context.assignment.id,
            selected_context.allow_retry?
          })

          if selected_context.allow_retry? do
            {:retry, :synthetic_retryable_failure}
          else
            {:ok, %{status: 200}}
          end
        end)

      assert {:ok, %{status: 200}} = result

      if connection_bound? do
        assert_receive {:retry_policy_candidate, true, selected_assignment_id, false}
        assert selected_assignment_id == List.first(planned_assignment_ids)
        refute_received {:retry_policy_candidate, true, _assignment_id, _allow_retry?}
      else
        assert_receive {:retry_policy_candidate, false, first_assignment_id, true}
        assert_receive {:retry_policy_candidate, false, second_assignment_id, false}
        assert [first_assignment_id, second_assignment_id] == planned_assignment_ids
      end
    end
  end

  test "context construction returns sanitized gateway error when route plan metadata cannot be recorded" do
    setup = gateway_setup(start_upstream(FakeUpstream.json_response(%{"data" => []})))
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(setup)
    request_options = request_options(auth, payload, setup)

    candidates = [{setup.assignment, setup.identity}]

    context_input = %{
      auth: auth,
      endpoint: @endpoint_path,
      payload: payload,
      model: setup.model,
      reserved: %{request: invalid_request()},
      candidates: candidates,
      request_options: request_options,
      route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates})
    }

    log =
      capture_log(fn ->
        assert {:error,
                %{
                  status: 500,
                  code: "gateway_accounting_failed",
                  message: "gateway accounting finalization failed"
                }} = Context.new(context_input)
      end)

    assert log =~ "gateway accounting finalization failed"
    assert log =~ "operation=merge_route_plan_metadata"
    assert log =~ "request_id=#{context_input.reserved.request.id}"
  end

  test "dispatch_from returns sanitized gateway error when selected route metadata cannot be recorded" do
    setup = gateway_setup(start_upstream(FakeUpstream.json_response(%{"data" => []})))
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(setup)
    request_options = request_options(auth, payload, setup)
    request = invalid_request()
    candidates = [{setup.assignment, setup.identity}]
    parent = self()

    context = %Context{
      auth: auth,
      endpoint: @endpoint_path,
      payload: payload,
      model: setup.model,
      reserved: %{request: request},
      candidates: candidates,
      request_options: request_options,
      route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates}),
      route_plan:
        BridgeRing.plan_route(%{
          auth: auth,
          model: setup.model,
          candidates: candidates,
          route_plan_input: RoutePlanInput.from_reserved(%{request: request}),
          request_options: request_options
        }),
      route_class: request_options.transport.route_class
    }

    log =
      capture_log(fn ->
        assert {:error,
                %{
                  status: 500,
                  code: "gateway_accounting_failed",
                  message: "gateway accounting finalization failed"
                }} =
                 Dispatch.dispatch_from(context, 0, fn _context ->
                   send(parent, :transport_called)
                 end)
      end)

    assert log =~ "gateway accounting finalization failed"
    assert log =~ "operation=merge_route_selection_metadata"
    assert log =~ "request_id=#{request.id}"
    refute_received :transport_called
  end

  test "candidate selection preserves a resolved Lite snapshot across a Full legacy assignment" do
    setup = gateway_setup(start_upstream(FakeUpstream.json_response(%{"data" => []})))

    %{assignment: fallback_assignment, identity: fallback_identity} =
      active_upstream_assignment_fixture(setup.pool, %{
        account_label: "Resolved snapshot fallback upstream"
      })

    prime_routing_quota!(fallback_identity)

    model =
      setup.model
      |> Ecto.Changeset.change(%{
        metadata: %{
          "source_assignment_ids" => [setup.assignment.id, fallback_assignment.id],
          "source_assignment_models" => %{
            setup.assignment.id => %{
              "slug" => setup.model.exposed_model_id,
              "use_responses_lite" => false
            },
            fallback_assignment.id => %{
              "slug" => setup.model.exposed_model_id,
              "use_responses_lite" => false
            }
          }
        }
      })
      |> Repo.update!()

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(%{setup | model: model})
    unresolved_options = request_options(auth, payload, %{setup | model: model})

    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.insert!(%ModelServingOverride{
      pool_id: setup.pool.id,
      exposed_model_id: model.exposed_model_id,
      mode: "lite",
      created_at: timestamp,
      updated_at: timestamp
    })

    assert {:ok, prepared} =
             PreDispatch.prepare(auth, @endpoint_path, payload, unresolved_options, model)

    request_options = prepared.request_options

    assert {:ok, reserved} =
             Accounting.reserve(auth, model, payload, %{
               endpoint: @endpoint_path,
               transport: "http_json",
               correlation_id: "resolved-selection-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    candidates = [
      {setup.assignment, setup.identity},
      {fallback_assignment, fallback_identity}
    ]

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: @endpoint_path,
               payload: payload,
               model: model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: model, candidates: candidates})
             })

    assert {:ok, %{status: 200}} =
             Dispatch.dispatch_from(context, 1, fn selected_context ->
               assert selected_context.assignment.id in [
                        setup.assignment.id,
                        fallback_assignment.id
                      ]

               assert RequestOptions.model_serving_mode_snapshot(selected_context.request_options) ==
                        %{
                          configured_mode: "lite",
                          effective_mode: "lite",
                          source: "override"
                        }

               assert RequestOptions.use_responses_lite?(selected_context.request_options)
               {:ok, %{status: 200}}
             end)
  end

  test "candidate selection exposes the unresolved selected-assignment Lite fallback" do
    setup = gateway_setup(start_upstream(FakeUpstream.json_response(%{"data" => []})))

    model =
      setup.model
      |> Ecto.Changeset.change(%{
        metadata: %{
          "source_assignment_ids" => [setup.assignment.id],
          "source_assignment_models" => %{
            setup.assignment.id => %{"use_responses_lite" => true}
          }
        }
      })
      |> Repo.update!()

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(%{setup | model: model})
    request_options = request_options(auth, payload, %{setup | model: model})

    assert RequestOptions.model_serving_mode_snapshot(request_options) == nil

    assert {:ok, reserved} =
             Accounting.reserve(auth, model, payload, %{
               endpoint: @endpoint_path,
               transport: "http_json",
               correlation_id: "unresolved-selection-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    candidates = [{setup.assignment, setup.identity}]

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: @endpoint_path,
               payload: payload,
               model: model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: model, candidates: candidates})
             })

    assert {:ok, %{status: 200}} =
             Dispatch.dispatch_from(context, 0, fn selected_context ->
               assert RequestOptions.model_serving_mode_snapshot(selected_context.request_options) ==
                        nil

               assert RequestOptions.use_responses_lite?(selected_context.request_options)
               {:ok, %{status: 200}}
             end)
  end

  test "bound reset probe scope mismatch finalizes before attempt or transport dispatch" do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(setup)
    initial_probe = ResetProbe.new()

    assert {:ok, reset_probe} =
             ResetProbe.bind(
               initial_probe,
               Ecto.UUID.generate(),
               setup.identity.id,
               setup.model.exposed_model_id,
               "proxy_http"
             )

    request_options =
      auth
      |> request_options(payload, setup)
      |> RequestOptions.put_routing(reset_probe: reset_probe)

    assert {:ok, reserved} =
             Accounting.reserve(auth, setup.model, payload, %{
               endpoint: @endpoint_path,
               transport: "http_json",
               correlation_id: "reset-probe-scope-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    candidates = [{setup.assignment, setup.identity}]

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: @endpoint_path,
               payload: payload,
               model: setup.model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates})
             })

    parent = self()

    assert {:error, %{status: 503, code: "no_eligible_backend"}} =
             Dispatch.dispatch(context, fn _selected_context ->
               send(parent, :transport_called)
               {:ok, %{status: 200}}
             end)

    refute_received :transport_called
    assert FakeUpstream.count(upstream) == 0

    assert %Request{status: "failed", last_error_code: "no_eligible_backend"} =
             Repo.reload!(reserved.request)

    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^reserved.request.id), :count) ==
             0
  end

  test "http_sse reservation rejected before dispatch releases promptly with a classified phase" do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(setup)

    assert {:ok, reset_probe} =
             ResetProbe.bind(
               ResetProbe.new(),
               Ecto.UUID.generate(),
               setup.identity.id,
               setup.model.exposed_model_id,
               "proxy_http"
             )

    request_options =
      auth
      |> request_options(payload, setup)
      |> RequestOptions.put_routing(reset_probe: reset_probe)

    assert {:ok, reserved} =
             Accounting.reserve(auth, setup.model, payload, %{
               endpoint: @endpoint_path,
               transport: "http_sse",
               correlation_id: "pre-attempt-phase-#{System.unique_integer([:positive])}",
               request_metadata: %{}
             })

    candidates = [{setup.assignment, setup.identity}]

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: @endpoint_path,
               payload: payload,
               model: setup.model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: setup.model, candidates: candidates})
             })

    parent = self()
    handler_id = {__MODULE__, :pre_attempt_release, System.unique_integer([:positive])}

    :telemetry.attach(
      handler_id,
      PreAttemptRelease.telemetry_event(),
      fn _event, measurements, metadata, _config ->
        send(parent, {handler_id, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    assert {:error, %{status: 503, code: "no_eligible_backend"}} =
             Dispatch.dispatch(context, fn _selected_context ->
               send(parent, :transport_called)
               {:ok, %{status: 200}}
             end)

    refute_received :transport_called
    assert FakeUpstream.count(upstream) == 0

    assert %Request{status: "failed", last_error_code: "no_eligible_backend"} =
             Repo.reload!(reserved.request)

    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^reserved.request.id), :count) ==
             0

    assert [release] =
             Repo.all(
               from entry in LedgerEntry,
                 where: entry.request_id == ^reserved.request.id and entry.entry_kind == "release"
             )

    assert release.attempt_id == nil
    assert release.details["release_reason"] == "no_eligible_backend"

    assert release.details[PreAttemptRelease.detail_key()] ==
             PreAttemptRelease.routing_rejected()

    assert_received {^handler_id, %{count: 1}, %{phase: "routing_rejected", transport: "http_sse"}}
  end

  test "bound reset probe scope mutations fail before accounting reservation" do
    upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    sibling_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(upstream)

    %{assignment: sibling_assignment} =
      active_upstream_assignment_fixture(setup.pool, %{
        account_label: "Reset probe pre-reservation sibling",
        base_url: FakeUpstream.url(sibling_upstream)
      })

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    generation = 1
    attempt_id = "pre-reservation-attempt"

    identity =
      setup.identity
      |> UpstreamIdentity.changeset(%{
        metadata: %{
          "saved_reset_redemption" => %{
            "status" => "redeeming",
            "phase" => "consumed_pending_probe",
            "attempt_id" => attempt_id,
            "generation" => generation,
            "consumed_at" => DateTime.to_iso8601(now),
            "deadline_at" => DateTime.to_iso8601(DateTime.add(now, 15, :minute)),
            "result" => %{"code" => "reset", "applied" => true}
          }
        }
      })
      |> Repo.update!()

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = payload(setup)

    base_options = request_options(auth, payload, setup)

    assert {:ok, probe} =
             ResetProbe.new()
             |> ResetProbe.bind(
               setup.assignment.id,
               identity.id,
               setup.model.exposed_model_id,
               "proxy_http"
             )

    assert {:ok, :claimed} =
             ProbeLease.claim(identity, generation, attempt_id, probe, now)

    persisted_redemption =
      Repo.reload!(identity).metadata["saved_reset_redemption"]

    mutations = [
      assignment: %{probe | pool_upstream_assignment_id: Ecto.UUID.generate()},
      identity: %{probe | upstream_identity_id: Ecto.UUID.generate()},
      effective_model: %{probe | effective_model: "gpt-other-model"},
      route_class: %{probe | route_class: "proxy_stream"}
    ]

    for {dimension, mismatch} <- mutations do
      request_options = %{
        base_options
        | routing: %{base_options.routing | reset_probe: mismatch}
      }

      route_state =
        RouteState.new(%{
          visible_model: setup.model,
          candidates: [{setup.assignment, identity}],
          reset_probe: mismatch
        })

      assert {:error, {:reset_probe_scope_mismatch, %{status: 503, code: "no_eligible_backend", param: "model"}}} =
               AccountingReservation.validate_reset_probe_scope(
                 [{setup.assignment, identity}],
                 request_options,
                 route_state
               ),
             "expected #{dimension} mismatch to fail closed"

      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 0
      assert Repo.aggregate(from(a in Attempt), :count) == 0
      assert FakeUpstream.count(upstream) == 0
      assert FakeUpstream.count(sibling_upstream) == 0

      assert Repo.reload!(identity).metadata["saved_reset_redemption"] == persisted_redemption
      assert persisted_redemption["probe"]["token"] == probe.token
      assert sibling_assignment.id != probe.pool_upstream_assignment_id
    end
  end

  defp assert_generation_cleanup!(newer, neutral) do
    fixture = committed_cleanup_fixture!()
    context = cleanup_selection!(fixture)
    parent = self()
    ref = make_ref()
    supervisor = start_supervised!(Task.Supervisor)

    stale_caller =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Repo.checkout(fn ->
          [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
          send(parent, {ref, :old_caller_ready, backend})

          receive do
            {^ref, :authority_changed} -> run_stale_cleanup(fixture, context, parent, ref, neutral)
          after
            15_000 -> flunk("authority writer did not release the stale caller")
          end
        end)
      end)

    assert_receive {^ref, :old_caller_ready, old_backend}, 15_000

    writer_backend =
      Repo.checkout(fn ->
        [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
        assert backend != old_backend
        assert {:ok, armed} = RequestReplay.arm(RequestReplayFixtures.arm_input(fixture))
        input = RequestReplayFixtures.consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
        assert {:ok, replay} = RequestReplay.consume(input)
        assert replay.attempt.replay_generation == 1
        assert replay.attempt.id != fixture.attempt.id

        if newer == :succeeded do
          assert {:ok, _settled} = AttemptSettlement.finalize_success(replay.request, replay.attempt, %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2}, %{response_status_code: 200})
        end

        backend
      end)

    before = replay_rows(fixture)
    send(stale_caller.pid, {ref, :authority_changed})
    result = Task.await(stale_caller, 15_000)
    assert_receive {^ref, :settlement, :stale_generation}, 15_000
    assert_receive {^ref, :neutral}, 15_000
    refute_received {^ref, :settlement, _}
    refute_received {^ref, :neutral}
    assert replay_rows(fixture) == before
    assert Repo.get!(RoutingCircuitState, context.routing_circuit_state.id).probe_admission_ids == []
    {:ok, owner} = WebsocketOwnerSession.lookup(fixture.session.id)
    assert Agent.get(:sys.get_state(owner).upstream_pid, & &1) == 0

    expected = if neutral == :ok, do: {:accounting_failure, :merge_compaction_projection_metadata, :synthetic_merge_failure}, else: {:error, neutral_cleanup_error()}
    assert result == expected
    CodexPooler.TestDiagnostics.puts("cleanup authority old_backend=#{old_backend} writer_backend=#{writer_backend} generation=0->1 newer=#{newer} stale=true neutral=#{neutral} rows_unchanged=true")
  end

  defp run_stale_cleanup(fixture, context, parent, ref, neutral) do
    CandidateDispatch.run_compaction_projection_cleanup(
      fn ->
        result = AttemptSettlement.finalize_failure(fixture.request, fixture.attempt, %{last_error_code: "gateway_accounting_failed", response_status_code: 500})
        send(parent, {ref, :settlement, elem(result, 0)})
        result
      end,
      fn ->
        send(parent, {ref, :neutral})
        assert :ok = DispatchLifecycle.neutral_completion(context)
        if neutral == :ok, do: :ok, else: {:error, neutral_cleanup_error()}
      end,
      :synthetic_merge_failure
    )
  rescue
    CaseClauseError -> {:raised, CaseClauseError}
  end

  defp neutral_cleanup_error, do: %{status: 500, code: "neutral_failed", message: "neutral cleanup failed"}

  defp install_deferred_cleanup_failure!(request_id) do
    suffix = request_id |> String.replace("-", "")
    sequence = "cleanup_commit_#{suffix}"
    function = "cleanup_abort_#{suffix}"
    trigger = "cleanup_abort_#{suffix}"

    on_exit(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{trigger} ON requests")
      Repo.query!("DROP FUNCTION IF EXISTS #{function}()")
      Repo.query!("DROP SEQUENCE IF EXISTS #{sequence}")
      assert [[nil, nil]] = Repo.query!("SELECT to_regclass($1), to_regprocedure($2)", [sequence, function <> "()"]).rows
      CodexPooler.TestDiagnostics.puts("current_owner_cleanup sql_objects_removed=true")
    end)

    Repo.query!("CREATE SEQUENCE #{sequence}")

    Repo.query!("""
    CREATE FUNCTION #{function}() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.status <> 'failed'
        OR (SELECT count(*) FROM attempts WHERE request_id = NEW.id AND status = 'failed') <> 1
        OR (SELECT count(*) FROM ledger_entries WHERE request_id = NEW.id AND entry_kind IN ('settlement', 'release')) <> 2
        OR (SELECT count(*) FROM codex_turns WHERE request_id = NEW.id AND status = 'failed') <> 1 THEN
        RAISE EXCEPTION 'settlement did not reach complete graph';
      END IF;
      PERFORM nextval('#{sequence}');
      RAISE EXCEPTION 'synthetic deferred settlement failure' USING ERRCODE = '40001';
    END;
    $$
    """)

    Repo.query!("CREATE CONSTRAINT TRIGGER #{trigger} AFTER UPDATE ON requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.id = '#{request_id}'::uuid) EXECUTE FUNCTION #{function}()")
    sequence
  end

  defp replay_rows(fixture) do
    %{
      request: Repo.get!(Request, fixture.request.id),
      attempts: Repo.all(from a in Attempt, where: a.request_id == ^fixture.request.id, order_by: a.attempt_number),
      entitlement: Repo.get_by(RequestReplayEntitlement, request_id: fixture.request.id),
      ledger: Repo.all(from l in LedgerEntry, where: l.request_id == ^fixture.request.id, order_by: l.id),
      turn: Repo.get!(CodexTurn, fixture.turn.id)
    }
  end

  defp cleanup_selection!(fixture) do
    route_class = if fixture.request.transport == "websocket", do: "proxy_websocket", else: "proxy_compact"
    half_open_dispatch_circuit!(fixture.auth, fixture.model, fixture.assignment, fixture.identity, route_class)
    assert {:ok, %{admission: admission, state: state}} = CircuitState.begin_attempt(fixture.auth, fixture.model, fixture.assignment, route_class)
    assert length(state.probe_admission_ids) == 1
    %SelectedCandidateContext{auth: fixture.auth, model: fixture.model, assignment: fixture.assignment, identity: fixture.identity, route_class: route_class, routing_circuit_state: state, routing_circuit_admission: admission, routing_attempt_metadata: %{}}
  end

  defp committed_cleanup_fixture!(kind \\ :replay) do
    %{user: owner} = AccountsFixtures.committed_bootstrap_owner_fixture!()
    {:ok, holder} = Agent.start(fn -> nil end)

    on_exit(fn ->
      try do
        if fixture = Agent.get(holder, & &1), do: UnboxedFixture.cleanup_unboxed!(fn -> delete_cleanup_fixture!(fixture) end)
      after
        Agent.stop(holder)
      end
    end)

    {:ok, fixture} = Repo.transaction(fn -> record_cleanup_fixture!(holder, owner, kind) end)
    fixture
  end

  defp record_cleanup_fixture!(holder, owner, kind) do
    fixture = if kind == :replay, do: RequestReplayFixtures.replay_fixture(owner: owner, reservation?: true), else: compact_cleanup_fixture!()
    Agent.update(holder, fn _ -> fixture end)
    fixture
  end

  defp delete_cleanup_fixture!(fixture) do
    RequestReplayFixtures.stop_replay_owner(fixture.session.id)
    Repo.delete_all(from e in RequestReplayEntitlement, where: e.request_id == ^fixture.request.id)
    PoolerFixtures.delete_committed_pools!([fixture.pool.id])
    Repo.delete_all(from i in UpstreamIdentity, where: i.id == ^fixture.identity.id)
    if pricing = Map.get(fixture, :pricing), do: Repo.delete!(pricing)
  end

  defp compact_cleanup_fixture! do
    setup = AccountingTestSupport.accounting_setup()
    downstream = compact_downstream(setup, "resp_current_owner_cleanup")
    compact = CompactionTrigger.project_responses_payload(downstream)
    assert {:ok, reserved} = reserve_compact(setup.auth, setup, compact)
    assert {:ok, session} = Websocket.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    assert {:ok, turn} = SessionContinuity.start_codex_turn(session, reserved.request, compact_request_options(compact, downstream))
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    Map.merge(setup, %{request: reserved.request, attempt: attempt, session: session, turn: turn})
  end

  defp payload(setup) do
    %{
      "model" => setup.model.exposed_model_id,
      "input" => [
        %{
          "type" => "message",
          "role" => "user",
          "content" => [
            %{"type" => "input_text", "text" => "runtime dispatch accounting regression"}
          ]
        }
      ]
    }
  end

  defp compact_downstream(setup, anchor) do
    %{
      "model" => setup.model.exposed_model_id,
      "previous_response_id" => anchor,
      "input" => [
        %{"type" => "custom_tool_call_output", "output" => "raw-dispatch-output"},
        %{"type" => "compaction_trigger"}
      ]
    }
  end

  defp projection_merge_failure_fixture(scenario_name) do
    setup_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    added_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(setup_upstream)

    %{assignment: second_assignment, identity: second_identity} =
      active_upstream_assignment_fixture(setup.pool, %{
        account_label: "Projection cleanup #{scenario_name}",
        base_url: FakeUpstream.url(added_upstream)
      })

    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    downstream = compact_downstream(setup, "resp_projection_cleanup_#{scenario_name}")
    compact = CompactionTrigger.project_responses_payload(downstream)
    request_options = compact_request_options(compact, downstream)
    assert {:ok, reserved} = reserve_compact(auth, setup, compact)

    candidates = [
      {setup.assignment, setup.identity},
      {second_assignment, second_identity}
    ]

    route_state = RouteState.new(%{visible_model: setup.model, candidates: candidates})

    assert {:ok, context} =
             Context.new(%{
               auth: auth,
               endpoint: "/backend-api/codex/responses/compact",
               payload: compact,
               model: setup.model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: route_state
             })

    [{first_assignment, first_identity}, {planned_second_assignment, _second_identity}] =
      context.route_plan.candidates

    {first_upstream, second_upstream} =
      if first_assignment.id == setup.assignment.id,
        do: {setup_upstream, added_upstream},
        else: {added_upstream, setup_upstream}

    circuit = half_open_dispatch_circuit!(auth, setup.model, first_assignment, first_identity)

    %{
      context: context,
      request: reserved.request,
      first_assignment: first_assignment,
      second_assignment: planned_second_assignment,
      first_upstream: first_upstream,
      second_upstream: second_upstream,
      circuit: circuit
    }
  end

  defp half_open_dispatch_circuit!(auth, model, assignment, identity, route_class \\ "proxy_compact") do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %RoutingCircuitState{
      pool_id: auth.pool.id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: identity.id,
      model_identifier: model.exposed_model_id,
      route_class: route_class,
      status: "half_open",
      reason_code: "projection_merge_test_probe",
      failure_count: 3,
      success_count: 0,
      opened_at: DateTime.add(now, -120, :second),
      half_opened_at: now,
      metadata: %{"probe_in_flight_count" => 0},
      created_at: DateTime.add(now, -120, :second),
      updated_at: now
    }
    |> Repo.insert!()
  end

  defp compact_request_options(compact, downstream) do
    %{
      request_id: "dispatch-projection-#{System.unique_integer([:positive])}",
      upstream_endpoint: @endpoint_path,
      compaction_trigger_bridge?: true,
      compaction_projection_context: CompactionProjectionContext.new(downstream, compact)
    }
    |> RequestOptions.build("/backend-api/codex/responses/compact", compact)
    |> RequestOptions.put_transport(route_class: "proxy_compact")
  end

  defp maybe_connection_bound_compact(request_options, true) do
    RequestOptions.put_transport(request_options,
      transport: "websocket",
      websocket_delivery_mode: :collect_compaction
    )
  end

  defp maybe_connection_bound_compact(request_options, false), do: request_options

  defp reserve_compact(auth, setup, compact) do
    Accounting.reserve(auth, setup.model, compact, %{
      endpoint: "/backend-api/codex/responses/compact",
      transport: "http_compact_json",
      correlation_id: "dispatch-projection-#{System.unique_integer([:positive])}",
      request_metadata: %{
        "compaction_bridge" => %{"applied" => true, "result_transport" => "buffered"}
      }
    })
  end

  defp request_options(auth, payload, setup) do
    {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)

    %{
      request_id: "dispatch-accounting-#{System.unique_integer([:positive])}",
      upstream_endpoint: @endpoint_path
    }
    |> RequestOptions.build(@endpoint_path, payload)
    |> RequestOptions.put_routing(
      requested_model: setup.model.exposed_model_id,
      effective_model: setup.model.exposed_model_id,
      api_key_policy: policy
    )
  end

  defp invalid_request do
    %{
      id: Ecto.UUID.generate(),
      correlation_id: "dispatch-accounting-#{System.unique_integer([:positive])}"
    }
  end
end
