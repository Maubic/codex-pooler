defmodule CodexPooler.Gateway.Runtime.PartitionFallbackRefusalReleaseTest do
  # The after-refusal hop to a held-back canonical partition runs after the
  # selected partition's candidate refused with a usage limit before any
  # output, so its attempt is already recorded. When route filtering then
  # refuses the held-back candidates (their state changed after
  # `PartitionFallback.available?/1` read them), the request ends on that
  # refusal, and its reservation release follows that attempt: by the
  # findings#221 rule it carries the attempt and no pre-attempt phase
  # (findings#321). The hop itself is driven as dispatch drives it, with the
  # selected candidate's attempt recorded the way its refusal records it.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, gateway_upstream: 4, prime_routing_quota!: 1, put_model_source_assignments!: 2, start_upstream: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, PartitionFallback, RouteState}
  alias CodexPooler.Repo

  @endpoint_path "/backend-api/codex/responses"

  test "a held-back partition refused after the selected candidate's attempt releases after that attempt" do
    setup = gateway_setup(start_upstream(FakeUpstream.json_response(%{"data" => []})))
    held_back = gateway_upstream(setup.pool, start_upstream(FakeUpstream.json_response(%{"data" => []})), "upstream-token-held-back", compact?: false)
    prime_routing_quota!(held_back.identity)
    model = put_model_source_assignments!(setup.model, [setup.assignment, held_back.assignment])
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = %{"model" => model.exposed_model_id, "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic held-back refusal"}]}], "stream" => true}
    assert {:ok, reserved} = Accounting.reserve(auth, model, payload, %{endpoint: @endpoint_path, transport: "http_sse", correlation_id: "held-back-refusal-#{System.unique_integer([:positive])}"})
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment, %{model: model})
    assert {:ok, %Attempt{status: "retryable_failed"} = attempt} = Accounting.record_retryable_attempt_failure(attempt, %{last_error_code: "usage_limit_reached", response_status_code: 429})
    open_circuit!(setup, model, held_back.assignment, held_back.identity)
    selected = [{setup.assignment, setup.identity}]
    route_state = %{visible_model: model, candidates: selected} |> RouteState.new() |> RouteState.put_partition_fallback([{held_back.assignment, held_back.identity}])
    {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)

    request_options =
      %{request_id: "held-back-refusal-#{System.unique_integer([:positive])}", upstream_endpoint: @endpoint_path}
      |> RequestOptions.build(@endpoint_path, payload)
      |> RequestOptions.put_routing(requested_model: model.exposed_model_id, effective_model: model.exposed_model_id, api_key_policy: policy)

    assert {:ok, context} = Context.new(%{auth: auth, endpoint: @endpoint_path, payload: payload, model: model, reserved: reserved, candidates: selected, request_options: request_options, route_state: route_state})

    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, PreAttemptRelease.telemetry_event(), fn _event, measurements, metadata, _config -> send(test_pid, {handler_id, measurements, metadata}) end, nil)
    assert {:error, %{status: 503, code: "no_eligible_backend"}} = PartitionFallback.context(context)
    assert %Request{status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend"} = Repo.reload!(reserved.request)
    attempt_id = attempt.id
    assert [{"release", ^attempt_id, nil}, {"reservation", nil, nil}] = ledger_entries(reserved.request)
    refute_received {^handler_id, _measurements, %{phase: "routing_rejected"}}
  end

  defp open_circuit!(setup, model, assignment, identity) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%RoutingCircuitState{pool_id: setup.pool.id, pool_upstream_assignment_id: assignment.id, upstream_identity_id: identity.id, model_identifier: model.exposed_model_id, route_class: "proxy_stream", status: "open", reason_code: "upstream_network_error", failure_count: 3, success_count: 0, opened_at: now, next_probe_at: DateTime.add(now, 30, :second), metadata: %{"probe_in_flight_count" => 0}, created_at: now, updated_at: now})
  end

  defp ledger_entries(request),
    do: Enum.sort(Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id, select: {l.entry_kind, l.attempt_id, fragment("?->>'pre_attempt_phase'", l.details)})))
end
