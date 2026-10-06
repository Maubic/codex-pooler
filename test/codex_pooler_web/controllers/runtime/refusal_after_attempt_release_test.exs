defmodule CodexPoolerWeb.Runtime.RefusalAfterAttemptReleaseTest do
  # A request whose first candidate failed retryably (an attempt recorded,
  # nothing generated) moves to the next candidate, and a routing refusal of
  # that candidate ends the request. The reservation release that refusal
  # writes follows an attempt, so by the findings#221 rule it carries that
  # attempt and no pre-attempt phase, and the pre-attempt release metric does
  # not count it (findings#321).
  #
  # One BEAM node, two accounts of one Pool with deterministic rotation,
  # FakeUpstream, native HTTP SSE.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [native_text_input: 1, stream_retry_setup: 2, stream_success_sse: 0, deterministic_rotation_seed: 2, register_unboxed_pool_cleanup!: 1, start_public_endpoint!: 0]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @turn_endpoint "/backend-api/codex/responses"
  @budget 15_000

  # The second candidate's circuit opens while the first candidate's response is
  # held, so route filtering admitted it for the request and refuses it when the
  # remaining cohort is filtered again after the first candidate's failure.
  test "a refusal of the remaining cohort after a retryable failure releases after that attempt" do
    ref = make_ref()
    refusal = FakeUpstream.barrier_json_response(%{"error" => %{"code" => "server_error", "message" => "synthetic pre-output refusal"}}, status: 503, notify: self(), release_ref: ref)
    {setup, first_upstream, second_upstream} = stream_retry_setup(refusal, stream_success_sse())
    parent = self()

    request =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())

        Phoenix.ConnTest.build_conn()
        |> Plug.Conn.put_req_header("authorization", setup.authorization)
        |> Plug.Conn.put_req_header("content-type", "application/json")
        |> Plug.Conn.put_req_header("x-request-id", deterministic_rotation_seed(2, 0))
        |> post(@turn_endpoint, CodexPooler.JSON.encode!(%{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic refusal after attempt"), "stream" => true}))
      end)

    assert_receive {:fake_upstream_timeout_barrier, :before_headers, provider, ^ref}, @budget
    open_circuit!(setup, setup.fallback_assignment, setup.fallback_identity, "proxy_stream")
    releases = attach_pre_attempt_release_telemetry!()
    send(provider, {:fake_upstream_release_timeout, ref})
    conn = Task.await(request, @budget)

    assert conn.status == 503
    assert %{"error" => %{"code" => "no_eligible_backend"}} = CodexPooler.JSON.decode!(conn.resp_body)
    assert {FakeUpstream.count(first_upstream), FakeUpstream.count(second_upstream)} == {1, 0}
    assert [%Request{status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend"} = row] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [%Attempt{id: attempt_id, status: "retryable_failed", pool_upstream_assignment_id: assignment_id}] = Repo.all(from(a in Attempt, where: a.request_id == ^row.id))
    assert assignment_id == setup.assignment.id
    assert [{"release", ^attempt_id, nil}, {"reservation", nil, nil}] = ledger_entries(row)
    refute_received {^releases, _measurements, %{phase: "routing_rejected"}}
  end

  # The second candidate passes the remaining cohort's filtering as a half-open
  # circuit's probe, and its probe slot is taken before its attempt claims it:
  # the circuit refuses the candidate at dispatch, the last of the request.
  test "a circuit refusal of the next candidate after a retryable failure releases after that attempt", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    ref = make_ref()
    refusal = FakeUpstream.barrier_json_response(%{"error" => %{"code" => "server_error", "message" => "synthetic pre-output refusal"}}, status: 503, notify: self(), release_ref: ref)
    {setup, first_upstream, second_upstream} = stream_retry_setup(refusal, stream_success_sse())
    register_unboxed_pool_cleanup!(setup)
    circuit = half_open_circuit!(setup, setup.fallback_assignment, setup.fallback_identity, "proxy_stream")
    port = start_public_endpoint!()
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"x-request-id", deterministic_rotation_seed(2, 0)}]
    request = Task.async(fn -> Req.post!("http://127.0.0.1:#{port}#{@turn_endpoint}", headers: headers, json: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic circuit refusal after attempt"), "stream" => true}, retry: false, receive_timeout: @budget, decode_body: false) end)
    assert_receive {:fake_upstream_timeout_barrier, :before_headers, provider, ^ref}, @budget
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query!("SELECT id FROM routing_circuit_states WHERE id = $1 FOR UPDATE", [Ecto.UUID.dump!(circuit.id)])
          Repo.query!("UPDATE routing_circuit_states SET metadata = $2 WHERE id = $1", [Ecto.UUID.dump!(circuit.id), %{"probe_in_flight_count" => 1}])
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:probe_slot_held, backend})

          receive do
            :release -> :ok
          after
            2 * @budget -> raise "probe slot release missing"
          end
        end)
      end)

    releases = attach_pre_attempt_release_telemetry!()

    try do
      assert_receive {:probe_slot_held, holder_backend}, @budget
      send(provider, {:fake_upstream_release_timeout, ref})
      _attempt_claim = await_relation_waiter(holder_backend, "routing_circuit_states", System.monotonic_time(:millisecond) + @budget)
      send(holder.pid, :release)
      assert {:ok, :ok} = Task.await(holder, @budget)
      response = Task.await(request, @budget)
      assert response.status == 503
      assert %{"error" => %{"code" => "no_eligible_backend"}} = CodexPooler.JSON.decode!(response.body)
    after
      send(holder.pid, :release)
      Task.shutdown(holder, :brutal_kill)
      Task.shutdown(request, :brutal_kill)
    end

    assert {FakeUpstream.count(first_upstream), FakeUpstream.count(second_upstream)} == {1, 0}
    assert [%Request{status: "failed", response_status_code: 503, last_error_code: "no_eligible_backend"} = row] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [%Attempt{id: attempt_id, status: "retryable_failed", pool_upstream_assignment_id: assignment_id}] = Repo.all(from(a in Attempt, where: a.request_id == ^row.id))
    assert assignment_id == setup.assignment.id
    assert [{"release", ^attempt_id, nil}, {"reservation", nil, nil}] = ledger_entries(row)
    refute_received {^releases, _measurements, %{phase: "routing_rejected"}}
  end

  defp half_open_circuit!(setup, assignment, identity, route_class) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%RoutingCircuitState{pool_id: setup.pool.id, pool_upstream_assignment_id: assignment.id, upstream_identity_id: identity.id, model_identifier: setup.model.exposed_model_id, route_class: route_class, status: "half_open", reason_code: "upstream_network_error", failure_count: 3, success_count: 0, opened_at: DateTime.add(now, -60, :second), half_opened_at: now, metadata: %{"probe_in_flight_count" => 0}, created_at: now, updated_at: now})
  end

  defp await_relation_waiter(holder, relation, deadline) do
    %{rows: rows} = Repo.query!("SELECT DISTINCT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid = a.pid WHERE $1 = ANY(pg_blocking_pids(a.pid)) AND l.relation = $2::text::regclass", [holder, relation])

    cond do
      length(rows) == 1 ->
        hd(hd(rows))

      System.monotonic_time(:millisecond) > deadline ->
        flunk("expected PostgreSQL relation waiter missing")

      true ->
        Process.sleep(10)
        await_relation_waiter(holder, relation, deadline)
    end
  end

  defp open_circuit!(setup, assignment, identity, route_class) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%RoutingCircuitState{pool_id: setup.pool.id, pool_upstream_assignment_id: assignment.id, upstream_identity_id: identity.id, model_identifier: setup.model.exposed_model_id, route_class: route_class, status: "open", reason_code: "upstream_network_error", failure_count: 3, success_count: 0, opened_at: now, next_probe_at: DateTime.add(now, 30, :second), metadata: %{"probe_in_flight_count" => 0}, created_at: now, updated_at: now})
  end

  # The marker the request's own release emits runs in the request's process; this module is synchronous, so no other test emits one meanwhile.
  defp attach_pre_attempt_release_telemetry! do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, PreAttemptRelease.telemetry_event(), fn _event, measurements, metadata, _config -> send(test_pid, {handler_id, measurements, metadata}) end, nil)
    handler_id
  end

  defp ledger_entries(request),
    do: Enum.sort(Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id, select: {l.entry_kind, l.attempt_id, fragment("?->>'pre_attempt_phase'", l.details)})))
end
