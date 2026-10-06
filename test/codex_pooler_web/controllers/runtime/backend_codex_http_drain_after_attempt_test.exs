defmodule CodexPoolerWeb.Runtime.BackendCodexHttpDrainAfterAttemptTest do
  # A native HTTP turn stopped by a rollout drain at a candidate's drain
  # checkpoint (`Dispatch.drain_checkpoint/1`) is told to retry, and the client
  # resends the turn on another pod. When an earlier candidate of the request
  # already failed before any output, the release follows that attempt: by the
  # findings#221 rule it carries the attempt, no pre-attempt phase and unknown
  # usage, and the pre-attempt release metric does not count it (findings#321
  # row 321-2). Only the writer changed: the native HTTP claim walk, which
  # never reads the release, still steps over the drained request and serves
  # the resend.
  #
  # One BEAM node, two accounts of one Pool with deterministic rotation,
  # FakeUpstream, a test-scoped deferred stream registry wired in through the
  # application env (as `backend_codex_http_drain_resend_test.exs` does).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, deterministic_rotation_seed: 2, native_text_input: 1, stream_retry_setup: 2, stream_success_sse: 0]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, PreAttemptRelease, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @turn_id "turn_321_drain_after_attempt"
  @budget 15_000

  setup do
    stream_registry = :"deferred-stream-registry-#{System.unique_integer([:positive])}"
    start_supervised!({DeferredStreamRegistry, name: stream_registry})
    previous = Application.get_env(:codex_pooler, DeferredStreamRegistry)
    Application.put_env(:codex_pooler, DeferredStreamRegistry, server_name: stream_registry)

    restore = fn ->
      if previous,
        do: Application.put_env(:codex_pooler, DeferredStreamRegistry, previous),
        else: Application.delete_env(:codex_pooler, DeferredStreamRegistry)
    end

    on_exit(restore)
    {:ok, stream_registry: stream_registry, leave_drained_pod: restore}
  end

  # The first candidate's 503 is held while the drain starts, so the turn
  # reaches the second candidate's drain checkpoint after the first attempt.
  test "a drain before the next candidate after a retryable failure releases after that attempt, and the resend is served", %{conn: conn, stream_registry: stream_registry, leave_drained_pod: leave_drained_pod} do
    ref = make_ref()
    refusal = FakeUpstream.barrier_json_response(%{"error" => %{"code" => "server_error", "message" => "synthetic pre-output refusal"}}, status: 503, notify: self(), release_ref: ref)
    {setup, first_upstream, second_upstream} = stream_retry_setup(FakeUpstream.strict_sequence([refusal, stream_success_sse()]), stream_success_sse())
    order_by_request_id!(setup)
    session = "codex-session-drain-after-attempt-#{System.unique_integer([:positive])}"
    parent = self()

    request =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())
        post_turn(conn, setup, session)
      end)

    assert_receive {:fake_upstream_timeout_barrier, :before_headers, provider, ^ref}, @budget
    {_epoch, _entries} = DeferredStreamRegistry.begin_drain(name: stream_registry)
    releases = attach_pre_attempt_release_telemetry!()
    send(provider, {:fake_upstream_release_timeout, ref})
    drained = Task.await(request, @budget)

    assert drained.status == 503
    assert %{"error" => %{"code" => "owner_drained"}} = CodexPooler.JSON.decode!(drained.resp_body)
    assert {FakeUpstream.count(first_upstream), FakeUpstream.count(second_upstream)} == {1, 0}
    assert [%Request{status: "failed", response_status_code: 499, last_error_code: "owner_drained", usage_status: "usage_unknown"} = predecessor] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [%Attempt{id: attempt_id, status: "retryable_failed", pool_upstream_assignment_id: assignment_id}] = Repo.all(from(a in Attempt, where: a.request_id == ^predecessor.id))
    assert assignment_id == setup.assignment.id
    assert [{"release", ^attempt_id, "usage_unknown", nil}, {"reservation", nil, _usage, nil}] = ledger_entries(predecessor)
    refute_received {^releases, _measurements, %{phase: "turn_interrupted"}}
    assert %CodexTurn{first_visible_output_at: nil} = Repo.one!(from(t in CodexTurn, where: t.request_id == ^predecessor.id))

    # The client retries on a pod that is not draining; the native HTTP claim
    # walk steps over the drained request, which delivered nothing.
    leave_drained_pod.()
    resend = post_turn(conn, setup, session)
    assert resend.status == 200
    assert [%Request{status: "succeeded"} = served] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^predecessor.id))
    assert served.correlation_id != predecessor.correlation_id
    refute Map.has_key?(served.request_metadata, "client_resend")
    refute Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id or l.successor_request_id == ^served.id))
  end

  # The control: a drain that stops the first candidate, before any attempt,
  # still writes the pre-attempt shape and counts it. The request is held after
  # admission and before its reservation while the drain starts.
  test "a drain before the first candidate still releases before any attempt", %{conn: conn, stream_registry: stream_registry} do
    {setup, first_upstream, second_upstream} = stream_retry_setup(stream_success_sse(), stream_success_sse())
    order_by_request_id!(setup)
    session = "codex-session-drain-first-candidate-#{System.unique_integer([:positive])}"
    ref = make_ref()
    parent = self()

    request =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())
        Process.put({Service, :runtime_authorization_barrier}, {parent, ref, {:reservation_lock, :before}})
        post_turn(conn, setup, session)
      end)

    assert_receive {:runtime_authorization_barrier, ^ref, :reservation_lock, :before, request_pid}, @budget
    {_epoch, _entries} = DeferredStreamRegistry.begin_drain(name: stream_registry)
    releases = attach_pre_attempt_release_telemetry!()
    send(request_pid, {:runtime_authorization_release, ref})
    drained = Task.await(request, @budget)

    assert drained.status == 503
    assert %{"error" => %{"code" => "owner_drained"}} = CodexPooler.JSON.decode!(drained.resp_body)
    assert {FakeUpstream.count(first_upstream), FakeUpstream.count(second_upstream)} == {0, 0}
    assert [%Request{status: "failed", response_status_code: 499, last_error_code: "owner_drained", usage_status: "not_applicable"} = row] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    refute Repo.exists?(from(a in Attempt, where: a.request_id == ^row.id))
    assert [{"release", nil, "not_applicable", "turn_interrupted"}, {"reservation", nil, _usage, nil}] = ledger_entries(row)
    assert_received {^releases, %{count: 1}, %{phase: "turn_interrupted", release_reason: "owner_drained"}}
  end

  # A Codex session would key the rotation on its random id; without session
  # stickiness the rotation keys on the request id, so the first account is
  # tried first (`deterministic_rotation_seed(2, 0)`).
  defp order_by_request_id!(setup) do
    setup.pool
    |> CodexPooler.Pools.ensure_routing_settings()
    |> Ecto.Changeset.change(%{sticky_websocket_sessions: false, updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)})
    |> Repo.update!()
  end

  defp post_turn(conn, setup, session) do
    conn
    |> Phoenix.ConnTest.recycle()
    |> auth(setup)
    |> put_req_header("session-id", session)
    |> put_req_header("x-request-id", deterministic_rotation_seed(2, 0))
    |> post(@path, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("drain after attempt turn"), "stream" => true, "client_metadata" => %{"session_id" => "client-metadata-session", "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"turn_id" => @turn_id, "request_kind" => "turn"})}})
  end

  # The marker is emitted in the request's process after its commit; this
  # module is synchronous, so no other test emits one meanwhile.
  defp attach_pre_attempt_release_telemetry! do
    handler_id = {__MODULE__, System.unique_integer([:positive])}
    test_pid = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)
    :ok = :telemetry.attach(handler_id, PreAttemptRelease.telemetry_event(), fn _event, measurements, metadata, _config -> send(test_pid, {handler_id, measurements, metadata}) end, nil)
    handler_id
  end

  defp ledger_entries(request),
    do: Enum.sort(Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id, select: {l.entry_kind, l.attempt_id, l.usage_status, fragment("?->>'pre_attempt_phase'", l.details)})))
end
