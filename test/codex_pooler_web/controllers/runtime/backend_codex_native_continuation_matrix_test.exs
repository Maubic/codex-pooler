defmodule CodexPoolerWeb.Runtime.BackendCodexNativeContinuationMatrixTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_send_text!: 4]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, receive_native_terminal!: 3]
  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink, RequestReplayEntitlement}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.{NativeTurnContinuation, RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, BridgeSessionAlias, CodexSession, CodexTurn, RuntimeCleanup, SessionContinuity}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.Aliases
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, as: OwnerSupport
  alias CodexPoolerWeb.Runtime.{MailboxLeaseLifecycleSupport, NativeContinuationMatrix, WebsocketCleanupFence}
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @budget 15_000
  @path "/backend-api/codex/responses"

  setup_all do
    if dir = System.get_env("NATIVE_CONTINUATION_MATRIX_EVIDENCE") do
      if ExUnit.configuration()[:include] == [] do
        on_exit(fn ->
          files = Path.wildcard(Path.join(dir, "*.json"))
          results = Enum.map(files, &NativeContinuationMatrix.read_receipt!/1)
          assert :ok = NativeContinuationMatrix.validate_census!(results)
          parent = Path.dirname(dir)
          File.write!(Path.join(parent, "task-8-matrix-manifest.json"), CodexPooler.JSON.encode!(NativeContinuationMatrix.cells()))
          File.write!(Path.join(parent, "task-8-cell-results.json"), CodexPooler.JSON.encode!(Enum.map(files, &(File.read!(&1) |> CodexPooler.JSON.decode!()))))
          File.write!(Path.join(parent, "task-8-negatives.json"), CodexPooler.JSON.encode!(Enum.map(Path.wildcard(Path.join([dir, "negatives", "*.json"])), &(File.read!(&1) |> CodexPooler.JSON.decode!()))))
        end)
      end
    end

    :ok
  end

  setup context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    :ok
  end

  for cell <- NativeContinuationMatrix.cells() do
    @tag matrix_cell: cell
    @tag matrix_path: cell.path, matrix_state: cell.state
    if {cell.role, cell.path, cell.mode, cell.state} in [{:opening, :http_sse, :full, :same}, {:steered_continuation, :http_sse, :full, :replacement_engaged}, {:tool_continuation, :websocket_owner_remote, :full, :replacement_fresh}, {:post_compaction_resume, :websocket_owner_local, :lite, :replacement_engaged}, {:opening, :websocket_to_http_fallback, :full, :replacement_fresh}], do: @tag(matrix_oracle_review: true)
    if cell.role == :tool_continuation and cell.state != :same, do: @tag(task8_baseline_tool_replacement: true)
    if cell.state != :same, do: @tag(slow: "observes a real one-second unchanged PostgreSQL lease deadline crossing")

    test "synthetic native continuation #{cell.id}", %{matrix_cell: cell} do
      run_cell!(cell)
    end
  end

  @tag native_continuation_matrix_negative: true
  test "omitted id, wrong path and unjustified N/A fail exact manifest assertions" do
    results = Enum.map(NativeContinuationMatrix.cells(), &synthetic_contract_result/1)
    assert :ok = NativeContinuationMatrix.validate_census!(results)
    assert_raise ArgumentError, "matrix census: expected exactly 120 unique declared ids", fn -> NativeContinuationMatrix.validate_census!(tl(results)) end
    assert_raise ArgumentError, "matrix topology: wrong path label", fn -> NativeContinuationMatrix.validate_census!([%{hd(results) | path: :websocket_direct} | tl(results)]) end
    assert_raise ArgumentError, "matrix census: no structural N/A is declared", fn -> NativeContinuationMatrix.validate_census!([%{hd(results) | outcome: :not_applicable} | tl(results)]) end
    assert_raise ArgumentError, "matrix census: observed outcome does not satisfy cell contract", fn -> NativeContinuationMatrix.validate_census!([%{hd(results) | outcome: :refusal} | tl(results)]) end
  end

  @tag native_continuation_matrix_negative: true
  @tag matrix_work_set_control: true
  test "an extra fully accounted request and physical send fail the exact work oracle" do
    result = synthetic_contract_result(hd(NativeContinuationMatrix.cells()))
    observed = result.work_observation
    extra_ledger = for kind <- ["reservation", "release", "settlement"], do: %{request_id: "synthetic_extra", kind: kind, count: 1}
    extra = %{observed | request_ids: observed.request_ids ++ ["synthetic_extra"], attempt_request_ids: observed.attempt_request_ids ++ ["synthetic_extra"], dispatch_count: observed.dispatch_count + 1, ledger: observed.ledger ++ extra_ledger}
    assert extra.dispatch_count == length(extra.request_ids)
    assert_raise ArgumentError, "matrix work: unexpected request identities", fn -> NativeContinuationMatrix.assert_work_set!(result, result.role_request_ids, extra) end
    assert_raise ArgumentError, "matrix work: unexpected physical dispatch count", fn -> NativeContinuationMatrix.assert_work_set!(result, result.role_request_ids, %{observed | dispatch_count: 3}) end
    assert_raise ArgumentError, "matrix work: unexpected retry edges", fn -> NativeContinuationMatrix.assert_work_set!(result, result.role_request_ids, %{observed | retry_edges: []}) end
    write_negative!(:extra_accounted_work_control, %{extra_request_accounted: true, extra_physical_dispatch: true, oracle_rejected: true})
  end

  @tag native_continuation_matrix_negative: true
  @tag matrix_work_set_control: true
  test "replacement chronology, canonical authority and false state labels fail independently" do
    cell = Enum.find(NativeContinuationMatrix.cells(), &(&1.state == :replacement_fresh))
    result = synthetic_contract_result(cell)
    observation = result.lifecycle_observation
    assert :ok = NativeContinuationMatrix.assert_replacement!(observation)
    assert_raise ArgumentError, "matrix lifecycle: replacement predates close", fn -> NativeContinuationMatrix.assert_replacement!(%{observation | replacement_created_at: DateTime.add(observation.previous_closed_at, -1, :microsecond)}) end
    assert_raise ArgumentError, "matrix lifecycle: canonical key differs", fn -> NativeContinuationMatrix.assert_replacement!(%{observation | same_canonical_key: false}) end
    assert_raise ArgumentError, "matrix lifecycle: replacement scope differs", fn -> NativeContinuationMatrix.assert_replacement!(%{observation | same_api_key: false}) end
    results = Enum.map(NativeContinuationMatrix.cells(), &synthetic_contract_result/1)
    false_label = Enum.map(results, fn row -> if row.id == cell.id, do: %{row | observed_state: :same}, else: row end)
    assert_raise ArgumentError, "matrix lifecycle: wrong observed state", fn -> NativeContinuationMatrix.validate_census!(false_label) end
    write_negative!(:replacement_authority_controls, %{wrong_created_rejected: true, wrong_canonical_key_rejected: true, wrong_scope_rejected: true, false_label_rejected: true})
  end

  defp synthetic_contract_result(cell) do
    roles = %{predecessor: "synthetic_predecessor", successor: "synthetic_successor", opener: if(cell.role == :steered_continuation, do: "synthetic_opener"), preparation: if(cell.state == :replacement_engaged, do: "synthetic_preparation")}
    ids = roles |> Map.values() |> Enum.reject(&is_nil/1)
    edges = if cell.retry_link, do: [%{predecessor: roles.predecessor, successor: roles.successor}], else: []
    work = %{request_ids: ids, attempt_request_ids: ids, dispatch_count: length(ids), retry_edges: edges, ledger: for(id <- ids, kind <- ["reservation", "release", "settlement"], do: %{request_id: id, kind: kind, count: 1})}
    chronology = %{previous_closed_at: ~U[2026-01-01 00:00:02.000000Z], replacement_created_at: ~U[2026-01-01 00:00:03.000000Z], previous_owner_deadline: ~U[2026-01-01 00:00:01.000000Z], close_reason: "owner_lease_expired", same_pool: true, same_api_key: true, same_canonical_key: true, genuine_expiry: true, unchanged_deadlines: true}
    Map.merge(cell, %{observed_role: cell.role, observed_state: cell.state, observed_mode: cell.mode, observed_claim_domain: cell.claim_domain, role_request_ids: roles, work_observation: work, lifecycle_observation: chronology})
  end

  for mutation <- [:other_principal, :epoch, :model, :live_request, :live_attempt, :entitlement, :expired_window, :changed_prefix, :skipped_prefix, :reordered_prefix, :partial_tool, :cross_receipt, :cross_transport, :stale_anchor, :different_canonical_session] do
    @tag native_continuation_matrix_negative: true
    @tag negative: mutation
    test "synthetic HTTP mailbox #{mutation} refuses with zero dispatch", %{negative: mutation} do
      {setup, upstream, port, thread, original, outputs} = http_scenario!(3)
      predecessor = send_turn!(if(mutation == :entitlement, do: :websocket_direct, else: :http_sse), port, setup, original, thread)
      continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
      continuation = mutate_negative!(mutation, setup, predecessor, continuation, outputs)
      before = pool_counts(setup)
      target_thread = if mutation == :different_canonical_session, do: thread <> "-different", else: thread
      {status, _body} = http!(port, setup, continuation, target_thread)
      assert status in [400, 409]
      assert FakeUpstream.count(upstream) == 1
      assert pool_counts(setup) == before
      write_negative!(mutation, %{outcome: :refusal, status: status, additional_dispatches: 0, additional_requests: 0, additional_ledger: 0})
    end
  end

  for mutation <- [:expiry_certificate_tampering, :legacy_close, :earlier_replacement, :canonical_key_mismatch] do
    @tag native_continuation_matrix_negative: true
    @tag authority_negative: mutation
    @tag slow: "observes real one-second unchanged PostgreSQL lease expiry before authority mutation"
    test "replacement authority #{mutation} refuses with zero dispatch", %{authority_negative: mutation} do
      {setup, upstream, port, thread, original, outputs} = http_scenario!(1)
      predecessor = send_turn!(:http_sse, port, setup, original, thread)
      previous = Repo.get_by!(CodexTurn, request_id: predecessor.id).codex_session_id
      cell = Enum.find(NativeContinuationMatrix.cells(), &(&1.role == :opening and &1.path == :http_sse and &1.mode == :full and &1.state == :replacement_fresh))
      {replacement, lifecycle} = establish_state!(cell, setup, previous, original, thread, port, nil)
      mutate_authority!(mutation, setup, previous, replacement)
      before = pool_counts(setup)
      continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
      assert {409, _} = http!(port, setup, continuation, thread)
      assert FakeUpstream.count(upstream) == 1
      assert pool_counts(setup) == before
      write_negative!(mutation, %{outcome: :refusal, additional_dispatches: 0, additional_ledger: 0, lifecycle_observation: lifecycle})
    end
  end

  @tag native_continuation_matrix_negative: true
  @tag closed_replacement_route: true
  @tag slow: "observes genuine one-second expiry and distinguishes a closed candidate from a later fresh controller replacement"
  test "a closed C refuses direct authority while the controller independently creates a valid fresh D" do
    {setup, upstream, port, thread, original, outputs} = http_scenario!(1)
    predecessor = send_turn!(:http_sse, port, setup, original, thread)
    previous = Repo.get_by!(CodexTurn, request_id: predecessor.id).codex_session_id
    cell = Enum.find(NativeContinuationMatrix.cells(), &(&1.role == :opening and &1.path == :http_sse and &1.mode == :full and &1.state == :replacement_fresh))
    {closed_candidate, lifecycle} = establish_state!(cell, setup, previous, original, thread, port, nil)
    mutate_authority!(:closed_replacement, setup, previous, closed_candidate)
    scope = %{pool_id: setup.pool.id, api_key_id: setup.api_key.id}
    assert {:ok, :rejected} = SessionContinuity.mailbox_admission_transaction(fn -> [previous, closed_candidate] end, fn -> SessionContinuity.mailbox_session_verdict(previous, closed_candidate, scope) end, :unexpected_rediscovery)
    assert FakeUpstream.count(upstream) == 1
    continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
    successor = send_turn!(:http_sse, port, setup, continuation, thread)
    current = Repo.get_by!(CodexTurn, request_id: successor.id).codex_session_id
    assert length(Enum.uniq([previous, closed_candidate, current])) == 3
    assert %{status: "closed"} = Repo.get!(CodexSession, closed_candidate)
    assert %{status: "active"} = Repo.get!(CodexSession, current)
    assert :ok = NativeContinuationMatrix.assert_replacement!(Map.merge(lifecycle, observe_replacement_authority!(setup, previous, current)))
    assert_ancestry!(true, predecessor, successor)
    roles = %{predecessor: predecessor.id, successor: successor.id}
    assert :ok = NativeContinuationMatrix.assert_work_set!(cell, roles, observe_work!(setup, requests(setup), upstream))
    Enum.each(requests(setup), &request_ledger_receipt!/1)
    write_negative!(:closed_candidate_fresh_controller_replacement, %{direct_closed_candidate: :refusal, controller_fresh_replacement: :admission, three_distinct_sessions: true, physical_dispatches: 2, exact_work_verified: true})
  end

  @tag native_continuation_matrix_negative: true
  @tag matrix_work_set_control: true
  test "the actual idle retirement writer cannot establish expiry replacement authority" do
    {setup, upstream, port, thread, original, outputs} = http_scenario!(1)
    predecessor = send_turn!(:http_sse, port, setup, original, thread)
    session = Repo.get!(CodexSession, Repo.get_by!(CodexTurn, request_id: predecessor.id).codex_session_id)
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()")
    ttl = OperationalSettings.current().expired_alias_ttl_seconds
    Repo.delete_all(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session.id))
    Repo.delete_all(from(a in BridgeSessionAlias, where: a.codex_session_id == ^session.id))
    Repo.update_all(from(s in CodexSession, where: s.id == ^session.id), set: [owner_lease_expires_at: DateTime.add(now, -ttl - 1, :second)])
    assert {:ok, %{closed_retired_sessions: 1}} = RuntimeCleanup.cleanup_expired(now)
    assert %{status: "closed", close_reason: nil} = Repo.get!(CodexSession, session.id)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, replacement} = SessionContinuity.start_codex_session(auth, RequestOptions.for_websocket(%{session_key: session.session_key}))
    assert replacement.id != session.id
    assert observe_replacement_authority!(setup, session.id, replacement.id).same_canonical_key
    continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
    before = pool_counts(setup)
    assert {409, _} = http!(port, setup, continuation, thread)
    assert FakeUpstream.count(upstream) == 1
    assert pool_counts(setup) == before
    write_negative!(:actual_nonexpiry_close, %{outcome: :refusal, writer: "retired_session_cleanup", seeded_retirement: true, actual_expiry_observed: false, additional_dispatches: 0, additional_ledger: 0})
  end

  @tag native_continuation_matrix_negative: true
  @tag slow: "observes real expiry and holds a distinct replacement request at the fake upstream terminal boundary"
  test "active unrelated replacement generation fences the historical mailbox" do
    {setup, upstream, port, thread, original, outputs} = http_scenario!(1)
    predecessor = send_turn!(:http_sse, port, setup, original, thread)
    previous = Repo.get_by!(CodexTurn, request_id: predecessor.id).codex_session_id
    cell = Enum.find(NativeContinuationMatrix.cells(), &(&1.role == :opening and &1.path == :http_sse and &1.mode == :full and &1.state == :replacement_fresh))
    {replacement, lifecycle} = establish_state!(cell, setup, previous, original, thread, port, nil)
    ref = make_ref()
    control_mode = FakeUpstream.barrier_sse_stream([{"response.created", %{"type" => "response.created", "response" => %{"id" => "resp_synthetic_control"}}}, {"response.completed", completed()}], notify: self(), release_ref: ref, barrier_after: 1)
    :ok = FakeUpstream.set_mode(upstream, control_mode)
    control_payload = independent_payload(original)
    supervisor = start_supervised!(Task.Supervisor)
    control = Task.Supervisor.async_nolink(supervisor, fn -> http!(port, setup, control_payload, thread, :supervised_task) end)
    monitor = Process.monitor(control.pid)
    assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^ref}, @budget
    on_exit(fn -> send(handler, {:fake_upstream_release_chunk, ref}) end)
    active = List.last(requests(setup))
    assert active.status == "in_progress"
    assert Repo.get_by!(CodexTurn, request_id: active.id).codex_session_id == replacement
    assert FakeUpstream.count(upstream) == 2
    before = pool_counts(setup)
    continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
    assert {409, _} = http!(port, setup, continuation, thread)
    assert FakeUpstream.count(upstream) == 2
    assert pool_counts(setup) == before
    send(handler, {:fake_upstream_release_chunk, ref})
    assert {200, _} = Task.await(control, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    assert latest!(setup).status == "succeeded"
    write_negative!(:active_unrelated_turn, %{outcome: :refusal, additional_dispatches: 0, preparation_dispatched: true, lifecycle_observation: lifecycle})
  end

  @tag native_continuation_matrix_negative: true
  test "historical ending mismatch cannot dispatch a later mailbox run" do
    {setup, upstream, port, thread, original, [output]} = http_scenario!(1)
    predecessor = send_turn!(:http_sse, port, setup, original, thread)
    first_mailbox = Map.update!(original, "input", &(&1 ++ [output, mailbox()]))
    successor = send_turn!(:http_sse, port, setup, first_mailbox, thread)
    assert get_in(successor.request_metadata, ["client_resend", "predecessor_request_id"]) == predecessor.id
    Repo.update!(Ecto.Changeset.change(successor, native_client_retry_digest: <<7::256>>))
    next_mailbox = Map.update!(first_mailbox, "input", &(&1 ++ [output, mailbox()]))
    before = pool_counts(setup)
    assert {409, _} = http!(port, setup, next_mailbox, thread)
    assert FakeUpstream.count(upstream) == 2
    assert pool_counts(setup) == before
    write_negative!(:historic_ending, %{outcome: :refusal, dispatches_before: 2, additional_dispatches: 0, additional_ledger: 0})
  end

  @tag native_continuation_matrix_negative: true
  test "a rotated header alias resolving the same canonical session preserves mailbox authority" do
    {setup, upstream, port, thread, original, outputs} = http_scenario!(1)
    predecessor = send_turn!(:http_sse, port, setup, original, thread)
    session = Repo.get!(CodexSession, Repo.get_by!(CodexTurn, request_id: predecessor.id).codex_session_id)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    rotated = thread <> "-rotated"
    assert :ok = Aliases.register_session_header_hash(session, auth, :crypto.hash(:sha256, "#{rotated}:0"))
    continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
    assert {200, _} = http!(port, setup, continuation, rotated, :test, :omit)
    successor = latest!(setup)
    assert Repo.get_by!(CodexTurn, request_id: successor.id).codex_session_id == session.id
    assert_ancestry!(true, predecessor, successor)
    assert FakeUpstream.count(upstream) == 2
    request_ledger_receipt!(predecessor)
    request_ledger_receipt!(successor)
    write_negative!(:same_canonical_alias, %{outcome: :admission, distinct_header: true, same_canonical_session: true, additional_dispatches: 1})
  end

  for count <- [3, 4, 5] do
    @tag native_continuation_matrix_negative: true
    @tag prefix_count: count
    test "completed output prefix boundary #{count}", %{prefix_count: count} do
      {setup, upstream, port, thread, original, outputs} = http_scenario!(count)
      predecessor = send_turn!(:http_sse, port, setup, original, thread)
      continuation = Map.update!(original, "input", &(&1 ++ outputs ++ [mailbox()]))
      before = pool_counts(setup)
      {status, _body} = http!(port, setup, continuation, thread)
      expected = if count <= 4, do: 200, else: 409
      assert status == expected

      if count <= 4 do
        successor = latest!(setup)
        assert get_in(successor.request_metadata, ["client_resend", "predecessor_request_id"]) == predecessor.id
        assert FakeUpstream.count(upstream) == 2
        Enum.each(requests(setup), &request_ledger_receipt!/1)
      else
        assert FakeUpstream.count(upstream) == 1
        assert pool_counts(setup) == before
      end

      write_negative!("prefix_#{count}", %{status: status, additional_dispatches: if(count <= 4, do: 1, else: 0), bound: 4})
    end
  end

  for runs <- [15, 16, 17] do
    @tag native_continuation_matrix_negative: true
    @tag mailbox_runs: runs
    @tag slow: "executes and settles fifteen to sixteen real controller/SSE ancestry links to test the fixed sixteen-run bound"
    test "mailbox run and ancestry depth boundary #{runs}", %{mailbox_runs: runs} do
      {setup, upstream, port, thread, original, [output]} = http_scenario!(1)
      send_turn!(:http_sse, port, setup, original, thread)

      Enum.reduce(1..runs, original, fn index, body ->
        advanced = Map.update!(body, "input", &(&1 ++ [output, mailbox()]))
        previous = latest!(setup)
        before = pool_counts(setup)
        {status, _} = http!(port, setup, advanced, thread)
        assert status == if(index <= 16, do: 200, else: 409)

        if index <= 16 do
          successor = latest!(setup)
          assert successor.status == "succeeded"
          assert_ancestry!(true, previous, successor)
          assert FakeUpstream.count(upstream) == index + 1
        else
          assert FakeUpstream.count(upstream) == 17
          assert pool_counts(setup) == before
        end

        advanced
      end)

      Enum.each(requests(setup), &request_ledger_receipt!/1)

      write_negative!("mailbox_runs_#{runs}", %{completed_runs: min(runs, 16), final_status: if(runs <= 16, do: 200, else: 409), dispatches: min(runs, 16) + 1, max_runs: 16, max_prefixes: 4, max_candidate_proofchecks: 64})
    end
  end

  defp http_scenario!(count) do
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    outputs = Enum.map(1..count, &%{"type" => "reasoning", "id" => "rs_synthetic_matrix_#{&1}", "summary" => [], "encrypted_content" => "synthetic_#{&1}"})
    upstream = start_upstream(FakeUpstream.sse_stream(Enum.map(outputs, &{"response.output_item.done", %{"type" => "response.output_item.done", "item" => &1}}) ++ [{"response.completed", completed()}]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    setup = Map.put(setup, :serving_mode, "full")
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    {setup, upstream, port, thread, payload(setup, thread, native_text_input("synthetic"), 0), Enum.map(outputs, &Map.put(&1, "content", nil))}
  end

  defp mutate_negative!(:other_principal, setup, predecessor, payload, _outputs) do
    %{api_key: key} = CodexPooler.PoolerFixtures.active_api_key_fixture(setup.pool)
    Repo.update_all(from(r in Request, where: r.id == ^predecessor.id), set: [api_key_id: key.id])
    payload
  end

  defp mutate_negative!(:epoch, setup, _predecessor, payload, _outputs) do
    Repo.update_all(from(k in CodexPooler.Access.APIKey, where: k.id == ^setup.api_key.id), inc: [runtime_revocation_epoch: 1])
    payload
  end

  defp mutate_negative!(:model, setup, predecessor, payload, _outputs) do
    model = CodexPooler.PoolerFixtures.model_fixture(setup.pool)
    Repo.update_all(from(r in Request, where: r.id == ^predecessor.id), set: [model_id: model.id])
    payload
  end

  defp mutate_negative!(:live_request, _setup, predecessor, payload, _outputs) do
    Repo.update_all(from(r in Request, where: r.id == ^predecessor.id), set: [status: "in_progress", completed_at: nil])
    payload
  end

  defp mutate_negative!(:live_attempt, _setup, predecessor, payload, _outputs) do
    Repo.update_all(from(a in Attempt, where: a.request_id == ^predecessor.id), set: [status: "in_progress", completed_at: nil])
    payload
  end

  defp mutate_negative!(:entitlement, setup, predecessor, payload, _outputs) do
    turn = Repo.get_by!(CodexTurn, request_id: predecessor.id)
    attempt = await_receipt!(predecessor, System.monotonic_time(:millisecond) + @budget)
    now = DateTime.utc_now()

    %RequestReplayEntitlement{}
    |> RequestReplayEntitlement.changeset(%{request_id: predecessor.id, codex_turn_id: turn.id, eligible_attempt_id: attempt.id, api_key_id: setup.api_key.id, api_key_runtime_epoch: setup.api_key.runtime_revocation_epoch, pool_id: setup.pool.id, model_id: setup.model.id, model_identifier: setup.model.exposed_model_id, semantic_turn_digest: turn.semantic_turn_digest || <<1::256>>, replay_claim_digest: predecessor.native_client_retry_digest, replay_generation: 1, owner_lease_digest: <<1::256>>, owner_lease_key_version: "test-v1", predecessor_epoch: 1, status: "armed", armed_at: now, expires_at: DateTime.add(now, 30, :second)})
    |> Repo.insert!()

    payload
  end

  defp mutate_negative!(:expired_window, _setup, predecessor, payload, _outputs) do
    Repo.update_all(from(r in Request, where: r.id == ^predecessor.id), set: [completed_at: DateTime.add(DateTime.utc_now(), -31, :second)])
    payload
  end

  defp mutate_negative!(:changed_prefix, _setup, _predecessor, payload, _outputs), do: put_in(payload, ["input", Elixir.Access.at(1), "encrypted_content"], "synthetic_changed")
  defp mutate_negative!(:skipped_prefix, _setup, _predecessor, payload, outputs), do: Map.put(payload, "input", native_text_input("synthetic") ++ tl(outputs) ++ [mailbox()])
  defp mutate_negative!(:reordered_prefix, _setup, _predecessor, payload, outputs), do: Map.put(payload, "input", native_text_input("synthetic") ++ Enum.reverse(outputs) ++ [mailbox()])
  defp mutate_negative!(:partial_tool, _setup, _predecessor, payload, outputs), do: Map.put(payload, "input", native_text_input("synthetic") ++ outputs ++ [%{"type" => "function_call", "call_id" => "sample_partial", "name" => "sample_tool", "arguments" => "{}"}, mailbox()])

  defp mutate_negative!(:cross_receipt, _setup, predecessor, payload, _outputs) do
    attempt = Repo.get_by!(Attempt, request_id: predecessor.id)
    metadata = Map.drop(attempt.response_metadata, ["native_http_resume_progress", "native_http_mailbox_prefix"])
    Repo.update!(Ecto.Changeset.change(attempt, response_metadata: metadata))
    payload
  end

  defp mutate_negative!(:cross_transport, _setup, predecessor, payload, _outputs) do
    Repo.update_all(from(a in Attempt, where: a.request_id == ^predecessor.id), set: [transport: "websocket"])
    payload
  end

  defp mutate_negative!(:stale_anchor, _setup, _predecessor, payload, _outputs), do: Map.put(payload, "previous_response_id", "resp_synthetic_stale_anchor")
  defp mutate_negative!(:different_canonical_session, _setup, _predecessor, payload, _outputs), do: payload

  defp mutate_authority!(:legacy_close, _setup, previous, _replacement), do: Repo.update_all(from(s in CodexSession, where: s.id == ^previous), set: [close_reason: nil])
  defp mutate_authority!(:expiry_certificate_tampering, _setup, previous, _replacement), do: Repo.update_all(from(s in CodexSession, where: s.id == ^previous), set: [close_reason: "owner_unavailable"])

  defp mutate_authority!(:earlier_replacement, _setup, previous, replacement) do
    closed = Repo.get!(CodexSession, previous)
    Repo.update_all(from(s in CodexSession, where: s.id == ^replacement), set: [created_at: DateTime.add(closed.closed_at, -1, :microsecond)])
  end

  defp mutate_authority!(:closed_replacement, _setup, _previous, replacement), do: Repo.update_all(from(s in CodexSession, where: s.id == ^replacement), set: [status: "closed", closed_at: DateTime.utc_now()])
  defp mutate_authority!(:canonical_key_mismatch, _setup, previous, _replacement), do: Repo.update_all(from(s in CodexSession, where: s.id == ^previous), set: [session_key: "sample-other-canonical-key"])

  defp independent_payload(original) do
    document = CodexPooler.JSON.decode!(original["client_metadata"]["x-codex-turn-metadata"]) |> Map.put("turn_id", "sample_preparation")
    original |> Map.put("input", native_text_input("synthetic independent preparation")) |> put_in(["client_metadata", "x-codex-turn-metadata"], CodexPooler.JSON.encode!(document))
  end

  defp pool_counts(setup) do
    ids = Enum.map(requests(setup), & &1.id)
    %{requests: length(ids), attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in ^ids), :count), ledger: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in ^ids), :count), links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in ^ids), :count)}
  end

  defp write_negative!(id, receipt) do
    if dir = System.get_env("NATIVE_CONTINUATION_MATRIX_EVIDENCE") do
      path = Path.join([dir, "negatives", "#{id}.json"])
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, CodexPooler.JSON.encode!(Map.put(receipt, :id, id)))
    end
  end

  defp run_cell!(cell) do
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, cell.path != :websocket_direct)
    outputs = [%{"type" => "reasoning", "id" => "rs_synthetic_matrix", "summary" => [], "encrypted_content" => "synthetic_matrix"}]
    upstream = start_upstream(FakeUpstream.sse_stream(Enum.map(outputs, &{"response.output_item.done", %{"type" => "response.output_item.done", "item" => &1}}) ++ [{"response.completed", completed()}]))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, Atom.to_string(cell.mode))
    setup = Map.put(setup, :serving_mode, Atom.to_string(cell.mode))
    thread = Ecto.UUID.generate()
    window = if cell.role == :steered_continuation, do: 1, else: 0
    input = role_input(cell.role)
    port = start_public_endpoint!()

    peer =
      if cell.path == :websocket_owner_remote do
        OwnerSupport.ensure_test_distribution_started!()
        OwnerSupport.start_peer_session_owner!(setup, %{accepted_turn_state: thread})
      end

    opener = if cell.role == :steered_continuation, do: send_turn!(:http_sse, port, setup, payload(setup, thread, native_text_input("synthetic"), 0), thread)
    original = payload(setup, thread, input, window)
    predecessor = send_turn!(cell.path, port, setup, original, thread)
    assert predecessor.status == "succeeded"
    assert predecessor.transport == Atom.to_string(hd(cell.transport_pair))
    assert get_in(predecessor.request_metadata, ["routing", "model_serving_mode"]) == Atom.to_string(cell.mode)
    assert String.split(predecessor.correlation_id, ":") |> hd() == cell.claim_domain
    if predecessor.transport == "http_sse", do: assert(predecessor.request_metadata["native_http_claim_arm"] == Atom.to_string(cell.role))
    predecessor_turn = Repo.get_by!(CodexTurn, request_id: predecessor.id)
    assert_role!(cell.role, predecessor, predecessor_turn, original, window)
    receipt = predecessor_receipt!(predecessor)
    previous_session = predecessor_turn.codex_session_id
    observed_topology = assert_topology!(cell, previous_session, peer)
    database_topology = observe_database_topology!(peer)
    {current, lifecycle} = establish_state!(cell, setup, previous_session, original, thread, port, peer)
    retained = Enum.map(outputs, &Map.put(&1, "content", nil))
    successor_payload = Map.update!(original, "input", &(&1 ++ retained ++ [mailbox()]))
    successor = send_turn!(if(cell.path == :websocket_to_http_fallback, do: :http_sse, else: cell.path), port, setup, successor_payload, thread)
    assert successor.status == "succeeded"
    assert successor.transport == Atom.to_string(List.last(cell.transport_pair))
    successor_session = Repo.get_by!(CodexTurn, request_id: successor.id).codex_session_id
    assert successor_session == current
    assert get_in(successor.request_metadata, ["routing", "model_serving_mode"]) == Atom.to_string(cell.mode)
    if cell.path == :websocket_owner_remote, do: assert(Repo.get!(CodexSession, current).owner_instance_id == Atom.to_string(peer.node))

    assert_ancestry!(cell.retry_link, predecessor, successor)
    roles = %{opener: opener && opener.id, predecessor: predecessor.id, preparation: lifecycle[:preparation_request_id], successor: successor.id}
    finish_cell!(%{cell: cell, setup: setup, upstream: upstream, predecessor: predecessor, previous_session: previous_session, current: current, original: original, receipt: receipt, lifecycle: lifecycle, observed_topology: observed_topology, roles: roles, database_topology: database_topology})
  end

  defp finish_cell!(%{cell: cell, setup: setup, upstream: upstream, predecessor: predecessor, previous_session: previous_session, current: current, original: original, receipt: receipt, lifecycle: lifecycle, observed_topology: observed_topology, roles: roles, database_topology: database_topology}) do
    requests = requests(setup)
    rows = Enum.map(requests, &request_ledger_receipt!/1)
    work = observe_work!(setup, requests, upstream)
    assert :ok = NativeContinuationMatrix.assert_work_set!(cell, roles, work)
    captured = FakeUpstream.requests(upstream)
    predecessor_index = Enum.find_index(requests, &(&1.id == predecessor.id))
    pair = [Enum.at(captured, predecessor_index), List.last(captured)]
    assert NativeTurnContinuation.turn_role(hd(pair).json) == NativeTurnContinuation.turn_role(original)
    assert Enum.map(pair, & &1.method) == Enum.map(cell.transport_pair, &if(&1 == :websocket, do: "WEBSOCKET", else: "POST"))
    observed_pair = Enum.map(pair, &if(&1.method == "WEBSOCKET", do: :websocket, else: :http_sse))
    observed_path = observed_path!(observed_pair, observed_topology)

    observed_state =
      cond do
        previous_session == current -> :same
        lifecycle[:preparation_request_id] -> :replacement_engaged
        lifecycle.genuine_expiry -> :replacement_fresh
      end

    observed_role = if predecessor.transport == "http_sse", do: String.to_existing_atom(predecessor.request_metadata["native_http_claim_arm"]), else: cell.role
    write_receipt!(cell, %{outcome: :admission, path: observed_path, request_count: length(requests), dispatch_count: FakeUpstream.count(upstream), predecessor_session: previous_session, successor_session: current, ancestry_verified: true, per_request_ledger_verified: true, requests: rows, lifecycle_observation: lifecycle, owner_topology: observed_topology, transport_pair: observed_pair, observed_role: observed_role, observed_state: observed_state, observed_mode: String.to_existing_atom(get_in(predecessor.request_metadata, ["routing", "model_serving_mode"])), observed_claim_domain: String.split(predecessor.correlation_id, ":") |> hd(), predecessor_receipt: receipt, role_request_ids: roles, work_observation: work, database_topology_observation: database_topology})
  end

  defp observe_work!(setup, requests, upstream) do
    ids = Enum.map(requests, & &1.id)
    %{request_ids: ids, attempt_request_ids: Repo.all(from(a in Attempt, where: a.request_id in ^ids, select: a.request_id)), dispatch_count: FakeUpstream.count(upstream), retry_edges: Repo.all(from(l in RequestClientRetryLink, join: r in Request, on: r.id == l.predecessor_request_id, where: r.pool_id == ^setup.pool.id, select: %{predecessor: l.predecessor_request_id, successor: l.successor_request_id})), ledger: Repo.all(from(l in LedgerEntry, where: l.request_id in ^ids, group_by: [l.request_id, l.entry_kind], select: %{request_id: l.request_id, kind: l.entry_kind, count: count(l.id)}))}
  end

  defp observe_database_topology!(nil), do: %{peer_node_distinct: false}

  defp observe_database_topology!(peer) do
    %{rows: [[local_backend, database]]} = Repo.query!("SELECT pg_backend_pid(), current_database()")
    %{rows: [[remote_backend, remote_database]]} = :erpc.call(peer.node, Repo, :query!, ["SELECT pg_backend_pid(), current_database()"])
    assert local_backend != remote_backend
    assert database == remote_database
    assert peer.node != node()
    %{peer_node_distinct: true, backend_distinct: true, same_database: true, local_backend: local_backend, remote_backend: remote_backend}
  end

  defp predecessor_receipt!(predecessor) do
    attempt = await_receipt!(predecessor, System.monotonic_time(:millisecond) + @budget)
    assert attempt.transport == predecessor.transport
    verify_receipt!(predecessor.transport, attempt.response_metadata)
  end

  defp verify_receipt!("http_sse", metadata) do
    receipt = metadata["native_http_mailbox_prefix"]
    assert receipt["output_item_done_count"] == 1
    assert length(receipt["item_digests"]) == 1
    receipt
  end

  defp verify_receipt!("websocket", metadata) do
    receipt = metadata["downstream_delivery"]
    assert receipt["outcome"] == "delivered"
    assert receipt["terminal_class"] == "response.completed"
    assert receipt["completed_items"] == 1
    assert length(receipt["completed_item_digests"]) == 1
    receipt
  end

  defp assert_ancestry!(true, predecessor, successor) do
    assert get_in(successor.request_metadata, ["client_resend", "predecessor_request_id"]) == predecessor.id
    assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id and l.successor_request_id == ^successor.id), :count) == 1
  end

  defp assert_ancestry!(false, predecessor, successor) do
    assert predecessor.correlation_id != successor.correlation_id
    refute successor.request_metadata["client_resend"]
    assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id and l.successor_request_id == ^successor.id), :count) == 0
  end

  defp request_ledger_receipt!(request) do
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert attempt.transport == request.transport
    ledger = Enum.frequencies(Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id, select: l.entry_kind)))
    assert ledger == %{"reservation" => 1, "release" => 1, "settlement" => 1}
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement" and l.amount_status == "recorded"), :count) == 1
    %{request_id: request.id, status: request.status, transport: request.transport, claim: String.split(request.correlation_id, ":") |> hd(), ledger: ledger, attempt_count: 1}
  end

  defp observed_path!([:http_sse, :http_sse], :none), do: :http_sse
  defp observed_path!([:websocket, :http_sse], :local), do: :websocket_to_http_fallback
  defp observed_path!([:websocket, :websocket], :direct), do: :websocket_direct
  defp observed_path!([:websocket, :websocket], :local), do: :websocket_owner_local
  defp observed_path!([:websocket, :websocket], :remote), do: :websocket_owner_remote

  defp assert_role!(:opening, predecessor, _turn, original, _window) do
    assert NativeTurnContinuation.turn_role(original) == :opening
    assert String.starts_with?(predecessor.correlation_id, "codex-turn:")
  end

  defp assert_role!(:steered_continuation, predecessor, turn, original, window) do
    assert NativeTurnContinuation.turn_role(original) == :opening
    expected = WebsocketTurnIdentity.steered_claim_key(semantic_key!(turn, original), NativeTurnContinuation.turn_progress(original, window))
    assert predecessor.correlation_id == expected
  end

  defp assert_role!(:post_compaction_resume, predecessor, turn, original, _window) do
    assert {:post_compaction_resume, anchor} = NativeTurnContinuation.turn_role(original)
    assert predecessor.correlation_id == WebsocketTurnIdentity.resume_claim_key(semantic_key!(turn, original), anchor)
  end

  defp assert_role!(:tool_continuation, predecessor, _turn, original, _window) do
    assert NativeTurnContinuation.turn_role(original) == :tool_continuation
    assert String.starts_with?(predecessor.correlation_id, "codex-request:")
    refute String.starts_with?(predecessor.correlation_id, "codex-request-retry:")
  end

  defp semantic_key!(%{semantic_turn_digest: <<_::256>> = digest}, _original), do: digest

  defp semantic_key!(turn, original) do
    document = CodexPooler.JSON.decode!(original["client_metadata"]["x-codex-turn-metadata"])
    scope = WebsocketTurnIdentity.claim_scope(Repo.get!(CodexSession, turn.codex_session_id), document["thread_id"])
    assert {:ok, identity} = WebsocketTurnIdentity.resolve(original, scope)
    identity.semantic_turn_key
  end

  defp role_input(:opening), do: native_text_input("synthetic")
  defp role_input(:steered_continuation), do: native_text_input("synthetic") ++ native_text_input("synthetic local summary")
  defp role_input(:tool_continuation), do: native_text_input("synthetic") ++ [%{"type" => "function_call", "call_id" => "sample_call", "name" => "sample_tool", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "sample_call", "output" => "synthetic"}]
  defp role_input(:post_compaction_resume), do: native_text_input("synthetic") ++ [%{"type" => "compaction", "encrypted_content" => "synthetic_compaction"}]

  defp establish_state!(%{state: :same}, _setup, session, _payload, _thread, _port, _peer), do: {session, %{same_session: true}}

  defp establish_state!(cell, setup, previous, original, thread, port, peer) do
    owner = owned_idle_owner(peer, previous)
    if owner, do: MailboxLeaseLifecycleSupport.suppress_owned_idle_renewal!(owner)
    lifecycle = MailboxLeaseLifecycleSupport.renew_and_observe_expiry!(Repo.get!(CodexSession, previous))
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    window = CodexPooler.JSON.decode!(original["client_metadata"]["x-codex-turn-metadata"])["window_number"]
    opts = RequestOptions.for_websocket(%{session_key: Repo.get!(CodexSession, previous).session_key})
    replacement_result = if peer, do: :erpc.call(peer.node, SessionContinuity, :start_codex_session, [auth, opts]), else: SessionContinuity.start_codex_session(auth, opts)
    assert {:ok, replacement} = replacement_result
    assert replacement.id != previous
    chronology = observe_replacement_authority!(setup, previous, replacement.id)
    assert :ok = NativeContinuationMatrix.assert_replacement!(Map.merge(lifecycle, chronology))
    if owner, do: MailboxLeaseLifecycleSupport.stop_owned_after_expiry!(owner)

    if peer do
      replacement_owner = OwnerSupport.start_shared_peer_session_owner!(setup, %{accepted_turn_state: thread}, peer.node)
      assert replacement_owner.session.id == replacement.id
    end

    preparation_id =
      if cell.state == :replacement_engaged do
        preparation = payload(setup, thread, native_text_input("synthetic independent preparation"), window)
        document = CodexPooler.JSON.decode!(preparation["client_metadata"]["x-codex-turn-metadata"]) |> Map.put("turn_id", "sample_preparation")
        preparation = put_in(preparation, ["client_metadata", "x-codex-turn-metadata"], CodexPooler.JSON.encode!(document))
        prepared = send_turn!(if(cell.path == :websocket_to_http_fallback, do: :http_sse, else: cell.path), port, setup, preparation, thread)
        assert prepared.status == "succeeded"
        assert Repo.get_by!(CodexTurn, request_id: prepared.id).codex_session_id == replacement.id
        refute prepared.request_metadata["client_resend"]
        prepared.id
      end

    {replacement.id, lifecycle |> Map.merge(chronology) |> Map.put(:preparation_request_id, preparation_id)}
  end

  defp observe_replacement_authority!(setup, previous_id, current_id) do
    previous = Repo.get!(CodexSession, previous_id)
    current = Repo.get!(CodexSession, current_id)
    assert previous.status == "closed"
    same_canonical_key = Repo.exists?(from p in CodexSession, join: c in CodexSession, on: c.id == ^current_id, where: p.id == ^previous_id and fragment("lower(?) = lower(?)", p.session_key, c.session_key))
    %{previous_closed_at: previous.closed_at, replacement_created_at: current.created_at, previous_owner_deadline: previous.owner_lease_expires_at, close_reason: previous.close_reason, same_pool: previous.pool_id == current.pool_id and current.pool_id == setup.pool.id, same_api_key: previous.api_key_id == current.api_key_id and current.api_key_id == setup.api_key.id, same_canonical_key: same_canonical_key}
  end

  defp owned_idle_owner(%{owner_pid: owner}, _session), do: owner

  defp owned_idle_owner(nil, session) do
    case WebsocketOwnerSession.lookup(session) do
      {:ok, owner} -> owner
      {:error, :owner_unavailable} -> nil
    end
  end

  defp assert_topology!(%{path: :http_sse}, _session, nil), do: :none

  defp assert_topology!(%{path: :websocket_direct}, session, nil) do
    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session)
    :direct
  end

  defp assert_topology!(%{path: :websocket_owner_remote}, session, peer) do
    assert peer.session.id == session
    assert node(peer.owner_pid) != node()
    assert :erpc.call(node(peer.owner_pid), Process, :alive?, [peer.owner_pid])
    :remote
  end

  defp assert_topology!(_cell, session, nil) do
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session)
    assert node(owner) == node()
    :local
  end

  defp payload(setup, thread, input, window) do
    metadata = CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "sample_matrix_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:#{window}", "window_number" => window})
    %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => metadata}}
  end

  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
  defp completed, do: %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_matrix", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}

  defp send_turn!(:http_sse, port, setup, body, thread) do
    assert {200, response} = http!(port, setup, body, thread)
    assert response =~ "response.completed"
    latest!(setup)
  end

  defp send_turn!(_path, port, setup, body, thread) do
    window = CodexPooler.JSON.decode!(body["client_metadata"]["x-codex-turn-metadata"])["window_number"]
    headers = [{"x-codex-window-id", "#{thread}:#{window}"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    sockets_before = WebsocketCleanupFence.listener_sockets()
    {conn, ws, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(sockets_before)
    {conn, ws} = public_websocket_send_text!(conn, ws, ref, CodexPooler.JSON.encode!(Map.put(body, "type", "response.create")))
    {_conn, _ws, terminal} = receive_native_terminal!(conn, ws, ref)
    assert terminal["type"] == "response.completed"
    monitor = Process.monitor(socket)
    Mint.HTTP.close(conn)
    # Neither the settled request nor the delivery receipt orders the turn's response task, which registers the
    # session's continuity, and so renews its owner lease, after the settlement: a socket closing while that task is
    # slower than its 250 ms terminate drain records the receipt without it when the owner is local, and a lease
    # forced to lapse next was renewed again for 45 s (findings#303 row 303-6). The socket's exit ends the task.
    assert_receive {:DOWN, ^monitor, :process, ^socket, _reason}, @budget
    WebsocketCleanupFence.await_session_cleanups!()
    latest!(setup)
  end

  defp http!(port, setup, body, thread, owner \\ :test, turn_state \\ :thread) do
    metadata = body["client_metadata"]["x-codex-turn-metadata"]
    window = CodexPooler.JSON.decode!(metadata)["window_number"]
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:#{window}"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if turn_state == :omit, do: headers, else: [{"x-codex-turn-state", thread} | headers]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    if owner == :test, do: on_exit(fn -> Mint.HTTP.close(conn) end)

    try do
      {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(body))
      receive_http!(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_http!(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, value}, {_, bytes, done} -> {value, bytes, done}
        {:data, ^ref, value}, {code, bytes, done} -> {code, bytes <> value, done}
        {:done, ^ref}, {code, bytes, _} -> {code, bytes, true}
        _, acc -> acc
      end)

    if done, do: {status, body}, else: receive_http!(conn, ref, status, body)
  end

  defp requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))
  defp latest!(setup), do: await_latest!(setup, System.monotonic_time(:millisecond) + @budget)

  defp await_receipt!(request, deadline) do
    attempt = Repo.get_by!(Attempt, request_id: request.id)
    key = if request.transport == "http_sse", do: "native_http_mailbox_prefix", else: "downstream_delivery"

    cond do
      is_map(attempt.response_metadata[key]) ->
        attempt

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("matrix predecessor receipt was not persisted")

      true ->
        receive do
        after
          10 -> await_receipt!(request, deadline)
        end
    end
  end

  defp await_latest!(setup, deadline) do
    row = List.last(requests(setup))

    cond do
      row && row.completed_at ->
        row

      System.monotonic_time(:millisecond) > deadline ->
        flunk("matrix request did not settle")

      true ->
        Process.sleep(10)
        await_latest!(setup, deadline)
    end
  end

  defp write_receipt!(cell, receipt) do
    if dir = System.get_env("NATIVE_CONTINUATION_MATRIX_EVIDENCE") do
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, String.replace(cell.id, "/", "--") <> ".json"), CodexPooler.JSON.encode!(Map.merge(cell, receipt)))
    end
  end
end
