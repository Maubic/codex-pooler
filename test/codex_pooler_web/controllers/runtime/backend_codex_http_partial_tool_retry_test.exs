defmodule CodexPoolerWeb.Runtime.BackendCodexHttpPartialToolRetryTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, ClientRetry, LedgerEntry, NativeHttpToolObservation, Request, RequestClientRetryLink, RequestReplayEntitlement}
  alias CodexPooler.Accounting.RequestLifecycle.FailedPredecessorResend
  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.{NativeHttpTurnIdentity, RequestOptions}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.InstanceSettings.AppSecretCrypto
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.PoolerFixtures
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @detection_timeout_ms 15_000

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation] do
    @tag zero_done_http_boundary: true, mode: mode, arm: arm
    test "#{mode} HTTP #{arm} zero-DONE interruption admits one identical successor", %{mode: mode, arm: arm} do
      events = [
        {"response.created", %{"type" => "response.created", "response" => %{"id" => "resp_zero_done", "status" => "in_progress", "output" => []}}},
        {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_zero_done", "summary" => [], "status" => "in_progress"}}},
        {"response.reasoning_summary_text.delta", %{"type" => "response.reasoning_summary_text.delta", "item_id" => "rs_zero_done", "output_index" => 0, "summary_index" => 0, "delta" => "synthetic incomplete reasoning"}}
      ]

      {upstream, setup, port, thread_id, payload} = scenario(mode, arm, events, [FakeUpstream.sse_stream([completed_event()])], :abrupt)
      {status, body} = post_stream!(port, setup, payload, thread_id)
      assert status == 200
      assert body =~ "response.reasoning_summary_text.delta"
      refute body =~ "response.output_item.done"
      refute body =~ "response.completed"
      assert [first] = pool_requests(setup)
      assert {first.status, first.last_error_code, first.response_status_code, first.transport} == {"failed", "upstream_stream_error", 200, "http_sse"}
      assert first.request_metadata["native_http_claim_arm"] == Atom.to_string(arm)
      assert first.native_client_retry_version == 1
      assert byte_size(first.native_client_retry_digest) == 32
      turn = Repo.get_by!(CodexTurn, request_id: first.id)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      assert {turn.status, turn.error_code, turn.final_attempt_id, turn.transport_kind} == {"failed", "upstream_stream_error", attempt.id, "http_sse"}
      assert %DateTime{} = turn.first_visible_output_at
      assert %DateTime{} = turn.completed_at
      assert {attempt.status, attempt.network_error_code, attempt.replay_generation, attempt.transport} == {"failed", "upstream_stream_error", 0, "http_sse"}
      observation = attempt.response_metadata["native_http_partial_tool"]
      assert observation == %{"version" => 1, "parser_complete" => true, "poisoned" => false, "partial_tool" => nil, "input_done" => false}
      refute NativeHttpToolObservation.eligible_metadata?(observation)
      progress = attempt.response_metadata["native_http_resume_progress"]
      assert progress["version"] == 1
      assert progress["output_item_done_count"] == 0
      assert byte_size(Base.url_decode64!(progress["digest"], padding: false)) == 32
      assert ClientRetry.native_http_progress_matches?(progress, [])
      assert attempt.response_metadata["native_http_mailbox_prefix"] == %{}
      delivery = attempt.response_metadata["downstream_delivery"]
      assert delivery["terminal_class"] == "none"
      assert delivery["outcome"] == "completed"
      assert delivery["frames_after_visible"] >= 1

      session = Repo.get!(CodexSession, turn.codex_session_id)
      options = RequestOptions.build(%{transport: "http_sse", codex_session: session, api_key_runtime_epoch: first.native_client_retry_auth_epoch}, @path, payload)
      assert {:ok, identity} = NativeHttpTurnIdentity.request_claim(options, payload)
      assert identity.arm == arm
      assert :crypto.hash(:sha256, identity.key) == :crypto.hash(:sha256, first.correlation_id)
      assert :crypto.hash(:sha256, identity.native_client_retry_witness.digest) == :crypto.hash(:sha256, first.native_client_retry_digest)
      assert identity.native_client_retry_witness.auth_epoch == first.native_client_retry_auth_epoch
      await_zero_done_execution_proof!(attempt, System.monotonic_time(:millisecond) + @detection_timeout_ms)
      assert ExecutionTerminalProofs.terminal?(attempt)
      proof = Repo.get_by!(ExecutionTerminalProof, execution_id: attempt.owner_execution_id)
      assert proof.end_kind == "completed"

      {retry_status, retry_body} = post_stream!(port, setup, payload, thread_id)
      assert {retry_status, retry_body =~ "response.completed"} == {200, true}
      assert FakeUpstream.count(upstream) == 2
      assert [_, %Request{status: "succeeded"} = successor] = pool_requests(setup)
      assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^first.id), :count) == 1
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id))
      assert successor_id == successor.id
      assert_settled_once!([first.id, successor.id])
      CodexPooler.TestDiagnostics.puts("zero_done_http_boundary mode=#{mode} arm=#{arm} request_id=#{first.id} attempt_id=#{attempt.id} session_id=#{session.id} successor_id=#{successor.id} progress_count=0 exact_empty_progress=true exact_original_witness=true exact_executor_terminal=true executor_end_kind=#{proof.end_kind} visible_class=partial_reasoning frames_after_visible=#{delivery["frames_after_visible"]} identical_retry=200 physical_dispatches=2 successor_links=1 ledger_each=reservation1_settlement1_release1")
    end
  end

  for control <- [:missing_execution_proof, :foreign_execution_proof, :wrong_boot, :interrupted_execution, :wrong_end_kind, :wrong_model, :changed_epoch, :missing_digest, :missing_final_attempt, :generation, :expired, :missing_progress, :malformed_progress, :nonzero_progress, :missing_observer, :poisoned, :incomplete_parser, :completed_input, :malformed_tool_proof, :missing_prefix, :nonempty_prefix, :missing_receipt, :wrong_receipt, :malformed_frames, :oversized_frames, :active_attempt, :entitlement, :changed_body, :appended_mail, :hard_anchor, :foreign_session, :window_drift] do
    @tag zero_done_guard: true, zero_done_control: control
    test "recognized zero-DONE HTTP candidate refuses #{control}", %{zero_done_control: control} do
      {upstream, setup, port, thread_id, payload} = scenario("full", :opening, zero_done_events())
      assert_zero_done_failed!(port, setup, payload, thread_id)
      [first] = pool_requests(setup)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      turn = Repo.get_by!(CodexTurn, request_id: first.id)
      await_zero_done_execution_proof!(attempt, System.monotonic_time(:millisecond) + @detection_timeout_ms)
      assert ClientRetry.native_http_progress_matches?(attempt.response_metadata["native_http_resume_progress"], [])
      assert ExecutionTerminalProofs.terminal?(attempt)
      apply_zero_done_negative!(first, attempt, turn, setup, control, payload)

      retry_payload =
        case control do
          :changed_body -> Map.put(payload, "instructions", "changed synthetic instructions")
          :appended_mail -> Map.update!(payload, "input", &(&1 ++ [%{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic appended mail"}]}]))
          :hard_anchor -> Map.put(payload, "previous_response_id", "resp_synthetic_anchor")
          _ -> payload
        end

      headers =
        case control do
          :foreign_session -> [{"session-id", Ecto.UUID.generate()}]
          :window_drift -> [{"x-codex-window-id", Ecto.UUID.generate()}]
          _ -> []
        end

      {status, body} = post_stream!(port, setup, retry_payload, thread_id, true, headers)
      assert status == 409
      assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
      assert FakeUpstream.count(upstream) == 1
      assert length(pool_requests(setup)) == 1
      refute Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id or l.successor_request_id == ^first.id))
      assert_settled_once!([first.id])
      CodexPooler.TestDiagnostics.puts("zero_done_guard control=#{control} recognized_original_candidate=true native_status=409 physical_dispatches=1 successor_links=0")
    end
  end

  defp zero_done_events do
    [
      {"response.created", %{"type" => "response.created", "response" => %{"id" => "resp_zero_done", "status" => "in_progress", "output" => []}}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_zero_done", "summary" => [], "status" => "in_progress"}}},
      {"response.reasoning_summary_text.delta", %{"type" => "response.reasoning_summary_text.delta", "item_id" => "rs_zero_done", "output_index" => 0, "summary_index" => 0, "delta" => "synthetic incomplete reasoning"}}
    ]
  end

  @tag zero_done_active_successor: true, timeout: 60_000
  test "a competing identical HTTP retry cannot dispatch while the zero-DONE successor is active", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    release_ref = make_ref()
    barrier = FakeUpstream.barrier_sse_stream([hd(zero_done_events()), completed_event()], barrier_after: 1, notify: self(), release_ref: release_ref, on_client_close: :expected)
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.abrupt_close_mid_stream(zero_done_events()), barrier]))
    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)

    on_exit(fn ->
      Sandbox.unboxed_run(Repo, fn ->
        executions = Repo.all(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id, select: a.owner_execution_id))
        Repo.delete_all(from(p in ExecutionTerminalProof, where: p.execution_id in ^executions))
      end)
    end)

    _revision = set_model_serving_mode!(model_serving_scope(), setup, "full")
    setup = Map.put(setup, :serving_mode, "full")
    port = start_public_endpoint!()
    thread_id = Ecto.UUID.generate()
    payload = native_payload(setup, thread_id, :tool_continuation)
    assert_zero_done_failed!(port, setup, payload, thread_id)
    [first] = pool_requests(setup)
    attempt = Repo.get_by!(Attempt, request_id: first.id)
    await_zero_done_execution_proof!(attempt, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    supervisor = start_supervised!(Task.Supervisor)
    client = Task.Supervisor.async_nolink(supervisor, fn -> post_stream!(port, setup, payload, thread_id, false) end)
    monitor = Process.monitor(client.pid)
    assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^release_ref}, @detection_timeout_ms
    on_exit(fn -> send(handler, {:fake_upstream_release_chunk, release_ref}) end)

    try do
      assert [_, %Request{status: "in_progress"} = successor] = pool_requests(setup)
      assert Repo.get_by!(Attempt, request_id: successor.id).status == "in_progress"
      assert_refused!(port, setup, payload, thread_id)
      assert FakeUpstream.count(upstream) == 2
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id))
      assert successor_id == successor.id
    after
      send(handler, {:fake_upstream_release_chunk, release_ref})
    end

    assert {200, body} = Task.await(client, @detection_timeout_ms)
    assert body =~ "response.completed"
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @detection_timeout_ms
    assert [_, %Request{status: "succeeded"} = final] = pool_requests(setup)
    final_attempt = Repo.get_by!(Attempt, request_id: final.id)
    await_zero_done_execution_proof!(final_attempt, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    assert_settled_once!([first.id, final.id])
    CodexPooler.TestDiagnostics.puts("zero_done_active_successor normal_auto_pg=true successor_observed_in_progress=true competitor_status=409 physical_dispatches=2 successor_links=1 terminal_released=true ledger_each=reservation1_settlement1_release1")
  end

  for completed <- [:reasoning, :tool] do
    @tag zero_done_completed_guard: completed
    test "an actual completed #{completed} item is not a zero-DONE retry", %{zero_done_completed_guard: completed} do
      events =
        if completed == :reasoning,
          do: zero_done_events() ++ [{"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_zero_done", "summary" => [], "encrypted_content" => "synthetic reasoning", "status" => "completed"}}}],
          else: partial_events("custom_tool_call") ++ [input_done_event("custom_tool_call")] ++ control_events(:completed_item)

      {upstream, setup, port, thread_id, payload} = scenario("full", :opening, events)
      {200, body} = post_stream!(port, setup, payload, thread_id)
      assert body =~ "response.output_item.done"
      [first] = pool_requests(setup)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      assert attempt.response_metadata["native_http_resume_progress"]["output_item_done_count"] == 1
      assert_refused!(port, setup, payload, thread_id)
      assert FakeUpstream.count(upstream) == 1
      assert_settled_once!([first.id])
    end
  end

  defp assert_zero_done_failed!(port, setup, payload, thread_id) do
    {status, body} = post_stream!(port, setup, payload, thread_id)
    assert status == 200
    assert body =~ "response.reasoning_summary_text.delta"
    refute body =~ "response.output_item.done"
    assert [first] = pool_requests(setup)
    assert {first.status, first.last_error_code} == {"failed", "upstream_stream_error"}
    attempt = Repo.get_by!(Attempt, request_id: first.id)
    assert attempt.response_metadata["native_http_partial_tool"] == %{"version" => 1, "parser_complete" => true, "poisoned" => false, "partial_tool" => nil, "input_done" => false}
    assert ClientRetry.native_http_progress_matches?(attempt.response_metadata["native_http_resume_progress"], [])
  end

  defp apply_zero_done_negative!(_first, attempt, _turn, _setup, control, _payload) when control in [:missing_execution_proof, :foreign_execution_proof, :interrupted_execution, :wrong_end_kind] do
    proof = Repo.get_by!(ExecutionTerminalProof, execution_id: attempt.owner_execution_id)

    case control do
      :missing_execution_proof ->
        Repo.delete!(proof)

      :foreign_execution_proof ->
        Repo.update!(Ecto.Changeset.change(proof, owner_process_id: "<0.999999.0>"))

      :interrupted_execution ->
        Repo.update!(Ecto.Changeset.change(proof, interruption_code: "owner_drained"))

      :wrong_end_kind ->
        Repo.update!(Ecto.Changeset.change(proof, end_kind: "process_down"))
    end
  end

  defp apply_zero_done_negative!(first, attempt, turn, setup, control, _payload) when control in [:wrong_boot, :wrong_model, :changed_epoch, :missing_digest, :missing_final_attempt, :generation, :expired, :active_attempt] do
    case control do
      :wrong_boot ->
        Repo.update!(Ecto.Changeset.change(attempt, owner_instance_boot_id: "synthetic-other-boot"))

      :wrong_model ->
        model = PoolerFixtures.model_fixture(setup.pool, %{exposed_model_id: "synthetic-other-#{System.unique_integer([:positive])}"})
        Repo.update!(Ecto.Changeset.change(first, model_id: model.id))

      :changed_epoch ->
        Repo.update!(Ecto.Changeset.change(first, native_client_retry_auth_epoch: first.native_client_retry_auth_epoch + 1))

      :missing_digest ->
        Repo.update!(Ecto.Changeset.change(first, native_client_retry_digest: nil))

      :missing_final_attempt ->
        Repo.update!(Ecto.Changeset.change(turn, final_attempt_id: nil))

      :generation ->
        Repo.update!(Ecto.Changeset.change(attempt, replay_generation: 1))

      :expired ->
        Repo.update!(Ecto.Changeset.change(first, completed_at: DateTime.add(DateTime.utc_now(), -(ClientRetry.retry_window_seconds() + 1), :second)))

      :active_attempt ->
        Repo.update!(Ecto.Changeset.change(attempt, status: "in_progress", completed_at: nil))
    end
  end

  defp apply_zero_done_negative!(first, attempt, turn, setup, :entitlement, payload) do
    {:ok, lease_digest} = RequestReplayEntitlement.owner_lease_digest(Ecto.UUID.generate())
    now = DateTime.utc_now()

    session = Repo.get!(CodexSession, turn.codex_session_id)
    options = RequestOptions.build(%{transport: "http_sse", codex_session: session, api_key_runtime_epoch: first.native_client_retry_auth_epoch}, @path, payload)
    assert {:ok, identity} = NativeHttpTurnIdentity.request_claim(options, payload)
    # HTTP normally leaves the turn digest unset. A competing entitlement
    # requires its real native identity to be published on the owned turn
    # before the database accepts that negative-state fixture.
    Repo.update!(Ecto.Changeset.change(turn, semantic_turn_digest: identity.semantic_turn_key))
    scope = %{pool_id: first.pool_id, api_key_id: first.api_key_id, model_id: first.model_id, endpoint: @path, codex_session_id: session.id, native_http_transport: "http_sse", native_client_retry_witness: identity.native_client_retry_witness, native_http_semantic_turn_key: identity.semantic_turn_key, payload: payload, anchor_present?: false}

    identity_only_result =
      case FailedPredecessorResend.resolve(identity.key, scope) do
        {:ok, %{predecessor_shape: shape}} -> shape
        {:error, reason} when is_atom(reason) -> reason
        _other -> :unexpected_resolution
      end

    assert identity_only_result == :identical_resend
    refute Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id))
    CodexPooler.TestDiagnostics.puts("zero_done_entitlement counterfactual_turn_identity=true normal_identity_api=true eligible_after_identity_only=true no_grant_or_dispatch_created=true")
    changeset = RequestReplayEntitlement.changeset(%RequestReplayEntitlement{}, %{request_id: first.id, codex_turn_id: turn.id, eligible_attempt_id: attempt.id, api_key_id: first.api_key_id, api_key_runtime_epoch: first.native_client_retry_auth_epoch, pool_id: first.pool_id, model_id: first.model_id, model_identifier: setup.model.exposed_model_id, semantic_turn_digest: identity.semantic_turn_key, replay_claim_digest: first.native_client_retry_digest, owner_lease_digest: lease_digest, owner_lease_key_version: AppSecretCrypto.key_version(), predecessor_epoch: 1, replay_generation: 1, status: "armed", armed_at: now, expires_at: DateTime.add(now, 30, :second)})

    case Repo.insert(changeset) do
      {:ok, _entitlement} -> :ok
      {:error, invalid} -> flunk("owned entitlement setup rejected fields=#{inspect(Keyword.keys(invalid.errors))}")
    end
  end

  defp apply_zero_done_negative!(_first, _attempt, _turn, _setup, control, _payload) when control in [:changed_body, :appended_mail, :hard_anchor, :foreign_session, :window_drift], do: :ok
  defp apply_zero_done_negative!(_first, attempt, _turn, _setup, control, _payload), do: mutate_zero_done_metadata!(attempt, control)

  defp mutate_zero_done_metadata!(attempt, control) do
    changed = zero_done_negative_metadata(attempt.response_metadata, control)
    Repo.update!(Ecto.Changeset.change(attempt, response_metadata: changed))
  end

  defp zero_done_negative_metadata(metadata, :missing_progress), do: Map.delete(metadata, "native_http_resume_progress")
  defp zero_done_negative_metadata(metadata, :malformed_progress), do: put_in(metadata, ["native_http_resume_progress", "digest"], String.duplicate("A", 43))
  defp zero_done_negative_metadata(metadata, :nonzero_progress), do: put_in(metadata, ["native_http_resume_progress", "output_item_done_count"], 1)
  defp zero_done_negative_metadata(metadata, :missing_observer), do: Map.delete(metadata, "native_http_partial_tool")
  defp zero_done_negative_metadata(metadata, :poisoned), do: put_in(metadata, ["native_http_partial_tool", "poisoned"], true)
  defp zero_done_negative_metadata(metadata, :incomplete_parser), do: put_in(metadata, ["native_http_partial_tool", "parser_complete"], false)
  defp zero_done_negative_metadata(metadata, :completed_input), do: put_in(metadata, ["native_http_partial_tool", "input_done"], true)
  defp zero_done_negative_metadata(metadata, :malformed_tool_proof), do: put_in(metadata, ["native_http_partial_tool", "partial_tool"], "unrecognized_tool")
  defp zero_done_negative_metadata(metadata, :missing_prefix), do: Map.delete(metadata, "native_http_mailbox_prefix")
  defp zero_done_negative_metadata(metadata, :nonempty_prefix), do: Map.put(metadata, "native_http_mailbox_prefix", %{"version" => 1, "output_item_done_count" => 1, "item_digests" => ["0123456789ab"]})
  defp zero_done_negative_metadata(metadata, :missing_receipt), do: Map.delete(metadata, "downstream_delivery")
  defp zero_done_negative_metadata(metadata, :wrong_receipt), do: put_in(metadata, ["downstream_delivery", "terminal_class"], "response.completed")
  defp zero_done_negative_metadata(metadata, :malformed_frames), do: put_in(metadata, ["downstream_delivery", "frames_after_visible"], "2")
  defp zero_done_negative_metadata(metadata, :oversized_frames), do: put_in(metadata, ["downstream_delivery", "frames_after_visible"], 65_536)

  defp await_zero_done_execution_proof!(attempt, deadline) do
    assert attempt.owner_instance_id == Atom.to_string(node())
    assert ExecutionIdentity.status(attempt) == :dead

    case ExecutionRegistry.pending_proofs([attempt.owner_execution_id]) do
      [proof] ->
        fields = [:owner_execution_id, :owner_instance_id, :owner_instance_boot_id, :owner_process_id]
        assert Map.take(proof, fields) == Map.take(attempt, fields)
        assert proof.end_kind == "completed"
        assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
        assert ExecutionTerminalProofs.terminal?(attempt)
        assert :ok = ExecutionRegistry.acknowledge([attempt.owner_execution_id])

      [] ->
        if ExecutionTerminalProofs.terminal?(attempt) do
          :ok
        else
          remaining = deadline - System.monotonic_time(:millisecond)
          assert remaining > 0, "the exact HTTP execution proof was not retained before the deadline"

          receive do
          after
            min(5, remaining) -> await_zero_done_execution_proof!(attempt, deadline)
          end
        end
    end
  end

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation], tool_type <- ["custom_tool_call", "function_call"] do
    @tag mode: mode, arm: arm, tool_type: tool_type
    test "an identical #{mode} native HTTP #{arm} retries a partial #{tool_type} exactly once", %{mode: mode, arm: arm, tool_type: tool_type} do
      {upstream, setup, port, thread_id, payload} = scenario(mode, arm, partial_events(tool_type), [FakeUpstream.sse_stream([completed_event()]), FakeUpstream.sse_stream([completed_event()])])
      assert_cut!(port, setup, payload, thread_id, tool_type)
      [first] = pool_requests(setup)
      contract = partial_retry_contract()
      assert first.transport == contract.predecessor_transport
      assert first.last_error_code == contract.predecessor_error
      assert first.request_metadata["native_http_claim_arm"] == Atom.to_string(arm)
      assert first.request_metadata["native_http_claim_arm"] in contract.claim_arms
      assert tool_type in contract.partial_tools

      {retry_status, retry_body} = post_stream!(port, setup, payload, thread_id)
      assert {retry_status, retry_body =~ "response.completed"} == {200, true}
      assert [_, %Request{status: "succeeded"} = retry] = pool_requests(setup)
      assert [%Attempt{status: "failed"} = first_attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^first.id))
      assert first_attempt.response_metadata["native_http_partial_tool"] == %{"version" => 1, "parser_complete" => contract.requires_complete_observation, "poisoned" => false, "partial_tool" => tool_type, "input_done" => false}
      assert retry.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id))
      assert successor_id == retry.id
      assert [%Attempt{status: "succeeded"}] = Repo.all(from(a in Attempt, where: a.request_id == ^retry.id))
      assert_settled_once!([first.id, retry.id])

      assert_completed_resend!(port, setup, payload, thread_id)
      assert FakeUpstream.count(upstream) == 2 + contract.retry_limit
      assert length(pool_requests(setup)) == 2 + contract.retry_limit
      assert_settled_once!(Enum.map(pool_requests(setup), & &1.id))
    end
  end

  for mode <- ["full", "lite"], tool_type <- ["custom_tool_call", "function_call"] do
    @tag mode: mode, tool_type: tool_type
    test "#{mode} native HTTP retries an incomplete #{tool_type} after reasoning and controls", %{mode: mode, tool_type: tool_type} do
      contract = partial_retry_contract()
      assert contract.incomplete_reasoning_prefix
      assert contract.tool_index == :tracked_non_negative
      assert contract.requires_trailing_tool_item
      assert "codex.rate_limits" in contract.neutral_controls
      [created | tool_events] = partial_events(tool_type)
      reasoning = %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => "reasoning", "id" => "rs_synthetic", "summary" => []}}
      summary = %{"type" => "response.reasoning_summary_text.delta", "item_id" => "rs_synthetic", "output_index" => 0, "summary_index" => 0, "delta" => "synthetic"}
      tool_events = Enum.map(tool_events, fn {type, event} -> {type, Map.put(event, "output_index", 1)} end)
      events = [created, {reasoning["type"], reasoning}, {summary["type"], summary}] ++ tool_events ++ [{"codex.rate_limits", %{"type" => "codex.rate_limits"}}]
      {upstream, setup, port, thread_id, payload} = scenario(mode, :opening, events, [FakeUpstream.sse_stream([completed_event()]), FakeUpstream.sse_stream([completed_event()])])
      assert_cut!(port, setup, payload, thread_id, tool_type)
      [first] = pool_requests(setup)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      assert attempt.response_metadata["native_http_partial_tool"]["poisoned"] == false
      {status, body} = post_stream!(port, setup, payload, thread_id)
      assert {status, body =~ "response.completed"} == {200, true}
      assert_completed_resend!(port, setup, payload, thread_id)
      assert FakeUpstream.count(upstream) == 3
      assert_settled_once!(Enum.map(pool_requests(setup), & &1.id))
    end
  end

  for control <- [:completed_item, :created_output, :unknown_event, :malformed_event, :truncated_event, :oversized_event, :missing_proof, :poisoned_proof, :incomplete_proof, :changed_body, :anchor, :replay_generation, :expired, :missing_digest, :changed_epoch] do
    @tag control: control
    test "a partial native HTTP tool call keeps the duplicate fence for #{control}", %{control: control} do
      events =
        if control == :created_output do
          [created | remaining] = partial_events("custom_tool_call")
          {name, body} = created
          [{name, put_in(body, ["response", "output"], [%{"type" => "message", "id" => "msg_synthetic_prior"}])} | remaining]
        else
          partial_events("custom_tool_call") ++ control_events(control)
        end

      {upstream, setup, port, thread_id, payload} = scenario("full", :opening, events)
      assert_cut!(port, setup, payload, thread_id, "custom_tool_call", control == :completed_item)
      [first] = pool_requests(setup)
      apply_negative_state!(first, control)

      retry_payload =
        case control do
          :changed_body -> Map.put(payload, "instructions", "different synthetic instructions")
          :anchor -> Map.put(payload, "previous_response_id", "resp_synthetic_anchor")
          _ -> payload
        end

      assert_refused!(port, setup, retry_payload, thread_id)
      assert FakeUpstream.count(upstream) == 1
      assert length(pool_requests(setup)) == 1
      assert [%Attempt{}] = Repo.all(from(a in Attempt, where: a.request_id == ^first.id))
      assert_settled_once!([first.id])
    end
  end

  test "a second partial cut cannot buy another retry in the same lineage" do
    {upstream, setup, port, thread_id, payload} = scenario("full", :opening, partial_events("custom_tool_call"), [FakeUpstream.abrupt_close_mid_stream(partial_events("custom_tool_call"))])
    assert_cut!(port, setup, payload, thread_id, "custom_tool_call")
    {retry_status, retry_body} = post_stream!(port, setup, payload, thread_id)
    assert {retry_status, retry_body =~ "response.custom_tool_call_input.delta"} == {200, true}
    requests = pool_requests(setup)
    assert Enum.map(requests, &{&1.status, &1.last_error_code}) == [{"failed", "upstream_stream_error"}, {"failed", "upstream_stream_error"}]
    assert_refused!(port, setup, payload, thread_id)
    assert FakeUpstream.count(upstream) == 2
    assert length(pool_requests(setup)) == 2
    assert_settled_once!(Enum.map(requests, & &1.id))
  end

  for mode <- ["full", "lite"], tool_type <- ["custom_tool_call", "function_call"] do
    @tag mode: mode, tool_type: tool_type
    test "clean EOF after #{mode} #{tool_type} input.done admits one identical retry", %{mode: mode, tool_type: tool_type} do
      events = partial_events(tool_type) ++ [input_done_event(tool_type)]
      {upstream, setup, port, thread_id, payload} = scenario(mode, :opening, events, [FakeUpstream.sse_stream([completed_event()]), FakeUpstream.sse_stream([completed_event()])], :clean)
      assert_cut!(port, setup, payload, thread_id, tool_type)
      [first] = pool_requests(setup)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      assert attempt.response_metadata["native_http_partial_tool"] == %{"version" => 1, "parser_complete" => true, "poisoned" => false, "partial_tool" => tool_type, "input_done" => true}
      {status, body} = post_stream!(port, setup, payload, thread_id)
      assert {status, body =~ "response.completed"} == {200, true}
      assert_completed_resend!(port, setup, payload, thread_id)
      assert FakeUpstream.count(upstream) == 3
      assert_settled_once!(Enum.map(pool_requests(setup), & &1.id))
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "clean EOF after #{mode} completed tool item stays fenced", %{mode: mode} do
      events = partial_events("custom_tool_call") ++ [input_done_event("custom_tool_call")] ++ control_events(:completed_item)
      {upstream, setup, port, thread_id, payload} = scenario(mode, :opening, events, [], :clean)
      {status, body} = post_stream!(port, setup, payload, thread_id)
      assert status == 200
      assert body =~ "response.output_item.done"
      refute body =~ "response.completed"
      assert_refused!(port, setup, payload, thread_id)
      assert FakeUpstream.count(upstream) == 1
      assert [request] = pool_requests(setup)
      assert_settled_once!([request.id])
    end
  end

  @tag timeout: 60_000
  test "concurrent identical HTTP retries on independent PostgreSQL connections admit one successor", context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.abrupt_close_mid_stream(partial_events("custom_tool_call")), FakeUpstream.sse_stream([completed_event()])]))
    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, "full")
    setup = Map.put(setup, :serving_mode, "full")
    port = start_public_endpoint!()
    thread_id = Ecto.UUID.generate()
    payload = native_payload(setup, thread_id, :opening)
    assert_cut!(port, setup, payload, thread_id, "custom_tool_call")
    [first] = pool_requests(setup)
    session_id = Repo.get_by!(CodexTurn, request_id: first.id).codex_session_id
    supervisor = start_supervised!(Task.Supervisor)
    parent = self()
    barrier = make_ref()

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Repo.transaction(fn ->
          Repo.one!(from(s in CodexSession, where: s.id == ^session_id, lock: "FOR UPDATE"))
          [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
          send(parent, {:session_locked, barrier, backend})

          receive do
            {:release_session, ^barrier} -> :ok
          after
            @detection_timeout_ms -> raise "session lock release not received"
          end
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    assert_receive {:session_locked, ^barrier, holder_backend}, @detection_timeout_ms

    clients =
      for _lane <- 1..2 do
        task = Task.Supervisor.async_nolink(supervisor, fn -> post_stream!(port, setup, payload, thread_id, false) end)
        {task, Process.monitor(task.pid)}
      end

    try do
      waiters = await_http_lock_waiters!(holder_backend, System.monotonic_time(:millisecond) + @detection_timeout_ms)
      assert length(Enum.uniq(waiters)) == 2
      refute holder_backend in waiters
    after
      send(holder.pid, {:release_session, barrier})
    end

    assert {:ok, :ok} = Task.await(holder, @detection_timeout_ms)
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @detection_timeout_ms

    results =
      for {task, monitor} <- clients do
        result = Task.await(task, @detection_timeout_ms)
        assert_receive {:DOWN, ^monitor, :process, _, :normal}, @detection_timeout_ms
        result
      end

    assert Enum.sort(Enum.map(results, &elem(&1, 0))) == [200, 409]
    assert [{200, success}] = Enum.filter(results, &(elem(&1, 0) == 200))
    assert success =~ "response.completed"
    assert [{409, refusal}] = Enum.filter(results, &(elem(&1, 0) == 409))
    assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(refusal)
    assert FakeUpstream.count(upstream) == 2
    assert [_, %Request{status: "succeeded"} = successor] = pool_requests(setup)
    assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id))
    assert successor_id == successor.id
    assert_settled_once!([first.id, successor.id])
  end

  defp await_http_lock_waiters!(holder_backend, deadline) do
    rows = Repo.query!("WITH RECURSIVE waiters(pid) AS (SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) UNION SELECT activity.pid FROM pg_stat_activity activity JOIN waiters ON waiters.pid = ANY(pg_blocking_pids(activity.pid))) SELECT pid FROM waiters", [holder_backend]).rows

    cond do
      length(rows) == 2 ->
        List.flatten(rows)

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("two independent HTTP session-lock waiters were not observed; count=#{length(rows)}")

      true ->
        receive do
        after
          10 -> :ok
        end

        await_http_lock_waiters!(holder_backend, deadline)
    end
  end

  defp input_done_event(tool_type) do
    type = if tool_type == "custom_tool_call", do: "response.custom_tool_call_input.done", else: "response.function_call_arguments.done"
    {type, %{"type" => type, "item_id" => "ctc_partial", "output_index" => 0, input_field(tool_type) => "synthetic complete input"}}
  end

  defp scenario(mode, arm, events, following \\ [], termination \\ :abrupt) do
    first = if termination == :clean, do: FakeUpstream.sse_stream(events, done: false), else: FakeUpstream.abrupt_close_mid_stream(events)
    upstream = start_upstream(FakeUpstream.strict_sequence([first | following]))
    setup = gateway_setup(upstream)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup = Map.put(setup, :serving_mode, mode)
    port = start_public_endpoint!()
    thread_id = Ecto.UUID.generate()
    {upstream, setup, port, thread_id, native_payload(setup, thread_id, arm)}
  end

  defp assert_cut!(port, setup, payload, thread_id, tool_type, completed_item? \\ false) do
    {first_status, first_body} = post_stream!(port, setup, payload, thread_id)
    assert first_status == 200
    assert first_body =~ delta_type(tool_type)
    assert first_body =~ "response.output_item.done" == completed_item?
    refute first_body =~ "response.completed"
    assert [first] = pool_requests(setup)
    assert {first.status, first.last_error_code, first.transport} == {"failed", "upstream_stream_error", "http_sse"}
    assert %DateTime{} = first.completed_at
    turn = Repo.get_by!(CodexTurn, request_id: first.id)
    assert %DateTime{} = turn.first_visible_output_at
    assert %DateTime{} = turn.completed_at
  end

  defp assert_completed_resend!(port, setup, payload, thread_id) do
    predecessor = List.last(pool_requests(setup))
    assert predecessor.status == "succeeded"
    {status, body} = post_stream!(port, setup, payload, thread_id)
    assert status == 200
    assert body =~ "response.completed"
    successor = List.last(pool_requests(setup))
    assert successor.id != predecessor.id
    assert successor.status == "succeeded"
    assert Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor.id and link.successor_request_id == ^successor.id))
  end

  defp assert_refused!(port, setup, payload, thread_id) do
    {status, body} = post_stream!(port, setup, payload, thread_id)
    assert status == 409
    assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
  end

  defp assert_settled_once!(request_ids) do
    ledger = Repo.all(from(l in LedgerEntry, where: l.request_id in ^request_ids, select: {l.request_id, l.entry_kind}))
    expected = for id <- request_ids, kind <- ["reservation", "settlement", "release"], into: %{}, do: {{id, kind}, 1}
    assert Enum.frequencies(ledger) == expected
  end

  defp apply_negative_state!(request, :expired), do: Repo.update!(Ecto.Changeset.change(request, completed_at: DateTime.add(DateTime.utc_now(), -(partial_retry_contract().retry_window_seconds + 1), :second)))
  defp apply_negative_state!(request, :missing_digest), do: Repo.update!(Ecto.Changeset.change(request, native_client_retry_digest: nil))
  defp apply_negative_state!(request, :changed_epoch), do: Repo.update!(Ecto.Changeset.change(request, native_client_retry_auth_epoch: request.native_client_retry_auth_epoch + 1))
  defp apply_negative_state!(request, :replay_generation), do: Repo.update_all(from(a in Attempt, where: a.request_id == ^request.id), set: [replay_generation: 1])

  defp apply_negative_state!(request, control) when control in [:missing_proof, :poisoned_proof, :incomplete_proof] do
    attempt = Repo.get_by!(Attempt, request_id: request.id)

    metadata =
      case control do
        :missing_proof -> Map.delete(attempt.response_metadata, "native_http_partial_tool")
        :poisoned_proof -> put_in(attempt.response_metadata, ["native_http_partial_tool", "poisoned"], true)
        :incomplete_proof -> put_in(attempt.response_metadata, ["native_http_partial_tool", "parser_complete"], false)
      end

    Repo.update!(Ecto.Changeset.change(attempt, response_metadata: metadata))
  end

  defp apply_negative_state!(_request, _control), do: :ok

  defp control_events(:completed_item), do: [{"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => %{"type" => "custom_tool_call", "id" => "ctc_partial", "call_id" => "call_partial", "name" => "synthetic_tool", "input" => "synthetic complete input", "status" => "completed"}}}]
  defp control_events(:unknown_event), do: [{"response.synthetic_unknown", %{"type" => "response.synthetic_unknown"}}]
  defp control_events(:malformed_event), do: ["event: response.custom_tool_call_input.delta\ndata: {malformed\n\n"]
  defp control_events(:truncated_event), do: ["event: response.custom_tool_call_input.delta\ndata: {"]

  defp control_events(:oversized_event) do
    limit = StreamProtocol.max_incomplete_sse_block_bytes()
    ["event: response.custom_tool_call_input.delta\ndata: " <> String.duplicate("x", limit + 1)]
  end

  defp control_events(_control), do: []

  defp partial_retry_contract, do: CompatibilityMatrix.by_slug!(:duplicate_turn_fence).duplicate_turn.partial_http_tool_retry

  defp native_payload(setup, thread_id, arm) do
    %{
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => native_input(arm),
      "stream" => true,
      "store" => false,
      "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => "partial-tool-turn", "request_kind" => "turn"})}
    }
  end

  defp native_input(:opening), do: native_text_input("synthetic partial tool retry")
  defp native_input(:tool_continuation), do: native_input(:opening) ++ [%{"type" => "function_call", "id" => "fc_history", "call_id" => "call_history", "name" => "synthetic_tool", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "call_history", "output" => "synthetic result"}]

  defp post_stream!(port, setup, payload, thread_id, register_cleanup? \\ true, extra_headers \\ []) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    if register_cleanup?, do: on_exit(fn -> Mint.HTTP.close(conn) end)
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread_id}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    headers = Enum.reject(headers, fn {name, _} -> Enum.any?(extra_headers, &(elem(&1, 0) == name)) end) ++ extra_headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))

    try do
      receive_all(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_all(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @detection_timeout_ms)

    {status, body, done?} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, next_status}, {_, body, done?} -> {next_status, body, done?}
        {:data, ^ref, data}, {status, body, done?} -> {status, body <> data, done?}
        {:done, ^ref}, {status, body, _} -> {status, body, true}
        _, acc -> acc
      end)

    if done?, do: {status, body}, else: receive_all(conn, ref, status, body)
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp partial_events(tool_type) do
    [
      {"response.created", %{"type" => "response.created", "response" => %{"id" => "resp_partial_tool", "status" => "in_progress"}}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => tool_type, "id" => "ctc_partial", "call_id" => "call_partial", "name" => "synthetic_tool", input_field(tool_type) => "", "status" => "in_progress"}}},
      {delta_type(tool_type), %{"type" => delta_type(tool_type), "item_id" => "ctc_partial", "output_index" => 0, "delta" => "synthetic partial input"}}
    ]
  end

  defp delta_type("custom_tool_call"), do: "response.custom_tool_call_input.delta"
  defp delta_type("function_call"), do: "response.function_call_arguments.delta"
  defp input_field("custom_tool_call"), do: "input"
  defp input_field("function_call"), do: "arguments"

  defp completed_event do
    {"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_partial_tool_retry", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}}
  end
end
