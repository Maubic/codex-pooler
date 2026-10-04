defmodule CodexPoolerWeb.Runtime.BackendCodexHttpMailboxPrefixTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]
  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.{NativeHttpTurnIdentity, RequestOptions}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionRegistry, ExecutionTerminalProof, ExecutionTerminalProofs}
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @budget 15_000

  for mode <- ["full", "lite"], {count, partial_tool?} <- [{1, false}, {2, false}, {1, true}], role <- [:opening, :local_summary, :remote_resume], delivery <- [:cut, :delivered] do
    @tag mode: mode, count: count, role: role, delivery: delivery, partial_tool?: partial_tool?
    test "#{mode} #{role} #{delivery} partial tool #{partial_tool?} retains first of #{count} coalesced items", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      outputs = Enum.map(1..context.count, fn n -> %{"type" => "reasoning", "id" => "rs_synthetic_#{n}", "summary" => [], "encrypted_content" => "synthetic_#{n}"} end)
      tool = %{"type" => "function_call", "id" => "fc_synthetic", "call_id" => "synthetic-call", "name" => "synthetic_tool", "arguments" => "{}"}
      chunk = Enum.map_join(outputs, fn item -> event(%{"type" => "response.output_item.done", "item" => item}) end)
      chunk = if context.partial_tool?, do: chunk <> event(%{"type" => "response.output_item.added", "output_index" => context.count, "item" => tool}), else: chunk
      gate = make_ref()
      tail = event(%{"type" => "response.reasoning_text.delta", "delta" => String.duplicate("synthetic", 10_000)})
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      # provenance: synthetic_adversarial; coalesced output followed by a held cancellation-detection tail
      terminal = FakeUpstream.sse_stream([{"response.completed", completed}])
      predecessor_response = if context.delivery == :cut, do: {:gated_terminal_sse, [chunk], [tail], self(), gate}, else: FakeUpstream.raw_response(chunk <> event(completed), headers: [{"content-type", "text/event-stream"}])
      prelude = if context.role == :local_summary, do: [terminal], else: []
      upstream = start_upstream(FakeUpstream.strict_sequence(prelude ++ [predecessor_response, terminal]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      base_input = native_text_input("synthetic")

      # provenance: local-summary/window shape from backend_codex_http_window_advance_test.exs;
      # the served window-0 opener forces the real window-1 steered claim path.
      input =
        case context.role do
          :opening -> base_input
          :local_summary -> base_input ++ native_text_input("synthetic local summary")
          :remote_resume -> base_input ++ [%{"type" => "compaction", "encrypted_content" => "synthetic_compaction"}]
        end

      payload = payload(setup, thread, input, if(context.role == :opening, do: 0, else: 1))

      prelude_id =
        if context.role == :local_summary do
          assert {200, _body} = post(port, setup, payload(setup, thread, base_input, 0), thread)
          prior = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
          assert prior.status == "succeeded"
          prior.id
        end

      retained =
        if context.delivery == :cut do
          {conn, ref} = start_request(port, setup, payload, thread)
          {conn, retained} = until_item(conn, ref, "")
          assert_receive {:fake_upstream_gate, :before_terminal, handler, ^gate}, @budget
          # Reset the real connection so the held upstream tail observes cancellation.
          :ok = :inet.setopts(Mint.HTTP.get_socket(conn), linger: {true, 0})
          Mint.HTTP.close(conn)
          send(handler, {:fake_upstream_release_gate, gate})
          retained
        else
          assert {200, _body} = post(port, setup, payload, thread)
          hd(outputs)
        end

      retained = Map.put(retained, "content", nil)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)

      if prelude_id do
        prior = Repo.get!(Request, prelude_id)
        assert first.id != prelude_id
        assert first.request_metadata["native_http_claim_arm"] == "steered_continuation"
        assert first.request_metadata["codex_session_id"] == prior.request_metadata["codex_session_id"]
        refute first.correlation_id == prior.correlation_id
      end

      attempt = Repo.get_by!(Attempt, request_id: first.id)
      turn = Repo.get_by!(CodexTurn, request_id: first.id)
      recorded = get_in(attempt.response_metadata, ["native_http_resume_progress", "output_item_done_count"])
      prefix = attempt.response_metadata["native_http_mailbox_prefix"]
      assert prefix["output_item_done_count"] == context.count
      assert length(prefix["item_digests"]) == context.count

      if context.delivery == :cut do
        assert {first.status, first.last_error_code, turn.status} == {"failed", "client_disconnected", "interrupted"}
      else
        assert {first.status, turn.status} == {"succeeded", "succeeded"}
      end

      assert recorded == context.count
      mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
      successor = Map.put(payload, "input", input ++ [retained, mailbox])
      bad = put_in(successor, ["input", Access.at(-2), "encrypted_content"], "changed_synthetic")
      {bad_status, _} = post(port, setup, bad, thread)
      assert bad_status == 409
      {call_without_output_status, _} = post(port, setup, Map.put(payload, "input", input ++ [retained, tool, mailbox]), thread)
      assert call_without_output_status == 409

      if context.count == 2 do
        {nonprefix_status, _} = post(port, setup, Map.put(payload, "input", input ++ [List.last(outputs), mailbox]), thread)
        assert nonprefix_status == 409
        {reordered_status, _} = post(port, setup, Map.put(payload, "input", input ++ Enum.reverse(outputs) ++ [mailbox]), thread)
        assert reordered_status == 409
      end

      {status, _} = post(port, setup, successor, thread)
      assert status == 200
      requests = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
      assert length(requests) == if(prelude_id, do: 3, else: 2)
      [predecessor, admitted] = Enum.take(requests, -2)
      assert predecessor.id == first.id
      assert admitted.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert Repo.aggregate(from(l in CodexPooler.Accounting.RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^admitted.id), :count) == 1

      for request <- requests do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end

      assert FakeUpstream.count(upstream) == if(prelude_id, do: 3, else: 2)
    end
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode, upstream_error_http_mailbox: true
    test "#{mode} completed HTTP item survives an upstream stream error for an addressed mailbox continuation", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      output = %{"type" => "reasoning", "id" => "rs_synthetic_interrupted", "summary" => [], "encrypted_content" => "synthetic_interrupted"}
      done = %{"type" => "response.output_item.done", "item" => output}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_successor", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      successor_gate = make_ref()
      successor_response = FakeUpstream.gated_terminal_sse_stream([], {"response.completed", completed}, notify: self(), release_ref: successor_gate)
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.abrupt_close_mid_stream([event(done)]), successor_response]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic interrupted stream")
      original = payload(setup, thread, input, 0)
      # The client reads the entire public stream before closing; only the
      # upstream socket is killed by the normal FakeUpstream failure mode.
      assert {200, wire} = post(port, setup, original, thread)
      [first_event | _] = String.split(wire, "\n\n", trim: true)
      [json] = for "data: " <> value <- String.split(first_event, "\n"), do: value
      assert %{"type" => "response.output_item.done", "item" => retained} = CodexPooler.JSON.decode!(json)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      turn = Repo.get_by!(CodexTurn, request_id: first.id)
      assert {first.status, first.last_error_code} == {"failed", "upstream_stream_error"}
      assert {attempt.status, attempt.network_error_code, attempt.replay_generation} == {"failed", "upstream_stream_error", 0}
      assert turn.request_id == first.id
      assert turn.final_attempt_id == attempt.id
      assert turn.status == "failed"
      assert first.completed_at && attempt.completed_at && turn.completed_at
      assert ClientRetry.original_witness_eligible?(first)
      assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^first.id and l.entry_kind == "settlement"), :count) == 1
      assert get_in(attempt.response_metadata, ["native_http_resume_progress", "output_item_done_count"]) == 1
      prefix = attempt.response_metadata["native_http_mailbox_prefix"]
      assert prefix["output_item_done_count"] == 1
      assert length(prefix["item_digests"]) == 1
      proof = await_scoped_execution_proof!(attempt, System.monotonic_time(:millisecond) + @budget)
      mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
      successor = Map.put(original, "input", input ++ [Map.put(retained, "content", nil), mailbox])
      assert_http_stream_mailbox_fences!(turn, first, attempt, successor)

      for invalid <- [put_in(successor, ["input", Access.at(-2), "encrypted_content"], "foreign_synthetic"), Map.put(successor, "input", input ++ [mailbox]), Map.put(successor, "previous_response_id", "resp_synthetic_unavailable")] do
        {invalid_status, _body} = post(port, setup, invalid, thread)
        refute invalid_status == 200
        assert FakeUpstream.count(upstream) == 1
      end

      %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
      expired_at = DateTime.add(db_now, -31, :second)

      try do
        Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [completed_at: expired_at])
        Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id), set: [completed_at: expired_at])
        Repo.update_all(from(t in CodexTurn, where: t.id == ^turn.id), set: [completed_at: expired_at])
        {expired_status, _body} = post(port, setup, successor, thread)
        assert expired_status == 409
        assert FakeUpstream.count(upstream) == 1
      after
        Repo.update_all(from(r in Request, where: r.id == ^first.id), set: [completed_at: first.completed_at])
        Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id), set: [completed_at: attempt.completed_at])
        Repo.update_all(from(t in CodexTurn, where: t.id == ^turn.id), set: [completed_at: turn.completed_at])
      end

      pending = Task.async(fn -> ExUnit.CaptureLog.with_log(fn -> post(port, setup, successor, thread, false) end) end)
      pending_monitor = Process.monitor(pending.pid)
      assert_receive {:fake_upstream_gate, :before_terminal, successor_handler, ^successor_gate}, @budget
      on_exit(fn -> send(successor_handler, {:fake_upstream_release_gate, successor_gate}) end)

      {{status, _body}, log} =
        try do
          active = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)
          assert active.id != first.id
          assert active.status == "in_progress"
          assert is_nil(active.completed_at)
          assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
          assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^active.id), :count) == 1
          assert Repo.aggregate(from(l in CodexPooler.Accounting.RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^active.id), :count) == 1
          {duplicate_status, _body} = post(port, setup, successor, thread)
          assert duplicate_status == 409
          assert FakeUpstream.count(upstream) == 2
          assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
          assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^active.id), :count) == 1
          assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^active.id and l.entry_kind == "settlement"), :count) == 0
          if System.get_env("HTTP_MAILBOX_REGRESSION_DIAGNOSTICS") == "1", do: IO.puts(CodexPooler.JSON.encode!(%{scenario: "active_successor_duplicate", active_request_id: active.id, active_status: active.status, duplicate_status: duplicate_status, dispatches: FakeUpstream.count(upstream), request_count: Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count)}))
          send(successor_handler, {:fake_upstream_release_gate, successor_gate})
          result = Task.await(pending, @budget)
          assert_receive {:DOWN, ^pending_monitor, :process, _pid, :normal}, @budget
          result
        after
          send(successor_handler, {:fake_upstream_release_gate, successor_gate})
          if Process.alive?(pending.pid), do: Task.shutdown(pending, :brutal_kill)
        end

      if System.get_env("HTTP_MAILBOX_REGRESSION_DIAGNOSTICS") == "1" do
        IO.puts(CodexPooler.JSON.encode!(%{mode: context.mode, request_id: first.id, attempt_id: attempt.id, turn_id: turn.id, request_status: first.status, request_error: first.last_error_code, attempt_status: attempt.status, attempt_error: attempt.network_error_code, generation: attempt.replay_generation, turn_status: turn.status, first_visible_output: not is_nil(turn.first_visible_output_at), completed_items: prefix["output_item_done_count"], prefix_digest_count: length(prefix["item_digests"]), receipt_outcome: get_in(attempt.response_metadata, ["downstream_delivery", "outcome"]), receipt_terminal_class: get_in(attempt.response_metadata, ["downstream_delivery", "terminal_class"]), execution_terminal: ExecutionTerminalProofs.terminal?(attempt), proof_kind: proof.end_kind, proof_interruption: proof.interruption_code, successor_status: status, rejected_at_settlement: String.contains?(log, "mailbox_check=settlement"), dispatches: FakeUpstream.count(upstream)}))
      end

      assert status == 200
      requests = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
      assert length(requests) == 2
      assert List.last(requests).request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert FakeUpstream.count(upstream) == 2

      for request <- requests do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end
    end
  end

  defp assert_http_stream_mailbox_fences!(turn, request, attempt, successor) do
    session = Repo.get!(CodexSession, turn.codex_session_id)
    options = RequestOptions.build(%{transport: "http_sse", codex_session: session, api_key_runtime_epoch: request.native_client_retry_auth_epoch}, @path, %{})
    assert {:ok, claim} = NativeHttpTurnIdentity.request_claim(options, successor)
    witness = claim.native_client_retry_witness
    assert [_candidate] = witness.mailbox
    assert %{stage: :verified} = ClientRetry.mailbox_check(turn, request, attempt, witness, nil, :same_session)

    absent = %{attempt | owner_execution_id: Ecto.UUID.generate()}
    foreign = %{attempt | owner_instance_boot_id: "foreign-synthetic-boot"}

    for invalid <- [absent, foreign, %{attempt | replay_generation: 1}, %{attempt | status: "in_progress"}, %{attempt | completed_at: nil}] do
      assert %{stage: :settlement} = ClientRetry.mailbox_check(turn, request, invalid, witness, nil, :same_session)
    end

    assert %{stage: :settlement} = ClientRetry.mailbox_check(%{turn | final_attempt_id: Ecto.UUID.generate()}, request, attempt, witness, nil, :same_session)
    assert %{stage: :authorization} = ClientRetry.mailbox_check(turn, request, attempt, %{witness | auth_epoch: witness.auth_epoch + 1}, nil, :same_session)
    assert %{stage: :session} = ClientRetry.mailbox_check_for_session(turn, request, attempt, witness, nil, Ecto.UUID.generate(), %{pool_id: request.pool_id, api_key_id: request.api_key_id})

    for metadata <- [Map.drop(attempt.response_metadata, ["native_http_resume_progress", "native_http_mailbox_prefix"]), Map.put(Map.delete(attempt.response_metadata, "native_http_resume_progress"), "native_http_mailbox_prefix", %{"version" => 1, "output_item_done_count" => 0, "item_digests" => []}), Map.put(Map.delete(attempt.response_metadata, "native_http_resume_progress"), "native_http_mailbox_prefix", %{"version" => 1, "output_item_done_count" => 1, "item_digests" => ["000000000000"]})] do
      assert %{stage: :output_prefix} = ClientRetry.mailbox_check(turn, request, %{attempt | response_metadata: metadata}, witness, nil, :same_session)
    end

    ended = %{witness | mailbox: Enum.map(witness.mailbox, &Map.put(&1, :current?, false))}
    assert %{stage: :ending} = ClientRetry.mailbox_check(turn, request, attempt, ended, nil, :same_session)
    assert {:ok, foreign_model} = NativeHttpTurnIdentity.request_claim(options, Map.put(successor, "model", "foreign-synthetic-model"))
    assert [_candidate] = foreign_model.native_client_retry_witness.mailbox
    refute ClientRetry.mailbox_check(turn, request, attempt, foreign_model.native_client_retry_witness, nil, :same_session).stage == :verified
    if System.get_env("HTTP_MAILBOX_REGRESSION_DIAGNOSTICS") == "1", do: IO.puts(CodexPooler.JSON.encode!(%{scenario: "recognized_http_upstream_error_mailbox_fences", candidate_verified: true, checks: ["absent_proof", "foreign_proof_boot", "generation_one", "active_attempt", "unfinished_attempt", "foreign_final_attempt", "auth_epoch", "foreign_session", "missing_prefix", "zero_prefix", "foreign_prefix", "ending", "foreign_model"]}))
  end

  defp await_scoped_execution_proof!(attempt, deadline) do
    registry_node = Enum.find([node() | Node.list(:connected)], &(Atom.to_string(&1) == attempt.owner_instance_id))
    assert registry_node, "the actual HTTP executor's registry node is unavailable"
    registry = {ExecutionRegistry, registry_node}

    case ExecutionRegistry.pending_proofs([attempt.owner_execution_id], registry) do
      [proof] ->
        fields = [:owner_execution_id, :owner_instance_id, :owner_instance_boot_id, :owner_process_id]
        assert Map.take(proof, fields) == Map.take(attempt, fields)
        assert proof.end_kind in ["completed", "process_down"]
        assert ExecutionIdentity.status(attempt) == :dead

        CodexPooler.UnboxedFixture.register_unboxed_cleanup!(fn ->
          Repo.delete_all(from p in ExecutionTerminalProof, where: p.execution_id == ^attempt.owner_execution_id and p.owner_instance_id == ^attempt.owner_instance_id and p.owner_instance_boot_id == ^attempt.owner_instance_boot_id and p.owner_process_id == ^attempt.owner_process_id)
          :ok = ExecutionRegistry.acknowledge([attempt.owner_execution_id], registry)
        end)

        assert {:ok, 1} = ExecutionTerminalProofs.publish([proof])
        assert ExecutionTerminalProofs.terminal?(attempt)
        assert :ok = ExecutionRegistry.acknowledge([attempt.owner_execution_id], registry)
        scoped_execution_proof!(attempt)

      [] ->
        if ExecutionTerminalProofs.terminal?(attempt) do
          scoped_execution_proof!(attempt)
        else
          remaining = deadline - System.monotonic_time(:millisecond)
          assert remaining > 0, "the actual HTTP executor's normal terminal proof did not become available"

          receive do
          after
            min(5, remaining) -> await_scoped_execution_proof!(attempt, deadline)
          end
        end

      :unknown ->
        flunk("the actual HTTP executor's registry is unavailable")
    end
  end

  defp scoped_execution_proof!(attempt) do
    Repo.one!(from p in ExecutionTerminalProof, where: p.execution_id == ^attempt.owner_execution_id and p.owner_instance_id == ^attempt.owner_instance_id and p.owner_instance_boot_id == ^attempt.owner_instance_boot_id and p.owner_process_id == ^attempt.owner_process_id)
  end

  defp payload(setup, thread, input, window) do
    %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:#{window}", "window_number" => window})}}
  end

  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  defp start_request(port, setup, payload, thread, register_exit? \\ true) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    if register_exit?, do: on_exit(fn -> Mint.HTTP.close(conn) end)
    metadata = payload["client_metadata"]["x-codex-turn-metadata"]
    window = CodexPooler.JSON.decode!(metadata)["window_number"]
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:#{window}"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp until_item(conn, ref, acc) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    bytes =
      Enum.reduce(responses, acc, fn
        {:data, ^ref, data}, a -> a <> data
        _, a -> a
      end)

    if String.contains?(bytes, "\n\n") do
      [block | _ignored_after_preemption] = String.split(bytes, "\n\n")
      [data] = for "data: " <> json <- String.split(block, "\n"), do: json
      assert %{"type" => "response.output_item.done", "item" => item} = CodexPooler.JSON.decode!(data)
      {conn, item}
    else
      until_item(conn, ref, bytes)
    end
  end

  defp post(port, setup, payload, thread, register_exit? \\ true) do
    {conn, ref} = start_request(port, setup, payload, thread, register_exit?)

    try do
      all(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp all(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _, a -> a
      end)

    if done, do: {status, body}, else: all(conn, ref, status, body)
  end

  defp await_latest_settled(setup, deadline) do
    row = Repo.one(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)

    cond do
      row && row.completed_at ->
        row

      System.monotonic_time(:millisecond) > deadline ->
        flunk("request never finalized")

      true ->
        Process.sleep(10)
        await_latest_settled(setup, deadline)
    end
  end
end
