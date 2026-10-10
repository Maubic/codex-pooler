defmodule CodexPoolerWeb.Runtime.BackendCodexHttpIncompleteToolTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPooler.PoolerFixtures, only: [request_fixture: 2, attempt_fixture: 3]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, NativeHttpToolObservation, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Runtime.Streaming.{DownstreamStream, StreamAttempt}
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  @budget 15_000
  @moduletag capture_log: true

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation], tool? <- [false, true], schedule <- [:whole, :split, :coalesced] do
    test "#{mode} #{arm} tool=#{tool?} #{schedule} max-output terminal is not missing EOF" do
      scenario(unquote(mode), unquote(arm), unquote(tool?), unquote(schedule))
    end
  end

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation], outcome <- [:quota, :eof, :completed, :retained_tail] do
    test "#{mode} #{arm} #{outcome} keeps terminal semantics separate from tool EOF" do
      scenario(unquote(mode), unquote(arm), true, :whole, unquote(outcome))
    end
  end

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation], tool? <- [false, true] do
    @tag :explicit_resend_policy
    test "#{mode} #{arm} tool=#{tool?} explicit resend differs from poisoned recovery" do
      scenario(unquote(mode), unquote(arm), unquote(tool?), :whole, :incomplete, :policy)
    end
  end

  defp scenario(mode, arm, tool?, schedule, outcome \\ :incomplete, replay \\ :none) do
    prefix = event("response.created", %{"response" => %{"status" => "in_progress", "output" => []}}) <> if(tool?, do: tool_event(), else: "")
    terminal = terminal_event(outcome)
    ref = make_ref()

    chunks = stream_chunks(prefix, terminal, schedule)
    first_mode = first_mode(schedule, chunks, ref)
    {setup, first, second} = stream_retry_setup(first_mode)
    use_routing_strategy!(setup.pool, "least_recent_success", 2)
    seed_request = request_fixture(setup, %{model_id: setup.model.id})
    attempt_fixture(seed_request, setup.fallback_assignment, %{completed_at: DateTime.utc_now()})
    timestamp = DateTime.utc_now()
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    {listener, port} = start_public_endpoint_with_server!()
    listener_monitor = Process.monitor(listener)
    ownership = capture_public_endpoint_ownership!(listener)
    thread = Ecto.UUID.generate()
    document = CodexPooler.JSON.encode!(%{"session_id" => thread, "thread_id" => thread, "turn_id" => "sample-incomplete-turn", "request_kind" => "turn"})
    payload = %{"model" => setup.model.exposed_model_id, "instructions" => "synthetic", "input" => input(arm), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => document}}
    headers = [{"authorization", setup.authorization}, {"session-id", thread}, {"x-request-id", deterministic_rotation_seed(2, 0)}, {"originator", "codex_cli_rs"}]
    headers = if mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    source_mfa = {StreamAttempt, :classify_first_event, 3}
    outcome_mfa = {DownstreamStream, :terminal_outcome, 1}
    Code.ensure_loaded!(StreamAttempt)
    Code.ensure_loaded!(DownstreamStream)

    on_exit(fn ->
      :erlang.trace(:new_processes, false, [:call])
      :erlang.trace_pattern(source_mfa, false, [:local])
      :erlang.trace_pattern(outcome_mfa, false, [:local])
    end)

    :erlang.trace_pattern(source_mfa, [{[:"$1", :_, :_], [{:is_binary, :"$1"}], [{:message, {:byte_size, :"$1"}}]}], [:local])
    :erlang.trace_pattern(outcome_mfa, [{:_, [], [{:return_trace}]}], [:local])
    :erlang.trace(:new_processes, true, [:call, :arity, {:tracer, self()}])
    supervisor = start_supervised!(Task.Supervisor)
    task = Task.Supervisor.async_nolink(supervisor, fn -> Req.post!("http://127.0.0.1:#{port}/backend-api/codex/responses", json: payload, headers: headers, retry: false, receive_timeout: @budget) end)
    monitor = Process.monitor(task.pid)

    first_lengths = release_first_chunk(schedule, ref, source_mfa)

    response = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    delivered = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^delivered}, @budget
    lengths = first_lengths ++ chunk_lengths(source_mfa, [])
    assert Enum.sum(lengths) == byte_size(prefix <> terminal)
    :erlang.trace(:new_processes, false, [:call])
    :erlang.trace_pattern(source_mfa, false, [:local])
    :erlang.trace_pattern(outcome_mfa, false, [:local])
    assert response.status == 200

    assert_wire(outcome, response.body, prefix <> terminal)

    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == 0
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^seed_request.id)
    assert request.request_metadata["native_http_claim_arm"] == Atom.to_string(arm)
    assert request.native_client_retry_version == 1
    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    assert attempt.response_metadata["routing"]["model_serving_mode"] == mode
    assert_terminal(outcome, request, attempt, outcome_mfa)

    assert request.retry_count == 0

    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    policy = verify_resends(replay, port, headers, payload, setup, seed_request, first, second)
    evidence(%{mode: mode, arm: arm, tool: tool?, schedule: schedule, outcome: outcome}, lengths, request, attempt, policy)
    :ok = ThousandIsland.stop(listener)
    assert_receive {:DOWN, ^listener_monitor, :process, ^listener, _}, @budget
    assert_public_endpoint_released!(ownership)
  end

  defp stream_chunks(prefix, terminal, :whole), do: [prefix, terminal]
  defp stream_chunks(prefix, terminal, :coalesced), do: [prefix <> terminal]

  defp stream_chunks(prefix, terminal, :split) do
    at = div(byte_size(terminal), 2)
    [prefix <> binary_part(terminal, 0, at), binary_part(terminal, at, byte_size(terminal) - at)]
  end

  defp first_mode(:coalesced, chunks, _ref), do: {:sse, chunks}
  defp first_mode(_schedule, chunks, ref), do: FakeUpstream.barrier_sse_stream(chunks, barrier_after: 1, notify: self(), release_ref: ref, done: false)

  defp release_first_chunk(:coalesced, _ref, _mfa), do: []

  defp release_first_chunk(_schedule, ref, mfa) do
    assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^ref}, @budget
    assert_receive {:trace, _, :call, ^mfa, bytes}, @budget
    assert is_integer(bytes) and bytes > 0
    send(handler, {:fake_upstream_release_chunk, ref})
    [bytes]
  end

  defp assert_wire(:quota, body, _original) do
    assert body =~ "insufficient_quota"
    assert body =~ "response.failed"
  end

  defp assert_wire(_outcome, body, original), do: assert(body == original)

  defp assert_terminal(outcome, request, attempt, outcome_mfa) do
    observation = attempt.response_metadata["native_http_partial_tool"]

    if outcome == :eof do
      assert NativeHttpToolObservation.eligible_metadata?(observation)
      assert request.last_error_code == "upstream_stream_error"
      assert request.status == "failed"
      assert attempt.status == "failed"
    else
      assert observation["poisoned"] == true
      refute NativeHttpToolObservation.eligible_metadata?(observation)
      assert request.status == if(outcome == :quota, do: "failed", else: "succeeded")
      assert attempt.status == request.status
      assert request.last_error_code == if(outcome == :quota, do: "insufficient_quota", else: nil)
      assert attempt.network_error_code == request.last_error_code
      assert request.usage_status == "usage_known"
      assert attempt.usage_status == "usage_known"
      assert [settlement] = Repo.all(from l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement")
      assert settlement.input_tokens == 4
      assert settlement.output_tokens == 16
      assert settlement.total_tokens == 20
      assert attempt.response_metadata["downstream_delivery"]["outcome"] == "delivered"

      if outcome in [:incomplete, :retained_tail] do
        assert_received {:trace, _, :return_from, ^outcome_mfa, :incomplete}
        assert attempt.response_metadata["downstream_delivery"]["terminal_class"] == "response.incomplete"
        assert attempt.response_metadata["downstream_delivery"]["incomplete_reason"] == "max_output_tokens"
      end
    end
  end

  defp verify_resends(:none, _port, _headers, _payload, _setup, _seed, _first, _second), do: %{}

  defp verify_resends(:policy, port, headers, payload, setup, seed, first, second) do
    url = "http://127.0.0.1:#{port}/backend-api/codex/responses"
    FakeUpstream.set_mode(first, stream_success_sse())
    changed = Req.post!(url, json: Map.put(payload, "instructions", "changed synthetic"), headers: headers, retry: false, receive_timeout: @budget)
    original = hd(scenario_requests(setup, seed))
    continuation? = original.request_metadata["native_http_claim_arm"] == "tool_continuation"
    assert changed.status == if(continuation?, do: 200, else: 409)
    changed_requests = scenario_requests(setup, seed)

    assert_changed_request(continuation?, changed, changed_requests, original, first)

    before_identical_calls = if continuation?, do: 2, else: 1
    assert FakeUpstream.http_request_count(first) == before_identical_calls
    assert FakeUpstream.http_request_count(second) == 0
    FakeUpstream.set_mode(first, stream_success_sse())
    identical = Req.post!(url, json: payload, headers: headers, retry: false, receive_timeout: @budget)
    assert identical.status == 200
    assert identical.body =~ "response.completed"
    assert FakeUpstream.http_request_count(first) == before_identical_calls + 1
    assert FakeUpstream.http_request_count(second) == 0
    requests = scenario_requests(setup, seed)
    assert length(requests) == before_identical_calls + 1
    assert List.last(requests).request_metadata["client_resend"]["predecessor_request_id"] == original.id
    assert List.last(requests).native_client_retry_digest == original.native_client_retry_digest

    for request <- requests do
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    end

    %{changed_status: changed.status, changed_admission: if(continuation?, do: "new_payload_scoped_continuation", else: "refused"), explicit_identical_status: identical.status, provider_calls: before_identical_calls + 1, requests: length(requests)}
  end

  defp scenario_requests(setup, seed), do: Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^seed.id, order_by: r.admitted_at)

  defp assert_changed_request(continuation?, changed, changed_requests, original, first) do
    if continuation? do
      assert length(changed_requests) == 2
      changed_request = List.last(changed_requests)
      assert changed_request.native_client_retry_digest != original.native_client_retry_digest
      assert changed_request.request_metadata["codex_session_id"] == original.request_metadata["codex_session_id"]
      refute changed_request.request_metadata["client_resend"]
      assert String.starts_with?(changed_request.correlation_id, "codex-request:")
      [original_wire, changed_wire] = FakeUpstream.requests(first)
      effective_payload_changed? = Map.take(original_wire.json, ["instructions", "input"]) != Map.take(changed_wire.json, ["instructions", "input"])
      assert effective_payload_changed?
    else
      assert changed.body["error"]["code"] == "duplicate_turn"
      assert length(changed_requests) == 1
    end
  end

  defp terminal_event(:eof), do: ""

  defp terminal_event(outcome) do
    type = if outcome == :completed, do: "response.completed", else: "response.incomplete"
    response = %{"status" => if(outcome == :completed, do: "completed", else: "incomplete"), "output" => [], "usage" => %{"input_tokens" => 4, "output_tokens" => 16, "total_tokens" => 20}}
    response = if outcome == :completed, do: response, else: Map.put(response, "incomplete_details", %{"reason" => if(outcome == :quota, do: "insufficient_quota", else: "max_output_tokens")})
    response = if outcome == :retained_tail, do: Map.put(response, "padding", String.duplicate("x", 70_000)), else: response
    event(type, %{"response" => response})
  end

  defp chunk_lengths(mfa, values) do
    receive do
      {:trace, _, :call, ^mfa, bytes} when is_integer(bytes) -> chunk_lengths(mfa, [bytes | values])
    after
      0 -> Enum.reverse(values)
    end
  end

  defp evidence(%{mode: mode, arm: arm, tool: tool?, schedule: schedule, outcome: outcome}, lengths, request, attempt, policy) do
    if directory = System.get_env("INCOMPLETE_TOOL_EVIDENCE") do
      path = Path.join(directory, "#{mode}-#{arm}-#{tool?}-#{schedule}-#{outcome}-#{map_size(policy)}.json")
      File.write!(path, CodexPooler.JSON.encode!(%{mode: mode, arm: arm, tool: tool?, schedule: schedule, outcome: outcome, source_chunk_lengths: lengths, explicit_request_policy: policy, request_status: request.status, attempt_status: attempt.status, error: request.last_error_code, usage: request.usage_status, retry_count: request.retry_count, delivery: Map.take(attempt.response_metadata["downstream_delivery"] || %{}, ~w(outcome terminal_class incomplete_reason frames_after_visible))}))
    end
  end

  defp input(:opening), do: native_text_input("synthetic incomplete")
  defp input(:tool_continuation), do: input(:opening) ++ [%{"type" => "function_call", "id" => "fc_history", "call_id" => "call_history", "name" => "sample_tool", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "call_history", "output" => "synthetic result"}]
  defp tool_event, do: event("response.output_item.added", %{"output_index" => 0, "item" => %{"id" => "fc_sample", "type" => "function_call", "name" => "sample_tool", "call_id" => "call_sample", "arguments" => "", "status" => "in_progress"}})
  defp event(type, fields), do: "event: #{type}\ndata: " <> CodexPooler.JSON.encode!(Map.put(fields, "type", type)) <> "\n\n"
end
