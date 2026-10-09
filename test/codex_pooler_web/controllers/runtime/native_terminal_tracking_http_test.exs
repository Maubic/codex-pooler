defmodule CodexPoolerWeb.Runtime.NativeTerminalTrackingHttpTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPooler.PoolerFixtures, only: [request_fixture: 2, attempt_fixture: 3]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Runtime.Streaming.StreamAttempt
  alias CodexPooler.Gateway.Transports.Streaming.StreamRelay
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  @budget 15_000
  @moduletag capture_log: true

  for mode <- ["full", "lite"], tool? <- [false, true], scenario <- [:completion_1mib, :completion_17mib, :completion_whole_17mib, :completion_crlf_17mib, :completion_eof_17mib, :cumulative_21mib, :ordinary_17mib_resync, :candidate_overflow_unknown, :candidate_failed_overflow_unknown, :whole_failure, :split_failure, :coalesced_failure] do
    @tag slow: "physical large native terminal and overflow boundary matrix"
    test "#{mode} tool=#{tool?} #{scenario} preserves native source and delivery terminal" do
      run_scenario(unquote(mode), unquote(scenario), unquote(tool?))
    end
  end

  for mode <- ["full", "lite"], failure? <- [false, true] do
    test "public #{mode} failure=#{failure?} keeps legacy receipt without native observation metadata" do
      terminal = if unquote(failure?), do: failure(), else: completion(0)
      {setup, first, second} = stream_retry_setup({:sse, [delta(), terminal]})
      timestamp = DateTime.utc_now()
      Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: unquote(mode), created_at: timestamp, updated_at: timestamp})
      {listener, port} = start_public_endpoint_with_server!()
      response = Req.post!("http://127.0.0.1:#{port}/v1/responses", json: %{"model" => setup.model.exposed_model_id, "input" => "synthetic public control", "stream" => true}, headers: [{"authorization", setup.authorization}, {"x-request-id", deterministic_rotation_seed(2, 0)}], retry: false)
      assert response.status == 200
      assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
      refute Map.has_key?(attempt.response_metadata, "native_sse_observation")
      assert request.status == if(unquote(failure?), do: "failed", else: "succeeded")
      assert attempt.response_metadata["downstream_delivery"]["outcome"] == "delivered"
      assert attempt.response_metadata["downstream_delivery"]["terminal_class"] == if(unquote(failure?), do: "response.failed", else: "response.completed")
      assert FakeUpstream.http_request_count(first) == 1
      assert FakeUpstream.http_request_count(second) == 0
      assert request.retry_count == 0
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      monitor = Process.monitor(listener)
      :ok = ThousandIsland.stop(listener)
      assert_receive {:DOWN, ^monitor, :process, ^listener, _}, @budget
    end
  end

  defp run_scenario(mode, scenario, tool?) do
    {chunks, terminal} = chunks(scenario)
    chunks = if tool?, do: [tool_event() | chunks], else: chunks
    ref = make_ref()
    held? = length(chunks) > 1
    barrier_after = if scenario == :split_failure, do: length(chunks) - 1, else: 1
    source = if held?, do: FakeUpstream.barrier_sse_stream(chunks, barrier_after: barrier_after, notify: self(), release_ref: ref, done: false, on_client_close: :expected), else: {:sse, chunks}
    {setup, first, second} = stream_retry_setup(source)
    use_routing_strategy!(setup.pool, "least_recent_success", 2)
    seed_request = request_fixture(setup, %{model_id: setup.model.id})
    attempt_fixture(seed_request, setup.fallback_assignment, %{completed_at: DateTime.utc_now()})
    thread = Ecto.UUID.generate()
    document = CodexPooler.JSON.encode!(%{"session_id" => thread, "thread_id" => thread, "turn_id" => "sample-tracking-turn", "request_kind" => "turn"})
    payload = %{"model" => setup.model.exposed_model_id, "instructions" => "synthetic", "input" => native_text_input("synthetic native tracking"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => document}}
    headers = [{"authorization", setup.authorization}, {"session-id", thread}, {"x-request-id", deterministic_rotation_seed(2, 0)}, {"originator", "codex_cli_rs"}]
    timestamp = DateTime.utc_now()
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    {listener, port} = start_public_endpoint_with_server!()
    listener_monitor = Process.monitor(listener)
    source_mfa = {StreamAttempt, :classify_first_event, 3}
    cancel_mfa = {StreamRelay, :source_cancel, 1}
    Code.ensure_loaded!(StreamAttempt)
    Code.ensure_loaded!(StreamRelay)

    on_exit(fn ->
      :erlang.trace(:new_processes, false, [:call])
      :erlang.trace_pattern(source_mfa, false, [:local])
      :erlang.trace_pattern(cancel_mfa, false, [:local])
    end)

    :erlang.trace_pattern(source_mfa, [{[:"$1", :_, :_], [{:is_binary, :"$1"}], [{:message, {:byte_size, :"$1"}}]}], [:local])
    :erlang.trace_pattern(cancel_mfa, [{:_, [], [{:message, :source_cancel}]}], [:local])
    :erlang.trace(:new_processes, true, [:call, :arity, {:tracer, self()}])
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Req.post!("http://127.0.0.1:#{port}/backend-api/codex/responses", json: payload, headers: headers, retry: false, receive_timeout: @budget)
      end)

    task_monitor = Process.monitor(task.pid)

    initial_lengths =
      if held? do
        assert_receive {:fake_upstream_chunk_barrier, ^barrier_after, handler, ^ref}, @budget
        prefix_bytes = chunks |> Enum.take(barrier_after) |> IO.iodata_length()
        witnessed = receive_source_bytes(source_mfa, prefix_bytes, [])
        send(handler, {:fake_upstream_release_chunk, ref})
        witnessed
      else
        []
      end

    response = Task.await(task, @budget)
    assert_receive {:DOWN, ^task_monitor, :process, _, :normal}, @budget
    delivered = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^delivered}, @budget
    lengths = initial_lengths ++ source_lengths(source_mfa, [])
    cancellations = cancel_count(cancel_mfa, 0)
    :erlang.trace(:new_processes, false, [:call])
    :erlang.trace_pattern(source_mfa, false, [:local])
    :erlang.trace_pattern(cancel_mfa, false, [:local])
    assert response.status == 200
    expected_bytes = IO.iodata_length(chunks)
    assert Enum.sum(lengths) == expected_bytes
    expected_wire = IO.iodata_to_binary(chunks) |> String.replace("\r\n", "\n")
    expected_wire = if scenario == :completion_eof_17mib, do: expected_wire <> "\n\n", else: expected_wire
    assert :crypto.hash(:sha256, response.body) == :crypto.hash(:sha256, expected_wire)
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id and r.id != ^seed_request.id)
    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == 0
    assert request.retry_count == 0
    assert attempt.response_metadata["routing"]["model_serving_mode"] == mode
    record(mode, scenario, tool?, lengths, cancellations, request, attempt, expected_bytes)
    assert_outcome(scenario, terminal, tool?, cancellations, request, attempt)
    :ok = ThousandIsland.stop(listener)
    assert_receive {:DOWN, ^listener_monitor, :process, ^listener, _}, @budget
    assert {:error, :econnrefused} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000)
  end

  defp assert_outcome(scenario, terminal, tool?, cancellations, request, attempt) do
    expected_status = if is_nil(expected_error(terminal, tool?)), do: "succeeded", else: "failed"
    assert request.usage_status == "usage_known"
    assert attempt.usage_status == "usage_known"
    assert observation = attempt.response_metadata["native_sse_observation"]
    assert observation["terminal_observed"] == not is_nil(terminal)
    assert observation["overflow_count"] == if(scenario in [:ordinary_17mib_resync, :candidate_overflow_unknown, :candidate_failed_overflow_unknown], do: 1, else: 0)
    assert observation["residue_bytes"] == 0
    assert observation["discard_carry_bytes"] == 0
    assert observation["discarding"] == false
    assert attempt.response_metadata["native_http_partial_tool"]["poisoned"] == true
    assert request.status == expected_status
    assert attempt.status == expected_status
    assert attempt.response_metadata["downstream_delivery"]["outcome"] == if(is_nil(terminal), do: "completed", else: "delivered")
    assert attempt.response_metadata["downstream_delivery"]["terminal_class"] == (terminal || "none")
    assert cancellations == if(terminal == "response.failed", do: 1, else: 0)

    assert request.last_error_code == expected_error(terminal, tool?)
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
  end

  defp expected_error("response.failed", _tool?), do: "server_error"
  defp expected_error(nil, true), do: "upstream_stream_error"
  defp expected_error(_terminal, _tool?), do: nil

  defp chunks(:completion_1mib), do: {[delta() | split(completion(1_048_576), 65_536)], "response.completed"}
  defp chunks(:completion_17mib), do: {[delta() | split(completion(17 * 1_048_576), 65_536)], "response.completed"}
  defp chunks(:completion_whole_17mib), do: {[delta(), completion(17 * 1_048_576)], "response.completed"}
  defp chunks(:completion_crlf_17mib), do: {[delta() | split(String.replace(completion(17 * 1_048_576), "\n", "\r\n"), 65_536)], "response.completed"}
  defp chunks(:completion_eof_17mib), do: {[delta() | split(String.trim_trailing(completion(17 * 1_048_576), "\n"), 65_536)], "response.completed"}

  defp chunks(:cumulative_21mib) do
    event = "event: response.output_text.delta\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.output_text.delta", "delta" => String.duplicate("x", 1_048_576)}) <> "\n\n"
    {[delta() | List.duplicate(event, 21)] ++ [completion(0)], "response.completed"}
  end

  defp chunks(:ordinary_17mib_resync) do
    event = "event: response.output_item.done\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => %{"type" => "reasoning", "encrypted_content" => String.duplicate("x", 17 * 1_048_576)}}) <> "\n\n"
    {[delta() | split(event, 65_536)] ++ [completion(0)], "response.completed"}
  end

  defp chunks(:candidate_overflow_unknown), do: {[delta() | split(completion(64 * 1_048_576), 65_536)], nil}

  defp chunks(:candidate_failed_overflow_unknown) do
    terminal = "event: response.failed\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "padding" => String.duplicate("x", 64 * 1_048_576), "error" => %{"code" => "server_error"}, "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}) <> "\n\n"
    {[delta(), terminal], nil}
  end

  defp chunks(:whole_failure), do: {[delta(), failure()], "response.failed"}
  defp chunks(:split_failure), do: {[delta(), "da", binary_part(failure(), 2, byte_size(failure()) - 2)], "response.failed"}
  defp chunks(:coalesced_failure), do: {[delta() <> failure()], "response.failed"}
  defp tool_event, do: "event: response.output_item.added\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"type" => "function_call", "id" => "fc_synthetic", "call_id" => "call_synthetic", "name" => "sample_tool", "arguments" => "", "status" => "in_progress"}}) <> "\n\n"
  defp delta, do: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\n"
  defp failure, do: "data: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "server_error"}, "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}) <> "\n\n"
  defp completion(size), do: "event: response.completed\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"status" => "completed", "padding" => String.duplicate("x", size), "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}) <> "\n\n"
  defp split(binary, size) when byte_size(binary) <= size, do: [binary]

  defp split(binary, size) do
    <<part::binary-size(^size), rest::binary>> = binary
    [part | split(rest, size)]
  end

  defp receive_source_bytes(_mfa, 0, acc), do: Enum.reverse(acc)

  defp receive_source_bytes(mfa, remaining, acc) do
    assert_receive {:trace, _, :call, ^mfa, bytes}, @budget
    assert bytes > 0 and bytes <= remaining
    receive_source_bytes(mfa, remaining - bytes, [bytes | acc])
  end

  defp source_lengths(mfa, acc) do
    receive do
      {:trace, _, :call, ^mfa, bytes} when is_integer(bytes) -> source_lengths(mfa, [bytes | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp cancel_count(mfa, count) do
    receive do
      {:trace, _, :call, ^mfa, :source_cancel} -> cancel_count(mfa, count + 1)
    after
      0 -> count
    end
  end

  defp record(mode, scenario, tool?, lengths, cancellations, request, attempt, bytes) do
    if directory = System.get_env("NATIVE_TRACKING_EVIDENCE") do
      File.write!(Path.join(directory, "#{mode}-tool-#{tool?}-#{scenario}.json"), CodexPooler.JSON.encode!(%{mode: mode, scenario: scenario, tool: tool?, observation: attempt.response_metadata["native_sse_observation"], source_bytes: bytes, actual_req_chunk_lengths: lengths, cancellations: cancellations, request_status: request.status, attempt_status: attempt.status, error: request.last_error_code, usage: request.usage_status, receipt: Map.take(attempt.response_metadata["downstream_delivery"] || %{}, ~w(outcome terminal_class frames_after_visible))}))
    end
  end
end
