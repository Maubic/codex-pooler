defmodule CodexPoolerWeb.Runtime.NativePreambleBudgetHttpTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPooler.PoolerFixtures, only: [request_fixture: 2, attempt_fixture: 3]
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Runtime.Finalization.Streaming
  alias CodexPooler.Gateway.Runtime.Streaming.{StreamAttempt, VisibleOutputMark}
  alias CodexPooler.Gateway.Transports.Streaming.StreamRelay
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  @bound 8_388_608
  @timeout 15_000
  @moduletag capture_log: true

  for mode <- ["full", "lite"], extra <- [-1, 0, 1] do
    test "#{mode} exact held preamble B#{extra} remains within selected budget" do
      run_scenario(unquote(mode), "boundary-#{unquote(extra)}", boundary_case(unquote(extra)))
    end
  end

  for mode <- ["full", "lite"], name <- [:many127, :many128, :single_large, :single_late_type, :single_malformed, :single_contradictory, :data_only_large, :visible_reset, :after_visible, :after_terminal, :retry_reset, :crossing_retry, :terminal_eof, :preamble_eof, :incomplete, :nonretryable, :probe_overflow, :single_data_only_eof, :utf8_boundary, :unknown_label_large, :contradictory_within, :oversized_then_undelivered_terminal, :timeout, :disconnect, :utf8_overflow, :claim_overflow, :after_terminal_eof, :data_only_timeout, :partial_preamble_eof, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout, :claim_after_visible] do
    test "#{mode} #{name} preamble ownership lifecycle" do
      run_scenario(unquote(mode), unquote(name), lifecycle_case(unquote(name)))
    end
  end

  defp run_scenario(mode, name, spec) do
    configure_limits(name)

    release = make_ref()
    barrier_after = if name == :probe_overflow, do: 1, else: length(spec.chunks)
    source_mode = if name in [:probe_overflow, :timeout, :disconnect, :data_only_timeout, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout], do: FakeUpstream.barrier_sse_stream(spec.chunks, barrier_after: barrier_after, notify: self(), release_ref: release, done: false, on_client_close: :expected), else: FakeUpstream.chunked_response(spec.chunks, headers: [{"content-type", "text/event-stream"}])
    second_mode = FakeUpstream.chunked_response(Map.get(spec, :successor, [completed()]), headers: [{"content-type", "text/event-stream"}])
    {fixture, first, second} = stream_retry_setup(source_mode, second_mode)
    circuit = if name == :probe_overflow, do: half_open_circuit!(fixture, fixture.assignment) |> Ecto.Changeset.change(route_class: "proxy_stream") |> Repo.update!()
    now = DateTime.utc_now()
    Repo.insert!(%ModelServingOverride{pool_id: fixture.pool.id, exposed_model_id: fixture.model.exposed_model_id, mode: mode, created_at: now, updated_at: now})
    {listener, port} = start_public_endpoint_with_server!()
    monitor = Process.monitor(listener)
    ownership = capture_public_endpoint_ownership!(listener)
    mfas = install_trace()
    retained = {StreamRelay, :stream_upstream, 4}
    url = "http://127.0.0.1:#{port}/backend-api/codex/responses"
    payload = %{"model" => fixture.model.exposed_model_id, "input" => native_text_input("synthetic preamble"), "stream" => true}
    headers = [{"authorization", fixture.authorization}, {"x-request-id", deterministic_rotation_seed(2, 0)}]
    {payload, headers, seed_id} = native_claim(name, fixture, payload, headers)
    {response, initial_samples} = execute_request(name, %{url: url, port: port, payload: payload, headers: headers, circuit: circuit, retained: retained, barrier_after: barrier_after, release: release})

    fence = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^fence}, @timeout
    samples = initial_samples ++ observations([])
    :erlang.trace(:new_processes, false, [:call])
    for mfa <- mfas, do: :erlang.trace_pattern(mfa, false, [:local])
    assert_scenario(%{mode: mode, name: name, spec: spec, fixture: fixture, first: first, second: second, circuit: circuit, seed_id: seed_id}, response, samples)
    verify_explicit_request(mode, name, url, payload, headers, first, second)

    :ok = ThousandIsland.stop(listener)
    assert_receive {:DOWN, ^monitor, :process, ^listener, _}, @timeout
    assert_public_endpoint_released!(ownership)
  end

  defp native_claim(name, fixture, payload, headers) when name in [:claim_overflow, :claim_after_visible] do
    use_routing_strategy!(fixture.pool, "least_recent_success", 2)
    seed = request_fixture(fixture, %{model_id: fixture.model.id})
    attempt_fixture(seed, fixture.fallback_assignment, %{completed_at: DateTime.utc_now()})
    thread = Ecto.UUID.generate()
    document = CodexPooler.JSON.encode!(%{"session_id" => thread, "thread_id" => thread, "turn_id" => "sample-preamble-turn", "request_kind" => "turn"})
    payload = Map.merge(payload, %{"instructions" => "synthetic", "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => document}})
    {payload, [{"session-id", thread}, {"originator", "codex_cli_rs"} | headers], seed.id}
  end

  defp native_claim(_name, _fixture, payload, headers), do: {payload, headers, nil}

  defp assert_claim_state(:claim_overflow, request) do
    assert request.native_client_retry_version == 1
    assert turn = Repo.one(from t in CodexTurn, where: t.request_id == ^request.id)
    assert turn.first_visible_output_at == nil
    assert Repo.get!(CodexSession, turn.codex_session_id).pool_upstream_assignment_id == nil
  end

  defp assert_claim_state(:claim_after_visible, request) do
    assert request.native_client_retry_version == 1
    assert turn = Repo.one(from t in CodexTurn, where: t.request_id == ^request.id)
    assert turn.first_visible_output_at != nil
    assert Repo.get!(CodexSession, turn.codex_session_id).pool_upstream_assignment_id != nil
  end

  defp assert_claim_state(_name, _request), do: :ok

  defp verify_explicit_request(mode, :claim_overflow, url, payload, headers, first, second) do
    response = Req.post!(url, json: payload, headers: headers, retry: false)
    assert response.status == 200
    assert response.body == ""
    assert FakeUpstream.http_request_count(first) == 2
    assert FakeUpstream.http_request_count(second) == 0
    requests = Repo.all(from r in Request, where: r.native_client_retry_version == 1)
    assert length(requests) == 2

    for request <- requests do
      assert request.last_error_code == "upstream_response_too_large"
      assert request.retry_count == 0
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    end

    assert Repo.aggregate(CodexTurn, :count) == 2

    if directory = System.get_env("PREAMBLE_EVIDENCE") do
      File.write!(Path.join(directory, "#{mode}-explicit-client-request.json"), CodexPooler.JSON.encode!(%{client_status: response.status, requests: length(requests), turns: Repo.aggregate(CodexTurn, :count), candidate_calls: [FakeUpstream.http_request_count(first), FakeUpstream.http_request_count(second)], retry_counts: Enum.map(requests, & &1.retry_count), settlements: Enum.map(requests, fn request -> Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) end)}))
    end
  end

  defp verify_explicit_request(mode, :claim_after_visible, url, payload, headers, first, second) do
    response = Req.post!(url, json: payload, headers: headers, retry: false)
    requests = Repo.all(from r in Request, where: r.native_client_retry_version == 1)

    if directory = System.get_env("PREAMBLE_EVIDENCE") do
      File.write!(Path.join(directory, "#{mode}-postvisible-client-request.json"), CodexPooler.JSON.encode!(%{client_status: response.status, requests: length(requests), candidate_calls: [FakeUpstream.http_request_count(first), FakeUpstream.http_request_count(second)], turns: Enum.map(Repo.all(CodexTurn), fn turn -> %{visible: not is_nil(turn.first_visible_output_at), error: turn.error_code} end), request_metadata_keys: Enum.map(requests, &Map.keys(&1.request_metadata)), retry_link_count: Repo.aggregate(RequestClientRetryLink, :count), attempts: Enum.map(requests, fn request -> Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) end), settlements: Enum.map(requests, fn request -> Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) end)}))
    end

    assert response.status == 409
    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == 0
    assert length(requests) == 1
    assert Repo.aggregate(RequestClientRetryLink, :count) == 0
  end

  defp verify_explicit_request(_mode, _name, _url, _payload, _headers, _first, _second), do: :ok

  defp configure_limits(name) do
    if name in [:timeout, :disconnect, :data_only_timeout, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout] do
      CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
      Application.put_env(:codex_pooler, OperationalSettings, settings: %OperationalSettings{upstream_receive_timeout_ms: if(name in [:timeout, :data_only_timeout, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout], do: 200, else: 60_000), sse_keepalive_interval_ms: if(name == :disconnect, do: 20, else: 0)})
    end
  end

  defp install_trace do
    source = {StreamAttempt, :classify_first_event, 3}
    retained = {StreamRelay, :stream_upstream, 4}
    cancel = {StreamRelay, :source_cancel, 1}
    visibility = {VisibleOutputMark, :mark, 2}
    Code.ensure_loaded!(VisibleOutputMark)
    Code.ensure_loaded!(Streaming)
    final_success = {Streaming, :finalize_success, 4}
    final_failure = {Streaming, :finalize_failure, 4}
    Code.ensure_loaded!(StreamAttempt)
    Code.ensure_loaded!(StreamRelay)

    on_exit(fn ->
      :erlang.trace(:new_processes, false, [:call])
      for mfa <- [source, retained, cancel, visibility, final_success, final_failure], do: :erlang.trace_pattern(mfa, false, [:local])
    end)

    :erlang.trace_pattern(source, [{[:"$1", :_, :_], [{:is_binary, :"$1"}], [{:message, {:byte_size, :"$1"}}]}], [:local])
    :erlang.trace_pattern(retained, [{[%{target: %{private: %{codex_pooler_withheld_retry_preamble: :"$1"}}}, :_, :_, :_], [], [{:message, {:byte_size, :"$1"}}]}], [:local])
    :erlang.trace_pattern(cancel, [{:_, [], [{:message, :cancel}]}], [:local])
    :erlang.trace_pattern(visibility, [{:_, [], [{:message, :visible}]}], [:local])

    for mfa <- [final_success, final_failure] do
      :erlang.trace_pattern(mfa, [{[:_, :_, :_, %{target: %{private: :"$1"}}], [{:not, {:is_map_key, :codex_pooler_withheld_retry_preamble, :"$1"}}], [{:message, 0}]}, {[:_, :_, :_, %{target: %{private: %{codex_pooler_withheld_retry_preamble: :"$1"}}}], [], [{:message, {:byte_size, :"$1"}}]}], [:local])
    end

    :erlang.trace(:new_processes, true, [:call, :arity, {:tracer, self()}])
    [source, retained, cancel, visibility, final_success, final_failure]
  end

  defp assert_scenario(%{mode: mode, name: name, spec: spec, fixture: fixture, first: first, second: second, circuit: circuit, seed_id: seed_id}, response, samples) do
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^fixture.pool.id) |> Enum.reject(&(&1.id == seed_id))
    attempts = Repo.all(from a in Attempt, where: a.request_id == ^request.id, order_by: a.started_at)
    assert length(attempts) == Map.get(spec, :attempts, 1)
    attempt = List.last(attempts)
    held = for {:stream_upstream, n} <- samples, do: n
    lengths = for {:classify_first_event, n} <- samples, do: n
    cancellation_count = Enum.count(samples, &(&1 == {:source_cancel, :cancel}))
    final_held = for {function, bytes} <- samples, function in [:finalize_success, :finalize_failure], do: bytes
    record(mode, name, request, attempt, held, lengths, cancellation_count, %{wire_bytes: byte_size(response.body), client_status: response.status, attempts: length(attempts), candidate_calls: [FakeUpstream.http_request_count(first), FakeUpstream.http_request_count(second)], visible_marks: Enum.count(samples, &(&1 == {:mark, :visible})), final_held_samples: final_held})
    assert Enum.max([0 | held]) <= @bound
    assert Enum.all?(final_held, &(&1 == 0))
    if spec.error == "upstream_response_too_large" or name in [:timeout, :disconnect, :data_only_timeout, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout], do: assert(final_held != [])
    assert response.status == 200
    assert request.status == spec.status
    assert request.last_error_code == spec.error
    assert :crypto.hash(:sha256, response.body) == :crypto.hash(:sha256, spec.wire)
    assert cancellation_count == spec.cancellations
    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == Map.get(spec, :successor_calls, 0)
    assert request.retry_count == Map.get(spec, :successor_calls, 0)
    for kind <- ["settlement", "release"], do: assert(Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == ^kind), :count) == 1)

    assert_local_policy(spec, fixture, request, attempt, samples)
    assert_claim_state(name, request)

    if circuit do
      current = Repo.reload!(circuit)
      assert current.metadata["probe_in_flight_count"] == 0
      assert {current.status, current.failure_count, current.success_count} == {circuit.status, circuit.failure_count, circuit.success_count}
    end
  end

  defp assert_local_policy(spec, fixture, request, attempt, samples) do
    if spec.error == "upstream_response_too_large" do
      assert attempt.response_metadata["native_preamble_limit"]["budget_bytes"] == @bound
      assert request.response_status_code == 200
      assert attempt.upstream_status_code == 200
      assert Repo.aggregate(BridgeDemotion, :count) == 0
      assert Repo.reload!(fixture.identity).status == fixture.identity.status
      if spec.wire == "", do: refute(Enum.any?(samples, &(&1 == {:mark, :visible})))
    end
  end

  defp execute_request(:disconnect, %{port: port, payload: payload, headers: headers, retained: retained, barrier_after: barrier_after, release: release}) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {:ok, conn, request_ref} = Mint.HTTP.request(conn, "POST", "/backend-api/codex/responses", [{"content-type", "application/json"} | headers], CodexPooler.JSON.encode!(payload))
    assert_receive {:fake_upstream_chunk_barrier, ^barrier_after, upstream, ^release}, @timeout
    assert_receive {:trace, relay, :call, ^retained, 4000}, @timeout
    monitor = Process.monitor(relay)
    {conn, status} = receive_status(conn, request_ref)
    {:ok, _} = Mint.HTTP.close(conn)
    assert_receive {:DOWN, ^monitor, :process, ^relay, _}, @timeout
    send(upstream, {:fake_upstream_release_chunk, release})
    {%{status: status, body: ""}, [{:stream_upstream, 4000}]}
  end

  defp execute_request(name, %{url: url, payload: payload, headers: headers, circuit: circuit, retained: retained, barrier_after: barrier_after, release: release}) do
    supervisor = start_supervised!(Task.Supervisor)
    task = Task.Supervisor.async_nolink(supervisor, fn -> Req.post!(url, json: payload, headers: headers, retry: false, receive_timeout: @timeout) end)
    monitor = Process.monitor(task.pid)

    {upstream, samples} =
      if name in [:timeout, :probe_overflow, :data_only_timeout, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout] do
        assert_receive {:fake_upstream_chunk_barrier, ^barrier_after, upstream, ^release}, @timeout

        if circuit do
          assert Repo.reload!(circuit).metadata["probe_in_flight_count"] == 1
          send(upstream, {:fake_upstream_release_chunk, release})
          {nil, []}
        else
          {upstream, await_barrier_retention(name, retained)}
        end
      else
        {nil, []}
      end

    response = Task.await(task, @timeout)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @timeout
    if upstream, do: send(upstream, {:fake_upstream_release_chunk, release})
    {response, samples}
  end

  defp await_barrier_retention(name, _retained) when name in [:data_only_timeout, :partial_preamble_timeout, :after_terminal_timeout, :after_incomplete_timeout], do: await_source_bytes(IO.iodata_length(lifecycle_case(name).chunks), [])

  defp await_barrier_retention(_name, retained) do
    assert_receive {:trace, _, :call, ^retained, 4000}, @timeout
    [{:stream_upstream, 4000}]
  end

  defp await_source_bytes(0, samples), do: Enum.reverse(samples)

  defp await_source_bytes(remaining, samples) do
    assert_receive {:trace, _, :call, {StreamAttempt, :classify_first_event, 3}, bytes}, @timeout
    assert bytes > 0 and bytes <= remaining
    await_source_bytes(remaining - bytes, [{:classify_first_event, bytes} | samples])
  end

  defp receive_status(conn, request_ref) do
    {:ok, conn, events} = Mint.HTTP.recv(conn, 0, @timeout)

    case Enum.find_value(events, fn
           {:status, ^request_ref, status} -> status
           _ -> nil
         end) do
      nil -> receive_status(conn, request_ref)
      status -> {conn, status}
    end
  end

  defp completed, do: "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":3,\"total_tokens\":7}}}\n\n"
  defp delta, do: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\n"
  defp failed(code), do: "data: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => code}}}) <> "\n\n"
  defp success(chunks, wire), do: %{chunks: chunks, wire: wire, status: "succeeded", error: nil, cancellations: 0}
  defp refused(chunks, wire \\ ""), do: %{chunks: chunks, wire: wire, status: "failed", error: "upstream_response_too_large", cancellations: 1}

  defp boundary_case(extra) do
    frames = preamble(@bound + extra)
    if extra > 0, do: refused(frames ++ [completed()]), else: success(frames ++ [completed()], frames ++ [completed()])
  end

  defp lifecycle_case(:many127), do: success(List.duplicate(progress(65_612), 127) ++ [completed()], List.duplicate(progress(65_612), 127) ++ [completed()])
  defp lifecycle_case(:many128), do: refused(List.duplicate(progress(65_612), 128) ++ [completed()])
  defp lifecycle_case(:single_large), do: refused([progress(@bound + 1), completed()])

  defp lifecycle_case(:single_late_type) do
    frame = "event: response.in_progress\ndata: {\"padding\":\"" <> String.duplicate("x", @bound) <> "\",\"type\":\"response.in_progress\"}\n\n"
    refused([frame, completed()])
  end

  defp lifecycle_case(:single_malformed), do: refused(["event: response.metadata\ndata: {" <> String.duplicate("x", @bound), completed()])
  defp lifecycle_case(:single_contradictory), do: refused(["event: response.in_progress\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.completed", "padding" => String.duplicate("x", @bound)}) <> "\n\n", completed()])
  defp lifecycle_case(:data_only_large), do: refused(["data: " <> CodexPooler.JSON.encode!(%{"type" => "response.in_progress", "padding" => String.duplicate("x", @bound)}) <> "\n\n", completed()])

  defp lifecycle_case(:visible_reset) do
    frames = preamble(div(@bound, 2) + 100)
    chunks = frames ++ [delta()] ++ frames ++ [completed()]
    success([IO.iodata_to_binary(chunks)], chunks)
  end

  defp lifecycle_case(:after_visible), do: refused([delta() | preamble(@bound + 1)] ++ [completed()], delta())
  defp lifecycle_case(:after_terminal), do: %{success([completed() | preamble(@bound + 1)], completed()) | cancellations: 1}

  defp lifecycle_case(:retry_reset) do
    frames = preamble(@bound)
    successor = frames ++ [completed()]
    success(frames ++ [failed("server_error")], successor) |> Map.merge(%{successor: successor, successor_calls: 1, attempts: 2, cancellations: 1})
  end

  defp lifecycle_case(:crossing_retry), do: refused(preamble(@bound - 100) ++ [progress(200) <> failed("server_error")])
  defp lifecycle_case(:terminal_eof), do: success([progress(1000), String.trim_trailing(completed(), "\n")], [progress(1000), completed()])
  defp lifecycle_case(:preamble_eof), do: success([progress(1000)], "")

  defp lifecycle_case(:incomplete) do
    terminal = "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
    success([progress(1000), terminal], [progress(1000), terminal])
  end

  defp lifecycle_case(:nonretryable), do: %{success([progress(1000), failed("invalid_request_error")], [progress(1000), failed("invalid_request_error")]) | status: "failed", error: "invalid_request_error", cancellations: 1}

  defp lifecycle_case(:single_data_only_eof) do
    frame = "data: {\"padding\":\"" <> String.duplicate("x", @bound) <> "\",\"type\":\"response.in_progress\"}"
    refused([frame])
  end

  defp lifecycle_case(:utf8_boundary) do
    frame = progress(@bound) |> String.replace("xx", "é", global: false)
    success([frame, completed()], [frame, completed()])
  end

  defp lifecycle_case(:unknown_label_large) do
    frame = "event: synthetic.unknown\ndata: {\"padding\":\"" <> String.duplicate("x", @bound + 1) <> "\",\"type\":\"response.in_progress\"}\n\n"
    success([frame, completed()], [frame, completed()])
  end

  defp lifecycle_case(:contradictory_within) do
    frame = "event: response.in_progress\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
    success([frame, completed()], [frame, completed()])
  end

  defp lifecycle_case(:oversized_then_undelivered_terminal), do: refused([IO.iodata_to_binary(preamble(@bound + 1) ++ [completed()])])
  defp lifecycle_case(:timeout), do: %{success(List.duplicate(progress(1000), 4), "") | status: "failed", error: "stream_idle_timeout"}
  defp lifecycle_case(:disconnect), do: %{success(List.duplicate(progress(1000), 4), "") | status: "failed", error: "client_disconnected", cancellations: 1}
  defp lifecycle_case(:utf8_overflow), do: refused([progress(@bound + 1) |> String.replace("xx", "é", global: false), completed()])

  defp lifecycle_case(:after_terminal_eof) do
    frame = "data: {\"padding\":\"" <> String.duplicate("x", @bound) <> "\",\"type\":\"response.in_progress\"}"
    %{success([completed(), frame], completed()) | cancellations: 1}
  end

  defp lifecycle_case(:partial_preamble_eof), do: success(["event: response.created\ndata: {\"type\":\"response.created\",\"response\":{"], "")
  defp lifecycle_case(:partial_preamble_timeout), do: %{lifecycle_case(:partial_preamble_eof) | status: "failed", error: "stream_idle_timeout"}
  defp lifecycle_case(:data_only_timeout), do: lifecycle_case(:single_data_only_eof)
  defp lifecycle_case(:after_terminal_timeout), do: lifecycle_case(:after_terminal_eof)

  defp lifecycle_case(:after_incomplete_timeout) do
    terminal = "data: {\"type\":\"response.incomplete\",\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n"
    %{lifecycle_case(:after_terminal_eof) | chunks: [terminal, List.last(lifecycle_case(:after_terminal_eof).chunks)], wire: terminal}
  end

  defp lifecycle_case(:claim_after_visible), do: lifecycle_case(:after_visible)
  defp lifecycle_case(:claim_overflow), do: boundary_case(1)
  defp lifecycle_case(:probe_overflow), do: boundary_case(1)

  defp preamble(bytes) do
    count = div(bytes, 65_536) - 1
    List.duplicate(progress(65_536), count) ++ [progress(bytes - count * 65_536)]
  end

  defp progress(size) do
    prefix = "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"padding\":\""
    suffix = "\"}\n\n"
    prefix <> String.duplicate("x", size - byte_size(prefix <> suffix)) <> suffix
  end

  defp observations(acc) do
    receive do
      {:trace, _, :call, {_, name, _}, n} -> observations([{name, n} | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  defp record(mode, extra, request, attempt, held, lengths, cancellations, facts) do
    if directory = System.get_env("PREAMBLE_EVIDENCE") do
      File.write!(Path.join(directory, "#{mode}-#{extra}.json"), CodexPooler.JSON.encode!(%{mode: mode, extra: extra, diagnostic: attempt.response_metadata["native_preamble_limit"], request_status: request.status, attempt_status: attempt.status, code: request.last_error_code, max_held: Enum.max([0 | held]), held_samples: held, callback_lengths: lengths, cancellations: cancellations, facts: facts, usage_status: request.usage_status, retry_count: request.retry_count, receipt: Map.take(attempt.response_metadata["downstream_delivery"] || %{}, ~w(outcome terminal_class frames_after_visible))}))
    end
  end
end
