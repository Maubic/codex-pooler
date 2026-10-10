defmodule CodexPoolerWeb.Runtime.HttpFirstEventBoundariesTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Streaming.StreamAttempt
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  @budget 15_000
  @moduletag capture_log: true

  test "listener cleanup is tied to owned socket identity when the numeric port is reused" do
    {listener, port} = start_public_endpoint_with_server!()
    monitor = Process.monitor(listener)
    owned = capture_listener_resources(listener, port)
    stop_owned_listener(listener, monitor, owned)
    {:ok, replacement} = :gen_tcp.listen(port, [:binary, active: false, ip: {127, 0, 0, 1}, reuseaddr: true])

    try do
      result = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false], 1000)
      if match?({:ok, _}, result), do: :gen_tcp.close(elem(result, 1))
      assert {:ok, _} = result
      assert :erlang.port_info(owned.socket) == :undefined
      assert Enum.all?(owned.processes, fn {pid, _ref} -> not Process.alive?(pid) end)
      record_cleanup("reused-port", owned, true)
    after
      :gen_tcp.close(replacement)
    end
  end

  for surface <- [:native, :public], mode <- ["full", "lite"], boundary <- [:whole, :label, :json, :crlf], type <- ["response.failed", "response.incomplete", "error"] do
    test "#{surface} #{mode} #{type} #{boundary} retryable first event reaches successor" do
      run_scenario(unquote(surface), unquote(mode), unquote(boundary), unquote(type), :none, :retry)
    end
  end

  for surface <- [:native, :public], mode <- ["full", "lite"] do
    test "#{surface} #{mode} lifecycle prefix keeps the split retry window open" do
      run_scenario(unquote(surface), unquote(mode), :label, "response.failed", :lifecycle, :retry)
    end

    test "#{surface} #{mode} nonretryable first failure never tries successor" do
      run_scenario(unquote(surface), unquote(mode), :json, "response.failed", :none, :nonretryable)
    end

    test "#{surface} #{mode} failing successor exhausts exactly two candidates" do
      run_scenario(unquote(surface), unquote(mode), :label, "response.failed", :none, :exhausted)
    end

    test "#{surface} #{mode} visible output closes retry window" do
      run_scenario(unquote(surface), unquote(mode), :whole, "response.failed", :visible, :nonretryable_visible)
    end
  end

  for surface <- [:native, :public], mode <- ["full", "lite"] do
    test "#{surface} #{mode} malformed EOF does not invent a retry or duplicate settlement" do
      run_malformed_eof(unquote(surface), unquote(mode))
    end
  end

  defmodule ClosedChunkAdapter do
    def chunk(socket, chunk), do: :gen_tcp.send(socket, chunk)
  end

  @tag :closed_eof_visibility
  test "failed buffered EOF write retains provider terminal without delivery counters" do
    terminal = "event: response.failed\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "context_length_exceeded"}}})
    midpoint = div(byte_size(terminal), 2)
    upstream = start_upstream(FakeUpstream.sse_stream(split(terminal, midpoint), done: false))
    setup = gateway_setup(upstream)
    {:ok, authorization} = Access.authenticate_authorization_header(setup.authorization)
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic closed EOF"), "stream" => true}
    options = RequestOptions.build(%{upstream_endpoint: "/backend-api/codex/responses", public_openai_responses_stream: true}, "/v1/responses", payload)
    assert {:ok, %{stream: stream}} = CodexPooler.Gateway.execute(authorization, "/v1/responses", payload, options)
    stream.(%{Phoenix.ConnTest.build_conn() | adapter: {ClosedChunkAdapter, closed_socket!()}, state: :chunked})
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.status == "failed"
    assert request.last_error_code == "response.failed"
    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    summary = attempt.response_metadata["public_openai_responses_stream"]
    assert summary["terminal_seen"] == true
    assert summary["terminal_kind"] == "failed"
    assert summary["stream_bytes"] > 0
    receipt = attempt.response_metadata["downstream_delivery"]
    assert receipt["outcome"] == "aborted"
    assert receipt["terminal_class"] == "none"
    assert receipt["frames_after_visible"] == 0

    assert Map.take(summary, ~w(visible_seen created_seen delta_count delta_bytes text_done_count text_done_bytes item_done_count relay_bytes synthetic_terminal_sent)) == %{
             "visible_seen" => false,
             "created_seen" => false,
             "delta_count" => 0,
             "delta_bytes" => 0,
             "text_done_count" => 0,
             "text_done_bytes" => 0,
             "item_done_count" => 0,
             "relay_bytes" => 0,
             "synthetic_terminal_sent" => false
           }
  end

  test "real public HTTP separator-less terminal successfully delivers its diagnostic summary" do
    terminal = "event: response.failed\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "context_length_exceeded"}}})
    {setup, first, second} = stream_retry_setup(FakeUpstream.sse_stream(split(terminal, div(byte_size(terminal), 2)), done: false))
    {listener, port} = start_public_endpoint_with_server!()
    monitor = Process.monitor(listener)
    ownership = capture_public_endpoint_ownership!(listener)
    response = Req.post!("http://127.0.0.1:#{port}/v1/responses", json: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic separatorless"), "stream" => true}, headers: [{"authorization", setup.authorization}, {"x-request-id", deterministic_rotation_seed(2, 0)}], retry: false, receive_timeout: @budget)
    assert response.status == 200
    assert response.body =~ "response.failed"
    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == 0
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.status == "failed"
    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    summary = attempt.response_metadata["public_openai_responses_stream"]
    assert summary["terminal_seen"] == true
    assert summary["visible_seen"] == true
    assert summary["relay_bytes"] > 0
    assert attempt.response_metadata["downstream_delivery"]["outcome"] == "delivered"
    :ok = ThousandIsland.stop(listener)
    assert_receive {:DOWN, ^monitor, :process, ^listener, _reason}, @budget
    assert_public_endpoint_released!(ownership)
  end

  defp closed_socket! do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, {_, port}} = :inet.sockname(listener)
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, port, [:binary, active: false])
    on_exit(fn -> :gen_tcp.close(client) end)
    {:ok, peer} = :gen_tcp.accept(listener)
    on_exit(fn -> :gen_tcp.close(peer) end)
    :ok = :gen_tcp.close(peer)
    :ok = :gen_tcp.close(listener)
    :ok = :gen_tcp.close(client)
    assert {:error, :closed} = :gen_tcp.send(client, "synthetic")
    client
  end

  defp run_malformed_eof(surface, mode) do
    {setup, first, second} = stream_retry_setup({:sse, ["data: {"]})
    timestamp = DateTime.utc_now()
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    {listener, port} = start_public_endpoint_with_server!()
    monitor = Process.monitor(listener)
    ownership = capture_public_endpoint_ownership!(listener)
    path = if surface == :native, do: "/backend-api/codex/responses", else: "/v1/responses"
    response = Req.post!("http://127.0.0.1:#{port}" <> path, json: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic EOF"), "stream" => true}, headers: [{"authorization", setup.authorization}, {"x-request-id", deterministic_rotation_seed(2, 0)}], retry: false, receive_timeout: @budget)
    assert response.status == 200
    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == 0
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.retry_count == 0
    assert request.status == "succeeded"
    assert is_nil(request.last_error_code)
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    :ok = ThousandIsland.stop(listener)
    assert_receive {:DOWN, ^monitor, :process, ^listener, _reason}, @budget
    assert_public_endpoint_released!(ownership)
  end

  defp run_scenario(surface, mode, boundary, type, prefix, expected) do
    code = event_code(type, expected)
    block = terminal_block(type, code, boundary)
    chunks = block |> chunks(boundary) |> prepend_scenario(prefix)
    ref = make_ref()
    held? = boundary != :whole or prefix == :visible
    fake_mode = if held?, do: FakeUpstream.barrier_sse_stream(chunks, barrier_after: 1, notify: self(), release_ref: ref, done: false, on_client_close: :expected), else: {:sse, chunks}
    second_mode = if expected == :exhausted, do: {:sse, [block]}, else: stream_success_sse()
    {setup, first, second} = stream_retry_setup(fake_mode, second_mode)
    timestamp = DateTime.utc_now()
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    {listener, port} = start_public_endpoint_with_server!()
    listener_monitor = Process.monitor(listener)
    path = if surface == :native, do: "/backend-api/codex/responses", else: "/v1/responses"
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic boundary"), "stream" => true}
    mfa = {StreamAttempt, :classify_first_event, 3}

    on_exit(fn ->
      :erlang.trace(:new_processes, false, [:call])
      :erlang.trace_pattern(mfa, false, [:local])
    end)

    Code.ensure_loaded!(StreamAttempt)
    :erlang.trace_pattern(mfa, [{:_, [], [{:message, :first_event_chunk}]}], [:local])
    :erlang.trace(:new_processes, true, [:call, :arity, {:tracer, self()}])
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Req.post!("http://127.0.0.1:#{port}" <> path, json: payload, headers: [{"authorization", setup.authorization}, {"x-request-id", deterministic_rotation_seed(2, 0)}], retry: false, receive_timeout: @budget)
      end)

    monitor = Process.monitor(task.pid)

    if held? do
      assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^ref}, @budget
      assert_receive {:trace, _pid, :call, ^mfa, :first_event_chunk}, @budget
      send(handler, {:fake_upstream_release_chunk, ref})
    end

    response = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    :erlang.trace(:new_processes, false, [:call])
    :erlang.trace_pattern(mfa, false, [:local])
    assert response.status == 200
    assert FakeUpstream.http_request_count(first) == 1
    assert FakeUpstream.http_request_count(second) == if(expected in [:retry, :exhausted], do: 1, else: 0)
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    attempts = Repo.all(from a in Attempt, where: a.request_id == ^request.id, order_by: a.attempt_number)
    assert hd(attempts).response_metadata["routing"]["model_serving_mode"] == mode
    assert hd(attempts).upstream_status_code == 200
    assert request.transport == "http_sse"
    assert Enum.all?(attempts, &(&1.transport == "http_sse"))

    assert_outcome(expected, request, attempts, response.body, code)
    assert_success_wire(expected, surface, response.body)

    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    owned = capture_listener_resources(listener, port)
    stop_owned_listener(listener, listener_monitor, owned)
    record_cleanup("#{surface}-#{mode}-#{type}-#{boundary}-#{expected}", owned, false)
  end

  defp capture_listener_resources(listener, port) do
    {:listener, owner, :worker, _modules} = Enum.find(Supervisor.which_children(listener), &(elem(&1, 0) == :listener))
    {:links, links} = Process.info(owner, :links)
    sockets = Enum.filter(links, fn socket -> is_port(socket) and :inet.sockname(socket) == {:ok, {{127, 0, 0, 1}, port}} end)
    assert [socket] = sockets
    assert {:connected, ^owner} = :erlang.port_info(socket, :connected)
    processes = Enum.map(supervision_pids(listener), &{&1, Process.monitor(&1)})
    %{socket: socket, socket_monitor: :erlang.monitor(:port, socket), processes: processes}
  end

  defp supervision_pids(supervisor) do
    children = Supervisor.which_children(supervisor)

    [
      supervisor
      | Enum.flat_map(children, fn
          {_id, pid, :supervisor, _modules} when is_pid(pid) -> supervision_pids(pid)
          {_id, pid, :worker, _modules} when is_pid(pid) -> [pid]
          _not_running -> []
        end)
    ]
  catch
    :exit, {:noproc, _call} -> [supervisor]
  end

  defp stop_owned_listener(listener, listener_monitor, owned) do
    :ok = ThousandIsland.stop(listener)
    assert_receive {:DOWN, ^listener_monitor, :process, ^listener, _reason}, @budget

    for {pid, monitor} <- owned.processes do
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, @budget
      refute Process.alive?(pid)
    end

    %{socket: socket, socket_monitor: monitor} = owned
    assert_receive {:DOWN, ^monitor, :port, ^socket, _reason}, @budget
    assert :erlang.port_info(socket) == :undefined
  end

  defp record_cleanup(scenario, owned, reused?) do
    if directory = System.get_env("FIRST_EVENT_CLEANUP_EVIDENCE") do
      File.write!(Path.join(directory, "#{scenario}.json"), CodexPooler.JSON.encode!(%{owned_process_count: length(owned.processes), all_owned_processes_down: Enum.all?(owned.processes, fn {pid, _} -> not Process.alive?(pid) end), original_listen_socket_closed: :erlang.port_info(owned.socket) == :undefined, numeric_port_reused_control: reused?}))
    end
  end

  defp assert_success_wire(:retry, :native, body) do
    {:sse, chunks} = stream_success_sse()
    assert body == Enum.join(chunks)
  end

  defp assert_success_wire(:retry, :public, body) do
    assert Regex.scan(~r/^event: ([^\r\n]+)$/m, body, capture: :all_but_first) |> List.flatten() == ["response.created", "response.completed"]
  end

  defp assert_success_wire(_expected, _surface, _body), do: :ok

  defp event_code(_type, :nonretryable), do: "invalid_request"
  defp event_code("response.incomplete", _expected), do: "stream_incomplete"
  defp event_code(_type, _expected), do: "server_error"

  defp terminal_block(type, code, boundary) do
    payload = if type == "error", do: %{"type" => type, "error" => %{"code" => code}}, else: %{"type" => type, "response" => %{"error" => %{"code" => code}}}
    ending = if boundary == :crlf, do: "\r\n", else: "\n"
    "event: " <> type <> ending <> "data: " <> CodexPooler.JSON.encode!(payload) <> ending <> ending
  end

  defp prepend_scenario(chunks, :none), do: chunks
  defp prepend_scenario(chunks, :lifecycle), do: prepend(chunks, event("response.created", %{"response" => %{"id" => "resp_withheld_boundary", "status" => "in_progress"}}))
  defp prepend_scenario(chunks, :visible), do: [event("response.output_text.delta", %{"delta" => "synthetic-visible"}) | chunks]

  defp assert_outcome(:retry, request, attempts, body, code) do
    assert body =~ "response.completed"
    refute body =~ "resp_withheld_boundary"
    assert request.status == "succeeded"
    assert request.retry_count == 1
    assert Enum.map(attempts, & &1.status) == ["retryable_failed", "succeeded"]
    assert hd(attempts).network_error_code == code
    assert hd(attempts).response_metadata["stream_error_code"] == code
    assert hd(attempts).response_metadata["stream_failure_stage"] == "first_event"
  end

  defp assert_outcome(expected, request, attempts, body, code) do
    assert request.status == "failed"
    assert request.retry_count == if(expected == :exhausted, do: 1, else: 0)
    assert length(attempts) == if(expected == :exhausted, do: 2, else: 1)
    assert request.last_error_code == code
    if expected == :nonretryable_visible, do: assert(body =~ "synthetic-visible")
  end

  defp chunks(block, :whole), do: [block]

  defp chunks(block, :label) do
    [label, rest] = String.split(block, "data: ", parts: 2)
    [label, "data: " <> rest]
  end

  defp chunks(block, :json) do
    {offset, _} = :binary.match(block, "code")
    split(block, offset + 2)
  end

  defp chunks(block, :crlf) do
    {offset, _} = :binary.match(block, "\r\n")
    split(block, offset + 1)
  end

  defp split(block, offset), do: [binary_part(block, 0, offset), binary_part(block, offset, byte_size(block) - offset)]
  defp prepend([first | rest], prefix), do: [prefix <> first | rest]
  defp event(type, attrs), do: "event: " <> type <> "\ndata: " <> CodexPooler.JSON.encode!(Map.put(attrs, "type", type)) <> "\n\n"
end
