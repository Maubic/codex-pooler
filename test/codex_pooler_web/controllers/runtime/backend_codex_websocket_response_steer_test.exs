defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketResponseSteerTest do
  # Provenance: direct native-provider websocket measurements in findings#342,
  # 2026-10-07. All identifiers, instructions, text and tool bytes are synthetic.
  # Accepted steering may end the original incomplete/steered or completed;
  # only a subsequent provider response.created opens an unsolicited successor.
  # This is not the Codex HTTP/client-queued "steered continuation" contract.
  # Direct and local-owner arms prove one-node behavior only. The peer arm
  # owns its producing connection on a second VM and shares committed rows.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [with_info_log: 1, model_serving_scope: 0, set_model_serving_mode!: 3, route_circuit_failures: 1, await_socket_connection_state!: 2, socket_transport_barrier!: 3]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true
  @detection_timeout_ms 15_000
  @turn_path "/backend-api/codex/responses"
  @installation_id "00000000-0000-4000-8000-000000000342"
  @steer_id "steer_synthetic_342"
  @steer_text "synthetic private steering content sentinel"
  @tool_output_text "synthetic client owned tool output sentinel"
  @tool_arguments ~s({"key":"synthetic client owned tool argument sentinel"})
  @native_lane_message "The experimental native turn lane cannot accept stateful WebSocket messages while a native turn is running. Start a new independent response.create turn instead."
  @original_usage %{"input_tokens" => 41, "input_tokens_details" => %{"cached_tokens" => 7}, "output_tokens" => 23, "output_tokens_details" => %{"reasoning_tokens" => 3}, "total_tokens" => 64}
  @completed_usage %{"input_tokens" => 41, "input_tokens_details" => %{"cached_tokens" => 7}, "output_tokens" => 860, "output_tokens_details" => %{"reasoning_tokens" => 37}, "total_tokens" => 901}
  @successor_usage %{"input_tokens" => 110, "input_tokens_details" => %{"cached_tokens" => 11}, "output_tokens" => 7, "output_tokens_details" => %{"reasoning_tokens" => 2}, "total_tokens" => 117}
  @tool_follow_up_usage %{"input_tokens" => 143, "input_tokens_details" => %{"cached_tokens" => 13}, "output_tokens" => 9, "output_tokens_details" => %{"reasoning_tokens" => 1}, "total_tokens" => 152}

  for forwarding <- [:off, :on], mode <- ["full", "lite"], ending <- [:steered, :completed], batches <- [:coalesced, :separate] do
    @tag forwarding: forwarding, serving_mode: mode, ending: ending, batches: batches
    test "#{mode} owner forwarding #{forwarding}: #{ending} then unsolicited successor survives #{batches} batches", ctx do
      assert_accepted_steering!(ctx.forwarding, ctx.serving_mode, ctx.ending, ctx.batches)
    end
  end

  @tag slow: "boots a second VM that owns the producing connection and shares committed accounting rows"
  test "a remote owner relays steering and independently settles the unsolicited successor on its connection" do
    assert_accepted_steering!(:on, "lite", :steered, :coalesced, peer?: true)
  end

  test "the /backend-api/codex/v1/responses native alias relays and accounts for steering" do
    assert_accepted_steering!(:off, "full", :completed, :coalesced, path: "/backend-api/codex/v1/responses")
  end

  for forwarding <- [:off, :on], mode <- ["full", "lite"], outcome <- [:response_not_found, :invalid_input] do
    @tag forwarding: forwarding, serving_mode: mode, outcome: outcome
    test "#{mode} owner forwarding #{forwarding}: provider #{outcome} is nonterminal and the original can complete", ctx do
      assert_provider_steer_failure!(ctx.forwarding, ctx.serving_mode, ctx.outcome)
    end
  end

  for forwarding <- [:off, :on], mode <- ["full", "lite"] do
    @tag forwarding: forwarding, serving_mode: mode
    test "#{mode} owner forwarding #{forwarding}: native-lane refusal and Close 1000 reach the client without fallback or a hang", ctx do
      assert_native_lane_refusal!(ctx.forwarding, ctx.serving_mode)
    end

    @tag forwarding: forwarding, serving_mode: mode
    test "#{mode} owner forwarding #{forwarding}: a reserved provider error without any steer is an ordinary failed terminal", ctx do
      assert_unsteered_native_lane_refusal!(ctx.forwarding, ctx.serving_mode)
    end
  end

  for forwarding <- [:off, :on] do
    @tag forwarding: forwarding
    test "lite owner forwarding #{forwarding}: steering after the original terminal still opens a successor on its producing connection", ctx do
      assert_late_lite_steering!(ctx.forwarding)
    end

    @tag forwarding: forwarding
    test "full owner forwarding #{forwarding}: a postterminal steer relays the provider's silent Close 1000", ctx do
      assert_late_full_close!(ctx.forwarding)
    end
  end

  for forwarding <- [:off, :on] do
    @tag forwarding: forwarding
    test "full owner forwarding #{forwarding}: provider close wins while the steering lane is delayed", ctx do
      assert_late_full_close!(ctx.forwarding, hold_close?: true)
    end
  end

  @tag slow: "boots a second VM owning the closing upstream connection"
  test "a remote owner's provider close wins while the local steering lane is delayed" do
    assert_late_full_close!(:on, hold_close?: true, peer?: true)
  end

  for forwarding <- [:off, :on], mode <- ["full", "lite"], successor? <- [false, true] do
    @tag forwarding: forwarding, serving_mode: mode, successor?: successor?
    test "#{mode} owner forwarding #{forwarding}: accepted steering with successor #{successor?} leaves the pending client tool output usable", ctx do
      assert_client_owned_tool_continuation!(ctx.forwarding, ctx.serving_mode, ctx.successor?)
    end
  end

  test "public /v1/responses still refuses steering without an upstream generation or reservation" do
    put_owner_forwarding!(false)
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_synthetic_public_steer_never_dispatched"}))
    setup = gateway_setup(upstream)
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, "full")
    client = connect!(port, setup, turn, "/v1/responses")
    {client, [frame]} = client |> send_frame!(steer_frame("resp_public_steering_refused")) |> receive_n!(1)

    assert %{"type" => "error", "status" => 400, "error" => %{"code" => "invalid_request", "param" => "type"}} = CodexPooler.JSON.decode!(frame)
    assert FakeUpstream.requests(upstream) == []
    assert FakeUpstream.websocket_steers(upstream) == []
    assert FakeUpstream.physical_counts(upstream).websocket_generation == 0
    assert Repo.aggregate(from(a in Attempt, join: r in Request, on: a.request_id == r.id, where: r.pool_id == ^setup.pool.id), :count) == 0
    assert Repo.aggregate(from(l in LedgerEntry, where: l.pool_id == ^setup.pool.id), :count) == 0
    assert :ok = FakeUpstream.verify!(upstream)
    drop!(client)
  end

  defp assert_accepted_steering!(forwarding, mode, ending, batches, opts \\ []) do
    put_owner_forwarding!(forwarding == :on)
    peer? = Keyword.get(opts, :peer?, false)
    if peer?, do: enter_peer_owner_topology!()
    hold = make_ref()
    {response_id, successor_id} = response_ids()
    terminal = original_terminal_events(response_id, ending)
    successor = completed_events(successor_id, @successor_usage)
    upstream = start_steerable!(response_id, opening_events(response_id), terminal, successor, hold, batches: batches)
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    turn = new_turn(setup, mode)
    peer_owner = if peer?, do: start_peer_window_owner!(setup, "#{turn.thread}:0")
    if peer?, do: assert(node(peer_owner.owner_pid) != node())
    {_server, port} = start_public_endpoint_with_server!()
    client = port |> connect!(setup, turn, Keyword.get(opts, :path, @turn_path)) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, opening} = receive_n!(client, length(opening_events(response_id)))
    assert_frames!(opening, opening_events(response_id))
    expected = [accepted_event(response_id)] ++ terminal ++ successor
    sent = steer_frame(response_id)

    {{client, tail, [original, successor_row]}, log} =
      with_info_log(fn ->
        client = send_frame!(client, sent)
        {client, tail} = receive_accepted_tail!(client, setup, upstream, hold, expected, batches)
        assert_receive {:fake_upstream_steered, ^handler, ^hold}, @detection_timeout_ms
        rows = await_settled_rows!(setup, 2)
        Enum.each(rows, &await_downstream_delivery!/1)
        client = await_idle_and_ping!(client)
        {client, tail, rows}
      end)

    assert_frames!(tail, expected)
    assert Enum.map(tail, &event_type/1) == ["response.steer.accepted", terminal_type(ending), "response.created", "response.output_item.added", "response.output_text.delta", "response.output_item.done", "response.completed"]
    assert Enum.map(tail, &response_id/1) |> Enum.reject(&is_nil/1) == [response_id, successor_id, successor_id]
    assert_steer_written_once!(upstream, sent)
    assert_two_response_settlements!(setup, original, successor_row, response_id, successor_id, ending)
    assert %{"frames_after_visible" => 10} = await_downstream_delivery!(original)
    assert %{"frames_after_visible" => 5, "completed_items" => 1} = await_downstream_delivery!(successor_row)
    assert_one_physical_generation!(upstream)
    assert_serving_shape!(upstream, mode)
    assert original.request_metadata["routing"]["model_serving_mode"] == mode
    assert successor_row.request_metadata["routing"]["model_serving_mode"] == mode
    assert_diagnostic_logs!(log, forwarding, response_id, "response.steer.accepted", "accepted", @steer_id)

    if peer? do
      assert [original_attempt] = request_attempts(original)
      assert [successor_attempt] = request_attempts(successor_row)
      assert original_attempt.owner_instance_id == Atom.to_string(node())
      assert original.request_metadata["websocket_owner_forwarding"]["owner_instance_id"] == Atom.to_string(peer_owner.node)
      assert original.request_metadata["websocket_owner_forwarding"]["proxy_instance_id"] == Atom.to_string(node())
      assert node(:erpc.call(peer_owner.node, :sys, :get_state, [peer_owner.owner_pid]).upstream_pid) == peer_owner.node
      # Both accounting executions are local; their physical producing stream
      # is the independently observed peer-owned upstream connection.
      assert successor_attempt.owner_instance_id == Atom.to_string(node())
      assert is_binary(successor_attempt.owner_execution_id)
      assert is_binary(successor_attempt.owner_instance_boot_id)
      assert successor_attempt.owner_process_id != nil
    end

    assert :ok = FakeUpstream.verify!(upstream)
    drop!(client)
  end

  defp receive_accepted_tail!(client, _setup, _upstream, _hold, expected, :coalesced), do: receive_n!(client, length(expected))

  defp receive_accepted_tail!(client, setup, upstream, hold, expected, :separate) do
    {client, frames} =
      expected
      |> Enum.with_index()
      |> Enum.reduce({client, []}, fn {expected_frame, ordinal}, {client, frames} ->
        assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^hold}, @detection_timeout_ms
        assert :ok = FakeUpstream.release_frame(upstream, hold)
        {client, [frame]} = receive_n!(client, 1)
        assert_frames!([frame], [expected_frame])
        if event_type(frame) == "response.created", do: assert_active_successor!(setup)
        {client, frames ++ [frame]}
      end)

    total = length(expected)
    assert_receive {:fake_upstream_frame_barrier, ^total, _handler, ^hold}, @detection_timeout_ms
    assert :ok = FakeUpstream.release_frame(upstream, hold)
    {client, frames}
  end

  defp assert_provider_steer_failure!(forwarding, mode, outcome) do
    put_owner_forwarding!(forwarding == :on)
    hold = make_ref()
    {response_id, _successor_id} = response_ids()
    terminal = original_terminal_events(response_id, :completed)
    upstream = start_steerable!(response_id, opening_events(response_id), terminal, [], hold, outcome: outcome)
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, mode)
    client = port |> connect!(setup, turn) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, _opening} = receive_n!(client, length(opening_events(response_id)))

    sent =
      case outcome do
        :response_not_found -> steer_frame("resp_synthetic_not_on_this_connection")
        :invalid_input -> steer_frame(response_id, %{"stream_id" => "synthetic_provider_owned_extra_key"})
      end

    {{client, [failed], [completed], [row]}, log} =
      with_info_log(fn ->
        {client, failed} = client |> send_frame!(sent) |> receive_n!(1)
        assert [open] = pool_requests(setup)
        assert open.status == "in_progress"
        assert is_nil(open.completed_at)
        assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^open.id and l.entry_kind == "settlement"), :count) == 0
        assert :ok = FakeUpstream.release_steerable(upstream, hold)
        assert_receive {:fake_upstream_steerable_released, ^handler, ^hold}, @detection_timeout_ms
        {client, completed} = receive_n!(client, length(terminal))
        rows = await_settled_rows!(setup, 1)
        Enum.each(rows, &await_downstream_delivery!/1)
        {await_idle_and_ping!(client), failed, completed, rows}
      end)

    assert_frames!([failed], [failed_event(CodexPooler.JSON.decode!(sent), outcome)])
    assert_frames!([completed], terminal)
    assert_response_settlement!(setup, row, @completed_usage, "response.completed")
    assert route_circuit_failures(setup.assignment.id) == []
    assert_steer_written_once!(upstream, sent)
    assert_one_physical_generation!(upstream)
    assert_diagnostic_logs!(log, forwarding, response_id, "response.steer.failed", "failed", nil)
    assert :ok = FakeUpstream.verify!(upstream)
    drop!(client)
  end

  defp assert_unsteered_native_lane_refusal!(forwarding, mode) do
    put_owner_forwarding!(forwarding == :on)
    hold = make_ref()
    # Synthetic adversarial provider: the reserved code alone must not turn an
    # ordinary response.create into an activated steering transaction.
    refusal = native_lane_error_event()
    response = FakeUpstream.websocket_terminal_then_close_barrier(refusal, notify: self(), release_ref: hold, code: 1000, reason: "")
    # provenance: synthetic_adversarial
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_request(response)]))
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, mode)
    client = port |> connect!(setup, turn) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_websocket_barrier, :before_terminal, handler, ^hold}, @detection_timeout_ms
    handler_monitor = Process.monitor(handler)
    state = await_socket_connection_state!(client.socket, &is_pid(Map.get(&1, :native_response_steering)))
    lane = state.native_response_steering
    lane_monitor = Process.monitor(lane)
    assert %{activated?: false, active: nil} = :sys.get_state(lane)
    assert FakeUpstream.websocket_steers(upstream) == []

    send(handler, {:fake_upstream_release_websocket, hold})
    assert_receive {:fake_upstream_websocket_barrier, :before_close, ^handler, ^hold}, @detection_timeout_ms
    send(handler, {:fake_upstream_release_websocket, hold})
    {client, frames} = receive_until_terminal!(client)
    assert_frames!(frames, [refusal])
    assert [row] = await_settled_rows!(setup, 1)
    assert row.status == "failed"
    assert row.last_error_code == "unsupported_native_inflight_message"
    assert row.retry_count == 0
    assert row.usage_status == "usage_unknown"
    refute Map.has_key?(row.request_metadata, "native_websocket_response_steering")
    assert [attempt] = request_attempts(row)
    assert attempt.status == "failed"
    assert attempt.retryable in [false, nil]
    assert attempt.response_metadata["rejection_error_code"] == "unsupported_native_inflight_message"
    assert %{"outcome" => "delivered", "terminal_class" => "error"} = await_downstream_delivery!(row)
    assert_complete_ledger!(row, attempt)
    client = await_idle_and_ping!(client)
    assert %{activated?: false, active: nil} = :sys.get_state(lane)
    assert_receive {:DOWN, ^handler_monitor, :process, ^handler, _reason}, @detection_timeout_ms
    assert [%{websocket_connection_id: 1, json: %{"type" => "response.create"}}] = FakeUpstream.requests(upstream)
    assert FakeUpstream.physical_counts(upstream).websocket_generation == 1
    assert FakeUpstream.physical_counts(upstream).http_generation == 0
    assert FakeUpstream.websocket_steers(upstream) == []
    assert :ok = FakeUpstream.verify!(upstream)
    drop!(client)
    assert_receive {:DOWN, ^lane_monitor, :process, ^lane, :normal}, @detection_timeout_ms
  end

  defp assert_native_lane_refusal!(forwarding, mode) do
    put_owner_forwarding!(forwarding == :on)
    hold = make_ref()
    {response_id, _successor_id} = response_ids()
    opening = encode([created(response_id)])
    upstream = start_steerable!(response_id, opening, [], [], hold, outcome: :native_lane_error)
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, mode)
    client = port |> connect!(setup, turn) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_steerable_open, _handler, ^hold}, @detection_timeout_ms
    {client, [created]} = receive_n!(client, 1)
    assert_frames!([created], opening)
    # The fixture scripts the observed refusal, never a Full/Lite policy.
    sent = steer_frame(response_id)

    {{client, frames, code, reason, [row]}, log} =
      with_info_log(fn ->
        {client, frames, code, reason} = client |> send_frame!(sent) |> receive_until_close!()
        drop!(client)
        rows = await_settled_rows!(setup, 1)
        Enum.each(rows, &await_downstream_delivery!/1)
        {client, frames, code, reason, rows}
      end)

    assert {code, reason} == {1000, ""}
    assert_frames!(frames, [native_lane_error_event()])
    assert row.status == "failed"
    assert row.retry_count == 0
    assert row.usage_status == "usage_unknown"
    assert row.completed_at != nil
    assert row.last_error_code == "unsupported_native_inflight_message"
    assert [attempt] = request_attempts(row)
    assert attempt.status == "failed"
    assert attempt.retryable in [false, nil]
    assert attempt.usage_status == "usage_unknown"
    assert attempt.response_metadata["error_kind"] == "unsupported_native_inflight_message"
    assert attempt.response_metadata["rejection_error_code"] == "unsupported_native_inflight_message"
    assert attempt.response_metadata["rejection_error_type"] == "invalid_request_error"
    assert %{"outcome" => "delivered", "terminal_class" => "error", "transport" => "websocket"} = await_downstream_delivery!(row)
    assert_complete_ledger!(row, attempt)
    assert_steer_written_once!(upstream, sent)
    assert_one_physical_generation!(upstream)
    assert log =~ "frame_type=error outcome=native_lane_error topology=#{topology(forwarding)} steer_id_fingerprint=none"
    assert_safe_diagnostics!(log, [response_id])
    assert :ok = FakeUpstream.verify!(upstream)
    assert client.frames == []
  end

  defp assert_late_lite_steering!(forwarding) do
    put_owner_forwarding!(forwarding == :on)
    hold = make_ref()
    {response_id, successor_id} = response_ids()
    terminal = original_terminal_events(response_id, :completed)
    successor = completed_events(successor_id, @successor_usage)
    upstream = start_steerable!(response_id, opening_events(response_id), terminal, successor, hold)
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, "lite")
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, "lite")
    client = port |> connect!(setup, turn) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, _opening} = receive_n!(client, length(opening_events(response_id)))
    assert :ok = FakeUpstream.release_steerable(upstream, hold)
    assert_receive {:fake_upstream_steerable_released, ^handler, ^hold}, @detection_timeout_ms
    {client, completed} = receive_n!(client, length(terminal))
    assert_frames!(completed, terminal)
    assert [original] = await_settled_rows!(setup, 1)
    _delivery = await_downstream_delivery!(original)
    client = await_idle_and_ping!(client)
    sent = steer_frame(response_id)

    {{client, tail, [original, successor_row]}, log} =
      with_info_log(fn ->
        {client, tail} = client |> send_frame!(sent) |> receive_n!(1 + length(successor))
        assert_receive {:fake_upstream_steered, ^handler, ^hold}, @detection_timeout_ms
        rows = await_settled_rows!(setup, 2)
        Enum.each(rows, &await_downstream_delivery!/1)
        {await_idle_and_ping!(client), tail, rows}
      end)

    assert_frames!(tail, [accepted_event(response_id)] ++ successor)
    assert_two_response_settlements!(setup, original, successor_row, response_id, successor_id, :completed)
    assert %{"frames_after_visible" => 9} = await_downstream_delivery!(original)
    assert %{"frames_after_visible" => 5, "completed_items" => 1} = await_downstream_delivery!(successor_row)
    assert_steer_written_once!(upstream, sent)
    assert_one_physical_generation!(upstream)
    assert_diagnostic_logs!(log, forwarding, response_id, "response.steer.accepted", "accepted", @steer_id)
    assert :ok = FakeUpstream.verify!(upstream)
    drop!(client)
  end

  defp assert_late_full_close!(forwarding, opts \\ []) do
    put_owner_forwarding!(forwarding == :on)
    peer? = Keyword.get(opts, :peer?, false)
    if peer?, do: enter_peer_owner_topology!()
    hold = make_ref()
    {response_id, _successor_id} = response_ids()
    terminal = original_terminal_events(response_id, :completed)
    upstream = start_steerable!(response_id, opening_events(response_id), terminal, [], hold, outcome: :post_terminal_close)
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, "full")
    turn = new_turn(setup, "full")
    peer_owner = if peer?, do: start_peer_window_owner!(setup, "#{turn.thread}:0")
    if peer?, do: assert(node(peer_owner.owner_pid) != node())
    {_server, port} = start_public_endpoint_with_server!()
    client = port |> connect!(setup, turn) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, _opening} = receive_n!(client, length(opening_events(response_id)))
    assert :ok = FakeUpstream.release_steerable(upstream, hold)
    assert_receive {:fake_upstream_steerable_released, ^handler, ^hold}, @detection_timeout_ms
    {client, completed} = receive_n!(client, length(terminal))
    assert_frames!(completed, terminal)
    assert [original] = await_settled_rows!(setup, 1)
    _delivery = await_downstream_delivery!(original)
    client = await_idle_and_ping!(client)
    lane = if Keyword.get(opts, :hold_close?, false), do: hold_lane_peer_close!(client)
    sent = steer_frame(response_id)

    {{client, frames, code, reason}, log} =
      with_info_log(fn ->
        result = client |> send_frame!(sent) |> receive_until_close!()

        if lane do
          assert_receive {:peer_close_held, ^lane, 1000, true}, @detection_timeout_ms
          send(lane, :release_peer_close)
        end

        {closed, _frames, _code, _reason} = result
        drop!(closed)
        result
      end)

    assert {frames, code, reason} == {[], 1000, ""}
    assert [row] = await_settled_rows!(setup, 1)
    assert row.id == original.id
    assert_response_settlement!(setup, row, @completed_usage, "response.completed")
    assert_steer_written_once!(upstream, sent)
    assert_one_physical_generation!(upstream)
    assert_safe_diagnostics!(log, [response_id])
    assert :ok = FakeUpstream.verify!(upstream)
    assert client.frames == []
  end

  defp hold_lane_peer_close!(client) do
    lane = await_socket_connection_state!(client.socket, &(MapSet.size(&1.tasks) == 0)).native_response_steering
    observer = self()
    on_exit(fn -> send(lane, :release_peer_close) end)

    :ok =
      :sys.install(
        lane,
        {fn
           acc, {:in, {:"$gen_cast", {:peer_close, code, reason}}}, _name ->
             send(observer, {:peer_close_held, self(), code, reason == ""})

             receive do
               :release_peer_close -> acc
             after
               @detection_timeout_ms -> raise "steering peer-close barrier timed out"
             end

           acc, _event, _name ->
             acc
         end, :ok}
      )

    lane
  end

  defp assert_client_owned_tool_continuation!(forwarding, mode, successor?) do
    put_owner_forwarding!(forwarding == :on)
    hold = make_ref()
    {response_id, successor_id} = response_ids()
    call_id = "call_synthetic_steer_pending_tool"
    call = %{"type" => "function_call", "id" => "fc_synthetic_steer_pending_tool", "call_id" => call_id, "name" => "synthetic_client_lookup", "arguments" => @tool_arguments, "status" => "completed"}
    opening = encode([created(response_id), %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{call | "arguments" => "", "status" => "in_progress"}}])
    terminal = encode([%{"type" => "response.function_call_arguments.done", "item_id" => call["id"], "output_index" => 0, "arguments" => @tool_arguments}, %{"type" => "response.output_item.done", "output_index" => 0, "item" => call}, completed(response_id, [call], @original_usage)])
    successor = if successor?, do: completed_events(successor_id, @successor_usage), else: []
    anchor = if successor?, do: successor_id, else: response_id
    tool_output = %{"type" => "function_call_output", "call_id" => call_id, "output" => @tool_output_text}
    follow_up_id = "resp_synthetic_after_client_tool_#{System.unique_integer([:positive])}"

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          turn_request(FakeUpstream.websocket_steerable(opening, notify: self(), ref: hold, response_id: response_id, terminal_frames: terminal, successor_frames: successor)),
          FakeUpstream.expect_request(method: "WEBSOCKET", path: @turn_path, websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => anchor}], respond: FakeUpstream.websocket_text_frames(completed_events(follow_up_id, @tool_follow_up_usage)))
        ])
      )

    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    {_server, port} = start_public_endpoint_with_server!()
    turn = new_turn(setup, mode)
    client = port |> connect!(setup, turn) |> prewarm!(turn) |> send_frame!(opener_frame(turn))
    assert_receive {:fake_upstream_steerable_open, handler, ^hold}, @detection_timeout_ms
    {client, opening_received} = receive_n!(client, length(opening))
    assert_frames!(opening_received, opening)
    sent = steer_frame(response_id)
    expected = [accepted_event(response_id)] ++ terminal ++ successor

    {{client, accepted, rows_before_tool}, log} =
      with_info_log(fn ->
        {client, accepted} = client |> send_frame!(sent) |> receive_n!(length(expected))
        assert_receive {:fake_upstream_steered, ^handler, ^hold}, @detection_timeout_ms
        rows = await_settled_rows!(setup, if(successor?, do: 2, else: 1))
        Enum.each(rows, &await_downstream_delivery!/1)
        {await_idle_and_ping!(client), accepted, rows}
      end)

    assert_frames!(accepted, expected)
    assert [opener] = FakeUpstream.requests(upstream)
    assert opener.websocket_connection_id == 1
    assert_one_physical_generation!(upstream)
    assert_steer_written_once!(upstream, sent)
    assert Enum.all?(accepted, fn raw -> event_type(raw) != "response.function_call_output" end)
    assert_diagnostic_logs!(log, forwarding, response_id, "response.steer.accepted", "accepted", @steer_id)
    assert [original | later] = rows_before_tool
    assert_response_settlement!(setup, original, @original_usage, "response.completed")

    if successor? do
      assert [successor_row] = later
      assert_response_settlement!(setup, successor_row, @successor_usage, "response.completed")
      assert successor_row.request_metadata["native_websocket_response_steering"]["predecessor_request_id"] == original.id
    else
      assert later == []
    end

    # Acceptance removes steering from the client's delta. With a pending
    # result only the output is sent; the provider prepends accepted steering.
    explicit = frame(turn, "turn", [tool_output], %{"previous_response_id" => anchor})
    {client, follow_up} = client |> send_frame!(explicit) |> receive_n!(length(completed_events(follow_up_id, @tool_follow_up_usage)))
    assert_frames!(follow_up, completed_events(follow_up_id, @tool_follow_up_usage))
    rows = await_settled_rows!(setup, length(rows_before_tool) + 1)
    assert_response_settlement!(setup, List.last(rows), @tool_follow_up_usage, "response.completed")
    client = await_idle_and_ping!(client)
    assert [_opener, output_request] = FakeUpstream.requests(upstream)
    assert output_request.websocket_connection_id == 1
    assert output_request.json["previous_response_id"] == anchor
    assert Enum.find(output_request.json["input"], &(&1["type"] == "function_call_output")) == tool_output
    refute Enum.any?(output_request.json["input"], &(&1 == user_message(@steer_text)))
    refute CodexPooler.JSON.encode!(output_request.json["input"]) =~ @steer_text
    assert FakeUpstream.physical_counts(upstream).websocket_generation == 2
    assert FakeUpstream.physical_counts(upstream).http_generation == 0
    assert length(FakeUpstream.websocket_steers(upstream)) == 1
    assert route_circuit_failures(setup.assignment.id) == []
    assert :ok = FakeUpstream.verify!(upstream)
    drop!(client)
  end

  defp assert_active_successor!(setup) do
    assert [original, successor] = await_rows!(setup, 2, fn rows -> Enum.at(rows, 0).status == "succeeded" and Enum.at(rows, 1).status == "in_progress" end)
    assert original.id != successor.id
    assert is_nil(successor.completed_at)
    assert [attempt] = request_attempts(successor)
    assert attempt.completed_at == nil
    assert attempt.attempt_number == 1
    assert [%LedgerEntry{entry_kind: "reservation"}] = request_ledger(successor)
    assert successor.request_metadata["native_websocket_response_steering"]["predecessor_request_id"] == original.id
  end

  defp assert_two_response_settlements!(setup, original, successor, original_id, successor_id, ending) do
    assert original.id != successor.id
    assert original.correlation_id != successor.correlation_id
    assert "codex-turn:" <> _claim = original.correlation_id
    assert "native-ws-steer:" <> _successor_claim = successor.correlation_id
    assert original_usage(ending) != @successor_usage
    assert_response_settlement!(setup, original, original_usage(ending), terminal_type(ending))
    assert_response_settlement!(setup, successor, @successor_usage, "response.completed")
    assert %{"predecessor_request_id" => predecessor, "response_id_fingerprint" => successor_fingerprint} = successor.request_metadata["native_websocket_response_steering"]
    assert predecessor == original.id
    assert successor_fingerprint == fingerprint(successor_id)
    assert [original_attempt] = request_attempts(original)
    assert [successor_attempt] = request_attempts(successor)
    assert original_attempt.id != successor_attempt.id
    assert original_attempt.pool_upstream_assignment_id == successor_attempt.pool_upstream_assignment_id
    assert original_attempt.upstream_identity_id == successor_attempt.upstream_identity_id

    if ending == :steered do
      assert %{"incomplete_reason" => "steered"} = await_downstream_delivery!(original)
    end

    assert route_circuit_failures(setup.assignment.id) == []
    metadata = inspect({original.request_metadata, successor.request_metadata, original_attempt.response_metadata, successor_attempt.response_metadata, Enum.map(request_ledger(original) ++ request_ledger(successor), & &1.details)})
    for sentinel <- [original_id, successor_id, @steer_id, @steer_text, @tool_output_text], do: refute(metadata =~ sentinel)
  end

  defp assert_response_settlement!(setup, row, usage, terminal_class) do
    assert row.status == "succeeded"
    assert row.usage_status == "usage_known"
    assert row.transport == "websocket"
    assert row.endpoint == @turn_path
    assert row.retry_count == 0
    assert row.last_error_code == nil
    assert row.completed_at != nil
    assert [attempt] = request_attempts(row)
    assert attempt.status == "succeeded"
    assert attempt.usage_status == "usage_known"
    assert attempt.transport == "websocket"
    assert attempt.attempt_number == 1
    assert attempt.pool_upstream_assignment_id == setup.assignment.id
    assert attempt.upstream_identity_id == setup.identity.id
    assert attempt.retryable in [nil, false]
    assert attempt.network_error_code == nil
    assert attempt.completed_at != nil
    assert_complete_ledger!(row, attempt)
    assert [settlement] = Enum.filter(request_ledger(row), &(&1.entry_kind == "settlement"))
    assert settlement.usage_status == "usage_known"
    assert settlement.attempt_id == attempt.id
    assert Map.take(settlement, [:input_tokens, :cached_input_tokens, :output_tokens, :reasoning_tokens, :total_tokens]) == usage_totals(usage)
    assert %{"outcome" => "delivered", "terminal_class" => ^terminal_class, "transport" => "websocket", "pushed_at" => pushed_at} = await_downstream_delivery!(row)
    assert {:ok, _timestamp, 0} = DateTime.from_iso8601(pushed_at)
  end

  defp assert_complete_ledger!(row, attempt) do
    entries = request_ledger(row)
    assert Enum.frequencies_by(entries, & &1.entry_kind) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
    assert Enum.all?(entries, &(&1.amount_status == "recorded"))
    assert [reservation] = Enum.filter(entries, &(&1.entry_kind == "reservation"))
    assert reservation.attempt_id == nil
    assert reservation.total_tokens > 0
    assert reservation.request_count == 1
    assert [settlement] = Enum.filter(entries, &(&1.entry_kind == "settlement"))
    assert settlement.attempt_id == attempt.id
    assert [release] = Enum.filter(entries, &(&1.entry_kind == "release"))
    assert release.attempt_id == attempt.id
    assert release.total_tokens == reservation.total_tokens
    assert release.details["reservation_source_event_id"] == reservation.source_event_id
    assert release.details["released_by_source_event_id"] == settlement.source_event_id
  end

  defp usage_totals(usage), do: %{input_tokens: usage["input_tokens"], cached_input_tokens: get_in(usage, ["input_tokens_details", "cached_tokens"]), output_tokens: usage["output_tokens"], reasoning_tokens: get_in(usage, ["output_tokens_details", "reasoning_tokens"]), total_tokens: usage["total_tokens"]}

  defp assert_one_physical_generation!(upstream) do
    assert [%{websocket_connection_id: 1, json: %{"type" => "response.create"}}] = FakeUpstream.requests(upstream)
    assert FakeUpstream.physical_counts(upstream).websocket_generation == 1
    assert FakeUpstream.physical_counts(upstream).http_generation == 0
    assert [%{kind: :generation, connection_id: 1}, %{kind: :other, connection_id: 1}] = Enum.filter(FakeUpstream.physical_receipts(upstream), &(&1.transport == :websocket))
  end

  defp assert_serving_shape!(upstream, mode) do
    assert [request] = FakeUpstream.requests(upstream)

    case mode do
      "full" ->
        assert request.json["instructions"] == "synthetic full instructions"
        assert request.json["tools"] == [tool()]
        refute get_in(request.json, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"])

      "lite" ->
        assert get_in(request.json, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"]) == "true"
        assert get_in(request.json, ["reasoning", "context"]) == "all_turns"
        refute Map.has_key?(request.json, "tools")
        assert hd(request.json["input"])["type"] == "additional_tools"
    end
  end

  defp assert_steer_written_once!(upstream, sent) do
    assert [%{websocket_connection_id: 1, body: body, json: json}] = FakeUpstream.websocket_steers(upstream)
    assert digest(body) == digest(sent)
    assert json == CodexPooler.JSON.decode!(sent)
  end

  defp assert_diagnostic_logs!(log, forwarding, response_id, frame_type, outcome, steer_id) do
    assert log =~ "native websocket response steer"
    assert log =~ "frame_type=response.steer outcome=written topology=#{topology(forwarding)}"
    assert log =~ "frame_type=#{frame_type} outcome=#{outcome} topology=#{topology(forwarding)}"
    assert log =~ "steer_id_fingerprint=#{if steer_id, do: fingerprint(steer_id), else: "none"}"
    assert_safe_diagnostics!(log, [response_id, "resp_synthetic_not_on_this_connection"])
  end

  defp assert_safe_diagnostics!(log, response_ids) do
    lines = log |> String.split("\n", trim: true) |> Enum.filter(&String.contains?(&1, "native websocket response steer "))
    assert lines != []

    for line <- lines do
      assert byte_size(line) < 512
      assert Regex.match?(~r/frame_type=[A-Za-z0-9_.-]{1,80} outcome=[a-z0-9_]{1,80} topology=(?:direct|owner) steer_id_fingerprint=(?:none|[a-f0-9]{12})(?:\s|$)/, line)
    end

    for sentinel <- response_ids ++ [@steer_id, @steer_text, @tool_output_text, @tool_arguments, "synthetic original answer", "synthetic successor answer"], do: refute(log =~ sentinel)
  end

  defp topology(:off), do: "direct"
  defp topology(:on), do: "owner"
  defp fingerprint(value), do: value |> digest() |> Base.encode16(case: :lower) |> binary_part(0, 12)
  defp digest(value), do: :crypto.hash(:sha256, value)

  defp start_steerable!(response_id, opening, terminal, successor, hold, opts \\ []) do
    response = FakeUpstream.websocket_steerable(opening, Keyword.merge([notify: self(), ref: hold, response_id: response_id, terminal_frames: terminal, successor_frames: successor], opts))
    start_upstream(FakeUpstream.strict_sequence([turn_request(response)]))
  end

  defp turn_request(response), do: FakeUpstream.expect_request(method: "WEBSOCKET", path: @turn_path, websocket_connection_ordinal: 1, json: [valid: true, equals: %{"type" => "response.create"}], respond: response)

  defp response_ids do
    suffix = System.unique_integer([:positive])
    {"resp_synthetic_steer_original_#{suffix}", "resp_synthetic_steer_successor_#{suffix}"}
  end

  defp opening_events(response_id) do
    item = message_item(response_id, "in_progress", [])
    encode([created(response_id), %{"type" => "response.output_item.added", "output_index" => 0, "item" => item}] ++ for(sequence <- 1..6, do: %{"type" => "response.output_text.delta", "item_id" => item["id"], "output_index" => 0, "content_index" => 0, "sequence_number" => sequence, "delta" => "synthetic partial output #{sequence}"}))
  end

  defp original_terminal_events(response_id, :steered), do: encode([%{"type" => "response.incomplete", "response" => %{"id" => response_id, "status" => "incomplete", "incomplete_details" => %{"reason" => "steered"}, "output" => [], "usage" => @original_usage}}])
  defp original_terminal_events(response_id, :completed), do: encode([completed(response_id, [message_item(response_id, "completed", [%{"type" => "output_text", "text" => "synthetic original answer"}])], @completed_usage)])
  defp original_usage(:steered), do: @original_usage
  defp original_usage(:completed), do: @completed_usage
  defp terminal_type(:steered), do: "response.incomplete"
  defp terminal_type(:completed), do: "response.completed"

  defp completed_events(response_id, usage) do
    item = message_item(response_id, "completed", [%{"type" => "output_text", "text" => "synthetic successor answer"}])
    encode([created(response_id), %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{item | "status" => "in_progress", "content" => []}}, %{"type" => "response.output_text.delta", "item_id" => item["id"], "output_index" => 0, "content_index" => 0, "delta" => "synthetic successor answer"}, %{"type" => "response.output_item.done", "output_index" => 0, "item" => item}, completed(response_id, [item], usage)])
  end

  defp created(response_id), do: %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}}
  defp completed(response_id, output, usage), do: %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => output, "usage" => usage}}
  defp message_item(response_id, status, content), do: %{"type" => "message", "id" => "msg_#{response_id}", "role" => "assistant", "status" => status, "content" => content}
  defp encode(events), do: Enum.map(events, &CodexPooler.JSON.encode!/1)
  defp accepted_event(response_id), do: CodexPooler.JSON.encode!(%{"type" => "response.steer.accepted", "sequence_number" => 1, "steer" => %{"id" => @steer_id, "previous_response_id" => response_id}})

  defp failed_event(frame, outcome) do
    message = if outcome == :response_not_found, do: "The target response is not available on this connection.", else: "response.steer requires only type, previous_response_id, and input."
    CodexPooler.JSON.encode!(%{"type" => "response.steer.failed", "sequence_number" => 1, "steer" => Map.take(frame, ["previous_response_id", "input"]), "error" => %{"type" => "invalid_request_error", "code" => Atom.to_string(outcome), "message" => message}})
  end

  defp native_lane_error_event, do: CodexPooler.JSON.encode!(%{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "code" => "unsupported_native_inflight_message", "message" => @native_lane_message}})
  defp steer_frame(response_id, extra \\ %{}), do: %{"type" => "response.steer", "previous_response_id" => response_id, "input" => [user_message(@steer_text)]} |> Map.merge(extra) |> CodexPooler.JSON.encode!()

  defp new_turn(setup, mode), do: %{model: setup.model.exposed_model_id, thread: Ecto.UUID.generate(), turn_id: Ecto.UUID.generate(), context: Ecto.UUID.generate(), mode: mode, started_at: System.system_time(:millisecond)}

  defp connect!(port, setup, turn, path \\ @turn_path) do
    before = WebsocketCleanupFence.listener_sockets()
    headers = [{"session-id", turn.thread}, {"thread-id", turn.thread}, {"x-client-request-id", turn.thread}, {"x-codex-window-id", "#{turn.thread}:0"}, {"x-codex-turn-metadata", CodexPooler.JSON.encode!(turn_metadata(turn, "prewarm", ""))}, {"openai-beta", "responses_websockets=2026-02-06"}, {"originator", "codex_cli_rs"}]
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, turn.thread, path, headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket, frames: []}
  end

  defp drop!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  defp prewarm!(client, turn) do
    prewarm = frame(turn, "prewarm", [developer_message("synthetic developer instructions")], %{"generate" => false})
    {client, events} = client |> send_frame!(prewarm) |> receive_until_terminal!()
    assert %{"type" => "response.completed", "response" => %{"id" => ""}} = events |> List.last() |> CodexPooler.JSON.decode!()
    client
  end

  defp opener_frame(turn) do
    input = [developer_message("synthetic developer instructions"), user_message("synthetic initial user input")]
    input = if turn.mode == "lite", do: [%{"type" => "additional_tools", "id" => "at_synthetic_steering", "role" => "developer", "tools" => [tool()]} | input], else: input
    frame(turn, "turn", input, %{})
  end

  defp frame(turn, kind, input, extra) do
    base = %{"type" => "response.create", "model" => turn.model, "tool_choice" => "auto", "parallel_tool_calls" => true, "reasoning" => %{"effort" => "low"}, "store" => false, "stream" => true, "include" => ["reasoning.encrypted_content"], "prompt_cache_key" => turn.thread, "input" => input, "client_metadata" => client_metadata(turn, kind)}
    base = if turn.mode == "full", do: Map.merge(base, %{"instructions" => "synthetic full instructions", "tools" => [tool()]}), else: base |> Map.put("parallel_tool_calls", false) |> put_in(["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true")
    base |> Map.merge(extra) |> CodexPooler.JSON.encode!()
  end

  defp client_metadata(turn, kind) do
    turn_id = if kind == "prewarm", do: "", else: turn.turn_id
    metadata = %{"session_id" => turn.thread, "thread_id" => turn.thread, "turn_id" => turn_id, "x-codex-installation-id" => @installation_id, "x-codex-window-id" => "#{turn.thread}:0", "x-codex-turn-metadata" => CodexPooler.JSON.encode!(turn_metadata(turn, kind, turn_id))}
    if kind == "prewarm", do: metadata, else: Map.put(metadata, "root_turn_id", turn.turn_id)
  end

  defp turn_metadata(turn, kind, turn_id) do
    metadata = %{"installation_id" => @installation_id, "session_id" => turn.thread, "thread_id" => turn.thread, "turn_id" => turn_id, "window_id" => "#{turn.thread}:0", "window_number" => 0, "context_window_id" => turn.context, "request_kind" => kind, "thread_source" => "user", "model" => turn.model, "reasoning_effort" => "low"}
    if kind == "prewarm", do: metadata, else: Map.merge(metadata, %{"root_turn_id" => turn.turn_id, "turn_trigger" => "exec", "turn_started_at_unix_ms" => turn.started_at})
  end

  defp tool, do: %{"type" => "function", "name" => "synthetic_client_lookup", "description" => "synthetic client owned tool", "strict" => false, "parameters" => %{"type" => "object", "properties" => %{"key" => %{"type" => "string"}}, "required" => ["key"]}}
  defp developer_message(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}
  defp user_message(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp send_frame!(client, frame) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_n!(client, count), do: receive_n!(client, count, [])
  defp receive_n!(client, 0, frames), do: {client, Enum.reverse(frames)}

  defp receive_n!(client, count, frames) do
    {client, frame} = receive_frame!(client)

    case frame do
      {:text, raw} -> receive_n!(client, count - 1, [raw | frames])
      {:close, code, _reason} -> flunk("native websocket closed #{code} before all expected events")
    end
  end

  defp receive_until_terminal!(client, frames \\ []) do
    {client, [raw]} = receive_n!(client, 1)
    frames = frames ++ [raw]
    if event_type(raw) in ["response.completed", "response.incomplete", "response.failed", "error"], do: {client, frames}, else: receive_until_terminal!(client, frames)
  end

  defp receive_until_close!(client, frames \\ []) do
    {client, frame} = receive_frame!(client)

    case frame do
      {:close, code, reason} -> {client, Enum.reverse(frames), code, reason}
      {:text, raw} -> receive_until_close!(client, [raw | frames])
    end
  end

  # Unlike a terminal-only helper, this queue retains every decoded trailing
  # frame, including the successor created/terminal and a coalesced Close.
  defp receive_frame!(%{frames: [frame | rest]} = client), do: {%{client | frames: rest}, frame}

  defp receive_frame!(client) do
    message = receive_mint_socket_message!(client.conn, @detection_timeout_ms, "timed out waiting for steering lifecycle frame")

    case Mint.WebSocket.stream(client.conn, message) do
      {:ok, conn, responses} ->
        {websocket, frames} = Enum.reduce(responses, {client.websocket, []}, &decode_response(&1, &2, client.ref))
        receive_frame!(%{client | conn: conn, websocket: websocket, frames: frames})

      {:error, _conn, reason, _responses} ->
        flunk("steering websocket receive failed: #{inspect(reason)}")

      :unknown ->
        receive_frame!(client)
    end
  end

  defp decode_response({:data, ref, data}, {websocket, frames}, ref) do
    assert {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)

    frames =
      frames ++
        Enum.filter(decoded, fn
          {:text, raw} -> not String.starts_with?(event_type(raw) || "", "codex.")
          {:close, _code, _reason} -> true
          _control -> false
        end)

    {websocket, frames}
  end

  defp decode_response(_response, acc, _ref), do: acc

  defp assert_frames!(received, expected) do
    assert Enum.map(received, &event_type/1) == Enum.map(expected, &event_type/1)
    assert Enum.map(received, &digest/1) == Enum.map(expected, &digest/1)
  end

  defp event_type(raw), do: CodexPooler.JSON.decode!(raw)["type"]
  defp response_id(raw), do: get_in(CodexPooler.JSON.decode!(raw), ["response", "id"])

  defp await_idle_and_ping!(client) do
    _state = await_socket_connection_state!(client.socket, &(MapSet.size(&1.tasks) == 0))
    assert client.frames == []
    {conn, websocket} = socket_transport_barrier!(client.conn, client.websocket, client.ref)
    %{client | conn: conn, websocket: websocket}
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))
  defp request_attempts(row), do: Repo.all(from(a in Attempt, where: a.request_id == ^row.id, order_by: [asc: a.attempt_number]))
  defp request_ledger(row), do: Repo.all(from(l in LedgerEntry, where: l.request_id == ^row.id))
  defp await_settled_rows!(setup, count), do: await_rows!(setup, count, &Enum.all?(&1, fn row -> row.completed_at != nil and row.status not in ["accepted", "in_progress"] end))

  defp await_rows!(setup, count, ready?, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    rows = pool_requests(setup)

    cond do
      length(rows) == count and ready?.(rows) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("steering lifecycle did not expose #{count} requests in the required phase; statuses=#{inspect(Enum.map(rows, & &1.status))}")

      true ->
        receive do
        after
          5 -> await_rows!(setup, count, ready?, deadline)
        end
    end
  end

  defp await_downstream_delivery!(row, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    attempts = request_attempts(row)

    receipt =
      case attempts do
        [attempt] -> Map.get(attempt.response_metadata || %{}, "downstream_delivery")
        _not_started -> nil
      end

    cond do
      is_map(receipt) ->
        receipt

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("steering response has no independent downstream delivery receipt")

      true ->
        receive do
        after
          5 -> await_downstream_delivery!(row, deadline)
        end
    end
  end
end
