defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketDrainAfterTerminalTest do
  # A drain's cut that lands after a turn's terminal reached the client but
  # before the turn settled (findings#287): the client holds its response, and
  # only the settlement, which the socket's response task runs, is left. The
  # cut treated that turn as live: the client got a 503 `owner_drained` error
  # frame after its `response.completed` (on a socket that stays open, the
  # released client reads it as the first event of its next request), the task
  # was stopped, and the request was recorded `failed/owner_drained` with the
  # provider's usage lost.
  #
  # Now the socket node's drain of its own tasks (owner forwarding off, and a
  # socket whose owner is on another VM) leaves a task whose terminal went out
  # to settle as the client got it, within half its post-deadline budget, and
  # nothing follows the terminal on the wire. A task still unsettled at that
  # bound is cut as it always was: an error frame follows the terminal, so the
  # client resends, and the request is recorded `failed/owner_drained`.
  #
  # Determinism: the provider holds its answer at a barrier until the socket's
  # response task is suspended, so the terminal reaches the client while the
  # turn cannot settle; the test then applies the drain's cut exactly as the
  # rollout drain does (`ActivityDrain.drain/4` past its deadline) and resumes
  # the task.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full); one node with owner forwarding off, and the session's owner on a
  # second VM with the socket node's drain.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true

  @detection_timeout_ms 15_000
  @turn_path "/backend-api/codex/responses"

  test "without owner forwarding: the socket node's drain leaves a task whose terminal went out to settle as answered" do
    put_owner_forwarding!(false)
    turn = relayed_turn_held!(start_turn!())

    assert_task_drain_waits_for_settlement!(turn, :direct)
    assert receive_frames_for!(turn.client, 300) == []
    drop!(turn.client)
    assert_settled_as_answered!(turn.setup)
  end

  @tag slow: "boots a second VM that owns the session and shares the committed database"
  test "owner on another VM: the socket node's drain leaves the proxy task whose terminal went out to settle as answered" do
    put_owner_forwarding!(true)
    enter_peer_owner_topology!()
    turn = relayed_turn_held!(start_turn!(peer: true))

    assert_task_drain_waits_for_settlement!(turn, :proxy)
    assert receive_frames_for!(turn.client, 300) == []
    drop!(turn.client)
    assert_settled_as_answered!(turn.setup)
  end

  # Once half its post-deadline budget is spent, the socket node's drain cuts
  # a task still settling as it cuts a live turn.
  test "without owner forwarding: a task still settling when half the post-deadline budget is spent is cut as it always was" do
    put_owner_forwarding!(false)
    turn = relayed_turn_held!(start_turn!())
    entry = activity_entry!(turn.task, :direct)
    policy = %{now_ms: fn -> System.monotonic_time(:millisecond) end, schedule_wait: &schedule_wait/3, cancel_wait: &cancel_wait/2, owner_post_deadline_call_budget_ms: 300}

    _outcome = ActivityDrain.drain(entry, System.monotonic_time(:millisecond) - 1, policy, ActivityRegistry)
    assert [error] = receive_frames_for!(turn.client, 300)
    assert_drained_error!(error)
    assert [%Request{status: "failed", last_error_code: "owner_drained"}] = await_settled_requests!(turn.setup)
    drop!(turn.client)
  end

  # The socket node's drain past its deadline, as `RolloutDrain` runs it for a
  # task of this node: it leaves the task alone (no cancellation reaches it),
  # and the task settles and finishes on its own within the post-deadline
  # budget instead of being stopped.
  defp assert_task_drain_waits_for_settlement!(turn, kind) do
    entry = activity_entry!(turn.task, kind)
    task_monitor = Process.monitor(turn.task)
    policy = %{now_ms: fn -> System.monotonic_time(:millisecond) end, schedule_wait: &schedule_wait/3, cancel_wait: &cancel_wait/2, owner_post_deadline_call_budget_ms: 5_000}
    drain = Task.async(fn -> ActivityDrain.drain(entry, System.monotonic_time(:millisecond) - 1, policy, ActivityRegistry) end)

    assert Task.yield(drain, 300) == nil
    assert [%Request{status: "in_progress"}] = pool_requests(turn.setup)

    true = :erlang.resume_process(turn.task)
    assert_receive {:DOWN, ^task_monitor, :process, _task, :normal}, @detection_timeout_ms
    assert {:ok, _outcome} = Task.yield(drain, @detection_timeout_ms)
  end

  # The drain's cut as it always was, after the terminal the client holds.
  defp assert_drained_error!(frame) do
    assert {:text, text} = frame
    assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_drained"}} = CodexPooler.JSON.decode!(text)
  end

  defp assert_settled_as_answered!(setup) do
    assert [row] = await_settled_requests!(setup)
    assert {row.status, row.last_error_code, row.response_status_code, row.usage_status} == {"succeeded", nil, 200, "usage_known"}
    assert [%Attempt{status: "succeeded"}] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^row.id))

    # One settlement, with the provider's usage, between the reservation and its release.
    entries = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^row.id, order_by: [asc: entry.created_at], select: {entry.entry_kind, entry.usage_status, entry.input_tokens, entry.output_tokens, entry.total_tokens}))
    assert Enum.map(entries, &elem(&1, 0)) == ["reservation", "settlement", "release"]
    assert Enum.filter(entries, &(elem(&1, 0) == "settlement")) == [{"settlement", "usage_known", 20, 5, 25}]
  end

  # The provider's answer is held until the turn's task is suspended; then the
  # answer is released and reaches the client whole, while the turn cannot
  # settle.
  defp relayed_turn_held!(%{client: client, upstream: upstream, hold: hold} = turn) do
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^hold}, @detection_timeout_ms
    state = await_socket_connection_state!(client.socket, &(MapSet.size(Map.get(&1, :tasks, MapSet.new())) > 0))
    [task] = MapSet.to_list(state.tasks)
    true = :erlang.suspend_process(task)
    on_exit(fn -> if Process.alive?(task), do: Process.exit(task, :kill) end)
    :ok = FakeUpstream.release_remaining_frames(upstream, hold)

    {client, events} = receive_until_terminal!(client)
    assert Enum.map(events, & &1["type"]) == ["response.created", "response.output_item.done", "response.completed"]
    assert [%Request{status: "in_progress"}] = pool_requests(turn.setup)

    Map.merge(turn, %{client: client, task: task, state: state})
  end

  defp start_turn!(opts \\ []) do
    hold = make_ref()
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_request(FakeUpstream.barrier_websocket_frames(message_events("resp_drain_after_terminal"), notify: self(), release_ref: hold))]))
    setup = gateway_setup(upstream)
    thread = Ecto.UUID.generate()
    owner = if Keyword.get(opts, :peer, false), do: start_peer_window_owner!(setup, "#{thread}:0").owner_pid
    {_server, port} = start_public_endpoint_with_server!()
    client = port |> connect!(setup, thread) |> send_frame!(turn_frame(setup, thread))
    %{client: client, setup: setup, upstream: upstream, hold: hold, owner: owner}
  end

  defp activity_entry!(task, kind) do
    assert %{kind: ^kind} = entry = Enum.find(ActivityRegistry.activities(), &(&1.pid == task))
    entry
  end

  defp schedule_wait(recipient, token, wait_ms), do: Process.send_after(recipient, {:rollout_drain_wait_elapsed, token}, wait_ms)

  defp cancel_wait(timer, _token) do
    _remaining = Process.cancel_timer(timer)
    :ok
  end

  defp pool_requests(setup), do: Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id))

  defp await_settled_requests!(setup) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    Stream.repeatedly(fn -> pool_requests(setup) end)
    |> Enum.find(fn rows ->
      cond do
        rows != [] and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"])) -> true
        System.monotonic_time(:millisecond) > deadline -> flunk("the turn never settled: #{inspect(Enum.map(rows, & &1.status))}")
        true -> Process.sleep(10) && false
      end
    end)
  end

  defp turn_request(respond) do
    FakeUpstream.expect_request(method: "WEBSOCKET", path: @turn_path, json: [valid: true, equals: %{"type" => "response.create"}], respond: respond)
  end

  defp message_events(response_id) do
    item = %{"type" => "message", "id" => "msg_#{response_id}", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 20, "output_tokens" => 5, "total_tokens" => 25}}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp connect!(port, setup, thread) do
    before = WebsocketCleanupFence.listener_sockets()
    headers = [{"session-id", thread}, {"thread-id", thread}, {"x-client-request-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"openai-beta", "responses_websockets=2026-02-06"}]
    {conn, websocket, ref, _response_headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @turn_path, headers)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket}
  end

  defp drop!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  defp turn_frame(setup, thread) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => native_text_input("synthetic prompt"),
      "tools" => [],
      "tool_choice" => "auto",
      "parallel_tool_calls" => true,
      "store" => false,
      "stream" => true,
      "prompt_cache_key" => thread,
      "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate()}
    })
  end

  defp send_frame!(client, frame) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_until_terminal!(client, events \\ []) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    event = CodexPooler.JSON.decode!(text)
    client = %{client | conn: conn, websocket: websocket}
    events = events ++ [event]

    if event["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {client, events},
      else: receive_until_terminal!(client, events)
  end

  # Every frame the client receives within `wait_ms`, the Close included.
  defp receive_frames_for!(client, wait_ms) do
    socket = Mint.HTTP.get_socket(client.conn)

    receive do
      {tag, ^socket, _data} = message when tag in [:tcp, :ssl] ->
        {:ok, conn, responses} = Mint.WebSocket.stream(client.conn, message)

        {websocket, frames} =
          Enum.reduce(responses, {client.websocket, []}, fn
            {:data, ref, data}, {websocket, frames} when ref == client.ref ->
              {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
              {websocket, frames ++ decoded}

            _response, acc ->
              acc
          end)

        frames ++ receive_frames_for!(%{client | conn: conn, websocket: websocket}, wait_ms)

      {tag, ^socket} when tag in [:tcp_closed, :ssl_closed] ->
        [:socket_closed]
    after
      wait_ms -> []
    end
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end
end
