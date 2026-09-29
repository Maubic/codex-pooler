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
  # Now the drain lets that turn settle as the client got it, and nothing
  # follows the terminal on the wire:
  # - the socket node's drain of its own tasks (owner forwarding off, and a
  #   socket whose owner is on another VM) leaves a task whose terminal went
  #   out to settle, within half its post-deadline budget;
  # - the owner (owner forwarding on) waits for the settlement, and for the
  #   provider session's result first when its own turn still waits for it,
  #   within the drain call's budget, then stops without an `owner_drained` to
  #   its downstream.
  # A turn still unsettled at those bounds is cut as it always was: an error
  # frame follows the terminal, so the client resends, and the request is
  # recorded `failed/owner_drained`.
  #
  # Determinism: the provider holds its answer at a barrier until the socket's
  # response task (or the owner's turn task) is suspended, so the terminal
  # reaches the client while the turn cannot settle; the test then applies the
  # drain's cut exactly as the rollout drain does
  # (`WebsocketOwnerSession.begin_drain/1` and `drain_owner/1` for an owner,
  # `ActivityDrain.drain/4` past its deadline for the socket node's tasks) and
  # resumes the task.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full); one node with the session's owner local (forwarding on) and with
  # forwarding off; the session's owner on a second VM, drained there or with
  # the socket node's drain.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry, OwnerDefaults, WebsocketOwnerSession}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true

  @detection_timeout_ms 15_000
  @turn_path "/backend-api/codex/responses"
  @drained_close {:close, 1001, "websocket owner is draining"}

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

  test "owner on this node: the cut waits for the forwarded turn's settlement, and nothing follows its terminal" do
    put_owner_forwarding!(true)
    turn = relayed_turn_held!(start_turn!())

    assert_owner_cut_waits_for_settlement!(turn, turn.state.websocket_owner_pid)
    assert receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2) == [@drained_close]
    assert_settled_as_answered!(turn.setup)
  end

  @tag slow: "boots a second VM that owns the session and shares the committed database"
  test "owner on another VM: the cut waits for the forwarded turn's settlement, and nothing follows its terminal" do
    put_owner_forwarding!(true)
    enter_peer_owner_topology!()
    turn = relayed_turn_held!(start_turn!(peer: true))

    assert node(turn.owner) != node()
    assert_owner_cut_waits_for_settlement!(turn, turn.owner)
    assert receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2) == [@drained_close]
    assert_settled_as_answered!(turn.setup)
  end

  # The owner forwarded the terminal while its own turn still waits for the
  # provider session's result (its turn task is held): the cut waits for that
  # result and the settlement after it, like a turn the owner already
  # finished.
  test "owner on this node: a cut while the forwarded turn still waits for its result waits for both" do
    put_owner_forwarding!(true)
    turn = owner_result_held!(start_turn!())
    :ok = WebsocketOwnerSession.begin_drain(turn.owner)
    monitor = Process.monitor(turn.owner)
    drain = Task.async(fn -> WebsocketOwnerSession.drain_owner(turn.owner) end)

    assert Task.yield(drain, 300) == nil
    assert [%Request{status: "in_progress"}] = pool_requests(turn.setup)

    true = :erlang.resume_process(turn.owner_task)
    assert Task.await(drain, @detection_timeout_ms) == :ok
    assert_receive {:DOWN, ^monitor, :process, _owner, :normal}, @detection_timeout_ms
    assert receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2) == [@drained_close]
    assert_settled_as_answered!(turn.setup)
  end

  # The owner's wait is bounded by the drain call's own budget: a settlement
  # still pending when it is spent is cut as it always was, so nothing is
  # left open.
  test "owner on this node: a settlement still pending when the budget is spent is cut as it always was" do
    put_owner_forwarding!(true)
    put_owner_call_timeout!(1_500)
    turn = relayed_turn_held!(start_turn!())
    owner = turn.state.websocket_owner_pid
    :ok = await_owner_settling!(owner)
    monitor = Process.monitor(owner)

    assert_waits_out_the_budget!(owner)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @detection_timeout_ms
    assert [%Request{status: "failed", last_error_code: "owner_drained"}] = pool_requests(turn.setup)

    Process.exit(turn.task, :kill)
    assert [error, @drained_close] = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2)
    assert_drained_error!(error)
  end

  test "owner on this node: a forwarded turn whose result is still missing when the budget is spent is cut as it always was" do
    put_owner_forwarding!(true)
    put_owner_call_timeout!(1_500)
    use_committed_repo!()
    turn = owner_result_held!(start_turn!(committed: true))
    :ok = WebsocketOwnerSession.begin_drain(turn.owner)
    monitor = Process.monitor(turn.owner)

    assert_waits_out_the_budget!(turn.owner)
    assert_receive {:DOWN, ^monitor, :process, _owner, :normal}, @detection_timeout_ms
    assert [error, @drained_close] = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2)
    assert_drained_error!(error)
    assert [%Request{status: "failed", last_error_code: "owner_drained"}] = await_settled_requests!(turn.setup)
  end

  # The owner cut as the rollout drain makes it: the owner already finished
  # the turn it forwarded and reports it active only because its settlement
  # is pending. While the task cannot settle, the owner neither stops nor
  # interrupts the request; once the task settles, the owner stops.
  defp assert_owner_cut_waits_for_settlement!(turn, owner) do
    :ok = await_owner_settling!(owner)
    monitor = Process.monitor(owner)
    drain = Task.async(fn -> WebsocketOwnerSession.drain_owner(owner) end)

    assert Task.yield(drain, 300) == nil
    assert [%Request{status: "in_progress"}] = pool_requests(turn.setup)

    true = :erlang.resume_process(turn.task)
    assert Task.await(drain, @detection_timeout_ms) == :ok
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @detection_timeout_ms
  end

  # With a 1.5 s owner call budget the owner waits 750 ms, and answers the
  # drain before the call's own timeout.
  defp assert_waits_out_the_budget!(owner) do
    started_at = System.monotonic_time(:millisecond)
    assert WebsocketOwnerSession.drain_owner(owner) == :ok
    assert (System.monotonic_time(:millisecond) - started_at) in 700..1_450
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

  # The owner relays the provider's answer, terminal included, while its turn
  # task, which waits for the provider session's result, is held: the client
  # holds its response and the owner's turn still waits for its result.
  defp owner_result_held!(%{client: client, upstream: upstream, hold: hold} = turn) do
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^hold}, @detection_timeout_ms
    %{websocket_owner_pid: owner} = await_socket_connection_state!(client.socket, &is_pid(Map.get(&1, :websocket_owner_pid)))
    assert %{active_turn: %{task_pid: owner_task}} = :sys.get_state(owner)
    true = :erlang.suspend_process(owner_task)
    on_exit(fn -> if Process.alive?(owner_task), do: Process.exit(owner_task, :kill) end)
    :ok = FakeUpstream.release_remaining_frames(upstream, hold)

    {client, events} = receive_until_terminal!(client)
    assert Enum.map(events, & &1["type"]) == ["response.created", "response.output_item.done", "response.completed"]
    assert %{active_turn: %{terminal_forwarded?: true, pending_result: nil}} = :sys.get_state(owner)
    assert [%Request{status: "in_progress"}] = pool_requests(turn.setup)

    Map.merge(turn, %{client: client, owner: owner, owner_task: owner_task})
  end

  defp start_turn!(opts \\ []) do
    hold = make_ref()
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_request(FakeUpstream.barrier_websocket_frames(message_events("resp_drain_after_terminal"), notify: self(), release_ref: hold))]))
    setup = gateway_setup(upstream)
    if Keyword.get(opts, :committed, false), do: register_unboxed_pool_cleanup!(setup)
    thread = Ecto.UUID.generate()
    owner = if Keyword.get(opts, :peer, false), do: start_peer_window_owner!(setup, "#{thread}:0").owner_pid
    {_server, port} = start_public_endpoint_with_server!()
    client = port |> connect!(setup, thread) |> send_frame!(turn_frame(setup, thread))
    %{client: client, setup: setup, upstream: upstream, hold: hold, owner: owner}
  end

  # The owner finished the forwarded turn (its terminal went out and the
  # provider's session answered), and the drain has begun: it reports the turn
  # active only because the settlement is pending.
  defp await_owner_settling!(owner) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    {:ok, %{active_turn?: false}} =
      Stream.repeatedly(fn -> WebsocketOwnerSession.owner_status(owner) end)
      |> Enum.find(fn
        {:ok, %{active_turn?: false}} -> true
        _active -> if System.monotonic_time(:millisecond) > deadline, do: flunk("the owner never finished the turn"), else: Process.sleep(5) && false
      end)

    :ok = WebsocketOwnerSession.begin_drain(owner)
    assert {:ok, %{draining?: true, active_turn?: true}} = WebsocketOwnerSession.owner_status(owner)
    :ok
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

  # The cut stops a task that can be inside a query, as it always did; in the
  # sandbox that breaks the one connection every process of the test shares,
  # so those tests commit their rows, as the peer arms do.
  defp use_committed_repo! do
    :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> :ok = Sandbox.mode(Repo, :manual) end)
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  defp put_owner_call_timeout!(timeout_ms) do
    config = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(config, owner_call_timeout_ms: timeout_ms))
  end
end
