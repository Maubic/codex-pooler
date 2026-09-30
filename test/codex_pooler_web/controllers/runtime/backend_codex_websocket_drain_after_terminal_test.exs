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
  # forwarding off. The arms with the session's owner on a second VM, drained
  # there or with the socket node's drain, are in
  # `backend_codex_websocket_drain_after_terminal_peer_test.exs`, which boots
  # that VM once for the module; the turn and the drain's cut are shared
  # through `DrainAfterTerminalScenario`.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport
  import CodexPoolerWeb.Runtime.DrainAfterTerminalScenario

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry, OwnerDefaults, RolloutDrain, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.CleanupProofRace
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true

  @detection_timeout_ms 15_000
  @drained_close {:close, 1001, "websocket owner is draining"}

  test "without owner forwarding: the socket node's drain leaves a task whose terminal went out to settle as answered" do
    put_owner_forwarding!(false)
    turn = relayed_turn_held!(start_turn!())

    assert_task_drain_waits_for_settlement!(turn, :direct)
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

    :ok = await_drain_holding!(drain, fn -> owner_waits_for_settlement?(turn.owner) end)
    assert [%Request{status: "in_progress"}] = pool_requests(turn.setup)

    true = :erlang.resume_process(turn.owner_task)
    assert Task.await(drain, @detection_timeout_ms) == {:ok, :settled}
    assert_receive {:DOWN, ^monitor, :process, _owner, :normal}, @detection_timeout_ms
    assert receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2) == [@drained_close]
    assert_settled_as_answered!(turn.setup)
  end

  # The rollout drain's deadline finds the turn active and cuts the owner; the
  # owner lets the turn settle during its wait, and the drain's summary counts
  # a completed turn, not an aborted one.
  test "owner on this node: the rollout drain counts a turn that settles during the owner's wait as completed" do
    put_owner_forwarding!(true)
    put_owner_call_timeout!(1_500)
    turn = owner_result_held!(start_turn!())
    harness = WebsocketRolloutDrainSupport.start_rollout_drain_harness(self())
    deadline = harness.deadline
    drain = Task.async(fn -> RolloutDrain.start_drain([name: harness.name, timeout_ms: 500] ++ WebsocketRolloutDrainSupport.deadline_options(deadline)) end)

    assert_receive {:rollout_drain_deadline_wait, ^deadline, _wait_ms}, @detection_timeout_ms
    :ok = WebsocketRolloutDrainSupport.VirtualDeadline.advance(deadline, 10_000)
    :ok = await_owner_drain_waiting!(turn.owner)

    true = :erlang.resume_process(turn.owner_task)
    assert %{owners_drained: 1, turns_completed: 1, turns_aborted: 0} = Task.await(drain, @detection_timeout_ms)
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

  # The drain's cut makes the socket stop its response task, the turn's
  # executor, and the owner then interrupts its own active turn. When the
  # stopped task's terminal proof lands between the two (the production
  # publisher; `CleanupProofRace.hold_owner_interrupt!/2` makes the owner's
  # interruption wait for it), the owner's interrupt used to take the end for
  # a lost executor and record `dead_execution_recovered`. A drain cut settles
  # its own turn for its own reason, so the request keeps `owner_drained`
  # (findings#270 row 270-362). What the client receives does not depend on
  # the record: after the terminal it already had, the drain's error frame and
  # the drained close, in both (the frames are checked before the row).
  # An owner crash keeps the recovery for a lost executor; its arm is in
  # `dead_execution_resend_recovery_test.exs`.
  test "owner on this node: a drain cut keeps owner_drained when the cut task's end is proven before the owner interrupts its turn" do
    start_supervised!({CodexPooler.Accounting.ExecutionRecovery, enabled: true})
    put_owner_forwarding!(true)
    put_owner_call_timeout!(1_500)
    turn = relayed_turn_held!(start_turn!())
    owner = turn.state.websocket_owner_pid
    :ok = await_owner_settling!(owner)
    assert [%Request{id: request_id}] = pool_requests(turn.setup)
    monitor = Process.monitor(owner)

    hold = CleanupProofRace.hold_owner_interrupt!(owner, request_id)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :normal}, @detection_timeout_ms
    attempt_id = CleanupProofRace.assert_owner_interrupt_proven!(hold)

    assert ExecutionTerminalProofs.terminal?(Repo.get!(CodexPooler.Accounting.Attempt, attempt_id))
    assert [error, @drained_close] = receive_frames_until_close!(turn.client.conn, turn.client.websocket, turn.client.ref) |> elem(2)
    assert_drained_error!(error)
    assert [%Request{status: "failed", response_status_code: 499, last_error_code: "owner_drained"}] = pool_requests(turn.setup)
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

  # The drain's cut reached the owner, which now waits for the turn.
  defp await_owner_drain_waiting!(owner) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    %{drain_settlement: %{}} =
      Stream.repeatedly(fn -> :sys.get_state(owner) end)
      |> Enum.find(fn state ->
        cond do
          is_map(state.drain_settlement) -> true
          System.monotonic_time(:millisecond) > deadline -> flunk("the drain never reached the owner")
          true -> Process.sleep(5) && false
        end
      end)

    :ok
  end

  # With a 1.5 s owner call budget the owner waits 750 ms, and answers the
  # drain before the call's own timeout.
  defp assert_waits_out_the_budget!(owner) do
    started_at = System.monotonic_time(:millisecond)
    assert WebsocketOwnerSession.drain_owner(owner) == :ok
    assert (System.monotonic_time(:millisecond) - started_at) in 700..1_450
  end

  # The drain's cut as it always was, after the terminal the client holds.
  defp assert_drained_error!(frame) do
    assert {:text, text} = frame
    assert %{"type" => "error", "status" => 503, "error" => %{"code" => "owner_drained"}} = CodexPooler.JSON.decode!(text)
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

  # The cut stops a task that can be inside a query, as it always did; in the
  # sandbox that breaks the one connection every process of the test shares,
  # so those tests commit their rows, as the peer arms do.
  defp use_committed_repo! do
    :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> :ok = Sandbox.mode(Repo, :manual) end)
  end

  defp put_owner_call_timeout!(timeout_ms) do
    config = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(config, owner_call_timeout_ms: timeout_ms))
  end
end
