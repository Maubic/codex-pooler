defmodule CodexPooler.Gateway.Transports.Websocket.ActivityRegistryTest do
  use ExUnit.Case, async: false

  alias CodexPooler.Gateway.Transports.Websocket.{ActivityDrain, ActivityRegistry}

  setup do
    name = :"websocket-activity-registry-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: name})
    {:ok, registry: name}
  end

  test "register-before-gate activity is included when cutoff wins the admission race", %{
    registry: registry
  } do
    parent = self()

    task =
      Task.async(fn ->
        {:ok, token} = ActivityRegistry.register(:direct, self(), name: registry)
        send(parent, {:activity_registered, self(), token})

        receive do
          :gate -> :ok
        end

        result = ActivityRegistry.admit(token, name: registry)
        :ok = ActivityRegistry.unregister(token, :aborted, name: registry)
        result
      end)

    assert_receive {:activity_registered, task_pid, token}

    assert {epoch, [%{kind: :direct, pid: ^task_pid, token: ^token}]} =
             ActivityRegistry.begin_drain(name: registry)

    send(task.pid, :gate)
    assert Task.await(task) == {:error, :owner_drained}
    assert {:finished, :aborted} = ActivityRegistry.status(token, name: registry)
    assert :ok = ActivityRegistry.complete_drain(epoch, name: registry)
  end

  test "post-cutoff activity is rejected synchronously and never joins the drain snapshot", %{
    registry: registry
  } do
    assert {epoch, []} = ActivityRegistry.begin_drain(name: registry)
    assert {:ok, token} = ActivityRegistry.register(:proxy, self(), name: registry)
    assert {:error, :owner_drained} = ActivityRegistry.admit(token, name: registry)
    assert :ok = ActivityRegistry.unregister(token, :aborted, name: registry)
    assert :unknown = ActivityRegistry.status(token, name: registry)
    assert :ok = ActivityRegistry.complete_drain(epoch, name: registry)
  end

  test "active entries remain enumerable when the rollout coordinator restarts", %{
    registry: registry
  } do
    assert {:ok, token} = ActivityRegistry.register(:proxy, self(), name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)

    assert {epoch, [%{token: ^token, status: :active}]} =
             ActivityRegistry.begin_drain(name: registry)

    assert {^epoch, [%{token: ^token, status: :active}]} =
             ActivityRegistry.begin_drain(name: registry)

    assert :ok = ActivityRegistry.unregister(token, :completed, name: registry)

    assert {^epoch, [%{token: ^token, status: {:finished, :completed}}]} =
             ActivityRegistry.begin_drain(name: registry)
  end

  test "task crashes are cleaned up and retained as failed drain outcomes", %{registry: registry} do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, token} = ActivityRegistry.register(:direct, self(), name: registry)
        :ok = ActivityRegistry.admit(token, name: registry)
        send(parent, {:activity_ready, self(), token})

        receive do
          :crash -> exit(:synthetic_activity_crash)
        end
      end)

    assert_receive {:activity_ready, ^pid, token}
    assert {_epoch, [%{token: ^token}]} = ActivityRegistry.begin_drain(name: registry)
    monitor = Process.monitor(pid)
    send(pid, :crash)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :synthetic_activity_crash}
    assert {:finished, :failed} = ActivityRegistry.status(token, name: registry)
  end

  # findings#287: once the socket pushed the turn's terminal, a drain's
  # cancellation leaves the activity alone and says so; the drain then counts
  # the task's own finish.
  test "a drain does not cancel an activity whose terminal was delivered", %{registry: registry} do
    assert {:ok, token} = ActivityRegistry.register(:direct, self(), name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)
    assert :ok = ActivityRegistry.mark_terminal_delivered(self(), name: registry)
    assert :ok = ActivityRegistry.mark_terminal_delivered(spawn(fn -> :ok end), name: registry)
    assert {epoch, [%{token: ^token, status: :active}]} = ActivityRegistry.begin_drain(name: registry)

    assert ActivityRegistry.cancel(token, :owner_drained, name: registry) == :terminal_delivered
    refute_received {:websocket_activity_cancel, ^token, :owner_drained}
    assert {:active, :admitted} = ActivityRegistry.status(token, name: registry)

    assert :ok = ActivityRegistry.complete(token, :completed, name: registry)
    assert {:finished, :completed} = ActivityRegistry.status(token, name: registry)
    assert :ok = ActivityRegistry.complete_drain(epoch, name: registry)
  end

  test "a forced cancellation cuts an activity whose terminal was delivered", %{registry: registry} do
    assert {:ok, token} = ActivityRegistry.register(:direct, self(), name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)
    assert :ok = ActivityRegistry.mark_terminal_delivered(self(), name: registry)
    assert {_epoch, [%{token: ^token}]} = ActivityRegistry.begin_drain(name: registry)

    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry, force: true)
    assert_received {:websocket_activity_cancel, ^token, :owner_drained}
    assert {:active, :cancelling} = ActivityRegistry.status(token, name: registry)
  end

  describe "a drain past its deadline" do
    # The socket already pushed the turn's terminal (findings#287): the task
    # gets half the post-deadline budget to settle and counts as the turn it
    # settled.
    test "leaves an activity whose terminal was delivered to settle, and counts it completed", %{registry: registry} do
      activity = delivered_activity!(registry)
      drain = Task.async(fn -> ActivityDrain.drain(activity.entry, System.monotonic_time(:millisecond) - 1, policy(400), registry) end)

      refute_receive {:activity_saw, {:websocket_activity_cancel, _token, _reason}}, 100
      :ok = ActivityRegistry.unregister(activity.entry.token, :completed, name: registry)
      send(activity.pid, :finish)

      assert Task.await(drain) == :completed
    end

    # One still running once half the budget is spent is cut as the deadline
    # cuts a live turn; the other half is its time to stop before the kill.
    test "cuts an activity whose terminal was delivered once half its budget is spent", %{registry: registry} do
      activity = delivered_activity!(registry)
      monitor = Process.monitor(activity.pid)
      started_at = System.monotonic_time(:millisecond)
      drain = Task.async(fn -> ActivityDrain.drain(activity.entry, System.monotonic_time(:millisecond) - 1, policy(400), registry) end)

      token = activity.entry.token
      assert_receive {:activity_saw, {:websocket_activity_cancel, ^token, :owner_drained}}, 1_000
      assert (System.monotonic_time(:millisecond) - started_at) in 180..390

      assert Task.await(drain) == :aborted
      assert_receive {:DOWN, ^monitor, :process, _pid, :killed}
    end
  end

  test "deadline cancellation and unregister are idempotent", %{registry: registry} do
    assert {:ok, token} = ActivityRegistry.register(:direct, self(), name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)
    assert {_epoch, [%{token: ^token}]} = ActivityRegistry.begin_drain(name: registry)

    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:websocket_activity_cancel, ^token, :owner_drained}
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    refute_received {:websocket_activity_cancel, ^token, :owner_drained}

    assert :ok = ActivityRegistry.unregister(token, :completed, name: registry)
    assert :ok = ActivityRegistry.unregister(token, :completed, name: registry)
    assert {:finished, :aborted} = ActivityRegistry.status(token, name: registry)
  end

  test "delivery completion can win after cancellation is marked", %{registry: registry} do
    assert {:ok, token} = ActivityRegistry.register(:proxy, self(), name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)
    assert {_epoch, [%{token: ^token}]} = ActivityRegistry.begin_drain(name: registry)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert {:active, :cancelling} = ActivityRegistry.status(token, name: registry)

    assert :ok = ActivityRegistry.complete(token, :completed, name: registry)

    assert {:finished, :completed} = ActivityRegistry.status(token, name: registry)
  end

  test "cancellation can target a watcher without changing the registered activity pid", %{
    registry: registry
  } do
    watcher = self()
    activity_pid = spawn(fn -> receive do: (:stop -> :ok) end)
    assert {:ok, token} = ActivityRegistry.register(:proxy, activity_pid, name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)
    assert :ok = ActivityRegistry.set_cancel_recipient(token, watcher, name: registry)
    assert [%{pid: ^activity_pid}] = ActivityRegistry.activities(name: registry)
    assert :ok = ActivityRegistry.cancel(token, :owner_drained, name: registry)
    assert_receive {:websocket_activity_cancel, ^token, :owner_drained}
    assert Process.alive?(activity_pid)
    send(activity_pid, :stop)
  end

  defp delivered_activity!(registry) do
    parent = self()

    pid =
      spawn(fn ->
        receive do
          :finish -> :ok
        end
      end)

    forwarder = spawn(fn -> forward_to(parent) end)
    on_exit(fn -> Enum.each([pid, forwarder], &Process.exit(&1, :kill)) end)
    assert {:ok, token} = ActivityRegistry.register(:direct, pid, name: registry)
    assert :ok = ActivityRegistry.admit(token, name: registry)
    assert :ok = ActivityRegistry.set_cancel_recipient(token, forwarder, name: registry)
    assert :ok = ActivityRegistry.mark_terminal_delivered(pid, name: registry)
    assert {_epoch, [%{token: ^token} = entry]} = ActivityRegistry.begin_drain(name: registry)
    %{pid: pid, entry: entry}
  end

  defp forward_to(parent) do
    receive do
      message ->
        send(parent, {:activity_saw, message})
        forward_to(parent)
    end
  end

  defp policy(budget_ms) do
    %{
      now_ms: fn -> System.monotonic_time(:millisecond) end,
      schedule_wait: fn recipient, token, wait_ms -> Process.send_after(recipient, {:rollout_drain_wait_elapsed, token}, wait_ms) end,
      cancel_wait: fn timer, _token ->
        _remaining = Process.cancel_timer(timer)
        :ok
      end,
      owner_post_deadline_call_budget_ms: budget_ms
    }
  end
end
