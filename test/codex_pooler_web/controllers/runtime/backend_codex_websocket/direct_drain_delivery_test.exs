defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.DirectDrainDeliveryTest do
  use CodexPoolerWeb.ConnCase, async: false

  alias CodexPoolerWeb.Runtime.PreTurnCompactionCutScenario, as: Scenario

  @moduletag capture_log: true
  @detection_timeout_ms 15_000

  test "direct drain delivers its interruption without waiting on the busy upstream session" do
    observer = self()
    tracer = spawn_link(fn -> collect_stops(observer, []) end)

    on_exit(fn ->
      :erlang.trace_pattern({GenServer, :stop, 3}, false, [:local])
      if Process.alive?(tracer), do: send(tracer, :stop)
    end)

    before_drain = fn state, entry ->
      upstream = state.upstream_websocket_session
      watcher = entry.cancel_pid
      send(observer, {:drain_watcher, watcher})
      task_monitor = Process.monitor(entry.pid)
      upstream_monitor = Process.monitor(upstream)
      send(observer, {:drain_resources, entry.pid, task_monitor, upstream, upstream_monitor})
      :erlang.trace_pattern({GenServer, :stop, 3}, [{[upstream, :_, :_], [], []}], [:local])
      :erlang.trace(watcher, true, [:call, {:tracer, tracer}])
    end

    after_drain = fn ->
      assert_receive {:drain_resources, task, task_monitor, upstream, upstream_monitor}, @detection_timeout_ms
      assert_receive {:DOWN, ^task_monitor, :process, ^task, _}, @detection_timeout_ms
      assert_receive {:DOWN, ^upstream_monitor, :process, ^upstream, _}, @detection_timeout_ms
      refute Process.alive?(task)
      refute Process.alive?(upstream)
    end

    assert Scenario.run_scenario("full", :pre_turn, :direct, :drain_before_output, :on_arrival, before_drain: before_drain, after_drain: after_drain) == Scenario.expected(:drain_before_output, :direct)
    assert_received {:drain_watcher, watcher}
    ref = :erlang.trace_delivered(watcher)
    assert_receive {:trace_delivered, ^watcher, ^ref}, @detection_timeout_ms
    send(tracer, :finish)
    assert_receive {:upstream_stops, calls}, @detection_timeout_ms
    assert calls == [], "the drain watcher synchronously stopped the busy upstream before delivering the interruption"
  end

  defp collect_stops(observer, calls) do
    receive do
      {:trace, _watcher, :call, {GenServer, :stop, [_upstream, reason, timeout]}} -> collect_stops(observer, [{reason, timeout} | calls])
      :finish -> send(observer, {:upstream_stops, Enum.reverse(calls)})
      :stop -> :ok
    end
  end
end
