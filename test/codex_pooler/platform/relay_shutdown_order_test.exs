defmodule CodexPooler.Platform.RelayShutdownOrderTest do
  use CodexPooler.DataCase, async: false
  alias CodexPooler.Gateway.OperationalStatus
  alias CodexPooler.Gateway.Transports.Streaming.DeferredStreamRegistry
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, RolloutDrain}
  alias CodexPooler.Release
  alias CodexPooler.Telemetry.{Relay, RelayEvent}
  alias CodexPooler.Telemetry.RelayRuntime
  alias CodexPooler.TestAppEnv
  alias Ecto.Adapters.SQL.Sandbox

  for invalid <- [:missing_parent, :parent_is_file] do
    test "#{invalid} marker preflight leaves the real consumer able to claim", context do
      assert_invalid_marker_can_still_claim(context, unquote(invalid))
    end
  end

  test "an owned read-only marker keeps its inode, ownership and content across repeated shutdown", context do
    root = marker_directory!()
    marker = Path.join(root, "draining")
    File.write!(marker, "existing marker")
    File.chmod!(marker, 0o444)
    original = File.stat!(marker)
    File.chmod!(root, 0o500)
    runtime = start_paused_runtime(context)

    for _ <- 1..2 do
      assert %{remaining: remaining} =
               Release.prepare_shutdown(
                 relay: runtime,
                 marker: marker,
                 budget_ms: 1000,
                 drain: fn remaining ->
                   assert :sys.get_state(runtime).quiesced?
                   assert File.read!(marker) == "existing marker"
                   assert Map.take(File.stat!(marker), [:inode, :uid, :gid, :mode]) == Map.take(original, [:inode, :uid, :gid, :mode])
                   %{remaining: remaining}
                 end
               )

      assert remaining in 1..1000
      assert File.ls!(root) == ["draining"]
    end
  end

  test "filesystem changes after preflight remain a visible post-quiesce failure", context do
    root = marker_directory!()
    directory = Path.join(root, "parent")
    File.mkdir!(directory)
    marker = Path.join(directory, "draining")
    parent = self()

    runtime =
      start_supervised!(
        {RelayRuntime,
         enabled: true,
         role: "web",
         name: nil,
         start_paused: true,
         claim_fun: fn limit, owner ->
           send(parent, {:real_claim_held, self()})

           receive do
             :release_claim -> Relay.claim(limit, owner)
           after
             15_000 -> raise "relay claim not released"
           end
         end}
      )

    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    callbacks = :sys.get_state(runtime).callbacks
    :ok = Relay.refresh_heartbeat("relay-runtime")
    assert {:ok, row} = Relay.insert("pre_attempt_release", %{}, 1)
    send(runtime, :drain)
    assert_receive {:real_claim_held, ^runtime}

    task =
      Task.async(fn ->
        try do
          Release.prepare_shutdown(relay: runtime, marker: marker, budget_ms: 1000, drain: fn _ -> flunk("changed filesystem must not drain") end)
        rescue
          error -> {:failed, error.__struct__}
        end
      end)

    monitor = Process.monitor(task.pid)
    await_gate_closed(callbacks, System.monotonic_time(:millisecond) + 15_000)
    refute File.exists?(marker)
    assert File.ls!(directory) == []
    File.rmdir!(directory)
    File.write!(directory, "replaced after preflight")
    send(runtime, :release_claim)
    assert {:failed, MatchError} = Task.await(task, 15_000)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}
    state = :sys.get_state(runtime)
    assert state.quiesced?
    assert Repo.get!(RelayEvent, row.id).claimed_by == state.owner
    refute File.exists?(marker)
    CodexPooler.TestDiagnostics.puts("marker-preflight postcheck_filesystem_change=true quiesced=true marker=false failure_visible=true")
  end

  test "existing marker preflight preserves its metadata before quiesce completes", context do
    root = marker_directory!()
    marker = Path.join(root, "draining")
    File.write!(marker, "existing marker")
    File.chmod!(marker, 0o444)
    File.chmod!(root, 0o500)
    File.touch!(marker, 1_600_000_000)
    original = File.stat!(marker, time: :posix)
    runtime = start_paused_runtime(context)
    callbacks = :sys.get_state(runtime).callbacks
    :ok = :sys.suspend(runtime)

    task = Task.async(fn -> Release.prepare_shutdown(relay: runtime, marker: marker, budget_ms: 1000, drain: fn remaining -> %{remaining: remaining} end) end)
    monitor = Process.monitor(task.pid)

    try do
      await_gate_closed(callbacks, System.monotonic_time(:millisecond) + 15_000)
      assert Map.take(File.stat!(marker, time: :posix), [:inode, :uid, :gid, :mode, :mtime]) == Map.take(original, [:inode, :uid, :gid, :mode, :mtime])
      assert File.read!(marker) == "existing marker"
      assert File.ls!(root) == ["draining"]
    after
      :sys.resume(runtime)
    end

    assert %{remaining: remaining} = Task.await(task)
    assert remaining in 1..1000
    assert_receive {:DOWN, ^monitor, :process, _, :normal}
    assert File.stat!(marker, time: :posix).mtime > original.mtime
  end

  defp assert_invalid_marker_can_still_claim(context, invalid) do
    root = marker_directory!()
    parent = Path.join(root, "parent")
    if invalid == :parent_is_file, do: File.write!(parent, "unrelated")
    marker = Path.join(parent, "draining")
    previous = TestAppEnv.restore_on_exit(OperationalStatus, [])
    Application.put_env(:codex_pooler, OperationalStatus, Keyword.put(previous, :drain_marker_path, marker))
    runtime = start_real_consumer(context)
    before = :sys.get_state(runtime)
    :ok = Relay.refresh_heartbeat("relay-runtime")
    assert {:ok, row} = Relay.insert("pre_attempt_release", %{}, 1)

    failure =
      try do
        Release.prepare_shutdown(relay: runtime, marker: marker, budget_ms: 1000, drain: fn _ -> flunk("invalid marker must not drain") end)
        :no_error
      rescue
        error -> {:error, error.__struct__}
      end

    assert match?({:error, _}, failure)
    refute :sys.get_state(runtime).quiesced?
    refute :ets.lookup(before.callbacks, :quiesced) == [{:quiesced, true}]
    refute OperationalStatus.marker_draining?()
    send(runtime, :drain)
    state = :sys.get_state(runtime)
    assert Repo.get!(RelayEvent, row.id).claimed_by == state.owner
    assert File.ls!(root) == if(invalid == :parent_is_file, do: ["parent"], else: [])
    CodexPooler.TestDiagnostics.puts("marker-preflight failure=#{invalid} quiesced=false marker=false actual_relay_claim=true")
  end

  defp marker_directory! do
    root = Path.join(System.tmp_dir!(), "marker-preflight-#{Ecto.UUID.generate()}")

    on_exit(fn ->
      File.chmod(root, 0o700)
      File.rm_rf!(root)
      refute File.exists?(root)
    end)

    File.mkdir!(root)
    root
  end

  defp start_real_consumer(context) do
    runtime = start_supervised!({RelayRuntime, enabled: true, role: "web", name: nil, start_paused: true})
    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    runtime
  end

  test "supervised consumer restart under readiness marker cannot claim", context do
    marker = Path.join(System.tmp_dir!(), "relay-restart-#{Ecto.UUID.generate()}")
    on_exit(fn -> File.rm(marker) end)
    prior = TestAppEnv.restore_on_exit(OperationalStatus, [])

    Application.put_env(
      :codex_pooler,
      OperationalStatus,
      Keyword.put(prior, :drain_marker_path, marker)
    )

    parent = self()

    spec =
      {RelayRuntime,
       enabled: true,
       role: "web",
       name: nil,
       start_paused: true,
       claim_fun: fn _, _ ->
         send(parent, :claimed)
         {:ok, []}
       end}

    first = start_supervised!(spec)
    Sandbox.allow(Repo, context.sandbox_owner, first)
    send(first, :drain)
    :sys.get_state(first)
    assert_receive :claimed
    File.touch!(marker)
    stop_supervised!(RelayRuntime)
    replacement = start_supervised!(spec)
    Sandbox.allow(Repo, context.sandbox_owner, replacement)
    assert :sys.get_state(replacement).quiesced?
    send(replacement, :drain)
    :sys.get_state(replacement)
    refute_received :claimed
    stop_supervised!(RelayRuntime)
    File.rm!(marker)
    fresh = start_supervised!(spec)
    Sandbox.allow(Repo, context.sandbox_owner, fresh)
    send(fresh, :drain)
    :sys.get_state(fresh)
    assert_receive :claimed
  end

  test "only a shutdown drain quiesces the relay consumer", context do
    parent = self()

    runtime =
      start_supervised!(
        {RelayRuntime,
         enabled: true,
         role: "web",
         name: nil,
         start_paused: true,
         claim_fun: fn _, _ ->
           send(parent, :claimed)
           {:ok, []}
         end}
      )

    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    activity_registry = :"relay-drain-activity-#{System.unique_integer([:positive])}"
    stream_registry = :"relay-drain-streams-#{System.unique_integer([:positive])}"
    drain_name = :"relay-drain-#{System.unique_integer([:positive])}"
    start_supervised!({ActivityRegistry, name: activity_registry})
    start_supervised!({DeferredStreamRegistry, name: stream_registry})

    start_supervised!({RolloutDrain, name: drain_name, activity_registry: activity_registry, stream_registry: stream_registry, relay: runtime})

    %{result: :ok} = RolloutDrain.start_drain(name: drain_name, timeout_ms: 1_000)
    refute :sys.get_state(runtime).quiesced?
    send(runtime, :drain)
    :sys.get_state(runtime)
    assert_receive :claimed

    %{result: :ok} = RolloutDrain.drain_for_shutdown(1_000, name: drain_name)
    assert :sys.get_state(runtime).quiesced?
    send(runtime, :drain)
    :sys.get_state(runtime)
    refute_received :claimed
  end

  test "release helper quiesces before readiness marker and charges the same budget", context do
    marker = Path.join(System.tmp_dir!(), "relay-shutdown-#{Ecto.UUID.generate()}")
    on_exit(fn -> File.rm(marker) end)
    parent = self()

    runtime =
      start_supervised!(
        {RelayRuntime,
         enabled: true,
         role: "web",
         name: nil,
         start_paused: true,
         claim_fun: fn _, _ ->
           send(parent, {:claim, self()})

           receive do
             :release -> {:ok, []}
           end
         end}
      )

    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    callbacks = :sys.get_state(runtime).callbacks
    send(runtime, :drain)
    assert_receive {:claim, ^runtime}

    task =
      Task.async(fn ->
        Release.prepare_shutdown(
          relay: runtime,
          marker: marker,
          budget_ms: 1000,
          drain: fn remaining ->
            assert File.exists?(marker)
            assert :sys.get_state(runtime).quiesced?
            assert remaining in 1..1000
            %{remaining: remaining}
          end
        )
      end)

    await_gate_closed(callbacks, System.monotonic_time(:millisecond) + 15_000)
    refute File.exists?(marker)
    send(runtime, :release)
    assert %{remaining: _} = Task.await(task)
    send(runtime, :drain)
    :sys.get_state(runtime)
    refute_received {:claim, _}
  end

  test "release helper resolves the marker from CODEX_POOLER_DRAIN_MARKER_PATH before quiescing",
       context do
    marker = Path.join(System.tmp_dir!(), "relay-shutdown-env-#{Ecto.UUID.generate()}")
    previous = System.get_env("CODEX_POOLER_DRAIN_MARKER_PATH")

    on_exit(fn ->
      File.rm(marker)

      if previous,
        do: System.put_env("CODEX_POOLER_DRAIN_MARKER_PATH", previous),
        else: System.delete_env("CODEX_POOLER_DRAIN_MARKER_PATH")
    end)

    runtime = start_paused_runtime(context)
    callbacks = :sys.get_state(runtime).callbacks

    # The production default is the release environment variable; a missing or
    # blank variable must fail before the consumer is quiesced, because quiesce
    # is permanent for that process (findings#216). The ETS claim gate closes
    # before the state flips, so both are checked.
    System.delete_env("CODEX_POOLER_DRAIN_MARKER_PATH")

    assert_raise System.EnvError, fn ->
      Release.prepare_shutdown(
        relay: runtime,
        budget_ms: 1000,
        drain: fn _ -> flunk("drained") end
      )
    end

    System.put_env("CODEX_POOLER_DRAIN_MARKER_PATH", "")

    assert_raise ArgumentError, ~r/drain marker/, fn ->
      Release.prepare_shutdown(
        relay: runtime,
        budget_ms: 1000,
        drain: fn _ -> flunk("drained") end
      )
    end

    refute :sys.get_state(runtime).quiesced?
    assert :ets.lookup(callbacks, :quiesced) != [{:quiesced, true}]
    refute File.exists?(marker)

    System.put_env("CODEX_POOLER_DRAIN_MARKER_PATH", marker)

    assert %{remaining: remaining} =
             Release.prepare_shutdown(
               relay: runtime,
               budget_ms: 1000,
               drain: fn remaining ->
                 assert File.exists?(marker)
                 assert :sys.get_state(runtime).quiesced?
                 %{remaining: remaining}
               end
             )

    assert remaining in 1..1000
  end

  defp start_paused_runtime(context) do
    runtime =
      start_supervised!({RelayRuntime, enabled: true, role: "web", name: nil, start_paused: true, claim_fun: fn _, _ -> {:ok, []} end})

    Sandbox.allow(Repo, context.sandbox_owner, runtime)
    runtime
  end

  defp await_gate_closed(table, deadline) do
    if :ets.lookup(table, :quiesced) != [{:quiesced, true}] do
      assert System.monotonic_time(:millisecond) < deadline

      receive do
      after
        1 -> :ok
      end

      await_gate_closed(table, deadline)
    end
  end
end
