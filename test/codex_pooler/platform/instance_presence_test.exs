defmodule CodexPooler.Platform.InstancePresenceTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Platform.InstanceHeartbeat
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.{Identity, Instance}
  alias CodexPooler.Repo

  setup do
    boot_id = Identity.boot_id()
    on_exit(fn -> :persistent_term.put({Identity, :boot_id}, boot_id) end)
    :ok
  end

  test "slot proof requires equal explicit slots, a different boot and a later publication" do
    now = InstancePresence.database_now()
    first = Identity.new("slot-first@example.invalid", unique_boot_id())
    second = Identity.new("slot-second@example.invalid", unique_boot_id())
    {:ok, _} = InstancePresence.record_heartbeat(first, DateTime.add(now, -600, :second))
    {:ok, _} = InstancePresence.record_heartbeat(second, now)
    refute InstancePresence.superseded?(first)
    slot = Ecto.UUID.generate() <> "/app"
    Repo.update_all(from(i in Instance, where: i.instance_id == ^first.instance_id), set: [slot_id: slot])
    refute InstancePresence.superseded?(first)
    Repo.update_all(from(i in Instance, where: i.instance_id == ^second.instance_id), set: [slot_id: slot <> "-other"])
    refute InstancePresence.superseded?(first)
    Repo.update_all(from(i in Instance, where: i.instance_id == ^second.instance_id), set: [slot_id: slot])
    assert InstancePresence.superseded?(first)
    refute InstancePresence.superseded?(second)
    Repo.update_all(from(i in Instance, where: i.instance_id == ^second.instance_id), set: [boot_id: first.boot_id])
    refute InstancePresence.superseded?(first)
    Repo.update_all(from(i in Instance, where: i.instance_id == ^second.instance_id), set: [boot_id: second.boot_id])
    Repo.update_all(from(i in Instance, where: i.instance_id == ^second.instance_id), set: [started_at: DateTime.add(now, -600, :second)])
    refute InstancePresence.superseded?(first)
  end

  test "anonymous node names require an exclusive slot before they prove supersession" do
    now = InstancePresence.database_now()
    first = Identity.new("nonode@nohost", unique_boot_id())
    second = Identity.new("nonode@nohost", unique_boot_id())
    {:ok, _} = InstancePresence.record_heartbeat(first, DateTime.add(now, -600, :second))
    {:ok, _} = InstancePresence.record_heartbeat(second, now)
    refute InstancePresence.superseded?(first)
    ids = [first.instance_id, second.instance_id]
    Repo.update_all(from(i in Instance, where: i.instance_id in ^ids), set: [slot_id: Ecto.UUID.generate() <> "/worker"])
    assert InstancePresence.superseded?(first)
    refute InstancePresence.superseded?(second)
  end

  test "local heartbeat stores the configured slot once without changing owner equality" do
    CodexPooler.TestAppEnv.restore_on_exit(:instance_slot_id)
    slot = Ecto.UUID.generate() <> "/app"
    Application.put_env(:codex_pooler, :instance_slot_id, slot)
    identity = Identity.local()
    Repo.delete_all(from(i in Instance, where: i.instance_id == ^identity.instance_id))
    {:ok, _} = InstancePresence.record_heartbeat()
    assert Repo.get!(Instance, identity.instance_id).slot_id == slot
    assert Identity.owner(identity.node_name, identity.boot_id) == identity
    Application.put_env(:codex_pooler, :instance_slot_id, slot <> "-other")
    {:ok, _} = InstancePresence.record_heartbeat()
    assert Repo.get!(Instance, identity.instance_id).slot_id == slot
  end

  test "the local live incarnation remains present when its published heartbeat is stale" do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    local = Identity.local()
    {:ok, _} = InstancePresence.record_heartbeat(local, DateTime.add(now, -180, :second))

    refute InstancePresence.absent?(local, now)
  end

  test "observer freshness requires its own successful heartbeat" do
    now = InstancePresence.database_now()
    local = Identity.local()
    refute InstancePresence.observer_fresh?(now)
    {:ok, _} = InstancePresence.record_heartbeat(local, DateTime.add(now, -180, :second))
    refute InstancePresence.observer_fresh?(now)
    {:ok, _} = InstancePresence.record_heartbeat(local)
    assert InstancePresence.observer_fresh?(InstancePresence.database_now())
  end

  test "non-distributed VM name collision stays unknown" do
    identity = Identity.new("nonode@nohost", Ecto.UUID.generate())
    assert InstancePresence.status(identity) == :unknown
    assert InstancePresence.status(Identity.local()) == :alive
  end

  test "a real PostgreSQL heartbeat write failure warns and emits a failure count" do
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)

    :telemetry.attach(
      handler,
      [:codex_pooler, :instance_presence, :heartbeat],
      &__MODULE__.capture_failure/4,
      self()
    )

    logs =
      ExUnit.CaptureLog.capture_log(fn ->
        pid =
          start_supervised!({InstanceHeartbeat, enabled: true, interval_ms: :timer.minutes(5), name: :heartbeat_invalid_row, identity: %Identity{instance_id: nil, node_name: "sample", boot_id: "sample"}})

        :sys.get_state(pid)
        stop_supervised!(InstanceHeartbeat)
      end)

    assert logs =~ "instance presence heartbeat write failed"
    assert_receive {:heartbeat_failure, %{failures: 1}, %{}}
  end

  @doc false
  def capture_failure(_event, measurements, metadata, parent),
    do: send(parent, {:heartbeat_failure, measurements, metadata})

  defp identity, do: Identity.new("codex_pooler@10.0.0.#{unique()}", "boot-#{unique()}")

  defp unique, do: System.unique_integer([:positive])

  # What a VM start does: mint one incarnation, then let the heartbeat publish
  # it. `CodexPooler.Application.start/2` makes the same call before any child
  # runs, and nothing restores the previous value because a minted incarnation
  # is exactly what a restart leaves behind.
  defp start_instance!(name) do
    _boot_id = Identity.mint_boot_id!()
    local = InstancePresence.local_identity()

    pid =
      start_supervised!(
        {InstanceHeartbeat, enabled: true, interval_ms: :timer.minutes(5), name: name},
        id: name
      )

    # The publish runs in the process's own continue, so one synchronous state
    # read is enough to know it has happened; no timer is waited out.
    _state = :sys.get_state(pid)

    local
  end

  test "record_heartbeat upserts one row and advances only the reporting timestamps" do
    instance = identity()

    started_at =
      DateTime.utc_now() |> DateTime.add(-90, :second) |> DateTime.truncate(:microsecond)

    later = DateTime.add(started_at, 45, :second)

    assert {:ok, first} = InstancePresence.record_heartbeat(instance, started_at)
    assert {:ok, second} = InstancePresence.record_heartbeat(instance, later)

    assert first.started_at == started_at
    assert second.last_seen_at == later

    row = Repo.get!(Instance, instance.instance_id)
    assert row.started_at == started_at
    assert row.last_seen_at == later
    assert row.node_name == instance.node_name
    assert row.boot_id == instance.boot_id
  end

  test "a second incarnation of one node name gets its own row and leaves the first ageing" do
    node_name = "codex_pooler@10.0.0.#{unique()}"
    first = Identity.new(node_name, "boot-#{unique()}")
    second = Identity.new(node_name, "boot-#{unique()}")
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    first_reported_at = DateTime.add(now, -10, :minute)

    {:ok, _first_row} = InstancePresence.record_heartbeat(first, first_reported_at)
    {:ok, _second_row} = InstancePresence.record_heartbeat(second, now)

    # The successor reuses the node name, as a container restarting in place
    # does. It must not refresh the row that proves its predecessor is gone.
    assert Repo.get!(Instance, first.instance_id).last_seen_at == first_reported_at
    assert Repo.get!(Instance, second.instance_id).last_seen_at == now
    assert InstancePresence.absent?(first, now)
    refute InstancePresence.absent?(second, now)
  end

  test "a later-started incarnation of the same node name supersedes an earlier one" do
    now = InstancePresence.database_now()
    node_name = "codex_pooler@10.42.#{System.unique_integer([:positive])}.9"
    first = Identity.new(node_name, unique_boot_id())
    second = Identity.new(node_name, unique_boot_id())
    anonymous = Identity.new("nonode@nohost", unique_boot_id())
    anonymous_successor = Identity.new("nonode@nohost", unique_boot_id())

    {:ok, _} = InstancePresence.record_heartbeat(first, DateTime.add(now, -600, :second))
    refute InstancePresence.superseded?(first)

    {:ok, _} = InstancePresence.record_heartbeat(second, now)
    assert InstancePresence.superseded?(first)
    # Only a later start counts, so the successor itself is never superseded by
    # the earlier row, and equal timestamps fail closed.
    refute InstancePresence.superseded?(second)
    {:ok, _} = InstancePresence.record_heartbeat(Identity.new(node_name, unique_boot_id()), now)
    refute InstancePresence.superseded?(second)

    {:ok, _} = InstancePresence.record_heartbeat(anonymous, DateTime.add(now, -600, :second))
    {:ok, _} = InstancePresence.record_heartbeat(anonymous_successor, now)
    refute InstancePresence.superseded?(anonymous)
    refute InstancePresence.superseded?(nil)
  end

  test "absence needs a stale row: unknown and recently reporting incarnations are present" do
    instance = identity()
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    refute InstancePresence.absent?(instance, now)
    refute InstancePresence.absent?(nil, now)

    {:ok, _inside_window} =
      InstancePresence.record_heartbeat(instance, DateTime.add(now, -30, :second))

    refute InstancePresence.absent?(instance, now)

    {:ok, _outside_window} =
      InstancePresence.record_heartbeat(instance, DateTime.add(now, -180, :second))

    assert InstancePresence.absent?(instance, now)

    # An owner recorded before incarnations existed names no incarnation at all,
    # so it can never be read as absent no matter how old its row is.
    assert Identity.owner(instance.node_name, nil) == nil
    refute InstancePresence.absent?(Identity.owner(instance.node_name, nil), now)
  end

  test "the liveness window outlasts the rollout drain budget" do
    # A pod that is still draining is still serving. The window has to clear the
    # largest configured drain budget, otherwise recovery could race a live drain.
    assert InstancePresence.liveness_window_seconds() >= 120
    assert InstancePresence.liveness_window_seconds() * 1000 > 85_000
  end

  test "prune removes long-gone rows and keeps rows recovery still reasons about" do
    gone = identity()
    recent = identity()
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    {:ok, _gone} = InstancePresence.record_heartbeat(gone, DateTime.add(now, -30, :day))
    {:ok, _recent} = InstancePresence.record_heartbeat(recent, DateTime.add(now, -10, :minute))

    assert {:ok, %{instance_presence_rows_pruned: pruned}} = InstancePresence.prune(now)
    assert pruned >= 1
    refute Repo.get(Instance, gone.instance_id)
    assert Repo.get(Instance, recent.instance_id)
  end

  test "the heartbeat process publishes this instance's own incarnation on start" do
    local = start_instance!(:instance_presence_first_test)

    row = Repo.get!(Instance, local.instance_id)
    assert row.node_name == local.node_name
    assert row.boot_id == local.boot_id
  end

  test "restarting the VM in place publishes a new row under the same node name" do
    first = start_instance!(:instance_presence_restart_first_test)
    :ok = stop_supervised!(:instance_presence_restart_first_test)
    second = start_instance!(:instance_presence_restart_second_test)

    # Same node name — the pod IP the name derives from is unchanged by an
    # in-place restart — but a different incarnation, and so a different row.
    assert second.node_name == first.node_name
    assert second.boot_id != first.boot_id
    assert second.instance_id != first.instance_id

    first_row = Repo.get!(Instance, first.instance_id)
    second_row = Repo.get!(Instance, second.instance_id)

    # The first row was published once and never refreshed again, so it is free
    # to age out of the liveness window while the successor reports normally.
    assert first_row.last_seen_at == first_row.started_at
    assert DateTime.compare(second_row.last_seen_at, first_row.last_seen_at) == :gt
  end

  test "the heartbeat process does not start when it is disabled" do
    assert :ignore =
             InstanceHeartbeat.start_link(enabled: false, name: :instance_presence_disabled_test)
  end

  defp unique_boot_id,
    do: Base.encode32(:crypto.strong_rand_bytes(10), case: :lower, padding: false)
end
