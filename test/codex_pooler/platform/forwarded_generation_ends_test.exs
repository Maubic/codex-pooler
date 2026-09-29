defmodule CodexPooler.Platform.ForwardedGenerationEndsTest do
  # The owner's record that a generation it served for an executor on another
  # node ended where that executor could no longer settle it as a success
  # (findings#290).
  use CodexPooler.DataCase, async: true

  alias CodexPooler.Jobs.RuntimeStateCleanup
  alias CodexPooler.Platform.{ForwardedGenerationEnd, ForwardedGenerationEnds, InstancePresence}

  test "an end is recorded once, as this instance, and the first reason stays" do
    attempt_id = Ecto.UUID.generate()
    refute ForwardedGenerationEnds.ended?(%{id: attempt_id})

    assert :ok = ForwardedGenerationEnds.record(attempt_id, "unreachable_downstream_cancelled")
    assert :ok = ForwardedGenerationEnds.record(attempt_id, "terminal_delivered_to_reattached")

    identity = InstancePresence.local_identity()
    assert %ForwardedGenerationEnd{reason: "unreachable_downstream_cancelled", ended_at: %DateTime{}} = ending = Repo.get!(ForwardedGenerationEnd, attempt_id)
    assert {ending.owner_instance_id, ending.owner_instance_boot_id} == {identity.node_name, identity.boot_id}
    assert ForwardedGenerationEnds.ended?(%{id: attempt_id})
    refute ForwardedGenerationEnds.ended?(%{id: Ecto.UUID.generate()})
  end

  test "only the fixed reasons, for an attempt id, are recorded" do
    assert {:error, :invalid_reason} = ForwardedGenerationEnds.record(Ecto.UUID.generate(), "terminal_delivered")
    assert {:error, :invalid_attempt_id} = ForwardedGenerationEnds.record("not-an-attempt", "unreachable_downstream_cancelled")
    assert Repo.aggregate(ForwardedGenerationEnd, :count) == 0

    assert_raise Postgrex.Error, ~r/forwarded_generation_ends_reason_check/, fn ->
      Repo.insert_all(ForwardedGenerationEnd, [%{attempt_id: Ecto.UUID.generate(), owner_instance_id: "owner@host", owner_instance_boot_id: "boot", reason: "delivered"}])
    end

    assert_raise Postgrex.Error, ~r/forwarded_generation_ends_owner_check/, fn ->
      Repo.insert_all(ForwardedGenerationEnd, [%{attempt_id: Ecto.UUID.generate(), owner_instance_id: "", owner_instance_boot_id: "boot", reason: "lost_turn_cancelled_at_output"}])
    end
  end

  test "rows past the retention window are pruned on the database clock" do
    expired = Ecto.UUID.generate()
    kept = Ecto.UUID.generate()
    :ok = ForwardedGenerationEnds.record(expired, "lost_turn_cancelled_at_output")
    :ok = ForwardedGenerationEnds.record(kept, "lost_turn_cancelled_at_output")
    age!(expired, ForwardedGenerationEnds.retention_seconds() + 1)
    age!(kept, ForwardedGenerationEnds.retention_seconds() - 60)

    # The caller's clock, skewed either way, moves nothing.
    assert {:ok, %{forwarded_generation_ends_pruned: 1}} = ForwardedGenerationEnds.prune(~U[2000-01-01 00:00:00Z])
    assert {:ok, %{forwarded_generation_ends_pruned: 0}} = ForwardedGenerationEnds.prune(~U[2200-01-01 00:00:00Z])
    refute ForwardedGenerationEnds.ended?(%{id: expired})
    assert ForwardedGenerationEnds.ended?(%{id: kept})
  end

  test "the retention window is the six-hour sweep's" do
    assert ForwardedGenerationEnds.retention_seconds() == 6 * 60 * 60
  end

  test "runtime state cleanup runs the prune and reports its count" do
    expired = Ecto.UUID.generate()
    :ok = ForwardedGenerationEnds.record(expired, "terminal_delivered_to_reattached")
    age!(expired, ForwardedGenerationEnds.retention_seconds() + 1)

    assert {:ok, summary} = RuntimeStateCleanup.run(InstancePresence.database_now())
    assert summary.forwarded_generation_ends_pruned == 1
    refute ForwardedGenerationEnds.ended?(%{id: expired})
  end

  defp age!(attempt_id, seconds) do
    Repo.query!("UPDATE forwarded_generation_ends SET ended_at = (clock_timestamp() AT TIME ZONE 'UTC') - ($2 * interval '1 second') WHERE attempt_id = $1", [Ecto.UUID.dump!(attempt_id), seconds])
  end
end
