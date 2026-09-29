defmodule CodexPooler.Repo.Migrations.CreateForwardedGenerationEnds do
  use Ecto.Migration

  # What the owner that served a forwarded attempt knows about the generation
  # it relayed: that it ended in a way the attempt's executor, on another node,
  # can no longer settle as a success (findings#290). Absent-instance recovery
  # takes a row as the exact death evidence it otherwise gets only from a
  # successor incarnation. Ids and times only. A new table, and no foreign key,
  # so no lock is taken on `attempts`; rows are pruned with the execution
  # terminal proofs' retention.
  def change do
    create table(:forwarded_generation_ends, primary_key: false) do
      add :attempt_id, :uuid, primary_key: true
      add :owner_instance_id, :string, size: 255, null: false
      add :owner_instance_boot_id, :string, size: 64, null: false
      add :reason, :text, null: false
      add :ended_at, :utc_datetime_usec, null: false, default: fragment("(clock_timestamp() AT TIME ZONE 'UTC')")
    end

    create constraint(:forwarded_generation_ends, :forwarded_generation_ends_owner_check, check: "owner_instance_id <> '' AND owner_instance_boot_id <> ''")

    create constraint(:forwarded_generation_ends, :forwarded_generation_ends_reason_check, check: "reason IN ('unreachable_downstream_cancelled', 'lost_turn_cancelled_at_output', 'terminal_delivered_to_reattached')")

    create index(:forwarded_generation_ends, [:ended_at, :attempt_id])
  end
end
