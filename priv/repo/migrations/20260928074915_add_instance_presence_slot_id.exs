defmodule CodexPooler.Repo.Migrations.AddInstancePresenceSlotId do
  use Ecto.Migration

  def change do
    alter table(:instance_presences) do
      add :slot_id, :string, size: 200
    end

    create index(:instance_presences, [:slot_id, :started_at], where: "slot_id IS NOT NULL")
  end
end
