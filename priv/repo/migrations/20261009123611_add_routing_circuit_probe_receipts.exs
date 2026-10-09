defmodule CodexPooler.Repo.Migrations.AddRoutingCircuitProbeReceipts do
  use Ecto.Migration

  def up do
    execute("LOCK TABLE public.routing_circuit_states IN ACCESS EXCLUSIVE MODE NOWAIT")

    alter table(:routing_circuit_states) do
      add :probe_generation, :uuid
      add :probe_admission_ids, {:array, :uuid}, null: false, default: []
    end
  end

  def down do
    execute("LOCK TABLE public.routing_circuit_states IN ACCESS EXCLUSIVE MODE NOWAIT")

    alter table(:routing_circuit_states) do
      remove :probe_admission_ids
      remove :probe_generation
    end
  end
end
