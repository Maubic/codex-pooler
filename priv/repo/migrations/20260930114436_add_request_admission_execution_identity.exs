defmodule CodexPooler.Repo.Migrations.AddRequestAdmissionExecutionIdentity do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:requests) do
      add :admission_instance_id, :string, size: 255
      add :admission_instance_boot_id, :string, size: 64
      add :admission_process_id, :string, size: 64
      add :admission_execution_id, :uuid
      add :admission_execution_checked_at, :timestamptz
    end
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:requests) do
      remove :admission_execution_checked_at
      remove :admission_execution_id
      remove :admission_process_id
      remove :admission_instance_boot_id
      remove :admission_instance_id
    end
  end
end
