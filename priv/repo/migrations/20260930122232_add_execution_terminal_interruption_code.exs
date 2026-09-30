defmodule CodexPooler.Repo.Migrations.AddExecutionTerminalInterruptionCode do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")

    alter table(:execution_terminal_proofs) do
      add :interruption_code, :text, default: "unobserved_exit"
    end

    execute("ALTER TABLE execution_terminal_proofs ADD CONSTRAINT execution_terminal_proofs_interruption_code_check CHECK (interruption_code IS NULL OR interruption_code IN ('client_disconnected', 'owner_drained', 'owner_task_exception', 'unobserved_exit')) NOT VALID")
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")
    drop constraint(:execution_terminal_proofs, :execution_terminal_proofs_interruption_code_check)

    alter table(:execution_terminal_proofs) do
      remove :interruption_code
    end
  end
end
