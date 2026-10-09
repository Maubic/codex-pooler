defmodule CodexPooler.Repo.Migrations.AddTotpLastUsedStep do
  use Ecto.Migration

  def change do
    alter table(:totp_settings) do
      add :last_used_step, :bigint
    end
  end
end
