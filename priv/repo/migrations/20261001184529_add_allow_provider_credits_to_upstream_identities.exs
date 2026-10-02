defmodule CodexPooler.Repo.Migrations.AddAllowProviderCreditsToUpstreamIdentities do
  use Ecto.Migration

  def change do
    alter table(:upstream_identities) do
      add :allow_provider_credits, :boolean, default: true, null: false
    end
  end
end
