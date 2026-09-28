defmodule CodexPooler.Repo.Migrations.AddPoolAudioTranscriptionPermission do
  use Ecto.Migration

  def change do
    alter table(:pool_routing_settings) do
      add :allow_audio_transcription, :boolean, null: false, default: true
    end
  end
end
