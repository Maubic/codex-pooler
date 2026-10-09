defmodule CodexPooler.Accounts.TOTPSetting do
  @moduledoc false
  use CodexPooler.Schema

  schema "totp_settings" do
    field :user_id, :binary_id
    # Never rendered by a struct or changeset inspect (findings#215, #221).
    field :secret_ciphertext, :binary, redact: true
    field :secret_key_version, :string
    field :recovery_generation, :integer
    field :status, :string
    field :enrolled_at, :utc_datetime_usec
    field :verified_at, :utc_datetime_usec
    field :last_used_step, :integer
    field :disabled_at, :utc_datetime_usec
    field :created_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
  end
end
