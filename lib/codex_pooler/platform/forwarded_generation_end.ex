defmodule CodexPooler.Platform.ForwardedGenerationEnd do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:attempt_id, :binary_id, autogenerate: false}
  schema "forwarded_generation_ends" do
    field :owner_instance_id, :string
    field :owner_instance_boot_id, :string
    field :reason, :string
    field :ended_at, :utc_datetime_usec
  end

  @type t :: %__MODULE__{
          attempt_id: Ecto.UUID.t(),
          owner_instance_id: String.t(),
          owner_instance_boot_id: String.t(),
          reason: String.t(),
          ended_at: DateTime.t()
        }
end
