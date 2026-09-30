defmodule CodexPooler.Platform.ExecutionTerminalProof do
  @moduledoc false
  use Ecto.Schema

  @primary_key {:execution_id, :binary_id, autogenerate: false}
  schema "execution_terminal_proofs" do
    field :owner_instance_id, :string
    field :owner_instance_boot_id, :string
    field :owner_process_id, :string
    field :end_kind, :string
    field :interruption_code, :string
    field :ended_at, :utc_datetime_usec
    field :published_at, :utc_datetime_usec
  end

  @type t :: %__MODULE__{
          execution_id: Ecto.UUID.t(),
          owner_instance_id: String.t(),
          owner_instance_boot_id: String.t(),
          owner_process_id: String.t(),
          end_kind: String.t(),
          interruption_code: String.t() | nil,
          ended_at: DateTime.t(),
          published_at: DateTime.t()
        }
end
