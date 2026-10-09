defmodule CodexPooler.Upstreams.Quota.AccountQuotaWindow do
  @moduledoc false
  use CodexPooler.Schema

  import Ecto.Changeset

  alias CodexPooler.Quotas.Evidence

  @window_kinds ~w(primary secondary)
  @max_credit_balance 9_223_372_036_854_775_807
  @max_window_minutes 2_147_483_647
  @freshness_states ~w(fresh stale unknown)
  @source_precisions ~w(authoritative observed inferred unknown)
  @quota_scopes ~w(account model upstream_model feature)

  @type t :: %__MODULE__{}
  @type attrs :: map()

  schema "account_quota_windows" do
    field :upstream_identity_id, :binary_id
    field :quota_key, :string
    field :window_kind, :string
    field :window_minutes, :integer
    field :active_limit, :integer
    field :credits, :integer
    field :reset_at, :utc_datetime_usec
    field :used_percent, :decimal
    field :display_label, :string
    field :limit_name, :string
    field :metered_feature, :string
    field :source, :string
    field :source_precision, :string
    field :quota_scope, :string
    field :quota_family, :string
    field :model, :string
    field :upstream_model, :string
    field :raw_limit_id, :string
    field :raw_limit_name, :string
    field :raw_metered_feature, :string
    field :freshness_state, :string
    field :last_sync_at, :utc_datetime_usec
    field :observed_at, :utc_datetime_usec
    field :merge_precedence, :integer
    field :metadata, :map
    field :created_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
  end

  @spec changeset(t() | Ecto.Changeset.t(), attrs()) :: Ecto.Changeset.t()
  def changeset(window, attrs) do
    window
    |> cast(attrs, [
      :upstream_identity_id,
      :quota_key,
      :window_kind,
      :window_minutes,
      :active_limit,
      :credits,
      :reset_at,
      :used_percent,
      :display_label,
      :limit_name,
      :metered_feature,
      :source,
      :source_precision,
      :quota_scope,
      :quota_family,
      :model,
      :upstream_model,
      :raw_limit_id,
      :raw_limit_name,
      :raw_metered_feature,
      :freshness_state,
      :last_sync_at,
      :observed_at,
      :merge_precedence,
      :metadata,
      :created_at,
      :updated_at
    ])
    |> update_change(:quota_key, &trim_optional_string/1)
    |> update_change(:display_label, &trim_optional_string/1)
    |> update_change(:limit_name, &trim_optional_string/1)
    |> update_change(:metered_feature, &trim_optional_string/1)
    |> update_change(:source, &trim_optional_string/1)
    |> update_change(:source_precision, &trim_optional_string/1)
    |> update_change(:quota_scope, &trim_optional_string/1)
    |> update_change(:quota_family, &trim_optional_string/1)
    |> update_change(:model, &trim_optional_string/1)
    |> update_change(:upstream_model, &trim_optional_string/1)
    |> update_change(:raw_limit_id, &trim_optional_string/1)
    |> update_change(:raw_limit_name, &trim_optional_string/1)
    |> update_change(:raw_metered_feature, &trim_optional_string/1)
    |> default_evidence_fields()
    |> validate_required([
      :upstream_identity_id,
      :quota_key,
      :window_kind,
      :window_minutes,
      :source,
      :source_precision,
      :quota_scope,
      :quota_family,
      :freshness_state,
      :last_sync_at,
      :observed_at,
      :merge_precedence,
      :metadata,
      :created_at,
      :updated_at
    ])
    |> validate_length(:quota_key, min: 1)
    |> validate_inclusion(:window_kind, @window_kinds)
    |> validate_inclusion(:freshness_state, @freshness_states)
    |> validate_inclusion(:source_precision, @source_precisions)
    |> validate_inclusion(:quota_scope, @quota_scopes)
    |> validate_number(:active_limit, greater_than_or_equal_to: 0, less_than_or_equal_to: @max_credit_balance)
    |> validate_number(:credits, greater_than_or_equal_to: 0, less_than_or_equal_to: @max_credit_balance)
    |> validate_number(:used_percent, greater_than_or_equal_to: 0, less_than_or_equal_to: 100)
    |> validate_length(:source, min: 1)
    |> validate_length(:quota_family, min: 1)
    |> validate_number(:window_minutes, greater_than: 0, less_than_or_equal_to: @max_window_minutes)
    |> validate_number(:merge_precedence, greater_than_or_equal_to: 0)
    |> unique_constraint(:window_kind,
      name: :account_quota_windows_evidence_identity_uq
    )
  end

  defp default_evidence_fields(changeset) do
    changeset
    |> put_default_change(:source_precision, "observed")
    |> put_default_change(:quota_scope, inferred_quota_scope(changeset))
    |> put_default_change(:quota_family, get_field(changeset, :quota_key))
    |> put_default_change(:observed_at, get_field(changeset, :last_sync_at))
    |> put_default_change(
      :merge_precedence,
      Evidence.merge_precedence(%{
        source: get_field(changeset, :source),
        reset_at: get_field(changeset, :reset_at),
        source_precision: get_field(changeset, :source_precision)
      })
    )
  end

  defp put_default_change(changeset, field, default) do
    case get_field(changeset, field) do
      nil -> put_change(changeset, field, default)
      _value -> changeset
    end
  end

  defp trim_optional_string(value) when is_binary(value), do: String.trim(value)
  defp trim_optional_string(value), do: value

  defp inferred_quota_scope(changeset) do
    cond do
      present_string?(get_field(changeset, :model)) -> "model"
      present_string?(get_field(changeset, :upstream_model)) -> "upstream_model"
      get_field(changeset, :quota_key) == "account" -> "account"
      true -> "feature"
    end
  end

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false
end
