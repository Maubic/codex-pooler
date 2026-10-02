defmodule CodexPoolerWeb.Admin.ProviderCreditsPolicyForm do
  @moduledoc false

  import Phoenix.Component, only: [to_form: 2]

  alias Ecto.Changeset

  @types %{allow_provider_credits: :boolean}
  @param_keys ["allow_provider_credits", "_unused_allow_provider_credits"]

  @type attrs :: %{required(:allow_provider_credits) => boolean()}
  @type policy :: %{required(:allow_provider_credits) => boolean()}
  @type params :: %{optional(String.t() | atom()) => term()}
  @type action :: :validate | :submit | nil

  @spec form(policy(), params(), action()) :: Phoenix.HTML.Form.t()
  def form(policy, params \\ %{}, action \\ nil)

  def form(%{allow_provider_credits: enabled}, params, action) when is_boolean(enabled) do
    changeset =
      if params == %{} and is_nil(action) do
        Changeset.change({%{allow_provider_credits: enabled}, @types})
      else
        changeset(params)
      end

    changeset
    |> Map.put(:action, action)
    |> form_for_changeset()
  end

  @spec form_for_changeset(Changeset.t()) :: Phoenix.HTML.Form.t()
  def form_for_changeset(%Changeset{} = changeset) do
    to_form(changeset, as: :provider_credits_policy)
  end

  @spec changeset(term()) :: Changeset.t()
  def changeset(params) when is_map(params) and not is_struct(params) do
    {%{allow_provider_credits: nil}, @types}
    |> Changeset.cast(Map.take(params, @param_keys), [:allow_provider_credits])
    |> Changeset.validate_required([:allow_provider_credits])
    |> validate_param_keys(params)
  end

  def changeset(_params) do
    {%{allow_provider_credits: nil}, @types}
    |> Changeset.change()
    |> Changeset.add_error(:allow_provider_credits, "must be a policy form")
  end

  @spec parse(term()) :: {:ok, attrs()} | {:error, Changeset.t()}
  def parse(params) do
    params
    |> changeset()
    |> Changeset.apply_action(:submit)
  end

  @spec validate_param_keys(Changeset.t(), params()) :: Changeset.t()
  defp validate_param_keys(changeset, params) do
    if Enum.all?(Map.keys(params), &(&1 in @param_keys)) do
      changeset
    else
      Changeset.add_error(changeset, :allow_provider_credits, "contains unexpected fields")
    end
  end
end
