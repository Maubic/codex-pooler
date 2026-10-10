defmodule CodexPooler.Access.APIKeys.PolicyPersistence do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Access.{APIKey, APIKeyPolicyBinding}
  alias CodexPooler.Access.APIKeys.RuntimeAuthorization
  alias CodexPooler.Repo

  @type create_result ::
          {:ok,
           %{
             api_key: APIKey.t(),
             raw_key: String.t(),
             policy_bindings: [APIKeyPolicyBinding.t()]
           }}
          | {:error, Ecto.Changeset.t()}
  @type update_policy_result ::
          {:ok, %{api_key: APIKey.t(), policy_bindings: [APIKeyPolicyBinding.t()]}}
          | {:error, Ecto.Changeset.t()}
  @type transaction_result(value) ::
          {:ok, value}
          | {:ok, %{result: value}}
          | {:error, term()}
          | {:error, term(), term(), term()}

  @edit_fields ~w(id pool_id display_name status dashboard_access max_active_requests expires_at allowed_model_identifiers enforced_model_identifier enforced_reasoning_effort maximum_reasoning_effort enforced_service_tier metadata)a
  @binding_edit_fields ~w(binding_scope model_identifier status max_requests_per_minute max_tokens_per_day max_tokens_per_week max_input_tokens_per_request max_output_tokens_per_request)a

  @doc """
  A content precondition for whole-form edits. Credentials, usage touches and
  binding row identities/timestamps are not fields the operator form writes.
  Every binding writer holds the key lock while replacing its binding values.
  """
  @spec edit_revision(APIKey.t(), [APIKeyPolicyBinding.t()]) :: String.t()
  def edit_revision(%APIKey{} = api_key, bindings) do
    key_values =
      api_key
      |> Map.take(@edit_fields)
      |> Map.update!(:allowed_model_identifiers, fn
        nil -> nil
        models -> Enum.sort(Enum.uniq(models))
      end)

    binding_values = bindings |> Enum.map(&Map.take(&1, @binding_edit_fields)) |> Enum.sort()
    content = :erlang.term_to_binary({:api_key_edit, 1, key_values, binding_values}, [:deterministic])
    :sha256 |> :crypto.hash(content) |> Base.encode16(case: :lower)
  end

  @spec create_api_key(map(), [map()], String.t(), DateTime.t()) :: create_result()
  def create_api_key(api_key_attrs, policy_inputs, raw_key, timestamp) do
    Repo.transaction(fn ->
      with {:ok, api_key} <- Repo.insert(APIKey.changeset(%APIKey{}, api_key_attrs)),
           {:ok, policy_bindings} <-
             insert_api_key_policy_bindings(Repo, policy_inputs, api_key, timestamp) do
        %{api_key: api_key, raw_key: raw_key, policy_bindings: policy_bindings}
      else
        {:error, reason} -> Repo.rollback(reason)
      end
    end)
  end

  @spec update_api_key_policy_in_transaction(
          APIKey.t(),
          map(),
          [map()],
          DateTime.t(),
          non_neg_integer()
        ) :: update_policy_result()
  def update_api_key_policy_in_transaction(
        api_key,
        update_attrs,
        policy_inputs,
        timestamp,
        runtime_revocation_epoch
      ) do
    if Repo.in_transaction?() do
      changeset = APIKey.changeset(api_key, update_attrs)

      changeset =
        Ecto.Changeset.put_change(
          changeset,
          :runtime_revocation_epoch,
          RuntimeAuthorization.epoch_for_policy_change(runtime_revocation_epoch, api_key, changeset)
        )

      with {:ok, updated_api_key} <- Repo.update(changeset),
           {_count, _rows} <-
             Repo.delete_all(from(binding in APIKeyPolicyBinding, where: binding.api_key_id == ^api_key.id)),
           {:ok, bindings} <-
             insert_api_key_policy_bindings(Repo, policy_inputs, updated_api_key, timestamp) do
        {:ok, %{api_key: updated_api_key, policy_bindings: bindings}}
      end
    else
      raise ArgumentError, "API key policy update requires an active transaction"
    end
  end

  @doc """
  Reads a key's policy bindings. An update reads them after it takes the key's
  writer lock, which every binding write also holds.
  """
  @spec list_policy_bindings(Ecto.UUID.t()) :: [APIKeyPolicyBinding.t()]
  def list_policy_bindings(api_key_id) do
    Repo.all(
      from binding in APIKeyPolicyBinding,
        where: binding.api_key_id == ^api_key_id,
        order_by: [asc: binding.binding_scope, asc: binding.model_identifier]
    )
  end

  @spec normalize_transaction_result(transaction_result(value)) :: {:ok, value} | {:error, term()}
        when value: term()
  def normalize_transaction_result({:ok, %{result: value}}), do: {:ok, value}
  def normalize_transaction_result({:ok, value}), do: {:ok, value}
  def normalize_transaction_result({:error, _operation, value, _changes}), do: {:error, value}
  def normalize_transaction_result({:error, value}), do: {:error, value}

  defp insert_api_key_policy_bindings(repo, policy_inputs, api_key, timestamp) do
    Enum.reduce_while(policy_inputs, {:ok, []}, fn policy_attrs, {:ok, acc} ->
      policy_attrs =
        Map.merge(policy_attrs, %{
          api_key_id: api_key.id,
          created_at: timestamp,
          updated_at: timestamp
        })

      case repo.insert(APIKeyPolicyBinding.changeset(%APIKeyPolicyBinding{}, policy_attrs)) do
        {:ok, binding} -> {:cont, {:ok, [binding | acc]}}
        {:error, changeset} -> {:halt, {:error, changeset}}
      end
    end)
    |> case do
      {:ok, bindings} -> {:ok, Enum.reverse(bindings)}
      {:error, _reason} = error -> error
    end
  end
end
