defmodule CodexPooler.Upstreams.Lifecycle.CredentialAuthorization do
  @moduledoc false
  import Ecto.Query
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment

  # Call only after canonical identity/assignment locks. Empty identities are
  # allowed here: the import boundary independently authorizes the target Pool.
  @spec require_served_pools(Scope.t(), [Ecto.UUID.t()]) :: :ok | {:error, map()}
  def require_served_pools(%Scope{} = scope, identity_ids) when is_list(identity_ids) do
    if Repo.in_transaction?() do
      pool_ids = Repo.all(from a in PoolUpstreamAssignment, where: a.upstream_identity_id in ^identity_ids and a.status != "deleted", distinct: true, select: a.pool_id, order_by: a.pool_id)

      Enum.reduce_while(pool_ids, :ok, &require_pool(scope, &1, &2))
    else
      {:error, %{code: :transaction_required, message: "credential authorization requires a caller-owned transaction"}}
    end
  end

  defp require_pool(scope, pool_id, :ok) do
    case Pools.require_capability(scope, Pools.capability(:pool_operate), pool_id: pool_id) do
      {:ok, _decision} -> {:cont, :ok}
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end
end
