defmodule CodexPoolerWeb.Admin.LensReadModel do
  @moduledoc false

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.RequestLogs.ModelHistory
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Pools
  alias CodexPoolerWeb.Admin.LensFilterForm
  alias CodexPoolerWeb.Admin.PoolFilterComponents

  @type result :: %{filters: map(), history: ModelHistory.result(), pool_options: [map()], model_options: [map()], pool_ids: MapSet.t(String.t())}

  @spec load(Scope.t(), map()) :: result()
  def load(scope, filters) do
    pools = Pools.list_log_filter_pools(scope)

    filters =
      if filters["pool_id"] != "" and not Enum.any?(pools, &(&1.id == filters["pool_id"])) do
        Map.merge(filters, %{"pool_id" => "", "upstream_identity_id" => ""})
      else
        filters
      end

    history = Accounting.model_declaration_history(scope, filters)

    watched_pools =
      Enum.filter(history.pools, fn pool -> filters["pool_id"] in ["", pool.id] end)

    %{
      filters: filters,
      history: history,
      pool_options: PoolFilterComponents.pool_filter_options(history.pools),
      model_options: LensFilterForm.model_options(history.models, filters["sent_model"]),
      pool_ids: MapSet.new(watched_pools, & &1.id)
    }
  end
end
