defmodule CodexPooler.Gateway.RequestCompression.WorkBudget do
  @moduledoc false

  # Deterministic cap on the compression work one dispatch may spend. Work is
  # charged in bytes read before the work happens: each candidate output handed
  # to a strategy, each text handed to the tokenizer, and a surcharge for the
  # bytes of tokenizer pieces long enough to take the heap merge. The same
  # payload therefore always stops at the same point, whatever the machine or
  # its load. Once a charge does not fit, the budget is spent for good: the
  # candidate being worked on and every later one keep their original bytes,
  # while rewrites already finished stay in place.

  @type t :: :counters.counters_ref()

  @spec new(pos_integer()) :: t()
  def new(units) when is_integer(units) and units > 0 do
    budget = :counters.new(1, [:atomics])
    :counters.put(budget, 1, units)
    budget
  end

  @spec charge(t() | nil, non_neg_integer()) :: :ok | {:error, :work_budget_exhausted}
  def charge(nil, _units), do: :ok

  def charge(budget, units) when is_integer(units) and units >= 0 do
    if units <= :counters.get(budget, 1) do
      :counters.sub(budget, 1, units)
      :ok
    else
      :counters.put(budget, 1, 0)
      {:error, :work_budget_exhausted}
    end
  end

  @spec from_opts(keyword() | map()) :: t() | nil
  def from_opts(opts) when is_list(opts), do: Keyword.get(opts, :work_budget)
  def from_opts(opts) when is_map(opts), do: Map.get(opts, :work_budget)
  def from_opts(_opts), do: nil
end
