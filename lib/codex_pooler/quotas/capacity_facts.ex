defmodule CodexPooler.Quotas.CapacityFacts do
  @moduledoc """
  Distinct, bounded capacity observations from one complete provider usage receipt.

  These are physical observations, not a model entitlement or permission to spend.
  The persisted policy and a separately qualified request contract decide admission.
  """

  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Quotas.Evidence.CodexParsers.RateLimitReachedType

  @version 1
  @denials [:none, :included_limit, :spend_limit, :workspace_limit, :model_limit, :malformed, :unknown]
  @max_balance_bytes 128
  @max_windows 2
  @max_window_minutes 525_600
  @enforce_keys [:observed_at, :included_permission, :credit_permission, :denial_category]
  defstruct @enforce_keys ++
              [version: @version, credential_epoch: nil, balance: nil, has_credits: nil, unlimited: nil, account_windows: [], source_kind: :usage_payload]

  @type included_permission :: :available | :exhausted | :unknown
  @type credit_permission :: :available | :unavailable | :unknown
  @type denial_category :: :none | :included_limit | :spend_limit | :workspace_limit | :model_limit | :malformed | :unknown
  @type source_kind :: :usage_payload | :wham_usage | :codex_usage | :api_codex_usage
  @type account_window :: %{
          window_kind: String.t(),
          window_minutes: pos_integer(),
          reset_at: DateTime.t(),
          used_percent: String.t()
        }
  @type t :: %__MODULE__{
          version: pos_integer(),
          observed_at: DateTime.t(),
          credential_epoch: pos_integer() | nil,
          included_permission: included_permission(),
          credit_permission: credit_permission(),
          denial_category: denial_category(),
          balance: String.t() | nil,
          has_credits: boolean() | nil,
          unlimited: boolean() | nil,
          account_windows: [account_window()],
          source_kind: source_kind()
        }

  @spec version() :: pos_integer()
  def version, do: @version

  @spec from_usage(map(), [Evidence.t()], :present | :absent | :unknown, DateTime.t()) :: t()
  def from_usage(payload, windows, account_window_state, %DateTime{} = observed_at) do
    included = included_signal(Map.fetch(payload, "rate_limit"))
    spend = spend_signal(Map.fetch(payload, "spend_control"))
    reached = reached_signal(Map.fetch(payload, "rate_limit_reached_type"))
    {credit, balance, has_credits, unlimited} = credits_signal(Map.fetch(payload, "credits"))
    denial = denial_category(included, spend, reached, credit, account_window_state)

    facts = %__MODULE__{
      observed_at: observed_at,
      included_permission: included_permission(included, denial),
      credit_permission: credit_permission(credit, spend, denial),
      denial_category: denial,
      balance: balance,
      has_credits: has_credits,
      unlimited: unlimited
    }

    case account_descriptors(windows) do
      {:ok, descriptors} -> preserve_raw_account_shape(%{facts | account_windows: descriptors}, payload)
      :error when denial in [:workspace_limit, :model_limit] -> facts
      :error -> revoke(facts, :malformed)
    end
  end

  # Routing has a historical weekly-primary projection that omits its second
  # account slot. That reduced view cannot attest a sole-resource credit contract.
  defp preserve_raw_account_shape(%__MODULE__{credit_permission: :available} = facts, %{"rate_limit" => %{} = rate_limit}) do
    raw_count =
      Enum.count(["primary", "secondary"], fn slot ->
        canonical = Map.get(rate_limit, "#{slot}_window")
        not is_nil(if(is_nil(canonical), do: Map.get(rate_limit, slot), else: canonical))
      end)

    if raw_count > length(facts.account_windows), do: %{facts | credit_permission: :unknown}, else: facts
  end

  defp preserve_raw_account_shape(facts, _payload), do: facts

  @spec source_for_path(String.t()) :: source_kind()
  def source_for_path("/backend-api/wham/usage"), do: :wham_usage
  def source_for_path("/backend-api/codex/usage"), do: :codex_usage
  def source_for_path("/api/codex/usage"), do: :api_codex_usage
  def source_for_path(_path), do: :usage_payload

  @spec positive_balance?(t() | nil) :: boolean()
  def positive_balance?(%__MODULE__{balance: balance}) when is_binary(balance) do
    case normalize_balance(balance) do
      {:ok, canonical} -> Decimal.positive?(Decimal.new(canonical))
      :error -> false
    end
  end

  def positive_balance?(_facts), do: false

  @spec normalize_balance(term()) :: {:ok, String.t()} | :error
  def normalize_balance(value) when is_integer(value) and value >= 0 do
    if value < Integer.pow(10, 34), do: normalize_balance(Integer.to_string(value)), else: :error
  end

  def normalize_balance(value) when is_float(value), do: normalize_balance(Float.to_string(value))

  def normalize_balance(value) when is_binary(value) and byte_size(value) <= @max_balance_bytes do
    with {%Decimal{sign: 1, coef: coefficient} = decimal, ""} when is_integer(coefficient) <-
           Decimal.parse(String.trim(value), max_digits: 34, max_exponent: 64),
         canonical <- decimal |> Decimal.normalize() |> Decimal.to_string(:normal, max_digits: @max_balance_bytes),
         true <- byte_size(canonical) <= @max_balance_bytes do
      {:ok, canonical}
    else
      _invalid -> :error
    end
  rescue
    _exception in [ArgumentError, Decimal.Error] -> :error
  end

  def normalize_balance(_value), do: :error

  @spec authority_observed?(t()) :: boolean()
  def authority_observed?(%__MODULE__{} = facts) do
    facts.included_permission != :unknown or facts.credit_permission != :unknown or
      facts.denial_category in [:included_limit, :spend_limit, :workspace_limit, :model_limit]
  end

  @spec revoke(t(), denial_category()) :: t()
  def revoke(%__MODULE__{} = facts, category \\ :unknown) when category in @denials,
    do: %{facts | included_permission: :unknown, credit_permission: :unknown, denial_category: category}

  defp included_signal({:ok, %{"allowed" => true, "limit_reached" => false}}), do: :available
  defp included_signal({:ok, %{"allowed" => false, "limit_reached" => true}}), do: :exhausted
  defp included_signal({:ok, nil}), do: :unknown
  defp included_signal(:error), do: :unknown
  defp included_signal(_malformed), do: :malformed

  defp spend_signal({:ok, %{"reached" => false}}), do: :clear
  defp spend_signal({:ok, %{"reached" => true}}), do: :reached
  defp spend_signal(:error), do: :absent
  defp spend_signal(_malformed), do: :malformed

  defp reached_signal(:error), do: :none
  defp reached_signal({:ok, nil}), do: :none

  defp reached_signal({:ok, %{"type" => type}}) do
    case RateLimitReachedType.parse(type) do
      "rate_limit_reached" -> :included_limit
      "workspace_" <> _suffix -> :workspace_limit
      _unknown -> :malformed
    end
  end

  defp reached_signal(_malformed), do: :malformed

  defp credits_signal(:error), do: {:unknown, nil, nil, nil}
  defp credits_signal({:ok, nil}), do: {:malformed, nil, nil, nil}

  defp credits_signal({:ok, %{"has_credits" => has_credits, "unlimited" => unlimited} = credits})
       when is_boolean(has_credits) and is_boolean(unlimited) do
    case Map.fetch(credits, "balance") do
      {:ok, value} -> credits_with_balance(normalize_balance(value), has_credits, unlimited)
      :error when unlimited -> {:available, nil, has_credits, unlimited}
      :error -> {:unknown, nil, has_credits, unlimited}
    end
  end

  defp credits_signal(_malformed), do: {:malformed, nil, nil, nil}

  defp credits_with_balance({:ok, balance}, has_credits, unlimited) do
    positive = Decimal.positive?(Decimal.new(balance))

    state =
      cond do
        unlimited -> :available
        has_credits and positive -> :available
        not has_credits and not positive -> :unavailable
        true -> :malformed
      end

    {state, balance, has_credits, unlimited}
  end

  defp credits_with_balance(:error, has_credits, unlimited), do: {:malformed, nil, has_credits, unlimited}

  defp denial_category(_included, _spend, :workspace_limit, _credit, _windows), do: :workspace_limit
  defp denial_category(:available, _spend, :included_limit, _credit, _windows), do: :malformed

  defp denial_category(included, spend, reached, credit, windows) do
    cond do
      malformed_observation?([included, spend, reached, credit], windows) -> :malformed
      spend == :reached -> :spend_limit
      reached == :included_limit or included == :exhausted -> :included_limit
      included == :available -> :none
      credit == :available and spend == :clear -> :none
      true -> :unknown
    end
  end

  defp malformed_observation?(signals, windows), do: :malformed in signals or windows == :unknown

  defp included_permission(_included, denial) when denial in [:workspace_limit, :model_limit, :malformed], do: :unknown
  defp included_permission(:available, _denial), do: :available
  defp included_permission(:exhausted, _denial), do: :exhausted
  defp included_permission(_included, _denial), do: :unknown

  defp credit_permission(_credit, _spend, :malformed), do: :unknown
  defp credit_permission(_credit, _spend, denial) when denial in [:workspace_limit, :model_limit, :spend_limit], do: :unavailable
  defp credit_permission(:available, :clear, _denial), do: :available
  defp credit_permission(:unavailable, _spend, _denial), do: :unavailable
  defp credit_permission(_credit, _spend, _denial), do: :unknown

  @spec account_descriptors([Evidence.t()]) :: {:ok, [account_window()]} | :error
  defp account_descriptors(windows) do
    windows
    |> Enum.reduce_while({:ok, []}, fn
      %{quota_scope: "account", quota_key: "account"} = window, {:ok, descriptors} ->
        case account_descriptor(window) do
          {:ok, descriptor} -> {:cont, {:ok, [descriptor | descriptors]}}
          :absent -> {:cont, {:ok, descriptors}}
          :error -> {:halt, :error}
        end

      _other_scope, result ->
        {:cont, result}
    end)
    |> case do
      {:ok, descriptors} ->
        descriptors = Enum.uniq(descriptors)
        if length(descriptors) <= @max_windows, do: {:ok, Enum.sort_by(descriptors, &{&1.window_kind, &1.window_minutes})}, else: :error

      :error ->
        :error
    end
  end

  @spec account_descriptor(Evidence.t()) :: {:ok, account_window()} | :absent | :error
  defp account_descriptor(%{window_kind: kind, window_minutes: minutes, reset_at: %DateTime{year: year} = reset_at, used_percent: %Decimal{coef: coefficient, sign: 1} = percent})
       when kind in ["primary", "secondary"] and minutes in 1..@max_window_minutes and year in 0..9999 and is_integer(coefficient) do
    with {:ok, reset_at} <- DateTime.shift_zone(reset_at, "Etc/UTC"),
         {:ok, used_percent} <- normalize_balance(Decimal.to_string(percent)),
         true <- Decimal.compare(Decimal.new(used_percent), 100) != :gt do
      {:ok, %{window_kind: kind, window_minutes: minutes, reset_at: reset_at, used_percent: used_percent}}
    else
      _invalid -> :error
    end
  end

  defp account_descriptor(%{window_kind: kind, window_minutes: minutes, reset_at: reset_at, used_percent: percent})
       when kind in ["primary", "secondary"] and minutes in 1..@max_window_minutes and (is_nil(reset_at) or is_nil(percent)),
       do: :absent

  defp account_descriptor(_unsupported), do: :error
end
