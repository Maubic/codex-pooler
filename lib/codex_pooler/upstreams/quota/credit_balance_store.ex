defmodule CodexPooler.Upstreams.Quota.CreditBalanceStore do
  @moduledoc false

  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Quotas.Evidence.CodexParsers

  @key "quota_credit_balance"
  @type snapshot :: %{
          balance: non_neg_integer(),
          observed_at: DateTime.t(),
          has_credits: boolean() | nil,
          unlimited: boolean() | nil
        }

  @spec reported?(map() | nil) :: boolean()
  def reported?(metadata) when is_map(metadata), do: Map.has_key?(metadata, @key)
  def reported?(_metadata), do: false

  @spec transition(map() | nil, term(), DateTime.t(), pos_integer()) :: map()
  def transition(metadata, payload, %DateTime{} = observed_at, epoch) do
    metadata = metadata || %{}

    with %{"credits" => %{} = credits} <- payload,
         balance when is_integer(balance) <- CodexParsers.codex_usage_credits(credits),
         true <- newer?(metadata[@key], observed_at, epoch) do
      Map.put(metadata, @key, %{
        "version" => 1,
        "balance" => balance,
        "has_credits" => boolean_or_nil(credits["has_credits"]),
        "unlimited" => boolean_or_nil(credits["unlimited"]),
        "observed_at" => DateTime.to_iso8601(observed_at),
        "credential_epoch" => epoch
      })
    else
      _unreported -> metadata
    end
  end

  @spec current(map() | nil, pos_integer(), DateTime.t()) :: snapshot() | nil
  def current(metadata, epoch, %DateTime{} = as_of) when is_map(metadata) do
    with {:ok, %{observed_at: observed_at} = snapshot, ^epoch} <- decode(metadata[@key]),
         age = DateTime.diff(as_of, observed_at, :microsecond),
         true <- age >= 0 and age <= Evidence.freshness_ttl_seconds() * 1_000_000 do
      snapshot
    else
      _unavailable -> nil
    end
  end

  def current(_metadata, _epoch, _as_of), do: nil

  @doc """
  Moves a valid balance recorded at `from_epoch` to `to_epoch` and leaves every
  other value as it is: a token refresh keeps the provider account whose
  balance this is (findings#334). Only the epoch changes; the balance and
  `observed_at` stay byte for byte, so the freshness bound is unchanged.
  """
  @spec carry_forward(map(), pos_integer(), pos_integer()) :: map()
  def carry_forward(metadata, from_epoch, to_epoch) when is_map(metadata) and is_integer(to_epoch) and to_epoch > 0 do
    case decode(metadata[@key]) do
      {:ok, _snapshot, ^from_epoch} -> put_in(metadata, [@key, "credential_epoch"], to_epoch)
      _absent_invalid_or_other_epoch -> metadata
    end
  end

  defp newer?(encoded, observed_at, epoch) do
    case decode(encoded) do
      {:ok, %{observed_at: previous}, ^epoch} -> DateTime.compare(observed_at, previous) == :gt
      _unavailable -> true
    end
  end

  defp decode(
         %{
           "version" => 1,
           "balance" => balance,
           "has_credits" => has_credits,
           "unlimited" => unlimited,
           "observed_at" => observed_at,
           "credential_epoch" => epoch
         } = encoded
       )
       when map_size(encoded) == 6 and is_integer(balance) and balance >= 0 and
              is_binary(observed_at) and is_integer(epoch) and epoch > 0 do
    with true <- valid_flags?(has_credits, unlimited),
         {:ok, parsed, 0} <- DateTime.from_iso8601(observed_at) do
      {:ok, %{balance: balance, observed_at: parsed, has_credits: has_credits, unlimited: unlimited}, epoch}
    else
      _invalid -> :error
    end
  end

  defp decode(_encoded), do: :error

  defp valid_flags?(has_credits, unlimited),
    do: has_credits in [true, false, nil] and unlimited in [true, false, nil]

  defp boolean_or_nil(value) when is_boolean(value), do: value
  defp boolean_or_nil(_value), do: nil
end
