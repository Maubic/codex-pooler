defmodule CodexPooler.Upstreams.Quota.CapacityFactsStore do
  @moduledoc false

  alias CodexPooler.Quotas.{CapacityFacts, Evidence}

  @key "quota_capacity_facts"
  @blocker_key "quota_capacity_blocker"
  @keys ~w(version observed_at credential_epoch included_permission credit_permission denial_category balance has_credits unlimited account_windows source_kind)
  @included Map.new([:available, :exhausted, :unknown], &{Atom.to_string(&1), &1})
  @credit Map.new([:available, :unavailable, :unknown], &{Atom.to_string(&1), &1})
  @denials Map.new([:none, :included_limit, :spend_limit, :workspace_limit, :model_limit, :malformed, :unknown], &{Atom.to_string(&1), &1})
  @sources Map.new([:usage_payload, :wham_usage, :codex_usage, :api_codex_usage], &{Atom.to_string(&1), &1})
  @window_keys ~w(window_kind window_minutes reset_at used_percent)
  @blocker_keys ~w(version credential_epoch overflowed observations)
  @max_blockers 16
  @hard_denials [:workspace_limit, :model_limit]
  @reset_rounding_seconds 5

  @type blockers :: %{credential_epoch: pos_integer(), observations: [CapacityFacts.t()], overflowed?: boolean()}

  @spec metadata_key() :: String.t()
  def metadata_key, do: @key

  @spec encode!(CapacityFacts.t(), pos_integer()) :: map()
  def encode!(%CapacityFacts{} = facts, epoch) when is_integer(epoch) and epoch > 0 do
    %{
      "version" => CapacityFacts.version(),
      "observed_at" => DateTime.to_iso8601(DateTime.shift_zone!(facts.observed_at, "Etc/UTC")),
      "credential_epoch" => epoch,
      "included_permission" => Atom.to_string(facts.included_permission),
      "credit_permission" => Atom.to_string(facts.credit_permission),
      "denial_category" => Atom.to_string(facts.denial_category),
      "balance" => facts.balance,
      "has_credits" => facts.has_credits,
      "unlimited" => facts.unlimited,
      "account_windows" => Enum.map(facts.account_windows, &encode_window/1),
      "source_kind" => Atom.to_string(facts.source_kind)
    }
    |> validate_encoded!()
  end

  @spec decode(term()) :: {:ok, CapacityFacts.t()} | :error
  def decode(%{} = encoded) when map_size(encoded) == length(@keys) do
    with true <- Enum.sort(Map.keys(encoded)) == Enum.sort(@keys),
         1 <- encoded["version"],
         epoch when is_integer(epoch) and epoch > 0 <- encoded["credential_epoch"],
         {:ok, observed_at} <- utc_datetime(encoded["observed_at"]),
         included when not is_nil(included) <- @included[encoded["included_permission"]],
         credit when not is_nil(credit) <- @credit[encoded["credit_permission"]],
         denial when not is_nil(denial) <- @denials[encoded["denial_category"]],
         source when not is_nil(source) <- @sources[encoded["source_kind"]],
         true <- valid_balance?(encoded["balance"]),
         true <- encoded["has_credits"] in [true, false, nil],
         true <- encoded["unlimited"] in [true, false, nil],
         {:ok, windows} <- decode_windows(encoded["account_windows"]),
         facts = %CapacityFacts{version: 1, observed_at: observed_at, credential_epoch: epoch, included_permission: included, credit_permission: credit, denial_category: denial, balance: encoded["balance"], has_credits: encoded["has_credits"], unlimited: encoded["unlimited"], account_windows: windows, source_kind: source},
         true <- coherent?(facts) do
      {:ok, facts}
    else
      _invalid -> :error
    end
  end

  def decode(_encoded), do: :error

  @spec load(map() | nil) :: {:ok, CapacityFacts.t()} | :error
  def load(metadata) when is_map(metadata), do: decode(metadata[@key])
  def load(_metadata), do: :error

  @spec load_blockers(map() | nil) :: {:ok, blockers()} | :error
  def load_blockers(metadata) when is_map(metadata), do: decode_blockers(metadata[@blocker_key])
  def load_blockers(_metadata), do: :error

  @spec decode_blockers(term()) :: {:ok, blockers()} | :error
  defp decode_blockers(%{} = encoded) when map_size(encoded) == 4 do
    with true <- Enum.sort(Map.keys(encoded)) == Enum.sort(@blocker_keys),
         1 <- encoded["version"],
         epoch when is_integer(epoch) and epoch > 0 <- encoded["credential_epoch"],
         overflowed when is_boolean(overflowed) <- encoded["overflowed"],
         observations when is_list(observations) and length(observations) <= @max_blockers <- encoded["observations"],
         {:ok, observations} <- decode_blocker_observations(observations, epoch) do
      {:ok, %{credential_epoch: epoch, observations: observations, overflowed?: overflowed}}
    else
      _invalid -> :error
    end
  end

  defp decode_blockers(_encoded), do: :error

  @spec decode_blocker_observations([term()], pos_integer()) :: {:ok, [CapacityFacts.t()]} | :error
  defp decode_blocker_observations(encoded, epoch) do
    Enum.reduce_while(encoded, {:ok, []}, fn observation, {:ok, observations} ->
      with {:ok, %CapacityFacts{credential_epoch: ^epoch} = facts} <- decode(observation),
           true <- blocking_observation?(facts) do
        {:cont, {:ok, [facts | observations]}}
      else
        _invalid -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, observations} -> {:ok, Enum.reverse(observations)}
      :error -> :error
    end
  end

  @spec transition(map() | nil, CapacityFacts.t(), pos_integer()) :: map()
  def transition(metadata, %CapacityFacts{credential_epoch: source_epoch}, epoch)
      when not is_nil(source_epoch) and source_epoch != epoch,
      do: if(is_map(metadata), do: metadata, else: %{})

  def transition(metadata, %CapacityFacts{} = observation, epoch) when is_integer(epoch) and epoch > 0 do
    metadata = if is_map(metadata), do: metadata, else: %{}

    case load(metadata) do
      {:ok, %CapacityFacts{credential_epoch: ^epoch} = current} ->
        case DateTime.compare(observation.observed_at, current.observed_at) do
          :lt -> metadata
          :eq -> equal_transition(metadata, current, observation, epoch)
          :gt -> metadata |> retain_replaced(current, epoch) |> write_observation(observation, epoch)
        end

      _other ->
        write_observation(metadata, observation, epoch)
    end
  end

  @doc "Retains and clears each queried witness without replacing the reduced complete authority."
  @spec record_observations(map(), [CapacityFacts.t()], pos_integer()) :: map()
  def record_observations(metadata, observations, epoch) do
    case load(metadata) do
      {:ok, %CapacityFacts{credential_epoch: ^epoch, observed_at: observed_at}} ->
        Enum.reduce(observations, metadata, fn
          %CapacityFacts{observed_at: ^observed_at, credential_epoch: source_epoch} = observation, current when source_epoch in [nil, epoch] ->
            write_blockers(current, observation, epoch)

          _superseded, current ->
            current
        end)

      _unverified ->
        metadata
    end
  end

  @doc """
  Moves valid capacity facts and valid retained blockers recorded at
  `from_epoch` to `to_epoch`, each on its own, and leaves every other value as
  it is: a token refresh keeps the provider account they describe
  (findings#334). Only the epoch tags change, the blockers' own and each
  retained observation's; every other field, `observed_at` included, stays
  byte for byte.
  """
  @spec carry_forward(map(), pos_integer(), pos_integer()) :: map()
  def carry_forward(metadata, from_epoch, to_epoch) when is_map(metadata) and is_integer(to_epoch) and to_epoch > 0 do
    metadata =
      case load(metadata) do
        {:ok, %CapacityFacts{credential_epoch: ^from_epoch}} -> put_in(metadata, [@key, "credential_epoch"], to_epoch)
        _absent_invalid_or_other_epoch -> metadata
      end

    case load_blockers(metadata) do
      {:ok, %{credential_epoch: ^from_epoch}} ->
        Map.update!(metadata, @blocker_key, fn blockers ->
          %{blockers | "credential_epoch" => to_epoch, "observations" => Enum.map(blockers["observations"], &Map.put(&1, "credential_epoch", to_epoch))}
        end)

      _absent_invalid_or_other_epoch ->
        metadata
    end
  end

  defp write_observation(metadata, observation, epoch) do
    metadata
    |> Map.put(@key, encode!(observation, epoch))
    |> write_blockers(observation, epoch)
  end

  # A blocking current reading denies while it is current. Before a newer reading
  # replaces it, it is retained unless a witness covers it, as on its own write,
  # so a denial an earlier release recorded only as the current reading does not
  # leave with it.
  defp retain_replaced(metadata, current, epoch) do
    if blocking_observation?(current),
      do: put_blockers(metadata, retain_blocking_observation(retained_blockers(metadata, epoch), current), epoch),
      else: metadata
  end

  defp write_blockers(metadata, observation, epoch) do
    blockers = retained_blockers(metadata, epoch)
    retained = Enum.reject(blockers.observations, &supersedes_blocker?(observation, &1))
    blockers = %{blockers | observations: retained}
    blockers = if blocking_observation?(observation), do: retain_blocking_observation(blockers, observation), else: blockers
    put_blockers(metadata, blockers, epoch)
  end

  defp put_blockers(metadata, blockers, epoch) do
    if blockers.observations == [] and not blockers.overflowed? do
      Map.delete(metadata, @blocker_key)
    else
      Map.put(metadata, @blocker_key, %{
        "version" => 1,
        "credential_epoch" => epoch,
        "overflowed" => blockers.overflowed?,
        "observations" => Enum.map(blockers.observations, &encode!(&1, epoch))
      })
    end
  end

  @spec retained_blockers(map(), pos_integer()) :: blockers()
  defp retained_blockers(metadata, epoch) do
    case load_blockers(metadata) do
      {:ok, %{credential_epoch: ^epoch} = blockers} -> blockers
      {:ok, %{credential_epoch: previous_epoch}} when previous_epoch < epoch -> %{credential_epoch: epoch, observations: [], overflowed?: false}
      {:ok, _future_epoch} -> %{credential_epoch: epoch, observations: [], overflowed?: true}
      :error -> %{credential_epoch: epoch, observations: [], overflowed?: Map.has_key?(metadata, @blocker_key)}
    end
  end

  # A denial is left out only when a retained witness already covers it. Otherwise
  # it is retained beside the first witness of its binding, which stays so the
  # binding remains denied from its earliest observation, and replaces the later
  # witnesses it covers: a denial repeated through many cycles keeps the first
  # witness and the newest one for each set of exhausted windows.
  @spec retain_blocking_observation(blockers(), CapacityFacts.t()) :: blockers()
  defp retain_blocking_observation(blockers, observation) do
    if Enum.any?(blockers.observations, &covers_blocker?(&1, observation)) do
      blockers
    else
      first = Enum.find(blockers.observations, &same_blocker_binding?(&1, observation))
      retained = Enum.reject(blockers.observations, &(&1 != first and covers_blocker?(observation, &1)))

      if length(retained) < @max_blockers,
        do: %{blockers | observations: retained ++ [observation]},
        else: %{blockers | overflowed?: true}
    end
  end

  defp covers_blocker?(witness, observation) do
    (same_blocker_binding?(witness, observation) or strengthens_blocker?(witness, observation)) and
      covers_clearance?(witness, observation)
  end

  # Every way the witness ends must also end the observation's denial. Permission
  # ends both. A hard witness that recorded exhausted windows also ends with their
  # cycle, so the observation must have recorded exhausted windows too, each one
  # the witness recorded exhausted and resetting no later. No reset rounding here:
  # the witness's release and lapse are measured from its own reset, so a denial
  # resetting even a second later would outlast it.
  defp covers_clearance?(%{denial_category: category} = witness, observation) when category in @hard_denials,
    do: covers_exhausted_windows?(exhausted_windows(witness), exhausted_windows(observation))

  defp covers_clearance?(_witness, _observation), do: true

  defp covers_exhausted_windows?([], _exhausted), do: true
  defp covers_exhausted_windows?(_witnessed, []), do: false
  defp covers_exhausted_windows?(witnessed, exhausted), do: Enum.all?(exhausted, fn window -> Enum.any?(witnessed, &resets_by?(window, &1)) end)

  defp resets_by?(window, witnessed) do
    window.window_kind == witnessed.window_kind and window.window_minutes == witnessed.window_minutes and
      DateTime.compare(window.reset_at, witnessed.reset_at) != :gt
  end

  defp same_blocker_binding?(left, right) do
    left.denial_category == right.denial_category and
      compatible_blocker_resources?(left, right) and compatible_blocker_resources?(right, left)
  end

  defp strengthens_blocker?(observation, blocker) do
    compatible_blocker_resources?(observation, blocker) and
      clearance_kind(observation.denial_category) == clearance_kind(blocker.denial_category) and
      blocker_strength(observation.denial_category) > blocker_strength(blocker.denial_category) and
      DateTime.compare(observation.observed_at, blocker.observed_at) != :gt
  end

  defp clearance_kind(category) when category in [:spend_limit, :malformed], do: :credit
  defp clearance_kind(category) when category in [:workspace_limit, :model_limit], do: :hard

  defp blocker_strength(:workspace_limit), do: 4
  defp blocker_strength(:model_limit), do: 3
  defp blocker_strength(:malformed), do: 2
  defp blocker_strength(:spend_limit), do: 1

  defp blocking_observation?(facts),
    do: facts.denial_category in [:workspace_limit, :model_limit, :spend_limit, :malformed]

  defp supersedes_blocker?(observation, blocker) do
    compatible_blocker_resources?(observation, blocker) and
      DateTime.compare(observation.observed_at, blocker.observed_at) == :gt and
      (permission_supersedes?(observation, blocker) or later_cycle_supersedes?(observation, blocker))
  end

  defp permission_supersedes?(observation, blocker),
    do: observation.denial_category in [:none, :included_limit] and superseding_permission?(observation, blocker)

  # A workspace or model denial recorded while an account window was exhausted
  # ends with that window's cycle. A newer reading that no longer reports a hard
  # denial and shows every such window in a later cycle below its limit (a
  # natural roll-over or a provider-side reset) releases it, whatever else that
  # reading leaves unknown. Without an exhausted window only permission clears it.
  defp later_cycle_supersedes?(observation, %{denial_category: category} = blocker) when category in @hard_denials,
    do: observation.denial_category not in @hard_denials and exhausted_windows_released?(blocker, &later_cycle_window?(observation, &1))

  defp later_cycle_supersedes?(_observation, _blocker), do: false

  defp later_cycle_window?(observation, exhausted) do
    Enum.any?(observation.account_windows, fn current ->
      current.window_kind == exhausted.window_kind and current.window_minutes == exhausted.window_minutes and
        DateTime.diff(current.reset_at, exhausted.reset_at, :second) > @reset_rounding_seconds and
        not exhausted_window?(current)
    end)
  end

  @doc """
  True when a workspace or model denial was recorded with at least one exhausted
  account window and every one of them has reached its reset by `as_of`: the
  denial ended with those windows' cycle, so it no longer denies the account.
  """
  @spec hard_denial_lapsed?(CapacityFacts.t(), DateTime.t()) :: boolean()
  def hard_denial_lapsed?(%CapacityFacts{denial_category: category} = facts, %DateTime{} = as_of) when category in @hard_denials,
    do: exhausted_windows_released?(facts, &(DateTime.compare(&1.reset_at, as_of) != :gt))

  def hard_denial_lapsed?(_facts, _as_of), do: false

  defp exhausted_windows_released?(facts, released?) do
    case exhausted_windows(facts) do
      [] -> false
      exhausted -> Enum.all?(exhausted, released?)
    end
  end

  defp exhausted_windows(facts), do: Enum.filter(facts.account_windows, &exhausted_window?/1)

  defp exhausted_window?(window), do: Decimal.compare(Decimal.new(window.used_percent), 100) != :lt

  defp compatible_blocker_resources?(observation, blocker) do
    observation.source_kind == blocker.source_kind and
      Enum.all?(blocker.account_windows, fn blocked ->
        Enum.any?(observation.account_windows, fn current ->
          current.window_kind == blocked.window_kind and current.window_minutes == blocked.window_minutes
        end)
      end)
  end

  defp superseding_permission?(observation, %{denial_category: category}) when category in [:spend_limit, :malformed],
    do: observation.credit_permission != :unknown

  defp superseding_permission?(observation, _blocker),
    do: observation.credit_permission != :unknown or observation.included_permission == :available

  @spec current?(CapacityFacts.t() | nil, pos_integer(), DateTime.t()) :: boolean()
  def current?(%CapacityFacts{credential_epoch: epoch, observed_at: at}, epoch, %DateTime{} = as_of),
    do: DateTime.compare(at, as_of) != :gt

  def current?(_facts, _epoch, _as_of), do: false

  @spec fresh?(CapacityFacts.t() | nil, pos_integer(), DateTime.t()) :: boolean()
  def fresh?(%CapacityFacts{observed_at: observed_at} = facts, epoch, %DateTime{} = as_of) do
    current?(facts, epoch, as_of) and
      DateTime.diff(as_of, observed_at, :microsecond) <= Evidence.freshness_ttl_seconds() * 1_000_000
  end

  def fresh?(_facts, _epoch, _as_of), do: false

  @spec hard_denial?(CapacityFacts.t() | nil, pos_integer(), DateTime.t()) :: boolean()
  def hard_denial?(%CapacityFacts{denial_category: category} = facts, epoch, as_of),
    do: current?(facts, epoch, as_of) and category in [:workspace_limit, :model_limit, :malformed]

  def hard_denial?(_facts, _epoch, _as_of), do: false

  defp equal_transition(metadata, current, observation, epoch) do
    encoded = encode!(observation, epoch)

    cond do
      encoded == metadata[@key] -> metadata
      blocking_observation?(current) -> if(blocking_observation?(observation), do: write_blockers(metadata, observation, epoch), else: metadata)
      observation.denial_category in [:workspace_limit, :model_limit, :malformed, :spend_limit] -> write_observation(metadata, observation, epoch)
      true -> Map.put(metadata, @key, encode!(CapacityFacts.revoke(observation), epoch))
    end
  end

  defp validate_encoded!(encoded) do
    case decode(encoded) do
      {:ok, _facts} -> encoded
      :error -> raise ArgumentError, "invalid quota capacity facts"
    end
  end

  defp coherent?(%CapacityFacts{credit_permission: :available} = facts) do
    facts.denial_category in [:none, :included_limit] and
      (facts.included_permission != :available or facts.denial_category == :none) and
      ((facts.has_credits == true and CapacityFacts.positive_balance?(facts)) or facts.unlimited == true)
  end

  defp coherent?(%CapacityFacts{included_permission: :available, denial_category: category}),
    do: category in [:none, :spend_limit, :unknown]

  defp coherent?(_facts), do: true

  defp valid_balance?(nil), do: true
  defp valid_balance?(value) when is_binary(value), do: CapacityFacts.normalize_balance(value) == {:ok, value}
  defp valid_balance?(_value), do: false

  defp encode_window(window) do
    %{"window_kind" => window.window_kind, "window_minutes" => window.window_minutes, "reset_at" => DateTime.to_iso8601(window.reset_at), "used_percent" => window.used_percent}
  end

  defp decode_windows(windows) when is_list(windows) and length(windows) <= 2 do
    Enum.reduce_while(windows, {:ok, []}, fn window, {:ok, decoded} ->
      case decode_window(window) do
        {:ok, descriptor} -> {:cont, {:ok, [descriptor | decoded]}}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      :error -> :error
    end
  end

  defp decode_windows(_windows), do: :error

  defp decode_window(%{} = window) when map_size(window) == 4 do
    with true <- Enum.sort(Map.keys(window)) == Enum.sort(@window_keys),
         kind when kind in ["primary", "secondary"] <- window["window_kind"],
         minutes when is_integer(minutes) and minutes > 0 and minutes <= 525_600 <- window["window_minutes"],
         {:ok, reset_at} <- utc_datetime(window["reset_at"]),
         {:ok, percent} <- CapacityFacts.normalize_balance(window["used_percent"]),
         true <- Decimal.compare(Decimal.new(percent), 100) != :gt do
      {:ok, %{window_kind: kind, window_minutes: minutes, reset_at: reset_at, used_percent: percent}}
    else
      _invalid -> :error
    end
  end

  defp decode_window(_window), do: :error

  defp utc_datetime(value) when is_binary(value) and byte_size(value) <= 40 do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> {:ok, DateTime.shift_zone!(datetime, "Etc/UTC")}
      _invalid -> :error
    end
  end

  defp utc_datetime(_value), do: :error
end
