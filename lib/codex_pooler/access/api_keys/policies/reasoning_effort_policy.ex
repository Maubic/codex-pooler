defmodule CodexPooler.Access.APIKeys.ReasoningEffortPolicy do
  @moduledoc false

  alias CodexPooler.Access.APIKey

  @known_efforts ~w(none minimal low medium high xhigh max ultra)
  @fallback_efforts ~w(low medium high xhigh)

  defmodule Decision do
    @moduledoc false

    @enforce_keys [:mode, :configured_effort, :requested_effort, :applied_effort]
    defstruct @enforce_keys

    @type mode :: :unrestricted | :allow_up_to | :always_use
    @type t :: %__MODULE__{
            mode: mode(),
            configured_effort: String.t() | nil,
            requested_effort: String.t() | non_neg_integer() | nil,
            applied_effort: String.t() | non_neg_integer() | nil
          }
  end

  defmodule MetadataProjection do
    @moduledoc false

    @enforce_keys [:levels, :default_effort]
    defstruct @enforce_keys

    @type level :: %{required(String.t()) => String.t()}
    @type t :: %__MODULE__{
            levels: [level()],
            default_effort: String.t() | nil
          }
  end

  @type model_level :: MetadataProjection.level()
  @type denial_metadata :: %{
          required(:policy_mode) => String.t(),
          required(:configured_effort) => String.t() | nil,
          required(:requested_effort) => String.t() | non_neg_integer() | nil,
          required(:applied_effort) => nil
        }
  @type resolution :: {:ok, Decision.t()} | {:error, :reasoning_effort_not_allowed}
  @type normalized_policy :: %{
          required(:allowed_model_identifiers) => [term()] | nil,
          required(:enforced_reasoning_effort) => String.t() | nil,
          required(:maximum_reasoning_effort) => String.t() | nil
        }

  @spec resolve(APIKey.t(), String.t() | non_neg_integer() | nil, [String.t()] | nil, String.t() | nil) ::
          resolution()
  def resolve(%APIKey{} = api_key, requested_effort, model_efforts, model_default) do
    case policy(api_key) do
      {:unrestricted, nil} ->
        decision(:unrestricted, nil, requested_effort, requested_effort)

      {:always_use, configured_effort} ->
        decision(:always_use, configured_effort, requested_effort, configured_effort)

      {:allow_up_to, configured_effort} ->
        resolve_allow_up_to(configured_effort, requested_effort, model_efforts, model_default)
    end
  end

  @spec project_metadata(
          APIKey.t() | normalized_policy(),
          [model_level()] | nil,
          String.t() | nil
        ) ::
          MetadataProjection.t()
  def project_metadata(api_key_or_policy, model_levels, model_default)
      when is_struct(api_key_or_policy, APIKey) or is_map(api_key_or_policy) do
    levels = effective_level_maps(model_levels)

    case policy(api_key_or_policy) do
      {:unrestricted, nil} ->
        projection(levels, model_default)

      {:allow_up_to, configured_effort} ->
        permitted = permitted_level_maps(levels, configured_effort)
        projection(permitted, permitted_default(permitted, model_default))

      {:always_use, configured_effort} ->
        case Enum.find(levels, &(normalize_known(level_effort(&1)) == configured_effort)) do
          nil -> projection([], nil)
          level -> projection([level], configured_effort)
        end
    end
  end

  @spec project_denial_metadata(APIKey.t(), String.t() | non_neg_integer() | nil) :: denial_metadata()
  def project_denial_metadata(%APIKey{} = api_key, requested_effort) do
    {mode, configured_effort} = policy(api_key)

    %{
      policy_mode: Atom.to_string(mode),
      configured_effort: configured_effort,
      requested_effort: requested_effort,
      applied_effort: nil
    }
  end

  defp resolve_allow_up_to(configured_effort, requested_effort, model_efforts, model_default) do
    permitted = permitted_efforts(effective_efforts(model_efforts), configured_effort)

    case requested_effort do
      nil ->
        resolve_default_effort(configured_effort, permitted, model_default)

      requested ->
        resolve_requested_effort(configured_effort, requested, permitted)
    end
  end

  defp resolve_default_effort(configured_effort, permitted, model_default) do
    case permitted_default_effort(permitted, model_default) do
      nil -> {:error, :reasoning_effort_not_allowed}
      applied -> decision(:allow_up_to, configured_effort, nil, applied)
    end
  end

  defp resolve_requested_effort(configured_effort, requested, permitted) do
    normalized = normalize_known(requested)

    if normalized in permitted do
      decision(:allow_up_to, configured_effort, requested, normalized)
    else
      {:error, :reasoning_effort_not_allowed}
    end
  end

  defp policy(%APIKey{enforced_reasoning_effort: effort}) when is_binary(effort),
    do: {:always_use, effort}

  defp policy(%APIKey{maximum_reasoning_effort: effort}) when is_binary(effort),
    do: {:allow_up_to, effort}

  defp policy(%APIKey{}), do: {:unrestricted, nil}

  defp policy(%{enforced_reasoning_effort: effort}) when is_binary(effort),
    do: {:always_use, effort}

  defp policy(%{maximum_reasoning_effort: effort}) when is_binary(effort),
    do: {:allow_up_to, effort}

  defp policy(%{}), do: {:unrestricted, nil}

  defp effective_efforts(nil), do: @fallback_efforts

  defp effective_efforts(efforts) when is_list(efforts) do
    efforts
    |> Enum.map(&normalize_known/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
  end

  defp effective_level_maps(nil),
    do: Enum.map(@fallback_efforts, &%{"effort" => &1, "description" => &1})

  defp effective_level_maps(levels) when is_list(levels), do: levels

  defp permitted_efforts(efforts, configured_effort) do
    maximum_rank = rank(configured_effort)
    Enum.filter(efforts, &(rank(&1) <= maximum_rank))
  end

  defp permitted_level_maps(levels, configured_effort) do
    maximum_rank = rank(configured_effort)

    Enum.filter(levels, fn level ->
      case normalize_known(level_effort(level)) do
        nil -> false
        effort -> rank(effort) <= maximum_rank
      end
    end)
  end

  defp permitted_default_effort(permitted, model_default) do
    default = normalize_known(model_default)

    if default in permitted,
      do: default,
      else: Enum.max_by(permitted, &rank/1, fn -> nil end)
  end

  defp permitted_default(levels, model_default) do
    levels
    |> Enum.map(&level_effort/1)
    |> permitted_default_effort(model_default)
  end

  defp normalize_known(value) when is_binary(value) do
    normalized = value |> String.trim() |> String.downcase()
    if normalized in @known_efforts, do: normalized
  end

  defp normalize_known(_value), do: nil

  defp level_effort(%{"effort" => effort}) when is_binary(effort), do: effort
  defp level_effort(%{effort: effort}) when is_binary(effort), do: effort
  defp level_effort(_level), do: nil

  defp rank(effort), do: Enum.find_index(@known_efforts, &(&1 == effort))

  defp decision(mode, configured_effort, requested_effort, applied_effort) do
    {:ok,
     %Decision{
       mode: mode,
       configured_effort: configured_effort,
       requested_effort: requested_effort,
       applied_effort: applied_effort
     }}
  end

  defp projection(levels, default_effort),
    do: %MetadataProjection{levels: levels, default_effort: default_effort}
end
