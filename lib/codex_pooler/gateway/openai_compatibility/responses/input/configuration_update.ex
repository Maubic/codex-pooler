defmodule CodexPooler.Gateway.OpenAICompatibility.Responses.Input.ConfigurationUpdate do
  @moduledoc false

  alias CodexPooler.Gateway.OpenAICompatibility.Error
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.UpstreamErrorParam

  # The provider accepts this exact item in Full and Lite, over HTTP and the websocket (findings#343,
  # direct probe 2026-10-07). Effort values and consecutive-update policy belong to the provider.
  @item_keys ~w(type reasoning)
  @reasoning_keys ~w(effort)

  @spec validate_item(term(), String.t()) :: :ok | {:error, Error.reason()}
  def validate_item(item, param \\ "input")

  def validate_item(%{"type" => "configuration_update"} = item, param) do
    with {:ok, reasoning} <- required_reasoning(item, param),
         :ok <- required_effort(reasoning, param <> ".reasoning"),
         :ok <- exact_keys(item, @item_keys, param) do
      exact_keys(reasoning, @reasoning_keys, param <> ".reasoning")
    end
  end

  def validate_item(_item, param), do: {:error, Error.invalid_request("input item shape is not translatable", param)}

  @spec required_reasoning(map(), String.t()) :: {:ok, map()} | {:error, Error.reason()}
  defp required_reasoning(item, param) do
    case Map.fetch(item, "reasoning") do
      :error -> missing_parameter(param <> ".reasoning")
      {:ok, reasoning} when is_map(reasoning) -> {:ok, reasoning}
      {:ok, _reasoning} -> invalid_type(param <> ".reasoning", "an object")
    end
  end

  @spec required_effort(map(), String.t()) :: :ok | {:error, Error.reason()}
  defp required_effort(reasoning, param) do
    case Map.fetch(reasoning, "effort") do
      :error -> missing_parameter(param <> ".effort")
      {:ok, effort} when is_binary(effort) -> :ok
      {:ok, _effort} -> invalid_type(param <> ".effort", "a string")
    end
  end

  @spec exact_keys(map(), [String.t()], String.t()) :: :ok | {:error, Error.reason()}
  defp exact_keys(map, allowed, param) do
    case Enum.find(map, fn {key, _value} -> key not in allowed end) do
      nil -> :ok
      {key, _value} -> unknown_parameter(unknown_parameter_path(param, key))
    end
  end

  @spec unknown_parameter_path(String.t(), term()) :: String.t()
  defp unknown_parameter_path(param, key) when is_binary(key), do: UpstreamErrorParam.sanitize(param <> "." <> key) || param
  defp unknown_parameter_path(param, _key), do: param

  @spec missing_parameter(String.t()) :: {:error, Error.reason()}
  defp missing_parameter(param), do: {:error, Error.reason(400, "missing_required_parameter", "missing required parameter #{param} (missing_required_parameter)", param)}

  @spec invalid_type(String.t(), String.t()) :: {:error, Error.reason()}
  defp invalid_type(param, expected), do: {:error, Error.reason(400, "invalid_type", "invalid type for parameter #{param} (invalid_type); expected #{expected}", param)}

  @spec unknown_parameter(String.t()) :: {:error, Error.reason()}
  defp unknown_parameter(param), do: {:error, Error.reason(400, "unknown_parameter", "unknown parameter #{param} (unknown_parameter)", param)}
end
