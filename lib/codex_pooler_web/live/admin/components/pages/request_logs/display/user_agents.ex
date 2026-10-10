defmodule CodexPoolerWeb.Admin.RequestLogsDisplay.UserAgents do
  @moduledoc false

  alias CodexPooler.Accounting.ClientIdentity

  @type css_class :: ClientIdentity.css_class()
  @type logo :: ClientIdentity.logo()
  @type classification :: ClientIdentity.classification()

  @spec display(map()) :: map() | nil
  def display(%{user_agent: user_agent}) when is_binary(user_agent) do
    user_agent = String.trim(user_agent)

    if user_agent == "" do
      nil
    else
      classification = classify(user_agent)

      %{
        text: compact(user_agent),
        title: "#{classification.label} user agent",
        kind: classification.kind,
        label: classification.label,
        icon: classification.icon,
        icon_class: classification.icon_class,
        logo: classification.logo
      }
    end
  end

  def display(_request_log), do: nil

  @spec format(map()) :: String.t() | nil
  def format(%{user_agent: user_agent}) when is_binary(user_agent) and user_agent != "",
    do: compact(user_agent)

  def format(_request_log), do: nil

  @spec classify(String.t() | nil) :: classification()
  defdelegate classify(user_agent), to: ClientIdentity

  defp compact(user_agent) do
    user_agent = String.trim(user_agent)

    case Regex.run(~r/^([^\/\(]+)\/([^\s\(]+)(?:\s+\(([^)]*)\))?/, user_agent) do
      [_match, product, version | _ignored] ->
        product
        |> compact_product(version)
        |> truncate()

      _no_match ->
        truncate(user_agent)
    end
  end

  defp compact_product(product, version) do
    [String.trim(product), String.trim(version)]
    |> Enum.reject(&blank?/1)
    |> Enum.join(" ")
  end

  defp truncate(user_agent) do
    if String.length(user_agent) > 72 do
      String.slice(user_agent, 0, 69) <> "..."
    else
      user_agent
    end
  end

  defp blank?(value), do: String.trim(to_string(value)) == ""
end
