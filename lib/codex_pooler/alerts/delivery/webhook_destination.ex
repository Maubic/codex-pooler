defmodule CodexPooler.Alerts.Delivery.WebhookDestination do
  @moduledoc false
  import Bitwise

  defmodule Error do
    @moduledoc false
    defexception [:reason]
    @type t :: %__MODULE__{reason: :invalid | :forbidden | :unresolved}
    @impl true
    def message(_error), do: "webhook destination is unavailable"
  end

  @type resolver :: (charlist(), :inet | :inet6 -> {:ok, [:inet.ip_address()]} | {:error, atom()})
  @type result :: {:ok, :inet.ip_address()} | {:error, Error.t()}

  @spec resolve(URI.t()) :: result()
  def resolve(uri), do: resolve(uri, resolver())

  @spec resolve(URI.t(), resolver()) :: result()
  def resolve(%URI{scheme: "https", host: host, port: port}, resolver) when is_binary(host) and port in 1..65_535 do
    case :inet.parse_address(String.to_charlist(host)) do
      {:ok, address} -> select([{:ok, [address]}])
      {:error, _} -> select(Enum.map([:inet, :inet6], &resolver.(String.to_charlist(host), &1)))
    end
  end

  def resolve(_uri, _resolver), do: error(:invalid)

  defp select(results) do
    addresses = for {:ok, values} <- results, address <- values, do: address

    cond do
      Enum.any?(addresses, &forbidden?/1) -> error(:forbidden)
      addresses == [] or not Enum.all?(results, &complete_family?/1) -> error(:unresolved)
      true -> {:ok, hd(addresses)}
    end
  end

  defp complete_family?({:ok, addresses}) when is_list(addresses), do: true
  defp complete_family?({:error, reason}) when reason in [:nxdomain, :eafnosupport], do: true
  defp complete_family?(_), do: false

  @spec forbidden?(:inet.ip_address()) :: boolean()
  def forbidden?({127, _, _, _}), do: true
  def forbidden?({169, 254, _, _}), do: true
  def forbidden?({0, 0, 0, 0}), do: true
  def forbidden?({100, 100, 100, 200}), do: true
  def forbidden?({0, 0, 0, 0, 0, 0, 0, value}) when value in [0, 1], do: true
  def forbidden?({first, _, _, _, _, _, _, _}) when (first &&& 0xFFC0) == 0xFE80, do: true
  def forbidden?({0xFD00, 0xEC2, 0, 0, 0, 0, 0, 0x254}), do: true
  def forbidden?({0xFD20, 0xCE, 0, 0, 0, 0, 0, 0x254}), do: true
  def forbidden?({0, 0, 0, 0, 0, 0xFFFF, high, low}), do: forbidden?({high >>> 8, high &&& 255, low >>> 8, low &&& 255})
  def forbidden?(_), do: false

  defp error(reason), do: {:error, %Error{reason: reason}}

  if Mix.env() == :test do
    defp resolver, do: Application.get_env(:codex_pooler, __MODULE__, []) |> Keyword.get(:resolver, &:inet.getaddrs/2)
  else
    defp resolver, do: &:inet.getaddrs/2
  end
end
