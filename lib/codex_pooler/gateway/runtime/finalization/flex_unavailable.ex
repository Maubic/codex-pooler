defmodule CodexPooler.Gateway.Runtime.Finalization.FlexUnavailable do
  @moduledoc false

  alias CodexPooler.Gateway.Runtime.Finalization.Metadata

  @spec response?(Req.Response.t()) :: boolean()
  def response?(%Req.Response{status: 429} = response) do
    case Metadata.rejection_body(response) do
      body when is_binary(body) and byte_size(body) <= 65_536 ->
        case CodexPooler.JSON.decode(body) do
          {:ok, %{"error" => %{"code" => "flex_unavailable"}}} -> true
          _other -> false
        end

      _other ->
        false
    end
  end

  def response?(_response), do: false

  @spec error() :: map()
  def error, do: %{"code" => "flex_unavailable", "type" => "rate_limit_error", "message" => "Flex capacity unavailable."}
end
