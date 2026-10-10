defmodule CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV9 do
  @moduledoc false

  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV8

  @fields [:version, :request, :native_response_steering]
  @enforce_keys @fields
  defstruct @fields

  @type t :: %__MODULE__{version: 9, request: WebsocketOwnerRequestV8.t(), native_response_steering: pid()}
  @type validation_error :: WebsocketOwnerRequestV8.validation_error()

  @spec new(map()) :: {:ok, t()} | {:error, validation_error()}
  def new(attrs) when is_map(attrs) and not is_struct(attrs) do
    with :ok <- exact_keys(attrs), request = struct!(__MODULE__, attrs), :ok <- validate(request), do: {:ok, request}
  end

  def new(_attrs), do: {:error, {:invalid_field, :envelope}}

  @spec validate(term()) :: :ok | {:error, validation_error()}
  def validate(%__MODULE__{version: 9, request: request, native_response_steering: lane} = envelope) when is_pid(lane) do
    with :ok <- exact_keys(Map.from_struct(envelope)),
         :ok <- WebsocketOwnerRequestV8.validate(request),
         true <- request.request.mapper == :native_codex_responses,
         true <- Map.get(request.request, :websocket_delivery_mode, :relay) == :relay do
      :ok
    else
      {:error, _reason} = error -> error
      false -> {:error, {:invalid_field, :native_response_steering}}
    end
  end

  def validate(_envelope), do: {:error, {:invalid_field, :native_response_steering}}

  defp exact_keys(attrs) do
    unknown = Map.keys(attrs) -- @fields
    missing = @fields -- Map.keys(attrs)

    cond do
      unknown != [] -> {:error, {:unknown_fields, Enum.sort_by(unknown, &to_string/1)}}
      missing != [] -> {:error, {:invalid_field, hd(missing)}}
      true -> :ok
    end
  end
end

defimpl Inspect, for: CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV9 do
  def inspect(_request, _opts), do: "#WebsocketOwnerRequestV9<version: 9, native_steering: resident, redacted>"
end
