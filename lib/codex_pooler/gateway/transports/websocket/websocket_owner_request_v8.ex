defmodule CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV8 do
  @moduledoc "Explicit owner protocol requiring the executing session's final persisted capacity gate."

  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Transports.ProviderCreditsAdmission
  alias CodexPooler.Gateway.Transports.Websocket.{WebsocketOwnerRequest, WebsocketOwnerRequestV2, WebsocketOwnerRequestV3, WebsocketOwnerRequestV4, WebsocketOwnerRequestV5, WebsocketOwnerRequestV6, WebsocketOwnerRequestV7}

  @fields [:version, :request, :provider_credits_context]
  @enforce_keys @fields
  defstruct @fields

  @type inner_request :: WebsocketOwnerRequest.t() | WebsocketOwnerRequestV2.t() | WebsocketOwnerRequestV3.t() | WebsocketOwnerRequestV4.t() | WebsocketOwnerRequestV5.t() | WebsocketOwnerRequestV6.t() | WebsocketOwnerRequestV7.t()
  @type t :: %__MODULE__{version: 8, request: inner_request(), provider_credits_context: ProviderCreditsAdmission.Context.t()}
  @type validation_error :: {:invalid_field, atom()} | {:unknown_fields, [atom()]}

  @spec new(map()) :: {:ok, t()} | {:error, validation_error()}
  def new(attrs) when is_map(attrs) and not is_struct(attrs) do
    with :ok <- exact_keys(attrs),
         request = struct!(__MODULE__, attrs),
         :ok <- validate(request) do
      {:ok, request}
    end
  end

  def new(_attrs), do: {:error, {:invalid_field, :envelope}}

  @spec validate(term()) :: :ok | {:error, validation_error()}
  def validate(%__MODULE__{} = envelope) do
    with :ok <- exact_keys(Map.from_struct(envelope)),
         true <- envelope.version == 8,
         true <- ProviderCreditsAdmission.valid_context?(envelope.provider_credits_context),
         :ok <- validate_inner(envelope.request),
         true <- context_matches?(envelope.request, envelope.provider_credits_context) do
      :ok
    else
      false -> {:error, {:invalid_field, :provider_credits_context}}
      {:error, _reason} = error -> error
    end
  end

  def validate(_envelope), do: {:error, {:invalid_field, :envelope}}

  defp validate_inner(%WebsocketOwnerRequest{} = request), do: WebsocketOwnerRequest.validate(request)
  defp validate_inner(%WebsocketOwnerRequestV2{} = request), do: WebsocketOwnerRequestV2.validate(request)
  defp validate_inner(%WebsocketOwnerRequestV3{} = request), do: WebsocketOwnerRequestV3.validate(request)
  defp validate_inner(%WebsocketOwnerRequestV4{} = request), do: WebsocketOwnerRequestV4.validate(request)
  defp validate_inner(%WebsocketOwnerRequestV5{} = request), do: WebsocketOwnerRequestV5.validate(request)
  defp validate_inner(%WebsocketOwnerRequestV6{} = request), do: WebsocketOwnerRequestV6.validate(request)
  defp validate_inner(%WebsocketOwnerRequestV7{} = request), do: WebsocketOwnerRequestV7.validate(request)
  defp validate_inner(_request), do: {:error, {:invalid_field, :request}}

  defp context_matches?(request, context) do
    request.upstream_identity_id == context.upstream_identity_id and
      request.observation.request_id == context.request_id and
      request.observation.attempt_id == context.attempt_id and
      effective_mode(request) == context.serving_mode and
      probe_matches?(request.reset_probe, context.reset_probe)
  end

  defp probe_matches?(nil, nil), do: true

  defp probe_matches?(%ResetProbe{} = probe, nil),
    do: ResetProbe.unbound?(probe)

  defp probe_matches?(probe, expected), do: probe == expected

  defp effective_mode(%{effective_serving_mode: mode}) when mode in [:full, :lite], do: mode
  defp effective_mode(%{observation: %{mode: "full"}}), do: :full
  defp effective_mode(%{observation: %{mode: "lite"}}), do: :lite
  defp effective_mode(_request), do: nil

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

defimpl Inspect, for: CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV8 do
  def inspect(_request, _opts), do: "#WebsocketOwnerRequestV8<version: 8, final_admission: required, redacted>"
end
