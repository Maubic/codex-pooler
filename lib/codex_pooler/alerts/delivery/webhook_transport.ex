defmodule CodexPooler.Alerts.Delivery.WebhookTransport do
  @moduledoc false
  alias CodexPooler.Alerts.Delivery.{Execution, WebhookDestination}
  alias CodexPooler.Platform.OutboundHTTP

  @spec run(Req.Request.t()) :: {Req.Request.t(), Req.Response.t() | Exception.t()}
  def run(request) do
    execution = Req.Request.get_private(request, :webhook_execution)
    result = with {:ok, address} <- WebhookDestination.resolve(request.url), do: connect(request, address, execution)

    case result do
      {:ok, response} -> {request, response}
      {:error, %Mint.TransportError{reason: reason}} -> {request, %Req.TransportError{reason: reason}}
      {:error, %Mint.HTTPError{reason: reason}} -> {request, %Req.HTTPError{protocol: :http1, reason: reason}}
      {:error, error} -> {request, error}
    end
  end

  defp connect(request, address, execution) do
    remaining = Execution.remaining(execution)

    if remaining == 0 do
      {:error, %Req.TransportError{reason: :timeout}}
    else
      options = connection_options(request.url, address, remaining)

      case Mint.HTTP.connect(:https, address, request.url.port, options) do
        {:ok, connection} ->
          try do
            send_request(connection, request, execution)
          after
            Mint.HTTP.close(connection)
          end

        {:error, error} ->
          {:error, error}
      end
    end
  end

  defp connection_options(uri, address, remaining) do
    transport = [timeout: min(5_000, remaining), send_timeout: remaining, send_timeout_close: true]
    transport = if tuple_size(address) == 8, do: Keyword.put(transport, :inet6, true), else: transport
    options = [transport_opts: transport]

    proxy =
      uri
      |> OutboundHTTP.proxy_options_for_url(options)
      |> Enum.map(fn
        {:proxy, {scheme, host, port, opts}} -> {:proxy, {scheme, host, port, Keyword.put(opts, :tunnel_timeout, remaining)}}
        option -> option
      end)

    options |> Keyword.merge(proxy) |> Keyword.merge(hostname: uri.host, protocols: [:http1], mode: :passive)
  end

  defp send_request(connection, request, execution) do
    path = if request.url.path in [nil, ""], do: "/", else: request.url.path
    path = if request.url.query, do: path <> "?" <> request.url.query, else: path
    headers = for {name, values} <- request.headers, value <- List.wrap(values), do: {name, value}

    case Mint.HTTP.request(connection, "POST", path, headers, request.body) do
      {:ok, connection, ref} -> receive_response(connection, ref, request, Req.Response.new(body: ""), execution)
      {:error, _connection, error} -> {:error, error}
    end
  end

  defp receive_response(connection, ref, request, response, execution) do
    case Mint.HTTP.recv(connection, 0, max(Execution.remaining(execution), 1)) do
      {:ok, connection, events} ->
        case consume(events, ref, request, response) do
          {:more, request, response} -> receive_response(connection, ref, request, response, execution)
          {:done, response} -> {:ok, response}
          {:error, error} -> {:error, error}
        end

      {:error, _connection, error, _events} ->
        {:error, error}
    end
  end

  defp consume([], _ref, request, response), do: {:more, request, response}
  defp consume([{:status, ref, status} | events], ref, request, response), do: consume(events, ref, request, %{response | status: status})

  defp consume([{:headers, ref, fields} | events], ref, request, response) do
    incoming = Req.Response.new(headers: fields).headers
    headers = Map.merge(response.headers, incoming, fn _key, old, new -> old ++ new end)
    consume(events, ref, request, %{response | headers: headers})
  end

  defp consume([{:data, ref, data} | events], ref, request, response) do
    {:cont, {request, response}} = request.into.({:data, data}, {request, response})
    consume(events, ref, request, response)
  end

  defp consume([{:done, ref} | _events], ref, _request, response), do: {:done, response}
  defp consume([{:error, ref, error} | _events], ref, _request, _response), do: {:error, error}
end
