defmodule CodexPooler.Gateway.Transports.RetryAfter do
  @moduledoc false

  @private_key :codex_pooler_retry_after
  @max_delay_seconds 18_446_744_073_709_551_615

  @spec capture({:ok, Req.Response.t()} | {:error, term()}, integer()) :: {:ok, Req.Response.t()} | {:error, term()}
  def capture(result, now \\ System.monotonic_time(:millisecond))

  def capture({:ok, %Req.Response{} = response}, now) do
    {:ok, Req.Response.put_private(response, @private_key, advice(response, now))}
  end

  def capture(result, _now), do: result

  @spec header(Req.Response.t(), integer()) :: String.t() | nil
  def header(response, now \\ System.monotonic_time(:millisecond)) do
    case Req.Response.get_private(response, @private_key, :uncaptured) do
      :uncaptured -> render(advice(response, now), now)
      advice -> render(advice, now)
    end
  end

  defp advice(%Req.Response{status: status} = response, now) when status == 429 or status in 500..599 do
    case Req.Response.get_header(response, "retry-after") do
      [value | _rest] when is_binary(value) and byte_size(value) <= 128 -> parse(String.trim(value), now)
      _other -> nil
    end
  end

  defp advice(_response, _now), do: nil

  defp parse(value, now) do
    case Integer.parse(value) do
      {seconds, ""} when seconds >= 0 and seconds <= @max_delay_seconds ->
        if String.match?(value, ~r/\A[0-9]+\z/), do: {:deadline, now + seconds * 1_000}

      _other ->
        case Req.Utils.parse_http_date(value) do
          {:ok, _date} -> {:date, value}
          {:error, _reason} -> nil
        end
    end
  end

  defp render({:deadline, deadline}, now), do: Integer.to_string(div(max(deadline - now, 0) + 999, 1_000))
  defp render({:date, value}, _now), do: value
  defp render(nil, _now), do: nil
end
