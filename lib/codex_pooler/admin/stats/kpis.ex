defmodule CodexPooler.Admin.Stats.Kpis do
  @moduledoc false

  alias CodexPooler.Admin.Stats.Aggregates

  # A turn has no rejected or cancelled status (`codex_turns_status_check`): a
  # failed turn is a failed or an interrupted one.
  @failed_turn_statuses ~w(failed interrupted)

  @type cache_rate_kpi :: %{
          value: float() | nil,
          cached_input_tokens: integer(),
          input_tokens: integer()
        }

  @type token_kpi_input :: %{
          cached_input_tokens: integer(),
          input_tokens: integer(),
          output_tokens: integer(),
          reasoning_tokens: integer(),
          total_tokens: integer()
        }

  @spec request_kpi([map()]) :: map()
  def request_kpi(request_buckets) do
    %{
      value: Aggregates.sum_integer(request_buckets, :requests),
      succeeded: Aggregates.sum_integer(request_buckets, :succeeded),
      failed: Aggregates.sum_integer(request_buckets, :failed),
      client_cancelled: Aggregates.sum_integer(request_buckets, :client_cancelled),
      in_progress: Aggregates.sum_integer(request_buckets, :in_progress)
    }
  end

  # A request the client cancelled is neither a success nor a failure, so it
  # leaves the rate's base as well (`RequestOutcome`).
  @spec success_rate_kpi([map()]) :: map()
  def success_rate_kpi([]), do: %{value: nil, unit: "percent", client_cancelled: 0}

  def success_rate_kpi(request_buckets) do
    requests = request_kpi(request_buckets)
    %{value: Aggregates.percentage(requests.succeeded, requests.value - requests.client_cancelled), unit: "percent", client_cancelled: requests.client_cancelled}
  end

  @spec token_kpi([map()]) :: map()
  def token_kpi(settlements) do
    %{
      input_tokens: Aggregates.sum_integer(settlements, :input_tokens),
      cached_input_tokens: Aggregates.sum_integer(settlements, :cached_input_tokens),
      output_tokens: Aggregates.sum_integer(settlements, :output_tokens),
      reasoning_tokens: Aggregates.sum_integer(settlements, :reasoning_tokens),
      total_tokens: Aggregates.sum_integer(settlements, :total_tokens)
    }
  end

  @spec cache_rate_kpi(token_kpi_input()) :: cache_rate_kpi()
  def cache_rate_kpi(%{
        cached_input_tokens: cached_input_tokens,
        input_tokens: input_tokens
      }) do
    %{
      value: Aggregates.percentage(cached_input_tokens, input_tokens),
      cached_input_tokens: cached_input_tokens,
      input_tokens: input_tokens
    }
  end

  @spec tokens_per_second_kpi([map()], [map()]) :: map()
  def tokens_per_second_kpi(settlements, attempts) do
    total_tokens = Aggregates.sum_integer(settlements, :total_tokens)
    latency_ms = Aggregates.sum_integer(Enum.filter(attempts, & &1.latency_ms), :latency_ms)

    value =
      if total_tokens > 0 and latency_ms > 0 do
        Float.round(total_tokens / (latency_ms / 1000), 2)
      end

    %{value: value, unit: "tokens/second"}
  end

  @spec settled_cost_kpi([map()]) :: map()
  def settled_cost_kpi([]), do: %{status: "unavailable", micros: 0, usd: nil}

  def settled_cost_kpi(settlements) do
    micros = Aggregates.sum_decimal_integer(settlements, :settled_cost_micros)

    %{
      status: if(micros > 0, do: "settled", else: "unpriced"),
      micros: micros,
      usd: Aggregates.micros_to_usd_decimal(micros)
    }
  end

  @spec average_latency_kpi([map()]) :: map()
  def average_latency_kpi(attempts) do
    latencies = attempts |> Enum.map(& &1.latency_ms) |> Enum.filter(&is_integer/1)

    value =
      case latencies do
        [] -> nil
        _latencies -> round(Enum.sum(latencies) / length(latencies))
      end

    %{value: value, unit: "ms"}
  end

  @spec turn_kpi([map()]) :: map()
  def turn_kpi(turns) do
    %{
      value: Enum.sum(Enum.map(turns, & &1.count)),
      succeeded: turn_count(turns, ["succeeded"]),
      failed: turn_count(turns, @failed_turn_statuses),
      in_progress: turn_count(turns, ["in_progress"])
    }
  end

  defp turn_count(turns, statuses), do: turns |> Enum.filter(&(&1.status in statuses)) |> Enum.reduce(0, &(&1.count + &2))
end
