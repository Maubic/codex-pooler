defmodule CodexPooler.Accounting.Reporting.StatsUsage do
  @moduledoc false

  alias CodexPooler.Repo

  @source """
  WITH source AS (
    SELECT pool_id, api_key_id, upstream_identity_id, occurred_at, request_count,
      CASE WHEN usage_status = 'usage_known' THEN COALESCE(input_tokens, 0) ELSE 0 END AS input_tokens,
      CASE WHEN usage_status = 'usage_known' THEN COALESCE(cached_input_tokens, 0) ELSE 0 END AS cached_input_tokens,
      CASE WHEN usage_status = 'usage_known' THEN COALESCE(output_tokens, 0) ELSE 0 END AS output_tokens,
      CASE WHEN usage_status = 'usage_known' THEN COALESCE(reasoning_tokens, 0) ELSE 0 END AS reasoning_tokens,
      CASE WHEN usage_status = 'usage_known' THEN COALESCE(total_tokens, 0) ELSE 0 END AS total_tokens,
      CASE WHEN usage_status = 'usage_known' THEN ROUND(COALESCE(settled_cost_micros, 0)) ELSE 0 END AS settled_cost_micros
    FROM public.ledger_entries
    WHERE pool_id = ANY($1::uuid[]) AND entry_kind = 'settlement' AND amount_status = 'recorded'
      AND occurred_at >= $2 AND occurred_at <= $3
  )
  """

  # Cost is rounded per settlement before summing, as the dashboard has always done.
  @sums """
    count(*)::bigint AS source_count,
    COALESCE(sum(request_count), 0) AS request_count,
    sum(input_tokens) AS input_tokens,
    sum(cached_input_tokens) AS cached_input_tokens,
    sum(output_tokens) AS output_tokens,
    sum(reasoning_tokens) AS reasoning_tokens,
    sum(total_tokens) AS total_tokens,
    sum(settled_cost_micros) AS settled_cost_micros
  """

  @columns [:source_count, :request_count, :input_tokens, :cached_input_tokens, :output_tokens, :reasoning_tokens, :total_tokens, :settled_cost_micros]
  @buckets @source <> "SELECT date_trunc($4::text, occurred_at, 'UTC') AS occurred_at, " <> @sums <> " FROM source GROUP BY 1 ORDER BY 1"
  @upstreams @source <> "SELECT upstream_identity_id::text, " <> @sums <> " FROM source GROUP BY upstream_identity_id"
  @keys @source <>
          ", grouped AS (SELECT api_key_id, array_agg(DISTINCT pool_id::text) AS pool_ids, " <>
          @sums <>
          """
           FROM source GROUP BY api_key_id),
          top_tokens AS (
            SELECT * FROM grouped ORDER BY total_tokens DESC, request_count DESC, COALESCE(api_key_id::text, '') ASC LIMIT 10
          ),
          top_cost AS (
            SELECT * FROM grouped ORDER BY settled_cost_micros DESC, total_tokens DESC, COALESCE(api_key_id::text, '') ASC LIMIT 10
          ),
          selected AS (SELECT * FROM top_tokens UNION SELECT * FROM top_cost)
          SELECT api_key_id::text, pool_ids, source_count, request_count, input_tokens, cached_input_tokens,
            output_tokens, reasoning_tokens, total_tokens, settled_cost_micros
          FROM selected
          """

  @type result :: %{buckets: [map()], upstreams: [map()], keys: [map()], source_count: non_neg_integer()}

  @spec query([Ecto.UUID.t()], :hour | :day, DateTime.t(), DateTime.t()) :: result()
  def query([], _granularity, _started_at, _ended_at), do: %{buckets: [], upstreams: [], keys: [], source_count: 0}

  def query(pool_ids, granularity, started_at, ended_at) when granularity in [:hour, :day] do
    params = [Enum.map(pool_ids, &Ecto.UUID.dump!/1), started_at, ended_at]
    buckets = project(@buckets, params ++ [Atom.to_string(granularity)], [:occurred_at | @columns], :stats_settlement_buckets)

    %{
      buckets: buckets,
      upstreams: project(@upstreams, params, [:upstream_identity_id | @columns], :stats_settlement_upstreams),
      keys: project(@keys, params, [:api_key_id, :pool_ids | @columns], :stats_settlement_top_keys),
      source_count: Enum.sum_by(buckets, & &1.source_count)
    }
  end

  defp project(statement, params, columns, projection) do
    %{rows: rows} = Repo.query!(statement, params, telemetry_options: [reporting_projection: projection])
    Enum.map(rows, fn row -> Map.new(Enum.zip(columns, row), fn {key, value} -> {key, aggregate_value(value)} end) end)
  end

  defp aggregate_value(%Decimal{} = value), do: Decimal.to_integer(value)
  defp aggregate_value(value), do: value
end
