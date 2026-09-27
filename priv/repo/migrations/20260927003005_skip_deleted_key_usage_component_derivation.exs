defmodule CodexPooler.Repo.Migrations.SkipDeletedKeyUsageComponentDerivation do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")
    Enum.each([:insert, :update, :delete], &execute(sync_function(&1, true)))
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")
    Enum.each([:insert, :update, :delete], &execute(sync_function(&1, false)))
  end

  # Preserve full old/new transition images: a live key detached to NULL must
  # still remove its old contribution. Only surviving-key affected requests
  # need the expensive current-row join and event derivation.
  defp sync_function(operation, surviving_only?) do
    old_rows =
      if operation == :insert,
        do: "SELECT * FROM public.ledger_entries WHERE false",
        else: "SELECT * FROM old_entries"

    new_rows =
      if operation == :delete,
        do: "SELECT * FROM public.ledger_entries WHERE false",
        else: "SELECT * FROM new_entries"

    live_old = "SELECT o.request_id FROM old_rows o WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = o.api_key_id)"
    live_new = "SELECT n.request_id FROM new_rows n WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = n.api_key_id)"
    affected = if surviving_only?, do: "#{live_old} UNION #{live_new}", else: "SELECT request_id FROM old_rows UNION SELECT request_id FROM new_rows"

    live_transition =
      case operation do
        :insert -> "SELECT 1 FROM new_entries e JOIN public.api_keys k ON k.id=e.api_key_id"
        :update -> "SELECT 1 FROM old_entries e JOIN public.api_keys k ON k.id=e.api_key_id UNION ALL SELECT 1 FROM new_entries e JOIN public.api_keys k ON k.id=e.api_key_id"
        :delete -> "SELECT 1 FROM old_entries e JOIN public.api_keys k ON k.id=e.api_key_id"
      end

    early_return = if surviving_only?, do: "IF NOT EXISTS (#{live_transition}) THEN RETURN NULL; END IF;", else: ""

    """
    CREATE OR REPLACE FUNCTION public.sync_api_key_usage_components_#{operation}()
    RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $function$
    BEGIN
      #{early_return}
      WITH old_rows AS MATERIALIZED (#{old_rows}),
      new_rows AS MATERIALIZED (#{new_rows}),
      affected AS (#{affected}),
      current_rows AS MATERIALIZED (
        SELECT e.* FROM affected a JOIN public.ledger_entries e ON e.request_id = a.request_id
      ), before_rows AS (
        SELECT e.* FROM current_rows e WHERE NOT EXISTS (SELECT 1 FROM new_rows n WHERE n.id = e.id)
        UNION ALL SELECT * FROM old_rows
      ), before_events AS (
        SELECT v.* FROM (SELECT array_agg(e::public.ledger_entries) AS entries FROM before_rows e GROUP BY request_id) r
        CROSS JOIN LATERAL public.api_key_usage_events(r.entries) v
      ), after_events AS (
        SELECT v.* FROM (SELECT array_agg(e::public.ledger_entries) AS entries FROM current_rows e GROUP BY request_id) r
        CROSS JOIN LATERAL public.api_key_usage_events(r.entries) v
      ), deltas AS (
        SELECT api_key_id, occurred_at, 0::bigint AS requests, 0::bigint AS tokens, 0::numeric AS cost,
          -known_total_tokens AS known, -provisional_total_tokens AS provisional,
          -admission_count AS admissions, -known_cost_micros AS known_cost FROM before_events
        UNION ALL SELECT api_key_id, occurred_at, 0, 0, 0,
          known_total_tokens, provisional_total_tokens, admission_count, known_cost_micros FROM after_events
        UNION ALL #{legacy_delta("old_rows", -1)}
        UNION ALL #{legacy_delta("new_rows", 1)}
      ), grouped AS (
        SELECT d.api_key_id, date_trunc('minute', d.occurred_at) AS bucket_started_at,
          SUM(requests) AS requests, SUM(tokens) AS tokens, SUM(cost) AS cost,
          SUM(known) AS known, SUM(provisional) AS provisional,
          SUM(admissions) AS admissions, SUM(known_cost) AS known_cost
        FROM deltas d WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = d.api_key_id)
        GROUP BY d.api_key_id, date_trunc('minute', d.occurred_at)
      )
      INSERT INTO public.api_key_usage_buckets AS b
        (api_key_id, bucket_started_at, effective_request_count, effective_total_tokens,
         effective_cost_micros, known_total_tokens, provisional_total_tokens, admission_count,
         known_cost_micros, created_at, updated_at)
      SELECT api_key_id, bucket_started_at, requests, tokens, cost, known, provisional,
        admissions, known_cost, statement_timestamp(), statement_timestamp()
      FROM grouped ORDER BY api_key_id, bucket_started_at
      ON CONFLICT (api_key_id, bucket_started_at) DO UPDATE SET
        effective_request_count = b.effective_request_count + EXCLUDED.effective_request_count,
        effective_total_tokens = b.effective_total_tokens + EXCLUDED.effective_total_tokens,
        effective_cost_micros = b.effective_cost_micros + EXCLUDED.effective_cost_micros,
        known_total_tokens = b.known_total_tokens + EXCLUDED.known_total_tokens,
        provisional_total_tokens = b.provisional_total_tokens + EXCLUDED.provisional_total_tokens,
        admission_count = b.admission_count + EXCLUDED.admission_count,
        known_cost_micros = b.known_cost_micros + EXCLUDED.known_cost_micros,
        updated_at = statement_timestamp();
      RETURN NULL;
    END
    $function$
    """
  end

  defp legacy_delta(rows, sign) do
    """
    SELECT api_key_id, occurred_at,
      #{sign} * CASE WHEN entry_kind = 'release' THEN -request_count ELSE request_count END,
      #{sign} * CASE
        WHEN entry_kind = 'release' THEN -COALESCE(total_tokens, 0)
        WHEN entry_kind = 'settlement' AND usage_status <> 'usage_known' THEN 0
        ELSE COALESCE(total_tokens, 0) END,
      #{sign} * CASE
        WHEN entry_kind = 'release' THEN -estimated_cost_micros
        WHEN entry_kind = 'settlement' AND usage_status = 'usage_known' THEN settled_cost_micros
        WHEN entry_kind = 'settlement' THEN 0 ELSE estimated_cost_micros END,
      0, 0, 0, 0 FROM #{rows} WHERE amount_status = 'recorded'
    """
  end
end
