defmodule CodexPooler.Repo.Migrations.SkipUnchangedLedgerUsageComponents do
  use Ecto.Migration

  def up do
    execute("SET LOCAL lock_timeout = '10s'")
    execute("SET LOCAL statement_timeout = '30s'")
    execute(sync_function(true))
  end

  def down do
    execute("SET LOCAL lock_timeout = '10s'")
    execute("SET LOCAL statement_timeout = '30s'")
    execute(sync_function(false))
  end

  # These are the inputs read by api_key_usage_events and the legacy deltas.
  # IDs and timestamps also determine request grouping and terminal selection.
  # Keep full details conservatively, including estimated_from_reserve.
  # A FULL JOIN catches changed row IDs; null-safe comparison catches key detach.
  # Only whole statements with identical accounting inputs can skip derivation.
  defp sync_function(skip_unchanged?) do
    early_return = "IF NOT EXISTS (SELECT 1 FROM old_entries e JOIN public.api_keys k ON k.id=e.api_key_id UNION ALL SELECT 1 FROM new_entries e JOIN public.api_keys k ON k.id=e.api_key_id) THEN RETURN NULL; END IF;"

    early_return = if skip_unchanged?, do: early_return <> unchanged_guard(), else: early_return

    """
    CREATE OR REPLACE FUNCTION public.sync_api_key_usage_components_update()
    RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $function$
    BEGIN
      #{early_return}
      WITH old_rows AS MATERIALIZED (SELECT * FROM old_entries),
      new_rows AS MATERIALIZED (SELECT * FROM new_entries),
      affected AS (SELECT o.request_id FROM old_rows o WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = o.api_key_id) UNION SELECT n.request_id FROM new_rows n WHERE EXISTS (SELECT 1 FROM public.api_keys k WHERE k.id = n.api_key_id)),
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

  defp unchanged_guard do
    """

      IF NOT EXISTS (
        SELECT 1 FROM old_entries o FULL JOIN new_entries n ON n.id = o.id
        WHERE ROW(o.id, o.request_id, o.api_key_id, o.attempt_id, o.entry_kind, o.amount_status, o.usage_status, o.occurred_at, o.created_at, o.request_count, o.total_tokens, o.estimated_cost_micros, o.settled_cost_micros, o.details)
          IS DISTINCT FROM ROW(n.id, n.request_id, n.api_key_id, n.attempt_id, n.entry_kind, n.amount_status, n.usage_status, n.occurred_at, n.created_at, n.request_count, n.total_tokens, n.estimated_cost_micros, n.settled_cost_micros, n.details)
      ) THEN RETURN NULL; END IF;
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
