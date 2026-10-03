defmodule CodexPooler.Repo.Migrations.AddCodexSessionCloseReason do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true

  @function_body """
  BEGIN
    IF NEW.close_reason IS NOT NULL THEN
      IF NEW.close_reason IS NOT DISTINCT FROM OLD.close_reason THEN
        IF (NEW.status, NEW.closed_at, NEW.owner_lease_token, NEW.owner_lease_expires_at, NEW.pool_id, NEW.api_key_id, NEW.session_key)
          IS DISTINCT FROM
          (OLD.status, OLD.closed_at, OLD.owner_lease_token, OLD.owner_lease_expires_at, OLD.pool_id, OLD.api_key_id, OLD.session_key) THEN
          NEW.close_reason := NULL;
        END IF;
      ELSIF NOT (OLD.close_reason IS NULL AND NEW.close_reason = 'owner_lease_expired'
        AND OLD.status IN ('active', 'interrupted') AND NEW.status = 'closed'
        AND NEW.owner_lease_expires_at IS NOT NULL AND NEW.closed_at IS NOT NULL
        AND NEW.owner_lease_expires_at <= NEW.closed_at) THEN
        NEW.close_reason := NULL;
      END IF;
    END IF;
    RETURN NEW;
  END
  """

  def up do
    online(fn ->
      query("LOCK TABLE public.codex_sessions IN ACCESS EXCLUSIVE MODE NOWAIT")
      ensure_column!()
      verify_function!()
      verify_trigger!()

      query("""
      CREATE OR REPLACE FUNCTION public.invalidate_codex_session_close_reason()
      RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog, public AS $function$
      #{@function_body}\
      $function$
      """)

      query("DROP TRIGGER IF EXISTS codex_sessions_invalidate_close_reason ON public.codex_sessions")
      query("CREATE TRIGGER codex_sessions_invalidate_close_reason BEFORE UPDATE ON public.codex_sessions FOR EACH ROW EXECUTE FUNCTION public.invalidate_codex_session_close_reason()")
      ensure_constraint!()
    end)
  end

  def down do
    online(fn ->
      query("LOCK TABLE public.codex_sessions IN ACCESS EXCLUSIVE MODE NOWAIT")
      verify_column!()
      verify_constraint!()
      verify_function!()
      verify_trigger!()
      query("DROP TRIGGER IF EXISTS codex_sessions_invalidate_close_reason ON public.codex_sessions")
      query("DROP FUNCTION IF EXISTS public.invalidate_codex_session_close_reason()")
      query("ALTER TABLE public.codex_sessions DROP CONSTRAINT IF EXISTS codex_sessions_close_reason_check, DROP COLUMN IF EXISTS close_reason")
    end)
  end

  defp online(fun) do
    execute(fn ->
      MigrationLockBudget.run(
        repo(),
        fn ->
          {:ok, _result} = repo().transaction(fun)
        end,
        lock_wait_ms: 5_000
      )
    end)
  end

  defp query(sql), do: repo().query!(sql, [], log: false)

  defp verify_function! do
    expected = "\n" <> @function_body

    case query("SELECT p.prosrc, p.prorettype = 'trigger'::regtype, l.lanname, p.prosecdef, p.proconfig FROM pg_proc p JOIN pg_language l ON l.oid = p.prolang WHERE p.oid = to_regprocedure('public.invalidate_codex_session_close_reason()')").rows do
      [] -> :absent
      [[^expected, true, "plpgsql", false, ["search_path=pg_catalog, public"]]] -> :present
      _other -> raise "conflicting invalidate_codex_session_close_reason function"
    end
  end

  defp verify_trigger! do
    case query("SELECT tgfoid = to_regprocedure('public.invalidate_codex_session_close_reason()'), tgtype FROM pg_trigger WHERE tgrelid = 'public.codex_sessions'::regclass AND tgname = 'codex_sessions_invalidate_close_reason'").rows do
      [] -> :absent
      [[true, 19]] -> :present
      _other -> raise "conflicting codex_sessions_invalidate_close_reason trigger"
    end
  end

  defp ensure_column! do
    case verify_column!() do
      :absent -> query("ALTER TABLE public.codex_sessions ADD COLUMN close_reason text")
      :present -> :ok
    end
  end

  defp verify_column! do
    case query("SELECT a.atttypid = 'text'::regtype, a.attnotnull, a.atthasdef FROM pg_attribute a WHERE a.attrelid = 'public.codex_sessions'::regclass AND a.attname = 'close_reason' AND NOT a.attisdropped").rows do
      [] -> :absent
      [[true, false, false]] -> :present
      _other -> raise "conflicting codex_sessions.close_reason column"
    end
  end

  defp ensure_constraint! do
    case verify_constraint!() do
      :absent -> query("ALTER TABLE public.codex_sessions ADD CONSTRAINT codex_sessions_close_reason_check CHECK (close_reason IS NULL OR (close_reason = 'owner_lease_expired' AND status = 'closed' AND closed_at IS NOT NULL AND owner_lease_expires_at IS NOT NULL AND owner_lease_expires_at <= closed_at)) NOT VALID")
      :present -> :ok
    end
  end

  defp verify_constraint! do
    expected = "((close_reason IS NULL) OR ((close_reason = 'owner_lease_expired'::text) AND (status = 'closed'::text) AND (closed_at IS NOT NULL) AND (owner_lease_expires_at IS NOT NULL) AND (owner_lease_expires_at <= closed_at)))"

    case query("SELECT contype, pg_get_expr(conbin, conrelid) FROM pg_constraint WHERE conrelid = 'public.codex_sessions'::regclass AND conname = 'codex_sessions_close_reason_check'").rows do
      [] -> :absent
      [["c", ^expected]] -> :present
      _other -> raise "conflicting codex_sessions_close_reason_check constraint"
    end
  end
end
