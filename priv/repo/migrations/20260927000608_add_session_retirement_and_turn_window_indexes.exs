defmodule CodexPooler.Repo.Migrations.AddSessionRetirementAndTurnWindowIndexes do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true
  @indexes [
    {"codex_sessions_retirement_idx", "codex_sessions", "(COALESCE(owner_lease_expires_at, updated_at), id) WHERE (status = ANY (ARRAY['active'::text, 'interrupted'::text]))"},
    {"codex_turns_started_idx", "codex_turns", "(started_at)"},
    {"attempts_model_history_started_idx", "attempts", "(started_at, id) WHERE (status <> ALL (ARRAY['queued'::text, 'in_progress'::text]))"}
  ]

  def up do
    execute(fn ->
      MigrationLockBudget.run(repo(), fn ->
        Enum.each(@indexes, &converge_index/1)
      end)
    end)
  end

  def down do
    execute(fn ->
      MigrationLockBudget.run(repo(), &drop_lookup_indexes/0)
    end)
  end

  defp drop_lookup_indexes do
    Enum.each(["codex_sessions_retirement_idx", "codex_turns_started_idx"], &drop_index/1)
  end

  defp converge_index({name, table, expression}) do
    definition = "CREATE INDEX #{name} ON public.#{table} USING btree #{expression}"

    case index_state(name) do
      [[true, true, ^definition]] ->
        :ok

      state when state in [[], [[false, false, definition]], [[false, true, definition]], [[true, false, definition]]] ->
        if state != [], do: drop_index(name)
        repo().query!("CREATE INDEX CONCURRENTLY #{name} ON public.#{table} #{expression}", [], log: false, timeout: :infinity)
        [[true, true, ^definition]] = index_state(name)
        :ok

      _conflicting ->
        raise "conflicting index: #{name}"
    end
  end

  defp index_state(name) do
    repo().query!("SELECT i.indisvalid, i.indisready, pg_get_indexdef(c.oid) FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid = c.oid WHERE c.oid = to_regclass('public.#{name}')", [], log: false).rows
  end

  defp drop_index(name), do: repo().query!("DROP INDEX CONCURRENTLY IF EXISTS public.#{name}", [], log: false, timeout: :infinity)
end
