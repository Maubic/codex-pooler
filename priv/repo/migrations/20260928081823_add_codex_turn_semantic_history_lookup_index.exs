defmodule CodexPooler.Repo.Migrations.AddCodexTurnSemanticHistoryLookupIndex do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true
  @index "codex_turns_semantic_history_idx"
  @expression "(semantic_turn_digest) WHERE (semantic_turn_digest IS NOT NULL)"

  def up do
    execute(fn -> MigrationLockBudget.run(repo(), &converge_index/0) end)
  end

  def down do
    execute(fn -> MigrationLockBudget.run(repo(), &drop_index/0) end)
  end

  defp converge_index do
    definition = "CREATE INDEX #{@index} ON public.codex_turns USING btree #{@expression}"

    case index_state() do
      [[true, true, ^definition]] ->
        :ok

      state when state in [[], [[false, false, definition]], [[false, true, definition]], [[true, false, definition]]] ->
        if state != [], do: drop_index()
        repo().query!("CREATE INDEX CONCURRENTLY #{@index} ON public.codex_turns #{@expression}", [], log: false, timeout: :infinity)
        [[true, true, ^definition]] = index_state()
        :ok

      _conflicting ->
        raise "conflicting index: #{@index}"
    end
  end

  defp index_state do
    repo().query!("SELECT i.indisvalid, i.indisready, pg_get_indexdef(c.oid) FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid = c.oid WHERE c.oid = to_regclass('public.#{@index}')", [], log: false).rows
  end

  defp drop_index, do: repo().query!("DROP INDEX CONCURRENTLY IF EXISTS public.#{@index}", [], log: false, timeout: :infinity)
end
