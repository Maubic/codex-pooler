defmodule CodexPooler.Repo.Migrations.AddExpiredQuotaWindowLookupIndex do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true
  @name "account_quota_windows_expired_reset_idx"

  def up do
    execute(fn -> MigrationLockBudget.run(repo(), &create_lookup_index/0) end)
  end

  def down do
    execute(fn -> MigrationLockBudget.run(repo(), &drop_lookup_index/0) end)
  end

  defp create_lookup_index do
    definition = "CREATE INDEX #{@name} ON public.account_quota_windows USING btree (reset_at, id) WHERE (reset_at IS NOT NULL)"

    case index_state() do
      [[true, true, ^definition]] ->
        :ok

      state when state in [[], [[false, false, definition]], [[false, true, definition]], [[true, false, definition]]] ->
        if state != [], do: drop_lookup_index()
        repo().query!("CREATE INDEX CONCURRENTLY #{@name} ON public.account_quota_windows (reset_at, id) WHERE reset_at IS NOT NULL", [], log: false, timeout: :infinity)
        [[true, true, ^definition]] = index_state()
        :ok

      _ ->
        raise "conflicting quota-window reset index"
    end
  end

  defp index_state do
    repo().query!("SELECT i.indisvalid, i.indisready, pg_get_indexdef(c.oid) FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid = c.oid WHERE c.oid = to_regclass('public.#{@name}')", [], log: false).rows
  end

  defp drop_lookup_index, do: repo().query!("DROP INDEX CONCURRENTLY IF EXISTS public.#{@name}", [], log: false, timeout: :infinity)
end
