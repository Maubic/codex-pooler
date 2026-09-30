defmodule CodexPooler.Repo.Migrations.AddRequestAdmissionExecutionIndexes do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true

  @indexes [
    %{
      name: "requests_open_admission_execution_idx",
      create: """
      CREATE INDEX CONCURRENTLY requests_open_admission_execution_idx
      ON public.requests (admission_execution_id, id)
      WHERE status IN ('accepted', 'in_progress') AND admission_execution_id IS NOT NULL
      """,
      definition: "CREATE INDEX requests_open_admission_execution_idx ON public.requests USING btree (admission_execution_id, id) WHERE ((status = ANY (ARRAY['accepted'::text, 'in_progress'::text])) AND (admission_execution_id IS NOT NULL))"
    },
    %{
      name: "requests_open_admission_checked_idx",
      create: """
      CREATE INDEX CONCURRENTLY requests_open_admission_checked_idx
      ON public.requests (COALESCE(admission_execution_checked_at, admitted_at), id)
      WHERE status IN ('accepted', 'in_progress') AND admission_execution_id IS NOT NULL
      """,
      definition: "CREATE INDEX requests_open_admission_checked_idx ON public.requests USING btree (COALESCE(admission_execution_checked_at, admitted_at), id) WHERE ((status = ANY (ARRAY['accepted'::text, 'in_progress'::text])) AND (admission_execution_id IS NOT NULL))"
    }
  ]

  def change do
    execute(
      fn -> with_lock_budget(fn -> Enum.each(@indexes, &converge_index/1) end) end,
      fn -> with_lock_budget(fn -> Enum.each(Enum.reverse(@indexes), &drop_index/1) end) end
    )
  end

  defp converge_index(%{name: name, create: create, definition: definition} = index) do
    case index_state(name) do
      [[true, true, ^definition]] ->
        :ok

      [] ->
        repo().query!(create, [], log: false, timeout: :infinity)
        [[true, true, ^definition]] = index_state(name)
        :ok

      [[_valid, _ready, ^definition]] ->
        drop_index(index)
        converge_index(index)

      _conflicting ->
        raise "conflicting index: #{name}"
    end
  end

  defp index_state(name) do
    repo().query!(
      """
      SELECT i.indisvalid,i.indisready,pg_get_indexdef(c.oid)
      FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
      WHERE c.oid=to_regclass('public.#{name}')
      """,
      [],
      log: false
    ).rows
  end

  defp drop_index(%{name: name}) do
    repo().query!("DROP INDEX CONCURRENTLY IF EXISTS public.#{name}", [], log: false, timeout: :infinity)
  end

  defp with_lock_budget(fun), do: MigrationLockBudget.run(repo(), fun)
end
