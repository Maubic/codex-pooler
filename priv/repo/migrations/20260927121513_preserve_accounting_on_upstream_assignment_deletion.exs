defmodule CodexPooler.Repo.Migrations.PreserveAccountingOnUpstreamAssignmentDeletion do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true

  def up do
    execute(fn ->
      catalog_change(fn ->
        repo().query!("ALTER TABLE public.attempts ALTER COLUMN pool_upstream_assignment_id DROP NOT NULL")
        replace_assignment_constraint("attempts", "SET NULL")
        replace_assignment_constraint("codex_sessions", "SET NULL")
      end)

      validate_assignment_constraint("attempts")
      validate_assignment_constraint("codex_sessions")
    end)
  end

  def down do
    execute(fn ->
      # Rollback cannot restore required references after an upstream purge.
      # Validate first and fail without deleting retained history.
      catalog_change(fn ->
        repo().query!("ALTER TABLE public.attempts DROP CONSTRAINT IF EXISTS attempts_assignment_required_for_rollback")
        repo().query!("ALTER TABLE public.attempts ADD CONSTRAINT attempts_assignment_required_for_rollback CHECK (pool_upstream_assignment_id IS NOT NULL) NOT VALID")
      end)

      try do
        validate_constraint("attempts", "attempts_assignment_required_for_rollback")
      rescue
        error ->
          catalog_change(fn ->
            repo().query!("ALTER TABLE public.attempts DROP CONSTRAINT attempts_assignment_required_for_rollback")
          end)

          reraise error, __STACKTRACE__
      end

      catalog_change(fn ->
        repo().query!("ALTER TABLE public.attempts ALTER COLUMN pool_upstream_assignment_id SET NOT NULL")
        repo().query!("ALTER TABLE public.attempts DROP CONSTRAINT attempts_assignment_required_for_rollback")
        replace_assignment_constraint("attempts", "CASCADE")
        replace_assignment_constraint("codex_sessions", "CASCADE")
      end)

      validate_assignment_constraint("attempts")
      validate_assignment_constraint("codex_sessions")
    end)
  end

  defp replace_assignment_constraint(table, action) do
    name = "#{table}_pool_upstream_assignment_id_fkey"
    repo().query!("ALTER TABLE public.#{table} DROP CONSTRAINT #{name}")
    repo().query!("ALTER TABLE public.#{table} ADD CONSTRAINT #{name} FOREIGN KEY (pool_upstream_assignment_id) REFERENCES public.pool_upstream_assignments(id) ON DELETE #{action} NOT VALID")
  end

  defp validate_assignment_constraint(table),
    do: validate_constraint(table, "#{table}_pool_upstream_assignment_id_fkey")

  defp validate_constraint(table, name) do
    MigrationLockBudget.run(repo(), fn ->
      repo().query!("ALTER TABLE public.#{table} VALIDATE CONSTRAINT #{name}", [], timeout: :infinity)
    end)
  end

  defp catalog_change(fun) do
    {:ok, _} =
      repo().transaction(fn ->
        repo().query!("SET LOCAL lock_timeout = '10s'")
        fun.()
      end)
  end
end
