defmodule CodexPooler.Repo.Migrations.ValidateCodexSessionCloseReason do
  use Ecto.Migration

  alias CodexPooler.Release.MigrationLockBudget

  @disable_ddl_transaction true

  def up do
    execute(fn ->
      MigrationLockBudget.run(repo(), &validate/0, lock_wait_ms: 5_000)
    end)
  end

  def down, do: :ok

  defp validate do
    {:ok, _result} =
      repo().transaction(
        fn ->
          repo().query!("SET LOCAL statement_timeout = '30min'", [], log: false)
          repo().query!("ALTER TABLE public.codex_sessions VALIDATE CONSTRAINT codex_sessions_close_reason_check", [], log: false, timeout: :infinity)
        end,
        timeout: :infinity
      )
  end
end
