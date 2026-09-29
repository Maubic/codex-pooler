defmodule CodexPooler.Repo.Migrations.AllowEnforcedUltrafastServiceTier do
  use Ecto.Migration

  @disable_ddl_transaction true
  @constraint "api_keys_enforced_service_tier_check"

  def up, do: replace_check("'auto', 'default', 'flex', 'priority', 'scale', 'ultrafast'")

  # No policy is silently cleared on rollback. Validation refuses a downgrade
  # while any key still enforces the newly supported tier.
  def down do
    execute(fn ->
      {:ok, _} = repo().transaction(&ensure_no_ultrafast_policies!/0)
    end)

    replace_check("'auto', 'default', 'flex', 'priority', 'scale'")
  end

  defp ensure_no_ultrafast_policies! do
    repo().query!("SET LOCAL statement_timeout = '60s'", [], log: false)
    %{rows: [[used?]]} = repo().query!("SELECT EXISTS (SELECT 1 FROM public.api_keys WHERE enforced_service_tier = 'ultrafast')", [], log: false)
    if used?, do: raise("change enforced ultrafast policies before rolling back this migration")
  end

  defp replace_check(tiers) do
    execute(fn ->
      {:ok, _} =
        repo().transaction(fn ->
          repo().query!("SET LOCAL lock_timeout = '10s'", [], log: false)
          repo().query!("SET LOCAL statement_timeout = '60s'", [], log: false)

          repo().query!(
            """
            ALTER TABLE public.api_keys
              DROP CONSTRAINT IF EXISTS #{@constraint},
              ADD CONSTRAINT #{@constraint}
                CHECK (enforced_service_tier IS NULL OR enforced_service_tier IN (#{tiers})) NOT VALID
            """,
            [],
            log: false
          )
        end)
    end)

    # Release the exclusive catalog lock before scanning existing rows.
    # Repeating the swap after interruption preserves checks on new writes.
    execute(fn ->
      {:ok, _} =
        repo().transaction(
          fn ->
            repo().query!("SET LOCAL lock_timeout = '10s'", [], log: false)
            repo().query!("SET LOCAL statement_timeout = '60s'", [], log: false)
            repo().query!("ALTER TABLE public.api_keys VALIDATE CONSTRAINT #{@constraint}", [], log: false, timeout: 65_000)
          end,
          timeout: 65_000
        )
    end)
  end
end
