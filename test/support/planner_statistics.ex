defmodule CodexPooler.PlannerStatistics do
  @moduledoc """
  ANALYZE for tests, kept inside the test's transaction.

  ANALYZE writes the row and page counts of a table and of its indexes into `pg_class` in place. In
  a sandbox transaction that rolls back they survive the rollback, and every later test of the
  partition plans with them: a later plan test met counts of a few rows over a heap the vacuum had
  truncated, and read the whole heap for every lookup (findings#270 rows 270-321 and 270-336).

  `analyze!/1` writes those `pg_class` rows in the transaction first, so the ANALYZE writes over
  the transaction's own row versions and the rollback takes its counts away. The stats functions
  are transactional but skip a row whose values would not change, so it writes two different
  states in turn. It then checks, from a connection outside the sandbox, that the committed counts
  did not move. The tables are locked first in the mode ANALYZE takes anyway, so an autovacuum
  worker cannot hold a table's lock this ANALYZE waits on while it waits on those rows.
  """

  import ExUnit.Assertions

  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  @spec analyze!([String.t(), ...]) :: :ok
  def analyze!([_ | _] = tables) do
    names = Enum.join(tables, ", ")
    Repo.query!("LOCK TABLE #{names} IN SHARE UPDATE EXCLUSIVE MODE")
    relations = relations(tables)

    for relation <- relations do
      Repo.query!("SELECT pg_restore_relation_stats('schemaname', 'public', 'relname', $1::text, 'reltuples', 1::real, 'relpages', 1::integer, 'relallvisible', 0::integer)", [relation])
      Repo.query!("SELECT pg_clear_relation_stats('public', $1)", [relation])
    end

    committed = committed_counts(relations)

    Repo.query!("ANALYZE #{names}", [], timeout: 60_000)

    assert committed_counts(relations) == committed, "ANALYZE #{names} changed pg_class counts outside the test's transaction"
    :ok
  end

  defp relations(tables) do
    %{rows: rows} = Repo.query!("SELECT indexrelid::regclass::text FROM pg_index WHERE indrelid = ANY($1::text[]::regclass[])", [Enum.map(tables, &("public." <> &1))])
    tables ++ Enum.map(rows, fn [index] -> String.replace_prefix(index, "public.", "") end)
  end

  defp committed_counts(relations) do
    Sandbox.unboxed_run(Repo, fn ->
      Repo.query!("SELECT relname, reltuples, relpages, relallvisible, relallfrozen FROM pg_class WHERE relname = ANY($1) AND relnamespace = 'public'::regnamespace ORDER BY relname", [relations]).rows
    end)
  end
end
