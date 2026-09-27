defmodule CodexPooler.Jobs.AccountingCompletionTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.{DailyRollupCoverage, Reporting, Rollups}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Jobs.DailyRollupRebuildWorker
  alias CodexPoolerWeb.Admin.UpstreamCockpitReadModel

  test "malformed rollup dates cancel instead of raising or retrying" do
    for args <- [%{}, %{"rollup_date" => nil}, %{"rollup_date" => "not-a-date"}, %{"rollup_date" => "2026-02-30"}] do
      assert {:cancel, :invalid_rollup_date} = perform_job(DailyRollupRebuildWorker, args)
    end
  end

  test "coverage publication reads the database clock after rebuild work" do
    date = ~D[2026-01-01]
    parent = self()

    assert {:ok, _} =
             Rollups.rebuild_for_date(date,
               before_coverage: fn ->
                 %{rows: [[published_after]]} = Repo.query!("SELECT clock_timestamp() AT TIME ZONE 'UTC'")
                 send(parent, {:database_publication_boundary, published_after})
               end
             )

    assert_receive {:database_publication_boundary, published_after}
    coverage = Repo.get!(DailyRollupCoverage, date)
    assert NaiveDateTime.compare(DateTime.to_naive(coverage.completed_at), published_after) in [:eq, :gt]
  end

  test "covered usage binds the runtime coverage contract version in its actual query" do
    pool = pool_fixture()
    dates = Enum.to_list(Date.range(~D[2026-01-01], ~D[2026-01-06]))
    for date <- dates, do: assert({:ok, _} = Rollups.rebuild_for_date(date))
    queries = capture_queries(fn -> assert {:ok, _} = Reporting.covered_pool_daily_usage_snapshot([pool.id], dates) end)
    assert Enum.any?(queries, fn {sql, params} -> sql =~ "WITH requested_pools" and DailyRollupCoverage.contract_version() in params end)
  end

  test "cockpit history counts stop at the displayed prefetch bound" do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    scope = Scope.for_user(owner)
    %{identity: identity} = upstream_assignment_fixture(pool_fixture())
    queries = capture_queries(fn -> assert {:ok, _} = UpstreamCockpitReadModel.load_visible(scope, identity.id) end)
    counts = Enum.filter(queries, fn {sql, _} -> sql =~ "count(" and sql =~ ~s("audit_events") end)
    assert counts != []
    for {sql, _params} <- counts, do: assert(sql =~ "LIMIT")
  end

  defp capture_queries(fun) do
    ref = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(ref) end)
    :telemetry.attach(ref, [:codex_pooler, :repo, :query], &__MODULE__.query/4, self())
    fun.()
    :telemetry.detach(ref)
    collect([])
  end

  def query(_event, _measurements, metadata, owner) do
    if self() == owner, do: send(owner, {:query, metadata.query, metadata.params})
  end

  defp collect(acc) do
    receive do
      {:query, sql, params} -> collect([{sql, params} | acc])
    after
      0 -> acc
    end
  end
end
