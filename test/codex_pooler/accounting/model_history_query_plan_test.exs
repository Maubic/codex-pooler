defmodule CodexPooler.Accounting.ModelHistoryQueryPlanTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.PoolerFixtures
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.Accounting.RequestLogs.ModelHistory
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL

  test "the bounded history read can start at the attempt time index instead of scanning retained history" do
    setup = active_api_key_fixture()
    %{assignment: assignment} = upstream_assignment_fixture(setup.pool)
    request = request_fixture(setup)
    now = DateTime.utc_now()
    old = DateTime.add(now, -30, :day)

    rows =
      for number <- 1..5000 do
        %{id: Ecto.UUID.generate(), request_id: request.id, attempt_number: number, pool_upstream_assignment_id: assignment.id, upstream_identity_id: assignment.upstream_identity_id, started_at: if(number >= 4999, do: DateTime.add(now, -1, :second), else: old), status: if(number == 4999, do: "in_progress", else: "succeeded"), transport: "http_json", upstream_model_id: "model-a"}
      end

    Repo.insert_all(Attempt, rows)
    Repo.query!("ANALYZE attempts")
    query = ModelHistory.query([setup.pool.id], DateTime.add(now, -3600, :second), now, ModelHistory.normalize_filters(%{}))
    {sql, params} = SQL.to_sql(:all, Repo, query)

    for mode <- ["force_custom_plan", "force_generic_plan"] do
      name = "model_history_plan_#{System.unique_integer([:positive])}"
      Repo.query!("SET LOCAL plan_cache_mode = '#{mode}'")
      Repo.query!(sql, params, cache_statement: name)
      values = Enum.map_join(params, ", ", &sql_literal/1)
      %{rows: [[plan]]} = Repo.query!("EXPLAIN (ANALYZE, FORMAT JSON) EXECUTE #{name}(#{values})")
      assert inspect(plan) =~ "attempts_model_history_started_idx", "#{mode}: #{inspect(plan)}"
      %{rows: [[generic, custom]]} = Repo.query!("SELECT generic_plans, custom_plans FROM pg_prepared_statements WHERE name = $1", [name])
      assert {generic, custom} == if(mode == "force_generic_plan", do: {2, 0}, else: {0, 2})
      Repo.query!("DEALLOCATE #{name}")
    end

    assert Repo.all(query) |> Enum.map(&{&1.attempt_number, &1.status}) |> Enum.sort() == [{4999, "in_progress"}, {5000, "succeeded"}]
  end

  defp sql_literal(%DateTime{} = value), do: "TIMESTAMPTZ '#{DateTime.to_iso8601(value)}'"
  defp sql_literal([<<_::128>> | _] = value), do: "ARRAY[#{Enum.map_join(value, ", ", &sql_literal/1)}]::uuid[]"
  defp sql_literal(<<_::128>> = value), do: "'#{Ecto.UUID.load!(value)}'"
  defp sql_literal(value) when is_binary(value), do: "'#{String.replace(value, "'", "''")}'"
end
