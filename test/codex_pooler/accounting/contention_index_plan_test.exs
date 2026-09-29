defmodule CodexPooler.Accounting.ContentionIndexPlanTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Repo

  @cost_index "ledger_entries_api_key_known_settlement_occurred_idx"
  @attempt_index "attempts_open_started_idx"

  setup do
    as_of = ~U[2026-09-17 12:00:00.000000Z]
    pool = pool_fixture()
    %{api_key: api_key} = active_api_key_fixture(pool)
    %{assignment: assignment} = upstream_assignment_fixture(pool)

    for ordinal <- 1..20 do
      request = request_fixture(%{pool: pool, api_key: api_key})

      ledger_entry_fixture(request, %{
        occurred_at: DateTime.add(as_of, -ordinal * 3_600, :second),
        settled_cost_micros: ordinal,
        details: %{"settled_cost_micros" => Integer.to_string(ordinal)}
      })

      terminal_request = request_fixture(%{pool: pool, api_key: api_key}, %{status: "succeeded"})

      terminal_request
      |> attempt_fixture(assignment, %{status: "in_progress", completed_at: nil})
      |> Ecto.Changeset.change(%{started_at: DateTime.add(as_of, -ordinal * 3_600, :second)})
      |> Repo.update!()
    end

    CodexPooler.PlannerStatistics.analyze!(["ledger_entries", "requests", "attempts"])
    %{as_of: as_of, pool: pool, api_key: api_key}
  end

  test "the emitted cost summary can use the known-settlement index in a generic plan", %{
    as_of: as_of,
    pool: pool,
    api_key: api_key
  } do
    {result, queries} =
      capture_repo_queries(fn ->
        Accounting.build_api_key_self_usage(pool, api_key, as_of: as_of)
      end)

    assert {:ok, _usage} = result
    sql = single_query!(queries, &cost_summary_query?/1, "cost summary")
    assert String.contains?(sql, "count(")
    assert String.contains?(sql, "sum(")
    assert String.contains?(sql, ~s("entry_kind" = 'settlement'))
    assert String.contains?(sql, ~s("usage_status" = 'usage_known'))
    assert uses_index?(generic_plan!(sql), @cost_index)
  end

  test "the emitted stale-attempt query can use the open-attempt index in a generic plan", %{
    as_of: as_of
  } do
    {result, queries} =
      capture_repo_queries(fn -> Accounting.recover_stale_reservations(as_of) end)

    assert {:ok, _summary} = result
    sql = single_query!(queries, &stale_attempt_query?/1, "stale terminal-attempt query")
    assert String.contains?(sql, ~s|"status" IN ('queued','in_progress')|)
    assert uses_index?(generic_plan!(sql), @attempt_index)
  end

  defp generic_plan!(sql) do
    Repo.query!("SET LOCAL enable_seqscan = off")

    %{rows: [[document]]} =
      Repo.query!("EXPLAIN (GENERIC_PLAN, FORMAT JSON) " <> sql, [], query_type: :text)

    [%{"Plan" => plan}] = CodexPooler.JSON.decode!(document)
    plan
  after
    Repo.query!("SET LOCAL enable_seqscan = on")
  end

  defp capture_repo_queries(fun) do
    handler = {__MODULE__, self(), make_ref()}
    parent = self()

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if metadata[:repo] == Repo, do: send(parent, {handler, metadata.query})
        end,
        nil
      )

    try do
      {fun.(), drain_queries(handler, [])}
    after
      :telemetry.detach(handler)
    end
  end

  defp drain_queries(handler, queries) do
    receive do
      {^handler, query} -> drain_queries(handler, [query | queries])
    after
      0 -> Enum.reverse(queries)
    end
  end

  defp single_query!(queries, matcher, label) do
    case queries |> Enum.filter(matcher) |> Enum.uniq() do
      [sql] -> sql
      matches -> flunk("expected one #{label}, got #{length(matches)}")
    end
  end

  defp cost_summary_query?(sql) do
    String.contains?(sql, ~s(FROM "ledger_entries")) and
      String.contains?(sql, "settled_cost_micros") and String.contains?(sql, "usage_status")
  end

  defp stale_attempt_query?(sql) do
    String.contains?(sql, ~s(FROM "attempts")) and String.contains?(sql, "ORDER BY") and
      String.contains?(sql, "started_at") and String.contains?(sql, ~s(JOIN "requests"))
  end

  defp uses_index?(node, index_name) do
    node["Index Name"] == index_name or
      Enum.any?(Map.get(node, "Plans", []), &uses_index?(&1, index_name))
  end
end
