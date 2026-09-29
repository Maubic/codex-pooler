defmodule CodexPooler.Accounting.ExecutionRecoveryQueryPlanTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Accounting.RequestLifecycle.FailedPredecessorResend
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo

  @index "codex_turns_semantic_history_idx"
  @history_size 10_000

  test "actual cross-session predecessor lookup reads only matching semantic history" do
    # Isolate planner cost from dead tuples left by earlier rolled-back tests;
    # this reset is itself sandboxed and never touches another test database.
    Repo.query!("TRUNCATE codex_turns, requests CASCADE")
    setup = active_api_key_fixture()
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    base = request_fixture(setup, %{status: "succeeded", admitted_at: DateTime.add(now, -30, :day)})
    session = Repo.insert!(%CodexSession{pool_id: setup.pool.id, api_key_id: setup.api_key.id, session_key: Ecto.UUID.generate(), status: "active", created_at: now, updated_at: now})
    template = base |> Map.from_struct() |> Map.delete(:__meta__)
    digest = :crypto.hash(:sha256, "target semantic turn")

    requests =
      for ordinal <- 1..@history_size do
        Map.merge(template, %{id: Ecto.UUID.generate(), correlation_id: "semantic-history-#{ordinal}", admitted_at: DateTime.add(now, -@history_size + ordinal, :second)})
      end

    Enum.each(Enum.chunk_every(requests, 1_000), &Repo.insert_all(Request, &1))

    turns =
      for {request, ordinal} <- Enum.with_index(requests, 1) do
        %{id: Ecto.UUID.generate(), codex_session_id: session.id, request_id: request.id, turn_sequence: ordinal, transport_kind: "websocket", semantic_turn_digest: if(ordinal >= @history_size - 1, do: digest, else: :crypto.hash(:sha256, Integer.to_string(ordinal))), status: "succeeded", started_at: request.admitted_at, completed_at: now, created_at: now, updated_at: now}
      end

    Enum.each(Enum.chunk_every(turns, 1_000), &Repo.insert_all(CodexTurn, &1))
    CodexPooler.PlannerStatistics.analyze!(["codex_turns", "requests"])

    {sql, params} = capture_lookup!(%{pool_id: setup.pool.id, api_key_id: setup.api_key.id, semantic_turn_digest: digest})
    %{rows: [row]} = Repo.query!(sql, params)
    assert Ecto.UUID.dump!(List.last(requests).id) in row
    %{rows: [[document]]} = Repo.query!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " <> sql, params)
    [%{"Plan" => plan}] = if is_binary(document), do: CodexPooler.JSON.decode!(document), else: document
    nodes = flatten(plan)
    assert Enum.any?(nodes, &(&1["Index Name"] == @index))
    turn_scans = Enum.filter(nodes, &(&1["Relation Name"] == "codex_turns"))
    assert turn_scans != []
    refute Enum.any?(turn_scans, &(&1["Node Type"] in ["Seq Scan", "Parallel Seq Scan"]))
    scanned = Enum.sum(Enum.map(turn_scans, &((&1["Actual Rows"] + Map.get(&1, "Rows Removed by Filter", 0)) * &1["Actual Loops"])))
    assert scanned <= 2
    assert plan["Actual Rows"] == 1
    CodexPooler.TestDiagnostics.puts("execution predecessor query: history=#{@history_size}; index=#{@index}; matching turn rows scanned=#{scanned}; result rows=#{plan["Actual Rows"]}")
  end

  defp capture_lookup!(scope) do
    handler = {__MODULE__, make_ref()}
    owner = self()
    on_exit(fn -> :telemetry.detach(handler) end)

    :ok =
      :telemetry.attach(
        handler,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if self() == owner and metadata[:repo] == Repo and String.contains?(metadata.query, ~s(FROM "codex_turns")) and String.contains?(metadata.query, ~s(JOIN "requests")) do
            send(owner, {handler, metadata.query, metadata.params})
          end
        end,
        nil
      )

    assert FailedPredecessorResend.recoverable_predecessor(scope) == nil
    assert_receive {^handler, sql, params}
    :telemetry.detach(handler)
    {sql, params}
  end

  defp flatten(node), do: [node | Enum.flat_map(Map.get(node, "Plans", []), &flatten/1)]
end
