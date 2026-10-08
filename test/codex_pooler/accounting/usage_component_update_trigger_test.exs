defmodule CodexPooler.Accounting.UsageComponentUpdateTriggerTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.LedgerEntry

  test "reference-only and empty updates leave bucket tuple versions untouched" do
    setup = accounting_setup()
    entry = entry!(setup)
    before = buckets()

    Repo.update_all(from(e in LedgerEntry, where: e.id == ^entry.id), set: [pool_upstream_assignment_id: nil, upstream_identity_id: nil, input_tokens: 8, output_tokens: 2])
    assert buckets() == before

    Repo.update_all(from(e in LedgerEntry, where: e.id == ^entry.id and false), set: [total_tokens: 99])
    assert buckets() == before
  end

  test "mixed reference and accounting updates preserve usage and cost corrections" do
    setup = accounting_setup()
    first = entry!(setup)
    second = entry!(setup)
    before = buckets()

    Repo.query!("UPDATE ledger_entries SET upstream_identity_id = NULL, total_tokens = CASE WHEN id = $1::text::uuid THEN 23 ELSE total_tokens END, settled_cost_micros = CASE WHEN id = $1::text::uuid THEN 17 ELSE settled_cost_micros END WHERE id = ANY($2::text[]::uuid[])", [first.id, [first.id, second.id]])

    refute buckets() == before
    assert totals(setup.api_key.id) == [33, "30", 33, "30"]

    first |> Ecto.Changeset.change(usage_status: "usage_unknown") |> Repo.update!()
    assert totals(setup.api_key.id) == [10, "13", 10, "13"]
  end

  test "key moves and live-key detachment reverse the old contribution" do
    setup = accounting_setup()
    other = accounting_setup()
    entry = entry!(setup)
    entry = entry |> Ecto.Changeset.change(api_key_id: other.api_key.id) |> Repo.update!()
    assert totals(setup.api_key.id) == [0, "0", 0, "0"]
    assert totals(other.api_key.id) == [10, "13", 10, "13"]

    entry |> Ecto.Changeset.change(api_key_id: nil) |> Repo.update!()
    assert totals(other.api_key.id) == [0, "0", 0, "0"]
  end

  test "every event and legacy projection dependency still runs the real bucket trigger" do
    setup = accounting_setup()
    other = accounting_setup()
    request = request_fixture(setup.auth, %{model_id: setup.model.id})
    attempt = attempt_fixture(request, setup.assignment)

    mutations = [
      id: Ecto.UUID.generate(),
      request_id: request.id,
      api_key_id: other.api_key.id,
      attempt_id: attempt.id,
      entry_kind: "release",
      amount_status: "voided",
      usage_status: "usage_unknown",
      total_tokens: 24,
      request_count: 2,
      estimated_cost_micros: Decimal.new(9),
      settled_cost_micros: Decimal.new(21),
      occurred_at: ~U[2026-08-02 00:01:00.000000Z],
      created_at: ~U[2026-08-02 00:00:01.000000Z],
      details: %{"estimated_from_reserve" => true}
    ]

    for {field, value} <- mutations do
      entry = entry!(setup)
      before = buckets()
      entry |> Ecto.Changeset.change(%{field => value}) |> Repo.update!()
      refute buckets() == before, "projection dependency #{field} skipped bucket maintenance"
    end
  end

  defp entry!(setup) do
    request = request_fixture(setup.auth, %{model_id: setup.model.id})

    Repo.insert!(%LedgerEntry{
      request_id: request.id,
      api_key_id: request.api_key_id,
      pool_id: request.pool_id,
      pool_upstream_assignment_id: setup.assignment.id,
      upstream_identity_id: setup.assignment.upstream_identity_id,
      model_id: setup.model.id,
      pricing_snapshot_id: setup.pricing.id,
      entry_kind: "settlement",
      amount_status: "recorded",
      usage_status: "usage_known",
      transport: request.transport,
      currency_code: "USD",
      total_tokens: 10,
      request_count: 1,
      estimated_cost_micros: Decimal.new(5),
      settled_cost_micros: Decimal.new(13),
      source_event_id: "trigger-#{Ecto.UUID.generate()}",
      occurred_at: ~U[2026-08-02 00:00:00.000000Z],
      created_at: ~U[2026-08-02 00:00:00.000000Z],
      details: %{}
    })
  end

  defp buckets do
    Repo.query!("SELECT ctid::text, to_jsonb(b) FROM api_key_usage_buckets b ORDER BY api_key_id, bucket_started_at").rows
  end

  defp totals(key_id) do
    [row] = Repo.query!("SELECT SUM(effective_total_tokens)::bigint, SUM(effective_cost_micros), SUM(known_total_tokens)::bigint, SUM(known_cost_micros) FROM api_key_usage_buckets WHERE api_key_id = $1::text::uuid", [key_id]).rows

    Enum.map(row, fn
      %Decimal{} = value -> value |> Decimal.normalize() |> Decimal.to_string(:normal)
      value -> value
    end)
  end
end
