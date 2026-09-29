defmodule CodexPooler.Accounting.DeletedKeyUsageDerivationTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  test "deleting a key bypasses event derivation for its discarded usage history" do
    setup = active_api_key_fixture()
    seed!(setup, 100, 10)
    for _ <- 1..3, do: seed!(active_api_key_fixture(setup.pool), 100, 10)
    CodexPooler.PlannerStatistics.analyze!(["api_keys", "ledger_entries", "requests"])
    Repo.query!("SET LOCAL track_functions = 'all'")
    before_calls = event_calls()
    Repo.query!("DELETE FROM api_keys WHERE id=$1", [Ecto.UUID.dump!(setup.api_key.id)])
    assert event_calls() == before_calls
    assert [[0]] = Repo.query!("SELECT count(*) FROM ledger_entries WHERE api_key_id=$1", [Ecto.UUID.dump!(setup.api_key.id)]).rows
    assert [[0]] = Repo.query!("SELECT count(*) FROM api_key_usage_buckets WHERE api_key_id=$1", [Ecto.UUID.dump!(setup.api_key.id)]).rows
  end

  test "a live-key batched detach still reverses its usage contribution" do
    setup = active_api_key_fixture()
    seed!(setup, 100, 10)
    Repo.query!("SET LOCAL track_functions = 'all'")
    before_calls = event_calls()
    assert [[100]] = Repo.query!("SELECT sum(admission_count)::bigint FROM api_key_usage_buckets WHERE api_key_id=$1", [Ecto.UUID.dump!(setup.api_key.id)]).rows
    assert %{num_rows: 1_000} = Repo.query!("UPDATE ledger_entries SET api_key_id=NULL WHERE id IN (SELECT id FROM ledger_entries WHERE api_key_id=$1 LIMIT 1000)", [Ecto.UUID.dump!(setup.api_key.id)])
    assert event_calls() > before_calls
    assert [[0]] = Repo.query!("SELECT sum(admission_count)::bigint FROM api_key_usage_buckets WHERE api_key_id=$1", [Ecto.UUID.dump!(setup.api_key.id)]).rows
    assert [[0]] = Repo.query!("SELECT sum(effective_total_tokens)::bigint FROM api_key_usage_buckets WHERE api_key_id=$1", [Ecto.UUID.dump!(setup.api_key.id)]).rows
  end

  defp event_calls do
    [[calls]] = Repo.query!("SELECT coalesce(sum(calls),0)::bigint FROM pg_stat_xact_user_functions WHERE funcid='public.api_key_usage_events(public.ledger_entries[])'::regprocedure").rows
    calls
  end

  defp seed!(setup, requests, entries) do
    Repo.query!(
      """
      WITH new_requests AS (
        INSERT INTO requests(id,pool_id,requested_model,endpoint,transport,status,usage_status,correlation_id,admitted_at)
        SELECT gen_random_uuid(), $1::uuid, 'sample-model', '/backend-api/codex/responses', 'http_json', 'accepted', 'usage_pending', gen_random_uuid()::text, now()
        FROM generate_series(1,$2) RETURNING id,pool_id
      )
      INSERT INTO ledger_entries(request_id,pool_id,api_key_id,entry_kind,amount_status,usage_status,transport,request_count,total_tokens,occurred_at,created_at)
      SELECT r.id,r.pool_id,$3::uuid,'reservation','recorded','usage_pending','http_json',1,100,now(),now()
      FROM new_requests r CROSS JOIN generate_series(1,$4)
      """,
      [Ecto.UUID.dump!(setup.pool.id), requests, Ecto.UUID.dump!(setup.api_key.id), entries]
    )
  end
end
