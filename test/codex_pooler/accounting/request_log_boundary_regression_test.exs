defmodule CodexPooler.Accounting.RequestLogBoundaryRegressionTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting.RequestLogs
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.{Audit, Repo, Upstreams}

  setup do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    scope = Scope.for_user(owner, ["instance_owner"])
    %{pool: pool, api_key: key} = active_api_key_fixture()
    request = request_fixture(%{pool: pool, api_key: key}, %{requested_model: "sample-model"})
    %{scope: scope, pool: pool, request: request}
  end

  test "malformed detail IDs fail closed while a valid visible request is returned", %{scope: scope, request: request} do
    for id <- ["not-a-uuid", "", nil, 123, %{}] do
      assert RequestLogs.get_for_scope(scope, id) == nil
    end

    assert RequestLogs.get_for_scope(scope, request.id).id == request.id
  end

  test "malformed selected and visible Pool IDs never broaden model history", %{pool: pool} do
    for invalid <- ["not-a-uuid", 123, %{}, %{id: nil}] do
      assert RequestLogs.list_models(invalid) == []
    end

    assert RequestLogs.list_models("not-a-uuid", visible_pool_ids: [pool.id]) == []
    assert RequestLogs.list_models(nil, visible_pool_ids: ["not-a-uuid"]) == []
    assert RequestLogs.list_models(nil, visible_pool_ids: [pool.id, "not-a-uuid"]) == ["sample-model"]
    assert RequestLogs.list_models(pool, visible_pool_ids: []) == []
    assert RequestLogs.list_models(pool) == ["sample-model"]
  end

  test "an actual account rename records an action supported by audit filters", %{scope: scope, pool: pool} do
    %{identity: identity} = upstream_assignment_fixture(pool)
    assert {:ok, _} = Upstreams.rename_account_for_scope(scope, identity, %{account_label: "Renamed sample"})
    event = Repo.get_by!(Audit.AuditEvent, action: "upstream_account.rename", target_id: identity.id)
    assert event.action in Audit.supported_actions()
    assert Audit.action_label(event.action) != nil
    assert Enum.any?(Audit.list_events_for_scope(scope, filters: [action: event.action]).items, &(&1.id == event.id))
  end

  for reader <- [:requests, :audit] do
    test "#{reader} counts clamp nonpositive limits without raising", %{pool: pool} do
      assert {:ok, _} = Audit.record_event(%{pool_id: pool.id, actor_type: "system", action: "pool.update", target_type: "pool", target_id: pool.id})

      for value <- [0, -1] do
        page = list(unquote(reader), pool, count_limit: value)
        assert page.total == 0
        assert page.total_exact? == false
        assert page.items != []
      end
    end

    test "#{reader} invalid count types retain the omitted-limit behavior", %{pool: pool} do
      assert {:ok, _} = Audit.record_event(%{pool_id: pool.id, actor_type: "system", action: "pool.update", target_type: "pool", target_id: pool.id})
      expected = list(unquote(reader), pool, [])

      for value <- ["invalid", 1.5, %{}] do
        page = list(unquote(reader), pool, count_limit: value)
        assert page.total == expected.total
        assert page.total_exact? == true
      end
    end
  end

  defp list(:requests, pool, opts), do: RequestLogs.list(pool, opts)
  defp list(:audit, pool, opts), do: Audit.list_events(pool, opts)
end
