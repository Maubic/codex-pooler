defmodule CodexPooler.Upstreams.PermanentDeletionAssignmentTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Admin.PoolWorkflow
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment

  for operation <- [:create, :activate] do
    test "#{operation} rejects stale references to a permanently deleting identity" do
      pool = pool_fixture()
      %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool, %{assignment_status: "deleted"})
      mark_deleting!(identity)

      result =
        case unquote(operation) do
          :create -> PoolAssignments.create_pool_assignment(pool_fixture(), identity)
          :activate -> PoolAssignments.activate_pool_assignment(assignment)
        end

      assert {:error, %{code: :upstream_account_deleting}} = result
      assert Repo.reload!(assignment) == assignment
      assert Repo.aggregate(from(row in PoolUpstreamAssignment, where: row.upstream_identity_id == ^identity.id), :count) == 1
    end
  end

  for selection <- ["upstream_identity_ids", "upstream_assignment_ids"] do
    test "Pool workflow rejects stale #{selection} and rolls back settings" do
      scope = Scope.for_user(bootstrap_owner_fixture().user, ["instance_owner"])
      pool = pool_fixture()
      %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool, %{assignment_status: "deleted"})
      mark_deleting!(identity)
      selected = if unquote(selection) == "upstream_identity_ids", do: identity.id, else: assignment.id

      assert {:error, %{code: :upstream_account_deleting}} =
               PoolWorkflow.update_pool_with_related_settings(scope, pool, %{"name" => "Rejected stale selection", "status" => "active", unquote(selection) => [selected]})

      assert Repo.get!(Pool, pool.id) == pool
      assert Repo.reload!(assignment) == assignment
    end
  end

  test "legacy soft deleted identities without a permanent marker remain attachable" do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool_fixture(), %{identity_status: "deleted", assignment_status: "deleted"})
    assert {:ok, created} = PoolAssignments.create_pool_assignment(pool_fixture(), identity)
    assert created.upstream_identity_id == identity.id
    assert {:ok, activated} = PoolAssignments.activate_pool_assignment(assignment)
    assert activated.status == "active"
  end

  defp mark_deleting!(identity) do
    identity |> change(status: "deleted", metadata: Map.put(identity.metadata || %{}, "permanent_deletion_requested_at", DateTime.to_iso8601(DateTime.utc_now()))) |> Repo.update!()
  end
end
