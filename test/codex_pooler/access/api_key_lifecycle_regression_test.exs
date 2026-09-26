defmodule CodexPooler.Access.APIKeyLifecycleRegressionTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import CodexPooler.RequestReplayFixtures

  alias CodexPooler.Access
  alias CodexPooler.Access.{APIKey, APIKeyPolicyBinding}
  alias CodexPooler.Accounting.RequestReplay
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Jobs.APIKeyDeletionWorker
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv

  for closed? <- [false, true] do
    test "scheduled deletion removes #{if closed?, do: "closed", else: "armed"} replay entitlements before detaching immutable request snapshots" do
      TestAppEnv.restore_on_exit(:api_key_deletion_immediate_request_limit)
      Application.put_env(:codex_pooler, :api_key_deletion_immediate_request_limit, 0)
      fixture = replay_fixture(reservation?: true)
      assert {:ok, _} = RequestReplay.arm(arm_input(fixture))
      other = replay_fixture(owner: fixture.scope.user, reservation?: true)
      assert {:ok, _} = RequestReplay.arm(arm_input(other))

      assert {:error, %Postgrex.Error{postgres: %{constraint: "request_replay_entitlements_request_snapshot_immutable_check"}}} =
               Repo.query("UPDATE requests SET api_key_id = NULL WHERE id = $1", [Ecto.UUID.dump!(fixture.request.id)], mode: :savepoint)

      if unquote(closed?), do: assert({:ok, :closed} = RequestReplay.close(fixture.request.id, :deleted))
      assert {:deleting, _} = Access.delete_api_key(fixture.scope, fixture.api_key)
      args = %{"api_key_id" => fixture.api_key.id}
      assert :ok = perform_job(APIKeyDeletionWorker, args)
      refute Repo.get(APIKey, fixture.api_key.id)
      assert Repo.query!("SELECT api_key_id FROM requests WHERE id = $1", [Ecto.UUID.dump!(fixture.request.id)]).rows == [[nil]]
      assert Repo.query!("SELECT count(*) FROM request_replay_entitlements WHERE api_key_id = $1", [Ecto.UUID.dump!(fixture.api_key.id)]).rows == [[0]]
      assert :ok = perform_job(APIKeyDeletionWorker, args)
      assert Repo.get!(APIKey, other.api_key.id).status == "active"
      assert Repo.query!("SELECT count(*) FROM request_replay_entitlements WHERE api_key_id = $1", [Ecto.UUID.dump!(other.api_key.id)]).rows == [[1]]
    end
  end

  for update <- [:update_api_key, :update_api_key_with_policy] do
    test "#{update} cannot reactivate a revoked secret from a stale key" do
      {scope, key, raw} = key_fixture()
      assert {:ok, revoked} = Access.revoke_api_key(scope, key)
      assert error_code(apply(Access, unquote(update), [scope, key, %{status: "active"}])) == :api_key_revoked
      assert Repo.get!(APIKey, key.id) == revoked
      assert {:error, %{code: :api_key_revoked}} = Access.authenticate_api_key(raw)
    end

    test "#{update} records the revocation timestamp on an explicit revoked target" do
      {scope, key, raw} = key_fixture()
      assert {:ok, _} = apply(Access, unquote(update), [scope, key, %{status: "revoked"}])
      updated = Repo.get!(APIKey, key.id)
      assert %DateTime{} = updated.revoked_at
      assert updated.runtime_revocation_epoch == key.runtime_revocation_epoch + 1
      assert {:error, %{code: :api_key_revoked}} = Access.authenticate_api_key(raw)
    end
  end

  test "rotation refuses a stale active struct after the locked row was revoked" do
    {scope, key, _raw} = key_fixture()
    assert {:ok, revoked} = Access.revoke_api_key(scope, key)
    assert error_code(Access.rotate_api_key(scope, key)) == :api_key_revoked
    assert Repo.get!(APIKey, key.id) == revoked
  end

  for field <- [:default_policy, :model_policies], value <- [nil, %{}] do
    test "key-row updates refuse nested #{field} even when value is #{inspect(value)}" do
      {scope, key, _raw} = key_fixture()
      assert {:error, %{code: :unsupported_field}} = Access.update_api_key(scope, key, %{policy: %{unquote(field) => unquote(Macro.escape(value))}})
    end
  end

  test "policy updates apply nested bindings and preserve omitted groups" do
    {scope, key, _raw} = key_fixture()
    assert {:ok, _} = Access.update_api_key_with_policy(scope, key, %{"policy" => %{"default_policy" => %{"max_tokens_per_day" => 17}}})
    binding = Repo.get_by!(APIKeyPolicyBinding, api_key_id: key.id, binding_scope: "default")
    assert binding.max_tokens_per_day == 17
    assert {:ok, _} = Access.update_api_key_with_policy(scope, key, %{policy: %{model_policies: [%{model_identifier: "gpt-alpha", max_tokens_per_day: 9}]}})
    assert Repo.get_by!(APIKeyPolicyBinding, api_key_id: key.id, binding_scope: "model", model_identifier: "gpt-alpha").max_tokens_per_day == 9
    assert Repo.get_by!(APIKeyPolicyBinding, api_key_id: key.id, binding_scope: "default").max_tokens_per_day == 17
    assert {:ok, _} = Access.update_api_key_with_policy(scope, key, %{"policy" => %{"model_policies" => []}})
    refute Repo.get_by(APIKeyPolicyBinding, api_key_id: key.id, binding_scope: "model")
    assert {:ok, _} = Access.update_api_key_with_policy(scope, key, %{display_name: "renamed"})
    assert Repo.get_by!(APIKeyPolicyBinding, api_key_id: key.id, binding_scope: "default").max_tokens_per_day == 17
    assert {:ok, _} = Access.update_api_key_with_policy(scope, key, %{policy: %{default_policy: nil}})
    assert Repo.get_by!(APIKeyPolicyBinding, api_key_id: key.id, binding_scope: "default").max_tokens_per_day == nil
  end

  defp error_code({:error, %{code: code}}), do: code
  defp error_code({:ok, _}), do: :unexpected_success

  defp key_fixture do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    scope = Scope.for_user(owner, ["instance_owner"])
    %{api_key: key, raw_key: raw} = active_api_key_fixture(pool_fixture(), %{scope: scope})
    {scope, key, raw}
  end
end
