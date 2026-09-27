defmodule CodexPooler.Upstreams.PermanentDeletionImportTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Schemas.{EncryptedSecret, PoolUpstreamAssignment}

  test "fresh re-import cannot replace credentials of an identity awaiting permanent deletion" do
    owner = bootstrap_owner_fixture().user
    scope = Scope.for_user(owner, ["instance_owner"])
    pool = pool_fixture(%{created_by_user_id: owner.id})

    attrs = %{
      chatgpt_account_id: "account-#{Ecto.UUID.generate()}",
      chatgpt_user_id: "user-#{Ecto.UUID.generate()}",
      account_email: "synthetic-#{System.unique_integer([:positive])}@example.com",
      account_label: "Synthetic upstream",
      token: "synthetic-access-token",
      refresh_token: "synthetic-refresh-token",
      credential_provenance: "codex_chatgpt_oauth"
    }

    assert {:ok, %{identity: identity}} = Upstreams.import_trusted_account(scope, pool, attrs)

    identity = identity |> Ecto.Changeset.change(status: "deleted", metadata: Map.put(identity.metadata, "permanent_deletion_requested_at", DateTime.to_iso8601(DateTime.utc_now()))) |> Repo.update!()
    Repo.update_all(from(row in PoolUpstreamAssignment, where: row.upstream_identity_id == ^identity.id), set: [status: "deleted"])
    Repo.update_all(from(row in EncryptedSecret, where: row.upstream_identity_id == ^identity.id), set: [status: "revoked"])
    secrets = Repo.all(from row in EncryptedSecret, where: row.upstream_identity_id == ^identity.id)
    assignments = Repo.all(from row in PoolUpstreamAssignment, where: row.upstream_identity_id == ^identity.id)

    assert {:error, %{code: :upstream_account_deleting}} = Upstreams.import_trusted_account(scope, pool, Map.put(attrs, :token, "replacement-synthetic-token"))
    assert Repo.reload!(identity) == identity
    assert Repo.all(from row in EncryptedSecret, where: row.upstream_identity_id == ^identity.id) == secrets
    assert Repo.all(from row in PoolUpstreamAssignment, where: row.upstream_identity_id == ^identity.id) == assignments
  end
end
