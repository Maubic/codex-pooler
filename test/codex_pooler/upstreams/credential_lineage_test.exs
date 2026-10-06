defmodule CodexPooler.Upstreams.CredentialLineageTest do
  # The credential lineage every credential epoch advance records
  # (findings#330): a token refresh keeps the lineage of the credential it
  # renews, every other advance starts a new one, and
  # `CredentialFencing.same_credential_since?/2` reads it. A content-filter
  # retry's binding, and anything else that must stay on the provider account
  # that produced its state, rides a refresh and refuses a replacement. Every
  # change below goes through the real writer: the refresh state machine
  # against FakeUpstream, the scoped pause and reactivation, the Codex
  # `auth.json` import and a targeted relink.
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Auth.TokenRefresh
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPooler.Upstreams.TokenLinking

  @lineage "credential_lineage"

  # The refresh path changes nothing but the lineage: for every identity shape
  # the token refresh can meet, `prepare_refresh_metadata/1` writes what
  # `prepare_replacement_metadata/1` writes (the epoch it computes, the
  # initialized usage-probe sequences, a terminal provider auth rejection moved
  # to `awaiting_fresh_quota`, every other key untouched, the same refusals)
  # except `credential_lineage`.
  test "a refresh writes exactly what a replacement writes except the lineage" do
    recovery = %{"status" => "terminal", "updated_at" => "2026-01-01T00:00:00Z"}
    trusted = %{"version" => 1, "since_epoch" => 2, "credential_epoch" => 3}
    stale = %{"version" => 1, "since_epoch" => 1, "credential_epoch" => 2}

    shapes =
      for status <- ["active", "refresh_failed", "reauth_required", "pending", "paused"],
          metadata <- [
            %{},
            %{"credential_epoch" => 1},
            %{"credential_epoch" => 3, "usage_probe_sequence" => 7, "synthetic_key" => "kept"},
            %{"credential_epoch" => 3, @lineage => trusted},
            %{"credential_epoch" => 3, @lineage => stale},
            %{"credential_epoch" => 2, "provider_auth_recovery" => recovery, "token_refresh" => %{"status" => "reauth_required"}}
          ],
          do: %UpstreamIdentity{status: status, metadata: metadata}

    refusals =
      [%UpstreamIdentity{status: "active", metadata: %{"permanent_deletion_requested_at" => "2026-01-01T00:00:00Z"}}] ++
        for epoch <- [nil, "3", %{}, 0, -1], do: %UpstreamIdentity{status: "active", metadata: %{"credential_epoch" => epoch}}

    for identity <- shapes do
      assert {:ok, refreshed, epoch} = CredentialFencing.prepare_refresh_metadata(identity)
      assert {:ok, replaced, ^epoch} = CredentialFencing.prepare_replacement_metadata(identity)
      assert without_lineage(refreshed) == without_lineage(replaced)
      assert %{"version" => 1, "credential_epoch" => ^epoch} = replaced[@lineage]
      assert replaced[@lineage]["since_epoch"] == epoch
      assert %{"version" => 1, "credential_epoch" => ^epoch, "since_epoch" => since} = refreshed[@lineage]
      assert since <= epoch
      refute inspect(refreshed[@lineage]) =~ "acct_"
    end

    for identity <- refusals do
      assert {:error, %{code: code}} = CredentialFencing.prepare_refresh_metadata(identity)
      assert {:error, %{code: ^code}} = CredentialFencing.prepare_replacement_metadata(identity)
    end
  end

  # Right after a deploy no identity carries the marker. The first refresh
  # starts the lineage at the epoch the refresh started at, never earlier: a
  # binding recorded at that epoch rides the refresh, an older one refuses.
  test "a refresh without a lineage starts it at the epoch the refresh started at" do
    fixture = fixture!(%{"credential_epoch" => 3})
    assert Repo.reload!(fixture.identity).metadata[@lineage] == nil
    refresh!(fixture)
    identity = Repo.reload!(fixture.identity)
    assert identity.metadata["credential_epoch"] == 4
    assert identity.metadata[@lineage] == %{"version" => 1, "since_epoch" => 3, "credential_epoch" => 4}
    assert CredentialFencing.same_credential_since?(identity, 3)
    assert CredentialFencing.same_credential_since?(identity, 4)
    refute CredentialFencing.same_credential_since?(identity, 2)
    refute CredentialFencing.same_credential_since?(identity, 5)
  end

  test "refreshes in a row keep the lineage" do
    fixture = fixture!(%{})
    for _refresh <- 1..3, do: refresh!(fixture)
    identity = Repo.reload!(fixture.identity)
    assert identity.metadata["credential_epoch"] == 4
    assert identity.metadata[@lineage] == %{"version" => 1, "since_epoch" => 1, "credential_epoch" => 4}
    assert Enum.all?(1..4, &CredentialFencing.same_credential_since?(identity, &1))
  end

  # Pause, reactivation, an operator re-import and a targeted relink each start
  # a new lineage at their own epoch, and its start only moves forward: no
  # refresh afterwards brings back a binding recorded before the change.
  for change <- [:pause, :reactivate, :reimport, :relink] do
    @tag change: change
    test "#{change} starts a new lineage that no later refresh undoes", context do
      fixture = fixture!(%{})
      refresh!(fixture)
      before = Repo.reload!(fixture.identity)
      assert CredentialFencing.same_credential_since?(before, 1)
      change!(context.change, fixture)
      changed = Repo.reload!(fixture.identity)
      epoch = changed.metadata["credential_epoch"]
      assert epoch > before.metadata["credential_epoch"]
      assert changed.metadata[@lineage] == %{"version" => 1, "since_epoch" => epoch, "credential_epoch" => epoch}
      refute CredentialFencing.same_credential_since?(changed, 1)
      refute CredentialFencing.same_credential_since?(changed, before.metadata["credential_epoch"])
      assert CredentialFencing.same_credential_since?(changed, epoch)

      if context.change in [:reimport, :relink, :reactivate] do
        for _refresh <- 1..2, do: refresh!(fixture)
        refreshed = Repo.reload!(fixture.identity)
        assert refreshed.metadata[@lineage]["since_epoch"] == epoch
        refute CredentialFencing.same_credential_since?(refreshed, before.metadata["credential_epoch"])
        assert CredentialFencing.same_credential_since?(refreshed, epoch)
      end
    end
  end

  # A marker not written for the identity's current epoch is not trusted: a
  # release that predates the marker advanced the epoch (a rolling deploy) or
  # the marker is left over. It reads as a new credential at the current epoch.
  test "a marker an epoch advance left behind refuses every older binding" do
    fixture = fixture!(%{})
    refresh!(fixture)
    identity = Repo.reload!(fixture.identity)
    assert %{"since_epoch" => 1, "credential_epoch" => 2} = identity.metadata[@lineage]
    # The advance of a release without the marker: the epoch moves, the marker stays.
    older_release = identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "credential_epoch", 3)) |> Repo.update!()
    refute CredentialFencing.same_credential_since?(older_release, 1)
    refute CredentialFencing.same_credential_since?(older_release, 2)
    assert CredentialFencing.same_credential_since?(older_release, 3)

    # A refresh from there trusts nothing older than the epoch it started at.
    refresh!(fixture)
    after_refresh = Repo.reload!(fixture.identity)
    assert after_refresh.metadata[@lineage] == %{"version" => 1, "since_epoch" => 3, "credential_epoch" => 4}
    refute CredentialFencing.same_credential_since?(after_refresh, 1)
    assert CredentialFencing.same_credential_since?(after_refresh, 3)

    for marker <- [%{"version" => 2, "since_epoch" => 1, "credential_epoch" => 4}, %{"version" => 1, "since_epoch" => 5, "credential_epoch" => 4}, %{"version" => 1, "since_epoch" => "1", "credential_epoch" => 4}, %{"version" => 1, "since_epoch" => 1, "credential_epoch" => 4, "extra" => 1}, "1"] do
      tampered = %{after_refresh | metadata: Map.put(after_refresh.metadata, @lineage, marker)}
      refute CredentialFencing.same_credential_since?(tampered, 3)
      assert CredentialFencing.same_credential_since?(tampered, 4)
    end

    for invalid <- [nil, 0, -1, "4", 4.0] do
      refute CredentialFencing.same_credential_since?(after_refresh, invalid)
    end
  end

  defp without_lineage(metadata) do
    metadata
    |> Map.delete(@lineage)
    |> then(fn
      %{"provider_auth_recovery" => %{} = recovery} = metadata -> Map.put(metadata, "provider_auth_recovery", Map.put(recovery, "updated_at", "normalized"))
      metadata -> metadata
    end)
  end

  defp fixture!(metadata) do
    {:ok, upstream} = FakeUpstream.start_link({:path_json, %{"/oauth/token" => {200, %{"access_token" => "synthetic-lineage-access", "expires_in" => 3600}}}})
    on_exit(fn -> FakeUpstream.stop(upstream) end)
    %{user: owner} = bootstrap_owner_fixture()
    pool = pool_fixture()
    account_id = "acct_lineage_#{System.unique_integer([:positive])}"
    identity = active_upstream_identity_fixture(%{chatgpt_account_id: account_id, metadata: Map.put(metadata, "base_url", FakeUpstream.url(upstream))})
    assert {:ok, _secret} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "access_token", plaintext: "synthetic-lineage-access-0"})
    assert {:ok, _secret} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "refresh_token", plaintext: "synthetic-lineage-refresh"})
    assert {:ok, assignment} = PoolAssignments.create_pool_assignment(pool, identity)
    assert {:ok, _assignment} = PoolAssignments.activate_pool_assignment(assignment)
    %{scope: Scope.for_user(owner), pool: pool, identity: Repo.reload!(identity), account_id: account_id}
  end

  # The token refresh state machine, through the provider's token endpoint.
  defp refresh!(fixture), do: assert({:ok, %{status: :active}} = TokenRefresh.refresh_access_token(fixture.identity.id, trigger_kind: "manual"))

  defp change!(:pause, fixture), do: assert({:ok, _paused} = Upstreams.pause_account_for_scope(fixture.scope, fixture.identity.id, %{}))

  defp change!(:reactivate, fixture) do
    assert {:ok, _paused} = Upstreams.pause_account_for_scope(fixture.scope, fixture.identity.id, %{})
    assert {:ok, _active} = Upstreams.reactivate_account_for_scope(fixture.scope, fixture.identity.id, %{})
  end

  defp change!(:reimport, fixture) do
    jwt = fn claims -> Enum.map_join([%{"alg" => "none", "typ" => "JWT"}, claims, "synthetic-signature"], ".", &Base.url_encode64(CodexPooler.JSON.encode!(&1), padding: false)) end
    id_token = jwt.(%{"email" => "lineage@example.com", "https://api.openai.com/auth" => %{"chatgpt_account_id" => fixture.account_id, "chatgpt_user_id" => "user_lineage", "chatgpt_plan_type" => "pro"}})
    access_token = jwt.(%{"exp" => DateTime.utc_now() |> DateTime.add(3600) |> DateTime.to_unix()})
    auth = CodexPooler.JSON.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"id_token" => id_token, "access_token" => access_token, "refresh_token" => "synthetic-lineage-reimported-refresh", "account_id" => fixture.account_id}})
    assert {:ok, %{status: :existing, identity: %{id: id}}} = Upstreams.import_codex_auth_json(fixture.scope, fixture.pool, auth)
    assert id == fixture.identity.id
  end

  defp change!(:relink, fixture) do
    attrs = %{chatgpt_account_id: fixture.account_id, account_label: fixture.identity.account_label, token: "synthetic-lineage-relinked-access", refresh_token: "synthetic-lineage-relinked-refresh"}
    assert {:ok, %{status: :existing, identity: %{id: id}}} = TokenLinking.link_tokens(fixture.scope, fixture.pool, attrs, target_identity_id: fixture.identity.id, credential_provenance: :codex_chatgpt)
    assert id == fixture.identity.id
  end
end
