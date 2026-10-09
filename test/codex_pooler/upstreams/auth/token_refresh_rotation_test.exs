defmodule CodexPooler.Upstreams.Auth.TokenRefreshRotationTest do
  use CodexPooler.DataCase, async: false
  alias CodexPooler.{FakeRefreshTokenProvider, Repo, Upstreams}
  alias CodexPooler.Upstreams.Auth.{RefreshTokenRecovery, TokenRefresh}
  alias CodexPooler.Upstreams.Lifecycle.IdentityLifecycle
  alias CodexPooler.Upstreams.Secrets

  @budget 15_000
  @moduletag :refresh_rotation_recovery

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(Upstreams)
    Application.put_env(:codex_pooler, Upstreams, upstream_secret_key: Base.encode64(:crypto.hash(:sha256, "synthetic-rotation-root")), upstream_secret_key_version: "test-v1")
    :ok
  end

  for policy <- [:old_token, :family] do
    test "ledger validates actual forms with #{policy} reuse policy" do
      {url, ledger} = provider(policy: unquote(policy))
      assert post(url, 0).status == 200
      assert post(url, 0).status == 400
      assert post(url, 1).status == if(unquote(policy) == :family, do: 400, else: 200)
      assert FakeRefreshTokenProvider.snapshot(ledger).consumed == if(unquote(policy) == :family, do: 1, else: 2)
    end
  end

  test "R1 expired access preserves returned rotation and next real refresh consumes it" do
    {url, ledger} = provider(steps: [%{access_token: expired_access()}])
    identity = identity(url)
    epoch = identity.metadata["credential_epoch"]
    assert {:ok, %{status: :refresh_failed}} = TokenRefresh.refresh_access_token(identity)
    assert_refresh_generation(identity, 1)
    assert_access_unchanged(identity)
    assert Repo.reload!(identity).metadata["credential_epoch"] == epoch
    assert {:ok, %{status: :active}} = TokenRefresh.refresh_access_token(identity)
    assert FakeRefreshTokenProvider.snapshot(ledger).consumed == 2
  end

  for {ordering, policy} <- [{:rejection_first, :old_token}, {:rotation_first, :old_token}, {:rejection_first, :family}] do
    test "late rotation #{ordering} with #{policy} preserves terminal state" do
      {url, ledger} = provider(policy: unquote(policy), steps: [%{hold: :after_consume}, %{hold: :after_consume}])
      identity = identity(url)
      supervisor = start_supervised!(Task.Supervisor)
      first = async_refresh(supervisor, fn -> TokenRefresh.refresh_access_token(identity) end)
      assert_receive {:rotation_barrier, :after_consume, 1, handler_a, ref_a}, @budget
      age_claim(identity)
      second = async_refresh(supervisor, fn -> TokenRefresh.refresh_access_token(identity) end)
      assert_receive {:rotation_barrier, :after_consume, 2, handler_b, ref_b}, @budget

      if unquote(ordering) == :rejection_first do
        send(handler_b, {:rotation_release, ref_b})
        assert {:ok, %{status: :reauth_required}} = await_refresh(second)
        send(handler_a, {:rotation_release, ref_a})
        await_refresh(first)
      else
        send(handler_a, {:rotation_release, ref_a})
        await_refresh(first)
        send(handler_b, {:rotation_release, ref_b})
        assert {:ok, %{status: :reauth_required}} = await_refresh(second)
      end

      assert_refresh_generation(identity, 1)
      assert_access_unchanged(identity)
      assert Repo.reload!(identity).status == "reauth_required"
      assert FakeRefreshTokenProvider.snapshot(ledger).consumed == 1
    end
  end

  for phase <- [:before_consume, :after_consume] do
    test "timeout at #{phase} cannot invent a decoded rotation" do
      {url, ledger} = provider(steps: [%{hold: unquote(phase)}])
      identity = identity(url)
      supervisor = start_supervised!(Task.Supervisor)
      task = async_refresh(supervisor, fn -> TokenRefresh.refresh_access_token(identity, receive_timeout: 100) end)
      assert_receive {:rotation_barrier, unquote(phase), 1, handler, ref}, @budget
      assert {:ok, %{status: :refresh_failed}} = await_refresh(task)
      send(handler, {if(unquote(phase) == :before_consume, do: :rotation_cancel, else: :rotation_release), ref})
      assert_refresh_generation(identity, 0)
      refute Repo.reload!(identity).metadata["refresh_token_recovery"]
      assert FakeRefreshTokenProvider.snapshot(ledger).consumed == if(unquote(phase) == :before_consume, do: 0, else: 1)
    end
  end

  test "recovery rejects absent transaction, malformed provenance and nonrotations" do
    {url, _ledger} = provider(steps: [%{hold: :after_consume}])
    identity = identity(url)
    supervisor = start_supervised!(Task.Supervisor)
    task = async_refresh(supervisor, fn -> TokenRefresh.refresh_access_token(identity) end)
    assert_receive {:rotation_barrier, :after_consume, 1, handler, ref}, @budget
    attempt = recovery_attempt(identity)
    attrs = %{refresh_token: "synthetic-refresh-1"}
    assert {:error, %{code: :refresh_recovery_transaction_required}} = RefreshTokenRecovery.retain(identity, attempt, attrs, "superseded_attempt")

    for damaged <- [Map.delete(attempt, :source_secret_id), %{attempt | credential_epoch: 0}, %{attempt | generation: 0}, %{attempt | attempt_id: "invalid"}, %{attempt | source_secret_id: Ecto.UUID.generate()}] do
      assert {:ok, {:ok, :ignored, _}} = Repo.transaction(fn -> RefreshTokenRecovery.retain(identity, damaged, attrs, "superseded_attempt") end)
    end

    for value <- [nil, "", "synthetic-refresh-0"] do
      assert {:ok, {:ok, :ignored, _}} = Repo.transaction(fn -> RefreshTokenRecovery.retain(identity, attempt, %{refresh_token: value}, "superseded_attempt") end)
    end

    assert_refresh_generation(identity, 0)
    send(handler, {:rotation_release, ref})
    assert {:ok, %{status: :active}} = await_refresh(task)
  end

  test "provenance API preserves old decrypt result and returns the real active row id" do
    {url, _} = provider([])
    identity = identity(url)
    assert {:ok, old} = Secrets.decrypt_active_secret(identity, "refresh_token")
    assert {:ok, token, id} = Secrets.decrypt_active_secret_with_id(identity, "refresh_token")
    assert token == old
    assert Enum.any?(Secrets.list_active_encrypted_secrets(identity), &(&1.id == id and &1.secret_kind == "refresh_token"))
    assert {:error, %{code: :upstream_secret_not_found}} = Secrets.decrypt_active_secret_with_id(identity, "id_token")
  end

  @tag :rotation_guard_controls
  test "terminal state and epoch independently fence otherwise valid recovery provenance" do
    {url, _} = provider(steps: [%{hold: :after_consume}])
    identity = identity(url)
    supervisor = start_supervised!(Task.Supervisor)
    task = async_refresh(supervisor, fn -> TokenRefresh.refresh_access_token(identity) end)
    assert_receive {:rotation_barrier, :after_consume, 1, handler, ref}, @budget
    attempt = recovery_attempt(identity)
    original = Repo.reload!(identity)

    for state <- [:epoch, :paused, :deleted, :permanent_deletion] do
      changed =
        case state do
          :epoch ->
            epoch = attempt.credential_epoch + 1
            metadata = original.metadata |> Map.put("credential_epoch", epoch) |> put_in(["token_refresh", "credential_epoch"], epoch)
            Ecto.Changeset.change(original, metadata: metadata)

          :permanent_deletion ->
            Ecto.Changeset.change(original, metadata: Map.put(original.metadata, "permanent_deletion_requested_at", DateTime.to_iso8601(DateTime.utc_now())))

          status ->
            Ecto.Changeset.change(original, status: Atom.to_string(status))
        end

      Repo.update!(changed)
      assert {:ok, {:ok, :ignored, _}} = Repo.transaction(fn -> RefreshTokenRecovery.retain(identity, attempt, %{refresh_token: "synthetic-refresh-1"}, "superseded_attempt") end)
      current = Repo.reload!(identity)
      Repo.update!(Ecto.Changeset.change(current, status: original.status, metadata: original.metadata))
    end

    assert_refresh_generation(identity, 0)
    send(handler, {:rotation_release, ref})
    assert {:ok, %{status: :active}} = await_refresh(task)
  end

  defp recovery_attempt(identity) do
    current = Repo.reload!(identity).metadata["token_refresh"]
    %{attempt_id: current["attempt_id"], generation: current["generation"], source_secret_id: current["source_secret_id"], credential_epoch: current["credential_epoch"]}
  end

  defp async_refresh(supervisor, fun) do
    task = Task.Supervisor.async_nolink(supervisor, fun)
    Process.put({__MODULE__, :refresh_monitor, task.ref}, Process.monitor(task.pid))
    task
  end

  defp await_refresh(task) do
    monitor = Process.delete({__MODULE__, :refresh_monitor, task.ref})
    result = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    result
  end

  defp provider(opts) do
    name = String.to_atom("rotation_#{System.unique_integer([:positive])}")
    pid = start_supervised!({FakeRefreshTokenProvider, Keyword.merge(opts, name: name, notify: self())})
    {FakeRefreshTokenProvider.url(pid), name}
  end

  defp identity(url) do
    assert {:ok, identity} = IdentityLifecycle.create_upstream_identity(%{chatgpt_account_id: "sample-#{System.unique_integer([:positive])}", account_label: "Sample", onboarding_method: "import", status: "active", metadata: %{"base_url" => url}})
    assert {:ok, _} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "access_token", plaintext: "synthetic-access-original"})
    assert {:ok, _} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "refresh_token", plaintext: "synthetic-refresh-0"})
    identity
  end

  defp assert_refresh_generation(identity, generation) do
    {:ok, token} = Secrets.decrypt_active_secret(identity, "refresh_token")
    assert token == "synthetic-refresh-#{generation}", "stored refresh credential differs from provider generation"
  end

  defp assert_access_unchanged(identity) do
    {:ok, token} = Secrets.decrypt_active_secret(identity, "access_token")
    assert token == "synthetic-access-original", "access credential changed"
  end

  defp post(url, generation), do: Req.post!(url <> "/oauth/token", form: [grant_type: "refresh_token", refresh_token: "synthetic-refresh-#{generation}"], retry: false)

  defp expired_access do
    payload = Base.url_encode64(CodexPooler.JSON.encode!(%{"exp" => DateTime.to_unix(DateTime.add(DateTime.utc_now(), -60))}), padding: false)
    "e30." <> payload <> ".synthetic"
  end

  defp age_claim(identity) do
    current = Repo.reload!(identity)
    metadata = put_in(current.metadata, ["token_refresh", "started_at"], DateTime.to_iso8601(DateTime.add(DateTime.utc_now(), -120)))
    Repo.update!(Ecto.Changeset.change(current, metadata: metadata))
  end
end
