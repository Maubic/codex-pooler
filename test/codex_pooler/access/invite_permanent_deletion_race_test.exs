defmodule CodexPooler.Access.InvitePermanentDeletionRaceTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query
  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1, run_unboxed: 1]

  alias CodexPooler.Access.{Invite, InviteOnboarding, Invites}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Auth.CodexAuth
  alias CodexPooler.Upstreams.Lifecycle.IdentitySlotLock
  alias CodexPooler.Upstreams.Schemas.{EncryptedSecret, PoolUpstreamAssignment, UpstreamIdentity}
  alias Ecto.Adapters.SQL.Sandbox

  @budget 10_000

  test "a device restart cannot overwrite deletion committed after pending-account selection" do
    CodexPooler.TestAppEnv.restore_on_exit(CodexAuth)
    Application.put_env(:codex_pooler, CodexAuth, client: __MODULE__.AuthClient)
    suffix = Ecto.UUID.generate()
    slug = "invite-deletion-#{suffix}"
    label = "Synthetic pending #{suffix}"
    token = Ecto.UUID.generate()
    invite_id = Ecto.UUID.generate()

    register_unboxed_cleanup!(fn ->
      Repo.delete_all(from row in UpstreamIdentity, where: fragment("?->>'invite_id'", row.metadata) == ^invite_id)
      Repo.delete_all(from row in Pool, where: row.slug == ^slug)
    end)

    fixture =
      run_unboxed(fn ->
        pool = pool_fixture(%{slug: slug})
        now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
        invite = Repo.insert!(%Invite{id: invite_id, pool_id: pool.id, token_hash: Invites.hash_invite_token(token), invited_email: "synthetic@example.com", status: "active", expires_at: DateTime.add(now, 600, :second), created_at: now, updated_at: now})
        upstream_assignment_fixture(pool, %{account_label: label, identity_status: "pending", assignment_status: "pending", onboarding_method: "invite", identity_metadata: %{"invite_id" => invite.id}, assignment_metadata: %{"invite_id" => invite.id}})
      end)

    parent = self()
    barrier = make_ref()
    handler = "invite-deletion-#{suffix}"
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_pending_read/4, %{parent: parent, barrier: barrier, identity_id: fixture.identity.id})

    worker =
      Task.async(fn ->
        Process.put(:invite_deletion_probe, barrier)
        Sandbox.unboxed_run(Repo, fn -> InviteOnboarding.start_device(token) end)
      end)

    monitor = Process.monitor(worker.pid)

    on_exit(fn ->
      ref = Process.monitor(worker.pid)
      if Process.alive?(worker.pid), do: Process.exit(worker.pid, :kill)
      assert_receive {:DOWN, ^ref, :process, _, _}, @budget
    end)

    assert_receive {^barrier, :pending_read}, @budget

    marked =
      run_unboxed(fn ->
        {:ok, marked} =
          Repo.transaction(fn ->
            IdentitySlotLock.lock_identity_rows!([fixture.identity.id])
            Repo.update_all(from(row in PoolUpstreamAssignment, where: row.id == ^fixture.assignment.id), set: [status: "deleted"])
            fixture.identity |> Ecto.Changeset.change(status: "deleted", metadata: Map.put(fixture.identity.metadata, "permanent_deletion_requested_at", DateTime.to_iso8601(DateTime.utc_now()))) |> Repo.update!()
          end)

        marked
      end)

    send(worker.pid, {barrier, :continue})

    outcome =
      case Task.await(worker, @budget) do
        {:error, %{code: code}} -> code
        {:ok, _started} -> :unexpected_success
      end

    assert outcome == :upstream_account_deleting
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget

    run_unboxed(fn ->
      assert Repo.reload!(marked) == marked
      assert Repo.reload!(fixture.assignment).status == "deleted"
      refute Repo.exists?(from row in EncryptedSecret, where: row.upstream_identity_id == ^marked.id)
    end)
  end

  def hold_pending_read(_event, _measurements, metadata, config) do
    query = Map.get(metadata, :query, "")

    if Process.get(:invite_deletion_probe) == config.barrier and String.contains?(query, ~s(FROM "upstream_identities")) and not String.contains?(query, "FOR UPDATE") do
      Process.delete(:invite_deletion_probe)
      send(config.parent, {config.barrier, :pending_read})

      receive do
        {barrier, :continue} when barrier == config.barrier -> :ok
      after
        @budget -> raise "pending read release missing"
      end
    end
  end

  defmodule AuthClient do
    def request_device_code do
      {:ok, %{"device_auth_id" => "synthetic-device", "user_code" => "SAMPLE", "verification_url" => "https://example.com/device", "expires_at" => DateTime.utc_now() |> DateTime.add(600, :second) |> DateTime.to_iso8601(), "poll_interval_seconds" => 5}}
    end
  end
end
