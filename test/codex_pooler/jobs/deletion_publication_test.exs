defmodule CodexPooler.Jobs.DeletionPublicationTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import ExUnit.CaptureLog

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Access.APIKeys
  alias CodexPooler.Access.APIKeys.Deletion, as: KeyDeletion
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Alerts.Incidents.NotificationEvents
  alias CodexPooler.Events
  alias CodexPooler.Events.PostgresBridge
  alias CodexPooler.Jobs.DeletionFailureNotifier
  alias CodexPooler.Pools
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture

  @detection_timeout_ms 5_000

  setup do
    name = String.to_atom("deletion_publication_repo_#{System.unique_integer([:positive])}")
    config = Repo.config() |> Keyword.put(:name, name) |> Keyword.put(:pool, DBConnection.ConnectionPool) |> Keyword.put(:pool_size, 2)
    start_supervised!({Repo, config})
    Repo.put_dynamic_repo(name)
    notifications = start_supervised!({Postgrex.Notifications, Keyword.take(config, [:hostname, :port, :database, :username, :password, :ssl])})
    {:ok, events_ref} = Postgrex.Notifications.listen(notifications, Events.postgres_channel())
    {:ok, alerts_ref} = Postgrex.Notifications.listen(notifications, NotificationEvents.postgres_channel())
    bridge_state = :sys.get_state(PostgresBridge)
    assert is_reference(bridge_state.listen_ref)
    assert is_reference(bridge_state.alert_listen_ref)
    %{repo: name, notifications: notifications, events_ref: events_ref, alerts_ref: alerts_ref}
  end

  for event <- [[:oban, :job, :exception], [:oban, :job, :stop]] do
    test "upstream deletion failure notification survives commit for #{inspect(event)}", %{notifications: notifications, events_ref: events_ref} do
      alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
      {pool, _key} = fixture()
      id = Ecto.UUID.generate()
      UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from identity in UpstreamIdentity, where: identity.id == ^id) end)
      now = DateTime.utc_now()
      Repo.insert!(%UpstreamIdentity{id: id, chatgpt_account_id: "sample-#{id}", account_label: "Sample deleted account", status: "deleted", onboarding_method: "import", created_at: now, updated_at: now, metadata: %{}})
      # An orphan's notice fans out to active Pools visible to the owner.
      pool |> change(status: "active") |> Repo.update!()
      job = %{worker: "CodexPooler.Jobs.UpstreamDeletionWorker", args: %{"upstream_identity_id" => id}}
      assert :ok = DeletionFailureNotifier.handle_event(unquote(event), %{}, %{state: :discard, job: job}, :ok)
      assert_receive {:notification, ^notifications, ^events_ref, _, payload}, @detection_timeout_ms
      attrs = CodexPooler.JSON.decode!(payload)
      assert attrs["reason"] == "upstream_account_deletion_failed"
      assert attrs["payload"]["upstream_identity_id"] == id
      assert String.starts_with?(attrs["origin_id"], "transaction:")
      assert :ok = CodexPooler.Upstreams.broadcast_account_deletion_failed(Ecto.UUID.generate())
    end
  end

  for target <- [:pool, :key] do
    @tag slow: "holds the real committed deletion executor until its one-second absolute deadline"
    test "#{target} deletion publishes after commit even when its executor cannot return", context do
      verify_publication(unquote(target), context)
    end

    test "#{target} deletion publishes nothing when its transaction rolls back", context do
      verify_rollback(unquote(target), context)
    end
  end

  defp verify_publication(target, %{repo: repo, notifications: notifications, events_ref: events_ref}) do
    {pool, key} = fixture()
    id = if target == :pool, do: pool.id, else: key.id
    schema = if target == :pool, do: Pool, else: APIKey
    table = if target == :pool, do: "pools", else: "api_keys"
    rule = alert_rule_fixture(pool)
    incident = alert_incident_fixture(pool: pool)
    alert_incident_target_fixture(incident, rule, pool)
    assert :ok = Events.subscribe_pool(pool.id, "pools")
    assert :ok = NotificationEvents.subscribe_pool(pool.id)
    assert :ok = Events.subscribe_dashboard_sessions(key.id)

    parent = self()
    capture = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(capture) end)
    :ok = :telemetry.attach(capture, [:codex_pooler, :repo, :query], &__MODULE__.hold_committed_delete/4, %{owner: parent, table: table, id: Ecto.UUID.dump!(id), capture: capture})

    runner =
      Task.async(fn ->
        Repo.put_dynamic_repo(repo)
        deadline = System.monotonic_time(:millisecond) + 1_000
        if target == :pool, do: Pools.continue_pool_deletion(id, nil, deadline), else: KeyDeletion.continue(id, nil, deadline)
      end)

    runner_monitor = Process.monitor(runner.pid)
    on_exit(fn -> if Process.alive?(runner.pid), do: Process.exit(runner.pid, :kill) end)
    assert_receive {:delete_committed, executor}, @detection_timeout_ms
    executor_monitor = Process.monitor(executor)
    refute Repo.get(schema, id)
    {result, log} = with_log(fn -> Task.await(runner, @detection_timeout_ms) end)
    assert result == :more
    assert log == "" or log =~ "timed out because it queued and checked out the connection"
    assert_receive {:DOWN, ^runner_monitor, :process, _, :normal}, @detection_timeout_ms
    assert_receive {:DOWN, ^executor_monitor, :process, ^executor, :killed}, @detection_timeout_ms
    assert_receive {:notification, ^notifications, ^events_ref, _, payload}, @detection_timeout_ms
    attrs = CodexPooler.JSON.decode!(payload)
    assert String.starts_with?(attrs["origin_id"], "transaction:")
    refute Map.has_key?(attrs, "origin_node")

    if target == :pool do
      assert_receive {Events, %{reason: "pool_deleted", pool_id: pool_id}}, @detection_timeout_ms
      assert pool_id == pool.id
      assert_receive {NotificationEvents, :invalidated, _}, @detection_timeout_ms
    else
      assert_receive {Events, %{reason: "api_key_deleted", payload: %{"api_key_id" => key_id}}}, @detection_timeout_ms
      assert key_id == key.id
      assert_receive {Events, %{reason: "dashboard_session_invalidated", payload: %{"api_key_id" => ^key_id}}}, @detection_timeout_ms
    end
  end

  def hold_committed_delete(_event, _measurements, %{query: query} = metadata, context) do
    cond do
      String.starts_with?(query, ~s(DELETE FROM "#{context.table}")) and context.id in Map.get(metadata, :params, []) ->
        Process.put(context.capture, true)

      String.downcase(query) == "commit" and Process.delete(context.capture) ->
        send(context.owner, {:delete_committed, self()})

        receive do
          :release_committed_delete -> :ok
        end

      true ->
        :ok
    end
  end

  defp verify_rollback(target, %{notifications: notifications, events_ref: events_ref, alerts_ref: alerts_ref}) do
    {pool, key} = fixture()
    rule = alert_rule_fixture(pool)
    incident = alert_incident_fixture(pool: pool)
    alert_incident_target_fixture(incident, rule, pool)
    assert :ok = Events.subscribe_pool(pool.id, "pools")
    assert :ok = Events.subscribe_dashboard_sessions(key.id)
    assert :ok = NotificationEvents.subscribe_pool(pool.id)

    assert {:error, :sample_rollback} =
             Repo.transaction(fn ->
               case target do
                 :pool -> assert {:ok, _} = Pools.Deletion.finish(pool.id, nil)
                 :key -> assert {:ok, _} = APIKeys.delete_api_key_row(%Scope{}, key, 1_000)
               end

               Repo.rollback(:sample_rollback)
             end)

    assert Repo.get(Pool, pool.id)
    assert Repo.get(APIKey, key.id)
    # A marker from the same backend traverses both channels after ROLLBACK.
    # Seeing it proves the listener drained all earlier notifications.
    marker = CodexPooler.JSON.encode!(%{origin_id: Events.origin_id(), marker: Ecto.UUID.generate()})
    Repo.query!("SELECT pg_notify($1, $2), pg_notify($3, $2)", [Events.postgres_channel(), marker, NotificationEvents.postgres_channel()])
    assert_receive {:notification, ^notifications, ^events_ref, _, ^marker}, @detection_timeout_ms
    assert_receive {:notification, ^notifications, ^alerts_ref, _, ^marker}, @detection_timeout_ms
    refute_received {:notification, ^notifications, _, _, _}
    refute_received {Events, %{reason: "pool_deleted"}}
    refute_received {Events, %{reason: "api_key_deleted"}}
    refute_received {Events, %{reason: "dashboard_session_invalidated"}}
    refute_received {NotificationEvents, :invalidated, _}
  end

  defp fixture do
    pool_id = Ecto.UUID.generate()
    key_id = Ecto.UUID.generate()

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.delete_all(from event in CodexPooler.Audit.AuditEvent, where: event.target_id in ^[pool_id, key_id])
      Repo.delete_all(from pool in Pool, where: pool.id == ^pool_id)
    end)

    now = DateTime.utc_now()
    pool = Repo.insert!(%Pool{id: pool_id, name: "Publication sample", slug: "publication-#{pool_id}", status: "archived", created_at: now, updated_at: now})
    key = Repo.insert!(%APIKey{id: key_id, pool_id: pool.id, display_name: "Publication key", key_prefix: "publication-#{key_id}", key_hash: :crypto.strong_rand_bytes(32), status: "revoked", revoked_at: now, created_at: now})
    {pool, key}
  end
end
