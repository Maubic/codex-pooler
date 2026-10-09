defmodule CodexPooler.SavedResetPostCommitFailureSupport do
  @moduledoc false

  import Ecto.Query
  import ExUnit.Assertions
  import ExUnit.Callbacks
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.TestDiagnostics
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport, as: GatewaySupport
  alias Ecto.Adapters.SQL.Sandbox

  @consume "/api/codex/rate-limit-reset-credits/consume"
  @usage_paths ["/api/codex/usage", "/backend-api/codex/usage", "/backend-api/wham/usage", "/wham/usage"]

  @spec start!(map()) :: pid()
  def start!(context) do
    {:ok, state} = Agent.start(fn -> %{owner: nil, witness: nil, fixture: nil, handler: nil, schema: nil, mode: :none, receipt: nil, armed: false, query_errors: 0, callback_error: nil} end)
    on_exit(fn -> cleanup!(state) end)
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    owner = Sandbox.start_owner!(Repo, shared: true, sandbox: false)
    Agent.update(state, &Map.put(&1, :owner, owner))

    config = Repo.config() |> Keyword.take([:hostname, :port, :database, :username, :password, :socket_dir, :ssl]) |> Keyword.put(:backoff_type, :stop)
    {:ok, witness} = Postgrex.start_link(config)
    Process.unlink(witness)
    Agent.update(state, &Map.put(&1, :witness, witness))
    assert %{rows: [[owner_backend]]} = Repo.query!("SELECT pg_backend_pid()")
    assert %{rows: [[search_path]]} = Repo.query!("SHOW search_path")
    assert %{rows: [[witness_backend]]} = Postgrex.query!(witness, "SELECT pg_backend_pid()", [])
    assert owner_backend != witness_backend
    refute Repo.in_transaction?()
    Agent.update(state, &Map.merge(&1, %{owner_backend: owner_backend, witness_backend: witness_backend, search_path: search_path}))
    state
  end

  @spec fixture!(pid(), keyword()) :: map()
  def fixture!(state, opts \\ []) do
    suffix = Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
    slug = "reset-postcommit-#{suffix}"
    model = "synthetic-reset-#{suffix}"
    Agent.update(state, &Map.merge(&1, %{slug: slug, model: model}))
    fake_name = :"reset_postcommit_fake_#{suffix}"
    Agent.update(state, &Map.put(&1, :fake_name, fake_name))
    phase = Keyword.get(opts, :phase, :pending)
    response_code = Keyword.get(opts, :response_code, "reset")
    routes = Map.new(@usage_paths, &{&1, {200, usage_payload(phase)}}) |> Map.put(@consume, {200, %{"code" => response_code}})
    {:ok, fake} = FakeUpstream.start_link({:path_json, routes}, supervisor_name: fake_name)
    Process.unlink(fake.supervisor)

    setup = GatewaySupport.gateway_setup(fake, quota?: false, pool_slug: slug, exposed_model_id: model, upstream_model_id: model)
    Agent.update(state, &Map.put(&1, :fixture, setup))
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    identity =
      setup.identity
      |> UpstreamIdentity.changeset(%{
        metadata: Map.merge(setup.identity.metadata || %{}, %{"usage_base_url" => FakeUpstream.url(fake), "saved_resets" => %{"status" => "reported", "available_count" => 2, "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => DateTime.to_iso8601(timestamp), "usage_path" => "/api/codex/usage"}}),
        saved_reset_auto_redeem_enabled: true,
        saved_reset_auto_redeem_min_blocked_minutes: 60,
        saved_reset_auto_redeem_keep_credits: 0,
        saved_reset_auto_redeem_trigger_mode: if(phase == :confirmed, do: "threshold", else: "blocked")
      })
      |> Repo.update!()

    assert {:ok, [_]} = Windows.upsert_quota_windows(identity, [%{quota_key: "account", quota_scope: "account", quota_family: "account", window_kind: "secondary", window_minutes: 10_080, used_percent: Decimal.new(if(phase == :confirmed, do: 97, else: 100)), reset_at: DateTime.add(timestamp, 7_200), observed_at: timestamp, last_sync_at: timestamp, source: "codex_usage_api", source_precision: "observed", freshness_state: "fresh"}])
    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity)
    fixture = setup |> Map.put(:identity, Repo.reload!(identity)) |> Map.put(:fake, fake) |> Map.put(:support, state)
    Agent.update(state, &Map.put(&1, :fixture, fixture))
    fixture
  end

  @spec arm!(map(), atom()) :: :ok
  def arm!(fixture, mode) do
    state = fixture.support
    schema = "reset_fault_" <> Base.encode16(:crypto.strong_rand_bytes(6), case: :lower)
    handler = {__MODULE__, make_ref()}
    Agent.update(state, &Map.merge(&1, %{schema: schema, handler: handler, mode: mode}))
    Repo.query!("CREATE SCHEMA #{schema}")
    Repo.query!("CREATE VIEW #{schema}.account_quota_windows AS SELECT 1 AS synthetic_fault_column")
    Repo.query!("CREATE VIEW #{schema}.pool_upstream_assignments AS SELECT 1 AS synthetic_fault_column")

    Repo.query!("""
    CREATE FUNCTION #{schema}.reject_probe() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.id = '#{fixture.identity.id}'::uuid
         AND OLD.metadata -> 'saved_reset_redemption' ->> 'phase' = 'consuming'
         AND NEW.metadata -> 'saved_reset_redemption' ->> 'phase' IN ('consumed_pending_probe', 'confirmed_by_quota') THEN
        IF current_setting('codex_pooler_test.reset_finalize_fault', true) = 'on' THEN
          RAISE EXCEPTION 'synthetic finalization failure';
        ELSIF current_setting('codex_pooler_test.reset_nested_publication_fault', true) = 'on' THEN
          PERFORM set_config('search_path', '#{schema}, public', false);
        ELSIF current_setting('codex_pooler_test.reset_nested_probe_fault', true) = 'on' THEN
          PERFORM set_config('codex_pooler_test.reset_probe_fault', 'on', false);
        END IF;
      END IF;
      IF NEW.id = '#{fixture.identity.id}'::uuid
         AND current_setting('codex_pooler_test.reset_probe_fault', true) = 'on'
         AND COALESCE((OLD.metadata -> 'saved_reset_redemption') ? 'probe', false) = false
         AND COALESCE((NEW.metadata -> 'saved_reset_redemption') ? 'probe', false) = true THEN
        RAISE EXCEPTION 'synthetic probe claim failure';
      END IF;
      RETURN NEW;
    END;
    $$
    """)

    Repo.query!("CREATE TRIGGER #{schema} BEFORE UPDATE ON public.upstream_identities FOR EACH ROW EXECUTE FUNCTION #{schema}.reject_probe()")

    if mode in [:precommit, :nested_publication, :nested_probe] do
      setting =
        case mode do
          :precommit -> "reset_finalize_fault"
          :nested_publication -> "reset_nested_publication_fault"
          :nested_probe -> "reset_nested_probe_fault"
        end

      Repo.query!("SELECT set_config($1, 'on', false)", ["codex_pooler_test.#{setting}"])
      Agent.update(state, &Map.put(&1, :armed, true))
    end

    :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.handle_query/4, %{state: state, identity_id: fixture.identity.id})
  end

  @spec assert_no_applied_witness!(map()) :: :ok
  def assert_no_applied_witness!(fixture) do
    snapshot = Agent.get(fixture.support, & &1)
    assert snapshot.callback_error == nil
    assert snapshot.receipt == nil
    :ok
  end

  @spec receipt!(map()) :: map()
  def receipt!(fixture) do
    state = Agent.get(fixture.support, & &1)
    assert state.callback_error == nil
    assert is_map(state.receipt)
    assert state.receipt.applied
    assert state.receipt.distinct_backends
    redemption = Repo.reload!(fixture.identity).metadata["saved_reset_redemption"]
    assert redemption["generation"] == state.receipt.generation
    assert attempt_fingerprint(redemption["attempt_id"]) == state.receipt.attempt_fingerprint
    assert redemption["result"]["applied"] == true
    state.receipt |> Map.put(:fault_armed, state.armed) |> Map.put(:query_errors, state.query_errors)
  end

  @spec restore!(map()) :: :ok
  def restore!(fixture) do
    %{handler: handler, search_path: search_path} = Agent.get(fixture.support, & &1)
    if handler, do: :telemetry.detach(handler)
    reset_session!(search_path)
    :ok
  end

  @spec emit(map(), String.t()) :: :ok
  def emit(fixture, scenario) do
    receipt = receipt!(fixture)
    TestDiagnostics.puts(Jason.encode!(Map.merge(receipt, %{scenario: scenario, consumes: FakeUpstream.physical_counts(fixture.fake).consume, postgrex_version: to_string(Application.spec(:postgrex, :vsn)), postgrex_module_md5: Base.encode16(Postgrex.module_info(:md5), case: :lower)})))
  end

  @doc false
  @spec handle_query([atom()], map(), map(), map()) :: :ok
  def handle_query(_event, _measurements, metadata, %{state: state, identity_id: identity_id}) do
    query = metadata[:query] |> to_string() |> String.trim() |> String.downcase()
    snapshot = Agent.get(state, & &1)

    cond do
      query == "commit" and is_nil(snapshot.receipt) -> witness_commit(state, snapshot, identity_id)
      later_snapshot?(snapshot, query, metadata) -> arm_later_read(state, snapshot)
      snapshot.armed && match?({:error, _}, metadata[:result]) -> Agent.update(state, &Map.update!(&1, :query_errors, fn count -> count + 1 end))
      true -> :ok
    end

    :ok
  rescue
    exception -> Agent.update(state, &Map.put(&1, :callback_error, exception.__struct__))
  end

  defp later_snapshot?(%{receipt: %{}, armed: false, mode: :later_snapshot}, query, %{result: {:ok, _result}}),
    do: String.contains?(query, "join \"account_quota_windows\"")

  defp later_snapshot?(_snapshot, _query, _metadata), do: false

  defp witness_commit(state, snapshot, identity_id) do
    assert %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
    assert backend == snapshot.owner_backend
    %{rows: [[redemption]]} = Postgrex.query!(snapshot.witness, "SELECT metadata->'saved_reset_redemption' FROM upstream_identities WHERE id = $1::uuid", [Ecto.UUID.dump!(identity_id)])

    if is_map(redemption) and get_in(redemption, ["result", "applied"]) == true and redemption["phase"] in ["consumed_pending_probe", "confirmed_by_quota"] do
      assert get_in(redemption, ["result", "code"]) in ["reset", "already_redeemed", "target_redeemed"]
      assert {:ok, _} = Ecto.UUID.cast(redemption["attempt_id"])
      assert is_integer(redemption["generation"])
      assert redemption["generation"] >= 0
      assert {:ok, consumed_at, 0} = DateTime.from_iso8601(redemption["consumed_at"])
      assert DateTime.compare(consumed_at, DateTime.utc_now()) != :gt
      assert get_in(redemption, ["provider_replay", "provider_dispatches"]) == 1
      receipt = %{applied: true, phase: redemption["phase"], generation: redemption["generation"], attempt_fingerprint: attempt_fingerprint(redemption["attempt_id"]), dispatches: 1, distinct_backends: snapshot.owner_backend != snapshot.witness_backend, outer_commit_witness: true}
      Agent.update(state, &Map.put(&1, :receipt, receipt))
      if snapshot.mode not in [:none, :later_snapshot], do: activate_fault(state, snapshot)
    end
  end

  defp arm_later_read(state, snapshot) do
    Agent.update(state, &Map.update!(&1, :receipt, fn receipt -> Map.put(receipt, :successful_snapshots_before_fault, 1) end))
    activate_fault(state, %{snapshot | mode: :snapshot})
  end

  defp activate_fault(state, %{mode: :probe_write}) do
    Repo.query!("SELECT set_config('codex_pooler_test.reset_probe_fault', 'on', false)")
    Agent.update(state, &Map.put(&1, :armed, true))
  end

  defp activate_fault(state, snapshot) do
    # Only the relevant shadow name exists in the active schema. The other
    # view is removed before changing this exact checked-out session's path.
    other = if snapshot.mode == :publication, do: "account_quota_windows", else: "pool_upstream_assignments"
    Repo.query!("DROP VIEW #{snapshot.schema}.#{other}")
    Repo.query!("SELECT set_config('search_path', $1, false)", ["#{snapshot.schema}, public"])
    Agent.update(state, &Map.put(&1, :armed, true))
  end

  defp usage_payload(phase) do
    timestamp = DateTime.utc_now() |> DateTime.truncate(:second)
    %{"plan_type" => "pro", "rate_limit_reset_credits" => %{"available_count" => 1}, "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"}, "spend_control" => %{"reached" => false}, "rate_limit" => %{"allowed" => true, "limit_reached" => false, "primary_window" => %{"used_percent" => if(phase == :confirmed, do: 98, else: 10), "limit_window_seconds" => 604_800, "reset_after_seconds" => if(phase == :confirmed, do: 14_400, else: 900), "reset_at" => timestamp |> DateTime.add(if(phase == :confirmed, do: 14_400, else: 900)) |> DateTime.to_unix()}}}
  end

  defp cleanup!(state) do
    snapshot = Agent.get(state, & &1)
    if snapshot.handler, do: :telemetry.detach(snapshot.handler)

    try do
      cleanup_database!(snapshot)
    after
      stop_fake!(snapshot[:fake_name])
      stop_owned_process!(snapshot.witness, &GenServer.stop/1)
      stop_owned_process!(snapshot.owner, &Sandbox.stop_owner/1)
      Agent.stop(state)
    end
  end

  defp cleanup_database!(%{owner: nil}), do: :ok

  defp cleanup_database!(snapshot) do
    if snapshot[:search_path], do: reset_session!(snapshot.search_path)

    if snapshot.schema do
      Repo.query!("DROP TRIGGER IF EXISTS #{snapshot.schema} ON public.upstream_identities")
      Repo.query!("DROP SCHEMA IF EXISTS #{snapshot.schema} CASCADE")
      assert %{rows: [[0]]} = Postgrex.query!(snapshot.witness, "SELECT count(*) FROM pg_namespace WHERE nspname = $1", [snapshot.schema])
    end

    cleanup_fixture(snapshot)
  end

  defp stop_fake!(nil), do: :ok

  defp stop_fake!(name) do
    stop_owned_process!(Process.whereis(name), fn pid -> FakeUpstream.stop(%FakeUpstream{supervisor: pid}) end)
    assert Process.whereis(name) == nil
  end

  defp stop_owned_process!(nil, _stop), do: :ok

  defp stop_owned_process!(pid, stop) do
    monitor = Process.monitor(pid)

    try do
      stop.(pid)
    catch
      :exit, _reason -> :ok
    end

    assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, 2_000
    :ok
  end

  defp cleanup_fixture(%{fixture: %{} = fixture}), do: GatewaySupport.cleanup_unboxed_pool!(fixture)

  defp cleanup_fixture(%{slug: slug, model: model}) do
    case Repo.get_by(Pool, slug: slug) do
      nil ->
        :ok

      pool ->
        identities = Repo.all(from i in UpstreamIdentity, join: a in CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment, on: a.upstream_identity_id == i.id, where: a.pool_id == ^pool.id)
        delete_committed_pools!([pool.id])
        Enum.each(identities, &Repo.delete!/1)
    end

    Repo.delete_all(from p in PricingSnapshot, where: p.model_identifier == ^model)
  end

  defp cleanup_fixture(_snapshot), do: :ok

  defp reset_session!(search_path) do
    Repo.query!("SELECT set_config('search_path', $1, false), set_config('codex_pooler_test.reset_probe_fault', '', false), set_config('codex_pooler_test.reset_finalize_fault', '', false), set_config('codex_pooler_test.reset_nested_publication_fault', '', false), set_config('codex_pooler_test.reset_nested_probe_fault', '', false)", [search_path])
  end

  defp attempt_fingerprint(attempt_id), do: :crypto.hash(:sha256, attempt_id) |> Base.encode16(case: :lower) |> binary_part(0, 12)
end
