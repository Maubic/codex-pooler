defmodule CodexPooler.ProviderCreditsFixtures do
  @moduledoc """
  Provider-free, committed fixtures and causal read barriers for transport tests.

  `request_context/4` carries the actual request scope without granting credit authority.
  Parsed synthetic provider permission exercises admission, not real provider billing.
  Each opened fixture owns its rows, loopback listener, peer and cleanup keeper.
  """

  import Ecto.Query
  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]

  alias CodexPooler.Accounting.RequestReplayEntitlement
  alias CodexPooler.Catalog.PricingSnapshot
  alias CodexPooler.{FakeUpstream, InstancePresencePeer, PeerRegistry, PoolerFixtures, Repo, UnboxedFixture}
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Quotas.Evidence.CodexParsers
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{AccountAvailabilityStore, CapacityFactsStore, CreditBalanceStore, RoutingQuotaSnapshot, Windows}
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, as: PeerSupport

  @budget 15_000
  @peer_boot_timeout 60_000
  @hold_timeout 60_000
  @states [:included, :weekly_credit_only, :windowless_credit_only, :windowless_included, :windowless_unknown, :legacy_windowless]
  @usage_paths ["/api/codex/usage", "/backend-api/codex/usage", "/wham/usage", "/backend-api/wham/usage"]
  @generation_path "/backend-api/codex/responses"

  @type t :: %{required(:keeper) => pid(), required(:pool_ids) => [Ecto.UUID.t()], required(:identity_ids) => [Ecto.UUID.t()], required(:pools) => map(), required(:identities) => map(), required(:assignments) => map(), required(:upstream) => FakeUpstream.t(), required(:peer) => map(), required(:now) => DateTime.t(), optional(atom()) => term()}
  @type barrier :: %{required(:fixture) => t(), required(:ref) => reference(), required(:phase) => :before_final_read | :after_final_read, required(:identity_id) => Ecto.UUID.t(), optional(atom()) => term()}

  @doc "Opens test-owned committed rows and an independent real-Repo peer; cleanup is registered first."
  @spec open!(keyword()) :: t()
  def open!(opts \\ []) do
    assert Mix.env() == :test, "provider-credit fixtures require the test environment"
    assert Repo.config()[:pool] == Ecto.Adapters.SQL.Sandbox, "provider-credit fixtures require the isolated test Repo"
    PeerSupport.ensure_test_distribution_started!()
    suffix = System.unique_integer([:positive, :monotonic])
    keeper_name = String.to_atom("provider_credits_fixture_#{suffix}")
    upstream_name = String.to_atom("provider_credits_upstream_#{suffix}")
    pool_ids = [Ecto.UUID.generate(), Ecto.UUID.generate()]
    identity_ids = Map.new(@states, &{&1, Ecto.UUID.generate()})
    owned = %{keeper_name: keeper_name, upstream_name: upstream_name, pool_ids: pool_ids, identity_ids: Map.values(identity_ids)}
    on_exit(fn -> close_owned!(owned) end)
    {:ok, keeper} = Agent.start(fn -> %{upstream: nil, peer: nil, connections: [], workers: [], held: %{}, handlers: [], pricing_ids: [], claimed: MapSet.new()} end, name: keeper_name)
    now = Keyword.get(opts, :now, DateTime.utc_now())
    mode = Keyword.get(opts, :mode, {:path_json, usage_routes(usage_payload(:included, now: now))})
    {:ok, upstream} = FakeUpstream.start_link(mode, supervisor_name: upstream_name)
    Process.unlink(upstream.supervisor)
    Agent.update(keeper, &%{&1 | upstream: upstream})
    rows = UnboxedFixture.run_unboxed(fn -> create_rows!(pool_ids, identity_ids, now, FakeUpstream.url(upstream), opts) end)
    fixture = Map.merge(owned, Map.merge(rows, %{keeper: keeper, upstream: upstream, now: now}))
    peer = start_peer!(fixture, suffix)
    Map.put(fixture, :peer, peer)
  end

  @doc "Stops barriers, workers, peer and listener before deleting only the owned committed graph."
  @spec close!(t()) :: :ok
  def close!(fixture), do: close_owned!(fixture)

  @doc "A fixed synthetic complete usage shape; time-dependent resets are derived from one supplied clock."
  @spec usage_payload(atom(), keyword()) :: map()
  def usage_payload(state, opts \\ []) do
    now = Keyword.get(opts, :now, DateTime.utc_now())
    reset_at = DateTime.add(now, Keyword.get(opts, :reset_after, 7_200), :second)
    window = Keyword.get(opts, :window, window_shape(state))
    included? = state in [:included, :windowless_included, :spend_blocked]
    rate_limit = %{"allowed" => included?, "limit_reached" => not included?}

    rate_limit =
      Enum.reduce(window_descriptors(window), rate_limit, fn {key, minutes}, rate ->
        Map.put(rate, key, %{"limit_window_seconds" => minutes * 60, "used_percent" => if(included?, do: 12, else: 100), "reset_after_seconds" => max(DateTime.diff(reset_at, now, :second), 0), "reset_at" => DateTime.to_unix(reset_at)})
      end)

    payload = %{"plan_type" => "synthetic", "rate_limit" => rate_limit, "spend_control" => %{"reached" => state == :spend_blocked}, "credits" => credit_payload(Keyword.get(opts, :credits, :full))}
    payload = if Keyword.get(opts, :credits) == :unknown, do: Map.delete(payload, "credits"), else: payload
    payload = if included?, do: payload, else: Map.put(payload, "rate_limit_reached_type", %{"type" => "rate_limit_reached"})

    case state do
      :workspace_blocked -> Map.put(payload, "rate_limit_reached_type", %{"type" => "workspace_owner_usage_limit_reached"})
      :malformed_spend -> Map.put(payload, "spend_control", %{"reached" => "not-a-boolean"})
      unknown when unknown in [:unknown, :windowless_unknown] -> Map.drop(payload, ["rate_limit", "spend_control", "credits", "rate_limit_reached_type"])
      _state -> payload
    end
  end

  @spec usage_routes(map()) :: map()
  def usage_routes(payload), do: Map.new(@usage_paths, &{&1, {200, payload}})

  @spec reset_inventory_payload(non_neg_integer(), keyword()) :: map()
  def reset_inventory_payload(count, opts \\ []) when is_integer(count) and count >= 0 do
    expires_at = DateTime.add(Keyword.get(opts, :now, DateTime.utc_now()), Keyword.get(opts, :expires_after, 86_400), :second)
    credits = if count == 0, do: [], else: Enum.map(1..count, &%{"id" => "synthetic_reset_#{&1}", "status" => "available", "expires_at" => DateTime.to_iso8601(expires_at)})
    %{"available_count" => count, "credits" => credits}
  end

  @doc "Actual request scope for pure evaluate/2; provider authority comes from the snapshot."
  @spec request_context(String.t(), :full | :lite, atom(), atom()) :: map()
  def request_context(model, mode, transport, _window \\ :weekly) do
    %{model: model, requested_model: model, upstream_model: model, serving_mode: mode, transport: transport}
  end

  @spec snapshot!(t(), atom()) :: RoutingQuotaSnapshot.t()
  def snapshot!(fixture, state) do
    id = Map.fetch!(fixture.identities, state).id
    UnboxedFixture.run_unboxed(fn -> Map.fetch!(RoutingQuotaSnapshot.load_by_identity_ids([id], DateTime.utc_now()), id) end)
  end

  @doc "Creates an authenticated runtime setup in either owned Pool, without qualifying credits."
  @spec runtime_setup!(t(), :a | :b, atom(), keyword()) :: map()
  def runtime_setup!(fixture, pool_key, state, opts \\ []) do
    UnboxedFixture.run_unboxed(fn ->
      pool = Map.fetch!(fixture.pools, pool_key)
      identity = Repo.get!(UpstreamIdentity, Map.fetch!(fixture.identities, state).id)
      assignment = Map.fetch!(fixture.assignments, {pool_key, state})
      key = PoolerFixtures.active_api_key_fixture(pool)
      model = PoolerFixtures.model_fixture(pool, %{exposed_model_id: Keyword.get(opts, :model, "synthetic-credits-#{pool.id}"), upstream_model_id: Keyword.get(opts, :upstream_model, "synthetic-credits-#{pool.id}"), supports_responses: true, supports_streaming: true, metadata: %{"source_assignment_ids" => [assignment.id], "source_assignment_models" => %{assignment.id => %{"slug" => Keyword.get(opts, :upstream_model, "synthetic-credits-#{pool.id}"), "use_responses_lite" => false}}}})
      pricing = BackendCodexTestSupport.pricing_snapshot!(model)
      Agent.update(fixture.keeper, &%{&1 | pricing_ids: [pricing.id | &1.pricing_ids]})
      Map.merge(key, %{identity: identity, assignment: assignment, model: model, pricing: pricing})
    end)
  end

  @doc "Commits policy directly on owned fixture rows, optionally on the independent peer Repo."
  @spec commit_policy!(t(), atom(), boolean(), node() | nil) :: DateTime.t()
  def commit_policy!(fixture, state, enabled, on_node \\ nil) when is_boolean(enabled) do
    id = Map.fetch!(fixture.identities, state).id

    if on_node do
      :erpc.call(on_node, __MODULE__, :commit_policy_row!, [id, enabled], @budget)
    else
      UnboxedFixture.run_unboxed(fn -> commit_policy_row!(id, enabled) end)
    end
  end

  @doc false
  @spec commit_policy_row!(Ecto.UUID.t(), boolean()) :: DateTime.t()
  def commit_policy_row!(id, enabled) do
    {1, _} = Repo.update_all(from(identity in UpstreamIdentity, where: identity.id == ^id), set: [allow_provider_credits: enabled])
    %{rows: [[committed_at]]} = Repo.query!("SELECT clock_timestamp()")
    committed_at
  end

  @doc "Holds the selected identity relation before a read; supply the final reader's PostgreSQL backend pid."
  @spec before_read_barrier!(t(), Ecto.UUID.t(), keyword()) :: barrier()
  def before_read_barrier!(fixture, identity_id, opts) do
    assert identity_id in fixture.identity_ids
    backend_pid = Keyword.get(opts, :backend_pid)
    connection = start_connection!(fixture)
    Postgrex.query!(connection, "BEGIN", [])

    relation =
      case Keyword.get(opts, :read_relation, :upstream_identities) do
        :upstream_identities -> "upstream_identities"
        :account_quota_windows -> "account_quota_windows"
      end

    Postgrex.query!(connection, "LOCK TABLE #{relation} IN ACCESS EXCLUSIVE MODE", [])
    %{rows: [[holder_pid]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
    %{fixture: fixture, ref: make_ref(), phase: :before_final_read, identity_id: identity_id, connection: connection, backend_pid: backend_pid, holder_pid: holder_pid, query_predicate: Keyword.get(opts, :query_predicate, &snapshot_query?/1)}
  end

  @doc "Observes the exact final SELECT blocked on the owned holder, not elapsed time or an empty mailbox."
  @spec await_before_read!(barrier()) :: map()
  def await_before_read!(barrier) do
    await_blocked_read!(barrier, System.monotonic_time(:millisecond) + @budget)
  end

  @doc "Commits a fixture-only policy update on the holder, releasing the blocked read atomically."
  @spec commit_policy_and_release!(barrier(), boolean()) :: DateTime.t()
  def commit_policy_and_release!(barrier, enabled) when is_boolean(enabled) do
    %{num_rows: 1} = Postgrex.query!(barrier.connection, "UPDATE upstream_identities SET allow_provider_credits = $1 WHERE id = $2", [enabled, Ecto.UUID.dump!(barrier.identity_id)])
    Postgrex.query!(barrier.connection, "COMMIT", [])
    %{rows: [[committed_at]]} = Postgrex.query!(barrier.connection, "SELECT clock_timestamp()", [])
    committed_at
  end

  @doc "One-shot synchronous Repo telemetry hold after the exact identity's final read and before caller evaluation/send."
  @spec after_read_barrier!(t(), Ecto.UUID.t(), keyword()) :: barrier()
  def after_read_barrier!(fixture, identity_id, opts \\ []) do
    assert identity_id in fixture.identity_ids
    target_node = Keyword.get(opts, :node, node())
    ref = make_ref()
    handler = {__MODULE__, ref}
    config = %{keeper: fixture.keeper, ref: ref, notify: self(), identity_id: identity_id, emitter: Keyword.get(opts, :emitter), query_predicate: Keyword.get(opts, :query_predicate, &snapshot_query?/1)}
    Agent.update(fixture.keeper, &%{&1 | handlers: [{target_node, handler} | &1.handlers]})
    assert :ok = on_node(target_node, :telemetry, :attach, [handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_read/4, config])
    %{fixture: fixture, ref: ref, phase: :after_final_read, identity_id: identity_id}
  end

  @doc false
  @spec hold_read(term(), map(), map(), map()) :: :ok
  def hold_read(_event, _measurements, metadata, config) do
    params = Map.get(metadata, :params, []) |> List.flatten()
    identity? = config.identity_id in params or Ecto.UUID.dump!(config.identity_id) in params
    emitter? = is_nil(config.emitter) or self() == config.emitter

    if identity? and emitter? and config.query_predicate.(metadata.query) and claim_barrier(config.keeper, config.ref, self()) do
      send(config.notify, {:provider_credits_barrier, config.ref, :after_final_read, self()})

      receive do
        {:provider_credits_release, ref} when ref == config.ref -> :ok
      after
        @hold_timeout -> raise "provider-credit final-read barrier release missing"
      end

      Agent.update(config.keeper, &%{&1 | held: Map.delete(&1.held, config.ref)})
      send(config.notify, {:provider_credits_barrier_released, config.ref, self()})
    end

    :ok
  end

  @spec release_barrier!(barrier()) :: :ok
  def release_barrier!(%{phase: :before_final_read} = barrier) do
    Postgrex.query!(barrier.connection, "ROLLBACK", [])
    :ok
  end

  def release_barrier!(%{fixture: fixture, ref: ref}) do
    pid = Agent.get(fixture.keeper, &Map.fetch!(&1.held, ref))
    send(pid, {:provider_credits_release, ref})
    :ok
  end

  @doc "The coherent snapshot query predicate; a caller can narrow it further for a specific final gate."
  @spec snapshot_query?(String.t()) :: boolean()
  def snapshot_query?(query) when is_binary(query) do
    String.starts_with?(query, "SELECT ") and String.contains?(query, ~s("upstream_identities")) and String.contains?(query, ~s("account_quota_windows")) and String.contains?(query, ~s("allow_provider_credits"))
  end

  @doc "Starts a monitored compiled MFA worker on the real peer; close!/1 owns failure cancellation."
  @spec start_peer_work!(t(), {module(), atom(), [term()]}) :: %{pid: pid(), monitor: reference(), ref: reference()}
  def start_peer_work!(fixture, {module, function, args} = work) when is_atom(module) and is_atom(function) and is_list(args) do
    ref = make_ref()

    pid =
      case :erpc.call(fixture.peer.node, __MODULE__, :spawn_work, [self(), ref, work], @budget) do
        pid when is_pid(pid) -> pid
        _invalid -> raise "owned peer work did not return a PID"
      end

    monitor = Process.monitor(pid)
    Agent.update(fixture.keeper, &%{&1 | workers: [pid | &1.workers]})
    send(pid, {:provider_credits_start_work, ref})
    %{pid: pid, monitor: monitor, ref: ref}
  end

  @doc false
  @spec spawn_work(pid(), reference(), {module(), atom(), [term()]}) :: pid()
  def spawn_work(notify, ref, {module, function, args}) do
    spawn(fn ->
      receive do
        {:provider_credits_start_work, ^ref} -> send(notify, {:provider_credits_work, ref, apply(module, function, args)})
      after
        @hold_timeout -> raise "provider-credit peer worker start missing"
      end
    end)
  end

  @doc "Fixture read workload, not a production admission check or provider qualification."
  @spec read_then_generation(Ecto.UUID.t(), String.t(), keyword()) :: {:sent, non_neg_integer()} | {:unsent, false}
  def read_then_generation(identity_id, url, opts \\ []) do
    Repo.checkout(fn ->
      if wait = Keyword.get(opts, :wait_before_read) do
        {notify, ref} = wait
        %{rows: [[backend_pid]]} = Repo.query!("SELECT pg_backend_pid()")
        send(notify, {:fixture_reader_ready, ref, self(), backend_pid})

        receive do
          {:read, ^ref} -> :ok
        after
          @hold_timeout -> raise "provider-credit fixture read release missing"
        end
      end

      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([identity_id], DateTime.utc_now())[identity_id]
      if snapshot.allow_provider_credits or Keyword.get(opts, :ignore_policy, false), do: {:sent, http_generation!(url)}, else: {:unsent, false}
    end)
  end

  @doc "Performs one real loopback Req generation; no boundary replacement or synthetic grant is involved."
  @spec http_generation!(String.t(), String.t()) :: non_neg_integer()
  def http_generation!(url, model \\ "synthetic-credits-model") do
    {:ok, _} = Application.ensure_all_started(:req)
    %{status: status} = Req.post!(url <> @generation_path, json: %{"model" => model, "input" => [], "stream" => false}, retry: false)
    status
  end

  @doc "Performs one real Mint websocket generation and consumes its terminal before closing."
  @spec websocket_generation!(String.t(), String.t()) :: String.t()
  def websocket_generation!(url, model \\ "synthetic-credits-model") do
    uri = URI.parse(url)
    {:ok, _} = Application.ensure_all_started(:mint_web_socket)
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", uri.port, protocols: [:http1])
    socket = Mint.HTTP.get_socket(conn)

    try do
      {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @generation_path, [])
      {:ok, conn, status, headers} = BackendCodexTestSupport.await_public_websocket_upgrade(conn, ref)
      {conn, websocket} = BackendCodexTestSupport.mint_websocket_new!(conn, ref, status, headers)
      payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => model, "input" => []})
      {conn, websocket} = BackendCodexTestSupport.public_websocket_send_text!(conn, websocket, ref, payload)
      {_conn, _websocket, terminal} = BackendCodexTestSupport.public_websocket_receive_text!(conn, websocket, ref)
      %{"type" => type} = CodexPooler.JSON.decode!(terminal)
      type
    after
      :gen_tcp.close(socket)
    end
  end

  defp create_rows!(pool_ids, identity_ids, now, url, opts) do
    {:ok, rows} =
      Repo.transaction(fn ->
        pools = Map.new(Enum.zip([:a, :b], pool_ids), fn {key, id} -> {key, Repo.insert!(%Pool{id: id, slug: "credits-#{id}", name: "Synthetic Pool #{key}", status: "active", created_at: now, updated_at: now})} end)

        identities =
          Map.new(identity_ids, fn {state, id} ->
            identity = Repo.insert!(%UpstreamIdentity{id: id, chatgpt_account_id: "synthetic-#{id}", account_label: "Synthetic #{state}", onboarding_method: "import", status: "active", headers_profile_version: 1, credential_provenance: "codex_chatgpt_oauth", auth_verified_at: now, auth_fresh_at: now, created_at: now, updated_at: now, allow_provider_credits: Keyword.get(opts, :allow_provider_credits, false), metadata: CredentialFencing.initialize_metadata(%{"base_url" => url, "usage_base_url" => url})})
            {:ok, _secret} = Upstreams.store_encrypted_secret(identity, %{secret_kind: "access_token", plaintext: "synthetic-fixture-token"})
            {state, initialize_capacity!(identity, state, now)}
          end)

        assignments =
          for {pool_key, pool} <- pools, {state, identity} <- identities, into: %{} do
            assignment = Repo.insert!(%PoolUpstreamAssignment{pool_id: pool.id, upstream_identity_id: identity.id, assignment_label: "Synthetic #{state}", status: "active", health_status: "active", eligibility_status: "eligible", created_at: now, updated_at: now, metadata: %{"base_url" => url, "usage_base_url" => url}})
            {{pool_key, state}, assignment}
          end

        %{pools: pools, identities: identities, assignments: assignments}
      end)

    rows
  end

  defp initialize_capacity!(identity, :legacy_windowless, now) do
    epoch = CredentialFencing.credential_epoch(identity)
    metadata = Map.put(identity.metadata, AccountAvailabilityStore.metadata_key(), AccountAvailabilityStore.encode!(:available, now, epoch))
    Repo.update!(Ecto.Changeset.change(identity, metadata: metadata))
  end

  defp initialize_capacity!(identity, state, now), do: persist_usage!(identity, usage_payload(state, now: now), now)

  @doc "Persists one complete synthetic observation with the real parser, window store and fenced codecs."
  @spec persist_usage!(UpstreamIdentity.t(), map(), DateTime.t()) :: UpstreamIdentity.t()
  def persist_usage!(identity, payload, now) do
    {:ok, parsed} = CodexParsers.parse_codex_usage_result(payload, now)
    windows = Enum.map(parsed.windows, &Map.from_struct/1)
    {:ok, _} = Windows.upsert_quota_windows(identity, windows, delete_missing?: true, broadcast?: false)
    identity = Repo.reload!(identity)
    epoch = CredentialFencing.credential_epoch(identity)
    metadata = identity.metadata |> AccountAvailabilityStore.transition(parsed.account_availability, now, epoch) |> CreditBalanceStore.transition(payload, now, epoch) |> CapacityFactsStore.transition(parsed.capacity_facts, epoch)
    Repo.update!(Ecto.Changeset.change(identity, metadata: metadata))
  end

  defp credit_payload(:full), do: %{"balance" => "25", "has_credits" => true, "unlimited" => false}
  defp credit_payload(:fractional), do: %{"balance" => "0.024", "has_credits" => true, "unlimited" => false}
  defp credit_payload(:unlimited), do: %{"has_credits" => true, "unlimited" => true}
  defp credit_payload(:none), do: %{"balance" => "0", "has_credits" => false, "unlimited" => false}
  defp credit_payload(:unknown), do: %{}
  defp window_shape(:short_credit_only), do: :short
  defp window_shape(:monthly_credit_only), do: :monthly
  defp window_shape(:mixed_credit_only), do: :mixed
  defp window_shape(state) when state in [:windowless_credit_only, :windowless_included, :windowless_unknown], do: :absent
  defp window_shape(_state), do: :weekly
  defp window_descriptors(:weekly), do: [{"secondary_window", 10_080}]
  defp window_descriptors(:short), do: [{"primary_window", 300}]
  defp window_descriptors(:monthly), do: [{"primary_window", 43_200}]
  defp window_descriptors(:mixed), do: [{"primary_window", 300}, {"secondary_window", 10_080}]
  defp window_descriptors(:absent), do: []

  defp start_peer!(fixture, suffix) do
    name = String.to_atom("provider_credits_peer_#{suffix}")
    {:ok, pid, peer_node} = :peer.start_link(%{name: name, args: [~c"+S", ~c"2:2", ~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]})
    Process.unlink(pid)
    boot_id = Ecto.UUID.generate()
    peer = %{pid: pid, node: peer_node, name: name, boot_id: boot_id, os_identity: nil}
    Agent.update(fixture.keeper, &%{&1 | peer: peer})
    :ok = :erpc.call(peer_node, :code, :add_paths, [:code.get_path()], @budget)
    signing_config = Application.fetch_env!(:codex_pooler, CodexPoolerWeb.Endpoint) |> Keyword.take([:secret_key_base])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, CodexPoolerWeb.Endpoint, signing_config], @budget)
    upstream_config = Application.get_env(:codex_pooler, Upstreams, [])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, Upstreams, upstream_config], @budget)
    os_pid = :erpc.call(peer_node, System, :pid, [], @budget)
    os_identity = InstancePresencePeer.capture_os_process_identity!(os_pid, budget_ms: @budget)
    peer = %{peer | os_identity: os_identity}
    Agent.update(fixture.keeper, &%{&1 | peer: peer})
    repo_config = Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, pool_size: 2, parameters: [application_name: InstancePresencePeer.peer_application_name(boot_id)])
    :ok = :erpc.call(peer_node, __MODULE__, :start_peer_runtime!, [repo_config], @peer_boot_timeout)
    %{rows: [[1]]} = :erpc.call(peer_node, Repo, :query!, ["SELECT 1"], @budget)
    peer
  end

  @doc false
  @spec start_peer_runtime!(keyword()) :: :ok
  def start_peer_runtime!(repo_config) do
    {:ok, _runtime} = WebsocketOwnerNodeHarness.start_owner_runtime()
    _repo = WebsocketOwnerNodeHarness.start_repo(repo_config)
    {:ok, _} = Application.ensure_all_started(:req)
    {:ok, _} = Application.ensure_all_started(:mint_web_socket)
    {:ok, _} = Application.ensure_all_started(:phoenix_pubsub)
    {:ok, _} = Application.ensure_all_started(:ex_unit)
    {:ok, pubsub} = Supervisor.start_link([{Phoenix.PubSub, name: CodexPooler.PubSub}], strategy: :one_for_one)
    Process.unlink(pubsub)
    :ok
  end

  defp start_connection!(fixture) do
    config = Keyword.take(Repo.config(), [:username, :password, :hostname, :port, :database, :ssl, :socket_dir])
    {:ok, connection} = Postgrex.start_link(config)
    Process.unlink(connection)
    Agent.update(fixture.keeper, &%{&1 | connections: [connection | &1.connections]})
    connection
  end

  defp claim_barrier(keeper, ref, pid) do
    Agent.get_and_update(keeper, fn state ->
      if MapSet.member?(state.claimed, ref), do: {false, state}, else: {true, %{state | claimed: MapSet.put(state.claimed, ref), held: Map.put(state.held, ref, pid)}}
    end)
  end

  defp await_blocked_read!(barrier, deadline) do
    Postgrex.query!(barrier.connection, "SELECT pg_stat_clear_snapshot()", [])
    %{rows: rows} = Postgrex.query!(barrier.connection, "SELECT pid, query, pg_blocking_pids(pid) FROM pg_stat_activity WHERE ($1::integer IS NULL OR pid = $1) AND datname = current_database() AND wait_event_type = 'Lock'", [barrier.backend_pid])
    matching = Enum.filter(rows, fn [_pid, query, blockers] -> barrier.holder_pid in blockers and barrier.query_predicate.(query) end)

    case matching do
      [[pid, _query, _blockers]] -> %{backend_pid: pid, holder_pid: barrier.holder_pid, phase: :before_final_read}
      _not_yet_blocked -> await_read_sample!(barrier, deadline)
    end
  end

  defp await_read_sample!(barrier, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)
    assert remaining > 0, "final snapshot SELECT never blocked on its owned relation-read fence backend=#{barrier.backend_pid}"

    receive do
    after
      min(10, remaining) -> await_blocked_read!(barrier, deadline)
    end
  end

  defp close_owned!(owned) do
    case Process.whereis(owned.keeper_name) do
      nil ->
        :ok

      keeper ->
        state = Agent.get(keeper, & &1)
        Enum.each(state.workers, &stop_process!/1)
        Enum.each(state.held, fn {ref, pid} -> send(pid, {:provider_credits_release, ref}) end)
        Enum.each(state.connections, &stop_process!/1)
        Enum.each(state.handlers, &detach_owned_handler/1)
        stop_peer!(state.peer)
        if state.upstream, do: FakeUpstream.stop(state.upstream)
        if pid = Process.whereis(owned.upstream_name), do: stop_process!(pid)

        UnboxedFixture.run_unboxed(fn -> delete_owned_rows!(owned, state) end, InstancePresencePeer.cleanup_timeout_ms(@budget))

        Agent.stop(keeper)
    end

    :ok
  end

  defp detach_owned_handler({target, handler}) do
    if target == node() or target in Node.list(:connected), do: on_node(target, :telemetry, :detach, [handler])
  end

  defp delete_owned_rows!(owned, state) do
    if state.peer, do: InstancePresencePeer.purge_peer_state!(state.peer.boot_id, fn -> :ok end)
    Enum.each(owned.pool_ids, &PeerSupport.stop_pool_owners!(%{id: &1}))
    Repo.delete_all(from(entitlement in RequestReplayEntitlement, where: entitlement.pool_id in ^owned.pool_ids))
    PoolerFixtures.delete_committed_pools!(owned.pool_ids)
    Repo.delete_all(from(identity in UpstreamIdentity, where: identity.id in ^owned.identity_ids))
    Repo.delete_all(from(pricing in PricingSnapshot, where: pricing.id in ^state.pricing_ids))
    assert Repo.aggregate(from(pool in Pool, where: pool.id in ^owned.pool_ids), :count) == 0
    assert Repo.aggregate(from(identity in UpstreamIdentity, where: identity.id in ^owned.identity_ids), :count) == 0
  end

  defp stop_peer!(nil), do: :ok

  defp stop_peer!(peer) do
    if Process.alive?(peer.pid), do: :peer.stop(peer.pid)
    PeerRegistry.assert_peer_absent!(peer.name, peer_node: peer.node, budget_ms: @budget)
    if peer.os_identity, do: InstancePresencePeer.assert_os_process_stopped!(peer.os_identity, budget_ms: @budget)
  end

  defp stop_process!(pid) do
    monitor = Process.monitor(pid)
    alive? = if node(pid) == node(), do: Process.alive?(pid), else: node(pid) in Node.list(:connected) and on_node(node(pid), Process, :alive?, [pid])
    if alive?, do: Process.exit(pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^pid, _}, @budget
    :ok
  end

  defp on_node(target, module, function, args) when target == node(), do: apply(module, function, args)
  defp on_node(target, module, function, args), do: :erpc.call(target, module, function, args, @budget)
end
