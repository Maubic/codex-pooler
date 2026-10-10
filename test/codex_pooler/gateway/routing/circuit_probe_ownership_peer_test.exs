defmodule CodexPooler.Gateway.Routing.CircuitProbeOwnershipPeerTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.{Access, Accounting, CircuitProbePeer, InstancePresencePeer, PeerRegistry, TestDiagnostics, UnboxedFixture}
  alias CodexPooler.Accounting.{LedgerEntry, Request}
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Routing.{CircuitHealth, CircuitState}
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, RouteState}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @budget 15_000

  setup_all do
    name = PeerRegistry.unique_node_name("circuit_probe")
    {:ok, cleanup_state} = Agent.start(fn -> nil end)

    on_exit(fn ->
      case Agent.get(cleanup_state, & &1) do
        nil ->
          :ok

        %{peer: peer, os_identity: os_identity, boot_id: boot_id} ->
          try do
            :peer.stop(peer.peer)
          catch
            :exit, _ -> :ok
          end

          InstancePresencePeer.assert_os_process_stopped!(os_identity)
          UnboxedFixture.run_unboxed(fn -> InstancePresencePeer.purge_peer_state!(boot_id, fn -> :ok end) end, InstancePresencePeer.cleanup_timeout_ms(@budget))
          TestDiagnostics.puts(Jason.encode!(%{scenario: "peer_cleanup", node: peer.remote, os_process_stopped: true, database_backends_absent: true}))
      end

      Agent.stop(cleanup_state)
    end)

    peer = InstancePresencePeer.start_presence_peer!(name)
    settings = %OperationalSettings{circuit_failure_threshold: 1, circuit_half_open_probe_limit: 1}
    boot_id = Ecto.UUID.generate()
    os_identity = InstancePresencePeer.capture_os_process_identity!(:erpc.call(peer.remote, System, :pid, []))
    Agent.update(cleanup_state, fn _ -> %{peer: peer, os_identity: os_identity, boot_id: boot_id} end)
    runtime = :erpc.call(peer.remote, CircuitProbePeer, :bootstrap, [Repo.config(), settings, boot_id])
    TestDiagnostics.puts(Jason.encode!(%{scenario: "peer_acquired", node: runtime.node, os_pid: runtime.os_pid, backend: runtime.backend}))
    %{peer: peer.remote, peer_runtime: runtime}
  end

  setup context do
    CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
    Application.put_env(:codex_pooler, OperationalSettings, settings: %OperationalSettings{circuit_failure_threshold: 1, circuit_half_open_probe_limit: 1})
    :ok = :erpc.call(context.peer, Application, :put_env, [:codex_pooler, OperationalSettings, [settings: %OperationalSettings{circuit_failure_threshold: 1, circuit_half_open_probe_limit: 1}]])
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    slug = "circuit-probe-#{Ecto.UUID.generate()}"

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      pools = Repo.all(from p in Pool, where: p.slug == ^slug, select: p.id)
      entry_kinds = Repo.all(from entry in LedgerEntry, where: entry.pool_id in ^pools, select: entry.entry_kind)
      delete_committed_pools!(pools)
      Repo.delete_all(from i in UpstreamIdentity, where: i.account_label == ^slug)
      refute Repo.exists?(from request in Request, where: request.pool_id in ^pools)
      refute Repo.exists?(from entry in LedgerEntry, where: entry.pool_id in ^pools)
      assert Enum.all?(entry_kinds, &(&1 == "reservation"))
      TestDiagnostics.puts(Jason.encode!(%{scenario: "peer_fixture_cleanup", reservations: length(entry_kinds), settlements: 0, remaining_requests: 0, remaining_ledger_entries: 0}))
    end)

    pool = pool_fixture(%{slug: slug})
    %{raw_key: raw_key} = active_api_key_fixture(pool)
    %{assignment: assignment, identity: identity} = upstream_assignment_fixture(pool, %{account_label: slug})
    model = model_fixture(pool)
    assert {:ok, auth} = Access.authenticate_api_key(raw_key)
    assert {:ok, circuit} = CircuitState.record_failure(auth, model, assignment, "proxy_websocket", :upstream_network_error)
    circuit |> Ecto.Changeset.change(next_probe_at: DateTime.add(DateTime.utc_now(), -1)) |> Repo.update!()
    local_runtime = CircuitProbePeer.runtime()
    assert context.peer_runtime.node != node()
    assert context.peer_runtime.backend != local_runtime.backend
    assert context.peer_runtime.beam == local_runtime.beam
    endpoint = "/backend-api/codex/responses"
    payload = %{"model" => model.exposed_model_id, "input" => [], "stream" => true}
    request_options = RequestOptions.for_websocket(%{}, payload)
    candidates = [{assignment, identity}]
    assert {:ok, reserved} = Accounting.reserve(auth, model, payload, %{endpoint: endpoint, transport: "websocket", correlation_id: Ecto.UUID.generate(), request_metadata: %{}})

    assert {:ok, dispatch_context} =
             Context.new(%{
               auth: auth,
               endpoint: endpoint,
               payload: payload,
               model: model,
               reserved: reserved,
               candidates: candidates,
               request_options: request_options,
               route_state: RouteState.new(%{visible_model: model, candidates: candidates})
             })

    %{fixture: %{auth: auth, model: model, assignment: assignment, identity: identity, circuit: circuit, context: dispatch_context}, local_runtime: local_runtime}
  end

  for first_writer <- [:local, :peer], outcome <- [:neutral, :success, :failure] do
    test "#{first_writer} stale #{outcome} completion cannot alter a probe admitted on the other node", context do
      fixture = context.fixture
      first_node = if unquote(first_writer) == :local, do: node(), else: context.peer
      second_node = if first_node == node(), do: context.peer, else: node()
      assert {:ok, first} = call(first_node, :admit, [fixture])
      Repo.reload!(fixture.circuit) |> Ecto.Changeset.change(updated_at: DateTime.add(DateTime.utc_now(), -61)) |> Repo.update!()
      assert {:ok, second} = call(second_node, :admit, [fixture])
      before = Repo.reload!(fixture.circuit)
      assert first.circuit_state.probe_generation != before.probe_generation
      assert before.probe_generation == second.circuit_state.probe_receipt.generation
      assert before.probe_admission_ids == [second.circuit_state.probe_receipt.admission_id]
      call(first_node, :complete, [fixture, first, unquote(outcome)])
      assert Repo.reload!(fixture.circuit) == before
      assert {:error, :routing_circuit_probe_in_flight} = call(first_node, :admit, [fixture])
      assert :ok = call(second_node, :complete, [fixture, second, :neutral])
      released = Repo.reload!(fixture.circuit)
      assert CircuitHealth.probe_in_flight_count(released) == 0
      assert released.probe_admission_ids == []
      call(second_node, :complete, [fixture, second, :neutral])
      assert Repo.reload!(fixture.circuit) == released
      assert {:ok, _third} = call(first_node, :admit, [fixture])
      TestDiagnostics.puts(Jason.encode!(%{scenario: "#{unquote(first_writer)}_stale_#{unquote(outcome)}", distinct_nodes: true, distinct_backends: true, code_equal: true, beam: context.local_runtime.beam, generation_changed: first.circuit_state.probe_generation != before.probe_generation, successor_status: before.status, successor_active_receipts: length(before.probe_admission_ids), successor_count_after_stale: CircuitHealth.probe_in_flight_count(before), successor_release_count: CircuitHealth.probe_in_flight_count(released), third_refused_until_release: true}))
    end
  end

  for first_writer <- [:local, :peer] do
    test "#{first_writer} completion before successor admission consumes only its own slot", context do
      fixture = context.fixture
      first_node = if unquote(first_writer) == :local, do: node(), else: context.peer
      second_node = if first_node == node(), do: context.peer, else: node()
      assert {:ok, first} = call(first_node, :admit, [fixture])
      assert :ok = call(first_node, :complete, [fixture, first, :neutral])
      assert {:ok, _second} = call(second_node, :admit, [fixture])
      before = Repo.reload!(fixture.circuit)
      assert :ok = call(first_node, :complete, [fixture, first, :neutral])
      assert Repo.reload!(fixture.circuit) == before
      assert {:error, :routing_circuit_probe_in_flight} = call(first_node, :admit, [fixture])
    end
  end

  for limit <- [1, 2] do
    test "two nodes concurrently admit no more than the configured #{limit} slots", context do
      settings = %OperationalSettings{circuit_failure_threshold: 1, circuit_half_open_probe_limit: unquote(limit), circuit_success_threshold: 2}
      Application.put_env(:codex_pooler, OperationalSettings, settings: settings)
      :ok = :erpc.call(context.peer, Application, :put_env, [:codex_pooler, OperationalSettings, [settings: settings]])
      parent = self()
      gate = make_ref()
      {:ok, owned} = Agent.start(fn -> [] end)

      on_exit(fn ->
        Enum.each(Agent.get(owned, & &1), &send(&1, {gate, :go}))
        Agent.stop(owned)
      end)

      tasks = Enum.map([node(), context.peer], fn target -> Task.async(fn -> call(target, :concurrent_admit, [context.fixture, parent, gate]) end) end)
      assert_receive {^gate, :ready, first, first_node, first_backend}, @budget
      Agent.update(owned, &[first | &1])
      assert_receive {^gate, :ready, second, second_node, second_backend}, @budget
      Agent.update(owned, &[second | &1])
      assert first_node != second_node
      assert first_backend != second_backend
      send(first, {gate, :go})
      send(second, {gate, :go})
      results = Enum.map(tasks, &Task.await(&1, @budget))
      accepted = for {:ok, selection} <- results, do: selection
      assert length(accepted) == unquote(limit)
      assert length(Repo.reload!(context.fixture.circuit).probe_admission_ids) == unquote(limit)
      assert {:error, :routing_circuit_probe_in_flight} = call(context.peer, :admit, [context.fixture])
      [completed | rest] = accepted
      assert :ok = call(context.peer, :complete, [context.fixture, completed, :neutral])
      before = Repo.reload!(context.fixture.circuit)
      assert :ok = call(node(), :complete, [context.fixture, completed, :neutral])
      assert Repo.reload!(context.fixture.circuit) == before
      Enum.each(rest, &call(node(), :complete, [context.fixture, &1, :neutral]))
      assert CircuitHealth.probe_in_flight_count(Repo.reload!(context.fixture.circuit)) == 0
      TestDiagnostics.puts(Jason.encode!(%{scenario: "concurrent_limit_#{unquote(limit)}", distinct_nodes: true, distinct_backends: true, admitted: length(accepted), duplicate_write_free: true, final_count: 0}))
    end
  end

  for {outcome, expected} <- [{:neutral, "half_open"}, {:success, "closed"}, {:failure, "open"}] do
    test "current #{outcome} receipt completes on a different node", context do
      assert {:ok, selection} = call(node(), :admit, [context.fixture])
      call(context.peer, :complete, [context.fixture, selection, unquote(outcome)])
      current = Repo.reload!(context.fixture.circuit)
      assert current.status == unquote(expected)
      assert current.probe_admission_ids == []
      assert CircuitHealth.probe_in_flight_count(current) == 0
      call(node(), :complete, [context.fixture, selection, unquote(outcome)])
      assert Repo.reload!(context.fixture.circuit) == current
    end
  end

  for scope <- [:generation, :route] do
    test "wrong #{scope} receipt cannot mutate the circuit on the other node", context do
      assert {:ok, selection} = call(node(), :admit, [context.fixture])

      invalid =
        if unquote(scope) == :generation do
          receipt = %{selection.circuit_state.probe_receipt | generation: Ecto.UUID.generate()}
          %{selection | circuit_state: %{selection.circuit_state | probe_receipt: receipt}}
        else
          %{selection | route_class: "proxy_stream"}
        end

      before = Repo.reload!(context.fixture.circuit)

      for outcome <- [:neutral, :success, :failure] do
        call(context.peer, :complete, [context.fixture, invalid, outcome])
        assert Repo.reload!(context.fixture.circuit) == before
      end

      assert :ok = call(context.peer, :complete, [context.fixture, selection, :neutral])
    end
  end

  defp call(target, function, args) when target == node(), do: apply(CircuitProbePeer, function, args)
  defp call(target, function, args), do: :erpc.call(target, CircuitProbePeer, function, args, @budget)
end
