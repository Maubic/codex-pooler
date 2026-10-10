defmodule CodexPooler.Gateway.Routing.CircuitProbeOwnershipTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Gateway.Routing.{CircuitHealth, CircuitState, RoutingSelection}
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, SelectedCandidateContext}
  alias CodexPooler.Gateway.Runtime.Routing.DispatchLifecycle

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(OperationalSettings)
    Application.put_env(:codex_pooler, OperationalSettings, settings: %OperationalSettings{circuit_failure_threshold: 1, circuit_half_open_probe_limit: 1})
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    %{assignment: assignment, identity: identity} = upstream_assignment_fixture(pool)
    model = model_fixture(pool)
    auth = %{pool: pool, api_key: api_key}
    assert {:ok, opened} = CircuitState.record_failure(auth, model, assignment, "proxy_websocket", :upstream_network_error)
    opened |> Ecto.Changeset.change(next_probe_at: DateTime.add(DateTime.utc_now(), -1)) |> Repo.update!()
    request = request_fixture(auth, %{model_id: model.id, status: "in_progress", completed_at: nil})
    %{auth: auth, model: model, assignment: assignment, identity: identity, circuit: opened, request: request}
  end

  for outcome <- [:neutral, :success, :failure] do
    test "late stale #{outcome} completion cannot alter its successor's circuit", fixture do
      first = admit!(fixture)
      stale!(fixture)
      second = admit!(fixture)
      before = Repo.reload!(fixture.circuit)
      complete(fixture, first, unquote(outcome))
      assert Repo.reload!(fixture.circuit) == before
      assert {:error, :routing_circuit_probe_in_flight} = RoutingSelection.begin_circuit(base_selection(fixture), fixture.auth, fixture.model)
      assert :ok = complete(fixture, second, :neutral)
      assert CircuitHealth.probe_in_flight_count(Repo.reload!(fixture.circuit)) == 0
      assert {:ok, _third} = RoutingSelection.begin_circuit(base_selection(fixture), fixture.auth, fixture.model)
    end
  end

  test "duplicate completion cannot release the next probe", fixture do
    first = admit!(fixture)
    assert :ok = complete(fixture, first, :neutral)
    _second = admit!(fixture)
    before = Repo.reload!(fixture.circuit)
    assert :ok = complete(fixture, first, :neutral)
    assert Repo.reload!(fixture.circuit) == before
    assert {:error, :routing_circuit_probe_in_flight} = RoutingSelection.begin_circuit(base_selection(fixture), fixture.auth, fixture.model)
  end

  test "multiple admitted slots are consumed once each", fixture do
    Application.put_env(:codex_pooler, OperationalSettings, settings: %OperationalSettings{circuit_failure_threshold: 1, circuit_half_open_probe_limit: 2, circuit_success_threshold: 2})
    first = admit!(fixture)
    second = admit!(fixture)
    assert {:error, :routing_circuit_probe_in_flight} = RoutingSelection.begin_circuit(base_selection(fixture), fixture.auth, fixture.model)
    assert :ok = complete(fixture, first, :neutral)
    _third = admit!(fixture)
    before = Repo.reload!(fixture.circuit)
    assert :ok = complete(fixture, first, :neutral)
    assert Repo.reload!(fixture.circuit) == before
    assert :ok = complete(fixture, second, :neutral)
    assert CircuitHealth.probe_in_flight_count(Repo.reload!(fixture.circuit)) == 1
  end

  for {outcome, status} <- [{:neutral, "half_open"}, {:success, "closed"}, {:failure, "open"}] do
    test "current #{outcome} completion retains its health transition", fixture do
      selection = admit!(fixture)
      complete(fixture, selection, unquote(outcome))
      state = Repo.reload!(fixture.circuit)
      assert state.status == unquote(status)
      assert CircuitHealth.probe_in_flight_count(state) == 0
    end
  end

  for field <- [:state_id, :generation, :admission_id] do
    test "wrong receipt #{field} is write-free", fixture do
      selection = admit!(fixture)
      receipt = Map.put(selection.circuit_state.probe_receipt, unquote(field), Ecto.UUID.generate())
      invalid = %{selection | circuit_state: %{selection.circuit_state | probe_receipt: receipt}}
      before = Repo.reload!(fixture.circuit)

      for outcome <- [:neutral, :success, :failure] do
        complete(fixture, invalid, outcome)
        assert Repo.reload!(fixture.circuit) == before
      end
    end
  end

  test "a receipt for another route cannot complete the current route", fixture do
    first = admit!(fixture)
    assert {:ok, other} = CircuitState.record_failure(fixture.auth, fixture.model, fixture.assignment, "proxy_stream", :upstream_network_error)
    other |> Ecto.Changeset.change(next_probe_at: DateTime.add(DateTime.utc_now(), -1)) |> Repo.update!()
    assert {:ok, second} = RoutingSelection.begin_circuit(%{base_selection(fixture) | route_class: "proxy_stream"}, fixture.auth, fixture.model)
    before = Repo.reload!(other)

    for outcome <- [:neutral, :success, :failure] do
      complete(fixture, %{first | route_class: "proxy_stream"}, outcome)
      assert Repo.reload!(other) == before
    end

    assert :ok = complete(fixture, second, :neutral)
    assert CircuitHealth.probe_in_flight_count(Repo.reload!(other)) == 0
  end

  test "legacy count-only rows release normally before the first fenced admission", fixture do
    legacy = Repo.reload!(fixture.circuit) |> Ecto.Changeset.change(status: "half_open", metadata: %{"probe_in_flight_count" => 1}) |> Repo.update!()
    assert {:ok, released} = CircuitState.record_neutral_completion(fixture.auth, fixture.model, fixture.assignment, "proxy_websocket", :probe)
    assert CircuitHealth.probe_in_flight_count(released) == 0
    selection = admit!(fixture)
    assert is_binary(selection.circuit_state.probe_generation)
    assert {:ok, ignored} = CircuitState.record_neutral_completion(fixture.auth, fixture.model, fixture.assignment, "proxy_websocket", :probe)
    assert ignored == Repo.reload!(legacy)
    assert CircuitHealth.probe_in_flight_count(ignored) == 1
    assert :ok = complete(fixture, selection, :neutral)
  end

  test "new receipt survives selection context transport without changing coarse admission", fixture do
    selection = admit!(fixture)
    context = SelectedCandidateContext.from_dispatch_context(%Context{auth: fixture.auth, model: fixture.model, route_plan: selection.route_plan}, selection, false)
    transported = context |> :erlang.term_to_binary() |> :erlang.binary_to_term([:safe])
    assert transported.routing_circuit_admission == :probe
    assert transported.routing_circuit_state.probe_receipt == selection.circuit_state.probe_receipt
    assert :ok = DispatchLifecycle.neutral_completion(transported)
    assert Repo.reload!(fixture.circuit).probe_admission_ids == []
  end

  test "state snapshots from the prior struct layout remain readable", fixture do
    selection = admit!(fixture)
    prior_shape = Map.drop(selection.circuit_state, [:probe_generation, :probe_admission_ids, :probe_receipt])
    assert CircuitHealth.probe_in_flight_count(prior_shape) == 1
    assert CircuitState.completion_admission(prior_shape) == :probe
  end

  test "a scoped receipt cannot create a circuit in another model or pool", fixture do
    selection = admit!(fixture)
    receipt = CircuitState.completion_admission(selection.circuit_state)
    other_model = model_fixture(fixture.auth.pool, %{exposed_model_id: "synthetic-other-#{System.unique_integer([:positive])}"})
    %{pool: other_pool, api_key: other_key} = active_api_key_fixture()
    before = Repo.reload!(fixture.circuit)

    for {auth, model} <- [{fixture.auth, other_model}, {%{pool: other_pool, api_key: other_key}, fixture.model}] do
      assert {:ok, _} = CircuitState.record_failure(auth, model, fixture.assignment, "proxy_websocket", :upstream_network_error, receipt)
      assert {:ok, _} = CircuitState.record_success(auth, model, fixture.assignment, "proxy_websocket", receipt)
      assert {:ok, _} = CircuitState.record_neutral_completion(auth, model, fixture.assignment, "proxy_websocket", receipt)
      assert Repo.reload!(fixture.circuit) == before
    end

    assert Repo.aggregate(RoutingCircuitState, :count) == 1
  end

  test "old count-only SQL writer remains an explicit mixed-version limitation", fixture do
    _selection = admit!(fixture)
    before = Repo.reload!(fixture.circuit)
    Repo.query!("UPDATE routing_circuit_states SET metadata = jsonb_set(metadata, '{probe_in_flight_count}', '0') WHERE id = $1", [Ecto.UUID.dump!(before.id)])
    after_old_writer = Repo.reload!(fixture.circuit)
    assert after_old_writer.probe_admission_ids == before.probe_admission_ids
    assert after_old_writer.probe_generation == before.probe_generation
    assert after_old_writer.metadata["probe_in_flight_count"] == 0
    assert CircuitHealth.probe_in_flight_count(after_old_writer) == 1
    Repo.query!("UPDATE routing_circuit_states SET status = 'closed' WHERE id = $1", [Ecto.UUID.dump!(before.id)])
    assert {:ok, %{admission: :normal}} = CircuitState.begin_attempt(fixture.auth, fixture.model, fixture.assignment, "proxy_websocket")
  end

  defp stale!(fixture), do: Repo.reload!(fixture.circuit) |> Ecto.Changeset.change(updated_at: DateTime.add(DateTime.utc_now(), -61)) |> Repo.update!()

  defp admit!(fixture) do
    assert {:ok, selection} = RoutingSelection.begin_circuit(base_selection(fixture), fixture.auth, fixture.model)
    selection
  end

  defp base_selection(fixture) do
    %RoutingSelection{assignment: fixture.assignment, identity: fixture.identity, route_class: "proxy_websocket", route_plan: %{planned_at: DateTime.utc_now(), affinity: %{enabled?: false, key_hash: nil, pool_id: fixture.auth.pool.id, api_key_id: fixture.auth.api_key.id, model_identifier: fixture.model.exposed_model_id}}}
  end

  defp complete(fixture, selection, outcome) do
    context = %Context{auth: fixture.auth, model: fixture.model, route_plan: selection.route_plan, reserved: %{request: fixture.request}}
    selected = SelectedCandidateContext.from_dispatch_context(context, selection, false)
    assert selected.routing_circuit_admission == selection.circuit_admission

    case outcome do
      :success -> DispatchLifecycle.success(selected)
      :failure -> DispatchLifecycle.failure(selected, :upstream_network_error)
      :neutral -> DispatchLifecycle.neutral_completion(selected)
    end
  end
end
