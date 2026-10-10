defmodule CodexPooler.Gateway.Runtime.Finalization.DownstreamIdleAttributionTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{LedgerEntry, RequestOutcome}
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Gateway.Runtime.NativeResponseSteering
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Gateway.Websocket.OwnerCleanup

  test "exact direct receipt records bounded attribution without finalizing or changing health" do
    fixture = fixture()
    request_before = fixture.request
    attempt_before = fixture.attempt
    ledger_before = Repo.all(LedgerEntry)
    assert :ok = CodexPooler.Events.subscribe_pool(fixture.setup.pool)
    task = Task.async(fn -> Interruption.record_settled_downstream_idle_timeout(fixture.receipt, "pong_observed") end)
    assert {:ok, :ok} = Task.await(task, 15_000)
    request = Repo.reload!(request_before)
    attempt = Repo.reload!(attempt_before)
    marker = %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "pong_observed"}
    assert request.request_metadata["downstream_interruption"] == marker
    assert attempt.response_metadata["downstream_interruption"] == marker
    assert request.status == request_before.status
    assert attempt.status == attempt_before.status
    assert Repo.all(LedgerEntry) == ledger_before
    assert :ok = Interruption.interrupt_direct_request(fixture.receipt, "client_disconnected")
    request = Repo.reload!(request)
    assert_receive {CodexPooler.Events, %{reason: "request_metadata_updated", payload: %{"request_id" => request_id, "status" => "failed"}}}
    assert request_id == request.id
    assert request.last_error_code == "client_disconnected"
    refute RequestOutcome.client_cancelled?(request)
    assert Repo.reload!(attempt).response_metadata["downstream_interruption"] == marker
  end

  for field <- [:request_id, :correlation_id, :api_key_id, :attempt_id, :replay_generation, :session_id] do
    test "stale #{field} cannot mark a request or attempt" do
      fixture = fixture()
      field = unquote(field)
      value = invalid_value(field)
      receipt = Map.put(fixture.receipt, field, value)
      assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(receipt, "unknown")
      assert Repo.reload!(fixture.request) == fixture.request
      assert Repo.reload!(fixture.attempt) == fixture.attempt
    end
  end

  test "a different live session of the same key cannot mark the bound turn" do
    fixture = fixture()
    {:ok, other} = Gateway.start_codex_session(fixture.setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(%{fixture.receipt | session_id: other.id}, "unknown")
    assert Repo.reload!(fixture.request) == fixture.request
    assert Repo.reload!(fixture.attempt) == fixture.attempt
  end

  test "a foreign owner binding remains unmarked" do
    fixture = fixture()
    receipt = Map.put(fixture.receipt, :owner_binding, %{owner_instance_id: "other-owner", owner_lease_token: Ecto.UUID.generate(), downstream_epoch: 1})
    assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(receipt, "unknown")
    assert :ok = Interruption.interrupt_direct_request(fixture.receipt, "client_disconnected")
    before = Repo.reload!(fixture.request)
    assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(receipt, "unknown")
    assert Repo.reload!(before) == before
    assert RequestOutcome.client_cancelled?(before)
  end

  for field <- [:owner_lease_token, :owner_instance_id, :downstream_epoch, :attempt_id, :replay_generation, :session_id] do
    test "owner witness rejects stale #{field}" do
      fixture = owner_fixture()
      field = unquote(field)
      value = invalid_owner_value(field)
      assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(Map.put(fixture.witness, field, value), "unknown")
      assert Repo.reload!(fixture.request) == fixture.request
      assert Repo.reload!(fixture.attempt) == fixture.attempt
    end
  end

  test "an owner without socket tasks marks only its active downstream and excludes already forwarded terminals" do
    fixture = owner_fixture()
    downstream = %{pid: self(), epoch: 1, correlation_id: Ecto.UUID.generate()}
    active = %{cleanup_witness: fixture.witness, downstream: downstream, terminal_forwarded?: false}
    state = %{downstream: downstream, active_turn: active}
    from = {self(), make_ref()}
    call = {:capture_downstream_idle_timeout, downstream, "unknown"}
    assert {:reply, {:ok, :stale}, _} = WebsocketOwnerSession.handle_call(call, from, %{state | active_turn: nil})
    assert {:reply, {:ok, :stale}, _} = WebsocketOwnerSession.handle_call(call, from, put_in(state.active_turn.terminal_forwarded?, true))
    assert {:reply, {:ok, :stale}, _} = WebsocketOwnerSession.handle_call(call, from, put_in(state.active_turn.downstream.epoch, 2))
    assert Repo.reload!(fixture.request) == fixture.request
    assert {:reply, {{:ok, :ok}, witness}, ^state} = WebsocketOwnerSession.handle_call(call, from, state)
    assert witness == fixture.witness
    assert Repo.reload!(fixture.request) == fixture.request
  end

  test "consuming a replay clears current attribution and fences the original receipt" do
    fixture = CodexPooler.RequestReplayFixtures.replay_fixture(owner?: true, reservation?: true)
    marker = %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "unknown"}
    {:ok, _} = Accounting.merge_request_metadata(fixture.request, %{"downstream_interruption" => marker})
    fixture.attempt |> Ecto.Changeset.change(response_metadata: Map.put(fixture.attempt.response_metadata, "downstream_interruption", marker)) |> Repo.update!()
    {:ok, armed} = Accounting.RequestReplay.arm(CodexPooler.RequestReplayFixtures.arm_input(fixture))
    input = CodexPooler.RequestReplayFixtures.consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
    assert {:ok, consumed} = Accounting.RequestReplay.consume(input)
    refute Map.has_key?(Repo.reload!(fixture.request).request_metadata, "downstream_interruption")
    refute Map.has_key?(consumed.request.request_metadata, "downstream_interruption")
    assert Repo.reload!(fixture.attempt).response_metadata["downstream_interruption"] == marker
    refute Map.has_key?(consumed.attempt.response_metadata, "downstream_interruption")
    old = %{session_id: fixture.session.id, request_id: fixture.request.id, correlation_id: fixture.request.correlation_id, api_key_id: fixture.api_key.id, attempt_id: fixture.attempt.id, replay_generation: 0}
    assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(old, "unknown")
    refute Map.has_key?(Repo.reload!(fixture.request).request_metadata, "downstream_interruption")
  end

  test "a busy owner or steering lane cannot add the ordinary five-second call wait" do
    lane = start_supervised!(%{id: :idle_attribution_lane, start: {GenServer, :start_link, [NativeResponseSteering, self()]}})
    :ok = :sys.suspend(lane)

    try do
      for call <- [fn -> WebsocketOwnerSession.capture_downstream_idle_timeout(lane, %{pid: self(), epoch: 1, correlation_id: Ecto.UUID.generate()}, "unknown") end, fn -> NativeResponseSteering.capture_downstream_idle_timeout(lane, %{request_id: Ecto.UUID.generate(), attempt_id: Ecto.UUID.generate(), replay_generation: 0}, "unknown") end] do
        task = Task.async(call)
        assert {:error, :attribution_unavailable} = Task.await(task, 1_000)
      end
    after
      :sys.resume(lane)
    end
  end

  test "late attribution marks only the same disconnected generation" do
    fixture = fixture()
    assert :ok = Interruption.interrupt_direct_request(fixture.receipt, "client_disconnected")
    assert {:ok, :ok} = Interruption.record_settled_downstream_idle_timeout(fixture.receipt, "pong_observed")
    assert Repo.reload!(fixture.request).request_metadata["downstream_interruption"]["client_activity"] == "pong_observed"

    for receipt <- [%{fixture.receipt | attempt_id: Ecto.UUID.generate()}, %{fixture.receipt | replay_generation: 1}, %{fixture.receipt | correlation_id: Ecto.UUID.generate()}] do
      assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(receipt, "unknown")
    end

    request = Repo.reload!(fixture.request)

    for {status, code} <- [{"succeeded", "client_disconnected"}, {"failed", "provider_error"}] do
      request |> Ecto.Changeset.change(status: status, last_error_code: code) |> Repo.update!()
      assert {:ok, :stale} = Interruption.record_settled_downstream_idle_timeout(fixture.receipt, "unknown")
    end
  end

  test "the metadata sanitizer keeps only the fixed origin, cause and activity vocabulary" do
    assert Accounting.sanitize_metadata(%{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "arbitrary", "detail" => "discard"}}) == %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout", "client_activity" => "unknown"}}
    assert Accounting.sanitize_metadata(%{"downstream_interruption" => %{"origin" => "arbitrary", "cause" => "idle_timeout"}}) == %{"downstream_interruption" => %{}}
  end

  defp invalid_owner_value(field) when field in [:downstream_epoch, :replay_generation], do: 2
  defp invalid_owner_value(_field), do: Ecto.UUID.generate()

  defp invalid_value(:replay_generation), do: 1
  defp invalid_value(_field), do: Ecto.UUID.generate()

  defp owner_fixture do
    fixture = fixture()
    session = fixture.session
    binding = %{"owner_instance_id" => session.owner_instance_id, "downstream_epoch" => 1}
    {:ok, request} = Accounting.merge_request_metadata(fixture.request, %{"websocket_owner_forwarding" => binding})
    witness = %OwnerCleanup{session_id: session.id, owner_instance_id: session.owner_instance_id, owner_lease_token: session.owner_lease_token, request_id: request.id, attempt_id: fixture.attempt.id, replay_generation: 0, downstream_epoch: 1}
    Map.merge(fixture, %{request: request, witness: witness})
  end

  defp fixture do
    setup = accounting_setup()
    {:ok, session} = Gateway.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{}, %{transport: "websocket", requested_model: setup.model.exposed_model_id, correlation_id: Ecto.UUID.generate()})
    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment, %{transport: "websocket"})
    {:ok, _turn} = Gateway.start_codex_turn(session, reserved.request)
    request = Repo.reload!(reserved.request)
    receipt = %{session_id: session.id, request_id: request.id, correlation_id: request.correlation_id, api_key_id: setup.api_key.id, owner_binding: nil, attempt_id: attempt.id, replay_generation: attempt.replay_generation}
    assert :ok = Interruption.interrupt_direct_request(receipt, "client_disconnected")
    %{request: Repo.reload!(request), attempt: Repo.reload!(attempt), receipt: receipt, session: Repo.reload!(session), setup: setup}
  end
end
