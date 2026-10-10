defmodule CodexPoolerWeb.Runtime.DownstreamKeepaliveScenarioTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPoolerWeb.Runtime.DownstreamKeepaliveScenario

  alias CodexPooler.Accounting
  alias CodexPooler.Events
  alias CodexPooler.Gateway.Runtime.Finalization.Interruption
  alias CodexPooler.Gateway.Websocket, as: Gateway

  test "idle attribution reloads the settled snapshot after its metadata has committed" do
    fixture = settled_fixture()
    assert {:ok, :ok} = Interruption.record_settled_downstream_idle_timeout(fixture.receipt, "pong_observed")
    assert fixture.request.request_metadata["downstream_interruption"] == nil

    assert_idle_attribution!(fixture.request, "pong_observed")
  end

  test "idle attribution waits for its request's metadata after settlement" do
    fixture = settled_fixture()
    test = self()
    hold = make_ref()
    handler_id = {__MODULE__, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        :ok = Events.subscribe_pool(fixture.setup.pool)
        send(test, {hold, :subscribed, self()})

        receive do
          {^hold, :start} ->
            assert_idle_attribution!(fixture.request, "pong_observed")
            Process.info(self(), :messages)
        after
          15_000 -> flunk("attribution waiter was never started")
        end
      end)

    waiter = task.pid
    assert_receive {^hold, :subscribed, ^waiter}, detection_timeout_ms()
    request_id = fixture.request.id
    dumped_request_id = Ecto.UUID.dump!(request_id)
    config = %{waiter: waiter, test: test, hold: hold, request_id: dumped_request_id}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.observe_request_read/4, config)

    send(waiter, {hold, :start})
    assert_receive {^hold, :request_read, ^waiter}, detection_timeout_ms()
    assert Repo.reload!(fixture.request).request_metadata["downstream_interruption"] == nil

    # An unrelated request's update is not this request's attribution fence.
    other_event = {Events, %{reason: "request_metadata_updated", payload: %{"request_id" => Ecto.UUID.generate(), "status" => "failed"}}}
    send(waiter, other_event)
    assert {:ok, :ok} = Interruption.record_settled_downstream_idle_timeout(fixture.receipt, "pong_observed")
    assert {:messages, messages} = Task.await(task, detection_timeout_ms())
    assert other_event in messages
  end

  @doc false
  def observe_request_read(_event, _measurements, %{query: query, params: params}, config) do
    if self() == config.waiter and String.starts_with?(query, "SELECT") and String.contains?(query, ~s(FROM "requests")) and config.request_id in List.flatten(params),
      do: send(config.test, {config.hold, :request_read, self()})
  end

  defp settled_fixture do
    setup = accounting_setup()
    assert :ok = Events.subscribe_pool(setup.pool)
    {:ok, session} = Gateway.start_codex_session(setup.auth, %{accepted_turn_state: Ecto.UUID.generate()})
    {:ok, reserved} = Accounting.reserve(setup.auth, setup.model, %{}, %{transport: "websocket", requested_model: setup.model.exposed_model_id, correlation_id: Ecto.UUID.generate()})
    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment, %{transport: "websocket"})
    {:ok, _turn} = Gateway.start_codex_turn(session, reserved.request)
    request = Repo.reload!(reserved.request)
    receipt = %{session_id: session.id, request_id: request.id, correlation_id: request.correlation_id, api_key_id: setup.api_key.id, owner_binding: nil, attempt_id: attempt.id, replay_generation: attempt.replay_generation}
    assert :ok = Interruption.interrupt_direct_request(receipt, "client_disconnected")
    request = Repo.reload!(request)
    assert request.status == "failed"
    assert request.last_error_code == "client_disconnected"
    %{request: request, receipt: receipt, setup: setup}
  end
end
