defmodule CodexPoolerWeb.Runtime.WebsocketUsageLimitRouteHealthTest do
  # A provider usage limit the upstream websocket sends as its first event
  # (`{"type":"error","status":429,"error":{"type":"usage_limit_reached"}}`)
  # completes the refusing candidate's route health as its HTTP `429` twin does
  # (`usage_limit_route_health_test.exs`, findings#206 row 206-594), whether
  # the turn fails over to the next candidate or the refusing candidate was the
  # last one (findings#325 row 325-5):
  #
  # - when the frame's headers exclude the account (an exhausted window with a
  #   reset still ahead), the refusal completes the route neutrally: a probe it
  #   claimed on a half-open circuit is released, and nothing is demoted or
  #   counted;
  # - a usage limit whose headers carry no window keeps the circuit rule: a
  #   route failure under `upstream_rate_limited`, with its demotion.
  #
  # The failover used to complete nothing, so a probe it claimed stayed counted
  # in flight until the staleness self-heal. One BEAM node, FakeUpstream, the
  # direct path and an owner session, Full and Lite.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [owner_socket: 3, pool_attempts: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.CodexResponsesSocket

  @moduletag capture_log: true

  # {scenario, circuit on the refusing candidate, usage-limit frame headers}
  @scenarios [
    {:failover, :half_open, :exhausted_window},
    {:failover, :closed, :exhausted_window},
    {:failover, :closed, :no_window},
    {:last_candidate, :closed, :no_window}
  ]

  for mode <- ["full", "lite"], path <- [:direct, :owner], {scenario, circuit, headers} <- @scenarios do
    @mode mode
    @path path
    @scenario scenario
    @circuit circuit
    @headers headers
    test "#{mode} #{path}: a usage limit with #{headers} on a #{circuit} #{scenario} candidate completes its route health like HTTP" do
      if @path == :owner do
        CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
        Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
      end

      refusing = start_upstream(FakeUpstream.strict_sequence([strict_native_request(1, FakeUpstream.websocket_text_frames([usage_limit_frame(@headers)]))]))
      {setup, served} = candidates!(@scenario, refusing, @mode)
      refusing_circuit = if @circuit == :half_open, do: half_open_websocket_circuit!(setup, setup.assignment)
      {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)

      frame = run_turn!(@path, auth, setup, "#{@mode}-#{@path}-#{@scenario}-#{@circuit}-#{@headers}")
      assert_turn!(@scenario, frame, setup, served)

      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert request.request_metadata["routing"]["model_serving_mode"] == @mode
      assert :ok = FakeUpstream.verify!(refusing)
      assert_route_health!(setup, refusing_circuit, @circuit, @headers)
    end
  end

  # A failover is served by the second candidate after the refusing one's
  # retryable attempt; the last candidate answers the client with the refusal.
  defp assert_turn!(:failover, frame, setup, %FakeUpstream{} = served) do
    assert frame["id"] == "resp_usage_limit_failover"
    assert [first_attempt, second_attempt] = pool_attempts(setup.pool.id)
    assert {first_attempt.pool_upstream_assignment_id, first_attempt.status} == {setup.assignment.id, "retryable_failed"}
    assert second_attempt.status == "succeeded"
    assert :ok = FakeUpstream.verify!(served)
  end

  defp assert_turn!(:last_candidate, frame, setup, nil) do
    assert %{"status" => 429, "error" => %{"type" => "usage_limit_reached"}} = frame
    assert [attempt] = pool_attempts(setup.pool.id)
    assert attempt.status == "failed"
  end

  # The exhausted window excludes the account: a neutral completion releases a
  # probe and records nothing on a closed circuit. A usage limit without a
  # window is a route failure under the HTTP code for the same status.
  defp assert_route_health!(setup, refusing_circuit, :half_open, :exhausted_window) do
    assert %RoutingCircuitState{status: "half_open", failure_count: 1, metadata: %{"probe_in_flight_count" => 0}} = Repo.get!(RoutingCircuitState, refusing_circuit.id)
    refute_demoted!(setup)
  end

  defp assert_route_health!(setup, nil, :closed, :exhausted_window) do
    assert route_circuit_failures(setup.assignment.id) == []
    refute_demoted!(setup)
  end

  defp assert_route_health!(setup, nil, :closed, :no_window) do
    assert route_circuit_failures(setup.assignment.id) == [{"upstream_rate_limited", 1}]
    assert Repo.exists?(from(d in BridgeDemotion, where: d.pool_upstream_assignment_id == ^setup.assignment.id and d.reason_code == "upstream_rate_limited"))
  end

  defp refute_demoted!(setup),
    do: refute(Repo.exists?(from(d in BridgeDemotion, where: d.pool_upstream_assignment_id == ^setup.assignment.id)))

  # The refusing candidate first; for a failover a second candidate, carrying
  # the ordering demotion that keeps the refusing one preferred, serves the turn.
  defp candidates!(:failover, refusing, mode) do
    # Strict: the second candidate serves the turn once, on its first connection.
    served =
      start_upstream(
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          strict_native_request(
            1,
            FakeUpstream.websocket_text_frames([
              CodexPooler.JSON.encode!(%{"id" => "resp_usage_limit_failover", "object" => "response", "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}})
            ])
          )
        ])
      )

    {setup, _second} = websocket_failover_candidates!(refusing, served, mode)
    {setup, served}
  end

  defp candidates!(:last_candidate, refusing, mode) do
    setup = gateway_setup(refusing)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    {setup, nil}
  end

  defp run_turn!(:direct, auth, setup, marker) do
    parent = self()

    _result =
      execute_websocket_response(auth, turn_payload(setup, marker), %{request_id: "ws-usage-limit-#{marker}"}, fn frame ->
        send(parent, {:websocket_frame, frame})
      end)

    assert_received {:websocket_frame, frame}
    CodexPooler.JSON.decode!(frame)
  end

  defp run_turn!(:owner, auth, setup, marker) do
    {:ok, state} = owner_socket(auth, "ws-owner-usage-limit-#{marker}", "owner-usage-limit-#{marker}")

    try do
      assert {:ok, state} = CodexResponsesSocket.handle_in({turn_payload(setup, marker), [opcode: :text]}, state)
      # The served turn's frame comes back through the owner, the refusal of
      # a last candidate as the socket's own chunk; both are collected here.
      assert {_state, [_ | _] = frames} = collect_native_turn_frames!(state)
      List.last(frames)
    after
      CodexResponsesSocket.terminate(:closed, state)
    end
  end

  defp usage_limit_frame(headers) do
    resets_at = DateTime.to_unix(DateTime.utc_now()) + 3 * 86_400

    %{"type" => "error", "status" => 429, "error" => %{"type" => "usage_limit_reached", "message" => "synthetic provider text", "resets_at" => resets_at}}
    |> put_frame_headers(headers, resets_at)
    |> CodexPooler.JSON.encode!()
  end

  defp put_frame_headers(frame, :no_window, _resets_at), do: frame

  defp put_frame_headers(frame, :exhausted_window, resets_at) do
    Map.put(frame, "headers", %{
      "x-codex-secondary-used-percent" => "100",
      "x-codex-secondary-window-minutes" => "10080",
      "x-codex-secondary-reset-at" => Integer.to_string(resets_at)
    })
  end

  defp turn_payload(setup, marker) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("usage limit route health #{marker}"),
      "stream" => true,
      "generate" => true
    })
  end
end
