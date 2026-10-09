defmodule CodexPooler.Gateway.Routing.SavedResetPostCommitFailureTest do
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.RouteFiltering
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.SavedResetPostCommitFailureSupport, as: Support

  @moduletag capture_log: true
  setup context do
    previous_level = Logger.level()
    on_exit(fn -> Logger.configure(level: previous_level) end)
    Logger.configure(level: :info)
    %{support: Support.start!(context)}
  end

  for {phase, fault} <- [{:pending, :publication}, {:pending, :snapshot}, {:pending, :probe_write}, {:confirmed, :snapshot}, {:confirmed, :later_snapshot}] do
    test "#{phase} committed reset survives #{fault} routing failure", %{support: support} do
      fixture = Support.fixture!(support, phase: unquote(phase))
      {input, state} = routing_input(fixture)
      :ok = Support.arm!(fixture, unquote(fault))

      {result, log} =
        with_log([level: :info], fn ->
          try do
            RouteFiltering.filter_candidates_with_route_state(input, state)
          rescue
            exception -> {:raised, exception.__struct__}
          end
        end)

      Support.restore!(fixture)
      receipt = Support.receipt!(fixture)
      Support.emit(fixture, "#{unquote(phase)}_#{unquote(fault)}")
      assert receipt.fault_armed
      assert receipt.query_errors > 0
      assert FakeUpstream.physical_counts(fixture.fake).consume == 1
      expected = if unquote(phase) == :pending, do: "pending", else: "confirmed"
      assert result_summary(result) == %{status: 503, outcome: expected}
      assert log =~ "result_code=reset applied=true"
      assert log =~ "failed after commit applied=true"
      refute log =~ "applied=false"
      refute Map.has_key?(elem(result, 1), :saved_reset_post_apply_failure)

      options = RequestOptions.put_routing(input.request_options, quota_decision: %{"non_credit_recovery_outcome" => expected})
      repeated_input = FilterInput.put_request_options(input, options)
      repeated = RouteFiltering.filter_candidates_with_route_state(repeated_input, state)
      assert result_summary(repeated).outcome == expected
      assert FakeUpstream.physical_counts(fixture.fake).consume == 1
    end
  end

  for phase <- [:pending, :confirmed] do
    test "healthy #{phase} committed recovery preserves its routing outcome", %{support: support} do
      fixture = Support.fixture!(support, phase: unquote(phase))
      {input, state} = routing_input(fixture)
      :ok = Support.arm!(fixture, :none)
      result = RouteFiltering.filter_candidates_with_route_state(input, state)
      Support.restore!(fixture)
      Support.emit(fixture, "healthy_#{unquote(phase)}")
      assert FakeUpstream.physical_counts(fixture.fake).consume == 1
      expected = if unquote(phase) == :pending, do: "pending", else: "confirmed"
      assert result_summary(result).outcome == expected
      assert {:ok, [_], _options, _state} = result
    end
  end

  test "a probe failure before the caller's outer commit cannot claim applied recovery", %{support: support} do
    fixture = Support.fixture!(support)
    {input, state} = routing_input(fixture)
    :ok = Support.arm!(fixture, :nested_probe)

    {:error, result} =
      Repo.transaction(fn ->
        result =
          try do
            RouteFiltering.filter_candidates_with_route_state(input, state)
          rescue
            exception -> {:raised, exception.__struct__}
          end

        Repo.rollback(result)
      end)

    Support.restore!(fixture)
    Support.assert_no_applied_witness!(fixture)
    refute result_summary(result)[:outcome] in ["pending", "confirmed"]
    assert Repo.reload!(fixture.identity).metadata["saved_reset_redemption"] == nil
    assert FakeUpstream.physical_counts(fixture.fake).consume == 1
  end

  defp routing_input(fixture) do
    payload = %{"model" => fixture.model.exposed_model_id, "input" => "synthetic reset recovery"}
    options = %{} |> RequestOptions.build("/backend-api/codex/responses", payload) |> RequestOptions.put_routing(reset_probe: ResetProbe.new())
    input = FilterInput.new(%{auth: %{pool: fixture.pool, api_key: fixture.api_key}, model: fixture.model, endpoint: "/backend-api/codex/responses", payload: payload, request_options: options, candidates: [{fixture.assignment, fixture.identity}]})
    state = RouteState.new(%{visible_model: input.model, candidates: input.candidates}) |> RouteState.preload_routing_snapshots(input.auth, input.model, options)
    {input, state}
  end

  defp result_summary({:error, error}), do: %{status: error.status, outcome: error[:non_credit_recovery_outcome]}
  defp result_summary({:ok, _candidates, options, _state}), do: %{status: :admitted, outcome: options.routing.quota_decision["non_credit_recovery_outcome"]}
  defp result_summary({:raised, module}), do: %{raised: module}
end
