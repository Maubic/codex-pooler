defmodule CodexPooler.Gateway.Runtime.WebsocketQuotaDenialTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Catalog.Model
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Routing.CandidateEligibility.AccountDenial
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext
  alias CodexPooler.Gateway.Runtime.Finalization.SideEffects
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.AccountAvailabilityStore
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Quota.Windows

  for code <- ["usage_limit_reached", "usage_limit_exceeded"] do
    test "terminal #{code} revokes same-cycle permission through persisted frame evidence" do
      {context, headers, reset_at} = permitted_context()
      assert eligibility(context).eligible?

      assert :ok =
               SideEffects.observe_websocket_response(context, %{
                 terminal: "response.failed",
                 upstream_error_code: unquote(code),
                 websocket_frame_headers: headers
               })

      refute eligibility(context).eligible?
      [window] = runtime_windows(context)
      assert window.window_kind == "secondary"
      assert window.window_minutes == 10_080
      assert DateTime.compare(window.reset_at, reset_at) == :eq
      assert Decimal.equal?(window.used_percent, Decimal.new(100))
      assert window.metadata["rate_limit_reached"] == true
      assert window.metadata["rate_limit_error_code"] == unquote(code)
      assert window.source == "codex_rate_limit_error"
      assert window.merge_precedence == 80
    end
  end

  test "pre-visible quota retry records the same denial" do
    {context, headers, _reset_at} = permitted_context()

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               reason: {:quota_exhausted_first_event, %{code: "usage_limit_reached"}},
               websocket_frame_headers: headers
             })

    refute eligibility(context).eligible?
    assert hd(runtime_windows(context)).metadata["rate_limit_reached"] == true
  end

  test "later percentage-only stream observation does not erase the explicit same-cycle denial" do
    {context, headers, _reset_at} = permitted_context()

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               reason: {:quota_exhausted_first_event, %{code: "usage_limit_reached"}},
               websocket_frame_headers: headers
             })

    assert :ok =
             SideEffects.observe_stream_response(
               context,
               %Req.Response{headers: headers},
               "",
               nil
             )

    refute eligibility(context).eligible?
    assert Enum.any?(runtime_windows(context), &(&1.metadata["rate_limit_reached"] == true))
  end

  test "observed denial blocks an available grant without previous quota windows" do
    {context, headers, _reset_at} = permitted_context()
    Repo.delete_all(AccountQuotaWindow)
    assert eligibility(context).eligible?

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               terminal: "response.failed",
               upstream_error_code: "usage_limit_reached",
               websocket_frame_headers: headers
             })

    refute eligibility(context).eligible?
    assert hd(runtime_windows(context)).metadata["rate_limit_reached"] == true
  end

  test "observed quota after the denied cycle resets becomes eligible" do
    {context, headers, reset_at} = permitted_context()

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               terminal: "response.failed",
               upstream_error_code: "usage_limit_reached",
               websocket_frame_headers: headers
             })

    refute eligibility(context).eligible?
    denied_snapshot = Windows.load_routing_quota_snapshots([context.identity.id], DateTime.utc_now())
    denied_state = %RouteState{visible_model: nil, quota_snapshots: denied_snapshot}
    assert AccountDenial.candidate_exclusion({context.assignment, context.identity}, denied_state, %Model{exposed_model_id: "sample-model", upstream_model_id: "sample-model"}, RequestOptions.for_websocket(%{}, %{}))
    recovered_at = DateTime.add(reset_at, 1, :second)

    recovered_headers =
      headers
      |> Map.put("x-codex-primary-used-percent", "20")
      |> Map.put(
        "x-codex-primary-reset-at",
        Integer.to_string(DateTime.to_unix(reset_at) + 604_800)
      )

    assert {:ok, [_window]} =
             Windows.upsert_quota_windows_from_codex_headers(
               context.identity,
               recovered_headers,
               recovered_at
             )

    snapshot =
      [context.identity.id]
      |> Windows.load_routing_quota_snapshots(recovered_at)
      |> Map.fetch!(context.identity.id)

    assert Windows.routing_quota_eligibility_from_snapshot(snapshot).eligible?
    recovered_state = %RouteState{visible_model: nil, quota_snapshots: %{context.identity.id => snapshot}}
    refute AccountDenial.candidate_exclusion({context.assignment, context.identity}, recovered_state, %Model{exposed_model_id: "sample-model", upstream_model_id: "sample-model"}, RequestOptions.for_websocket(%{}, %{}))
  end

  test "denial does not attach to a non-exhausted window in the same observation" do
    {context, headers, reset_at} = permitted_context()

    headers =
      Map.merge(headers, %{
        "x-codex-secondary-used-percent" => "25",
        "x-codex-secondary-window-minutes" => "300",
        "x-codex-secondary-reset-at" => Integer.to_string(DateTime.to_unix(reset_at))
      })

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               terminal: "response.failed",
               upstream_error_code: "usage_limit_reached",
               websocket_frame_headers: headers
             })

    windows = runtime_windows(context)
    assert length(windows) == 2
    assert Enum.find(windows, &(&1.window_minutes == 10_080)).metadata["rate_limit_reached"]
    refute Enum.find(windows, &(&1.window_minutes == 300)).metadata["rate_limit_reached"]
  end

  test "denial retains the dispatched model scope for generic spark headers" do
    {context, headers, _reset_at} = permitted_context()
    context = %{context | model: %{upstream_model_id: "gpt-5.3-codex-spark"}}

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               terminal: "response.failed",
               upstream_error_code: "usage_limit_reached",
               websocket_frame_headers: headers
             })

    [window] = runtime_windows(context)
    assert window.quota_scope == "model"
    assert window.model == "gpt-5.3-codex-spark"
    assert window.metadata["rate_limit_reached"] == true
  end

  test "out-of-order denial cannot overwrite a newer header observation" do
    {context, headers, _reset_at} = permitted_context()
    newer_at = DateTime.add(DateTime.utc_now(), 1, :second)

    assert {:ok, [newer]} =
             Windows.upsert_quota_windows_from_codex_headers(
               context.identity,
               Map.put(headers, "x-codex-primary-used-percent", "20"),
               newer_at
             )

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               terminal: "response.failed",
               upstream_error_code: "usage_limit_reached",
               websocket_frame_headers: headers
             })

    window = Enum.find(runtime_windows(context), &(&1.source == "codex_response_headers"))
    assert window.observed_at == newer.observed_at
    assert Decimal.equal?(window.used_percent, Decimal.new(20))
    refute Map.has_key?(window.metadata, "rate_limit_reached")
  end

  for {terminal, code} <- [
        {"response.failed", "server_error"},
        {"response.completed", nil},
        {"response.completed", "usage_limit_reached"}
      ] do
    test "#{terminal}/#{inspect(code)} keeps percentage-only observations" do
      {context, headers, _reset_at} = permitted_context()

      assert :ok =
               SideEffects.observe_websocket_response(context, %{
                 terminal: unquote(terminal),
                 upstream_error_code: unquote(code),
                 websocket_frame_headers: headers
               })

      assert eligibility(context).eligible?
      [window] = runtime_windows(context)
      refute Map.has_key?(window.metadata, "rate_limit_reached")
      refute Map.has_key?(window.metadata, "rate_limit_error_code")
    end
  end

  test "denial without a reset does not synthesize reset-bearing denial evidence" do
    {context, headers, _reset_at} = permitted_context()

    assert :ok =
             SideEffects.observe_websocket_response(context, %{
               terminal: "response.failed",
               upstream_error_code: "usage_limit_reached",
               websocket_frame_headers: Map.delete(headers, "x-codex-primary-reset-at")
             })

    [window] = runtime_windows(context)
    assert is_nil(window.reset_at)
    refute Map.has_key?(window.metadata, "rate_limit_reached")
  end

  defp permitted_context do
    %{identity: identity, assignment: assignment} = active_upstream_assignment_fixture()
    observed_at = DateTime.add(DateTime.utc_now(), -1, :second)
    reset_at = observed_at |> DateTime.add(3600, :second) |> DateTime.truncate(:second)

    metadata =
      identity.metadata
      |> Map.put("credential_epoch", 1)
      |> Map.put(
        AccountAvailabilityStore.metadata_key(),
        AccountAvailabilityStore.encode!(:available, observed_at, 1)
      )

    identity = identity |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()

    assert {:ok, [_window]} =
             Windows.upsert_quota_windows_from_codex_usage_payload(
               identity,
               %{
                 "rate_limit" => %{
                   "allowed" => true,
                   "limit_reached" => false,
                   "primary_window" => %{
                     "used_percent" => 100,
                     "limit_window_seconds" => 604_800,
                     "reset_at" => DateTime.to_unix(reset_at)
                   }
                 }
               },
               observed_at
             )

    headers = %{
      "x-codex-primary-used-percent" => "100",
      "x-codex-primary-window-minutes" => "10080",
      "x-codex-primary-reset-at" => Integer.to_string(DateTime.to_unix(reset_at))
    }

    {%SelectedCandidateContext{identity: identity, assignment: assignment, model: %{upstream_model_id: "sample-model"}}, headers, reset_at}
  end

  defp eligibility(context) do
    snapshot =
      [context.identity.id]
      |> Windows.load_routing_quota_snapshots(DateTime.utc_now())
      |> Map.fetch!(context.identity.id)

    Windows.routing_quota_eligibility_from_snapshot(snapshot)
  end

  defp runtime_windows(context) do
    context.identity
    |> Windows.list_evidence()
    |> Enum.filter(&(&1.source in ["codex_response_headers", "codex_rate_limit_error"]))
  end
end
