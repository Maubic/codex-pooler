defmodule CodexPooler.Gateway.Routing.AccountDenialBoundaryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Routing.CandidateEligibility.AccountDenial
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.CandidateEligibility.Quota
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Quota.AccountAvailabilityStore
  alias CodexPooler.Upstreams.Quota.Windows

  setup do
    pool = pool_fixture()
    denied = active_upstream_assignment_fixture(pool)
    model = model_fixture(pool, %{exposed_model_id: "sample-model"})
    %{pool: pool, denied: denied, model: model, now: DateTime.utc_now() |> DateTime.truncate(:second)}
  end

  for code <- ~w(usage_limit_reached usage_limit_exceeded) do
    test "#{code} on a model window does not deny an ordinary-model candidate", ctx do
      assert {:ok, [window]} = write_denial(ctx.denied.identity, ctx.now, "gpt-5.3-codex-spark", nil, unquote(code))
      assert window.quota_scope == "model"
      assert window.metadata["rate_limit_error_code"] == unquote(code)
      {input, state} = filter_context(ctx, [ctx.denied], ctx.now)
      assert {:ok, input.candidates} == AccountDenial.filter_candidates(input, input.candidates, nil, state)
      refute AccountDenial.candidate_exclusion(hd(input.candidates), state)
    end
  end

  test "account usage errors and workspace markers on model windows remain account-wide", ctx do
    for {model, marker} <- [{"sample-model", nil}, {"gpt-5.3-codex-spark", "workspace_member_credits_depleted"}] do
      target = active_upstream_assignment_fixture(ctx.pool)
      assert {:ok, [_]} = write_denial(target.identity, ctx.now, model, marker)
      {input, state} = filter_context(ctx, [target], ctx.now)
      assert {:error, %{code: "quota_exhausted"}} = AccountDenial.filter_candidates(input, input.candidates, nil, state)
    end
  end

  test "usage refusal on an unnamed feature meter retains the account denial", ctx do
    feature_headers = headers(ctx.now, "300", "42", nil) |> Map.new(fn {key, value} -> {String.replace(key, "x-codex-", "x-sample-feature-"), value} end)
    assert {:ok, [window]} = Windows.upsert_quota_windows_from_codex_headers(ctx.denied.identity, feature_headers, ctx.now, nil, "usage_limit_reached")
    assert window.quota_scope == "feature"
    {input, state} = filter_context(ctx, [ctx.denied], ctx.now)
    assert {:error, %{code: "quota_exhausted"}} = AccountDenial.filter_candidates(input, input.candidates, nil, state)
  end

  test "a confirmed reset candidate does not exempt its workspace-denied sibling", ctx do
    consumed_at = DateTime.add(ctx.now, -60, :second)

    probe =
      active_upstream_assignment_fixture(ctx.pool, %{
        metadata: %{
          "saved_reset_redemption" => %{
            "status" => "succeeded",
            "phase" => "confirmed_by_upstream",
            "generation" => 2,
            "attempt_id" => Ecto.UUID.generate(),
            "trigger_kind" => "gateway_auto",
            "consumed_at" => DateTime.to_iso8601(consumed_at),
            "deadline_at" => DateTime.to_iso8601(DateTime.add(ctx.now, 600, :second)),
            "result" => %{"code" => "reset", "applied" => true}
          }
        }
      })

    assert {:ok, [_]} = write_denial(ctx.denied.identity, ctx.now, nil, "workspace_member_credits_depleted")
    assert {:ok, [_]} = write_denial(probe.identity, ctx.now, nil, "workspace_member_credits_depleted", "usage_limit_reached", "10080", "100")
    {input, state} = filter_context(ctx, [ctx.denied, probe], ctx.now)
    assert {:ok, candidates, decision} = Quota.filter_quota_eligible_candidates(input, state)
    assert decision["reset_probe_candidate_count"] == 1
    assert length(candidates) == 2
    assert {:ok, [{assignment, _identity}]} = AccountDenial.filter_candidates(input, candidates, decision, state)
    assert assignment.id == probe.assignment.id
  end

  test "a bound reset probe only exempts its own assignment and identity", ctx do
    probe = active_upstream_assignment_fixture(ctx.pool)
    for target <- [ctx.denied, probe], do: assert({:ok, [_]} = write_denial(target.identity, ctx.now, nil, "workspace_member_credits_depleted"))
    {input, state} = filter_context(ctx, [ctx.denied, probe], ctx.now)
    {:ok, bound} = ResetProbe.bind(ResetProbe.new(), probe.assignment.id, probe.identity.id, ctx.model.upstream_model_id, "responses")
    state = RouteState.put_reset_probe(state, bound)
    assert {:ok, [{assignment, _identity}]} = AccountDenial.filter_candidates(input, input.candidates, nil, state)
    assert assignment.id == probe.assignment.id
  end

  test "unbound probe and route summary alone do not exempt a denied candidate", ctx do
    assert {:ok, [_]} = write_denial(ctx.denied.identity, ctx.now, nil, "workspace_member_credits_depleted")
    {input, state} = filter_context(ctx, [ctx.denied], ctx.now)
    state = RouteState.put_reset_probe(state, ResetProbe.new())
    assert {:error, %{code: "quota_exhausted"}} = AccountDenial.filter_candidates(input, input.candidates, %{"routing_state" => "reset_probe", "reset_probe_candidate_count" => 1}, state)
  end

  test "stale available evidence cannot lift a denial while fresh evidence can", ctx do
    assert {:ok, [_]} = write_denial(ctx.denied.identity, ctx.now, nil, "workspace_member_credits_depleted")
    available_at = DateTime.add(ctx.now, 60, :second)

    ctx.denied.identity
    |> Ecto.Changeset.change(metadata: Map.put(ctx.denied.identity.metadata, AccountAvailabilityStore.metadata_key(), AccountAvailabilityStore.encode!(:available, available_at, 1)))
    |> Repo.update!()

    {input, fresh} = filter_context(ctx, [ctx.denied], available_at)
    assert {:ok, input.candidates} == AccountDenial.filter_candidates(input, input.candidates, nil, fresh)
    {input, stale} = filter_context(ctx, [ctx.denied], DateTime.add(available_at, Evidence.freshness_ttl_seconds() + 1, :second))
    refute AccountAvailabilityStore.available?(stale.quota_snapshots[ctx.denied.identity.id].availability, 1, stale.quota_snapshots[ctx.denied.identity.id].as_of)
    assert {:error, %{code: "quota_exhausted"}} = AccountDenial.filter_candidates(input, input.candidates, nil, stale)
  end

  test "earliest reported reset deliberately releases denial despite a later marked reset", ctx do
    headers =
      headers(ctx.now, "300", "97", "workspace_member_credits_depleted")
      |> Map.merge(%{
        "x-codex-secondary-used-percent" => "96",
        "x-codex-secondary-window-minutes" => "10080",
        "x-codex-secondary-reset-at" => Integer.to_string(DateTime.to_unix(ctx.now) + 86_400)
      })

    assert {:ok, [_, _]} = Windows.upsert_quota_windows_from_codex_headers(ctx.denied.identity, headers, ctx.now, nil, "usage_limit_reached")
    {input, before_reset} = filter_context(ctx, [ctx.denied], DateTime.add(ctx.now, 3599, :second))
    assert {:error, %{code: "quota_exhausted"}} = AccountDenial.filter_candidates(input, input.candidates, nil, before_reset)
    {input, after_reset} = filter_context(ctx, [ctx.denied], DateTime.add(ctx.now, 3600, :second))
    assert {:ok, input.candidates} == AccountDenial.filter_candidates(input, input.candidates, nil, after_reset)
  end

  defp write_denial(identity, now, model, marker, code \\ "usage_limit_reached", minutes \\ "300", percent \\ "42") do
    Windows.upsert_quota_windows_from_codex_headers(identity, headers(now, minutes, percent, marker), now, model, code)
  end

  defp headers(now, minutes, percent, marker) do
    %{"x-codex-primary-used-percent" => percent, "x-codex-primary-window-minutes" => minutes, "x-codex-primary-reset-at" => Integer.to_string(DateTime.to_unix(now) + 3600)}
    |> then(fn headers -> if marker, do: Map.put(headers, "x-codex-rate-limit-reached-type", marker), else: headers end)
  end

  defp filter_context(ctx, targets, as_of) do
    candidates = Enum.map(targets, &{&1.assignment, Repo.reload!(&1.identity)})
    input = FilterInput.new(%{model: ctx.model, endpoint: "/backend-api/codex/responses", payload: %{}, request_options: RequestOptions.for_websocket(%{}, %{}), candidates: candidates})
    state = RouteState.new(%{visible_model: ctx.model, candidates: candidates, quota_snapshots: Windows.load_routing_quota_snapshots(Enum.map(targets, & &1.identity.id), as_of)})
    {input, state}
  end
end
