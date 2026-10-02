defmodule CodexPooler.Gateway.Routing.ProviderCreditsTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Quotas.CapacityFacts
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.ProviderCreditsPolicy
  alias CodexPooler.Upstreams.Quota.{AccountAvailabilityStore, AccountQuotaWindow, CapacityAssessment, CapacityFactsStore, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows

  @model "synthetic-ordinary"
  @context %{model: @model, upstream_model: @model, serving_mode: :full, transport: :http_sse}

  for policy <- [true, false] do
    test "included capacity is independent of credits and policy #{policy}" do
      snapshot = snapshot(:included, unquote(policy))
      decision = Upstreams.provider_credits_decision(snapshot, @context)
      assert decision.eligible?
      assert decision.capacity_basis == :included_window
      assert CapacityAssessment.non_credit_usable?(snapshot, @context)
    end

    test "spend limit preserves independent included permission with policy #{policy}" do
      snapshot = snapshot(:included, unquote(policy))
      snapshot = %{snapshot | capacity_facts: %{snapshot.capacity_facts | credit_permission: :unavailable, denial_category: :spend_limit}}
      decision = Upstreams.provider_credits_decision(snapshot, @context)
      assert decision.eligible?
      assert decision.capacity_basis == :included_window
      refute CapacityAssessment.credit_usable?(snapshot, @context)
    end

    test "fresh windowless included permission serves with policy #{policy}" do
      snapshot = snapshot(:windowless_included, unquote(policy))
      decision = Upstreams.provider_credits_decision(snapshot, @context)
      assert decision.eligible?
      assert decision.capacity_basis == :windowless_provider_permission
    end
  end

  @tag credits_negative: true
  test "credit-only facts never launder legacy availability when disabled" do
    snapshot = snapshot(:windowless_credit, false)
    decision = Upstreams.provider_credits_decision(snapshot, @context)
    refute decision.eligible?
    assert decision.capacity_basis == :provider_credits
    assert decision.reason_codes == ["provider_credits_disabled"]
    refute CapacityAssessment.non_credit_usable?(snapshot, @context)
    assert snapshot.raw_windows == []
  end

  @tag credits_negative: true
  test "caller qualification cannot replace current provider credit authority" do
    snapshot = snapshot(:windowless_credit, true)
    scope = scope(snapshot)
    proposed = Map.put(@context, :qualified_credit_scopes, [scope])
    decision = Upstreams.provider_credits_decision(snapshot, proposed)
    assert decision.eligible?
    assert decision == Upstreams.provider_credits_decision(snapshot, @context)
    assert decision.qualification.status == :provider_attested

    unverified = %{snapshot | capacity_facts: %{snapshot.capacity_facts | credit_permission: :unknown}}
    refute ProviderCreditsPolicy.evaluate(unverified, proposed).eligible?
    refute Upstreams.provider_credits_decision(unverified, proposed).eligible?
  end

  for {shape, minutes} <- [{:weekly, 10_080}, {:short, 300}, {:monthly, 43_200}, {:windowless_credit, nil}] do
    test "fresh #{shape} credit authority is policy and actual request scoped" do
      snapshot = snapshot(unquote(shape), true)
      scope = scope(snapshot)
      context = Map.put(@context, :qualified_credit_scopes, [scope])
      on = ProviderCreditsPolicy.evaluate(snapshot, context)
      assert on.eligible?
      assert on.capacity_basis == :provider_credits
      assert on.qualification.status == :provider_attested
      refute ProviderCreditsPolicy.evaluate(%{snapshot | allow_provider_credits: false}, context).eligible?
      assert ProviderCreditsPolicy.evaluate(snapshot, %{context | upstream_model: "other-model"}).eligible?
      assert ProviderCreditsPolicy.evaluate(snapshot, %{context | serving_mode: :lite}).eligible?
      assert ProviderCreditsPolicy.evaluate(snapshot, %{context | transport: :native_websocket}).eligible?
      refute ProviderCreditsPolicy.evaluate(snapshot, %{context | serving_mode: nil}).eligible?
      refute ProviderCreditsPolicy.evaluate(snapshot, %{context | transport: nil}).eligible?
      assert scope.account_windows == if(is_nil(unquote(minutes)), do: [], else: [{if(unquote(minutes) == 10_080, do: "secondary", else: "primary"), unquote(minutes)}])
    end
  end

  for invalid <- [:stale, :future, :epoch, :malformed, :later_denial] do
    @tag credits_negative: true
    test "#{invalid} evidence cannot grant even under an exact proposed contract" do
      valid = snapshot(:weekly, true)
      context = Map.put(@context, :qualified_credit_scopes, [scope(valid)])
      snapshot = invalidate(valid, unquote(invalid))
      refute ProviderCreditsPolicy.evaluate(snapshot, context).eligible?
      refute CapacityAssessment.credit_usable?(snapshot, @context)
    end
  end

  for phase <- ["consuming", "consumed_pending_probe"] do
    for policy <- [true, false] do
      @tag credits_negative: true
      test "#{phase} same-identity credit fallback stays fenced with policy #{policy}" do
        snapshot = snapshot(:weekly, unquote(policy))
        snapshot = %{snapshot | redemption: %{"phase" => unquote(phase)}}
        context = Map.put(@context, :qualified_credit_scopes, [scope(snapshot)])
        decision = ProviderCreditsPolicy.evaluate(snapshot, context)
        refute decision.eligible?
        assert decision.reason_codes == ["saved_reset_probe_pending"]
      end
    end
  end

  for policy <- [true, false], credit <- [:available, :unknown, :unavailable] do
    test "guarded reset proof is independent of policy #{policy} and credit #{credit}" do
      snapshot = snapshot(:weekly, unquote(policy))
      snapshot = %{snapshot | capacity_facts: %{snapshot.capacity_facts | credit_permission: unquote(credit)}}
      assert CapacityAssessment.guarded_probe_permitted?(snapshot, @context) == (unquote(credit) == :unavailable)
    end
  end

  @tag credits_negative: true
  test "rounded display balance and successful history are not credit authority" do
    snapshot = snapshot(:weekly, true)
    snapshot = %{snapshot | capacity_facts: nil, capacity_facts_reported?: false, credit_balance: %{balance: 99_976}}
    refute Upstreams.provider_credits_decision(snapshot, @context).eligible?
    refute CapacityAssessment.credit_usable?(snapshot, @context)
  end

  test "fresh provider credit authority admits a compatible model without a probe-model allowlist" do
    snapshot = qualified_weekly_snapshot()
    decision = Upstreams.provider_credits_decision(snapshot, @context)

    assert decision.eligible?
    assert decision.capacity_basis == :provider_credits
    assert decision.qualification.status == :provider_attested
    assert decision.qualification.scope.model == @model
    refute CapacityAssessment.non_credit_usable?(snapshot, @context)
  end

  for phase <- ["reblocked", "expired"] do
    test "terminal #{phase} reset recovery does not disable independently usable credits" do
      snapshot = %{qualified_weekly_snapshot() | redemption: %{"phase" => unquote(phase)}}
      decision = Upstreams.provider_credits_decision(snapshot, @context)

      assert decision.eligible?
      assert decision.capacity_basis == :provider_credits
      refute CapacityAssessment.guarded_probe_permitted?(snapshot, @context)

      disabled = Upstreams.provider_credits_decision(%{snapshot | allow_provider_credits: false}, @context)
      refute disabled.eligible?
      assert disabled.reason_codes == ["provider_credits_disabled"]
    end
  end

  @tag credits_negative: true
  test "legacy attested windowless capacity is last unknown basis only on" do
    snapshot = snapshot(:windowless_included, true)
    snapshot = %{snapshot | capacity_facts: nil, capacity_facts_reported?: false}
    on = Upstreams.provider_credits_decision(snapshot, @context)
    assert on.eligible?
    assert on.capacity_basis == :unknown_legacy
    refute Upstreams.provider_credits_decision(%{snapshot | allow_provider_credits: false}, @context).eligible?
    refute CapacityAssessment.non_credit_usable?(snapshot, @context)
    refute Upstreams.provider_credits_decision(%{snapshot | capacity_facts_reported?: true}, @context).eligible?
  end

  for shape <- [:weekly, :short, :monthly, :windowless_credit] do
    @tag credits_negative: true
    test "guarded recovery never widens #{shape} quota exclusions merely because credits are unavailable" do
      snapshot = snapshot(unquote(shape), false)
      snapshot = %{snapshot | availability: nil, capacity_facts: %{snapshot.capacity_facts | credit_permission: :unavailable, has_credits: false, balance: "0"}}
      assert CapacityAssessment.guarded_probe_permitted?(snapshot, @context) == (unquote(shape) == :weekly)
      refute CapacityAssessment.guarded_probe_exclusions?([])
    end
  end

  for mode <- [:full, :lite], transport <- [:http_sse, :native_websocket, :bridged_websocket] do
    test "fresh weekly provider credit authority permits #{mode} #{transport} only while enabled" do
      snapshot = qualified_weekly_snapshot()
      context = %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: unquote(mode), transport: unquote(transport)}
      decision = Upstreams.provider_credits_decision(snapshot, context)
      assert decision.eligible?
      assert decision.capacity_basis == :provider_credits
      assert decision.qualification.status == :provider_attested
      refute CapacityAssessment.non_credit_usable?(snapshot, context)
      refute CapacityAssessment.guarded_probe_permitted?(snapshot, context)
      disabled = Upstreams.provider_credits_decision(%{snapshot | allow_provider_credits: false}, context)
      refute disabled.eligible?
      assert disabled.reason_codes == ["provider_credits_disabled"]
    end
  end

  test "account-only projection reports conditional capacity while runtime still requires request context" do
    snapshot = qualified_weekly_snapshot()
    projection = Upstreams.provider_credits_decision(snapshot, %{account_only: true})
    assert projection.eligible?
    assert projection.capacity_basis == :provider_credits
    assert projection.qualification.status == :provider_attested
    refute Upstreams.provider_credits_decision(snapshot, %{}).eligible?
    assert Upstreams.provider_credits_decision(snapshot, %{model: "ordinary-model", upstream_model: "ordinary-model", serving_mode: :full, transport: :http_sse}).eligible?
  end

  for invalid <- [:zero, :window_shape, :pending, :workspace, :mode] do
    @tag credits_negative: true
    test "credit authority never overrides #{invalid}" do
      snapshot = qualified_weekly_snapshot()
      context = %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: :full, transport: :http_sse}
      {snapshot, context} = invalidate_qualified(snapshot, context, unquote(invalid))
      refute Upstreams.provider_credits_decision(snapshot, context).eligible?
      if unquote(invalid) != :mode, do: refute(Upstreams.provider_credits_decision(snapshot, %{account_only: true}).eligible?)
    end
  end

  for variation <- [:unlimited, :source, :model, :transport] do
    test "provider authority does not inherit the diagnostic restriction #{variation}" do
      snapshot = qualified_weekly_snapshot()
      context = %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: :full, transport: :http_sse}
      {snapshot, context} = invalidate_qualified(snapshot, context, unquote(variation))
      assert Upstreams.provider_credits_decision(snapshot, context).eligible?
      refute Upstreams.provider_credits_decision(%{snapshot | allow_provider_credits: false}, context).eligible?
    end
  end

  test "same-receipt canonical weekly reset pinning does not erase observed credit authority" do
    snapshot = qualified_weekly_snapshot()
    [weekly] = snapshot.raw_windows
    weekly = %{weekly | reset_at: DateTime.add(weekly.reset_at, 3, :second), metadata: Map.put(weekly.metadata || %{}, "credential_epoch", snapshot.credential_epoch)}
    snapshot = %{snapshot | raw_windows: [weekly]}
    context = %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: :full, transport: :http_sse}
    assert Upstreams.provider_credits_decision(snapshot, context).eligible?
    refute Upstreams.provider_credits_decision(%{snapshot | raw_windows: [%{weekly | reset_at: DateTime.add(weekly.reset_at, 60, :second)}]}, context).eligible?
    refute Upstreams.provider_credits_decision(%{snapshot | raw_windows: [%{weekly | observed_at: DateTime.add(weekly.observed_at, -1, :second)}]}, context).eligible?
  end

  test "fresh qualified weekly credits supersede only older ordinary refusals, not workspace or current provider refusal" do
    snapshot = qualified_weekly_snapshot()
    [weekly] = snapshot.raw_windows
    refusal = %{weekly | source: "codex_rate_limit_error", observed_at: DateTime.add(weekly.observed_at, -120, :second), metadata: %{"rate_limit_error_code" => "usage_limit_reached", "rate_limit_reached" => true}}
    context = %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: :full, transport: :http_sse}
    snapshot = %{snapshot | raw_windows: [weekly, refusal]}
    assert Upstreams.provider_credits_decision(snapshot, context).eligible?
    refute CapacityAssessment.non_credit_usable?(snapshot, context)
    workspace = %{refusal | metadata: Map.put(refusal.metadata, "rate_limit_reached_type", "workspace_owner_usage_limit_reached")}
    refute Upstreams.provider_credits_decision(%{snapshot | raw_windows: [weekly, workspace]}, context).eligible?
    equal_clock = %{refusal | observed_at: weekly.observed_at}
    refute Upstreams.provider_credits_decision(%{snapshot | raw_windows: [weekly, equal_clock]}, context).eligible?
    newest = %{refusal | observed_at: DateTime.add(weekly.observed_at, 1, :second)}
    refute Upstreams.provider_credits_decision(%{snapshot | as_of: newest.observed_at, raw_windows: [weekly, newest]}, context).eligible?
  end

  test "one-second ordinary rate-limit stream rounding does not revoke fresh qualified weekly credits" do
    snapshot = qualified_weekly_snapshot()
    [weekly] = snapshot.raw_windows
    runtime = %{weekly | source: "codex_rate_limit_event", observed_at: DateTime.add(weekly.observed_at, 1, :second), reset_at: DateTime.add(weekly.reset_at, 1, :second), metadata: %{}}
    context = %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: :lite, transport: :bridged_websocket}
    snapshot = %{snapshot | as_of: runtime.observed_at, raw_windows: [weekly, runtime]}
    assert Upstreams.provider_credits_decision(snapshot, context).eligible?
    shifted = %{runtime | reset_at: DateTime.add(weekly.reset_at, 6, :second)}
    refute Upstreams.provider_credits_decision(%{snapshot | raw_windows: [weekly, shifted]}, context).eligible?
    denied = %{runtime | metadata: %{"rate_limit_reached_type" => "workspace_member_credits_depleted"}}
    refute Upstreams.provider_credits_decision(%{snapshot | raw_windows: [weekly, denied]}, context).eligible?
  end

  defp qualified_weekly_snapshot do
    snapshot = snapshot(:weekly, true)
    %{snapshot | capacity_facts: %{snapshot.capacity_facts | source_kind: :wham_usage}}
  end

  defp invalidate_qualified(snapshot, context, :unlimited), do: {%{snapshot | capacity_facts: %{snapshot.capacity_facts | unlimited: true, balance: nil}}, context}
  defp invalidate_qualified(snapshot, context, :zero), do: {%{snapshot | capacity_facts: %{snapshot.capacity_facts | balance: "0", has_credits: false, credit_permission: :unavailable}}, context}
  defp invalidate_qualified(snapshot, context, :source), do: {%{snapshot | capacity_facts: %{snapshot.capacity_facts | source_kind: :api_codex_usage}}, context}
  defp invalidate_qualified(snapshot, context, :model), do: {snapshot, %{context | upstream_model: "unqualified-model"}}
  defp invalidate_qualified(snapshot, context, :mode), do: {snapshot, %{context | serving_mode: :unknown}}
  defp invalidate_qualified(snapshot, context, :transport), do: {snapshot, %{context | transport: :http_json}}
  defp invalidate_qualified(snapshot, context, :window_shape), do: {%{snapshot | raw_windows: Enum.map(snapshot.raw_windows, &%{&1 | window_kind: "primary", window_minutes: 300})}, context}
  defp invalidate_qualified(snapshot, context, :pending), do: {%{snapshot | redemption: %{"phase" => "consumed_pending_probe"}}, context}
  defp invalidate_qualified(snapshot, context, :workspace), do: {%{snapshot | capacity_facts: %{snapshot.capacity_facts | denial_category: :workspace_limit}}, context}

  defp snapshot(shape, policy) do
    %{identity: identity} = active_upstream_assignment_fixture()
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    reset = DateTime.add(now, 7200, :second)
    windowless? = shape in [:windowless_credit, :windowless_included]
    included? = shape in [:included, :windowless_included]

    {kind, minutes} = window_shape(shape)
    percent = if included?, do: "10", else: "100"
    descriptors = descriptors(windowless?, kind, minutes, reset, percent)
    facts = %CapacityFacts{observed_at: now, credential_epoch: 1, included_permission: if(included?, do: :available, else: :exhausted), credit_permission: :available, denial_category: if(included?, do: :none, else: :included_limit), balance: "0.25", has_credits: true, unlimited: false, account_windows: descriptors, source_kind: :api_codex_usage}

    metadata =
      identity.metadata
      |> Map.put("credential_epoch", 1)
      |> Map.put(CapacityFactsStore.metadata_key(), CapacityFactsStore.encode!(facts, 1))
      |> Map.put(AccountAvailabilityStore.metadata_key(), AccountAvailabilityStore.encode!(if(included? or windowless?, do: :available, else: :blocked), now, 1))

    identity = identity |> Ecto.Changeset.change(metadata: metadata, allow_provider_credits: policy) |> CodexPooler.Repo.update!()

    windows =
      if windowless? do
        []
      else
        {:ok, windows} = Windows.upsert_quota_windows(identity, [%{quota_key: "account", quota_scope: "account", quota_family: "account", window_kind: kind, window_minutes: minutes, used_percent: Decimal.new(percent), reset_at: reset, observed_at: now, last_sync_at: now, source: "codex_usage_api", source_precision: "observed", freshness_state: "fresh"}])
        windows
      end

    RoutingQuotaSnapshot.from_identity(identity, windows, now)
  end

  defp window_shape(:short), do: {"primary", 300}
  defp window_shape(:monthly), do: {"primary", 43_200}
  defp window_shape(_weekly), do: {"secondary", 10_080}
  defp descriptors(true, _kind, _minutes, _reset, _percent), do: []
  defp descriptors(false, kind, minutes, reset, percent), do: [%{window_kind: kind, window_minutes: minutes, reset_at: reset, used_percent: percent}]

  defp scope(snapshot), do: %{model: @model, serving_mode: :full, transport: :http_sse, source_kind: snapshot.capacity_facts.source_kind, account_windows: CapacityAssessment.credit_window_shape(snapshot, @context)}
  defp invalidate(snapshot, :stale), do: %{snapshot | as_of: DateTime.add(snapshot.as_of, 3600, :second)}
  defp invalidate(snapshot, :future), do: %{snapshot | capacity_facts: %{snapshot.capacity_facts | observed_at: DateTime.add(snapshot.as_of, 1, :second)}}
  defp invalidate(snapshot, :epoch), do: %{snapshot | credential_epoch: 2}
  defp invalidate(snapshot, :malformed), do: %{snapshot | capacity_facts: %{snapshot.capacity_facts | credit_permission: :unknown, denial_category: :malformed}}

  defp invalidate(snapshot, :later_denial) do
    [%AccountQuotaWindow{} = window] = snapshot.raw_windows
    denial = %AccountQuotaWindow{window | source: "codex_rate_limit_error", observed_at: DateTime.add(snapshot.as_of, 1, :second), metadata: %{"rate_limit_allowed" => false}}
    %{snapshot | as_of: DateTime.add(snapshot.as_of, 2, :second), raw_windows: [window, denial]}
  end
end
