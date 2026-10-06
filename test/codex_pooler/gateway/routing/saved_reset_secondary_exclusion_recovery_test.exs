defmodule CodexPooler.Gateway.Routing.SavedResetSecondaryExclusionRecoveryTest do
  use CodexPooler.DataCase, async: false
  import CodexPooler.PoolerFixtures
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.RouteFiltering
  alias CodexPooler.Gateway.Routing.SavedResetAutoRedeem
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.TestDiagnostics
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows
  alias CodexPooler.Upstreams.SavedResets.AutoEligibility
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  test "usable five-hour account plus corroborated exhausted secondary recovers through the real route" do
    %{upstream: fake, target: target, input: input} = arrangement()
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    {:ok, context} = %{filter_input: input} |> SavedResetAutoRedeem.gateway_auto_context(target.assignment, Repo.reload!(target.identity), :blocked_weekly_exhaustion) |> AutoEligibility.normalize_context()
    assert AutoEligibility.validate_locked_gateway_auto(Repo.reload!(target.identity), target.assignment, context, timestamp) == :ok
    assert AutoEligibility.validate_reserved_gateway_auto(Repo.reload!(target.identity), target.assignment, context, timestamp) == :ok
    {result, calls} = traced_route(input)
    TestDiagnostics.puts("SECONDARY_FENCE_CALLS " <> CodexPooler.JSON.encode!(calls))
    receipt("secondary_positive", fake)
    assert {:ok, [{assignment, identity}], options, _state} = result
    assert Map.fetch!(calls, :validate_locked_gateway_auto) >= 1
    assert Map.fetch!(calls, :validate_reserved_gateway_auto) >= 1
    assert Map.fetch!(calls, :target_windows_resettable?) >= 1
    assert assignment.id == target.assignment.id
    assert identity.id == target.identity.id
    assert ResetProbe.bound?(options.routing.reset_probe)
    assert consume_count(fake) == 1
    assert priority_generation_count(fake) == 0
    assert Repo.reload!(identity).metadata["saved_resets"]["available_count"] == 1
    assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "consumed_pending_probe"
  end

  for veto <- [:disabled, :uncorroborated, :unexplained_jump, :cooldown, :latch, :keep_credits, :primary, :model, :feature] do
    test "secondary recovery preserves #{veto} zero-spend fence" do
      veto = unquote(veto)
      %{upstream: fake, target: target, input: input} = arrangement(veto)
      assert {:error, _} = route(input)
      receipt(Atom.to_string(veto), fake)
      assert consume_count(fake) == 0
      assert priority_generation_count(fake) == 0
      assert Repo.reload!(target.identity).metadata["saved_resets"]["available_count"] == 2
    end
  end

  # The window read runs only for a candidate the policy and bank already accept: the scan
  # evaluates the two predicates once per candidate, in that order, and stops at the first refusal.
  for veto <- [:disabled, :keep_credits] do
    test "a #{veto} candidate is refused before its windows are read" do
      %{upstream: fake, target: target, input: input} = arrangement(unquote(veto))
      {result, calls} = traced_route(input)
      assert {:error, _} = result
      assert Map.get(calls, :target_windows_resettable?, 0) == 0
      assert consume_count(fake) == 0
      assert Repo.reload!(target.identity).metadata["saved_resets"]["available_count"] == 2
    end
  end

  for veto <- [:not_fresh, :stale, :model_scope, :feature_family, :wrong_key, :empty_reasons, :mixed_reasons, :wrong_code] do
    test "secondary scan refuses #{veto} shape before real provider I/O" do
      %{upstream: fake, target: target, input: input} = arrangement()
      reason = %{code: "quota_window_unusable", quota_key: "account", quota_scope: "account", quota_family: "account", window_kind: "secondary", reason_codes: ["exhausted"]}

      reason =
        case unquote(veto) do
          :not_fresh -> %{reason | reason_codes: ["not_fresh"]}
          :stale -> %{reason | reason_codes: ["exhausted", "stale"]}
          :model_scope -> %{reason | quota_scope: "model"}
          :feature_family -> %{reason | quota_family: "additional"}
          :wrong_key -> %{reason | quota_key: "sample_feature"}
          :empty_reasons -> %{reason | reason_codes: []}
          :mixed_reasons -> %{reason | reason_codes: ["exhausted", "not_fresh"]}
          :wrong_code -> %{reason | code: "quota_missing"}
        end

      error = {:error, %{code: "quota_exhausted"}}
      exclusions = [%{pool_upstream_assignment_id: target.assignment.id, upstream_identity_id: target.identity.id, reasons: [reason]}]
      assert {:error, %{code: "quota_exhausted", non_credit_recovery_outcome: "unavailable"}} = SavedResetAutoRedeem.recover_non_credit_exhaustion(%{candidate_exclusions: exclusions, result: error}, %{filter_input: input}, :required, DateTime.utc_now(), [])
      receipt(Atom.to_string(unquote(veto)), fake)
      assert consume_count(fake) == 0
      assert priority_generation_count(fake) == 0
      refute Repo.reload!(target.identity).metadata["saved_reset_redemption"]
    end
  end

  defp traced_route(input) do
    collector = start_supervised!({Task, fn -> collect_calls(%{}) end})
    monitor = Process.monitor(collector)
    functions = [validate_locked_gateway_auto: 4, validate_reserved_gateway_auto: 4, target_windows_resettable?: 3]
    for {function, arity} <- functions, do: :erlang.trace_pattern({AutoEligibility, function, arity}, true, [:local])
    :erlang.trace(self(), true, [:call, {:tracer, collector}])

    try do
      result = route(input)
      :erlang.trace(self(), false, [:call])
      reference = :erlang.trace_delivered(self())
      assert_receive {:trace_delivered, _tracee, ^reference}
      send(collector, {:counts, self()})
      assert_receive {:counts, counts}
      {result, counts}
    after
      :erlang.trace(self(), false, [:call])
      for {function, arity} <- functions, do: :erlang.trace_pattern({AutoEligibility, function, arity}, false, [:local])
      send(collector, :stop)
      assert_receive {:DOWN, ^monitor, :process, ^collector, :normal}
    end
  end

  defp collect_calls(counts) do
    receive do
      {:trace, _pid, :call, {AutoEligibility, function, _arguments}} ->
        collect_calls(Map.update(counts, function, 1, &(&1 + 1)))

      {:counts, caller} ->
        send(caller, {:counts, counts})
        collect_calls(counts)

      :stop ->
        :ok
    end
  end

  defp route(input) do
    candidates = Enum.map(input.candidates, fn {assignment, identity} -> {assignment, Repo.reload!(identity)} end)
    input = FilterInput.put_candidates(input, candidates)
    state = RouteState.new(%{visible_model: input.model, candidates: input.candidates}) |> RouteState.preload_routing_snapshots(input.auth, input.model, input.request_options)
    RouteFiltering.filter_candidates_with_route_state(input, state)
  end

  defp arrangement(veto \\ nil) do
    {:ok, fake} = FakeUpstream.start_link({:path_json, %{"/api/codex/rate-limit-reset-credits/consume" => {200, %{"code" => "reset"}}, "/api/codex/usage" => {200, usage_payload(1)}}})
    on_exit(fn -> FakeUpstream.stop(fake) end)
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    target = active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake, 2)})
    sibling = active_upstream_assignment_fixture(pool, %{metadata: saved_reset_metadata(fake, 1)})
    target = %{target | identity: enable_saved_reset_auto_redeem!(target.identity, %{saved_reset_auto_redeem_trigger_mode: "blocked"})}
    input = filter_input(pool, api_key, [{sibling.assignment, sibling.identity}, {target.assignment, target.identity}], "secondary-recovery")
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    assert {:ok, [_]} = QuotaWindows.upsert_quota_windows(target.identity, [primary_quota_attrs(Decimal.new("10")) |> Map.merge(%{observed_at: now, last_sync_at: now})])
    assert {:ok, [_]} = QuotaWindows.upsert_quota_windows(target.identity, [weekly_exhausted_quota_attrs() |> Map.merge(%{observed_at: now, last_sync_at: now})])

    confirmation_opts =
      case veto do
        :uncorroborated -> [observations: 1]
        :unexplained_jump -> [approach: false]
        _ -> []
      end

    SavedResetConfirmationFixtures.confirm_automatic_pressure!(target.identity, confirmation_opts)
    # The expired model group has just crossed the account Usage API drop TTL.
    old = DateTime.add(now, -Evidence.freshness_ttl_seconds(), :second)
    assert {:ok, [_]} = QuotaWindows.upsert_quota_windows(target.identity, [weekly_exhausted_quota_attrs() |> Map.merge(%{quota_key: "sample_model", quota_scope: "model", quota_family: "codex_model", model: input.model.exposed_model_id, reset_at: DateTime.add(now, -1, :second), observed_at: old, last_sync_at: old})])
    apply_veto(target.identity, veto, input.model, now)
    %{upstream: fake, target: target, input: input}
  end

  defp apply_veto(identity, :disabled, _model, _now),
    do: identity |> Repo.reload!() |> Ecto.Changeset.change(saved_reset_auto_redeem_enabled: false) |> Repo.update!()

  defp apply_veto(identity, :keep_credits, _model, _now),
    do: identity |> Repo.reload!() |> Ecto.Changeset.change(saved_reset_auto_redeem_keep_credits: 2) |> Repo.update!()

  defp apply_veto(identity, phase, _model, now) when phase in [:cooldown, :latch] do
    identity = Repo.reload!(identity)
    redemption = %{"status" => "succeeded", "phase" => if(phase == :cooldown, do: "confirmed_by_quota", else: "consumed_pending_probe"), "attempt_id" => Ecto.UUID.generate(), "generation" => 1, "trigger_kind" => "gateway_auto", "consumed_at" => DateTime.to_iso8601(DateTime.add(now, -1, :minute)), "result" => %{"code" => "reset", "applied" => true}}
    identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption)) |> Repo.update!()
    assert AutoEligibility.identity_consume_latch(Repo.reload!(identity), now) == if(phase == :cooldown, do: :cooldown, else: :blocked_awaiting_quota)
  end

  defp apply_veto(identity, :primary, _model, _now) do
    assert {:ok, [_]} = QuotaWindows.upsert_quota_windows(identity, [primary_quota_attrs(Decimal.new("100"))])
  end

  defp apply_veto(identity, scope, model, _now) when scope in [:model, :feature] do
    Repo.delete_all(from(w in AccountQuotaWindow, where: w.upstream_identity_id == ^identity.id and w.quota_scope == "model"))
    assert {:ok, [_]} = QuotaWindows.upsert_quota_windows(identity, [weekly_exhausted_quota_attrs() |> Map.merge(%{quota_key: "sample_meter", quota_scope: "model", quota_family: if(scope == :model, do: "codex_model", else: "additional"), model: model.exposed_model_id})])
  end

  defp apply_veto(_identity, _veto, _model, _now), do: :ok

  defp receipt(scenario, fake) do
    TestDiagnostics.puts("SECONDARY_RECOVERY_RECEIPT " <> CodexPooler.JSON.encode!(%{scenario: scenario, consumes: consume_count(fake), generation_sends: priority_generation_count(fake)}))
  end

  defp filter_input(pool, api_key, candidates, suffix) when is_list(candidates) do
    model =
      model_fixture(pool, %{
        exposed_model_id: "gpt-route-filtering-#{suffix}-#{System.unique_integer([:positive])}",
        metadata: %{
          "source_assignment_ids" => Enum.map(candidates, fn {assignment, _identity} -> assignment.id end)
        }
      })

    payload = %{"model" => model.exposed_model_id, "input" => "route filtering"}
    request_options = request_options(payload)

    FilterInput.new(%{
      auth: %{pool: pool, api_key: api_key},
      model: model,
      endpoint: "/backend-api/codex/responses",
      payload: payload,
      request_options: request_options,
      candidates: candidates
    })
  end

  defp request_options(payload) do
    %{}
    |> RequestOptions.build("/backend-api/codex/responses", payload)
    |> RequestOptions.put_routing(reset_probe: ResetProbe.new())
  end

  defp priority_generation_count(fake) do
    Enum.count(FakeUpstream.requests(fake), fn request ->
      not String.ends_with?(request.path, "/usage") and not String.contains?(request.path, "/rate-limit-reset-credits")
    end)
  end

  defp saved_reset_metadata(upstream, available_count, saved_reset_attrs \\ %{}) do
    observed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601()

    saved_resets =
      Map.merge(
        %{
          "status" => "reported",
          "available_count" => available_count,
          "source" => "codex_usage_api",
          "path_style" => "codex_api",
          "observed_at" => observed_at,
          "usage_path" => "/api/codex/usage",
          "reason" => nil
        },
        saved_reset_attrs
      )

    %{
      "usage_base_url" => FakeUpstream.url(upstream),
      "saved_resets" => saved_resets
    }
  end

  defp enable_saved_reset_auto_redeem!(%UpstreamIdentity{} = identity, attrs) do
    identity
    |> UpstreamIdentity.changeset(
      Map.merge(
        %{
          saved_reset_auto_redeem_enabled: true,
          saved_reset_auto_redeem_min_blocked_minutes: 60,
          saved_reset_auto_redeem_keep_credits: 0,
          updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
        },
        attrs
      )
    )
    |> Repo.update!()
  end

  defp weekly_exhausted_quota_attrs do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %{
      quota_key: "account",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new("100"),
      reset_at: DateTime.add(now, 2, :hour),
      observed_at: now,
      last_sync_at: now,
      source: "codex_usage_api",
      source_precision: "observed",
      quota_scope: "account",
      quota_family: "account",
      freshness_state: "fresh"
    }
  end

  defp primary_quota_attrs(used_percent) do
    weekly_exhausted_quota_attrs()
    |> Map.merge(%{
      window_kind: "primary",
      window_minutes: 300,
      used_percent: used_percent
    })
  end

  defp usage_payload(available_count, opts \\ []) do
    observed_at = Keyword.get_lazy(opts, :observed_at, &DateTime.utc_now/0)
    window_minutes = Keyword.get(opts, :window_minutes, 10_080)
    reset_at = Keyword.get(opts, :reset_at, DateTime.add(observed_at, 900, :second))

    %{
      "plan_type" => "pro",
      "rate_limit_reset_credits" => %{"available_count" => available_count},
      "credits" => %{"has_credits" => false, "unlimited" => false, "balance" => "0"},
      "spend_control" => %{"reached" => false},
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => %{
          "used_percent" => Keyword.get(opts, :used_percent, 10),
          "limit_window_seconds" => window_minutes * 60,
          "reset_after_seconds" => DateTime.diff(reset_at, observed_at, :second),
          "reset_at" => DateTime.to_unix(reset_at)
        }
      }
    }
  end

  defp consume_count(fake) do
    Enum.count(
      FakeUpstream.requests(fake),
      &(&1.path == "/api/codex/rate-limit-reset-credits/consume")
    )
  end
end
