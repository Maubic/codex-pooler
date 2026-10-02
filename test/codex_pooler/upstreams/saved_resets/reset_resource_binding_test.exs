defmodule CodexPooler.Upstreams.SavedResets.ResetResourceBindingTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.{FakeUpstream, ProviderCreditsFixtures, Repo}
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation
  alias CodexPooler.Upstreams.SavedResetRedemption
  alias CodexPooler.Upstreams.SavedResets.{Convergence, RedemptionLifecycle}

  for {resource, kind, minutes} <- [{:weekly, "secondary", 10_080}, {:monthly, "primary", 43_200}] do
    @tag :credits_negative
    test "an applied #{resource} consume stays pending for unrelated 5h evidence and confirms its captured resource" do
      fixture = reset_fixture!(unquote(resource), :applied)
      expected = [%{"window_kind" => unquote(kind), "window_minutes" => unquote(minutes)}]

      assert {:ok, %{applied?: true, phase: "consumed_pending_probe"}} =
               SavedResetRedemption.redeem(fixture.assignment)

      pending = redemption(fixture.identity)
      assert pending["included_window_descriptors"] == expected
      assert pending["result"]["applied"] == true
      assert RedemptionLifecycle.gateway_auto_latch(pending, DateTime.utc_now()) == :blocked_awaiting_quota

      prove_matching_resource_convergence!(fixture, pending, expected)
    end
  end

  @tag :credits_negative
  test "an actual ambiguous consume recovered from the same redeemed bank item keeps its original weekly binding" do
    fixture = reset_fixture!(:weekly, :ambiguous)

    assert {:error, :saved_reset_consume_outcome_ambiguous} = SavedResetRedemption.redeem(fixture.assignment)
    consuming = redemption(fixture.identity)
    expected = [%{"window_kind" => "secondary", "window_minutes" => 10_080}]
    assert consuming["phase"] == "consuming"
    assert consuming["included_window_descriptors"] == expected

    recovery_at = DateTime.add(DateTime.utc_now(), 90, :second)

    FakeUpstream.set_mode(
      fixture.fake,
      {:path_json,
       ProviderCreditsFixtures.usage_routes(omitted_usage())
       |> Map.put(
         "/backend-api/wham/rate-limit-reset-credits",
         {200,
          %{
            "available_count" => 0,
            "credits" => [%{"id" => "synthetic-resource-reset", "status" => "redeemed", "redeemed_at" => consuming["provider_replay"]["last_provider_dispatched_at"]}]
          }}
       )}
    )

    assert {:ok, %{applied?: true, phase: "consumed_pending_probe"}} =
             SavedResetRedemption.resume_stale_consuming(
               fixture.assignment,
               fixture.identity.id,
               consuming["attempt_id"],
               consuming["generation"],
               now: recovery_at,
               clock: fn -> recovery_at end,
               receive_timeout: 1_000
             )

    pending = redemption(fixture.identity)
    assert pending["included_window_descriptors"] == expected
    assert Map.take(pending, ["attempt_id", "generation"]) == Map.take(consuming, ["attempt_id", "generation"])
    assert pending["provider_replay"]["provider_dispatches"] == 1

    prove_matching_resource_convergence!(fixture, pending, expected)
  end

  defp reset_fixture!(resource, outcome) do
    supervisor_name = String.to_atom("reset_resource_binding_#{System.unique_integer([:positive, :monotonic])}")

    on_exit(fn ->
      case Process.whereis(supervisor_name) do
        pid when is_pid(pid) -> FakeUpstream.stop(%FakeUpstream{supervisor: pid})
        nil -> :ok
      end
    end)

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    initial = usage(resource, false, now, 1)
    {:ok, fake} = FakeUpstream.start_link({:path_json, ProviderCreditsFixtures.usage_routes(initial)}, supervisor_name: supervisor_name)

    %{assignment: assignment, identity: identity} =
      active_upstream_assignment_fixture(pool_fixture(), %{
        metadata: %{
          "base_url" => FakeUpstream.url(fake),
          "usage_base_url" => FakeUpstream.url(fake),
          "saved_resets" => %{
            "status" => "reported",
            "available_count" => 1,
            "source" => "codex_usage_api",
            "path_style" => "chatgpt_api",
            "usage_path" => "/backend-api/wham/usage",
            "observed_at" => DateTime.to_iso8601(now)
          }
        }
      })

    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(identity, assignment, observed_at: now)
    assert Enum.any?(Windows.list_evidence(identity), &(&1.window_minutes in [10_080, 43_200] and Decimal.equal?(&1.used_percent, 100)))

    consume = if outcome == :applied, do: {200, %{"code" => "reset"}}, else: :close_before_headers

    FakeUpstream.set_mode(
      fake,
      {:path_json,
       ProviderCreditsFixtures.usage_routes(omitted_usage())
       |> Map.put("/backend-api/wham/rate-limit-reset-credits", {200, %{"available_count" => 1, "credits" => [%{"id" => "synthetic-resource-reset", "status" => "available"}]}})
       |> Map.put("/backend-api/wham/rate-limit-reset-credits/consume", consume)}
    )

    %{fake: fake, identity: identity, assignment: assignment, resource: resource}
  end

  defp prove_matching_resource_convergence!(fixture, pending, expected) do
    observed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    FakeUpstream.set_mode(fixture.fake, {:path_json, ProviderCreditsFixtures.usage_routes(usage(:short, true, observed_at, 0))})

    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(Repo.reload!(fixture.identity), fixture.assignment, observed_at: observed_at)
    assert {:ok, :unchanged} = Convergence.converge(identity, observed_at, "reconciliation")
    unrelated = redemption(identity)
    assert unrelated["phase"] == "consumed_pending_probe"
    assert unrelated["included_window_descriptors"] == expected

    assert Map.take(unrelated, ["attempt_id", "generation", "consumed_at", "result"]) ==
             Map.take(pending, ["attempt_id", "generation", "consumed_at", "result"])

    assert RedemptionLifecycle.gateway_auto_latch(unrelated, observed_at) == :blocked_awaiting_quota
    assert {:error, :redemption_in_progress} = SavedResetRedemption.redeem(fixture.assignment)

    observed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    FakeUpstream.set_mode(fixture.fake, {:path_json, ProviderCreditsFixtures.usage_routes(usage(fixture.resource, true, observed_at, 0))})

    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(Repo.reload!(identity), fixture.assignment, observed_at: observed_at)
    assert {:ok, :confirmed_by_quota} = Convergence.converge(identity, observed_at, "reconciliation")
    confirmed = redemption(identity)
    assert confirmed["phase"] == "confirmed_by_quota"
    assert confirmed["included_window_descriptors"] == expected

    assert Map.take(confirmed, ["attempt_id", "generation", "consumed_at", "result"]) ==
             Map.take(pending, ["attempt_id", "generation", "consumed_at", "result"])

    assert FakeUpstream.physical_counts(fixture.fake).consume == 1
    assert FakeUpstream.physical_counts(fixture.fake).http_generation == 0
    assert FakeUpstream.physical_counts(fixture.fake).websocket_generation == 0
  end

  defp usage(resource, available?, now, reset_count) do
    state = if available?, do: :included, else: :weekly_credit_only
    # The restored long window reports a forward cycle, not a same-cycle lower snapshot that the evidence store must quarantine.
    reset_after = if available? and resource in [:weekly, :monthly], do: 14_400, else: 7_200

    ProviderCreditsFixtures.usage_payload(state, now: now, window: resource, credits: :none, reset_after: reset_after)
    |> Map.put("rate_limit_reset_credits", %{"available_count" => reset_count})
  end

  defp omitted_usage, do: %{"plan_type" => "synthetic", "rate_limit_reset_credits" => %{"available_count" => 0}}
  defp redemption(identity), do: Repo.reload!(identity).metadata["saved_reset_redemption"]
end
