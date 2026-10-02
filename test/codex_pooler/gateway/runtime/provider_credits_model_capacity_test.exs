defmodule CodexPooler.Gateway.Runtime.ProviderCreditsModelCapacityTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, start_upstream: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Quota.{CapacityAssessment, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation

  @endpoint "/backend-api/codex/responses"
  @spark "gpt-5.3-codex-spark"

  for policy <- [true, false] do
    test "N2 N3 ordinary permission at 100 survives policy #{policy} without claiming a model override" do
      usage =
        ProviderCreditsFixtures.usage_payload(:included, credits: :none)
        |> put_in(["rate_limit", "secondary_window", "used_percent"], 100)
        |> Map.put("normal_model_slug", "synthetic-reserve")

      {fake, setup} = setup_model_capacity(usage, policy: unquote(policy), model: "synthetic-reserve")
      assert {:ok, %{status: 200}} = execute(setup)
      assert FakeUpstream.physical_counts(fake).http_generation == 1
      assert basis(setup) == :ordinary_provider_permission
    end

    test "N4 existing exact Spark allowance remains non-credit with policy #{policy}" do
      {fake, setup} = setup_model_capacity(spark_usage(), policy: unquote(policy), model: @spark)
      assert {:ok, %{status: 200}} = execute(setup)
      assert FakeUpstream.physical_counts(fake).http_generation == 1
      assert FakeUpstream.physical_counts(fake).consume == 0
      assert basis(setup) == :model_allowance
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([setup.identity.id], DateTime.utc_now())[setup.identity.id]
      refute CapacityAssessment.guarded_probe_permitted?(snapshot, %{model: @spark, upstream_model: @spark})
    end

    for counterfeit <- [:catalog, :normal_slug, :new_meter, :wrong_model] do
      @tag credits_negative: true
      test "N5 N6 N7 #{counterfeit} cannot grant requested capacity with policy #{policy}" do
        usage = counterfeit_usage(unquote(counterfeit))
        {fake, setup} = setup_model_capacity(usage, policy: unquote(policy), model: "synthetic-other")
        assert {:error, _error} = execute(setup)
        assert FakeUpstream.physical_counts(fake).http_generation == 0
        assert FakeUpstream.physical_counts(fake).consume == 0
        assert Repo.aggregate(Attempt, :count) == 0
      end
    end

    @tag credits_negative: true
    test "N6 historical successful generation cannot renew denied capacity with policy #{policy}" do
      {fake, setup} = setup_model_capacity(ProviderCreditsFixtures.usage_payload(:included, credits: :none), policy: unquote(policy), model: "synthetic-other")
      assert {:ok, %{status: 200}} = execute(setup)
      assert Repo.one!(Attempt).status == "succeeded"
      payload = counterfeit_usage(:history)
      FakeUpstream.set_mode(fake, {:path_json, Map.put(ProviderCreditsFixtures.usage_routes(payload), @endpoint, {200, %{"id" => "resp_forbidden_history", "object" => "response", "output" => []}})})
      assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(Repo.reload!(setup.identity), setup.assignment)
      assert {:error, _error} = execute(%{setup | identity: identity})
      assert FakeUpstream.physical_counts(fake).http_generation == 1
      assert FakeUpstream.physical_counts(fake).consume == 0
      assert Repo.aggregate(Attempt, :count) == 1
    end

    @tag credits_negative: true
    test "B2 exact requested-model denial beats ordinary permission and credits with policy #{policy}" do
      usage =
        spark_usage()
        |> put_in(["rate_limit", "allowed"], true)
        |> put_in(["rate_limit", "limit_reached"], false)
        |> Map.delete("rate_limit_reached_type")
        |> update_in(["additional_rate_limits"], fn [meter] ->
          [meter |> put_in(["rate_limit", "allowed"], false) |> put_in(["rate_limit", "limit_reached"], true)]
        end)
        |> Map.put("credits", %{"balance" => "25", "has_credits" => true, "unlimited" => false})

      {fake, setup} = setup_model_capacity(usage, policy: unquote(policy), model: @spark)
      now = DateTime.utc_now()
      bank = %{"status" => "reported", "available_count" => 2, "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => DateTime.to_iso8601(now), "usage_path" => "/api/codex/usage", "reason" => nil}
      identity = Repo.update!(Ecto.Changeset.change(setup.identity, saved_reset_auto_redeem_enabled: true, saved_reset_auto_redeem_min_blocked_minutes: 60, saved_reset_auto_redeem_keep_credits: 0, metadata: Map.put(setup.identity.metadata, "saved_resets", bank)))
      setup = %{setup | identity: identity}
      assert {:error, _error} = execute(setup)
      assert FakeUpstream.physical_counts(fake).http_generation == 0
      assert FakeUpstream.physical_counts(fake).consume == 0
    end
  end

  defp setup_model_capacity(usage, opts) do
    fake = start_upstream({:path_json, Map.put(ProviderCreditsFixtures.usage_routes(usage), @endpoint, {200, %{"id" => "resp_synthetic_model", "object" => "response", "output" => []}})})
    setup = gateway_setup(fake, quota?: false, exposed_model_id: opts[:model], upstream_model_id: opts[:model])
    identity = setup.identity |> Ecto.Changeset.change(metadata: Map.put(setup.identity.metadata, "usage_base_url", FakeUpstream.url(fake)), allow_provider_credits: opts[:policy]) |> Repo.update!()
    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(identity, setup.assignment)
    {fake, %{setup | identity: identity}}
  end

  defp execute(setup) do
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)
    payload = %{"model" => setup.model.exposed_model_id, "input" => []}
    options = RequestOptions.build(%{api_key_policy: policy}, @endpoint, payload)
    Service.execute(auth, @endpoint, payload, options)
  end

  defp basis(setup) do
    snapshot = RoutingQuotaSnapshot.load_by_identity_ids([setup.identity.id], DateTime.utc_now())[setup.identity.id]
    Upstreams.provider_credits_decision(snapshot, %{model: setup.model.exposed_model_id, upstream_model: setup.model.upstream_model_id}).capacity_basis
  end

  defp spark_usage do
    usage = ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :none)
    now = DateTime.utc_now()
    usage |> Map.put("additional_rate_limits", [%{"limit_name" => "GPT-5.3-Codex-Spark", "metered_feature" => "codex_bengalfox", "rate_limit" => %{"allowed" => true, "limit_reached" => false, "primary_window" => %{"used_percent" => 0, "limit_window_seconds" => 18_000, "reset_after_seconds" => 18_000, "reset_at" => DateTime.to_unix(DateTime.add(now, 18_000, :second))}, "secondary_window" => %{"used_percent" => 0, "limit_window_seconds" => 604_800, "reset_after_seconds" => 604_800, "reset_at" => DateTime.to_unix(DateTime.add(now, 604_800, :second))}}}])
  end

  defp counterfeit_usage(:wrong_model), do: spark_usage()

  defp counterfeit_usage(:new_meter) do
    update_in(spark_usage(), ["additional_rate_limits"], fn [meter] -> [Map.merge(meter, %{"limit_name" => "Synthetic Reserve", "metered_feature" => "synthetic_unknown_meter", "model" => "synthetic-other"})] end)
  end

  defp counterfeit_usage(:normal_slug), do: Map.put(ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :none), "normal_model_slug", "synthetic-other")
  defp counterfeit_usage(_kind), do: ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :none)
end
