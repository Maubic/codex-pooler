defmodule CodexPooler.Gateway.Runtime.ProviderCreditsBoundaryTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 2, native_text_input: 1, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.CapacityFactsStore
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation

  @upstream_path "/backend-api/codex/responses"

  for endpoint <- ["/backend-api/codex/responses", "/v1/responses"], mode <- ["full", "lite"], stream <- [false, true] do
    @tag provider_credit_cases: ["C01", "C17", "B1"]
    test "included capacity survives opt-out and spend control on #{endpoint} #{mode} stream=#{stream}", %{conn: conn} do
      usage = ProviderCreditsFixtures.usage_payload(:spend_blocked, credits: :full)
      {upstream, setup} = reconciled_setup(usage, unquote(mode), false, unquote(stream))
      response = request(conn, setup, unquote(endpoint), unquote(stream))
      assert response.status == 200
      assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
      assert [%Request{status: "succeeded"}] = Repo.all(Request)
      assert [%Attempt{status: "succeeded"}] = Repo.all(Attempt)
      assert Repo.aggregate(LedgerEntry, :count) == 3
      identity = Repo.reload!(setup.identity)
      refute identity.allow_provider_credits
      assert {:ok, facts} = CapacityFactsStore.load(identity.metadata)
      assert facts.included_permission == :available
      assert facts.credit_permission == :unavailable
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([identity.id], DateTime.utc_now())[identity.id]
      assert Enum.all?(snapshot.raw_windows, &(&1.metadata["credential_epoch"] == facts.credential_epoch))
      if unquote(stream), do: assert(response.resp_body =~ "response.completed")
    end

    for state <- [:weekly_credit_only, :short_credit_only, :monthly_credit_only, :mixed_credit_only, :windowless_credit_only] do
      @tag provider_credit_cases: ["C09", "C11", "C12", "C13", "C14", "C28"]
      test "fresh #{state} permission admits the actual scope on #{endpoint} #{mode} stream=#{stream}", %{conn: conn} do
        {upstream, setup} = reconciled_setup(ProviderCreditsFixtures.usage_payload(unquote(state)), unquote(mode), true, unquote(stream))
        response = request(conn, setup, unquote(endpoint), unquote(stream))
        assert response.status == 200
        if unquote(stream), do: assert(response.resp_body =~ "response.completed")
        assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
        assert [%Attempt{status: "succeeded"} = attempt] = Repo.all(Attempt)
        assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
        refute attempt.response_metadata["provider_credits_admission"]["non_credit_guarded_probe"]
        assert Repo.aggregate(LedgerEntry, :count) == 3
        assert {:ok, facts} = CapacityFactsStore.load(Repo.reload!(setup.identity).metadata)
        assert facts.credit_permission == :available
        assert Repo.reload!(setup.identity).allow_provider_credits
      end
    end

    @tag credits_negative: true
    @tag provider_credit_cases: ["C19", "B2"]
    test "workspace denial cannot be laundered by credits on #{endpoint} #{mode} stream=#{stream}", %{conn: conn} do
      {upstream, setup} = reconciled_setup(ProviderCreditsFixtures.usage_payload(:workspace_blocked), unquote(mode), true, unquote(stream))
      response = request(conn, setup, unquote(endpoint), unquote(stream))
      assert response.status in [429, 503]
      assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0
      assert {:ok, %{denial_category: :workspace_limit}} = CapacityFactsStore.load(Repo.reload!(setup.identity).metadata)
    end

    @tag provider_credit_cases: ["C31", "T5"]
    test "newly attested included capacity resumes with opt-out unchanged on #{endpoint} #{mode} stream=#{stream}", %{conn: conn} do
      {upstream, setup} = reconciled_setup(ProviderCreditsFixtures.usage_payload(:windowless_credit_only), unquote(mode), false, unquote(stream))
      assert request(conn, setup, unquote(endpoint), unquote(stream)).status in [429, 503]
      assert FakeUpstream.physical_counts(upstream).http_generation == 0
      included = ProviderCreditsFixtures.usage_payload(:windowless_included, credits: :none)
      FakeUpstream.set_mode(upstream, upstream_routes(included, unquote(stream)))
      assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(Repo.reload!(setup.identity), setup.assignment)
      refute identity.allow_provider_credits
      response = request(Phoenix.ConnTest.build_conn(), %{setup | identity: identity}, unquote(endpoint), unquote(stream))
      assert response.status == 200
      assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
      assert [%Attempt{status: "succeeded"}] = Repo.all(Attempt)
    end
  end

  @tag credits_negative: true
  @tag provider_credit_cases: ["C20", "B3"]
  test "credential replacement cannot inherit the prior credit permission", %{conn: conn} do
    {upstream, setup} = reconciled_setup(ProviderCreditsFixtures.usage_payload(:windowless_credit_only), "full", true, false)
    identity = Repo.reload!(setup.identity)
    old_epoch = CredentialFencing.credential_epoch(identity)
    identity = Repo.update!(Ecto.Changeset.change(identity, metadata: Map.put(identity.metadata, "credential_epoch", old_epoch + 1)))
    assert {:ok, facts} = CapacityFactsStore.load(identity.metadata)
    refute CapacityFactsStore.fresh?(facts, old_epoch + 1, DateTime.utc_now())
    FakeUpstream.set_mode(upstream, FakeUpstream.json_response(%{}))
    response = request(conn, %{setup | identity: identity}, "/backend-api/codex/responses", false)
    assert response.status in [429, 503]
    assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
    assert Repo.aggregate(Attempt, :count) == 0
    assert Repo.aggregate(LedgerEntry, :count) == 0
  end

  for endpoint <- ["/backend-api/codex/responses", "/v1/responses"], mode <- ["full", "lite"], enabled <- [true, false] do
    @tag credits_negative: true
    test "fresh weekly WHAM finite credits physically stream only with opt-in on #{endpoint} #{mode} enabled=#{enabled}", %{conn: conn} do
      usage = ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :fractional) |> Map.put("rate_limit_reset_credits", %{"available_count" => 0})
      {upstream, setup} = luna_setup(usage, unquote(mode), unquote(enabled), true)
      assert {:ok, facts} = CapacityFactsStore.load(Repo.reload!(setup.identity).metadata)
      assert facts.source_kind == :wham_usage
      assert facts.included_permission == :exhausted
      assert facts.credit_permission == :available
      assert [%{window_kind: "secondary", window_minutes: 10_080}] = Enum.map(facts.account_windows, &Map.take(&1, [:window_kind, :window_minutes]))
      bank_before = Repo.reload!(setup.identity).metadata["saved_resets"]

      response = request(conn, setup, unquote(endpoint), true)

      if unquote(enabled) do
        assert response.status == 200
        assert response.resp_body =~ "response.completed"
        assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
        assert [%Attempt{status: "succeeded"} = attempt] = Repo.all(Attempt)
        assert attempt.upstream_identity_id == setup.identity.id
        assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
        refute attempt.response_metadata["provider_credits_admission"]["non_credit_guarded_probe"]
        assert [%Request{status: "succeeded", usage_status: "usage_known", transport: "http_sse"} = logged] = Repo.all(Request)
        assert logged.requested_model == "gpt-6-luna"
        settlement = Repo.get_by!(LedgerEntry, request_id: logged.id, entry_kind: "settlement")
        assert settlement.usage_status == "usage_known"
        assert {settlement.input_tokens, settlement.output_tokens, settlement.total_tokens} == {10, 2, 12}
        assert [generation] = Enum.filter(FakeUpstream.requests(upstream), &(&1.path == @upstream_path))
        assert generation.json["model"] == "gpt-6-luna"
        headers = Map.new(generation.headers)
        if unquote(mode) == "lite", do: assert(headers["x-openai-internal-codex-responses-lite"] == "true"), else: refute(Map.has_key?(headers, "x-openai-internal-codex-responses-lite"))
        assert attempt.response_metadata["routing"]["model_serving_mode"] == unquote(mode)
        assert generation.json["stream"]
      else
        assert response.status in [429, 503]
        assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
        assert Repo.aggregate(Attempt, :count) == 0
        assert Repo.aggregate(LedgerEntry, :count) == 0
      end

      identity = Repo.reload!(setup.identity)
      assert identity.allow_provider_credits == unquote(enabled)
      assert identity.metadata["saved_resets"] == bank_before
      refute identity.metadata["saved_reset_redemption"]
    end
  end

  for mode <- ["full", "lite"], boundary <- [:other_model, :short, :monthly, :mixed, :windowless, :codex_source, :unlimited, :unlimited_flag_missing, :workspace, :spend, :malformed_spend, :unknown] do
    @tag credits_negative: true
    test "actual credit authority distinguishes #{boundary} from unusable permission #{mode}", %{conn: conn} do
      state = %{short: :short_credit_only, monthly: :monthly_credit_only, mixed: :mixed_credit_only, windowless: :windowless_credit_only, workspace: :workspace_blocked, malformed_spend: :malformed_spend}[unquote(boundary)] || :weekly_credit_only
      credits = %{unlimited: :unlimited, unknown: :unknown}[unquote(boundary)] || :fractional
      usage = ProviderCreditsFixtures.usage_payload(state, credits: credits) |> Map.put("rate_limit_reset_credits", %{"available_count" => 0})
      usage = if unquote(boundary) == :unlimited_flag_missing, do: update_in(usage["credits"], &Map.delete(&1, "unlimited")), else: usage
      usage = if unquote(boundary) == :spend, do: Map.put(usage, "spend_control", %{"reached" => true}), else: usage
      source = if unquote(boundary) == :codex_source, do: :codex_usage, else: :wham_usage
      upstream_model = if unquote(boundary) == :other_model, do: "gpt-6-astra", else: "gpt-6-luna"
      {upstream, setup} = luna_setup(usage, unquote(mode), true, true, source: source, upstream_model_id: upstream_model)
      assert {:ok, facts} = CapacityFactsStore.load(Repo.reload!(setup.identity).metadata)
      assert facts.source_kind == source

      response = request(conn, setup, "/backend-api/codex/responses", true)

      if unquote(boundary) in [:unlimited_flag_missing, :workspace, :spend, :malformed_spend, :unknown] do
        assert response.status in [429, 503]
        assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
        assert Repo.aggregate(Attempt, :count) == 0
        assert Repo.aggregate(LedgerEntry, :count) == 0
      else
        assert response.status == 200
        assert response.resp_body =~ "response.completed"
        assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
        assert [%Attempt{status: "succeeded"} = attempt] = Repo.all(Attempt)
        assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
        assert [generation] = Enum.filter(FakeUpstream.requests(upstream), &(&1.path == @upstream_path))
        assert generation.json["model"] == upstream_model
        assert Repo.aggregate(LedgerEntry, :count) == 3
      end

      refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
    end
  end

  for mode <- ["full", "lite"], extra <- [:short, :weekly, :monthly] do
    @tag credits_negative: true
    test "weekly-primary WHAM receipt cannot hide its second raw #{extra} account resource #{mode}", %{conn: conn} do
      usage = ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :fractional)
      weekly = get_in(usage, ["rate_limit", "secondary_window"])
      extra_window = Map.put(weekly, "limit_window_seconds", %{short: 18_000, weekly: 604_800, monthly: 2_592_000}[unquote(extra)])
      usage = usage |> put_in(["rate_limit", "primary_window"], weekly) |> put_in(["rate_limit", "secondary_window"], extra_window)
      {upstream, setup} = luna_setup(usage, unquote(mode), true, true)
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([setup.identity.id], DateTime.utc_now())[setup.identity.id]
      decision = Upstreams.provider_credits_decision(snapshot, %{model: "gpt-6-luna", upstream_model: "gpt-6-luna", serving_mode: unquote(mode), transport: :http_sse})
      refute decision.eligible?
      response = request(conn, setup, "/backend-api/codex/responses", true)
      assert response.status in [429, 503]
      assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0
    end
  end

  for mode <- ["full", "lite"] do
    test "fresh permission admits HTTP JSON generation #{mode}", %{conn: conn} do
      usage = ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :fractional) |> Map.put("rate_limit_reset_credits", %{"available_count" => 0})
      {upstream, setup} = luna_setup(usage, unquote(mode), true, false)
      response = request(conn, setup, "/backend-api/codex/responses", false)
      assert response.status == 200
      assert json_response(response, 200)["status"] == "completed"
      assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(upstream)
      assert [%Attempt{status: "succeeded"} = attempt] = Repo.all(Attempt)
      assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
      assert Repo.aggregate(LedgerEntry, :count) == 3
    end
  end

  defp luna_setup(usage, mode, enabled, stream, opts \\ []) do
    source = Keyword.get(opts, :source, :wham_usage)
    opts = Keyword.merge(opts, exposed_model_id: "gpt-6-luna", upstream_model_id: Keyword.get(opts, :upstream_model_id, "gpt-6-luna"), source: source)
    reconciled_setup(usage, mode, enabled, stream, opts)
  end

  defp reconciled_setup(usage, mode, enabled, stream, opts \\ []) do
    {:path_json, routes} = upstream_routes(usage, stream)

    routes =
      case opts[:source] do
        :wham_usage -> routes |> Map.put("/api/codex/usage", {404, %{}}) |> Map.put("/backend-api/codex/usage", {404, %{}})
        :codex_usage -> routes |> Map.put("/api/codex/usage", {404, %{}}) |> Map.put("/backend-api/wham/usage", {404, %{}})
        nil -> routes
      end

    upstream = start_upstream({:path_json, routes})

    setup =
      gateway_setup(upstream,
        quota?: false,
        exposed_model_id: Keyword.get_lazy(opts, :exposed_model_id, fn -> "synthetic-boundary-#{System.unique_integer([:positive])}" end),
        upstream_model_id: Keyword.get(opts, :upstream_model_id, "synthetic-provider-boundary")
      )

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: now, updated_at: now})
    identity = Repo.update!(Ecto.Changeset.change(setup.identity, metadata: Map.put(setup.identity.metadata, "usage_base_url", FakeUpstream.url(upstream)), allow_provider_credits: enabled))
    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(identity, setup.assignment)
    {upstream, %{setup | identity: identity}}
  end

  defp request(conn, setup, endpoint, stream) do
    conn |> auth(setup) |> post(endpoint, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic provider credit boundary"), "stream" => stream})
  end

  defp upstream_routes(usage, stream) do
    completed = %{"id" => "resp_synthetic_boundary", "object" => "response", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 2, "total_tokens" => 12}}

    generation =
      if stream,
        do: FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => completed}}], headers: [{"x-synthetic-boundary", "true"}]),
        else: FakeUpstream.json_response(completed)

    {:path_json, Map.put(ProviderCreditsFixtures.usage_routes(usage), @upstream_path, generation)}
  end
end
