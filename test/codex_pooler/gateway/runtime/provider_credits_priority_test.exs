defmodule CodexPooler.Gateway.Runtime.ProviderCreditsPriorityTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, gateway_upstream: 4, start_upstream: 1, execute_backend_stream!: 2, first_event_terminal_sse: 2]
  import ExUnit.CaptureLog

  alias CodexPooler.Access
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.BridgeAffinity
  alias CodexPooler.Gateway.Routing.CandidateEligibility.FilterInput
  alias CodexPooler.Gateway.Routing.SavedResetAutoRedeem
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.UnboxedFixture
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Quota.{RoutingQuotaSnapshot, Windows}
  alias CodexPooler.Upstreams.Reconciliation.PoolReconciliation
  alias CodexPooler.Upstreams.SavedResetRedemption
  alias CodexPooler.Upstreams.SavedResets.Convergence

  @endpoint "/backend-api/codex/responses"
  @consume "/api/codex/rate-limit-reset-credits/consume"

  for permission <- [:positive_drift, :finite_zero, :opt_out] do
    test "real reconciliation #{permission} controls final credit dispatch without included capacity" do
      {fake, setup} = arrangement(:weekly_credit_only, unquote(permission) != :opt_out, bank: 0, auto: false)
      payload = usage(:weekly_credit_only, credits: :full)
      second_credits = if unquote(permission) == :finite_zero, do: %{"balance" => "0", "has_credits" => false, "unlimited" => false}, else: %{"balance" => "0.25", "has_credits" => true, "unlimited" => false}
      {:path_json, replies} = routes(payload)
      replies = Map.put(replies, "/backend-api/codex/usage", {200, Map.put(payload, "credits", second_credits)})
      FakeUpstream.set_mode(fake, {:path_json, replies})
      setup = reconcile(setup, fake)

      if unquote(permission) == :positive_drift do
        assert {:ok, %{status: 200}} = execute(setup)
        attempt = Repo.one!(Attempt)
        assert attempt.pool_upstream_assignment_id == setup.assignment.id
        assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
        assert FakeUpstream.physical_counts(fake).http_generation == 1
      else
        assert {:error, _error} = execute(setup)
        assert Repo.aggregate(Attempt, :count) == 0
        assert FakeUpstream.physical_counts(fake).http_generation == 0
      end

      assert FakeUpstream.physical_counts(fake).consume == 0
    end
  end

  for refusal_clock <- [:same, :newer] do
    test "controller selects usable credits when below-limit included sibling has a #{refusal_clock} runtime account refusal" do
      {credit_fake, setup} = arrangement(:weekly_credit_only, true, bank: 2, auto: true)
      included_fake = start_upstream(routes(usage(:included, credits: :none)))
      included = gateway_upstream(setup.pool, included_fake, "synthetic-runtime-refused", []) |> reconcile(included_fake)
      setup = add_source(setup, included.assignment)
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([included.identity.id], DateTime.utc_now())[included.identity.id]
      observed_at = if unquote(refusal_clock) == :same, do: snapshot.capacity_facts.observed_at, else: DateTime.utc_now() |> DateTime.truncate(:microsecond)

      assert {:ok, [_window]} =
               Windows.upsert_quota_windows_from_codex_headers(included.identity, %{"x-codex-primary-used-percent" => "42", "x-codex-primary-window-minutes" => "300", "x-codex-primary-reset-at" => Integer.to_string(DateTime.to_unix(observed_at) + 3600)}, observed_at, setup.model.upstream_model_id, "usage_limit_reached")

      conn =
        build_conn()
        |> put_req_header("authorization", setup.authorization)
        |> Phoenix.ConnTest.dispatch(CodexPoolerWeb.Endpoint, :post, @endpoint, %{"model" => setup.model.exposed_model_id, "input" => []})

      assert conn.status == 200
      assert FakeUpstream.physical_counts(credit_fake).http_generation == 1
      assert FakeUpstream.physical_counts(credit_fake).consume == 0
      assert FakeUpstream.physical_counts(included_fake).http_generation == 0
      assert FakeUpstream.physical_counts(included_fake).consume == 0
      attempt = Repo.one!(Attempt)
      assert attempt.upstream_identity_id == setup.identity.id
      assert attempt.pool_upstream_assignment_id == setup.assignment.id
      assert attempt.status == "succeeded"
      assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
      assert Repo.reload!(setup.identity).metadata["saved_resets"]["available_count"] == 2
      refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
    end
  end

  for policy <- [true, false] do
    test "N1 N2 included weekly sibling wins before exhausted credits and bank with policy #{policy}" do
      {credit_fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 2, auto: true)
      included_fake = start_upstream(routes(usage(:included, window: :weekly, credits: :none)))
      sibling = gateway_upstream(setup.pool, included_fake, "synthetic-sibling-token", [])
      sibling = reconcile(sibling, included_fake)
      setup = add_source(setup, sibling.assignment)

      assert {:ok, %{status: 200}} = execute(setup)
      assert FakeUpstream.physical_counts(included_fake).http_generation == 1
      assert FakeUpstream.physical_counts(credit_fake).http_generation == 0
      assert FakeUpstream.physical_counts(credit_fake).consume == 0
      assert Repo.one!(Attempt).upstream_identity_id == sibling.identity.id
      assert Repo.reload!(setup.identity).allow_provider_credits == unquote(policy)
    end

    test "R1 R2 authorized bank restores included when credits are unavailable with policy #{policy}" do
      {fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 2, auto: true, credits: :none)
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(setup.identity, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")
      restored = usage(:included, window: :weekly, credits: :full) |> Map.put("rate_limit_reset_credits", %{"available_count" => 1})
      FakeUpstream.set_mode(fake, routes(restored))
      before = length(FakeUpstream.physical_receipts(fake))

      capture_log(fn -> assert {:ok, %{status: 200}} = execute(%{setup | identity: Repo.reload!(setup.identity)}) end)
      receipts = Enum.drop(FakeUpstream.physical_receipts(fake), before)
      assert Enum.count(receipts, &(&1.kind == :consume)) == 1
      assert Enum.count(receipts, &(&1.kind == :generation)) == 1
      consume = Enum.find(receipts, &(&1.kind == :consume))
      confirmation = Enum.find(receipts, &(&1.kind == :usage and &1.ordinal > consume.ordinal))
      generation = Enum.find(receipts, &(&1.kind == :generation))
      assert consume.ordinal < confirmation.ordinal
      assert confirmation.ordinal < generation.ordinal
      identity = Repo.reload!(setup.identity)
      assert identity.metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
      snapshot = RoutingQuotaSnapshot.load_by_identity_ids([identity.id], DateTime.utc_now())[identity.id]
      assert Upstreams.provider_credits_decision(snapshot, %{model: setup.model.exposed_model_id, upstream_model: setup.model.upstream_model_id}).capacity_basis == :recovered_included
      assert Repo.one!(Attempt).upstream_identity_id == identity.id
    end

    for fence <- [:auto_off, :empty, :reserved, :timing, :confirmation, :cooldown] do
      @tag credits_negative: true
      test "R3 #{fence} prevents consume while independent credits follow policy #{policy}" do
        {fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 2, auto: true)
        SavedResetConfirmationFixtures.confirm_automatic_pressure!(setup.identity, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage", observations: if(unquote(fence) == :confirmation, do: 1, else: 2))
        setup = apply_fence(setup, unquote(fence))
        redemption = Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
        capture_log(fn -> assert_credit_policy_generation!(setup, fake, unquote(policy)) end)
        assert FakeUpstream.physical_counts(fake).consume == 0
        assert Repo.reload!(setup.identity).metadata["saved_reset_redemption"] == redemption
        assert Repo.aggregate(Attempt, :count) == if(unquote(policy), do: 1, else: 0)
      end
    end

    test "T5 fresh natural included recovery serves without changing opt-out #{policy}" do
      {fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 0, auto: false)
      assert_credit_policy_generation!(setup, fake, unquote(policy))
      FakeUpstream.set_mode(fake, routes(usage(:included, window: :weekly, credits: :none)))
      setup = reconcile(setup, fake)
      assert {:ok, %{status: 200}} = execute(setup)
      assert FakeUpstream.physical_counts(fake).http_generation == if(unquote(policy), do: 2, else: 1)
      assert FakeUpstream.physical_counts(fake).consume == 0
      assert Repo.reload!(setup.identity).allow_provider_credits == unquote(policy)
    end
  end

  for shape <- [:windowless_credit_only, :short_credit_only, :monthly_credit_only, :mixed_credit_only], policy <- [true, false] do
    @tag credits_negative: true
    test "C2 C3 #{shape} uses only authorized credits without inventing included capacity or a reset with policy #{policy}" do
      {fake, setup} = arrangement(unquote(shape), unquote(policy), bank: 0, auto: false)
      assert_credit_policy_generation!(setup, fake, unquote(policy))
      assert FakeUpstream.physical_counts(fake).consume == 0
    end
  end

  for policy <- [true, false] do
    @tag credits_negative: true
    test "C13 reported bank cannot restore nonexistent windowless credit-only quota with policy #{policy}" do
      {fake, setup} = arrangement(:windowless_credit_only, unquote(policy), bank: 2, auto: true)
      assert_credit_policy_generation!(setup, fake, unquote(policy))
      assert FakeUpstream.physical_counts(fake).consume == 0
      refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
    end
  end

  for policy <- [true, false] do
    test "C4 explicit windowless non-credit capacity physically serves with policy #{policy}" do
      {fake, setup} = arrangement(:windowless_included, unquote(policy), bank: 2, auto: true)
      assert {:ok, %{status: 200}} = execute(setup)
      assert FakeUpstream.physical_counts(fake).http_generation == 1
      assert FakeUpstream.physical_counts(fake).consume == 0
    end
  end

  for policy <- [true, false] do
    test "T1 T34 shared identity policy never launders another Pool's included sibling with policy #{policy}" do
      {credit_fake, pool_a} = arrangement(:weekly_credit_only, unquote(policy), bank: 0, auto: false)
      included_fake = start_upstream(routes(usage(:included, credits: :none)))
      included = gateway_upstream(pool_a.pool, included_fake, "synthetic-pool-a-token", []) |> reconcile(included_fake)
      pool_a = add_source(pool_a, included.assignment)
      assert {:ok, %{status: 200}} = execute(pool_a)
      %{pool: pool_b, authorization: authorization_b} = CodexPooler.PoolerFixtures.active_api_key_fixture()
      assert {:ok, assignment_b} = PoolAssignments.create_pool_assignment(pool_b, pool_a.identity, %{assignment_label: "Synthetic shared", status: "active", health_status: "active", eligibility_status: "eligible", metadata: pool_a.assignment.metadata})
      source = pool_a.model.metadata["source_assignment_models"][pool_a.assignment.id]
      model_b = CodexPooler.PoolerFixtures.model_fixture(pool_b, %{exposed_model_id: pool_a.model.exposed_model_id, upstream_model_id: pool_a.model.upstream_model_id, supports_responses: true, supports_streaming: true, metadata: %{"source_assignment_ids" => [assignment_b.id], "source_assignment_models" => %{assignment_b.id => source}}})
      assert_credit_policy_generation!(%{pool_a | pool: pool_b, authorization: authorization_b, assignment: assignment_b, model: model_b}, credit_fake, unquote(policy))
      assert FakeUpstream.physical_counts(included_fake).http_generation == 1
      assert FakeUpstream.physical_counts(credit_fake).consume == 0
    end

    test "T2 remaining-cohort retry uses enabled credits before authorized bank with policy #{policy}" do
      {recovery_fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 2, auto: true)
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(setup.identity, usage_url: FakeUpstream.url(recovery_fake) <> "/api/codex/usage")
      failed_fake = start_upstream(routes(usage(:included, credits: :none)))
      first = gateway_upstream(setup.pool, failed_fake, "synthetic-retry-token", []) |> reconcile(failed_fake)
      setup = add_source(setup, first.assignment)
      FakeUpstream.set_mode(failed_fake, {:json_error, 503, %{"error" => %{"code" => "server_error", "message" => "synthetic pre-output refusal"}}})
      FakeUpstream.set_mode(recovery_fake, routes(usage(:included, credits: :full)))
      capture_log(fn -> assert {:ok, %{status: 200}} = execute(setup) end)
      [failed, recovered] = Repo.all(from(attempt in Attempt, order_by: attempt.attempt_number))
      assert failed.upstream_identity_id == first.identity.id
      assert failed.status == "retryable_failed"
      assert recovered.upstream_identity_id == setup.identity.id
      assert recovered.status == "succeeded"
      assert FakeUpstream.physical_counts(failed_fake).http_generation == 1
      assert FakeUpstream.physical_counts(recovery_fake).consume == if(unquote(policy), do: 0, else: 1)
      assert FakeUpstream.physical_counts(recovery_fake).http_generation == 1
      assert Repo.reload!(setup.identity).metadata["saved_reset_redemption"]["phase"] == if(unquote(policy), do: nil, else: "confirmed_by_quota")
      assert recovered.response_metadata["provider_credits_admission"]["capacity_basis"] == if(unquote(policy), do: "provider_credits", else: "recovered_included")
    end
  end

  for policy <- [true, false], visible? <- [false, true] do
    test "T2 SSE failure visible #{visible?} retains recovery replay boundary with policy #{policy}" do
      {recovery_fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 2, auto: true)
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(setup.identity, usage_url: FakeUpstream.url(recovery_fake) <> "/api/codex/usage")
      failed_fake = start_upstream(routes(usage(:included, credits: :none)))
      first = gateway_upstream(setup.pool, failed_fake, "synthetic-sse-retry-token", []) |> reconcile(failed_fake)
      setup = add_source(setup, first.assignment)

      failure =
        if unquote(visible?) do
          FakeUpstream.sse_stream(
            [
              {"response.output_text.delta", %{"type" => "response.output_text.delta", "delta" => "synthetic visible output"}},
              {"response.failed", %{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "server_error"}}}}
            ],
            done: false
          )
        else
          first_event_terminal_sse("response.failed", "upstream_request_timeout")
        end

      FakeUpstream.set_mode(failed_fake, failure)
      {:path_json, routes} = routes(usage(:included, credits: :full))
      success = FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_sse_recovered", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}}}], done: false, headers: [{"cache-control", "no-cache"}])
      FakeUpstream.set_mode(recovery_fake, {:path_json, Map.put(routes, @endpoint, success)})
      capture_log(fn -> execute_backend_stream!(setup, "synthetic-credit-sse-boundary") end)
      assert FakeUpstream.physical_counts(failed_fake).http_generation == 1
      assert FakeUpstream.physical_counts(recovery_fake).consume == unquote(if visible? or policy, do: 0, else: 1)
      assert FakeUpstream.physical_counts(recovery_fake).http_generation == if(unquote(visible?), do: 0, else: 1)
      attempts = Repo.all(from(attempt in Attempt, order_by: attempt.attempt_number))
      assert Enum.map(attempts, & &1.upstream_identity_id) == if(unquote(visible?), do: [first.identity.id], else: [first.identity.id, setup.identity.id])
      assert List.last(attempts).status == if(unquote(visible?), do: "failed", else: "succeeded")
    end
  end

  for policy <- [true, false], credits <- [:full, :none, :unknown] do
    @tag credits_negative: true
    test "C12 request-driven monthly bank follows usable credits with #{credits} credits policy #{policy}" do
      {fake, setup} = arrangement(:monthly_credit_only, unquote(policy), bank: 2, auto: true, credits: unquote(credits))
      identity = Repo.reload!(setup.identity)
      windows = Windows.list_evidence(identity)
      assert [%{window_kind: "primary", window_minutes: 43_200}] = Enum.map(windows, &Map.take(&1, [:window_kind, :window_minutes]))
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, windows: windows, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")
      restored = usage(:included, window: :monthly, credits: unquote(credits), reset_after: 14_400) |> Map.put("rate_limit_reset_credits", %{"available_count" => 1})
      FakeUpstream.set_mode(fake, routes(restored))
      assert FakeUpstream.physical_counts(fake).consume == 0
      capture_log(fn -> assert {:ok, %{status: 200}} = execute(%{setup | identity: Repo.reload!(identity)}) end)
      redemption = Repo.reload!(identity).metadata["saved_reset_redemption"]
      assert [generation] = Enum.filter(FakeUpstream.physical_receipts(fake), &(&1.kind == :generation))

      if unquote(policy) and unquote(credits) == :full do
        refute redemption
        assert FakeUpstream.physical_counts(fake).consume == 0
        assert Repo.one!(Attempt).response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
      else
        assert redemption["trigger_kind"] == "gateway_auto"
        assert redemption["trigger_detail"] == "exhausted"
        assert redemption["included_window_descriptors"] == [%{"window_kind" => "primary", "window_minutes" => 43_200}]
        assert redemption["phase"] == "confirmed_by_quota"
        assert [consume] = Enum.filter(FakeUpstream.physical_receipts(fake), &(&1.kind == :consume))
        assert confirmation = Enum.find(FakeUpstream.physical_receipts(fake), &(&1.kind == :usage and &1.ordinal > consume.ordinal))
        assert consume.ordinal < confirmation.ordinal and confirmation.ordinal < generation.ordinal
        assert Repo.one!(Attempt).response_metadata["provider_credits_admission"]["capacity_basis"] == "recovered_included"
      end

      assert Repo.one!(Attempt).upstream_identity_id == identity.id
      assert Repo.reload!(identity).allow_provider_credits == unquote(policy)
    end
  end

  for policy <- [true, false], state <- [:monthly_credit_only, :spend_blocked_exhausted, :malformed_spend] do
    test "C12 C18 scheduled recovery restores #{state} before a later request with policy #{policy}" do
      {fake, setup} = arrangement(unquote(state), unquote(policy), bank: 2, auto: true)
      now = DateTime.utc_now()
      reset = DateTime.add(now, 14_400, :second) |> DateTime.to_iso8601()
      bank = setup.identity.metadata["saved_resets"] |> Map.merge(%{"available_expires_at" => [reset], "available_expirations" => [%{"expires_at" => reset, "first_seen_at" => DateTime.to_iso8601(now)}], "next_expires_at" => reset, "expires_observed_at" => DateTime.to_iso8601(now), "expires_refresh_attempted_at" => DateTime.to_iso8601(now)})
      identity = Repo.update!(Ecto.Changeset.change(setup.identity, metadata: Map.put(setup.identity.metadata, "saved_resets", bank)))
      restored = usage(:included, window: if(unquote(state) == :monthly_credit_only, do: :monthly, else: :weekly), credits: :none) |> Map.put("rate_limit_reset_credits", %{"available_count" => 1})
      FakeUpstream.set_mode(fake, routes(restored))
      capture_log(fn -> assert {:ok, %{applied?: true}} = SavedResetRedemption.redeem_scheduled_expiry(setup.assignment, identity.id, started_at: now) end)
      assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(Repo.reload!(identity), setup.assignment)
      assert {:ok, _} = Convergence.converge(identity)
      assert Repo.reload!(identity).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_quota"
      assert {:ok, %{status: 200}} = execute(%{setup | identity: Repo.reload!(identity)})
      receipts = FakeUpstream.physical_receipts(fake)
      consume = Enum.find_index(receipts, &(&1.kind == :consume))
      generation = Enum.find_index(receipts, &(&1.kind == :generation))
      assert consume < generation
      assert Enum.count(receipts, &(&1.kind == :consume)) == 1
      assert Repo.one!(Attempt).upstream_identity_id == identity.id
    end
  end

  for policy <- [true, false], state <- [:spend_blocked_exhausted, :malformed_spend] do
    @tag credits_negative: true
    test "C18 exhausted #{state} without usable bank cannot generate with policy #{policy}" do
      {fake, setup} = arrangement(unquote(state), unquote(policy), bank: 0, auto: false)
      assert {:error, _} = execute(setup)
      assert FakeUpstream.physical_counts(fake).consume == 0
      assert FakeUpstream.physical_counts(fake).http_generation == 0
    end
  end

  for policy <- [true, false] do
    test "C32 incompatible included model cannot veto the actual cohort's authorized bank with policy #{policy}" do
      {fake, setup} = arrangement(:weekly_credit_only, unquote(policy), bank: 2, auto: true, credits: :none)
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(setup.identity, usage_url: FakeUpstream.url(fake) <> "/api/codex/usage")
      incompatible_fake = start_upstream(routes(usage(:included, credits: :none)))
      incompatible = gateway_upstream(setup.pool, incompatible_fake, "synthetic-incompatible-token", []) |> reconcile(incompatible_fake)
      # Included headroom on a source that explicitly cannot serve Responses
      # is outside this request's compatible recovery cohort.
      metadata = setup.model.metadata
      source = metadata["source_assignment_models"][setup.assignment.id]
      incompatible_source = source |> Map.put("slug", "synthetic-other-model") |> Map.put("capabilities", %{"responses" => false})
      metadata = metadata |> Map.put("source_assignment_ids", [setup.assignment.id, incompatible.assignment.id]) |> Map.put("source_assignment_models", %{setup.assignment.id => source, incompatible.assignment.id => incompatible_source})
      model = Repo.update!(Ecto.Changeset.change(setup.model, metadata: metadata))
      FakeUpstream.set_mode(fake, routes(usage(:included, credits: :none) |> Map.put("rate_limit_reset_credits", %{"available_count" => 1})))
      capture_log(fn -> assert {:ok, %{status: 200}} = execute(%{setup | model: model}) end)
      assert FakeUpstream.physical_counts(fake).consume == 1
      assert FakeUpstream.physical_counts(fake).http_generation == 1
      assert FakeUpstream.physical_counts(incompatible_fake).http_generation == 0
      assert Repo.one!(Attempt).upstream_identity_id == setup.identity.id
    end

    @tag credits_negative: true
    test "C34 Pool-A-only recovery bank never supplies Pool B pressure or generation with policy #{policy}" do
      {shared_fake, pool_a} = arrangement(:weekly_credit_only, unquote(policy), bank: 0, auto: false)
      bank_fake = start_upstream(routes(usage(:weekly_credit_only, credits: :full)))
      bank_account = gateway_upstream(pool_a.pool, bank_fake, "synthetic-pool-a-bank", []) |> reconcile(bank_fake)
      now = DateTime.utc_now()
      bank = %{"status" => "reported", "available_count" => 2, "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => DateTime.to_iso8601(now), "usage_path" => "/api/codex/usage", "reason" => nil}
      bank_identity = Repo.update!(Ecto.Changeset.change(bank_account.identity, saved_reset_auto_redeem_enabled: true, metadata: Map.put(bank_account.identity.metadata, "saved_resets", bank)))
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(bank_identity, usage_url: FakeUpstream.url(bank_fake) <> "/api/codex/usage")
      %{pool: pool_b, authorization: authorization_b} = CodexPooler.PoolerFixtures.active_api_key_fixture()
      assert {:ok, assignment_b} = PoolAssignments.create_pool_assignment(pool_b, pool_a.identity, %{assignment_label: "Synthetic shared", status: "active", health_status: "active", eligibility_status: "eligible", metadata: pool_a.assignment.metadata})
      source = pool_a.model.metadata["source_assignment_models"][pool_a.assignment.id]
      model_b = CodexPooler.PoolerFixtures.model_fixture(pool_b, %{exposed_model_id: pool_a.model.exposed_model_id, upstream_model_id: pool_a.model.upstream_model_id, supports_responses: true, supports_streaming: true, metadata: %{"source_assignment_ids" => [assignment_b.id], "source_assignment_models" => %{assignment_b.id => source}}})
      assert_credit_policy_generation!(%{pool_a | pool: pool_b, authorization: authorization_b, assignment: assignment_b, model: model_b}, shared_fake, unquote(policy))
      assert FakeUpstream.physical_counts(shared_fake).consume == 0
      assert FakeUpstream.physical_counts(bank_fake).consume == 0
      assert FakeUpstream.physical_counts(bank_fake).http_generation == 0
    end
  end

  for mode <- [:full, :lite] do
    @tag credits_negative: true
    test "included-compatible weekly sibling precedes provider-attested credits despite durable affinity #{mode}" do
      {credit_fake, setup} = luna_arrangement(bank: 2, auto: true)
      included_fake = start_upstream(luna_routes(usage(:included, window: :weekly, credits: :none) |> put_in(["rate_limit", "secondary_window", "used_percent"], 99)))
      sibling = gateway_upstream(setup.pool, included_fake, "synthetic-luna-included", []) |> reconcile(included_fake)
      assert [window] = Windows.list_evidence(sibling.identity)
      assert Decimal.equal?(window.used_percent, Decimal.new("99"))
      setup = add_source(setup, sibling.assignment)
      request_id = Ecto.UUID.generate()
      assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      digest = :crypto.hash(:sha256, Enum.join([setup.pool.id, auth.api_key.id, setup.model.exposed_model_id, "request_correlation", request_id], ":"))
      affinity = Repo.insert!(%BridgeAffinity{pool_id: setup.pool.id, api_key_id: auth.api_key.id, model_identifier: setup.model.exposed_model_id, affinity_kind: "request_correlation", affinity_key_hash: digest, pool_upstream_assignment_id: setup.assignment.id, upstream_identity_id: setup.identity.id, status: "active", last_hit_at: now, created_at: now, updated_at: now})
      assert affinity.pool_upstream_assignment_id == setup.assignment.id

      finish_luna_stream!(setup, unquote(mode), request_id: request_id)

      assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(included_fake)
      assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(credit_fake)
      attempt = Repo.one!(Attempt)
      assert attempt.upstream_identity_id == sibling.identity.id
      assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] in ["included_window", "ordinary_provider_permission"]
      assert attempt.response_metadata["routing"]["model_serving_mode"] == Atom.to_string(unquote(mode))
      assert Repo.reload!(setup.identity).metadata["saved_resets"]["available_count"] == 2
      refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
    end

    @tag credits_negative: true
    test "usable credits preserve authorized independent bank #{mode}" do
      {credit_fake, setup} = luna_arrangement(bank: 0, auto: false)
      bank_fake = start_upstream(luna_routes(usage(:weekly_credit_only, credits: :none)))
      bank = gateway_upstream(setup.pool, bank_fake, "synthetic-luna-bank", []) |> reconcile(bank_fake) |> put_bank(2, true)
      SavedResetConfirmationFixtures.confirm_automatic_pressure!(bank.identity, usage_url: FakeUpstream.url(bank_fake) <> "/backend-api/wham/usage")
      setup = add_source(setup, bank.assignment)
      restored = usage(:included, window: :weekly, credits: :full) |> Map.put("rate_limit_reset_credits", %{"available_count" => 1})
      FakeUpstream.set_mode(bank_fake, luna_routes(restored))

      capture_log(fn -> finish_luna_stream!(setup, unquote(mode)) end)

      assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(credit_fake)
      assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(bank_fake)
      attempt = Repo.one!(Attempt)
      assert attempt.upstream_identity_id == setup.identity.id
      assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
      assert Repo.reload!(bank.identity).metadata["saved_resets"]["available_count"] == 2
      refute Repo.reload!(bank.identity).metadata["saved_reset_redemption"]
      refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
    end

    for fence <- [:auto_off, :empty] do
      @tag credits_negative: true
      test "provider-attested credits remain usable when independent bank is #{fence} #{mode}" do
        {credit_fake, setup} = luna_arrangement(bank: 0, auto: false)
        bank_fake = start_upstream(luna_routes(usage(:weekly_credit_only, credits: :none)))
        bank = gateway_upstream(setup.pool, bank_fake, "synthetic-luna-unavailable-bank", []) |> reconcile(bank_fake) |> put_bank(2, true)
        SavedResetConfirmationFixtures.confirm_automatic_pressure!(bank.identity, usage_url: FakeUpstream.url(bank_fake) <> "/backend-api/wham/usage")
        bank = apply_fence(bank, unquote(fence))
        setup = add_source(setup, bank.assignment)
        bank_before = Repo.reload!(bank.identity).metadata["saved_resets"]

        capture_log(fn -> finish_luna_stream!(setup, unquote(mode)) end)

        assert %{http_generation: 1, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(credit_fake)
        assert %{http_generation: 0, websocket_generation: 0, consume: 0} = FakeUpstream.physical_counts(bank_fake)
        attempt = Repo.one!(Attempt)
        assert attempt.upstream_identity_id == setup.identity.id
        assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
        refute attempt.response_metadata["provider_credits_admission"]["non_credit_guarded_probe"]
        assert Repo.reload!(bank.identity).metadata["saved_resets"] == bank_before
        refute Repo.reload!(bank.identity).metadata["saved_reset_redemption"]
        refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
      end
    end
  end

  for capacity_owner <- [:target, :sibling], stage <- [:claim, :reservation] do
    test "locked blocked #{stage} observes newly enabled #{capacity_owner} credits and preserves bank" do
      {credit_fake, credit} = luna_arrangement(bank: 2, auto: true)
      disabled_identity = Repo.update!(Ecto.Changeset.change(credit.identity, allow_provider_credits: false))
      credit = %{credit | identity: disabled_identity}

      {setup, bank_fake, bank, candidates} =
        if unquote(capacity_owner) == :target do
          {credit, credit_fake, credit, [{credit.assignment, credit.identity}]}
        else
          bank_fake = start_upstream(luna_routes(usage(:weekly_credit_only, credits: :none)))
          bank = gateway_upstream(credit.pool, bank_fake, "synthetic-locked-bank", []) |> reconcile(bank_fake) |> put_bank(2, true)
          {add_source(credit, bank.assignment), bank_fake, bank, [{bank.assignment, bank.identity}, {credit.assignment, credit.identity}]}
        end

      SavedResetConfirmationFixtures.confirm_automatic_pressure!(bank.identity, usage_url: FakeUpstream.url(bank_fake) <> "/backend-api/wham/usage")
      context = blocked_context(setup, bank, candidates)

      if unquote(stage) == :claim do
        Repo.update!(Ecto.Changeset.change(Repo.reload!(credit.identity), allow_provider_credits: true))
      else
        enable_credits_after_claim!(credit.identity, bank.identity)
      end

      original_bank = Repo.reload!(bank.identity).metadata["saved_resets"]

      capture_log(fn ->
        assert {:ok, %{applied?: false, code: "gateway_auto_sibling_usable_capacity"}} =
                 SavedResetRedemption.redeem(bank.assignment, trigger_kind: "gateway_auto", gateway_auto_context: context)
      end)

      assert FakeUpstream.physical_counts(bank_fake).consume == 0
      assert FakeUpstream.physical_counts(credit_fake).http_generation == 0
      assert Repo.reload!(bank.identity).metadata["saved_resets"] == original_bank

      if unquote(stage) == :claim do
        refute Repo.reload!(bank.identity).metadata["saved_reset_redemption"]
      else
        assert_receive :credits_enabled_after_claim
        assert Repo.reload!(bank.identity).metadata["saved_reset_redemption"]["result"]["code"] == "consume_not_applied"
      end
    end
  end

  test "blocked reset claim observes credits enabled by an independent replica" do
    fixture = ProviderCreditsFixtures.open!(mode: luna_routes(usage(:weekly_credit_only, credits: :fractional)))
    setup = ProviderCreditsFixtures.runtime_setup!(fixture, :a, :weekly_credit_only, model: "gpt-6-luna", upstream_model: "gpt-6-luna")

    {setup, context} =
      UnboxedFixture.run_unboxed(fn ->
        setup = setup |> reconcile(fixture.upstream) |> put_bank(2, true)
        SavedResetConfirmationFixtures.confirm_automatic_pressure!(setup.identity, usage_url: FakeUpstream.url(fixture.upstream) <> "/backend-api/wham/usage")
        {setup, blocked_context(setup, setup, [{setup.assignment, setup.identity}])}
      end)

    ProviderCreditsFixtures.commit_policy!(fixture, :weekly_credit_only, true, fixture.peer.node)

    capture_log(fn ->
      assert {:ok, %{applied?: false, code: "gateway_auto_sibling_usable_capacity"}} =
               UnboxedFixture.run_unboxed(fn ->
                 SavedResetRedemption.redeem(setup.assignment, trigger_kind: "gateway_auto", gateway_auto_context: context)
               end)
    end)

    assert FakeUpstream.physical_counts(fixture.upstream).consume == 0

    UnboxedFixture.run_unboxed(fn ->
      assert Repo.reload!(setup.identity).metadata["saved_resets"]["available_count"] == 2
      refute Repo.reload!(setup.identity).metadata["saved_reset_redemption"]
    end)

    ProviderCreditsFixtures.close!(fixture)
  end

  defp enable_credits_after_claim!(credit_identity, bank_identity) do
    owner = self()
    handler = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler) end)

    :telemetry.attach(
      handler,
      [:codex_pooler, :repo, :query],
      fn _event, _measurements, metadata, _config ->
        if self() == owner and metadata[:source] == "encrypted_secrets" and not Process.get(handler, false) do
          Process.put(handler, true)
          enable_claimed_credits!(credit_identity, bank_identity, owner, handler)
        end
      end,
      nil
    )
  end

  defp enable_claimed_credits!(credit_identity, bank_identity, owner, handler) do
    if Repo.reload!(bank_identity).metadata["saved_reset_redemption"]["phase"] == "consuming" do
      Repo.update!(Ecto.Changeset.change(Repo.reload!(credit_identity), allow_provider_credits: true))
      send(owner, :credits_enabled_after_claim)
    else
      Process.delete(handler)
    end
  end

  defp blocked_context(setup, bank, candidates) do
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    body = %{"model" => setup.model.exposed_model_id, "input" => [], "stream" => true}
    options = RequestOptions.build(%{transport: "http_sse", model_serving_mode: "full", model_serving_mode_configured: "full", model_serving_mode_source: "override"}, @endpoint, body)
    input = FilterInput.new(%{auth: auth, model: setup.model, endpoint: @endpoint, payload: body, request_options: options, candidates: candidates})
    state = RouteState.new(%{visible_model: setup.model, candidates: candidates}) |> RouteState.preload_routing_snapshots(auth, setup.model, options)
    SavedResetAutoRedeem.gateway_auto_context(%{filter_input: input, route_state: state}, bank.assignment, Repo.reload!(bank.identity), :blocked_weekly_exhaustion)
  end

  defp luna_arrangement(opts) do
    fake = start_upstream(luna_routes(usage(:weekly_credit_only, credits: :fractional)))
    setup = gateway_setup(fake, quota?: false, exposed_model_id: "gpt-6-luna", upstream_model_id: "gpt-6-luna") |> reconcile(fake) |> put_bank(opts[:bank], opts[:auto])
    identity = Repo.update!(Ecto.Changeset.change(setup.identity, allow_provider_credits: true))
    {fake, %{setup | identity: identity}}
  end

  defp put_bank(setup, count, auto) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    bank = %{"status" => "reported", "available_count" => count, "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => DateTime.to_iso8601(now), "usage_path" => "/api/codex/usage", "reason" => nil}
    update_identity(setup, metadata: Map.put(Repo.reload!(setup.identity).metadata, "saved_resets", bank), saved_reset_auto_redeem_enabled: auto, saved_reset_auto_redeem_min_blocked_minutes: 60, saved_reset_auto_redeem_keep_credits: 0)
  end

  defp luna_routes(payload) do
    {:path_json, routes} = routes(payload)
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_luna_priority", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}}
    {:path_json, routes |> Map.put("/api/codex/usage", {404, %{}}) |> Map.put("/backend-api/codex/usage", {404, %{}}) |> Map.put(@endpoint, FakeUpstream.sse_stream([completed], done: false, headers: [{"x-synthetic-luna-priority", "true"}]))}
  end

  defp finish_luna_stream!(setup, mode, opts \\ []) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: Atom.to_string(mode), created_at: now, updated_at: now})
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)
    payload = %{"model" => setup.model.exposed_model_id, "input" => [], "stream" => true}
    options = RequestOptions.build(%{api_key_policy: policy, request_id: opts[:request_id], transport: "http_sse", upstream_endpoint: @endpoint, model_serving_mode: Atom.to_string(mode), model_serving_mode_configured: Atom.to_string(mode), model_serving_mode_source: "override"}, @endpoint, payload)
    assert {:ok, %{stream: stream}} = Service.execute(auth, @endpoint, payload, options)
    conn = Phoenix.ConnTest.build_conn() |> Plug.Conn.put_resp_content_type("text/event-stream") |> Plug.Conn.send_chunked(200)
    assert {:ok, conn} = stream.(conn)
    assert conn.resp_body =~ "response.completed"
  end

  defp assert_credit_policy_generation!(setup, fake, enabled?) do
    if enabled? do
      assert {:ok, %{status: 200}} = execute(setup)
      attempt = Repo.one!(from attempt in Attempt, where: attempt.upstream_identity_id == ^setup.identity.id)
      assert attempt.response_metadata["provider_credits_admission"]["capacity_basis"] == "provider_credits"
      assert attempt.pool_upstream_assignment_id == setup.assignment.id
    else
      assert {:error, _} = execute(setup)
    end

    assert FakeUpstream.physical_counts(fake).http_generation == if(enabled?, do: 1, else: 0)
  end

  defp arrangement(state, policy, opts) do
    fake = start_upstream(routes(usage(state, credits: Keyword.get(opts, :credits, :full))))
    setup = gateway_setup(fake, quota?: false, exposed_model_id: "synthetic-credit-#{System.unique_integer([:positive])}", upstream_model_id: "synthetic-credit-model")
    setup = reconcile(setup, fake)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    metadata = Map.put(setup.identity.metadata, "saved_resets", %{"status" => "reported", "available_count" => opts[:bank], "source" => "codex_usage_api", "path_style" => "codex_api", "observed_at" => DateTime.to_iso8601(now), "usage_path" => "/api/codex/usage", "reason" => nil})
    identity = setup.identity |> Ecto.Changeset.change(metadata: metadata, allow_provider_credits: policy, saved_reset_auto_redeem_enabled: opts[:auto], saved_reset_auto_redeem_min_blocked_minutes: 60, saved_reset_auto_redeem_keep_credits: 0) |> Repo.update!()
    {fake, %{setup | identity: identity}}
  end

  defp reconcile(setup, fake) do
    identity = setup.identity |> Ecto.Changeset.change(metadata: Map.put(setup.identity.metadata, "usage_base_url", FakeUpstream.url(fake))) |> Repo.update!()
    assert {:ok, identity} = PoolReconciliation.refresh_quota_from_usage(identity, setup.assignment)
    %{setup | identity: identity}
  end

  defp execute(setup) do
    assert {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    assert {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)
    payload = %{"model" => setup.model.exposed_model_id, "input" => []}
    options = RequestOptions.build(%{api_key_policy: policy}, @endpoint, payload)
    Service.execute(auth, @endpoint, payload, options)
  end

  defp usage(:spend_blocked_exhausted, opts), do: ProviderCreditsFixtures.usage_payload(:weekly_credit_only, opts) |> Map.put("spend_control", %{"reached" => true})
  defp usage(state, opts), do: ProviderCreditsFixtures.usage_payload(state, opts)

  defp routes(payload) do
    {:path_json, Map.merge(ProviderCreditsFixtures.usage_routes(payload), %{@consume => {200, %{"code" => "reset"}}, @endpoint => {200, %{"id" => "resp_synthetic_credit", "object" => "response", "output" => []}}})}
  end

  defp add_source(setup, assignment) do
    metadata = setup.model.metadata
    [original] = metadata["source_assignment_ids"]
    source = metadata["source_assignment_models"][original]
    metadata = metadata |> Map.put("source_assignment_ids", [original, assignment.id]) |> put_in(["source_assignment_models", assignment.id], source)
    %{setup | model: setup.model |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()}
  end

  defp apply_fence(setup, :auto_off), do: update_identity(setup, saved_reset_auto_redeem_enabled: false)
  defp apply_fence(setup, :reserved), do: update_identity(setup, saved_reset_auto_redeem_keep_credits: 2)
  defp apply_fence(setup, :timing), do: update_identity(setup, saved_reset_auto_redeem_min_blocked_minutes: 10_080)
  defp apply_fence(setup, :confirmation), do: %{setup | identity: Repo.reload!(setup.identity)}

  defp apply_fence(setup, :empty) do
    identity = Repo.reload!(setup.identity)
    update_identity(setup, metadata: put_in(identity.metadata, ["saved_resets", "available_count"], 0))
  end

  defp apply_fence(setup, :cooldown) do
    identity = Repo.reload!(setup.identity)
    redemption = %{"phase" => "confirmed_by_quota", "status" => "succeeded", "consumed_at" => DateTime.to_iso8601(DateTime.utc_now()), "generation" => 1, "result" => %{"applied" => true}}
    update_identity(setup, metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption))
  end

  defp update_identity(setup, attrs), do: %{setup | identity: setup.identity |> Repo.reload!() |> Ecto.Changeset.change(attrs) |> Repo.update!()}
end
