defmodule CodexPoolerWeb.Runtime.SavedResetRetryRefilterTest do
  # One request spends at most one banked reset (findings#331).
  #
  # Topology: one Pool, one canonical partition of two accounts, R1 created
  # first, each with two banked resets and automatic redemption on; one BEAM
  # node; one FakeUpstream per account serving its Usage API, the consume and
  # the Responses route; native HTTP SSE in Full and Lite, and the native
  # websocket in Full with owner forwarding off and on. Nothing reaches a real
  # provider.
  #
  # Threshold shape: R1's weekly window at 99%, R2's spent, both in threshold
  # mode with Usage-API-confirmed pressure. Route filtering redeems R1's reset
  # (R2 has no usable capacity), the reset stays pending confirmation, and no
  # guarded probe is permitted (no fresh credit authority). Filtering used to
  # re-admit R1 on its reading from before the redemption; the final
  # provider-credits admission then refused the send (`saved_reset_probe_pending`,
  # nothing sent), and the retry over the rest of the cohort narrowed the
  # redemption cohort to R2, where the sibling consume barrier no longer saw
  # R1's consume, and redeemed R2's reset too: two banked resets for a request
  # that sent nothing. Now the refusal is judged on quota reread after the
  # redemption, so the request is refused before dispatch and never retries.
  #
  # Confirmed shape: R1's reset is confirmed by its first post-consume Usage
  # API reading (a new weekly cycle), so filtering admits R1 and the request is
  # sent to it. The provider still refuses R1 with a usage limit before any
  # output, and the request retries over the rest of its cohort, where R2's
  # spent window and banked reset would open the blocked scan. The retry finds
  # the recovery recorded on the request and runs no scan; before, it redeemed
  # R2's reset as well.
  #
  # Blocked shape: both weekly windows spent, blocked mode. The blocked scan
  # redeems R1, which still reads exhausted while its reset is pending, so the
  # request is refused before any attempt and no retry runs.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  @moduletag capture_log: true

  @consume_path "/api/codex/rate-limit-reset-credits/consume"
  @usage_paths ["/api/codex/usage", "/backend-api/codex/usage", "/wham/usage", "/backend-api/wham/usage"]
  @turn_endpoint "/backend-api/codex/responses"

  for mode <- ["full", "lite"] do
    test "native HTTP #{mode}, threshold shape: refused before dispatch after one reset" do
      pool = two_account_pool!(unquote(mode), {"threshold", "99"}, {"threshold", "100"})

      response = post_native(pool, unquote(mode))
      [request] = rows!(pool)

      assert_one_reset!(pool)
      assert response.status == 503
      assert {request.status, request.last_error_code} == {"rejected", "quota_exhausted"}
      assert attempts!(request, pool) == []
    end
  end

  for forwarding <- [false, true] do
    test "native websocket full forwarding=#{forwarding}, threshold shape: refused before dispatch after one reset" do
      put_owner_forwarding!(unquote(forwarding))
      pool = two_account_pool!("full", {"threshold", "99"}, {"threshold", "100"})
      {_server, port} = start_public_endpoint_with_server!()

      terminal = native_websocket_turn!(port, pool)
      [request] = await_settled_rows!(pool)

      assert_one_reset!(pool)
      assert %{"type" => "error"} = terminal
      assert {request.status, request.last_error_code} == {"rejected", "quota_exhausted"}
      assert attempts!(request, pool) == []
    end
  end

  for mode <- ["full", "lite"] do
    test "native HTTP #{mode}, confirmed shape: the retry after the provider's refusal redeems no second reset" do
      pool = confirmed_pool!(unquote(mode), :http)

      response = post_native(pool, unquote(mode))
      [request] = rows!(pool)

      assert_confirmed_reset!(pool)
      assert response.status == 503
      assert {request.status, request.last_error_code} == {"failed", "quota_exhausted"}
      assert attempts!(request, pool) == [{"retryable_failed", 429, "retryable_upstream_status", :r1}]
      assert get_in(request.request_metadata, ["quota_decision", "non_credit_recovery_outcome"]) == "confirmed"
    end
  end

  for forwarding <- [false, true] do
    test "native websocket full forwarding=#{forwarding}, confirmed shape: the retry after the provider's refusal redeems no second reset" do
      put_owner_forwarding!(unquote(forwarding))
      pool = confirmed_pool!("full", :websocket)
      {_server, port} = start_public_endpoint_with_server!()

      terminal = native_websocket_turn!(port, pool)
      [request] = await_settled_rows!(pool)

      assert_confirmed_reset!(pool)
      assert %{"type" => "error"} = terminal
      assert {request.status, request.last_error_code} == {"failed", "quota_exhausted"}
      assert attempts!(request, pool) == [{"retryable_failed", 200, "usage_limit_reached", :r1}]
    end
  end

  for mode <- ["full", "lite"] do
    test "native HTTP #{mode}, blocked shape: refused before any attempt after one reset" do
      pool = two_account_pool!(unquote(mode), {"blocked", "100"}, {"blocked", "100"})

      response = post_native(pool, unquote(mode))
      [request] = rows!(pool)

      assert_one_reset!(pool)
      assert response.status == 503
      assert {request.status, request.last_error_code} == {"rejected", "quota_exhausted"}
      assert attempts!(request, pool) == []
    end
  end

  # R1 redeemed once and R2 never; neither account received a generation.
  defp assert_one_reset!(pool) do
    assert {consumes(pool.r1.upstream), consumes(pool.r2.upstream)} == {1, 0}
    assert redemption_phase(pool.r1.identity) == "consumed_pending_probe"
    assert redemption_phase(pool.r2.identity) == nil
    assert {provider_sends(pool.r1.upstream), provider_sends(pool.r2.upstream)} == {0, 0}
  end

  # R1 redeemed once and confirmed, R2 never; R1 received the one generation the
  # provider refused, R2 none.
  defp assert_confirmed_reset!(pool) do
    assert {consumes(pool.r1.bank), consumes(pool.r2.bank)} == {1, 0}
    assert redemption_phase(pool.r1.identity) == "confirmed_by_quota"
    assert redemption_phase(pool.r2.identity) == nil
    assert {provider_sends(pool.r1.seat), provider_sends(pool.r2.seat)} == {1, 0}
  end

  defp confirmed_pool!(mode, transport) do
    r1_seat = start_upstream(usage_limit_refusal(transport))
    r2_seat = start_upstream(usage_limit_refusal(transport))
    r1_bank = start_upstream(bank_routes(confirming_usage()))
    r2_bank = start_upstream(bank_routes(saved_reset_usage_payload(1)))
    setup = gateway_setup(r1_seat, quota?: false)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    r2 = gateway_upstream(setup.pool, r2_seat, "upstream-token-r2", compact?: false)
    model = put_model_source_assignments!(setup.model, [setup.assignment, r2.assignment])

    %{
      setup: %{setup | model: model},
      r1: %{assignment: setup.assignment, identity: threshold_pressure!(setup.identity, r1_bank), seat: r1_seat, bank: r1_bank},
      r2: %{assignment: r2.assignment, identity: bank!(r2.identity, r2_bank, "threshold", "100"), seat: r2_seat, bank: r2_bank}
    }
  end

  # The weekly window at 97% of its cycle with Usage-API-confirmed pressure,
  # read with the account's credit facts (none).
  defp threshold_pressure!(identity, bank) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    identity =
      identity
      |> UpstreamIdentity.changeset(%{
        metadata: Map.merge(identity.metadata || %{}, saved_reset_metadata(bank, 2)),
        saved_reset_auto_redeem_enabled: true,
        saved_reset_auto_redeem_min_blocked_minutes: 60,
        saved_reset_auto_redeem_keep_credits: 0,
        saved_reset_auto_redeem_trigger_mode: "threshold",
        updated_at: now
      })
      |> Repo.update!()

    payload = :included |> ProviderCreditsFixtures.usage_payload(now: now, credits: :none) |> put_in(["rate_limit", "secondary_window", "used_percent"], 97)
    identity = ProviderCreditsFixtures.persist_usage!(identity, payload, now)
    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity, usage_url: FakeUpstream.url(bank) <> "/api/codex/usage")
    Repo.reload!(identity)
  end

  # The first post-consume reading: a new weekly cycle at 98%, which confirms
  # the reset.
  defp confirming_usage do
    :included
    |> ProviderCreditsFixtures.usage_payload(credits: :none, reset_after: 14_400)
    |> put_in(["rate_limit", "secondary_window", "used_percent"], 98)
    |> Map.put("rate_limit_reset_credits", %{"available_count" => 1})
  end

  defp bank_routes(usage), do: {:path_json, @usage_paths |> Map.new(&{&1, {200, usage}}) |> Map.put(@consume_path, {200, %{"code" => "reset"}})}

  # The provider's usage-limit refusal of the generation, before any output.
  defp usage_limit_refusal(transport) do
    resets_at = DateTime.to_unix(DateTime.utc_now()) + 1_800
    error = %{"type" => "usage_limit_reached", "message" => "synthetic provider text", "resets_at" => resets_at}
    headers = %{"x-codex-secondary-used-percent" => "100", "x-codex-secondary-window-minutes" => "10080", "x-codex-secondary-reset-at" => Integer.to_string(resets_at)}

    case transport do
      :http -> {:json_headers, 429, %{"error" => error}, Map.to_list(headers)}
      :websocket -> FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "error", "status" => 429, "error" => error, "headers" => headers})])
    end
  end

  defp two_account_pool!(mode, {r1_trigger, r1_weekly}, {r2_trigger, r2_weekly}) do
    r1_upstream = start_upstream(seat_routes())
    r2_upstream = start_upstream(seat_routes())
    setup = gateway_setup(r1_upstream, quota?: false)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    r2 = gateway_upstream(setup.pool, r2_upstream, "upstream-token-r2", compact?: false)
    model = put_model_source_assignments!(setup.model, [setup.assignment, r2.assignment])

    %{
      setup: %{setup | model: model},
      r1: %{assignment: setup.assignment, identity: bank!(setup.identity, r1_upstream, r1_trigger, r1_weekly), upstream: r1_upstream},
      r2: %{assignment: r2.assignment, identity: bank!(r2.identity, r2_upstream, r2_trigger, r2_weekly), upstream: r2_upstream}
    }
  end

  # Two banked resets, automatic redemption in the given trigger mode, and the
  # weekly window at the given percentage with Usage-API-confirmed pressure.
  defp bank!(identity, upstream, trigger, weekly_percent) do
    identity =
      identity
      |> UpstreamIdentity.changeset(%{
        metadata: Map.merge(identity.metadata || %{}, saved_reset_metadata(upstream, 2)),
        saved_reset_auto_redeem_enabled: true,
        saved_reset_auto_redeem_min_blocked_minutes: 60,
        saved_reset_auto_redeem_keep_credits: 0,
        saved_reset_auto_redeem_trigger_mode: trigger,
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })
      |> Repo.update!()

    assert {:ok, [_window]} = QuotaWindows.upsert_quota_windows(identity, [weekly_window(weekly_percent)])
    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity)
    Repo.reload!(identity)
  end

  defp weekly_window(used_percent) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %{
      quota_key: "account",
      window_kind: "secondary",
      window_minutes: 10_080,
      used_percent: Decimal.new(used_percent),
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

  # The account's Usage API (a 5-hour window and one banked reset left after a
  # consume), the consume, and a Responses route that fails if it is reached.
  defp seat_routes do
    usage = Map.new(@usage_paths, &{&1, {200, saved_reset_usage_payload(1)}})
    failure = {502, %{"error" => %{"message" => "synthetic upstream failure", "type" => "server_error"}}}
    {:path_json, usage |> Map.put(@consume_path, {200, %{"code" => "reset"}}) |> Map.put(@turn_endpoint, failure)}
  end

  defp post_native(pool, mode) do
    build_conn()
    |> put_req_header("authorization", pool.setup.authorization)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("session-id", "saved-reset-retry-#{System.unique_integer([:positive])}")
    |> then(&if mode == "lite", do: put_req_header(&1, "x-openai-internal-codex-responses-lite", "true"), else: &1)
    |> post(@turn_endpoint, CodexPooler.JSON.encode!(%{"model" => pool.setup.model.exposed_model_id, "input" => native_text_input("synthetic retry prompt"), "stream" => true}))
  end

  defp native_websocket_turn!(port, pool) do
    thread_id = Ecto.UUID.generate()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", pool.setup.authorization}, {"session-id", thread_id}, {"thread-id", thread_id}, {"x-client-request-id", thread_id}, {"x-codex-window-id", "#{thread_id}:0"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @turn_endpoint, headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)

    frame =
      CodexPooler.JSON.encode!(%{
        "type" => "response.create",
        "model" => pool.setup.model.exposed_model_id,
        "instructions" => "synthetic instructions",
        "input" => native_text_input("synthetic retry prompt"),
        "tools" => [],
        "tool_choice" => "auto",
        "parallel_tool_calls" => true,
        "store" => false,
        "stream" => true,
        "client_metadata" => %{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => "#{thread_id}-turn"}
      })

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
      receive_terminal!(conn, websocket, ref)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => type} = terminal when type in ["response.completed", "response.failed", "error"] -> terminal
      _progress -> receive_terminal!(conn, websocket, ref)
    end
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  defp rows!(pool), do: Repo.all(from(r in Request, where: r.pool_id == ^pool.setup.pool.id, order_by: [asc: r.admitted_at]))

  # The socket answers before its request settles. The deadline only bounds
  # failure detection; a green run returns as soon as the row settles.
  defp await_settled_rows!(pool, deadline \\ System.monotonic_time(:millisecond) + 15_000) do
    rows = rows!(pool)

    cond do
      rows != [] and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"])) ->
        rows

      System.monotonic_time(:millisecond) > deadline ->
        flunk("the request never settled: #{inspect(Enum.map(rows, & &1.status))}")

      true ->
        Process.sleep(20)
        await_settled_rows!(pool, deadline)
    end
  end

  defp attempts!(request, pool) do
    from(a in Attempt, where: a.request_id == ^request.id, order_by: [asc: a.attempt_number])
    |> Repo.all()
    |> Enum.map(&{&1.status, &1.upstream_status_code, &1.network_error_code, if(&1.pool_upstream_assignment_id == pool.r1.assignment.id, do: :r1, else: :r2)})
  end

  defp redemption_phase(identity), do: get_in(Repo.reload!(identity).metadata, ["saved_reset_redemption", "phase"])
  defp consumes(upstream), do: FakeUpstream.physical_counts(upstream).consume

  # Generation requests over HTTP and generation frames over a websocket; the
  # websocket arm may open the upstream connection before the final admission
  # refuses the frame, which sends nothing.
  defp provider_sends(upstream) do
    counts = FakeUpstream.physical_counts(upstream)
    counts.http_generation + counts.websocket_generation
  end
end
