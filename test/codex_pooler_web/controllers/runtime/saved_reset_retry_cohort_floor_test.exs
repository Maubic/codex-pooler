defmodule CodexPoolerWeb.Runtime.SavedResetRetryCohortFloorTest do
  # After any applied redemption, automatic recovery waits thirty minutes, and
  # for definitive evidence, before another automatic consume in the accounts
  # considered together for a request (findings#331). The redemption claim
  # enforces it over its locked cohort (`gateway_auto_sibling_consume_barrier`),
  # so the cohort a request's retry filters with must still hold every account
  # the request considered, not only the candidates it has left.
  #
  # Topology: one Pool, one canonical partition of three accounts, one BEAM
  # node, one FakeUpstream per account; native HTTP SSE in Full and Lite, and
  # the native websocket in Full with owner forwarding off and on. R1 carries
  # an applied automatic consume an earlier request made five minutes ago, its
  # reset still being verified or already confirmed; its weekly window reads
  # spent. R2 has included quota and its provider refuses the generation with
  # a usage limit before any output. R3's weekly window is spent with
  # Usage-API-confirmed pressure and it holds two banked resets, blocked mode.
  #
  # The request is admitted on R2 without a saved-reset scan, R2 is refused,
  # and the request retries over R3. The retry used to narrow the redemption
  # cohort to R3, where the barrier no longer saw R1's consume, and it redeemed
  # R3's reset inside R1's protection period. A request whose first filtering
  # already met R3 spent (control) never did: its cohort holds R1.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.SavedResetConfirmationFixtures
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows
  alias CodexPooler.Upstreams.SavedResets.RedemptionLifecycle
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  @moduletag capture_log: true

  @consume_path "/api/codex/rate-limit-reset-credits/consume"
  @usage_paths ["/api/codex/usage", "/backend-api/codex/usage", "/wham/usage", "/backend-api/wham/usage"]
  @turn_endpoint "/backend-api/codex/responses"

  for phase <- ["consumed_pending_probe", "confirmed_by_quota"] do
    for mode <- ["full", "lite"] do
      test "native HTTP #{mode}, earlier #{phase} consume: the retry after R2's refusal redeems nothing" do
        pool = three_account_pool!(unquote(mode), :http, unquote(phase))

        response = post_native(pool, unquote(mode))
        [request] = rows!(pool)

        assert_floor_held!(pool)
        assert response.status == 429
        assert {request.status, request.last_error_code} == {"failed", "quota_exhausted"}
        assert attempts!(request, pool) == [{"retryable_failed", 429, "retryable_upstream_status", :r2}]
      end
    end

    for forwarding <- [false, true] do
      test "native websocket full forwarding=#{forwarding}, earlier #{phase} consume: the retry after R2's refusal redeems nothing" do
        put_owner_forwarding!(unquote(forwarding))
        pool = three_account_pool!("full", :websocket, unquote(phase))
        {_server, port} = start_public_endpoint_with_server!()

        terminal = native_websocket_turn!(port, pool)
        [request] = await_settled_rows!(pool)

        assert_floor_held!(pool)
        assert %{"type" => "error"} = terminal
        assert {request.status, request.last_error_code} == {"failed", "quota_exhausted"}
        assert attempts!(request, pool) == [{"retryable_failed", 200, "usage_limit_reached", :r2}]
      end
    end
  end

  test "control: a first filtering that meets R3 spent redeems nothing inside R1's protection period" do
    pool = three_account_pool!("full", :http, "consumed_pending_probe", r2_quota: :spent)

    response = post_native(pool, "full")
    [request] = rows!(pool)

    assert_floor_held!(pool)
    assert {response.status, request.status, attempts!(request, pool)} == {503, "rejected", []}
  end

  # Neither banked account redeemed: R1 keeps the earlier request's consume,
  # R3 keeps its bank. R2 received the one generation it refused.
  defp assert_floor_held!(pool) do
    assert {consumes(pool.r1.seat), consumes(pool.r3.seat)} == {0, 0}
    assert redemption_phase(pool.r3.identity) == nil
    assert redemption_phase(pool.r1.identity) == pool.r1.phase
  end

  defp three_account_pool!(mode, transport, r1_phase, opts \\ []) do
    r1_seat = start_upstream(bank_routes())
    r2_seat = start_upstream(usage_limit_refusal(transport))
    r3_seat = start_upstream(bank_routes())
    setup = gateway_setup(r1_seat, quota?: false)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    r2 = gateway_upstream(setup.pool, r2_seat, "upstream-token-r2", compact?: false)
    r3 = gateway_upstream(setup.pool, r3_seat, "upstream-token-r3", compact?: false)
    model = put_model_source_assignments!(setup.model, [setup.assignment, r2.assignment, r3.assignment])

    if Keyword.get(opts, :r2_quota, :routable) == :routable,
      do: prime_routing_quota!(r2.identity),
      else: prime_exhausted_routing_quota!(r2.identity)

    r1_identity = setup.identity |> banked!(r1_seat) |> put_earlier_consume!(r1_phase)

    %{
      setup: %{setup | model: model},
      r1: %{assignment: setup.assignment, identity: r1_identity, seat: r1_seat, phase: r1_phase},
      r2: %{assignment: r2.assignment, identity: r2.identity, seat: r2_seat},
      r3: %{assignment: r3.assignment, identity: banked!(r3.identity, r3_seat), seat: r3_seat}
    }
  end

  # Two banked resets, blocked-mode automatic redemption, and the weekly window
  # spent with Usage-API-confirmed pressure.
  defp banked!(identity, upstream) do
    identity =
      identity
      |> UpstreamIdentity.changeset(%{
        metadata: Map.merge(identity.metadata || %{}, saved_reset_metadata(upstream, 2)),
        saved_reset_auto_redeem_enabled: true,
        saved_reset_auto_redeem_min_blocked_minutes: 60,
        saved_reset_auto_redeem_keep_credits: 0,
        saved_reset_auto_redeem_trigger_mode: "blocked",
        updated_at: DateTime.utc_now() |> DateTime.truncate(:microsecond)
      })
      |> Repo.update!()

    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    window = %{
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

    assert {:ok, [_window]} = QuotaWindows.upsert_quota_windows(identity, [window])
    SavedResetConfirmationFixtures.confirm_automatic_pressure!(identity)
    Repo.reload!(identity)
  end

  # The earlier request's applied gateway consume, five minutes old.
  defp put_earlier_consume!(identity, phase) do
    consumed_at = DateTime.utc_now() |> DateTime.add(-5, :minute) |> DateTime.truncate(:microsecond)

    redemption = %{
      "phase" => phase,
      "status" => if(phase == "consumed_pending_probe", do: "redeeming", else: "succeeded"),
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 1,
      "trigger_kind" => "gateway_auto",
      "started_at" => DateTime.to_iso8601(consumed_at),
      "consumed_at" => DateTime.to_iso8601(consumed_at),
      "deadline_at" => DateTime.to_iso8601(RedemptionLifecycle.deadline_at(consumed_at)),
      "result" => %{"code" => "reset", "applied" => true}
    }

    identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption)) |> Repo.update!()
  end

  # The account's Usage API, the consume, and a Responses route that fails if
  # it is reached.
  defp bank_routes do
    usage = Map.new(@usage_paths, &{&1, {200, saved_reset_usage_payload(1)}})
    failure = {502, %{"error" => %{"message" => "synthetic upstream failure", "type" => "server_error"}}}
    {:path_json, usage |> Map.put(@consume_path, {200, %{"code" => "reset"}}) |> Map.put(@turn_endpoint, failure)}
  end

  # The provider's usage-limit refusal of R2's generation, before any output.
  defp usage_limit_refusal(transport) do
    resets_at = DateTime.to_unix(DateTime.utc_now()) + 1_800
    error = %{"type" => "usage_limit_reached", "message" => "synthetic provider text", "resets_at" => resets_at}
    headers = %{"x-codex-primary-used-percent" => "100", "x-codex-primary-window-minutes" => "300", "x-codex-primary-reset-at" => Integer.to_string(resets_at)}

    case transport do
      :http -> {:json_headers, 429, %{"error" => error}, Map.to_list(headers)}
      :websocket -> FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "error", "status" => 429, "error" => error, "headers" => headers})])
    end
  end

  defp post_native(pool, mode) do
    build_conn()
    |> put_req_header("authorization", pool.setup.authorization)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("session-id", "saved-reset-floor-#{System.unique_integer([:positive])}")
    |> then(&if mode == "lite", do: put_req_header(&1, "x-openai-internal-codex-responses-lite", "true"), else: &1)
    |> post(@turn_endpoint, CodexPooler.JSON.encode!(%{"model" => pool.setup.model.exposed_model_id, "input" => native_text_input("synthetic floor prompt"), "stream" => true}))
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
        "input" => native_text_input("synthetic floor prompt"),
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
    labels = %{pool.r1.assignment.id => :r1, pool.r2.assignment.id => :r2, pool.r3.assignment.id => :r3}

    from(a in Attempt, where: a.request_id == ^request.id, order_by: [asc: a.attempt_number])
    |> Repo.all()
    |> Enum.map(&{&1.status, &1.upstream_status_code, &1.network_error_code, Map.fetch!(labels, &1.pool_upstream_assignment_id)})
  end

  defp redemption_phase(identity), do: get_in(Repo.reload!(identity).metadata, ["saved_reset_redemption", "phase"])
  defp consumes(upstream), do: FakeUpstream.physical_counts(upstream).consume
end
