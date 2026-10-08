defmodule CodexPoolerWeb.Admin.RequestLogsServiceTierLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  # Failure-detection budget for an asynchronous load the test awaits: a green
  # run returns as soon as the view has settled.
  @detection_timeout_ms 15_000

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Pools

  setup :register_and_log_in_user

  # The ChatGPT Codex backend reports `default` on the terminal event of a
  # `priority` request. Settlements before findings#206 row 206-271 priced the
  # reported tier; these rows carry the persisted columns such a settlement
  # wrote, which the list and drawer keep showing as billed.
  @echo_mismatch %{
    requested_service_tier: "priority",
    actual_service_tier: "default",
    service_tier: "standard",
    settlement_details: %{"pricing_status" => "priced", "settled_cost_micros" => "40"}
  }

  @sensitive_marker "service-tier-log-prompt-must-not-render"

  test "list rows say which tier was requested when the upstream reported another", %{
    conn: conn,
    scope: scope
  } do
    pool = create_pool!(scope, "tier-echo-list")

    %{request: sse_mismatch} =
      request_log_fixture(
        pool,
        Map.merge(@echo_mismatch, %{correlation_id: "req-tier-sse", transport: "http_sse"})
      )

    %{request: websocket_mismatch} =
      request_log_fixture(
        pool,
        Map.merge(@echo_mismatch, %{correlation_id: "req-tier-ws", transport: "websocket"})
      )

    %{request: priority_reported} =
      request_log_fixture(pool, %{
        correlation_id: "req-tier-priority-reported",
        transport: "http_sse",
        requested_service_tier: "priority",
        actual_service_tier: "priority",
        service_tier: "priority",
        settlement_details: %{"pricing_status" => "priced"}
      })

    %{request: no_tier} =
      request_log_fixture(pool, %{correlation_id: "req-tier-none", transport: "websocket"})

    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    for request <- [sse_mismatch, websocket_mismatch] do
      model_cell = "#request-log-#{request.id}-model-details"

      refute has_element?(view, "#{model_cell} [data-role='model-service-tier']")
      refute has_element?(view, "#request-log-#{request.id}-requested-tier")
      refute has_element?(view, "#request-log-#{request.id}-protocol [data-role='fast-mode-indicator']")

      assert has_element?(view, "#{model_cell} [data-role='model-speed'][data-speed-level='1'][title*='priority requested, not applied']")
      refute has_element?(view, "#{model_cell} [data-role='model-speed'][data-speed-level='2']")
    end

    assert has_element?(view, "#request-log-#{sse_mismatch.id}-protocol", "HTTP SSE")
    assert has_element?(view, "#request-log-#{websocket_mismatch.id}-protocol", "WebSocket")

    refute has_element?(view, "#request-log-#{priority_reported.id}-model-details [data-role='model-service-tier']")
    refute has_element?(view, "#request-log-#{priority_reported.id}-requested-tier")
    refute has_element?(view, "#request-log-#{priority_reported.id}-protocol [data-role='fast-mode-indicator']")

    assert has_element?(view, "#request-log-#{priority_reported.id}-model-details [data-role='model-speed'][data-speed-level='2'][title='Fast (priority tier)']")

    refute has_element?(view, "#request-log-#{no_tier.id}-requested-tier")
    assert has_element?(view, "#request-log-#{no_tier.id}-model-details [data-role='model-speed'][data-speed-level='1'][title='Normal speed']")
    refute has_element?(view, "#request-log-#{no_tier.id}-model-details [data-role='model-speed'][data-speed-level='2']")

    refute render(view) =~ @sensitive_marker
  end

  test "drawer separates the requested, upstream-reported, and billed tiers", %{
    conn: conn,
    scope: scope
  } do
    pool = create_pool!(scope, "tier-echo-drawer")

    %{request: sse_mismatch} =
      request_log_fixture(
        pool,
        Map.merge(@echo_mismatch, %{correlation_id: "req-drawer-tier-sse", transport: "http_sse"})
      )

    %{request: websocket_mismatch} =
      request_log_fixture(
        pool,
        Map.merge(@echo_mismatch, %{
          correlation_id: "req-drawer-tier-ws",
          transport: "websocket"
        })
      )

    %{request: priority_reported} =
      request_log_fixture(pool, %{
        correlation_id: "req-drawer-tier-priority",
        transport: "websocket",
        requested_service_tier: "priority",
        actual_service_tier: "priority",
        service_tier: "priority",
        settlement_details: %{"pricing_status" => "priced"}
      })

    %{request: unrequested} =
      request_log_fixture(pool, %{
        correlation_id: "req-drawer-tier-unrequested",
        transport: "http_sse",
        actual_service_tier: "default",
        service_tier: "standard",
        settlement_details: %{"pricing_status" => "priced"}
      })

    %{request: unpriced_mismatch} =
      request_log_fixture(
        pool,
        Map.merge(@echo_mismatch, %{
          correlation_id: "req-drawer-tier-unpriced",
          transport: "http_sse",
          settlement_details: %{"pricing_status" => "unpriced_missing_model"}
        })
      )

    %{request: no_tier} =
      request_log_fixture(pool, %{correlation_id: "req-drawer-tier-none", transport: "http_sse"})

    for request <- [sse_mismatch, websocket_mismatch] do
      view = open_selected_request(conn, pool, request)

      assert has_element?(view, "#request-log-detail-requested-tier", "Requested tier")
      assert has_element?(view, "#request-log-detail-requested-tier", "priority")
      assert has_element?(view, "#request-log-detail-upstream-reported-tier", "Upstream reported")
      assert has_element?(view, "#request-log-detail-upstream-reported-tier", "default")
      assert has_element?(view, "#request-log-detail-priced-tier", "Priced as")
      assert has_element?(view, "#request-log-detail-priced-tier", "standard")
      refute render(view) =~ @sensitive_marker
    end

    view = open_selected_request(conn, pool, priority_reported)
    assert has_element?(view, "#request-log-detail-requested-tier", "priority")
    assert has_element?(view, "#request-log-detail-upstream-reported-tier", "priority")
    assert has_element?(view, "#request-log-detail-priced-tier", "priority")

    view = open_selected_request(conn, pool, unrequested)
    assert has_element?(view, "#request-log-detail-requested-tier", "Not set")
    assert has_element?(view, "#request-log-detail-upstream-reported-tier", "default")
    assert has_element?(view, "#request-log-detail-priced-tier", "standard")

    # Without a priced settlement no tier was billed, so the row is omitted.
    view = open_selected_request(conn, pool, unpriced_mismatch)
    assert has_element?(view, "#request-log-detail-requested-tier", "priority")
    assert has_element?(view, "#request-log-detail-upstream-reported-tier", "default")
    refute has_element?(view, "#request-log-detail-priced-tier")

    view = open_selected_request(conn, pool, no_tier)
    refute has_element?(view, "#request-log-detail-requested-tier")
    refute has_element?(view, "#request-log-detail-upstream-reported-tier")
    refute has_element?(view, "#request-log-detail-priced-tier")
  end

  test "a claimed priority request shows an unknown tier until reservation and a priced badge only after settlement", %{conn: conn, scope: scope} do
    pool = create_pool!(scope, "tier-lifecycle")
    %{api_key: key} = active_api_key_fixture(pool)
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    identifier = "tier-transition-#{System.unique_integer([:positive])}"
    model = model_fixture(pool, %{upstream_model_id: identifier, exposed_model_id: identifier, pricing_ref: identifier})
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    CodexPooler.Repo.insert!(%CodexPooler.Catalog.PricingSnapshot{model_identifier: identifier, price_version: "tier-transition", currency_code: "USD", billing_unit: "token", input_token_micros: Decimal.new(100), output_token_micros: Decimal.new(200), effective_at: DateTime.add(now, -60, :second), captured_at: now, config: %{"service_tier" => "priority", "price_bucket" => "default", "pricing_type" => "per_1m_tokens", "availability" => "priced"}})
    auth = %{pool: pool, api_key: key}
    correlation = Ecto.UUID.generate()
    assert {:ok, %{request: claim}} = CodexPooler.Accounting.claim_websocket_turn(auth, model, %{endpoint: "/backend-api/codex/responses", correlation_id: correlation})
    {:ok, view, _} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    model_cell = "#request-log-#{claim.id}-model-details"
    speed = "#{model_cell} [data-role='model-speed']"
    protocol = "#request-log-#{claim.id}-protocol"
    refute has_element?(view, "#{model_cell} [data-role='model-service-tier']")
    assert has_element?(view, "#{speed}[data-speed-level='1']")
    refute has_element?(view, "#{speed}[data-speed-level='2']")

    payload = %{"model" => identifier, "service_tier" => "priority", "max_output_tokens" => 1}
    assert {:ok, reserved} = CodexPooler.Accounting.reserve(auth, model, payload, %{endpoint: "/backend-api/codex/responses", transport: "websocket", correlation_id: correlation, turn_claim: claim})
    send(view.pid, :refresh_request_logs_from_events)
    await_request_logs(view)
    assert has_element?(view, "#{speed}[data-speed-level='2'][title='Fast (priority tier)']")
    refute has_element?(view, "#{protocol} [data-role='fast-mode-indicator']")

    assert {:ok, attempt} = CodexPooler.Accounting.create_attempt(reserved.request, assignment)
    assert {:ok, _} = CodexPooler.Accounting.finalize_success(reserved.request, attempt, %{status: "usage_known", input_tokens: 2, output_tokens: 1, total_tokens: 3}, %{response_status_code: 200, attempt_metadata: %{"service_tier" => "default"}})
    send(view.pid, :refresh_request_logs_from_events)
    await_request_logs(view)
    refute has_element?(view, "#{model_cell} [data-role='model-service-tier']")
    refute has_element?(view, "#request-log-#{claim.id}-requested-tier")
    assert has_element?(view, "#{speed}[data-speed-level='2'][title='Fast (priority tier)']")
  end

  test "a row with the ultrafast tier renders speed level 3, priced or not", %{conn: conn, scope: scope} do
    pool = create_pool!(scope, "tier-ultrafast")

    %{request: priced} =
      request_log_fixture(pool, %{
        correlation_id: "req-tier-ultrafast-priced",
        transport: "websocket",
        requested_service_tier: "ultrafast",
        actual_service_tier: "ultrafast",
        service_tier: "ultrafast",
        settlement_details: %{"pricing_status" => "priced"}
      })

    %{request: unpriced} =
      request_log_fixture(pool, %{
        correlation_id: "req-tier-ultrafast-unpriced",
        transport: "http_sse",
        requested_service_tier: "ultrafast",
        actual_service_tier: "ultrafast",
        settlement_details: %{"pricing_status" => "unpriced_missing_model"}
      })

    {:ok, view, _html} = live_request_logs(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")

    for request <- [priced, unpriced] do
      speed = "#request-log-#{request.id}-model-details [data-role='model-speed']"
      assert has_element?(view, "#{speed}[data-speed-level='3'][title='Ultrafast']")
      refute has_element?(view, "#{speed}[data-speed-level='2']")
    end
  end

  defp create_pool!(scope, slug) do
    {:ok, pool} = Pools.create_pool(scope, %{slug: slug, name: slug})
    pool
  end

  defp request_log_fixture(pool, attrs) do
    %{api_key: api_key} = active_api_key_fixture(pool, %{display_name: "Tier log key"})

    %{identity: identity, assignment: assignment} =
      upstream_assignment_fixture(pool, %{
        account_label: "Tier log upstream",
        assignment_label: "Tier log assignment"
      })

    request =
      request_fixture(%{pool: pool, api_key: api_key}, %{
        requested_model: "gpt-tier-log",
        endpoint: "/backend-api/codex/responses",
        status: "succeeded",
        correlation_id: Map.fetch!(attrs, :correlation_id),
        transport: Map.fetch!(attrs, :transport),
        request_metadata: %{"prompt" => @sensitive_marker},
        response_status_code: 200,
        usage_status: "usage_known",
        service_tier: Map.get(attrs, :service_tier),
        requested_service_tier: Map.get(attrs, :requested_service_tier),
        actual_service_tier: Map.get(attrs, :actual_service_tier)
      })

    attempt =
      attempt_fixture(request, assignment, %{
        status: "succeeded",
        usage_status: "usage_known",
        upstream_status_code: 200
      })

    ledger_entry_fixture(request, %{
      attempt_id: attempt.id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: identity.id,
      input_tokens: 2,
      cached_input_tokens: 0,
      output_tokens: 1,
      total_tokens: 3,
      settled_cost_micros: 40,
      usage_status: "usage_known",
      details: Map.get(attrs, :settlement_details, %{})
    })

    %{request: request}
  end

  defp open_selected_request(conn, pool, request) do
    {:ok, view, _html} =
      live_request_logs(
        conn,
        ~p"/admin/request-logs?pool_id=#{pool.id}&selected_request_id=#{request.id}"
      )

    assert has_element?(view, "#request-log-detail-request-id", request.id)
    view
  end

  defp live_request_logs(conn, path) do
    with {:ok, view, html} <- live(conn, path) do
      _ = await_request_logs(view)
      {:ok, view, html}
    end
  end

  defp await_request_logs(view),
    do: await_request_logs(view, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_request_logs(view, deadline) do
    _ = render_async(view, 5_000)
    state = :sys.get_state(view.pid)

    if state.socket.assigns.request_logs_loading? or
         state.socket.assigns.request_logs_running? do
      if System.monotonic_time(:millisecond) >= deadline, do: flunk("request logs did not finish loading: #{inspect(:sys.get_state(view.pid))}")

      receive do
      after
        1 -> await_request_logs(view, deadline)
      end
    else
      state
    end
  end
end
