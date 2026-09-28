defmodule CodexPoolerWeb.Runtime.ProviderRetryAfterTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.FakeUpstream

  @moduletag capture_log: true

  for mode <- ["full", "lite"], route <- ["/backend-api/codex/responses", "/v1/responses"], status <- [429, 502, 503, 504], hint <- ["3", "Mon, 28 Sep 2026 12:00:00 GMT", "Wed, 28 Sep 2050 12:00:00 GMT"] do
    @mode mode
    @route route
    @status status
    @hint hint
    test "#{mode} #{route} #{@status} forwards #{@hint}", %{conn: conn} do
      upstream = start_upstream({:json_headers, @status, %{"error" => %{"code" => "rate_limit_exceeded"}}, [{"retry-after", @hint}]})
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, @mode)
      conn = conn |> auth(setup) |> then(&if @mode == "lite", do: put_req_header(&1, "x-openai-internal-codex-responses-lite", "true"), else: &1)
      started_at = System.monotonic_time(:millisecond)
      conn = post(conn, @route, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic input"), "stream" => true})
      assert conn.status == @status
      elapsed_ms = System.monotonic_time(:millisecond) - started_at
      assert [hint] = get_resp_header(conn, "retry-after")

      if @hint == "3" do
        # Capture lies inside this request interval. Remaining whole seconds
        # cannot exceed the original advice or lose more than that interval.
        assert {remaining, ""} = Integer.parse(hint)
        assert remaining in max(3 - div(elapsed_ms + 999, 1_000), 0)..3
      else
        assert hint == @hint
      end

      assert get_resp_header(conn, "x-should-retry") == []
      assert FakeUpstream.count(upstream) == 1
    end
  end
end
