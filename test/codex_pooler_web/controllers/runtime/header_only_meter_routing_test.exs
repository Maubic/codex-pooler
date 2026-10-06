defmodule CodexPoolerWeb.Runtime.HeaderOnlyMeterRoutingTest do
  # A meter that only responses carry (the `x-<limit>-*` rate-limit headers or
  # a `codex.rate_limits` event; the provider's usage read lists no such
  # descriptor) is refreshed only by traffic on its account. Once it is older
  # than the freshness TTL it reads stale, the request's quota refresh reads
  # the account's usage and cannot refresh it, so before findings#305 row
  # 498-9 the account stayed out of routing until the meter's reset: for its
  # model when the meter is named, for every model when it is not, and no
  # response could refresh it while it blocked. Routing now ignores such a
  # stale meter when its last reading was not exhausted; an exhausted one
  # keeps blocking until its reset, and the provider's refusal stays the
  # authority for a meter that has run out since.
  #
  # Topology: one Pool, one BEAM node, owner forwarding off, Full serving mode,
  # one FakeUpstream account, native HTTP SSE turns on model `gpt-test-model`.
  # The account answers the provider usage read with an available 5-hour
  # window and no meter, and serves a turn.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows

  @moduletag capture_log: true

  @turn_endpoint "/backend-api/codex/responses"
  @usage_path "/backend-api/wham/usage"

  describe "a stale header-only meter whose last reading was not exhausted" do
    test "named for the model, it no longer keeps the model off the account, turn after turn" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity)
      put_meter!(setup, :model, used: "10", observed_at: minutes_ago(20))

      for session <- ["header-meter-first", "header-meter-second"] do
        conn = post_turn(setup, session)
        assert conn.status == 200, body(conn)
      end

      assert Enum.map(rows!(setup), &attempt_statuses/1) == [[{"succeeded", 200}], [{"succeeded", 200}]]
      assert usage_reads(upstream) == 0
      # The row stays for the read surfaces.
      assert Enum.any?(QuotaWindows.list_quota_windows(setup.identity), &(&1.source == "codex_response_headers" and &1.quota_scope == "model"))
    end

    test "unnamed (feature scope), it no longer keeps every model off the account" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity)
      put_meter!(setup, :feature, used: "10", observed_at: minutes_ago(20))

      conn = post_turn(setup, "header-feature-meter")

      assert conn.status == 200, body(conn)
      assert [row] = rows!(setup)
      assert attempt_statuses(row) == [{"succeeded", 200}]
      assert usage_reads(upstream) == 0
    end
  end

  describe "controls" do
    test "a stale header-only meter whose last reading was exhausted keeps blocking until its reset" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity)
      put_meter!(setup, :model, used: "100", observed_at: minutes_ago(20))

      conn = post_turn(setup, "header-meter-exhausted")

      assert conn.status in [429, 503], body(conn)
      assert [%{status: "rejected"}] = rows!(setup)
      assert model_posts(upstream) == 0
    end

    test "a stale meter the usage read reports keeps blocking: its own read can refresh it" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity)
      put_meter!(setup, :model, used: "10", observed_at: minutes_ago(20), source: "codex_usage_api")

      conn = post_turn(setup, "usage-api-meter-stale")

      assert conn.status == 503, body(conn)
      assert [%{status: "rejected"}] = rows!(setup)
      assert model_posts(upstream) == 0
    end

    test "a fresh header-only meter below its limit lets the turn through" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity)
      put_meter!(setup, :model, used: "10", observed_at: now())

      conn = post_turn(setup, "header-meter-fresh")

      assert conn.status == 200, body(conn)
      assert usage_reads(upstream) == 0
    end
  end

  defp single_account! do
    upstream = start_upstream(account_routes())
    %{setup: gateway_setup(upstream, quota?: false), upstream: upstream}
  end

  # The account answers its usage read with an available 5-hour window and
  # no meter, and serves a turn.
  defp account_routes do
    {:path_json,
     %{
       @usage_path =>
         {200,
          %{
            "rate_limit" => %{
              "primary_window" => %{
                "used_percent" => 12,
                "limit_window_seconds" => 18_000,
                "reset_after_seconds" => 7_200,
                "reset_at" => DateTime.to_unix(minutes_from_now(120))
              }
            }
          }},
       @turn_endpoint =>
         FakeUpstream.sse_stream(
           [
             {"response.completed",
              %{
                "type" => "response.completed",
                "response" => %{"id" => "resp_header_only_meter", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 4, "output_tokens" => 2, "total_tokens" => 6}}
              }}
           ],
           headers: [{"x-synthetic-account", "header-only-meter"}]
         )
     }}
  end

  defp put_account_window!(identity) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(identity, [
               primary_quota_window_attrs(%{used_percent: Decimal.new("12"), reset_at: minutes_from_now(120), source: "codex_usage_api", observed_at: now(), last_sync_at: now()})
             ])
  end

  # The meter's 5-hour window, its cycle two hours from its end. A meter
  # from the response headers by default (`source:` overrides).
  defp put_meter!(setup, scope, opts) do
    observed_at = Keyword.fetch!(opts, :observed_at)

    attrs =
      %{
        window_kind: "primary",
        window_minutes: 300,
        used_percent: Decimal.new(Keyword.fetch!(opts, :used)),
        reset_at: minutes_from_now(120),
        source: Keyword.get(opts, :source, "codex_response_headers"),
        source_precision: "observed",
        freshness_state: "fresh",
        observed_at: observed_at,
        last_sync_at: observed_at
      }
      |> Map.merge(meter_identity(setup.model, scope))

    assert {:ok, [_window]} = QuotaWindows.upsert_quota_windows(setup.identity, [attrs])
  end

  defp meter_identity(model, :model),
    do: %{quota_key: String.replace(model.exposed_model_id, "-", "_"), quota_scope: "model", quota_family: "codex_model", model: model.exposed_model_id, upstream_model: model.upstream_model_id}

  defp meter_identity(_model, :feature), do: %{quota_key: "synthetic_feature", quota_scope: "feature", quota_family: "synthetic_feature"}

  defp post_turn(setup, session) do
    build_conn()
    |> put_req_header("authorization", setup.authorization)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("session-id", session)
    |> post(@turn_endpoint, CodexPooler.JSON.encode!(%{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic prompt"), "stream" => true, "store" => false}))
  end

  defp rows!(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp attempt_statuses(row) do
    from(a in Attempt, where: a.request_id == ^row.id, order_by: [asc: a.attempt_number], select: {a.status, a.upstream_status_code})
    |> Repo.all()
  end

  defp usage_reads(upstream), do: Enum.count(FakeUpstream.requests(upstream), &(&1.path == @usage_path))
  defp model_posts(upstream), do: Enum.count(FakeUpstream.requests(upstream), &(&1.path == @turn_endpoint))
  defp body(conn), do: String.slice(conn.resp_body, 0, 300)

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second)
  defp minutes_ago(minutes), do: DateTime.add(now(), -minutes * 60, :second)
  defp minutes_from_now(minutes), do: DateTime.add(now(), minutes * 60, :second)
end
