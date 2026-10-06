defmodule CodexPoolerWeb.Runtime.DroppedModelMeterRoutingTest do
  # An account's usage reading can list an additional meter for one model (a
  # model-scoped 5-hour or weekly window). When the provider stops listing it,
  # its last row stays: the usage refresh deletes only descriptors its payload
  # covers, and retention keeps evidence 30 days past its reset. Once that
  # meter's reset has passed and the account's usage reading kept syncing for a
  # full freshness TTL after the meter's last report, routing reads the meter as
  # dropped (`Windows.Routing.reject_dropped_meter_windows/2`, findings#305 row
  # 498-4) instead of blocking the model for up to 30 days; the row stays for
  # every read surface.
  #
  # Topology: one Pool, one BEAM node, owner forwarding off, Full serving mode,
  # one FakeUpstream per account, native HTTP SSE turns on model
  # `gpt-test-model`. Each account answers the provider usage read with an
  # available 5-hour window and no meter for the model.
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

  describe "one account" do
    test "a weekly model meter the provider stopped reporting days ago no longer blocks the model" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity, observed_at: now())
      put_meter!(setup, "secondary", 10_080, reset_at: days_ago(13), observed_at: days_ago(20))

      conn = post_turn(setup, "dropped-weekly-meter")

      assert conn.status == 200, body(conn)
      assert [row] = rows!(setup)
      assert attempt_statuses(row) == [{"succeeded", 200}]
      assert usage_reads(upstream) == 0
      # The row stays for retention and the read surfaces.
      assert Enum.any?(QuotaWindows.list_quota_windows(setup.identity), &(&1.quota_scope == "model" and &1.model == setup.model.exposed_model_id))
    end

    test "the reproduced case: an expired meter next to a stale account reading serves after the seat's usage refresh" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity, observed_at: minutes_ago(20))
      put_meter!(setup, "primary", 300, reset_at: minutes_ago(60), observed_at: minutes_ago(400))

      conn = post_turn(setup, "dropped-meter-stale-account")

      assert conn.status == 200, body(conn)
      assert usage_reads(upstream) == 1
      assert [row] = rows!(setup)
      assert attempt_statuses(row) == [{"succeeded", 200}]
    end

    test "control: an expired meter last reported within one freshness TTL of the account's reading keeps blocking" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity, observed_at: now())
      # The cycle ended a minute ago and the meter was read five minutes ago:
      # the provider may still list it, so its new cycle is not known yet.
      put_meter!(setup, "primary", 300, reset_at: minutes_ago(1), observed_at: minutes_ago(5))

      conn = post_turn(setup, "recent-expired-meter")

      assert conn.status == 503, body(conn)
      assert [row] = rows!(setup)
      assert row.status == "rejected"
      assert model_posts(upstream) == 0
    end

    test "control: an exhausted meter still in its cycle keeps blocking" do
      %{setup: setup, upstream: upstream} = single_account!()
      put_account_window!(setup.identity, observed_at: now())
      put_meter!(setup, "primary", 300, reset_at: minutes_from_now(90), observed_at: now(), used_percent: Decimal.new("100"))

      conn = post_turn(setup, "running-exhausted-meter")

      assert conn.status in [429, 503], body(conn)
      assert [%{status: "rejected"}] = rows!(setup)
      assert model_posts(upstream) == 0
    end
  end

  describe "a held-back account" do
    # Two canonical partitions of one member each: the anchor account (older)
    # is exhausted; the other advertises the model with another behavioral
    # source field and carries the dropped meter. Selection reads it routable
    # and moves the turn to it.
    test "a dropped meter on a held-back account no longer keeps selection on an exhausted partition" do
      anchor_upstream = start_upstream(account_routes("anchor"))
      other_upstream = start_upstream(account_routes("other"))
      setup = gateway_setup(anchor_upstream, quota?: false)
      other = gateway_upstream(setup.pool, other_upstream, "upstream-token-dropped-meter-other", compact?: false)
      other = %{other | assignment: other.assignment |> Ecto.Changeset.change(created_at: DateTime.add(setup.assignment.created_at, 10, :second)) |> Repo.update!()}

      prime_exhausted_routing_quota!(setup.identity, %{reset_at: minutes_from_now(180)})
      put_account_window!(other.identity, observed_at: now())
      put_meter!(%{setup | identity: other.identity}, "secondary", 10_080, reset_at: days_ago(13), observed_at: days_ago(20))

      model =
        setup.model
        |> put_model_source_assignments!([setup.assignment, other.assignment])
        |> put_source_field!(other.assignment, "supports_experimental_context", true)

      setup = %{setup | model: model}

      conn = post_turn(setup, "dropped-meter-held-back")

      assert conn.status == 200, body(conn)
      assert [row] = rows!(setup)
      assert [{"succeeded", 200, assignment_id}] = attempts!(row)
      assert assignment_id == other.assignment.id
      assert %{"partition_count" => 2, "selected_count" => 1, "routable_selection" => true, "selected_routable_count" => 1, "held_back_routable_count" => 0} = row.request_metadata["canonical_partition"]
      assert model_posts(anchor_upstream) == 0
    end
  end

  defp single_account! do
    upstream = start_upstream(account_routes("single"))
    %{setup: gateway_setup(upstream, quota?: false), upstream: upstream}
  end

  # The account answers its usage read with an available 5-hour window and
  # no meter for the model, and serves a turn.
  defp account_routes(label) do
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
                "response" => %{"id" => "resp_dropped_meter_#{label}", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 4, "output_tokens" => 2, "total_tokens" => 6}}
              }}
           ],
           headers: [{"x-synthetic-account", label}]
         )
     }}
  end

  defp put_account_window!(identity, observed_at: observed_at) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(identity, [
               primary_quota_window_attrs(%{used_percent: Decimal.new("12"), reset_at: minutes_from_now(120), source: "codex_usage_api", observed_at: observed_at, last_sync_at: observed_at})
             ])
  end

  defp put_meter!(setup, kind, minutes, opts) do
    model = setup.model

    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(setup.identity, [
               %{
                 quota_key: String.replace(model.exposed_model_id, "-", "_"),
                 window_kind: kind,
                 window_minutes: minutes,
                 used_percent: Keyword.get(opts, :used_percent, Decimal.new("0")),
                 reset_at: Keyword.fetch!(opts, :reset_at),
                 source: "codex_usage_api",
                 source_precision: "observed",
                 quota_scope: "model",
                 quota_family: "codex_model",
                 model: model.exposed_model_id,
                 upstream_model: model.upstream_model_id,
                 freshness_state: "fresh",
                 observed_at: Keyword.fetch!(opts, :observed_at),
                 last_sync_at: Keyword.fetch!(opts, :observed_at)
               }
             ])
  end

  defp put_source_field!(model, assignment, field, value) do
    sources = Map.update!(model.metadata["source_assignment_models"], assignment.id, &Map.put(&1, field, value))
    model |> Ecto.Changeset.change(metadata: Map.put(model.metadata, "source_assignment_models", sources)) |> Repo.update!()
  end

  defp post_turn(setup, session) do
    build_conn()
    |> put_req_header("authorization", setup.authorization)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("session-id", session)
    |> post(@turn_endpoint, CodexPooler.JSON.encode!(%{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic prompt"), "stream" => true, "store" => false}))
  end

  defp rows!(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp attempts!(row) do
    from(a in Attempt, where: a.request_id == ^row.id, order_by: [asc: a.attempt_number])
    |> Repo.all()
    |> Enum.map(&{&1.status, &1.upstream_status_code, &1.pool_upstream_assignment_id})
  end

  defp attempt_statuses(row), do: Enum.map(attempts!(row), fn {status, code, _assignment_id} -> {status, code} end)

  defp usage_reads(upstream), do: Enum.count(FakeUpstream.requests(upstream), &(&1.path == @usage_path))
  defp model_posts(upstream), do: Enum.count(FakeUpstream.requests(upstream), &(&1.path == @turn_endpoint))
  defp body(conn), do: String.slice(conn.resp_body, 0, 300)

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:second)
  defp minutes_ago(minutes), do: DateTime.add(now(), -minutes * 60, :second)
  defp minutes_from_now(minutes), do: DateTime.add(now(), minutes * 60, :second)
  defp days_ago(days), do: DateTime.add(now(), -days * 86_400, :second)
end
