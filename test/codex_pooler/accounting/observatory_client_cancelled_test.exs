defmodule CodexPooler.Accounting.ObservatoryClientCancelledTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Access.DashboardSessions.Principal, as: DashboardPrincipal
  alias CodexPooler.Accounting.Usage.Observatory
  alias CodexPooler.Repo

  @as_of ~U[2026-07-17 12:00:00Z]

  # One key's hour: five requests in the first half, seven in the second (the
  # halves the success-rate trend compares). A client cancellation is a failed
  # request recorded with `client_disconnected` (findings#292): a websocket
  # closes with the 499, an HTTP stream that had already answered keeps its
  # 200. Every other failure keeps counting, a Pooler-side cut and a failure
  # without a code included.
  setup do
    pool = pool_fixture()
    api_key = dashboard_api_key_fixture(pool)
    %{api_key: other_key} = active_api_key_fixture(pool)
    model = model_fixture(pool, %{exposed_model_id: "gpt-observatory-cancelled"})

    for {minute, attrs} <- [
          {"11:02", %{status: "succeeded"}},
          {"11:07", %{status: "succeeded"}},
          {"11:12", %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}},
          {"11:17", %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"}},
          {"11:22", %{status: "failed", last_error_code: nil, response_status_code: 502}},
          {"11:32", %{status: "succeeded"}},
          {"11:37", %{status: "failed", last_error_code: "upstream_unavailable", response_status_code: 502}},
          {"11:42", %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, request_metadata: %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout"}}}},
          {"11:45", %{status: "rejected", last_error_code: "pinned_continuation_unavailable", response_status_code: 409}},
          {"11:47", %{status: "succeeded", last_error_code: "client_disconnected"}},
          {"11:50", %{status: "failed", last_error_code: "websocket_replay_expired", response_status_code: 499}},
          {"11:52", %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499}}
        ] do
      timed_request(pool, api_key, minute, Map.put(attrs, :model_id, model.id))
    end

    # Another key of the same Pool: never part of this key's numbers.
    timed_request(pool, other_key, "11:33", %{model_id: model.id, status: "failed", last_error_code: "client_disconnected", response_status_code: 499})

    assert {:ok, projection} = Observatory.read(dashboard_principal(pool, api_key), "1h", as_of: @as_of)
    %{projection: projection}
  end

  test "the totals count a client cancellation apart, never as a failure", %{projection: projection} do
    assert projection.totals.requests == %{total: 12, succeeded: 4, failed: 5, in_progress: 0, client_cancelled: 3}
  end

  test "each bucket counts the class apart as well", %{projection: projection} do
    assert %{failed: 0, client_cancelled: 1} = Enum.at(projection.buckets, 2).requests
    assert %{failed: 0, client_cancelled: 1} = Enum.at(projection.buckets, 3).requests
    assert %{failed: 1, client_cancelled: 0} = Enum.at(projection.buckets, 4).requests
    assert %{failed: 1, client_cancelled: 0} = Enum.at(projection.buckets, 8).requests
    assert %{failed: 1, client_cancelled: 1} = Enum.at(projection.buckets, 10).requests
  end

  # The trend compares the success rate of the two halves over the requests
  # the client did not cancel, the base the headline rate uses: 2 of 3, then 2 of 6.
  test "the success-rate trend leaves the cancellations out of both halves", %{projection: projection} do
    assert projection.trends.success_rate == %{current: 33.3, previous: 66.7, delta: -33.4}
  end

  test "a recent outcome shows the class instead of a failure reason, and a stale code never labels a success", %{projection: projection} do
    assert Enum.map(projection.outcomes, &{&1.status, &1.code}) == [
             {"client_cancelled", nil},
             {"failed", "request_failed"},
             {"succeeded", nil},
             {"rejected", "request_failed"},
             {"failed", "service_unavailable"},
             {"failed", "service_unavailable"},
             {"succeeded", nil},
             {"failed", nil},
             {"client_cancelled", nil},
             {"client_cancelled", nil},
             {"succeeded", nil},
             {"succeeded", nil}
           ]
  end

  test "the projection carries no raw error code", %{projection: projection} do
    rendered = inspect(projection, limit: :infinity)

    for raw <- ~w(client_disconnected owner_drained websocket_replay_expired pinned_continuation_unavailable) do
      refute rendered =~ raw
    end
  end

  defp dashboard_api_key_fixture(pool) do
    %{api_key: api_key} = active_api_key_fixture(pool)

    api_key
    |> APIKey.changeset(%{dashboard_access: true})
    |> Repo.update!()
  end

  defp dashboard_principal(pool, api_key) do
    DashboardPrincipal.new(%{api_key_id: api_key.id, pool_id: pool.id, display_name: api_key.display_name, key_prefix: api_key.key_prefix})
  end

  defp timed_request(pool, api_key, minute, attrs) do
    [hour, minute] = minute |> String.split(":") |> Enum.map(&String.to_integer/1)
    admitted_at = %{DateTime.new!(~D[2026-07-17], Time.new!(hour, minute, 0), "Etc/UTC") | microsecond: {0, 6}}

    %{pool: pool, api_key: api_key}
    |> request_fixture(attrs)
    |> Ecto.Changeset.change(%{admitted_at: admitted_at, completed_at: admitted_at})
    |> Repo.update!()
  end
end
