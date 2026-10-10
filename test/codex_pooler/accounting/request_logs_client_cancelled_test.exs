defmodule CodexPooler.Accounting.RequestLogsClientCancelledTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Accounting
  alias CodexPooler.Accounts.Scope

  setup do
    %{pool: pool, api_key: api_key} = active_api_key_fixture()
    context = %{pool: pool, api_key: api_key}

    rows = %{
      cancelled_websocket: request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket"}),
      cancelled_stream: request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 200, transport: "http_sse"}),
      timeout: request_fixture(context, %{status: "failed", last_error_code: "client_disconnected", response_status_code: 499, transport: "websocket", request_metadata: %{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout"}}}),
      drained: request_fixture(context, %{status: "failed", last_error_code: "owner_drained", response_status_code: 499, transport: "websocket"}),
      recovered: request_fixture(context, %{status: "failed", last_error_code: "dead_execution_recovered", response_status_code: 499}),
      uncoded: request_fixture(context, %{status: "failed", last_error_code: nil, response_status_code: 502}),
      succeeded: request_fixture(context)
    }

    %{pool: pool, rows: rows}
  end

  test "each row carries the status it shows: a client cancellation keeps its recorded status beside the class", %{pool: pool, rows: rows} do
    items = pool |> Accounting.list_request_logs() |> Map.fetch!(:items) |> Map.new(&{&1.id, &1})

    assert items[rows.cancelled_websocket.id].status == "failed"
    assert items[rows.cancelled_websocket.id].display_status == "client_cancelled"
    assert items[rows.cancelled_stream.id].display_status == "client_cancelled"

    for key <- [:timeout, :drained, :recovered, :uncoded] do
      assert items[rows[key].id].display_status == "failed", "#{key} must show failed"
    end

    assert items[rows.succeeded.id].display_status == "succeeded"
  end

  test "client_cancelled selects the class, and false keeps every other failure, 499 cuts and uncoded failures included", %{pool: pool, rows: rows} do
    assert listed_ids(pool, client_cancelled: true) == ids(rows, [:cancelled_websocket, :cancelled_stream])
    assert listed_ids(pool, status: "failed", client_cancelled: false) == ids(rows, [:timeout, :drained, :recovered, :uncoded])
    assert listed_ids(pool, client_cancelled: false) == ids(rows, [:timeout, :drained, :recovered, :uncoded, :succeeded])
  end

  test "status alone keeps matching the recorded status, the reading every other caller relies on", %{pool: pool, rows: rows} do
    assert listed_ids(pool, status: "failed") == ids(rows, [:cancelled_websocket, :cancelled_stream, :timeout, :drained, :recovered, :uncoded])
  end

  test "the scoped detail read carries the class too", %{pool: pool, rows: rows} do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    scope = Scope.for_user(owner)

    assert %{display_status: "client_cancelled", status: "failed"} = Accounting.get_request_log_for_scope(scope, rows.cancelled_stream.id)
    assert %{display_status: "failed"} = Accounting.get_request_log_for_scope(scope, rows.drained.id)
    assert pool.id == Accounting.get_request_log_for_scope(scope, rows.drained.id).pool_id
  end

  defp listed_ids(pool, filters) do
    pool
    |> Accounting.list_request_logs(filters: filters)
    |> Map.fetch!(:items)
    |> MapSet.new(& &1.id)
  end

  defp ids(rows, keys), do: MapSet.new(keys, &rows[&1].id)
end
