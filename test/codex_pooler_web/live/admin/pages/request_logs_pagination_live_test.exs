defmodule CodexPoolerWeb.Admin.RequestLogsPaginationLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Phoenix.LiveViewTest
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Events
  alias CodexPooler.Repo

  @reload_event [:codex_pooler, :admin, :request_logs, :reload]

  setup :register_and_log_in_user

  setup do
    %{pool: pool, api_key: api_key} = fixture = active_api_key_fixture()
    admitted_at = DateTime.utc_now() |> DateTime.add(-120, :second)

    requests =
      for index <- 1..101 do
        fixture
        |> request_fixture(%{correlation_id: "pagination-request-#{index}"})
        |> Ecto.Changeset.change(admitted_at: DateTime.add(admitted_at, index, :second))
        |> Repo.update!()
      end

    %{pool: pool, api_key: api_key, requests: Enum.reverse(requests)}
  end

  test "next and previous retain the frozen rows and page after arrivals and resume", %{conn: conn, pool: pool, api_key: api_key, requests: requests} do
    {:ok, view, _html} = live(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    render_async(view)
    assert_page(view, requests, 1)

    view |> element("#request-log-pagination-top-next") |> render_click()
    page_two_path = assert_patch(view)
    render_async(view)
    assert_page(view, requests, 2)
    assert URI.decode_query(URI.parse(page_two_path).query)["as_of_id"] == hd(requests).id

    reload_ref = attach_reload_telemetry(view)
    arrival = request_fixture(%{pool: pool, api_key: api_key}, %{correlation_id: "pagination-new-arrival"})
    assert {:ok, _event} = Events.broadcast_request_logs(pool.id, "request_log_created", %{request_id: arrival.id, status: arrival.status})
    assert_receive {^reload_ref, :event_refresh}, 15_000
    render_async(view)

    assert_page(view, requests, 2)
    assert has_element?(view, "[data-role='request-log-newer-count']", "1 newer")
    refute has_element?(view, "#request-log-row-#{arrival.id}")

    send(view.pid, :live_updates_resumed)
    assert_receive {^reload_ref, :event_refresh}, 15_000
    render_async(view)
    assert_page(view, requests, 2)

    view |> element("#request-log-pagination-next") |> render_click()
    assert_patch(view)
    render_async(view)
    assert_page(view, requests, 3)

    view |> element("#request-log-pagination-prev") |> render_click()
    assert_patch(view, page_two_path)
    render_async(view)
    assert_page(view, requests, 2)

    view |> element("#request-log-pagination-prev") |> render_click()
    first_page_path = assert_patch(view)
    render_async(view)
    refute URI.decode_query(URI.parse(first_page_path).query) |> Map.has_key?("as_of")
    assert_page(view, [arrival | requests], 1)
  end

  test "a direct pinned URL keeps its page through drawer navigation and resets on filtering", %{conn: conn, pool: pool, requests: requests} do
    head = hd(requests)
    selected = Enum.at(requests, 50)
    params = %{pool_id: pool.id, page: 2, as_of: DateTime.to_iso8601(head.admitted_at), as_of_id: head.id, selected_request_id: selected.id}
    {:ok, view, _html} = live(conn, ~p"/admin/request-logs?#{params}")
    render_async(view)
    assert_page(view, requests, 2)
    assert has_element?(view, "#request-log-detail-outcome")

    render_click(view, "close_request_log")
    closed_path = assert_patch(view)
    assert URI.decode_query(URI.parse(closed_path).query)["page"] == "2"
    assert_page(view, requests, 2)
    refute has_element?(view, "#request-log-detail-outcome")

    render_click(view, "open_request_log", %{"request-id" => selected.id})
    opened_path = assert_patch(view)
    assert URI.decode_query(URI.parse(opened_path).query)["page"] == "2"
    assert_page(view, requests, 2)
    assert has_element?(view, "#request-log-detail-outcome")

    render_click(view, "filter", %{"filters" => %{"pool_id" => pool.id, "request_id" => selected.id}})
    filtered_path = assert_patch(view)
    render_async(view)
    query = URI.decode_query(URI.parse(filtered_path).query)
    assert Map.take(query, ~w(page as_of as_of_id selected_request_id)) == %{}
    assert_page(view, [selected], 1)
    refute has_element?(view, "#request-log-detail-outcome")
  end

  test "an event refresh during page navigation completes the requested page", %{conn: conn, pool: pool, requests: requests} do
    {:ok, view, _html} = live(conn, ~p"/admin/request-logs?pool_id=#{pool.id}")
    render_async(view)
    assert_page(view, requests, 1)

    handler_id = {__MODULE__, :blocked_page_query, make_ref()}
    test_pid = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if metadata[:source] == "requests" and view.pid in Process.get(:"$callers", []) do
            owner_ref = Process.monitor(test_pid)
            send(test_pid, {handler_id, self()})

            receive do
              {^handler_id, :release} -> :ok
              {:DOWN, ^owner_ref, :process, ^test_pid, _reason} -> exit(:shutdown)
            after
              15_000 -> raise "page query was not released"
            end

            Process.demonitor(owner_ref, [:flush])
          end
        end,
        nil
      )

    view |> element("#request-log-pagination-top-next") |> render_click()
    assert_patch(view)
    assert_receive {^handler_id, query_pid}, 15_000
    on_exit(fn -> send(query_pid, {handler_id, :release}) end)
    :telemetry.detach(handler_id)

    send(view.pid, :refresh_request_logs_from_events)
    _ = :sys.get_state(view.pid)
    reload_ref = attach_reload_telemetry(view)
    send(query_pid, {handler_id, :release})
    assert_receive {^reload_ref, _stage}, 15_000
    render_async(view)
    assert_page(view, requests, 2)
  end

  test "an event refresh during initial pinned loading preserves the requested page", %{conn: conn, pool: pool, requests: requests} do
    handler_id = {__MODULE__, :blocked_initial_query, make_ref()}
    test_pid = self()
    {:ok, pool_id} = Ecto.UUID.dump(pool.id)
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if metadata[:source] == "requests" and pool_id in metadata.params and self() != test_pid do
            owner_ref = Process.monitor(test_pid)
            send(test_pid, {handler_id, self()})

            receive do
              {^handler_id, :release} -> :ok
              {:DOWN, ^owner_ref, :process, ^test_pid, _reason} -> exit(:shutdown)
            after
              15_000 -> raise "initial query was not released"
            end

            Process.demonitor(owner_ref, [:flush])
          end
        end,
        nil
      )

    head = hd(requests)
    params = %{pool_id: pool.id, page: 2, as_of: DateTime.to_iso8601(head.admitted_at), as_of_id: head.id}
    {:ok, view, _html} = live(conn, ~p"/admin/request-logs?#{params}")
    assert_receive {^handler_id, query_pid}, 15_000
    on_exit(fn -> send(query_pid, {handler_id, :release}) end)
    :telemetry.detach(handler_id)

    send(view.pid, :refresh_request_logs_from_events)
    _ = :sys.get_state(view.pid)
    reload_ref = attach_reload_telemetry(view)
    send(query_pid, {handler_id, :release})
    assert_receive {^reload_ref, _stage}, 15_000
    render_async(view)
    assert_page(view, requests, 2)
  end

  defp assert_page(view, requests, page) do
    expected = requests |> Enum.slice((page - 1) * 50, 50) |> Enum.map(&"request-log-row-#{&1.id}")
    actual = view |> render() |> LazyHTML.from_document() |> LazyHTML.query("tr[id^='request-log-row-']") |> LazyHTML.attribute("id")
    assert actual == expected

    for id <- ["request-log-pagination", "request-log-pagination-top"] do
      assert has_element?(view, "##{id} [data-role='pagination-status']", "Page #{page} of #{ceil(length(requests) / 50)}")
      assert has_element?(view, "##{id} [data-role='pagination-range']", "#{(page - 1) * 50 + 1}-#{min(page * 50, length(requests))} of #{length(requests)}")
    end

    document = view |> render() |> LazyHTML.from_document()

    for direction <- ["prev", "next"] do
      assert document |> LazyHTML.query("#request-log-pagination-#{direction}") |> LazyHTML.attribute("href") ==
               document |> LazyHTML.query("#request-log-pagination-top-#{direction}") |> LazyHTML.attribute("href")
    end
  end

  defp attach_reload_telemetry(view) do
    ref = make_ref()
    handler_id = {__MODULE__, ref}
    test_pid = self()
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        @reload_event,
        fn _event, _measurements, metadata, _config ->
          if self() == view.pid, do: send(test_pid, {ref, metadata.stage})
        end,
        nil
      )

    ref
  end
end
