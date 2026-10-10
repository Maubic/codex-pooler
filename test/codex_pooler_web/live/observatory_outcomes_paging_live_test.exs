defmodule CodexPoolerWeb.ObservatoryOutcomesPagingLiveTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPooler.PoolerFixtures
  import Phoenix.LiveViewTest

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv
  alias CodexPoolerWeb.ObservatoryControllerTestHelpers, as: Helpers

  @owner_env :observatory_outcomes_paging_test_owner

  defmodule ControlledReader do
    def read(_principal, window), do: await_result({:refresh, window})
    def read_outcomes(principal, opts), do: await_result({:outcomes, principal, opts})

    defp await_result(operation) do
      owner = Application.fetch_env!(:codex_pooler, :observatory_outcomes_paging_test_owner)
      reference = make_ref()
      send(owner, {:paging_read_started, self(), reference, operation})

      receive do
        {:complete_paging_read, ^reference, result} -> result
      after
        5_000 -> {:error, :controlled_reader_timeout}
      end
    end
  end

  test "all last-hour outcomes append beyond 200 rows with tied timestamps and normalized clients", %{conn: conn} do
    context = authenticated_context(conn)
    timestamp = DateTime.utc_now() |> DateTime.add(-10) |> DateTime.truncate(:microsecond)

    expected_tokens =
      for tokens <- 1..205 do
        request = timed_request(context, timestamp, %{user_agent: "codex-tui/1.2.3 synthetic-private-ua-marker"})
        ledger_entry_fixture(request, %{total_tokens: tokens, occurred_at: timestamp})
        {request.id, Integer.to_string(tokens)}
      end
      |> Enum.sort_by(&elem(&1, 0), :desc)
      |> Enum.map(&elem(&1, 1))

    old_model = model_fixture(context.pool, %{exposed_model_id: "old-chart-model", display_name: "old-chart-model"})
    old_request = timed_request(context, DateTime.add(timestamp, -7_200), %{model_id: old_model.id, requested_model: "old-chart-model"})
    ledger_entry_fixture(old_request, %{model_id: old_model.id, total_tokens: 999, occurred_at: old_request.admitted_at})
    %{api_key: other_key} = active_api_key_fixture(context.pool)
    timed_request(%{context | api_key: other_key}, timestamp, %{requested_model: "other-key-model"})

    {:ok, view, _html} = live(context.conn, "/observatory")
    render_click(view, "select-window", %{"window" => "7d"})
    Helpers.await_async(view)

    assert outcome_tokens(view) == Enum.take(expected_tokens, 200)
    assert has_element?(view, "#observatory-outcomes-load-more:not([disabled])")
    assert has_element?(view, "#observatory-outcomes-heading", "Last 60 minutes")
    assert has_element?(view, "#observatory-window-7d[aria-pressed='true']")
    assert has_element?(view, "[data-role='observatory-model-row']", "old-chart-model")
    refute has_element?(view, "[data-role='outcome-model']", "old-chart-model")
    refute has_element?(view, "[data-role='outcome-model']", "other-key-model")
    assert has_element?(view, "[data-role='outcome-client'][data-client-kind='codex']", "Codex")
    assert has_element?(view, "[data-role='outcome-client'] .request-client-logo[style*='codex.svg']")
    refute render(view) =~ "synthetic-private-ua-marker"
    refute render(view) =~ "codex-tui/1.2.3"

    view |> element("#observatory-outcomes-load-more") |> render_click()
    Helpers.await_async(view)
    assert outcome_tokens(view) == expected_tokens
    refute has_element?(view, "#observatory-outcomes-load-more")

    for reason <- ["manual", "periodic"] do
      render_hook(view, "observatory-refresh", %{"reason" => reason})
      Helpers.await_async(view)
      assert outcome_tokens(view) == Enum.take(expected_tokens, 200)
      assert has_element?(view, "#observatory-outcomes-load-more")
      view |> element("#observatory-outcomes-load-more") |> render_click()
      Helpers.await_async(view)
      assert outcome_tokens(view) == expected_tokens
    end

    render_click(view, "select-window", %{"window" => "1h"})
    Helpers.await_async(view)
    assert outcome_tokens(view) == Enum.take(expected_tokens, 200)
    refute has_element?(view, "[data-role='observatory-model-row']", "old-chart-model")
  end

  test "pagination uses the stored snapshot and cursor and retries an error without losing rows", %{conn: conn} do
    {view, context, initial} = controlled_view(conn)
    render_click(view, "load-more-outcomes", %{"as_of" => "forged", "before" => "forged", "api_key_id" => "forged"})
    pending = receive_read()
    assert {:outcomes, principal, opts} = pending.operation
    assert principal.api_key_id == context.api_key.id
    assert opts == [as_of: initial.outcomes_page.as_of, before: initial.outcomes_page.next_cursor]
    assert has_element?(view, "#observatory-outcomes-load-more[disabled]")

    complete_read(pending, {:error, %{detail: "synthetic-internal-page-error"}})
    Helpers.await_async(view)
    assert has_element?(view, "#observatory-outcomes-load-error")
    assert outcome_tokens(view) == ["1"]
    refute render(view) =~ "synthetic-internal-page-error"

    view |> element("#observatory-outcomes-load-more") |> render_click()
    retry = receive_read()
    assert retry.operation == pending.operation
    complete_read(retry, {:ok, page(initial, 2)})
    Helpers.await_async(view)
    assert outcome_tokens(view) == ["1", "2"]
    refute has_element?(view, "#observatory-outcomes-load-error")
    refute has_element?(view, "#observatory-outcomes-load-more")
  end

  test "a page completed after a newer refresh cannot append stale outcomes or restore its cursor", %{conn: conn} do
    {view, _context, initial} = controlled_view(conn)
    view |> element("#observatory-outcomes-load-more") |> render_click()
    older_page = receive_read()

    render_hook(view, "observatory-refresh", %{"reason" => "manual"})
    refresh = receive_read()
    refute has_element?(view, "#observatory-outcomes-load-more")
    render_click(view, "load-more-outcomes", %{"before" => "forged"})
    assigns = :sys.get_state(view.pid).socket.assigns
    assert assigns.outcomes_page == nil
    refute assigns.loading_outcomes
    refute_received {:paging_read_started, _, _, {:outcomes, _, _}}
    newer = %{report(33) | outcomes_page: %{initial.outcomes_page | has_more: false, next_cursor: nil}}
    complete_read(refresh, {:ok, newer})
    assert has_element?(view, "[data-role='outcome-tokens']", "33")

    complete_read(older_page, {:ok, page(initial, 2)})
    Helpers.await_async(view)
    assert outcome_tokens(view) == ["33"]
    refute has_element?(view, "#observatory-outcomes-load-more")
    assert has_element?(view, "#observatory-page[data-freshness-generation='2']")
  end

  test "disabling dashboard access before loading a page redirects the holder", %{conn: conn} do
    {view, context, _initial} = controlled_view(conn)
    disable_access(context.api_key)
    render_click(view, "load-more-outcomes")
    assert_redirect(view, "/observatory/login")
    refute_received {:paging_read_started, _, _, {:outcomes, _, _}}
  end

  test "disabling dashboard access while a page is pending rejects its completed result", %{conn: conn} do
    {view, context, initial} = controlled_view(conn)
    view |> element("#observatory-outcomes-load-more") |> render_click()
    pending = receive_read()
    disable_access(context.api_key)
    complete_read(pending, {:ok, page(initial, 2)})
    assert_redirect(view, "/observatory/login")
  end

  defp authenticated_context(conn) do
    pool = pool_fixture()
    %{api_key: api_key, raw_key: raw_key} = active_api_key_fixture(pool)
    api_key = Helpers.enable_dashboard_access!(api_key)
    conn = Helpers.post_login(conn, CodexPoolerWeb.Endpoint, %{"observatory" => %{"api_key" => raw_key}})
    %{conn: conn, pool: pool, api_key: api_key}
  end

  defp controlled_view(conn) do
    TestAppEnv.restore_on_exit(:observatory_reader)
    TestAppEnv.restore_on_exit(@owner_env)
    Application.put_env(:codex_pooler, :observatory_reader, ControlledReader)
    Application.put_env(:codex_pooler, @owner_env, self())
    context = authenticated_context(conn)
    {:ok, view, _html} = live(context.conn, "/observatory")
    render_hook(view, "observatory-refresh", %{"reason" => "initial"})
    initial = report(1)
    complete_read(receive_read(), {:ok, initial})
    Helpers.await_async(view)
    {view, context, initial}
  end

  defp receive_read do
    assert_receive {:paging_read_started, task, reference, operation}, 2_000
    %{task: task, reference: reference, monitor: Process.monitor(task), operation: operation}
  end

  defp complete_read(%{task: task, reference: reference, monitor: monitor}, result) do
    send(task, {:complete_paging_read, reference, result})
    assert_receive {:DOWN, ^monitor, :process, ^task, :normal}, 2_000
  end

  defp report(tokens) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %{
      accounting: %{status: "complete"},
      buckets: [],
      models: [],
      outcomes: [outcome(tokens, now)],
      outcomes_page: %{as_of: now, has_more: true, next_cursor: %{timestamp: DateTime.add(now, -1), id: Ecto.UUID.generate()}},
      performance: %{},
      totals: %{cost: %{}, requests: %{failed: 0, succeeded: 1, total: 1}, tokens: %{cached_input: 0, input: tokens, total: tokens}},
      trends: %{},
      window: %{ended_at: now, key: "24h", started_at: DateTime.add(now, -86_400)}
    }
  end

  defp page(initial, tokens) do
    %{outcomes: [outcome(tokens, DateTime.add(initial.outcomes_page.as_of, -2))], outcomes_page: %{initial.outcomes_page | has_more: false, next_cursor: nil}}
  end

  defp outcome(tokens, timestamp), do: %{model: "sample-model", total_tokens: tokens, timestamp: timestamp, status: "succeeded", endpoint_class: "responses"}

  defp outcome_tokens(view) do
    view |> render() |> LazyHTML.from_fragment() |> LazyHTML.query("[data-role='outcome-tokens']") |> Enum.map(&(LazyHTML.text(&1) |> String.trim()))
  end

  defp disable_access(api_key), do: api_key |> APIKey.changeset(%{dashboard_access: false}) |> Repo.update!()

  defp timed_request(context, timestamp, attrs) do
    context |> request_fixture(attrs) |> Ecto.Changeset.change(%{admitted_at: timestamp, completed_at: timestamp}) |> Repo.update!()
  end
end
