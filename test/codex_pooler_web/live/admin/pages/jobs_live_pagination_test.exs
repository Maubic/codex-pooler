defmodule CodexPoolerWeb.Admin.JobsLivePaginationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import Phoenix.LiveViewTest

  alias CodexPooler.Jobs.RuntimeStateCleanupWorker
  alias CodexPooler.Repo

  setup :register_and_log_in_user

  setup do
    Repo.delete_all(Oban.Job)
    :ok
  end

  test "a dropdown filter starts at page one and keeps the other filters", %{conn: conn} do
    jobs = insert_jobs(21)
    selected_job = hd(jobs)
    set_state([selected_job], "retryable")
    worker = selected_job.worker

    {:ok, view, _html} = live(conn, ~p"/admin/jobs?page=2&worker=#{worker}&show_completed=true&job_id=#{selected_job.id}")

    assert has_element?(view, "#admin-jobs-explorer-pagination", "Page 2 of 2")

    view
    |> element("#job-state-filter [data-role='state-filter-option'][data-state='retryable']")
    |> render_click()

    assert_patch_params(view, %{"state" => "retryable", "worker" => worker, "show_completed" => "true", "job_id" => to_string(selected_job.id)})
    assert_page(view, 1, 1)
    assert has_element?(view, "#job-#{selected_job.id}")
    assert has_element?(view, "#job-detail-sidebar", "Job ##{selected_job.id}")
  end

  for {selector, expected, total} <- [
        {"#job-attention-filter [data-attention='active_failure']", %{"attention" => "active_failure"}, 0},
        {"#job-worker-filter [data-worker='CodexPooler.Jobs.RuntimeStateCleanupWorker']", %{"worker" => "CodexPooler.Jobs.RuntimeStateCleanupWorker"}, 21},
        {"#job-target-kind-filter [data-target-kind='system']", %{"target_kind" => "system"}, 21},
        {"#job-show-completed-filter [data-show-completed='true']", %{"show_completed" => "true"}, 21}
      ] do
    test "#{selector} resets its page", %{conn: conn} do
      insert_jobs(21)
      {:ok, view, _html} = live(conn, ~p"/admin/jobs?page=2")

      view |> element(unquote(selector)) |> render_click()

      assert_patch_params(view, unquote(Macro.escape(expected)))
      assert_page(view, 1, unquote(total))
    end
  end

  test "clearing the target kind resets the page", %{conn: conn} do
    insert_jobs(21)
    {:ok, view, _html} = live(conn, ~p"/admin/jobs?page=2&target_kind=system")

    view |> element("#job-target-kind-filter [data-target-kind='']") |> render_click()

    assert_patch_params(view, %{})
    assert_page(view, 1, 21)
  end

  for event <- [:change, :submit] do
    test "form #{event} starts at page one despite the hidden page value", %{conn: conn} do
      jobs = insert_jobs(21)
      selected_job = hd(jobs)
      set_state([selected_job], "retryable")

      {:ok, view, _html} = live(conn, ~p"/admin/jobs?page=2&show_completed=true")
      form = element(view, "#job-filter-form")
      params = %{"filters" => %{"state" => "retryable", "page" => "2", "show_completed" => "true"}}

      case unquote(event) do
        :change -> render_change(form, params)
        :submit -> render_submit(form, params)
      end

      assert_patch_params(view, %{"state" => "retryable", "show_completed" => "true"})
      assert_page(view, 1, 1)
      assert has_element?(view, "#job-#{selected_job.id}")
    end
  end

  for message <- [:refresh_jobs, :fallback_refresh_jobs, :live_updates_resumed] do
    test "#{message} recovers the last valid page when completed jobs leave it empty", %{conn: conn} do
      jobs = insert_jobs(41)
      worker = hd(jobs).worker
      {:ok, view, _html} = live(conn, ~p"/admin/jobs?page=3&worker=#{worker}")

      assert_page(view, 3, 41)
      set_state([hd(jobs)], "completed")
      send(view.pid, unquote(message))

      assert_page(view, 2, 40)
      assert_patch_params(view, %{"page" => "2", "worker" => worker})
      assert has_element?(view, "#admin-jobs-explorer-pagination-range", "Showing 21-40 of 40")
      assert has_element?(view, "#job-#{Enum.at(jobs, 1).id}")
      refute has_element?(view, "#admin-jobs-empty-state")
    end
  end

  test "an empty result set resets the page without dropping its filters", %{conn: conn} do
    jobs = insert_jobs(21)
    worker = hd(jobs).worker
    {:ok, view, _html} = live(conn, ~p"/admin/jobs?page=2&worker=#{worker}")

    set_state(jobs, "completed")
    send(view.pid, :refresh_jobs)

    assert_page(view, 1, 0)
    assert_patch_params(view, %{"worker" => worker})
    assert has_element?(view, "#admin-jobs-empty-state", "No jobs match these filters")
    refute has_element?(view, "#admin-jobs-explorer-pagination")
  end

  test "an out of range URL redirects to the last valid page and preserves a valid selection", %{conn: conn} do
    jobs = insert_jobs(21)
    selected_job = hd(jobs)
    worker = selected_job.worker
    params = %{"page" => "99", "worker" => worker, "job_id" => to_string(selected_job.id)}
    path = ~p"/admin/jobs?#{params}"
    corrected_path = ~p"/admin/jobs?#{Map.put(params, "page", "2")}"

    assert {:error, {:live_redirect, %{to: ^corrected_path}}} = live(conn, path)

    {:ok, view, _html} = live(conn, corrected_path)
    assert_page(view, 2, 21)
    assert has_element?(view, "#job-detail-sidebar", "Job ##{selected_job.id}")
  end

  test "a refresh keeps a valid current page and its selected job", %{conn: conn} do
    jobs = insert_jobs(22)
    selected_job = hd(jobs)
    params = %{"page" => "2", "worker" => selected_job.worker, "job_id" => to_string(selected_job.id)}
    {:ok, view, _html} = live(conn, ~p"/admin/jobs?#{params}")

    set_state([List.last(jobs)], "completed")
    send(view.pid, :refresh_jobs)

    assert_page(view, 2, 21)
    assert :sys.get_state(view.pid).socket.assigns.current_params == params
    assert has_element?(view, "#job-detail-sidebar", "Job ##{selected_job.id}")
  end

  test "filter reset and page recovery preserve a visible worker failure panel", %{conn: conn} do
    jobs = insert_jobs(21)
    selected_job = hd(jobs)
    completing_job = List.last(jobs)
    set_state([selected_job], "discarded")

    Repo.update_all(from(job in Oban.Job, where: job.id == ^completing_job.id), set: [worker: "Example.Jobs.PaginationWorker"])

    Repo.update_all(from(job in Oban.Job, where: job.id == ^selected_job.id),
      set: [attempt: 1, errors: [%{"attempt" => 1, "kind" => "RuntimeError", "error" => "cleanup failed"}]]
    )

    params = %{"page" => "2", "failure_job_id" => to_string(selected_job.id)}
    {:ok, view, _html} = live(conn, ~p"/admin/jobs?#{params}")

    assert has_element?(view, "#job-failure-#{selected_job.id}[aria-expanded='true']")
    set_state([completing_job], "completed")
    send(view.pid, :refresh_jobs)

    assert_page(view, 1, 20)
    assert_patch_params(view, Map.delete(params, "page"))
    assert has_element?(view, "#job-failure-#{selected_job.id}[aria-expanded='true']")

    view
    |> element("#job-state-filter [data-role='state-filter-option'][data-state='discarded']")
    |> render_click()

    assert_patch_params(view, %{"state" => "discarded", "failure_job_id" => to_string(selected_job.id)})
    assert_page(view, 1, 1)
    assert has_element?(view, "#job-failure-#{selected_job.id}[aria-expanded='true']")
  end

  test "page recovery clears missing drawer and failure selections with one URL correction", %{conn: conn} do
    jobs = insert_jobs(21)
    selected_job = hd(jobs)
    params = %{"page" => "2", "job_id" => to_string(selected_job.id)}
    {:ok, view, _html} = live(conn, ~p"/admin/jobs?#{params}")
    assert has_element?(view, "#job-detail-drawer[checked]")

    set_state(jobs, "completed")
    send(view.pid, :refresh_jobs)

    assert_page(view, 1, 0)
    assert_patch_params(view, %{})
    refute has_element?(view, "#job-detail-drawer[checked]")

    assert {:error, {:live_redirect, %{to: "/admin/jobs"}}} =
             live(conn, ~p"/admin/jobs?page=99&job_id=#{selected_job.id}&failure_job_id=#{selected_job.id}")
  end

  defp insert_jobs(count) do
    for index <- 1..count do
      {:ok, job} = %{"index" => index} |> RuntimeStateCleanupWorker.new() |> Oban.insert()
      inserted_at = DateTime.add(~U[2026-05-04 12:00:00Z], index, :second)
      {1, _rows} = Repo.update_all(from(row in Oban.Job, where: row.id == ^job.id), set: [inserted_at: inserted_at])
      job
    end
  end

  defp set_state(jobs, state) do
    ids = Enum.map(jobs, & &1.id)
    Repo.update_all(from(job in Oban.Job, where: job.id in ^ids), set: [state: state])
  end

  defp assert_patch_params(view, expected) do
    assert %URI{path: "/admin/jobs", query: query} = URI.parse(assert_patch(view))
    assert URI.decode_query(query || "") == expected
  end

  defp assert_page(view, page, total) do
    assigns = :sys.get_state(view.pid).socket.assigns
    assert assigns.filters.page == page
    assert assigns.explorer.offset == (page - 1) * 20
    assert assigns.explorer.total == total
    assert has_element?(view, "#filters_page[value='#{page}']")
  end
end
