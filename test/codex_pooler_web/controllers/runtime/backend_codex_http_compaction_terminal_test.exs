defmodule CodexPoolerWeb.Runtime.BackendCodexHttpCompactionTerminalTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Gateway.Runtime.Streaming.CompactionResultCollector
  alias CodexPooler.Gateway.Transports.Streaming.CollectedBody
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  for mode <- ["full", "lite"] do
    @mode mode
    test "#{mode} HTTP compact preserves a valid fatal quota terminal without changing settlement", %{conn: conn} do
      upstream = start_upstream({:sse, [quota_terminal()]})
      setup = gateway_setup(upstream, compact?: true)
      serving_mode!(setup, @mode)

      {response, log} = with_log(fn -> post_compact(conn, setup) end)

      assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
      assert request.response_status_code == 429
      assert request.status == "failed"
      assert request.last_error_code == "invalid_compaction_response"
      assert request.transport == "http_compact_json"
      assert request.retry_count == 0
      assert attempt.status == "failed"
      assert attempt.network_error_code == "invalid_compaction_response"
      assert attempt.upstream_status_code == 200
      refute attempt.retryable
      assert attempt.response_metadata["compaction_invalid_reason"] == "provider_failure"
      assert attempt.response_metadata["upstream_error_code"] == "insufficient_quota"
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
      assert [%{reason_code: "invalid_compaction_response"}] = Repo.all(from(d in BridgeDemotion))
      assert [%{reason_code: "invalid_compaction_response"}] = Repo.all(from(c in RoutingCircuitState))
      assert FakeUpstream.http_request_count(upstream) == 1
      refute inspect({request, attempt}) =~ "synthetic private provider text"
      refute log =~ "synthetic private provider text"
      assert %{"error" => %{"code" => "insufficient_quota", "message" => "upstream rejected the compact request"}} = json_response(response, 429)
      refute response.resp_body =~ "synthetic private provider text"
    end

    @mode mode
    test "#{mode} HTTP compact success remains canonical", %{conn: conn} do
      item = %{"type" => "compaction", "encrypted_content" => "synthetic-compact"}

      upstream =
        start_upstream(
          FakeUpstream.sse_stream([
            {"response.output_item.done", %{"type" => "response.output_item.done", "item" => item}},
            {"response.completed", %{"type" => "response.completed", "response" => %{"status" => "completed", "output" => [item]}}}
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      serving_mode!(setup, @mode)
      response = post_compact(conn, setup)
      events = response(response, 200) |> String.split("\n") |> Enum.filter(&(String.starts_with?(&1, "data: ") and &1 != "data: [DONE]")) |> Enum.map(fn line -> line |> String.trim_leading("data: ") |> CodexPooler.JSON.decode!() end)
      assert Enum.any?(events, fn event -> event["type"] == "response.output_item.done" and event["item"] == item end)
      assert Enum.any?(events, fn event -> event["type"] == "response.completed" end)
      assert FakeUpstream.http_request_count(upstream) == 1
      assert [%{status: "succeeded", response_status_code: 200}] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    end
  end

  for mode <- ["full", "lite"], variant <- [:plain, :preamble, :split] do
    @tag :compact_client_status
    test "#{mode} #{variant} compact provider error records client502 and upstream200", %{conn: conn} do
      assert_compact_error_status(conn, unquote(mode), unquote(variant))
    end
  end

  defp assert_compact_error_status(conn, mode, variant) do
    release = make_ref()
    upstream = start_upstream(error_stream(variant, release))
    setup = gateway_setup(upstream, compact?: true)
    serving_mode!(setup, mode)
    {response, log} = collect_error_response(conn, setup, variant, release)
    assert %{"error" => %{"code" => "invalid_compaction_response", "message" => "upstream compact stream was invalid"}} = json_response(response, 502)
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.response_status_code == 502
    assert request.status == "failed"
    assert request.last_error_code == "invalid_compaction_response"
    assert request.usage_status == "usage_unknown"
    assert request.retry_count == 0
    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    assert attempt.upstream_status_code == 200
    assert attempt.status == "failed"
    assert attempt.network_error_code == "invalid_compaction_response"
    refute attempt.retryable
    assert attempt.response_metadata["compaction_invalid_reason"] == "provider_failure"
    assert attempt.response_metadata["upstream_error_code"] == "internal_error"
    assert attempt.response_metadata["stream_terminal_type"] == "error"
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    assert FakeUpstream.http_request_count(upstream) == 1
    refute inspect({request, attempt}) =~ "synthetic private provider text"
    refute response.resp_body =~ "synthetic private provider text"
    refute log =~ "synthetic private provider text"
    assert log =~ "compaction_invalid_reason=provider_failure"
    assert log =~ "provider_terminal_type=error"
    assert log =~ "upstream_error_code=internal_error"
  end

  defp error_stream(:plain, _release), do: {:sse, [internal_error_terminal()]}
  defp error_stream(:preamble, _release), do: {:sse, [block(%{"type" => "response.created", "response" => %{"status" => "in_progress"}}), internal_error_terminal()]}

  defp error_stream(:split, release) do
    [label, data] = String.split(internal_error_terminal(), "data: ", parts: 2)
    FakeUpstream.barrier_sse_stream([label, "data: " <> data], barrier_after: 1, notify: self(), release_ref: release, done: false)
  end

  defp collect_error_response(conn, setup, :split, release) do
    mfa = {CompactionResultCollector, :collect_sse_data, 2}
    on_exit(fn -> :erlang.trace_pattern(mfa, false, [:local]) end)
    :erlang.trace_pattern(mfa, [{:_, [], [{:message, :collector_chunk}]}], [:local])
    supervisor = start_supervised!(Task.Supervisor)

    task =
      Task.Supervisor.async_nolink(supervisor, fn ->
        receive do
          :start -> with_log(fn -> post_compact(conn, setup) end)
        end
      end)

    monitor = Process.monitor(task.pid)
    :erlang.trace(task.pid, true, [:call, :arity, :set_on_spawn, {:tracer, self()}])
    send(task.pid, :start)
    assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^release}, 15_000
    assert_receive {:trace, _pid, :call, ^mfa, :collector_chunk}, 15_000
    send(handler, {:fake_upstream_release_chunk, release})
    result = Task.await(task, 15_000)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, 15_000
    :erlang.trace_pattern(mfa, false, [:local])
    result
  end

  defp collect_error_response(conn, setup, _variant, _release), do: with_log(fn -> post_compact(conn, setup) end)

  for mode <- ["full", "lite"] do
    test "#{mode} ordinary committed SSE keeps client request and upstream200", %{conn: conn} do
      assert_committed_sse_status(conn, unquote(mode))
    end
  end

  defp assert_committed_sse_status(conn, mode) do
    upstream = start_upstream({:sse, [block(%{"type" => "response.output_text.delta", "delta" => "synthetic-visible"}), internal_error_terminal()]})
    setup = gateway_setup(upstream)
    serving_mode!(setup, mode)

    {response, log} =
      with_log(fn ->
        conn |> auth(setup) |> post("/backend-api/codex/responses", %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic boundary"), "stream" => true})
      end)

    assert response(response, 200) =~ "internal_error"
    assert [request] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
    assert request.response_status_code == 200
    assert request.status == "failed"
    assert request.retry_count == 0
    assert [attempt] = Repo.all(from a in Attempt, where: a.request_id == ^request.id)
    assert attempt.upstream_status_code == 200
    assert FakeUpstream.http_request_count(upstream) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    refute inspect({request, attempt}) =~ "synthetic private provider text"
    refute log =~ "synthetic private provider text"
  end

  defp internal_error_terminal, do: block(%{"type" => "error", "error" => %{"code" => "internal_error", "message" => "synthetic private provider text"}})

  test "malformed oversized and post-terminal quota material cannot project a fatal quota", %{conn: conn} do
    overflow = block(%{"type" => CollectedBody.overflow_event_type()})
    unrelated = block(%{"type" => "response.output_text.delta", "delta" => "synthetic"})

    cases = [
      ["data: malformed\n\n", quota_terminal()],
      [overflow, quota_terminal()],
      [quota_terminal() <> unrelated],
      [quota_terminal(), unrelated],
      [quota_terminal(), "data: unfinished"],
      [block(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "insufficient_quota "}}})]
    ]

    for chunks <- cases do
      upstream = start_upstream({:sse, chunks})
      setup = gateway_setup(upstream, compact?: true)
      {response, log} = with_log(fn -> post_compact(recycle(conn), setup) end)
      assert %{"error" => %{"code" => "invalid_compaction_response"}} = json_response(response, 502)
      refute log =~ "synthetic private provider text"
      assert FakeUpstream.http_request_count(upstream) == 1
    end
  end

  test "a separately delivered chunk after a quota terminal invalidates its witness", %{conn: conn} do
    release = make_ref()
    terminal = quota_event()

    upstream =
      start_upstream(
        FakeUpstream.barrier_sse_stream(
          [
            {"response.failed", terminal},
            {"response.output_text.delta", %{"type" => "response.output_text.delta", "delta" => "synthetic"}}
          ],
          barrier_after: 1,
          notify: self(),
          release_ref: release,
          done: false
        )
      )

    setup = gateway_setup(upstream, compact?: true)
    supervisor = start_supervised!(Task.Supervisor)
    mfa = {CompactionResultCollector, :collect_sse_data, 2}
    on_exit(fn -> :erlang.trace_pattern(mfa, false, [:call_count]) end)
    :erlang.trace_pattern(mfa, true, [:call_count])
    task = Task.Supervisor.async_nolink(supervisor, fn -> with_log(fn -> post_compact(conn, setup) end) end)
    assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^release}, 15_000

    try do
      await_collector_chunk!(mfa, System.monotonic_time(:millisecond) + 15_000)
    after
      send(handler, {:fake_upstream_release_chunk, release})
    end

    {response, log} = Task.await(task, 15_000)
    :erlang.trace_pattern(mfa, false, [:call_count])
    assert %{"error" => %{"code" => "invalid_compaction_response"}} = json_response(response, 502)
    assert log =~ "source_stage=collector_invalid"
    assert [%{response_metadata: %{"compaction_invalid_reason" => "invalid_after_provider_failure"}}] = Repo.all(from(a in Attempt))
    assert FakeUpstream.http_request_count(upstream) == 1
  end

  defp await_collector_chunk!(mfa, deadline) do
    case :erlang.trace_info(mfa, :call_count) do
      {:call_count, count} when count > 0 ->
        :ok

      _pending ->
        assert System.monotonic_time(:millisecond) < deadline, "collector did not consume the first chunk"

        receive do
        after
          10 -> await_collector_chunk!(mfa, deadline)
        end
    end
  end

  defp post_compact(conn, setup) do
    conn
    |> auth(setup)
    |> post("/backend-api/codex/responses", %{
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("synthetic compact boundary") ++ [%{"type" => "compaction_trigger"}],
      "stream" => true,
      "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"compaction" => %{"implementation" => "responses_compaction_v2"}})}
    })
  end

  defp serving_mode!(setup, mode) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: now, updated_at: now})
  end

  defp quota_terminal, do: block(quota_event())
  defp quota_event, do: %{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "insufficient_quota", "message" => "synthetic private provider text"}}}

  defp block(event), do: "event: #{event["type"]}\ndata: #{CodexPooler.JSON.encode!(event)}\n\n"
end
