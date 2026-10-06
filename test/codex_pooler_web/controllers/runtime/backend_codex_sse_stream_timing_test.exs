defmodule CodexPoolerWeb.Runtime.BackendCodexSseStreamTimingTest do
  # Two tiny `/v1` requests took 9.6 s and 14.5 s although the provider sent its 200 headers within a second and
  # then held the stream; no row could show that, because nothing recorded when the headers, the first event and the
  # first output arrived. The attempt of an upstream HTTP SSE request now records them, as integers in milliseconds
  # from the attempt start, with whether the request opened its connection or reused a pooled one. The serving node
  # is not a timing field: `attempts.owner_instance_id` already names it. Provenance: the stream shapes are the
  # documented Responses SSE vocabulary with invented ids and texts; the holds are real, short pauses injected by the
  # fake provider, so every relation below is asserted against half of the pause rather than an exact value.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, execute_backend_stream!: 2, first_event_terminal_sse: 2, gateway_setup: 1, native_text_input: 1, start_upstream: 1, stream_retry_setup: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @hold_ms 300
  @timing_keys ~w(connection first_event_ms first_visible_ms headers_ms)
  @moduletag capture_log: true

  for mode <- ["full", "lite"] do
    test "#{mode}: a body held after the headers is a small headers_ms and a large first_event_ms and first_visible_ms" do
      release_ref = make_ref()
      upstream = start_upstream(FakeUpstream.barrier_sse_stream(events("resp_timing_hold"), barrier_after: 0, notify: self(), release_ref: release_ref, done: false))
      setup = serving_setup(upstream, unquote(mode))

      task = Task.async(fn -> post_native(setup, unquote(mode), "held body") end)
      assert_receive {:fake_upstream_chunk_barrier, 0, handler, ^release_ref}, 5_000
      pause(@hold_ms)
      send(handler, {:fake_upstream_release_chunk, release_ref})
      conn = Task.await(task, 5_000)

      assert conn.status == 200
      {request, attempt} = settled_rows(setup)
      assert request.status == "succeeded"

      timing = attempt.response_metadata["stream_timing"]
      assert Enum.sort(Map.keys(timing)) == @timing_keys
      assert %{"headers_ms" => headers_ms, "first_event_ms" => first_event_ms, "first_visible_ms" => first_visible_ms, "connection" => "fresh"} = timing
      assert Enum.all?([headers_ms, first_event_ms, first_visible_ms], &(is_integer(&1) and &1 >= 0))

      # The headers were sent at once; everything after them waited for the hold.
      assert first_event_ms - headers_ms >= div(@hold_ms, 2)
      assert headers_ms < first_event_ms
      assert first_visible_ms >= first_event_ms
      assert attempt.latency_ms >= first_visible_ms

      # The serving node is already on the row, so per-pod latency reads from `owner_instance_id`.
      assert attempt.owner_instance_id == Atom.to_string(node())
    end
  end

  test "a lifecycle event first and the output later is a small first_event_ms and a larger first_visible_ms" do
    upstream = start_upstream(FakeUpstream.delayed_sse_stream(events("resp_timing_lifecycle"), interval_ms: @hold_ms, done: false))
    setup = gateway_setup(upstream)

    conn = post_native(setup, "full", "lifecycle then output")

    assert conn.status == 200
    {_request, attempt} = settled_rows(setup)
    assert %{"first_event_ms" => first_event_ms, "first_visible_ms" => first_visible_ms} = attempt.response_metadata["stream_timing"]
    assert first_visible_ms - first_event_ms >= div(@hold_ms, 2)
  end

  test "the first request to an upstream opens a connection and the next one reuses it" do
    upstream = start_upstream(FakeUpstream.sse_stream(events("resp_timing_connection"), done: false))
    setup = gateway_setup(upstream)

    for label <- ["first", "second"] do
      assert post_native(setup, "full", "connection #{label}").status == 200
    end

    attempts = Repo.all(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id, order_by: [asc: a.started_at]))
    assert Enum.map(attempts, & &1.response_metadata["stream_timing"]["connection"]) == ["fresh", "reused"]
  end

  test "a first-event provider failure keeps the timing of the attempt it ended" do
    upstream = start_upstream(first_event_terminal_sse("response.failed", "server_error"))
    setup = gateway_setup(upstream)

    conn = post_native(setup, "full", "first event failure")

    assert conn.status == 200
    {request, attempt} = settled_rows(setup)
    assert request.status == "failed"
    assert attempt.response_metadata["stream_failure_stage"] == "first_event"

    timing = attempt.response_metadata["stream_timing"]
    assert Enum.sort(Map.keys(timing)) == ~w(connection first_event_ms headers_ms)
    assert timing["headers_ms"] <= timing["first_event_ms"]
  end

  test "a retryable first-event failure keeps its own timing and the retried attempt records its own" do
    {setup, first_upstream, second_upstream} = stream_retry_setup(first_event_terminal_sse("response.failed", "server_error"))

    execute_backend_stream!(setup, "stream-timing-retry")

    assert FakeUpstream.count(first_upstream) == 1
    assert FakeUpstream.count(second_upstream) == 1
    assert [first_attempt, second_attempt] = Repo.all(from(a in Attempt, order_by: [asc: a.attempt_number]))

    assert first_attempt.status == "retryable_failed"
    assert first_attempt.response_metadata["stream_failure_stage"] == "first_event"
    assert Enum.sort(Map.keys(first_attempt.response_metadata["stream_timing"])) == ~w(connection first_event_ms headers_ms)

    # The retry is a new attempt with its own start, so its marks are its own, not the failed attempt's.
    assert second_attempt.status == "succeeded"
    assert Enum.sort(Map.keys(second_attempt.response_metadata["stream_timing"])) == ~w(connection first_event_ms headers_ms)
    assert second_attempt.response_metadata["stream_timing"]["first_event_ms"] <= second_attempt.latency_ms
  end

  test "a provider that sends headers and then nothing leaves a headers_ms and no first_event_ms" do
    release_ref = make_ref()
    upstream = start_upstream(FakeUpstream.timeout_after_sse_headers(notify: self(), release_ref: release_ref))
    setup = gateway_setup(upstream)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("silent after headers"), "stream" => true}

    assert {:ok, %{stream: stream}} = Gateway.execute(auth, @path, payload, RequestOptions.build(%{upstream_endpoint: @path, receive_timeout: 100}, @path, payload))

    stream_conn = Phoenix.ConnTest.build_conn() |> Plug.Conn.put_resp_content_type("text/event-stream") |> Plug.Conn.send_chunked(200)
    assert {:ok, _stream_conn} = stream.(stream_conn)
    assert_receive {:fake_upstream_timeout_barrier, :after_sse_headers, upstream_pid, ^release_ref}, 5_000
    send(upstream_pid, {:fake_upstream_release_timeout, release_ref})

    {_request, attempt} = settled_rows(setup)
    assert attempt.network_error_code == "stream_idle_timeout"
    timing = attempt.response_metadata["stream_timing"]
    assert Enum.sort(Map.keys(timing)) == ~w(connection headers_ms)
    assert timing["connection"] == "fresh"
  end

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_native(setup, mode, label) do
    conn = build_conn() |> auth(setup)
    conn = if mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @path, %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic #{label}"), "stream" => true})
  end

  defp settled_rows(setup) do
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    {request, attempt}
  end

  defp pause(ms) do
    receive do
    after
      ms -> :ok
    end
  end

  defp events(response_id) do
    [
      {"response.created", %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}},
      {"response.output_text.delta", %{"type" => "response.output_text.delta", "delta" => "synthetic delta"}},
      {"response.completed", %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}}
    ]
  end
end
