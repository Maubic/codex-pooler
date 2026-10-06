defmodule CodexPoolerWeb.V1.ResponsesStreamTimingTest do
  # The two requests that motivated the timing fields were tiny `/v1` requests (9.6 s and 14.5 s, headers within a
  # second, then a held stream). A `/v1/responses` call always reaches the provider as an HTTP SSE request: relayed
  # when the client streams, collected into one JSON body when it does not. Both settle an attempt that records the
  # headers, first event and first model output marks. Provenance: the stream shapes are the documented Responses SSE
  # vocabulary with invented ids and texts; the hold is a real short pause injected by the fake provider.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, gateway_setup: 1, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @hold_ms 300
  @timing_keys ~w(connection first_event_ms first_visible_ms headers_ms)
  @moduletag capture_log: true

  for mode <- ["full", "lite"], stream? <- [true, false] do
    test "#{mode} /v1 stream #{stream?}: a body held after the headers is a small headers_ms and a large first_event_ms" do
      release_ref = make_ref()
      upstream = start_upstream(FakeUpstream.barrier_sse_stream(events("resp_v1_timing_hold"), barrier_after: 0, notify: self(), release_ref: release_ref))
      setup = gateway_setup(upstream)
      set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))

      task = Task.async(fn -> post_responses(setup, unquote(stream?)) end)
      assert_receive {:fake_upstream_chunk_barrier, 0, handler, ^release_ref}, 5_000
      pause(@hold_ms)
      send(handler, {:fake_upstream_release_chunk, release_ref})
      conn = Task.await(task, 5_000)

      assert conn.status == 200
      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert request.status == "succeeded"

      timing = attempt.response_metadata["stream_timing"]
      assert Enum.sort(Map.keys(timing)) == @timing_keys
      assert %{"headers_ms" => headers_ms, "first_event_ms" => first_event_ms, "first_visible_ms" => first_visible_ms, "connection" => "fresh"} = timing
      assert first_event_ms - headers_ms >= div(@hold_ms, 2)
      assert headers_ms < first_event_ms
      assert first_visible_ms >= first_event_ms
      assert attempt.latency_ms >= first_visible_ms
    end
  end

  test "/v1 stream: the first request to an upstream opens a connection and the next one reuses it" do
    upstream = start_upstream(FakeUpstream.sse_stream(events("resp_v1_timing_connection")))
    setup = gateway_setup(upstream)

    for stream? <- [true, false], do: assert(post_responses(setup, stream?).status == 200)

    attempts = Repo.all(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id, order_by: [asc: a.started_at]))
    assert Enum.map(attempts, & &1.response_metadata["stream_timing"]["connection"]) == ["fresh", "reused"]
  end

  defp post_responses(setup, stream?) do
    build_conn() |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic #{stream?}", "stream" => stream?})
  end

  defp pause(ms) do
    receive do
    after
      ms -> :ok
    end
  end

  defp events(response_id) do
    item = %{"id" => "msg_#{response_id}", "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}
    done_item = %{item | "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic answer", "annotations" => []}]}
    address = %{"item_id" => item["id"], "output_index" => 0, "content_index" => 0}

    [
      {"response.created", %{"type" => "response.created", "response" => response_body(response_id, "in_progress", [])}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => item}},
      {"response.output_text.delta", Map.merge(address, %{"type" => "response.output_text.delta", "delta" => "synthetic answer"})},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => done_item}},
      {"response.completed", %{"type" => "response.completed", "response" => response_body(response_id, "completed", [done_item])}}
    ]
  end

  defp response_body(response_id, status, output) do
    %{
      "id" => response_id,
      "object" => "response",
      "created_at" => 1_790_000_000,
      "model" => "provider-gpt-test-model",
      "status" => status,
      "output" => output,
      "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
    }
  end
end
