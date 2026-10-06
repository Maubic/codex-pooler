defmodule CodexPooler.Gateway.Runtime.Streaming.StreamTimingTest do
  # The timing of an upstream HTTP SSE attempt, as bounded integers in milliseconds from the attempt start: when the
  # response headers arrived, when the first SSE event arrived, when the first model output arrived. Two tiny `/v1`
  # requests took 9.6 s and 14.5 s although the provider sent its headers within a second and then held the stream;
  # no row could show that. Provenance of the shapes: synthetic Responses SSE events (the event names are the
  # documented Responses stream vocabulary); the clock is the real monotonic clock, offsets are produced with
  # `started` in the past and short sleeps.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Runtime.Streaming.StreamTiming
  alias CodexPooler.Gateway.Transports.Streaming.WebsocketBridgeStream
  alias CodexPooler.Gateway.Transports.UpstreamConnectionProbe

  @past_ms 2_000

  defp started, do: System.monotonic_time(:millisecond) - @past_ms

  defp async_response(opts \\ []) do
    response = %Req.Response{status: 200, body: %Req.Response.Async{pid: self(), ref: make_ref(), stream_fun: fn _ref, _message -> :unknown end, cancel_fun: fn _ref -> :ok end}}
    if connection = opts[:connection], do: UpstreamConnectionProbe.put_connection(response, connection), else: response
  end

  defp sse(type, payload \\ %{}), do: "event: #{type}\ndata: #{CodexPooler.JSON.encode!(Map.put(payload, "type", type))}\n\n"

  defp created, do: sse("response.created", %{"response" => %{"id" => "resp_timing_unit", "status" => "in_progress"}})
  defp item_added, do: sse("response.output_item.added", %{"output_index" => 0, "item" => %{"id" => "msg_timing_unit", "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}})
  defp delta, do: sse("response.output_text.delta", %{"delta" => "synthetic delta"})

  defp timed_state(opts \\ []) do
    response = async_response(opts) |> then(&StreamTiming.attach(&1, started()))
    StreamTiming.init_state(%{}, response)
  end

  defp fields(state), do: state |> StreamTiming.metadata() |> Map.fetch!(StreamTiming.metadata_key())

  describe "attach/2 and init_state/2" do
    test "stamp the headers mark and the connection class on an async upstream response" do
      response = async_response(connection: "reused") |> then(&StreamTiming.attach(&1, started()))
      state = StreamTiming.init_state(%{}, response)

      assert %{"headers_ms" => headers_ms, "connection" => "reused"} = fields = fields(state)
      assert is_integer(headers_ms) and headers_ms >= @past_ms and headers_ms < @past_ms + 1_000
      assert Enum.sort(Map.keys(fields)) == ~w(connection headers_ms)
    end

    test "leave a response whose connection could not be observed without a connection field" do
      assert %{"headers_ms" => _headers_ms} = fields = fields(timed_state())
      refute Map.has_key?(fields, "connection")
    end

    test "do nothing for a response that is not an upstream HTTP stream" do
      binary = %Req.Response{status: 200, body: "{}"}
      bridge = %Req.Response{status: 200, body: %WebsocketBridgeStream{ref: make_ref(), relay: self(), correlation_id: "synthetic", settle_timeout_ms: 1_000}}

      for response <- [binary, bridge] do
        assert StreamTiming.attach(response, started()) == response
        assert StreamTiming.init_state(%{relay: true}, response) == %{relay: true}
      end
    end

    test "init_state leaves a state alone when the response carries no timing" do
      assert StreamTiming.init_state(%{relay: true}, async_response()) == %{relay: true}
    end
  end

  describe "observe_chunk/2" do
    test "stamps the first event at the first complete block and the first output at the first model output" do
      state = timed_state()

      state = StreamTiming.observe_chunk(state, created())
      assert %{"first_event_ms" => first_event_ms} = fields = fields(state)
      refute Map.has_key?(fields, "first_visible_ms")
      assert first_event_ms >= @past_ms

      Process.sleep(20)
      state = StreamTiming.observe_chunk(state, item_added() <> delta())
      assert %{"first_event_ms" => ^first_event_ms, "first_visible_ms" => first_visible_ms, "headers_ms" => headers_ms} = fields(state)
      assert first_visible_ms - first_event_ms >= 15
      assert headers_ms <= first_event_ms
    end

    test "a lifecycle event, response.metadata and a terminal are not model output" do
      state = timed_state()
      metadata = sse("response.metadata", %{"metadata" => %{"safety" => "synthetic"}})
      failed = sse("response.failed", %{"response" => %{"id" => "resp_timing_failed", "error" => %{"code" => "server_error", "message" => "synthetic"}}})
      completed = sse("response.completed", %{"response" => %{"id" => "resp_timing_empty", "status" => "completed"}})

      state = Enum.reduce([created(), sse("response.in_progress"), sse("response.queued"), metadata, failed, completed], state, &StreamTiming.observe_chunk(&2, &1))

      assert %{"first_event_ms" => _first_event_ms} = fields = fields(state)
      refute Map.has_key?(fields, "first_visible_ms")
    end

    test "reads an event split across chunks once it is complete" do
      state = timed_state()
      block = item_added()
      {first, second} = String.split_at(block, div(byte_size(block), 2))

      state = StreamTiming.observe_chunk(state, first)
      refute Map.has_key?(fields(state), "first_event_ms")

      Process.sleep(20)
      state = StreamTiming.observe_chunk(state, second)
      assert %{"first_event_ms" => first_event_ms, "first_visible_ms" => first_visible_ms} = fields(state)
      assert first_visible_ms == first_event_ms
    end

    test "does no further work once both marks are stamped" do
      state = timed_state() |> then(&StreamTiming.observe_chunk(&1, item_added()))
      assert %{"first_visible_ms" => _first_visible_ms} = fields(state)

      assert StreamTiming.observe_chunk(state, delta()) == state
      assert StreamTiming.observe_chunk(state, "not even sse") == state
    end

    test "ignores non-SSE bodies and a state without timing" do
      assert StreamTiming.observe_chunk(%{relay: true}, created()) == %{relay: true}

      state = timed_state()
      state = StreamTiming.observe_chunk(state, ~s({"error":{"message":"synthetic"}}))
      assert Map.keys(fields(state)) == ["headers_ms"]
    end
  end

  describe "metadata/1" do
    test "is empty without timing and bounded to the fixed fields with it" do
      assert StreamTiming.metadata(%{}) == %{}
      assert StreamTiming.metadata(nil) == %{}
      assert StreamTiming.metadata(%{stream_timing: %{}}) == %{}

      state = timed_state(connection: "fresh") |> then(&StreamTiming.observe_chunk(&1, item_added()))
      assert Enum.sort(Map.keys(fields(state))) == ~w(connection first_event_ms first_visible_ms headers_ms)
      assert fields(state)["connection"] in StreamTiming.connections()
      assert StreamTiming.metadata_key() == "stream_timing"
    end

    test "drops a value outside the vocabulary or the integer bounds instead of persisting it" do
      state = %{stream_timing: %{started: started(), headers_ms: -5, first_event_ms: "soon", first_visible_ms: 12, connection: "Authorization: Bearer sk-secret"}}

      assert StreamTiming.metadata(state) == %{"stream_timing" => %{"first_visible_ms" => 12}}
    end
  end

  describe "failure carriers" do
    test "a first-event failure carries the timing of its attempt into its metadata" do
      state = timed_state(connection: "fresh") |> then(&StreamTiming.observe_chunk(&1, created()))
      failure = %{code: "server_error", event_type: "response.failed"}

      carried = StreamTiming.attach_to_failure(failure, state)
      assert carried.code == "server_error"
      assert StreamTiming.failure_metadata(carried) == StreamTiming.metadata(state)
    end

    test "a failure without timing carries nothing" do
      failure = %{code: "server_error", event_type: "response.failed"}

      assert StreamTiming.attach_to_failure(failure, %{}) == failure
      assert StreamTiming.failure_metadata(failure) == %{}
    end
  end
end
