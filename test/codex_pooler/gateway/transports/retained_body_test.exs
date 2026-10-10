defmodule CodexPooler.Gateway.Transports.Streaming.RetainedBodyTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.RetainedBody
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.ReceiveState

  @truncated_event [:codex_pooler, :gateway, :stream_buffer, :truncated]

  @property_seed {20_260_730, 91, 37}

  test "matches the bounded suffix reference across seeded binary and iodata appends" do
    rng = :rand.seed_s(:exsss, @property_seed)

    Enum.reduce(1..120, {rng, RetainedBody.empty(), ""}, fn _iteration, {rng, retained, reference} ->
      {data, rng} = random_iodata(rng)
      retained = RetainedBody.append(retained, data)
      reference = reference_append(reference, data)

      assert RetainedBody.read(retained) == reference
      {rng, retained, reference}
    end)
  end

  test "owns the bounded suffix instead of retaining an oversized parent" do
    parent = :binary.copy(<<7>>, RetainedBody.max_bytes() * 128)
    retained = RetainedBody.append(RetainedBody.empty(), parent)

    retained = RetainedBody.read(retained)
    assert byte_size(retained) == RetainedBody.max_bytes()
    assert :binary.referenced_byte_size(retained) == byte_size(retained)
  end

  for {scenario, chunk_sizes, crossing_bytes} <- [
        {"empty", [0], nil},
        {"below the limit", [65_535], nil},
        {"exactly the limit", [65_536], nil},
        {"empty after exactly the limit", [65_536, 0], nil},
        {"one oversized chunk", [65_537], 65_537},
        {"crossing from below the limit", [65_535, 2], 65_537},
        {"crossing from exactly the limit", [65_536, 1], 65_537},
        {"crossing from a multichunk exact limit", [32_768, 32_768, 1], 65_537},
        {"512 aligned 4 KiB chunks", List.duplicate(4096, 512), 69_632},
        {"repeated small-chunk compaction", [65_535, 2, 32_768, 32_768, 1, 32_768, 32_768, 1], 65_537},
        {"repeated oversized chunks", [65_537, 65_537, 1, 65_537], 65_537},
        {"700 nonaligned chunks", List.duplicate(3000, 700), 66_000}
      ] do
    test "emits one first-crossing observation for #{scenario}" do
      events = capture_stream_buffer_telemetry()
      opts = [endpoint: "/v1/responses", transport: :http, route_class: :responses]
      crossing_bytes = unquote(crossing_bytes)

      Enum.reduce(Enum.with_index(unquote(chunk_sizes)), {RetainedBody.empty(), "", 0}, fn {size, index}, {body, reference, total_bytes} ->
        data = :binary.copy(<<rem(index, 256)>>, size)
        body = RetainedBody.append(body, data, opts)
        reference = reference_append(reference, data)
        retained = RetainedBody.read(body)
        total_bytes = total_bytes + size

        assert retained == reference
        assert :binary.referenced_byte_size(retained) == byte_size(retained)

        if total_bytes > RetainedBody.max_bytes() do
          assert events.() == [
                   {@truncated_event, %{bytes: crossing_bytes, count: 1, max_bytes: 65_536}, %{buffer: "retained_body", endpoint: "/v1/responses", transport: "http", route_class: "responses"}}
                 ]
        else
          assert events.() == []
        end

        {body, reference, total_bytes}
      end)
    end
  end

  test "empty iodata does not truncate and each new body has its own first crossing" do
    events = capture_stream_buffer_telemetry()

    for expected_count <- 1..2 do
      body = RetainedBody.append(RetainedBody.empty(), :binary.copy(<<1>>, RetainedBody.max_bytes()))
      body = RetainedBody.append(body, [[], [""]])
      assert length(events.()) == expected_count - 1

      body = RetainedBody.append(body, [<<2>>])
      assert byte_size(RetainedBody.read(body)) == RetainedBody.max_bytes()
      assert length(events.()) == expected_count
    end

    assert Enum.all?(events.(), fn {event, measurements, metadata} ->
             event == @truncated_event and measurements == %{bytes: 65_537, count: 1, max_bytes: 65_536} and
               metadata == %{buffer: "retained_body", endpoint: "unknown", route_class: "unknown", transport: "unknown"}
           end)
  end

  test "websocket receive-state initializes a fresh retained body" do
    events = capture_stream_buffer_telemetry()
    initial = %ReceiveState{}
    assert initial.body == RetainedBody.empty()
    assert RetainedBody.read(initial.body) == ""

    body = RetainedBody.append(initial.body, :binary.copy(<<1>>, RetainedBody.max_bytes()))
    assert events.() == []
    body = RetainedBody.append(body, <<2>>, transport: :websocket, route_class: :proxy_websocket)
    assert byte_size(RetainedBody.read(body)) == RetainedBody.max_bytes()

    assert [{@truncated_event, %{bytes: 65_537, count: 1, max_bytes: 65_536}, %{transport: "websocket", route_class: "proxy_websocket"}}] = events.()
  end

  test "telemetry capture excludes other emitter processes" do
    events = capture_stream_buffer_telemetry()
    task = Task.async(fn -> RetainedBody.append(RetainedBody.empty(), :binary.copy(<<1>>, 65_537)) end)
    assert byte_size(task |> Task.await() |> RetainedBody.read()) == RetainedBody.max_bytes()
    assert events.() == []

    RetainedBody.append(RetainedBody.empty(), :binary.copy(<<1>>, 65_537))
    assert length(events.()) == 1
  end

  def handle_buffer_event(event, measurements, metadata, {owner, key}) do
    if self() == owner do
      Process.put(key, [{event, measurements, metadata} | Process.get(key, [])])
    end
  end

  defp capture_stream_buffer_telemetry do
    handler_id = {__MODULE__, self(), System.unique_integer([:positive])}
    key = {__MODULE__, make_ref()}
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok = :telemetry.attach(handler_id, @truncated_event, &__MODULE__.handle_buffer_event/4, {self(), key})

    # The installed telemetry library invokes handlers synchronously in the emitter.
    # Once append returns, this test process's captured observations are complete.
    fn -> Process.get(key, []) |> Enum.reverse() end
  end

  defp random_iodata(rng) do
    {size, rng} = uniform(rng, RetainedBody.max_bytes() * 3)
    {byte, rng} = uniform(rng, 256)
    binary = :binary.copy(<<byte - 1>>, size - 1)

    case rem(size, 3) do
      0 ->
        {binary, rng}

      1 ->
        {[
           binary_part(binary, 0, div(byte_size(binary), 2)),
           binary_part(
             binary,
             div(byte_size(binary), 2),
             byte_size(binary) - div(byte_size(binary), 2)
           )
         ], rng}

      2 ->
        {[[], [binary]], rng}
    end
  end

  defp reference_append(body, data) do
    appended = IO.iodata_to_binary([body, data])
    max_bytes = RetainedBody.max_bytes()

    if byte_size(appended) <= max_bytes do
      appended
    else
      binary_part(appended, byte_size(appended) - max_bytes, max_bytes)
    end
  end

  defp uniform(rng, max) do
    {value, rng} = :rand.uniform_s(max, rng)
    {value, rng}
  end
end
