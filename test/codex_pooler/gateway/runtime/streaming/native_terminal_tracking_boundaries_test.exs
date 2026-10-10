defmodule CodexPooler.Gateway.Runtime.Streaming.NativeTerminalTrackingBoundariesTest do
  use ExUnit.Case, async: false
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Streaming.{DownstreamDeliveryEvidence, DownstreamStream, StreamUsageObserver}
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.SSEParser

  @endpoint "/backend-api/codex/responses"

  for size <- [1, 17] do
    @tag :terminal_tracking_red
    test "#{size}MiB fragmented completion retains observed and delivered terminal" do
      terminal = completion(unquote(size) * 1024 * 1024)
      {wire, state} = track(split(terminal, 65_536))
      assert wire == terminal
      assert DownstreamStream.terminal_outcome(state) == :completed
      assert DownstreamDeliveryEvidence.receipt(state)["terminal_class"] == "response.completed"
      assert DownstreamDeliveryEvidence.receipt(state)["outcome"] == "delivered"
    end
  end

  @tag :terminal_tracking_red
  test "a split data prefix retains a structural failed outcome after visible output" do
    delta = delta()
    terminal = "data: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "server_error"}}}) <> "\n\n"
    <<first::binary-size(2), rest::binary>> = terminal
    {wire, state} = track([delta, first, rest])
    assert wire == delta <> terminal
    assert {:failed, %{code: "server_error"}} = DownstreamStream.terminal_outcome(state)
    assert DownstreamDeliveryEvidence.receipt(state)["terminal_class"] == "response.failed"
  end

  @tag :terminal_tracking_red
  test "coalesced visible delta and failure agree on source and delivery outcome" do
    terminal = "event: response.failed\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"status" => "failed", "error" => %{"code" => "server_error"}}}) <> "\n\n"
    {wire, state} = track([delta() <> terminal])
    assert wire == delta() <> terminal
    assert {:failed, %{code: "server_error"}} = DownstreamStream.terminal_outcome(state)
    assert DownstreamDeliveryEvidence.receipt(state)["terminal_class"] == "response.failed"
  end

  for {kind, limit, prefix} <- [
        {:ordinary, 8_388_608, "event: response.output_item.added\ndata: "},
        {:candidate, 67_108_864, "event: response.completed\ndata: "}
      ] do
    @tag slow: "exercises exact8MiB/64MiB parser bounds with whole and fragmented blocks"
    test "#{kind} exact observation boundary is independent of chunking and delimiter fragmentation" do
      limit = unquote(limit)
      prefix = unquote(prefix)

      for extra <- [-1, 0, 1] do
        block = prefix <> String.duplicate("x", limit - byte_size(prefix) + extra)

        for chunks <- [[block <> "\r\n\r\n"], [block, "\r", "\n", "\r", "\n"]] do
          {parts, state} = observe_parser(chunks)
          assert state.buffer == ""
          assert state.carry == ""
          refute state.discarding?
          assert state.overflow_count == max(extra, 0)

          if extra == 1 do
            assert IO.iodata_length(Enum.map(parts, &part_wire/1)) == byte_size(block) + 4
          else
            assert [{:block, ^block, separator}] = parts
            assert separator in ["\r\n\r\n", "\r\n\r"]
          end

          assert Enum.count(parts, &match?({:block, _, _}, &1)) == 1 - max(extra, 0)
          assert Enum.count(parts, &match?({:overflow, _, _}, &1)) == max(extra, 0)
          assert :binary.referenced_byte_size(state.buffer) == 0
          assert :binary.referenced_byte_size(state.carry) == 0
        end
      end
    end
  end

  test "discarded suffix cannot invent a terminal and a later delimited terminal resynchronizes" do
    prefix = "event: response.output_item.added\ndata: " <> String.duplicate("x", 8_388_608)
    fake_suffix = "data: {\"type\":\"response.completed\"}"
    {parts, state} = observe_parser([prefix, fake_suffix, "\r", "\n", "\r", "\n", completion(0)])
    assert state.overflow_count == 1
    refute state.discarding?
    assert [{:block, valid, "\n\n"}] = Enum.filter(parts, &match?({:block, _, _}, &1))
    assert valid <> "\n\n" == completion(0)
    {wire, tracked} = track([prefix, fake_suffix, "\r\n\r\n", completion(0)])
    assert wire == prefix <> fake_suffix <> "\r\n\r\n" <> completion(0)
    assert DownstreamStream.terminal_outcome(tracked) == :completed
    assert tracked.codex_responses_sse_block_state.overflow_count == 1
  end

  test "overflow EOF remains unknown and permanently poisons tool recovery" do
    opts = RequestOptions.build(%{transport: "http_sse"}, @endpoint, %{})
    state = :relay |> DownstreamStream.initial_state(opts) |> DownstreamStream.enable_native_http_tool_observation()
    prefix = "event: response.output_item.added\ndata: " <> String.duplicate("x", 8_388_609)
    {^prefix, state} = DownstreamStream.normalize_data(prefix, @endpoint, opts, state)
    {"", state} = DownstreamStream.flush_eof_data(@endpoint, opts, state)
    assert DownstreamStream.terminal_outcome(state) == nil
    assert get_in(DownstreamStream.native_http_tool_metadata(state), ["native_http_partial_tool", "poisoned"])
    assert %{buffer: "", carry: "", discarding?: true, overflow_count: 1} = state.codex_responses_sse_block_state
    {_, state} = DownstreamStream.normalize_data("\n\n" <> completion(0), @endpoint, opts, state)
    assert DownstreamStream.terminal_outcome(state) == :completed
    assert get_in(DownstreamStream.native_http_tool_metadata(state), ["native_http_partial_tool", "poisoned"])
  end

  test "unknown labels and data-only terminal JSON keep a key-order-independent candidate budget" do
    json = "{\"padding\":\"" <> String.duplicate("x", 9 * 1_048_576) <> "\",\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}"

    for prefix <- ["data: ", "event: synthetic.unknown\ndata: "] do
      wire = prefix <> json <> "\n\n"

      for chunks <- [[wire], split(wire, 65_536)] do
        {output, state} = track(chunks)
        assert output == wire
        assert state.codex_responses_sse_block_state.overflow_count == 0
        assert DownstreamStream.terminal_outcome(state) == if(prefix == "data: ", do: :completed, else: nil)
      end
    end
  end

  test "malformed and truncated labels never grant terminal authority at EOF" do
    opts = RequestOptions.build(%{transport: "http_sse"}, @endpoint, %{})

    for bytes <- ["event: response.completed\ndata: {", "event: response.failed\ndata: {}", "event: response.completed\n", "event: response.completed\ndata: invalid\n\n"] do
      {_, state} = track([bytes])
      {_, state} = DownstreamStream.flush_eof_data(@endpoint, opts, state)
      assert DownstreamStream.terminal_outcome(state) == nil
    end
  end

  test "small residue copies its bytes instead of retaining a large complete parent chunk" do
    wire = completion(1_048_576) <> "data: {"
    {_, state} = observe_parser([wire])
    assert state.buffer == "data: {"
    assert :binary.referenced_byte_size(state.buffer) == byte_size(state.buffer)
  end

  test "overflow diagnostic counter saturates and stores only primitive bounds" do
    state = %{SSEParser.new_observation_state() | overflow_count: 65_535}
    {_, state} = SSEParser.observe_blocks(state, "data: " <> String.duplicate("x", 8_388_609))
    assert SSEParser.observation_metadata(state) == %{overflow_count: 65_535, last_limit_bytes: 8_388_608, discarding: true, residue_bytes: 0, discard_carry_bytes: 0}
  end

  test "cumulative complete events exceed20MiB without consuming a per-event budget" do
    event = "event: response.output_text.delta\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.output_text.delta", "delta" => String.duplicate("x", 1_048_576)}) <> "\n\n"
    chunks = List.duplicate(event, 21) ++ [completion(0)]
    {wire, state} = track(chunks)
    assert byte_size(wire) > 20 * 1_048_576
    assert state.codex_responses_sse_block_state.overflow_count == 0
    assert DownstreamStream.terminal_outcome(state) == :completed
  end

  test "only a large terminal supplies source usage after diagnostic prefixes are absent" do
    chunks = split(completion(17 * 1_048_576), 65_536)
    observer = Enum.reduce(chunks, StreamUsageObserver.new(), &StreamUsageObserver.observe(&2, &1))
    assert %{status: "usage_known", input_tokens: 4, output_tokens: 3, total_tokens: 7} = StreamUsageObserver.result(observer)
    assert StreamUsageObserver.candidate_bytes(observer) <= 16_384
    assert byte_size(:erlang.term_to_binary(observer)) < 65_536
  end

  for extra <- [-1, 0, 1] do
    @tag slow: "decodes exact64MiB terminal and overflow with whole and fragmented bytes"
    test "valid terminal at64MiB plus#{extra} has equivalent source and delivery authority" do
      overhead = byte_size(completion(0)) - 2
      terminal = completion(67_108_864 - overhead + unquote(extra))

      for chunks <- [[terminal], split(terminal, 65_536)] do
        {wire, state} = track(chunks)
        assert :crypto.hash(:sha256, wire) == :crypto.hash(:sha256, terminal)
        assert state.codex_responses_sse_block_state.overflow_count == max(unquote(extra), 0)
        assert DownstreamStream.terminal_outcome(state) == if(unquote(extra) <= 0, do: :completed, else: nil)
        assert DownstreamDeliveryEvidence.receipt(state)["terminal_class"] == if(unquote(extra) <= 0, do: "response.completed", else: "none")
        assert DownstreamDeliveryEvidence.fetch(state).sse.buffer == ""
      end
    end
  end

  test "CRLF and every byte boundary keep mismatched labels unknown and valid failure recognized" do
    failure = "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\"}}}\n\n"

    for label <- ["", "event: response.completed\n"] do
      wire = label <> failure
      expected = if label == "", do: :failed, else: nil

      for bytes <- [wire, String.replace(wire, "\n", "\r\n")], at <- 1..(byte_size(bytes) - 1) do
        <<first::binary-size(^at), rest::binary>> = bytes
        {output, state} = track([first, rest])
        assert output == wire
        outcome = DownstreamStream.terminal_outcome(state)
        assert if(match?({:failed, _}, outcome), do: :failed, else: outcome) == expected
      end
    end
  end

  test "observed source completion is separate from a failed downstream write" do
    opts = RequestOptions.build(%{transport: "http_sse"}, @endpoint, %{})
    initial = DownstreamStream.initial_state(:relay, opts)
    {wire, state} = DownstreamStream.normalize_data(completion(0), @endpoint, opts, initial)
    assert DownstreamStream.terminal_outcome(state) == :completed
    failed = DownstreamDeliveryEvidence.record_write_failure(state)
    assert DownstreamDeliveryEvidence.receipt(failed)["outcome"] == "aborted"
    assert DownstreamDeliveryEvidence.receipt(failed)["terminal_class"] == "none"
    delivered = DownstreamDeliveryEvidence.record_write(state, wire)
    assert DownstreamDeliveryEvidence.receipt(delivered)["outcome"] == "delivered"
    assert DownstreamDeliveryEvidence.receipt(delivered)["terminal_class"] == "response.completed"
  end

  test "public receipt retains legacy8MiB bounded parser and has no native diagnostics" do
    opts = RequestOptions.build(%{public_openai_responses_stream: true}, "/v1/responses", %{"stream" => true})
    state = DownstreamStream.initial_state(:relay, opts)
    state = DownstreamDeliveryEvidence.record_write(state, "event: response.completed\ndata: " <> String.duplicate("x", 8_388_609))
    assert DownstreamDeliveryEvidence.fetch(state).sse == %{buffer: "", skip_leading_lf?: false}
    assert DownstreamStream.native_sse_observation_metadata(state) == %{}
    assert DownstreamDeliveryEvidence.receipt(state)["terminal_class"] == "none"
  end

  test "EOF observes valid terminal with LF CRLF or standalone CR source lines" do
    opts = RequestOptions.build(%{transport: "http_sse"}, @endpoint, %{})
    canonical = completion(0)

    for ending <- ["\n", "\r\n", "\r"] do
      bytes = canonical |> String.trim_trailing("\n") |> String.replace("\n", ending)

      for chunks <- [[bytes], split(bytes, 7)] do
        {wire, state} = track(chunks)
        {tail, state} = DownstreamStream.flush_eof_data(@endpoint, opts, state)
        assert wire <> tail == canonical
        assert DownstreamStream.terminal_outcome(state) == :completed
      end
    end
  end

  defp observe_parser(chunks) do
    {parts, state} = Enum.map_reduce(chunks, SSEParser.new_observation_state(), &SSEParser.observe_blocks(&2, &1))
    {List.flatten(parts), state}
  end

  defp part_wire({:block, raw, separator}), do: [raw, separator]
  defp part_wire({:overflow, raw, _}), do: raw
  defp part_wire({:passthrough, raw}), do: raw

  defp track(chunks) do
    opts = RequestOptions.build(%{transport: "http_sse"}, @endpoint, %{})
    state = :relay |> DownstreamStream.initial_state(opts) |> Map.put(:visible_output_marked?, true)

    {parts, state} =
      Enum.map_reduce(chunks, state, fn chunk, state ->
        {wire, state} = DownstreamStream.normalize_data(chunk, @endpoint, opts, state)
        state = DownstreamDeliveryEvidence.record_write(state, wire)

        assert_observation_bounds(state)

        {wire, state}
      end)

    {IO.iodata_to_binary(parts), state}
  end

  defp assert_observation_bounds(state) do
    for parser <- [state.codex_responses_sse_block_state, DownstreamDeliveryEvidence.fetch(state).sse] do
      limit = if parser.event_kind == :ordinary, do: 8_388_608, else: 67_108_864
      assert byte_size(parser.buffer) <= limit
      assert :binary.referenced_byte_size(parser.buffer) <= limit
      assert byte_size(parser.carry) <= 2
      assert :binary.referenced_byte_size(parser.carry) <= 2
    end
  end

  defp split(binary, size) when byte_size(binary) <= size, do: [binary]

  defp split(binary, size) do
    <<part::binary-size(^size), rest::binary>> = binary
    [part | split(rest, size)]
  end

  defp delta, do: "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\n"
  defp completion(size), do: "event: response.completed\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"status" => "completed", "output" => [%{"type" => "message", "content" => String.duplicate("x", size)}], "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}) <> "\n\n"
end
