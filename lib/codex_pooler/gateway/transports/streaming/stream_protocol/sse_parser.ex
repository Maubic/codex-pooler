defmodule CodexPooler.Gateway.Transports.Streaming.StreamProtocol.SSEParser do
  @moduledoc false

  # Upstream Responses streams carry single non-terminal SSE events well past
  # 64 KiB (reasoning items with encrypted content scale with request context),
  # so the ordinary bound must stay comfortably above real provider event sizes.
  @max_incomplete_sse_block_bytes 8_388_608
  @max_incomplete_terminal_sse_block_bytes 64 * 1024 * 1024
  @ordinary_observation_events ~w(response.created response.in_progress response.output_item.added response.output_item.done response.content_part.added response.content_part.done response.output_text.delta response.output_text.done response.function_call_arguments.delta response.function_call_arguments.done response.custom_tool_call_input.delta response.custom_tool_call_input.done response.reasoning_summary_text.delta response.reasoning_summary_text.done codex.rate_limits)

  @type block_state :: %{
          required(:buffer) => binary(),
          required(:skip_leading_lf?) => boolean()
        }

  @spec max_incomplete_sse_block_bytes() :: pos_integer()
  def max_incomplete_sse_block_bytes, do: @max_incomplete_sse_block_bytes

  @spec oversized_incomplete_sse_block?(binary()) :: boolean()
  def oversized_incomplete_sse_block?(buffer) when is_binary(buffer),
    do: byte_size(buffer) > @max_incomplete_sse_block_bytes

  @spec max_incomplete_terminal_sse_block_bytes() :: pos_integer()
  def max_incomplete_terminal_sse_block_bytes,
    do: @max_incomplete_terminal_sse_block_bytes

  @spec oversized_incomplete_terminal_sse_block?(binary()) :: boolean()
  def oversized_incomplete_terminal_sse_block?(buffer) when is_binary(buffer),
    do: byte_size(buffer) > @max_incomplete_terminal_sse_block_bytes

  @spec new_block_state() :: block_state()
  def new_block_state, do: %{buffer: "", skip_leading_lf?: false}

  @type observation_state :: %{
          buffer: binary(),
          preamble_policy?: boolean(),
          skip_leading_lf?: boolean(),
          skip_lf_passthrough?: boolean(),
          discarding?: boolean(),
          carry: binary(),
          event_kind: :ordinary | :candidate | :preamble | nil,
          overflow_count: non_neg_integer(),
          last_limit_bytes: pos_integer() | nil
        }
  @type observed_part :: {:block, binary(), binary()} | {:passthrough, iodata()} | {:overflow, iodata(), pos_integer()} | {:unparsed, iodata()} | {:preamble_overflow, iodata(), pos_integer()} | {:preamble_unparsed, iodata()}

  # Native observation is opt-in. These are observation budgets, not wire
  # rejection limits. The overflow counter saturates at 65535; discard carry
  # holds at most one CRLF so suffixes cannot become invented new events.
  @spec new_observation_state() :: observation_state()
  @spec new_observation_state(keyword()) :: observation_state()
  def new_observation_state(opts \\ []) do
    %{preamble_policy?: Keyword.get(opts, :preamble_policy?, false), buffer: "", skip_leading_lf?: false, skip_lf_passthrough?: false, discarding?: false, carry: "", event_kind: nil, overflow_count: 0, last_limit_bytes: nil}
  end

  @spec observe_blocks(observation_state(), binary()) :: {[observed_part()], observation_state()}
  def observe_blocks(state, data) when is_binary(data) do
    {parts, state} = observe_native(state, data, [])
    {Enum.reverse(parts), state}
  end

  @spec finish_observation(observation_state()) :: {[observed_part()], observation_state()}
  def finish_observation(%{discarding?: true} = state), do: {[], state}
  def finish_observation(%{buffer: "", carry: ""} = state), do: {[], state}

  def finish_observation(state) do
    buffer = state.buffer
    canonical = canonicalize_line_endings(buffer)
    payload = sse_field(canonical, "data") || canonical
    separator = if match?({:ok, %{}}, CodexPooler.JSON.decode(payload)), do: eof_separator(state.carry), else: ""
    part = eof_observation_part(state, buffer, separator)
    {[part], reset_observation(state, false)}
  end

  defp eof_observation_part(%{preamble_policy?: true} = state, buffer, "") do
    if state.event_kind == :preamble or sse_field(buffer, "event") in ["response.created", "response.in_progress", "response.metadata"],
      do: {:preamble_unparsed, buffer <> state.carry},
      else: {:unparsed, buffer <> state.carry}
  end

  defp eof_observation_part(state, buffer, ""), do: {:unparsed, buffer <> state.carry}
  defp eof_observation_part(_state, buffer, separator), do: {:block, buffer, separator}

  @spec observation_metadata(observation_state()) :: map()
  def observation_metadata(state) do
    %{overflow_count: state.overflow_count, last_limit_bytes: state.last_limit_bytes, discarding: state.discarding?, residue_bytes: byte_size(state.buffer), discard_carry_bytes: byte_size(state.carry)}
  end

  defp observe_native(state, "", parts), do: {parts, state}

  defp observe_native(%{skip_leading_lf?: true} = state, <<"\n", rest::binary>>, parts) do
    parts = if state.skip_lf_passthrough?, do: [{:passthrough, "\n"} | parts], else: parts
    observe_native(%{state | skip_leading_lf?: false, skip_lf_passthrough?: false}, rest, parts)
  end

  defp observe_native(%{skip_leading_lf?: true} = state, data, parts),
    do: observe_native(%{state | skip_leading_lf?: false}, data, parts)

  defp observe_native(%{discarding?: true} = state, data, parts) do
    combined = state.carry <> data

    case observation_delimiter(combined, 0) do
      nil ->
        {[{:passthrough, data} | parts], %{state | carry: observation_carry(combined)}}

      {_start, after_separator, skip?} ->
        consumed = max(after_separator - byte_size(state.carry), 0)
        <<discarded::binary-size(^consumed), rest::binary>> = data
        observe_native(%{reset_observation(state, skip?) | skip_lf_passthrough?: skip?}, rest, [{:passthrough, discarded} | parts])
    end
  end

  defp observe_native(state, data, parts) do
    combined = if state.carry == "", do: state.buffer <> data, else: state.buffer <> state.carry <> data
    delimiter = appended_observation_delimiter(state, data, combined)

    case delimiter do
      nil ->
        retain_observation_prefix(state, combined, parts)

      {start, after_separator, skip?} ->
        block = binary_part(combined, 0, start)
        separator = binary_part(combined, start, after_separator - start)
        rest = binary_part(combined, after_separator, byte_size(combined) - after_separator)
        limit = observation_limit(state.event_kind || observation_kind(state, block))

        {part, state} =
          if byte_size(block) > limit,
            do: {overflow_part(state.event_kind || observation_kind(state, block), [block, separator], limit), overflowed(state, limit)},
            else: {{:block, block, separator}, state}

        next = %{reset_observation(state, skip?) | skip_lf_passthrough?: skip? and raw_overflow_part?(part)}
        observe_native(next, rest, [part | parts])
    end
  end

  defp appended_observation_delimiter(state, data, combined) do
    if state.carry == "" and appendable_without_scan?(state, data), do: nil, else: observation_delimiter(combined, 0)
  end

  defp retain_observation_prefix(state, combined, parts) do
    kind = state.event_kind || observation_kind(state, combined)
    limit = observation_limit(kind)
    carry = observation_carry(combined)
    content_size = byte_size(combined) - byte_size(carry)

    if content_size > limit do
      next = %{overflowed(state, limit) | buffer: "", discarding?: true, carry: observation_carry(combined), event_kind: nil}
      {[overflow_part(kind, combined, limit) | parts], next}
    else
      content = if carry == "", do: combined, else: binary_part(combined, 0, content_size)
      # Appended binaries can reserve a parent larger than their visible prefix.
      # Detach only when that retained parent would exceed this observer's bound.
      buffer = if state.buffer == "" or :binary.referenced_byte_size(content) > limit, do: :binary.copy(content), else: content
      {parts, %{state | buffer: buffer, carry: carry, event_kind: kind}}
    end
  end

  defp raw_overflow_part?({kind, _, _}) when kind in [:overflow, :preamble_overflow], do: true
  defp raw_overflow_part?(_part), do: false

  # A known label is potential preamble for local resource admission only;
  # it proves neither JSON validity/type agreement nor a provider terminal.
  defp overflow_part(:preamble, raw, limit), do: {:preamble_overflow, raw, limit}
  defp overflow_part(_kind, raw, limit), do: {:overflow, raw, limit}

  defp reset_observation(state, skip?),
    do: %{state | buffer: "", skip_leading_lf?: skip?, skip_lf_passthrough?: false, discarding?: false, carry: "", event_kind: nil}

  defp overflowed(state, limit),
    do: %{state | overflow_count: min(state.overflow_count + 1, 65_535), last_limit_bytes: limit}

  defp observation_limit(:preamble), do: @max_incomplete_sse_block_bytes
  defp observation_limit(:ordinary), do: @max_incomplete_sse_block_bytes
  defp observation_limit(_potential_terminal), do: @max_incomplete_terminal_sse_block_bytes

  defp observation_kind(%{preamble_policy?: true}, buffer) do
    if potential_preamble_label?(buffer), do: :preamble, else: observation_kind(buffer)
  end

  defp observation_kind(_state, buffer), do: observation_kind(buffer)

  defp potential_preamble_label?(buffer) do
    case :binary.match(buffer, ["\r", "\n"]) do
      :nomatch -> false
      {index, _} -> sse_field(binary_part(buffer, 0, index), "event") in ["response.created", "response.in_progress", "response.metadata"]
    end
  end

  # Explicit nonterminal labels retain the ordinary budget. A data-only frame,
  # direct JSON or a label not yet complete remains a potential terminal: JSON
  # key order must not decide whether a 17MiB terminal can be observed.
  defp observation_kind(<<"data:", rest::binary>>) do
    case String.trim_leading(rest) do
      "" -> nil
      <<"{", _::binary>> -> :candidate
      _non_json_object -> :ordinary
    end
  end

  defp observation_kind(<<"{", _::binary>>), do: :candidate
  defp observation_kind(<<":", _::binary>>), do: :candidate

  defp observation_kind(buffer) do
    case :binary.match(buffer, ["\r", "\n"]) do
      :nomatch ->
        nil

      {index, _} ->
        line = binary_part(buffer, 0, index)

        case sse_field(line, "event") do
          label when label in ["response.completed", "response.done", "response.failed", "response.incomplete", "error"] -> :candidate
          label when label in @ordinary_observation_events -> :ordinary
          _unknown_or_data -> :candidate
        end
    end
  end

  defp observation_delimiter(data, offset) when offset >= byte_size(data), do: nil

  defp observation_delimiter(data, offset) do
    case :binary.match(data, ["\r", "\n"], scope: {offset, byte_size(data) - offset}) do
      :nomatch ->
        nil

      {start, _} ->
        first_length = line_ending_length(data, start)
        second = start + first_length

        case line_ending_length(data, second) do
          0 -> observation_delimiter(data, second)
          length -> {start, second + length, length == 1 and :binary.at(data, second) == ?\r and second + length == byte_size(data)}
        end
    end
  end

  defp eof_separator(""), do: "\n\n"
  defp eof_separator("\r"), do: "\r\n\n"
  defp eof_separator("\r\n"), do: "\r\n\r\n"
  defp eof_separator("\n"), do: "\n\n"

  defp observation_carry(data) do
    cond do
      String.ends_with?(data, "\r\n") -> "\r\n"
      String.ends_with?(data, "\r") -> "\r"
      String.ends_with?(data, "\n") -> "\n"
      true -> ""
    end
  end

  @doc """
  The `data:` field lines of one SSE event carrying a websocket text frame,
  without the terminating blank line. A frame whose text spans several lines,
  such as a pretty-printed provider object, needs one `data:` line per text
  line: otherwise every line after the first falls outside the event and the
  event decodes to nothing (findings#254 rows 254-60 and 254-53). A
  single-line frame keeps its exact bytes.
  """
  @spec data_lines(binary()) :: iodata()
  def data_lines(text) when is_binary(text) do
    if String.contains?(text, ["\n", "\r"]) do
      text
      |> String.split(["\r\n", "\r", "\n"])
      |> Enum.map_intersperse("\n", &["data: ", &1])
    else
      ["data: ", text]
    end
  end

  # A trailing standalone CR completes its line immediately. If that CR is the
  # last byte in a chunk, the next chunk may start with its optional LF
  # continuation; retaining that one bit of state prevents the LF from being
  # counted as another line ending.
  @spec complete_sse_blocks(block_state(), binary(), keyword()) :: {[binary()], block_state()}
  def complete_sse_blocks(
        %{buffer: buffer, skip_leading_lf?: skip_leading_lf?} = state,
        data,
        opts
      )
      when is_binary(buffer) and is_boolean(skip_leading_lf?) and is_binary(data) do
    bounded? = Keyword.fetch!(opts, :bounded?)

    if appendable_without_scan?(state, data) do
      next_state = %{state | buffer: buffer <> data}
      {[], maybe_bound_incomplete_sse_state(next_state, bounded?)}
    else
      {data, pending_skip_leading_lf?} =
        consume_optional_leading_lf(data, skip_leading_lf?)

      {blocks, residue, trailing_skip_leading_lf?} = split_complete_blocks(buffer <> data)

      next_state = %{
        buffer: residue,
        skip_leading_lf?: pending_skip_leading_lf? or trailing_skip_leading_lf?
      }

      {blocks, maybe_bound_incomplete_sse_state(next_state, bounded?)}
    end
  end

  defp appendable_without_scan?(%{buffer: ""}, _data), do: false
  defp appendable_without_scan?(%{skip_leading_lf?: true}, _data), do: false

  defp appendable_without_scan?(%{buffer: buffer}, data) do
    not String.ends_with?(buffer, "\r") and
      not (String.ends_with?(buffer, "\n") and String.starts_with?(data, "\n")) and
      not String.contains?(data, ["\r", "\n\n"])
  end

  defp consume_optional_leading_lf("", true), do: {"", true}
  defp consume_optional_leading_lf(<<"\n", rest::binary>>, true), do: {rest, false}
  defp consume_optional_leading_lf(data, true), do: {data, false}
  defp consume_optional_leading_lf(data, false), do: {data, false}

  @spec complete_sse_blocks(binary(), keyword()) :: {[binary()], binary()}
  def complete_sse_blocks(data, opts) when is_binary(data) do
    {blocks, state} = complete_sse_blocks(new_block_state(), data, opts)
    {blocks, state.buffer}
  end

  defp split_complete_blocks(data) do
    {blocks, residue_start, skip_leading_lf?} =
      scan_complete_blocks(data, 0, 0, [])

    residue =
      data
      |> binary_part(residue_start, byte_size(data) - residue_start)
      |> :binary.copy()

    {Enum.reverse(blocks), residue, skip_leading_lf?}
  end

  defp scan_complete_blocks(data, block_start, scan_index, blocks)
       when scan_index >= byte_size(data),
       do: {blocks, block_start, false}

  defp scan_complete_blocks(data, block_start, scan_index, blocks) do
    case :binary.match(data, ["\r", "\n"], scope: {scan_index, byte_size(data) - scan_index}) do
      :nomatch ->
        {blocks, block_start, false}

      {ending_index, _length} ->
        first_ending_length = line_ending_length(data, ending_index)
        scan_after_first_ending(data, block_start, ending_index, first_ending_length, blocks)
    end
  end

  defp scan_after_first_ending(data, block_start, scan_index, first_ending_length, blocks) do
    second_ending_index = scan_index + first_ending_length
    second_ending_length = line_ending_length(data, second_ending_index)

    if second_ending_length == 0 do
      scan_complete_blocks(data, block_start, second_ending_index, blocks)
    else
      block = binary_part(data, block_start, scan_index - block_start)
      blocks = if block == "", do: blocks, else: [canonicalize_line_endings(block) | blocks]
      next_index = second_ending_index + second_ending_length

      if next_index == byte_size(data) do
        trailing_cr? =
          second_ending_length == 1 and :binary.at(data, second_ending_index) == ?\r

        {blocks, next_index, trailing_cr?}
      else
        scan_complete_blocks(data, next_index, next_index, blocks)
      end
    end
  end

  defp line_ending_length(data, index) when index >= byte_size(data), do: 0

  defp line_ending_length(data, index) do
    case :binary.at(data, index) do
      ?\n ->
        1

      ?\r ->
        if index + 1 < byte_size(data) and :binary.at(data, index + 1) == ?\n, do: 2, else: 1

      _byte ->
        0
    end
  end

  defp canonicalize_line_endings(block) do
    block
    |> :binary.replace("\r\n", "\n", [:global])
    |> :binary.replace("\r", "\n", [:global])
    |> String.trim_leading("\n")
  end

  @spec sse_field(binary(), binary()) :: binary() | nil
  def sse_field(block, name) do
    prefix = name <> ":"

    block
    |> String.split("\n")
    |> Enum.map(&String.trim/1)
    |> Enum.flat_map(fn line ->
      if String.starts_with?(line, prefix) do
        [line |> String.replace_prefix(prefix, "") |> String.trim_leading()]
      else
        []
      end
    end)
    |> case do
      [] -> nil
      values -> Enum.join(values, "\n")
    end
  end

  @spec normalize_sse_event_label(term()) :: binary() | nil
  def normalize_sse_event_label(label) when is_binary(label) do
    case String.trim(label) do
      "" -> nil
      normalized -> normalized
    end
  end

  def normalize_sse_event_label(_label), do: nil

  @spec decode_sse_data(term()) :: map()
  def decode_sse_data(data) when is_binary(data) do
    case CodexPooler.JSON.decode(data) do
      {:ok, %{} = decoded} -> decoded
      _other -> %{}
    end
  end

  def decode_sse_data(_data), do: %{}

  @spec valid_json?(term()) :: boolean()
  def valid_json?(body) when is_binary(body), do: match?({:ok, _}, CodexPooler.JSON.decode(body))
  def valid_json?(_body), do: false

  @spec stream_block_event(binary()) :: {String.t() | nil, map()}
  def stream_block_event(block) do
    data = sse_field(block, "data")
    decoded = if is_binary(data), do: decode_sse_data(data), else: decode_sse_data(block)

    event_type =
      normalize_sse_event_label(sse_field(block, "event")) || decoded_string(decoded, "type")

    {event_type, decoded}
  end

  defp decoded_string(decoded, key) when is_map(decoded) do
    case Map.get(decoded, key) do
      value when is_binary(value) -> value
      _value -> nil
    end
  end

  defp maybe_bound_incomplete_sse_state(state, false), do: state

  defp maybe_bound_incomplete_sse_state(%{buffer: buffer} = state, true) do
    if oversized_incomplete_sse_block?(buffer), do: new_block_state(), else: state
  end
end
