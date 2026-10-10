defmodule CodexPooler.Gateway.Transports.PublicResponsesTerminalOutputTest do
  # The public relays' record of the items a stream delivered, for a terminal the provider sent with an empty output
  # (findings#335). The controller-level contract, per transport and serving mode, is pinned in
  # `test/codex_pooler_web/controllers/v1/responses_streamed_terminal_output_test.exs`.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.PublicResponsesTerminalOutput, as: TerminalOutput

  @reasoning %{"type" => "reasoning", "id" => "rs_unit", "summary" => []}
  @message %{"type" => "message", "id" => "msg_unit", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "unit reply", "annotations" => []}]}
  @call %{"type" => "function_call", "id" => "fc_unit", "call_id" => "call_unit", "name" => "lookup_fixture", "arguments" => "{}"}

  test "an empty or absent completed or incomplete output takes the done items in output order" do
    state = observe([done(1, @message), done(0, @reasoning)])

    for type <- ["response.completed", "response.incomplete"], response <- [%{"output" => []}, %{}, %{"output" => nil}] do
      event = %{"type" => type, "response" => Map.put(response, "id", "resp_unit")}
      assert TerminalOutput.fill(type, event, state) == %{"type" => type, "response" => %{"id" => "resp_unit", "output" => [@reasoning, @message]}}
    end
  end

  test "a non-empty output, a failed or error terminal and any other event are left as sent" do
    state = observe([done(0, @reasoning), done(1, @message)])
    completed = %{"type" => "response.completed", "response" => %{"output" => [@message]}}
    assert TerminalOutput.fill("response.completed", completed, state) == completed

    for type <- ["response.failed", "error", "response.output_item.done", "response.created", nil] do
      event = %{"type" => type, "response" => %{"output" => []}}
      assert TerminalOutput.fill(type, event, state) == event
    end

    assert TerminalOutput.fill("response.completed", %{"type" => "response.completed"}, state) == %{"type" => "response.completed"}
  end

  test "nothing recorded leaves the terminal as sent" do
    completed = %{"type" => "response.completed", "response" => %{"output" => []}}
    assert TerminalOutput.fill("response.completed", completed, TerminalOutput.new_state()) == completed
    assert TerminalOutput.fill("response.completed", completed, observe([%{"type" => "response.output_item.done", "output_index" => 0}])) == completed
  end

  test "a later done item at the same output index replaces the earlier one" do
    replaced = %{@call | "arguments" => ~s({"key":"b"})}
    state = observe([done(0, @call), done(1, @message), done(0, replaced)])
    assert filled_output(state) == [replaced, @message]
  end

  test "events without an output index keep their arrival order" do
    state = observe([done(1, @message), %{"type" => "response.output_item.done", "item" => @reasoning}])
    assert filled_output(state) == [@message, @reasoning]
  end

  test "past the byte bound nothing is recorded and the terminal is relayed as sent" do
    state = TerminalOutput.observe(TerminalOutput.new_state(), done(0, @reasoning), 60, 100)
    assert filled_output(state) == [@reasoning]

    state = TerminalOutput.observe(state, done(1, @message), 60, 100)
    assert state.overflow?
    assert state.items == []

    state = TerminalOutput.observe(state, done(2, @call), 1, 100)
    completed = %{"type" => "response.completed", "response" => %{"output" => []}}
    assert TerminalOutput.fill("response.completed", completed, state) == completed
  end

  test "the default bound is the largest terminal the public relay accepts" do
    bound = StreamProtocol.max_incomplete_terminal_sse_block_bytes()
    assert TerminalOutput.observe(TerminalOutput.new_state(), done(0, @reasoning), bound).overflow? == false
    assert TerminalOutput.observe(TerminalOutput.new_state(), done(0, @reasoning), bound + 1).overflow?
  end

  defp observe(events), do: Enum.reduce(events, TerminalOutput.new_state(), &TerminalOutput.observe(&2, &1, 64))

  defp filled_output(state) do
    TerminalOutput.fill("response.completed", %{"type" => "response.completed", "response" => %{"output" => []}}, state)
    |> get_in(["response", "output"])
  end

  defp done(index, item), do: %{"type" => "response.output_item.done", "output_index" => index, "item" => item}
end
