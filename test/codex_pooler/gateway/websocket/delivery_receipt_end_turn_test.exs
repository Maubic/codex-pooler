defmodule CodexPooler.Gateway.Websocket.DeliveryReceiptEndTurnTest do
  # The provider's `response.completed` may carry `response.end_turn`; a client that reads `false` re-samples the turn
  # (findings#311). The receipt of a pushed `response.completed` names the class (`true`, `false` or `absent`) so that
  # exposure can be counted from attempt rows instead of probes. Provenance: the field and its client reading are
  # source-derived (codex-rs `ResponseCompleted.end_turn`); no provider capture has shown the field on the wire yet
  # (direct probe, 2026-10-06), so the arms below are synthetic shapes of the documented contract.
  use ExUnit.Case, async: false

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  alias CodexPooler.Gateway.Runtime.Streaming.DownstreamDeliveryEvidence
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt

  @classes ~w(true false absent)

  defp completed(response_extra) do
    %{"type" => "response.completed", "response" => Map.merge(%{"id" => "resp_end_turn_unit", "status" => "completed", "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}, response_extra)}
  end

  defp sse(%{"type" => type} = payload), do: "event: #{type}\ndata: #{CodexPooler.JSON.encode!(payload)}\n\n"

  defp outcome!(data) do
    assert {:ok, outcome} = StreamProtocol.terminal_outcome(data)
    outcome
  end

  describe "the completed terminal outcome" do
    for {label, extra, class} <- [
          {"true", %{"end_turn" => true}, "true"},
          {"false", %{"end_turn" => false}, "false"},
          {"missing", %{}, "absent"},
          {"null", %{"end_turn" => nil}, "absent"},
          {"a string", %{"end_turn" => "false"}, "absent"},
          {"a number", %{"end_turn" => 0}, "absent"}
        ] do
      test "names end_turn #{label} as #{class}, from an SSE block and from a bare frame" do
        payload = completed(unquote(Macro.escape(extra)))

        assert %{kind: :completed, end_turn: unquote(class)} = outcome!(sse(payload))
        assert %{kind: :completed, end_turn: unquote(class)} = outcome!(CodexPooler.JSON.encode!(payload))
      end
    end

    test "names the class on a response.done terminal and on the legacy bare response" do
      done = %{"type" => "response.done", "response" => %{"id" => "resp_done", "end_turn" => false}}

      assert %{kind: :completed, end_turn: "false"} = outcome!(sse(done))
      assert %{kind: :completed, end_turn: "absent"} = outcome!(CodexPooler.JSON.encode!(%{"id" => "resp_legacy"}))
    end

    test "is only ever one of the fixed classes" do
      assert DeliveryReceipt.end_turn_class_values() == @classes
    end

    test "no other terminal carries an end_turn class" do
      failed = %{"type" => "response.failed", "response" => %{"id" => "resp_failed", "end_turn" => false, "error" => %{"code" => "server_error", "message" => "synthetic"}}}
      incomplete = %{"type" => "response.incomplete", "response" => %{"id" => "resp_incomplete", "end_turn" => false, "incomplete_details" => %{"reason" => "max_output_tokens"}}}
      error = %{"type" => "error", "error" => %{"type" => "server_error", "code" => "server_error", "message" => "synthetic"}}

      for payload <- [failed, incomplete, error] do
        outcome = outcome!(sse(payload))
        refute Map.has_key?(outcome, :end_turn)
        assert DeliveryReceipt.end_turn_class_from_outcome(outcome) == nil
      end
    end

    test "end_turn_class_from_outcome reads only a completed outcome" do
      assert DeliveryReceipt.end_turn_class_from_outcome(%{kind: :completed, end_turn: "false"}) == "false"
      assert DeliveryReceipt.end_turn_class_from_outcome(%{kind: :completed}) == nil
      assert DeliveryReceipt.end_turn_class_from_outcome(%{kind: :incomplete, end_turn: "false"}) == nil
      assert DeliveryReceipt.end_turn_class_from_outcome(nil) == nil
    end
  end

  describe "the receipt" do
    @pushed_at ~U[2026-10-06 09:30:00.123000Z]

    test "carries the class of a pushed completed terminal and nothing else changes" do
      base = %{outcome: "delivered", terminal_class: "response.completed", pushed_at: @pushed_at, frames_after_visible: 3, transport: "http_sse"}
      without = DeliveryReceipt.build(base)

      assert Enum.sort(Map.keys(without)) == ~w(frames_after_visible outcome pushed_at terminal_class transport)

      for class <- @classes do
        with_class = DeliveryReceipt.build(Map.put(base, :end_turn, class))
        assert with_class == Map.put(without, "end_turn", class)
      end
    end

    test "coerces anything outside the vocabulary to absent and refuses non-completed terminals" do
      base = %{outcome: "delivered", pushed_at: @pushed_at, frames_after_visible: 1}

      for junk <- ["maybe", "TRUE", 1, :unexpected, "end_turn=false Authorization: Bearer sk-secret"] do
        receipt = DeliveryReceipt.build(Map.merge(base, %{terminal_class: "response.completed", end_turn: junk}))
        assert receipt["end_turn"] == "absent"
        refute inspect(receipt) =~ "secret"
      end

      for terminal_class <- ["response.failed", "response.incomplete", "error", nil] do
        receipt = DeliveryReceipt.build(Map.merge(base, %{terminal_class: terminal_class, end_turn: "false"}))
        refute Map.has_key?(receipt, "end_turn")
      end
    end

    test "the receipt log line names the class after the existing fields" do
      receipt = DeliveryReceipt.build(%{outcome: "delivered", terminal_class: "response.completed", pushed_at: @pushed_at, frames_after_visible: 2, transport: "http_sse", end_turn: "false"})

      {:ok, logs} = with_info_log(fn -> DeliveryReceipt.record(%{request_id: "req-end-turn-unit", codex_session_id: "sess-end-turn-unit"}, receipt) end)

      assert logs =~ "http_sse downstream terminal pushed request_id=req-end-turn-unit codex_session_id=sess-end-turn-unit outcome=delivered terminal_class=response.completed frames_after_visible=2 end_turn=false"
    end
  end

  describe "the HTTP SSE evidence" do
    defp receipt_after(chunks) do
      state = Map.put(%{}, :visible_output_marked?, true)

      chunks
      |> Enum.reduce(state, &DownstreamDeliveryEvidence.record_write(&2, &1))
      |> DownstreamDeliveryEvidence.receipt()
    end

    for {label, extra, class} <- [{"true", %{"end_turn" => true}, "true"}, {"false", %{"end_turn" => false}, "false"}, {"missing", %{}, "absent"}] do
      test "records end_turn #{label} from the written terminal" do
        delta = sse(%{"type" => "response.output_text.delta", "delta" => "synthetic delta"})
        receipt = receipt_after([delta, sse(completed(unquote(Macro.escape(extra))))])

        assert %{"outcome" => "delivered", "terminal_class" => "response.completed", "end_turn" => unquote(class)} = receipt
        assert Enum.sort(Map.keys(receipt)) == ~w(end_turn frames_after_visible outcome pushed_at terminal_class transport)
      end
    end

    test "reads a terminal split across two writes" do
      block = sse(completed(%{"end_turn" => false}))
      {first, second} = String.split_at(block, div(byte_size(block), 2))

      assert %{"terminal_class" => "response.completed", "end_turn" => "false"} = receipt_after([first, second])
    end

    test "a failed terminal and an undelivered turn carry no class" do
      failed = %{"type" => "response.failed", "response" => %{"id" => "resp_failed", "end_turn" => false, "error" => %{"code" => "server_error", "message" => "synthetic"}}}

      assert %{"terminal_class" => "response.failed"} = failed_receipt = receipt_after([sse(failed)])
      refute Map.has_key?(failed_receipt, "end_turn")

      aborted = DownstreamDeliveryEvidence.receipt(DownstreamDeliveryEvidence.record_write_failure(%{}))
      refute Map.has_key?(aborted, "end_turn")
      assert aborted["terminal_class"] == "none"
    end
  end
end
