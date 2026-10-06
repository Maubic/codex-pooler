defmodule CodexPooler.Gateway.Payloads.NativeMailboxPartialAnswerSerializationTest do
  # Mailbox mail can stop Codex after a completed `partial_answer` message exactly as after commentary (Codex
  # 989c01a41 / 822e58cc3: a partial answer is nonterminal and mailbox-preemptible, a `final_answer` or unphased
  # message stays terminal). The client resends the delivered partial answer from its typed `ResponseItem::Message`
  # model, which keeps `id`, `role`, `phase` and the `output_text` parts' `text`, adds internal metadata and drops
  # every other provider field (findings#306, findings#307). Provenance: source-derived from codex-rs/protocol at
  # 8b6bb1c77 and the observed 0.160.0 commentary serialization in the sibling characterization test; no released
  # client sends the phase yet. Ids, text, tools and mail are synthetic.
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.Gateway.Payloads.{NativeMailboxContinuation, RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexTurn

  @semantic :crypto.hash(:sha256, "synthetic partial answer mailbox turn")
  @now ~U[2026-10-06 00:00:00.000000Z]
  @attempt_id "018f60df-713f-7ca8-b9a0-0d12c508a003"
  @moduletag mailbox_serialization: true

  for transport <- ["http_sse", "websocket"] do
    test "#{transport} known partial-answer fields discarded by the client retain their proof" do
      emitted = partial_answer() |> Map.put("status", "completed") |> Map.put("internal_chat_message_metadata_passthrough", %{"executed_tool_calls" => []}) |> put_in(["content", Access.at(0), "annotations"], []) |> put_in(["content", Access.at(0), "logprobs"], [])
      serialized = Map.put(partial_answer(), "internal_chat_message_metadata_passthrough", %{"executed_tool_calls" => []})
      proof = predecessor(emitted, unquote(transport))
      assert_stage(proof, append(payload(), [serialized, mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} a discarded unknown partial-answer field retains the projection proof" do
      proof = predecessor(Map.put(partial_answer(), "provider_extension", "synthetic"), unquote(transport))
      assert_stage(proof, append(payload(), [partial_answer(), mailbox()]), :verified)
      assert_stage(proof, append(payload(), [Map.put(partial_answer(), "provider_extension", "synthetic"), mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} a changed retained partial-answer identity stays fenced" do
      proof = predecessor(partial_answer(), unquote(transport))

      for changed <- [Map.put(partial_answer(), "id", "msg_other"), put_in(partial_answer(), ["content", Access.at(0), "text"], "synthetic changed text"), Map.put(partial_answer(), "phase", "commentary")] do
        assert_stage(proof, append(payload(), [changed, mailbox()]), :output_prefix)
      end

      # A resend whose item became a terminal answer is no preemptible output at all.
      for terminal <- [Map.put(partial_answer(), "phase", "final_answer"), Map.delete(partial_answer(), "phase")] do
        assert_stage(proof, append(payload(), [terminal, mailbox()]), :no_candidate)
      end

      assert_stage(proof, append(payload(), [partial_answer(), mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} a delivered commentary message cannot be resent as a partial answer" do
      commentary = Map.put(partial_answer(), "phase", "commentary")
      proof = predecessor(commentary, unquote(transport))
      assert_stage(proof, append(payload(), [partial_answer(), mailbox()]), :output_prefix)
      assert_stage(proof, append(payload(), [commentary, mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} a final answer before incoming mail has intent without a candidate" do
      final = Map.put(partial_answer(), "phase", "final_answer")
      proof = predecessor(final, unquote(transport))
      candidate = append(payload(), [final, mailbox()])
      assert witness(candidate).mailbox_intent?
      assert_stage(proof, candidate, :no_candidate)
      assert_stage(predecessor(partial_answer(), unquote(transport)), append(payload(), [partial_answer(), mailbox()]), :verified)
    end

    test "#{transport} a partial answer retained with preceding reasoning keeps the ordered proof" do
      reasoning = reasoning()
      {:ok, reasoning_digest} = WebsocketTurnIdentity.completed_item_digest(reasoning)
      {:ok, partial_digest} = WebsocketTurnIdentity.completed_item_digest(partial_answer())
      proof = predecessor([reasoning, Map.put(partial_answer(), "provider_extension", "synthetic")], unquote(transport))
      assert reasoning_digest != partial_digest
      assert_stage(proof, append(payload(), [reasoning, partial_answer(), mailbox()]), :verified)
      assert_stage(proof, append(payload(), [partial_answer(), reasoning, mailbox()]), :output_prefix)
    end
  end

  defp assert_stage(proof, payload, stage, successor \\ nil) do
    assert check(proof, witness(payload), successor).stage == stage
    assert ClientRetry.verified_mailbox_continuation?(proof.turn, proof.request, proof.attempt, witness(payload), successor) == (stage == :verified)
  end

  defp check(proof, witness, successor), do: ClientRetry.mailbox_check(proof.turn, proof.request, proof.attempt, witness, successor, :same_session)

  defp predecessor(outputs, transport) do
    outputs = List.wrap(outputs)
    {:ok, digest} = WebsocketTurnIdentity.replay_claim_digest(@semantic, payload())
    item_digests = Enum.map(outputs, fn output -> elem(WebsocketTurnIdentity.completed_item_digest(output), 1) end)
    progress = Enum.reduce(outputs, ClientRetry.new_native_http_progress(), &ClientRetry.observe_native_http_output_item(&2, &1))
    metadata = %{"native_http_resume_progress" => ClientRetry.native_http_progress_metadata(progress), "native_http_mailbox_prefix" => ClientRetry.native_http_mailbox_prefix_metadata(progress), "downstream_delivery" => %{"outcome" => "aborted", "terminal_class" => "none", "highest_frame_class" => "item_done", "completed_items" => length(outputs), "completed_item_digests" => item_digests}}
    request = %Request{status: "failed", last_error_code: "client_disconnected", endpoint: "/backend-api/codex/responses", transport: transport, completed_at: @now, request_metadata: %{"native_http_claim_arm" => "opening"}, native_client_retry_version: 1, native_client_retry_digest: digest, native_client_retry_auth_epoch: 1}
    turn = %CodexTurn{status: "interrupted", error_code: "client_disconnected", transport_kind: transport, final_attempt_id: @attempt_id, completed_at: @now}
    attempt = %Attempt{id: @attempt_id, status: "failed", network_error_code: "client_disconnected", transport: transport, replay_generation: 0, completed_at: @now, response_metadata: metadata}
    %{request: request, turn: turn, attempt: attempt}
  end

  defp witness(payload), do: ClientRetry.original_witness!(:crypto.hash(:sha256, "synthetic current witness"), 1) |> NativeMailboxContinuation.attach(@semantic, payload, RequestOptions.build(%{}, "/backend-api/codex/responses", %{}))
  defp payload, do: %{"type" => "response.create", "model" => "synthetic-model", "instructions" => "synthetic instructions", "parallel_tool_calls" => true, "tools" => [tool("synthetic_tool")], "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}], "client_metadata" => %{"x-codex-turn-metadata" => %{"turn_id" => "synthetic-turn", "request_kind" => "turn", "agent_name" => "/root"}}}
  defp tool(name), do: %{"type" => "function", "name" => name, "parameters" => %{"type" => "object", "properties" => %{}}}
  defp append(payload, items), do: Map.update!(payload, "input", &(&1 ++ items))
  defp partial_answer, do: %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => "partial_answer", "content" => [%{"type" => "output_text", "text" => "synthetic partial answer"}]}
  defp reasoning, do: %{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic reasoning ciphertext"}
  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
end
