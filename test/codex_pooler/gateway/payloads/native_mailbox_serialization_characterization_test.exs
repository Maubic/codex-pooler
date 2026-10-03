defmodule CodexPooler.Gateway.Payloads.NativeMailboxSerializationCharacterizationTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.Gateway.Payloads.{NativeMailboxContinuation, RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexTurn

  @semantic :crypto.hash(:sha256, "synthetic serialized mailbox turn")
  @now ~U[2026-10-03 00:00:00.000000Z]
  @attempt_id "018f60df-713f-7ca8-b9a0-0d12c508a002"
  @moduletag mailbox_serialization: true

  # These neutral shapes reproduce observed 0.160.0 serialization. The fixture
  # emits status/annotations/logprobs; the client retains id/role/phase/text and
  # adds internal metadata. Unknown provider fields are discarded by the client.
  for transport <- ["http_sse", "websocket"] do
    test "#{transport} known commentary fields discarded by the client retain their proof" do
      emitted = commentary() |> Map.put("status", "completed") |> Map.put("internal_chat_message_metadata_passthrough", %{"executed_tool_calls" => []}) |> put_in(["content", Access.at(0), "annotations"], []) |> put_in(["content", Access.at(0), "logprobs"], [])
      serialized = Map.put(commentary(), "internal_chat_message_metadata_passthrough", %{"executed_tool_calls" => []})
      proof = predecessor(emitted, unquote(transport))
      assert_stage(proof, append(payload(), [serialized, mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} discarded unknown commentary field retains the observed client projection proof" do
      proof = predecessor(Map.put(commentary(), "provider_extension", "synthetic"), unquote(transport))
      assert_stage(proof, append(payload(), [commentary(), mailbox()]), :verified)
      assert_stage(proof, append(payload(), [Map.put(commentary(), "provider_extension", "synthetic"), mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} changed retained commentary identity stays fenced" do
      proof = predecessor(commentary(), unquote(transport))

      for changed <- [Map.put(commentary(), "id", "msg_other"), put_in(commentary(), ["content", Access.at(0), "text"], "synthetic changed text")] do
        assert_stage(proof, append(payload(), [changed, mailbox()]), :output_prefix)
      end

      assert_stage(proof, append(payload(), [commentary(), mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} each non-input option and tool or MCP catalogue remains part of its original witness" do
      proof = predecessor(commentary(), unquote(transport))
      original = append(payload(), [commentary(), mailbox()])

      for changed <- [Map.put(original, "instructions", "synthetic changed instructions"), Map.put(original, "parallel_tool_calls", false), Map.put(original, "tools", [tool("synthetic_other_tool")]), Map.put(original, "tools", [%{"type" => "namespace", "name" => "mcp__synthetic", "tools" => [tool("synthetic_other_tool")]}])] do
        assert_stage(proof, changed, :witness)
      end

      assert_stage(proof, original, :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} invalid recipient has intent without a candidate" do
      proof = predecessor(commentary(), unquote(transport))
      candidate = append(payload(), [commentary(), Map.put(mailbox(), "recipient", "/root/other")])
      assert witness(candidate).mailbox_intent?
      assert_stage(proof, candidate, :no_candidate)
      assert_stage(proof, append(payload(), [commentary(), mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} historical ending requires its exact successor" do
      proof = predecessor(commentary(), unquote(transport))
      historical = append(payload(), [commentary(), mailbox()])
      candidate = append(historical, [reasoning(), mailbox()])
      assert_stage(proof, candidate, :ending)
      {:ok, digest} = WebsocketTurnIdentity.replay_claim_digest(@semantic, historical)
      successor = %{proof.request | native_client_retry_digest: digest}
      assert_stage(proof, candidate, :verified, successor)
      assert_stage(proof, candidate, :ending, %{successor | native_client_retry_digest: :crypto.hash(:sha256, "other ending")})
    end

    test "#{transport} released reasoning null, empty content and projected provider fields retain their proof" do
      for content <- [nil, [], [%{"type" => "reasoning_text", "text" => "synthetic reasoning", "provider_extension" => "synthetic"}]] do
        emitted = reasoning() |> Map.put("content", content) |> Map.put("status", "completed") |> Map.put("provider_extension", "synthetic")
        serialized = if is_list(content) and content != [], do: Map.put(reasoning(), "content", [%{"type" => "reasoning_text", "text" => "synthetic reasoning"}]), else: Map.put(reasoning(), "content", nil)
        proof = predecessor(emitted, unquote(transport))
        assert_stage(proof, append(payload(), [serialized, mailbox()]), :verified)
      end
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} changed retained reasoning content or ciphertext reaches output_prefix" do
      emitted = Map.put(reasoning(), "content", [%{"type" => "reasoning_text", "text" => "synthetic reasoning"}])
      proof = predecessor(emitted, unquote(transport))

      for changed <- [Map.put(emitted, "encrypted_content", "synthetic changed ciphertext"), put_in(emitted, ["content", Access.at(0), "text"], "synthetic changed reasoning")] do
        assert_stage(proof, append(payload(), [changed, mailbox()]), :output_prefix)
      end

      assert_stage(proof, append(payload(), [emitted, mailbox()]), :verified)
    end

    @tag mailbox_serialization_negative: true
    test "#{transport} mixed serialized candidates cannot share witness and output proofs" do
      proof = predecessor(commentary(), unquote(transport))
      [valid] = witness(append(payload(), [commentary(), mailbox()])).mailbox
      bad_output = %{valid | items: ["syntheticbad"], http_progress: []}
      bad_prefix = %{valid | prefix: %{http: nil, websocket: []}}
      mixed = %{witness(payload()) | mailbox: [bad_output, bad_prefix]}
      assert check(proof, mixed, nil) == %{stage: :output_prefix, candidate_index: 1}
      assert check(proof, %{mixed | mailbox: [bad_output, valid]}, nil) == %{stage: :verified, candidate_index: 2}
    end
  end

  defp assert_stage(proof, payload, stage, successor \\ nil) do
    assert check(proof, witness(payload), successor).stage == stage
    assert ClientRetry.verified_mailbox_continuation?(proof.turn, proof.request, proof.attempt, witness(payload), successor) == (stage == :verified)
  end

  defp check(proof, witness, successor), do: ClientRetry.mailbox_check(proof.turn, proof.request, proof.attempt, witness, successor, :same_session)

  defp predecessor(output, transport) do
    {:ok, digest} = WebsocketTurnIdentity.replay_claim_digest(@semantic, payload())
    {:ok, item_digest} = WebsocketTurnIdentity.completed_item_digest(output)
    progress = ClientRetry.new_native_http_progress() |> ClientRetry.observe_native_http_output_item(output)
    metadata = %{"native_http_resume_progress" => ClientRetry.native_http_progress_metadata(progress), "native_http_mailbox_prefix" => ClientRetry.native_http_mailbox_prefix_metadata(progress), "downstream_delivery" => %{"outcome" => "aborted", "terminal_class" => "none", "highest_frame_class" => "item_done", "completed_items" => 1, "completed_item_digests" => [item_digest]}}
    request = %Request{status: "failed", last_error_code: "client_disconnected", endpoint: "/backend-api/codex/responses", transport: transport, completed_at: @now, request_metadata: %{"native_http_claim_arm" => "opening"}, native_client_retry_version: 1, native_client_retry_digest: digest, native_client_retry_auth_epoch: 1}
    turn = %CodexTurn{status: "interrupted", error_code: "client_disconnected", transport_kind: transport, final_attempt_id: @attempt_id, completed_at: @now}
    attempt = %Attempt{id: @attempt_id, status: "failed", network_error_code: "client_disconnected", transport: transport, replay_generation: 0, completed_at: @now, response_metadata: metadata}
    %{request: request, turn: turn, attempt: attempt}
  end

  defp witness(payload), do: ClientRetry.original_witness!(:crypto.hash(:sha256, "synthetic current witness"), 1) |> NativeMailboxContinuation.attach(@semantic, payload, RequestOptions.build(%{}, "/backend-api/codex/responses", %{}))
  defp payload, do: %{"type" => "response.create", "model" => "synthetic-model", "instructions" => "synthetic instructions", "parallel_tool_calls" => true, "tools" => [tool("synthetic_tool")], "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}], "client_metadata" => %{"x-codex-turn-metadata" => %{"turn_id" => "synthetic-turn", "request_kind" => "turn", "agent_name" => "/root"}}}
  defp tool(name), do: %{"type" => "function", "name" => name, "parameters" => %{"type" => "object", "properties" => %{}}}
  defp append(payload, items), do: Map.update!(payload, "input", &(&1 ++ items))
  defp commentary, do: %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => "commentary", "content" => [%{"type" => "output_text", "text" => "synthetic commentary"}]}
  defp reasoning, do: %{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic reasoning ciphertext"}
  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
end
