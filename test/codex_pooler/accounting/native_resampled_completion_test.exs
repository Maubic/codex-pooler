defmodule CodexPooler.Accounting.NativeResampledCompletionTest do
  # The re-sample proof (findings#311) from persisted rows alone: the
  # predecessor's turn, request and final attempt as stored (claim arm, input
  # count and input-only witness in the request metadata, the delivery receipt
  # and the completed-item digests on the attempt) and the successor's side.
  # No process state and no payload bytes of the predecessor are read, so any
  # node decides it the same way. The end-to-end arms live in
  # `test/codex_pooler_web/controllers/runtime/backend_codex_http_resample_test.exs`.
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Metadata, NativeResampledCompletion, Request}
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.CodexTurn

  @semantic :crypto.hash(:sha256, "synthetic resample proof turn")
  @now ~U[2026-10-01 00:00:00.000000Z]
  @request_id "018f60df-713f-7ca8-b9a0-0d12c508a101"
  @attempt_id "018f60df-713f-7ca8-b9a0-0d12c508a102"
  @session_id "018f60df-713f-7ca8-b9a0-0d12c508a103"
  @epoch 3

  setup do
    %{proof: proof([provider_message("msg_1")])}
  end

  describe "verified" do
    test "the predecessor's input, exactly its completed items, then nothing or an allowed tail", %{proof: proof} do
      assert check(proof) == %{stage: :verified}
      assert proof |> with_tail([developer("synthetic time reminder"), %{"type" => "configuration_update"}]) |> check() == %{stage: :verified}
    end

    test "up to four completed items, reasoning resent without its content", %{proof: proof} do
      outputs = [provider_reasoning("rs_1"), provider_message("msg_1"), provider_message("msg_2"), provider_message("msg_3")]
      assert outputs |> proof() |> check() == %{stage: :verified}
      assert proof.request.request_metadata["native_http_claim_arm"] == "opening"
    end

    test "a steered continuation and a post-compaction resume, whose input-only digest is its sealed witness" do
      steered = proof([provider_message("msg_1")], "steered_continuation")
      assert check(steered) == %{stage: :verified}

      resume = proof([provider_message("msg_1")], "post_compaction_resume")
      metadata = Map.delete(resume.request.request_metadata, "native_http_input_witness")
      resume = %{resume | request: %{resume.request | request_metadata: metadata, native_client_retry_digest: input_digest(resume.input)}}
      assert check(resume) == %{stage: :verified}
      assert check(%{resume | request: %{resume.request | native_client_retry_digest: :crypto.hash(:sha256, "another input")}}) == %{stage: :witness}
    end

    # The provider's `end_turn` is not a trust input: the request decides.
    test "whatever end_turn class the receipt names", %{proof: proof} do
      for class <- ["true", "false", "absent", nil] do
        receipt = if class, do: Map.put(receipt(), "end_turn", class), else: Map.delete(receipt(), "end_turn")
        assert proof |> put_receipt(receipt) |> check() == %{stage: :verified}
      end
    end

    # On a chain edge the predecessor's successor fixed the edge's end: only
    # the items up to its recorded count are judged, so what the client added
    # after it (mail, a user message) belongs to a later node.
    test "only up to the count of the edge being proved", %{proof: proof} do
      proof = with_tail(proof, [developer("synthetic fragment")])
      later = proof.side.input ++ [%{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => []}, user("synthetic later input")]
      assert check(%{proof | side: %{proof.side | input: later}}) == %{stage: :verified}
      assert check(%{proof | side: %{proof.side | input: later, validation_count: length(later)}}).stage == :tail
    end
  end

  describe "refusals, each at the furthest stage it reaches" do
    for row <- [:turn, :request, :attempt] do
      test "settlement: a nonterminal #{row}", %{proof: proof} do
        assert proof |> Map.update!(unquote(row), &%{&1 | completed_at: nil}) |> check() == %{stage: :settlement}
      end
    end

    test "settlement: transport, endpoint, status, arm, generation and the final attempt", %{proof: proof} do
      for changed <- [
            %{proof | turn: %{proof.turn | status: "interrupted"}},
            %{proof | turn: %{proof.turn | transport_kind: "websocket"}},
            %{proof | turn: %{proof.turn | final_attempt_id: "018f60df-713f-7ca8-b9a0-0d12c508a1ff"}},
            %{proof | request: %{proof.request | status: "failed"}},
            %{proof | request: %{proof.request | transport: "websocket"}},
            %{proof | request: %{proof.request | transport: "http_json"}},
            %{proof | request: %{proof.request | endpoint: "/v1/responses"}},
            %{proof | request: %{proof.request | request_metadata: Map.put(proof.request.request_metadata, "native_http_claim_arm", "tool_continuation")}},
            %{proof | request: %{proof.request | request_metadata: Map.delete(proof.request.request_metadata, "native_http_claim_arm")}},
            %{proof | attempt: %{proof.attempt | replay_generation: 1}},
            %{proof | attempt: %{proof.attempt | status: "failed"}},
            %{proof | turn: nil},
            %{proof | attempt: nil}
          ] do
        assert check(changed) == %{stage: :settlement}
      end
    end

    test "authorization: the successor's sealed witness, its epoch and the predecessor's eligibility", %{proof: proof} do
      for changed <- [
            %{proof | side: %{proof.side | witness: nil}},
            %{proof | side: %{proof.side | witness: ClientRetry.original_witness!(:crypto.hash(:sha256, "successor"), @epoch + 1)}},
            %{proof | request: %{proof.request | native_client_retry_auth_epoch: @epoch + 1}},
            %{proof | request: %{proof.request | native_client_retry_version: nil}},
            %{proof | request: %{proof.request | native_client_retry_digest: nil}}
          ] do
        assert check(changed) == %{stage: :authorization}
      end
    end

    test "session: another or no codex session", %{proof: proof} do
      assert check(%{proof | side: %{proof.side | codex_session_id: "018f60df-713f-7ca8-b9a0-0d12c508a1fe"}}) == %{stage: :session}
      assert check(%{proof | side: %{proof.side | codex_session_id: nil}}) == %{stage: :session}
    end

    test "delivery: no receipt yet, or one that names no delivered response.completed", %{proof: proof} do
      for receipt <- [nil, %{receipt() | "outcome" => "completed"}, %{receipt() | "outcome" => "aborted"}, %{receipt() | "terminal_class" => "response.incomplete"}, %{receipt() | "terminal_class" => "response.failed"}] do
        assert proof |> put_receipt(receipt) |> check() == %{stage: :delivery}
      end
    end

    test "count: no recorded input count, no digests, or no item more than the predecessor sent", %{proof: proof} do
      without_count = %{proof | request: %{proof.request | request_metadata: Map.delete(proof.request.request_metadata, "native_http_input_count")}}
      without_digests = %{proof | attempt: %{proof.attempt | response_metadata: Map.delete(proof.attempt.response_metadata, "native_http_mailbox_prefix")}}
      poisoned = put_in(proof.attempt.response_metadata["native_http_mailbox_prefix"]["item_digests"], [])
      identical = %{proof | side: %{proof.side | input: proof.input, validation_count: length(proof.input)}}
      dropped = %{proof | side: %{proof.side | input: proof.input, validation_count: length(proof.input) + 1}}

      for changed <- [without_count, without_digests, poisoned, identical, dropped] do
        assert %{stage: :count} = check(changed)
      end
    end

    # A successor admitted under the derived claim fixes the edge's count; one
    # without a recorded count leaves the edge unprovable, and the request's own
    # length never stands in for it.
    test "count: an edge whose successor recorded no count", %{proof: proof} do
      assert check(%{proof | side: %{proof.side | validation_count: :missing}}) == %{stage: :count, output_items: 1}
    end

    test "count: more completed items than the receipt keeps, with how many there were" do
      outputs = Enum.map(1..5, &provider_message("msg_#{&1}"))
      assert outputs |> proof() |> check() == %{stage: :count, output_items: 5}
    end

    test "witness: another input, or no stored input-only digest", %{proof: proof} do
      changed_input = [user("synthetic other request") | tl(proof.side.input)]
      malformed = put_in(proof.request.request_metadata["native_http_input_witness"]["digest"], "not-a-digest")

      for changed <- [%{proof | side: %{proof.side | input: changed_input}}, %{proof | request: %{proof.request | request_metadata: Map.delete(proof.request.request_metadata, "native_http_input_witness")}}, malformed] do
        assert check(changed) == %{stage: :witness}
      end
    end

    test "output: a changed, reordered or replaced item" do
      outputs = [provider_message("msg_1"), provider_message("msg_2")]
      proof = proof(outputs)
      [user_item, first, second] = proof.side.input

      for input <- [[user_item, Map.put(first, "content", [%{"type" => "output_text", "text" => "synthetic text not written"}]), second], [user_item, second, first], [user_item, first, client_message("msg_other")]] do
        assert check(%{proof | side: %{proof.side | input: input}}) == %{stage: :output}
      end
    end

    test "tail: the first item outside the grammar, in the closed vocabulary", %{proof: proof} do
      assert %{stage: :tail, tail: %{reason: :item, tail_index: 1, tail_length: 2, item_type: "message", item_role: "user"}} = proof |> with_tail([developer("synthetic fragment"), user("synthetic steer")]) |> check()
      assert %{stage: :tail, tail: %{reason: :too_long, tail_length: 17}} = proof |> with_tail(List.duplicate(developer("synthetic fragment"), 17)) |> check()

      refusal = proof |> with_tail([%{"type" => "sentinel_resample_type", "content" => "sentinel resample content"}]) |> check()
      assert %{stage: :tail, tail: %{item_type: "other", item_role: "none", item_type_fingerprint: _fingerprint}} = refusal
      refute inspect(refusal) =~ "sentinel"
    end
  end

  # The predecessor's count and input-only witness are stored in request
  # metadata, whose sanitizer redacts any non-integer value under a key that
  # names `input`; the witness has its own clause.
  describe "metadata" do
    test "the input count and the input witness survive the sanitizer exactly; anything else under the witness key is dropped" do
      witness = NativeResampledCompletion.input_witness_metadata(input_digest([user("synthetic request")]))
      metadata = %{"native_http_claim_arm" => "opening", "native_http_input_count" => 2, "native_http_input_witness" => witness}

      assert Metadata.sanitize_metadata(metadata) == metadata
      assert byte_size(witness["digest"]) == 43

      for malformed <- [%{witness | "version" => 2}, Map.put(witness, "extra", 1), %{witness | "digest" => "short"}, %{witness | "digest" => String.duplicate("!", 43)}] do
        assert Metadata.sanitize_metadata(%{"native_http_input_witness" => malformed}) == %{"native_http_input_witness" => %{}}
      end

      # A bare digest string is what the generic rule redacts, which is why the
      # witness is a map with a clause of its own.
      assert Metadata.sanitize_metadata(%{"native_http_input_witness" => witness["digest"], "native_http_input_digest" => witness["digest"]}) == %{"native_http_input_witness" => "[REDACTED]", "native_http_input_digest" => "[REDACTED]"}
    end
  end

  defp check(proof), do: NativeResampledCompletion.check(proof.turn, proof.request, proof.attempt, proof.side)

  defp proof(outputs, arm \\ "opening") do
    input = [user("synthetic request")]
    digests = Enum.map(outputs, &item_digest/1)

    request = %Request{
      id: @request_id,
      status: "succeeded",
      transport: "http_sse",
      endpoint: "/backend-api/codex/responses",
      completed_at: @now,
      native_client_retry_version: 1,
      native_client_retry_digest: :crypto.hash(:sha256, "synthetic whole-frame witness"),
      native_client_retry_auth_epoch: @epoch,
      request_metadata: %{"native_http_claim_arm" => arm, "native_http_input_count" => length(input), "native_http_input_witness" => NativeResampledCompletion.input_witness_metadata(input_digest(input))}
    }

    attempt = %Attempt{
      id: @attempt_id,
      request_id: @request_id,
      status: "succeeded",
      transport: "http_sse",
      replay_generation: 0,
      completed_at: @now,
      response_metadata: %{
        "downstream_delivery" => receipt(),
        "native_http_resume_progress" => %{"version" => 1, "output_item_done_count" => length(outputs), "digest" => Base.url_encode64(:crypto.hash(:sha256, "synthetic progress"), padding: false)},
        "native_http_mailbox_prefix" => %{"version" => 1, "output_item_done_count" => length(outputs), "item_digests" => Enum.take(digests, 4)}
      }
    }

    turn = %CodexTurn{request_id: @request_id, status: "succeeded", transport_kind: "http_sse", final_attempt_id: @attempt_id, completed_at: @now, codex_session_id: @session_id}
    successor_input = input ++ Enum.map(outputs, &resent/1)
    side = %{input: successor_input, semantic_turn_key: @semantic, validation_count: length(successor_input), codex_session_id: @session_id, witness: ClientRetry.original_witness!(:crypto.hash(:sha256, "synthetic successor witness"), @epoch)}

    %{turn: turn, request: request, attempt: attempt, side: side, input: input}
  end

  defp with_tail(proof, tail) do
    input = proof.side.input ++ tail
    %{proof | side: %{proof.side | input: input, validation_count: length(input)}}
  end

  defp put_receipt(proof, nil), do: %{proof | attempt: %{proof.attempt | response_metadata: Map.delete(proof.attempt.response_metadata, "downstream_delivery")}}
  defp put_receipt(proof, receipt), do: put_in(proof.attempt.response_metadata["downstream_delivery"], receipt)

  defp receipt, do: %{"outcome" => "delivered", "terminal_class" => "response.completed", "end_turn" => "false"}

  defp item_digest(item) do
    {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(item)
    digest
  end

  defp input_digest(input) do
    {:ok, digest} = WebsocketTurnIdentity.http_resume_input_digest(@semantic, input)
    digest
  end

  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}
  defp developer(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}

  # What the provider writes, and what the released client resends of it: only
  # its typed model's fields, its local passthrough metadata added, a reasoning
  # item's content nulled.
  defp provider_message(id), do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => "commentary", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}", "annotations" => [], "logprobs" => []}]}
  defp provider_reasoning(id), do: %{"type" => "reasoning", "id" => id, "summary" => [%{"type" => "summary_text", "text" => "synthetic summary of #{id}"}], "encrypted_content" => "synthetic-encrypted-#{id}"}

  defp client_message(id), do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => "commentary", "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}"}], "internal_chat_message_metadata_passthrough" => %{"turn_id" => "synthetic"}}

  defp resent(%{"type" => "reasoning"} = item), do: Map.put(item, "content", nil)
  defp resent(%{"type" => "message", "id" => id}), do: client_message(id)
end
