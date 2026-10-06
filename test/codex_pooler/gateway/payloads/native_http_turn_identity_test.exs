defmodule CodexPooler.Gateway.Payloads.NativeHttpTurnIdentityTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.ClientRetry
  alias CodexPooler.Gateway.Payloads.{NativeHttpTurnIdentity, NativeMailboxContinuation, RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexSession

  @session_id "018f60df-713f-7ca8-b9a0-0d12c508a003"

  test "opening and local-summary HTTP claims attach mailbox proof without changing claim or witness bytes" do
    for window <- [0, 2] do
      original = payload(window)
      candidate = append_mailbox(original)
      assert {:ok, predecessor} = NativeHttpTurnIdentity.request_claim(options(), original)
      assert {:ok, current} = NativeHttpTurnIdentity.request_claim(options(), candidate)
      assert current.arm == :opening
      assert current.key == predecessor.key
      assert current.steered_claim == predecessor.steered_claim
      assert [proof] = current.native_client_retry_witness.mailbox
      assert predecessor.native_client_retry_witness.digest in proof.prefix.websocket
      assert current.native_client_retry_witness.digest in proof.ending.websocket
      assert proof.current?
      assert {:ok, expected} = WebsocketTurnIdentity.replay_claim_digest(current.semantic_turn_key, Map.put(candidate, "type", "response.create"))
      assert current.native_client_retry_witness.digest == expected
    end
  end

  test "HTTP tool continuation keeps its request digest and attaches a matching unframed prefix proof" do
    original = Map.update!(payload(0), "input", &(&1 ++ [%{"type" => "function_call_output", "call_id" => "synthetic-call", "output" => "synthetic"}]))
    candidate = append_mailbox(original)
    assert {:ok, predecessor} = NativeHttpTurnIdentity.request_claim(options(), original)
    assert {:ok, current} = NativeHttpTurnIdentity.request_claim(options(), candidate)
    assert current.arm == :tool_continuation
    assert {:ok, expected} = WebsocketTurnIdentity.replay_claim_digest(current.semantic_turn_key, candidate)
    assert current.native_client_retry_witness.digest == expected
    assert [proof] = current.native_client_retry_witness.mailbox
    assert predecessor.native_client_retry_witness.digest in proof.prefix.websocket
    assert current.native_client_retry_witness.digest in proof.ending.websocket
    assert current.key == WebsocketTurnIdentity.request_claim_key(current.semantic_turn_key, candidate)
  end

  # The released client fills `workspaces` in its turn metadata after a turn's
  # first request can already have gone out, and its retry carries it
  # (findings#314 row 314-2). The retry names the request it repeats among a
  # fixed number of transient alternates; the claim and the stored witness stay
  # those of the request as sent.
  @workspaces %{"/synthetic/repository" => %{"latest_git_commit_hash" => String.duplicate("c", 40), "has_changes" => true}}

  test "an opener and a tool continuation that gained the async workspaces name the request sent before them" do
    tool_round = &Map.update!(&1, "input", fn input -> input ++ [%{"type" => "function_call_output", "call_id" => "synthetic-call", "output" => "synthetic"}] end)

    for {arm, earlier, extra} <- [{:opening, payload(0), 2}, {:tool_continuation, tool_round.(payload(0)), 1}] do
      later = put_in(earlier, ["client_metadata", "x-codex-turn-metadata", "workspaces"], @workspaces)
      assert {:ok, before} = NativeHttpTurnIdentity.request_claim(options(), earlier)
      assert {:ok, current} = NativeHttpTurnIdentity.request_claim(options(), later)

      assert current.arm == arm
      assert current.key == before.key
      refute current.native_client_retry_witness.digest == before.native_client_retry_witness.digest
      assert before.native_client_retry_witness.digest in current.native_client_retry_witness.alternates
      refute current.native_client_retry_witness.digest in before.native_client_retry_witness.alternates
      assert length(current.native_client_retry_witness.alternates) == length(before.native_client_retry_witness.alternates) + extra
      assert length(current.native_client_retry_witness.grown) == length(before.native_client_retry_witness.grown)
    end
  end

  test "a mailbox continuation that gained the async workspaces proves the predecessor sent before them" do
    original = payload(0)
    candidate = original |> append_mailbox() |> put_in(["client_metadata", "x-codex-turn-metadata", "workspaces"], @workspaces)
    assert {:ok, predecessor} = NativeHttpTurnIdentity.request_claim(options(), original)
    assert {:ok, current} = NativeHttpTurnIdentity.request_claim(options(), candidate)
    assert [proof] = current.native_client_retry_witness.mailbox
    assert predecessor.native_client_retry_witness.digest in proof.prefix.websocket
    assert current.native_client_retry_witness.digest in proof.ending.websocket

    # A websocket frame keeps its own witnesses.
    websocket = RequestOptions.build(%{transport: "websocket", codex_session: %CodexSession{id: @session_id}, api_key_runtime_epoch: 1}, "/backend-api/codex/responses", %{})
    assert {:ok, witness} = ClientRetry.original_witness(current.native_client_retry_witness.digest, 1)
    assert [frame_proof] = NativeMailboxContinuation.attach(witness, current.semantic_turn_key, candidate, websocket).mailbox
    refute predecessor.native_client_retry_witness.digest in frame_proof.prefix.websocket
    assert length(proof.prefix.websocket) == length(frame_proof.prefix.websocket) + 3
  end

  # A re-sample (findings#311) is its predecessor's input, then that response's
  # completed items, then harness items: it derives the opener's claim and
  # steered claim again, which is why the reservation has to tell the two
  # apart. The input count and input-only digest an opener records are what a
  # later re-sample is proved at; neither is part of any claim, and the digest
  # ignores the turn metadata document the client rebuilds for every request.
  test "a re-sample derives its opener's claims, and the opener records its input count and input-only digest" do
    opener = payload(0)
    resample = Map.update!(opener, "input", &(&1 ++ [%{"type" => "message", "role" => "assistant", "phase" => "commentary", "content" => [%{"type" => "output_text", "text" => "synthetic"}]}, %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "synthetic reminder"}]}]))
    assert {:ok, first} = NativeHttpTurnIdentity.request_claim(options(), opener)
    assert {:ok, next} = NativeHttpTurnIdentity.request_claim(options(), resample)

    assert {first.arm, next.arm} == {:opening, :opening}
    assert next.key == first.key
    assert next.steered_claim == first.steered_claim
    assert {first.input_count, next.input_count} == {1, 3}
    assert {:ok, digest} = WebsocketTurnIdentity.http_resume_input_digest(first.semantic_turn_key, opener["input"])
    assert first.input_digest == digest
    refute next.input_digest == first.input_digest

    filled = put_in(opener, ["client_metadata", "x-codex-turn-metadata", "workspaces"], @workspaces)
    assert {:ok, later} = NativeHttpTurnIdentity.request_claim(options(), filled)
    assert later.input_digest == first.input_digest
    refute later.native_client_retry_witness.digest == first.native_client_retry_witness.digest
  end

  test "a post-compaction resume records its count beside the input-only witness it already seals; a tool continuation records neither" do
    resume = Map.update!(payload(0), "input", &(&1 ++ [%{"type" => "compaction", "encrypted_content" => "synthetic-compaction"}]))
    assert {:ok, claim} = NativeHttpTurnIdentity.request_claim(options(), resume)
    assert claim.arm == :post_compaction_resume
    assert claim.input_count == 2
    assert {:ok, digest} = WebsocketTurnIdentity.http_resume_input_digest(claim.semantic_turn_key, resume["input"])
    assert claim.native_client_retry_witness.digest == digest
    refute Map.has_key?(claim, :input_digest)

    tool = Map.update!(payload(0), "input", &(&1 ++ [%{"type" => "function_call_output", "call_id" => "synthetic-call", "output" => "synthetic"}]))
    assert {:ok, tool_claim} = NativeHttpTurnIdentity.request_claim(options(), tool)
    assert tool_claim.arm == :tool_continuation
    assert tool_claim.input_count == nil
    refute Map.has_key?(tool_claim, :input_digest)
  end

  defp options do
    RequestOptions.build(%{transport: "http_sse", codex_session: %CodexSession{id: @session_id}, api_key_runtime_epoch: 1}, "/backend-api/codex/responses", %{})
  end

  defp payload(window) do
    %{"model" => "synthetic-model", "instructions" => "synthetic instructions", "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic summary or opening"}], "client_metadata" => %{"x-codex-turn-metadata" => %{"turn_id" => "synthetic-turn", "request_kind" => "turn", "agent_name" => "/root", "window_number" => window}}}
  end

  defp append_mailbox(payload) do
    Map.update!(payload, "input", &(&1 ++ [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic-reasoning"}, %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}]))
  end
end
