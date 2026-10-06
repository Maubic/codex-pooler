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
