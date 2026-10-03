defmodule CodexPooler.Gateway.Payloads.NativeContentFilterRetryTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.{Attempt, ClientRetry, NativeContentFilterRetry, Request}
  alias CodexPooler.Gateway.Payloads.NativeContentFilterRetry, as: Candidates
  alias CodexPooler.Gateway.Payloads.{RequestOptions, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexTurn

  @key :crypto.hash(:sha256, "synthetic content filter")
  @now ~U[2026-10-03 00:00:00.000000Z]

  for transport <- ["http_sse", "websocket"], outputs <- [0, 1, 2] do
    test "#{transport} exact complete output #{outputs} and one guidance is required" do
      {original, output, turn, request, attempt} = fixture(unquote(transport), unquote(outputs))
      candidate = append(original, output ++ [guidance()])
      assert NativeContentFilterRetry.verified?(turn, request, attempt, witness(candidate), nil)

      for key <- ["native_content_filter_terminal", "native_content_filter_source", "native_client_retry_observation", "downstream_delivery"] do
        refute NativeContentFilterRetry.verified?(turn, request, %{attempt | response_metadata: Map.delete(attempt.response_metadata, key)}, witness(candidate), nil)
      end

      for {key, value} <- [{"version", 2}, {"reason", "interrupted"}, {"reason", "max_output_tokens"}, {"event_type", "response.completed"}] do
        changed = put_in(attempt.response_metadata["native_content_filter_terminal"][key], value)
        refute NativeContentFilterRetry.verified?(turn, request, changed, witness(candidate), nil)
      end

      for {key, value} <- [{"outcome", "aborted"}, {"outcome", "skipped"}, {"terminal_class", "response.completed"}, {"incomplete_reason", "interrupted"}] do
        changed = put_in(attempt.response_metadata["downstream_delivery"][key], value)
        refute NativeContentFilterRetry.verified?(turn, request, changed, witness(candidate), nil)
      end

      for changed <- [Map.put(candidate, "instructions", "changed"), Map.put(candidate, "model", "changed"), Map.put(candidate, "temperature", 0.5), append(candidate, [guidance()]), append(candidate, [%{"type" => "message", "role" => "user", "content" => "changed"}]), put_in(candidate, ["input", Access.at(0), "content"], "changed")] do
        refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(changed), nil)
      end

      refute NativeContentFilterRetry.verified?(turn, request, %{attempt | id: Ecto.UUID.generate()}, witness(candidate), nil)
      refute NativeContentFilterRetry.verified?(turn, request, %{attempt | replay_generation: 1}, witness(candidate), nil)
      refute NativeContentFilterRetry.verified?(turn, %{request | native_client_retry_auth_epoch: 2}, attempt, witness(candidate), nil)
      refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(candidate, "lite"), nil)

      for change <- [%{"authority_complete" => false}, %{"output_item_done_count_saturated" => true}, %{"terminal_seen" => false}] do
        changed = update_in(attempt.response_metadata["native_client_retry_observation"], &Map.merge(&1, change))
        refute NativeContentFilterRetry.verified?(turn, request, changed, witness(candidate), nil)
      end

      receipt_key = if unquote(transport) == "websocket", do: "downstream_delivery", else: "native_http_resume_progress"
      changed = %{attempt | response_metadata: Map.delete(attempt.response_metadata, receipt_key)}
      refute NativeContentFilterRetry.verified?(turn, request, changed, witness(candidate), nil)

      for change <- [%{"completed_items" => 65_535}, %{"completed_item_digests" => []}, %{"write_failure" => "closed"}] do
        changed = update_in(attempt.response_metadata["downstream_delivery"], &Map.merge(&1, change))
        if unquote(transport) == "websocket" and (change != %{"completed_item_digests" => []} or unquote(outputs) > 0), do: refute(NativeContentFilterRetry.verified?(turn, request, changed, witness(candidate), nil))
      end

      for change <- [%{"version" => 2}, %{"output_item_done_count" => -1}, %{"digest" => "invalid"}] do
        changed = update_in(attempt.response_metadata["native_http_resume_progress"], &Map.merge(&1, change))
        if unquote(transport) == "http_sse", do: refute(NativeContentFilterRetry.verified?(turn, request, changed, witness(candidate), nil))
      end

      if unquote(outputs) > 0 do
        refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(append(original, [guidance()])), nil)
        refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(append(original, output ++ output ++ [guidance()])), nil)
      end

      if unquote(outputs) == 2 do
        refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(append(original, Enum.reverse(output) ++ [guidance()])), nil)
      end
    end
  end

  test "historical candidates must end at the actual successor witness" do
    {original, output, turn, request, attempt} = fixture("websocket", 1)
    next = append(original, output ++ [guidance()])
    current = append(next, output ++ [guidance()])
    {:ok, digest} = WebsocketTurnIdentity.replay_claim_digest(@key, next)
    successor = %{request | id: Ecto.UUID.generate(), native_client_retry_digest: digest, request_metadata: %{"native_content_filter_original" => %{"version" => 1, "digest" => Base.url_encode64(digest, padding: false)}}}
    refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(current), nil)
    assert NativeContentFilterRetry.verified?(turn, request, attempt, witness(current), successor)
    changed = put_in(successor.request_metadata["native_content_filter_original"]["digest"], Base.url_encode64(:crypto.hash(:sha256, "changed"), padding: false))
    refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(current), changed)
  end

  test "malformed guidance and anchors never mint a candidate" do
    {original, _, _, _, _} = fixture("websocket", 0)

    for item <- [Map.put(guidance(), "role", "user"), Map.put(guidance(), "extra", true), Map.put(guidance(), "content", []), Map.update!(guidance(), "content", &(&1 ++ &1)), put_in(guidance(), ["content", Access.at(0), "text"], "<content_filter_guidance>\nnested <content_filter_guidance>\n</content_filter_guidance>")] do
      assert witness(append(original, [item])).content_filter == []
    end

    assert witness(append(original, [guidance()]) |> Map.put("previous_response_id", "resp_synthetic")).content_filter == []
  end

  test "released 0.160.0 guidance keeps its stamped message identity and host annotations" do
    # Observed binary 0.160.0 / a956835d: the retry item has five fields;
    # context-fragments RenderedFragment plus session history stamps id and
    # internal metadata. Synthetic values preserve that measured shape.
    {original, output, turn, request, attempt} = fixture("http_sse", 1)
    item = guidance() |> Map.put("id", "msg_synthetic_guidance") |> Map.put("internal_chat_message_metadata_passthrough", %{"turn_id" => "synthetic-turn", "create_time" => 1_790_982_900.125})
    assert NativeContentFilterRetry.verified?(turn, request, attempt, witness(append(original, output ++ [item])), nil)

    for changed <- [Map.put(item, "id", "ordinary"), put_in(item, ["internal_chat_message_metadata_passthrough", "content_item_kinds"], ["other"]), Map.put(item, "content", item["content"] ++ item["content"])] do
      refute NativeContentFilterRetry.verified?(turn, request, attempt, witness(append(original, output ++ [changed])), nil)
    end
  end

  defp fixture(transport, count) do
    original = %{"type" => "response.create", "model" => "synthetic-model", "instructions" => "synthetic", "input" => [%{"type" => "message", "role" => "user", "content" => "synthetic"}], "client_metadata" => %{"x-codex-turn-metadata" => %{"turn_id" => "synthetic", "request_kind" => "turn"}}}
    output = for n <- Enum.take([1, 2], count), do: %{"type" => "reasoning", "id" => "rs_synthetic_#{n}", "summary" => [], "encrypted_content" => "synthetic_#{n}"}
    {:ok, digest} = WebsocketTurnIdentity.replay_claim_digest(@key, original)
    request_id = Ecto.UUID.generate()
    attempt_id = Ecto.UUID.generate()
    assignment_id = Ecto.UUID.generate()
    identity_id = Ecto.UUID.generate()
    request = %Request{id: request_id, requested_model: "synthetic-model", status: "succeeded", endpoint: "/backend-api/codex/responses", transport: transport, completed_at: @now, request_metadata: %{"native_content_filter_original" => %{"version" => 1, "digest" => Base.url_encode64(digest, padding: false)}}, native_client_retry_version: 1, native_client_retry_digest: digest, native_client_retry_auth_epoch: 1}
    turn = %CodexTurn{request_id: request_id, status: "succeeded", final_attempt_id: attempt_id, completed_at: @now}

    items =
      Enum.map(output, fn item ->
        {:ok, digest} = WebsocketTurnIdentity.completed_item_digest(item)
        digest
      end)

    progress = Enum.reduce(output, ClientRetry.new_native_http_progress(), &ClientRetry.observe_native_http_output_item(&2, &1)) |> ClientRetry.native_http_progress_metadata()
    metadata = %{"native_content_filter_terminal" => %{"version" => 1, "event_type" => "response.incomplete", "reason" => "content_filter"}, "native_content_filter_source" => %{"version" => 1, "attempt_id" => attempt_id, "assignment_id" => assignment_id, "identity_id" => identity_id, "credential_epoch" => 1, "serving_mode" => "full"}, "native_client_retry_observation" => %{"version" => 1, "authority_complete" => true, "output_item_done_count_saturated" => false, "terminal_seen" => true}, "native_http_resume_progress" => progress, "downstream_delivery" => %{"outcome" => "delivered", "terminal_class" => "response.incomplete", "incomplete_reason" => "content_filter", "transport" => transport, "completed_items" => count, "completed_item_digests" => items}}
    metadata = put_in(metadata, ["native_client_retry_observation", "output_item_done_count"], count)
    metadata = update_in(metadata, ["native_content_filter_source"], &Map.merge(&1, %{"requested_model" => "synthetic-model", "effective_model" => "synthetic-model", "upstream_model" => "synthetic-model"}))
    attempt = %Attempt{id: attempt_id, request_id: request_id, upstream_model_id: "synthetic-model", pool_upstream_assignment_id: assignment_id, upstream_identity_id: identity_id, transport: transport, status: "succeeded", completed_at: @now, replay_generation: 0, response_metadata: metadata}
    {original, output, turn, request, attempt}
  end

  defp witness(payload, mode \\ "full") do
    options = RequestOptions.build(%{}, "/backend-api/codex/responses", %{})
    options = %{options | routing: %{options.routing | model_serving_mode: mode}}
    ClientRetry.original_witness!(:crypto.hash(:sha256, "current"), 1) |> Candidates.attach(@key, payload, options)
  end

  defp append(payload, items), do: Map.update!(payload, "input", &(&1 ++ items))
  defp guidance, do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "<content_filter_guidance>\nsynthetic\n</content_filter_guidance>"}]}
end
