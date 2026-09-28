defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.PreviousResponseMissResendTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport
  import CodexPooler.PoolerFixtures, only: [model_fixture: 2]
  import CodexPooler.AccountingTestSupport, only: [key_usage_events: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.Accounting.RequestLifecycle.WindowUsage
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, RoutingCircuitState}
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams

  # Detection budget for a settlement the test only observes.
  @settlement_detection_timeout_ms 15_000

  @answer %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}
  @tool_call %{"type" => "function_call", "call_id" => "call_refresh_sample", "name" => "sample_lookup", "arguments" => "{}"}

  # The Codex backend's websocket refusal of an anchor the connection cannot
  # resolve (a connection that did not produce the response): a codeless 400
  # `invalid_request_error` (findings#232 row 232-277, live probe 2026-09-23).
  @provider_refusal %{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "message" => "Invalid `previous_response_id`."}}

  # The refusal reaches the native client as the `previous_response_not_found`
  # event the Pooler's own connection-bound guard sends (before, as the generic
  # retryable `response.failed` `stream_incomplete`), the attempt keeps the
  # provider's refusal, and the client's full resend of the same turn, without
  # the anchor, is served on the websocket (findings#232 row 232-278). With
  # owner forwarding off the released client met `409 duplicate_turn` on every
  # resend and finished the turn over HTTPS. Frames carry the released client's
  # turn metadata, so the resend is judged on its turn claim. The resend here
  # arrives on a new socket, as the released client sends it; the guard arm
  # below resends on the same socket.
  for forwarding? <- [false, true] do
    test "the provider's codeless anchor refusal reaches the native client as previous_response_not_found and the full resend completes (owner forwarding #{forwarding?})" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(forwarding?))

      first_input = native_text_input("anchor")
      next_input = native_text_input("next")

      upstream =
        start_upstream(
          # The anchored delta is refused as the provider refuses an anchor its
          # connection did not produce; the client's full resend completes.
          # provenance: observed findings#232 row 232-277 (provider refusal frame, live probe 2026-09-23)
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_invalid_anchor_opener", [@answer], 2, 1)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => "resp_ws_invalid_anchor_opener"}],
              respond: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(@provider_refusal)])
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_invalid_anchor_resend", [], 4, 3)
            )
          ])
        )

      setup = gateway_setup(upstream)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      port = start_public_endpoint!()
      thread = "ws-invalid-anchor-#{System.unique_integer([:positive])}"
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      frame = released_client_frame(setup, thread)
      second_turn_id = Ecto.UUID.generate()

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input, Ecto.UUID.generate(), %{}))
        {conn, websocket, opener_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_invalid_anchor_opener"}} = opener_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(next_input, second_turn_id, %{"previous_response_id" => "resp_ws_invalid_anchor_opener"}))
        {conn, _websocket, refusal_frame} = public_websocket_receive_text!(conn, websocket, ref)
        assert CodexPooler.JSON.decode!(refusal_frame) == native_previous_response_retry_event()
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "failed"}}}, @settlement_detection_timeout_ms

        # The Pooler dispatches nothing again: the anchored delta reached the
        # upstream once.
        assert [_opener, _anchored] = FakeUpstream.requests(upstream)

        # The released client closes its socket after the refusal and resends
        # the turn on a new one (its refused turn's delivery receipt reads
        # `aborted`, isolated-runtime lane of row 232-278).
        Mint.HTTP.close(conn)
        {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input ++ [@answer] ++ next_input, second_turn_id, %{}))
        {conn, _websocket, resend_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_invalid_anchor_resend"}} = resend_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        assert [_opener, _anchored, resend_request] = FakeUpstream.requests(upstream)
        assert resend_request.json["input"] == first_input ++ [@answer] ++ next_input

        assert [_opener_row, refused, resend] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
        assert {refused.status, refused.last_error_code, resend.status} == {"failed", "stream_incomplete", "succeeded"}
        assert linked_successor?(refused, resend)
        assert [refused_attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^refused.id))

        # The attempt keeps the provider's refusal: its fixed message class and
        # no code, since the provider sent none.
        assert %{
                 "upstream_error_code" => "previous_response_not_found",
                 "rejection_error_type" => "invalid_request_error",
                 "rejection_message_class" => "invalid_previous_response_id",
                 "rejection_upstream_status" => 400
               } = refused_attempt.response_metadata

        refute Map.has_key?(refused_attempt.response_metadata, "rejection_error_code")
        # The provider received the anchored request, so its usage stays
        # unknown and the reservation estimate stays provisional.
        assert_unknown_usage!(refused, refused_attempt)
        assert Repo.all(from(demotion in BridgeDemotion)) == []
        assert Repo.all(from(circuit in RoutingCircuitState)) == []
        refute inspect({refused.request_metadata, refused_attempt.response_metadata}) =~ "resp_ws_invalid_anchor_opener"
        assert :ok = FakeUpstream.verify!(upstream)
        conn
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  # The Pooler's own connection-bound guard (here a Full-to-Lite flip under the
  # anchor, findings#232 row 232-210) sends the same retry event, and the full
  # resend of that turn met the same `409 duplicate_turn` on its turn claim
  # (row 232-278).
  for forwarding? <- [false, true] do
    test "the full resend after the connection-bound guard's refusal is served on the websocket (owner forwarding #{forwarding?})" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(forwarding?))

      tools = [%{"type" => "function", "name" => "sample_lookup", "parameters" => %{"type" => "object", "properties" => %{}, "required" => []}}]
      first_input = native_text_input("anchor")
      next_input = native_text_input("next")

      upstream =
        start_upstream(
          # The anchored Lite delta is refused before it is sent; the full
          # resend opens a Lite context on the same connection.
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "tools.0.name" => "sample_lookup"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_guard_anchor_opener", [@answer], 2, 1)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "input.0.type" => "additional_tools"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_guard_anchor_resend", [], 4, 3)
            )
          ])
        )

      setup = gateway_setup(upstream)
      scope = model_serving_scope()
      revision = set_model_serving_mode!(scope, setup, "full")
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      port = start_public_endpoint!()
      thread = "ws-guard-anchor-#{System.unique_integer([:positive])}"
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      base = released_client_frame(setup, thread)
      frame = fn input, turn_id, extra -> base.(input, turn_id, Map.merge(%{"instructions" => "synthetic base instructions", "tools" => tools}, extra)) end
      second_turn_id = Ecto.UUID.generate()

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input, Ecto.UUID.generate(), %{}))
        {conn, websocket, opener_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_guard_anchor_opener"}} = opener_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        _revision = set_model_serving_mode!(scope, setup, "lite", revision)

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(next_input, second_turn_id, %{"previous_response_id" => "resp_ws_guard_anchor_opener"}))
        {conn, websocket, refusal_frame} = public_websocket_receive_text!(conn, websocket, ref)
        assert CodexPooler.JSON.decode!(refusal_frame) == native_previous_response_retry_event()
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "failed"}}}, @settlement_detection_timeout_ms
        assert [_opener] = FakeUpstream.requests(upstream)

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input ++ [@answer] ++ next_input, second_turn_id, %{}))
        {conn, _websocket, resend_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_guard_anchor_resend"}} = resend_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        assert [_opener_row, refused, resend] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
        assert {refused.status, resend.status} == {"failed", "succeeded"}
        assert linked_successor?(refused, resend)
        assert [refused_attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^refused.id))
        assert_no_provider_work!(refused, refused_attempt)
        assert :ok = FakeUpstream.verify!(upstream)
        conn
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  # An automatic refresh of the account's access token between two requests
  # of a turn changes the upstream connection's reuse key
  # (`request_key_changed`, `changed_headers=credential`), so the next request
  # gets a fresh upstream connection, where the released client's anchored
  # tool-output continuation is refused before it is sent. The client resends
  # the whole turn without the anchor on a new socket, and it completes. The
  # refusal did no provider work and settles with no usage; as unknown usage
  # its reservation estimate counted toward the key's effective tokens.
  for forwarding? <- [false, true] do
    test "an anchored continuation after an access token refresh is refused before it is sent, with no usage, and the full resend completes (owner forwarding #{forwarding?})" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(forwarding?))

      refreshed_token = "upstream-token-refreshed-#{System.unique_integer([:positive])}"
      first_input = native_text_input("refresh anchor")
      tool_output = %{"type" => "function_call_output", "call_id" => "call_refresh_sample", "output" => "sample output"}

      upstream =
        start_upstream(
          # Only the opener, under the original credential, and the full
          # resend, under the refreshed one, reach the upstream.
          # provenance: synthetic_adversarial
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              headers: [required: %{"authorization" => "Bearer upstream-token"}],
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_refresh_anchor_opener", [@tool_call], 2, 1)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              headers: [required: %{"authorization" => "Bearer #{refreshed_token}"}],
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_refresh_anchor_resend", [], 4, 3)
            )
          ])
        )

      setup = gateway_setup(upstream)
      window_start = DateTime.add(DateTime.utc_now(), -60, :second)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      port = start_public_endpoint!()
      thread = "ws-refresh-anchor-#{System.unique_integer([:positive])}"
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      frame = released_client_frame(setup, thread)
      turn_id = Ecto.UUID.generate()

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input, turn_id, %{}))
        {conn, websocket, opener_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_refresh_anchor_opener"}} = opener_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        assert {:ok, _secret} = Upstreams.store_encrypted_secret(setup.identity, %{secret_kind: "access_token", plaintext: refreshed_token})

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.([tool_output], turn_id, %{"previous_response_id" => "resp_ws_refresh_anchor_opener"}))
        {conn, _websocket, refusal_frame} = public_websocket_receive_text!(conn, websocket, ref)
        assert CodexPooler.JSON.decode!(refusal_frame) == native_previous_response_retry_event()
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "failed"}}}, @settlement_detection_timeout_ms
        assert [_opener] = FakeUpstream.requests(upstream)

        Mint.HTTP.close(conn)
        {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input ++ [@tool_call, tool_output], turn_id, %{}))
        {conn, _websocket, resend_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_refresh_anchor_resend"}} = resend_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        assert [opener_request, resend_request] = FakeUpstream.requests(upstream)
        refute opener_request.websocket_connection_id == resend_request.websocket_connection_id
        assert resend_request.json["input"] == first_input ++ [@tool_call, tool_output]

        assert [_opener_row, refused, resend] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
        # A tool-result continuation's claim is scoped to its payload, so the
        # full resend of the turn is admitted under its own claim.
        assert {refused.status, refused.last_error_code, resend.status} == {"failed", "stream_incomplete", "succeeded"}
        assert [refused_attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^refused.id))

        assert %{"reason" => "previous_response_generation_mismatch", "connection_use" => "fresh", "termination_source" => "continuation_generation_guard", "upstream_committed" => false} =
                 refused_attempt.response_metadata["transport_failure"]

        assert_no_provider_work!(refused, refused_attempt)

        # The key's token windows hold the two served requests and nothing of
        # the refusal but its admission.
        assert %{window: %{known_total_tokens: 10, provisional_total_tokens: 0, pending_total_tokens: 0, effective_total_tokens: 10, effective_request_count: 3}} =
                 WindowUsage.window_usages(setup.api_key.id, [window: window_start], DateTime.add(DateTime.utc_now(), 60, :second))

        assert Repo.all(from(demotion in BridgeDemotion)) == []
        assert Repo.all(from(circuit in RoutingCircuitState)) == []
        refute inspect({refused.request_metadata, refused_attempt.response_metadata}) =~ refreshed_token
        assert :ok = FakeUpstream.verify!(upstream)
        conn
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  # The anchored turn names a model the account cannot serve, on the
  # connection that produced its anchor: the provider resolves the anchor and
  # refuses the model with a codeless `400 invalid_request_error` whose message
  # names the model (live probe 2026-09-23, findings#232 rows 232-279/232-281).
  # It is not an anchor miss, so it is never the retry event: the client gets
  # the final wrapped refusal, and nothing is retried or dispatched again.
  for forwarding? <- [false, true] do
    test "the provider's model refusal of an anchored turn on its producing connection stays a final refusal (owner forwarding #{forwarding?})" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(forwarding?))

      first_input = native_text_input("model anchor")
      next_input = native_text_input("model next")

      upstream =
        start_upstream(
          # provenance: observed findings#232 row 232-279 (provider model refusal frame shape, live probe 2026-09-23); reply frames synthetic
          FakeUpstream.strict_sequence([
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
              respond: completed_response_frames("resp_ws_model_refusal_opener", [@answer], 2, 1)
            ),
            FakeUpstream.expect_request(
              method: "WEBSOCKET",
              path: "/backend-api/codex/responses",
              websocket_connection_ordinal: 1,
              json: [valid: true, equals: %{"type" => "response.create", "model" => "provider-gpt-example-unservable", "previous_response_id" => "resp_ws_model_refusal_opener"}],
              respond: FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(model_refusal("provider-gpt-example-unservable"))])
            )
          ])
        )

      setup = gateway_setup(upstream)
      target = unservable_model(setup)
      assert :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      port = start_public_endpoint!()
      thread = "ws-model-refusal-#{System.unique_integer([:positive])}"
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      frame = released_client_frame(setup, thread)

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(first_input, Ecto.UUID.generate(), %{}))
        {conn, websocket, opener_terminal} = receive_until_terminal(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_model_refusal_opener"}} = opener_terminal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, @settlement_detection_timeout_ms

        {conn, websocket} =
          public_websocket_send_text!(conn, websocket, ref, frame.(next_input, Ecto.UUID.generate(), %{"model" => target.exposed_model_id, "previous_response_id" => "resp_ws_model_refusal_opener"}))

        {conn, _websocket, refusal_frame} = public_websocket_receive_text!(conn, websocket, ref)
        refusal = CodexPooler.JSON.decode!(refusal_frame)
        refute refusal == native_previous_response_retry_event()
        assert %{"type" => "error", "status" => 400, "error" => %{"code" => "invalid_request", "type" => "invalid_request_error"}} = refusal
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "failed"}}}, @settlement_detection_timeout_ms

        assert [_opener, anchored] = FakeUpstream.requests(upstream)
        assert anchored.json["previous_response_id"] == "resp_ws_model_refusal_opener"
        assert [_opener_row, refused] = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
        assert {refused.status, refused.requested_model, refused.retry_count} == {"failed", target.exposed_model_id, 0}
        assert [refused_attempt] = Repo.all(from(attempt in Attempt, where: attempt.request_id == ^refused.id))
        refute refused_attempt.response_metadata["rejection_message_class"] == "invalid_previous_response_id"
        assert :ok = FakeUpstream.verify!(upstream)
        conn
      after
        Mint.HTTP.close(conn)
      end
    end
  end

  # The resend is admitted as the refused request's one successor: through the
  # owner's client-retry preflight (a retry link) or on its turn claim (the
  # predecessor recorded on the resend).
  defp linked_successor?(%Request{id: refused_id}, %Request{id: resend_id} = resend) do
    resend.request_metadata["client_resend"]["predecessor_request_id"] == refused_id or
      Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^refused_id and link.successor_request_id == ^resend_id))
  end

  # A refusal answered before anything was sent did no provider work: no usage
  # applies to it, and it adds only its admission to its key's usage buckets.
  defp assert_no_provider_work!(%Request{} = request, %Attempt{} = attempt) do
    assert {request.usage_status, attempt.usage_status} == {"not_applicable", "not_applicable"}
    assert [settlement] = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id and entry.entry_kind == "settlement"))
    assert {settlement.usage_status, settlement.total_tokens, settlement.details["estimated_from_reserve"]} == {"not_applicable", nil, false}
    assert key_usage_events(request.id) == %{known: 0, provisional: 0, admissions: 1}
  end

  defp assert_unknown_usage!(%Request{} = request, %Attempt{} = attempt) do
    assert [reservation] = Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request.id and entry.entry_kind == "reservation"))
    assert reservation.total_tokens > 0
    assert {request.usage_status, attempt.usage_status} == {"usage_unknown", "usage_unknown"}
    assert key_usage_events(request.id) == %{known: 0, provisional: reservation.total_tokens, admissions: 1}
  end

  # The Codex backend's websocket refusal of a model the ChatGPT account
  # cannot serve: codeless, frame keys `type`/`status`/`error`, error keys
  # `type`/`message` (P55 probe `provider-ws-order-probe-2.jsonl`).
  defp model_refusal(model), do: %{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "message" => "The '#{model}' model is not supported when using Codex with a ChatGPT account."}}

  defp unservable_model(setup) do
    source = Map.put(setup.model.metadata["source_assignment_models"][setup.assignment.id], "slug", "gpt-example-unservable")

    model_fixture(setup.pool, %{
      exposed_model_id: "gpt-example-unservable",
      upstream_model_id: "provider-gpt-example-unservable",
      metadata: %{"source_assignment_ids" => [setup.assignment.id], "source_assignment_models" => %{setup.assignment.id => source}}
    })
  end

  defp receive_until_terminal(conn, websocket, ref) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal when type in ["response.completed", "response.failed", "error"] -> {conn, websocket, terminal}
      _progress -> receive_until_terminal(conn, websocket, ref)
    end
  end
end
