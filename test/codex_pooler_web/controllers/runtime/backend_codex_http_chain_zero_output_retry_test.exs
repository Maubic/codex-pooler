defmodule CodexPoolerWeb.Runtime.BackendCodexHttpChainZeroOutputRetryTest do
  # A native HTTP SSE request that bought nothing (the provider ended it at its
  # first event) is stepped over when it is the first request of its turn
  # (findings#212 row 212-50), but once a delivered request held the turn's claim
  # the client's retry of such a request met `409 duplicate_turn`: the resend
  # policy judged the zero-output node with the HTTP stream-cut shapes, which
  # admit only `upstream_stream_error` (findings#314 row 314-1). The released
  # client resends a request whose response it never read and retries a
  # retryable first-event verdict, so a provider overload during that resend
  # failed the turn. Every request keeps its own settlement and the retry is
  # linked to the zero-output request it repeats.
  #
  # Provenance: the request shapes follow the released client's HTTP body
  # (canonical turn metadata in `client_metadata` plus its header copy); the
  # provider events, ids and texts are synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, first_event_terminal_sse: 2, gateway_setup: 1, native_text_input: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation] do
    test "#{mode} #{arm}: the retry of a resend that failed at its first event after a delivered request is served" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a delivered completion, a first-event provider failure, then a completion
          FakeUpstream.strict_sequence([delivered_sse("resp_chain_delivered"), first_event_terminal_sse("response.failed", "server_error"), delivered_sse("resp_chain_retry")])
        )

      fixture = fixture!(upstream, unquote(mode), unquote(arm))

      assert post_native(fixture, fixture.payload).resp_body =~ ~s("type":"response.completed")
      resend = post_native(fixture, fixture.payload)
      assert resend.status == 200
      assert resend.resp_body =~ "server_error"

      assert [delivered, failed] = pool_requests(fixture.setup)
      assert {delivered.status, failed.status, failed.last_error_code} == {"succeeded", "failed", "server_error"}
      assert linked?(delivered, failed)

      {retry, logs} = with_info_log(fn -> post_native(fixture, fixture.payload) end)
      assert retry.status == 200, "the retry was refused: #{inspect(rejection_lines(logs))}"
      assert retry.resp_body =~ ~s("type":"response.completed")

      assert [^delivered, ^failed, served] = pool_requests(fixture.setup)
      assert served.status == "succeeded"
      assert served.request_metadata["client_resend"]["predecessor_request_id"] == failed.id
      assert linked?(failed, served)
      assert String.starts_with?(served.correlation_id, "codex-request-retry:")
      assert FakeUpstream.count(upstream) == 3
      assert :ok = FakeUpstream.verify!(upstream)

      for request <- [delivered, failed, served] do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end
    end
  end

  # A steered continuation holds its own `codex-resume:` claim; its zero-output
  # resend was refused `authorization_changed` (an HTTP node of that arm is
  # outside the resume claim's transport scope), the same defect on another arm.
  for mode <- ["full", "lite"] do
    test "#{mode} steered continuation: the retry of a resend that failed at its first event is served" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; the opener and the steered request complete, the steered resend fails at its first event, its retry completes
          FakeUpstream.strict_sequence([
            delivered_sse("resp_chain_opener"),
            delivered_sse("resp_chain_steered"),
            first_event_terminal_sse("response.failed", "server_error"),
            delivered_sse("resp_chain_steered_retry")
          ])
        )

      fixture = fixture!(upstream, unquote(mode), :opening)
      assert post_native(fixture, fixture.payload).status == 200

      steered_input =
        fixture.payload["input"] ++
          [%{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}] ++ native_text_input("synthetic steered input")

      steered = Map.put(fixture.payload, "input", steered_input)
      assert post_native(fixture, steered).status == 200
      assert post_native(fixture, steered).resp_body =~ "server_error"

      assert [_opener, steer, failed] = pool_requests(fixture.setup)
      assert steer.request_metadata["native_http_claim_arm"] == "steered_continuation"
      assert {failed.status, failed.last_error_code} == {"failed", "server_error"}

      {retry, logs} = with_info_log(fn -> post_native(fixture, steered) end)
      assert retry.status == 200, "the retry was refused: #{inspect(rejection_lines(logs))}"
      assert [_opener, ^steer, ^failed, served] = pool_requests(fixture.setup)
      assert served.request_metadata["client_resend"]["predecessor_request_id"] == failed.id
      assert linked?(failed, served)
      assert FakeUpstream.count(upstream) == 4
    end
  end

  # The node the retry chains onto keeps its own retry window.
  for mode <- ["full", "lite"] do
    test "#{mode}: the retry of a failed resend after its retry window is refused retry_expired" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a delivered completion then a first-event provider failure
          FakeUpstream.strict_sequence([delivered_sse("resp_chain_delivered"), first_event_terminal_sse("response.failed", "server_error")])
        )

      fixture = fixture!(upstream, unquote(mode), :opening)
      assert post_native(fixture, fixture.payload).status == 200
      assert post_native(fixture, fixture.payload).status == 200
      assert [_delivered, failed] = pool_requests(fixture.setup)

      %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
      {1, _} = Repo.update_all(from(r in Request, where: r.id == ^failed.id), set: [completed_at: DateTime.add(db_now, -31, :second)])

      {refused, logs} = with_info_log(fn -> post_native(fixture, fixture.payload) end)
      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ "resend_disposition=retry_expired"
      assert FakeUpstream.count(upstream) == 2
    end
  end

  # The step-over is not a blank cheque: the zero-output request is retried only
  # by its exact copy. A request of the same turn that changed its history is
  # neither the delivered request's identical resend nor the failed request's
  # retry, and keeps the refusal it gets today.
  for mode <- ["full", "lite"] do
    test "#{mode}: a changed request after the failed resend is still refused" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a delivered completion then a first-event provider failure
          FakeUpstream.strict_sequence([delivered_sse("resp_chain_delivered"), first_event_terminal_sse("response.failed", "server_error")])
        )

      fixture = fixture!(upstream, unquote(mode), :opening)

      assert post_native(fixture, fixture.payload).status == 200
      assert post_native(fixture, fixture.payload).status == 200

      changed = Map.put(fixture.payload, "input", native_text_input("a different request of the same turn"))
      {refused, logs} = with_info_log(fn -> post_native(fixture, changed) end)

      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ "native http replay rejection stage=native_http_turn_claim reason_code=reservation_duplicate"
      assert length(pool_requests(fixture.setup)) == 2
      assert FakeUpstream.count(upstream) == 2
    end
  end

  defp fixture!(upstream, mode, arm) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    metadata = CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_chain_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})

    payload = %{
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "tools" => [],
      "parallel_tool_calls" => true,
      "input" => input(arm),
      "stream" => true,
      "store" => false,
      "client_metadata" => %{"x-codex-turn-metadata" => metadata, "thread_id" => thread, "turn_id" => "synthetic_chain_turn", "x-codex-window-id" => "#{thread}:0"}
    }

    %{setup: setup, mode: mode, thread: thread, payload: payload}
  end

  defp input(:opening), do: native_text_input("synthetic chain request")

  defp input(:tool_continuation) do
    native_text_input("synthetic chain request") ++
      [
        %{"type" => "function_call", "call_id" => "call_synthetic_chain", "name" => "synthetic_tool", "arguments" => "{}"},
        %{"type" => "function_call_output", "call_id" => "call_synthetic_chain", "output" => "synthetic tool result"}
      ]
  end

  defp post_native(fixture, payload) do
    conn =
      build_conn()
      |> auth(fixture.setup)
      |> put_req_header("session-id", fixture.thread)
      |> put_req_header("thread-id", fixture.thread)
      |> put_req_header("x-codex-window-id", "#{fixture.thread}:0")
      |> put_req_header("x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"])

    conn = if fixture.mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @path, payload)
  end

  defp delivered_sse(response_id) do
    item = %{"type" => "message", "id" => "msg_#{response_id}", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic answer", "annotations" => []}]}

    FakeUpstream.sse_stream([
      {"response.created", %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => item}},
      {"response.completed", %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}}
    ])
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp linked?(predecessor, successor),
    do: Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id and l.successor_request_id == ^successor.id))

  defp rejection_lines(logs), do: logs |> String.split("\n") |> Enum.filter(&(&1 =~ "replay rejection"))
end
