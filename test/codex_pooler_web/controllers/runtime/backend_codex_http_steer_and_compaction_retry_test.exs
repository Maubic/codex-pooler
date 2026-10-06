defmodule CodexPoolerWeb.Runtime.BackendCodexHttpSteerAndCompactionRetryTest do
  # Two requests the released Codex client (0.156.1, and Desktop 0.155.0-alpha.16.3)
  # sends over native HTTP once a session has fallen back from websockets to
  # HTTPS, which it does for the rest of the session:
  #
  #   * user input steered into a running turn is drained into the SAME turn,
  #     under the same `turn_id`, before the next model request
  #     (`session/turn_input.rs` `steer_input` returns the active turn's id,
  #     `session/turn.rs` drains pending input at the top of the sampling loop,
  #     and right after a mid-turn compaction when the model needed no follow-up:
  #     `can_drain_pending_input = !model_needs_follow_up`). That request ends
  #     with a user message and derived the turn's own `codex-turn:` claim, so it
  #     was refused `409 duplicate_turn` (findings#206 row 206-403);
  #   * a remote compaction whose `response.completed` the client never read is
  #     retried with the same prompt, up to twice (`compact_remote_v2.rs`
  #     `MAX_REMOTE_COMPACTION_V2_STREAM_RETRIES`); the retry met its own claim
  #     and was refused, and three refusals fail the turn and lose the compaction
  #     (findings#206 row 206-404).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true

  @session_header "session-id"
  @metadata_key "x-codex-turn-metadata"
  @lite_header "x-openai-internal-codex-responses-lite"

  for mode <- ["full", "lite"] do
    @mode mode

    test "a steer drained right after a mid-turn compaction is served as a later request of its turn, and its resend is refused (#{mode})",
         %{conn: conn} do
      upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_open"), compaction_sse("resp_compaction"), turn_sse("resp_steer")]))
      setup = setup!(upstream, @mode)
      ids = ids()

      open = native_text_input("open the turn")
      assert response(post(conn, setup, ids, "turn", open), 200)
      assert response(post(conn, setup, ids, "compaction", open ++ [assistant("working"), %{"type" => "compaction_trigger"}]), 200)

      steered = open ++ [compaction_item("mid-turn"), user("steer the running turn")]
      assert response(post(conn, setup, ids, "turn", steered, 1), 200)

      # The steered request rebuilt for a retry: model output appended, the
      # user's progress unchanged. It is the same request and stays fenced.
      assert %{"error" => %{"code" => "duplicate_turn"}} =
               json_response(post(conn, setup, ids, "turn", steered ++ [assistant("partial answer")], 1), 409)

      assert FakeUpstream.count(upstream) == 3

      assert [
               {"codex-turn", "opening", "succeeded"},
               {"codex-request", "compaction", "succeeded"},
               {"codex-resume", "steered_continuation", "succeeded"}
             ] = rows(setup)
    end

    test "an HTTP compaction whose reply the client never read is chained on each of its retries (#{mode})", %{conn: conn} do
      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            turn_sse("resp_open"),
            compaction_sse("resp_compaction_one"),
            compaction_sse("resp_compaction_two"),
            compaction_sse("resp_compaction_three")
          ])
        )

      setup = setup!(upstream, @mode)
      ids = ids()
      open = native_text_input("open the turn")
      compaction = open ++ [assistant("working"), %{"type" => "compaction_trigger"}]

      assert response(post(conn, setup, ids, "turn", open), 200)

      # The client's first attempt and its two retries with the same prompt.
      for _attempt <- 1..3, do: assert(response(post(conn, setup, ids, "compaction", compaction), 200))

      assert FakeUpstream.count(upstream) == 4
      assert [_open, first, second, third] = pool_requests(setup)
      assert Enum.map([first, second, third], & &1.request_metadata["native_http_claim_arm"]) == ["compaction", "compaction", "compaction"]
      assert String.starts_with?(first.correlation_id, "codex-request:")

      # Each retry is one successor of the attempt before it, with its own
      # single settlement.
      assert second.request_metadata["client_resend"] == %{"predecessor_request_id" => first.id, "reason" => "failed_predecessor"}
      assert third.request_metadata["client_resend"] == %{"predecessor_request_id" => second.id, "reason" => "failed_predecessor"}

      for request <- [first, second, third] do
        assert Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request.id and e.entry_kind == "settlement"), :count) == 1
      end
    end
  end

  # The released client notices a reply it lost without a close only when its
  # stream idle timeout fires, 300 s after the last event it read, and then
  # retries the compaction with the same prompt. The retry is chained within
  # the compaction window, 330 s, as over the websocket; it used to meet the
  # ordinary 30 s window and was refused `409 duplicate_turn` like the retry
  # outside the window here, and three refusals fail the turn and lose the
  # compaction (findings#270 row 270-373). The first attempt's completion is
  # moved back, so the retry lands a margin inside or outside the window
  # whatever the machine's speed.
  test "an HTTP compaction retried 300 s after its reply was lost is chained", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_open"), compaction_sse("resp_compaction_one"), compaction_sse("resp_compaction_two")]))
    {setup, ids, compaction, first} = lost_http_compaction!(conn, upstream, 300)

    assert response(post(conn, setup, ids, "compaction", compaction), 200)
    assert [_open, %Request{id: first_id}, retry] = pool_requests(setup)
    assert first_id == first.id
    assert retry.request_metadata["client_resend"] == %{"predecessor_request_id" => first.id, "reason" => "failed_predecessor"}
    assert String.starts_with?(retry.correlation_id, "codex-request-retry:")
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "an HTTP compaction retried 325 s after its reply was lost is chained", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_open"), compaction_sse("resp_compaction_one"), compaction_sse("resp_compaction_two")]))
    {setup, ids, compaction, first} = lost_http_compaction!(conn, upstream, 325)

    assert response(post(conn, setup, ids, "compaction", compaction), 200)
    assert [_open, %Request{id: first_id}, retry] = pool_requests(setup)
    assert first_id == first.id
    assert retry.request_metadata["client_resend"] == %{"predecessor_request_id" => first.id, "reason" => "failed_predecessor"}
    assert String.starts_with?(retry.correlation_id, "codex-request-retry:")
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "an HTTP compaction retried 335 s after its reply was lost is refused", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_open"), compaction_sse("resp_compaction_one")]))
    {setup, ids, compaction, _first} = lost_http_compaction!(conn, upstream, 335)

    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(post(conn, setup, ids, "compaction", compaction), 409)
    assert length(pool_requests(setup)) == 2
    assert :ok = FakeUpstream.verify!(upstream)
  end

  # The provider cuts the compaction at each stage of its measured stream
  # (before any output, after `response.compaction.compacting`, after the
  # closed item), by a dropped connection or by a stream that ends without a
  # terminal, with owner forwarding off and on. The Pooler writes the client
  # nothing of a compaction before its terminal, so the cut answers 502 with no
  # provider output delivered: the claim walk steps over it, the identical
  # resend is served once under the derived claim, and the resend of that
  # resend's lost reply chains onto it. A native HTTP compaction keeps no
  # client-retry observation, so no stage changes the outcome through one.
  for forwarding <- [false, true], {ending, code} <- [{:abrupt, "upstream_stream_error"}, {:clean, "invalid_compaction_response"}], {stage, frames} <- [{:before_output, 2}, {:after_compacting, 4}, {:after_done, 5}] do
    test "an HTTP compaction the provider cut #{stage} (#{ending}, forwarding #{forwarding}) is served once on its resend", %{conn: conn} do
      assert provider_cut_compaction!(conn, unquote(forwarding), unquote(ending), unquote(frames)) == %{
               cut: {502, "failed", unquote(code)},
               observation: %{},
               resends: [200, 200],
               claims: ["codex-turn", "codex-request", "codex-request-retry", "codex-request-retry"],
               first_resend_linked?: false,
               second_resend_predecessor: :first_resend,
               settlements: [1, 1, 1, 1],
               upstream_requests: 4
             }
    end
  end

  defp provider_cut_compaction!(conn, forwarding?, ending, frames) do
    put_owner_forwarding!(forwarding?)
    events = Enum.take(measured_compaction_events(), frames)
    cut = if ending == :abrupt, do: FakeUpstream.abrupt_close_mid_stream(events), else: FakeUpstream.sse_stream(events, done: false)
    # provenance: synthetic_adversarial; the provider's measured compaction stream cut after the given event count, then the released client's two identical HTTPS resends
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_open"), cut, compaction_sse("resp_compaction_two"), compaction_sse("resp_compaction_three")]))
    setup = setup!(upstream, "full")
    ids = ids()
    open = native_text_input("open the turn")
    compaction = open ++ [assistant("working"), %{"type" => "compaction_trigger"}]

    assert response(post(conn, setup, ids, "turn", open), 200)
    cut_status = post(conn, setup, ids, "compaction", compaction).status
    resends = for _retry <- 1..2, do: post(conn, setup, ids, "compaction", compaction).status
    [_open, cut_request, first_resend, second_resend] = requests = pool_requests(setup)
    [cut_attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^cut_request.id))
    second_predecessor = get_in(second_resend.request_metadata, ["client_resend", "predecessor_request_id"])

    %{
      cut: {cut_status, cut_request.status, cut_request.last_error_code},
      observation: Map.take(cut_attempt.response_metadata, ["native_client_retry_observation", "native_client_retry_authority_loss"]),
      resends: resends,
      claims: Enum.map(requests, &(&1.correlation_id |> String.split(":") |> hd())),
      first_resend_linked?: Map.has_key?(first_resend.request_metadata, "client_resend"),
      second_resend_predecessor: if(second_predecessor == first_resend.id, do: :first_resend, else: second_predecessor),
      settlements: Enum.map(requests, &Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^&1.id and e.entry_kind == "settlement"), :count)),
      upstream_requests: FakeUpstream.count(upstream)
    }
  end

  # The provider's compaction stream as measured on the wire, with synthetic
  # ciphertexts of the measured lengths (the announcement's 996 bytes, the
  # closed item's 1252); the completed response lists nothing.
  defp measured_compaction_events do
    announced = %{"type" => "compaction", "id" => nil, "encrypted_content" => "gAAAAA-announced-" <> String.duplicate("a", 979)}
    closed = %{announced | "encrypted_content" => "gAAAAA-closed-" <> String.duplicate("c", 1238)}
    opening = %{"id" => "resp_cut", "status" => "in_progress", "output" => []}
    completed = %{"id" => "resp_cut", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => 2, "total_tokens" => 7}}

    Enum.map(
      [
        %{"type" => "response.created", "response" => opening},
        %{"type" => "response.in_progress", "response" => opening},
        %{"type" => "response.output_item.added", "output_index" => 0, "item" => announced},
        %{"type" => "response.compaction.compacting", "output_index" => 0},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => closed},
        %{"type" => "response.completed", "response" => completed}
      ],
      &{&1["type"], &1}
    )
  end

  defp put_owner_forwarding!(enabled?) do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)

    on_exit(fn ->
      case previous do
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
      end
    end)
  end

  defp lost_http_compaction!(conn, upstream, age) do
    setup = setup!(upstream, "full")
    ids = ids()
    open = native_text_input("open the turn")
    compaction = open ++ [assistant("working"), %{"type" => "compaction_trigger"}]

    assert response(post(conn, setup, ids, "turn", open), 200)
    assert response(post(conn, setup, ids, "compaction", compaction), 200)
    [_open, first] = pool_requests(setup)
    backdate_completion!(first, age)
    {setup, ids, compaction, first}
  end

  test "a steer into an uncompacted turn and its identical resend are served, while an unverified rebuilt opener stays refused", %{conn: conn} do
    upstream = start_upstream(FakeUpstream.strict_sequence([turn_sse("resp_open"), turn_sse("resp_steer"), turn_sse("resp_steer_resend")]))
    setup = setup!(upstream, "full")
    ids = ids()
    open = native_text_input("open the turn")

    assert response(post(conn, setup, ids, "turn", open), 200)

    # Control: the opener rebuilt with its delivered answer is not a steer.
    assert %{"error" => %{"code" => "duplicate_turn"}} =
             json_response(post(conn, setup, ids, "turn", open ++ [assistant("delivered answer")]), 409)

    steered = open ++ [assistant("delivered answer"), user("steer the running turn")]
    assert response(post(conn, setup, ids, "turn", steered), 200)

    assert response(post(conn, setup, ids, "turn", steered), 200)
    assert FakeUpstream.count(upstream) == 3
    assert [{"codex-turn", "opening", "succeeded"}, {"codex-resume", "steered_continuation", "succeeded"}, {"codex-request-retry", "steered_continuation", "succeeded"}] = rows(setup)
    [_opening, predecessor, successor] = pool_requests(setup)
    assert Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor.id and link.successor_request_id == ^successor.id))
    assert :ok = FakeUpstream.verify!(upstream)
  end

  defp setup!(upstream, mode) do
    setup = gateway_setup(upstream, compact?: true)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    Map.put(setup, :serving_mode, mode)
  end

  defp ids, do: %{turn: "turn-" <> unique_suffix(), thread: Ecto.UUID.generate()}

  # The released client's HTTP request (P69 wire capture of 0.156.1): a JSON
  # body carrying the canonical document, `stream: true`, the document echoed
  # as `x-codex-turn-metadata`, the thread as `session-id`, the current window
  # as `x-codex-window-id`, and in Lite the marker moved to a header. The window
  # advances after every compaction the client completes.
  defp post(conn, setup, ids, kind, input, window \\ 0) do
    document =
      CodexPooler.JSON.encode!(%{
        "session_id" => ids.thread,
        "thread_id" => ids.thread,
        "turn_id" => ids.turn,
        "window_id" => "#{ids.thread}:#{window}",
        "request_kind" => kind
      })

    payload = %{
      "model" => setup.model.exposed_model_id,
      "input" => input,
      "stream" => true,
      "client_metadata" => %{@metadata_key => document}
    }

    conn
    |> recycle()
    |> auth(setup)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", "text/event-stream")
    |> put_req_header(@session_header, ids.thread)
    |> put_req_header("thread-id", ids.thread)
    |> put_req_header("x-codex-window-id", "#{ids.thread}:#{window}")
    |> put_req_header(@metadata_key, document)
    |> put_req_header("originator", "codex_cli_rs")
    |> then(&if setup.serving_mode == "lite", do: put_req_header(&1, @lite_header, "true"), else: &1)
    |> post("/backend-api/codex/responses", CodexPooler.JSON.encode!(payload))
  end

  defp turn_sse(id) do
    FakeUpstream.sse_stream([
      {"response.completed", %{"type" => "response.completed", "response" => %{"id" => id, "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}}
    ])
  end

  defp compaction_sse(id) do
    FakeUpstream.compaction_stream(%{
      "id" => id,
      "output" => [compaction_item(id)],
      "usage" => %{"input_tokens" => 5, "output_tokens" => 2, "total_tokens" => 7}
    })
  end

  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}
  defp assistant(text), do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}
  defp compaction_item(label), do: %{"type" => "compaction", "encrypted_content" => "synthetic-compaction-" <> label}

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: r.admitted_at))

  # The request's completion, `seconds` before the database's now: its retry
  # window starts there (`ClientRetry.retry_window_start/3`).
  defp backdate_completion!(%Request{id: request_id}, seconds) do
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()")
    completed_at = DateTime.add(now, -seconds, :second)
    {1, _} = Repo.update_all(from(request in Request, where: request.id == ^request_id), set: [completed_at: completed_at])
    {1, _} = Repo.update_all(from(turn in CodexPooler.Gateway.Persistence.CodexTurn, where: turn.request_id == ^request_id), set: [completed_at: completed_at])
    {_attempts, _} = Repo.update_all(from(attempt in CodexPooler.Accounting.Attempt, where: attempt.request_id == ^request_id), set: [completed_at: completed_at])
    :ok
  end

  defp rows(setup) do
    for request <- pool_requests(setup) do
      {request.correlation_id |> String.split(":") |> hd(), request.request_metadata["native_http_claim_arm"], request.status}
    end
  end
end
