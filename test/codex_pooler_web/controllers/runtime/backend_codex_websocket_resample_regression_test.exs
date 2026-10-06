defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketResampleRegressionTest do
  # The websocket side of findings#311. A Codex client samples a turn again
  # when the response it read completed with `end_turn: false`; on the socket
  # that delivered that response the next request is anchored on it
  # (`previous_response_id`) and carries only the items recorded since, often
  # none. That frame was always served; the native HTTP re-sample proof
  # (`NativeResampledCompletion`) is asked only of a native HTTP SSE request,
  # so these arms pin that nothing changed on the websocket: the anchored
  # re-sample in Full and Lite with owner forwarding off and on, and the two
  # cross-transport meetings, a full-history websocket frame after a native
  # HTTP request of the turn and a native HTTP re-sample after a websocket
  # request of the turn.
  #
  # One node (the owner on this node with forwarding on), native
  # `/backend-api/codex/responses`, the Pool's serving mode forced to Full or
  # Lite, FakeUpstream answering every request, the released client's turn
  # frames (turn metadata naming thread and turn), synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, public_websocket_connect!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, receive_native_terminal!: 3, released_client_frame: 2, set_model_serving_mode!: 3, stop_websocket_owner_session: 1, with_info_log: 1]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @timeout_ms 15_000
  @poll_ms 20

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    :ok
  end

  for forwarding <- [:off, :on], mode <- ["full", "lite"], delta <- [:empty, :reminder] do
    test "forwarding #{forwarding}, #{mode}: the anchored re-sample with #{delta} delta after an end_turn=false completion is served on its socket" do
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(forwarding) == :on)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a commentary completion ending end_turn=false, then the anchored re-sample's completion on the same connection
          FakeUpstream.strict_sequence([
            upstream_request(1, [forbidden: ["previous_response_id"]], completed_frames("resp_ws_p", [provider_message("commentary", "msg_ws_p")], false)),
            upstream_request(1, [equals: %{"previous_response_id" => "resp_ws_p"}], completed_frames("resp_ws_s", [provider_message("final_answer", "msg_ws_s")], true))
          ])
        )

      setup = gateway_setup(upstream)
      stop_owners_on_exit!(setup)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      frame = released_client_frame(setup, thread)
      turn_id = Ecto.UUID.generate()
      delta = if unquote(delta) == :empty, do: [], else: [developer("<current_time_reminder>\nsynthetic time\n</current_time_reminder>")]

      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(native_text_input("synthetic websocket resample"), turn_id, %{}))
        {conn, websocket, first} = receive_native_terminal!(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_p"}} = first

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(delta, turn_id, %{"previous_response_id" => "resp_ws_p"}))
        {_conn, _websocket, second} = receive_native_terminal!(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_s"}} = second
      after
        Mint.HTTP.close(conn)
      end

      assert [predecessor, successor] = await_settled!(setup, 2)
      assert {predecessor.status, successor.status} == {"succeeded", "succeeded"}
      assert {predecessor.transport, successor.transport} == {"websocket", "websocket"}
      refute Enum.any?([predecessor, successor], &(&1.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"))
      assert FakeUpstream.count(upstream) == 2
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  # Review test 8: a full-history websocket frame of the turn after a native
  # HTTP request of it completed. The frame takes the websocket claim path,
  # whose scope carries no native HTTP transport, so the re-sample proof is
  # never asked and the outcome is the one this meeting had before.
  for mode <- ["full", "lite"] do
    test "#{mode}: a full-history websocket frame after a completed native HTTP request of the turn keeps its outcome" do
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; the native HTTP request's commentary completion ending end_turn=false
          FakeUpstream.strict_sequence([http_request(completed_events("resp_http_p", [provider_message("commentary", "msg_http_p")], false))])
        )

      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      frame = released_client_frame(setup, thread)
      turn_id = Ecto.UUID.generate()
      opener = native_text_input("synthetic cross transport resample")

      assert {200, _body} = post_http!(setup, thread, opener, turn_id, unquote(mode))
      assert [predecessor] = await_settled!(setup, 1)
      assert predecessor.transport == "http_sse"

      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)

      {terminal, logs} =
        try do
          with_info_log(fn ->
            {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(opener ++ [client_message("commentary", "msg_http_p")], turn_id, %{}))
            {_conn, _websocket, terminal} = receive_native_terminal!(conn, websocket, ref)
            terminal
          end)
        after
          Mint.HTTP.close(conn)
        end

      assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = terminal
      refute logs =~ "resample_check="
      assert [^predecessor] = await_settled!(setup, 1)
      assert FakeUpstream.count(upstream) == 1
    end
  end

  # The bare claim of a turn whose opener ran on the websocket is held by that
  # websocket request, and the native HTTP re-sample proof is never asked of
  # it. A native HTTP re-sample sent after it (a client that fell back to HTTPS
  # between two sampling requests) is served by the websocket completed-item
  # rule as before: the request is that frame with exactly the item its socket
  # pushed appended. Deferred (findings#311): the next native HTTP re-sample
  # of the turn stays refused, because the walk proves the websocket request's
  # edge again with the newer request, whose last items that socket never
  # pushed.
  for mode <- ["full", "lite"] do
    test "#{mode}: native HTTP re-samples after a websocket request of the turn: the first is served as before, the second stays refused (deferred)" do
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; the websocket request's commentary completion ending end_turn=false, then the native HTTP re-sample's
          FakeUpstream.strict_sequence([upstream_request(1, [forbidden: ["previous_response_id"]], completed_frames("resp_ws_p", [provider_message("commentary", "msg_ws_p")], false)), http_request(completed_events("resp_http_s", [provider_message("commentary", "msg_http_s")], false))])
        )

      setup = gateway_setup(upstream)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      frame = released_client_frame(setup, thread)
      turn_id = Ecto.UUID.generate()
      opener = native_text_input("synthetic cross transport resample")

      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame.(opener, turn_id, %{}))
        {_conn, _websocket, first} = receive_native_terminal!(conn, websocket, ref)
        assert %{"type" => "response.completed", "response" => %{"id" => "resp_ws_p"}} = first
      after
        Mint.HTTP.close(conn)
      end

      assert [predecessor] = await_settled!(setup, 1)
      assert predecessor.transport == "websocket"

      first_resample = opener ++ [client_message("commentary", "msg_ws_p")]
      {{status, _body}, logs} = with_info_log(fn -> post_http!(setup, thread, first_resample, turn_id, unquote(mode)) end)
      assert status == 200, "the first re-sample was refused: #{logs}"
      assert [^predecessor, served] = await_settled!(setup, 2)
      assert {served.transport, served.status} == {"http_sse", "succeeded"}
      assert served.request_metadata["client_resend"] == %{"predecessor_request_id" => predecessor.id, "reason" => "failed_predecessor"}

      {{status, body}, logs} = with_info_log(fn -> post_http!(setup, thread, first_resample ++ [client_message("commentary", "msg_http_s")], turn_id, unquote(mode)) end)
      assert status == 409
      assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
      assert logs =~ "resend_disposition=terminal_predecessor"
      refute logs =~ "resample_check="
      assert [^predecessor, ^served] = await_settled!(setup, 2)
      assert FakeUpstream.count(upstream) == 2
    end
  end

  # With owner forwarding on the session's owner outlives the socket; stop it
  # with the test, as the owner-forwarding families do. The sessions are read
  # at exit, before the sandbox owner stops.
  defp stop_owners_on_exit!(setup) do
    on_exit(fn ->
      for session_id <- Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id)), do: stop_websocket_owner_session(session_id)
    end)
  end

  defp upstream_request(connection_ordinal, json, respond) do
    FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, websocket_connection_ordinal: connection_ordinal, json: [valid: true, equals: Map.merge(%{"type" => "response.create"}, Keyword.get(json, :equals, %{}))] ++ Keyword.take(json, [:forbidden]), respond: respond)
  end

  defp http_request(events), do: FakeUpstream.expect_request(method: "POST", path: @path, respond: FakeUpstream.sse_stream(events))

  defp completed_frames(response_id, items, end_turn), do: FakeUpstream.websocket_text_frames(response_id |> completed_events(items, end_turn) |> Enum.map(fn {_event, data} -> CodexPooler.JSON.encode!(data) end))

  defp completed_events(response_id, items, end_turn) do
    response = %{"id" => response_id, "status" => "completed", "output" => items, "end_turn" => end_turn, "usage" => %{"input_tokens" => 10, "output_tokens" => 3, "total_tokens" => 13}}

    [{"response.created", %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}}}] ++
      (items |> Enum.with_index() |> Enum.map(fn {item, index} -> {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => index, "item" => item}} end)) ++
      [{"response.completed", %{"type" => "response.completed", "response" => response}}]
  end

  # The released client's HTTP body of the same request: the frame's body
  # without `type`, keyed to the socket's session through its turn state.
  defp post_http!(setup, thread, input, turn_id, mode) do
    body = released_client_frame(setup, thread).(input, turn_id, %{}) |> CodexPooler.JSON.decode!() |> Map.delete("type")

    conn =
      build_conn()
      |> put_req_header("authorization", setup.authorization)
      |> put_req_header("x-codex-turn-state", thread)
      |> put_req_header("content-type", "application/json")

    conn = if mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    conn = post(conn, @path, CodexPooler.JSON.encode!(body))
    {conn.status, conn.resp_body}
  end

  defp provider_message(phase, id),
    do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => phase, "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}", "annotations" => [], "logprobs" => []}]}

  defp client_message(phase, id), do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => phase, "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}"}]}

  defp developer(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}

  defp await_settled!(setup, count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms
    requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))

    cond do
      length(requests) == count and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_settled!(setup, count, deadline)
        end

      true ->
        flunk("the Pool's requests did not settle: #{inspect(Enum.map(requests, &{&1.status, &1.last_error_code}))}")
    end
  end
end
