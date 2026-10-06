defmodule CodexPoolerWeb.V1.ResponsesPartialAnswerReplayTest do
  # The Codex Responses model gained an assistant message phase `partial_answer` (stable answer text that may be
  # followed by more output or tools; Codex 8b6bb1c77, in no released client yet). An SDK or agent client that kept
  # such an item from a `/v1/responses` result replays it as assistant history. Replay validation used to accept
  # only `commentary`, `final_answer` or no phase, so that replay was a 400 "input item shape is not translatable"
  # (findings#306). The provider accepted the phase on a stateless assistant input item in the Full and Lite request
  # shapes (direct probe, 2026-10-06), so the adapter forwards it unchanged on both serving modes, streaming and not.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      auth: 2,
      gateway_setup: 1,
      public_websocket_connect_with_request_headers!: 5,
      public_websocket_receive_text!: 3,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @answer "synthetic stable answer text"

  for mode <- ["full", "lite"], stream? <- [true, false] do
    test "#{mode} serving, stream #{stream?}: a replayed partial_answer item reaches the upstream with its phase" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_partial_answer_forward", [message("msg_after_partial", "final_answer", "synthetic closing text")])]))
      setup = serving_setup(upstream, unquote(mode))

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => unquote(stream?), "input" => history("partial_answer")})

      assert conn.status == 200
      assert conn.resp_body =~ "resp_partial_answer_forward"
      assert [captured] = FakeUpstream.requests(upstream)
      assert [%{"type" => "message", "role" => "assistant", "phase" => "partial_answer", "content" => [%{"type" => "output_text", "text" => @answer}]}] = Enum.filter(captured.json["input"], &(&1["role"] == "assistant"))
      assert [%{status: "succeeded"}] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    end
  end

  for phase <- ["commentary", "final_answer"] do
    test "a replayed #{phase} item keeps reaching the upstream with its phase" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_phase_control_#{unquote(phase)}", [message("msg_after_control", "final_answer", "synthetic closing text")])]))
      setup = gateway_setup(upstream)

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => history(unquote(phase))})

      assert conn.status == 200
      assert [captured] = FakeUpstream.requests(upstream)
      assert [%{"phase" => unquote(phase)}] = Enum.filter(captured.json["input"], &(&1["role"] == "assistant"))
    end
  end

  test "an item the upstream tagged partial_answer round-trips through the public stream and replays" do
    partial = message("msg_partial_round_trip", "partial_answer", @answer)
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_partial_first", [partial])]))
    setup = gateway_setup(upstream)

    first = post_responses(setup, %{"model" => setup.model.exposed_model_id, "input" => "synthetic first turn", "stream" => true})
    assert first.status == 200
    assert [%{"type" => "message", "role" => "assistant", "phase" => "partial_answer"} = public_item] = completed_output!(first.resp_body)

    replay_upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_partial_second", [message("msg_after_round_trip", "final_answer", "synthetic closing text")])]))
    replay_setup = gateway_setup(replay_upstream)

    replay = post_responses(replay_setup, %{"model" => replay_setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => [user("synthetic first turn"), public_item, user("synthetic follow-up")]})

    assert replay.status == 200
    assert [captured] = FakeUpstream.requests(replay_upstream)
    assert [%{"phase" => "partial_answer", "content" => [%{"type" => "output_text", "text" => @answer}]}] = Enum.filter(captured.json["input"], &(&1["role"] == "assistant"))
  end

  test "the /v1 responses websocket accepts a replayed partial_answer item and forwards its phase" do
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_partial_answer_ws", [message("msg_after_partial_ws", "final_answer", "synthetic closing text")])], done: false))
    setup = gateway_setup(upstream)
    port = start_public_endpoint!()
    headers = [{"openai-beta", "responses_websockets=2026-02-06"}]
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, "partial-answer-ws-#{System.unique_integer([:positive])}", "/v1/responses", headers)
    on_exit(fn -> Mint.HTTP.close(conn) end)

    frame = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => history("partial_answer"), "store" => false, "generate" => true}
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(frame))
    {_conn, _websocket, events} = receive_until_terminal(conn, websocket, ref, [])

    assert List.last(events)["type"] == "response.completed"
    assert [captured] = FakeUpstream.requests(upstream)
    assert [%{"phase" => "partial_answer", "content" => [%{"type" => "output_text", "text" => @answer}]}] = Enum.filter(captured.json["input"], &(&1["role"] == "assistant"))
  end

  for phase <- ["progress", "partial-answer", "Partial_Answer", ""] do
    test "an assistant item whose phase is #{inspect(phase)} is still refused before any upstream work" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_never", [message("msg_never", "final_answer", "synthetic never sent")])]))
      setup = gateway_setup(upstream)

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => history(unquote(phase))})

      assert %{"error" => %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "input", "message" => "input item shape is not translatable"}} = json_response(conn, 400)
      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 0
    end
  end

  defp receive_until_terminal(conn, websocket, ref, events) when length(events) < 8 do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    event = CodexPooler.JSON.decode!(text)
    events = events ++ [event]

    if event["type"] in ["response.completed", "response.failed", "error"],
      do: {conn, websocket, events},
      else: receive_until_terminal(conn, websocket, ref, events)
  end

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_responses(setup, payload), do: build_conn() |> auth(setup) |> post("/v1/responses", payload)

  defp history(phase), do: [user("synthetic first turn"), %{"role" => "assistant", "phase" => phase, "content" => [%{"type" => "output_text", "text" => @answer}]}, user("synthetic follow-up")]

  defp user(text), do: %{"role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp message(id, phase, text) do
    %{"type" => "message", "id" => id, "role" => "assistant", "status" => "completed", "phase" => phase, "content" => [%{"type" => "output_text", "text" => text, "annotations" => []}]}
  end

  defp completed(response_id, output) do
    {"response.completed",
     %{
       "type" => "response.completed",
       "response" => %{
         "id" => response_id,
         "object" => "response",
         "created_at" => 1_790_000_000,
         "model" => "provider-gpt-test-model",
         "status" => "completed",
         "output" => output,
         "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
       }
     }}
  end

  defp completed_output!(body) do
    body
    |> String.split("\n\n", trim: true)
    |> Enum.find_value(fn block ->
      fields = block |> String.split("\n") |> Map.new(&List.to_tuple(String.split(&1, ": ", parts: 2)))

      case fields do
        %{"event" => "response.completed", "data" => data} -> CodexPooler.JSON.decode!(data)["response"]["output"]
        _other -> nil
      end
    end) || flunk("no response.completed event in the public stream")
  end
end
