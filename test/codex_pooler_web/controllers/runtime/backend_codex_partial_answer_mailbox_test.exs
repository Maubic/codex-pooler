defmodule CodexPoolerWeb.Runtime.BackendCodexPartialAnswerMailboxTest do
  # Mailbox mail can stop a native Codex client after a completed `partial_answer` message (Codex 989c01a41 /
  # 822e58cc3: nonterminal and mailbox-preemptible, unlike a `final_answer`). The client records the item through its
  # typed `ResponseItem::Message` model and resends it, with the mail, as the grown request; Pooler admits that resend
  # as one linked successor only when the item it names is the item the provider pushed (findings#306, findings#307).
  # Provenance: the partial-answer provider and client fields are source-derived from codex-rs/protocol at 8b6bb1c77
  # and the observed 0.160.0 commentary serialization (`backend_codex_mailbox_commentary_projection_test.exs`); no
  # released client sends the phase yet. Ids, text, terminals and the adversarial replay mutations are synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1, model_serving_scope: 0, set_model_serving_mode!: 3, await_succeeded_pool_requests!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true

  for mode <- ["full", "lite"] do
    test "#{mode} HTTP altered partial answers stay fenced without sends, then the released projection is admitted with one linked successor" do
      fixture = fixture!(unquote(mode), :http)
      assert send_http(fixture, fixture.payload).status == 200
      before = counts(fixture)

      for output <- invalid_outputs() do
        candidate = continuation(fixture, output)
        {result, logs} = with_info_log(fn -> send_http(fixture, candidate) end)
        assert json_response(result, 409)["error"]["code"] == "duplicate_turn"
        assert logs =~ "mailbox_check="
        assert counts(fixture) == before
        assert FakeUpstream.count(fixture.upstream) == 1
      end

      assert send_http(fixture, continuation(fixture)).status == 200
      assert_accounting!(fixture)
    end

    test "#{mode} HTTP a delivered final answer before incoming mail is terminal and stays fenced" do
      fixture = fixture!(unquote(mode), :http, "final_answer")
      assert send_http(fixture, fixture.payload).status == 200
      before = counts(fixture)

      {result, logs} = with_info_log(fn -> send_http(fixture, continuation(fixture)) end)
      assert json_response(result, 409)["error"]["code"] == "duplicate_turn"
      assert logs =~ "mailbox_check=no_candidate"
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
    end
  end

  for mode <- ["full", "lite"], owner? <- [false, true] do
    test "#{mode} native websocket partial-answer projection uses real delivery receipts (owner forwarding #{owner?})" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(owner?))
      fixture = fixture!(unquote(mode), :websocket)
      {conn, websocket, ref} = connect!(fixture)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(fixture.payload))
      {conn, websocket, item} = receive_event(conn, websocket, ref)
      assert item["type"] == "response.output_item.done"
      assert item["item"] == provider("partial_answer")
      {conn, websocket, completed} = receive_event(conn, websocket, ref)
      assert completed["type"] == "response.completed"
      await_succeeded_pool_requests!(fixture.setup.pool.id, 1)
      before = counts(fixture)

      {conn, websocket} =
        Enum.reduce(invalid_outputs(), {conn, websocket}, fn output, {conn, websocket} ->
          {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(continuation(fixture, output)))
          {conn, websocket, refusal} = receive_event(conn, websocket, ref)
          assert refusal["type"] == "error"
          assert refusal["error"]["code"] == "duplicate_turn"
          assert counts(fixture) == before
          assert FakeUpstream.count(fixture.upstream) == 1
          assert Mint.HTTP.open?(conn)
          {conn, websocket}
        end)

      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(continuation(fixture)))
      {conn, _websocket, completed} = receive_event(conn, websocket, ref)
      assert completed["type"] == "response.completed"
      await_succeeded_pool_requests!(fixture.setup.pool.id, 2)
      assert_accounting!(fixture)
      Mint.HTTP.close(conn)
    end

    if not owner? do
      test "#{mode} native websocket a delivered final answer before incoming mail is terminal and stays fenced" do
        fixture = fixture!(unquote(mode), :websocket, "final_answer")
        {conn, websocket, ref} = connect!(fixture)
        on_exit(fn -> Mint.HTTP.close(conn) end)
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(fixture.payload))
        {conn, websocket, item} = receive_event(conn, websocket, ref)
        assert item["item"] == provider("final_answer")
        {conn, websocket, completed} = receive_event(conn, websocket, ref)
        assert completed["type"] == "response.completed"
        await_succeeded_pool_requests!(fixture.setup.pool.id, 1)
        before = counts(fixture)

        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(continuation(fixture)))
        {conn, _websocket, refusal} = receive_event(conn, websocket, ref)
        assert refusal["type"] == "error"
        assert refusal["error"]["code"] == "duplicate_turn"
        assert counts(fixture) == before
        assert FakeUpstream.count(fixture.upstream) == 1
        assert Mint.HTTP.open?(conn)
        Mint.HTTP.close(conn)
      end
    end
  end

  defp fixture!(mode, transport, phase \\ "partial_answer") do
    done = %{"type" => "response.output_item.done", "item" => provider(phase)}
    first = [done, completed("resp_synthetic_first")]
    second = [completed("resp_synthetic_second")]
    respond = fn frames -> if transport == :http, do: FakeUpstream.sse_stream(Enum.map(frames, &{&1["type"], &1})), else: FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)) end
    upstream = start_upstream(FakeUpstream.strict_sequence([respond.(first), respond.(second)]))
    setup = gateway_setup(upstream, compact?: true)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    metadata = CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})
    payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "parallel_tool_calls" => true, "tools" => [], "input" => native_text_input("synthetic"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => metadata, "thread_id" => thread, "turn_id" => "synthetic_turn", "x-codex-window-id" => "#{thread}:0"}}
    %{setup: setup, upstream: upstream, thread: thread, mode: mode, payload: payload, phase: phase}
  end

  defp connect!(fixture) do
    port = start_public_endpoint!()
    headers = [{"session-id", fixture.thread}, {"thread-id", fixture.thread}, {"x-codex-window-id", "#{fixture.thread}:0"}]
    headers = if fixture.mode == "lite", do: headers ++ [{"x-openai-internal-codex-responses-lite", "true"}], else: headers
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, fixture.setup, Ecto.UUID.generate(), @path, headers)
    {conn, websocket, ref}
  end

  defp receive_event(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    {conn, websocket, CodexPooler.JSON.decode!(text)}
  end

  defp send_http(fixture, payload) do
    conn = build_conn() |> auth(fixture.setup) |> put_req_header("session-id", fixture.thread) |> put_req_header("thread-id", fixture.thread) |> put_req_header("x-codex-window-id", "#{fixture.thread}:0") |> put_req_header("x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"])
    conn = if fixture.mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @path, payload)
  end

  defp assert_accounting!(fixture) do
    assert FakeUpstream.count(fixture.upstream) == 2
    assert :ok = FakeUpstream.verify!(fixture.upstream)
    assert [first, second] = requests(fixture)
    assert second.request_metadata["client_resend"]["predecessor_request_id"] == first.id
    assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^second.id), :count) == 1
    assert Repo.get_by!(CodexTurn, request_id: first.id).codex_session_id == Repo.get_by!(CodexTurn, request_id: second.id).codex_session_id

    for request <- [first, second] do
      assert request.status == "succeeded"
      assert request.request_metadata["routing"]["model_serving_mode"] == fixture.mode
      assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
      kinds = Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id, select: l.entry_kind))
      assert Enum.frequencies(kinds) == %{"reservation" => 1, "release" => 1, "settlement" => 1}
      refute Map.has_key?(request.request_metadata, "mailbox_check")
    end
  end

  defp requests(fixture), do: Repo.all(from r in Request, where: r.pool_id == ^fixture.setup.pool.id, order_by: [asc: r.admitted_at])

  defp counts(fixture) do
    ids = from r in Request, where: r.pool_id == ^fixture.setup.pool.id, select: r.id
    %{requests: Repo.aggregate(ids, :count), attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in subquery(ids)), :count), turns: Repo.aggregate(from(t in CodexTurn, where: t.request_id in subquery(ids)), :count), ledger: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in subquery(ids)), :count), links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in subquery(ids)), :count)}
  end

  defp continuation(fixture, output \\ nil), do: Map.update!(fixture.payload, "input", &(&1 ++ [output || client_item(fixture.phase), mailbox()]))

  # What the released client resends for the item the provider pushed: only the fields its typed message model keeps,
  # plus the internal metadata it adds.
  defp client_item(phase), do: %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => phase, "content" => [%{"type" => "output_text", "text" => "synthetic one"}, %{"type" => "output_text", "text" => "synthetic two"}], "internal_chat_message_metadata_passthrough" => %{"executed_tool_calls" => []}}
  defp provider(phase), do: client_item(phase) |> Map.merge(%{"status" => "completed", "provider_extension" => "synthetic"}) |> update_in(["content", Access.all()], &Map.merge(&1, %{"annotations" => [], "logprobs" => [], "provider_extension" => "synthetic"}))
  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
  defp completed(id), do: %{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}

  defp invalid_outputs do
    item = client_item("partial_answer")
    [Map.put(item, "id", "msg_other"), Map.put(item, "phase", "commentary"), Map.put(item, "phase", "final_answer"), put_in(item, ["content", Access.at(0), "text"], "synthetic changed"), Map.update!(item, "content", &Enum.reverse/1), put_in(item, ["content", Access.at(0), "type"], "unknown_part"), put_in(item, ["content", Access.at(0), "text"], 1)]
  end
end
