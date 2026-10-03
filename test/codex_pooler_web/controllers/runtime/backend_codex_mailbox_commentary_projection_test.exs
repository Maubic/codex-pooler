defmodule CodexPoolerWeb.Runtime.BackendCodexMailboxCommentaryProjectionTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1, model_serving_scope: 0, set_model_serving_mode!: 3, await_succeeded_pool_requests!: 2]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.{NativeTurnContinuation, WebsocketTurnIdentity}
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true

  for mode <- ["full", "lite"] do
    test "#{mode} HTTP user-role input uses its established steered claim without mailbox proof" do
      fixture = fixture!(unquote(mode), :http)
      assert send_http(fixture, fixture.payload).status == 200
      user_payload = continuation(fixture, Map.put(commentary(), "role", "user"))
      assert NativeTurnContinuation.turn_role(user_payload) == :opening
      assert send_http(fixture, user_payload).status == 200
      assert [first, second] = requests(fixture)
      assert second.request_metadata["native_http_claim_arm"] == "steered_continuation"
      refute Map.has_key?(second.request_metadata, "client_resend")
      assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id), :count) == 0
      assert FakeUpstream.count(fixture.upstream) == 2
      assert :ok = FakeUpstream.verify!(fixture.upstream)
    end

    test "#{mode} HTTP released commentary projection is admitted with exactly one linked successor" do
      fixture = fixture!(unquote(mode), :http)
      assert send_http(fixture, fixture.payload).status == 200
      assert send_http(fixture, continuation(fixture)).status == 200
      assert_accounting!(fixture)
    end

    test "#{mode} HTTP receipts made by the old commentary projection stay refused" do
      fixture = fixture!(unquote(mode), :http)
      assert send_http(fixture, fixture.payload).status == 200
      [request] = requests(fixture)
      attempt = Repo.get_by!(Attempt, request_id: request.id)
      legacy_digest = legacy_completed_digest(provider())
      assert {:ok, current_digest} = WebsocketTurnIdentity.completed_item_digest(provider())
      refute legacy_digest == current_digest
      metadata = put_in(attempt.response_metadata, ["native_http_mailbox_prefix", "item_digests"], [legacy_digest])
      attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
      before = counts(fixture)
      {result, logs} = with_info_log(fn -> send_http(fixture, continuation(fixture)) end)
      assert result.status == 409
      assert logs =~ "mailbox_check=output_prefix"
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1
    end

    test "#{mode} HTTP retained or malformed commentary stays fenced without sends, then valid projection is admitted" do
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
  end

  for mode <- ["full", "lite"], owner? <- [false, true] do
    test "#{mode} native websocket commentary projection uses real delivery receipts (owner forwarding #{owner?})" do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(owner?))
      fixture = fixture!(unquote(mode), :websocket)
      port = start_public_endpoint!()
      headers = [{"session-id", fixture.thread}, {"thread-id", fixture.thread}, {"x-codex-window-id", "#{fixture.thread}:0"}]
      headers = if fixture.mode == "lite", do: headers ++ [{"x-openai-internal-codex-responses-lite", "true"}], else: headers
      {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, fixture.setup, Ecto.UUID.generate(), @path, headers)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(fixture.payload))
      {conn, websocket, item} = receive_event(conn, websocket, ref)
      assert item["type"] == "response.output_item.done"
      assert item["item"] == provider()
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
  end

  defp fixture!(mode, transport) do
    done = %{"type" => "response.output_item.done", "item" => provider()}
    first = [done, completed("resp_synthetic_first")]
    second = [completed("resp_synthetic_second")]
    respond = fn frames -> if transport == :http, do: FakeUpstream.sse_stream(Enum.map(frames, &{&1["type"], &1})), else: FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)) end
    # provenance: observed 0.160.0 commentary provider/client fields; synthetic ids, text, terminals and adversarial replay mutations
    upstream = start_upstream(FakeUpstream.strict_sequence([respond.(first), respond.(second)]))
    setup = gateway_setup(upstream, compact?: true)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    metadata = CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})
    payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "parallel_tool_calls" => true, "tools" => [], "input" => native_text_input("synthetic"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => metadata, "thread_id" => thread, "turn_id" => "synthetic_turn", "x-codex-window-id" => "#{thread}:0"}}
    %{setup: setup, upstream: upstream, thread: thread, mode: mode, payload: payload}
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

  defp continuation(fixture, output \\ commentary()), do: Map.update!(fixture.payload, "input", &(&1 ++ [output, mailbox()]))
  defp commentary, do: %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => "commentary", "content" => [%{"type" => "output_text", "text" => "synthetic one"}, %{"type" => "output_text", "text" => "synthetic two"}], "internal_chat_message_metadata_passthrough" => %{"executed_tool_calls" => []}}
  defp provider, do: commentary() |> Map.merge(%{"status" => "completed", "provider_extension" => "synthetic"}) |> update_in(["content", Access.all()], &Map.merge(&1, %{"annotations" => [], "logprobs" => [], "provider_extension" => "synthetic"}))
  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
  defp completed(id), do: %{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}

  # The prior producer's exact generic projection, used only as a negative
  # stored-receipt control. Discarded provider fields cannot be recovered.
  defp legacy_completed_digest(item) do
    identity = item |> Map.drop(["status", "internal_chat_message_metadata_passthrough"]) |> Map.update!("content", &Enum.map(&1, fn part -> Map.drop(part, ["annotations", "logprobs"]) end))
    secret = Application.fetch_env!(:codex_pooler, CodexPoolerWeb.Endpoint) |> Keyword.fetch!(:secret_key_base)
    key = :crypto.hash(:sha256, secret <> <<0>> <> "native_websocket_completed_item_v1")
    :crypto.mac(:hmac, :sha256, key, :erlang.term_to_binary(identity, [:deterministic])) |> Base.encode16(case: :lower) |> String.slice(0, 12)
  end

  # A user-role item starts a new input segment; it has no commentary replay
  # intent. Role inequality is covered at the completed identity boundary.
  defp invalid_outputs, do: [Map.put(commentary(), "id", "msg_other"), Map.put(commentary(), "phase", "final_answer"), put_in(commentary(), ["content", Access.at(0), "text"], "synthetic changed"), Map.update!(commentary(), "content", &Enum.reverse/1), put_in(commentary(), ["content", Access.at(0), "type"], "unknown_part"), put_in(commentary(), ["content", Access.at(0), "text"], 1)]
end
