defmodule CodexPoolerWeb.Runtime.BackendCodexMailboxSerializationTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1, model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true, mailbox_serialization: true

  for mode <- ["full", "lite"], {boundary, stage} <- [instructions: :witness, parallel_tool_calls: :witness, tool_catalogue: :witness, mcp_catalogue: :witness, no_candidate: :no_candidate, historical_ending: :ending, retained_content: :output_prefix] do
    @tag mailbox_serialization_negative: true
    test "#{mode} HTTP #{boundary} reports #{stage} without sends or ledger writes, then the valid serialization is admitted" do
      fixture = fixture!(unquote(mode), false)
      continuation = continuation(fixture)
      before = counts(fixture)
      {result, logs} = with_info_log(fn -> send_payload(fixture, change(continuation, unquote(boundary))) end)
      assert json_response(result, 409)["error"]["code"] == "duplicate_turn"
      assert logs =~ "stage=native_http_turn_claim"
      assert logs =~ "mailbox_check=#{unquote(stage)}"
      assert counts(fixture) == before
      assert FakeUpstream.count(fixture.upstream) == 1

      assert send_payload(fixture, continuation).status == 200
      assert FakeUpstream.count(fixture.upstream) == 2
      [first, second] = requests(fixture)
      assert second.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^second.id), :count) == 1
      assert Repo.get_by!(CodexTurn, request_id: first.id).codex_session_id == Repo.get_by!(CodexTurn, request_id: second.id).codex_session_id

      for request <- [first, second] do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
        refute Map.has_key?(request.request_metadata, "mailbox_check")
      end
    end
  end

  for mode <- ["full", "lite"] do
    @tag mailbox_serialization_negative: true
    test "#{mode} HTTP actual client commentary projection of an unknown provider field is admitted" do
      fixture = fixture!(unquote(mode), true)
      {result, logs} = with_info_log(fn -> send_payload(fixture, continuation(fixture)) end)
      assert result.status == 200
      refute logs =~ "mailbox_check=output_prefix"
      assert FakeUpstream.count(fixture.upstream) == 2
      [first, second] = requests(fixture)
      assert second.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^second.id), :count) == 1
    end
  end

  defp fixture!(mode, extra?) do
    serialized = %{"type" => "message", "id" => "msg_synthetic", "role" => "assistant", "phase" => "commentary", "content" => [%{"type" => "output_text", "text" => "synthetic commentary"}], "internal_chat_message_metadata_passthrough" => %{"executed_tool_calls" => []}}
    emitted = serialized |> Map.put("status", "completed") |> put_in(["content", Access.at(0), "annotations"], []) |> put_in(["content", Access.at(0), "logprobs"], [])
    emitted = if extra?, do: Map.put(emitted, "provider_extension", "synthetic"), else: emitted
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
    upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.sse_stream([{"response.output_item.done", %{"type" => "response.output_item.done", "item" => emitted}}, {"response.completed", completed}]), FakeUpstream.sse_stream([{"response.completed", completed}])]))
    setup = gateway_setup(upstream, compact?: true)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    metadata = CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0})
    payload = %{"model" => setup.model.exposed_model_id, "instructions" => "synthetic instructions", "parallel_tool_calls" => true, "tools" => [tool("synthetic_tool")], "input" => native_text_input("synthetic"), "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => metadata}}
    fixture = %{setup: setup, upstream: upstream, thread: thread, mode: mode, payload: payload, output: serialized}
    assert send_payload(fixture, payload).status == 200
    [request] = requests(fixture)
    assert request.request_metadata["native_http_claim_arm"] == "opening"
    assert request.status == "succeeded"
    assert Repo.get_by!(Attempt, request_id: request.id).response_metadata["native_http_mailbox_prefix"]["output_item_done_count"] == 1
    fixture
  end

  defp continuation(fixture), do: Map.update!(fixture.payload, "input", &(&1 ++ [fixture.output, mailbox()]))
  defp change(payload, :instructions), do: Map.put(payload, "instructions", "synthetic changed instructions")
  defp change(payload, :parallel_tool_calls), do: Map.put(payload, "parallel_tool_calls", false)
  defp change(payload, :tool_catalogue), do: Map.put(payload, "tools", [tool("synthetic_other_tool")])
  defp change(payload, :mcp_catalogue), do: Map.put(payload, "tools", [%{"type" => "namespace", "name" => "mcp__synthetic", "tools" => [tool("synthetic_other_tool")]}])
  defp change(payload, :no_candidate), do: update_in(payload, ["input", Access.at(-1), "recipient"], fn _ -> "/root/other" end)
  defp change(payload, :historical_ending), do: Map.update!(payload, "input", &(&1 ++ [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic reasoning"}]))
  defp change(payload, :retained_content), do: put_in(payload, ["input", Access.at(-2), "content", Access.at(0), "text"], "synthetic changed commentary")
  defp tool(name), do: %{"type" => "function", "name" => name, "parameters" => %{"type" => "object", "properties" => %{}}}
  defp mailbox, do: %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}

  defp send_payload(fixture, payload) do
    conn = build_conn() |> auth(fixture.setup) |> put_req_header("session-id", fixture.thread) |> put_req_header("thread-id", fixture.thread) |> put_req_header("x-codex-window-id", "#{fixture.thread}:0") |> put_req_header("x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"])
    conn = if fixture.mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @path, payload)
  end

  defp requests(fixture), do: Repo.all(from r in Request, where: r.pool_id == ^fixture.setup.pool.id, order_by: [asc: r.admitted_at])

  defp counts(fixture) do
    ids = from r in Request, where: r.pool_id == ^fixture.setup.pool.id, select: r.id
    %{requests: Repo.aggregate(ids, :count), attempts: Repo.aggregate(from(a in Attempt, where: a.request_id in subquery(ids)), :count), turns: Repo.aggregate(from(t in CodexTurn, where: t.request_id in subquery(ids)), :count), ledger: Repo.aggregate(from(l in LedgerEntry, where: l.request_id in subquery(ids)), :count), links: Repo.aggregate(from(l in RequestClientRetryLink, where: l.predecessor_request_id in subquery(ids)), :count)}
  end
end
