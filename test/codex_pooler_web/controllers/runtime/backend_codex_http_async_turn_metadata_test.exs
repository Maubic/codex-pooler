defmodule CodexPoolerWeb.Runtime.BackendCodexHttpAsyncTurnMetadataTest do
  # The released Codex client fills `workspaces` in its `x-codex-turn-metadata`
  # document from a git task it spawns per turn and does not wait for
  # (`turn_metadata.rs` `spawn_git_enrichment_task`, `current_workspaces`), and it
  # rebuilds the document for every request it sends (`session/turn.rs`
  # `responses_metadata` per sampling attempt). A request sent before the task
  # finished carries no `workspaces`; its retry, sent after, carries them. The
  # native HTTP resend witness binds the whole frame, that document included
  # (only its `turn_id` is dropped), so the retry no longer matched its own
  # predecessor (findings#314 row 314-2).
  #
  # Provenance: the field, its absence before the task finishes and the
  # per-attempt rebuild are source-derived (codex-rs 0.160.1 and main); no
  # capture of a real retry carrying it exists. The request shapes follow the
  # released client's HTTP body; ids, texts and git values are synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, gateway_setup: 1, native_text_input: 1, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Accounting.{Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true
  @workspaces %{"/synthetic/repository" => %{"associated_remote_urls" => %{"origin" => "https://example.com/sample-app.git"}, "latest_git_commit_hash" => String.duplicate("a", 40), "has_changes" => true}}

  for mode <- ["full", "lite"], arm <- [:opening, :tool_continuation] do
    test "#{mode} #{arm}: an identical resend that gained the async workspaces is chained" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; two delivered completions
          FakeUpstream.strict_sequence([delivered_sse("resp_async_metadata"), delivered_sse("resp_async_metadata_resend")])
        )

      fixture = fixture!(upstream, unquote(mode), input(unquote(arm)))
      assert post_native(fixture, request(fixture, %{})).resp_body =~ ~s("type":"response.completed")

      {resend, logs} = with_info_log(fn -> post_native(fixture, request(fixture, %{"workspaces" => @workspaces})) end)
      assert resend.status == 200, "the resend was refused: #{inspect(rejection_lines(logs))}"

      assert [predecessor, successor] = pool_requests(fixture.setup)
      assert successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id
      assert linked?(predecessor, successor)
      assert FakeUpstream.count(upstream) == 2
    end
  end

  for mode <- ["full", "lite"] do
    test "#{mode}: a mailbox continuation that gained the async workspaces is chained" do
      reasoning = %{"type" => "reasoning", "id" => "rs_async_metadata", "summary" => [], "encrypted_content" => "synthetic_reasoning"}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a completion that delivered one reasoning item, then a completion
          FakeUpstream.strict_sequence([delivered_sse("resp_async_mailbox", reasoning), delivered_sse("resp_async_mailbox_successor")])
        )

      fixture = fixture!(upstream, unquote(mode), native_text_input("synthetic mailbox request"))
      assert post_native(fixture, request(fixture, %{})).status == 200

      mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
      continuation = fixture |> request(%{"workspaces" => @workspaces}) |> Map.update!("input", &(&1 ++ [Map.put(reasoning, "content", nil), mailbox]))

      {served, logs} = with_info_log(fn -> post_native(fixture, continuation) end)
      assert served.status == 200, "the continuation was refused: #{inspect(rejection_lines(logs))}"

      assert [predecessor, successor] = pool_requests(fixture.setup)
      assert successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id
      assert linked?(predecessor, successor)
      assert FakeUpstream.count(upstream) == 2
    end
  end

  # Only the field the client fills late is set aside. A retry whose turn
  # metadata changed anything else is not the same request and keeps the
  # refusal, and so does one that lost the field it had.
  for mode <- ["full", "lite"], {label, before, later} <- [{"another document field", %{"sandbox" => "read-only"}, %{"sandbox" => "workspace-write"}}, {"the workspaces it had", %{"workspaces" => @workspaces}, %{}}] do
    test "#{mode}: a resend that changed #{label} is still refused" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; one delivered completion
          FakeUpstream.strict_sequence([delivered_sse("resp_async_metadata")])
        )

      fixture = fixture!(upstream, unquote(mode), native_text_input("synthetic request"))
      assert post_native(fixture, request(fixture, unquote(Macro.escape(before)))).status == 200

      {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, unquote(Macro.escape(later)))) end)
      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ "resend_disposition=terminal_predecessor"
      assert length(pool_requests(fixture.setup)) == 1
      assert FakeUpstream.count(upstream) == 1
    end
  end

  defp fixture!(upstream, mode, input) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    %{setup: setup, mode: mode, thread: Ecto.UUID.generate(), input: input}
  end

  defp input(:opening), do: native_text_input("synthetic request")

  defp input(:tool_continuation) do
    native_text_input("synthetic request") ++
      [
        %{"type" => "function_call", "call_id" => "call_synthetic_async", "name" => "synthetic_tool", "arguments" => "{}"},
        %{"type" => "function_call_output", "call_id" => "call_synthetic_async", "output" => "synthetic tool result"}
      ]
  end

  # The released client's HTTP body: the canonical document in
  # `client_metadata`, which the resend witness binds, beside its flat copies.
  defp request(fixture, document_fields) do
    thread = fixture.thread

    metadata =
      %{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_async_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0", "window_number" => 0}
      |> Map.merge(document_fields)
      |> CodexPooler.JSON.encode!()

    %{
      "model" => fixture.setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "tools" => [],
      "parallel_tool_calls" => true,
      "input" => fixture.input,
      "stream" => true,
      "store" => false,
      "client_metadata" => %{"x-codex-turn-metadata" => metadata, "thread_id" => thread, "turn_id" => "synthetic_async_turn", "x-codex-window-id" => "#{thread}:0"}
    }
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

  defp delivered_sse(response_id, item \\ nil) do
    item = item || %{"type" => "message", "id" => "msg_#{response_id}", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic answer", "annotations" => []}]}

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
