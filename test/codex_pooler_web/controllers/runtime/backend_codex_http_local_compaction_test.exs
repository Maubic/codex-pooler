defmodule CodexPoolerWeb.Runtime.BackendCodexHttpLocalCompactionTest do
  # A Codex client whose provider is not named `OpenAI` compacts locally
  # (`compact.rs`): it summarizes the thread with a request of its own
  # (`request_kind: "compaction"`, `implementation: "responses"`), rebuilds the
  # history from the thread's initial context, its most recent user messages
  # (as many as fit the compaction's budget) and the summary as one more user
  # message, and resumes the turn on its next context window. A local
  # compaction leaves no compaction item, so the resume stood no further along
  # its turn than a request before it: from a thread's second local compaction
  # on (in the same turn or a later one) it was refused `409 duplicate_turn` on
  # every try, and the released client 0.159.0 failed the turn after five
  # retries over HTTP (findings#282, findings#270 row 270-286). The resume's
  # context window now stands in for the compaction point the compaction did
  # not leave (`NativeTurnContinuation.turn_progress/2`).
  #
  # Requests: the released client's HTTP shape (a JSON body carrying the
  # canonical turn document with its `window_number`, `stream: true`, the
  # document echoed as `x-codex-turn-metadata`, the thread as `session-id` and
  # `thread-id`, the window as `x-codex-window-id`, the Lite marker as a
  # header), with synthetic text and ids. Topology: the real HTTP route, one
  # node (HTTP is never forwarded), the Pool's serving mode forced to Full and
  # to Lite, FakeUpstream.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true

  @metadata_key "x-codex-turn-metadata"
  @lite_header "x-openai-internal-codex-responses-lite"
  @compaction_prompt "You are performing a CONTEXT CHECKPOINT COMPACTION. Create a handoff summary (synthetic)."
  @summary_prefix "Another language model started to solve this problem and produced a summary (synthetic)."

  for mode <- ["full", "lite"], shape <- [:same_turn, :next_turn, :trimmed] do
    @mode mode
    @shape shape

    test "a #{mode} thread's resume after a #{shape} local compaction is served, and a true resend of it is refused", %{conn: conn} do
      requests = requests(@shape)
      upstream = start_upstream(FakeUpstream.strict_sequence(for step <- 1..length(requests), do: turn_sse("resp_local_compaction_#{step}")))
      setup = setup!(upstream, @mode)
      thread = %{id: Ecto.UUID.generate(), turns: %{first: Ecto.UUID.generate(), second: Ecto.UUID.generate()}}

      for {{turn, kind, input, window} = request, step} <- Enum.with_index(requests, 1) do
        conn = post_request(conn, setup, thread, request)
        assert conn.status == 200, "step #{step} (#{turn} #{kind} on window #{window}, #{length(input)} items) answered #{conn.status}: #{conn.resp_body}"
      end

      assert FakeUpstream.count(upstream) == length(requests)
      assert rows(setup) == expected_rows(@shape)

      # Each resume is a request of its own: its claim is no other request's.
      claims = Enum.map(pool_requests(setup), & &1.correlation_id)
      assert claims == Enum.uniq(claims)

      # A true resend of the last resume meets it, and so does its rebuilt
      # retry (model output appended): neither reaches the provider.
      {turn, kind, input, window} = List.last(requests)

      for resend <- [input, input ++ [assistant("partial answer")]] do
        assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(post_request(conn, setup, thread, {turn, kind, resend, window}), 409)
      end

      assert FakeUpstream.count(upstream) == length(requests)
      assert rows(setup) == expected_rows(@shape)
      assert :ok = FakeUpstream.verify!(upstream)
    end
  end

  # The requests of one thread, in the order the released client sends them:
  # {turn, request kind, input, window}. A summarization request carries the
  # whole history, the tool round included, then the compaction prompt; the
  # resume carries the initial context, the retained user messages and the
  # summary.
  defp requests(:same_turn) do
    task = user("local compaction sample: work, then answer")

    [
      {:first, "turn", context() ++ [task], 0},
      {:first, "compaction", context() ++ [task] ++ tool_round("one") ++ [user(@compaction_prompt)], 0},
      {:first, "turn", context() ++ [task, summary("one")], 1},
      {:first, "compaction", context() ++ [task, summary("one")] ++ tool_round("two") ++ [user(@compaction_prompt)], 1},
      {:first, "turn", context() ++ [task, summary("two")], 2}
    ]
  end

  defp requests(:next_turn) do
    task = user("local compaction sample: work, then answer")
    next = user("local compaction sample: the next task")
    after_first_turn = [task, summary("one"), assistant("first answer"), next]

    [
      {:first, "turn", context() ++ [task], 0},
      {:first, "compaction", context() ++ [task] ++ tool_round("one") ++ [user(@compaction_prompt)], 0},
      {:first, "turn", context() ++ [task, summary("one")], 1},
      {:second, "turn", context() ++ after_first_turn, 1},
      {:second, "compaction", context() ++ after_first_turn ++ tool_round("two") ++ [user(@compaction_prompt)], 1},
      {:second, "turn", context() ++ [task, next, summary("two")], 2}
    ]
  end

  # The thread's first local compaction, in a turn whose history holds more
  # user messages than the compaction's budget kept.
  defp requests(:trimmed) do
    earlier = Enum.flat_map(1..4, &[user("local compaction sample: earlier request #{&1}"), assistant("earlier answer #{&1}")])
    task = user("local compaction sample: work, then answer")

    [
      {:first, "turn", context() ++ earlier ++ [task], 0},
      {:first, "compaction", context() ++ earlier ++ [task] ++ tool_round("one") ++ [user(@compaction_prompt)], 0},
      {:first, "turn", context() ++ [task, summary("one")], 1}
    ]
  end

  defp expected_rows(:same_turn), do: [opening(), compaction(), resume(), compaction(), resume()]
  defp expected_rows(:next_turn), do: [opening(), compaction(), resume(), opening(), compaction(), resume()]
  defp expected_rows(:trimmed), do: [opening(), compaction(), resume()]

  defp opening, do: {"codex-turn", "opening", "succeeded"}
  defp compaction, do: {"codex-request", "compaction", "succeeded"}
  defp resume, do: {"codex-resume", "steered_continuation", "succeeded"}

  defp setup!(upstream, mode) do
    setup = gateway_setup(upstream, compact?: true)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    Map.put(setup, :serving_mode, mode)
  end

  defp post_request(conn, setup, thread, {turn, kind, input, window}) do
    turn_id = Map.fetch!(thread.turns, turn)

    document =
      %{
        "session_id" => thread.id,
        "thread_id" => thread.id,
        "turn_id" => turn_id,
        "root_turn_id" => turn_id,
        "window_id" => "#{thread.id}:#{window}",
        "window_number" => window,
        "request_kind" => kind
      }
      |> then(&if kind == "compaction", do: Map.put(&1, "compaction", %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses", "phase" => "mid_turn", "strategy" => "memento"}), else: &1)
      |> CodexPooler.JSON.encode!()

    payload = %{
      "model" => setup.model.exposed_model_id,
      "input" => input,
      "stream" => true,
      "prompt_cache_key" => thread.id,
      "client_metadata" => %{@metadata_key => document}
    }

    conn
    |> recycle()
    |> auth(setup)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", "text/event-stream")
    |> put_req_header("session-id", thread.id)
    |> put_req_header("thread-id", thread.id)
    |> put_req_header("x-codex-window-id", "#{thread.id}:#{window}")
    |> put_req_header(@metadata_key, document)
    |> put_req_header("originator", "codex_exec")
    |> then(&if setup.serving_mode == "lite", do: put_req_header(&1, @lite_header, "true"), else: &1)
    |> post("/backend-api/codex/responses", CodexPooler.JSON.encode!(payload))
  end

  defp turn_sse(id) do
    FakeUpstream.sse_stream([
      {"response.completed", %{"type" => "response.completed", "response" => %{"id" => id, "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}}}
    ])
  end

  defp context, do: [developer("synthetic developer instructions"), user("synthetic environment context")]

  defp tool_round(label) do
    call_id = "call_local_compaction_#{label}"

    [
      %{"type" => "function_call", "name" => "exec_command", "arguments" => ~s({"cmd":"printf sample"}), "call_id" => call_id},
      %{"type" => "function_call_output", "call_id" => call_id, "output" => "sample"}
    ]
  end

  defp summary(label), do: user("#{@summary_prefix}\nsynthetic summary #{label}")

  defp developer(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}
  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}
  defp assistant(text), do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: r.admitted_at))

  defp rows(setup) do
    for request <- pool_requests(setup) do
      {request.correlation_id |> String.split(":") |> hd(), request.request_metadata["native_http_claim_arm"], request.status}
    end
  end
end
