defmodule CodexPoolerWeb.V1.ResponsesWebSearchCallTest do
  # A Full-served model that ran the hosted `web_search` tool returns a `web_search_call` output item, and an SDK or
  # gateway client sends `response.output` back as the history of its next stateless request. The item used to be a
  # `400 invalid_request` on `input`. The provider reads it in the Full and in the Lite request shape (direct probe,
  # 2026-10-06), so `/v1/responses` forwards the exact shape it emits, streaming or not, and drops only the id the
  # public stream invents for an item the upstream sent without one (the provider wants an id that begins with `ws`).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @query "synthetic private search query"
  @source_url "https://example.com/synthetic-private-source"
  @search %{
    "id" => "ws_synthetic_0001",
    "type" => "web_search_call",
    "status" => "completed",
    "action" => %{"type" => "search", "query" => @query, "queries" => [@query], "sources" => [%{"type" => "url", "url" => @source_url}]}
  }

  for mode <- ["auto", "lite", "full"], stream? <- [true, false] do
    test "#{mode} serving, stream #{stream?}: a replayed web_search_call reaches the upstream untouched" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_call_forward", [message("msg_after_search", "synthetic closing text")])]))
      setup = serving_setup(upstream, unquote(mode))

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => unquote(stream?), "input" => history(@search)})

      assert conn.status == 200
      assert conn.resp_body =~ "resp_web_search_call_forward"
      assert [captured] = FakeUpstream.requests(upstream)
      assert Enum.filter(captured.json["input"], &(&1["type"] == "web_search_call")) == [@search]
      assert Enum.map(captured.json["input"], & &1["type"]) -- ["additional_tools"] == ["message", "web_search_call", "message", "message"]
      assert [%{status: "succeeded"}] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    end
  end

  test "the open_page and find_in_page actions reach the upstream untouched" do
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_call_actions", [message("msg_after_actions", "synthetic closing text")])]))
    setup = gateway_setup(upstream)
    open_page = %{"id" => "ws_synthetic_0002", "type" => "web_search_call", "status" => "completed", "action" => %{"type" => "open_page", "url" => "https://example.com/page"}}
    find_in_page = %{"id" => "ws_synthetic_0003", "type" => "web_search_call", "status" => "completed", "action" => %{"type" => "find_in_page", "url" => "https://example.com/page", "pattern" => "synthetic"}}

    conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => [user("synthetic task"), @search, open_page, find_in_page, message(nil, "synthetic answer"), user("synthetic follow-up")]})

    assert conn.status == 200
    assert [captured] = FakeUpstream.requests(upstream)
    assert Enum.filter(captured.json["input"], &(&1["type"] == "web_search_call")) == [@search, open_page, find_in_page]
  end

  test "an upstream item without an id round-trips through a public stream and replays without the fallback id" do
    searched = Map.delete(@search, "id")

    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_call_first", [searched, message(nil, "synthetic answer")])]))
    setup = gateway_setup(upstream)

    first = post_responses(setup, %{"model" => setup.model.exposed_model_id, "input" => "synthetic first turn", "stream" => true})
    assert first.status == 200
    assert [%{"type" => "web_search_call", "id" => "web_search_call_0"} = public_item | _rest] = completed_output!(first.resp_body)

    replay_upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_call_replay", [message("msg_after_replay", "synthetic closing text")])]))
    replay_setup = gateway_setup(replay_upstream)

    replay = post_responses(replay_setup, %{"model" => replay_setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => [user("synthetic first turn"), public_item, user("synthetic follow-up")]})

    assert replay.status == 200
    assert [captured] = FakeUpstream.requests(replay_upstream)
    assert [_user, replayed, _follow_up] = captured.json["input"]
    assert replayed == searched
  end

  test "neither the query nor the sources of a replayed search are persisted in request or attempt rows" do
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_call_privacy", [message("msg_after_privacy", "synthetic closing text")])]))
    setup = gateway_setup(upstream)

    conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => history(@search)})

    assert conn.status == 200
    persisted = inspect({Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id)), Repo.all(Attempt)}, limit: :infinity, printable_limit: :infinity)
    refute persisted =~ @query
    refute persisted =~ @source_url
  end

  for {label, variant} <- [
        {"an unknown item key", :unknown_item_key},
        {"a null query list", :null_queries},
        {"an action of an unknown type", :unknown_action_type},
        {"a search carrying the open_page url", :search_with_url}
      ] do
    test "a web_search_call with #{label} is refused before any upstream work" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_web_search_call_never", [message("msg_never", "synthetic never sent")])]))
      setup = gateway_setup(upstream)

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => history(mutate(unquote(variant), @search))})

      assert %{"error" => %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "input", "message" => "input item shape is not translatable"}} = json_response(conn, 400)
      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 0
    end
  end

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    if mode != "auto", do: set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_responses(setup, payload), do: build_conn() |> auth(setup) |> post("/v1/responses", payload)

  defp history(item), do: [user("synthetic task"), item, message(nil, "synthetic answer"), user("synthetic follow-up")]

  defp user(text), do: %{"role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp message(id, text) do
    item = %{"type" => "message", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => text, "annotations" => []}]}
    if id, do: Map.put(item, "id", id), else: item
  end

  defp mutate(:unknown_item_key, item), do: Map.put(item, "zz_probe_unknown_key", true)
  defp mutate(:null_queries, item), do: put_in(item, ["action", "queries"], nil)
  defp mutate(:unknown_action_type, item), do: put_in(item, ["action", "type"], "bogus_action")
  defp mutate(:search_with_url, item), do: put_in(item, ["action", "url"], "https://example.com/")

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
