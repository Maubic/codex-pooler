defmodule CodexPoolerWeb.V1.ResponsesAgentMessageTest do
  # A gateway that relays a Codex multi-agent v2 history to `/v1/responses` replays `agent_message` items: a
  # subagent's plaintext `FINAL_ANSWER` handed back to its lead, or the lead's sealed `NEW_TASK` handed to a subagent.
  # Both used to be a `400 invalid_request` on `input` ("input item shape is not translatable"). The provider reads
  # both forms in a stateless request in the Full and the Lite request shape (direct probe, 2026-10-06), so the
  # adapter forwards the exact shapes the native backend route recognizes on both serving modes, streaming and not,
  # and still refuses every other shape before any upstream work.
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
  @cipher "gAAAAA-synthetic-cipher-0001"
  @answer "synthetic worker answer"

  for mode <- ["full", "lite"], stream? <- [true, false], form <- [:plaintext, :sealed] do
    test "#{mode} serving, stream #{stream?}: a replayed #{form} agent_message reaches the upstream untouched" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_agent_message_forward")]))
      setup = serving_setup(upstream, unquote(mode))
      item = item(unquote(form))

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => unquote(stream?), "input" => history(item)})

      assert conn.status == 200
      assert conn.resp_body =~ "resp_agent_message_forward"
      assert [captured] = FakeUpstream.requests(upstream)
      assert Enum.filter(captured.json["input"], &(&1["type"] == "agent_message")) == [item]
      assert [%{status: "succeeded"}] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    end
  end

  test "the lead's own call items, the mailbox answer and the sealed task keep the order the client sent" do
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_agent_message_order")]))
    setup = gateway_setup(upstream)
    call = %{"type" => "function_call", "call_id" => "call_synthetic_wait", "name" => "wait_agent", "namespace" => "collaboration", "arguments" => "{}"}
    output = %{"type" => "function_call_output", "call_id" => "call_synthetic_wait", "output" => "{\"timed_out\":false}"}
    input = [user("synthetic task"), call, output, item(:sealed), item(:plaintext), user("synthetic follow-up")]

    conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => input})

    assert conn.status == 200
    assert [captured] = FakeUpstream.requests(upstream)
    assert Enum.map(captured.json["input"], & &1["type"]) == ["message", "function_call", "function_call_output", "agent_message", "agent_message", "message"]
    assert Enum.filter(captured.json["input"], &(&1["type"] == "agent_message")) == [item(:sealed), item(:plaintext)]
  end

  test "neither the sealed task nor the mailbox answer is persisted in request or attempt rows" do
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_agent_message_privacy")]))
    setup = gateway_setup(upstream)

    conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => [user("synthetic task"), item(:sealed), item(:plaintext)]})

    assert conn.status == 200
    persisted = inspect({Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id)), Repo.all(Attempt)}, limit: :infinity, printable_limit: :infinity)
    refute persisted =~ @cipher
    refute persisted =~ @answer
  end

  for {label, variant} <- [
        {"an unknown key", :status_key},
        {"a role", :role_key},
        {"an empty content list", :empty_content},
        {"an output_text part", :output_text_part},
        {"a blank author", :blank_author},
        {"a part with an extra key", :part_extra_key}
      ] do
    test "an agent_message with #{label} is refused before any upstream work" do
      upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_agent_message_never")]))
      setup = gateway_setup(upstream)

      conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => history(mutate(unquote(variant), item(:plaintext)))})

      assert %{"error" => %{"type" => "invalid_request_error", "code" => "invalid_request", "param" => "input", "message" => "input item shape is not translatable"}} = json_response(conn, 400)
      assert FakeUpstream.count(upstream) == 0
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 0
    end
  end

  test "a sealed item that is not the exact handoff the native route recognizes is refused instead of filtered" do
    upstream = start_upstream(FakeUpstream.sse_stream([completed("resp_agent_message_never")]))
    setup = gateway_setup(upstream)
    unrecognized = Map.update!(item(:sealed), "content", fn [envelope, cipher] -> [%{envelope | "text" => "Message Type: MESSAGE\nPayload:\n"}, cipher] end)

    conn = post_responses(setup, %{"model" => setup.model.exposed_model_id, "store" => false, "stream" => true, "input" => history(unrecognized)})

    assert %{"error" => %{"code" => "invalid_request", "param" => "input"}} = json_response(conn, 400)
    assert FakeUpstream.count(upstream) == 0
  end

  defp mutate(:status_key, item), do: Map.put(item, "status", "completed")
  defp mutate(:role_key, item), do: Map.put(item, "role", "assistant")
  defp mutate(:empty_content, item), do: Map.put(item, "content", [])
  defp mutate(:output_text_part, item), do: Map.put(item, "content", [%{"type" => "output_text", "text" => @answer}])
  defp mutate(:blank_author, item), do: Map.put(item, "author", " ")
  defp mutate(:part_extra_key, item), do: Map.put(item, "content", [%{"type" => "input_text", "text" => @answer, "annotations" => []}])

  defp serving_setup(upstream, mode) do
    setup = gateway_setup(upstream)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup
  end

  defp post_responses(setup, payload), do: build_conn() |> auth(setup) |> post("/v1/responses", payload)

  defp history(item), do: [user("synthetic task"), item, user("synthetic follow-up")]

  defp user(text), do: %{"role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp item(:plaintext) do
    %{
      "type" => "agent_message",
      "id" => "amsg_synthetic_0001",
      "author" => "/root/worker",
      "recipient" => "/root",
      "content" => [%{"type" => "input_text", "text" => "Message Type: FINAL_ANSWER\nTask name: /root\nSender: /root/worker\nPayload:\n#{@answer}"}]
    }
  end

  defp item(:sealed) do
    %{
      "type" => "agent_message",
      "id" => "amsg_synthetic_0002",
      "author" => "/root",
      "recipient" => "/root/worker",
      "content" => [
        %{"type" => "input_text", "text" => "Message Type: NEW_TASK\nTask name: /root/worker\nSender: /root\nPayload:\n"},
        %{"type" => "encrypted_content", "encrypted_content" => @cipher}
      ]
    }
  end

  defp completed(response_id) do
    {"response.completed",
     %{
       "type" => "response.completed",
       "response" => %{
         "id" => response_id,
         "object" => "response",
         "created_at" => 1_790_000_000,
         "model" => "provider-gpt-test-model",
         "status" => "completed",
         "output" => [%{"type" => "message", "id" => "msg_agent_message_reply", "role" => "assistant", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic reply", "annotations" => []}]}],
         "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
       }
     }}
  end
end
