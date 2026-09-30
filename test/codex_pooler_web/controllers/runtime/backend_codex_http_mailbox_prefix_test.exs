defmodule CodexPoolerWeb.Runtime.BackendCodexHttpMailboxPrefixTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @budget 15_000

  for mode <- ["full", "lite"], count <- [1, 2] do
    @tag mode: mode, count: count
    test "#{mode} retains first of #{count} coalesced items", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      outputs = Enum.map(1..context.count, fn n -> %{"type" => "reasoning", "id" => "rs_synthetic_#{n}", "summary" => [], "encrypted_content" => "synthetic_#{n}"} end)
      chunk = Enum.map_join(outputs, fn item -> event(%{"type" => "response.output_item.done", "item" => item}) end)
      gate = make_ref()
      tail = event(%{"type" => "response.reasoning_text.delta", "delta" => String.duplicate("synthetic", 10_000)})
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      # provenance: synthetic_adversarial; coalesced output followed by a held cancellation-detection tail
      upstream = start_upstream(FakeUpstream.strict_sequence([{:gated_terminal_sse, [chunk], [tail], self(), gate}, FakeUpstream.sse_stream([{"response.completed", completed}])]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic") ++ [%{"type" => "compaction", "encrypted_content" => "synthetic_compaction"}]
      payload = %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root"})}}
      {conn, ref} = start_request(port, setup, payload, thread)
      {conn, retained} = until_item(conn, ref, "")
      retained = Map.put(retained, "content", nil)
      assert_receive {:fake_upstream_gate, :before_terminal, handler, ^gate}, @budget
      # Reset the real connection so the held upstream tail observes cancellation.
      :ok = :inet.setopts(Mint.HTTP.get_socket(conn), linger: {true, 0})
      Mint.HTTP.close(conn)
      send(handler, {:fake_upstream_release_gate, gate})
      first = await_failed(setup, System.monotonic_time(:millisecond) + @budget)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      turn = Repo.get_by!(CodexTurn, request_id: first.id)
      recorded = get_in(attempt.response_metadata, ["native_http_resume_progress", "output_item_done_count"])
      prefix = attempt.response_metadata["native_http_mailbox_prefix"]
      assert prefix["output_item_done_count"] == context.count
      assert length(prefix["item_digests"]) == context.count
      assert {first.status, first.last_error_code, turn.status} == {"failed", "client_disconnected", "interrupted"}
      assert recorded == context.count
      mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
      successor = Map.put(payload, "input", input ++ [retained, mailbox])
      bad = put_in(successor, ["input", Access.at(-2), "encrypted_content"], "changed_synthetic")
      {bad_status, _} = post(port, setup, bad, thread)
      assert bad_status == 409

      if context.count == 2 do
        {nonprefix_status, _} = post(port, setup, Map.put(payload, "input", input ++ [List.last(outputs), mailbox]), thread)
        assert nonprefix_status == 409
        {reordered_status, _} = post(port, setup, Map.put(payload, "input", input ++ Enum.reverse(outputs) ++ [mailbox]), thread)
        assert reordered_status == 409
      end

      {status, _} = post(port, setup, successor, thread)
      assert status == 200
      [predecessor, admitted] = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
      assert predecessor.id == first.id
      assert admitted.request_metadata["client_resend"]["predecessor_request_id"] == first.id
      assert Repo.exists?(from l in CodexPooler.Accounting.RequestClientRetryLink, where: l.predecessor_request_id == ^first.id and l.successor_request_id == ^admitted.id)

      for request <- [predecessor, admitted] do
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end

      assert FakeUpstream.count(upstream) == 2
    end
  end

  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  defp start_request(port, setup, payload, thread) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp until_item(conn, ref, acc) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    bytes =
      Enum.reduce(responses, acc, fn
        {:data, ^ref, data}, a -> a <> data
        _, a -> a
      end)

    if String.contains?(bytes, "\n\n") do
      [block | _ignored_after_preemption] = String.split(bytes, "\n\n")
      [data] = for "data: " <> json <- String.split(block, "\n"), do: json
      assert %{"type" => "response.output_item.done", "item" => item} = CodexPooler.JSON.decode!(data)
      {conn, item}
    else
      until_item(conn, ref, bytes)
    end
  end

  defp post(port, setup, payload, thread) do
    {conn, ref} = start_request(port, setup, payload, thread)

    try do
      all(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp all(conn, ref, status, body) do
    assert {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _, a -> a
      end)

    if done, do: {status, body}, else: all(conn, ref, status, body)
  end

  defp await_failed(setup, deadline) do
    row = Repo.one(from r in Request, where: r.pool_id == ^setup.pool.id)

    cond do
      row && row.completed_at ->
        row

      System.monotonic_time(:millisecond) > deadline ->
        flunk("request never finalized")

      true ->
        Process.sleep(10)
        await_failed(setup, deadline)
    end
  end
end
