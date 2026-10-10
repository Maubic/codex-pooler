defmodule CodexPoolerWeb.Runtime.BackendCodexHttpMailboxRaceTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.{MailboxPrefixRaceSupport, SettlementTransactionHold}
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  # Detection budgets cover concurrent HTTP shutdown and PostgreSQL settlement;
  # successful paths finish on socket, transaction and process signals.
  @budget 15_000

  setup_all do
    %{peer_node: MailboxPrefixRaceSupport.start_http_peer!()}
  end

  setup context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    :ok
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode} consumed-prefix successor arriving inside predecessor settlement waits and resumes once", %{mode: mode} do
      gate = make_ref()
      {upstream, setup, port, thread, payload} = scenario(mode, [cut_response(1, gate), completed_response()])
      watcher = SettlementTransactionHold.start_lock_watcher!()
      hold = SettlementTransactionHold.inside_transaction!()
      {retained, handler} = consume_and_cut!(port, setup, payload, thread, gate)
      send(handler, {:fake_upstream_release_gate, gate})
      {settler, %{backend: backend}} = SettlementTransactionHold.await_held!(hold)
      [first] = requests(setup)
      assert {first.status, Repo.get_by!(CodexTurn, request_id: first.id).status} == {"in_progress", "in_progress"}
      successor = append_mailbox(payload, retained, 1)
      task = start_client!(fn -> post!(port, setup, successor, thread) end)

      try do
        assert SettlementTransactionHold.await_session_lookup_wait!(watcher, backend) == "codex_sessions"
        assert FakeUpstream.count(upstream) == 1
      after
        SettlementTransactionHold.release(hold, settler)
      end

      assert {200, body} = await_client!(task)
      assert body =~ "response.completed"
      [predecessor, admitted] = await_settled!(setup, 2)
      assert predecessor.last_error_code == "client_disconnected"
      assert admitted.status == "succeeded"
      assert_link!(predecessor, admitted)
      assert_settled_once!([predecessor, admitted])
      assert FakeUpstream.count(upstream) == 2
      FakeUpstream.verify!(upstream)
    end

    @tag mode: mode
    test "#{mode} consumed-prefix successor before any settlement row lock keeps the live fence", %{mode: mode} do
      gate = make_ref()
      {upstream, setup, port, thread, payload} = scenario(mode, [cut_response(1, gate), completed_response()])
      {conn, retained, handler} = consume_until_gate!(port, setup, payload, thread, gate)
      [first] = requests(setup)
      attempt = Repo.get_by!(Attempt, request_id: first.id)
      executor = :erlang.list_to_pid(String.to_charlist(attempt.owner_process_id))
      assert Process.alive?(executor)
      hold = MailboxPrefixRaceSupport.hold_before_session_lock!(executor)
      on_exit(fn -> send(executor, {hold, :release}) end)
      cut_downstream!(conn)
      send(handler, {:fake_upstream_release_gate, gate})
      assert_receive {^hold, :before_session_lock, ^executor}, @budget
      first = Repo.get!(Request, first.id)
      turn = Repo.get_by!(CodexTurn, request_id: first.id)
      assert {first.status, turn.status} == {"in_progress", "in_progress"}
      assert %DateTime{} = turn.first_visible_output_at
      successor = append_mailbox(payload, retained, 1)
      task = start_client!(fn -> post!(port, setup, successor, thread) end)

      try do
        assert {409, body} = await_client!(task)
        assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
        assert FakeUpstream.count(upstream) == 1
        assert length(requests(setup)) == 1
      after
        send(executor, {hold, :release})
      end

      [predecessor] = await_settled!(setup, 1)
      assert {200, body} = post!(port, setup, successor, thread)
      assert body =~ "response.completed"
      [_, admitted] = all = await_settled!(setup, 2)
      assert_link!(predecessor, admitted)
      assert_settled_once!(all)
      assert FakeUpstream.count(upstream) == 2
      FakeUpstream.verify!(upstream)
    end

    @tag mode: mode
    test "#{mode} two web nodes race mailbox successors on separate PostgreSQL connections and only one dispatches", %{mode: mode, peer_node: peer_node} do
      cut_gate = make_ref()
      successor_gate = make_ref()
      held_successor = {:gated_terminal_sse, [], [event(completed_event())], self(), successor_gate}
      {upstream, setup, port, thread, payload} = scenario(mode, [cut_response(1, cut_gate), held_successor])
      peer_port = MailboxPrefixRaceSupport.start_peer_listener!(peer_node)
      {retained, cut_handler} = consume_and_cut!(port, setup, payload, thread, cut_gate)
      send(cut_handler, {:fake_upstream_release_gate, cut_gate})
      [predecessor] = await_settled!(setup, 1)
      successor = append_mailbox(payload, retained, 1)
      session = Repo.get_by!(CodexTurn, request_id: predecessor.id).codex_session_id
      watcher = SettlementTransactionHold.start_lock_watcher!()
      parent = self()
      barrier = make_ref()
      holder = start_client!(fn -> hold_session!(session, parent, barrier) end)
      assert_receive {:session_locked, ^barrier, holder_backend}, @budget
      on_exit(fn -> send(elem(holder, 0).pid, {:release_session, barrier}) end)
      clients = for target_port <- [port, peer_port], do: start_client!(fn -> post!(target_port, setup, successor, thread) end)

      try do
        waiters = await_two_waiters!(watcher, holder_backend)
        assert length(Enum.uniq(Enum.map(waiters, &hd/1))) == 2
        assert Enum.sort(Enum.map(waiters, &List.last/1)) == ["codex_pooler_test", "synthetic_mailbox_peer"]
        assert FakeUpstream.count(upstream) == 1
      after
        send(elem(holder, 0).pid, {:release_session, barrier})
      end

      assert {:ok, :ok} = await_client!(holder)
      assert_receive {:fake_upstream_gate, :before_terminal, successor_handler, ^successor_gate}, @budget
      on_exit(fn -> send(successor_handler, {:fake_upstream_release_gate, successor_gate}) end)
      # Keep the winner in progress until the other web node has answered: a
      # completed identical request is a different, permitted resend contract.
      refused = await_first_client!(clients)
      assert {409, body} = elem(refused, 1)
      assert %{"error" => %{"code" => "duplicate_turn"}} = CodexPooler.JSON.decode!(body)
      assert [_, active] = requests(setup)
      assert active.status == "in_progress"
      assert FakeUpstream.count(upstream) == 2
      send(successor_handler, {:fake_upstream_release_gate, successor_gate})
      remaining = Enum.reject(clients, fn {task, _} -> task.ref == elem(refused, 0) end)
      assert [{200, completed}] = Enum.map(remaining, &await_client!/1)
      assert completed =~ "response.completed"
      [first, admitted] = await_settled!(setup, 2)
      assert_link!(first, admitted)
      assert_settled_once!([first, admitted])
      assert FakeUpstream.count(upstream) == 2
      FakeUpstream.verify!(upstream)
    end

    @tag mode: mode
    test "#{mode} two consumed-prefix mailbox cuts preserve history through a tool result continuation", %{mode: mode} do
      first_gate = make_ref()
      second_gate = make_ref()
      tool = %{"type" => "function_call", "id" => "fc_synthetic", "call_id" => "call_synthetic", "name" => "synthetic_tool", "arguments" => "{}", "status" => "completed"}
      tool_completed = put_in(completed_event(), ["response", "output"], [tool])
      tool_response = FakeUpstream.sse_stream([{"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => tool}}, {"response.completed", tool_completed}])
      {upstream, setup, port, thread, payload} = scenario(mode, [cut_response(1, first_gate), cut_response(2, second_gate), tool_response, completed_response()])
      {first_output, handler} = consume_and_cut!(port, setup, payload, thread, first_gate)
      send(handler, {:fake_upstream_release_gate, first_gate})
      [first] = await_settled!(setup, 1)
      first_resume = append_mailbox(payload, first_output, 1)
      {second_output, handler} = consume_and_cut!(port, setup, first_resume, thread, second_gate)
      send(handler, {:fake_upstream_release_gate, second_gate})
      [_, second] = await_settled!(setup, 2)
      assert_link!(first, second)
      second_resume = append_mailbox(first_resume, second_output, 2)
      changed_history = put_in(second_resume, ["input", Access.at(-4), "encrypted_content"], "synthetic_altered")
      assert {409, _} = post!(port, setup, changed_history, thread)
      assert FakeUpstream.count(upstream) == 2
      assert {200, body} = post!(port, setup, second_resume, thread)
      assert body =~ "response.completed"
      [_, _, third] = await_settled!(setup, 3)
      assert_link!(second, third)
      result = %{"type" => "function_call_output", "call_id" => "call_synthetic", "output" => "synthetic tool result"}
      tool_continuation = Map.update!(second_resume, "input", &(&1 ++ [tool, result]))
      assert {200, terminal} = post!(port, setup, tool_continuation, thread)
      assert terminal =~ "response.completed"
      all = await_settled!(setup, 4)
      assert Enum.map(all, & &1.status) == ["failed", "failed", "succeeded", "succeeded"]
      assert List.last(all).request_metadata["native_http_claim_arm"] == "tool_continuation"
      assert_settled_once!(all)
      assert FakeUpstream.count(upstream) == 4
      FakeUpstream.verify!(upstream)
    end
  end

  defp scenario(mode, responses) do
    # provenance: synthetic_adversarial; two coalesced output items, a held
    # cancellation-detection tail, exact database barriers and invented terminals
    upstream = start_upstream(FakeUpstream.strict_sequence(responses))
    setup = gateway_setup(upstream, compact?: true)
    register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    setup = Map.put(setup, :serving_mode, mode)
    port = start_public_endpoint!()
    thread = Ecto.UUID.generate()
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic") ++ [%{"type" => "compaction", "encrypted_content" => "synthetic_compaction"}], "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root"})}}
    {upstream, setup, port, thread, payload}
  end

  defp cut_response(n, gate) do
    outputs = for ordinal <- 1..2, do: %{"type" => "reasoning", "id" => "rs_synthetic_#{n}_#{ordinal}", "summary" => [], "encrypted_content" => "synthetic_#{n}_#{ordinal}"}
    chunk = Enum.map_join(outputs, &event(%{"type" => "response.output_item.done", "item" => &1}))
    tail = event(%{"type" => "response.reasoning_text.delta", "delta" => String.duplicate("synthetic", 10_000)})
    {:gated_terminal_sse, [chunk], [tail], self(), gate}
  end

  defp completed_event, do: %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
  defp completed_response, do: FakeUpstream.sse_stream([{"response.completed", completed_event()}])
  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  defp append_mailbox(payload, retained, n) do
    mailbox = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update #{n}"}]}
    Map.update!(payload, "input", &(&1 ++ [Map.put(retained, "content", nil), mailbox]))
  end

  defp consume_and_cut!(port, setup, payload, thread, gate) do
    {conn, retained, handler} = consume_until_gate!(port, setup, payload, thread, gate)
    cut_downstream!(conn)
    {retained, handler}
  end

  defp consume_until_gate!(port, setup, payload, thread, gate) do
    {conn, ref} = start_request!(port, setup, payload, thread)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    {conn, retained} = receive_first_item!(conn, ref, "")
    assert_receive {:fake_upstream_gate, :before_terminal, handler, ^gate}, @budget
    on_exit(fn -> send(handler, {:fake_upstream_release_gate, gate}) end)
    {conn, retained, handler}
  end

  defp cut_downstream!(conn) do
    :ok = :inet.setopts(Mint.HTTP.get_socket(conn), linger: {true, 0})
    Mint.HTTP.close(conn)
  end

  defp start_request!(port, setup, payload, thread) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
  end

  defp post!(port, setup, payload, thread) do
    {conn, ref} = start_request!(port, setup, payload, thread)

    try do
      receive_all!(conn, ref, nil, "")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp receive_first_item!(conn, ref, acc) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    bytes =
      Enum.reduce(responses, acc, fn
        {:data, ^ref, data}, a -> a <> data
        _, a -> a
      end)

    if String.contains?(bytes, "\n\n") do
      [block | _unconsumed] = String.split(bytes, "\n\n")
      [data] = for "data: " <> json <- String.split(block, "\n"), do: json
      assert %{"type" => "response.output_item.done", "item" => item} = CodexPooler.JSON.decode!(data)
      {conn, item}
    else
      receive_first_item!(conn, ref, bytes)
    end
  end

  defp receive_all!(conn, ref, status, body) do
    {:ok, conn, responses} = Mint.HTTP.recv(conn, 0, @budget)

    {status, body, done} =
      Enum.reduce(responses, {status, body, false}, fn
        {:status, ^ref, s}, {_, b, d} -> {s, b, d}
        {:data, ^ref, data}, {s, b, d} -> {s, b <> data, d}
        {:done, ^ref}, {s, b, _} -> {s, b, true}
        _, a -> a
      end)

    if done, do: {status, body}, else: receive_all!(conn, ref, status, body)
  end

  defp start_client!(fun) do
    supervisor = start_supervised!({Task.Supervisor, []}, id: make_ref())
    task = Task.Supervisor.async_nolink(supervisor, fun)
    {task, Process.monitor(task.pid)}
  end

  defp await_client!({task, monitor}) do
    result = Task.await(task, @budget)
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    result
  end

  defp await_first_client!(clients) do
    refs = Map.new(clients, fn {task, monitor} -> {task.ref, {task, monitor}} end)

    receive do
      {ref, result} when is_map_key(refs, ref) ->
        {task, monitor} = Map.fetch!(refs, ref)
        Process.demonitor(task.ref, [:flush])
        assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
        {ref, result}
    after
      @budget -> flunk("neither concurrent successor answered")
    end
  end

  defp hold_session!(session_id, parent, barrier) do
    Repo.transaction(fn ->
      Repo.one!(from(s in CodexSession, where: s.id == ^session_id, lock: "FOR UPDATE"))
      [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
      send(parent, {:session_locked, barrier, backend})

      receive do
        {:release_session, ^barrier} -> :ok
      after
        @budget -> raise "session lock not released"
      end
    end)
  end

  defp await_two_waiters!(watcher, holder_backend), do: await_two_waiters!(watcher, holder_backend, System.monotonic_time(:millisecond) + @budget)

  defp await_two_waiters!(watcher, backend, deadline) do
    sql = "WITH RECURSIVE waiters(pid) AS (SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) UNION SELECT a.pid FROM pg_stat_activity a JOIN waiters w ON w.pid = ANY(pg_blocking_pids(a.pid))) SELECT a.pid, a.application_name FROM waiters w JOIN pg_stat_activity a ON a.pid = w.pid WHERE a.query LIKE '%FROM \"codex_sessions\" AS c0 INNER JOIN \"bridge_session_aliases\"%'"
    rows = Postgrex.query!(watcher, sql, [backend]).rows

    cond do
      length(rows) == 2 ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("two web-node session lookups never waited together: #{inspect(rows)}")

      true ->
        Process.sleep(10)
        await_two_waiters!(watcher, backend, deadline)
    end
  end

  defp requests(setup), do: Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])
  defp await_settled!(setup, count), do: await_settled!(setup, count, System.monotonic_time(:millisecond) + @budget)

  defp await_settled!(setup, count, deadline) do
    rows = requests(setup)

    cond do
      length(rows) == count and Enum.all?(rows, &(&1.completed_at != nil)) ->
        rows

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("expected #{count} settled requests, saw #{inspect(Enum.map(rows, &{&1.status, &1.last_error_code}))}")

      true ->
        Process.sleep(10)
        await_settled!(setup, count, deadline)
    end
  end

  defp assert_link!(first, successor) do
    assert successor.request_metadata["client_resend"]["predecessor_request_id"] == first.id
    assert [%RequestClientRetryLink{successor_request_id: id}] = Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id)
    assert id == successor.id
    assert Repo.get_by!(Attempt, request_id: first.id).response_metadata["native_http_mailbox_prefix"]["output_item_done_count"] == 2
  end

  defp assert_settled_once!(requests) do
    ids = Enum.map(requests, & &1.id)
    entries = Repo.all(from l in LedgerEntry, where: l.request_id in ^ids, select: {l.request_id, l.entry_kind})
    expected = for id <- ids, kind <- ["reservation", "settlement", "release"], into: %{}, do: {{id, kind}, 1}
    assert Enum.frequencies(entries) == expected
  end
end
