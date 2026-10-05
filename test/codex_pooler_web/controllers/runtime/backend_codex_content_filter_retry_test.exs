defmodule CodexPoolerWeb.Runtime.BackendCodexContentFilterRetryTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 2, register_unboxed_pool_cleanup!: 1, native_text_input: 1, start_public_endpoint!: 0, start_upstream: 1, public_websocket_connect!: 3, public_websocket_send_text!: 4, public_websocket_receive_text!: 3]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]
  alias CodexPooler.Accounting.{Attempt, NativeContentFilterRetry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox
  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @budget 15_000

  for expires? <- [false, true] do
    @tag expires?: expires?, slow: "real PostgreSQL receipt writer and retry contend on separate connections"
    test "HTTP retry waits for durable CF receipt and checks lock-time expiry #{expires?}", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_delayed", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_after_receipt", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}}
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(event(terminal), headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, "full")
      setup = Map.put(setup, :serving_mode, "full")
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      original = payload(setup, thread, native_text_input("synthetic delayed receipt"), 0)
      lock_key = System.unique_integer([:positive])
      trigger = "cf_receipt_#{lock_key}"
      Repo.query!("CREATE FUNCTION #{trigger}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.response_metadata::jsonb ? 'downstream_delivery' AND OLD.response_metadata::jsonb ? 'native_content_filter_terminal' AND EXISTS (SELECT 1 FROM requests WHERE id = NEW.request_id AND pool_id = '#{setup.pool.id}'::uuid) THEN PERFORM pg_advisory_xact_lock(#{lock_key}); END IF; RETURN NEW; END $$")
      Repo.query!("CREATE TRIGGER #{trigger} BEFORE UPDATE OF response_metadata ON attempts FOR EACH ROW EXECUTE FUNCTION #{trigger}()")

      on_exit(fn ->
        Repo.query!("DROP TRIGGER IF EXISTS #{trigger} ON attempts")
        Repo.query!("DROP FUNCTION IF EXISTS #{trigger}()")
      end)

      parent = self()

      holder =
        Task.async(fn ->
          Repo.transaction(fn ->
            Repo.query!("SELECT pg_advisory_xact_lock($1)", [lock_key])
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {:receipt_gate, backend})

            receive do
              :release -> :ok
            after
              2 * @budget -> raise "receipt gate release missing"
            end
          end)
        end)

      assert_receive {:receipt_gate, holder_backend}, @budget
      {conn, reference} = start_request(port, setup, original, thread)
      {conn, _terminal} = read_http_terminal(conn, reference, "")
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)

      if context.expires? do
        Repo.query!("UPDATE requests SET completed_at = clock_timestamp() - interval '29 seconds' WHERE id = $1", [Ecto.UUID.dump!(first.id)])
      end

      attempt = Repo.get_by!(Attempt, request_id: first.id)
      assert attempt.response_metadata["native_content_filter_terminal"]["reason"] == "content_filter"
      refute attempt.response_metadata["downstream_delivery"]
      writer_backend = await_relation_waiter(holder_backend, "attempts", System.monotonic_time(:millisecond) + @budget)
      successor = Map.update!(original, "input", &(&1 ++ [guidance()]))
      headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"x-codex-turn-metadata", original["client_metadata"]["x-codex-turn-metadata"]}, {"originator", "codex_cli_rs"}]
      contender = Task.async(fn -> Req.post!("http://127.0.0.1:#{port}#{@path}", headers: headers, json: successor, retry: false, receive_timeout: @budget).status end)

      try do
        retry_backend = await_relation_waiter(writer_backend, "attempts", System.monotonic_time(:millisecond) + @budget)
        assert length(Enum.uniq([holder_backend, writer_backend, retry_backend])) == 3
        assert FakeUpstream.count(upstream) == 1
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 1
        if context.expires?, do: await_retry_expired(first.id, System.monotonic_time(:millisecond) + @budget)
        send(holder.pid, :release)
        assert {:ok, :ok} = Task.await(holder, @budget)
        assert Task.await(contender, @budget) == if(context.expires?, do: 409, else: 200)
        assert FakeUpstream.count(upstream) == if(context.expires?, do: 1, else: 2)
      after
        send(holder.pid, :release)
        Task.shutdown(holder, :brutal_kill)
        Task.shutdown(contender, :brutal_kill)
        Mint.HTTP.close(conn)
      end
    end
  end

  defp await_retry_expired(request_id, deadline) do
    %{rows: [[expired?]]} = Repo.query!("SELECT clock_timestamp() > completed_at + interval '30 seconds' FROM requests WHERE id = $1", [Ecto.UUID.dump!(request_id)])

    cond do
      expired? ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        flunk("retry window did not expire")

      true ->
        Process.sleep(10)
        await_retry_expired(request_id, deadline)
    end
  end

  defp read_http_terminal(conn, reference, acc) do
    assert {:ok, conn, messages} = Mint.HTTP.recv(conn, 0, @budget)

    acc =
      Enum.reduce(messages, acc, fn
        {:data, ^reference, bytes}, acc -> acc <> bytes
        _, acc -> acc
      end)

    if String.contains?(acc, "response.incomplete") and String.ends_with?(acc, "\n\n"), do: {conn, true}, else: read_http_terminal(conn, reference, acc)
  end

  defp await_relation_waiter(holder, relation, deadline) do
    %{rows: rows} = Repo.query!("SELECT DISTINCT a.pid FROM pg_stat_activity a JOIN pg_locks l ON l.pid = a.pid WHERE $1 = ANY(pg_blocking_pids(a.pid)) AND l.relation = $2::text::regclass", [holder, relation])

    cond do
      length(rows) == 1 ->
        hd(hd(rows))

      System.monotonic_time(:millisecond) > deadline ->
        flunk("expected PostgreSQL relation waiter missing")

      true ->
        Process.sleep(10)
        await_relation_waiter(holder, relation, deadline)
    end
  end

  for mode <- ["full", "lite"], forwarding? <- [false, true], retained? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?, retained?: retained?
    test "#{mode} websocket owner #{forwarding?} content-filter retains #{retained?}", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      output = if context.retained?, do: [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}], else: []
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      frames = Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++ [terminal]
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      setup = gateway_setup(upstream, compact?: true)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0) |> Map.put("type", "response.create")
      original = if context.mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.incomplete"
      Mint.HTTP.close(conn)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      successor = Map.put(original, "input", input ++ output ++ [guidance()])
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(successor))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert terminal["type"] == "response.completed"
      assert FakeUpstream.count(upstream) == 2
      assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 2
    end
  end

  # Codex closes its connection right after reading a content-filter terminal. The socket confirms what it pushed at its next callback and again at `terminate/2`, and a close that wins that race takes the connection's port with it, where the driver queue can no longer be read (findings#303 row 303-4). The race is held open at Bandit's write of the terminal: a telemetry handler runs in the socket's own connection process right after that write, parks it until the client's close has taken the port, and only then lets it reach its next callback.
  for mode <- ["full", "lite"], forwarding? <- [false, true] do
    @tag mode: mode, forwarding?: forwarding?
    test "#{mode} websocket owner #{forwarding?} content-filter terminal stays delivered when the client closes before the socket's next callback", context do
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, context.forwarding?)
      output = [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}]
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      frames = Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++ [terminal]
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.websocket_text_frames(Enum.map(frames, &CodexPooler.JSON.encode!/1)), FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])]))
      setup = gateway_setup(upstream, compact?: true)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      original = payload(setup, thread, input, 0) |> Map.put("type", "response.create")
      original = if context.mode == "lite", do: put_in(original, ["client_metadata", "ws_request_header_x_openai_internal_codex_responses_lite"], "true"), else: original
      hold = hold_after_terminal_write!(setup)
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      on_exit(fn -> Mint.HTTP.close(conn) end)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(original))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.incomplete"
      assert_receive {:terminal_written, ^hold, socket}, @budget
      port_monitor = Port.monitor(connection_port!(socket))
      Mint.HTTP.close(conn)
      assert_receive {:DOWN, ^port_monitor, :port, _port, _reason}, @budget
      send(socket, {hold, :release})
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      await_delivery(first, System.monotonic_time(:millisecond) + @budget, %{type: terminal["type"], reason: get_in(terminal, ["response", "incomplete_details", "reason"])})
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      successor = Map.put(original, "input", input ++ output ++ [guidance()])
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(successor))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      Mint.HTTP.close(conn)
      assert terminal["type"] == "response.completed"
      assert FakeUpstream.count(upstream) == 2
    end
  end

  for mode <- ["full", "lite"], retained? <- [false, true], resume? <- [false, true] do
    @tag mode: mode, retained?: retained?, resume?: resume?
    test "#{mode} content-filter retry retains complete output #{retained?} after compaction #{resume?}", context do
      CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      :ok = Sandbox.mode(Repo, :auto)
      output = if context.retained?, do: [%{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [], "encrypted_content" => "synthetic"}], else: []
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => output, "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_complete", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      chunk = Enum.map_join(output, &event(%{"type" => "response.output_item.done", "item" => &1})) <> event(terminal)
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.raw_response(chunk, headers: [{"content-type", "text/event-stream"}]), FakeUpstream.raw_response(event(completed), headers: [{"content-type", "text/event-stream"}])]))
      setup = gateway_setup(upstream, compact?: true)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, context.mode)
      setup = Map.put(setup, :serving_mode, context.mode)
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      input = native_text_input("synthetic")
      input = if context.resume?, do: input ++ [%{"type" => "compaction", "encrypted_content" => "synthetic-compaction"}], else: input
      original = payload(setup, thread, input, 0)
      assert {200, _} = post(port, setup, original, thread)
      first = await_latest_settled(setup, System.monotonic_time(:millisecond) + @budget)
      assert first.status == "succeeded"
      guidance = guidance()
      successor = Map.put(original, "input", input ++ output ++ [guidance])

      for changed <- [Map.put(successor, "instructions", "synthetic changed options"), put_in(successor, ["input", Access.at(0), "content"], [%{"type" => "input_text", "text" => "synthetic changed original input"}])] do
        assert {409, _} = post(port, setup, changed, thread)
        assert FakeUpstream.count(upstream) == 1
      end

      first_attempt = Repo.get_by!(Attempt, request_id: first.id)
      setup.model |> Ecto.Changeset.change(upstream_model_id: "synthetic-changed-mapping") |> Repo.update!()
      assert {409, _} = post(port, setup, successor, thread)
      assert FakeUpstream.count(upstream) == 1
      Repo.get!(CodexPooler.Catalog.Model, setup.model.id) |> Ecto.Changeset.change(upstream_model_id: setup.model.upstream_model_id) |> Repo.update!()

      first |> Ecto.Changeset.change(completed_at: DateTime.add(first.completed_at, -31, :second)) |> Repo.update!()
      assert {409, _} = post(port, setup, successor, thread)
      Repo.get!(Request, first.id) |> Ecto.Changeset.change(completed_at: first.completed_at) |> Repo.update!()
      identity = Repo.get!(CodexPooler.Upstreams.Schemas.UpstreamIdentity, setup.identity.id)
      identity |> Ecto.Changeset.change(metadata: Map.put(identity.metadata, "credential_epoch", 2)) |> Repo.update!()
      assert {409, _} = post(port, setup, successor, thread)
      Repo.get!(CodexPooler.Upstreams.Schemas.UpstreamIdentity, identity.id) |> Ecto.Changeset.change(metadata: identity.metadata) |> Repo.update!()
      key = Repo.get!(CodexPooler.Access.APIKey, setup.api_key.id)
      key |> Ecto.Changeset.change(runtime_revocation_epoch: key.runtime_revocation_epoch + 1) |> Repo.update!()
      {status, _} = post(port, setup, successor, thread)
      assert status in [401, 403, 409]
      Repo.get!(CodexPooler.Access.APIKey, key.id) |> Ecto.Changeset.change(runtime_revocation_epoch: key.runtime_revocation_epoch) |> Repo.update!()
      assert FakeUpstream.count(upstream) == 1

      for mutation <- [:marker_missing, :marker_version, :reason, :delivery, :output_missing, :output_saturated, :attempt_source, :poisoned] do
        metadata = corrupt_proof(first_attempt.response_metadata, mutation)
        first_attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
        assert {409, _} = post(port, setup, successor, thread)
        assert FakeUpstream.count(upstream) == 1
        assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 1
        Repo.get!(Attempt, first_attempt.id) |> Ecto.Changeset.change(response_metadata: first_attempt.response_metadata) |> Repo.update!()
      end

      if context.mode == "full" and not context.retained? do
        race_successor(port, setup, successor, thread, first.request_metadata["codex_session_id"])
      else
        assert {200, _} = post(port, setup, successor, thread)
      end

      assert FakeUpstream.count(upstream) == 2
      requests = Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id)
      assert length(requests) == 2
      admitted = Enum.find(requests, &(&1.id != first.id))
      binding = admitted.request_metadata["native_content_filter_binding"]
      scope = %{assignment_id: binding["assignment_id"], identity_id: binding["identity_id"], credential_epoch: binding["credential_epoch"], serving_mode: binding["serving_mode"], effective_model: binding["effective_model"], upstream_model: binding["upstream_model"]}
      assert NativeContentFilterRetry.dispatch_allowed?(admitted, scope)

      for key <- [:assignment_id, :identity_id, :credential_epoch, :serving_mode, :effective_model, :upstream_model] do
        value = if key == :credential_epoch, do: 2, else: "changed"
        refute NativeContentFilterRetry.dispatch_allowed?(admitted, Map.put(scope, key, value))
      end

      stripped = admitted |> Ecto.Changeset.change(request_metadata: Map.delete(admitted.request_metadata, "native_content_filter_binding")) |> Repo.update!()
      refute NativeContentFilterRetry.dispatch_allowed?(stripped, scope)
      Repo.get!(Request, admitted.id) |> Ecto.Changeset.change(request_metadata: admitted.request_metadata) |> Repo.update!()

      for request <- requests do
        assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
        assert Repo.aggregate(from(l in CodexPooler.Accounting.LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
      end
    end
  end

  defp corrupt_proof(metadata, :marker_missing), do: Map.delete(metadata, "native_content_filter_terminal")
  defp corrupt_proof(metadata, :marker_version), do: put_in(metadata, ["native_content_filter_terminal", "version"], 2)
  defp corrupt_proof(metadata, :reason), do: put_in(metadata, ["native_content_filter_terminal", "reason"], "interrupted")
  defp corrupt_proof(metadata, :delivery), do: put_in(metadata, ["downstream_delivery", "outcome"], "aborted")
  defp corrupt_proof(metadata, :output_missing), do: Map.delete(metadata, "native_http_resume_progress")
  defp corrupt_proof(metadata, :output_saturated), do: put_in(metadata, ["native_client_retry_observation", "output_item_done_count_saturated"], true)
  defp corrupt_proof(metadata, :attempt_source), do: put_in(metadata, ["native_content_filter_source", "attempt_id"], Ecto.UUID.generate())
  defp corrupt_proof(metadata, :poisoned), do: Map.put(metadata, "native_client_retry_authority_loss", %{"version" => 1, "authority_lost_reason" => "malformed_event"})

  defp race_successor(port, setup, payload, thread, session_id) do
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          Repo.query!("SELECT id FROM codex_sessions WHERE id = $1 FOR UPDATE", [Ecto.UUID.dump!(session_id)])
          %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
          send(parent, {:session_locked, backend})

          receive do
            :release -> :ok
          after
            2 * @budget -> raise "holder release missing"
          end
        end)
      end)

    assert_receive {:session_locked, holder_backend}, @budget
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:0"}, {"x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"]}, {"originator", "codex_cli_rs"}]
    contenders = for _ <- 1..2, do: Task.async(fn -> Req.post!("http://127.0.0.1:#{port}#{@path}", headers: headers, json: payload, retry: false, receive_timeout: @budget).status end)

    try do
      backends = await_blocked_backends(holder_backend, System.monotonic_time(:millisecond) + @budget)
      assert length(Enum.uniq(backends)) == 2
      assert holder_backend not in backends
      send(holder.pid, :release)
      assert {:ok, :ok} = Task.await(holder, @budget)
      assert Enum.sort(Enum.map(contenders, &Task.await(&1, @budget))) == [200, 409]
      assert {409, _} = post(port, setup, payload, thread)
    after
      send(holder.pid, :release)
      Task.shutdown(holder, :brutal_kill)
      Enum.each(contenders, &Task.shutdown(&1, :brutal_kill))
    end
  end

  defp await_blocked_backends(holder, deadline) do
    %{rows: rows} = Repo.query!("WITH RECURSIVE blocked(pid) AS (SELECT a.pid FROM pg_stat_activity a WHERE $1 = ANY(pg_blocking_pids(a.pid)) UNION SELECT a.pid FROM pg_stat_activity a JOIN blocked b ON b.pid = ANY(pg_blocking_pids(a.pid))) SELECT DISTINCT b.pid FROM blocked b JOIN pg_locks l ON l.pid = b.pid WHERE l.relation = 'codex_sessions'::regclass", [holder])

    cond do
      length(rows) == 2 ->
        List.flatten(rows)

      System.monotonic_time(:millisecond) > deadline ->
        flunk("two independent PostgreSQL contenders did not reach the held session")

      true ->
        Process.sleep(10)
        await_blocked_backends(holder, deadline)
    end
  end

  defp guidance, do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "<content_filter_guidance>\nsynthetic guidance\n</content_filter_guidance>"}]}

  defp receive_terminal(conn, websocket, ref) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal when type in ["response.completed", "response.incomplete", "response.failed", "error"] -> {conn, websocket, terminal}
      _progress -> receive_terminal(conn, websocket, ref)
    end
  end

  # Runs in the socket's connection process right after Bandit wrote a frame (ThousandIsland reports each successful write synchronously, in that process): the frame that carries the content-filter terminal parks the process until the test releases it, or until the test is gone. The socket is the one process of the listener subscribed to its Pool's events. The write watch's own send handler, attached when the application boots, must read the write's driver queue before this one parks the process: a read after the port exited does not count the terminal, and the arm then fails `aborted`. Telemetry calls handlers in attachment order without promising it (`:telemetry.persist/0` reverses it), so the hold asserts that order, which `:telemetry.list_handlers/1` reports as dispatched.
  def park_after_terminal_write(_event, %{data: data}, _metadata, %{test: test, topic: topic, hold: hold}) do
    if String.contains?(IO.iodata_to_binary(data), "response.incomplete") and List.keymember?(Registry.lookup(CodexPooler.PubSub, topic), self(), 0) do
      test_monitor = Process.monitor(test)
      send(test, {:terminal_written, hold, self()})

      receive do
        {^hold, :release} -> :ok
        {:DOWN, ^test_monitor, :process, ^test, _reason} -> :ok
      end

      Process.demonitor(test_monitor, [:flush])
    end

    :ok
  end

  defp hold_after_terminal_write!(setup) do
    hold = make_ref()
    handler_id = {__MODULE__, hold}
    on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{test: self(), topic: CodexPooler.Events.pubsub_topic(setup.pool.id, "pools"), hold: hold}
    :ok = :telemetry.attach(handler_id, [:thousand_island, :connection, :send], &__MODULE__.park_after_terminal_write/4, config)
    watch = {CodexPoolerWeb.WebsocketDownstreamWriteWatch, :send}
    assert [^watch, ^handler_id] = for(%{id: id} <- :telemetry.list_handlers([:thousand_island, :connection, :send]), id in [watch, handler_id], do: id)
    hold
  end

  defp connection_port!(socket) do
    {:links, links} = Process.info(socket, :links)
    Enum.find(links, &(is_port(&1) and Port.info(&1, :name) == {:name, ~c"tcp_inet"})) || flunk("the websocket connection process owns no TCP port")
  end

  defp await_delivery(%Request{id: request_id} = selected_request, deadline, observed_terminal) do
    attempt = Repo.get_by!(Attempt, request_id: request_id)

    cond do
      get_in(attempt.response_metadata, ["downstream_delivery", "outcome"]) == "delivered" ->
        :ok

      System.monotonic_time(:millisecond) > deadline ->
        request = Repo.get!(Request, request_id)
        metadata = attempt.response_metadata || %{}
        receipt = metadata["downstream_delivery"] || %{}
        source = metadata["native_content_filter_source"] || %{}
        latest_attempt = Repo.one(from a in Attempt, where: a.request_id == ^request_id, order_by: [desc: a.attempt_number], limit: 1)
        pool_request_count = Repo.aggregate(from(r in Request, where: r.pool_id == ^selected_request.pool_id), :count)
        flunk("delivery receipt not delivered: " <> CodexPooler.JSON.encode!(%{observed_terminal: observed_terminal, selected_request_completed_at: selected_request.completed_at, pool_request_count: pool_request_count, attempt_request_match: attempt.request_id == request_id, attempt_number: attempt.attempt_number, latest_attempt_match: latest_attempt && latest_attempt.id == attempt.id, request_id: request_id, request_status: request.status, request_error: request.last_error_code, attempt_id: attempt.id, attempt_status: attempt.status, replay_generation: attempt.replay_generation, receipt_present: Map.has_key?(metadata, "downstream_delivery"), outcome: receipt["outcome"], terminal_class: receipt["terminal_class"], highest_frame_class: receipt["highest_frame_class"], incomplete_reason: receipt["incomplete_reason"], completed_items: receipt["completed_items"], digest_count: length(receipt["completed_item_digests"] || []), write_failure: receipt["write_failure"], source_attempt_match: source["attempt_id"] == attempt.id, marker_reason: get_in(metadata, ["native_content_filter_terminal", "reason"])}))

      true ->
        Process.sleep(10)
        await_delivery(selected_request, deadline, observed_terminal)
    end
  end

  defp payload(setup, thread, input, window) do
    %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread, "session_id" => thread, "turn_id" => "synthetic_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:#{window}", "window_number" => window})}}
  end

  defp event(data), do: "event: #{data["type"]}\ndata: " <> CodexPooler.JSON.encode!(data) <> "\n\n"

  defp start_request(port, setup, payload, thread) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    metadata = payload["client_metadata"]["x-codex-turn-metadata"]
    window = CodexPooler.JSON.decode!(metadata)["window_number"]
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", "#{thread}:#{window}"}, {"x-codex-turn-metadata", metadata}, {"originator", "codex_cli_rs"}]
    headers = if setup.serving_mode == "lite", do: [{"x-openai-internal-codex-responses-lite", "true"} | headers], else: headers
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", @path, headers, CodexPooler.JSON.encode!(payload))
    {conn, ref}
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

  defp await_latest_settled(setup, deadline) do
    row = Repo.one(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [desc: r.admitted_at], limit: 1)

    cond do
      row && row.completed_at ->
        row

      System.monotonic_time(:millisecond) > deadline ->
        flunk("request never finalized")

      true ->
        Process.sleep(10)
        await_latest_settled(setup, deadline)
    end
  end
end
