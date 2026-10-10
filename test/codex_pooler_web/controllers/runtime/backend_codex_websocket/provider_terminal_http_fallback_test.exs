defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.ProviderTerminalHttpFallbackTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, stop_websocket_owner_session: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @budget 15_000
  @path "/backend-api/codex/responses"

  setup_all do
    %{peer: start_shared_bridge_peer!()}
  end

  for mode <- ["full", "lite"], topology <- [:off, :local, :remote], visible? <- [false, true] do
    test "#{mode} #{topology} HTTP fallback after provider terminal stays linked, model output=#{visible?}", context do
      run_fallback(unquote(mode), unquote(topology), unquote(visible?), context)
    end
  end

  for mode <- ["full", "lite"], fault <- [:expired, :epoch, :witness, :other_key] do
    test "#{mode} #{fault} provider-terminal HTTP fallback preserves the existing fence", context do
      run_fallback(unquote(mode), :off, true, context, unquote(fault))
    end
  end

  for mode <- ["full", "lite"] do
    test "#{mode} a live visible websocket keeps its HTTP fallback fenced until settlement" do
      :ok = Sandbox.mode(Repo, :auto)
      on_exit(fn -> Sandbox.mode(Repo, :manual) end)
      CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
      release = make_ref()
      {:websocket_text, frames} = provider_failure_frames(true)
      reply = FakeUpstream.barrier_websocket_frames(frames, notify: self(), release_ref: release)
      upstream = start_upstream(FakeUpstream.strict_sequence([FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, respond: reply), FakeUpstream.expect_request(method: "POST", path: @path, respond: completed_stream())]))
      setup = gateway_setup(upstream)
      register_unboxed_pool_cleanup!(setup)
      set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      :ok = CodexPooler.Events.subscribe_pool(setup.pool)
      port = start_public_endpoint!()
      thread = "synthetic-live-#{Ecto.UUID.generate()}"
      metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn"}
      encoded = CodexPooler.JSON.encode!(metadata)
      payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic live fallback"), "stream" => true, "generate" => true, "client_metadata" => Map.put(metadata, "x-codex-turn-metadata", encoded)}
      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)

      try do
        {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
        assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release}, @budget
        assert :ok = FakeUpstream.release_frame(upstream, release)
        assert_receive {:fake_upstream_frame_barrier, 1, _handler, ^release}, @budget
        {conn, websocket, created} = public_websocket_receive_text!(conn, websocket, ref)
        assert CodexPooler.JSON.decode!(created)["type"] == "response.created"
        assert :ok = FakeUpstream.release_frame(upstream, release)
        assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^release}, @budget
        {conn, websocket, delta} = public_websocket_receive_text!(conn, websocket, ref)
        assert CodexPooler.JSON.decode!(delta)["type"] == "response.output_text.delta"
        [original] = pool_requests(setup)
        assert original.status == "in_progress"
        assert Repo.get_by!(CodexTurn, request_id: original.id).first_visible_output_at
        post = fn -> Req.post!("http://127.0.0.1:#{port}#{@path}", headers: [{"authorization", setup.authorization}, {"x-codex-turn-state", thread}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-turn-metadata", encoded}], json: Map.delete(payload, "type"), retry: false, decode_body: false, receive_timeout: @budget) end
        response = post.()
        assert response.status == 409, "live predecessor fallback status=#{response.status}, provider calls=#{FakeUpstream.http_request_count(upstream)}"
        assert FakeUpstream.http_request_count(upstream) == 0
        assert :ok = FakeUpstream.release_remaining_frames(upstream, release)
        assert_receive {:fake_upstream_frame_barrier, 3, _handler, ^release}, @budget
        {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
        assert terminal["type"] == "response.failed"
        assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "failed"}}}, @budget
        Mint.HTTP.close(conn)
        assert post.().status == 200
        assert [predecessor, successor] = pool_requests(setup)
        assert linked?(predecessor, successor)
        assert FakeUpstream.http_request_count(upstream) == 1
        assert :ok = FakeUpstream.verify!(upstream)
      after
        FakeUpstream.release_remaining_frames(upstream, release)
        Mint.HTTP.close(conn)
      end
    end
  end

  defp run_fallback(mode, topology, visible?, context, fault \\ :none) do
    :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, topology != :off)

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence(
          [
            FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, respond: provider_failure_frames(visible?))
          ] ++ if(fault in [:none, :other_key], do: [FakeUpstream.expect_request(method: "POST", path: @path, respond: completed_stream())], else: [])
        )
      )

    setup = gateway_setup(upstream)
    if topology != :remote, do: register_unboxed_pool_cleanup!(setup)
    set_model_serving_mode!(model_serving_scope(), setup, mode)

    on_exit(fn ->
      for session <- Repo.all(from s in CodexSession, where: s.pool_id == ^setup.pool.id), do: stop_websocket_owner_session(session.id)
    end)

    :ok = CodexPooler.Events.subscribe_pool(setup.pool)
    port = start_public_endpoint!()
    thread = "synthetic-fallback-#{Ecto.UUID.generate()}"
    window = "#{thread}:0"
    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn", "window_id" => window, "window_number" => 0}
    encoded_metadata = CodexPooler.JSON.encode!(metadata)
    payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic fallback"), "stream" => true, "generate" => true, "client_metadata" => Map.put(metadata, "x-codex-turn-metadata", encoded_metadata)}
    sockets = WebsocketCleanupFence.listener_sockets()
    if topology == :remote, do: start_shared_peer_window_owner!(setup, window, context.peer)
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, [{"x-codex-window-id", window}, {"session-id", thread}, {"thread-id", thread}])
    socket = WebsocketCleanupFence.await_new_listener_socket!(sockets)

    try do
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {conn, _websocket, terminal} = receive_terminal(conn, websocket, ref)
      assert terminal["type"] == "response.failed"
      assert_receive {CodexPooler.Events, %{reason: "request_finalized", payload: %{"status" => "failed"}}}, @budget
      Mint.HTTP.close(conn)
      :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)

      [predecessor] = pool_requests(setup)
      if topology == :remote, do: assert(predecessor.request_metadata["websocket_owner_forwarding"]["owner_instance_id"] == Atom.to_string(context.peer))
      alter_scope(fault, setup, predecessor)
      payload = if fault == :witness, do: Map.put(payload, "input", native_text_input("changed synthetic witness")), else: payload
      authorization = fallback_authorization(fault, setup)
      response = Req.post!("http://127.0.0.1:#{port}#{@path}", headers: [{"authorization", authorization}, {"x-codex-turn-state", thread}, {"session-id", thread}, {"thread-id", thread}, {"x-codex-window-id", window}, {"x-codex-turn-metadata", encoded_metadata}], json: Map.delete(payload, "type"), retry: false, decode_body: false, receive_timeout: @budget)

      assert_fallback_result(fault, response, setup, predecessor, upstream)

      assert :ok = FakeUpstream.verify!(upstream)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp fallback_authorization(:other_key, setup), do: CodexPooler.PoolerFixtures.api_key_fixture(setup.pool, %{scope: model_serving_scope()}).authorization
  defp fallback_authorization(_fault, setup), do: setup.authorization

  defp assert_fallback_result(:other_key, response, setup, predecessor, upstream) do
    assert response.status == 200
    assert [^predecessor, independent] = pool_requests(setup)
    assert independent.api_key_id != predecessor.api_key_id
    assert independent.status == "succeeded"
    refute linked?(predecessor, independent)
    assert is_nil(independent.request_metadata["client_resend"])
    assert FakeUpstream.http_request_count(upstream) == 1
  end

  defp assert_fallback_result(:none, response, setup, predecessor, upstream) do
    assert response.status == 200
    assert [^predecessor, successor] = pool_requests(setup)
    assert {predecessor.status, predecessor.last_error_code, successor.status, successor.transport} == {"failed", "server_error", "succeeded", "http_sse"}
    assert linked?(predecessor, successor), "HTTP fallback dispatched without its verified predecessor link"
    for request <- [predecessor, successor], do: assert(Repo.aggregate(from(e in LedgerEntry, where: e.request_id == ^request.id and e.entry_kind == "settlement"), :count) == 1)
    assert FakeUpstream.http_request_count(upstream) == 1
  end

  defp assert_fallback_result(fault, response, setup, _predecessor, upstream) do
    assert response.status == 409, "scope=#{fault}, response=#{response.status}"
    assert Repo.aggregate(from(r in Request, where: r.pool_id == ^setup.pool.id), :count) == 1
    assert FakeUpstream.http_request_count(upstream) == 0
  end

  defp pool_requests(setup), do: Repo.all(from r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at])

  defp alter_scope(:expired, _setup, predecessor) do
    expired = DateTime.add(DateTime.utc_now(), -31, :second)
    Repo.update_all(from(r in Request, where: r.id == ^predecessor.id), set: [completed_at: expired])
    Repo.update_all(from(a in Attempt, where: a.request_id == ^predecessor.id), set: [completed_at: expired])
    Repo.update_all(from(t in CodexTurn, where: t.request_id == ^predecessor.id), set: [completed_at: expired])
  end

  defp alter_scope(:epoch, setup, _predecessor), do: Repo.update_all(from(k in APIKey, where: k.id == ^setup.api_key.id), inc: [runtime_revocation_epoch: 1])
  defp alter_scope(_fault, _setup, _predecessor), do: :ok

  defp linked?(predecessor, successor), do: successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id or Repo.exists?(from l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id and l.successor_request_id == ^successor.id)

  defp receive_terminal(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    decoded = CodexPooler.JSON.decode!(text)
    if decoded["type"] in ["response.failed", "error", "response.completed"], do: {conn, websocket, decoded}, else: receive_terminal(conn, websocket, ref)
  end

  defp provider_failure_frames(visible?) do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_synthetic_terminal_failure", "status" => "in_progress"}}
    delta = %{"type" => "response.output_text.delta", "item_id" => "msg_synthetic", "output_index" => 0, "content_index" => 0, "delta" => "synthetic"}
    failed = %{"type" => "response.failed", "response" => %{"id" => "resp_synthetic_terminal_failure", "status" => "failed", "error" => %{"code" => "server_error", "message" => "synthetic provider failure"}}}
    FakeUpstream.websocket_text_frames(Enum.map(if(visible?, do: [created, delta, failed], else: [created, failed]), &CodexPooler.JSON.encode!/1))
  end

  defp completed_stream, do: FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_terminal_success", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}}])
end
