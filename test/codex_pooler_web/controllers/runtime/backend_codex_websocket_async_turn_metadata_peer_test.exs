defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketAsyncTurnMetadataPeerTest do
  # The remote-owner arm of findings#319 row 1. The session's owner and its
  # provider connection run on a second VM sharing the committed database; the
  # socket runs on this node. When a socket dies without its cleanup before any
  # output, the owner keeps its turn running for a resend to reattach to and
  # matches the reattach control on the exact replay claim of the request it
  # runs. The released client fills `workspaces` in its turn metadata after a
  # turn's first request can already have gone out, so its resend can be that
  # request with the field added: this node's replay preflight finds the lost
  # request's stored witness among the resend's alternates and rebinds the
  # frame to it, so the control the owner receives carries the claim it holds,
  # in the control's existing field. The module boots the peer VM once.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full), owner forwarding on, the session's owner on the peer VM, the
  # released client's upgrade headers and frame shape, synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint_with_server!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  import Ecto.Query

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @timeout_ms 15_000
  @poll_ms 20
  @workspaces %{"/synthetic/repository" => %{"associated_remote_urls" => %{"origin" => "https://example.com/sample-app.git"}, "latest_git_commit_hash" => String.duplicate("b", 40), "has_changes" => true}}

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  test "owner on another VM: the resend of a lost turn that gained the async workspaces reattaches to the running generation", %{peer_node: peer_node} do
    result = lost_turn!(peer_node, %{"workspaces" => @workspaces}, :reattached)

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_async_peer_turn"}} = result.outcome, "the resend was refused: #{inspect(result.outcome)} #{result.logs}"
    assert result.logs =~ "reconnect_disposition=same_turn_replay"
    assert [%Request{status: "succeeded"} = served] = await_settled!(result.setup)
    assert served.id == result.original.id
    assert Repo.all(from(a in Attempt, where: a.request_id == ^served.id, select: {a.replay_generation, a.status})) == [{0, "succeeded"}]
    assert FakeUpstream.count(result.upstream) == 1
  end

  # Only the fields the client fills late are set aside: a resend with another
  # document field changed is a different request, and the remote owner
  # refuses its reattach as before.
  test "owner on another VM: the resend of a lost turn with another document field changed is refused owner_busy", %{peer_node: peer_node} do
    result = lost_turn!(peer_node, %{"sandbox" => "workspace-write"}, :refused)

    assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = result.outcome
    assert result.logs =~ "reason_code=owner_busy"
    assert FakeUpstream.count(result.upstream) == 1
  end

  defp lost_turn!(peer_node, resend_document, expect) do
    release_ref = make_ref()
    enter_peer_owner_topology!()

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; the turn's completion held before its first frame until the resend attached or was refused
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, json: [valid: true, equals: %{"type" => "response.create"}], respond: FakeUpstream.barrier_websocket_frames(completed_frames("resp_async_peer_turn"), notify: self(), release_ref: release_ref))
        ])
      )

    setup = gateway_setup(upstream)
    thread = Ecto.UUID.generate()
    turn_id = Ecto.UUID.generate()
    owner = start_shared_peer_window_owner!(setup, "#{thread}:0", peer_node).owner_pid
    {_server, port} = start_public_endpoint_with_server!()
    input = native_text_input("synthetic peer turn")

    client = connect!(port, setup, thread)
    {_conn, _websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame(setup, thread, turn_id, input, %{}))
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @timeout_ms

    # The socket process dies without running its cleanup; the owner on the
    # peer marks the turn it runs lost and keeps it for a reattach.
    socket_pid = :sys.get_state(owner).downstream.pid
    assert node(socket_pid) == node()
    monitor = Process.monitor(socket_pid)
    Process.exit(socket_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^socket_pid, :killed}, @timeout_ms
    await_owner!(owner, &match?(%{active_turn: %{descriptor: %{downstream_status: :lost}}}, &1), "the owner never marked the turn lost")
    [original] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))

    {outcome, logs} =
      with_info_log(fn ->
        resend = connect!(port, setup, thread)
        {conn, websocket} = public_websocket_send_text!(resend.conn, resend.websocket, resend.ref, frame(setup, thread, turn_id, input, resend_document))

        if expect == :reattached do
          await_owner!(owner, &match?(%{active_turn: %{descriptor: %{downstream_status: :attached}}}, &1), "the resend never reattached to the running generation")
          :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
        end

        {conn, outcome} = receive_terminal!(conn, websocket, resend.ref)
        _closed = Mint.HTTP.close(conn)
        if expect == :refused, do: :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
        outcome
      end)

    %{original: original, outcome: outcome, logs: logs, setup: setup, upstream: upstream}
  end

  # The released client's upgrade: the session keyed by its window, the turn
  # state, the session and thread ids.
  defp connect!(port, setup, thread) do
    headers = [{"session-id", thread}, {"thread-id", thread}, {"x-client-request-id", thread}, {"x-codex-window-id", "#{thread}:0"}]
    {conn, websocket, ref, _response_headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, headers)
    %{conn: conn, websocket: websocket, ref: ref}
  end

  # The released client's frame: the turn metadata document in
  # `client_metadata` beside its flat copies.
  defp frame(setup, thread, turn_id, input, document) do
    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn_id, "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0"} |> Map.merge(document)

    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => input,
      "stream" => true,
      "generate" => true,
      "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn_id, "x-codex-window-id" => "#{thread}:0", "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}
    })
  end

  defp completed_frames(response_id) do
    [CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}})]
  end

  defp await_owner!(owner, ready?, message, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms

    cond do
      ready?.(:sys.get_state(owner)) ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_owner!(owner, ready?, message, deadline)
        end

      true ->
        flunk(message)
    end
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, frame},
      else: receive_terminal!(conn, websocket, ref)
  end

  defp await_settled!(setup, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms
    requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))

    cond do
      requests != [] and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_settled!(setup, deadline)
        end

      true ->
        flunk("the Pool's requests did not settle: #{inspect(Enum.map(requests, &{&1.status, &1.last_error_code}))}")
    end
  end
end
