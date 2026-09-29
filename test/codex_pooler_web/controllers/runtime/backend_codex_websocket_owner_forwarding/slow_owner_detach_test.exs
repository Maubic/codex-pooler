defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.SlowOwnerDetachTest do
  # The client closes its socket mid-turn while the session's remote owner is
  # alive but slower than the owner call budget (findings#270 row 270-257).
  # The socket's detach times out, and the detach is still waiting for the
  # owner, which runs it once it answers again. The socket used to take the
  # timeout for an owner that is gone: its owner-lost recovery interrupted the
  # turn `owner_unavailable` under the owner.
  #
  #   * A turn that showed output: the client's resend met
  #     `409 duplicate_turn`, where an owner that answered in time lets it
  #     through.
  #   * A pre-visible turn: its replay could no longer be armed. The owner
  #     generated the turn to its end for nobody, its late success rewrote the
  #     interruption as `succeeded`, and the resend was served by a second
  #     generation.
  #
  # The socket now leaves the turn to the owner, and both end as with an owner
  # that answers in time. Owner forwarding on, native route, the Pool's
  # default serving mode, the released client's frames, FakeUpstream holding
  # the turn before its first frame (or after two). The owner runs on a
  # second VM sharing the database and is suspended (`:sys.suspend/1`) past a
  # one-second owner call budget, set on both nodes, until the socket's
  # cleanup gave up on it.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [completed_response_frames: 4, receive_native_terminal!: 3, released_client_frame: 2, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]
  import CodexPoolerWeb.Runtime.UnreachableNodeSupport, only: [start_pacer!: 1, pace_upstream!: 2, await_frame_barrier!: 2, release_frames!: 2]

  alias CodexPooler.Accounting.{Request, RequestReplayEntitlement}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Transports.Websocket.OwnerDefaults
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @moduletag capture_log: true

  @owner_call_budget_ms 1_000
  @detection_timeout_ms 15_000

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup %{peer_node: peer_node} do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    enter_peer_owner_topology!()

    original_env = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(original_env, owner_call_timeout_ms: @owner_call_budget_ms))
    peer_env = :erpc.call(peer_node, Application, :get_env, [:codex_pooler, OwnerDefaults, []])
    :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, OwnerDefaults, Keyword.merge(peer_env, owner_call_timeout_ms: @owner_call_budget_ms)])

    # Through `:erpc` with the standard library only: this module is compiled
    # on this node alone.
    on_exit(fn ->
      if peer_node in Node.list(), do: :ok = :erpc.call(peer_node, Application, :put_env, [:codex_pooler, OwnerDefaults, peer_env])
    end)
  end

  @tag slow: "suspends a peer VM's owner past two one-second detach budgets while the client closes its socket mid-turn"
  test "a turn that showed output is settled client_disconnected once the owner answers, and the client's resend is served", ctx do
    turn = start_turn!(ctx, :visible)

    :ok = close_while_owner_suspended!(turn)

    # The owner cancelled the turn and settled it for the client that left.
    assert :ok = await_request!(turn.request_id, &match?(%Request{status: "failed", response_status_code: 499, last_error_code: "client_disconnected"}, &1))
    # The turn row settles right after its request's.
    assert :ok = await!(fn -> match?(%CodexTurn{status: "interrupted", error_code: "client_disconnected"}, Repo.get_by!(CodexTurn, request_id: turn.request_id)) end, "the turn never settled client_disconnected")

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_detach_successor"}} = resend!(turn)
    assert FakeUpstream.count(turn.upstream) == 3
  end

  @tag slow: "suspends a peer VM's owner past two one-second detach budgets while the client closes its socket before any output"
  test "a pre-visible turn's replay is armed once the owner answers, and the client's resend redeems it", ctx do
    turn = start_turn!(ctx, :previsible)

    :ok = close_while_owner_suspended!(turn)

    assert :ok = await_armed_replay!(turn.request_id)
    assert %{"type" => "response.completed", "response" => %{"id" => "resp_slow_detach_successor"}} = resend!(turn)

    # The replay settles the turn's own request: one generation for the resend,
    # none for nobody.
    assert :ok = await_request!(turn.request_id, &match?(%Request{status: "succeeded", response_status_code: 200}, &1))
    assert [_turn_one, _replayed] = Repo.all(from(r in Request, where: r.pool_id == ^turn.setup.pool.id))
    assert FakeUpstream.count(turn.upstream) == 3
  end

  # The session's owner on the peer, a first turn served, and the turn under
  # test held by FakeUpstream before any frame (`:visible`: after the created
  # event and a delta the client received).
  defp start_turn!(ctx, shown) do
    release_ref = make_ref()
    pacer = start_pacer!(release_ref)

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a client that leaves mid-turn while the session's owner does not answer within its call budget)
        FakeUpstream.repeat_last([
          completed_response_frames("resp_slow_detach_one", [], 3, 2),
          FakeUpstream.barrier_websocket_frames(turn_frames(), notify: pacer, release_ref: release_ref),
          completed_response_frames("resp_slow_detach_successor", [], 3, 2)
        ])
      )

    :ok = pace_upstream!(pacer, upstream)
    setup = gateway_setup(upstream)
    window = Scenario.window()
    owner = start_shared_peer_window_owner!(setup, window.id, ctx.peer_node).owner_pid
    {_server, port} = start_public_endpoint_with_server!()
    client = Scenario.connect!(port, setup, Scenario.native_route(), window)
    {client, _one} = Scenario.turn!(client, setup, "turn one")
    frame = released_client_frame(setup, window.thread).(native_text_input("the turn whose client leaves"), Ecto.UUID.generate(), %{})
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    :ok = await_frame_barrier!(pacer, 0)

    {conn, websocket} =
      if shown == :visible do
        :ok = release_frames!(pacer, 2)
        {conn, websocket, _created} = receive_text!(conn, websocket, client.ref, "response.created")
        {conn, websocket, _delta} = receive_text!(conn, websocket, client.ref, "response.output_text.delta")
        {conn, websocket}
      else
        {conn, websocket}
      end

    request_id = Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress", select: r.id))
    %{client: %{client | conn: conn, websocket: websocket}, owner: owner, upstream: upstream, setup: setup, window: window, port: port, frame: frame, request_id: request_id}
  end

  # The client closes its socket while the owner is suspended. The socket's
  # cleanup gives up on the owner's replay arm and on its detach, one call
  # budget each, and leaves the turn untouched; then the owner answers again.
  defp close_while_owner_suspended!(turn) do
    :ok = :sys.suspend(turn.owner)

    {_cleaned, log} =
      with_info_log(fn ->
        Mint.HTTP.close(turn.client.conn)
        :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(turn.client.socket, @detection_timeout_ms)
      end)

    assert log =~ "websocket owner detach left to the owner after its call budget"
    assert %Request{status: "in_progress"} = Repo.get!(Request, turn.request_id)
    assert %CodexTurn{status: "in_progress"} = Repo.get_by!(CodexTurn, request_id: turn.request_id)
    :sys.resume(turn.owner)
  end

  defp resend!(turn) do
    retry = Scenario.connect!(turn.port, turn.setup, Scenario.native_route(), turn.window)
    {conn, websocket} = public_websocket_send_text!(retry.conn, retry.websocket, retry.ref, turn.frame)
    {conn, websocket, terminal} = receive_native_terminal!(conn, websocket, retry.ref)
    Scenario.close!(%{retry | conn: conn, websocket: websocket})
    terminal
  end

  defp await_armed_replay!(request_id) do
    await!(fn -> match?(%RequestReplayEntitlement{status: "armed"}, Repo.get_by(RequestReplayEntitlement, request_id: request_id)) end, "the owner never armed the turn's replay")
  end

  defp await_request!(request_id, predicate), do: await!(fn -> predicate.(Repo.get!(Request, request_id)) end, "the turn's request never settled as expected")

  defp await!(fun, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_until!(fun, message, deadline)
  end

  defp await_until!(fun, message, deadline) do
    cond do
      fun.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk(message)

      true ->
        Process.sleep(20)
        await_until!(fun, message, deadline)
    end
  end

  defp receive_text!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => ^type} = event -> {conn, websocket, event}
      %{"type" => "codex.response.metadata"} -> receive_text!(conn, websocket, ref, type)
    end
  end

  defp turn_frames do
    created = %{"type" => "response.created", "response" => %{"id" => "resp_slow_detach_turn", "status" => "in_progress"}}
    deltas = for i <- 1..10, do: %{"type" => "response.output_text.delta", "delta" => "synthetic #{i} "}
    completed = %{"type" => "response.completed", "response" => %{"id" => "resp_slow_detach_turn", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => 10, "total_tokens" => 15}}}
    Enum.map([created | deltas] ++ [completed], &CodexPooler.JSON.encode!/1)
  end
end
