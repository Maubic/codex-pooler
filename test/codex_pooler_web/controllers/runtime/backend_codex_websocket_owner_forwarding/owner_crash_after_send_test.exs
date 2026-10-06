defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.OwnerCrashAfterSendTest do
  # A session's owner ends without its `terminate/2` while it runs a turn:
  # killed (the reuse check kills an owner blocked in a callback) or crashed.
  # The turn's submission catches the owner's exit and used to hand the turn
  # to a replacement owner whenever no frame of it had arrived, so a turn whose
  # payload the provider had already received went to the provider a second
  # time on a new connection, with one succeeded attempt in the request log
  # (findings#327). The upstream session now tells the submission before the
  # payload can leave, and the submission resubmits only a turn whose payload
  # never started to leave; any other turn settles `owner_crashed` (findings#325
  # row 325-6: settle after commit, never resend).
  #
  # One node: the real public listener, owner forwarding on, the session's
  # owner and its upstream session on this node, the Pool's serving mode
  # forced to Full or Lite, FakeUpstream over a real websocket, the released
  # client's native frames or a public `/v1` SDK request. The socket is held
  # while the owner goes (`OwnerCrashAfterSendScenario`), so its response task
  # meets the owner's exit first; the peer family is
  # `remote_owner_crash_after_send_test.exs`.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [receive_native_terminal!: 3, socket_connection_state!: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, RequestClientRetryLink}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Platform.ExecutionTerminalProofs
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario, as: Crash
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario

  @moduletag capture_log: true

  @crashed_close {:close, 1011, "websocket owner crashed"}
  @detection_timeout_ms 15_000

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for route <- [Scenario.native_route(), Scenario.public_route()], mode <- ["full", "lite"] do
    @tag route: route, mode: mode
    test "#{route} #{mode}: a turn the provider received settles owner_crashed when its owner is killed, and is never sent again", ctx do
      release_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_owner_sent", release_ref)
      %{setup: setup, client: client} = connect!(upstream, ctx)
      client = Crash.send_turn!(client, setup, "the turn the provider holds")

      :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_owner_sent", release_ref)
      assert Crash.generations(upstream) == 1
      %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)

      :ok = Crash.hold_socket!(client.socket)
      :ok = Crash.kill_owner!(owner)

      assert Crash.await_outcome!(upstream, request_id, 1) == :settled
      assert Crash.generations(upstream) == 1
      :ok = Crash.assert_settled_on_crash!(request_id, session_id)
      assert List.last(Crash.close_client!(client)) == @crashed_close
    end

    @tag route: route, mode: mode
    test "#{route} #{mode}: a turn whose payload never left is served once by a replacement owner when its owner is killed", ctx do
      release_ref = make_ref()
      upstream = Crash.upstream!(:before_write, "resp_owner_unsent", release_ref)
      %{setup: setup, client: client} = connect!(upstream, ctx)
      client = Crash.send_turn!(client, setup, "the turn whose connection is still opening")

      :ok = Crash.await_kill_point!(:before_write, upstream, "resp_owner_unsent", release_ref)
      assert Crash.generations(upstream) == 0
      %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)
      :ok = Crash.stop_session_owner_on_exit(node(), session_id)

      :ok = Crash.hold_socket!(client.socket)
      :ok = Crash.kill_owner!(owner)

      assert Crash.await_outcome!(upstream, request_id, 1) == :settled
      assert Crash.generations(upstream) == 1
      :ok = Crash.assert_recovered!(request_id, session_id)
      # The socket handles its killed owner's DOWN before the replacement's frames, as before.
      assert List.last(Crash.close_client!(client)) == @crashed_close
    end
  end

  # The forwarder's payload write observer has run (the turn counts as
  # started) and the payload is not written. Linked, the upstream session goes
  # with its killed owner and never writes; unlinked, it stands for a session
  # that handles its owner's exit only after its write and writes once
  # released. Either way the turn settles `owner_crashed` and nothing sends it
  # again.
  for link <- [:linked, :unlinked] do
    @tag link: link
    test "native full: an owner killed between the payload write observer and the write (#{link}) settles the turn without sending it again", ctx do
      release_ref = make_ref()
      hold_ref = make_ref()
      upstream = Crash.upstream!(:after_receipt, "resp_owner_marked", release_ref)
      setup = gateway_setup(upstream)
      :ok = Crash.serve!(setup, "full")
      window = Scenario.window()
      held = Crash.start_held_owner!(setup, window.id, node(), hold_ref, ctx.link)
      {_server, port} = start_public_endpoint_with_server!()
      client = Scenario.connect!(port, setup, Scenario.native_route(), window)
      client = Crash.send_turn!(client, setup, "the turn held at its write")

      assert_receive {:payload_write_held, session_pid, ^hold_ref, :ok}, @detection_timeout_ms
      assert Crash.generations(upstream) == 0
      request_id = Crash.turn_request_id!(setup)
      session_monitor = Process.monitor(session_pid)

      :ok = Crash.hold_socket!(client.socket)
      :ok = Crash.kill_owner!(held.owner_pid)
      assert Crash.await_outcome!(upstream, request_id, 0) == :settled

      case ctx.link do
        :linked ->
          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
          assert Crash.generations(upstream) == 0

        :unlinked ->
          :ok = Crash.release_held_write(session_pid, hold_ref)
          :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_owner_marked", release_ref)
          assert Crash.generations(upstream) == 1
          Process.exit(session_pid, :kill)
          assert_receive {:DOWN, ^session_monitor, :process, ^session_pid, :killed}, @detection_timeout_ms
      end

      :ok = Crash.assert_settled_on_crash!(request_id, held.session.id)
      assert List.last(Crash.close_client!(client)) == @crashed_close
      assert Crash.generations(upstream) == if(ctx.link == :linked, do: 0, else: 1)
    end
  end

  # The released client closes on the socket's 1011 and resends the turn whole
  # on a new socket. Once the proof of the attempt's executor's end exists the
  # duplicate-turn fence serves it once, linked to the settled turn, which stays
  # at no charge: the provider has the turn twice, once for each request.
  test "native full: the client's resend of a turn settled owner_crashed after its payload left is served once" do
    _publisher = CodexPooler.ExecutionProofSupport.start_publisher!()
    release_ref = make_ref()
    upstream = Crash.upstream!(:after_receipt, "resp_owner_sent", release_ref)
    %{setup: setup, client: client, port: port, window: window} = connect!(upstream, %{route: Scenario.native_route(), mode: "full"})
    frame = Crash.frame(setup, client, "the turn the provider holds")
    client = Crash.send_frame!(client, frame)

    :ok = Crash.await_kill_point!(:after_receipt, upstream, "resp_owner_sent", release_ref)
    %{owner: owner, session_id: session_id, request_id: request_id} = turn!(client, setup)
    :ok = Crash.hold_socket!(client.socket)
    :ok = Crash.kill_owner!(owner)
    assert Crash.await_outcome!(upstream, request_id, 1) == :settled
    :ok = Crash.assert_settled_on_crash!(request_id, session_id)
    assert List.last(Crash.close_client!(client)) == @crashed_close
    :ok = await_executor_proof!(request_id)

    retry = Scenario.connect!(port, setup, Scenario.native_route(), window)
    retry = Crash.send_frame!(retry, frame)
    {conn, websocket, served} = receive_native_terminal!(retry.conn, retry.websocket, retry.ref)
    Scenario.close!(%{retry | conn: conn, websocket: websocket})

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_owner_sent_written_again"}} = served
    assert [%RequestClientRetryLink{successor_request_id: successor_id}] = Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^request_id))
    assert Scenario.settled_statuses!(setup, 2) == ["failed", "succeeded"]
    assert Crash.generations(upstream) == 2
    assert settled_cost(request_id) == Decimal.new(0)
    assert Decimal.gt?(settled_cost(successor_id), 0)
  end

  defp connect!(upstream, ctx) do
    setup = gateway_setup(upstream)
    :ok = Crash.serve!(setup, ctx.mode)
    {_server, port} = start_public_endpoint_with_server!()
    window = Scenario.window()
    %{setup: setup, client: Scenario.connect!(port, setup, ctx.route, window), port: port, window: window}
  end

  defp await_executor_proof!(request_id) do
    attempt = Repo.one!(from(a in Attempt, where: a.request_id == ^request_id))
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_executor_proof!(attempt, deadline)
  end

  defp await_executor_proof!(attempt, deadline) do
    cond do
      ExecutionTerminalProofs.terminal?(attempt) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the executor's proof was never published")

      true ->
        receive do
        after
          20 -> await_executor_proof!(attempt, deadline)
        end
    end
  end

  defp settled_cost(request_id) do
    Repo.one!(from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", select: entry.settled_cost_micros))
    |> Decimal.normalize()
  end

  defp turn!(client, setup) do
    state = socket_connection_state!(client.socket)
    assert {:ok, owner} = WebsocketOwnerSession.lookup(state.codex_session.id)
    %{owner: owner, session_id: state.codex_session.id, request_id: Crash.turn_request_id!(setup)}
  end
end
