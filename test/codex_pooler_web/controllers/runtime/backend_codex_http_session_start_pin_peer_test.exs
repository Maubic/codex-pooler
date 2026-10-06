defmodule CodexPoolerWeb.Runtime.BackendCodexHttpSessionStartPinPeerTest do
  # The two-web-node arms of `backend_codex_http_session_start_pin_test.exs`
  # (findings#324): production runs two web pods, and a session's next request
  # lands on either. The pin a serving account claims is a row of
  # `codex_sessions`, so the next request reads it on the other node through
  # PostgreSQL, whichever node served the first.
  #
  # Topology: this node and a peer BEAM running the whole application with its
  # own PostgreSQL pool, each serving the real HTTP route on its own listener;
  # committed rows; owner forwarding off (HTTP is never forwarded); one Pool
  # with two accounts serving the model, forced to Full and to Lite; a
  # FakeUpstream per account on this node. `HttpSessionStartPinSupport`
  # describes the scenario. Synthetic text and identifiers.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.HttpSessionStartPinSupport

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.MailboxPrefixRaceSupport
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true

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
    test "#{mode}: a request on the other node while the first streams follows the account serving it", %{mode: mode, peer_node: peer_node} do
      gate = make_ref()
      s = session_start!(mode, p: [held_output(gate, "resp_pin_peer_stream_first"), completed_sse("resp_pin_peer_stream_second")], b: [completed_sse("resp_pin_peer_stream_moved")])
      peer_port = MailboxPrefixRaceSupport.start_peer_listener!(peer_node)
      {conn, ref, _item} = open_first!(s, "turn-peer-stream-first")
      handler = await_gate!(gate)

      assert {200, body} = post!(peer_port, s, "turn-peer-stream-second")
      assert body =~ "response.completed"

      release_gate(handler, gate)
      assert {200, _rest} = receive_all!(conn, ref, 200, "")
      assert_followed!(s)
      assert_served_on_nodes!(s, [node(), peer_node])
    end

    @tag mode: mode
    test "#{mode}: a request on this node follows the account the other node's first request is serving", %{mode: mode, peer_node: peer_node} do
      gate = make_ref()
      s = session_start!(mode, p: [held_output(gate, "resp_pin_peer_reverse_first"), completed_sse("resp_pin_peer_reverse_second")], b: [completed_sse("resp_pin_peer_reverse_moved")])
      peer_port = MailboxPrefixRaceSupport.start_peer_listener!(peer_node)
      {conn, ref, _item} = open_first!(s, "turn-peer-reverse-first", peer_port)
      handler = await_gate!(gate)

      assert {200, body} = post!(s, "turn-peer-reverse-second")
      assert body =~ "response.completed"

      release_gate(handler, gate)
      assert {200, _rest} = receive_all!(conn, ref, 200, "")
      assert_followed!(s)
      assert_served_on_nodes!(s, [peer_node, node()])
    end

    @tag mode: mode
    test "#{mode}: a request on the other node after the client cut the first request follows the account that served it", %{mode: mode, peer_node: peer_node} do
      gate = make_ref()
      s = session_start!(mode, p: [cut_output(gate, "resp_pin_peer_cut_first"), completed_sse("resp_pin_peer_cut_second")], b: [completed_sse("resp_pin_peer_cut_moved")])
      peer_port = MailboxPrefixRaceSupport.start_peer_listener!(peer_node)
      handler = consume_and_cut!(s, "turn-peer-cut-first", gate)
      release_gate(handler, gate)
      assert [_predecessor, %Request{status: "failed", last_error_code: "client_disconnected"}] = await_settled!(s, 2)

      assert {200, body} = post!(peer_port, s, "turn-peer-cut-second")
      assert body =~ "response.completed"
      assert_followed!(s)
      assert_served_on_nodes!(s, [node(), peer_node])
    end
  end

  # The session's first and second requests ran on the given nodes: the
  # attempt records the VM that executed it.
  defp assert_served_on_nodes!(s, nodes) do
    [_predecessor, first, second] = await_settled!(s, 3)
    assert Enum.map([first, second], &executor_node/1) == Enum.map(nodes, &Atom.to_string/1)
  end

  defp executor_node(%Request{id: id}) do
    Repo.one!(from(a in Attempt, where: a.request_id == ^id, order_by: [desc: a.attempt_number], limit: 1, select: a.owner_instance_id))
  end
end
