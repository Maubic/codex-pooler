defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocket.SettlementExecutorKillTest do
  # A response task killed inside its settlement transaction, with no rescue
  # to run (findings#288). The settlement writes the request, its attempt, its
  # ledger entries and the turn in one transaction, so the kill leaves nothing
  # half-committed: PostgreSQL rolls the whole transaction back with the
  # connection, and the request, attempt and turn stay `in_progress` with only
  # their reservation. The execution registry saw the task die, its terminal
  # proof is published, and the scheduled dead-execution recovery settles the
  # request, its attempt and its turn together; the released client's resend is
  # then served as the recovered request's successor.
  #
  # The settlement used to complete the turn in a second transaction: the same
  # kill there left the request and attempt settled `succeeded` behind a turn
  # still `in_progress`, which no recovery pass reaches.
  #
  # One node, committed rows, native websocket, owner forwarding off (the
  # socket's own response task settles), Full, FakeUpstream. The socket is
  # suspended across the kill so the recovery is the only path that can settle
  # the turn.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Platform.{ExecutionIdentity, ExecutionTerminalProofs}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.SettlementTransactionHold
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @detection_timeout_ms 15_000

  @tag slow: "kills a real response task inside its settlement transaction and waits for its connection's rollback and its durable proof"
  test "a response task killed inside its settlement transaction leaves nothing half-committed, and recovery settles request and turn together" do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (a served turn whose settling task dies inside its settlement transaction; the resend is served)
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: completed_frames("resp_killed_settler")),
          FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: completed_frames("resp_killed_settler_resend"))
        ])
      )

    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    watcher = SettlementTransactionHold.start_lock_watcher!()
    {server, port} = start_public_endpoint_with_server!()
    session_id = Ecto.UUID.generate()
    payload = turn_payload(setup, session_id)
    hold = SettlementTransactionHold.inside_transaction!()

    {conn, websocket, ref} = connect!(port, setup, session_id)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, payload)
    {settler, %{backend: settler_backend}} = SettlementTransactionHold.await_held!(hold)

    request = Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id))
    attempt = Repo.one!(from(a in Attempt, where: a.request_id == ^request.id))
    turn = Repo.one!(from(t in CodexTurn, where: t.request_id == ^request.id))
    assert attempt.owner_process_id |> String.to_charlist() |> :erlang.list_to_pid() == settler
    assert ExecutionIdentity.status(attempt) == :alive

    # The socket would settle the turn itself once its task is gone.
    assert {:ok, [socket]} = ThousandIsland.connection_pids(server)
    monitor = Process.monitor(settler)
    :erlang.suspend_process(socket)

    try do
      Process.exit(settler, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^settler, :killed}, @detection_timeout_ms
      :ok = await_backend_gone!(watcher, settler_backend)

      # Nothing of the settlement survived the kill.
      assert {Repo.reload!(request).status, Repo.reload!(attempt).status, Repo.reload!(turn).status} == {"in_progress", "in_progress", "in_progress"}
      assert ledger_kinds(request) == ["reservation"]
      assert ExecutionIdentity.status(attempt) == :dead

      :ok = CodexPooler.ExecutionProofSupport.publish_committed_terminal!(attempt)
      assert ExecutionTerminalProofs.terminal?(attempt)

      # The scheduled recovery entry, at a time past the liveness window.
      assert {:ok, %{dead_execution_attempts_recovered: 1}} = Accounting.recover_dead_execution_attempts(DateTime.add(DateTime.utc_now(), 121, :second))
    after
      :erlang.resume_process(socket)
    end

    assert %Request{status: "failed", last_error_code: "dead_execution_recovered"} = Repo.reload!(request)
    assert %Attempt{status: "failed"} = Repo.reload!(attempt)
    assert %CodexTurn{status: "interrupted", error_code: "dead_execution_recovered"} = Repo.reload!(turn)
    assert ledger_kinds(request) == ["release", "reservation", "settlement"]

    {conn, _websocket, terminal_frames} = receive_until_terminal(conn, websocket, ref, [])
    assert %{"type" => "response.completed"} = List.last(terminal_frames)
    assert %{"terminal_class" => "response.completed", "highest_frame_class" => "terminal"} = await_receipt!(attempt)

    socket_monitor = Process.monitor(socket)
    Mint.HTTP.close(conn)
    assert_receive {:DOWN, ^socket_monitor, :process, ^socket, _}, @detection_timeout_ms

    # The released client resends the same request on a new connection.
    {retry_conn, retry_websocket, retry_ref} = connect!(port, setup, session_id)
    {retry_conn, retry_websocket} = public_websocket_send_text!(retry_conn, retry_websocket, retry_ref, payload)
    {retry_conn, _retry_websocket, frames} = receive_until_terminal(retry_conn, retry_websocket, retry_ref, [])
    Mint.HTTP.close(retry_conn)

    assert %{"type" => "response.completed", "response" => %{"id" => "resp_killed_settler_resend"}} = List.last(frames)
    assert [_recovered, %Request{id: successor_id}] = await_settled!(setup)
    assert Repo.exists?(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^request.id and link.successor_request_id == ^successor_id))
    assert FakeUpstream.count(upstream) == 2
  end

  defp await_receipt!(attempt, remaining \\ 100)
  defp await_receipt!(_attempt, 0), do: flunk("the socket recorded no delivery receipt")

  defp await_receipt!(attempt, remaining) do
    case Repo.reload!(attempt).response_metadata do
      %{"downstream_delivery" => %{} = receipt} -> receipt
      _pending -> Process.sleep(10) && await_receipt!(attempt, remaining - 1)
    end
  end

  # The killed task's connection goes with it: PostgreSQL ends the backend and
  # rolls its transaction back. Polled on the watcher's own connection.
  defp await_backend_gone!(watcher, backend) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_backend_gone!(watcher, backend, deadline)
  end

  defp await_backend_gone!(watcher, backend, deadline) do
    %{rows: [[present?]]} = Postgrex.query!(watcher, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND state = 'idle in transaction')", [backend])

    cond do
      not present? -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("the killed task's transaction stayed open")
      true -> Process.sleep(10) && await_backend_gone!(watcher, backend, deadline)
    end
  end

  defp await_settled!(setup) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_settled!(setup, deadline)
  end

  defp await_settled!(setup, deadline) do
    requests = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))
    live? = Repo.exists?(from(t in CodexTurn, where: t.request_id in ^Enum.map(requests, & &1.id) and t.status == "in_progress"))

    cond do
      not live? and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) -> requests
      System.monotonic_time(:millisecond) >= deadline -> requests
      true -> Process.sleep(10) && await_settled!(setup, deadline)
    end
  end

  defp receive_until_terminal(conn, websocket, ref, frames) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "error"],
      do: {conn, websocket, Enum.reverse([frame | frames])},
      else: receive_until_terminal(conn, websocket, ref, [frame | frames])
  end

  defp ledger_kinds(%Request{id: request_id}),
    do: request_id |> Accounting.list_ledger_entries_for_request() |> Enum.map(& &1.entry_kind) |> Enum.sort()

  defp turn_payload(setup, session_id) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("synthetic killed settler turn"),
      "stream" => true,
      "store" => false,
      "client_metadata" => %{
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => session_id, "thread_id" => session_id, "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn"})
      }
    })
  end

  defp completed_frames(response_id) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 1, "total_tokens" => 3}}})
    ])
  end

  defp connect!(port, setup, session_id) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"session-id", session_id}, {"x-request-id", session_id}, {"user-agent", "codex_cli_rs/0.159.0"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/backend-api/codex/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end
end
