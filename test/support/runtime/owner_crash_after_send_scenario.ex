defmodule CodexPoolerWeb.Runtime.OwnerCrashAfterSendScenario do
  @moduledoc false

  # A websocket owner that ends without its `terminate/2` while it runs a turn
  # (findings#327), shared by the one-node family
  # (`backend_codex_websocket_owner_forwarding/owner_crash_after_send_test.exs`)
  # and the peer one (`.../remote_owner_crash_after_send_test.exs`). The kill
  # points:
  #
  #   * `:after_receipt`: the provider received the turn's payload and holds
  #     it before its first frame (FakeUpstream's frame barrier, ordinal zero);
  #     a second send of the turn would be answered at once.
  #   * `:before_write`: the owner's upstream session is still waiting for the
  #     provider's answer to its websocket handshake (FakeUpstream holds the
  #     upgrade), so nothing of the turn left; the next handshake is served.
  #   * the hold of `held_write_boundary/3`: the forwarder's payload write
  #     observer has run and the payload is not written yet.
  #
  # The order the socket and its response task meet the owner's exit in is
  # either left to the schedulers, as in production, or forced: a held socket
  # (`hold_socket!/1`) lets the response task meet it first, and
  # `kill_with_task_holding_connection!/5` also makes the task hold a database
  # connection after its settlement while the socket handles the DOWN. The
  # socket closes `1011` on its owner's crash whenever it handles the DOWN. The
  # client then resends as the released Codex client does
  # (`resend_like_released_client!/5`, findings#328).
  #
  # An arm in which a process may end, or be held, while it uses the database
  # that another process then needs gives every process a connection of its
  # own, as production does (`per_process_connections!/0`; the peer topology
  # always does).

  import ExUnit.Assertions
  import ExUnit.Callbacks, only: [on_exit: 1]
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [completed_response_frames: 4, receive_frames_until_close!: 3, receive_native_terminal!: 3, released_client_frame: 2, socket_connection_state!: 1, with_info_log: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.BridgeOwnerLease
  alias CodexPooler.Gateway.Transports.Websocket.{UpstreamWebsocketSession, WebsocketOwnerSession}
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Platform.ExecutionTerminalProof
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias CodexPoolerWeb.Runtime.OwnerLossScenario, as: Scenario
  alias CodexPoolerWeb.Runtime.SettlementTransactionHold
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  @native_route "/backend-api/codex/responses"
  @detection_timeout_ms 15_000
  # The released Codex client resends a turn whose socket closed after 200 ms,
  # then backs off on each `409 duplicate_turn` (the duplicate-turn fence manual).
  @released_client_backoff_ms [200, 400, 800, 1600, 3200]
  # The fence's two lines (findings#329 row J): the submission's decision on
  # the owner's node, and a session's write refused after a claim.
  @fence_line "websocket owner exit fence "
  @write_refused_line "upstream websocket payload write refused "

  defmodule TaskRelease do
    @moduledoc false

    # The `:logger` handler of `kill_with_task_holding_connection!/5`, run in
    # the process that logs: once the held task's socket has handled its
    # owner's crash, it releases the task's connection and tells the test.

    alias CodexPoolerWeb.Runtime.SettlementTransactionHold
    alias CodexPoolerWeb.WebsocketConnectionLogger

    @spec log(:logger.log_event(), :logger.handler_config()) :: :ok
    def log(%{msg: {:string, chardata}}, %{config: %{socket: socket, task: task, hold: hold, test: test}}) do
      if self() == socket and String.starts_with?(IO.chardata_to_string(chardata), WebsocketConnectionLogger.downstream_closed_after_owner_exit_message()) do
        :ok = SettlementTransactionHold.release(hold, task)
        send(test, {:task_released_on_owner_exit_close, hold})
      end

      :ok
    end

    def log(_event, _config), do: :ok
  end

  @doc "The provider for `kill_point`, notifying the calling test process."
  def upstream!(:after_receipt, prefix, release_ref) do
    start_upstream(
      # provenance: synthetic_adversarial (a turn the provider holds after its payload and before its first frame; a second send of it completes at once)
      FakeUpstream.repeat_last([
        FakeUpstream.barrier_websocket_frames(turn_frames(prefix), notify: self(), release_ref: release_ref),
        completed_response_frames("#{prefix}_written_again", [], 3, 2)
      ])
    )
  end

  def upstream!(:before_write, _prefix, release_ref),
    do: start_upstream(FakeUpstream.websocket_upgrade_timeout(notify: self(), release_ref: release_ref))

  @doc "Waits until the turn reached `kill_point`; `:before_write` then serves the next handshake."
  def await_kill_point!(:after_receipt, _upstream, _prefix, release_ref) do
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @detection_timeout_ms
    :ok
  end

  def await_kill_point!(:before_write, upstream, prefix, release_ref) do
    assert_receive {:fake_upstream_timeout_barrier, :websocket_upgrade, handler, ^release_ref}, @detection_timeout_ms
    # The held handshake stays held; the replacement owner's connection meets
    # this mode. Released at the end, before the fake stops, which otherwise
    # waits out its listener's shutdown timeout (15 s) for the held handler.
    on_exit(fn -> send(handler, {:fake_upstream_release_timeout, release_ref}) end)

    :ok =
      FakeUpstream.set_mode(
        upstream,
        # provenance: synthetic_adversarial (the turn's first and second provider sends after its owner's death, each answered at once)
        FakeUpstream.repeat_last([completed_response_frames("#{prefix}_first_send", [], 3, 2), completed_response_frames("#{prefix}_second_send", [], 3, 2)])
      )
  end

  @doc """
  Gives every process of the test a database connection of its own, as
  production does: the sandbox's `:auto` mode, so writes commit and the test
  registers `register_unboxed_pool_cleanup!/1` for its Pool right after
  `gateway_setup/2`. An arm needs it when a process may end, or be held,
  while it uses the database another process then needs. A public `/v1`
  socket cancels its turn's response task when it handles its owner's crash
  (`maybe_abort_public_owner_turn/2`), and the task may still be settling the
  turn or publishing what it settled: on the one connection the sandbox shares
  between processes, that cancellation took every other process's connection
  down (Drone 1865). The socket's own crash cleanup then exited in its
  checkout and the socket closed `1011 websocket control unavailable`, the
  execution proof publisher crashed on `DBConnection.OwnershipError`, and
  nothing after it could read the database. A native socket waits for its
  task instead. The peer topology always runs this way
  (`enter_peer_owner_topology!/0`).
  """
  @spec per_process_connections!() :: :ok
  def per_process_connections! do
    assert :ok = Sandbox.mode(Repo, :auto)
    on_exit(fn -> assert :ok = Sandbox.mode(Repo, :manual) end)
    :ok
  end

  @doc """
  Runs the execution proof publisher as production does, so the duplicate-turn
  fence admits the client's resend of a turn settled `owner_crashed` once its
  executor's end is proven, and returns it. When the test's writes commit
  (`per_process_connections!/0`, the peer topology), the proofs published
  during the test are removed at its end (`:committed`).
  """
  @spec start_proof_publisher!(:sandboxed | :committed) :: pid()
  def start_proof_publisher!(sandbox) when sandbox in [:sandboxed, :committed] do
    if sandbox == :committed do
      before = Repo.all(from(proof in ExecutionTerminalProof, select: proof.execution_id))
      UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from(proof in ExecutionTerminalProof, where: proof.execution_id not in ^before)) end)
    end

    CodexPooler.ExecutionProofSupport.start_publisher!()
  end

  @doc "The turn's internal correlators, as the fence lines carry them."
  def turn_correlators(request_id, session_id) do
    attempt_id = Repo.one!(from(a in Attempt, where: a.request_id == ^request_id, select: a.id))
    %{request_id: request_id, attempt_id: attempt_id, session_id: session_id}
  end

  @doc "The fields of every `websocket owner exit fence` line in `log`."
  def fence_lines(log), do: lines_after(log, @fence_line)

  @doc "The fields of every `upstream websocket payload write refused` line in `log`."
  def write_refusal_lines(log), do: lines_after(log, @write_refused_line)

  @doc "The line prefixes the fence logs, for `PeerLogRelay.attach!/3`."
  def fence_line_prefixes, do: [@fence_line, @write_refused_line]

  @doc """
  Exactly one fence line, deciding `decision` for `reason` about the turn whose
  owner was killed: its fixed fields and the turn's internal correlators, every
  one of them a UUID, and nothing else (no provider identifier, no content).
  """
  def assert_fence_decision!(log, decision, reason, turn, extra \\ %{}) do
    expected =
      Map.merge(
        %{"decision" => decision, "reason" => reason, "owner_exit" => "killed", "codex_session_id" => turn.session_id, "request_id" => turn.request_id, "attempt_id" => turn.attempt_id},
        extra
      )

    assert fence_lines(log) == [expected]
    assert Enum.all?([turn.session_id, turn.request_id, turn.attempt_id], &match?({:ok, _uuid}, Ecto.UUID.cast(&1)))
    :ok
  end

  @doc "Exactly one refused write, of the turn's attempt."
  def assert_write_refused!(log, turn) do
    assert write_refusal_lines(log) == [%{"reason_code" => "turn_claimed", "request_id" => turn.request_id, "attempt_id" => turn.attempt_id}]
    :ok
  end

  @doc "Waits for `count` lines a peer relayed (`PeerLogRelay`) and returns them as one log."
  def await_peer_lines!(peer_node, count) do
    Enum.map_join(1..count//1, "\n", fn _line ->
      assert_receive {:peer_log, ^peer_node, line}, @detection_timeout_ms
      line
    end)
  end

  defp lines_after(log, prefix) do
    log
    |> String.split("\n")
    |> Enum.flat_map(fn line ->
      case String.split(line, prefix, parts: 2) do
        [_before, fields] -> [fields |> String.split(" ", trim: true) |> Map.new(&field/1)]
        [_other] -> []
      end
    end)
  end

  defp field(pair) do
    [key, value] = String.split(pair, "=", parts: 2)
    {key, value}
  end

  @doc "Forces the Pool's serving mode for the setup's model."
  def serve!(setup, mode) when mode in ["full", "lite"] do
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode})
    :ok
  end

  @doc "Sends one turn on the client's socket without waiting for any of it."
  def send_turn!(client, setup, text), do: send_frame!(client, frame(setup, client, text))

  @doc "Sends `frame` on the client's socket without waiting for any of it."
  def send_frame!(client, frame) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    %{client | conn: conn, websocket: websocket}
  end

  @doc "The Pool's request still in progress: the held turn's."
  def turn_request_id!(setup),
    do: Repo.one!(from(r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress", select: r.id))

  @doc "Suspends the socket's listener connection process until `close_client!/1` resumes it."
  def hold_socket!(socket) do
    :ok = :sys.suspend(socket)

    on_exit(fn ->
      try do
        :sys.resume(socket)
      catch
        :exit, _socket_gone -> :ok
      end
    end)

    :ok
  end

  @doc "Kills `owner` (local or on a peer), as `stop_stale_owner/2` kills an owner blocked in a callback."
  def kill_owner!(owner) do
    monitor = Process.monitor(owner)
    _ordered = :sys.get_state(owner)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^owner, :killed}, @detection_timeout_ms
    :ok
  end

  @doc """
  Waits until the provider receives more than `generations` websocket
  generations (`:written_again`) or the turn's request settles
  (`:settled`), whichever comes first.
  """
  def await_outcome!(upstream, request_id, generations) do
    await_outcome!(upstream, request_id, generations, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_outcome!(upstream, request_id, generations, deadline) do
    cond do
      generations(upstream) > generations ->
        :written_again

      Repo.get!(Request, request_id).status in ["failed", "succeeded"] ->
        :settled

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the turn neither settled nor reached the provider again")

      true ->
        receive do
        after
          10 -> await_outcome!(upstream, request_id, generations, deadline)
        end
    end
  end

  @doc "The provider's websocket generations so far."
  def generations(upstream), do: FakeUpstream.physical_counts(upstream).websocket_generation

  @doc "The turn settled on the owner's crash, with one attempt and no takeover of the session."
  def assert_settled_on_crash!(request_id, session_id) do
    request = Repo.get!(Request, request_id)
    assert {request.status, request.response_status_code, request.last_error_code} == {"failed", 502, "owner_crashed"}
    assert [%Attempt{status: "failed", network_error_code: "owner_crashed"}] = attempts(request_id)
    refute taken_over?(session_id)
    :ok
  end

  @doc "The turn was served once by a replacement owner that took the session over, with one attempt (the HTTP bridge)."
  def assert_recovered!(request_id, session_id) do
    request = Repo.get!(Request, request_id)
    assert {request.status, request.response_status_code, request.last_error_code} == {"succeeded", 200, nil}
    assert [%Attempt{status: "succeeded"}] = attempts(request_id)
    assert taken_over?(session_id)
    :ok
  end

  @doc "Resumes the held socket and reads its client's frames until the Close; the socket is gone after."
  def close_client!(client) do
    :ok = :sys.resume(client.socket)
    await_close!(client)
  end

  @doc "Reads the client's frames until the Close of a socket nobody holds; the socket is gone after."
  def await_close!(client) do
    {conn, _websocket, frames} = receive_frames_until_close!(client.conn, client.websocket, client.ref)
    Mint.HTTP.close(conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket, @detection_timeout_ms)
    frames
  end

  @doc """
  The interleaving of Drone 1865, forced. Kills `owner_pid` (local or on a
  peer) with `client`'s socket held and lets the turn's response task settle
  the turn, then holds the task right after its settlement's commit with a
  database connection checked out, as it holds one to publish what it settled
  (`SettlementTransactionHold.after_commit!/2`), and only then resumes the
  socket, which handles its owner's crash while the task holds that
  connection. A public `/v1` socket cancels the task inside it
  (`maybe_abort_public_owner_turn/2`); a native socket leaves its task
  running and closes, and its `websocket downstream closed after owner exit`
  line releases the task, so the socket runs at the info level whatever its
  caller's. Checks the close, how the task ended and that its settlement
  stands; returns the frames the client read until the Close.
  """
  def kill_with_task_holding_connection!(client, owner_pid, route, request_id, session_id) do
    [task] = client.socket |> socket_connection_state!() |> Map.fetch!(:tasks) |> MapSet.to_list()
    hold = SettlementTransactionHold.after_commit!(request_id, connection: true)
    :ok = release_task_on_owner_exit_close!(client.socket, task, hold)
    :ok = hold_socket!(client.socket)
    :ok = kill_owner!(owner_pid)
    assert {^task, _facts} = SettlementTransactionHold.await_held!(hold)

    # `:sys.resume/1` in `close_client!/1` sends this monitor's request to the
    # held task before anything can end it (`CodexPooler.TestProcess`).
    task_monitor = Process.monitor(task)
    {frames, _log} = with_info_log(fn -> close_client!(client) end)

    assert List.last(frames) == {:close, 1011, "websocket owner crashed"}
    assert_receive {:task_released_on_owner_exit_close, ^hold}, @detection_timeout_ms
    assert_receive {:DOWN, ^task_monitor, :process, ^task, task_exit}, @detection_timeout_ms
    assert task_exit == if(route == @native_route, do: :normal, else: {:shutdown, :owner_monitor_down})
    :ok = assert_settled_on_crash!(request_id, session_id)
    frames
  end

  defp release_task_on_owner_exit_close!(socket, task, hold) do
    handler_id = :"owner_crash_task_release_#{System.unique_integer([:positive])}"
    on_exit(fn -> :logger.remove_handler(handler_id) end)
    :logger.add_handler(handler_id, TaskRelease, %{level: :info, config: %{socket: socket, task: task, hold: hold, test: self()}})
  end

  @doc """
  Resends `frame` on new sockets as the released Codex client does after its
  socket closed: after 200 ms, then after each `409 duplicate_turn` with the
  client's backoff. Returns the served terminal and every answer in order.
  """
  def resend_like_released_client!(port, setup, route, window, frame),
    do: resend_like_released_client!(port, setup, route, window, frame, @released_client_backoff_ms, [])

  defp resend_like_released_client!(_port, _setup, _route, _window, _frame, [], answers),
    do: flunk("the released client's resends were all refused: #{inspect(Enum.reverse(answers))}")

  defp resend_like_released_client!(port, setup, route, window, frame, [delay | backoff], answers) do
    receive do
    after
      delay -> :ok
    end

    retry = Scenario.connect!(port, setup, route, window)
    retry = send_frame!(retry, frame)
    {conn, websocket, terminal} = receive_native_terminal!(retry.conn, retry.websocket, retry.ref)
    Scenario.close!(%{retry | conn: conn, websocket: websocket})

    case terminal do
      %{"type" => "error", "status" => 409} -> resend_like_released_client!(port, setup, route, window, frame, backoff, [terminal | answers])
      served -> {served, Enum.reverse([served | answers])}
    end
  end

  @doc """
  The turn reached the provider once, by the client's resend: the predecessor
  failed `owner_crashed` at no charge (`499` when the socket's crash cleanup
  settled it first, `502` when its response task did), the resend succeeded
  and is the one charge, linked to the predecessor on the native route (a
  translated `/v1` request is not claimed by the duplicate-turn fence, so its
  resend is a request of its own).
  """
  def assert_resend_served_once!(setup, upstream, route, predecessor_id) do
    assert generations(upstream) == 1
    assert [predecessor, successor] = await_settled!(setup, 2)
    assert predecessor.id == predecessor_id
    assert {predecessor.status, predecessor.last_error_code} == {"failed", "owner_crashed"}
    assert predecessor.response_status_code in [499, 502]
    assert [%Attempt{status: "failed", network_error_code: "owner_crashed"}] = attempts(predecessor_id)
    assert {successor.status, successor.response_status_code} == {"succeeded", 200}
    assert [%Attempt{status: "succeeded"}] = attempts(successor.id)
    assert recorded_costs(predecessor_id) == [Decimal.new(0)]
    assert [successor_cost] = recorded_costs(successor.id)
    assert Decimal.gt?(successor_cost, 0)
    links = Repo.all(from(link in RequestClientRetryLink, where: link.predecessor_request_id == ^predecessor_id, select: link.successor_request_id))
    assert links == if(route == @native_route, do: [successor.id], else: [])
    :ok
  end

  @doc "The Pool's requests, oldest first, once `count` of them exist and none is still running."
  def await_settled!(setup, count), do: await_settled!(setup, count, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_settled!(setup, count, deadline) do
    requests = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

    cond do
      length(requests) == count and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the Pool's requests never settled: #{inspect(Enum.map(requests, & &1.status))}")

      true ->
        receive do
        after
          20 -> await_settled!(setup, count, deadline)
        end
    end
  end

  defp recorded_costs(request_id) do
    Repo.all(from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.amount_status == "recorded", select: entry.settled_cost_micros))
    |> Enum.map(&Decimal.normalize/1)
  end

  @doc """
  Starts the session the socket on `window_id` resolves, owned on
  `owner_node` (this node or a peer's) by an owner whose upstream boundary is
  `held_write_boundary/4` holding at `at`. Call it before the socket connects.
  """
  def start_held_owner!(%{authorization: authorization}, window_id, owner_node, hold_ref, link, at \\ :after_mark) do
    {:ok, auth} = Access.authenticate_authorization_header(authorization)
    {:ok, session} = Gateway.start_codex_session(auth, %{session_header: window_id, session_header_source: "x-codex-window-id", owner_instance_id: Atom.to_string(owner_node)})
    boundary = :erpc.call(owner_node, __MODULE__, :held_write_boundary, [self(), hold_ref, link, at], @detection_timeout_ms)

    persistence =
      if owner_node == node(),
        do: [],
        else: [persistence: :erpc.call(owner_node, WebsocketOwnerNodeHarness, :real_persistence_boundary, [], @detection_timeout_ms)]

    start_opts = [codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: session.owner_instance_id, owner_renewal_ms: 60_000, upstream: boundary] ++ persistence
    {:ok, owner} = :erpc.call(owner_node, WebsocketOwnerSession, :start_owner, [start_opts], @detection_timeout_ms)
    on_exit(fn -> stop_owner!(owner) end)
    %{session: session, owner_pid: owner}
  end

  @doc """
  The owner's real upstream boundary, holding a turn at its payload write.
  Built on the owner's node, where its closures run. `:after_mark` holds after
  the forwarder's payload write observer ran (the turn counts as started),
  `:before_mark` before it (the turn's payload never started to leave). With
  `:unlinked` the upstream session first unlinks from its owner: it stands for
  a session that handles its owner's exit signal only after its write, so once
  released it writes with its owner gone. The hold reports
  `{:payload_write_held, session_pid, hold_ref, answer}` (`answer` is the
  forwarder's observer answer, `:unmarked` before the mark) and waits for
  `release_held_write/2`.
  """
  def held_write_boundary(test_pid, hold_ref, link, at \\ :after_mark)
      when is_pid(test_pid) and is_reference(hold_ref) and link in [:linked, :unlinked] and at in [:after_mark, :before_mark] do
    real = WebsocketOwnerSession.default_upstream_boundary()
    %{real | send: fn upstream_pid, payload, writer -> real.send.(upstream_pid, hold_write(payload, test_pid, hold_ref, link, at), writer) end}
  end

  @doc "Lets a held upstream session write its payload."
  def release_held_write(session_pid, hold_ref) do
    send(session_pid, {:payload_write_release, hold_ref})
    :ok
  end

  @doc "Stops an owner, a peer's too, that may already be gone or retiring."
  def stop_owner!(owner) do
    monitor = Process.monitor(owner)

    try do
      :erpc.call(node(owner), GenServer, :stop, [owner, :normal, 5_000], @detection_timeout_ms)
    catch
      kind, _gone_or_retiring when kind in [:exit, :error] -> :ok
    end

    assert_receive {:DOWN, ^monitor, :process, ^owner, _reason}, @detection_timeout_ms
    :ok
  end

  @doc "Stops the session's owner on `owner_node` at the test's end, whichever owner holds it then."
  def stop_session_owner_on_exit(owner_node, session_id) do
    on_exit(fn ->
      case :erpc.call(owner_node, WebsocketOwnerSession, :lookup, [session_id], @detection_timeout_ms) do
        {:ok, owner} -> stop_owner!(owner)
        {:error, :owner_unavailable} -> :ok
      end
    end)
  end

  defp hold_write(%UpstreamWebsocketSession.Request{payload_write_observer: observer} = request, test_pid, hold_ref, link, at) when is_function(observer, 0) do
    held = fn ->
      marked = if at == :after_mark, do: observer.(), else: :unmarked
      if link == :unlinked, do: unlink_owner()
      send(test_pid, {:payload_write_held, self(), hold_ref, marked})

      receive do
        {:payload_write_release, ^hold_ref} -> :ok
      after
        @detection_timeout_ms -> :ok
      end

      if at == :after_mark, do: marked, else: observer.()
    end

    %{request | payload_write_observer: held}
  end

  defp hold_write(payload, _test_pid, _hold_ref, _link, _at), do: payload

  # The owner starts its upstream session with `start_link/1` from `init/1`.
  defp unlink_owner do
    [owner | _ancestors] = Process.get(:"$ancestors")
    true = Process.unlink(owner)
    :ok
  end

  defp attempts(request_id), do: Repo.all(from(a in Attempt, where: a.request_id == ^request_id))

  defp taken_over?(session_id) do
    Repo.exists?(from(l in BridgeOwnerLease, where: l.codex_session_id == ^session_id and fragment("?->>'source'", l.metadata) == "owner_unavailable_takeover"))
  end

  @doc "The client's frame for one turn: the released client's native frame, or a public `/v1` SDK request."
  def frame(setup, %{route: @native_route, thread: thread}, text),
    do: released_client_frame(setup, thread).(native_text_input(text), Ecto.UUID.generate(), %{})

  def frame(setup, %{route: "/v1/responses"}, text),
    do: CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input(text), "stream" => true})

  defp turn_frames(prefix) do
    created = %{"type" => "response.created", "response" => %{"id" => "#{prefix}_held", "status" => "in_progress"}}
    delta = %{"type" => "response.output_text.delta", "delta" => "synthetic output"}
    completed = %{"type" => "response.completed", "response" => %{"id" => "#{prefix}_held", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => 10, "total_tokens" => 15}}}
    Enum.map([created, delta, completed], &CodexPooler.JSON.encode!/1)
  end
end
