defmodule CodexPoolerWeb.Runtime.OwnerLossScenario do
  @moduledoc false

  # An owner-forwarded socket whose websocket owner goes away between two
  # requests (findings#276), shared by the one-node family
  # (`backend_codex_websocket_owner_forwarding/owner_loss_test.exs`) and the
  # peer-only one (`.../remote_owner_loss_test.exs`). The released client's
  # native frames (turn metadata naming thread and turn) or a public `/v1`
  # SDK's `response.create`, synthetic text; the socket is keyed by its
  # `x-codex-window-id`.

  import ExUnit.Assertions
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [
      await_socket_connection_state!: 2,
      completed_response_frames: 4,
      receive_frames_until_close!: 3,
      released_client_frame: 2,
      socket_connection_state!: 1,
      socket_transport_barrier!: 3
    ]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @native_route "/backend-api/codex/responses"
  @public_route "/v1/responses"
  @terminal_types ~w(response.completed response.failed response.incomplete error)
  # Detection budget for an owner exit or a socket state the test only observes.
  @detection_timeout_ms 15_000

  def native_route, do: @native_route
  def public_route, do: @public_route

  @doc "Every request is served: the first as `<prefix>_one`, each later one as `<prefix>_next`."
  def upstream!(prefix) do
    start_upstream(FakeUpstream.repeat_last([completed_response_frames("#{prefix}_one", [], 3, 2), completed_response_frames("#{prefix}_next", [], 3, 2)]))
  end

  @doc "A released-client window of its own thread (`<thread>:0`)."
  def window do
    suffix = System.unique_integer([:positive]) |> Integer.to_string(16) |> String.downcase() |> String.pad_leading(12, "0")
    thread = "019a0000-0000-7000-8000-#{suffix}"
    %{thread: thread, id: "#{thread}:0"}
  end

  @doc "Opens a socket on `route` keyed by the window, with the listener connection process behind it."
  def connect!(port, setup, route, window) do
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref, _headers} = public_websocket_connect_with_request_headers!(port, setup, Ecto.UUID.generate(), route, [{"x-codex-window-id", window.id}])
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    %{conn: conn, websocket: websocket, ref: ref, socket: socket, route: route, thread: window.thread}
  end

  @doc "One request on the client's socket: every frame of the turn (decoded, its terminal last), once the socket settled it."
  def turn!(client, setup, text) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame(setup, client, text))
    {conn, websocket, frames} = receive_turn_frames!(conn, websocket, client.ref, [])
    {settle!(%{client | conn: conn, websocket: websocket}), frames}
  end

  @doc "The socket tracks no turn and sent nothing after the last terminal: a later frame would precede the barrier's pong."
  def settle!(client) do
    _idle = await_socket_connection_state!(client.socket, &(MapSet.size(&1.tasks) == 0 and not is_pid(Map.get(&1, :public_response_task_pid))))
    {conn, websocket} = socket_transport_barrier!(client.conn, client.websocket, client.ref)
    %{client | conn: conn, websocket: websocket}
  end

  @doc """
  Makes the owner go away and returns its exit reason: killed, its upstream
  connection process killed (the owner retires `owner_crashed`), drained, or
  stopped normally.
  """
  def lose_owner!(owner, loss) when is_pid(owner) do
    ref = Process.monitor(owner)

    case loss do
      :owner_killed -> Process.exit(owner, :kill)
      :upstream_killed -> Process.exit(:sys.get_state(owner).upstream_pid, :kill)
      # The owner's last turn, relayed and settled, before the drain (findings#287).
      :owner_drained -> {:ok, :settled} = WebsocketOwnerSession.drain_owner(owner)
      :owner_stopped -> :ok = GenServer.stop(owner, :normal)
    end

    assert_receive {:DOWN, ^ref, :process, ^owner, reason}, @detection_timeout_ms
    reason
  end

  @doc "The idle socket closes on its own with `close`, with nothing before the Close, and its connection goes."
  def assert_closed!(client, {code, reason}) do
    {conn, _websocket, frames} = receive_frames_until_close!(client.conn, client.websocket, client.ref)
    Mint.HTTP.close(conn)
    assert frames == [{:close, code, reason}]
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  @doc "The socket saw its owner leave and stays open (the lost owner is marked), with nothing sent to its idle client."
  def await_open_without_owner!(client) do
    lost = await_socket_connection_state!(client.socket, &Map.get(&1, :websocket_owner_lost?, false))
    {settle!(client), lost}
  end

  @doc """
  The next request on the open socket is served by an owner on this node,
  under a lease the socket took over from the one it lost, an owner the socket
  now monitors; the request after it is served by that owner too, without a
  second takeover.
  """
  def assert_next_requests_served!(client, setup, lost, next_id) do
    {client, two} = turn!(client, setup, "after the owner left")
    assert terminal(two) == {"response.completed", next_id}
    refute Enum.any?(two, &(&1["type"] == "error"))

    session = Repo.get!(CodexSession, lost.codex_session.id)
    assert session.owner_instance_id == Atom.to_string(node())
    after_two = socket_connection_state!(client.socket)
    assert session.owner_lease_token != lost.websocket_owner_lease_token
    assert after_two.websocket_owner_lease_token == session.owner_lease_token
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session.id)
    assert after_two.websocket_owner_pid == owner
    refute Map.has_key?(after_two, :websocket_owner_lost?)

    {client, three} = turn!(client, setup, "the request after it")
    assert terminal(three) == {"response.completed", next_id}
    assert socket_connection_state!(client.socket).websocket_owner_downstream == after_two.websocket_owner_downstream
    client
  end

  @doc "The client's next request on a new socket is served by an owner on this node that took the session over."
  def assert_new_socket_served!(port, setup, route, window, session_id, next_id) do
    before = Repo.get!(CodexSession, session_id)
    retry = connect!(port, setup, route, window)
    {retry, next} = turn!(retry, setup, "on the client's new socket")
    assert terminal(next) == {"response.completed", next_id}
    session = Repo.get!(CodexSession, session_id)
    assert session.owner_instance_id == Atom.to_string(node())
    assert session.owner_lease_token != before.owner_lease_token
    close!(retry)
  end

  @doc "The owner-exit lines of `log`, from their message on."
  def owner_exit_lines(log) do
    log
    |> String.split("\n")
    |> Enum.filter(&(&1 =~ "after owner exit"))
    |> Enum.map(&Regex.replace(~r/\A.*?(websocket downstream)/, &1, "\\1"))
  end

  def terminal(frames) do
    %{"type" => type} = last = List.last(frames)
    {type, get_in(last, ["response", "id"]) || get_in(last, ["error", "code"])}
  end

  def close!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.socket)
  end

  @doc "The Pool's request rows once `count` of them settled, oldest first."
  def settled_statuses!(setup, count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    statuses = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at], select: r.status))

    cond do
      length(statuses) == count and "in_progress" not in statuses ->
        statuses

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          10 -> settled_statuses!(setup, count, deadline)
        end

      true ->
        flunk("request rows did not settle: #{inspect(statuses)}")
    end
  end

  defp frame(setup, %{route: @native_route, thread: thread}, text),
    do: released_client_frame(setup, thread).(native_text_input(text), Ecto.UUID.generate(), %{})

  defp frame(setup, %{route: @public_route}, text),
    do: CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input(text), "stream" => true})

  defp receive_turn_frames!(conn, websocket, ref, frames) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frames = frames ++ [CodexPooler.JSON.decode!(text)]

    if List.last(frames)["type"] in @terminal_types,
      do: {conn, websocket, frames},
      else: receive_turn_frames!(conn, websocket, ref, frames)
  end
end
