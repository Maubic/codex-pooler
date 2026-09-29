defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketSecondRemoteCompactionTest do
  # Two remote compactions inside one turn (findings#270 rows 270-351,
  # 270-354 and 270-357). A Codex client whose provider is named `OpenAI`
  # compacts remotely: when a tool round takes the turn past its
  # auto-compaction limit, the released client (0.159.0,
  # `compact_remote_v2.rs`) sends a `compaction_trigger` on the turn's
  # websocket, anchored on the response that asked for the tool (as full
  # history on a connection that cannot resolve the anchor, and over HTTPS once
  # the session fell back), and resumes the turn on the compacted history in
  # its next context window (`advance_auto_compact_window`: a new `window_id`
  # and `window_number` in the turn metadata). When the resume's tool round
  # takes it past the limit again, it compacts a second time, same turn, one
  # window later.
  #
  # Both compactions derived one claim: the claim was taken from the payload
  # the compaction bridge rewrote, which keeps no client metadata and so no
  # window, and without `input` and `previous_response_id` the two compactions
  # of one turn are the same request. The second anchored compaction met the
  # first one's claim and was refused `409 duplicate_turn` (owner forwarding
  # off and on). Sent as full history without forwarding, or over HTTPS, it
  # was chained to the first compaction as a resend of a compaction that had
  # been delivered; with forwarding the owner's compaction preflight took the
  # newest request of the turn, the resume, as the predecessor and refused
  # every full-history send `authorization_changed`, and the client fell back
  # to HTTPS for the rest of its session (P1's probe of the released client,
  # `compact-remote-twice`, 3 of 3 in each topology). The claim now binds the
  # compaction's own window, read from its turn metadata before the bridge,
  # and the owner's predecessor is the chain of the request holding the
  # resend's own compaction claim.
  #
  # Frames keep the released client's key sets (as in
  # `PreTurnCompactionCutScenario`); identifiers, prompt text and reply frames
  # are synthetic. One node, owner forwarding off (each socket's own upstream
  # session) and on (the session's owner on this node), the Pool's serving
  # mode Full and Lite. FakeUpstream.
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.{NativeCompactionAdmission, WebsocketOwnerSession}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @thread_id "019a0000-0000-7000-8000-00000000f351"
  @turn_id "019a0000-0000-7000-8000-00000000f352"
  @installation_id "00000000-0000-4000-8000-00000000f353"
  @opener_response "resp_second_compaction_opener01"
  @first_compaction_response "resp_second_compaction_first001"
  @resume_response "resp_second_compaction_resume01"
  @second_compaction_response "resp_second_compaction_second01"
  @resent_compaction_response "resp_second_compaction_resent01"
  @final_response "resp_second_compaction_final001"
  @lite_marker "ws_request_header_x_openai_internal_codex_responses_lite"
  @compact_endpoint "/backend-api/codex/responses/compact"
  @turn_endpoint "/backend-api/codex/responses"
  # Detection budget for a frame, a barrier or a row the test only observes.
  @detection_timeout_ms 15_000
  # A compaction resend's retry window on every path, and how far inside or
  # outside it a resend arrives: well above the time the resend takes to reach
  # the window check after the backdate, so inside is 325 s, past the
  # released client's 300 s stream idle timeout.
  @compaction_window_seconds 330
  @window_margin_seconds 5

  # The released client's own flow while its socket lives: both compactions
  # anchored on the turn's connection. Each is served once under a claim of
  # its own, and nothing is chained.
  for mode <- ["full", "lite"], forwarding <- [:off, :on] do
    @tag mode: mode, forwarding: forwarding
    test "#{mode} forwarding #{forwarding}: the turn's second anchored remote compaction is served under a claim of its own", ctx do
      scenario = start_scenario!(ctx, [opener(), first_compaction(), resume(), anchored_second_compaction(), final_resume()])

      first = serve_until_second_compaction!(scenario)
      await_armed!(scenario, 3)
      first = first |> send_frame!(anchored_second_compaction_frame(scenario)) |> assert_compaction_served!()
      first = first |> send_frame!(resume_frame(scenario, 2)) |> assert_completed!()
      close!(first)

      assert rows(scenario) == first_three_rows() ++ [{@compact_endpoint, "websocket", "succeeded", "codex-request", nil}, {@turn_endpoint, "websocket", "succeeded", "codex-resume", nil}]
      assert_distinct_claims_and_one_charge_each!(scenario, 2)
    end
  end

  # The turn's socket closed during the resume's tool round (an idle close
  # while a long tool ran): the second compaction's first send is full
  # history on a new connection, in the window the resume moved to. It is a
  # new compaction, not a resend of the first one: without forwarding it was
  # chained to the first compaction, and with forwarding it was refused
  # `authorization_changed` against the resume (and, with the preflight
  # taking the newest compaction of the turn instead, it was chained to the
  # first compaction there too).
  for mode <- ["full", "lite"], forwarding <- [:off, :on] do
    @tag mode: mode, forwarding: forwarding
    test "#{mode} forwarding #{forwarding}: the turn's second remote compaction sent first as full history on a new socket is served as a new compaction", ctx do
      scenario = start_scenario!(ctx, [opener(), first_compaction(), resume(), full_history_second_compaction(@second_compaction_response), final_resume()])

      scenario |> serve_until_second_compaction!() |> close!()
      second = connect!(scenario, 1)
      second = second |> send_frame!(full_history_second_compaction_frame(scenario)) |> assert_compaction_served!()
      second = second |> send_frame!(resume_frame(scenario, 2)) |> assert_completed!()
      close!(second)

      assert rows(scenario) == first_three_rows() ++ [{@compact_endpoint, "websocket", "succeeded", "codex-request", nil}, {@turn_endpoint, "websocket", "succeeded", "codex-resume", nil}]
      assert_distinct_claims_and_one_charge_each!(scenario, 2)
    end
  end

  # The session fell back to HTTPS after the turn's first compaction (the
  # released client does, for the rest of its session, once a websocket
  # request exhausted its retries): the second compaction and its resume go
  # over HTTPS. The websocket claim its HTTPS form derives binds its own
  # window, so it meets no compaction of the turn and is served under its own
  # claim. It used to find the first compaction and be chained to it, as the
  # resend of a compaction the client had completed.
  for forwarding <- [:off, :on] do
    @tag mode: "full", forwarding: forwarding
    test "full forwarding #{forwarding}: the turn's second remote compaction over HTTPS is served as a new compaction", ctx do
      scenario = start_scenario!(ctx, [opener(), first_compaction(), resume(), https_second_compaction(@second_compaction_response), https_final_resume()])

      scenario |> serve_until_second_compaction!() |> close!()
      assert_https_compaction_served!(post_native!(scenario, https_body(full_history_second_compaction_frame(scenario)), 1))
      assert_https_completed!(post_native!(scenario, https_body(resume_frame(scenario, 2)), 2))

      assert rows(scenario) == first_three_rows() ++ [{@compact_endpoint, "http_compact_json", "succeeded", "codex-request", nil}, {@turn_endpoint, "http_sse", "succeeded", "codex-resume", nil}]
      assert_distinct_claims_and_one_charge_each!(scenario, 2)
    end
  end

  # The second compaction's anchored send is admitted and its connection is
  # cut before the provider produced anything; the client resends the same
  # compaction, as full history on a new connection or over HTTPS. The resend
  # meets the second compaction, not the first, while it is inside its retry
  # window, and is served as that compaction's successor, one charge per
  # request. The window is 330 s on every path
  # (`ClientRetry.compaction_retry_window_seconds/0`): the released client
  # resends a reply it lost silently only once its 300 s stream idle timeout
  # fired. Without forwarding and over HTTPS it used to be the ordinary 30 s,
  # and a resend after the idle timeout was refused twice, after which the
  # client left its websocket for HTTPS for the rest of its session
  # (findings#270 row 270-373). Outside the window no chain is formed: the
  # websocket resend is refused `409 duplicate_turn` (`retry_expired`), the
  # released client's second websocket retry the same, and its HTTPS fallback
  # is served as a new compaction, so the compaction is served once more,
  # not lost. The cut compaction's completion is moved back, so the resend
  # lands a margin inside or outside the window whatever the machine's speed.
  for {forwarding, path} <- [{:off, :websocket}, {:on, :websocket}, {:off, :https}, {:on, :https}], side <- [:inside, :outside] do
    @tag mode: "full", forwarding: forwarding, path: path, side: side
    test "full forwarding #{forwarding}: the #{path} resend of the turn's cut second remote compaction #{side} its window", ctx do
      release_ref = make_ref()
      scenario = start_scenario!(ctx, [opener(), first_compaction(), resume(), held_second_compaction(release_ref)] ++ resend_sequence(ctx.path, ctx.side))

      first = serve_until_second_compaction!(scenario)
      await_armed!(scenario, 3)
      first = send_frame!(first, anchored_second_compaction_frame(scenario))
      await_barrier!(0, release_ref)
      close!(first)
      await!(fn -> Enum.any?(requests(scenario), &(&1.endpoint == @compact_endpoint and &1.status == "failed")) end, "the cut compaction never settled")
      backdate_cut_compaction!(scenario, @compaction_window_seconds + if(ctx.side == :inside, do: -@window_margin_seconds, else: @window_margin_seconds))

      assert resend!(scenario, ctx.path, ctx.side) == expected_client_outcomes(ctx.path, ctx.side)
      release_held_compaction!(scenario.upstream, release_ref)

      assert rows(scenario) == first_three_rows() ++ [{@compact_endpoint, "websocket", "failed", "codex-request", nil} | expected_resend_rows(ctx.forwarding, ctx.path, ctx.side)]
      assert [1, 0, 1] == scenario |> requests() |> Enum.filter(&(&1.endpoint == @compact_endpoint)) |> Enum.map(&charges/1)
      assert :ok = FakeUpstream.verify!(scenario.upstream)
    end
  end

  # A request of the turn after its compaction proves the client read that
  # compaction: the client goes on only once it completed the compaction, and
  # resends only a compaction it did not complete. A same-window duplicate of
  # the turn's first compaction after the resume is therefore refused
  # `409 duplicate_turn` before anything is dispatched, in both topologies.
  # Without forwarding it used to be chained to the first compaction and
  # billed again; the owner's compaction policy refuses it.
  for forwarding <- [:off, :on] do
    @tag mode: "full", forwarding: forwarding
    test "full forwarding #{forwarding}: a duplicate of the turn's first remote compaction after its resume is refused", ctx do
      scenario = start_scenario!(ctx, [opener(), first_compaction(), resume()])

      scenario |> serve_until_second_compaction!() |> close!()
      duplicate = connect!(scenario, 0)
      {duplicate, frames} = duplicate |> send_frame!(full_history_first_compaction_frame(scenario)) |> receive_until_terminal([])
      close!(duplicate)

      assert [%{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}}] = frames
      assert rows(scenario) == first_three_rows()
      assert :ok = FakeUpstream.verify!(scenario.upstream)
    end
  end

  defp resend_sequence(:websocket, :inside), do: [full_history_second_compaction(@resent_compaction_response), final_resume()]
  defp resend_sequence(_path, _side), do: [https_second_compaction(@resent_compaction_response), https_final_resume()]

  # What the client meets: the websocket resend served, or refused on both of
  # the released client's websocket retries and then served over HTTPS.
  defp resend!(scenario, :websocket, :inside) do
    second = connect!(scenario, 1)
    second = second |> send_frame!(full_history_second_compaction_frame(scenario)) |> assert_compaction_served!()
    second |> send_frame!(resume_frame(scenario, 2)) |> assert_completed!() |> close!()
    [:websocket_served]
  end

  defp resend!(scenario, :websocket, :outside) do
    refusals =
      for _retry <- 1..2 do
        retry = connect!(scenario, 1)
        {retry, frames} = retry |> send_frame!(full_history_second_compaction_frame(scenario)) |> receive_until_terminal([])
        close!(retry)
        assert [%{"type" => "error", "status" => status, "error" => %{"code" => code}}] = frames
        {:websocket_refused, status, code}
      end

    refusals ++ resend!(scenario, :https, :outside)
  end

  defp resend!(scenario, :https, _side) do
    assert_https_compaction_served!(post_native!(scenario, https_body(full_history_second_compaction_frame(scenario)), 1))
    assert_https_completed!(post_native!(scenario, https_body(resume_frame(scenario, 2)), 2))
    [:https_served]
  end

  defp expected_client_outcomes(:websocket, :inside), do: [:websocket_served]
  defp expected_client_outcomes(:websocket, :outside), do: [{:websocket_refused, 409, "duplicate_turn"}, {:websocket_refused, 409, "duplicate_turn"}, :https_served]
  defp expected_client_outcomes(:https, _side), do: [:https_served]

  # After the cut second compaction (row 3): its resend and the resume.
  defp expected_resend_rows(forwarding, :websocket, :inside),
    do: [{@compact_endpoint, "websocket", "succeeded", successor_claim_class(forwarding), 3}, {@turn_endpoint, "websocket", "succeeded", "codex-resume", nil}]

  defp expected_resend_rows(_forwarding, _path, :inside),
    do: [{@compact_endpoint, "http_compact_json", "succeeded", "codex-request-retry", 3}, {@turn_endpoint, "http_sse", "succeeded", "codex-resume", nil}]

  defp expected_resend_rows(_forwarding, _path, :outside),
    do: [{@compact_endpoint, "http_compact_json", "succeeded", "codex-request", nil}, {@turn_endpoint, "http_sse", "succeeded", "codex-resume", nil}]

  defp successor_claim_class(:off), do: "codex-request-retry"
  defp successor_claim_class(:on), do: "client-retry-v1"

  defp first_three_rows do
    [
      {@turn_endpoint, "websocket", "succeeded", "codex-turn", nil},
      {@compact_endpoint, "websocket", "succeeded", "codex-request", nil},
      {@turn_endpoint, "websocket", "succeeded", "codex-resume", nil}
    ]
  end

  # The turn up to its second compaction on its first socket: the opener
  # asks for a tool, the first compaction carries its output, the resume
  # (one window later) asks for a second tool.
  defp serve_until_second_compaction!(scenario) do
    first = connect!(scenario, 0)
    first = first |> send_frame!(opener_frame(scenario)) |> assert_completed!()
    await_armed!(scenario, 1)
    first = first |> send_frame!(first_compaction_frame(scenario)) |> assert_compaction_served!()
    first = first |> send_frame!(resume_frame(scenario, 1)) |> assert_completed!()
    await_settled!(scenario, 3)
    first
  end

  defp start_scenario!(ctx, sequence) do
    put_owner_forwarding!(ctx.forwarding == :on)
    upstream = start_upstream(FakeUpstream.strict_sequence(Enum.map(sequence, & &1.(ctx.mode))))
    setup = gateway_setup(upstream, compact?: true)
    if ctx.mode == "lite", do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    %{mode: ctx.mode, forwarding: ctx.forwarding, upstream: upstream, setup: setup, port: start_public_endpoint!()}
  end

  # The cut compaction's completion, `seconds` before the database's now: its
  # retry window starts there (`ClientRetry.retry_window_start/3`; the cut
  # wrote nothing downstream, so no failed write moves it).
  defp backdate_cut_compaction!(scenario, seconds) do
    [%Request{id: request_id}] = scenario |> requests() |> Enum.filter(&(&1.endpoint == @compact_endpoint and &1.status == "failed"))
    %{rows: [[now]]} = Repo.query!("SELECT clock_timestamp()")
    completed_at = DateTime.add(now, -seconds, :second)
    {1, _} = Repo.update_all(from(request in Request, where: request.id == ^request_id), set: [completed_at: completed_at])
    {1, _} = Repo.update_all(from(turn in CodexTurn, where: turn.request_id == ^request_id), set: [completed_at: completed_at])
    {1, _} = Repo.update_all(from(attempt in Attempt, where: attempt.request_id == ^request_id), set: [completed_at: completed_at])
    :ok
  end

  # Rows in admission order: `{endpoint, transport, status, claim class,
  # index of the request it was chained to}`, the chain read from the owner's
  # client-retry link or, without forwarding and over HTTPS, the resend
  # policy's `client_resend` marker.
  defp rows(scenario) do
    requests = requests(scenario)
    index = requests |> Enum.with_index() |> Map.new(fn {request, position} -> {request.id, position} end)

    Enum.map(requests, fn request ->
      {request.endpoint, request.transport, request.status, request.correlation_id |> String.split(":") |> hd(), Map.get(index, predecessor_id(request))}
    end)
  end

  defp predecessor_id(%Request{request_metadata: %{"client_resend" => %{"predecessor_request_id" => predecessor}}}) when is_binary(predecessor), do: predecessor

  defp predecessor_id(%Request{id: request_id}),
    do: Repo.one(from(link in RequestClientRetryLink, where: link.successor_request_id == ^request_id, select: link.predecessor_request_id))

  defp assert_distinct_claims_and_one_charge_each!(scenario, compactions) do
    compaction_rows = scenario |> requests() |> Enum.filter(&(&1.endpoint == @compact_endpoint))
    assert length(Enum.uniq_by(compaction_rows, & &1.correlation_id)) == compactions
    assert Enum.map(compaction_rows, &charges/1) == List.duplicate(1, compactions)
    assert scenario.upstream |> FakeUpstream.requests() |> Enum.count(&compaction_request?/1) == compactions
    assert :ok = FakeUpstream.verify!(scenario.upstream)
  end

  defp compaction_request?(%{json: %{"input" => input}}) when is_list(input), do: match?(%{"type" => "compaction_trigger"}, List.last(input))
  defp compaction_request?(_request), do: false

  # A charge is a settlement that billed known usage.
  defp charges(%Request{id: request_id}) do
    Repo.aggregate(
      from(entry in LedgerEntry, where: entry.request_id == ^request_id and entry.entry_kind == "settlement" and entry.usage_status == "usage_known" and entry.settled_cost_micros > 0),
      :count
    )
  end

  defp requests(scenario) do
    pool_id = scenario.setup.pool.id
    await_settled!(scenario, nil)
    Repo.all(from(request in Request, where: request.pool_id == ^pool_id, order_by: [asc: request.admitted_at, asc: request.id]))
  end

  # Every response task settles its request after its terminal frame; no
  # completion signal reaches the test, so the rows are polled within a
  # bounded detection budget. `count`, when given, is how many rows exist.
  defp await_settled!(scenario, count) do
    pool_id = scenario.setup.pool.id

    await!(
      fn ->
        statuses = Repo.all(from(request in Request, where: request.pool_id == ^pool_id, select: request.status))
        (is_nil(count) or length(statuses) == count) and Enum.all?(statuses, &(&1 not in ["accepted", "in_progress"]))
      end,
      "requests did not settle"
    )
  end

  # The owner arms the native compaction admission once the terminal frame of
  # the turn before it left; the direct upstream session arms before that
  # turn settles, so its settlement is enough.
  defp await_armed!(%{forwarding: :off} = scenario, settled), do: await_settled!(scenario, settled)

  defp await_armed!(%{forwarding: :on} = scenario, settled) do
    await_settled!(scenario, settled)
    [session_id] = Repo.all(from(session in CodexSession, where: session.pool_id == ^scenario.setup.pool.id, select: session.id))
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session_id)

    await!(
      fn ->
        match?(
          %{native_compaction_admission: %NativeCompactionAdmission{phase: :pending_compact}, native_compaction_admission_downstream: %{pid: pid}, downstream: %{pid: pid}},
          :sys.get_state(owner)
        )
      end,
      "the owner never armed the native compaction admission for the attached socket"
    )
  end

  defp await!(condition, message) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    Stream.repeatedly(condition)
    |> Enum.reduce_while(nil, fn
      true, _acc ->
        {:halt, :ok}

      false, _acc ->
        if System.monotonic_time(:millisecond) >= deadline, do: flunk(message)
        Process.sleep(10)
        {:cont, nil}
    end)
  end

  defp await_barrier!(barrier, release_ref) do
    receive do
      {:fake_upstream_frame_barrier, ^barrier, _handler, ^release_ref} -> :ok
    after
      @detection_timeout_ms -> flunk("the upstream never reached frame barrier #{barrier}")
    end
  end

  # The provider finishes the cut generation, as a live provider would; the
  # Pooler already settled it and may have closed that provider connection,
  # in which case the rest of the reply is never pushed.
  defp release_held_compaction!(upstream, release_ref) do
    %{websocket_connection_id: connection} = Enum.find(FakeUpstream.requests(upstream), &(&1.json["previous_response_id"] == @resume_response))
    _released = FakeUpstream.release_remaining_frames(upstream, release_ref)

    await!(
      fn ->
        receive do
          {:fake_upstream_frame_barrier, 2, _handler, ^release_ref} -> true
        after
          0 -> not FakeUpstream.websocket_connection_alive?(upstream, connection)
        end
      end,
      "the held provider reply neither finished nor lost its connection"
    )

    for barrier <- 1..2, do: FakeUpstream.acknowledge(upstream, {:frame_barrier, release_ref, barrier})
    :ok
  end

  # The upgrade names the client's window at connect time.
  defp connect!(scenario, window) do
    sockets = WebsocketCleanupFence.listener_sockets()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", scenario.port, protocols: [:http1])

    headers = [
      {"authorization", scenario.setup.authorization},
      {"session-id", @thread_id},
      {"thread-id", @thread_id},
      {"x-client-request-id", @thread_id},
      {"x-codex-window-id", window_id(window)}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, @turn_endpoint, headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    %{conn: conn, websocket: websocket, ref: ref, cleanup_socket: WebsocketCleanupFence.await_new_listener_socket!(sockets)}
  end

  defp close!(client) do
    Mint.HTTP.close(client.conn)
    :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(client.cleanup_socket)
  end

  defp send_frame!(client, payload) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, CodexPooler.JSON.encode!(payload))
    %{client | conn: conn, websocket: websocket}
  end

  defp assert_completed!(client) do
    {client, frames} = receive_until_terminal(client, [])
    assert %{"type" => "response.completed"} = List.last(frames), inspect(Enum.map(frames, &Map.take(&1, ["type", "status", "error"])))
    client
  end

  # The Pooler collects a native compaction and shows the client only its
  # item and its terminal.
  defp assert_compaction_served!(client) do
    {client, frames} = receive_until_terminal(client, [])
    assert Enum.map(frames, & &1["type"]) == ["response.output_item.done", "response.completed"], inspect(Enum.map(frames, &Map.take(&1, ["type", "status", "error"])))
    client
  end

  defp receive_until_terminal(client, seen) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    client = %{client | conn: conn, websocket: websocket}
    frame = CodexPooler.JSON.decode!(text)
    seen = [frame | seen]

    if frame["type"] in ["response.completed", "response.failed", "error"],
      do: {client, Enum.reverse(seen)},
      else: receive_until_terminal(client, seen)
  end

  # The HTTP request the released client builds from the websocket one: the
  # same body without the websocket-only keys and the turn metadata echoed as
  # a header, on the window it names.
  defp post_native!(scenario, body, window) do
    build_conn()
    |> put_req_header("authorization", scenario.setup.authorization)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("accept", "text/event-stream")
    |> put_req_header("session-id", @thread_id)
    |> put_req_header("thread-id", @thread_id)
    |> put_req_header("x-client-request-id", @thread_id)
    |> put_req_header("x-codex-window-id", window_id(window))
    |> put_req_header("x-codex-turn-metadata", body["client_metadata"]["x-codex-turn-metadata"])
    |> put_req_header("originator", "codex_cli_rs")
    |> post(@turn_endpoint, CodexPooler.JSON.encode!(body))
  end

  defp https_body(frame) do
    frame
    |> Map.delete("type")
    |> Map.update!("client_metadata", &Map.drop(&1, ["x-codex-ws-stream-request-start-ms", @lite_marker]))
  end

  defp assert_https_compaction_served!(conn) do
    assert conn.status == 200 and conn.resp_body =~ "response.completed", inspect({conn.status, conn.resp_body})
  end

  defp assert_https_completed!(conn) do
    assert conn.status == 200 and conn.resp_body =~ "response.completed", inspect({conn.status, conn.resp_body})
  end

  defp put_owner_forwarding!(enabled?) do
    previous = Application.fetch_env(:codex_pooler, :websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)

    on_exit(fn ->
      case previous do
        :error -> Application.delete_env(:codex_pooler, :websocket_owner_forwarding_enabled)
        {:ok, value} -> Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, value)
      end
    end)
  end

  # The provider's side, in order. The anchored compactions reach the
  # connection that produced their anchor (the first one).

  defp opener, do: fn _mode -> FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames(@opener_response, [function_call(1)])) end

  defp first_compaction do
    fn mode ->
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: lite_marker_expectation(%{"previous_response_id" => @opener_response}, mode)],
        respond: compaction_frames(compaction_item(1), @first_compaction_response)
      )
    end
  end

  defp resume, do: fn _mode -> FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames(@resume_response, [function_call(2)])) end

  defp anchored_second_compaction do
    fn mode ->
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: lite_marker_expectation(%{"previous_response_id" => @resume_response}, mode)],
        respond: compaction_frames(compaction_item(2), @second_compaction_response)
      )
    end
  end

  defp held_second_compaction(release_ref) do
    fn mode ->
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: lite_marker_expectation(%{"previous_response_id" => @resume_response}, mode)],
        respond: FakeUpstream.barrier_websocket_frames(compaction_messages(compaction_item(2), @second_compaction_response), notify: self(), release_ref: release_ref)
      )
    end
  end

  defp full_history_second_compaction(response_id) do
    fn mode ->
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        json: [valid: true, equals: lite_marker_expectation(%{"type" => "response.create"}, mode), forbidden: ["previous_response_id"]],
        respond: compaction_frames(compaction_item(2), response_id)
      )
    end
  end

  defp https_second_compaction(response_id) do
    fn _mode ->
      FakeUpstream.expect_request(
        method: "POST",
        path: @turn_endpoint,
        json: [valid: true, forbidden: ["previous_response_id", "type"]],
        respond: FakeUpstream.sse_stream(compaction_events(compaction_item(2), response_id))
      )
    end
  end

  defp final_resume, do: fn _mode -> FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames(@final_response, [answer()])) end

  defp https_final_resume,
    do: fn _mode -> FakeUpstream.expect_request(method: "POST", path: @turn_endpoint, json: [valid: true, forbidden: ["previous_response_id"]], respond: FakeUpstream.sse_stream(completed_events(@final_response, [answer()]))) end

  defp lite_marker_expectation(expected, "lite"), do: Map.put(expected, "client_metadata.#{@lite_marker}", "true")
  defp lite_marker_expectation(expected, "full"), do: expected

  # The client's side. The opener asks for the first tool; the first
  # compaction carries its output, anchored on the opener; the resume, one
  # window later, continues from the compaction item and asks for the second
  # tool; the second compaction carries that tool's output, anchored on the
  # resume (or as full history); the last resume continues from the second
  # compaction item, one more window later.

  defp opener_frame(scenario), do: frame(scenario, context_prefix(scenario.mode) ++ [prompt()], 0, turn_metadata("turn", 0))

  defp first_compaction_frame(scenario) do
    scenario
    |> frame([function_call_output(1), %{"type" => "compaction_trigger"}], 0, turn_metadata("compaction", 0))
    |> Map.put("previous_response_id", @opener_response)
  end

  defp resume_frame(scenario, window), do: frame(scenario, context_prefix(scenario.mode) ++ [compaction_item(window)], window, turn_metadata("turn", window))

  defp anchored_second_compaction_frame(scenario) do
    scenario
    |> frame([function_call_output(2), %{"type" => "compaction_trigger"}], 1, turn_metadata("compaction", 1))
    |> Map.put("previous_response_id", @resume_response)
  end

  defp full_history_first_compaction_frame(scenario) do
    input = context_prefix(scenario.mode) ++ [prompt(), function_call(1), function_call_output(1), %{"type" => "compaction_trigger"}]
    frame(scenario, input, 0, turn_metadata("compaction", 0))
  end

  defp full_history_second_compaction_frame(scenario) do
    input = context_prefix(scenario.mode) ++ [compaction_item(1), function_call(2), function_call_output(2), %{"type" => "compaction_trigger"}]
    frame(scenario, input, 1, turn_metadata("compaction", 1))
  end

  # Full: the released client's top-level `instructions` and `tools`, parallel
  # tool calls on. Lite: neither top-level key, parallel tool calls off, and
  # the Lite marker in `client_metadata`.
  defp frame(scenario, input, window, metadata) do
    client_metadata = %{
      "session_id" => @thread_id,
      "thread_id" => @thread_id,
      "turn_id" => @turn_id,
      "root_turn_id" => @turn_id,
      "x-codex-installation-id" => @installation_id,
      "x-codex-window-id" => window_id(window),
      "x-codex-turn-metadata" => metadata,
      "x-codex-ws-stream-request-start-ms" => Integer.to_string(System.system_time(:millisecond))
    }

    base = %{
      "type" => "response.create",
      "model" => scenario.setup.model.exposed_model_id,
      "input" => input,
      "tool_choice" => "auto",
      "reasoning" => %{"effort" => "low"},
      "store" => false,
      "stream" => true,
      "include" => ["reasoning.encrypted_content"],
      "text" => %{"verbosity" => "low"},
      "prompt_cache_key" => @thread_id
    }

    case scenario.mode do
      "full" -> Map.merge(base, %{"instructions" => "synthetic instructions", "tools" => [], "parallel_tool_calls" => true, "client_metadata" => client_metadata})
      "lite" -> Map.merge(base, %{"parallel_tool_calls" => false, "client_metadata" => Map.put(client_metadata, @lite_marker, "true")})
    end
  end

  defp turn_metadata(kind, window) do
    compaction = %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "mid_turn", "strategy" => "memento"}

    %{
      "agent_name" => "/root",
      "analytics_enabled" => true,
      "auto_review_enabled" => false,
      "context_window_id" => "00000000-0000-4000-8000-00000000f36#{window}",
      "installation_id" => @installation_id,
      "root_turn_id" => @turn_id,
      "sandbox" => "seatbelt",
      "sandbox_mode" => "read-only",
      "session_id" => @thread_id,
      "thread_id" => @thread_id,
      "turn_id" => @turn_id,
      "turn_started_at_unix_ms" => 1_790_000_000_000,
      "window_id" => window_id(window),
      "window_number" => window,
      "model" => "gpt-test-model",
      "reasoning_effort" => "low",
      "request_kind" => kind
    }
    |> then(&if(kind == "compaction", do: Map.put(&1, "compaction", compaction), else: &1))
    |> CodexPooler.JSON.encode!()
  end

  defp window_id(window), do: "#{@thread_id}:#{window}"

  # The released Lite client opens a provider context with its tool manifest.
  defp context_prefix("lite"), do: [%{"type" => "additional_tools", "role" => "developer", "tools" => []}]
  defp context_prefix("full"), do: []

  defp prompt, do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic second compaction prompt"}]}

  defp answer, do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}

  defp function_call(round), do: %{"type" => "function_call", "call_id" => "call_second_compaction_#{round}", "name" => "shell", "arguments" => "{}"}

  defp function_call_output(round), do: %{"type" => "function_call_output", "call_id" => "call_second_compaction_#{round}", "output" => "synthetic output #{round}"}

  defp compaction_item(window), do: %{"type" => "compaction", "encrypted_content" => "synthetic-second-compaction-#{window}"}

  defp usage, do: %{"input_tokens" => 20_000, "output_tokens" => 10, "total_tokens" => 20_010}

  defp completed_frames(response_id, output), do: FakeUpstream.websocket_text_frames(Enum.map(completed_events(response_id, output), &CodexPooler.JSON.encode!/1))

  defp completed_events(response_id, output) do
    [%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}] ++
      Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++
      [%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => output, "usage" => usage()}}]
  end

  defp compaction_frames(item, response_id), do: FakeUpstream.websocket_text_frames(compaction_messages(item, response_id))

  defp compaction_messages(item, response_id) do
    [
      CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => item}),
      CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => usage()}})
    ]
  end

  defp compaction_events(item, response_id) do
    [
      %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}},
      %{"type" => "response.output_item.done", "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => usage()}}
    ]
  end
end
