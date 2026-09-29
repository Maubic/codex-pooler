defmodule CodexPoolerWeb.Runtime.BackendCodexCompactionUpstreamFidelityTest do
  # What a remote compaction carries to the provider (findings#270 rows
  # 270-368 and 270-359). The released client (0.159.0) sends a remote
  # compaction as a Responses request whose last input item is a
  # `compaction_trigger`, with its `client_metadata` (its turn metadata among
  # it), `include` and `tool_choice` like any other request of the turn; sent
  # direct, the provider receives all of it. Through the Pooler the compaction
  # bridge kept only the compaction's own fields: the provider received no
  # client metadata, no `include` and no `tool_choice` for a compaction, over
  # the websocket and over HTTPS, while every other request of the turn kept
  # them (P1's wire comparison of 270-361, direct and through the Pooler, with
  # the released client). The bridged frame kept no turn metadata of its own
  # either, so helpers reading that body could fall back to the metadata of
  # the upgrade, the prewarm that opened the socket: kind `prewarm`,
  # no turn, window 0.
  #
  # A compaction now reaches the provider as the turn's other requests do: the
  # client's metadata with the same scrubs (the websocket frame's `turn_id`),
  # the Lite marker merged into it, and its `include` and `tool_choice`.
  #
  # Frames keep the released client's key sets and handshake; identifiers,
  # prompt text and reply frames are synthetic. One node, owner forwarding off
  # and on, the Pool's serving mode Full and Lite. FakeUpstream.
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.WebsocketTurnIdentity
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Transports.Websocket.{NativeCompactionAdmission, WebsocketOwnerSession}
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @thread_id "019a0000-0000-7000-8000-00000000f368"
  @turn_id "019a0000-0000-7000-8000-00000000f369"
  @installation_id "00000000-0000-4000-8000-00000000f370"
  @opener_response "resp_fidelity_compaction_opener"
  @compaction_response "resp_fidelity_compaction_compct"
  @resume_response "resp_fidelity_compaction_resume"
  @lite_marker "ws_request_header_x_openai_internal_codex_responses_lite"
  @turn_endpoint "/backend-api/codex/responses"
  # Detection budget for a frame or a row the test only observes.
  @detection_timeout_ms 15_000

  # On a socket a prewarm opened: the turn, its anchored compaction and the
  # resume. The provider receives the compaction with the client's metadata,
  # `include` and `tool_choice`, exactly as it receives the turn and the
  # resume, and the compaction's turn metadata is its own, not the prewarm's.
  for mode <- ["full", "lite"], forwarding <- [:off, :on] do
    @tag mode: mode, forwarding: forwarding
    test "#{mode} forwarding #{forwarding}: an anchored remote compaction reaches the provider with the client's metadata, include and tool_choice", ctx do
      scenario = start_scenario!(ctx, [turn_request(@opener_response, [function_call()]), compaction_request(:anchored), turn_request(@resume_response, [answer()])])

      client = connect!(scenario, 0)
      client = client |> send_frame!(prewarm_frame(scenario)) |> assert_prewarm_answered!()
      opener = opener_frame(scenario)
      client = client |> send_frame!(opener) |> assert_completed!()
      await_armed!(scenario, 1)
      compaction = anchored_compaction_frame(scenario)
      client = client |> send_frame!(compaction) |> assert_compaction_served!()
      resume = resume_frame(scenario)
      client |> send_frame!(resume) |> assert_completed!() |> close!()

      assert [opener_sent, compaction_sent, resume_sent] = upstream_requests(scenario)

      for {sent, frame} <- [{opener_sent, opener}, {compaction_sent, compaction}, {resume_sent, resume}] do
        assert client_fields(sent.json) == expected_websocket_fields(frame, scenario.mode)
      end

      assert %{"request_kind" => "compaction", "window_id" => window} = compaction_sent.json["client_metadata"]["x-codex-turn-metadata"] |> CodexPooler.JSON.decode!()
      assert window == window_id(0)
      assert :ok = FakeUpstream.verify!(scenario.upstream)
    end
  end

  # After a reconnect the compaction is sent as full history on a new socket.
  for mode <- ["full", "lite"], forwarding <- [:off, :on] do
    @tag mode: mode, forwarding: forwarding
    test "#{mode} forwarding #{forwarding}: a full-history remote compaction reaches the provider with the client's metadata, include and tool_choice", ctx do
      scenario = start_scenario!(ctx, [turn_request(@opener_response, [function_call()]), compaction_request(:full_history)])

      client = connect!(scenario, 0)
      client = client |> send_frame!(prewarm_frame(scenario)) |> assert_prewarm_answered!()
      client |> send_frame!(opener_frame(scenario)) |> assert_completed!() |> close!()
      await_settled!(scenario, 1)

      compaction = full_history_compaction_frame(scenario)
      connect!(scenario, 0) |> send_frame!(compaction) |> assert_compaction_served!() |> close!()

      assert [_opener_sent, compaction_sent] = upstream_requests(scenario)
      assert client_fields(compaction_sent.json) == expected_websocket_fields(compaction, scenario.mode)
      assert :ok = FakeUpstream.verify!(scenario.upstream)
    end
  end

  # Over HTTPS, once the session fell back: the body carries the client's
  # metadata as it sent it, and its `include` and `tool_choice`.
  for mode <- ["full", "lite"], forwarding <- [:off, :on] do
    @tag mode: mode, forwarding: forwarding
    test "#{mode} forwarding #{forwarding}: a remote compaction over HTTPS reaches the provider with the client's metadata, include and tool_choice", ctx do
      scenario = start_scenario!(ctx, [https_compaction_request()])
      body = https_body(full_history_compaction_frame(scenario))

      conn = post_native!(scenario, body)
      assert conn.status == 200 and conn.resp_body =~ "response.completed", inspect({conn.status, conn.resp_body})

      assert [compaction_sent] = upstream_requests(scenario)
      assert client_fields(compaction_sent.json) == client_fields(body)
      assert :ok = FakeUpstream.verify!(scenario.upstream)
    end
  end

  # Dequeue still prepares a descriptor at upstream dispatch from the frame's
  # pre-bridge continuity. This full-history path has no admission capability;
  # it does not depend on recovering a turn id from the scrubbed upstream body.
  for mode <- ["full", "lite"] do
    @tag mode: mode, forwarding: :on
    test "#{mode}: a queued full-history compaction keeps its pre-bridge turn descriptor", ctx do
      hold = make_ref()
      barrier = make_ref()
      handler_id = {__MODULE__, hold}
      on_exit(fn -> :telemetry.detach(handler_id) end)
      :ok = :telemetry.attach(handler_id, [:codex_pooler, :gateway, :stream, :outcome], &__MODULE__.hold_settled_turn/4, %{test: self(), hold: hold, claimed: :atomics.new(1, [])})
      {:websocket_text, frames} = compaction_frames(compaction_item(), @compaction_response)
      compact = FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: FakeUpstream.barrier_websocket_frames(frames, notify: self(), release_ref: barrier))
      scenario = start_scenario!(ctx, [turn_request(@opener_response, [function_call()]), compact])
      client = connect!(scenario, 0)
      client = client |> send_frame!(prewarm_frame(scenario)) |> assert_prewarm_answered!()
      client = client |> send_frame!(opener_frame(scenario)) |> assert_completed!()
      assert_receive {^hold, :held, task, callers}, @detection_timeout_ms
      :telemetry.detach(handler_id)
      on_exit(fn -> send(task, {hold, :release}) end)

      client = send_frame!(client, full_history_compaction_frame(scenario))
      await!(fn -> Enum.any?(callers, &compaction_queued?/1) end, "the full-history compaction never queued")
      send(task, {hold, :release})
      assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^barrier}, @detection_timeout_ms
      [session] = Repo.all(from(session in CodexSession, where: session.pool_id == ^scenario.setup.pool.id))
      assert {:ok, owner} = WebsocketOwnerSession.lookup(session.id)
      assert %{active_turn: %{descriptor: %{kind: :native, semantic_turn_key: key}, admission_phase: nil}} = :sys.get_state(owner)
      scope = WebsocketTurnIdentity.claim_scope(session, @thread_id)
      assert {:ok, %{semantic_turn_key: ^key}} = WebsocketTurnIdentity.resolve(full_history_compaction_frame(scenario), scope)
      assert :ok = FakeUpstream.release_remaining_frames(scenario.upstream, barrier)
      client |> assert_compaction_served!() |> close!()
      assert_receive {:fake_upstream_frame_barrier, 2, _handler, ^barrier}, @detection_timeout_ms
      assert :ok = FakeUpstream.verify!(scenario.upstream)
    end
  end

  @doc false
  def hold_settled_turn(_event, _measurements, %{outcome: "succeeded", downstream_transport: "websocket"}, %{hold: hold, test: test, claimed: claimed}) do
    if not Repo.in_transaction?() and :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {hold, :held, self(), Process.get(:"$callers", [])})

      receive do
        {^hold, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  def hold_settled_turn(_event, _measurements, _metadata, _config), do: :ok

  defp compaction_queued?(pid) do
    pid |> :sys.get_state(1_000) |> queued_frames(6) |> Enum.any?(&match?(%{endpoint: "/backend-api/codex/responses/compact"}, &1))
  catch
    :exit, _ -> false
  end

  defp queued_frames(%{queued_response_payloads: queue}, _depth), do: :queue.to_list(queue)
  defp queued_frames(_term, 0), do: []
  defp queued_frames(%_{} = struct, depth), do: struct |> Map.from_struct() |> queued_frames(depth)
  defp queued_frames(map, depth) when is_map(map), do: Enum.flat_map(Map.values(map), &queued_frames(&1, depth - 1))
  defp queued_frames(tuple, depth) when is_tuple(tuple), do: tuple |> Tuple.to_list() |> Enum.flat_map(&queued_frames(&1, depth - 1))
  defp queued_frames(_term, _depth), do: []

  # The fields a compaction used to lose on its way to the provider, the turn
  # metadata decoded so that its encoding does not matter.
  defp client_fields(json) do
    json
    |> Map.take(["client_metadata", "include", "tool_choice"])
    |> update_in([Access.key("client_metadata", %{}), Access.key("x-codex-turn-metadata")], &decode_document/1)
    |> then(&if(&1["client_metadata"] == %{"x-codex-turn-metadata" => nil}, do: Map.delete(&1, "client_metadata"), else: &1))
  end

  defp decode_document(document) when is_binary(document), do: CodexPooler.JSON.decode!(document)
  defp decode_document(document), do: document

  # A websocket frame reaches the provider without the client's `turn_id`,
  # in its metadata and in its turn metadata (the Pooler's replay scrub for
  # every native frame), and in Lite with the Lite marker the Pooler sets.
  defp expected_websocket_fields(frame, mode) do
    metadata = Map.delete(frame["client_metadata"], "turn_id")
    metadata = Map.update!(metadata, "x-codex-turn-metadata", &(&1 |> CodexPooler.JSON.decode!() |> Map.delete("turn_id") |> CodexPooler.JSON.encode!()))
    metadata = if mode == "lite", do: Map.put(metadata, @lite_marker, "true"), else: metadata
    frame |> Map.take(["include", "tool_choice"]) |> Map.put("client_metadata", metadata) |> client_fields()
  end

  defp start_scenario!(ctx, sequence) do
    put_owner_forwarding!(ctx.forwarding == :on)
    upstream = start_upstream(FakeUpstream.strict_sequence(sequence))
    setup = gateway_setup(upstream, compact?: true)
    if ctx.mode == "lite", do: set_model_serving_mode!(model_serving_scope(), setup, "lite")
    %{mode: ctx.mode, forwarding: ctx.forwarding, upstream: upstream, setup: setup, port: start_public_endpoint!()}
  end

  defp upstream_requests(scenario), do: scenario.upstream |> FakeUpstream.requests() |> Enum.reject(&(&1.method not in ["WEBSOCKET", "POST"]))

  defp await_settled!(scenario, count) do
    pool_id = scenario.setup.pool.id

    await!(
      fn ->
        statuses = Repo.all(from(request in Request, where: request.pool_id == ^pool_id, select: request.status))
        length(statuses) == count and Enum.all?(statuses, &(&1 not in ["accepted", "in_progress"]))
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

  # The released client's handshake names the request it opens the socket
  # for: the prewarm, with its turn metadata (no turn, window 0).
  defp connect!(scenario, window) do
    sockets = WebsocketCleanupFence.listener_sockets()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", scenario.port, protocols: [:http1])

    headers = [
      {"authorization", scenario.setup.authorization},
      {"session-id", @thread_id},
      {"thread-id", @thread_id},
      {"x-client-request-id", @thread_id},
      {"x-codex-window-id", window_id(window)},
      {"x-codex-turn-metadata", turn_metadata("prewarm", window)},
      {"x-codex-beta-features", "remote_compaction_v2"},
      {"originator", "codex_cli_rs"}
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

  # The Pooler answers the prewarm itself, with an empty response id.
  defp assert_prewarm_answered!(client) do
    {client, frames} = receive_until_terminal(client, [])
    assert %{"type" => "response.completed", "response" => %{"id" => ""}} = List.last(frames)
    client
  end

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
  # a header; in Lite the marker is a header.
  defp post_native!(scenario, body) do
    conn =
      build_conn()
      |> put_req_header("authorization", scenario.setup.authorization)
      |> put_req_header("content-type", "application/json")
      |> put_req_header("accept", "text/event-stream")
      |> put_req_header("session-id", @thread_id)
      |> put_req_header("thread-id", @thread_id)
      |> put_req_header("x-client-request-id", @thread_id)
      |> put_req_header("x-codex-window-id", window_id(0))
      |> put_req_header("x-codex-turn-metadata", body["client_metadata"]["x-codex-turn-metadata"])
      |> put_req_header("originator", "codex_cli_rs")

    conn = if scenario.mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @turn_endpoint, CodexPooler.JSON.encode!(body))
  end

  defp https_body(frame) do
    frame
    |> Map.delete("type")
    |> Map.update!("client_metadata", &Map.drop(&1, ["x-codex-ws-stream-request-start-ms", @lite_marker]))
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

  # The provider's side.

  defp turn_request(response_id, output),
    do: FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: completed_frames(response_id, output))

  defp compaction_request(:anchored),
    do:
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: %{"previous_response_id" => @opener_response}],
        respond: compaction_frames(compaction_item(), @compaction_response)
      )

  defp compaction_request(:full_history),
    do: FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: compaction_frames(compaction_item(), @compaction_response))

  defp https_compaction_request,
    do:
      FakeUpstream.expect_request(
        method: "POST",
        path: @turn_endpoint,
        json: [valid: true, forbidden: ["previous_response_id", "type"]],
        respond: FakeUpstream.sse_stream(compaction_events(compaction_item(), @compaction_response))
      )

  # The client's side.

  defp prewarm_frame(scenario) do
    scenario
    |> frame(context_prefix(scenario.mode) ++ [developer_message(), environment_message()], 0, turn_metadata("prewarm", 0))
    |> Map.put("generate", false)
    |> update_in(["client_metadata"], &(&1 |> Map.put("turn_id", "") |> Map.delete("root_turn_id")))
  end

  defp opener_frame(scenario), do: frame(scenario, history(scenario), 0, turn_metadata("turn", 0))

  defp anchored_compaction_frame(scenario) do
    scenario
    |> frame([function_call_output(), %{"type" => "compaction_trigger"}], 0, turn_metadata("compaction", 0))
    |> Map.put("previous_response_id", @opener_response)
  end

  defp full_history_compaction_frame(scenario),
    do: frame(scenario, history(scenario) ++ [function_call(), function_call_output(), %{"type" => "compaction_trigger"}], 0, turn_metadata("compaction", 0))

  defp resume_frame(scenario), do: frame(scenario, context_prefix(scenario.mode) ++ [compaction_item()], 1, turn_metadata("turn", 1))

  defp history(scenario), do: context_prefix(scenario.mode) ++ [developer_message(), environment_message(), prompt()]

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
      "full" -> Map.merge(base, %{"instructions" => "synthetic instructions", "tools" => [tool()], "parallel_tool_calls" => true, "client_metadata" => client_metadata})
      "lite" -> Map.merge(base, %{"parallel_tool_calls" => false, "client_metadata" => Map.put(client_metadata, @lite_marker, "true")})
    end
  end

  defp turn_metadata(kind, window) do
    compaction = %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "mid_turn", "strategy" => "memento"}
    turn_id = if kind == "prewarm", do: "", else: @turn_id

    %{
      "agent_name" => "/root",
      "analytics_enabled" => true,
      "auto_review_enabled" => false,
      "context_window_id" => "00000000-0000-4000-8000-00000000f37#{window}",
      "installation_id" => @installation_id,
      "sandbox" => "seatbelt",
      "sandbox_mode" => "read-only",
      "session_id" => @thread_id,
      "thread_id" => @thread_id,
      "turn_id" => turn_id,
      "window_id" => window_id(window),
      "window_number" => window,
      "model" => "gpt-test-model",
      "reasoning_effort" => "low",
      "request_kind" => kind
    }
    |> then(&if(kind == "prewarm", do: &1, else: Map.merge(&1, %{"root_turn_id" => @turn_id, "turn_started_at_unix_ms" => 1_790_000_000_000})))
    |> then(&if(kind == "compaction", do: Map.put(&1, "compaction", compaction), else: &1))
    |> CodexPooler.JSON.encode!()
  end

  defp window_id(window), do: "#{@thread_id}:#{window}"

  # The released Lite client opens a provider context with its tool manifest.
  defp context_prefix("lite"), do: [%{"type" => "additional_tools", "role" => "developer", "tools" => [tool()]}]
  defp context_prefix("full"), do: []

  defp tool, do: %{"type" => "function", "name" => "shell", "description" => "synthetic tool", "strict" => false, "parameters" => %{"type" => "object", "properties" => %{}}}
  defp developer_message, do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "synthetic developer instructions"}]}
  defp environment_message, do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic environment context"}]}
  defp prompt, do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic fidelity prompt"}]}
  defp answer, do: %{"type" => "message", "role" => "assistant", "content" => [%{"type" => "output_text", "text" => "synthetic answer"}]}
  defp function_call, do: %{"type" => "function_call", "call_id" => "call_fidelity", "name" => "shell", "arguments" => "{}"}
  defp function_call_output, do: %{"type" => "function_call_output", "call_id" => "call_fidelity", "output" => "synthetic output"}
  defp compaction_item, do: %{"type" => "compaction", "encrypted_content" => "synthetic-fidelity-compaction"}

  defp usage, do: %{"input_tokens" => 20_000, "output_tokens" => 10, "total_tokens" => 20_010}

  defp completed_frames(response_id, output) do
    FakeUpstream.websocket_text_frames(
      [CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}})] ++
        Enum.map(output, &CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => &1})) ++
        [CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => output, "usage" => usage()}})]
    )
  end

  defp compaction_frames(item, response_id) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.output_item.done", "item" => item}),
      CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => usage()}})
    ])
  end

  defp compaction_events(item, response_id) do
    [
      %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}},
      %{"type" => "response.output_item.done", "item" => item},
      %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => usage()}}
    ]
  end
end
