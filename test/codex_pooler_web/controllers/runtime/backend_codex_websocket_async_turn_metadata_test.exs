defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketAsyncTurnMetadataTest do
  # The websocket side of findings#314 row 314-2. The released Codex client
  # fills `workspaces` in its `x-codex-turn-metadata` document from a git task
  # it starts for every turn, and nothing on the request path waits for it
  # (`turn_metadata.rs` `spawn_git_enrichment_task`; observed with a released
  # client over HTTP: the first request of a turn went out without the field
  # and the next one carried it). A websocket request's resend witness binds
  # that document (every field except `turn_id`), so a request sent before the
  # task finished and its resend sent after it differ. The resend of a turn
  # cut short must still meet the request it repeats, whether that request was
  # the turn's first on its socket (unanchored, witnessed by its replay digest)
  # or a later turn's first request, which the client sends anchored on the
  # previous turn's response (witnessed by its anchor-free tail digest) and
  # resends as full history on a new socket or over HTTPS.
  #
  # One node, owner forwarding off (the closing socket stops the turn) and on
  # (the session's owner on this node suspends a turn cut before any output
  # into a replay the resend redeems), native websocket
  # `/backend-api/codex/responses`, the Pool's serving mode forced to Full and
  # to Lite, FakeUpstream holding the cut request, the released client's frame
  # shape (turn metadata naming thread and turn in `client_metadata`),
  # synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, native_text_input: 1, public_websocket_connect!: 3, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, stop_websocket_owner_session: 1, with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @timeout_ms 15_000
  @poll_ms 20
  @workspaces %{"/synthetic/repository" => %{"associated_remote_urls" => %{"origin" => "https://example.com/sample-app.git"}, "latest_git_commit_hash" => String.duplicate("b", 40), "has_changes" => true}}
  @filled %{"workspaces" => @workspaces}
  @changed %{"sandbox" => "workspace-write"}
  # The frames before the second text delta: the client saw output, and no
  # completed item, when its socket closed.
  @delta_hold 5

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    :ok
  end

  # Forwarding off, a request cut before any output: the closing socket stops
  # the turn, the request settles `client_disconnected`, and the resend on a
  # new socket is admitted by the resend policy as its linked successor.
  for mode <- ["full", "lite"], position <- [:first_turn, :anchored_turn] do
    test "forwarding off, #{mode}, #{position}: the resend of a request cut before any output that gained the async workspaces is served" do
      result = scenario!(%{forwarding: :off, cut: :previsible, mode: unquote(mode), position: unquote(position), transport: :websocket, original: %{}, resend: @filled})

      assert_served_successor!(result, :resend_policy)
    end
  end

  # Forwarding on, a request cut before any output: the owner suspends it into
  # a replay entitlement and the resend redeems it (`RequestReplay`, which
  # finds the request's witness among the resend's alternates and rebinds the
  # resend to the entitlement's claim): the same request is served by one more
  # generation.
  for mode <- ["full", "lite"], position <- [:first_turn, :anchored_turn] do
    test "forwarding on, #{mode}, #{position}: the resend of a suspended request that gained the async workspaces redeems its replay" do
      result = scenario!(%{forwarding: :on, cut: :previsible, mode: unquote(mode), position: unquote(position), transport: :websocket, original: %{}, resend: @filled})

      assert %{"type" => "response.completed"} = result.outcome, "the resend was refused: #{inspect(result.outcome)} #{result.logs}"
      assert [%Request{status: "succeeded"} = served] = later_requests(result)
      assert served.id == result.original.id
      assert Enum.sort(Repo.all(from(a in Attempt, where: a.request_id == ^served.id, select: {a.replay_generation, a.status}))) == [{0, "retryable_failed"}, {1, "succeeded"}]
      assert FakeUpstream.count(result.upstream) == length(result.earlier) + 2
    end
  end

  # A request cut after its client was shown output and no completed item is
  # resent unchanged; over HTTPS (the released client's fallback) and, with
  # forwarding on, through the owner's client-retry preflight, the resend is
  # matched by the same witness alternates.
  for forwarding <- [:off, :on], position <- [:first_turn, :anchored_turn], transport <- [:https, :websocket] do
    test "forwarding #{forwarding}, #{position}: the #{transport} resend of a request cut after partial output that gained the async workspaces is served" do
      result = scenario!(%{forwarding: unquote(forwarding), cut: :delta, mode: "lite", position: unquote(position), transport: unquote(transport), original: %{}, resend: @filled})

      assert_served_successor!(result, if(unquote(forwarding) == :on and unquote(transport) == :websocket, do: :owner_preflight, else: :resend_policy))
    end
  end

  # Only the fields the client fills late are set aside, and only when the
  # resend carries them: a resend with another document field changed, or one
  # whose request carried `workspaces` and the resend not, is a different
  # request.
  for forwarding <- [:off, :on], position <- [:first_turn, :anchored_turn], {label, original, resend} <- [{"another document field changed", %{}, @changed}, {"the workspaces the request carried lost", @filled, %{}}] do
    test "forwarding #{forwarding}, #{position}: a resend with #{label} stays refused" do
      result = scenario!(%{forwarding: unquote(forwarding), cut: :previsible, mode: "full", position: unquote(position), transport: :websocket, original: unquote(Macro.escape(original)), resend: unquote(Macro.escape(resend))})

      assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = result.outcome
      assert FakeUpstream.count(result.upstream) == length(result.earlier) + 1
    end
  end

  # Incoming mail stops the client after a completed reasoning item; its next
  # request is the previous input, the reasoning it kept and the mail. When it
  # gained `workspaces` meanwhile, the mailbox proof still names the request it
  # continues (the websocket mailbox branch).
  for forwarding <- [:off, :on], mode <- ["full", "lite"] do
    test "forwarding #{forwarding}, #{mode}: a websocket mailbox continuation that gained the async workspaces is served" do
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, unquote(forwarding) == :on)
      reasoning = %{"type" => "reasoning", "id" => "rs_async_mailbox", "summary" => [%{"type" => "summary_text", "text" => "synthetic summary"}], "encrypted_content" => "synthetic-reasoning"}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a completed reasoning item and the terminal, then the continuation's completion on a new connection
          FakeUpstream.strict_sequence([
            native_request(1, [forbidden: ["previous_response_id"]], FakeUpstream.websocket_text_frames(completed_frames("resp_async_mailbox", [reasoning]))),
            FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: FakeUpstream.websocket_text_frames(completed_frames("resp_async_mailbox_continuation", [])))
          ])
        )

      setup = gateway_setup(upstream)
      on_exit(fn -> for id <- Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id)), do: stop_websocket_owner_session(id) end)
      _revision = set_model_serving_mode!(model_serving_scope(), setup, unquote(mode))
      port = start_public_endpoint!()
      thread = Ecto.UUID.generate()
      turn_id = Ecto.UUID.generate()
      input = native_text_input("synthetic mailbox turn")

      {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame(setup, thread, turn_id, input, %{}, %{}))
      {conn, _websocket, first} = receive_terminal!(conn, websocket, ref)
      assert first["type"] == "response.completed"
      _closed = Mint.HTTP.close(conn)
      assert [original] = await_settled!(setup, 1)
      await_receipt!(original)

      mail = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}
      continuation = frame(setup, thread, turn_id, input ++ [Map.put(reasoning, "content", nil), mail], @filled, %{})
      {outcome, logs} = with_info_log(fn -> resend_on_new_socket!(port, setup, thread, continuation) end)

      assert %{"type" => "response.completed"} = outcome, "the continuation was refused: #{inspect(outcome)} #{logs}"
      assert [first, successor] = await_settled!(setup, 2)
      assert first.id == original.id
      assert successor.status == "succeeded"
      assert Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^original.id and l.successor_request_id == ^successor.id))
      assert FakeUpstream.count(upstream) == 2
    end
  end

  # Known gap, pinned: a socket that dies without running its cleanup leaves
  # its turn running in the owner for an identical resend to reattach to, and
  # the owner matches the reattach on the exact replay claim. A resend that
  # gained `workspaces` is answered `owner_busy` while that turn runs; the turn
  # then ends `owner_unavailable` with no downstream, the next websocket resend
  # is refused `terminal_predecessor`, and the HTTPS fallback the released
  # client turns to is served, the native HTTP walk stepping over the failed
  # request.
  test "forwarding on: the resend of a lost turn that gained the async workspaces does not reattach, and the HTTPS fallback serves the turn" do
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    release_ref = make_ref()

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; the cut request held before its first frame, then the HTTPS fallback's completion
        FakeUpstream.strict_sequence([
          native_request(1, [forbidden: ["previous_response_id"]], FakeUpstream.barrier_websocket_frames(completed_frames("resp_async_cut_turn", []), notify: self(), release_ref: release_ref)),
          FakeUpstream.expect_request(method: "POST", path: @path, respond: FakeUpstream.sse_stream(Enum.map(completed_events("resp_async_fallback", []), &{&1["type"], &1})))
        ])
      )

    setup = gateway_setup(upstream)
    on_exit(fn -> for id <- Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id)), do: stop_websocket_owner_session(id) end)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, "full")
    client = %{port: start_public_endpoint!(), setup: setup, thread: Ecto.UUID.generate(), turn_id: Ecto.UUID.generate(), mode: "full"}
    input = native_text_input("synthetic cut turn")
    {conn, websocket, ref} = public_websocket_connect!(client.port, setup, client.thread)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame(setup, client.thread, client.turn_id, input, %{}, %{}))
    original = cut!(:lost, :on, conn, websocket, ref, setup, upstream, release_ref)
    resend = frame(setup, client.thread, client.turn_id, input, @filled, %{})

    {busy, busy_logs} = with_info_log(fn -> resend!(:websocket, client, resend) end)
    assert %{"type" => "error"} = busy
    assert busy_logs =~ "reason_code=owner_busy"

    :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
    assert [%Request{status: "failed", last_error_code: "owner_unavailable"} = settled] = await_all_settled!(setup)
    assert settled.id == original.id

    {refused, refused_logs} = with_info_log(fn -> resend!(:websocket, client, resend) end)
    assert %{"type" => "error", "error" => %{"code" => "duplicate_turn"}} = refused
    assert refused_logs =~ "reason_code=terminal_predecessor"

    assert %{"type" => "response.completed"} = resend!(:https, client, resend)
    assert [_original, %Request{status: "succeeded", transport: "http_sse"}] = await_all_settled!(setup)
    assert FakeUpstream.count(upstream) == 2
  end

  # The resend policy (the turn claim's chain) records the predecessor on the
  # successor; with owner forwarding on, the owner's client-retry preflight
  # admits a websocket resend under its own `client-retry-v1:` claim. Both
  # link the successor to the request it repeats.
  defp assert_served_successor!(result, path) do
    assert %{"type" => "response.completed"} = result.outcome, "the resend was refused: #{inspect(result.outcome)} #{result.logs}"
    assert [original, successor] = later_requests(result)
    assert original.id == result.original.id
    assert {original.status, original.last_error_code} == {"failed", "client_disconnected"}
    assert successor.status == "succeeded"
    assert Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^result.original.id and l.successor_request_id == ^successor.id))

    case path do
      :resend_policy -> assert successor.request_metadata["client_resend"]["predecessor_request_id"] == result.original.id
      :owner_preflight -> assert String.starts_with?(successor.correlation_id, "client-retry-v1:")
    end

    assert FakeUpstream.count(result.upstream) == length(result.earlier) + 2
  end

  # The requests after the previous turn's, in admission order.
  defp later_requests(result) do
    earlier = Enum.map(result.earlier, & &1.id)
    Enum.reject(result.requests, &(&1.id in earlier))
  end

  defp scenario!(%{forwarding: forwarding, cut: cut, mode: mode, position: position, transport: transport, original: original_document, resend: resend_document}) do
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding == :on)
    release_ref = make_ref()
    upstream = start_upstream(upstream_script(cut, position, transport, release_ref))
    setup = gateway_setup(upstream)
    on_exit(fn -> for id <- Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id)), do: stop_websocket_owner_session(id) end)
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    port = start_public_endpoint!()
    client = %{port: port, setup: setup, thread: Ecto.UUID.generate(), turn_id: Ecto.UUID.generate(), mode: mode}
    {conn, websocket, ref} = public_websocket_connect!(port, setup, client.thread)
    {conn, websocket, cut_frame, full_history, earlier} = open_turn!(position, client, {conn, websocket, ref}, original_document)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, cut_frame)
    original = cut!(cut, forwarding, conn, websocket, ref, setup, upstream, release_ref)
    resend = frame(setup, client.thread, client.turn_id, full_history, resend_document, %{})
    {outcome, logs} = with_info_log(fn -> resend!(transport, client, resend) end)
    if cut == :lost, do: :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
    requests = if forwarding == :on and cut == :previsible and outcome["type"] == "error", do: all_requests(setup), else: await_all_settled!(setup)
    %{original: original, outcome: outcome, logs: logs, requests: requests, earlier: earlier, upstream: upstream, client: client, resend: resend}
  end

  # The cut request is the turn's first on its socket, or, when the previous
  # turn completed on this socket, the next turn's first request, which the
  # client sends anchored on that response with only the new item. Its full
  # history is then the previous turn's input, the response's item as the
  # client keeps it, and the new item.
  defp open_turn!(:first_turn, client, {conn, websocket, _ref}, document) do
    input = native_text_input("synthetic cut turn")
    {conn, websocket, frame(client.setup, client.thread, client.turn_id, input, document, %{}), input, []}
  end

  defp open_turn!(:anchored_turn, client, {conn, websocket, ref}, document) do
    first_input = native_text_input("synthetic first turn")
    input = native_text_input("synthetic cut turn")
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame(client.setup, client.thread, Ecto.UUID.generate(), first_input, document, %{}))
    {conn, websocket, first} = receive_terminal!(conn, websocket, ref)
    assert first["type"] == "response.completed"
    earlier = await_settled!(client.setup, 1)
    anchored = frame(client.setup, client.thread, client.turn_id, input, document, %{"previous_response_id" => "resp_async_first_turn"})
    {conn, websocket, anchored, first_input ++ [client_message("msg_async_first_turn")] ++ input, earlier}
  end

  defp resend!(:websocket, client, resend), do: resend_on_new_socket!(client.port, client.setup, client.thread, resend)
  defp resend!(:https, client, resend), do: resend_over_https!(client.setup, client.thread, client.mode, resend)

  # The previous turn's response (anchored position), then the cut request,
  # then the response to whatever is dispatched after the cut: the resend over
  # websocket or HTTPS, or the redeemed replay's generation.
  defp upstream_script(cut, position, transport, release_ref) do
    previous = if position == :anchored_turn, do: [native_request(1, [forbidden: ["previous_response_id"]], FakeUpstream.websocket_text_frames(completed_frames("resp_async_first_turn", [provider_message("msg_async_first_turn")])))], else: []
    anchor = if position == :anchored_turn, do: [equals: %{"previous_response_id" => "resp_async_first_turn"}], else: [forbidden: ["previous_response_id"]]

    held =
      case cut do
        :previsible -> FakeUpstream.websocket_close_without_terminal_barrier(notify: self(), release_ref: release_ref, code: 1001, reason: "synthetic pre-visible downstream loss")
        :delta -> FakeUpstream.barrier_websocket_frames(stream_frames("resp_async_cut_turn"), notify: self(), release_ref: release_ref)
        :lost -> FakeUpstream.barrier_websocket_frames(completed_frames("resp_async_cut_turn", []), notify: self(), release_ref: release_ref)
      end

    after_cut =
      case transport do
        :websocket -> FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: FakeUpstream.websocket_text_frames(completed_frames("resp_async_resend", [])))
        :https -> FakeUpstream.expect_request(method: "POST", path: @path, respond: FakeUpstream.sse_stream(Enum.map(completed_events("resp_async_resend", []), &{&1["type"], &1})))
      end

    # provenance: synthetic_adversarial; the previous turn's completion, the cut request held by its barrier, then the completion of what is dispatched after the cut
    FakeUpstream.strict_sequence(previous ++ [native_request(1, anchor, held), after_cut])
  end

  # Forwarding off, the client closes its socket and the closing cleanup stops
  # the turn; forwarding on, the owner suspends a turn cut before any output.
  # After partial output the client reads the delta first. A lost turn's socket
  # process dies without running its cleanup.
  defp cut!(:previsible, forwarding, conn, _websocket, _ref, setup, _upstream, release_ref) do
    assert_receive {:fake_upstream_websocket_barrier, :before_close, upstream_pid, ^release_ref}, @timeout_ms
    owner = if forwarding == :on, do: owner!(setup)
    _closed = Mint.HTTP.close(conn)

    request =
      if forwarding == :on do
        await_owner!(owner, &match?(%{active_turn: nil, suspended_replay: %{provisional_status: :armed}}, &1), "the owner never suspended the cut turn")
        latest_request(setup)
      else
        setup |> await_all_settled!() |> List.last()
      end

    send(upstream_pid, {:fake_upstream_release_websocket, release_ref})
    request
  end

  defp cut!(:delta, _forwarding, conn, websocket, ref, setup, upstream, release_ref) do
    for ordinal <- 0..(@delta_hold - 1) do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _handler, ^release_ref}, @timeout_ms
      :ok = FakeUpstream.release_frame(upstream, release_ref)
    end

    assert_receive {:fake_upstream_frame_barrier, @delta_hold, _handler, ^release_ref}, @timeout_ms
    conn = receive_until!(conn, websocket, ref, "response.output_text.delta")
    _closed = Mint.HTTP.close(conn)
    request = setup |> await_all_settled!() |> List.last()
    await_receipt!(request)
    :ok = FakeUpstream.release_remaining_frames(upstream, release_ref)
    request
  end

  defp cut!(:lost, :on, _conn, _websocket, _ref, setup, _upstream, release_ref) do
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^release_ref}, @timeout_ms
    owner = owner!(setup)
    socket_pid = :sys.get_state(owner).downstream.pid
    monitor = Process.monitor(socket_pid)
    Process.exit(socket_pid, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^socket_pid, :killed}, @timeout_ms
    await_owner!(owner, &match?(%{active_turn: %{descriptor: %{downstream_status: :lost}}}, &1), "the owner never marked the turn lost")
    latest_request(setup)
  end

  # The resend shape after partial output is judged on the cut request's
  # delivery receipt.
  defp await_receipt!(request, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms

    cond do
      Repo.exists?(from(a in Attempt, where: a.request_id == ^request.id and fragment("? \\? 'downstream_delivery'", a.response_metadata))) ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_receipt!(request, deadline)
        end

      true ->
        flunk("no delivery receipt for the cut request")
    end
  end

  defp resend_on_new_socket!(port, setup, thread, resend) do
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, resend)
    {conn, _websocket, outcome} = receive_terminal!(conn, websocket, ref)
    _closed = Mint.HTTP.close(conn)
    outcome
  end

  # The released client's HTTPS fallback of the websocket request: the same
  # body without `type`, the session named by its turn state, the Lite marker
  # as a header.
  defp resend_over_https!(setup, thread, mode, resend) do
    body = resend |> CodexPooler.JSON.decode!() |> Map.delete("type")

    conn =
      build_conn()
      |> auth(setup)
      |> put_req_header("x-codex-turn-state", thread)
      |> put_req_header("content-type", "application/json")

    conn = if mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    conn = post(conn, @path, CodexPooler.JSON.encode!(body))

    case conn.status do
      200 -> if conn.resp_body =~ ~s("type":"response.completed"), do: %{"type" => "response.completed"}, else: %{"type" => "http_200_without_terminal"}
      status -> Map.merge(CodexPooler.JSON.decode!(conn.resp_body), %{"type" => "error", "status" => status})
    end
  end

  # The released client's frame: the turn metadata document in
  # `client_metadata` beside its flat copies.
  defp frame(setup, thread, turn_id, input, document, extra) do
    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn_id, "request_kind" => "turn", "agent_name" => "/root"} |> Map.merge(document)

    %{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "input" => input,
      "stream" => true,
      "generate" => true,
      "client_metadata" => %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn_id, "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}
    }
    |> Map.merge(extra)
    |> CodexPooler.JSON.encode!()
  end

  defp native_request(ordinal, json, respond),
    do: FakeUpstream.expect_request(method: "WEBSOCKET", path: @path, websocket_connection_ordinal: ordinal, json: [valid: true, equals: Map.merge(%{"type" => "response.create"}, Keyword.get(json, :equals, %{}))] ++ Keyword.take(json, [:forbidden]), respond: respond)

  defp completed_frames(response_id, items), do: response_id |> completed_events(items) |> Enum.map(&CodexPooler.JSON.encode!/1)

  defp completed_events(response_id, items) do
    Enum.map(items, &%{"type" => "response.output_item.done", "output_index" => 0, "item" => &1}) ++
      [%{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => items, "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}]
  end

  # A message streamed delta by delta, then completed: held before the second
  # delta, the client saw output and no completed item.
  defp stream_frames(response_id) do
    item_id = "msg_" <> response_id
    item = provider_message(item_id)

    Enum.map(
      [
        %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.in_progress", "response" => %{"id" => response_id, "status" => "in_progress", "output" => []}},
        %{"type" => "response.output_item.added", "output_index" => 0, "item" => %{"id" => item_id, "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}},
        %{"type" => "response.content_part.added", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "part" => %{"type" => "output_text", "text" => "", "annotations" => []}},
        %{"type" => "response.output_text.delta", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "delta" => "synthetic "},
        %{"type" => "response.output_text.delta", "item_id" => item_id, "output_index" => 0, "content_index" => 0, "delta" => "answer"},
        %{"type" => "response.output_item.done", "output_index" => 0, "item" => item},
        %{"type" => "response.completed", "response" => %{"id" => response_id, "status" => "completed", "output" => [item], "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
      ],
      &CodexPooler.JSON.encode!/1
    )
  end

  defp provider_message(id), do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => "final_answer", "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}", "annotations" => [], "logprobs" => []}]}
  defp client_message(id), do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => "final_answer", "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}"}]}

  defp owner!(setup) do
    session_id = Repo.one!(from(s in CodexSession, where: s.pool_id == ^setup.pool.id, select: s.id))
    {:ok, owner} = WebsocketOwnerSession.lookup(session_id)
    owner
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

  defp receive_until!(conn, websocket, ref, type) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    if CodexPooler.JSON.decode!(text)["type"] == type, do: conn, else: receive_until!(conn, websocket, ref, type)
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, websocket, frame},
      else: receive_terminal!(conn, websocket, ref)
  end

  defp all_requests(setup), do: Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))
  defp latest_request(setup), do: setup |> all_requests() |> List.last()

  defp await_settled!(setup, count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms
    requests = all_requests(setup)

    cond do
      length(requests) == count and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_settled!(setup, count, deadline)
        end

      true ->
        flunk("the Pool's requests did not settle: #{inspect(Enum.map(requests, &{&1.status, &1.last_error_code}))}")
    end
  end

  defp await_all_settled!(setup, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms
    requests = all_requests(setup)

    cond do
      requests != [] and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_all_settled!(setup, deadline)
        end

      true ->
        flunk("the Pool's requests did not settle: #{inspect(Enum.map(requests, &{&1.status, &1.last_error_code}))}")
    end
  end
end
