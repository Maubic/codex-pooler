defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwarding.PostcompactionUserSteerTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [released_client_connect!: 4, model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport, only: [enter_peer_owner_topology!: 0, start_peer_window_owner!: 2]
  alias CodexPooler.Accounting.{Attempt, Request, RequestClientRetryLink}
  alias CodexPooler.Dev.NativeCompactionAuthorizationObserver
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Repo
  @moduletag capture_log: true
  # A pre-turn compaction creates no bare opener. The runtime-proof resume
  # claims codex-resume; later user input must reach ordinary claim admission
  # after its delivered reasoning is cut. All protocol inputs are synthetic.
  # HTTP controls use the router; websocket controls use the real listener.
  @budget 15_000

  for forwarding <- [true, false], shape <- [:mail, :user_mail, :user], transport <- [:websocket, :http], mode <- ["full", "lite"] do
    test "postcompact #{forwarding} #{shape} #{transport} #{mode}", %{conn: conn} do
      scenario(conn, unquote(forwarding), unquote(shape), unquote(transport), unquote(mode))
    end
  end

  for shape <- [:mail, :user_mail] do
    @tag slow: "starts a real peer BEAM owner and drives compaction, delivered reasoning cut and successor across nodes"
    test "postcompact peer #{shape} websocket full", %{conn: conn} do
      scenario(conn, :peer, unquote(shape), :websocket, "full")
    end
  end

  test "ordinary opener holder control", %{conn: conn} do
    scenario(conn, true, :user_mail, :websocket, "full", true)
  end

  for variant <- [:missing_position, :legacy_position, :equal_position, :trimmed_position, :changed_pivot, :changed_epoch, :changed_model, :missing_final_attempt] do
    test "unproved postcompaction progression stays refused: #{variant}", %{conn: conn} do
      scenario(conn, true, :user_mail, :websocket, "full", false, unquote(variant))
    end
  end

  for forwarding <- [true, :peer] do
    if forwarding == :peer, do: @tag(slow: "starts a real peer BEAM owner and races two public socket copies while its generation is held")

    test "#{forwarding}: concurrent copies of a postcompaction user steer dispatch once", %{conn: conn} do
      scenario(conn, unquote(forwarding), :user_mail, :websocket, "full", false, :concurrent)
    end
  end

  defp scenario(conn, forwarding, shape, transport, mode, ordinary_opener \\ false, variant \\ :normal) do
    context = fixture!(forwarding, shape, transport, mode, ordinary_opener, variant)
    predecessor = compact_resume_and_cut!(context)
    apply_position_control!(predecessor, variant)
    candidate = candidate(context)
    {outcome, log} = with_info_log(fn -> submit(conn, context, candidate) end)
    assert_outcome!(context, outcome, log)
  end

  defp fixture!(forwarding, shape, transport, mode, ordinary_opener, variant) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding != false)
    on_exit(fn -> NativeCompactionAuthorizationObserver.disarm() end)
    :ok = NativeCompactionAuthorizationObserver.arm()
    hold = make_ref()
    successor_hold = make_ref()
    compact_item = %{"type" => "compaction", "encrypted_content" => "synthetic-resume"}
    reasoning = %{"type" => "reasoning", "id" => "rs_synthetic", "summary" => [%{"type" => "summary_text", "text" => "synthetic reasoning"}], "encrypted_content" => "synthetic-reasoning"}
    upstream = scenario_upstream!(hold, compact_item, reasoning, transport, ordinary_opener, variant, successor_hold)
    if forwarding == :peer, do: enter_peer_owner_topology!()
    setup = gateway_setup(upstream, compact?: true)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    thread = Ecto.UUID.generate()
    peer = if forwarding == :peer, do: start_peer_window_owner!(setup, window_id(thread, 1))
    port = start_public_endpoint!()
    client = released_client_connect!(port, setup.authorization, thread, window_id(thread, 1))
    on_exit(fn -> Mint.HTTP.close(client.conn) end)
    %{setup: setup, upstream: upstream, port: port, client: client, thread: thread, turn: Ecto.UUID.generate(), peer: peer, hold: hold, successor_hold: successor_hold, compact_item: compact_item, reasoning: reasoning, mode: mode, shape: shape, transport: transport, ordinary_opener: ordinary_opener, variant: variant}
  end

  defp scenario_upstream!(hold, compact_item, reasoning, transport, ordinary_opener, variant, successor_hold) do
    prelude = if ordinary_opener, do: [request(completed_frames("resp_opener"))], else: []
    compact = FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true, forbidden: ["previous_response_id"]], respond: FakeUpstream.websocket_text_frames(encode_frames([%{"type" => "response.output_item.done", "item" => compact_item}, completed("resp_compact", [compact_item])])))

    resume =
      request(
        FakeUpstream.barrier_websocket_frames(
          encode_frames([
            %{"type" => "response.created", "response" => %{"id" => "resp_resume", "status" => "in_progress", "output" => []}},
            %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.put(reasoning, "summary", [])},
            %{"type" => "response.output_item.done", "output_index" => 0, "item" => reasoning},
            completed("resp_resume", [reasoning])
          ]),
          notify: self(),
          release_ref: hold
        )
      )

    successors =
      case variant do
        :normal -> [successor_request(transport)]
        :concurrent -> [request(FakeUpstream.barrier_websocket_frames(encode_frames([%{"type" => "response.created", "response" => %{"id" => "resp_successor", "status" => "in_progress"}}, completed("resp_successor", [])]), notify: self(), release_ref: successor_hold))]
        _negative -> []
      end

    start_upstream(FakeUpstream.strict_sequence(prelude ++ [compact, resume] ++ successors))
  end

  defp successor_request(:http), do: FakeUpstream.expect_request(method: "POST", respond: FakeUpstream.sse_stream([{"response.completed", completed("resp_successor", [])}]))
  defp successor_request(:websocket), do: request(completed_frames("resp_successor"))

  defp compact_resume_and_cut!(context) do
    %{client: client, setup: setup, thread: thread, turn: turn, hold: hold} = context
    client = opening_control!(client, context)
    compact_metadata = metadata(thread, turn, 1, :compaction) |> CodexPooler.JSON.decode!() |> put_in(["compaction", "phase"], if(context.ordinary_opener, do: "mid_turn", else: "pre_turn")) |> CodexPooler.JSON.encode!()
    compact = frame(setup, [user("anchor"), %{"type" => "compaction_trigger"}], nil, compact_metadata)
    {client, compact_result} = exchange(client, compact)
    assert compact_result["type"] == "response.completed"
    resume = frame(setup, resume_input(context), nil, metadata(thread, turn, 2, :turn))
    client = send_frame(client, resume)

    for ordinal <- 0..2 do
      assert_receive {:fake_upstream_frame_barrier, ^ordinal, _, ^hold}, @budget
      FakeUpstream.release_frame(context.upstream, hold)
    end

    assert_receive {:fake_upstream_frame_barrier, 3, _, ^hold}, @budget
    {client, done} = receive_until(client, ["response.output_item.done"])
    assert done["item"]["type"] == "reasoning"
    assert NativeCompactionAuthorizationObserver.captures()["counts"]["final_runtime_proof_redeemed"] == 1
    predecessor = assert_resume!(context)
    close_and_settle!(client, predecessor, context)
    Repo.reload!(predecessor)
  end

  defp opening_control!(client, %{ordinary_opener: false}), do: client

  defp opening_control!(client, context) do
    {client, result} = exchange(client, frame(context.setup, [user("anchor")], nil, metadata(context.thread, context.turn, 1, :turn)))
    assert result["type"] == "response.completed"
    client
  end

  defp assert_resume!(context) do
    predecessor = List.last(rows(context.setup.pool.id))
    assert length(rows(context.setup.pool.id)) == if(context.ordinary_opener, do: 3, else: 2)
    assert String.starts_with?(predecessor.correlation_id, "codex-resume:")
    assert predecessor.request_metadata["routing"]["model_serving_mode"] == context.mode

    if context.peer do
      forwarding = predecessor.request_metadata["websocket_owner_forwarding"]
      assert forwarding["owner_instance_id"] == Atom.to_string(context.peer.node)
      refute forwarding["owner_instance_id"] == forwarding["proxy_instance_id"]
    end

    predecessor
  end

  defp close_and_settle!(client, predecessor, context) do
    Mint.HTTP.close(client.conn)

    receipt =
      wait_for(fn ->
        case Repo.all(from a in Attempt, where: a.request_id == ^predecessor.id) do
          [%{response_metadata: %{"downstream_delivery" => receipt}}] -> receipt
          _ -> nil
        end
      end)

    wait_for(fn -> if Repo.get!(Request, predecessor.id).status not in ["accepted", "in_progress"], do: true end)
    FakeUpstream.release_remaining_frames(context.upstream, context.hold)
    assert %{"completed_items" => 1, "highest_frame_class" => "item_done", "terminal_class" => "none"} = receipt
  end

  defp resume_input(context), do: [user("anchor"), context.compact_item]

  defp candidate(context) do
    extra = if context.shape == :mail, do: [], else: [user("page context"), %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "synthetic developer context"}]}, user("actual steer")]
    mail = %{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic mail"}]}
    input = resume_input(context) ++ [context.reasoning] ++ extra ++ if(context.shape == :user, do: [], else: [mail])
    frame(context.setup, input, nil, metadata(context.thread, context.turn, 2, :turn))
  end

  defp submit(conn, %{variant: :concurrent} = context, candidate) do
    first = released_client_connect!(context.port, context.setup.authorization, context.thread, window_id(context.thread, 2))
    on_exit(fn -> Mint.HTTP.close(first.conn) end)
    first = send_frame(first, candidate)
    hold = context.successor_hold
    assert_receive {:fake_upstream_frame_barrier, 0, _, ^hold}, @budget
    duplicate = submit(conn, %{context | variant: :normal}, candidate)
    assert duplicate.code == "duplicate_turn"
    assert FakeUpstream.count(context.upstream) == 3
    FakeUpstream.release_remaining_frames(context.upstream, hold)
    {first, result} = receive_until(first, ["response.completed", "error"])
    assert_receive {:fake_upstream_frame_barrier, 2, _, ^hold}, @budget
    Mint.HTTP.close(first.conn)
    %{type: result["type"], code: get_in(result, ["error", "code"]), status: result["status"]}
  end

  defp submit(_conn, %{transport: :websocket} = context, candidate) do
    second = released_client_connect!(context.port, context.setup.authorization, context.thread, window_id(context.thread, 2))
    on_exit(fn -> Mint.HTTP.close(second.conn) end)
    {second, result} = exchange(second, candidate)
    Mint.HTTP.close(second.conn)
    %{type: result["type"], code: get_in(result, ["error", "code"]), status: result["status"]}
  end

  defp submit(conn, %{transport: :http} = context, candidate) do
    body = candidate |> CodexPooler.JSON.decode!() |> Map.delete("type")
    result = conn |> recycle() |> put_req_header("authorization", context.setup.authorization) |> put_req_header("session-id", context.thread) |> put_req_header("thread-id", context.thread) |> put_req_header("x-codex-window-id", window_id(context.thread, 2)) |> put_req_header("x-codex-turn-metadata", metadata(context.thread, context.turn, 2, :turn)) |> put_req_header("originator", "codex_cli_rs") |> post("/backend-api/codex/responses", body)
    %{status: result.status, type: if(result.status == 200, do: "response.completed", else: "error")}
  end

  defp assert_outcome!(context, outcome, log) do
    wait_for(fn -> if Enum.all?(rows(context.setup.pool.id), &(&1.status not in ["accepted", "in_progress"])), do: true end)
    final_rows = rows(context.setup.pool.id)
    row_ids = Enum.map(final_rows, & &1.id)
    links = Repo.all(from l in RequestClientRetryLink, where: l.predecessor_request_id in ^row_ids, select: {l.predecessor_request_id, l.successor_request_id})
    assert outcome.type == if(context.variant in [:normal, :concurrent], do: "response.completed", else: "error")
    assert FakeUpstream.count(context.upstream) == if(context.variant in [:normal, :concurrent], do: 3, else: 2) + if(context.ordinary_opener, do: 1, else: 0)
    if context.ordinary_opener, do: assert(log =~ "steered turn claim rebound")
    if context.variant not in [:normal, :concurrent], do: assert(outcome.code == "duplicate_turn")
    if context.shape == :mail, do: assert(length(links) == 1)

    assert_user_claim!(context, final_rows, links)
    :ok = FakeUpstream.acknowledge(context.upstream, {:frame_barrier, context.hold, 4})
    assert :ok = FakeUpstream.verify!(context.upstream)
  end

  defp assert_user_claim!(context, final_rows, links) do
    if context.variant in [:normal, :concurrent] and context.shape != :mail and not context.ordinary_opener do
      assert List.last(final_rows).status == "succeeded"
      assert String.starts_with?(List.last(final_rows).correlation_id, "codex-turn:")
      assert links == []
    end
  end

  # These controls emulate rows written without the current positional witness
  # or under a different scope; the compaction, resume and delivery remain real.
  defp apply_position_control!(_request, variant) when variant in [:normal, :concurrent], do: :ok

  defp apply_position_control!(request, :missing_final_attempt) do
    Repo.get_by!(CodexTurn, request_id: request.id) |> Ecto.Changeset.change(final_attempt_id: nil) |> Repo.update!()
  end

  defp apply_position_control!(request, :changed_epoch), do: request |> Ecto.Changeset.change(native_client_retry_auth_epoch: request.native_client_retry_auth_epoch + 1) |> Repo.update!()
  defp apply_position_control!(request, :changed_model), do: request |> Ecto.Changeset.change(requested_model: "synthetic-other-model") |> Repo.update!()

  defp apply_position_control!(request, variant) do
    metadata =
      case variant do
        :missing_position -> Map.delete(request.request_metadata, "native_turn_progress")
        :legacy_position -> update_in(request.request_metadata, ["native_turn_progress"], &Map.take(&1, ["version", "digest"]))
        :equal_position -> put_in(request.request_metadata, ["native_turn_progress", "user_messages"], 2)
        :trimmed_position -> put_in(request.request_metadata, ["native_turn_progress", "user_messages"], 3)
        :changed_pivot -> put_in(request.request_metadata, ["native_turn_progress", "pivot"], Base.url_encode64(:crypto.hash(:sha256, "other-pivot"), padding: false))
      end

    request |> Ecto.Changeset.change(request_metadata: metadata) |> Repo.update!()
  end

  defp wait_for(fun), do: wait_for(fun, System.monotonic_time(:millisecond) + @budget)

  defp wait_for(fun, deadline) do
    case fun.() do
      nil ->
        assert System.monotonic_time(:millisecond) < deadline, "bounded state observation expired"

        receive do
        after
          25 -> :ok
        end

        wait_for(fun, deadline)

      value ->
        value
    end
  end

  defp rows(pool_id), do: Repo.all(from r in Request, where: r.pool_id == ^pool_id, order_by: [asc: r.admitted_at, asc: r.id])
  defp request(respond), do: FakeUpstream.expect_request(method: "WEBSOCKET", json: [valid: true], respond: respond)
  defp user(text), do: %{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic " <> text}]}
  defp completed(id, output), do: %{"type" => "response.completed", "response" => %{"id" => id, "status" => "completed", "output" => output, "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}}}
  defp completed_frames(id), do: FakeUpstream.websocket_text_frames(encode_frames([%{"type" => "response.created", "response" => %{"id" => id, "status" => "in_progress"}}, completed(id, [])]))
  defp encode_frames(frames), do: Enum.map(frames, &CodexPooler.JSON.encode!/1)

  defp send_frame(client, payload) do
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, payload)
    %{client | conn: conn, websocket: websocket}
  end

  defp exchange(client, payload), do: client |> send_frame(payload) |> receive_until(["response.completed", "response.failed", "response.incomplete", "error"])

  defp receive_until(client, types) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    client = %{client | conn: conn, websocket: websocket}
    decoded = CodexPooler.JSON.decode!(text)
    if decoded["type"] in types, do: {client, decoded}, else: receive_until(client, types)
  end

  defp frame(setup, input, previous, metadata) do
    %{"type" => "response.create", "model" => setup.model.exposed_model_id, "stream" => true, "input" => input, "client_metadata" => %{"x-codex-turn-metadata" => metadata}}
    |> then(&if previous, do: Map.put(&1, "previous_response_id", previous), else: &1)
    |> CodexPooler.JSON.encode!()
  end

  defp window_id(thread, number), do: "#{thread}:#{number}"

  defp metadata(thread, turn, number, kind) do
    %{"turn_id" => turn, "thread_id" => thread, "agent_name" => "/root", "window_id" => window_id(thread, number), "context_window_id" => "00000000-0000-4000-8000-00000000#{number}b01", "window_number" => number, "request_kind" => Atom.to_string(kind)}
    |> then(fn doc -> if kind == :compaction, do: Map.put(doc, "compaction", %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "mid_turn", "strategy" => "memento"}), else: doc end)
    |> CodexPooler.JSON.encode!()
  end
end
