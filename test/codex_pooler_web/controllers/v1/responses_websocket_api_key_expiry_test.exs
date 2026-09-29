defmodule CodexPoolerWeb.V1.ResponsesWebsocketApiKeyExpiryTest do
  # An SDK client on the public `GET /v1/responses` websocket whose API key
  # expires while its turn is in flight gets the turn's terminal and then the
  # `1008` close, whatever the order in which the socket takes the expiry check
  # and the turn's messages (findings#270 row 270-276): while the provider still
  # holds its answer, with the whole answer queued behind the check, with the
  # check queued behind the whole answer, and between the terminal and the
  # response task's result. Revocation drops queued client submissions
  # (`drop_queued_responses/1`), never a turn's frames, and closes once the
  # public turn is over; the turn settles `succeeded` with the provider's usage.
  #
  # Determinism: the provider holds its answer at a barrier; the check is the
  # socket's own timer message with its current token, sent while the socket
  # is suspended (so the order in its mailbox is the test's) or while the
  # response task is held before it hands its result over.
  #
  # Topology: the real public listener, FakeUpstream, the Pool's default mode
  # (Full); owner forwarding off (direct task), on with the session's owner on
  # this node, and on with the owner on a second VM (slow-tagged).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      await_public_websocket_upgrade: 2,
      gateway_setup: 1,
      mint_websocket_new!: 4,
      public_websocket_send_text!: 4,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [await_socket_connection_state!: 2]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_peer_session_owner!: 2]

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.Request
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  @frame_timeout_ms 15_000
  @response_id "resp_public_ws_expiry"
  @terminal_then_close ["response.created", "response.output_item.added", "response.output_text.delta", "response.output_item.done", "response.completed", {:close, 1008, "api key is no longer active"}]

  for topology <- [:direct, :local_owner, :remote_owner], order <- [:while_provider_holds, :queued_before_frames, :queued_after_frames, :between_terminal_and_done] do
    if topology == :remote_owner, do: @tag(slow: "boots a second VM that owns the session and shares the committed database")

    test "#{topology}: an expiry check #{order |> Atom.to_string() |> String.replace("_", " ")} still delivers the terminal before the 1008 close" do
      run_scenario!(unquote(topology), unquote(order))
    end
  end

  defp run_scenario!(topology, order) do
    put_owner_forwarding!(topology != :direct)
    if topology == :remote_owner, do: enter_peer_owner_topology!()
    hold = make_ref()
    events = Enum.map(upstream_events(), &CodexPooler.JSON.encode!/1)
    upstream = start_upstream(FakeUpstream.barrier_websocket_frames(events, notify: self(), release_ref: hold))
    setup = gateway_setup(upstream)
    turn_state = "public-ws-expiry-#{System.unique_integer([:positive])}"
    if topology == :remote_owner, do: start_peer_session_owner!(setup, %{accepted_turn_state: turn_state})
    port = start_public_endpoint!()
    before = WebsocketCleanupFence.listener_sockets()
    {conn, websocket, ref} = public_v1_websocket_connect!(port, setup, turn_state)
    socket = WebsocketCleanupFence.await_new_listener_socket!(before)
    gate = if order == :between_terminal_and_done, do: install_completion_gate!(socket, topology)

    payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => "synthetic public expiry turn", "stream" => true})
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, payload)
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^hold}, @frame_timeout_ms

    %{api_key_expiry_check: %{token: token}} = await_socket_connection_state!(socket, &is_map(Map.get(&1, :api_key_expiry_check)))
    expire!(setup.api_key)

    case order do
      :while_provider_holds ->
        send(socket, {:api_key_expiry_check, token})
        assert %{public_response_task_pid: task} = await_socket_connection_state!(socket, &Map.get(&1, :api_key_revoked?, false))
        assert is_pid(task)
        :ok = FakeUpstream.release_remaining_frames(upstream, hold)

      :queued_before_frames ->
        true = :erlang.suspend_process(socket)
        send(socket, {:api_key_expiry_check, token})
        :ok = FakeUpstream.release_remaining_frames(upstream, hold)
        :ok = await_queued!(socket, topology)
        true = :erlang.resume_process(socket)

      :queued_after_frames ->
        true = :erlang.suspend_process(socket)
        :ok = FakeUpstream.release_remaining_frames(upstream, hold)
        :ok = await_queued!(socket, topology)
        send(socket, {:api_key_expiry_check, token})
        true = :erlang.resume_process(socket)

      :between_terminal_and_done ->
        :ok = FakeUpstream.release_remaining_frames(upstream, hold)
        assert_receive {:completion_gate, task}, @frame_timeout_ms
        send(socket, {:api_key_expiry_check, token})
        assert %{public_response_task_pid: ^task} = await_socket_connection_state!(socket, &Map.get(&1, :api_key_revoked?, false))
        send(task, {:release_completion, gate})
    end

    assert conn |> receive_frames_until_close(websocket, ref, []) |> Enum.map(&frame_summary/1) == @terminal_then_close
    assert [{"succeeded", nil, "usage_known"}] = await_settled_rows!(setup)
  end

  defp await_settled_rows!(setup) do
    deadline = System.monotonic_time(:millisecond) + @frame_timeout_ms

    Stream.repeatedly(fn -> Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, select: {r.status, r.last_error_code, r.usage_status})) end)
    |> Enum.find(fn rows ->
      cond do
        rows != [] and Enum.all?(rows, &(elem(&1, 0) not in ["accepted", "in_progress"])) -> true
        System.monotonic_time(:millisecond) > deadline -> flunk("the turn never settled: #{inspect(rows)}")
        true -> Process.sleep(10) && false
      end
    end)
  end

  defp install_completion_gate!(socket, topology) do
    test = self()
    ref = make_ref()

    hold = fn ->
      send(test, {:completion_gate, self()})

      receive do
        {:release_completion, ^ref} -> :ok
      after
        @frame_timeout_ms -> raise "completion gate was not released"
      end
    end

    option =
      case topology do
        :local_owner -> [before_local_completion_handoff: hold]
        :direct -> [before_completion_handoff: fn _token, _watcher -> hold.() end]
        :remote_owner -> [before_local_completion_handoff: hold, before_completion_handoff: fn _token, _watcher -> hold.() end]
      end

    :sys.replace_state(socket, fn {state_name, data} ->
      state = Map.put(data.connection.websock_state, :response_task_start_options, option)
      {state_name, %{data | connection: %{data.connection | websock_state: state}}}
    end)

    ref
  end

  defp frame_summary({:text, text}), do: CodexPooler.JSON.decode!(text)["type"]
  defp frame_summary(other), do: other

  defp await_queued!(socket, topology) do
    deadline = System.monotonic_time(:millisecond) + @frame_timeout_ms

    {:messages, _messages} =
      Stream.repeatedly(fn -> Process.info(socket, :messages) end)
      |> Enum.find(fn {:messages, messages} ->
        done? =
          Enum.any?(messages, fn
            {:codex_response_done, _pid, _result} -> true
            {:websocket_owner_frame, _cid, _epoch, _turn, :complete} -> topology == :local_owner
            _message -> false
          end)

        cond do
          done? -> true
          System.monotonic_time(:millisecond) > deadline -> flunk("nothing queued: #{inspect(Enum.map(messages, &elem(&1, 0)))}")
          true -> Process.sleep(5) && false
        end
      end)

    :ok
  end

  defp expire!(api_key) do
    past = DateTime.add(DateTime.utc_now(), -60, :second)
    {1, _rows} = Repo.update_all(from(key in APIKey, where: key.id == ^api_key.id), set: [expires_at: past])
  end

  defp receive_frames_until_close(conn, websocket, ref, frames) do
    receive do
      message ->
        case Mint.WebSocket.stream(conn, message) do
          {:ok, conn, responses} ->
            {websocket, new} =
              Enum.reduce(responses, {websocket, []}, fn
                {:data, ^ref, data}, {websocket, acc} ->
                  {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
                  {websocket, acc ++ decoded}

                _part, acc ->
                  acc
              end)

            frames = frames ++ new

            if Enum.any?(new, &match?({:close, _, _}, &1)),
              do: frames,
              else: receive_frames_until_close(conn, websocket, ref, frames)

          {:error, _conn, reason, _responses} ->
            frames ++ [{:stream_error, reason}]

          :unknown ->
            receive_frames_until_close(conn, websocket, ref, frames)
        end
    after
      3_000 -> frames ++ [:timeout]
    end
  end

  defp upstream_events do
    item = %{"id" => "msg_public_ws_expiry", "type" => "message", "role" => "assistant", "status" => "in_progress", "content" => []}
    part = %{"type" => "output_text", "text" => "", "annotations" => []}
    done_part = %{part | "text" => "synthetic expiry answer"}
    done_item = %{item | "status" => "completed", "content" => [done_part]}
    address = %{"item_id" => "msg_public_ws_expiry", "output_index" => 0, "content_index" => 0}

    [
      %{"type" => "response.created", "response" => response_body("in_progress", [])},
      %{"type" => "response.output_item.added", "output_index" => 0, "item" => item},
      Map.merge(address, %{"type" => "response.output_text.delta", "delta" => "synthetic expiry answer"}),
      %{"type" => "response.output_item.done", "output_index" => 0, "item" => done_item},
      %{"type" => "response.completed", "response" => response_body("completed", [done_item])}
    ]
  end

  defp response_body(status, output) do
    %{
      "id" => @response_id,
      "object" => "response",
      "created_at" => 1_790_000_000,
      "model" => "provider-gpt-test-model",
      "status" => status,
      "output" => output,
      "usage" => %{"input_tokens" => 3, "output_tokens" => 2, "total_tokens" => 5}
    }
  end

  defp public_v1_websocket_connect!(port, setup, turn_state) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers = [
      {"authorization", setup.authorization},
      {"x-codex-turn-state", turn_state},
      {"openai-beta", "responses_websockets=2026-02-06"}
    ]

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    {conn, websocket, ref}
  end

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end
end
