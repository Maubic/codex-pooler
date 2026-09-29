defmodule CodexPoolerWeb.CodexResponsesSocketOwnerErrorAfterTerminalTest do
  # An owner-forwarded native turn reaches the client with at most one
  # terminal. The owner relays its error for a turn when it settles the turn;
  # once the turn's terminal went out, that error is logged and not sent, as
  # the socket's own error for a task failure after the terminal is not
  # (findings#270 rows 270-325 and 270-345): the released client reads a frame
  # that arrives while it is idle as the failure of its next request. No driven
  # path relays an owner error after a pushed terminal today, so the branch is
  # pinned here. The owner's frames reach the socket as the messages the owner
  # sends, the terminal first and the error after it, on a native socket whose
  # one tracked task is the owner-forwarded turn.
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Transports.Websocket.{ActivityRegistry, WebsocketOwnerContract}
  alias CodexPoolerWeb.CodexResponsesSocket

  @correlation_id "corr-owner-error-after-terminal"
  @epoch 7

  setup do
    registry = start_supervised!({ActivityRegistry, name: nil})
    task_pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> send(task_pid, :stop) end)
    %{task_pid: task_pid, state: native_owner_state(task_pid, registry)}
  end

  for terminal <- ["response.completed", "response.failed"] do
    @tag terminal: terminal
    test "an owner error after the turn's #{terminal} went out sends nothing more", %{task_pid: task_pid, state: state, terminal: terminal} do
      {:push, {:text, pushed}, state} = owner_frame(state, task_pid, {:data, terminal_frame(terminal)})
      assert CodexPooler.JSON.decode!(pushed)["type"] == terminal

      {result, log} = with_info_log(fn -> owner_frame(state, task_pid, owner_error()) end)

      assert {:ok, _state} = result
      assert log =~ "websocket turn error not sent after its terminal"
      assert log =~ "terminal_class=#{terminal} error_code=upstream_stream_error"
    end
  end

  test "an owner error before any terminal of the turn is the turn's terminal", %{task_pid: task_pid, state: state} do
    {:push, {:text, _delta}, state} = owner_frame(state, task_pid, {:data, ~s({"type":"response.output_text.delta","delta":"synthetic"})})

    assert {:push, {:text, error}, state} = owner_frame(state, task_pid, owner_error())
    assert %{"type" => "error", "status" => 502} = CodexPooler.JSON.decode!(error)

    # A second owner error for the same turn: the first one won.
    assert {:ok, _state} = owner_frame(state, task_pid, owner_error())
  end

  defp owner_frame(state, task_pid, payload),
    do: CodexResponsesSocket.handle_info({:websocket_owner_frame, @correlation_id, @epoch, task_pid, payload}, state)

  defp owner_error do
    {:ok, payload} = WebsocketOwnerContract.safe_error_payload(:upstream_stream_error, nil)
    {:error, :upstream_stream_error, payload}
  end

  defp terminal_frame("response.completed"),
    do: CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_owner_error_after_terminal", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 1, "output_tokens" => 1, "total_tokens" => 2}}})

  defp terminal_frame("response.failed"),
    do: CodexPooler.JSON.encode!(%{"type" => "response.failed", "response" => %{"id" => "resp_owner_error_after_terminal", "status" => "failed", "error" => %{"code" => "server_error", "message" => "synthetic provider failure"}}})

  defp native_owner_state(task_pid, registry) do
    %{
      opts: RequestOptions.for_websocket(%{}),
      tasks: MapSet.new([task_pid]),
      task_monitors: %{},
      queued_response_payloads: :queue.new(),
      response_task_activity_registry: registry,
      websocket_owner_downstream: %{pid: self(), epoch: @epoch, correlation_id: @correlation_id, active_turn_reconnect?: false}
    }
  end

  # The socket notes the error it did not send at `info`; the suite logs at
  # `warning`, where that line would not even be built.
  defp with_info_log(fun) do
    previous_level = Logger.level()
    on_exit(fn -> Logger.configure(level: previous_level) end)
    Logger.configure(level: :info)

    try do
      ExUnit.CaptureLog.with_log([level: :info], fun)
    after
      Logger.configure(level: previous_level)
    end
  end
end
