defmodule CodexPooler.Platform.ExecutionProofShutdownTest do
  # findings#270 row 270-371: a rollout drain cuts a response task, which ends
  # `process_down`, and a client's resend of its turn on the other node is
  # admitted as the cut request's successor only once that execution's proof
  # exists. The registry asks the publisher to write it 100 ms later, but the
  # VM exits right after the drain: on two replicas the socket node's VM was
  # gone 53 to 106 ms after its drain, with no proof written, and the resend
  # met `409 duplicate_turn` until the HTTPS fallback bought the compaction
  # again. The in-VM drain arms could not see this: their node stays up after
  # the drain, and they publish the proof themselves. Here a child VM runs the
  # application, its drain cuts a response task at `init:stop/0` (what SIGTERM
  # runs), and the proof must be in the database once that VM has exited.
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1, run_unboxed: 1]
  import Ecto.Query

  alias CodexPooler.Platform.ExecutionTerminalProof
  alias CodexPooler.Repo

  # The child VM plays the socket: its response task runs an execution until
  # the shutdown drain cuts it, and the drain's `owner_drained` error is not
  # delivered (the receipt is `aborted`), as when the client's connection is
  # the one the drain closes.
  @probe ~S"""
  alias CodexPooler.Gateway.Websocket.ResponseTask
  alias CodexPooler.Platform.ExecutionIdentity

  Application.put_env(:codex_pooler, CodexPooler.Platform.ExecutionProofPublisher, enabled: true)
  {:ok, _apps} = Application.ensure_all_started(:codex_pooler)
  socket = self()

  {:ok, _task} =
    ResponseTask.start(
      socket,
      :proxy,
      fn _coordinator ->
        send(socket, {:executing, ExecutionIdentity.local()})

        receive do
        end
      end,
      fn _coordinator, :owner_drained -> :ok end
    )

  receive do
    {:executing, execution} ->
      IO.puts("execution owner_execution_id=#{execution.owner_execution_id} owner_process_id=#{execution.owner_process_id}")
  after
    15_000 -> raise "the response task never ran"
  end

  :ok = :init.stop()

  receive do
    {:websocket_response_activity_cancelled, _coordinator, token, watcher, :owner_drained} ->
      ResponseTask.acknowledge_delivery(watcher, token, :aborted)
  after
    15_000 -> raise "the shutdown drain never cut the response task"
  end

  Process.sleep(:infinity)
  """

  @tag slow: "boots a child VM with the whole application and stops it: the property is what that VM writes before it exits"
  test "the proof of an execution the shutdown drain cut is written before the VM exits" do
    {output, exit_code} = System.cmd("mix", ["run", "--no-compile", "--no-start", "-e", @probe], env: [{"MIX_ENV", "test"}], stderr_to_stdout: true)

    assert exit_code == 0, output
    assert [_line, execution_id, owner_process_id] = Regex.run(~r/execution owner_execution_id=([0-9a-f-]{36}) owner_process_id=(<[0-9.]+>)/, output), output
    register_unboxed_cleanup!(fn -> Repo.delete_all(from proof in ExecutionTerminalProof, where: proof.execution_id == ^execution_id) end)

    assert %ExecutionTerminalProof{end_kind: "process_down", owner_process_id: ^owner_process_id} =
             run_unboxed(fn -> Repo.get(ExecutionTerminalProof, execution_id) end)
  end
end
