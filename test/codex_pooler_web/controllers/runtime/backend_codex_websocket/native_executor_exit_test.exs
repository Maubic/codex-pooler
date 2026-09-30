defmodule CodexPoolerWeb.Runtime.NativeExecutorExitTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import Ecto.Query

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Repo

  @moduletag capture_log: true

  test "an active native executor death closes the real client socket instead of leaving its delivery handshake live" do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, false)
    barrier = make_ref()

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            path: "/backend-api/codex/responses",
            respond:
              FakeUpstream.barrier_websocket_frames(
                [
                  CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_native_executor_exit", "status" => "completed", "output" => []}})
                ],
                notify: self(),
                release_ref: barrier
              )
          )
        ])
      )

    setup = gateway_setup(upstream)
    {_server, port} = start_public_endpoint_with_server!()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, Ecto.UUID.generate())
    payload = %{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => [], "stream" => true, "store" => false, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => Ecto.UUID.generate(), "thread_id" => Ecto.UUID.generate(), "turn_id" => Ecto.UUID.generate(), "request_kind" => "turn"})}}
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
    assert_receive {:fake_upstream_frame_barrier, 0, _handler, ^barrier}, 15_000
    request = Repo.one!(from r in Request, where: r.pool_id == ^setup.pool.id and r.status == "in_progress")
    attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id)
    assert :alive = ExecutionIdentity.status(attempt)
    executor = attempt.owner_process_id |> String.to_charlist() |> :erlang.list_to_pid()
    monitor = Process.monitor(executor)
    Process.exit(executor, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^executor, :killed}, 15_000
    {conn, _websocket, code, reason} = public_websocket_receive_close!(conn, websocket, ref, 5_000)
    assert code == 1011
    assert reason == "websocket response task failed"
    Mint.HTTP.close(conn)
    assert FakeUpstream.count(upstream) == 1
  end
end
