defmodule CodexPoolerWeb.V1.ResponsesWebsocketFailedTurnVisibleOutputTest do
  # A websocket turn that fails after the client was shown output is logged
  # `visible_output=after_visible_output` on the public `/v1/responses` socket
  # and on the native one, with owner forwarding off and on: the socket
  # answers from what it pushed. With owner forwarding off the response task's
  # writer runs in the upstream websocket session's process, so a flag the
  # task keeps for itself never sees the output; with forwarding on the
  # owner's `:complete` clears the native pushed-output marker before the
  # task's error arrives (findings#271).
  #
  # One node; public `/v1/responses` and native `/backend-api/codex/responses`
  # websockets, owner forwarding off and on; the Pool's default serving mode,
  # FakeUpstream closing its connection after visible output without a
  # terminal, synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.FakeUpstream
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    :ok
  end

  for forwarding <- [:off, :on], {route, turn_state} <- [{"/v1/responses", ""}, {"/backend-api/codex/responses", "ws-visible-output-native"}] do
    @tag forwarding: forwarding, route: route, turn_state: turn_state
    test "a #{route} turn that fails after visible output logs after_visible_output with owner forwarding #{forwarding}", ctx do
      Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, ctx.forwarding == :on)

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial (the provider closes its connection after visible output, without a terminal)
          FakeUpstream.websocket_sse_then_close([
            %{"type" => "response.created", "response" => %{"id" => "resp_visible_then_close", "status" => "in_progress"}},
            %{"type" => "response.output_text.delta", "delta" => "partial visible output"}
          ])
        )

      setup = gateway_setup(upstream)
      {_server, port} = start_public_endpoint_with_server!()
      before = WebsocketCleanupFence.listener_sockets()
      {conn, websocket, ref} = public_websocket_connect!(port, setup, ctx.turn_state, ctx.route)
      socket = WebsocketCleanupFence.await_new_listener_socket!(before)
      payload = CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => native_text_input("visible then close"), "stream" => true, "generate" => true})

      {frames, log} =
        with_info_log(fn ->
          {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, payload)
          {conn, frames} = receive_until_terminal!(conn, websocket, ref, [])
          _state = await_socket_connection_state!(socket, &(MapSet.size(&1.tasks) == 0))
          Mint.HTTP.close(conn)
          :ok = WebsocketCleanupFence.await_listener_socket_cleanup!(socket)
          frames
        end)

      assert Enum.map(frames, & &1["type"]) == ["response.created", "response.output_text.delta", "error"]
      assert [line] = log |> String.split("\n") |> Enum.filter(&(&1 =~ "websocket native turn failed"))
      assert line =~ "visible_output=after_visible_output"
    end
  end

  defp receive_until_terminal!(conn, websocket, ref, frames) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)
    frames = frames ++ [frame]

    if frame["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, frames},
      else: receive_until_terminal!(conn, websocket, ref, frames)
  end
end
