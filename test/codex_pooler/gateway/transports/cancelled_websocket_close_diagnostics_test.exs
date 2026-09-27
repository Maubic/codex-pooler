defmodule CodexPooler.Gateway.Transports.CancelledWebsocketCloseDiagnosticsTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.CloseDiagnostics
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.ReceiveState

  setup do
    previous_level = Logger.level()
    on_exit(fn -> Logger.configure(level: previous_level) end)
    Logger.configure(level: :info)
    :ok
  end

  test "a cancelled connection logs bounded protocol progress and internal correlators only" do
    lifecycle_id = Ecto.UUID.generate()
    request_id = Ecto.UUID.generate()
    attempt_id = Ecto.UUID.generate()
    state = %{conn: :closed_connection, websocket: :established, lifecycle_id: lifecycle_id, generation: 3}

    receive_state = %ReceiveState{
      request_id: request_id,
      attempt_id: attempt_id,
      last_upstream_event_type: "response.output_item",
      last_upstream_event_class: "response_event",
      text_frame_count: 8,
      response_id: "resp_synthetic_private_identifier",
      body: {["synthetic private content"], 25}
    }

    log = capture_log([level: :info], fn -> CloseDiagnostics.log_cancelled_request_close(state, receive_state) end)

    assert log =~ "request_id=#{request_id} attempt_id=#{attempt_id}"
    assert log =~ "lifecycle_id=#{lifecycle_id} generation=3"
    assert log =~ "terminal_seen=false last_upstream_event_type=response.output_item"
    assert log =~ "text_frame_count=8"
    refute log =~ receive_state.response_id
    refute log =~ "synthetic private content"
  end

  test "malformed diagnostics cannot inject fields or retain provider text" do
    sentinel = "synthetic unsafe\nfield=value"
    state = %{conn: :closed_connection, websocket: :established, lifecycle_id: sentinel, generation: -1}
    receive_state = %ReceiveState{request_id: sentinel, attempt_id: sentinel, last_upstream_event_type: sentinel, last_upstream_event_class: sentinel, text_frame_count: -1}

    log = capture_log([level: :info], fn -> CloseDiagnostics.log_cancelled_request_close(state, receive_state) end)

    refute log =~ sentinel
    refute log =~ "field=value"
    assert log =~ "generation=none"
    assert log =~ "last_upstream_event_type=none last_upstream_event_class=none text_frame_count=none"
  end

  test "a cancellation without an established connection does not claim a socket close" do
    log =
      capture_log([level: :info], fn ->
        CloseDiagnostics.log_cancelled_request_close(%{generation: 0}, %ReceiveState{})
        CloseDiagnostics.log_cancelled_request_close(%{conn: :unupgraded_connection, generation: 0}, %ReceiveState{}, :connect)
      end)

    assert log == ""
  end
end
