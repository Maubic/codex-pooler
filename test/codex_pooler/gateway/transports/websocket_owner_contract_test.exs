defmodule CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerContractTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.Websocket.OwnerDefaults
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.CloseDiagnostics
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerContract

  describe "reconnect handoff controls" do
    test "accepts only the matching downstream, owner turn, and opaque control ref" do
      owner_turn_id =
        spawn(fn ->
          receive do
            :stop -> :ok
          end
        end)

      downstream_pid = self()
      control_ref = make_ref()

      ready =
        {:websocket_owner_handoff_ready, "corr-control", 3, owner_turn_id, downstream_pid, control_ref}

      failed =
        {:websocket_owner_handoff_failed, "corr-control", 3, owner_turn_id, downstream_pid, control_ref, :owner_drained}

      assert WebsocketOwnerContract.accept_handoff_message(
               ready,
               downstream_pid,
               3,
               "corr-control",
               owner_turn_id,
               control_ref
             ) == {:ok, :ready}

      assert WebsocketOwnerContract.accept_handoff_message(
               failed,
               downstream_pid,
               3,
               "corr-control",
               owner_turn_id,
               control_ref
             ) == {:ok, {:failed, :owner_drained}}

      for stale <- [
            put_elem(ready, 1, "stale-correlation"),
            put_elem(ready, 2, 4),
            put_elem(ready, 3, self()),
            put_elem(ready, 4, owner_turn_id),
            put_elem(ready, 5, make_ref())
          ] do
        assert WebsocketOwnerContract.accept_handoff_message(
                 stale,
                 downstream_pid,
                 3,
                 "corr-control",
                 owner_turn_id,
                 control_ref
               ) == :drop
      end

      refute inspect(ready) =~ "semantic"
      send(owner_turn_id, :stop)
    end
  end

  @sentinel "SECRET_SENTINEL_DO_NOT_STORE_123"
  @required_errors [
    :owner_unavailable,
    :stale_owner,
    :owner_forward_timeout,
    :owner_crashed,
    :owner_drained,
    :duplicate_downstream,
    :stale_downstream,
    :owner_forwarding_disabled,
    :owner_busy,
    :client_disconnected,
    :upstream_stream_error,
    :upstream_websocket_terminal_delivery_timeout
  ]

  describe "owner error taxonomy" do
    test "recognizes only the required owner error atoms" do
      assert WebsocketOwnerContract.owner_errors() == @required_errors

      for error <- @required_errors do
        assert WebsocketOwnerContract.owner_error?(error)
      end

      refute WebsocketOwnerContract.owner_error?(:unknown_owner_error)
      refute WebsocketOwnerContract.owner_error?("owner_busy")
    end

    test "maps owner_busy to the exact backpressure contract" do
      assert {:ok, payload} = WebsocketOwnerContract.safe_error_payload(:owner_busy, @sentinel)

      assert payload.status == 409
      assert payload.code == "owner_busy"
      assert payload.request_status == "failed"
      assert payload.attempt_status == "failed"
      assert payload.metadata.reason == "owner_busy_backpressure"
      assert payload.metadata.owner_error == "owner_busy"
      refute inspect(payload) =~ @sentinel
    end

    test "maps owner_forward_timeout to the exact timeout contract" do
      assert {:ok, payload} =
               WebsocketOwnerContract.safe_error_payload(:owner_forward_timeout, @sentinel)

      assert payload.status == 504
      assert payload.code == "owner_forward_timeout"
      assert payload.request_status == "failed"
      assert payload.attempt_status == "failed"
      assert payload.metadata.reason == "owner_forward_timeout"
      assert payload.metadata.owner_error == "owner_forward_timeout"
      refute inspect(payload) =~ @sentinel
    end

    test "maps client_disconnected to the exact downstream close contract" do
      assert {:ok, payload} =
               WebsocketOwnerContract.safe_error_payload(:client_disconnected, @sentinel)

      assert payload.status == 499
      assert payload.code == "client_disconnected"
      assert payload.request_status == "failed"
      assert payload.attempt_status == "failed"
      assert payload.metadata.reason == "client_disconnected"
      assert payload.metadata.owner_error == "client_disconnected"
      refute inspect(payload) =~ @sentinel
    end

    test "maps terminal delivery timeout to the committed stream failure contract" do
      assert {:ok, payload} =
               WebsocketOwnerContract.safe_error_payload(
                 :upstream_websocket_terminal_delivery_timeout,
                 @sentinel
               )

      assert payload.status == 502
      assert payload.code == "upstream_stream_error"
      assert payload.request_status == "failed"
      assert payload.attempt_status == "failed"
      assert payload.metadata.reason == "upstream_websocket_terminal_delivery_timeout"
      assert payload.metadata.owner_error == "upstream_stream_error"
      refute inspect(payload) =~ @sentinel
    end

    test "maps upstream stream interruption to the public websocket failure contract" do
      assert {:ok, payload} =
               WebsocketOwnerContract.safe_error_payload(:upstream_stream_error, @sentinel)

      assert payload.status == 502
      assert payload.code == "server_error"

      assert payload.message ==
               "upstream request failed: stream interrupted before terminal response event"

      assert payload.request_status == "failed"
      assert payload.attempt_status == "failed"
      assert payload.metadata.reason == "upstream_stream_error"
      assert payload.metadata.owner_error == "server_error"
      refute inspect(payload) =~ @sentinel
    end

    test "maps owner topology errors to deterministic safe 502, 503, and 409 classes" do
      expected = %{
        owner_unavailable: {503, "owner_unavailable", "owner_unavailable"},
        owner_crashed: {502, "owner_crashed", "owner_crashed"},
        owner_drained: {503, "owner_drained", "owner_drained"},
        stale_owner: {409, "stale_owner", "stale_owner"},
        duplicate_downstream: {409, "duplicate_downstream", "duplicate_downstream"},
        stale_downstream: {409, "stale_downstream", "stale_downstream"},
        owner_forwarding_disabled: {503, "owner_forwarding_disabled", "owner_forwarding_disabled"}
      }

      for {error, {status, code, reason}} <- expected do
        assert {:ok, payload} = WebsocketOwnerContract.safe_error_payload(error, @sentinel)
        assert payload.status == status
        assert payload.code == code
        assert payload.request_status == "failed"
        assert payload.attempt_status == "failed"
        assert payload.metadata.reason == reason
        assert payload.metadata.owner_error == code
        refute inspect(payload) =~ @sentinel
      end
    end

    test "rejects unknown owner errors deterministically without leaking context" do
      assert WebsocketOwnerContract.safe_error_payload(:not_a_known_owner_error, @sentinel) ==
               {:error, :unknown_owner_error}

      refute inspect({:error, :unknown_owner_error}) =~ @sentinel
    end
  end

  describe "timeout defaults" do
    test "exposes bounded positive timeout defaults" do
      assert OwnerDefaults.forward_timeout_ms() ==
               WebsocketOwnerContract.default_forward_timeout_ms()

      assert OwnerDefaults.owner_call_timeout_ms() ==
               WebsocketOwnerContract.default_owner_call_timeout_ms()

      assert OwnerDefaults.downstream_send_timeout_ms() ==
               WebsocketOwnerContract.default_downstream_send_timeout_ms()

      assert WebsocketOwnerContract.default_forward_timeout_ms() == 5_000
      assert WebsocketOwnerContract.default_owner_call_timeout_ms() == 5_000
      assert WebsocketOwnerContract.default_downstream_send_timeout_ms() == 1_000
    end
  end

  describe "downstream owner messages" do
    test "accepts only the permitted downstream tuple payload shapes" do
      assert WebsocketOwnerContract.downstream_message?({:websocket_owner_frame, "corr-1", 1, {:data, "encoded text"}})

      assert {:ok, safe_payload} =
               WebsocketOwnerContract.safe_error_payload(:owner_busy, @sentinel)

      assert WebsocketOwnerContract.downstream_message?({:websocket_owner_frame, "corr-1", 1, {:error, :owner_busy, safe_payload}})

      assert WebsocketOwnerContract.downstream_message?({:websocket_owner_frame, "corr-1", 1, :complete})
    end

    test "rejects invalid downstream tuple shapes and mismatched error payloads" do
      assert {:ok, safe_payload} =
               WebsocketOwnerContract.safe_error_payload(:owner_busy, @sentinel)

      invalid_messages = [
        {:websocket_owner_frame, "corr-1", 1, {:data, :not_binary}},
        {:websocket_owner_frame, "corr-1", 0, :complete},
        {:websocket_owner_frame, :not_binary, 1, :complete},
        {:websocket_owner_frame, "corr-1", 1, {:error, :unknown_owner_error, %{}}},
        {:websocket_owner_frame, "corr-1", 1, {:error, :owner_busy, put_in(safe_payload.metadata.reason, "wrong_reason")}},
        {:websocket_owner_frame, "corr-1", 1, {:complete, @sentinel}},
        {:unexpected_owner_frame, "corr-1", 1, :complete}
      ]

      for message <- invalid_messages do
        refute WebsocketOwnerContract.downstream_message?(message)

        refute inspect(WebsocketOwnerContract.accept_downstream_message(message, 1, "corr-1")) =~
                 @sentinel
      end
    end

    test "accepts owner frames only when epoch and correlation match" do
      frame = {:websocket_owner_frame, "corr-1", 3, {:data, "encoded text"}}

      assert WebsocketOwnerContract.accept_downstream_message(frame, 3, "corr-1") ==
               {:ok, {:data, "encoded text"}}

      assert WebsocketOwnerContract.accept_downstream_message(frame, 4, "corr-1") == :drop
      assert WebsocketOwnerContract.accept_downstream_message(frame, 3, "corr-2") == :drop
      assert WebsocketOwnerContract.accept_downstream_message(frame, 4, "corr-2") == :drop
    end

    test "public owner frames require the matching immutable owner turn pid" do
      owner_turn_id = self()
      old_owner_turn_id = spawn(fn -> :ok end)

      frame =
        {:websocket_owner_frame, "corr-public", 7, owner_turn_id, {:data, "encoded public text"}}

      stale_frame =
        {:websocket_owner_frame, "corr-public", 7, old_owner_turn_id, {:data, "encoded stale text"}}

      legacy_frame =
        {:websocket_owner_frame, "corr-public", 7, {:data, "encoded legacy text"}}

      assert WebsocketOwnerContract.downstream_message?(frame)

      assert WebsocketOwnerContract.accept_downstream_message(
               frame,
               7,
               "corr-public",
               owner_turn_id
             ) == {:ok, {:data, "encoded public text"}}

      assert WebsocketOwnerContract.accept_downstream_message(
               stale_frame,
               7,
               "corr-public",
               owner_turn_id
             ) == :drop

      assert WebsocketOwnerContract.accept_downstream_message(
               legacy_frame,
               7,
               "corr-public",
               owner_turn_id
             ) == :drop

      assert WebsocketOwnerContract.accept_downstream_message(legacy_frame, 7, "corr-public") ==
               {:ok, {:data, "encoded legacy text"}}

      assert WebsocketOwnerContract.accept_downstream_message(frame, 7, "corr-public") == :drop
    end

    test "drops stale valid owner errors without exposing payload details" do
      assert {:ok, safe_payload} =
               WebsocketOwnerContract.safe_error_payload(:owner_forward_timeout, @sentinel)

      frame =
        {:websocket_owner_frame, "corr-1", 3, {:error, :owner_forward_timeout, safe_payload}}

      assert WebsocketOwnerContract.accept_downstream_message(frame, 2, "corr-1") == :drop
      assert WebsocketOwnerContract.accept_downstream_message(frame, 3, "corr-late") == :drop

      refute inspect(WebsocketOwnerContract.accept_downstream_message(frame, 2, "corr-1")) =~
               @sentinel
    end

    test "rejects malformed matching owner frames without leaking raw payloads" do
      wrong_payload_type = {:websocket_owner_frame, "corr-1", 3, {:data, :not_binary}}

      assert WebsocketOwnerContract.accept_downstream_message(wrong_payload_type, 3, "corr-1") ==
               {:error, :invalid_downstream_message}

      refute inspect(WebsocketOwnerContract.accept_downstream_message(wrong_payload_type, 3, "corr-1")) =~ @sentinel
    end
  end

  describe "output commitment barrier messages" do
    test "validates a probe and returns the carried reply pid and opaque refs" do
      active_turn_ref = make_ref()
      probe_ref = make_ref()
      owner_turn_id = self()

      probe =
        {:websocket_owner_output_commit_probe, "corr-probe", 3, owner_turn_id, active_turn_ref, self(), probe_ref}

      assert WebsocketOwnerContract.output_commit_probe?(probe)

      assert WebsocketOwnerContract.accept_output_commit_probe(
               probe,
               3,
               "corr-probe",
               owner_turn_id
             ) == {:ok, active_turn_ref, self(), probe_ref}

      assert WebsocketOwnerContract.accept_output_commit_probe(
               probe,
               4,
               "corr-probe",
               owner_turn_id
             ) == :drop
    end

    test "matches every acknowledgement identity including the probe ref" do
      active_turn_ref = make_ref()
      probe_ref = make_ref()
      owner_turn_id = self()

      ack =
        {:websocket_owner_output_commit_ack, "corr-ack", 5, owner_turn_id, active_turn_ref, probe_ref, true}

      assert WebsocketOwnerContract.output_commit_ack?(ack)

      assert WebsocketOwnerContract.accept_output_commit_ack(
               ack,
               5,
               "corr-ack",
               owner_turn_id,
               active_turn_ref,
               probe_ref
             ) == {:ok, true}

      assert WebsocketOwnerContract.accept_output_commit_ack(
               ack,
               5,
               "corr-ack",
               owner_turn_id,
               active_turn_ref,
               make_ref()
             ) == :drop
    end

    test "rejects malformed probe and acknowledgement values" do
      refute WebsocketOwnerContract.output_commit_probe?({:websocket_owner_output_commit_probe, "corr", 1, self(), :not_ref, self(), make_ref()})

      refute WebsocketOwnerContract.output_commit_ack?({:websocket_owner_output_commit_ack, "corr", 1, self(), make_ref(), make_ref(), :not_boolean})
    end
  end

  # findings#270: the owner tells its attached downstream that the upstream
  # connection behind it closed between requests.
  describe "upstream connection close instruction" do
    test "keeps cause, lifecycle id and generation of every anchor-ending close the session reports" do
      causes = CloseDiagnostics.anchor_invalidating_causes()
      assert :peer_close_frame in causes

      for cause <- causes do
        lifecycle_id = Ecto.UUID.generate()
        signal = %{cause: cause, lifecycle_id: lifecycle_id, generation: 2, connection_requests: 3}

        assert WebsocketOwnerContract.upstream_closed_signal(signal) == {:ok, %{cause: cause, lifecycle_id: lifecycle_id, generation: 2}}
      end
    end

    test "refuses a cause the session never reports to its subscriber" do
      for cause <- [:request_key_changed, :owner_drained, :unknown_close_cause, "peer_close_frame", nil] do
        assert WebsocketOwnerContract.upstream_closed_signal(%{cause: cause, lifecycle_id: Ecto.UUID.generate(), generation: 1}) == :error
      end
    end

    test "refuses a lifecycle id or generation that names no connection" do
      lifecycle_id = Ecto.UUID.generate()

      for signal <- [
            %{cause: :peer_close_frame, lifecycle_id: @sentinel, generation: 1},
            %{cause: :peer_close_frame, lifecycle_id: Ecto.UUID.bingenerate(), generation: 1},
            %{cause: :peer_close_frame, lifecycle_id: lifecycle_id <> "0", generation: 1},
            %{cause: :peer_close_frame, lifecycle_id: :lifecycle, generation: 1},
            %{cause: :peer_close_frame, lifecycle_id: lifecycle_id, generation: 0},
            %{cause: :peer_close_frame, lifecycle_id: lifecycle_id, generation: -1},
            %{cause: :peer_close_frame, lifecycle_id: lifecycle_id, generation: 1.0},
            %{cause: :peer_close_frame, lifecycle_id: lifecycle_id},
            %{cause: :peer_close_frame, generation: 1},
            %{lifecycle_id: lifecycle_id, generation: 1},
            {:peer_close_frame, lifecycle_id, 1}
          ] do
        assert WebsocketOwnerContract.upstream_closed_signal(signal) == :error
      end
    end

    test "the instruction carries exactly the kept signal for one downstream epoch" do
      signal = %{cause: :transport_closed, lifecycle_id: Ecto.UUID.generate(), generation: 4}
      message = {:websocket_owner_upstream_closed, "corr-close", 2, signal}

      assert WebsocketOwnerContract.upstream_closed_message?(message)
      refute WebsocketOwnerContract.upstream_closed_message?({:websocket_owner_upstream_closed, "corr-close", 2, Map.put(signal, :connection_requests, 1)})
      refute WebsocketOwnerContract.upstream_closed_message?({:websocket_owner_upstream_closed, "corr-close", 0, signal})
      refute WebsocketOwnerContract.upstream_closed_message?({:websocket_owner_upstream_closed, :corr_close, 2, signal})
      refute WebsocketOwnerContract.upstream_closed_message?({:websocket_owner_upstream_closed, "corr-close", 2, %{signal | cause: :request_key_changed}})
      refute WebsocketOwnerContract.upstream_closed_message?({:websocket_owner_upstream_close, "corr-close", 2, signal})
      refute WebsocketOwnerContract.upstream_closed_message?({:websocket_owner_upstream_closed, "corr-close", 2})
    end

    test "accepts the instruction only for the matching downstream and drops a stale one" do
      signal = %{cause: :pong_deadline, lifecycle_id: Ecto.UUID.generate(), generation: 7}
      message = {:websocket_owner_upstream_closed, "corr-close", 3, signal}

      assert WebsocketOwnerContract.accept_upstream_closed_message(message, 3, "corr-close") == {:ok, signal}
      assert WebsocketOwnerContract.accept_upstream_closed_message(message, 4, "corr-close") == :drop
      assert WebsocketOwnerContract.accept_upstream_closed_message(message, 3, "corr-other") == :drop

      invalid = {:websocket_owner_upstream_closed, "corr-close", 3, %{signal | lifecycle_id: @sentinel}}
      assert WebsocketOwnerContract.accept_upstream_closed_message(invalid, 3, "corr-close") == {:error, :invalid_upstream_closed_message}
      assert WebsocketOwnerContract.accept_upstream_closed_message(invalid, 4, "corr-close") == {:error, :invalid_upstream_closed_message}
      refute inspect(WebsocketOwnerContract.accept_upstream_closed_message(invalid, 3, "corr-close")) =~ @sentinel
    end

    test "is not an owner frame, so the owner error vocabulary never carries it" do
      message = {:websocket_owner_upstream_closed, "corr-close", 1, %{cause: :peer_close_frame, lifecycle_id: Ecto.UUID.generate(), generation: 1}}

      refute WebsocketOwnerContract.downstream_message?(message)
      assert WebsocketOwnerContract.accept_downstream_message(message, 1, "corr-close") == {:error, :invalid_downstream_message}
      refute WebsocketOwnerContract.owner_error?(:upstream_connection_closed)
    end
  end
end
