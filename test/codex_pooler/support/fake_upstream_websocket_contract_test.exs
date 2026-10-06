defmodule CodexPooler.FakeUpstreamWebsocketContractTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.Request

  @timeouts %{connect_timeout_ms: 1_000, receive_timeout_ms: 1_000}

  # The refusal names the constructors of the nine modes the fake accepts for a native websocket expectation; the
  # `FakeUpstream` bullet of test/support/AGENTS.md lists the same nine.
  @native_websocket_refusal "native websocket expectation requires one of websocket_text_frames/1, websocket_text_frames_then_abrupt_close/1, barrier_websocket_frames/2, interruptible_websocket_frames/2, websocket_sse_then_close/2, websocket_terminal_then_close_barrier/2, websocket_connection_limit_terminal_barrier/1, websocket_close_without_terminal_barrier/1, websocket_upgrade_error/2"

  @tag :fake_upstream_strict_contract
  test "strict websocket expectations validate discriminator and connection ordinal" do
    event = completed_event("strict")

    {upstream, session} =
      start_resources(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            path: "/backend-api/codex/responses",
            websocket_connection_ordinal: 1,
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond: FakeUpstream.websocket_text_frames([event])
          )
        ])
      )

    assert {:ok, %{terminal: "response.completed", status: 200, provider_credits_admission: %{capacity_basis: :windowless_provider_permission, context: %{model: "example-model", serving_mode: :full, transport: :native_websocket}}}} =
             UpstreamWebsocketSession.request(
               session,
               websocket_request(upstream, [], %{"type" => "response.create"})
             )

    assert :ok = FakeUpstream.verify!(upstream)
  end

  @tag :fake_upstream_strict_contract
  test "native websocket success cannot be satisfied by an SSE-derived shortcut" do
    mode =
      FakeUpstream.strict_sequence([
        FakeUpstream.expect_request(
          method: "WEBSOCKET",
          json: [valid: true, equals: %{"type" => "response.create"}],
          respond:
            FakeUpstream.sse_stream([
              {"response.completed", %{"type" => "response.completed", "response" => %{}}}
            ])
        )
      ])

    assert_raise ArgumentError, @native_websocket_refusal, fn -> start_resources(mode) end
  end

  @tag :fake_upstream_strict_contract
  test "the native websocket refusal names the nine constructors whose modes the fake accepts" do
    {:ok, fake} = FakeUpstream.start_link(FakeUpstream.json_response(%{}))
    on_exit(fn -> FakeUpstream.stop(fake) end)

    event = completed_event("accepted")
    notify = self()
    release_ref = make_ref()

    accepted = [
      {"websocket_text_frames/1", FakeUpstream.websocket_text_frames([event])},
      {"websocket_text_frames_then_abrupt_close/1", FakeUpstream.websocket_text_frames_then_abrupt_close([event])},
      {"barrier_websocket_frames/2", FakeUpstream.barrier_websocket_frames([event], notify: notify, release_ref: release_ref)},
      {"interruptible_websocket_frames/2", FakeUpstream.interruptible_websocket_frames([event], response_id: "resp_accepted", interrupted: [event], completion: [event], notify: notify, release_ref: release_ref)},
      {"websocket_sse_then_close/2", FakeUpstream.websocket_sse_then_close([{"response.completed", %{"type" => "response.completed", "response" => %{}}}])},
      {"websocket_terminal_then_close_barrier/2", FakeUpstream.websocket_terminal_then_close_barrier(%{"type" => "response.completed"}, notify: notify, release_ref: release_ref)},
      {"websocket_connection_limit_terminal_barrier/1", FakeUpstream.websocket_connection_limit_terminal_barrier(shape: :top_level, notify: notify, release_ref: release_ref)},
      {"websocket_close_without_terminal_barrier/1", FakeUpstream.websocket_close_without_terminal_barrier(notify: notify, release_ref: release_ref)},
      {"websocket_upgrade_error/2", FakeUpstream.websocket_upgrade_error(%{"error" => "denied"})}
    ]

    # Every constructor the message names exists and builds a mode the fake accepts, through both validation paths.
    for {constructor, mode} <- accepted do
      [name, arity] = String.split(constructor, "/")
      assert function_exported?(FakeUpstream, String.to_existing_atom(name), String.to_integer(arity)), "#{constructor} is a FakeUpstream function"
      assert :ok = FakeUpstream.set_mode(fake, FakeUpstream.strict_sequence([native_expectation(mode)])), "set_mode/2 accepts #{constructor}"
      assert :ok = FakeUpstream.set_mode(fake, FakeUpstream.repeat_last([native_expectation(mode)])), "set_mode/2 accepts #{constructor} in repeat_last/1"
    end

    # The message names those nine, in that order.
    assert @native_websocket_refusal == "native websocket expectation requires one of " <> Enum.map_join(accepted, ", ", &elem(&1, 0))

    # Everything else is refused with that message, on both paths, whatever wraps it.
    sse = FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{}}}])
    paced = FakeUpstream.delayed_sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{}}}], interval_ms: 1)

    for mode <- [FakeUpstream.json_response(%{}), sse, paced] do
      expectation = native_expectation(mode)
      assert_raise ArgumentError, @native_websocket_refusal, fn -> FakeUpstream.set_mode(fake, FakeUpstream.strict_sequence([expectation])) end
      assert_raise ArgumentError, @native_websocket_refusal, fn -> FakeUpstream.set_mode(fake, FakeUpstream.repeat_last([expectation])) end
      assert_raise ArgumentError, @native_websocket_refusal, fn -> FakeUpstream.start_link(FakeUpstream.strict_sequence([expectation])) end
    end

    # Only a native websocket expectation is held to it: the same SSE mode answers an HTTP expectation.
    http_expectation = FakeUpstream.expect_request(method: "POST", path: "/backend-api/codex/responses", respond: sse)
    assert :ok = FakeUpstream.set_mode(fake, FakeUpstream.strict_sequence([http_expectation]))
  end

  @tag :fake_upstream_strict_contract
  test "websocket ordinal mismatch withholds success and reports exact ordinals" do
    {upstream, session} =
      start_resources(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            websocket_connection_ordinal: 2,
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond: FakeUpstream.websocket_text_frames([completed_event("wrong-ordinal")])
          )
        ])
      )

    assert {:error, %{reason: :upstream_websocket_closed_before_terminal}} =
             UpstreamWebsocketSession.request(
               session,
               websocket_request(upstream, [], %{"type" => "response.create"})
             )

    assert_raise ExUnit.AssertionError,
                 ~r/expectation_mismatch.*field=websocket_connection_ordinal expected=2 actual=1/s,
                 fn -> FakeUpstream.verify!(upstream) end
  end

  @tag :fake_upstream_strict_contract
  test "peer-close acknowledgement is required and recorded by the real websocket lifecycle" do
    {upstream, session} =
      start_resources(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            websocket_connection_ordinal: 1,
            json: [valid: true, equals: %{"type" => "response.create"}],
            respond: FakeUpstream.websocket_text_frames([completed_event("peer-close")])
          )
        ])
      )

    assert {:ok, %{terminal: "response.completed"}} =
             UpstreamWebsocketSession.request(
               session,
               websocket_request(upstream, [], %{"type" => "response.create"})
             )

    close_ref = make_ref()

    assert :ok =
             FakeUpstream.close_websocket_connection(upstream, 1,
               notify: self(),
               close_ref: close_ref
             )

    assert_receive {:fake_upstream_websocket_peer_closed, 1, ^close_ref}, 2_000
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "real upstream websocket session upgrades and counts one fake connection" do
    event =
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => "resp_contract"}
      })

    {:ok, upstream} = FakeUpstream.start_link(FakeUpstream.websocket_text_frames([event]))
    {:ok, session} = UpstreamWebsocketSession.start_link([])

    on_exit(fn ->
      UpstreamWebsocketSession.close(session)
      FakeUpstream.stop(upstream)
    end)

    request = %Request{
      url: FakeUpstream.url(upstream) <> "/backend-api/codex/responses",
      headers: [],
      payload: CodexPooler.JSON.encode!(%{"model" => "example-model"}),
      timeouts: @timeouts,
      writer: fn _text -> :ok end,
      message_mapper: nil
    }

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, CodexPooler.ProviderCreditsDispatchSupport.wire_request!(request))

    assert FakeUpstream.websocket_connection_count(upstream) == 1
    assert [connection_id] = FakeUpstream.websocket_connection_ids(upstream)
    assert is_reference(connection_id)
  end

  test "keeps one opaque connection ID across warm request reuse" do
    {upstream, session} = start_resources(FakeUpstream.websocket_text_frames([completed_event()]))
    request = websocket_request(upstream)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, request)

    assert [connection_id] = FakeUpstream.websocket_connection_ids(upstream)
    assert FakeUpstream.websocket_connection_count(upstream) == 1

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, request)

    assert FakeUpstream.websocket_connection_count(upstream) == 1
    assert [^connection_id] = FakeUpstream.websocket_connection_ids(upstream)
  end

  test "reconnects on the next explicit request after a forced pre-visible close" do
    {upstream, session} =
      start_resources(FakeUpstream.websocket_text_frames([completed_event("first")]))

    request = websocket_request(upstream)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, request)

    assert [first_connection_id] = FakeUpstream.websocket_connection_ids(upstream)

    # provenance: synthetic_adversarial
    FakeUpstream.set_mode(
      upstream,
      FakeUpstream.strict_sequence([
        FakeUpstream.expect_request(
          method: "WEBSOCKET",
          websocket_connection_ordinal: 1,
          respond: FakeUpstream.websocket_sse_then_close([], code: 1001, reason: "synthetic close")
        ),
        FakeUpstream.expect_request(
          method: "WEBSOCKET",
          websocket_connection_ordinal: 2,
          respond: FakeUpstream.websocket_text_frames([completed_event("reconnected")])
        )
      ])
    )

    assert {:error, %{reason: :upstream_websocket_closed_before_terminal}} =
             UpstreamWebsocketSession.request(session, request)

    assert FakeUpstream.websocket_connection_count(upstream) == 1
    assert [^first_connection_id] = FakeUpstream.websocket_connection_ids(upstream)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, request)

    assert FakeUpstream.websocket_connection_count(upstream) == 2

    assert [^first_connection_id, second_connection_id] =
             FakeUpstream.websocket_connection_ids(upstream)

    assert is_reference(second_connection_id)
    refute first_connection_id == second_connection_id
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "does not invent an ID when the next explicit reconnect fails its upgrade" do
    {upstream, session} =
      start_resources(FakeUpstream.websocket_text_frames([completed_event("first")]))

    request = websocket_request(upstream)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, request)

    assert [connection_id] = FakeUpstream.websocket_connection_ids(upstream)

    # provenance: synthetic_adversarial
    FakeUpstream.set_mode(
      upstream,
      FakeUpstream.strict_sequence([
        FakeUpstream.expect_request(
          method: "WEBSOCKET",
          websocket_connection_ordinal: 1,
          respond: FakeUpstream.websocket_sse_then_close([], code: 1001, reason: "synthetic close")
        ),
        FakeUpstream.expect_request(
          method: "GET",
          path: "/backend-api/codex/responses",
          respond:
            FakeUpstream.websocket_upgrade_error(%{"error" => %{"code" => "upgrade_rejected"}},
              status: 503
            )
        )
      ])
    )

    assert {:error, %{reason: :upstream_websocket_closed_before_terminal}} =
             UpstreamWebsocketSession.request(session, request)

    assert {:error, %{body: "", reason: {:websocket_upgrade_failed, 503, _}}} =
             UpstreamWebsocketSession.request(session, request)

    assert FakeUpstream.websocket_connection_count(upstream) == 1
    assert [^connection_id] = FakeUpstream.websocket_connection_ids(upstream)
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "observes a new ID when a request key changes its headers" do
    {upstream, session} =
      start_resources(
        # provenance: synthetic_adversarial
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            websocket_connection_ordinal: 1,
            headers: [required: %{"x-test-key" => "old"}],
            respond: FakeUpstream.websocket_text_frames([completed_event("old-key")])
          ),
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            websocket_connection_ordinal: 2,
            headers: [required: %{"x-test-key" => "new"}],
            respond: FakeUpstream.websocket_text_frames([completed_event("new-key")])
          )
        ])
      )

    old_request = websocket_request(upstream, [{"x-test-key", "old"}])
    new_request = websocket_request(upstream, [{"x-test-key", "new"}])

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, old_request)

    assert [first_connection_id] = FakeUpstream.websocket_connection_ids(upstream)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, new_request)

    assert FakeUpstream.websocket_connection_count(upstream) == 2

    assert [^first_connection_id, second_connection_id] =
             FakeUpstream.websocket_connection_ids(upstream)

    assert is_reference(second_connection_id)
    refute first_connection_id == second_connection_id
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "records no ID for a rejected initial websocket upgrade" do
    {upstream, _session} =
      start_resources(
        FakeUpstream.websocket_upgrade_error(%{"error" => %{"code" => "upgrade_rejected"}},
          status: 401
        )
      )

    request = websocket_request(upstream)

    assert {:error, %{reason: {:websocket_upgrade_failed, 401, _}}} =
             UpstreamWebsocketSession.request_once(request)

    assert FakeUpstream.websocket_connection_count(upstream) == 0
    assert [] = FakeUpstream.websocket_connection_ids(upstream)
  end

  test "keeps malformed upgrade failure output bounded" do
    {upstream, _session} =
      start_resources(FakeUpstream.websocket_upgrade_error(%{"error" => "malformed"}, status: 502))

    assert {:error, %{body: "", reason: {:websocket_upgrade_failed, 502, _}}} =
             UpstreamWebsocketSession.request_once(websocket_request(upstream))

    assert FakeUpstream.websocket_connection_count(upstream) == 0
    assert [] = FakeUpstream.websocket_connection_ids(upstream)
  end

  test "repeated close requests remain bounded and do not add IDs" do
    {upstream, session} = start_resources(FakeUpstream.websocket_text_frames([completed_event()]))
    request = websocket_request(upstream)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             UpstreamWebsocketSession.request(session, request)

    assert [connection_id] = FakeUpstream.websocket_connection_ids(upstream)
    assert :ok = FakeUpstream.close_websocket_connections(upstream)
    assert :ok = FakeUpstream.close_websocket_connections(upstream)
    assert FakeUpstream.websocket_connection_count(upstream) == 1
    assert [^connection_id] = FakeUpstream.websocket_connection_ids(upstream)
  end

  defp native_expectation(mode), do: FakeUpstream.expect_request(method: "WEBSOCKET", path: "/backend-api/codex/responses", respond: mode)

  defp start_resources(mode) do
    {:ok, upstream} = FakeUpstream.start_link(mode)
    {:ok, session} = UpstreamWebsocketSession.start_link([])

    on_exit(fn ->
      UpstreamWebsocketSession.close(session)
      FakeUpstream.stop(upstream)
    end)

    {upstream, session}
  end

  defp websocket_request(upstream, headers \\ [], payload \\ %{}) do
    %Request{
      url: FakeUpstream.url(upstream) <> "/backend-api/codex/responses",
      headers: headers,
      payload: CodexPooler.JSON.encode!(Map.put_new(payload, "model", "example-model")),
      timeouts: @timeouts,
      writer: fn _text -> :ok end,
      message_mapper: nil
    }
    |> CodexPooler.ProviderCreditsDispatchSupport.wire_request!()
  end

  defp completed_event(id \\ "resp_contract") do
    CodexPooler.JSON.encode!(%{
      "type" => "response.completed",
      "response" => %{"id" => id}
    })
  end
end
