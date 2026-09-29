defmodule CodexPooler.Gateway.Payloads.ContinuityPayloadTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.ContinuityPayload
  alias CodexPooler.Gateway.Payloads.RequestOptions

  test "hydrates previous response id from payload only when continuity lacks one" do
    options = RequestOptions.build(%{}, "/backend-api/codex/responses", %{})

    hydrated =
      ContinuityPayload.put_previous_response_id(options, %{
        "previous_response_id" => " resp_payload "
      })

    assert hydrated.continuity.previous_response_id == "resp_payload"

    preserved =
      hydrated
      |> RequestOptions.put_continuity(previous_response_id: "resp_explicit")
      |> ContinuityPayload.put_previous_response_id(%{
        previous_response_id: "resp_payload_next"
      })

    assert preserved.continuity.previous_response_id == "resp_explicit"
  end

  describe "previous window session header (findings#289)" do
    @thread "019a0000-0000-7000-8000-000000000289"

    test "a native HTTP request keyed by its window falls back to the previous window of its thread" do
      for {endpoint, payload} <- [
            {"/backend-api/codex/responses", %{"stream" => true}},
            {"/backend-api/codex/responses", %{}},
            {"/backend-api/codex/responses/compact", %{}}
          ] do
        options = http_options(" #{@thread}:2 ", endpoint, payload)
        assert options.transport.transport in ["http_sse", "http_json", "http_compact_json"]
        assert ContinuityPayload.previous_window_session_header(options) == "#{@thread}:1"
      end
    end

    test "nothing else falls back" do
      http = http_options("#{@thread}:2")

      refused = [
        websocket: RequestOptions.for_websocket(%{session_header: "#{@thread}:2", session_header_source: "x-codex-window-id"}),
        v1_origin: RequestOptions.mark_openai_compatibility_origin(http, "/v1/responses", "/backend-api/codex/responses"),
        session_id_header: RequestOptions.build(%{session_header: "#{@thread}:2", session_header_source: "session-id"}, "/backend-api/codex/responses", %{}),
        client_turn_state: RequestOptions.put_continuity(http, accepted_turn_state: "client-turn-state"),
        response_anchor: RequestOptions.put_continuity(http, previous_response_id: "resp_anchor"),
        owner_attach: RequestOptions.put_continuity(http, authenticated_owner_attach: true),
        first_window: http_options("#{@thread}:0"),
        malformed_window: http_options("#{@thread}:x"),
        blank_window: http_options("  ")
      ]

      for {case_name, options} <- refused do
        assert {case_name, ContinuityPayload.previous_window_session_header(options)} == {case_name, nil}
      end
    end

    defp http_options(window, endpoint \\ "/backend-api/codex/responses", payload \\ %{"stream" => true}) do
      RequestOptions.build(%{session_header: window, session_header_source: "x-codex-window-id"}, endpoint, payload)
    end
  end

  # The websocket upgrade does not join the previous window's session; the
  # session it opens prefers that session's assignment (findings#270 row
  # 270-283).
  describe "previous window preference header (findings#270 row 270-283)" do
    @thread "019a0000-0000-7000-8000-000000000283"

    test "a native websocket upgrade keyed by its window names the previous window of its thread" do
      upgrade = upgrade_options(" #{@thread}:2 ")
      assert ContinuityPayload.previous_window_preference_header(upgrade) == "#{@thread}:1"

      # The turn state the Pooler issues on an upgrade names only that connection.
      issued = RequestOptions.put_continuity(upgrade, accepted_turn_state: "pooler-issued", pooler_issued_turn_state?: true, authenticated_owner_attach: true)
      assert ContinuityPayload.previous_window_preference_header(issued) == "#{@thread}:1"

      # The upgrade never joins that session.
      assert ContinuityPayload.previous_window_session_header(upgrade) == nil
      assert ContinuityPayload.previous_window_session_header(issued) == nil
    end

    test "nothing else names one" do
      upgrade = upgrade_options("#{@thread}:2")

      refused = [
        native_http: RequestOptions.build(%{session_header: "#{@thread}:2", session_header_source: "x-codex-window-id"}, "/backend-api/codex/responses", %{"stream" => true}),
        v1_origin: RequestOptions.mark_openai_compatibility_origin(upgrade, "/v1/responses", "/backend-api/codex/responses"),
        session_id_header: RequestOptions.for_websocket(%{session_header: "#{@thread}:2", session_header_source: "session-id"}),
        client_turn_state: RequestOptions.put_continuity(upgrade, accepted_turn_state: "client-turn-state"),
        response_anchor: RequestOptions.put_continuity(upgrade, previous_response_id: "resp_anchor"),
        first_window: upgrade_options("#{@thread}:0"),
        malformed_window: upgrade_options("#{@thread}:x"),
        blank_window: upgrade_options("  ")
      ]

      for {case_name, options} <- refused do
        assert {case_name, ContinuityPayload.previous_window_preference_header(options)} == {case_name, nil}
      end
    end

    defp upgrade_options(window), do: RequestOptions.for_websocket(%{session_header: window, session_header_source: "x-codex-window-id"})
  end
end
