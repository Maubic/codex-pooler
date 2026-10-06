defmodule CodexPoolerWeb.Runtime.PartitionHeldBackQuotaRefusalTest do
  # A model whose per-account catalog sources differ in a behavioral field is
  # split into canonical partitions, and a native turn routes over the selected
  # partition only. Selection prefers the partition with the most quota-routable
  # members (`PartitionRoutability` over `CandidateEligibility.Quota.quota_routable?/4`,
  # against the one quota snapshot pre-dispatch reads), and the request's quota
  # filtering then classifies only the selected partition's seats: it refreshes
  # their stale evidence synchronously before it refuses. A held-back seat is
  # classified by selection alone, so a quota refusal of the selected
  # partition first runs the held-back seats once through the same filtering
  # (`PartitionFallback.before_dispatch/3`, codex-pooler#498): the turn moves
  # there when one is admitted, and otherwise the Pool's refusal names both
  # partitions' seats, the held-back ones marked.
  #
  # Topology: one Pool, one BEAM node, owner forwarding off unless an arm says
  # otherwise, Full serving mode, one FakeUpstream per account, the Codex
  # Desktop originator. Three "plus" assignments advertise the model with
  # `supports_experimental_context: false`; one "pro" assignment advertises it
  # with `true` and another `default_reasoning_level` (the first field is inside
  # the partition digest, the second outside it), so the model has two
  # canonical partitions: plus (3 members, oldest anchor) and pro (1 member).
  # Every plus seat is exhausted on its 5-hour account window with a distinct
  # reset; the pro seat's quota evidence varies per arm. Every seat answers the
  # provider usage read with an available 5-hour window, so a synchronous quota
  # refresh finds it routable.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.RoutingCircuitState
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows

  @moduletag capture_log: true

  @model "gpt-test-astra"
  @upstream_model "provider-gpt-test-astra"
  @turn_endpoint "/backend-api/codex/responses"
  @compact_endpoint "/backend-api/codex/responses/compact"
  @usage_path "/backend-api/wham/usage"
  @desktop "Codex Desktop"
  @plus_reset_offsets [3 * 3_600 + 6 * 60, 3 * 3_600 + 15 * 60, 3 * 3_600 + 35 * 60]
  @admitted_line "canonical partition fallback phase=pre_dispatch outcome=admitted"
  @pool_refusal_line "canonical partition fallback phase=pre_dispatch outcome=pool_refusal"
  # Failure-detection budget of the bounded row and evidence polls; a green
  # run returns as soon as the awaited state is visible.
  @detection_timeout_ms 15_000

  describe "HTTP remote compaction (responses_compaction_v2 over HTTPS)" do
    test "control: a quota-fresh held-back seat wins selection and serves the compaction" do
      pool = astra_pool!(:fresh)

      conn = post_v2_compaction(pool, "astra-fresh")
      row = only_row!(pool)
      diagnostics("v2 fresh", conn, row, pool)

      assert conn.status == 200, observed(conn, row, pool)
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
      assert partition(row) == {2, 1, 3, true}
      assert %{"selected_routable_count" => 1, "held_back_routable_count" => 0} = summary(row)
      refute Map.has_key?(summary(row), "held_back_fallback")
    end

    test "control: a quota-fresh weekly-only held-back seat wins selection and serves the compaction" do
      pool = astra_pool!(:weekly_only)

      conn = post_v2_compaction(pool, "astra-weekly-only")
      row = only_row!(pool)
      diagnostics("v2 weekly_only", conn, row, pool)

      assert conn.status == 200, observed(conn, row, pool)
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
      assert partition(row) == {2, 1, 3, true}
    end

    test "control: the same stale seat inside the selected partition is refreshed and serves" do
      pool = astra_pool!(:stale_idle, drift?: false)

      conn = post_v2_compaction(pool, "astra-stale-one-partition")
      row = only_row!(pool)
      diagnostics("v2 stale_idle one partition", conn, row, pool)

      assert conn.status == 200, observed(conn, row, pool)
      assert usage_reads(pool.pro.upstream) == 1
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
      assert row.request_metadata["canonical_partition"] == nil
      assert get_in(row.request_metadata, ["quota_decision", "refreshed_stale_quota"]) == true
    end

    test "a held-back seat whose evidence went stale is re-read before the Pool refuses, and serves" do
      pool = astra_pool!(:stale_idle)

      {conn, log} = with_info_log(fn -> post_v2_compaction(pool, "astra-stale") end)
      row = only_row!(pool)
      diagnostics("v2 stale_idle", conn, row, pool)

      assert usage_reads(pool.pro.upstream) == 1,
             "the held-back seat's stale evidence was not refreshed on the request path: " <> observed(conn, row, pool)

      assert conn.status == 200, observed(conn, row, pool)
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]

      # Selection read no seat as routable and kept the larger plus
      # partition; the pre-dispatch fallback admitted the refreshed pro seat.
      assert %{
               "partition_count" => 2,
               "selected_count" => 3,
               "filtered_count" => 1,
               "routable_selection" => false,
               "selected_routable_count" => 0,
               "held_back_routable_count" => 0,
               "held_back_fallback" => "pre_dispatch",
               "held_back_fallback_outcome" => "admitted"
             } = summary(row)

      assert [line] = fallback_lines(log)
      assert line =~ @admitted_line
      assert line =~ "selected_refusal=quota_exhausted held_back_refusal=none"
      assert line =~ "partition_count=2 selected_count=3 selected_routable_count=0 held_back_count=1 held_back_routable_count=0 admitted_count=1"
      refute line =~ pool.pro.assignment.id
      refute line =~ pool.setup.pool.id
    end

    test "a held-back seat exhausted until an earlier reset sets the Pool's usage-limit advice" do
      pro_reset_offset = 30 * 60
      pool = astra_pool!({:exhausted, pro_reset_offset})

      {conn, log} = with_info_log(fn -> post_v2_compaction(pool, "astra-pro-exhausted-earlier") end)
      row = only_row!(pool)
      diagnostics("v2 pro exhausted earlier", conn, row, pool)

      assert conn.status == 400, observed(conn, row, pool)
      assert %{"error" => %{"code" => "invalid_prompt", "resets_in_seconds" => seconds}} = CodexPooler.JSON.decode!(conn.resp_body)

      assert seconds in (pro_reset_offset - 10)..pro_reset_offset,
             "the Pool's earliest return is the held-back seat's reset (#{pro_reset_offset} s), the answer advised #{seconds} s: " <> observed(conn, row, pool)

      # The refusal names every seat of the Pool, the held-back one marked.
      assert {row.status, row.response_status_code, row.last_error_code} == {"rejected", 429, "quota_exhausted"}
      assert Enum.sort(Enum.map(exclusion_labels(row, pool), &elem(&1, 0))) == [:plus1, :plus2, :plus3, :pro]
      assert [%{"partition" => "held_back"}] = Enum.filter(row.request_metadata["candidate_exclusions"], &(&1["pool_upstream_assignment_id"] == pool.pro.assignment.id))
      assert Enum.all?(Enum.reject(row.request_metadata["candidate_exclusions"], &(&1["pool_upstream_assignment_id"] == pool.pro.assignment.id)), &(not Map.has_key?(&1, "partition")))
      assert %{"held_back_fallback" => "pre_dispatch", "held_back_fallback_outcome" => "pool_refusal"} = summary(row)
      assert [line] = fallback_lines(log)
      assert line =~ @pool_refusal_line
      assert line =~ "selected_refusal=quota_exhausted held_back_refusal=quota_exhausted"
    end

    test "a held-back seat with no quota evidence leaves the Pool's return unknown, and the answer is the retryable 503" do
      pool = astra_pool!(:missing)

      conn = post_v2_compaction(pool, "astra-pro-missing")
      row = only_row!(pool)
      diagnostics("v2 pro missing", conn, row, pool)

      # The rule the selected partition's refusal applies (UsageLimit,
      # findings#206 row 206-508): a candidate with no known return keeps the
      # retryable 503 instead of a terminal usage limit with a reset.
      assert conn.status == 503, observed(conn, row, pool)
      assert {row.status, row.response_status_code, row.last_error_code} == {"rejected", 503, "quota_exhausted"}
      assert {:pro, [%{"code" => "quota_evidence_missing"}]} in exclusion_labels(row, pool)
      assert %{"held_back_fallback" => "pre_dispatch", "held_back_fallback_outcome" => "pool_refusal"} = summary(row)
    end
  end

  describe "HTTP direct compaction and an ordinary turn" do
    test "direct /responses/compact: a stale held-back seat is re-read and serves" do
      pool = astra_pool!(:stale_idle)

      conn = post_direct_compaction(pool, "astra-direct-stale")
      row = only_row!(pool)
      diagnostics("direct stale_idle", conn, row, pool)

      assert usage_reads(pool.pro.upstream) == 1, observed(conn, row, pool)
      assert conn.status == 200, observed(conn, row, pool)
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
    end

    test "ordinary turn: a stale held-back seat is re-read and serves" do
      pool = astra_pool!(:stale_idle)

      conn = post_ordinary_turn(pool, "astra-turn-stale")
      row = only_row!(pool)
      diagnostics("turn stale_idle", conn, row, pool)

      assert usage_reads(pool.pro.upstream) == 1, observed(conn, row, pool)
      assert conn.status == 200, observed(conn, row, pool)
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
    end

    test "control: ordinary turn, a quota-fresh held-back seat wins selection" do
      pool = astra_pool!(:fresh)

      conn = post_ordinary_turn(pool, "astra-turn-fresh")
      row = only_row!(pool)
      diagnostics("turn fresh", conn, row, pool)

      assert conn.status == 200, observed(conn, row, pool)
      assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
      assert partition(row) == {2, 1, 3, true}
    end

    test "a held-back seat behind an open circuit keeps the Pool refusal retryable, with the circuit's Retry-After" do
      pool = astra_pool!(:stale_idle)
      open_circuits!(pool, pool.pro.assignment)

      conn = post_ordinary_turn(pool, "astra-held-back-circuit")
      row = only_row!(pool)
      diagnostics("turn held-back circuit", conn, row, pool)

      # The fallback ran: the held-back seat's circuit refused it before its
      # quota was read, and an open circuit has no known return.
      assert conn.status == 503, observed(conn, row, pool)
      assert [retry_after] = get_resp_header(conn, "retry-after")
      assert String.to_integer(retry_after) in 1..60
      assert {row.status, row.last_error_code} == {"rejected", "quota_exhausted"}
      assert {:pro, [%{"code" => "routing_circuit_open"}]} in exclusion_labels(row, pool)
      assert %{"held_back_fallback" => "pre_dispatch", "held_back_fallback_outcome" => "pool_refusal"} = summary(row)
      assert usage_reads(pool.pro.upstream) == 0
    end

    test "a circuit refusal of the selected partition takes no fallback and records why" do
      pool = astra_pool!(:fresh, plus: :fresh)

      for seat <- pool.plus, do: open_circuits!(pool, seat.assignment)

      {conn, log} = with_info_log(fn -> post_ordinary_turn(pool, "astra-circuit") end)
      row = only_row!(pool)
      diagnostics("turn circuit-open plus", conn, row, pool)

      # Quota selection does not read circuits, so the plus partition (three
      # routable seats) is kept; the fallback answers quota refusals only.
      assert conn.status == 503, observed(conn, row, pool)
      assert {row.status, row.last_error_code} == {"rejected", "no_eligible_backend"}
      assert %{"selected_routable_count" => 3, "held_back_routable_count" => 1, "held_back_skip_reason" => "non_quota_refusal"} = summary(row)
      refute Map.has_key?(summary(row), "held_back_fallback")
      assert usage_reads(pool.pro.upstream) == 0
      assert fallback_lines(log) == []
    end
  end

  describe "which pro evidence selection reads as routable (control: today's classification)" do
    # Each arm records the selection outcome for one shape of the held-back
    # seat's evidence, next to a fresh 5-hour account window that alone would
    # make it routable. `true` means selection moved the turn to the pro
    # partition, `false` that it kept the exhausted plus partition.
    for {state, routable?} <- [
          {:model_window_exhausted, false},
          {:other_model_window_exhausted, true},
          {:feature_window_exhausted, false},
          {:model_window_stale, false},
          {:account_window_stale_weekly_fresh, true}
        ] do
      @state state
      @routable routable?
      test "#{state} reads as routable=#{routable?}" do
        pool = astra_pool!(@state)

        conn = post_v2_compaction(pool, "astra-#{@state}")
        row = only_row!(pool)
        diagnostics("classification #{@state}", conn, row, pool)

        assert {2, _selected, _filtered, @routable} = partition(row), observed(conn, row, pool)
      end
    end
  end

  describe "native websocket ordinary turn refused before dispatch" do
    # The same pre-dispatch quota refusal on the native websocket: the socket's
    # first turn has no pin, so the stale held-back seat is re-read and the
    # turn moves there on its own upstream websocket connection. One node;
    # with forwarding on, the turn runs through the local owner.
    for topology <- [:direct, :owner_forwarded] do
      @topology topology
      test "#{topology}: a stale held-back seat is re-read and serves the turn" do
        put_owner_forwarding!(@topology == :owner_forwarded)
        pool = astra_pool!(:stale_idle)
        ids = new_ids()
        {_server, port} = start_public_endpoint_with_server!()

        {terminal, log} =
          with_info_log(fn ->
            websocket_session!(port, pool, ids, fn conn, websocket, ref ->
              turn = websocket_frame(ids, native_text_input("synthetic websocket prompt"), :turn)
              {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(turn))
              {_conn, _websocket, terminal} = receive_terminal!(conn, websocket, ref)
              terminal
            end)
          end)

        [row] = await_settled_rows!(pool, 1)
        CodexPooler.TestDiagnostics.puts(fn -> "partition held-back websocket turn #{@topology}: " <> inspect(%{terminal: terminal["type"], row: {row.transport, row.status}, attempts: attempts!(row, pool), summary: summary(row)}) end)

        assert %{"type" => "response.completed"} = terminal
        assert {row.transport, row.status} == {"websocket", "succeeded"}
        # A forwarded turn records the owner binding it ran under.
        assert Map.has_key?(row.request_metadata, "websocket_owner_forwarding") == (@topology == :owner_forwarded)
        assert attempts!(row, pool) == [{"succeeded", 200, :pro}]
        assert usage_reads(pool.pro.upstream) == 1
        assert FakeUpstream.websocket_connection_count(pool.pro.upstream) == 1
        assert %{"held_back_fallback" => "pre_dispatch", "held_back_fallback_outcome" => "admitted"} = summary(row)
        assert [line] = fallback_lines(log)
        assert line =~ @admitted_line
      end
    end

    test "an anchored continuation stays on its pinned seat and records the hard pin" do
      put_owner_forwarding!(false)
      ids = new_ids()
      opening_response_id = "resp_astra_opening_#{System.unique_integer([:positive])}"
      pool = astra_pool!(:fresh, plus1: :fresh, plus1_mode: anchored_session_seat_mode(opening_response_id))
      {_server, port} = start_public_endpoint_with_server!()

      terminals =
        websocket_session!(port, pool, ids, fn conn, websocket, ref ->
          opening = websocket_frame(ids, native_text_input("synthetic websocket prompt"), :turn)
          {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(opening))
          {conn, websocket, opening_terminal} = receive_terminal!(conn, websocket, ref)
          await_settled_rows!(pool, 1)

          continuation =
            ids
            |> websocket_frame([%{"type" => "function_call_output", "call_id" => "call_synthetic_astra", "output" => "synthetic result"}], :turn)
            |> Map.put("previous_response_id", opening_response_id)

          {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(continuation))
          {_conn, _websocket, continuation_terminal} = receive_terminal!(conn, websocket, ref)
          {opening_terminal, continuation_terminal}
        end)

      [opening_row, continuation_row] = await_settled_rows!(pool, 2)
      CodexPooler.TestDiagnostics.puts(fn -> "partition held-back anchored continuation: " <> inspect(%{terminals: terminals, opening: summary(opening_row), continuation: summary(continuation_row)}) end)

      assert {%{"type" => "response.completed"}, %{"type" => "response.completed"}} = terminals
      assert attempts!(opening_row, pool) == [{"succeeded", 200, :plus1}]
      assert attempts!(continuation_row, pool) == [{"succeeded", 200, :plus1}]
      # One routable seat in each partition: the tie goes to the larger plus
      # partition, and the pro seat stays a possible fallback for the opening.
      assert %{"selected_routable_count" => 1, "held_back_routable_count" => 1} = summary(opening_row)
      refute Map.has_key?(summary(opening_row), "held_back_skip_reason")
      assert %{"held_back_skip_reason" => "hard_pin"} = summary(continuation_row)
    end
  end

  describe "native websocket compaction, then the client's HTTPS compaction" do
    # The reporter's sequence: the thread's turns run on the one plus seat that
    # still reads routable, the post-turn compaction is anchored on that seat's
    # upstream connection, and the provider refuses it with its usage limit.
    # The compaction can run only on that connection, so the refusal speaks
    # for the capacity of the whole Pool before the connection pin, both
    # partitions (findings#305 row 498-5): with a seat that can serve or has
    # no known return it is the retryable `503 pinned_continuation_unavailable`
    # an anchored continuation gets, otherwise the Pool's usage limit with the
    # earliest return. Before, the compaction result adapter answered every
    # such refusal `502 invalid_compaction_response`, "upstream compact
    # response was not valid JSON".
    #
    # The released client (0.160.1) retries the 503: the failed request used
    # up the connection's last response, so its resend on the socket carries
    # the full history without the anchor, and the resend is refused
    # `409 duplicate_turn` before dispatch (no row, no provider request; the
    # client retries that too). Once its stream retries are spent it sends the
    # compaction over HTTPS, two seconds later in the report, and that request
    # can move to the held-back partition (`PartitionFallback.before_dispatch/3`).
    test "with a quota-fresh pro seat the refusal is retryable and the HTTPS compaction moves to the pro partition" do
      timeline = websocket_compaction_timeline!(:fresh)

      assert timeline.turn_partition == {2, 3, 1, false}, inspect(timeline, limit: :infinity)
      assert_retryable_compaction_refusal!(timeline)
      assert timeline.compaction_skip_reason == "connection_bound_compaction"
      assert timeline.http.status == 200, inspect(timeline, limit: :infinity)
      assert timeline.http_attempts == [{"succeeded", 200, :pro}]
      assert timeline.http_partition == {2, 1, 3, true}
    end

    test "with a stale pro seat the refusal is retryable and the HTTPS compaction re-reads the seat and moves to the pro partition" do
      timeline = websocket_compaction_timeline!(:stale_idle)

      assert timeline.turn_partition == {2, 3, 1, false}, inspect(timeline, limit: :infinity)
      # A held-back seat with stale evidence has no known return.
      assert_retryable_compaction_refusal!(timeline)
      assert timeline.compaction_skip_reason == "connection_bound_compaction"
      assert timeline.pro_websocket_connections == 0

      assert timeline.http.status == 200,
             "the HTTPS compaction was refused although the held-back seat was never re-read: " <> inspect(timeline, limit: :infinity)

      assert timeline.http_attempts == [{"succeeded", 200, :pro}]
      assert timeline.http_partition == {2, 3, 1, false}
      assert %{"held_back_fallback" => "pre_dispatch", "held_back_fallback_outcome" => "admitted"} = timeline.http_summary
    end

    # Every seat is exhausted; the held-back pro seat returns first, ten
    # minutes before any plus seat. The client stops on the terminal answer,
    # so no resend and no HTTPS compaction follow.
    test "with every seat exhausted the refusal is the Pool's usage limit with the held-back seat's return, as the Codex Desktop app reads it" do
      timeline = websocket_compaction_timeline!({:exhausted, 600}, follow_up: :none)

      assert %{"type" => "error", "status" => 400, "error" => %{"code" => "invalid_prompt", "type" => "invalid_request_error", "resets_in_seconds" => seconds} = error} = timeline.compaction_terminal
      assert seconds in 590..600, inspect(timeline, limit: :infinity)
      assert error["message"] =~ "The Pool's usage limit is reached. Try again at "
      assert_terminal_compaction_refusal!(timeline, seconds, 400)
    end

    test "with every seat exhausted the refusal is the Pool's usage limit with the held-back seat's return, as the Codex CLI reads it" do
      timeline = websocket_compaction_timeline!({:exhausted, 600}, follow_up: :none, originator: "codex_cli_rs")

      assert %{"type" => "error", "status" => 429, "error" => %{"type" => "usage_limit_reached", "resets_in_seconds" => seconds}} = timeline.compaction_terminal
      assert seconds in 590..600, inspect(timeline, limit: :infinity)
      assert_terminal_compaction_refusal!(timeline, seconds, 429)
    end
  end

  # The row and the attempt record the provider's refusal as a 429 on the
  # session's plus seat; the attempt keeps no Pool advice; the resend met the
  # recorded turn before dispatch.
  defp assert_retryable_compaction_refusal!(timeline) do
    assert %{"type" => "response.failed", "status" => 503, "error" => %{"code" => "pinned_continuation_unavailable", "type" => "server_error"} = error} = timeline.compaction_terminal,
           inspect(timeline, limit: :infinity)

    refute Map.has_key?(error, "resets_at")
    refute Map.has_key?(timeline.compaction_terminal, "headers")
    assert timeline.compaction_row == {@compact_endpoint, "websocket", "failed", 429, "usage_limit_reached"}
    assert timeline.compaction_attempts == [{"failed", 429, :plus1}]
    refute Map.has_key?(timeline.compaction_attempt_metadata, "usage_limit")
    assert [line] = timeline.usage_limit_lines
    assert line =~ "status=429 error_code=usage_limit_reached advice=withheld answered_status=503"

    assert %{"type" => "error", "status" => 409, "error" => %{"code" => "duplicate_turn"}} = timeline.resend_terminal
    assert timeline.rows_before_https == 2
    assert timeline.plus1_websocket_requests == 2
  end

  # The attempt records the advice when the refusal is answered; the socket
  # renders the client's countdown a moment later, so the two can differ by a
  # second.
  defp assert_terminal_compaction_refusal!(timeline, client_seconds, answered_status) do
    assert timeline.compaction_row == {@compact_endpoint, "websocket", "failed", 429, "usage_limit_reached"}
    assert timeline.compaction_attempts == [{"failed", 429, :plus1}]
    assert %{"resets_at" => resets_at, "resets_in_seconds" => seconds} = timeline.compaction_attempt_metadata["usage_limit"]
    assert_in_delta resets_at, DateTime.to_unix(timeline.pro_reset_at), 1
    assert_in_delta seconds, client_seconds, 1
    assert [line] = timeline.usage_limit_lines
    assert line =~ "status=429 error_code=usage_limit_reached advice=pool resets_at=#{resets_at} resets_in_seconds=#{seconds} answered_status=#{answered_status}"
  end

  # `follow_up: :client_retries` (default) sends what the released client
  # sends after a retryable answer: the compaction again on the socket, full
  # history and no anchor, then over HTTPS. `:none` stops after the refusal.
  defp websocket_compaction_timeline!(pro_state, opts \\ []) do
    put_owner_forwarding!(false)
    follow_up = Keyword.get(opts, :follow_up, :client_retries)
    ids = new_ids()
    anchor_response_id = "resp_astra_turn_#{System.unique_integer([:positive])}"
    refusal_reset = reset_in(hd(@plus_reset_offsets))

    pool = astra_pool!(pro_state, plus1: :fresh, plus1_mode: refusing_session_seat_mode(anchor_response_id, refusal_reset))
    {_server, port} = start_public_endpoint_with_server!()

    {{turn_terminal, compaction_terminal, resend_terminal}, log} =
      with_info_log(fn ->
        websocket_session!(port, pool, ids, Keyword.get(opts, :originator, @desktop), &compaction_scenario!(&1, &2, &3, pool, ids, anchor_response_id, follow_up))
      end)

    [turn_row, compaction_row] = await_settled_rows!(pool, 2)
    await_exhausted!(hd(pool.plus).identity)
    plus1_websocket_requests = pool.plus |> hd() |> Map.fetch!(:upstream) |> FakeUpstream.requests() |> Enum.count(&(&1.method == "WEBSOCKET"))

    timeline = %{
      turn_terminal_type: turn_terminal["type"],
      compaction_terminal: compaction_terminal,
      resend_terminal: resend_terminal,
      rows_before_https: length(rows!(pool)),
      plus1_websocket_requests: plus1_websocket_requests,
      usage_limit_lines: log |> String.split("\n") |> Enum.filter(&(&1 =~ "websocket usage limit answered")),
      pro_reset_at: pro_reset_at(pool, pro_state),
      turn_partition: partition(turn_row),
      turn_attempts: attempts!(turn_row, pool),
      compaction_row: {compaction_row.endpoint, compaction_row.transport, compaction_row.status, compaction_row.response_status_code, compaction_row.last_error_code},
      compaction_partition: partition(compaction_row),
      compaction_skip_reason: summary(compaction_row)["held_back_skip_reason"],
      compaction_attempts: attempts!(compaction_row, pool),
      compaction_attempt_metadata: only_attempt!(compaction_row).response_metadata,
      pro_websocket_connections: FakeUpstream.websocket_connection_count(pool.pro.upstream)
    }

    timeline = if follow_up == :client_retries, do: Map.merge(timeline, https_compaction!(pool, ids, turn_row, compaction_row)), else: timeline
    CodexPooler.TestDiagnostics.puts(fn -> "partition held-back websocket timeline #{inspect(pro_state)}: " <> inspect(timeline, limit: :infinity) end)
    timeline
  end

  # The ordinary turn, the compaction anchored on it, then what the client
  # sends on the socket after the refusal.
  defp compaction_scenario!(conn, websocket, ref, pool, ids, anchor_response_id, follow_up) do
    turn = websocket_frame(ids, native_text_input("synthetic turn prompt"), :turn)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(turn))
    {conn, websocket, turn_terminal} = receive_terminal!(conn, websocket, ref)
    await_settled_rows!(pool, 1)

    compaction = ids |> websocket_frame([%{"type" => "compaction_trigger"}], :post_turn_compaction) |> Map.put("previous_response_id", anchor_response_id)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(compaction))
    {conn, websocket, compaction_terminal} = receive_terminal!(conn, websocket, ref)
    await_settled_rows!(pool, 2)

    {turn_terminal, compaction_terminal, client_resend!(conn, websocket, ref, ids, follow_up)}
  end

  defp client_resend!(_conn, _websocket, _ref, _ids, :none), do: nil

  defp client_resend!(conn, websocket, ref, ids, :client_retries) do
    resend = websocket_frame(ids, native_text_input("synthetic turn prompt") ++ [%{"type" => "compaction_trigger"}], :post_turn_compaction)
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(resend))
    {_conn, _websocket, resend_terminal} = receive_terminal!(conn, websocket, ref)
    resend_terminal
  end

  defp https_compaction!(pool, ids, turn_row, compaction_row) do
    http =
      post_native(pool, ids.session, @turn_endpoint, %{
        "model" => @model,
        "input" => native_text_input("synthetic turn prompt") ++ [%{"type" => "compaction_trigger"}],
        "stream" => true,
        "store" => false,
        "client_metadata" => %{"x-codex-turn-metadata" => compaction_turn_metadata(ids.session, "post_turn")}
      })

    assert [%{id: turn_id}, %{id: compaction_id}, http_row] = rows!(pool)
    assert {turn_id, compaction_id} == {turn_row.id, compaction_row.id}

    %{
      http: %{status: http.status, body: String.slice(http.resp_body, 0, 320)},
      http_row: {http_row.endpoint, http_row.transport, http_row.status, http_row.response_status_code, http_row.last_error_code},
      http_partition: partition(http_row),
      http_summary: summary(http_row),
      http_exclusions: exclusion_labels(http_row, pool),
      http_attempts: attempts!(http_row, pool),
      pro_usage_reads: usage_reads(pool.pro.upstream)
    }
  end

  defp only_attempt!(row) do
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^row.id))
    attempt
  end

  defp pro_reset_at(pool, {:exhausted, _seconds}) do
    pool.pro.identity
    |> QuotaWindows.list_quota_windows()
    |> Enum.find(&(&1.quota_scope == "account" and &1.window_kind == "primary"))
    |> Map.fetch!(:reset_at)
  end

  defp pro_reset_at(_pool, _pro_state), do: nil

  # One public websocket session as the released client opens it, with the
  # client's own `originator`; the connection is closed whatever the scenario
  # does.
  defp websocket_session!(port, pool, ids, originator \\ @desktop, scenario) do
    {conn, websocket, ref, _headers} =
      public_websocket_connect_with_request_headers!(port, pool.setup, "astra-ws-#{ids.session}", @turn_endpoint, [
        {"session-id", ids.session},
        {"thread-id", ids.session},
        {"x-client-request-id", ids.session},
        {"x-codex-window-id", "#{ids.session}:0"},
        {"originator", originator}
      ])

    try do
      scenario.(conn, websocket, ref)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp new_ids, do: %{session: Ecto.UUID.generate(), turn: Ecto.UUID.generate(), context_window: Ecto.UUID.generate()}

  defp put_owner_forwarding!(enabled?) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, enabled?)
  end

  # The session's plus seat serves the ordinary turn, then refuses the
  # compaction anchored on it with the provider's usage limit; both on the
  # first upstream connection.
  defp refusing_session_seat_mode(anchor_response_id, refusal_reset) do
    # provenance: observed released-client frame shapes (post-turn compaction capture pinned in backend_codex_websocket_post_turn_compaction_test.exs, provider usage-limit frame shape in partition_usage_limit_failover_test.exs); identifiers, text and reset synthetic
    FakeUpstream.strict_sequence([
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
        respond: completed_frames(anchor_response_id)
      ),
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => anchor_response_id, "input.0.type" => "compaction_trigger"}],
        respond: FakeUpstream.websocket_text_frames([usage_limit_frame(refusal_reset)])
      )
    ])
  end

  # The session's plus seat serves the opening turn and the tool-output
  # continuation anchored on it, both on the first upstream connection.
  defp anchored_session_seat_mode(opening_response_id) do
    # provenance: synthetic_adversarial (released-client response.create key set; tool-output continuation anchored with previous_response_id; identifiers and text synthetic)
    FakeUpstream.strict_sequence([
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]],
        respond: completed_frames(opening_response_id)
      ),
      FakeUpstream.expect_request(
        method: "WEBSOCKET",
        websocket_connection_ordinal: 1,
        json: [valid: true, equals: %{"type" => "response.create", "previous_response_id" => opening_response_id, "input.0.type" => "function_call_output"}],
        respond: completed_frames("#{opening_response_id}_continued")
      )
    ])
  end

  defp websocket_frame(ids, input, kind) do
    %{
      "type" => "response.create",
      "model" => @model,
      "instructions" => "synthetic instructions",
      "input" => input,
      "tools" => [],
      "tool_choice" => "auto",
      "parallel_tool_calls" => true,
      "store" => false,
      "stream" => true,
      "include" => ["reasoning.encrypted_content"],
      "prompt_cache_key" => ids.session,
      "client_metadata" => %{
        "session_id" => ids.session,
        "thread_id" => ids.session,
        "turn_id" => ids.turn,
        "x-codex-window-id" => "#{ids.session}:0",
        "x-codex-turn-metadata" => websocket_turn_metadata(ids, kind)
      }
    }
  end

  defp websocket_turn_metadata(ids, kind) do
    base = %{
      "session_id" => ids.session,
      "thread_id" => ids.session,
      "turn_id" => ids.turn,
      "root_turn_id" => ids.turn,
      "window_id" => "#{ids.session}:0",
      "window_number" => 0,
      "context_window_id" => ids.context_window,
      "thread_source" => "user",
      "turn_trigger" => "exec"
    }

    case kind do
      :turn ->
        CodexPooler.JSON.encode!(Map.merge(base, %{"request_kind" => "turn", "model" => @model}))

      :post_turn_compaction ->
        CodexPooler.JSON.encode!(
          Map.merge(base, %{
            "request_kind" => "compaction",
            "compaction" => %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => "post_turn", "strategy" => "memento"}
          })
        )
    end
  end

  defp completed_frames(response_id) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}),
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 20, "output_tokens" => 1, "total_tokens" => 21}}
      })
    ])
  end

  defp usage_limit_frame(reset_at) do
    resets_at = DateTime.to_unix(reset_at)

    CodexPooler.JSON.encode!(%{
      "type" => "error",
      "status" => 429,
      "error" => %{"type" => "usage_limit_reached", "message" => "synthetic provider text", "resets_at" => resets_at},
      "headers" => %{"x-codex-primary-used-percent" => "100", "x-codex-primary-window-minutes" => "300", "x-codex-primary-reset-at" => Integer.to_string(resets_at)}
    })
  end

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(text) do
      %{"type" => type} = terminal when type in ["response.completed", "response.failed", "response.incomplete", "error"] -> {conn, websocket, terminal}
      _progress -> receive_terminal!(conn, websocket, ref)
    end
  end

  # No completion signal survives the receive helpers, so the rows are the
  # authority (bounded poll, returns as soon as they settle).
  defp await_settled_rows!(pool, count) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_settled_rows!(pool, count, deadline)
  end

  defp await_settled_rows!(pool, count, deadline) do
    rows = rows!(pool)

    cond do
      length(rows) >= count and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"])) -> rows
      System.monotonic_time(:millisecond) >= deadline -> flunk("rows did not settle: #{inspect(Enum.map(rows, &{&1.endpoint, &1.status}))}")
      true -> Process.sleep(10) && await_settled_rows!(pool, count, deadline)
    end
  end

  # The refusal's quota evidence is recorded by the stream observers after the
  # row settles; the HTTPS compaction must see it as the reporter's did.
  defp await_exhausted!(identity) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms
    await_exhausted!(identity, deadline)
  end

  defp await_exhausted!(identity, deadline) do
    exhausted? =
      identity
      |> QuotaWindows.list_quota_windows()
      |> Enum.any?(&(&1.window_kind == "primary" and match?(%Decimal{}, &1.used_percent) and Decimal.compare(&1.used_percent, 100) != :lt))

    cond do
      exhausted? -> :ok
      System.monotonic_time(:millisecond) >= deadline -> flunk("the refusal's exhausted window was not recorded")
      true -> Process.sleep(10) && await_exhausted!(identity, deadline)
    end
  end

  defp open_circuits!(pool, assignment) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    for route_class <- ["proxy_http", "proxy_stream", "proxy_websocket", "proxy_compact"] do
      Repo.insert!(%RoutingCircuitState{
        pool_id: pool.setup.pool.id,
        pool_upstream_assignment_id: assignment.id,
        upstream_identity_id: assignment.upstream_identity_id,
        model_identifier: @model,
        route_class: route_class,
        status: "open",
        reason_code: "upstream_5xx",
        failure_count: 3,
        success_count: 0,
        opened_at: now,
        last_failure_at: now,
        next_probe_at: DateTime.add(now, 60, :second),
        metadata: %{"probe_in_flight_count" => 0},
        created_at: now,
        updated_at: now
      })
    end
  end

  # ---------------------------------------------------------------------------
  # Fixture
  # ---------------------------------------------------------------------------

  defp astra_pool!(pro_state, opts \\ []) do
    drift? = Keyword.get(opts, :drift?, true)

    plus_upstreams = for index <- 1..3, do: start_upstream(Keyword.get(opts, :"plus#{index}_mode", seat_routes(:plus)))
    pro_upstream = start_upstream(seat_routes(:pro))

    [first_upstream | other_plus_upstreams] = plus_upstreams

    setup =
      gateway_setup(first_upstream,
        quota?: false,
        compact?: true,
        exposed_model_id: @model,
        upstream_model_id: @upstream_model,
        display_name: "GPT Test Astra"
      )

    anchor = setup.assignment.created_at

    other_plus =
      other_plus_upstreams
      |> Enum.with_index(1)
      |> Enum.map(fn {upstream, offset} ->
        setup.pool
        |> gateway_upstream(upstream, "upstream-token-plus-#{offset + 1}", compact?: true)
        |> shift_created_at!(anchor, offset)
        |> Map.put(:upstream, upstream)
      end)

    plus = [%{assignment: setup.assignment, identity: setup.identity, upstream: first_upstream} | other_plus]

    pro =
      setup.pool
      |> gateway_upstream(pro_upstream, "upstream-token-pro", compact?: true)
      |> shift_created_at!(anchor, 10)
      |> Map.put(:upstream, pro_upstream)

    plus_resets = Enum.map(@plus_reset_offsets, &reset_in/1)

    for {{seat, reset_at}, index} <- plus |> Enum.zip(plus_resets) |> Enum.with_index(1) do
      put_plus_quota!(seat, reset_at, plus_state(opts, index))
    end

    put_pro_quota!(pro, pro_state)

    model = put_astra_sources!(setup.model, plus, pro, drift?)

    %{setup: %{setup | model: model}, plus: plus, pro: pro, plus_resets: plus_resets}
  end

  # `plus: :fresh` makes every plus seat routable; `plus1: :fresh` only the
  # first. A fresh plus seat reads 40% of the same 5-hour cycle its refusal
  # later reports exhausted.
  defp plus_state(opts, index) do
    cond do
      Keyword.get(opts, :plus) == :fresh -> :fresh
      index == 1 and Keyword.get(opts, :plus1) == :fresh -> :fresh
      true -> :exhausted
    end
  end

  defp put_plus_quota!(seat, reset_at, :fresh), do: prime_routing_quota!(seat.identity, %{reset_at: reset_at, used_percent: Decimal.new("40")})
  defp put_plus_quota!(seat, reset_at, :exhausted), do: prime_exhausted_routing_quota!(seat.identity, %{reset_at: reset_at})

  defp put_pro_quota!(_pro, :missing), do: :ok

  defp put_pro_quota!(pro, :fresh), do: prime_routing_quota!(pro.identity, %{reset_at: reset_in(2 * 3_600), used_percent: Decimal.new("12")})

  defp put_pro_quota!(pro, :weekly_only) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(pro.identity, [
               weekly_quota_window_attrs(%{used_percent: Decimal.new("20"), source: "codex_usage_api"})
             ])
  end

  # The seat served nothing of this model for a while (selection held it back)
  # and its last usage reading is older than the freshness window.
  defp put_pro_quota!(pro, :stale_idle) do
    assert {:ok, [_window]} =
             QuotaWindows.upsert_quota_windows(pro.identity, [
               primary_quota_window_attrs(%{
                 used_percent: Decimal.new("12"),
                 reset_at: reset_in(2 * 3_600),
                 source: "codex_usage_api",
                 observed_at: minutes_ago(20),
                 last_sync_at: minutes_ago(20)
               })
             ])
  end

  defp put_pro_quota!(pro, {:exhausted, seconds}), do: prime_exhausted_routing_quota!(pro.identity, %{reset_at: reset_in(seconds)})

  defp put_pro_quota!(pro, :model_window_exhausted), do: put_pro_quota_with_extra!(pro, model_window(@model, %{used_percent: Decimal.new("100")}))
  defp put_pro_quota!(pro, :other_model_window_exhausted), do: put_pro_quota_with_extra!(pro, model_window("gpt-test-other", %{used_percent: Decimal.new("100")}))
  defp put_pro_quota!(pro, :model_window_stale), do: put_pro_quota_with_extra!(pro, model_window(@model, %{observed_at: minutes_ago(20), last_sync_at: minutes_ago(20)}))

  defp put_pro_quota!(pro, :feature_window_exhausted) do
    put_pro_quota_with_extra!(pro, %{
      quota_key: "synthetic_feature",
      window_kind: "primary",
      window_minutes: 300,
      used_percent: Decimal.new("100"),
      reset_at: reset_in(3_600),
      source: "codex_usage_api",
      source_precision: "observed",
      quota_scope: "feature",
      quota_family: "synthetic_feature",
      freshness_state: "fresh"
    })
  end

  defp put_pro_quota!(pro, :account_window_stale_weekly_fresh) do
    assert {:ok, [_primary, _weekly]} =
             QuotaWindows.upsert_quota_windows(pro.identity, [
               primary_quota_window_attrs(%{used_percent: Decimal.new("12"), reset_at: reset_in(2 * 3_600), source: "codex_usage_api", observed_at: minutes_ago(20), last_sync_at: minutes_ago(20)}),
               weekly_quota_window_attrs(%{used_percent: Decimal.new("20"), source: "codex_usage_api"})
             ])
  end

  defp put_pro_quota_with_extra!(pro, extra) do
    assert {:ok, [_account, _extra]} =
             QuotaWindows.upsert_quota_windows(pro.identity, [
               primary_quota_window_attrs(%{used_percent: Decimal.new("12"), reset_at: reset_in(2 * 3_600), source: "codex_usage_api"}),
               extra
             ])
  end

  defp model_window(model, overrides) do
    Map.merge(
      %{
        quota_key: String.replace(model, "-", "_"),
        window_kind: "primary",
        window_minutes: 300,
        used_percent: Decimal.new("5"),
        reset_at: reset_in(3_600),
        source: "codex_usage_api",
        source_precision: "observed",
        quota_scope: "model",
        quota_family: "codex_model",
        model: model,
        freshness_state: "fresh"
      },
      overrides
    )
  end

  defp put_astra_sources!(model, plus, pro, drift?) do
    template = model.metadata |> Map.fetch!("source_assignment_models") |> Map.fetch!(hd(plus).assignment.id)
    plus_source = Map.merge(template, %{"supports_experimental_context" => false, "default_reasoning_level" => "medium"})

    pro_source =
      if drift?,
        do: Map.merge(template, %{"supports_experimental_context" => true, "default_reasoning_level" => "high"}),
        else: plus_source

    sources = plus |> Map.new(&{&1.assignment.id, plus_source}) |> Map.put(pro.assignment.id, pro_source)
    ids = Enum.map(plus, & &1.assignment.id) ++ [pro.assignment.id]

    model
    |> Ecto.Changeset.change(
      source_assignment_count: length(ids),
      metadata: model.metadata |> Map.put("source_assignment_ids", ids) |> Map.put("source_assignment_models", sources)
    )
    |> Repo.update!()
  end

  # Every seat answers the provider usage read (an available 5-hour window),
  # a remote compaction on both compaction routes, and an ordinary turn. The
  # Responses route serves the compaction SSE (also as websocket frames): the
  # ordinary-turn arms read only the status, the terminal and the attempt.
  defp seat_routes(kind) do
    {:path_json,
     %{
       @usage_path => {200, usage_payload(kind)},
       @turn_endpoint => v2_compaction_sse(kind),
       @compact_endpoint =>
         {200,
          %{
            "id" => "resp_astra_direct_compact_#{kind}",
            "object" => "response.compaction",
            "output" => [compaction_item(kind)]
          }}
     }}
  end

  defp usage_payload(kind) do
    %{
      "plan_type" => if(kind == :pro, do: "prolite", else: "plus"),
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 12,
          "limit_window_seconds" => 18_000,
          "reset_after_seconds" => 7_200,
          "reset_at" => DateTime.to_unix(reset_in(7_200))
        }
      }
    }
  end

  # A nested `path_json` mode must not be a 2-tuple (`{status, payload}`), so
  # the SSE route carries a header and is the 3-tuple `:sse_headers` mode.
  defp v2_compaction_sse(kind) do
    item = compaction_item(kind)

    completion = %{
      "type" => "response.completed",
      "response" => %{
        "id" => "resp_astra_v2_compact_#{kind}",
        "status" => "completed",
        "output" => [item],
        "usage" => %{"input_tokens" => 8, "output_tokens" => 3, "total_tokens" => 11}
      }
    }

    FakeUpstream.sse_stream(
      [
        {"response.output_item.done", %{"type" => "response.output_item.done", "item" => item}},
        "event: response.completed\ndata: #{CodexPooler.JSON.encode!(completion)}"
      ],
      done: false,
      headers: [{"x-synthetic-seat", Atom.to_string(kind)}]
    )
  end

  defp compaction_item(kind), do: %{"type" => "compaction", "encrypted_content" => "synthetic-astra-compaction-#{kind}"}

  defp shift_created_at!(upstream, anchor, seconds) do
    assignment = upstream.assignment |> Ecto.Changeset.change(created_at: DateTime.add(anchor, seconds, :second)) |> Repo.update!()
    %{upstream | assignment: assignment}
  end

  defp reset_in(seconds), do: DateTime.utc_now() |> DateTime.add(seconds, :second) |> DateTime.truncate(:second)
  defp minutes_ago(minutes), do: DateTime.utc_now() |> DateTime.add(-minutes * 60, :second) |> DateTime.truncate(:second)

  # ---------------------------------------------------------------------------
  # Requests
  # ---------------------------------------------------------------------------

  defp post_v2_compaction(pool, session) do
    post_native(pool, session, @turn_endpoint, %{
      "model" => @model,
      "input" => native_text_input("synthetic history before compaction") ++ [%{"type" => "compaction_trigger"}],
      "stream" => true,
      "store" => false,
      "client_metadata" => %{"x-codex-turn-metadata" => compaction_turn_metadata(session, "mid_turn")}
    })
  end

  defp post_direct_compaction(pool, session) do
    post_native(pool, session, @compact_endpoint, %{
      "model" => @model,
      "instructions" => "synthetic instructions",
      "input" => native_text_input("synthetic history before compaction")
    })
  end

  defp post_ordinary_turn(pool, session) do
    post_native(pool, session, @turn_endpoint, %{
      "model" => @model,
      "input" => native_text_input("synthetic ordinary prompt"),
      "stream" => true,
      "store" => false
    })
  end

  defp post_native(pool, session, path, body) do
    build_conn()
    |> put_req_header("authorization", pool.setup.authorization)
    |> put_req_header("content-type", "application/json")
    |> put_req_header("session-id", session)
    |> put_req_header("originator", @desktop)
    |> post(path, CodexPooler.JSON.encode!(body))
  end

  defp compaction_turn_metadata(session, phase) do
    CodexPooler.JSON.encode!(%{
      "session_id" => session,
      "thread_id" => session,
      "turn_id" => "#{session}-turn",
      "request_kind" => "compaction",
      "compaction" => %{"trigger" => "auto", "reason" => "context_limit", "implementation" => "responses_compaction_v2", "phase" => phase, "strategy" => "memento"}
    })
  end

  # ---------------------------------------------------------------------------
  # Observation
  # ---------------------------------------------------------------------------

  defp rows!(pool), do: Repo.all(from(r in Request, where: r.pool_id == ^pool.setup.pool.id, order_by: [asc: r.admitted_at]))

  defp only_row!(pool) do
    assert [row] = rows!(pool)
    row
  end

  defp summary(row), do: row.request_metadata["canonical_partition"] || %{}

  defp partition(row) do
    case row.request_metadata["canonical_partition"] do
      %{} = summary -> {summary["partition_count"], summary["selected_count"], summary["filtered_count"], summary["routable_selection"]}
      nil -> nil
    end
  end

  defp fallback_lines(log), do: log |> String.split("\n") |> Enum.filter(&(&1 =~ "canonical partition fallback"))

  defp attempts!(row, pool) do
    from(a in Attempt, where: a.request_id == ^row.id, order_by: [asc: a.attempt_number])
    |> Repo.all()
    |> Enum.map(&{&1.status, &1.upstream_status_code, label(&1.pool_upstream_assignment_id, pool)})
  end

  defp label(assignment_id, pool) do
    cond do
      assignment_id == pool.pro.assignment.id -> :pro
      index = Enum.find_index(pool.plus, &(&1.assignment.id == assignment_id)) -> :"plus#{index + 1}"
      true -> :unknown
    end
  end

  defp usage_reads(upstream), do: Enum.count(FakeUpstream.requests(upstream), &(&1.path == @usage_path))

  defp observed(conn, row, pool) do
    inspect(
      %{
        status: conn.status,
        body: String.slice(conn.resp_body, 0, 320),
        row: {row.endpoint, row.transport, row.status, row.response_status_code, row.last_error_code},
        canonical_partition: row.request_metadata["canonical_partition"],
        exclusions: exclusion_labels(row, pool),
        attempts: attempts!(row, pool),
        pro_usage_reads: usage_reads(pool.pro.upstream)
      },
      limit: :infinity
    )
  end

  defp exclusion_labels(row, pool) do
    row.request_metadata
    |> Map.get("candidate_exclusions")
    |> List.wrap()
    |> Enum.map(fn exclusion -> {label(exclusion["pool_upstream_assignment_id"], pool), Enum.map(exclusion["reasons"] || [], &Map.take(&1, ["code", "reason_codes", "window_kind", "reset_at"]))} end)
  end

  defp diagnostics(arm, conn, row, pool) do
    CodexPooler.TestDiagnostics.puts(fn -> "partition held-back #{arm}: " <> observed(conn, row, pool) end)
  end
end
