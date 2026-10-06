defmodule CodexPoolerWeb.Runtime.BackendCodexHttpResampleTest do
  # A Codex client samples a turn again when the response it read completed
  # with `end_turn: false` (or input is pending). Over HTTP SSE the next request
  # of the turn is the previous input, then that response's completed output
  # items, then any harness items the turn loop recorded; with no tool result or
  # user message it derives the same claim as the request it follows, and the
  # fence refused it `409 duplicate_turn` while its websocket form is served
  # (findings#311). It is admitted now as the linked successor of a settled,
  # succeeded native HTTP SSE request whose receipt says it delivered
  # `response.completed`, when its input is that request's input (input-only
  # digest), then exactly the items the request wrote, then an allowed tail.
  #
  # Provenance: the follow-up rule, the re-sample input and the tail items are
  # source-derived (codex-rs `session/turn.rs` `run_turn`, `time_reminder.rs`,
  # `context/rollout_budget.rs`, `reasoning_effort.rs`); no wire capture of a
  # real re-sample exists. The request bodies follow the released client's HTTP
  # body; provider events, ids and texts are synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, first_event_terminal_sse: 2, gateway_setup: 2, native_text_input: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3, with_info_log: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, NativeResampledCompletion, Request, RequestClientRetryLink}
  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.NativeContinuationTail
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo

  @path "/backend-api/codex/responses"
  @moduletag capture_log: true

  # The shape the released client fills late in its turn metadata (findings#314 row 314-2).
  @workspaces %{"/synthetic/repository" => %{"associated_remote_urls" => %{"origin" => "https://example.com/sample-app.git"}, "latest_git_commit_hash" => String.duplicate("a", 40), "has_changes" => true}}

  # ---------------------------------------------------------------------------
  # The re-sample itself
  # ---------------------------------------------------------------------------

  for mode <- ["full", "lite"], phase <- ["commentary", "partial_answer"] do
    test "#{mode} #{phase}: the re-sample after a completed end_turn=false response is served once and linked" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a completion ending end_turn=false, then the re-sample's completion
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message(unquote(phase), "msg_p")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, unquote(mode))
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).resp_body =~ ~s("type":"response.completed")

      {served, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [client_message(unquote(phase), "msg_p")])) end)
      assert served.status == 200, "the re-sample was refused: #{inspect(rejection_lines(logs))}"
      assert served.resp_body =~ ~s("type":"response.completed")

      assert [predecessor, successor] = pool_requests(fixture.setup)
      assert predecessor.request_metadata["native_http_claim_arm"] == "opening"
      assert predecessor.request_metadata["native_http_input_count"] == 1
      assert %{"version" => 1, "digest" => digest} = predecessor.request_metadata["native_http_input_witness"]
      assert byte_size(digest) == 43
      assert successor.request_metadata["client_resend"] == %{"predecessor_request_id" => predecessor.id, "reason" => "failed_predecessor", "predecessor_shape" => "resampled_completion"}
      assert successor.request_metadata["native_http_input_count"] == 2
      assert String.starts_with?(predecessor.correlation_id, "codex-turn:")
      assert String.starts_with?(successor.correlation_id, "codex-request-retry:")
      assert linked?(predecessor, successor)
      assert receipt(predecessor)["end_turn"] == "false"
      assert_one_settlement_each!([predecessor, successor])
      assert FakeUpstream.count(upstream) == 2
      refute inspect({predecessor.request_metadata, successor.request_metadata}) =~ "synthetic resample request"
    end
  end

  # The provider's `end_turn` is never read: the request decides.
  for mode <- ["full", "lite"], end_turn <- [true, :absent] do
    test "#{mode}: the same re-sample after end_turn #{end_turn} is admitted the same way" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a completion with end_turn true or absent, then the re-sample's completion
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], unquote(end_turn)), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, unquote(mode))
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).status == 200
      assert post_native(fixture, request(fixture, opener ++ [client_message("commentary", "msg_p")])).status == 200
      assert [predecessor, successor] = pool_requests(fixture.setup)
      assert successor.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
      assert receipt(predecessor)["end_turn"] == if(unquote(end_turn) == :absent, do: "absent", else: "true")
    end
  end

  # A completed answer whose terminal the client never read: its grown retry has
  # the bytes of an empty-tail re-sample, so it is admitted and billed again,
  # like an identical resend of a settled response (the narrowed rule).
  test "a grown retry carrying exactly a delivered final answer is admitted as documented" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; a final answer, then its grown retry's completion
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("final_answer", "msg_p")], :absent), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], :absent)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert post_native(fixture, request(fixture, opener ++ [client_message("final_answer", "msg_p")])).status == 200
    assert [_predecessor, successor] = pool_requests(fixture.setup)
    assert successor.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
    assert FakeUpstream.count(upstream) == 2
  end

  # ---------------------------------------------------------------------------
  # The tail: what the client records between two sampling requests
  # ---------------------------------------------------------------------------

  @tails [
    time_reminder: [:time_reminder],
    rollout_budget: [:rollout_budget],
    world_state: [:world_state],
    configuration_update: [:configuration_update],
    tool_manifest: [:tool_manifest],
    combined: [:time_reminder, :rollout_budget, :world_state, :configuration_update]
  ]

  for mode <- ["full", "lite"], {label, items} <- @tails do
    test "#{mode}: a re-sample whose tail is #{label} is served" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; reasoning and a commentary message, then the re-sample's completion
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_reasoning("rs_p"), provider_message("commentary", "msg_p")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, unquote(mode))
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).status == 200

      resample = opener ++ [client_reasoning("rs_p"), client_message("commentary", "msg_p")] ++ Enum.map(unquote(items), &tail_item/1)
      {served, logs} = with_info_log(fn -> post_native(fixture, request(fixture, resample)) end)
      assert served.status == 200, "the re-sample was refused: #{inspect(rejection_lines(logs))}"
      assert [predecessor, successor] = pool_requests(fixture.setup)
      assert successor.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
      assert linked?(predecessor, successor)
    end
  end

  # ---------------------------------------------------------------------------
  # What is not a re-sample keeps its refusal
  # ---------------------------------------------------------------------------

  @negative_arms [
    changed_text: {"output", [:changed_text, :second]},
    swapped_order: {"output", [:second, :first]},
    one_item_dropped: {"count", [:first]},
    extra_assistant_item: {"tail", [:first, :second, :extra_assistant]},
    unknown_tail_item: {"tail", [:first, :second, :unknown]}
  ]

  for mode <- ["full", "lite"], {label, {stage, shape}} <- @negative_arms do
    test "#{mode}: a request with #{label} is not a re-sample and stays refused at #{stage}" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; one completion with two commentary messages
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_first"), provider_message("commentary", "msg_second")], false)])
        )

      fixture = fixture!(upstream, unquote(mode))
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).status == 200
      before = accounting_counts(fixture.setup)

      {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ Enum.map(unquote(shape), &negative_item/1))) end)
      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ "resend_disposition=terminal_predecessor"
      assert logs =~ "resample_check=#{unquote(stage)}"
      assert accounting_counts(fixture.setup) == before
      assert FakeUpstream.count(upstream) == 1

      if unquote(label) == :unknown_tail_item do
        # Only the closed vocabulary reaches the log: the type is `other` with a
        # fingerprint, and neither the type nor the content is printed.
        assert logs =~ "resample_item_type=other"
        assert logs =~ ~r/resample_item_type_fingerprint=[0-9a-f]{12}/
        assert logs =~ "resample_tail_index=0"
        refute logs =~ "sentinel"
      end

      if unquote(label) == :extra_assistant_item do
        assert logs =~ "resample_item_type=message"
        assert logs =~ "resample_item_role=assistant"
      end
    end
  end

  # A request that carries something only the client authors keeps the claim
  # and the policy it had: a user message moves the turn's progress (steered
  # claim), a tool result names its own payload claim, and addressed mail after
  # a preemptible item is a mailbox continuation.
  for {label, extra, expected} <- [{"a user message", :user, "steered_continuation"}, {"a tool result", :tool_result, "tool_continuation"}, {"addressed mail", :mail, "opening"}] do
    test "a re-sample-shaped request ending in #{label} keeps its own path" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a commentary completion, then the follow-up's completion
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, "full")
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).status == 200
      follow_up = opener ++ [client_message("commentary", "msg_p")] ++ follow_up_items(unquote(extra))
      assert post_native(fixture, request(fixture, follow_up)).status == 200

      assert [_predecessor, successor] = pool_requests(fixture.setup)
      assert successor.request_metadata["native_http_claim_arm"] == unquote(expected)
      refute successor.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
      if unquote(extra) == :mail, do: assert(successor.request_metadata["client_resend"]["reason"] == "failed_predecessor")
    end
  end

  # ---------------------------------------------------------------------------
  # Duplicates are still refused, and chains grow one node per re-sample
  # ---------------------------------------------------------------------------

  # The re-sample holds the claim derived from its predecessor, so a second copy
  # meets it: refused while it streams, and once it settled admitted and billed
  # as its identical resend, the rule every settled native HTTP request keeps
  # inside its retry window.
  for mode <- ["full", "lite"] do
    test "#{mode}: an identical copy of the re-sample is refused while it streams and chained once it settled" do
      release = make_ref()

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; the opener's completion, the re-sample's completion held after its first event, then the copy's
          FakeUpstream.strict_sequence([
            completed_sse("resp_p", [provider_message("commentary", "msg_p")], false),
            FakeUpstream.barrier_sse_stream(completed_events("resp_s", [provider_message("final_answer", "msg_s")], true), barrier_after: 1, notify: self(), release_ref: release),
            completed_sse("resp_copy", [provider_message("final_answer", "msg_copy")], true)
          ])
        )

      fixture = fixture!(upstream, unquote(mode))
      opener = native_text_input("synthetic resample request")
      resample = request(fixture, opener ++ [client_message("commentary", "msg_p")])
      assert post_native(fixture, request(fixture, opener)).status == 200

      streaming = Task.async(fn -> post_native(fixture, resample) end)
      assert_receive {:fake_upstream_chunk_barrier, 1, handler, ^release}, 15_000

      {refused, logs} =
        try do
          with_info_log(fn -> post_native(fixture, resample) end)
        after
          send(handler, {:fake_upstream_release_chunk, release})
        end

      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ "resend_disposition=active_predecessor"
      assert Task.await(streaming, 15_000).status == 200
      assert FakeUpstream.count(upstream) == 2
      assert [_predecessor, successor] = pool_requests(fixture.setup)

      {copy_conn, copy_logs} = with_info_log(fn -> post_native(fixture, resample) end)
      assert copy_conn.status == 200, "the settled copy was refused: #{inspect(rejection_lines(copy_logs))}"
      assert [_predecessor, %Request{id: successor_id}, copy] = pool_requests(fixture.setup)
      assert successor_id == successor.id
      assert copy.request_metadata["client_resend"]["predecessor_request_id"] == successor.id
      refute copy.request_metadata["client_resend"]["predecessor_shape"]
      assert linked?(successor, copy)
      assert FakeUpstream.count(upstream) == 3
    end
  end

  test "a second and a third re-sample chain through the previous one" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; four completions, each ending end_turn=false but the last
        FakeUpstream.strict_sequence([
          completed_sse("resp_p", [provider_message("commentary", "msg_1")], false),
          completed_sse("resp_s1", [provider_message("partial_answer", "msg_2")], false),
          completed_sse("resp_s2", [provider_reasoning("rs_3"), provider_message("commentary", "msg_3")], false),
          completed_sse("resp_s3", [provider_message("final_answer", "msg_4")], true)
        ])
      )

    fixture = fixture!(upstream, "full")
    p_input = native_text_input("synthetic resample request")
    s1_input = p_input ++ [client_message("commentary", "msg_1"), tail_item(:time_reminder)]
    s2_input = s1_input ++ [client_message("partial_answer", "msg_2")]
    s3_input = s2_input ++ [client_reasoning("rs_3"), client_message("commentary", "msg_3"), tail_item(:configuration_update)]

    for input <- [p_input, s1_input, s2_input, s3_input] do
      {conn, logs} = with_info_log(fn -> post_native(fixture, request(fixture, input)) end)
      assert conn.status == 200, "refused: #{inspect(rejection_lines(logs))}"
    end

    assert [p, s1, s2, s3] = pool_requests(fixture.setup)
    for {previous, next} <- [{p, s1}, {s1, s2}, {s2, s3}], do: assert(linked?(previous, next))
    for node <- [s1, s2, s3], do: assert(node.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion")
    assert_one_settlement_each!([p, s1, s2, s3])
    assert FakeUpstream.count(upstream) == 4
  end

  # ---------------------------------------------------------------------------
  # Every arm that can be re-sampled, and the checks the shape keeps on each
  # ---------------------------------------------------------------------------

  for mode <- ["full", "lite"], arm <- [:steered, :resume] do
    test "#{mode} #{arm}: the re-sample of a completed #{arm} request is served and linked" do
      upstream = start_upstream(arm_sequence(unquote(arm), [completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)]))
      fixture = fixture!(upstream, unquote(mode))
      {predecessor_input, predecessor} = arm_predecessor!(fixture, unquote(arm))

      {served, logs} = with_info_log(fn -> post_native(fixture, request(fixture, predecessor_input ++ [client_message("commentary", "msg_p"), tail_item(:time_reminder)])) end)
      assert served.status == 200, "the re-sample was refused: #{inspect(rejection_lines(logs))}"
      successor = pool_requests(fixture.setup) |> List.last()
      assert successor.request_metadata["client_resend"]["predecessor_request_id"] == predecessor.id
      assert successor.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
      assert linked?(predecessor, successor)
      assert String.starts_with?(predecessor.correlation_id, "codex-resume:")
      assert String.starts_with?(successor.correlation_id, "codex-request-retry:")
    end
  end

  # The shape skips only the exact-witness comparison: the predecessor's sealed
  # witness must stay eligible under the request's authorization epoch, the
  # node may carry only the chain's own edges, and it must be the request's
  # session (findings#311).
  for arm <- [:opening, :steered, :resume], guard <- [:foreign_link, :rotated_epoch, :other_session] do
    test "#{arm}: a re-sample is refused when the predecessor has #{guard}" do
      upstream = start_upstream(arm_sequence(unquote(arm), []))
      fixture = fixture!(upstream, "full")
      {predecessor_input, predecessor} = arm_predecessor!(fixture, unquote(arm))
      apply_guard!(fixture, predecessor, unquote(guard))
      before = accounting_counts(fixture.setup)

      {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, predecessor_input ++ [client_message("commentary", "msg_p")])) end)
      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ "resend_disposition=terminal_predecessor"

      expected_stage =
        case unquote(guard) do
          :foreign_link -> "verified"
          :rotated_epoch -> "authorization"
          :other_session -> "session"
        end

      assert logs =~ "resample_check=#{expected_stage}"
      assert accounting_counts(fixture.setup) == before
    end
  end

  test "a request that is not native HTTP SSE never takes the shape" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; one commentary completion
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, opener)).status == 200

    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [client_message("commentary", "msg_p")]) |> Map.delete("stream")) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    assert logs =~ "transport=http_json"
    refute logs =~ "resample_check="
  end

  # ---------------------------------------------------------------------------
  # Timing: a live or just-settled predecessor, and the retry window
  # ---------------------------------------------------------------------------

  # The released client sends the re-sample once it read `response.completed`,
  # which can be before the Pooler finished the predecessor (it relays the
  # terminal, then reads the upstream to its end and settles). A predecessor
  # still running is stepped over, as the walk does for any request of the
  # turn that has not settled: the re-sample is served at once, unlinked, and
  # the next re-sample still chains through it at its recorded count.
  test "a re-sample while the predecessor still streams is served unlinked and the next one chains through it" do
    release = make_ref()

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; the opener's completion held after its terminal event, then two completions
        FakeUpstream.strict_sequence([
          FakeUpstream.barrier_sse_stream(completed_events("resp_p", [provider_message("commentary", "msg_p")], false), barrier_after: 3, notify: self(), release_ref: release),
          completed_sse("resp_s1", [provider_message("commentary", "msg_s1")], false),
          completed_sse("resp_s2", [provider_message("final_answer", "msg_s2")], true)
        ])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    s1_input = opener ++ [client_message("commentary", "msg_p")]
    streaming = Task.async(fn -> post_native(fixture, request(fixture, opener)) end)
    assert_receive {:fake_upstream_chunk_barrier, 3, handler, ^release}, 15_000

    s1_conn =
      try do
        post_native(fixture, request(fixture, s1_input))
      after
        send(handler, {:fake_upstream_release_chunk, release})
      end

    assert s1_conn.status == 200
    assert Task.await(streaming, 15_000).status == 200
    assert [predecessor, s1] = pool_requests(fixture.setup)
    assert predecessor.status == "succeeded"
    assert s1.request_metadata["client_resend"] == nil
    refute linked?(predecessor, s1)

    {served, logs} = with_info_log(fn -> post_native(fixture, request(fixture, s1_input ++ [client_message("commentary", "msg_s1")])) end)
    assert served.status == 200, "the second re-sample was refused: #{inspect(rejection_lines(logs))}"
    assert [_predecessor, ^s1, s2] = pool_requests(fixture.setup)
    assert s2.request_metadata["client_resend"]["predecessor_request_id"] == s1.id
    assert s2.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
    assert linked?(s1, s2)
    assert FakeUpstream.count(upstream) == 3
  end

  # The delivery receipt is merged after the request settled; a re-sample that
  # meets the settled row before it refuses at `delivery`, and the client's next
  # retry is admitted.
  test "a re-sample before the predecessor's receipt is merged is refused at delivery, then admitted" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; a commentary completion, then the re-sample's completion
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert [predecessor] = pool_requests(fixture.setup)
    attempt = Repo.get_by!(Attempt, request_id: predecessor.id)
    {1, _} = Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id), set: [response_metadata: Map.delete(attempt.response_metadata, "downstream_delivery")])

    resample = request(fixture, opener ++ [client_message("commentary", "msg_p")])
    {refused, logs} = with_info_log(fn -> post_native(fixture, resample) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    assert logs =~ "resample_check=delivery"

    {1, _} = Repo.update_all(from(a in Attempt, where: a.id == ^attempt.id), set: [response_metadata: attempt.response_metadata])
    assert post_native(fixture, resample).status == 200
    assert FakeUpstream.count(upstream) == 2
  end

  test "a re-sample after the predecessor's retry window is refused retry_expired" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; one commentary completion
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert [predecessor] = pool_requests(fixture.setup)
    %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
    {1, _} = Repo.update_all(from(r in Request, where: r.id == ^predecessor.id), set: [completed_at: DateTime.add(db_now, -31, :second)])

    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [client_message("commentary", "msg_p")])) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    assert logs =~ "resend_disposition=retry_expired"
    assert FakeUpstream.count(upstream) == 1
  end

  # ---------------------------------------------------------------------------
  # Inputs the client may change between the two requests
  # ---------------------------------------------------------------------------

  # The opener's sealed witness binds the whole frame; the re-sample is proved
  # on the input alone, so turn metadata the client fills late (`workspaces`,
  # findings#314 row 314-2) and, in Full, top-level tools refreshed mid-turn do
  # not refuse it.
  for mode <- ["full", "lite"] do
    test "#{mode}: a re-sample whose turn metadata gained workspaces (and, in Full, whose tools changed) is served" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a commentary completion, then the re-sample's completion
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, unquote(mode))
      opener = native_text_input("synthetic resample request")
      tools = if unquote(mode) == "full", do: [%{"type" => "function", "name" => "synthetic_new_tool", "parameters" => %{"type" => "object"}}], else: []
      assert post_native(fixture, request(fixture, opener)).status == 200

      later = request(fixture, opener ++ [client_message("commentary", "msg_p")], document: %{"workspaces" => @workspaces}, tools: tools)
      {served, logs} = with_info_log(fn -> post_native(fixture, later) end)
      assert served.status == 200, "the re-sample was refused: #{inspect(rejection_lines(logs))}"
      assert pool_requests(fixture.setup) |> List.last() |> get_in([Access.key(:request_metadata), "client_resend", "predecessor_shape"]) == "resampled_completion"
    end
  end

  # A client that builds the Lite request itself carries the tool manifest and
  # the instructions as its first input items; unchanged they are part of the
  # proved prefix, changed (its tools changed) the request is not the
  # predecessor's input and fails closed at `witness`.
  for {label, manifest_tool, expected} <- [{"unchanged", "synthetic_tool", 200}, {"changed", "synthetic_other_tool", 409}] do
    test "a client-Lite prefix #{label} between the two requests" do
      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a commentary completion, then (when admitted) the re-sample's completion
          FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, "lite")
      prefix = fn tool -> [%{"type" => "additional_tools", "id" => "at_synthetic", "role" => "developer", "tools" => [%{"type" => "function", "name" => tool, "parameters" => %{"type" => "object"}}]}, developer_message("synthetic instructions")] end
      opener = prefix.("synthetic_tool") ++ native_text_input("synthetic resample request")
      assert post_native(fixture, %{request(fixture, opener) | "instructions" => ""}).status == 200

      resample = prefix.(unquote(manifest_tool)) ++ native_text_input("synthetic resample request") ++ [client_message("commentary", "msg_p")]
      {conn, logs} = with_info_log(fn -> post_native(fixture, %{request(fixture, resample) | "instructions" => ""}) end)
      assert conn.status == unquote(expected)
      if unquote(expected) == 409, do: assert(logs =~ "resample_check=witness")
    end
  end

  # ---------------------------------------------------------------------------
  # Output bounds and terminal classes
  # ---------------------------------------------------------------------------

  for {items, expected} <- [{4, 200}, {5, 409}] do
    test "a predecessor that wrote #{items} completed items #{if expected == 200, do: "is", else: "is not"} re-sampled" do
      outputs = Enum.map(1..unquote(items), &provider_message("commentary", "msg_#{&1}"))

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a completion with several commentary messages, then (when admitted) the re-sample's
          FakeUpstream.strict_sequence([completed_sse("resp_p", outputs, false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
        )

      fixture = fixture!(upstream, "full")
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).status == 200

      resample = opener ++ Enum.map(1..unquote(items), &client_message("commentary", "msg_#{&1}"))
      {conn, logs} = with_info_log(fn -> post_native(fixture, request(fixture, resample)) end)
      assert conn.status == unquote(expected)
      if unquote(expected) == 409, do: assert(logs =~ "resample_check=count" and logs =~ "resample_output_items=#{unquote(items)}")
    end
  end

  # Codex 0.159.0 and later treat `response.incomplete` with reason
  # `interrupted` as completed with `end_turn=false` (`sse/responses.rs`); its
  # re-sample stays refused, because the receipt names no delivered
  # `response.completed`.
  for reason <- ["interrupted", "max_output_tokens"] do
    test "a re-sample after response.incomplete (#{reason}) stays refused" do
      incomplete = %{"type" => "response.incomplete", "response" => %{"id" => "resp_p", "status" => "incomplete", "incomplete_details" => %{"reason" => unquote(reason)}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 3, "total_tokens" => 13}}}

      upstream =
        start_upstream(
          # provenance: synthetic_adversarial; a commentary message ending response.incomplete
          FakeUpstream.strict_sequence([FakeUpstream.sse_stream([{"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => provider_message("commentary", "msg_p")}}, {"response.incomplete", incomplete}])])
        )

      fixture = fixture!(upstream, "full")
      opener = native_text_input("synthetic resample request")
      assert post_native(fixture, request(fixture, opener)).status == 200

      {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [client_message("commentary", "msg_p")])) end)
      assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
      assert logs =~ ~r/resample_check=(delivery|settlement)/
      assert FakeUpstream.count(upstream) == 1
    end
  end

  # A predecessor the provider cut after it wrote an item never completed: the
  # client's grown retry of it carries the bytes of a re-sample but is judged
  # by the stream-cut rules it had, because the proof is asked only of a
  # succeeded request.
  test "a grown retry after a predecessor cut after its output keeps its refusal" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; a commentary message, then the connection drops before any terminal
        FakeUpstream.strict_sequence([FakeUpstream.abrupt_close_mid_stream(Enum.drop(completed_events("resp_p", [provider_message("commentary", "msg_p")], false), -1))])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    post_native(fixture, request(fixture, opener))
    assert [%Request{status: "failed"}] = pool_requests(fixture.setup)

    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [client_message("commentary", "msg_p")])) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    refute logs =~ "resample_check="
    assert FakeUpstream.count(upstream) == 1
  end

  # ---------------------------------------------------------------------------
  # Chains with other shapes
  # ---------------------------------------------------------------------------

  test "a re-sample after a mailbox continuation proves the continuation and is served" do
    reasoning = provider_reasoning("rs_p")

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; reasoning, the mailbox continuation's commentary, then the re-sample's completion
        FakeUpstream.strict_sequence([completed_sse("resp_p", [reasoning], false), completed_sse("resp_m", [provider_message("commentary", "msg_m")], false), completed_sse("resp_s", [provider_message("final_answer", "msg_s")], true)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    mailbox = opener ++ [client_reasoning("rs_p")] ++ follow_up_items(:mail)
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert post_native(fixture, request(fixture, mailbox)).status == 200

    {served, logs} = with_info_log(fn -> post_native(fixture, request(fixture, mailbox ++ [client_message("commentary", "msg_m")])) end)
    assert served.status == 200, "the re-sample was refused: #{inspect(rejection_lines(logs))}"
    assert [predecessor, continuation, resample] = pool_requests(fixture.setup)
    assert linked?(predecessor, continuation)
    assert linked?(continuation, resample)
    assert resample.request_metadata["client_resend"]["predecessor_shape"] == "resampled_completion"
  end

  # A re-sample is a chain node, so a provider overload during it must not fail
  # the turn: its exact retry follows it as a zero-output node (findings#314
  # row 314-1).
  test "a re-sample that failed at its first event is retried" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; a commentary completion, a first-event server_error, then a completion
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), first_event_terminal_sse("response.failed", "server_error"), completed_sse("resp_retry", [provider_message("final_answer", "msg_retry")], true)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    resample = request(fixture, opener ++ [client_message("commentary", "msg_p")])
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert post_native(fixture, resample).resp_body =~ "server_error"

    {served, logs} = with_info_log(fn -> post_native(fixture, resample) end)
    assert served.status == 200, "the retry was refused: #{inspect(rejection_lines(logs))}"
    assert [predecessor, failed, retry] = pool_requests(fixture.setup)
    assert failed.last_error_code == "server_error"
    assert linked?(predecessor, failed)
    assert linked?(failed, retry)
    assert FakeUpstream.count(upstream) == 3
  end

  # A successor that recorded no input count (a row an older release wrote)
  # leaves the edge unprovable; the shape never falls back to the request's
  # own length.
  test "a chain edge whose successor recorded no input count refuses" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; two commentary completions
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), completed_sse("resp_s1", [provider_message("commentary", "msg_s1")], false)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    s1_input = opener ++ [client_message("commentary", "msg_p")]
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert post_native(fixture, request(fixture, s1_input)).status == 200
    assert [_predecessor, s1] = pool_requests(fixture.setup)
    {1, _} = Repo.update_all(from(r in Request, where: r.id == ^s1.id), set: [request_metadata: Map.delete(s1.request_metadata, "native_http_input_count")])

    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, s1_input ++ [client_message("commentary", "msg_s1")])) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    assert logs =~ "resample_check=count"
    assert FakeUpstream.count(upstream) == 2
  end

  # Deferred (findings#311): after an identical resend of the opener, the
  # opener's edge to it cannot be re-proved by a grown request, because the
  # resend's count equals the opener's. The mailbox proof has the same gap.
  test "a re-sample after an identical resend of the opener stays refused (deferred)" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; the opener's and its identical resend's commentary completions
        FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false), completed_sse("resp_r", [provider_message("commentary", "msg_r")], false)])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, opener)).status == 200
    assert post_native(fixture, request(fixture, opener)).status == 200

    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [client_message("commentary", "msg_r")])) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    assert logs =~ "resample_check=count"
    assert FakeUpstream.count(upstream) == 2
  end

  # Deferred (findings#311): the chain is bounded like every resend chain;
  # the seventeenth consecutive re-sample is refused `chain_exhausted`.
  @tag slow: "drives seventeen native HTTP re-samples of one turn to cross the sixteen-node chain bound"
  test "sixteen consecutive re-samples are served and the seventeenth is refused chain_exhausted" do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; seventeen commentary completions, each ending end_turn=false
        FakeUpstream.strict_sequence(Enum.map(0..16, &completed_sse("resp_#{&1}", [provider_message("commentary", "msg_#{&1}")], false)))
      )

    fixture = fixture!(upstream, "full")

    final_input =
      Enum.reduce(0..16, native_text_input("synthetic resample request"), fn step, input ->
        {conn, logs} = with_info_log(fn -> post_native(fixture, request(fixture, input)) end)
        assert conn.status == 200, "request #{step} was refused: #{inspect(rejection_lines(logs))}"
        input ++ [client_message("commentary", "msg_#{step}")]
      end)

    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, final_input)) end)
    assert %{"error" => %{"code" => "duplicate_turn"}} = json_response(refused, 409)
    assert logs =~ "resend_disposition=chain_exhausted"
    assert FakeUpstream.count(upstream) == 17
  end

  # ---------------------------------------------------------------------------
  # The compatibility matrix entry, value by value against what the route did
  # ---------------------------------------------------------------------------

  test "the compatibility matrix re-sample entry is what this route serves and refuses" do
    contract = CompatibilityMatrix.by_slug!(:duplicate_turn_fence).duplicate_turn.resampled_completion
    bound = contract.max_output_items
    outputs = fn count, prefix -> Enum.map(1..count, &provider_message("commentary", "#{prefix}_#{&1}")) end
    resent = fn count, prefix -> Enum.map(1..count, &client_message("commentary", "#{prefix}_#{&1}")) end

    upstream =
      start_upstream(
        # provenance: synthetic_adversarial; per turn a completion with commentary messages, the first one ending end_turn=true, then the served re-sample
        FakeUpstream.strict_sequence([
          completed_sse("resp_bound", outputs.(bound, "msg_bound"), true),
          completed_sse("resp_bound_s", [provider_message("final_answer", "msg_bound_s")], true),
          completed_sse("resp_over", outputs.(bound + 1, "msg_over"), false),
          completed_sse("resp_window", outputs.(1, "msg_window"), false)
        ])
      )

    fixture = fixture!(upstream, "full")
    opener = native_text_input("synthetic resample request")

    # Exactly the bound of completed items after an end_turn=true completion; a
    # changed item first, then the exact re-sample with every tail kind and turn
    # metadata the client filled late.
    assert post_native(fixture, request(fixture, opener, turn_id: "matrix_bound")).status == 200
    [changed | rest] = resent.(bound, "msg_bound")
    changed = put_in(changed, ["content"], [%{"type" => "output_text", "text" => "synthetic text not written"}])
    {refused, logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ [changed | rest], turn_id: "matrix_bound")) end)
    assert json_response(refused, 409)["error"]["code"] == "duplicate_turn"
    assert logs =~ "resample_check=output"
    assert contract.requires_exact_delivered_output

    tail = [tail_item(:time_reminder), tail_item(:configuration_update), tail_item(:tool_manifest)]
    assert contract.tail_items == ["developer_message", "configuration_update", "developer_additional_tools"]
    assert contract.max_tail_items == NativeContinuationTail.max_items()
    resample = request(fixture, opener ++ resent.(bound, "msg_bound") ++ tail, turn_id: "matrix_bound", document: %{"workspaces" => @workspaces})
    {served, served_logs} = with_info_log(fn -> post_native(fixture, resample) end)
    assert served.status == 200, "the re-sample was refused: #{inspect(rejection_lines(served_logs))}"
    assert [predecessor, successor] = pool_requests(fixture.setup)
    assert contract.input_witness == :input_only
    assert {predecessor.transport, predecessor.status, receipt(predecessor)["terminal_class"]} == {contract.predecessor_transport, contract.predecessor_settlement, contract.predecessor_terminal}
    assert predecessor.request_metadata["native_http_claim_arm"] in contract.predecessor_claim_arms
    assert contract.predecessor_claim_arms == NativeResampledCompletion.claim_arms()
    assert receipt(predecessor)["end_turn"] == "true"
    refute contract.end_turn_read
    assert String.starts_with?(successor.correlation_id, contract.successor_prefix)
    assert successor.request_metadata["client_resend"]["predecessor_shape"] == contract.predecessor_shape

    # One completed item over the bound.
    assert post_native(fixture, request(fixture, opener, turn_id: "matrix_over")).status == 200
    {over, over_logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ resent.(bound + 1, "msg_over"), turn_id: "matrix_over")) end)
    assert json_response(over, 409)["error"]["code"] == "duplicate_turn"
    assert over_logs =~ "resample_check=count resample_output_items=#{bound + 1}"

    # One second past the retry window.
    assert post_native(fixture, request(fixture, opener, turn_id: "matrix_window")).status == 200
    window_predecessor = fixture.setup |> pool_requests() |> List.last()
    %{rows: [[db_now]]} = Repo.query!("SELECT clock_timestamp()")
    {1, _} = Repo.update_all(from(r in Request, where: r.id == ^window_predecessor.id), set: [completed_at: DateTime.add(db_now, -(contract.retry_window_seconds + 1), :second)])
    {expired, expired_logs} = with_info_log(fn -> post_native(fixture, request(fixture, opener ++ resent.(1, "msg_window"), turn_id: "matrix_window")) end)
    assert json_response(expired, 409)["error"]["code"] == "duplicate_turn"
    assert expired_logs =~ "resend_disposition=retry_expired"
    assert FakeUpstream.count(upstream) == 4
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp fixture!(upstream, mode) do
    setup = gateway_setup(upstream, compact?: true)
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    %{setup: setup, mode: mode, thread: Ecto.UUID.generate(), upstream: upstream}
  end

  # The released client's HTTP body: the canonical document in
  # `client_metadata` beside its flat copies.
  defp request(fixture, input, opts \\ []) do
    thread = fixture.thread
    window = Keyword.get(opts, :window, 0)
    turn_id = Keyword.get(opts, :turn_id, "synthetic_resample_turn")

    metadata =
      %{"thread_id" => thread, "session_id" => thread, "turn_id" => turn_id, "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:#{window}", "window_number" => window}
      |> Map.merge(Keyword.get(opts, :document, %{}))
      |> CodexPooler.JSON.encode!()

    %{
      "model" => fixture.setup.model.exposed_model_id,
      "instructions" => "synthetic instructions",
      "tools" => Keyword.get(opts, :tools, []),
      "parallel_tool_calls" => true,
      "input" => input,
      "stream" => true,
      "store" => false,
      "client_metadata" => %{"x-codex-turn-metadata" => metadata, "thread_id" => thread, "turn_id" => turn_id, "x-codex-window-id" => "#{thread}:#{window}"}
    }
  end

  defp post_native(fixture, payload) do
    window = payload["client_metadata"]["x-codex-window-id"]

    conn =
      build_conn()
      |> auth(fixture.setup)
      |> put_req_header("session-id", fixture.thread)
      |> put_req_header("thread-id", fixture.thread)
      |> put_req_header("x-codex-window-id", window)
      |> put_req_header("x-codex-turn-metadata", payload["client_metadata"]["x-codex-turn-metadata"])

    conn = if fixture.mode == "lite", do: put_req_header(conn, "x-openai-internal-codex-responses-lite", "true"), else: conn
    post(conn, @path, payload)
  end

  # What the provider writes and what the released client resends of the same
  # message: the client keeps only its typed model's fields and adds its local
  # passthrough metadata.
  defp provider_message(phase, id),
    do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => phase, "status" => "completed", "content" => [%{"type" => "output_text", "text" => "synthetic text of #{id}", "annotations" => [], "logprobs" => []}]}

  defp client_message(phase, id, text \\ nil),
    do: %{"type" => "message", "id" => id, "role" => "assistant", "phase" => phase, "content" => [%{"type" => "output_text", "text" => text || "synthetic text of #{id}"}], "internal_chat_message_metadata_passthrough" => %{"turn_id" => "synthetic_resample_turn"}}

  defp provider_reasoning(id), do: %{"type" => "reasoning", "id" => id, "summary" => [%{"type" => "summary_text", "text" => "synthetic summary of #{id}"}], "encrypted_content" => "synthetic-encrypted-#{id}"}
  defp client_reasoning(id), do: id |> provider_reasoning() |> Map.put("content", nil)

  defp completed_sse(response_id, items, end_turn), do: FakeUpstream.sse_stream(completed_events(response_id, items, end_turn))

  defp completed_events(response_id, items, end_turn) do
    response = %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 3, "total_tokens" => 13}}
    response = if end_turn == :absent, do: response, else: Map.put(response, "end_turn", end_turn)

    [{"response.created", %{"type" => "response.created", "response" => %{"id" => response_id, "status" => "in_progress"}}}] ++
      (items |> Enum.with_index() |> Enum.map(fn {item, index} -> {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => index, "item" => item}} end)) ++
      [{"response.completed", %{"type" => "response.completed", "response" => response}}]
  end

  # What leaves a completed predecessor of each arm, its response ending with
  # the commentary `msg_p`: the turn's opener; a steered request (a second user
  # message after the opener's commentary `msg_o`); or a post-compaction resume
  # (retained history, then the compaction item and nothing after it).
  defp arm_sequence(:steered, rest) do
    # provenance: synthetic_adversarial; the opener's and the steered request's commentary completions, then the scripted rest
    FakeUpstream.strict_sequence([completed_sse("resp_o", [provider_message("commentary", "msg_o")], false), completed_sse("resp_p", [provider_message("commentary", "msg_p")], false) | rest])
  end

  defp arm_sequence(_arm, rest) do
    # provenance: synthetic_adversarial; the predecessor's commentary completion, then the scripted rest
    FakeUpstream.strict_sequence([completed_sse("resp_p", [provider_message("commentary", "msg_p")], false) | rest])
  end

  defp arm_predecessor!(fixture, :opening) do
    input = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, input)).status == 200
    {input, fixture.setup |> pool_requests() |> List.last()}
  end

  defp arm_predecessor!(fixture, :steered) do
    opener = native_text_input("synthetic resample request")
    assert post_native(fixture, request(fixture, opener)).status == 200
    input = opener ++ [client_message("commentary", "msg_o")] ++ native_text_input("synthetic steered input")
    assert post_native(fixture, request(fixture, input)).status == 200
    predecessor = fixture.setup |> pool_requests() |> List.last()
    assert predecessor.request_metadata["native_http_claim_arm"] == "steered_continuation"
    {input, predecessor}
  end

  defp arm_predecessor!(fixture, :resume) do
    input = native_text_input("synthetic retained history") ++ [%{"type" => "compaction", "encrypted_content" => "synthetic-compaction"}]
    assert post_native(fixture, request(fixture, input)).status == 200
    predecessor = fixture.setup |> pool_requests() |> List.last()
    assert predecessor.request_metadata["native_http_claim_arm"] == "post_compaction_resume"
    {input, predecessor}
  end

  # A successor admitted under another claim (the owner's client-retry
  # preflight) leaves the predecessor a link outside the chain.
  defp apply_guard!(fixture, predecessor, :foreign_link) do
    now = DateTime.utc_now()

    foreign =
      Repo.insert!(%Request{pool_id: fixture.setup.pool.id, api_key_id: fixture.setup.api_key.id, model_id: fixture.setup.model.id, requested_model: fixture.setup.model.exposed_model_id, endpoint: @path, transport: "websocket", status: "succeeded", completed_at: now, usage_status: "usage_pending", correlation_id: "client-retry-v1:synthetic-#{System.unique_integer([:positive])}", admitted_at: now})

    Repo.insert!(%RequestClientRetryLink{predecessor_request_id: predecessor.id, successor_request_id: foreign.id, created_at: now})
  end

  defp apply_guard!(_fixture, predecessor, :rotated_epoch), do: Repo.update!(Ecto.Changeset.change(predecessor, native_client_retry_auth_epoch: predecessor.native_client_retry_auth_epoch + 1))

  defp apply_guard!(_fixture, predecessor, :other_session) do
    turn = Repo.get_by!(CodexTurn, request_id: predecessor.id)
    original = Repo.get!(CodexSession, turn.codex_session_id)
    other = Repo.insert!(%CodexSession{pool_id: original.pool_id, api_key_id: original.api_key_id, session_key: Ecto.UUID.generate(), status: "active", created_at: original.created_at, updated_at: original.updated_at})
    Repo.update!(Ecto.Changeset.change(turn, codex_session_id: other.id))
  end

  # The items `session/turn.rs` records before a sampling step: developer
  # fragments with their markers, a reasoning-effort change, and a developer
  # tool manifest (Codex main, unreleased).
  defp tail_item(:time_reminder), do: developer_message("<current_time_reminder>\nsynthetic time\n</current_time_reminder>")
  defp tail_item(:rollout_budget), do: developer_message("<rollout_budget>\nsynthetic remaining tokens\n</rollout_budget>")
  defp tail_item(:world_state), do: developer_message("<collaboration_mode>\nsynthetic mode update\n</collaboration_mode>")
  defp tail_item(:configuration_update), do: %{"type" => "configuration_update", "reasoning" => %{"effort" => "high"}}
  defp tail_item(:tool_manifest), do: %{"type" => "additional_tools", "role" => "developer", "tools" => [%{"type" => "function", "name" => "synthetic_tool", "parameters" => %{"type" => "object"}}]}

  defp developer_message(text), do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => text}]}

  defp negative_item(:first), do: client_message("commentary", "msg_first")
  defp negative_item(:second), do: client_message("commentary", "msg_second")
  defp negative_item(:changed_text), do: client_message("commentary", "msg_first", "synthetic text that was not written")
  defp negative_item(:extra_assistant), do: client_message("commentary", "msg_extra")
  defp negative_item(:unknown), do: %{"type" => "sentinel_unknown_item", "content" => "sentinel unknown content"}

  defp follow_up_items(:user), do: native_text_input("synthetic steered input")
  defp follow_up_items(:tool_result), do: [%{"type" => "function_call", "call_id" => "call_resample", "name" => "synthetic_tool", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "call_resample", "output" => "synthetic tool result"}]
  defp follow_up_items(:mail), do: [%{"type" => "agent_message", "author" => "/root/worker", "recipient" => "/root", "content" => [%{"type" => "input_text", "text" => "synthetic update"}]}]

  defp accounting_counts(setup) do
    ids = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, select: r.id))
    {length(ids), Repo.aggregate(from(a in Attempt, where: a.request_id in ^ids), :count), Repo.aggregate(from(l in LedgerEntry, where: l.request_id in ^ids), :count)}
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id, order_by: [asc: r.admitted_at]))

  defp linked?(predecessor, successor),
    do: Repo.exists?(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^predecessor.id and l.successor_request_id == ^successor.id))

  defp receipt(request), do: Repo.get_by!(Attempt, request_id: request.id).response_metadata["downstream_delivery"]

  defp assert_one_settlement_each!(requests) do
    for request <- requests do
      assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 1
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    end
  end

  defp rejection_lines(logs), do: logs |> String.split("\n") |> Enum.filter(&(&1 =~ "replay rejection"))
end
