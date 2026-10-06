defmodule CodexPoolerWeb.Runtime.BackendCodexHttpSessionStartPinTest do
  # A native HTTP session's account pin (`codex_sessions.pool_upstream_assignment_id`)
  # became visible only when one of its turns succeeded: the settlement
  # transaction writes it. In production (findings#324) the first request of a
  # session recreated after its owner lease lapsed went to the thread's
  # previous account through the recreation's one-request preference, and the
  # session's next request, sent while the first still streamed or after the
  # client cut it for pending input, read the session unpinned and was routed on
  # its own. It moved to another account, which accepts the reasoning the client
  # replays and drops it (findings#318).
  #
  # The session now takes its pin from the account serving its client output,
  # when the relay releases that output (`SessionContinuity.AssignmentClaim`),
  # so the next request follows it in each window the trace found: while the
  # first request streams, once the client holds the first request's terminal
  # but the Pooler has not settled it, and after the client cut the first
  # request. A planned account that refused is never pinned, a pin is never
  # overwritten, and a pinned account that became unavailable falls through to
  # ordinary ordering, which is how a failover still moves the session.
  #
  # Topology: one node serving the real HTTP route on a listener, committed
  # rows, owner forwarding off (HTTP is never forwarded), one Pool with two
  # accounts serving the model, the Pool's model forced to Full and to Lite, a
  # FakeUpstream per account (`HttpSessionStartPinSupport` describes the
  # scenario). The routing strategy is `least_recent_success`, so an unpinned
  # request deterministically prefers the account without the thread's recent
  # success; production runs `bridge_ring`, whose rendezvous winner for a fresh
  # session id is random, and the mechanism does not depend on the strategy.
  # Synthetic text and identifiers.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog, only: [with_log: 1]
  import CodexPoolerWeb.Runtime.HttpSessionStartPinSupport

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Schemas.PoolUpstreamAssignment
  alias Ecto.Adapters.SQL.Sandbox

  @moduletag capture_log: true
  @budget 15_000

  setup context do
    CodexPooler.DataCase.stop_sandbox(context.sandbox_owner, context.sandbox_settings_cache)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    :ok = Sandbox.mode(Repo, :auto)
    :ok
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode}: a request sent while the session's first request streams follows the account serving it", %{mode: mode} do
      gate = make_ref()
      s = session_start!(mode, p: [held_output(gate, "resp_pin_stream_first"), completed_sse("resp_pin_stream_second")], b: [completed_sse("resp_pin_stream_moved")])
      {conn, ref, _item} = open_first!(s, "turn-stream-first")
      handler = await_gate!(gate)

      assert {200, body} = post!(s, "turn-stream-second")
      assert body =~ "response.completed"
      pin_while_first_streams = session_pin(s)

      release_gate(handler, gate)
      assert {200, _rest} = receive_all!(conn, ref, 200, "")
      assert_followed!(s)
      assert pin_while_first_streams == s.p.id
    end

    @tag mode: mode
    test "#{mode}: a request sent once the client holds the first request's terminal, before the Pooler settled it, follows its account", %{mode: mode} do
      gate = make_ref()
      s = session_start!(mode, p: [held_after_terminal(gate, "resp_pin_tail_first"), completed_sse("resp_pin_tail_second")], b: [completed_sse("resp_pin_tail_moved")])
      {conn, ref} = start_request!(s.port, s, "turn-tail-first")
      conn = receive_until!(conn, ref, "response.completed")
      handler = await_gate!(gate)

      # The client holds the terminal; the Pooler waits for the provider's end
      # of stream and has settled nothing.
      assert [_predecessor, %Request{status: "in_progress"}] = requests(s)
      assert {200, body} = post!(s, "turn-tail-second")
      assert body =~ "response.completed"
      pin_before_first_settled = session_pin(s)

      release_gate(handler, gate)
      assert {200, _rest} = receive_all!(conn, ref, 200, "")
      assert_followed!(s)
      assert pin_before_first_settled == s.p.id
    end

    @tag mode: mode
    test "#{mode}: a request sent after the client cut the session's first request follows the account that served it", %{mode: mode} do
      gate = make_ref()
      s = session_start!(mode, p: [cut_output(gate, "resp_pin_cut_first"), completed_sse("resp_pin_cut_second")], b: [completed_sse("resp_pin_cut_moved")])
      handler = consume_and_cut!(s, "turn-cut-first", gate)
      release_gate(handler, gate)
      assert [_predecessor, %Request{status: "failed", last_error_code: "client_disconnected"}] = await_settled!(s, 2)
      pin_after_cut = session_pin(s)

      assert {200, body} = post!(s, "turn-cut-second")
      assert body =~ "response.completed"
      assert_followed!(s)
      assert pin_after_cut == s.p.id
    end

    @tag mode: mode
    test "#{mode}: a first request its planned account refused pins the account that served it, never the refusing one", %{mode: mode} do
      gate = make_ref()
      s = session_start!(mode, p: [refused_first_event("resp_pin_refused_first")], b: [held_output(gate, "resp_pin_refused_served")])
      {conn, ref, _item} = open_first!(s, "turn-refused-first")
      handler = await_gate!(gate)

      assert session_pin(s) == s.b.id
      release_gate(handler, gate)
      assert {200, _rest} = receive_all!(conn, ref, 200, "")
      [_predecessor, first] = await_settled!(s, 2)
      assert first.status == "succeeded"
      assert attempt_trail(first) == [{s.p.id, "retryable_failed"}, {s.b.id, "succeeded"}]
      assert session_pin(s) == s.b.id
    end

    @tag mode: mode
    test "#{mode}: a first request every account refused leaves the session unpinned", %{mode: mode} do
      s = session_start!(mode, p: [refused_first_event("resp_pin_all_refused_p")], b: [refused_first_event("resp_pin_all_refused_b")])
      assert {status, _body} = post!(s, "turn-all-refused")
      assert status in [200, 503]
      [_predecessor, first] = await_settled!(s, 2)
      assert first.status == "failed"
      assert Enum.map(attempt_trail(first), &elem(&1, 0)) == [s.p.id, s.b.id]
      assert session_pin(s) == nil
    end

    # Both requests attach and dispatch before either account served (the
    # window the claim cannot close: neither has a served account yet). The
    # first serves first and claims; the second's claim finds that pin and
    # writes nothing, and its own completion then writes the outcome as every
    # success does.
    @tag mode: mode
    test "#{mode}: of two requests dispatched before either served, the later claim never overwrites the first", %{mode: mode} do
      first_gate = make_ref()
      second_gate = make_ref()
      s = session_start!(mode, p: [held_before_output(first_gate, "resp_pin_race_first")], b: [held_before_output(second_gate, "resp_pin_race_second")])
      watch_claims!(s)

      first = start_client!(fn -> post!(s, "turn-race-first") end)
      first_handler = await_gate!(first_gate)
      second = start_client!(fn -> post!(s, "turn-race-second") end)
      second_handler = await_gate!(second_gate)
      assert session_pin(s) == nil

      release_gate(first_handler, first_gate)
      assert {200, _body} = await_client!(first)
      assert_receive {:session_claim, :p, 1}, @budget

      release_gate(second_handler, second_gate)
      assert_receive {:session_claim, :b, 0}, @budget
      assert {200, _body} = await_client!(second)
      [_predecessor, served_first, served_second] = await_settled!(s, 3)
      assert {served_account(served_first), served_account(served_second)} == {s.p.id, s.b.id}
      assert session_pin(s) == s.b.id
    end

    @tag mode: mode
    test "#{mode}: an established pin is never overwritten while a failover serves the turn, and the outcome moves it", %{mode: mode} do
      gate = make_ref()
      s = session_start!(mode, [p: [refused_first_event("resp_pin_failover_refused")], b: [held_output(gate, "resp_pin_failover_served")]], keep_session?: true)
      assert session_pin(s) == s.p.id
      {conn, ref, _item} = open_first!(s, "turn-failover")
      handler = await_gate!(gate)

      assert session_pin(s) == s.p.id
      release_gate(handler, gate)
      assert {200, _rest} = receive_all!(conn, ref, 200, "")
      [_predecessor, moved] = await_settled!(s, 2)
      assert attempt_trail(moved) == [{s.p.id, "retryable_failed"}, {s.b.id, "succeeded"}]
      assert session_pin(s) == s.b.id
    end

    @tag mode: mode
    test "#{mode}: a pinned account that became unavailable falls through to the other account", %{mode: mode} do
      gate = make_ref()
      s = session_start!(mode, p: [cut_output(gate, "resp_pin_unavailable_first")], b: [completed_sse("resp_pin_unavailable_second")])
      handler = consume_and_cut!(s, "turn-unavailable-first", gate)
      release_gate(handler, gate)
      _settled = await_settled!(s, 2)
      assert session_pin(s) == s.p.id

      Repo.update_all(from(a in PoolUpstreamAssignment, where: a.id == ^s.p.id), set: [eligibility_status: PoolUpstreamAssignment.ineligible_status()])
      assert {200, body} = post!(s, "turn-unavailable-second")
      assert body =~ "response.completed"
      [_predecessor, _cut, second] = await_settled!(s, 3)
      assert served_account(second) == s.b.id
      assert second.request_metadata["routing"]["session_preference_kind"] == "pinned"
      assert second.request_metadata["routing"]["session_preference_status"] == "candidate_unavailable"
      assert session_pin(s) == s.b.id
    end
  end

  # A database error in the claim (a trigger raising `lock_not_available` for the
  # claim's statement alone: every other pin writer also moves
  # `last_heartbeat_at`) costs the claim and never the stream. The turn is
  # served, the warning names only the session and the condition, and the
  # turn's success then pins the session as before.
  test "full: a database error in the claim leaves the stream intact and logs a bounded warning" do
    gate = make_ref()
    s = session_start!("full", p: [held_output(gate, "resp_pin_claim_error")], b: [completed_sse("resp_pin_claim_error_unused")])
    fail_claims!(s)

    {{pin_while_streaming, response}, log} =
      with_log(fn ->
        {conn, ref, _item} = open_first!(s, "turn-claim-error")
        handler = await_gate!(gate)
        pin_while_streaming = session_pin(s)
        release_gate(handler, gate)
        {pin_while_streaming, receive_all!(conn, ref, 200, "")}
      end)

    assert pin_while_streaming == nil
    assert {200, body} = response
    assert body =~ "response.completed"
    [_predecessor, first] = await_settled!(s, 2)
    assert first.status == "succeeded"
    assert [line] = for(line <- String.split(log, "\n"), line =~ "gateway session assignment claim failed", do: line)
    assert line =~ "gateway session assignment claim failed codex_session_id=#{first.request_metadata["codex_session_id"]} reason_code=lock_not_available"
    refute line =~ s.p.id
    assert session_pin(s) == s.p.id
  end

  defp fail_claims!(s) do
    name = "w324_claim_gate_#{System.unique_integer([:positive])}"

    on_exit(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{name} ON codex_sessions")
      Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
    end)

    Repo.query!("CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic claim failure' USING ERRCODE = '55P03'; END $$")

    Repo.query!(
      "CREATE TRIGGER #{name} BEFORE UPDATE OF pool_upstream_assignment_id ON codex_sessions FOR EACH ROW " <>
        "WHEN (NEW.pool_id = '#{s.setup.pool.id}'::uuid AND OLD.pool_upstream_assignment_id IS NULL AND NEW.last_heartbeat_at IS NOT DISTINCT FROM OLD.last_heartbeat_at) " <>
        "EXECUTE FUNCTION #{name}()"
    )
  end
end
