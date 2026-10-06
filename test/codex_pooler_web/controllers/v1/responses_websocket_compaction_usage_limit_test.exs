defmodule CodexPoolerWeb.V1.ResponsesWebsocketCompactionUsageLimitTest do
  # A public `/v1` websocket compaction anchored on the connection's lineage
  # runs only on the upstream connection that produced the anchor, collected
  # whole (`compaction_result_mode: :public_websocket`). When the provider
  # refuses it with its usage limit before any output, the client gets the
  # usage-limit answer a public anchored turn gets (`ProviderUsageLimit.pool_frame/4`
  # over the request's own candidates, which for this pinned request are the
  # anchor's account): the wrapped 429 `usage_limit_reached` with that
  # account's return and `retry-after`. Before findings#305 row 498-5 the
  # collected refusal reached the client as the compaction result adapter's
  # `502 invalid_compaction_response`, "upstream compact response was not
  # valid JSON", in both topologies.
  #
  # Topology: one BEAM node, owner forwarding on (local owner) and off, Full
  # serving mode, FakeUpstream accounts, one public socket per arm.
  # Identifiers, prompts and provider frames are synthetic.
  use CodexPoolerWeb.ConnCase, async: false

  @moduletag capture_log: true

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [
      await_public_websocket_upgrade: 2,
      gateway_setup: 2,
      gateway_upstream: 4,
      mint_websocket_new!: 4,
      prime_routing_quota!: 1,
      public_websocket_receive_text!: 3,
      public_websocket_send_text!: 4,
      put_model_source_assignments!: 2,
      start_public_endpoint!: 0,
      start_upstream: 1
    ]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.TestAppEnv

  @detection_timeout_ms 15_000

  for forwarding <- [true, false] do
    @tag forwarding: forwarding
    test "an anchored compaction refused on the only account's usage limit answers the Pool's return (forwarding #{forwarding})", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, reset: reset, frames: frames} = refused_compaction!(forwarding, sibling?: false)

      assert [%{"type" => "error", "status" => 429, "stream_id" => "compaction-usage-limit", "error" => %{"type" => "usage_limit_reached", "code" => "quota_exhausted", "resets_at" => resets_at, "resets_in_seconds" => seconds}} = frame] = frames
      assert resets_at == reset
      assert seconds in 3_590..3_600
      assert %{"retry-after" => _seconds} = frame["headers"]
      assert_refused_row!(setup, upstream)
    end

    # The request as sent can run only on the anchor's connection, and a
    # public SDK client does not rebuild it without its anchor, so its answer
    # is that account's return even while another account could serve a new
    # request. The native socket answers for the Pool's capacity instead,
    # because the Codex client resends the compaction without the anchor and
    # then over HTTPS (partition_held_back_quota_refusal_test.exs).
    @tag forwarding: forwarding
    test "an anchored compaction refused on a usage limit while another account could serve answers the anchor account's return (forwarding #{forwarding})", %{forwarding: forwarding} do
      %{setup: setup, upstream: upstream, reset: reset, frames: frames} = refused_compaction!(forwarding, sibling?: true)

      assert [%{"type" => "error", "status" => 429, "stream_id" => "compaction-usage-limit", "error" => %{"type" => "usage_limit_reached", "code" => "quota_exhausted", "resets_at" => ^reset}}] = frames
      assert_refused_row!(setup, upstream)
    end
  end

  defp refused_compaction!(forwarding, sibling?: sibling?) do
    TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, false)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding)

    lineage = "resp_v1_usage_limit_lineage_#{forwarding}_#{sibling?}"
    reset = DateTime.to_unix(DateTime.utc_now()) + 3_600

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          native_turn(completed_frames(lineage), forbidden: ["previous_response_id"]),
          native_turn(usage_limit_frames(reset), equals: %{"previous_response_id" => lineage})
        ])
      )

    setup = gateway_setup(upstream, compact?: true)
    sibling = if sibling?, do: gateway_upstream(setup.pool, start_upstream(completed_frames("resp_v1_usage_limit_sibling")), "upstream-token-v1-usage-limit-sibling", compact?: true)
    port = start_public_endpoint!()
    client = connect!(port, setup, "v1-usage-limit-#{forwarding}-#{sibling?}-#{System.unique_integer([:positive])}")

    try do
      {client, frames} = client |> send_create!(setup, %{"input" => "synthetic lineage request"}) |> receive_until_terminal([])
      assert [%{"type" => "response.completed", "response" => %{"id" => ^lineage}}] = frames

      # The sibling joins the model's sources once the lineage is anchored on
      # the first account, as a sibling that reports capacity later would.
      if sibling do
        prime_routing_quota!(sibling.identity)
        put_model_source_assignments!(setup.model, [setup.assignment, sibling.assignment])
      end

      {client, frames} =
        client
        |> send_create!(setup, %{"previous_response_id" => lineage, "input" => tool_output_compaction_trigger_input(), "stream_id" => "compaction-usage-limit"})
        |> receive_until_terminal([])

      _client = client
      %{setup: setup, upstream: upstream, reset: reset, frames: frames}
    after
      Mint.HTTP.close(client.conn)
    end
  end

  # The compaction's row and attempt record the provider's refusal as a 429
  # on the account that produced the anchor, which got both requests.
  defp assert_refused_row!(setup, upstream) do
    assert [lineage_row, refused] = await_settled!(setup.pool.id, 2)
    assert {lineage_row.status, refused.endpoint, refused.transport, refused.status, refused.response_status_code, refused.last_error_code} == {"succeeded", "/v1/responses", "websocket", "failed", 429, "usage_limit_reached"}
    assert [{"failed", 429, assignment_id}] = Repo.all(from(a in Attempt, where: a.request_id == ^refused.id, select: {a.status, a.upstream_status_code, a.pool_upstream_assignment_id}))
    assert assignment_id == setup.assignment.id
    assert FakeUpstream.count(upstream) == 2
    assert :ok = FakeUpstream.verify!(upstream)
  end

  # provenance: provider usage-limit frame shape as in partition_usage_limit_failover_test.exs; identifiers, text and reset synthetic
  defp usage_limit_frames(reset) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{
        "type" => "error",
        "status" => 429,
        "error" => %{"type" => "usage_limit_reached", "message" => "synthetic provider text", "resets_at" => reset},
        "headers" => %{"x-codex-primary-used-percent" => "100", "x-codex-primary-window-minutes" => "300", "x-codex-primary-reset-at" => Integer.to_string(reset)}
      })
    ])
  end

  defp native_turn(respond, json_expectations) do
    FakeUpstream.expect_request(
      method: "WEBSOCKET",
      path: "/backend-api/codex/responses",
      websocket_connection_ordinal: 1,
      json: Keyword.merge([valid: true, equals: %{"type" => "response.create"}], json_expectations, fn :equals, base, extra -> Map.merge(base, extra) end),
      respond: respond
    )
  end

  defp completed_frames(response_id) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 6, "output_tokens" => 2, "total_tokens" => 8}}
      })
    ])
  end

  defp tool_output_compaction_trigger_input do
    [
      %{"type" => "function_call_output", "call_id" => "call_v1_usage_limit", "output" => "synthetic tool output"},
      %{"type" => "compaction_trigger"}
    ]
  end

  defp connect!(port, setup, turn_state) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"x-codex-turn-state", turn_state}, {"openai-beta", "responses_websockets=2026-02-06"}]
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/v1/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    %{conn: conn, websocket: websocket, ref: ref}
  end

  defp send_create!(client, setup, attrs) do
    payload = Map.merge(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "stream" => false, "store" => true, "generate" => true}, attrs)
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, CodexPooler.JSON.encode!(payload))
    %{client | conn: conn, websocket: websocket}
  end

  defp receive_until_terminal(client, seen) do
    {conn, websocket, text} = public_websocket_receive_text!(client.conn, client.websocket, client.ref)
    client = %{client | conn: conn, websocket: websocket}
    frame = CodexPooler.JSON.decode!(text)
    seen = [frame | seen]

    if frame["type"] in ["response.completed", "response.failed", "error"],
      do: {client, Enum.reverse(seen)},
      else: receive_until_terminal(client, seen)
  end

  defp pool_requests(pool_id), do: Repo.all(from(request in Request, where: request.pool_id == ^pool_id, order_by: [asc: request.admitted_at, asc: request.id]))

  # Each response task settles its request after its terminal reached the
  # client; no completion signal reaches the test, so poll the rows.
  defp await_settled!(pool_id, count) do
    deadline = System.monotonic_time(:millisecond) + @detection_timeout_ms

    Stream.repeatedly(fn -> pool_requests(pool_id) end)
    |> Enum.find(fn rows ->
      settled = length(rows) == count and Enum.all?(rows, &(&1.status not in ["accepted", "in_progress"]))
      if not settled and System.monotonic_time(:millisecond) >= deadline, do: flunk("requests did not settle")
      settled or (Process.sleep(10) && false)
    end)
  end
end
