defmodule CodexPoolerWeb.Runtime.PinnedWebsocketQuotaScenario do
  @moduledoc false

  import ExUnit.Assertions
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport
  @detection_timeout_ms 15_000

  def run(%{forwarding: forwarding, mode: mode, sibling_state: sibling_state} = context) do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled, nil)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, forwarding)
    reset = DateTime.to_unix(DateTime.utc_now()) + 3_600
    refusal = FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(%{"type" => "error", "status" => 429, "error" => if(Map.get(context, :resetless, false), do: %{"type" => "usage_limit_reached"}, else: %{"type" => "usage_limit_reached", "resets_at" => reset}), "headers" => %{"x-codex-secondary-used-percent" => "100", "x-codex-secondary-window-minutes" => "10080", "x-codex-secondary-reset-at" => Integer.to_string(reset), "x-codex-rate-limit-reached-type" => "rate_limit_reached"}})])

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          strict_native_request(1, completed_frames("resp_synthetic_quota_setup", 3, 1)),
          strict_native_request(1, refusal)
        ])
      )

    sibling_upstream = start_upstream(completed_frames("resp_synthetic_quota_recovered", 5, 2))
    setup = gateway_setup(upstream)
    sibling = gateway_upstream(setup.pool, sibling_upstream, "upstream-token-sibling", compact?: false)
    use_routing_strategy!(setup.pool, "bridge_ring", 2)

    put_serving_mode!(setup, mode, context)

    {_server, port} = start_public_endpoint_with_server!()
    window = Ecto.UUID.generate()
    peer = if Map.has_key?(context, :peer_node), do: BackendCodexWebsocketOwnerForwardingSupport.start_shared_peer_session_owner!(setup, %{accepted_turn_state: window}, context.peer_node, [sibling.identity])
    {conn, websocket, ref} = public_websocket_connect!(port, setup, window)
    thread = Ecto.UUID.generate()
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, turn_payload(setup, thread, "opening", native_text_input("synthetic opening")))
    {conn, websocket, _, first_terminal} = receive_until_terminal(conn, websocket, ref, [])
    assert %{"type" => "response.completed"} = first_terminal
    [first] = pool_requests(setup.pool.id)
    await_turn_settled!(first.id)

    case sibling_state do
      :healthy -> prime_routing_quota!(sibling.identity)
      :exhausted -> prime_exhausted_routing_quota!(sibling.identity, %{reset_at: DateTime.utc_now() |> DateTime.add(900, :second) |> DateTime.truncate(:second)})
      :unknown -> :ok
    end

    put_model_source_assignments!(setup.model, [setup.assignment, sibling.assignment])
    anchored = setup |> turn_payload(thread, "continuation", [%{"type" => "custom_tool_call_output", "call_id" => "call_synthetic", "output" => "synthetic result"}]) |> CodexPooler.JSON.decode!() |> Map.put("previous_response_id", "resp_synthetic_quota_setup") |> CodexPooler.JSON.encode!()
    {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, anchored)
    {conn, _websocket, _, terminal} = receive_until_terminal(conn, websocket, ref, [])
    Mint.HTTP.close(conn)
    assert FakeUpstream.count(upstream) == 2
    assert FakeUpstream.count(sibling_upstream) == 0
    [_, refused] = pool_requests(setup.pool.id)
    await_turn_settled!(refused.id)
    assert [refused_attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^refused.id))
    assert refused_attempt.upstream_status_code == 429
    assert refused_attempt.pool_upstream_assignment_id == setup.assignment.id

    if sibling_state == :exhausted do
      assert %{"type" => "error", "status" => 429, "error" => %{"type" => "usage_limit_reached", "resets_in_seconds" => seconds}} = terminal
      assert seconds in 895..900
    else
      assert %{"type" => "response.failed", "status" => 503, "error" => %{"code" => "pinned_continuation_unavailable"}} = terminal
      refute get_in(terminal, ["error", "resets_at"])
      refute Map.has_key?(terminal, "headers")
      refute Map.has_key?(refused_attempt.response_metadata, "usage_limit")
    end

    if sibling_state == :healthy do
      # The client alone reconstructs full history. The refused anchor is
      # never transported to a different upstream websocket.
      history =
        native_text_input("synthetic opening") ++
          [
            %{"type" => "custom_tool_call", "call_id" => "call_synthetic", "name" => "apply_patch", "input" => "synthetic patch"},
            %{"type" => "custom_tool_call_output", "call_id" => "call_synthetic", "output" => "synthetic result"}
          ]

      {retry_conn, retry_websocket, retry_ref} = public_websocket_connect!(port, setup, window)
      {retry_conn, retry_websocket} = public_websocket_send_text!(retry_conn, retry_websocket, retry_ref, turn_payload(setup, thread, "continuation", history))
      {retry_conn, _, _, recovered} = receive_until_terminal(retry_conn, retry_websocket, retry_ref, [])
      Mint.HTTP.close(retry_conn)
      assert %{"type" => "response.completed"} = recovered
      assert FakeUpstream.count(upstream) == 2
      assert FakeUpstream.count(sibling_upstream) == 1
      assert [%{json: moved}] = FakeUpstream.requests(sibling_upstream)
      refute Map.has_key?(moved, "previous_response_id")
      assert Enum.any?(moved["input"], &(&1["type"] == "custom_tool_call_output"))
      [_, _, completed] = pool_requests(setup.pool.id)
      await_turn_settled!(completed.id)
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^completed.id))
      assert attempt.pool_upstream_assignment_id == sibling.assignment.id
    end

    if peer do
      assert Repo.get!(CodexSession, peer.session.id).owner_instance_id == Atom.to_string(peer.node)
      assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(peer.session.id)
    end
  end

  defp put_serving_mode!(setup, mode, context) do
    if Map.has_key?(context, :peer_node) do
      timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      Repo.insert!(%CodexPooler.Pools.ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    else
      _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    end
  end

  # The released client's frame: its turn metadata names the thread and the
  # turn in `client_metadata` (Codex rust-v0.156.0), which is what keys the
  # request claim an identical resend meets.
  defp turn_payload(setup, thread_id, turn_id, input) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "client_metadata" => %{
        "x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"session_id" => thread_id, "thread_id" => thread_id, "turn_id" => turn_id, "request_kind" => "turn"})
      },
      "input" => input,
      "stream" => true,
      "generate" => true
    })
  end

  defp completed_frames(response_id, input_tokens, output_tokens) do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{
          "id" => response_id,
          "status" => "completed",
          "usage" => %{"input_tokens" => input_tokens, "output_tokens" => output_tokens, "total_tokens" => input_tokens + output_tokens}
        }
      })
    ])
  end

  defp pool_requests(pool_id) do
    Repo.all(from(r in Request, where: r.pool_id == ^pool_id, order_by: [asc: r.admitted_at]))
  end

  defp receive_until_terminal(conn, websocket, ref, seen_types) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal when type in ["response.completed", "response.failed", "error"] ->
        {conn, websocket, Enum.reverse(seen_types), terminal}

      %{"type" => type} ->
        receive_until_terminal(conn, websocket, ref, [type | seen_types])
    end
  end

  defp await_turn_settled!(request_id, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    case Repo.all(from(t in CodexTurn, where: t.request_id == ^request_id)) do
      [%CodexTurn{status: status} = turn] when status != "in_progress" ->
        turn

      turns ->
        if System.monotonic_time(:millisecond) < deadline do
          receive do
          after
            5 -> await_turn_settled!(request_id, deadline)
          end
        else
          flunk("expected the turn to settle, got #{inspect(Enum.map(turns, & &1.status))}")
        end
    end
  end
end
