defmodule CodexPoolerWeb.Runtime.BackendCodexContentFilterRefreshPeerTest do
  # The remote-owner arm of findings#330. A guided content-filter retry is
  # bound to the credential that produced the content-filtered response. When
  # the session's owner runs on another VM, the retry's provider handshake
  # there meets a 401, the owner refreshes the account's access token and
  # retries on the same assignment, and the retry's binding is checked on that
  # VM. The refresh renews the credential of the same provider account, so the
  # retry is served; it used to read as a replaced credential and every such
  # retry was refused 409 `duplicate_turn`. The module boots the peer VM once.
  #
  # Topology: the real public listener on this node, FakeUpstream, Full and
  # Lite, owner forwarding on, the session's owner and its provider connection
  # on the peer VM sharing the committed database, the released client's
  # upgrade headers and frame shape, synthetic text.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 2, native_text_input: 1, public_websocket_connect_with_request_headers!: 5, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint_with_server!: 0, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport, only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport,
    only: [enter_peer_owner_topology!: 0, start_shared_bridge_peer!: 0, start_shared_peer_window_owner!: 3]

  import Ecto.Query

  alias CodexPooler.Accounting.{Attempt, Request, RequestClientRetryLink}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  @moduletag capture_log: true
  @path "/backend-api/codex/responses"
  @timeout_ms 15_000
  @poll_ms 20

  setup_all do
    %{peer_node: start_shared_bridge_peer!()}
  end

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
    Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
    :ok
  end

  for mode <- ["full", "lite"] do
    @tag mode: mode
    test "#{mode} owner on another VM: a guided retry after a 401 and its account's token refresh there is served", %{peer_node: peer_node, mode: mode} do
      enter_peer_owner_topology!()
      terminal = %{"type" => "response.incomplete", "response" => %{"id" => "resp_synthetic_peer_cf", "status" => "incomplete", "incomplete_details" => %{"reason" => "content_filter"}, "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      completed = %{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_peer_refreshed", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 10, "output_tokens" => 1, "total_tokens" => 11}}}
      unauthorized = FakeUpstream.websocket_upgrade_error(%{"error" => %{"code" => "invalid_api_key"}}, status: 401, headers: [{"x-openai-authorization-error", "invalid_api_key"}])
      refreshed = FakeUpstream.json_response(%{"access_token" => "synthetic-peer-refreshed-token", "expires_in" => 3600})
      # provenance: synthetic_adversarial; the provider drops the owner's connection after the terminal, so the guided retry needs a new handshake on the peer
      upstream =
        start_upstream(
          FakeUpstream.strict_sequence([
            FakeUpstream.websocket_text_frames_then_abrupt_close([CodexPooler.JSON.encode!(terminal)]),
            FakeUpstream.expect_request(method: "GET", respond: unauthorized),
            FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: refreshed),
            FakeUpstream.websocket_text_frames([CodexPooler.JSON.encode!(completed)])
          ])
        )

      setup = gateway_setup(upstream, compact?: true)
      assert {:ok, _secret} = Upstreams.store_encrypted_secret(setup.identity, %{secret_kind: "refresh_token", plaintext: "synthetic-peer-refresh-token"})
      set_model_serving_mode!(model_serving_scope(), setup, mode)
      thread = Ecto.UUID.generate()
      owner = start_shared_peer_window_owner!(setup, "#{thread}:0", peer_node)
      assert node(owner.owner_pid) == peer_node
      {_server, port} = start_public_endpoint_with_server!()
      input = native_text_input("synthetic peer content filter")

      assert %{"type" => "response.incomplete"} = turn!(port, setup, thread, frame(setup, thread, input, mode))
      [first] = await_settled!(setup, 1)
      await_delivery!(first)
      bound = first |> final_attempt!() |> Map.fetch!(:response_metadata) |> get_in(["native_content_filter_source", "credential_epoch"])

      outcome = turn!(port, setup, thread, frame(setup, thread, input ++ [guidance()], mode))

      assert %{"type" => "response.completed"} = outcome, "the guided retry was refused: #{inspect(outcome)}"
      # The refused handshake is not a recorded request.
      assert [{"WEBSOCKET", @path}, {"POST", "/oauth/token"}, {"WEBSOCKET", @path}] = Enum.map(FakeUpstream.requests(upstream), &{&1.method, &1.path})
      assert [_first, %Request{status: "succeeded"} = served] = await_settled!(setup, 2)
      assert [served.id] == Repo.all(from(l in RequestClientRetryLink, where: l.predecessor_request_id == ^first.id, select: l.successor_request_id))
      assert served.request_metadata["native_content_filter_binding"]["credential_epoch"] == bound
      assert Repo.get!(UpstreamIdentity, setup.identity.id).metadata["credential_epoch"] > bound
      assert %Attempt{status: "succeeded", pool_upstream_assignment_id: assignment_id} = final_attempt!(served)
      assert assignment_id == setup.assignment.id
    end
  end

  # One request on a fresh downstream connection, closed once its terminal or error frame arrives.
  defp turn!(port, setup, thread, frame) do
    client = connect!(port, setup, thread)
    {conn, websocket} = public_websocket_send_text!(client.conn, client.websocket, client.ref, frame)
    {conn, _websocket, outcome} = receive_terminal!(conn, websocket, client.ref)
    _closed = Mint.HTTP.close(conn)
    outcome
  end

  # The released client's upgrade: the session keyed by its window, the session and thread ids.
  defp connect!(port, setup, thread) do
    headers = [{"session-id", thread}, {"thread-id", thread}, {"x-client-request-id", thread}, {"x-codex-window-id", "#{thread}:0"}]
    {conn, websocket, ref, _response_headers} = public_websocket_connect_with_request_headers!(port, setup, thread, @path, headers)
    on_exit(fn -> Mint.HTTP.close(conn) end)
    %{conn: conn, websocket: websocket, ref: ref}
  end

  # The released client's frame: the turn metadata document in `client_metadata`, the Lite marker for a Lite-served model.
  defp frame(setup, thread, input, mode) do
    metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => "synthetic_peer_turn", "request_kind" => "turn", "agent_name" => "/root", "window_id" => "#{thread}:0"}
    client_metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => "synthetic_peer_turn", "x-codex-window-id" => "#{thread}:0", "x-codex-turn-metadata" => CodexPooler.JSON.encode!(metadata)}
    client_metadata = if mode == "lite", do: Map.put(client_metadata, "ws_request_header_x_openai_internal_codex_responses_lite", "true"), else: client_metadata
    CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => setup.model.exposed_model_id, "input" => input, "stream" => true, "store" => false, "client_metadata" => client_metadata})
  end

  defp guidance, do: %{"type" => "message", "role" => "developer", "content" => [%{"type" => "input_text", "text" => "<content_filter_guidance>\nsynthetic guidance\n</content_filter_guidance>"}]}

  defp receive_terminal!(conn, websocket, ref) do
    {conn, websocket, text} = public_websocket_receive_text!(conn, websocket, ref)
    frame = CodexPooler.JSON.decode!(text)

    if frame["type"] in ["response.completed", "response.failed", "response.incomplete", "error"],
      do: {conn, websocket, frame},
      else: receive_terminal!(conn, websocket, ref)
  end

  defp final_attempt!(%Request{id: request_id}), do: Repo.one!(from(a in Attempt, where: a.request_id == ^request_id, order_by: [desc: a.attempt_number], limit: 1))

  # The guided retry is admitted only after the socket recorded the content-filter terminal as delivered.
  defp await_delivery!(request, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms

    cond do
      get_in(final_attempt!(request).response_metadata, ["downstream_delivery", "outcome"]) == "delivered" ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_delivery!(request, deadline)
        end

      true ->
        flunk("the content-filter terminal was never recorded as delivered")
    end
  end

  defp await_settled!(setup, count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @timeout_ms
    requests = Repo.all(from(request in Request, where: request.pool_id == ^setup.pool.id, order_by: [asc: request.admitted_at]))

    cond do
      length(requests) == count and Enum.all?(requests, &(&1.status not in ["accepted", "in_progress"])) ->
        requests

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          @poll_ms -> await_settled!(setup, count, deadline)
        end

      true ->
        flunk("the Pool's requests did not settle: #{inspect(Enum.map(requests, &{&1.status, &1.last_error_code}))}")
    end
  end
end
