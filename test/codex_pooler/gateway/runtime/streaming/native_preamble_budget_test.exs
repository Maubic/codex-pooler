defmodule CodexPooler.Gateway.Runtime.Streaming.NativePreambleBudgetTest do
  use CodexPoolerWeb.ConnCase, async: false
  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  alias CodexPooler.{Access, Accounting, FakeUpstream, Repo}
  alias CodexPooler.Accounting.{Attempt, LedgerEntry}
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Routing.BridgeRing
  alias CodexPooler.Gateway.Routing.RoutePlanInput
  alias CodexPooler.Gateway.Runtime.Dispatch.{RouteState, SelectedCandidateContext}
  alias CodexPooler.Gateway.Runtime.Streaming.{DownstreamStream, StreamDispatch}
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.SSEParser
  @endpoint "/backend-api/codex/responses"
  @bound 8_388_608
  @moduletag capture_log: true

  for schedule <- [:whole, :split], arm <- [:crossing_before_terminal, :two_runs, :delivered_before_crossing, :delivered_before_timeout] do
    test "exact async #{schedule} #{arm} keeps semantic order and local precedence" do
      run_scenario(unquote(schedule), unquote(arm))
    end
  end

  test "potential preamble overflow provenance is opt-in and websocket metadata keeps its observer budget" do
    event = "event: response.metadata\ndata: " <> CodexPooler.JSON.encode!(%{"type" => "response.metadata", "padding" => String.duplicate("x", 17 * 1_048_576)}) <> "\n\n"
    {legacy, legacy_state} = SSEParser.observe_blocks(SSEParser.new_observation_state(), event)
    assert [{:block, _, _}] = legacy
    assert legacy_state.overflow_count == 0
    {guarded, guarded_state} = SSEParser.observe_blocks(SSEParser.new_observation_state(preamble_policy?: true), event)
    assert [{:preamble_overflow, _, 8_388_608}] = guarded
    assert guarded_state.overflow_count == 1
    opts = RequestOptions.build(%{}, @endpoint, %{})
    state = DownstreamStream.initial_state(:websocket, opts)
    {wire, state, nil} = DownstreamStream.normalize_delivery(event, @endpoint, opts, state)
    assert :crypto.hash(:sha256, wire) == :crypto.hash(:sha256, event)
    assert state.codex_responses_sse_block_state.overflow_count == 0
  end

  defp run_scenario(schedule, arm) do
    {setup, _, _} = stream_retry_setup(FakeUpstream.sse_stream([]), FakeUpstream.sse_stream([]))
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic ordered preamble"), "stream" => true}
    opts = RequestOptions.build(%{upstream_endpoint: @endpoint}, @endpoint, payload) |> RequestOptions.put_routing(requested_model: setup.model.exposed_model_id, effective_model: setup.model.exposed_model_id, api_key_policy: policy)
    {:ok, reserved} = Accounting.reserve(auth, setup.model, payload, %{endpoint: @endpoint, transport: "http_sse", correlation_id: Ecto.UUID.generate(), request_metadata: %{}})
    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    candidates = [{setup.assignment, setup.identity}]
    route_state = RouteState.new(%{visible_model: setup.model, candidates: candidates}) |> RouteState.preload_routing_snapshots(auth, setup.model, opts)
    route_plan = BridgeRing.plan_route(%{auth: auth, model: setup.model, candidates: candidates, route_plan_input: RoutePlanInput.from_reserved(reserved), request_options: opts, route_state: route_state})
    context = %SelectedCandidateContext{auth: auth, endpoint: @endpoint, payload: payload, model: setup.model, reserved: reserved, request_options: opts, route_state: route_state, route_plan: route_plan, assignment: setup.assignment, identity: setup.identity, index: 0, retry_count: 0, allow_retry?: false, routing_attempt_metadata: %{}, route_class: opts.transport.route_class, attempt: attempt, started: System.monotonic_time(:millisecond)}
    {blocks, expected} = blocks(arm)
    chunks = if schedule == :whole, do: [IO.iodata_to_binary(blocks)], else: blocks
    ref = make_ref()
    Enum.each(chunks, &send(self(), {ref, {:data, &1}}))
    if arm == :delivered_before_timeout, do: send(self(), {ref, {:error, %Req.TransportError{reason: :timeout}}}), else: send(self(), {ref, :done})

    response = %Req.Response{
      status: 200,
      headers: %{"content-type" => ["text/event-stream"]},
      body: %Req.Response.Async{
        pid: self(),
        ref: ref,
        stream_fun: &parse/2,
        cancel_fun: fn _ ->
          send(self(), :source_cancelled)
          :ok
        end
      }
    }

    callbacks = %{finalization_callbacks: %{register_continuity: fn _, _, _ -> :ok end, stream_result: fn _, _ -> :ok end}, http_first_event_retry: fn _, _ -> fn _, _, _ -> flunk("local policy granted retry") end end}
    %{stream: stream} = StreamDispatch.streaming_result(response, context, callbacks)
    conn = build_conn() |> put_resp_content_type("text/event-stream") |> send_chunked(200)
    result = stream.(conn)
    assert {:ok, final_conn} = result
    assert :crypto.hash(:sha256, final_conn.resp_body) == :crypto.hash(:sha256, expected)
    request = Repo.reload!(reserved.request)
    saved = Repo.get!(Attempt, attempt.id)
    assert request.status == if(arm == :crossing_before_terminal, do: "failed", else: "succeeded")
    assert request.last_error_code == if(arm == :crossing_before_terminal, do: "upstream_response_too_large", else: nil)
    assert saved.response_metadata["downstream_delivery"]["terminal_class"] == if(arm == :crossing_before_terminal, do: "none", else: "response.completed")
    refute Map.has_key?(final_conn.private, :codex_pooler_withheld_retry_preamble)
    if arm == :two_runs, do: refute_received(:source_cancelled), else: assert_received(:source_cancelled)
    refute_received :source_cancelled
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"), :count) == 1
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1
    drain(ref)
  end

  defp blocks(:crossing_before_terminal), do: {[progress(div(@bound, 2)), progress(div(@bound, 2) + 1), terminal()], ""}

  defp blocks(:two_runs) do
    blocks = [progress(div(@bound, 2) + 100), delta(), progress(div(@bound, 2) + 100), terminal()]
    {blocks, blocks}
  end

  defp blocks(:delivered_before_timeout) do
    preamble = "data: " <> CodexPooler.JSON.encode!(%{"type" => "response.in_progress", "padding" => String.duplicate("x", @bound)})
    {[terminal(), preamble], terminal()}
  end

  defp blocks(:delivered_before_crossing), do: {[terminal(), progress(div(@bound, 2)), progress(div(@bound, 2) + 1)], terminal()}

  defp progress(bytes) do
    prefix = "event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"padding\":\""
    suffix = "\"}\n\n"
    prefix <> String.duplicate("x", bytes - byte_size(prefix <> suffix)) <> suffix
  end

  defp terminal, do: "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":4,\"output_tokens\":3,\"total_tokens\":7}}}\n\n"
  defp delta, do: "data: {\"type\":\"response.output_text.delta\",\"delta\":\"synthetic\"}\n\n"
  defp parse(ref, {ref, {:data, data}}), do: {:ok, [data: data]}
  defp parse(ref, {ref, {:error, reason}}), do: {:error, reason}
  defp parse(ref, {ref, :done}), do: {:ok, [:done]}

  defp drain(ref) do
    receive do
      {^ref, _} -> drain(ref)
    after
      0 -> :ok
    end
  end
end
