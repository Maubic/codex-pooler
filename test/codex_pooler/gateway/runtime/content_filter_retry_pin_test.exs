defmodule CodexPooler.Gateway.Runtime.ContentFilterRetryPinTest do
  # The two guards that keep a pinned content-filter retry on its account after
  # routing picked it (findings#318 row 318-3). The pin narrows the routed plan
  # to the pinned candidate (`ContentFilterRetryPin`), but two later steps reach
  # candidates outside that plan: a retryable failure moves the turn over the
  # remaining cohort, which also holds the route filter's deferred-recovery
  # candidates (`Dispatch.retry_remaining?`), and a pre-output usage limit can
  # move it to a held-back canonical partition (`PartitionFallback.available?/1`).
  # Each test drives the real dispatch over a plan or route state that offers
  # such a second candidate, with a stub transport in place of the provider.
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [gateway_setup: 1, prime_routing_quota!: 1, start_upstream: 1]
  import CodexPooler.PoolerFixtures, only: [active_upstream_assignment_fixture: 2]

  alias CodexPooler.Access
  alias CodexPooler.Accounting
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Dispatch
  alias CodexPooler.Gateway.Runtime.Dispatch.{Context, PartitionFallback, RouteState}
  alias CodexPooler.Repo

  @endpoint_path "/backend-api/codex/responses"

  # A pinned request's plan holds only its account, so in production the second
  # candidate of its remaining cohort comes from the route filter's deferred
  # recovery; here the plan is built before the pin is set, which offers the
  # same second candidate to the guard.
  for pinned? <- [true, false] do
    @tag pinned?: pinned?
    test "a retryable failure moves the turn to another candidate only when the request is not pinned (pinned #{pinned?})", context do
      %{setup: setup, auth: auth, candidates: candidates} = two_accounts!()
      payload = payload(setup)
      reserved = reserve!(auth, setup, payload, %{})
      assert {:ok, dispatch_context} = Context.new(context_input(auth, setup, payload, reserved, candidates, RouteState.new(%{visible_model: setup.model, candidates: candidates})))
      [{first, first_identity}, {second, _second_identity}] = dispatch_context.route_plan.candidates
      dispatch_context = if context.pinned?, do: put_pin(dispatch_context, first, first_identity), else: dispatch_context
      parent = self()

      result =
        Dispatch.dispatch(dispatch_context, fn selected ->
          send(parent, {:candidate, selected.assignment.id, selected.allow_retry?})
          if selected.allow_retry?, do: {:retry, :synthetic_retryable_failure}, else: {:ok, %{status: 200}}
        end)

      assert {:ok, %{status: 200}} = result

      if context.pinned? do
        assert_receive {:candidate, first_id, false}
        assert first_id == first.id
        refute_received {:candidate, _assignment_id, _allow_retry?}
      else
        assert_receive {:candidate, first_id, true}
        assert_receive {:candidate, second_id, false}
        assert {first_id, second_id} == {first.id, second.id}
      end
    end
  end

  for pinned? <- [true, false] do
    @tag pinned?: pinned?
    test "a held-back partition is offered only to a request that is not pinned (pinned #{pinned?})", context do
      %{setup: setup, auth: auth, candidates: [selected_candidate, held_back]} = two_accounts!()
      payload = payload(setup)
      {assignment, identity} = selected_candidate
      metadata = if context.pinned?, do: %{"native_content_filter_pin" => pin(assignment, identity)}, else: %{}
      reserved = reserve!(auth, setup, payload, metadata)
      route_state = %{visible_model: setup.model, candidates: [selected_candidate]} |> RouteState.new() |> RouteState.put_partition_fallback([held_back])
      assert {:ok, dispatch_context} = Context.new(context_input(auth, setup, payload, reserved, [selected_candidate], route_state))
      parent = self()

      assert {:ok, %{status: 200}} =
               Dispatch.dispatch(dispatch_context, fn selected ->
                 send(parent, {:fallback, selected.assignment.id, PartitionFallback.available?(selected)})
                 {:ok, %{status: 200}}
               end)

      assert_receive {:fallback, selected_id, available?}
      assert selected_id == assignment.id
      assert available? == not context.pinned?
    end
  end

  defp two_accounts! do
    first_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    second_upstream = start_upstream(FakeUpstream.json_response(%{"data" => []}))
    setup = gateway_setup(first_upstream)
    %{assignment: second, identity: second_identity} = active_upstream_assignment_fixture(setup.pool, %{account_label: "Pinned retry second account", base_url: FakeUpstream.url(second_upstream)})
    prime_routing_quota!(second_identity)
    source = setup.model.metadata["source_assignment_models"][setup.assignment.id]
    metadata = setup.model.metadata |> Map.update!("source_assignment_ids", &(&1 ++ [second.id])) |> put_in(["source_assignment_models", second.id], source)
    model = setup.model |> Ecto.Changeset.change(metadata: metadata) |> Repo.update!()
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    %{setup: %{setup | model: model}, auth: auth, candidates: [{setup.assignment, setup.identity}, {second, second_identity}]}
  end

  defp payload(setup), do: %{"model" => setup.model.exposed_model_id, "input" => [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic pinned retry"}]}], "stream" => true}

  defp reserve!(auth, setup, payload, metadata) do
    assert {:ok, reserved} = Accounting.reserve(auth, setup.model, payload, %{endpoint: @endpoint_path, transport: "http_sse", correlation_id: "pinned-retry-#{System.unique_integer([:positive])}", request_metadata: metadata})
    reserved
  end

  defp context_input(auth, setup, payload, reserved, candidates, route_state) do
    {:ok, policy} = Access.normalize_api_key_policy(auth.api_key)

    request_options =
      %{request_id: "pinned-retry-#{System.unique_integer([:positive])}", upstream_endpoint: @endpoint_path}
      |> RequestOptions.build(@endpoint_path, payload)
      |> RequestOptions.put_routing(requested_model: setup.model.exposed_model_id, effective_model: setup.model.exposed_model_id, api_key_policy: policy)

    %{auth: auth, endpoint: @endpoint_path, payload: payload, model: setup.model, reserved: reserved, candidates: candidates, request_options: request_options, route_state: route_state}
  end

  defp pin(assignment, identity), do: %{"version" => 1, "assignment_id" => assignment.id, "identity_id" => identity.id}

  defp put_pin(%Context{reserved: %{request: request} = reserved} = dispatch_context, assignment, identity) do
    request = %{request | request_metadata: Map.put(request.request_metadata || %{}, "native_content_filter_pin", pin(assignment, identity))}
    %{dispatch_context | reserved: %{reserved | request: request}}
  end
end
