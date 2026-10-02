defmodule CodexPooler.Gateway.Transports.OwnerAccountingSeed do
  @moduledoc false
  import ExUnit.Assertions
  import CodexPooler.PoolerFixtures

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.WebsocketOwnerBinding
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{CodexSession, SessionContinuity}
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Pools.Pool
  alias CodexPooler.ProviderCreditsDispatchSupport
  alias CodexPooler.Repo

  def submit(owner, downstream, transport_request) do
    state = :sys.get_state(owner)

    with {:ok, id} <- Ecto.UUID.cast(state.codex_session_id),
         %CodexSession{} = session <- Repo.get(CodexSession, id) do
      submit_accounted(owner, downstream, transport_request, session)
    else
      _transport_only ->
        WebsocketOwnerSession.submit_request(owner, downstream, ProviderCreditsDispatchSupport.wire_request!(transport_request))
    end
  end

  defp submit_accounted(owner, downstream, transport_request, session) do
    pool = Repo.get!(Pool, session.pool_id)
    key = Repo.get!(APIKey, session.api_key_id)

    auth = %{
      pool: pool,
      api_key: key,
      pool_id: pool.id,
      api_key_id: key.id,
      key_prefix: key.key_prefix
    }

    %{assignment: assignment, identity: identity} = upstream_assignment_fixture(pool)

    model =
      model_fixture(pool, %{
        exposed_model_id: "seed-model-#{System.unique_integer([:positive])}",
        metadata: %{"source_assignment_ids" => [assignment.id]}
      })

    request =
      request_fixture(auth, %{
        model_id: model.id,
        transport: "websocket",
        status: "in_progress",
        usage_status: "usage_pending",
        completed_at: nil,
        response_status_code: nil
      })

    reservation =
      ledger_entry_fixture(request, %{
        entry_kind: "reservation",
        amount_status: "recorded",
        usage_status: "usage_pending",
        attempt_id: nil,
        pool_upstream_assignment_id: assignment.id,
        upstream_identity_id: assignment.upstream_identity_id,
        model_id: model.id
      })

    reservation
    |> Ecto.Changeset.change(source_event_id: "request:#{request.id}:reservation")
    |> Repo.update!()

    attempt =
      attempt_fixture(request, assignment, %{
        status: "in_progress",
        completed_at: nil,
        upstream_status_code: nil,
        usage_status: "usage_pending"
      })

    opts =
      RequestOptions.for_websocket(%{
        codex_session: session,
        websocket_owner_forwarding_enabled?: true,
        websocket_owner_lease_token: session.owner_lease_token,
        websocket_owner_instance_id: session.owner_instance_id,
        websocket_owner_proxy_instance_id: Atom.to_string(node()),
        websocket_owner_downstream_epoch: downstream.epoch
      })

    assert {:ok, _turn} = SessionContinuity.start_codex_turn(session, request, opts)
    assert {:ok, _bound} = WebsocketOwnerBinding.bind(auth, request, attempt, opts)
    transport_request = %{transport_request | request_id: request.id, attempt_id: attempt.id}
    transport_request = ProviderCreditsDispatchSupport.wire_request!(transport_request, identity: identity, pool: pool)
    result = WebsocketOwnerSession.submit_request(owner, downstream, transport_request)
    assert {:ok, _} = result

    assert {:ok, _} =
             AttemptSettlement.finalize_success(
               request,
               attempt,
               %{status: "usage_unknown", source: "fixture_cleanup"},
               %{response_status_code: 200}
             )

    assert %{status: "succeeded"} =
             Repo.get_by!(CodexPooler.Gateway.Persistence.CodexTurn, request_id: request.id)

    result
  end
end
