defmodule CodexPooler.Gateway.Transports.ProviderCreditsDispatchSupportTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Catalog.Model
  alias CodexPooler.Gateway.Transports.ProviderCreditsAdmission
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.{PoolerFixtures, ProviderCreditsDispatchSupport, ProviderCreditsFixtures, Repo}
  alias CodexPooler.Upstreams.Quota.RoutingQuotaSnapshot
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  test "wire fixtures bind supplied correlations to persisted accounting and included capacity" do
    request_id = Ecto.UUID.generate()
    attempt_id = Ecto.UUID.generate()
    model = "synthetic-dispatch-contract"

    wire = ProviderCreditsDispatchSupport.wire_request!(%UpstreamWebsocketSession.Request{payload: CodexPooler.JSON.encode!(%{"model" => model}), request_id: request_id, attempt_id: attempt_id, effective_serving_mode: "full"})
    context = wire.provider_credits_context

    assert wire.request_id == context.request_id and context.request_id == request_id
    assert wire.attempt_id == context.attempt_id and context.attempt_id == attempt_id
    assert context.model == model and context.upstream_model == model
    assert %Request{pool_id: pool_id, requested_model: ^model, model_id: model_id, transport: "websocket"} = Repo.get(Request, request_id)
    assert pool_id == context.pool_id
    assert %Model{pool_id: ^pool_id, exposed_model_id: ^model, upstream_model_id: ^model} = Repo.get!(Model, model_id)
    assert %Attempt{request_id: ^request_id, pool_upstream_assignment_id: assignment_id, upstream_identity_id: identity_id, model_id: ^model_id, upstream_model_id: ^model, transport: "websocket"} = Repo.get(Attempt, attempt_id)
    assert assignment_id == context.pool_upstream_assignment_id and identity_id == context.upstream_identity_id
    Repo.get!(UpstreamIdentity, identity_id) |> Ecto.Changeset.change(allow_provider_credits: false) |> Repo.update!()
    snapshot = Map.fetch!(RoutingQuotaSnapshot.load_by_identity_ids([identity_id], DateTime.utc_now()), identity_id)
    assert %{raw_windows: [], allow_provider_credits: false, capacity_facts: %{included_permission: :available, credit_permission: :unavailable}} = snapshot
    assert {:ok, %{capacity_basis: :windowless_provider_permission, non_credit_guarded_probe: false}} = ProviderCreditsAdmission.admit(context)

    request = Repo.get!(Request, request_id)
    attempt = Repo.get!(Attempt, attempt_id)
    assert ProviderCreditsDispatchSupport.wire_request!(wire) == wire
    assert Repo.get!(Request, request_id) == request
    assert Repo.get!(Attempt, attempt_id) == attempt

    missing = %{context | request_id: Ecto.UUID.generate()}
    assert {:error, %{reason_codes: ["provider_credit_capacity_unverified"], started: false}} = ProviderCreditsAdmission.admit(missing)
  end

  test "explicit exhausted quota evidence remains denied and is never replaced by fixture permission" do
    %{identity: identity} = PoolerFixtures.upstream_assignment_fixture()
    identity = ProviderCreditsFixtures.persist_usage!(identity, ProviderCreditsFixtures.usage_payload(:weekly_credit_only, credits: :none), DateTime.utc_now())
    metadata = identity.metadata
    context = ProviderCreditsDispatchSupport.context!(identity, request_id: Ecto.UUID.generate(), attempt_id: Ecto.UUID.generate())

    assert Repo.get!(UpstreamIdentity, identity.id).metadata == metadata
    assert {:error, %{started: false}} = ProviderCreditsAdmission.admit(context)
  end
end
