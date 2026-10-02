defmodule CodexPooler.Upstreams.ProviderCreditsPolicy do
  @moduledoc """
  Identity-wide provider credits policy with scoped, transactional operator updates.
  """

  import Ecto.Query

  alias CodexPooler.Accounts.{Scope, User}
  alias CodexPooler.Events
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.{AccountAudit, AccountLifecycle, IdentitySlotLock}
  alias CodexPooler.Upstreams.Quota.{CapacityAssessment, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPooler.Upstreams.Secrets

  @audit_action "upstream_account.provider_credits_policy_update"
  @broadcast_reason "upstream_account_provider_credits_policy_updated"

  @type lifecycle_error :: %{required(:code) => atom(), required(:message) => String.t()}
  @type identity_ref :: UpstreamIdentity.t() | Ecto.UUID.t()
  @type policy_result :: %{
          required(:status) => :provider_credits_policy_updated | :provider_credits_policy_unchanged,
          required(:identity) => UpstreamIdentity.t(),
          required(:assignments) => [PoolUpstreamAssignment.t()],
          required(:secret_status) => :present | :missing | :expired | :refresh_due | :reauth_required
        }
  @type update_result :: {:ok, policy_result()} | {:error, Ecto.Changeset.t() | lifecycle_error()}

  @type credit_scope :: %{model: String.t() | nil, serving_mode: :full | :lite | nil, transport: :http_sse | :http_json | :native_websocket | :bridged_websocket | nil, source_kind: CodexPooler.Quotas.CapacityFacts.source_kind() | nil, account_windows: [{String.t(), pos_integer()}]}

  @spec management_permissions(Scope.t(), [Ecto.UUID.t()]) :: %{Ecto.UUID.t() => boolean()}
  def management_permissions(scope, identity_ids) do
    existing = Repo.all(from identity in UpstreamIdentity, where: identity.id in ^identity_ids, select: identity.id) |> MapSet.new()
    owner? = Pools.owner?(scope)

    operable_pools =
      case Pools.list_pools(scope) do
        {:ok, pools} -> pools
        {:error, _reason} -> []
      end

    operable = MapSet.new(operable_pools, & &1.id)

    assignments = Repo.all(from assignment in PoolUpstreamAssignment, where: assignment.upstream_identity_id in ^identity_ids and assignment.status != "deleted", select: %{upstream_identity_id: assignment.upstream_identity_id, pool_id: assignment.pool_id}) |> Enum.group_by(& &1.upstream_identity_id)

    Map.new(identity_ids, fn id ->
      pool_ids = Map.get(assignments, id, []) |> Enum.map(& &1.pool_id) |> Enum.uniq()
      allowed? = if owner?, do: Enum.any?(pool_ids, &MapSet.member?(operable, &1)), else: pool_ids != [] and Enum.all?(pool_ids, &MapSet.member?(operable, &1))
      {id, MapSet.member?(existing, id) and allowed?}
    end)
  end

  @type decision :: %{eligible?: boolean(), capacity_basis: CapacityAssessment.capacity_basis(), reason_codes: [String.t()], qualification: map(), eligibility: map(), credit_available?: boolean(), credit_ambiguous?: boolean()}

  @doc """
  Evaluates physical authority, the saved identity policy and one request scope.

  Current provider credit permission supplies credit authority, independently of
  the model selected for a diagnostic probe. Account summaries report conditional
  capacity; runtime callers must provide the actual model, mode and transport.
  Neither a caller-supplied qualification nor a completed response supplies
  permission. This pure evaluator does not order candidates, spend resets or
  establish the provider's billing source.
  """
  @spec evaluate(RoutingQuotaSnapshot.t(), CapacityAssessment.request_context()) :: decision()
  def evaluate(%RoutingQuotaSnapshot{} = snapshot, context) do
    context = if is_list(context), do: Map.new(context), else: context
    assessment = CapacityAssessment.assess(snapshot, context)
    scope = credit_scope(snapshot, context)
    qualification = %{status: :unverified, scope: scope}

    cond do
      CapacityAssessment.recovery_pending?(snapshot) ->
        decision(assessment, false, :none, ["saved_reset_probe_pending"], %{status: :not_applicable})

      assessment.eligible? and assessment.capacity_basis != :unknown_legacy ->
        decision(assessment, true, assessment.capacity_basis, [], %{status: :established})

      assessment.credit_available? ->
        evaluate_credit(snapshot, context, assessment, scope, qualification)

      assessment.eligible? ->
        evaluate_legacy(snapshot.allow_provider_credits, assessment)

      true ->
        reasons = Enum.uniq(assessment.reason_codes ++ ["provider_credit_capacity_unverified"])
        decision(assessment, false, :none, reasons, qualification)
    end
  end

  defp evaluate_legacy(true, assessment),
    do: decision(assessment, true, :unknown_legacy, [], %{status: :legacy_attested})

  defp evaluate_legacy(false, assessment),
    do: decision(assessment, false, :unknown_legacy, ["provider_credits_disabled", "capacity_basis_unknown"], %{status: :unverified})

  defp evaluate_credit(snapshot, context, assessment, scope, qualification) do
    cond do
      not snapshot.allow_provider_credits ->
        decision(assessment, false, :provider_credits, ["provider_credits_disabled"], qualification)

      account_summary?(context, scope) or valid_credit_scope?(scope) ->
        decision(assessment, true, :provider_credits, [], %{status: :provider_attested, scope: scope})

      true ->
        decision(assessment, false, :provider_credits, ["provider_credit_capacity_unverified"], qualification)
    end
  end

  defp account_summary?(context, scope),
    do: Map.get(context, :account_only) == true and is_nil(scope.model) and is_nil(scope.serving_mode) and is_nil(scope.transport)

  defp decision(assessment, eligible?, basis, reasons, qualification),
    do: Map.merge(assessment, %{eligible?: eligible?, capacity_basis: basis, reason_codes: reasons, qualification: qualification})

  defp credit_scope(snapshot, context) do
    %{
      model: normalized_model(Map.get(context, :upstream_model) || Map.get(context, :upstream_model_id) || Map.get(context, :model) || Map.get(context, :requested_model)),
      serving_mode: normalize_mode(Map.get(context, :serving_mode)),
      transport: normalize_transport(Map.get(context, :transport)),
      source_kind: snapshot.capacity_facts && snapshot.capacity_facts.source_kind,
      account_windows: CapacityAssessment.credit_window_shape(snapshot, context)
    }
  end

  defp valid_credit_scope?(scope), do: is_binary(scope.model) and not is_nil(scope.serving_mode) and not is_nil(scope.transport) and not is_nil(scope.source_kind)

  defp normalized_model(value) when is_binary(value) do
    case String.trim(value) do
      "" -> nil
      model -> model
    end
  end

  defp normalized_model(_value), do: nil
  defp normalize_mode(mode) when mode in [:full, "full"], do: :full
  defp normalize_mode(mode) when mode in [:lite, "lite"], do: :lite
  defp normalize_mode(_mode), do: nil
  defp normalize_transport(transport) when transport in [:http_sse, :http_json, :native_websocket, :bridged_websocket], do: transport
  defp normalize_transport(_transport), do: nil

  @doc """
  Updates the policy only when the persisted identity is operable across its Pools.

  This auto-publishing boundary rejects caller-owned transactions so subscribers
  cannot observe a change that a surrounding transaction later rolls back.
  """
  @spec update_for_scope(Scope.t(), identity_ref(), map()) :: update_result()
  def update_for_scope(%Scope{user: %User{}} = scope, identity_or_id, attrs) when is_map(attrs) do
    if Repo.in_transaction?() do
      {:error,
       lifecycle_error(
         :transaction_not_allowed,
         "auto-publishing provider credits policy updates are not allowed inside a caller-owned transaction"
       )}
    else
      update_outside_transaction(scope, identity_or_id, attrs)
    end
  end

  def update_for_scope(_scope, _identity_or_id, _attrs),
    do: {:error, lifecycle_error(:invalid_request, "user scope and provider credits policy control are required")}

  defp update_outside_transaction(scope, identity_or_id, attrs) do
    with {:ok, identity_id} <- identity_id(identity_or_id) do
      Repo.transact(fn -> update_locked_policy(scope, identity_id, attrs) end)
      |> publish_policy_change()
    end
  end

  defp update_locked_policy(scope, identity_id, attrs) do
    %{identities: identities, assignments: assignments} = IdentitySlotLock.lock_identity_rows!([identity_id])

    with [identity] <- identities,
         {:ok, persisted_identity} <- AccountLifecycle.authorize(scope, identity.id) do
      changeset = UpstreamIdentity.provider_credits_policy_changeset(persisted_identity, attrs)

      apply_policy_change(Ecto.Changeset.apply_action(changeset, :update), scope, persisted_identity, assignments, changeset)
    else
      [] -> {:error, lifecycle_error(:upstream_identity_not_found, "upstream identity was not found")}
      {:error, _reason} = error -> error
    end
  end

  defp apply_policy_change({:ok, requested}, scope, identity, assignments, changeset) do
    if requested.allow_provider_credits == identity.allow_provider_credits do
      {:ok, result(:provider_credits_policy_unchanged, identity, assignments)}
    else
      persist_policy_change(scope, identity, assignments, changeset)
    end
  end

  defp apply_policy_change({:error, changeset}, _scope, _identity, _assignments, _original), do: {:error, changeset}

  defp persist_policy_change(scope, identity, assignments, changeset) do
    with {:ok, updated_identity} <-
           changeset
           |> Ecto.Changeset.put_change(:updated_at, now())
           |> Repo.update() do
      updated_result = result(:provider_credits_policy_updated, updated_identity, assignments)

      AccountAudit.record_change_strict({:ok, updated_result}, scope, @audit_action,
        previous_allow_provider_credits: identity.allow_provider_credits,
        allow_provider_credits: updated_identity.allow_provider_credits
      )
    end
  end

  defp result(status, identity, assignments) do
    %{
      status: status,
      identity: identity,
      assignments: assignments,
      secret_status: Secrets.secret_status(identity)
    }
  end

  defp publish_policy_change({:ok, %{status: :provider_credits_policy_updated} = result} = ok) do
    result.assignments
    |> Enum.uniq_by(& &1.pool_id)
    |> Enum.each(fn assignment ->
      Events.broadcast_upstreams(assignment.pool_id, @broadcast_reason, %{
        upstream_identity_id: result.identity.id
      })
    end)

    ok
  end

  defp publish_policy_change(result), do: result

  defp identity_id(%UpstreamIdentity{id: id}), do: identity_id(id)

  defp identity_id(id) do
    case Ecto.UUID.cast(id) do
      {:ok, id} -> {:ok, id}
      :error -> {:error, lifecycle_error(:upstream_identity_not_found, "upstream identity was not found")}
    end
  end

  defp lifecycle_error(code, message), do: %{code: code, message: message}
  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
