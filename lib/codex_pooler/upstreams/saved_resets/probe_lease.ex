defmodule CodexPooler.Upstreams.SavedResets.ProbeLease do
  @moduledoc """
  The one-shot, cross-node probe claim for a pending saved-reset redemption.

  After a credit is consumed but the account quota window is omitted by the
  provider, exactly one request is allowed to route to that identity as a guarded
  probe. This module makes that claim under the identity's `FOR UPDATE` lock so
  that competing replicas cannot both probe: the first correlation token to claim
  wins, everyone else is rejected before dispatch.

  The claim is irreversible. If the claiming request fails (network, 5xx,
  cancellation), the probe is NOT handed to another request — recovery then comes
  only from fresh provider evidence (see `Convergence`) or the bounded-window
  expiry. This guarantees a consumed credit can never trigger a second
  consumption or a second account's probe.
  """

  import Ecto.Query

  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Quotas.CapacityFacts
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{CapacityFactsStore, RoutingQuotaSnapshot, Windows}
  alias CodexPooler.Upstreams.Quota.Windows.Routing
  alias CodexPooler.Upstreams.SavedResets.RedemptionLifecycle
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}

  @type claim_result :: {:ok, :claimed} | {:error, :unavailable | :not_found}

  defmodule VerifiedConfirmation do
    @moduledoc false
    @enforce_keys [:credential_epoch, :probe, :upstream_model, :serving_mode, :transport]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            credential_epoch: pos_integer(),
            probe: CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe.t(),
            upstream_model: String.t(),
            serving_mode: :full | :lite,
            transport: :http_sse | :http_json | :native_websocket | :bridged_websocket
          }
  end

  @confirmation_key "non_credit_confirmation"
  @confirmation_keys ~w(version credential_epoch generation attempt_id confirmed_at included_window_descriptors scope)
  @scope_keys ~w(pool_upstream_assignment_id upstream_identity_id effective_model upstream_model route_class serving_mode transport)
  @weekly_descriptor [%{"window_kind" => "secondary", "window_minutes" => 10_080}]

  @type grace_context :: keyword() | map()

  @doc """
  Attempts to claim the probe for `token` on the identity's pending redemption,
  matching the expected `generation` and `attempt_id` (compare-and-set).

  Returns `{:ok, :claimed}` for the winning token (idempotent for the same token)
  and `{:error, :unavailable}` when the probe is already claimed by another
  token, the redemption is not pending, or the window has elapsed.
  """
  @spec claim(
          UpstreamIdentity.t() | Ecto.UUID.t(),
          integer(),
          term(),
          ResetProbe.t() | String.t()
        ) :: claim_result()
  @spec claim(
          UpstreamIdentity.t() | Ecto.UUID.t(),
          integer(),
          term(),
          ResetProbe.t() | String.t(),
          DateTime.t()
        ) :: claim_result()
  def claim(identity_or_id, generation, attempt_id, probe, now \\ now())
      when is_binary(probe) or is_struct(probe, ResetProbe) do
    case identity_id(identity_or_id) do
      nil ->
        {:error, :not_found}

      id ->
        Repo.transaction(fn -> claim_locked(id, generation, attempt_id, probe, now) end)
        |> case do
          {:ok, :claimed} -> {:ok, :claimed}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc "Confirms a historical token claim without granting verified non-credit recovery authority."
  @spec confirm_upstream(UpstreamIdentity.t() | Ecto.UUID.t(), String.t()) ::
          {:ok, :confirmed | :unchanged} | {:error, :not_found}
  @spec confirm_upstream(UpstreamIdentity.t() | Ecto.UUID.t(), String.t(), DateTime.t()) ::
          {:ok, :confirmed | :unchanged} | {:error, :not_found}
  def confirm_upstream(identity_or_id, token, now \\ now()) when is_binary(token) do
    case identity_id(identity_or_id) do
      nil ->
        {:error, :not_found}

      id ->
        Repo.transaction(fn -> confirm_locked(id, token, now) end)
        |> case do
          {:ok, outcome} -> {:ok, outcome}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc "Persists bounded non-credit proof only after the runtime success finalizer verifies its actual admission receipt."
  @spec confirm_upstream(
          UpstreamIdentity.t() | Ecto.UUID.t(),
          integer(),
          term(),
          VerifiedConfirmation.t()
        ) :: {:ok, :confirmed | :unchanged} | {:error, :not_found}
  @spec confirm_upstream(
          UpstreamIdentity.t() | Ecto.UUID.t(),
          integer(),
          term(),
          VerifiedConfirmation.t(),
          DateTime.t()
        ) :: {:ok, :confirmed | :unchanged} | {:error, :not_found}
  def confirm_upstream(identity_or_id, generation, attempt_id, confirmation, now \\ now()) do
    case identity_id(identity_or_id) do
      nil ->
        {:error, :not_found}

      id ->
        Repo.transaction(fn -> confirm_locked(id, generation, attempt_id, confirmation, now) end)
        |> case do
          {:ok, outcome} -> {:ok, outcome}
          {:error, reason} -> {:error, reason}
        end
    end
  end

  defp confirm_locked(id, token, now) do
    identity = lock_identity!(id)
    redemption = (identity.metadata || %{})["saved_reset_redemption"]
    target = RedemptionLifecycle.confirmed_by_upstream()

    can_confirm? =
      valid_legacy_probe?(redemption, token) and
        valid_future_deadline?(redemption, now) and
        RedemptionLifecycle.can_transition?(
          redemption,
          target,
          Map.get(redemption, "generation"),
          Map.get(redemption, "attempt_id")
        )

    confirm_transition(identity, redemption, target, can_confirm?, now)
  end

  defp confirm_locked(id, generation, attempt_id, %VerifiedConfirmation{probe: %ResetProbe{} = probe} = confirmation, now) do
    identity = lock_identity!(id)

    case lock_probe_assignment(probe, id) do
      :ok ->
        redemption = (identity.metadata || %{})["saved_reset_redemption"]
        target = RedemptionLifecycle.confirmed_by_upstream()

        can_confirm? =
          valid_v2_probe?(identity, redemption, generation, attempt_id, probe) and
            valid_verified_confirmation?(identity, redemption, confirmation, now) and
            RedemptionLifecycle.can_transition?(redemption, target, generation, attempt_id)

        if can_confirm? do
          proof = confirmation_marker(redemption, confirmation, now)
          confirm_transition(identity, Map.put(redemption, @confirmation_key, proof), target, true, now)
        else
          :unchanged
        end

      :error ->
        :unchanged
    end
  end

  defp confirm_locked(_id, _generation, _attempt_id, _unverified, _now), do: :unchanged

  @doc "Reads scoped verified non-credit recovery within its original deadline; legacy phase alone never grants capacity."
  @spec verified_grace?(RoutingQuotaSnapshot.t(), grace_context()) :: boolean()
  def verified_grace?(%RoutingQuotaSnapshot{redemption: redemption} = snapshot, context)
      when is_map(redemption) and (is_map(context) or is_list(context)) do
    context = if is_list(context), do: Map.new(context), else: context
    proof = redemption[@confirmation_key]

    valid_confirmation_marker?(proof, redemption, snapshot) and
      matching_confirmation_scope?(proof["scope"], snapshot.upstream_identity_id, context) and
      current_non_credit_facts?(snapshot) and current_weekly_resource?(snapshot)
  end

  def verified_grace?(_snapshot, _context), do: false

  defp valid_verified_confirmation?(identity, redemption, confirmation, now) do
    snapshot = RoutingQuotaSnapshot.from_identity(identity, Windows.list_evidence(identity), now)

    valid_confirmation_carrier?(confirmation) and
      CredentialFencing.validate_current_credential_epoch(identity) == {:ok, confirmation.credential_epoch} and
      captured_weekly_attempt?(redemption) and RedemptionLifecycle.applied_consume?(redemption) and
      valid_confirmation_time?(redemption, DateTime.to_iso8601(now), now) and
      current_non_credit_facts?(snapshot) and current_weekly_resource?(snapshot)
  end

  defp valid_confirmation_carrier?(%VerifiedConfirmation{} = confirmation),
    do:
      is_integer(confirmation.credential_epoch) and confirmation.credential_epoch > 0 and
        ResetProbe.bound?(confirmation.probe) and exact_string?(confirmation.probe.effective_model) and
        exact_string?(confirmation.probe.route_class) and exact_string?(confirmation.upstream_model) and
        confirmation.serving_mode in [:full, :lite] and
        confirmation.transport in [:http_sse, :http_json, :native_websocket, :bridged_websocket]

  defp confirmation_marker(redemption, confirmation, now) do
    probe = confirmation.probe

    %{
      "version" => 1,
      "credential_epoch" => confirmation.credential_epoch,
      "generation" => redemption["generation"],
      "attempt_id" => redemption["attempt_id"],
      "confirmed_at" => DateTime.to_iso8601(now),
      "included_window_descriptors" => redemption["included_window_descriptors"],
      "scope" => %{
        "pool_upstream_assignment_id" => probe.pool_upstream_assignment_id,
        "upstream_identity_id" => probe.upstream_identity_id,
        "effective_model" => probe.effective_model,
        "upstream_model" => confirmation.upstream_model,
        "route_class" => probe.route_class,
        "serving_mode" => Atom.to_string(confirmation.serving_mode),
        "transport" => Atom.to_string(confirmation.transport)
      }
    }
  end

  defp valid_confirmation_marker?(proof, redemption, snapshot) when is_map(proof) do
    exact_keys?(proof, @confirmation_keys) and proof["version"] == 1 and
      RedemptionLifecycle.phase(redemption) == RedemptionLifecycle.confirmed_by_upstream() and
      RedemptionLifecycle.applied_consume?(redemption) and
      matching_confirmation_attempt?(proof, redemption, snapshot.credential_epoch) and
      proof["included_window_descriptors"] == @weekly_descriptor and
      proof["included_window_descriptors"] == redemption["included_window_descriptors"] and
      proof["confirmed_at"] == redemption["finished_at"] and valid_confirmation_time?(redemption, proof["confirmed_at"], snapshot.as_of)
  end

  defp valid_confirmation_marker?(_proof, _redemption, _snapshot), do: false

  defp matching_confirmation_scope?(scope, identity_id, context) when is_map(scope) and is_map(context) do
    exact_keys?(scope, @scope_keys) and matching_scope_identity?(scope, identity_id, context) and
      matching_scope_models?(scope, context) and matching_scope_transport?(scope, context)
  end

  defp matching_confirmation_scope?(_scope, _identity_id, _context), do: false

  defp captured_weekly_attempt?(redemption),
    do:
      redemption["included_window_descriptors"] == @weekly_descriptor and
        is_integer(redemption["generation"]) and redemption["generation"] > 0 and uuid?(redemption["attempt_id"])

  defp matching_confirmation_attempt?(proof, redemption, epoch),
    do:
      is_integer(proof["credential_epoch"]) and proof["credential_epoch"] > 0 and proof["credential_epoch"] == epoch and
        is_integer(proof["generation"]) and proof["generation"] > 0 and proof["generation"] == redemption["generation"] and
        uuid?(proof["attempt_id"]) and proof["attempt_id"] == redemption["attempt_id"]

  defp matching_scope_identity?(scope, identity_id, context),
    do:
      uuid?(scope["pool_upstream_assignment_id"]) and uuid?(scope["upstream_identity_id"]) and
        scope["upstream_identity_id"] == identity_id and context[:upstream_identity_id] == scope["upstream_identity_id"] and
        context[:pool_upstream_assignment_id] == scope["pool_upstream_assignment_id"]

  defp matching_scope_models?(scope, context),
    do:
      Enum.all?(~w(effective_model upstream_model route_class), &exact_string?(scope[&1])) and
        scope["effective_model"] == (context[:model] || context[:requested_model]) and
        scope["upstream_model"] == (context[:upstream_model] || context[:upstream_model_id]) and scope["route_class"] == context[:route_class]

  defp matching_scope_transport?(scope, context),
    do:
      scope["serving_mode"] in ["full", "lite"] and scope["serving_mode"] == scope_string(context[:serving_mode]) and
        scope["transport"] in ~w(http_sse http_json native_websocket bridged_websocket) and scope["transport"] == scope_string(context[:transport])

  defp current_non_credit_facts?(snapshot) do
    case snapshot.capacity_facts do
      %CapacityFacts{credit_permission: :unavailable, denial_category: category} = facts when category in [:none, :included_limit, :spend_limit, :unknown] ->
        CapacityFactsStore.fresh?(facts, snapshot.credential_epoch, snapshot.as_of)

      _unverified ->
        false
    end
  end

  defp current_weekly_resource?(snapshot) do
    snapshot
    |> RoutingQuotaSnapshot.time_visible_raw_windows()
    |> Routing.included_only_windows()
    |> Windows.effective_quota_windows(snapshot.as_of)
    |> Enum.any?(fn window ->
      window.quota_key == "account" and window.quota_scope == "account" and window.quota_family == "account" and
        window.window_kind == "secondary" and window.window_minutes == 10_080 and
        window.source == "codex_usage_api" and window.source_precision in ["authoritative", "observed"] and
        (Windows.usable_window?(window, snapshot.as_of) or Routing.window_reason_codes(window, snapshot.as_of) == ["exhausted"])
    end)
  end

  defp valid_confirmation_time?(redemption, confirmed_at, now) do
    with {:ok, consumed_at} <- utc_datetime(redemption["consumed_at"]),
         {:ok, deadline_at} <- utc_datetime(redemption["deadline_at"]),
         {:ok, claimed_at} <- utc_datetime(get_in(redemption, ["probe", "claimed_at"])),
         {:ok, confirmed_at} <- utc_datetime(confirmed_at) do
      DateTime.compare(consumed_at, claimed_at) != :gt and DateTime.compare(claimed_at, confirmed_at) != :gt and
        DateTime.compare(confirmed_at, now) != :gt and DateTime.compare(now, deadline_at) == :lt and
        DateTime.compare(deadline_at, RedemptionLifecycle.deadline_at(consumed_at)) != :gt
    else
      _invalid -> false
    end
  end

  defp utc_datetime(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> {:ok, datetime}
      _invalid -> :error
    end
  end

  defp utc_datetime(_value), do: :error
  defp exact_keys?(map, keys), do: map_size(map) == length(keys) and Enum.sort(Map.keys(map)) == Enum.sort(keys)
  defp exact_string?(value), do: is_binary(value) and byte_size(value) in 1..1_024 and String.valid?(value) and String.trim(value) == value
  defp uuid?(value), do: is_binary(value) and Ecto.UUID.cast(value) == {:ok, value}
  defp scope_string(value) when is_atom(value), do: Atom.to_string(value)
  defp scope_string(value) when is_binary(value), do: value
  defp scope_string(_value), do: nil

  defp confirm_transition(identity, redemption, target, true, now) do
    updated =
      Map.merge(redemption, %{
        "phase" => target,
        "status" => RedemptionLifecycle.legacy_status_for(target),
        "finished_at" => DateTime.to_iso8601(now),
        "terminal_reason" => "probe_upstream_confirmed"
      })

    persist_redemption!(identity, updated, now)
    :confirmed
  end

  defp confirm_transition(_identity, _redemption, _target, false, _now), do: :unchanged

  defp claim_locked(id, generation, attempt_id, %ResetProbe{} = probe, now) do
    identity = lock_identity!(id)

    case lock_probe_assignment(probe, id) do
      :ok -> claim_locked_identity(identity, generation, attempt_id, probe, now)
      :error -> Repo.rollback(:unavailable)
    end
  end

  defp claim_locked(id, generation, attempt_id, token, now) when is_binary(token) do
    id
    |> lock_identity!()
    |> claim_locked_identity(generation, attempt_id, token, now)
  end

  defp claim_locked_identity(identity, generation, attempt_id, probe, now) do
    redemption = (identity.metadata || %{})["saved_reset_redemption"]

    cond do
      held_probe_matches?(identity, redemption, generation, attempt_id, probe) and
          valid_claim_deadline?(redemption, probe, now) ->
        :claimed

      is_struct(probe, ResetProbe) and
          claimable?(identity, redemption, generation, attempt_id, probe, now) ->
        write_probe!(identity, redemption, probe, now)
        :claimed

      true ->
        Repo.rollback(:unavailable)
    end
  end

  defp claimable?(identity, redemption, generation, attempt_id, probe, now) do
    valid_claim_deadline?(redemption, probe, now) and
      RedemptionLifecycle.probe_claimable?(redemption, now) and
      Map.get(redemption, "generation") == generation and
      Map.get(redemption, "attempt_id") == attempt_id and
      valid_claim_probe?(identity, probe)
  end

  defp write_probe!(identity, redemption, %ResetProbe{} = probe, now) do
    updated = Map.put(redemption, "probe", persisted_v2_probe(probe, now))

    persist_redemption!(identity, updated, now)
  end

  defp held_probe_matches?(identity, redemption, generation, attempt_id, %ResetProbe{} = probe),
    do: valid_v2_probe?(identity, redemption, generation, attempt_id, probe)

  defp held_probe_matches?(_identity, redemption, generation, attempt_id, token)
       when is_binary(token) do
    Map.get(redemption || %{}, "generation") == generation and
      Map.get(redemption || %{}, "attempt_id") == attempt_id and
      valid_legacy_probe?(redemption, token)
  end

  defp valid_claim_probe?(%UpstreamIdentity{id: identity_id}, %ResetProbe{} = probe) do
    ResetProbe.bound?(probe) and probe.upstream_identity_id == identity_id
  end

  defp valid_claim_probe?(_identity, _probe), do: false

  defp persisted_v2_probe(%ResetProbe{} = probe, now) do
    %{
      "version" => probe.version,
      "token" => probe.token,
      "claimed_at" => DateTime.to_iso8601(now),
      "scope" => %{
        "pool_upstream_assignment_id" => probe.pool_upstream_assignment_id,
        "upstream_identity_id" => probe.upstream_identity_id,
        "effective_model" => probe.effective_model,
        "route_class" => probe.route_class
      }
    }
  end

  defp valid_v2_probe?(identity, redemption, generation, attempt_id, %ResetProbe{} = probe) do
    Map.get(redemption || %{}, "generation") == generation and
      Map.get(redemption || %{}, "attempt_id") == attempt_id and
      valid_claim_probe?(identity, probe) and
      exact_v2_probe?(Map.get(redemption || %{}, "probe"), probe)
  end

  defp exact_v2_probe?(
         %{
           "version" => 2,
           "token" => token,
           "claimed_at" => claimed_at,
           "scope" =>
             %{
               "pool_upstream_assignment_id" => assignment_id,
               "upstream_identity_id" => identity_id,
               "effective_model" => effective_model,
               "route_class" => route_class
             } = scope
         } = persisted,
         %ResetProbe{} = probe
       ) do
    Enum.sort(Map.keys(persisted)) == ~w(claimed_at scope token version) and
      Enum.sort(Map.keys(scope)) ==
        ~w(effective_model pool_upstream_assignment_id route_class upstream_identity_id) and
      valid_datetime?(claimed_at) and token == probe.token and
      assignment_id == probe.pool_upstream_assignment_id and
      identity_id == probe.upstream_identity_id and effective_model == probe.effective_model and
      route_class == probe.route_class
  end

  defp exact_v2_probe?(_persisted, _probe), do: false

  defp valid_legacy_probe?(redemption, token) when is_binary(token) do
    case Map.get(redemption || %{}, "probe") do
      %{"token" => ^token, "claimed_at" => claimed_at} = probe ->
        Enum.sort(Map.keys(probe)) == ~w(claimed_at token) and valid_datetime?(claimed_at)

      _invalid ->
        false
    end
  end

  defp valid_datetime?(value) when is_binary(value),
    do: match?({:ok, %DateTime{}, _offset}, DateTime.from_iso8601(value))

  defp valid_datetime?(_value), do: false

  defp valid_future_deadline?(%{"deadline_at" => deadline_at}, %DateTime{} = now)
       when is_binary(deadline_at) do
    case DateTime.from_iso8601(deadline_at) do
      {:ok, deadline, _offset} -> DateTime.compare(now, deadline) == :lt
      _invalid -> false
    end
  end

  defp valid_future_deadline?(_redemption, _now), do: false

  defp valid_claim_deadline?(redemption, probe, now)
       when is_binary(probe) or is_struct(probe, ResetProbe),
       do: valid_future_deadline?(redemption, now)

  defp lock_probe_assignment(%ResetProbe{} = probe, identity_id) do
    assignment =
      Repo.one(
        from assignment in PoolUpstreamAssignment,
          where: assignment.id == ^probe.pool_upstream_assignment_id,
          lock: "FOR UPDATE"
      )

    if match?(%PoolUpstreamAssignment{upstream_identity_id: ^identity_id}, assignment),
      do: :ok,
      else: :error
  end

  defp persist_redemption!(identity, redemption, now) do
    identity
    |> UpstreamIdentity.changeset(%{
      metadata: Map.put(identity.metadata || %{}, "saved_reset_redemption", redemption),
      updated_at: now
    })
    |> Repo.update!()
  end

  defp lock_identity!(id) do
    Repo.one!(
      from identity in UpstreamIdentity,
        where: identity.id == ^id,
        lock: "FOR UPDATE"
    )
  end

  defp identity_id(%UpstreamIdentity{id: id}), do: id
  defp identity_id(id) when is_binary(id), do: id
  defp identity_id(_identity_or_id), do: nil

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end

defimpl Inspect, for: CodexPooler.Upstreams.SavedResets.ProbeLease.VerifiedConfirmation do
  def inspect(_confirmation, _opts), do: "#ProbeLease.VerifiedConfirmation<redacted>"
end
