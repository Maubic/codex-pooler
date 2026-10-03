defmodule CodexPooler.Gateway.Transports.ProviderCreditsAdmission do
  @moduledoc """
  Authoritative, request-scoped capacity admission immediately before a generation send.

  The joined read is the admission linearization point. It holds no database lock
  or transaction across provider I/O, and its receipt is never connection permission.
  """

  import Ecto.Query

  alias CodexPooler.Accounting.NativeContentFilterRetry
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Payloads.RequestOptions.Transport
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Quota.{AccountQuotaWindow, CapacityAssessment, RoutingQuotaSnapshot}
  alias CodexPooler.Upstreams.Quota.Windows.AccountDenial
  alias CodexPooler.Upstreams.SavedResets.RedemptionLifecycle
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPooler.Upstreams.StatusVocabulary.Identity, as: IdentityStatus

  @generation_endpoints [
    "/backend-api/transcribe",
    "/backend-api/codex/responses",
    "/backend-api/codex/v1/responses",
    "/backend-api/codex/responses/compact",
    "/backend-api/codex/v1/responses/compact",
    "/backend-api/codex/images/generations",
    "/backend-api/codex/images/edits",
    "/v1/responses",
    "/v1/responses/compact",
    "/v1/images/generations",
    "/v1/images/edits"
  ]

  defmodule Context do
    @moduledoc false
    @fields [:version, :pool_id, :pool_upstream_assignment_id, :upstream_identity_id, :credential_epoch, :model, :upstream_model, :serving_mode, :transport, :route_class, :request_id, :attempt_id, :reset_probe, :redemption_generation, :redemption_attempt_id]
    @enforce_keys @fields
    defstruct @fields

    @type transport :: :http_sse | :http_json | :native_websocket | :bridged_websocket
    @type t :: %__MODULE__{
            version: 1,
            pool_id: Ecto.UUID.t(),
            pool_upstream_assignment_id: Ecto.UUID.t(),
            upstream_identity_id: Ecto.UUID.t(),
            credential_epoch: pos_integer(),
            model: String.t(),
            upstream_model: String.t(),
            serving_mode: :full | :lite,
            transport: transport(),
            route_class: String.t(),
            request_id: Ecto.UUID.t() | nil,
            attempt_id: Ecto.UUID.t() | nil,
            reset_probe: CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe.t() | nil,
            redemption_generation: pos_integer() | nil,
            redemption_attempt_id: Ecto.UUID.t() | nil
          }

    @spec fields() :: [atom()]
    def fields, do: @fields
  end

  defmodule Receipt do
    @moduledoc false
    @enforce_keys [:version, :context, :capacity_basis, :reason_codes, :non_credit_guarded_probe]
    defstruct @enforce_keys

    @type t :: %__MODULE__{
            version: 1,
            context: CodexPooler.Gateway.Transports.ProviderCreditsAdmission.Context.t(),
            capacity_basis: CodexPooler.Upstreams.Quota.CapacityAssessment.capacity_basis(),
            reason_codes: [String.t()],
            non_credit_guarded_probe: boolean()
          }
  end

  @type denial :: %{
          reason: :provider_credits_policy_denied,
          started: false,
          reason_codes: [String.t()],
          capacity_basis: CapacityAssessment.capacity_basis(),
          candidate_exclusions: [map()]
        }
  @type result :: {:ok, Receipt.t()} | {:error, denial()}

  @doc "Builds the bounded trusted context from the selected runtime phase, never from provider payloads."
  @spec from_selected(map(), RequestOptions.t() | nil) :: Context.t() | nil
  def from_selected(context, options \\ nil) do
    options = options || context.request_options

    if generation_endpoint?(options.transport.upstream_endpoint) do
      redemption = Map.get(context.identity.metadata || %{}, "saved_reset_redemption") || %{}
      probe = bound_probe(options.routing.reset_probe)

      %Context{
        version: 1,
        pool_id: context.assignment.pool_id,
        pool_upstream_assignment_id: context.assignment.id,
        upstream_identity_id: context.identity.id,
        credential_epoch: CredentialFencing.credential_epoch(context.identity),
        model: effective_model(context, options),
        upstream_model: upstream_model(context, options),
        serving_mode: normalize_mode(RequestOptions.model_serving_mode(options)),
        transport: Transport.upstream_transport(options.transport),
        route_class: context.route_class,
        request_id: context.reserved.request.id,
        attempt_id: context.attempt && context.attempt.id,
        reset_probe: probe,
        redemption_generation: probe && redemption["generation"],
        redemption_attempt_id: probe && redemption["attempt_id"]
      }
    end
  end

  defp bound_probe(%ResetProbe{} = probe), do: if(ResetProbe.bound?(probe), do: probe)
  defp bound_probe(nil), do: nil

  @spec generation_endpoint?(String.t() | nil) :: boolean()
  def generation_endpoint?(endpoint), do: endpoint in @generation_endpoints

  @doc "Strict internal codec: no additional authority, qualifiers, facts or arbitrary fields are accepted."
  @spec new_context(map()) :: {:ok, Context.t()} | {:error, :invalid_admission_context}
  def new_context(attrs) when is_map(attrs) and not is_struct(attrs) do
    if Enum.sort(Map.keys(attrs)) == Enum.sort(Context.fields()) do
      context = struct!(Context, attrs)
      if valid_context?(context), do: {:ok, context}, else: {:error, :invalid_admission_context}
    else
      {:error, :invalid_admission_context}
    end
  end

  def new_context(_attrs), do: {:error, :invalid_admission_context}

  @spec valid_context?(term()) :: boolean()
  def valid_context?(%Context{version: 1} = context) do
    exact_context_fields?(context) and valid_context_scope?(context) and valid_context_request?(context)
  end

  def valid_context?(_context), do: false

  defp exact_context_fields?(context), do: Enum.sort(Map.keys(Map.from_struct(context))) == Enum.sort(Context.fields())

  defp valid_context_scope?(context),
    do:
      Enum.all?([context.pool_id, context.pool_upstream_assignment_id, context.upstream_identity_id], &uuid?/1) and
        is_integer(context.credential_epoch) and context.credential_epoch > 0 and
        exact_string?(context.model) and exact_string?(context.upstream_model) and exact_string?(context.route_class)

  defp valid_context_request?(context),
    do:
      context.serving_mode in [:full, :lite] and context.transport in [:http_sse, :http_json, :native_websocket, :bridged_websocket] and
        nullable_uuid?(context.request_id) and nullable_uuid?(context.attempt_id) and valid_probe_context?(context)

  @spec admit(Context.t() | nil) :: result()
  def admit(context) do
    if valid_context?(context) and not Repo.in_transaction?() and NativeContentFilterRetry.dispatch_context_allowed?(context) do
      context |> load_current_rows() |> evaluate(context)
    else
      {:error, denial(context, :none, ["provider_credit_capacity_unverified"])}
    end
  rescue
    _exception in [DBConnection.ConnectionError, Ecto.QueryError, Postgrex.Error] ->
      {:error, denial(context, :none, ["provider_credit_capacity_unverified"])}
  end

  @doc "Only the final pre-send receipt can authorize the existing bound non-credit confirmation probe."
  @spec confirms_probe?(Receipt.t() | nil, map(), ResetProbe.t()) :: boolean()
  def confirms_probe?(%Receipt{version: 1, capacity_basis: :recovered_included, non_credit_guarded_probe: true, context: admitted}, context, %ResetProbe{} = probe) do
    valid_context?(admitted) and confirmation_scope_matches?(admitted, context) and admitted.reset_probe == probe and
      ResetProbe.matches?(probe, context.assignment.id, context.identity.id, context.request_options.routing.effective_model || context.model.exposed_model_id, context.route_class)
  end

  def confirms_probe?(_receipt, _context, _probe), do: false

  defp confirmation_scope_matches?(admitted, context),
    do:
      uuid?(admitted.request_id) and uuid?(admitted.attempt_id) and
        admitted.upstream_identity_id == context.identity.id and admitted.pool_upstream_assignment_id == context.assignment.id and
        admitted.request_id == context.reserved.request.id and admitted.attempt_id == (context.attempt && context.attempt.id)

  @spec valid_receipt?(term()) :: boolean()
  def valid_receipt?(%Receipt{version: 1, context: context, capacity_basis: basis, reason_codes: reasons, non_credit_guarded_probe: guarded?} = receipt) do
    Enum.sort(Map.keys(Map.from_struct(receipt))) == Enum.sort([:version, :context, :capacity_basis, :reason_codes, :non_credit_guarded_probe]) and
      valid_context?(context) and basis in [:included_window, :ordinary_provider_permission, :model_allowance, :windowless_provider_permission, :recovered_included, :provider_credits, :unknown_legacy] and
      valid_receipt_reasons?(reasons) and is_boolean(guarded?) and valid_guarded_receipt?(guarded?, basis, context)
  end

  def valid_receipt?(_receipt), do: false

  defp valid_receipt_reasons?(reasons), do: is_list(reasons) and length(reasons) <= 16 and Enum.all?(reasons, &exact_string?/1)
  defp valid_guarded_receipt?(false, _basis, _context), do: true
  defp valid_guarded_receipt?(true, :recovered_included, context), do: not is_nil(context.reset_probe)
  defp valid_guarded_receipt?(_guarded?, _basis, _context), do: false

  @spec metadata(Receipt.t() | nil) :: map()
  def metadata(%Receipt{version: 1} = receipt) do
    if valid_receipt?(receipt) do
      %{"version" => 1, "capacity_basis" => Atom.to_string(receipt.capacity_basis), "reason_codes" => receipt.reason_codes, "non_credit_guarded_probe" => receipt.non_credit_guarded_probe}
    else
      %{}
    end
  end

  def metadata(_receipt), do: %{}

  @spec attach_http_receipt({:ok, Req.Response.t()} | {:error, term()}, Receipt.t() | nil) :: {:ok, Req.Response.t()} | {:error, term()}
  def attach_http_receipt({:ok, response}, %Receipt{} = receipt),
    do: {:ok, Req.Response.put_private(response, :provider_credits_admission, receipt)}

  def attach_http_receipt(result, _receipt), do: result

  defp load_current_rows(%Context{} = context) do
    Repo.all(
      from assignment in PoolUpstreamAssignment,
        join: identity in UpstreamIdentity,
        on: identity.id == assignment.upstream_identity_id,
        left_join: window in AccountQuotaWindow,
        on: window.upstream_identity_id == identity.id,
        where:
          assignment.id == ^context.pool_upstream_assignment_id and
            assignment.pool_id == ^context.pool_id and identity.id == ^context.upstream_identity_id,
        select: %{
          assignment: map(assignment, [:id, :pool_id, :upstream_identity_id, :status, :health_status, :eligibility_status, :cooldown_until]),
          identity: struct(identity, [:id, :metadata, :allow_provider_credits, :status]),
          window: window
        }
    )
  end

  defp evaluate([], context), do: {:error, denial(context, :none, ["provider_credit_capacity_unverified"])}

  defp evaluate([first | _] = rows, context) do
    now = DateTime.utc_now()

    windows =
      Enum.flat_map(rows, fn
        %{window: %AccountQuotaWindow{} = window} -> [window]
        %{window: nil} -> []
      end)

    snapshot = RoutingQuotaSnapshot.from_identity(first.identity, windows, now)
    decision = Upstreams.provider_credits_decision(snapshot, request_scope(context))

    account_denial = if decision.capacity_basis == :provider_credits, do: AccountDenial.active_for_credits(snapshot), else: AccountDenial.active(snapshot)

    cond do
      snapshot.credential_epoch != context.credential_epoch or not current_assignment?(first, now) ->
        {:error, denial(context, :none, ["provider_credit_capacity_unverified"])}

      not is_nil(account_denial) ->
        {:error, denial(context, :none, ["exhausted", "provider_denied"], [account_denial_reason(account_denial)])}

      guarded_probe?(snapshot, context) ->
        {:ok, receipt(context, :recovered_included, [], true)}

      decision.eligible? ->
        {:ok, receipt(context, decision.capacity_basis, decision.reason_codes, false)}

      true ->
        {:error, denial(context, decision.capacity_basis, decision.reason_codes, decision.eligibility.exclusions)}
    end
  end

  defp current_assignment?(%{assignment: assignment, identity: identity}, now) do
    assignment.status == PoolUpstreamAssignment.active_status() and
      assignment.eligibility_status == PoolUpstreamAssignment.eligible_status() and
      assignment.health_status == PoolUpstreamAssignment.active_health_status() and
      identity.status in IdentityStatus.model_routable_statuses() and
      not Map.has_key?(identity.metadata || %{}, "permanent_deletion_requested_at") and
      (is_nil(assignment.cooldown_until) or DateTime.compare(assignment.cooldown_until, now) != :gt)
  end

  defp guarded_probe?(snapshot, %Context{reset_probe: %ResetProbe{} = probe} = context) do
    redemption = snapshot.redemption || %{}
    persisted = redemption["probe"]
    expected_scope = %{"pool_upstream_assignment_id" => context.pool_upstream_assignment_id, "upstream_identity_id" => context.upstream_identity_id, "effective_model" => context.model, "route_class" => context.route_class}

    RedemptionLifecycle.phase(redemption) == RedemptionLifecycle.consumed_pending_probe() and
      redemption["generation"] == context.redemption_generation and
      redemption["attempt_id"] == context.redemption_attempt_id and
      current_probe_lease?(persisted, probe.token, expected_scope) and
      valid_past_datetime?(persisted["claimed_at"], snapshot.as_of) and
      valid_future_datetime?(redemption["deadline_at"], snapshot.as_of) and
      CapacityAssessment.guarded_probe_permitted?(snapshot, request_scope(context))
  end

  defp guarded_probe?(_snapshot, _context), do: false

  defp current_probe_lease?(%{"version" => 2, "token" => token, "claimed_at" => _, "scope" => scope} = persisted, token, scope),
    do: Enum.sort(Map.keys(persisted)) == ~w(claimed_at scope token version)

  defp current_probe_lease?(_persisted, _token, _scope), do: false

  defp valid_probe_context?(%Context{reset_probe: nil, redemption_generation: nil, redemption_attempt_id: nil}), do: true

  defp valid_probe_context?(%Context{reset_probe: %ResetProbe{} = probe} = context),
    do:
      ResetProbe.matches?(probe, context.pool_upstream_assignment_id, context.upstream_identity_id, context.model, context.route_class) and
        is_integer(context.redemption_generation) and context.redemption_generation > 0 and uuid?(context.redemption_attempt_id)

  defp valid_probe_context?(_context), do: false

  defp request_scope(context),
    do: %{pool_upstream_assignment_id: context.pool_upstream_assignment_id, upstream_identity_id: context.upstream_identity_id, model: context.model, requested_model: context.model, upstream_model: context.upstream_model, upstream_model_id: context.upstream_model, serving_mode: context.serving_mode, transport: context.transport, route_class: context.route_class}

  defp receipt(context, basis, reasons, guarded?),
    do: %Receipt{version: 1, context: context, capacity_basis: basis, reason_codes: reasons, non_credit_guarded_probe: guarded?}

  defp denial(context, basis, reasons, physical_reasons \\ []) do
    scope = if is_struct(context, Context), do: context, else: %{}
    reasons = Enum.uniq(reasons)

    exclusions =
      if physical_reasons == [],
        do: [%{code: "quota_window_unusable", reason_codes: reasons}],
        else: Enum.map(physical_reasons, &Map.put(&1, :provider_credits_reason_codes, reasons))

    %{reason: :provider_credits_policy_denied, started: false, reason_codes: reasons, capacity_basis: basis, candidate_exclusions: [%{pool_upstream_assignment_id: Map.get(scope, :pool_upstream_assignment_id), upstream_identity_id: Map.get(scope, :upstream_identity_id), reasons: exclusions}]}
  end

  defp account_denial_reason(denial) do
    %{
      "code" => "quota_window_unusable",
      "message" => "the provider refused this account until its reset time",
      "reason_codes" => ["exhausted", "provider_denied"],
      "quota_key" => "account",
      "quota_scope" => "account",
      "quota_family" => "account",
      "source" => denial.source,
      "rate_limit_reached_type" => denial.reached_type,
      "reset_at" => iso8601_or_nil(denial.reset_at),
      "hint_reset_at" => iso8601_or_nil(denial.hint_reset_at)
    }
  end

  defp iso8601_or_nil(%DateTime{} = value), do: DateTime.to_iso8601(value)
  defp iso8601_or_nil(nil), do: nil

  defp effective_model(_context, %RequestOptions{payload_context: %{forced_transcription_model: model}, transport: %{upstream_endpoint: "/backend-api/transcribe"}})
       when is_binary(model), do: model

  defp effective_model(context, options), do: options.routing.effective_model || context.model.exposed_model_id

  defp upstream_model(_context, %RequestOptions{payload_context: %{forced_transcription_model: model}, transport: %{upstream_endpoint: "/backend-api/transcribe"}})
       when is_binary(model), do: model

  defp upstream_model(_context, %RequestOptions{payload_context: %{native_image_request?: true}, routing: %{effective_model: model}, transport: %{upstream_endpoint: endpoint}})
       when endpoint in ["/backend-api/codex/images/generations", "/backend-api/codex/images/edits"] and is_binary(model), do: model

  defp upstream_model(context, _options), do: context.model.upstream_model_id

  defp normalize_mode(mode) when mode in ["full", :full], do: :full
  defp normalize_mode(mode) when mode in ["lite", :lite], do: :lite
  defp normalize_mode(_mode), do: nil
  defp uuid?(value), do: is_binary(value) and Ecto.UUID.cast(value) == {:ok, value}
  defp nullable_uuid?(nil), do: true
  defp nullable_uuid?(value), do: uuid?(value)
  defp exact_string?(value), do: is_binary(value) and byte_size(value) in 1..256 and String.valid?(value) and String.trim(value) == value
  defp valid_past_datetime?(value, now), do: datetime_compare?(value, now, [:lt, :eq])
  defp valid_future_datetime?(value, now), do: datetime_compare?(value, now, [:gt])

  defp datetime_compare?(value, now, comparisons) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, date, 0} -> DateTime.compare(date, now) in comparisons
      _invalid -> false
    end
  end

  defp datetime_compare?(_value, _now, _comparisons), do: false
end

defimpl Inspect, for: CodexPooler.Gateway.Transports.ProviderCreditsAdmission.Context do
  def inspect(_context, _opts), do: "#ProviderCreditsAdmission.Context<version: 1, redacted>"
end

defimpl Inspect, for: CodexPooler.Gateway.Transports.ProviderCreditsAdmission.Receipt do
  def inspect(_receipt, _opts), do: "#ProviderCreditsAdmission.Receipt<version: 1, redacted>"
end
