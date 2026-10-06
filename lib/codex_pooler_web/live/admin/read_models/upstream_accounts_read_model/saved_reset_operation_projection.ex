defmodule CodexPoolerWeb.Admin.UpstreamAccountsReadModel.SavedResetOperationProjection do
  @moduledoc false

  alias CodexPooler.Admin.UpstreamRoutingReadiness
  alias CodexPooler.Upstreams.SavedResets
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.{Formatting, SavedResetConfirmationProjection}
  alias CodexPoolerWeb.DateTimeDisplay

  @type request_state :: :none | :queued | :processing | :stopped | :unavailable
  @type provider_outcome :: :applied | :not_applied | :unknown | :not_recorded
  @type verification :: :not_started | :pending | :candidate | :quota_confirmed | :request_verified | :reblocked | :expired | :unknown
  @type request_fact :: %{required(:state) => request_state(), optional(:requested_at) => DateTime.t() | nil, optional(:scheduled_at) => DateTime.t() | nil}
  @type request_summary :: %{required(:open) => request_fact() | nil, required(:latest_terminal) => request_fact() | nil}
  @type usage_pause :: %{required(:paused_until) => DateTime.t(), optional(atom()) => term()}
  @type context :: %{
          required(:snapshot_at) => DateTime.t(),
          required(:datetime_preferences) => DateTimeDisplay.preferences(),
          optional(:redemption) => map() | nil,
          optional(:request_summary) => request_summary() | :unavailable | nil,
          optional(:confirmation) => SavedResetConfirmationProjection.t() | nil,
          optional(:serving_readiness) => UpstreamRoutingReadiness.t() | nil,
          optional(:usage_poll_pause) => usage_pause() | :unavailable | nil,
          optional(:last_checked_at) => DateTime.t() | nil,
          optional(:view_paused?) => boolean(),
          optional(:view_connected?) => boolean()
        }
  @type request :: %{required(:state) => request_state(), required(:headline) => String.t() | nil, required(:summary) => String.t() | nil, optional(:requested_at) => String.t(), optional(:scheduled_at) => String.t()}
  @type t :: %{
          required(:request) => request(),
          required(:provider_outcome) => provider_outcome(),
          required(:verification) => verification(),
          required(:headline) => String.t(),
          required(:summary) => String.t(),
          required(:detail) => String.t(),
          required(:usage_poll_pause) => %{state: :none | :paused | :unavailable, pause_until: String.t() | nil},
          required(:view_paused?) => boolean(),
          required(:active?) => boolean(),
          required(:refreshable?) => boolean(),
          required(:show_latest_receipt?) => boolean(),
          required(:serving_readiness) => UpstreamRoutingReadiness.t() | nil,
          optional(:started_at) => String.t(),
          optional(:consumed_at) => String.t(),
          optional(:finished_at) => String.t(),
          optional(:deadline_at) => String.t(),
          optional(:last_checked_at) => String.t(),
          optional(:pause_until) => String.t()
        }

  @spec project(context()) :: t()
  def project(%{snapshot_at: %DateTime{} = now, datetime_preferences: preferences} = context) do
    redemption = Map.get(context, :redemption)
    record = if is_map(redemption), do: redemption, else: %{}
    request = request(Map.get(context, :request_summary), preferences, now)
    outcome = provider_outcome(redemption, now)
    verification = verification(record, Map.get(context, :confirmation))
    pause = usage_pause(Map.get(context, :usage_poll_pause), preferences, now)
    paused? = Map.get(context, :view_paused?, false) == true
    latest? = is_map(redemption) and map_size(redemption) > 0
    active? = request.state in [:queued, :processing] or active_lifecycle?(record, verification)

    {headline, summary} = copy(record, request, outcome, verification, now)
    {headline, summary} = observation_copy(headline, summary, latest? or active?, pause, paused?, Map.get(context, :view_connected?, true))

    %{
      request: request,
      provider_outcome: outcome,
      verification: verification,
      headline: headline,
      summary: summary,
      detail: outcome_detail(outcome, result_code(record)),
      usage_poll_pause: pause,
      view_paused?: paused?,
      active?: active?,
      refreshable?: latest? or request.state != :none,
      show_latest_receipt?: latest?,
      serving_readiness: Map.get(context, :serving_readiness)
    }
    |> put_time(:started_at, record["started_at"], preferences, now)
    |> put_time(:finished_at, record["finished_at"], preferences, now)
    |> put_consumed_time(outcome, record, preferences, now)
    |> put_time(:deadline_at, record["deadline_at"], preferences, nil)
    |> put_time(:last_checked_at, Map.get(context, :last_checked_at), preferences, now)
    |> put_pause_time(pause)
  end

  defp request(summary, preferences, now) do
    fact = request_fact(summary)
    state = Map.get(fact, :state, :none)
    {headline, summary} = request_copy(state)

    %{state: state, headline: headline, summary: summary}
    |> put_time(:requested_at, Map.get(fact, :requested_at), preferences, now)
    |> put_time(:scheduled_at, Map.get(fact, :scheduled_at), preferences, nil)
  end

  defp request_fact(%{open: %{state: state} = open}) when state in [:queued, :processing], do: open
  defp request_fact(%{latest_terminal: %{state: :stopped} = terminal}), do: terminal
  defp request_fact(:unavailable), do: %{state: :unavailable}
  defp request_fact(nil), do: %{state: :none}
  defp request_fact(%{open: nil, latest_terminal: nil}), do: %{state: :none}
  defp request_fact(_invalid), do: %{state: :unavailable}

  defp request_copy(:queued), do: {"Request accepted", "Your request is queued. Provider application has not been confirmed yet."}
  defp request_copy(:processing), do: {"Request accepted", "Your request is being processed. Provider application has not been confirmed yet."}
  defp request_copy(:stopped), do: {"Request stopped", "This request is no longer queued. Its job status does not establish whether a reset was applied."}
  defp request_copy(:unavailable), do: {"Request status unavailable", "The recorded manual request status is unavailable."}
  defp request_copy(:none), do: {nil, nil}

  defp provider_outcome(nil, _now), do: :not_recorded

  defp provider_outcome(record, now) when is_map(record) do
    cond do
      map_size(record) == 0 -> :not_recorded
      not valid_result_clocks?(record, now) -> :unknown
      not valid_replay?(Map.get(record, "provider_replay"), now) -> :unknown
      true -> application_result(record["result"], record)
    end
  end

  defp provider_outcome(_invalid, _now), do: :unknown

  defp application_result(%{"applied" => true, "code" => code}, record) when code in ["reset", "already_redeemed", "target_redeemed"] do
    if contradictory_applied?(record), do: :unknown, else: :applied
  end

  defp application_result(%{"applied" => false, "code" => code}, record) when code in ["no_credit", "nothing_to_reset"] do
    if record["phase"] in ["consumed_pending_probe", "confirmed_by_upstream", "confirmed_by_quota"] or record["consumed_at"] != nil, do: :unknown, else: :not_applied
  end

  defp application_result(%{"applied" => false, "code" => "consume_not_applied"}, record) do
    if record["phase"] == "consume_not_applied" and match?(%{"version" => 1, "provider_dispatches" => 0}, record["provider_replay"]) and record["consumed_at"] == nil, do: :not_applied, else: :unknown
  end

  defp application_result(_invalid, _record), do: :unknown

  defp contradictory_applied?(record) do
    record["phase"] in ["consuming", "consume_not_applied"] or
      match?(%{"version" => 1, "provider_dispatches" => 0}, record["provider_replay"])
  end

  defp valid_result_clocks?(record, now) do
    valid? =
      Enum.all?(["started_at", "consumed_at", "finished_at"], fn key ->
        record[key] == nil or match?(%DateTime{}, nonfuture_time(record[key], now))
      end)

    started = Formatting.parse_datetime(record["started_at"])
    consumed = Formatting.parse_datetime(record["consumed_at"])
    finished = Formatting.parse_datetime(record["finished_at"])
    valid? and ordered_times?(started, consumed) and ordered_times?(started, finished) and ordered_times?(consumed, finished)
  end

  defp ordered_times?(%DateTime{} = earlier, %DateTime{} = later), do: DateTime.compare(earlier, later) != :gt
  defp ordered_times?(_earlier, _later), do: true

  defp valid_replay?(nil, _now), do: true

  defp valid_replay?(%{"version" => 1, "provider_dispatches" => count} = replay, now) when is_integer(count) and count in 0..6 do
    dispatch_time = Map.get(replay, "last_provider_dispatched_at")
    clock_valid? = dispatch_time == nil or match?(%DateTime{}, nonfuture_time(dispatch_time, now))
    clock_valid? and zero_dispatch_consistent?(count, replay)
  end

  defp valid_replay?(_invalid, _now), do: false

  defp zero_dispatch_consistent?(0, replay), do: Map.get(replay, "last_provider_dispatched_at") == nil and pre_dispatch_observation?(replay)
  defp zero_dispatch_consistent?(_positive, _replay), do: true

  defp pre_dispatch_observation?(%{"mode" => "observe_only", "last_code" => code}) when code in ["write_budget_exhausted", "scope_changed"], do: true
  defp pre_dispatch_observation?(replay), do: Map.get(replay, "last_code") == nil

  defp verification(%{"phase" => "confirmed_by_quota"}, _confirmation), do: :quota_confirmed
  defp verification(%{"phase" => "confirmed_by_upstream"}, _confirmation), do: :request_verified
  defp verification(%{"phase" => "reblocked"}, _confirmation), do: :reblocked
  defp verification(%{"phase" => "expired"}, _confirmation), do: :expired
  defp verification(%{"phase" => "consumed_pending_probe"}, %{challenged_evidence_state: :candidate_progressing}), do: :candidate
  defp verification(%{"phase" => "consumed_pending_probe"}, _confirmation), do: :pending
  defp verification(%{"phase" => "consuming"}, _confirmation), do: :not_started
  defp verification(%{"phase" => "consume_not_applied"}, _confirmation), do: :not_started
  defp verification(%{"phase" => _unknown}, _confirmation), do: :unknown
  defp verification(_legacy, _confirmation), do: :not_started

  defp active_lifecycle?(%{"phase" => "consuming"}, _verification), do: true
  defp active_lifecycle?(%{"status" => "redeeming"}, _verification), do: true
  defp active_lifecycle?(_record, verification), do: verification in [:pending, :candidate, :request_verified]

  defp copy(_record, _request, _outcome, :quota_confirmed, _now), do: {"Quota confirmed", "The latest account quota has been confirmed. Serving availability follows the account's current readiness."}
  defp copy(_record, _request, _outcome, :request_verified, _now), do: {"Recovery verified by a request", "Quota synchronization is still pending. Availability remains subject to the existing recovery limits."}
  defp copy(_record, _request, _outcome, :reblocked, _now), do: {"Quota is still unavailable", "Latest quota availability remains blocked. This does not authorize another redemption."}
  defp copy(_record, _request, _outcome, :expired, _now), do: {"Quota confirmation timed out", "The recorded confirmation window expired. This does not refund a reset or authorize another redemption."}

  defp copy(record, _request, :applied, verification, now) when verification in [:pending, :candidate] do
    if deadline_passed?(record, now) do
      {"Confirmation deadline passed; latest status is being checked", "The recorded deadline has passed. The latest persisted verification status is still pending."}
    else
      {"Reset applied — verifying quota", "The reset was applied. Quota availability is still being verified. This verification does not start another redemption."}
    end
  end

  defp copy(record, _request, :not_applied, _verification, _now), do: noop_copy(result_code(record))

  defp copy(record, request, outcome, _verification, now) do
    cond do
      record["phase"] == "consuming" and fresh_consuming?(record, now) -> {"Reset request in progress", "Provider application has not been confirmed yet."}
      outcome == :unknown -> {"Reset outcome not confirmed", "The request may have reached the provider. Do not submit another redemption while its outcome is unresolved."}
      outcome == :applied -> {"Reset applied", "The reset was applied. Current availability follows the account's serving readiness."}
      request.state in [:queued, :processing, :stopped, :unavailable] -> {request.headline, request.summary}
      true -> {"No reset result recorded", "No latest account reset result is recorded."}
    end
  end

  defp noop_copy("no_credit"), do: {"No saved reset was available", "The provider reported that no saved reset was available. No credit consumption is confirmed."}
  defp noop_copy("nothing_to_reset"), do: {"No eligible quota needed resetting", "The provider reported that no eligible quota needed resetting. No credit consumption is confirmed."}
  defp noop_copy("consume_not_applied"), do: {"Reset was not applied", "The request stopped before provider dispatch. Action availability still requires current account checks."}

  defp fresh_consuming?(record, now) do
    with nil <- Map.get(record, "result"),
         true <- valid_replay?(Map.get(record, "provider_replay"), now),
         :clear <- replay_category(Map.get(record, "provider_replay")),
         %DateTime{} = started <- nonfuture_time(record["started_at"], now) do
      DateTime.diff(now, started, :millisecond) < SavedResets.redemption_receive_timeout_ms() + SavedResets.redemption_stale_grace_ms()
    else
      _uncertain -> false
    end
  end

  # Replay codes are decoded into closed categories and never copied to output.
  defp replay_category(nil), do: :clear
  defp replay_category(%{"version" => 1, "last_code" => code}) when code in ["transport_error", "provider_failed", "persistence_failed", "no_credit", "nothing_to_reset", "list_failed", "target_redeeming", "target_available", "quota_unresolved", "scope_changed", "target_invalid", "missing_access_token", "legacy_unresolved", "write_budget_exhausted"], do: :ambiguous

  defp replay_category(%{"version" => 1, "provider_dispatches" => count} = replay) when is_integer(count) and count >= 0 do
    if Map.get(replay, "last_code") in [nil, "dispatch_reserved"], do: :clear, else: :ambiguous
  end

  defp replay_category(_invalid), do: :ambiguous

  defp deadline_passed?(record, now) do
    case Formatting.parse_datetime(record["deadline_at"]) do
      %DateTime{} = deadline -> DateTime.compare(now, deadline) != :lt
      nil -> false
    end
  end

  defp outcome_detail(:applied, _code), do: "The reset was applied. Current serving readiness is shown separately."
  defp outcome_detail(:not_applied, "consume_not_applied"), do: "The request stopped before provider dispatch."
  defp outcome_detail(:not_applied, _code), do: "The provider reported that the reset was not applied."
  defp outcome_detail(:unknown, _code), do: "Application details are not available or are unresolved. Do not assume another redemption is safe."
  defp outcome_detail(:not_recorded, _code), do: "Application details are not available."

  defp result_code(%{"result" => %{"code" => code}}) when code in ["reset", "already_redeemed", "target_redeemed", "no_credit", "nothing_to_reset", "consume_not_applied"], do: code
  defp result_code(_record), do: nil

  defp usage_pause(%{paused_until: %DateTime{} = until}, preferences, now) do
    if DateTime.compare(until, now) == :gt do
      %{state: :paused, pause_until: DateTimeDisplay.format_datetime(until, preferences)}
    else
      %{state: :none, pause_until: nil}
    end
  end

  defp usage_pause(nil, _preferences, _now), do: %{state: :none, pause_until: nil}
  defp usage_pause(_unavailable, _preferences, _now), do: %{state: :unavailable, pause_until: nil}

  defp observation_copy(_headline, _summary, true, _pause, _paused?, false), do: {"Status updates are disconnected", "The operation continues independently. Reconnect to read the current recorded status before acting."}
  defp observation_copy(_headline, _summary, true, %{state: state}, _paused?, _connected?) when state in [:paused, :unavailable], do: {"Quota checks are delayed", "Quota polling is paused or unavailable. Refresh status reads stored data only; it does not contact the provider or clear the pause."}
  defp observation_copy(_headline, _summary, true, _pause, true, _connected?), do: {"Live updates are paused", "The operation continues independently. Refresh status reads stored data without resuming live updates."}
  defp observation_copy(headline, summary, _visible?, _pause, _paused?, _connected?), do: {headline, summary}

  defp put_consumed_time(result, :applied, record, preferences, now), do: put_time(result, :consumed_at, record["consumed_at"], preferences, now)
  defp put_consumed_time(result, _outcome, _record, _preferences, _now), do: result
  defp put_pause_time(result, %{pause_until: nil}), do: result
  defp put_pause_time(result, %{pause_until: until}), do: Map.put(result, :pause_until, until)

  defp put_time(result, key, value, preferences, upper) do
    case nonfuture_time(value, upper) do
      %DateTime{} = time -> Map.put(result, key, DateTimeDisplay.format_datetime(time, preferences))
      nil -> result
    end
  end

  defp nonfuture_time(value, upper) do
    case Formatting.parse_datetime(value) do
      %DateTime{} = time -> if upper == nil or DateTime.compare(time, upper) != :gt, do: time, else: nil
      nil -> nil
    end
  end
end
