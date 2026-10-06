defmodule CodexPoolerWeb.Admin.UpstreamAccountsReadModel.SavedResetOperationProjection do
  @moduledoc false

  alias CodexPooler.Admin.UpstreamRoutingReadiness
  alias CodexPooler.Upstreams.SavedResets
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.{Formatting, SavedResetConfirmationProjection}
  alias CodexPoolerWeb.DateTimeDisplay

  @unknown_caveat "The request may have reached the provider. Don't redeem again until this resolves."

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
          required(:summary) => String.t() | nil,
          required(:detail) => String.t() | nil,
          required(:outcome_caveat) => String.t() | nil,
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
    latest? = is_map(redemption) and map_size(redemption) > 0
    active? = request.state in [:queued, :processing] or active_lifecycle?(record, verification)
    {headline, summary, outcome_stated?} = copy(record, request, outcome, verification, now)
    caveat = outcome_caveat(outcome, result_code(record))

    %{
      request: request,
      provider_outcome: outcome,
      verification: verification,
      headline: headline,
      summary: summary,
      detail: if(outcome_stated?, do: nil, else: caveat),
      outcome_caveat: caveat,
      usage_poll_pause: pause,
      view_paused?: false,
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
    |> observe(paused?: Map.get(context, :view_paused?, false) == true, connected?: Map.get(context, :view_connected?, true))
  end

  @doc """
  Applies the viewer's live-update state and any quota-polling pause to a
  projected operation. An override replaces the outcome summary, so the
  outcome caveat moves into `detail` and a warning such as "Don't redeem
  again" survives a paused or disconnected view.
  """
  @spec observe(t(), keyword()) :: t()
  def observe(%{} = operation, opts) do
    paused? = Keyword.get(opts, :paused?, false) or operation.view_paused?
    visible? = operation.show_latest_receipt? or operation.active?

    case observation_copy(visible?, operation.usage_poll_pause, paused?, Keyword.get(opts, :connected?, true)) do
      {headline, summary} -> %{operation | headline: headline, summary: summary, detail: operation.outcome_caveat, view_paused?: paused?}
      nil -> %{operation | view_paused?: paused?}
    end
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

  defp request_copy(:queued), do: {"Request accepted", "Queued. Nothing has been sent to the provider yet."}
  defp request_copy(:processing), do: {"Request accepted", "Being processed. The provider has not answered yet."}
  defp request_copy(:stopped), do: {"Request stopped", "The job ended without a recorded result. Check the latest reset before acting."}
  defp request_copy(:unavailable), do: {"Request status unavailable", nil}
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

  # The third element says whether the summary already states the provider
  # outcome; when it does not, the outcome caveat is shown as `detail`. Only an
  # applied reset may be described as applied or spent: a reblocked record can
  # be a non-consuming failure, and an expired one can follow an unresolved
  # consume.
  defp copy(_record, _request, _outcome, :quota_confirmed, _now), do: {"Quota confirmed", "The new quota cycle is confirmed.", false}
  defp copy(_record, _request, _outcome, :request_verified, _now), do: {"Recovery verified by a request", "A request succeeded on the new quota; the usage report has not caught up yet.", false}
  defp copy(_record, _request, :applied, :reblocked, _now), do: {"Quota is still unavailable", "The reset was applied, but quota is still blocked. No further reset is spent automatically.", true}
  defp copy(_record, _request, _outcome, :reblocked, _now), do: {"Quota is still unavailable", "Quota is still blocked.", false}
  defp copy(_record, _request, :applied, :expired, _now), do: {"Quota confirmation timed out", "No usage report confirmed the new quota in time. The spent reset is not refunded.", true}
  defp copy(_record, _request, _outcome, :expired, _now), do: {"Quota confirmation timed out", "No usage report confirmed the new quota in time.", false}

  defp copy(record, _request, :applied, verification, now) when verification in [:pending, :candidate] do
    if deadline_passed?(record, now) do
      {"Deadline passed — checking quota", "Waiting for the latest quota check.", true}
    else
      {"Reset applied — verifying quota", "Waiting for a usage report to confirm the new quota.", true}
    end
  end

  defp copy(record, _request, :not_applied, _verification, _now), do: noop_copy(result_code(record))

  defp copy(record, request, outcome, _verification, now) do
    cond do
      record["phase"] == "consuming" and fresh_consuming?(record, now) -> {"Reset request in progress", "Waiting for the provider's answer.", true}
      outcome == :unknown -> {"Reset outcome not confirmed", @unknown_caveat, true}
      outcome == :applied -> {"Reset applied", "The provider applied the reset.", true}
      request.state in [:queued, :processing, :stopped, :unavailable] -> {request.headline, request.summary, false}
      true -> {"No reset result recorded", nil, false}
    end
  end

  defp noop_copy("no_credit"), do: {"No saved reset was available", "The provider had no saved reset to apply.", true}
  defp noop_copy("nothing_to_reset"), do: {"Nothing needed resetting", "The provider found no exhausted quota to reset.", true}
  defp noop_copy("consume_not_applied"), do: {"Reset was not applied", "The request stopped before reaching the provider.", true}

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

  defp outcome_caveat(:not_applied, "consume_not_applied"), do: "The request stopped before reaching the provider."
  defp outcome_caveat(:not_applied, _code), do: "The provider reported that the reset was not applied."
  defp outcome_caveat(:unknown, _code), do: @unknown_caveat
  defp outcome_caveat(_applied_or_not_recorded, _code), do: nil

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

  defp observation_copy(true, _pause, _paused?, false), do: {"Live updates disconnected", "The reset continues. Reconnect to see the latest status before acting."}
  defp observation_copy(true, %{state: state}, _paused?, _connected?) when state in [:paused, :unavailable], do: {"Quota checks are delayed", "Usage polling is paused or unavailable, so confirmation can take longer."}
  defp observation_copy(true, _pause, true, _connected?), do: {"Live updates paused", "The reset continues. Refresh reads the stored status."}
  defp observation_copy(_visible?, _pause, _paused?, _connected?), do: nil

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
