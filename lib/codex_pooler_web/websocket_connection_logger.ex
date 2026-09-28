defmodule CodexPoolerWeb.WebsocketConnectionLogger do
  @moduledoc false

  require Logger

  alias CodexPooler.Gateway.Contracts
  alias CodexPooler.Gateway.Transports.Websocket.DiagnosticTaxonomy
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.CloseDiagnostics

  @init_failed_message "websocket init failed before request reservation"
  @closed_message "websocket closed before request reservation"
  @failed_native_websocket_turn_message "websocket native turn failed"
  @reconnect_disposition_message "websocket reconnect disposition"
  @handoff_outcome_message "websocket handoff outcome"
  @replay_rejection_message "websocket replay rejection"
  @bandit_oversize_fragmented_message_reason "Received oversize fragmented message"

  @metadata_keys [
    :request_id,
    :endpoint,
    :transport,
    :route_class,
    :error_code,
    :phase,
    :reason_class,
    :reason_code,
    :elapsed_ms,
    :codex_session_id,
    :visible_output,
    :owner_instance_id,
    :proxy_instance_id,
    :rejection_stage,
    :public_code,
    :downstream_epoch,
    :resets_at,
    :resets_in_seconds
  ]
  @reconnect_event_keys [:reconnect_disposition, :handoff_outcome]
  @reconnect_metadata_keys @metadata_keys ++ @reconnect_event_keys
  # `discarded_submission` is the stage for a queued or parked frame the socket
  # threw away without ever dispatching it (findings#175).
  @replay_rejection_stages ~w(owner_preflight replay_preflight native_compaction_deferral discarded_submission)

  # A native socket whose upstream connection closed between two requests
  # closes itself once idle, or says why it stays open, so a
  # `previous_response_not_found` refusal that still follows such a close can
  # be attributed (findings#270). `reason_code` is the upstream close cause and
  # the line joins the upstream close line on `lifecycle_id` and `generation`.
  # With owner forwarding on the owner logs the same kept-open line, with its
  # own skip reasons, for a close it does not pass on; `stale_downstream` is an
  # owner's word that reached a socket bound to the owner anew since.
  @downstream_closed_after_upstream_close_message "websocket downstream closed after upstream connection close"
  @downstream_kept_open_after_upstream_close_message "websocket downstream kept open after upstream connection close"
  @upstream_close_metadata_keys [:reason_code, :skip_reason, :lifecycle_id, :generation, :forwarding, :codex_session_id]
  @upstream_close_skip_reasons ~w(client_frame busy queued public_route revoked handoff reconnect no_completed_response stale_downstream)
  @upstream_close_forwarding ~w(off on)

  # A socket whose websocket owner exited closes, or says why it stays open
  # (findings#276): `reason_code` is how the owner went (`owner_crashed` closes
  # 1011 at once, `owner_drained` closes an idle native socket 1001) and
  # `owner` whether it ran on this node or another.
  @downstream_closed_after_owner_exit_message "websocket downstream closed after owner exit"
  @downstream_kept_open_after_owner_exit_message "websocket downstream kept open after owner exit"
  @owner_exit_metadata_keys [:reason_code, :skip_reason, :owner, :codex_session_id]
  @owner_exit_reason_codes ~w(owner_drained owner_crashed)
  @owner_exit_skip_reasons ~w(client_frame queued public_route revoked handoff reconnect)
  @owner_exit_owners ~w(local remote)

  @type event_metadata :: keyword() | map()

  @spec init_failed_message() :: String.t()
  def init_failed_message, do: @init_failed_message

  @spec closed_message() :: String.t()
  def closed_message, do: @closed_message

  @spec failed_native_websocket_turn_message() :: String.t()
  def failed_native_websocket_turn_message, do: @failed_native_websocket_turn_message

  @spec reconnect_disposition_message() :: String.t()
  def reconnect_disposition_message, do: @reconnect_disposition_message

  @spec handoff_outcome_message() :: String.t()
  def handoff_outcome_message, do: @handoff_outcome_message

  @spec replay_rejection_message() :: String.t()
  def replay_rejection_message, do: @replay_rejection_message

  @spec downstream_closed_after_upstream_close_message() :: String.t()
  def downstream_closed_after_upstream_close_message, do: @downstream_closed_after_upstream_close_message

  @spec downstream_kept_open_after_upstream_close_message() :: String.t()
  def downstream_kept_open_after_upstream_close_message, do: @downstream_kept_open_after_upstream_close_message

  @doc "Why a native socket keeps its connection open after its upstream connection closed."
  @spec upstream_close_skip_reasons() :: [String.t()]
  def upstream_close_skip_reasons, do: @upstream_close_skip_reasons

  @spec downstream_closed_after_owner_exit_message() :: String.t()
  def downstream_closed_after_owner_exit_message, do: @downstream_closed_after_owner_exit_message

  @spec downstream_kept_open_after_owner_exit_message() :: String.t()
  def downstream_kept_open_after_owner_exit_message, do: @downstream_kept_open_after_owner_exit_message

  @doc "How a socket's websocket owner went, as its owner-exit lines name it."
  @spec owner_exit_reason_codes() :: [String.t()]
  def owner_exit_reason_codes, do: @owner_exit_reason_codes

  @doc "Why a socket keeps its connection open after its websocket owner exited."
  @spec owner_exit_skip_reasons() :: [String.t()]
  def owner_exit_skip_reasons, do: @owner_exit_skip_reasons

  @doc """
  Logs a socket closing itself because its websocket owner exited. The
  metadata names `reason_code` (`owner_exit_reason_codes/0`), `owner`
  (`local` or `remote`) and `codex_session_id`; a reason outside that
  vocabulary logs nothing.
  """
  @spec log_downstream_closed_after_owner_exit(event_metadata()) :: :ok
  def log_downstream_closed_after_owner_exit(metadata) do
    metadata = metadata |> normalize_metadata() |> Map.delete(:skip_reason) |> Map.delete("skip_reason")
    log_owner_exit_event(@downstream_closed_after_owner_exit_message, metadata)
  end

  @doc """
  Logs why a socket keeps its connection open after its websocket owner
  exited: the same metadata as `log_downstream_closed_after_owner_exit/1` plus
  a `skip_reason` from `owner_exit_skip_reasons/0`. A reason outside those
  vocabularies logs nothing.
  """
  @spec log_downstream_kept_open_after_owner_exit(event_metadata()) :: :ok
  def log_downstream_kept_open_after_owner_exit(metadata) do
    metadata = normalize_metadata(metadata)

    case fixed_vocabulary(metadata_value(metadata, :skip_reason), @owner_exit_skip_reasons) do
      nil -> :ok
      _skip_reason -> log_owner_exit_event(@downstream_kept_open_after_owner_exit_message, metadata)
    end
  end

  defp log_owner_exit_event(message, metadata) do
    case fixed_vocabulary(metadata_value(metadata, :reason_code), @owner_exit_reason_codes) do
      nil -> :ok
      _reason_code -> log_event(:info, message, metadata, nil, @owner_exit_metadata_keys)
    end
  end

  @doc """
  Logs a native socket closing itself because its upstream connection closed
  between two requests. The metadata names the close `reason_code` (a cause
  from `CloseDiagnostics.anchor_invalidating_causes/0`), `lifecycle_id`,
  `generation`, `forwarding` (`off` or `on`) and `codex_session_id`; a cause
  outside that vocabulary logs nothing.
  """
  @spec log_downstream_closed_after_upstream_close(event_metadata()) :: :ok
  def log_downstream_closed_after_upstream_close(metadata) do
    metadata = metadata |> normalize_metadata() |> Map.delete(:skip_reason) |> Map.delete("skip_reason")
    log_upstream_close_event(@downstream_closed_after_upstream_close_message, metadata)
  end

  @doc """
  Logs why a native socket keeps its connection open after its upstream
  connection closed between two requests: the same metadata as
  `log_downstream_closed_after_upstream_close/1` plus a `skip_reason` from
  `upstream_close_skip_reasons/0`. A reason or cause outside those
  vocabularies logs nothing.
  """
  @spec log_downstream_kept_open_after_upstream_close(event_metadata()) :: :ok
  def log_downstream_kept_open_after_upstream_close(metadata) do
    metadata = normalize_metadata(metadata)

    case fixed_vocabulary(metadata_value(metadata, :skip_reason), @upstream_close_skip_reasons) do
      nil -> :ok
      _skip_reason -> log_upstream_close_event(@downstream_kept_open_after_upstream_close_message, metadata)
    end
  end

  # The cause is checked at run time against the upstream session's own
  # vocabulary, never a copy of it.
  defp log_upstream_close_event(message, metadata) do
    if CloseDiagnostics.anchor_invalidating_cause?(metadata_value(metadata, :reason_code)),
      do: log_event(:info, message, metadata, nil, @upstream_close_metadata_keys),
      else: :ok
  end

  @spec log_init_failed_before_request_reservation(event_metadata(), term()) :: :ok
  def log_init_failed_before_request_reservation(metadata, reason) do
    log_event(:warning, @init_failed_message, metadata, reason)
  end

  @spec log_closed_before_request_reservation(event_metadata(), term()) :: :ok
  def log_closed_before_request_reservation(metadata, reason) do
    log_event(:info, @closed_message, metadata, reason)
  end

  @spec log_failed_native_websocket_turn(event_metadata(), term()) :: :ok
  def log_failed_native_websocket_turn(metadata, reason) do
    usage_limit = Contracts.usage_limit_record(unwrap_reason(reason))

    metadata =
      metadata
      |> normalize_metadata()
      |> put_native_reason_code(reason)
      |> Map.put(:resets_at, usage_limit["resets_at"])
      |> Map.put(:resets_in_seconds, usage_limit["resets_in_seconds"])

    log_event(
      failed_native_websocket_turn_level(metadata_value(metadata, :error_code), usage_limit),
      @failed_native_websocket_turn_message,
      failure_log_metadata(metadata),
      reason
    )
  end

  # The terminal usage-limit refusal of an all-exhausted Pool is the designed
  # answer, logged at `info` with the reset it advised like the HTTP
  # `request_completed` line of the same refusal (findings#206 row 206-553).
  defp failed_native_websocket_turn_level(_error_code, %{"resets_at" => _resets_at}), do: :info
  defp failed_native_websocket_turn_level(error_code, _usage_limit), do: failed_native_websocket_turn_level(error_code)

  defp unwrap_reason({:error, reason}), do: unwrap_reason(reason)
  defp unwrap_reason(reason), do: reason

  @spec log_reconnect_disposition(event_metadata(), term()) :: :ok
  def log_reconnect_disposition(metadata, disposition) do
    log_fixed_reconnect_event(
      @reconnect_disposition_message,
      metadata,
      :reconnect_disposition,
      DiagnosticTaxonomy.reconnect_disposition(disposition)
    )
  end

  @spec log_handoff_outcome(event_metadata(), term()) :: :ok
  def log_handoff_outcome(metadata, outcome) do
    log_fixed_reconnect_event(
      @handoff_outcome_message,
      metadata,
      :handoff_outcome,
      DiagnosticTaxonomy.handoff_outcome(outcome)
    )
  end

  @doc """
  Logs a websocket replay rejection with its stage and the owner's reason.

  `public_code` is the error code the client actually received, which can
  differ from the reason (an owner `owner_unavailable` refusal of a recorded
  turn's resend reaches the client as `duplicate_turn`); like the runtime
  preflight line, the line carries both (findings#217 row 217-63).
  """
  @spec log_replay_rejection(event_metadata(), term(), term(), term()) :: :ok
  def log_replay_rejection(metadata, stage, reason, public_code \\ nil) do
    case fixed_vocabulary(stage, @replay_rejection_stages) do
      nil ->
        :ok

      rejection_stage ->
        metadata =
          metadata
          |> normalize_metadata()
          |> Map.put(:rejection_stage, rejection_stage)
          |> Map.delete("public_code")
          |> Map.put(:public_code, public_code)
          |> put_native_reason_code(reason)

        log_event(:info, @replay_rejection_message, metadata, reason)
    end
  end

  @spec failed_native_websocket_turn_level(term()) :: :info | :warning
  def failed_native_websocket_turn_level(:client_disconnected), do: :info
  def failed_native_websocket_turn_level(:owner_drained), do: :info
  def failed_native_websocket_turn_level("client_disconnected"), do: :info
  def failed_native_websocket_turn_level("owner_drained"), do: :info
  def failed_native_websocket_turn_level(_error_code), do: :warning

  @spec reason_class(term()) :: String.t()
  def reason_class(:normal), do: "normal"
  def reason_class(:closed), do: "closed"
  def reason_class(:remote), do: "remote"
  def reason_class(:timeout), do: "timeout"
  def reason_class(:shutdown), do: "shutdown"
  def reason_class({:shutdown, _reason}), do: "shutdown"
  def reason_class({:error, reason}), do: reason_class(reason)
  def reason_class({:EXIT, _reason}), do: "exit"
  def reason_class({:deserializing, reason}), do: reason_class(reason)
  def reason_class({reason, _details}) when is_atom(reason), do: Atom.to_string(reason)
  def reason_class(reason) when is_atom(reason), do: Atom.to_string(reason)

  def reason_class(@bandit_oversize_fragmented_message_reason),
    do: "max_fragmented_message_size_exceeded"

  def reason_class(reason) when is_binary(reason), do: "binary_reason"
  def reason_class(reason) when is_integer(reason), do: "numeric_reason"
  def reason_class(%module{}) when is_atom(module), do: safe_log_value(inspect(module))

  def reason_class(reason) when is_map(reason),
    do: DiagnosticTaxonomy.reason_code(reason) || "non_atom_reason"

  def reason_class(_reason), do: "non_atom_reason"

  defp log_fixed_reconnect_event(_message, _metadata, _key, nil), do: :ok

  defp log_fixed_reconnect_event(message, metadata, key, value) do
    metadata =
      metadata
      |> normalize_metadata()
      |> drop_reconnect_event_values()
      |> Map.put(key, value)

    log_event(:info, message, metadata, nil, @reconnect_metadata_keys)
  end

  defp log_event(level, message, metadata, reason),
    do: log_event(level, message, metadata, reason, @metadata_keys)

  defp log_event(level, message, metadata, reason, metadata_keys) do
    log_metadata =
      metadata
      |> normalize_metadata()
      |> maybe_put_reason_class(reason)
      |> allowed_metadata(metadata_keys)
      |> Enum.map_join(" ", fn {key, value} -> "#{key}=#{safe_log_value(key, value)}" end)

    Logger.log(level, fn -> message <> metadata_suffix(log_metadata) end)

    :ok
  end

  defp metadata_suffix(""), do: ""
  defp metadata_suffix(metadata), do: " " <> metadata

  defp normalize_metadata(metadata) when is_list(metadata), do: Map.new(metadata)
  defp normalize_metadata(metadata) when is_map(metadata), do: metadata
  defp normalize_metadata(_metadata), do: %{}

  defp drop_reconnect_event_values(metadata) do
    Enum.reduce(@reconnect_event_keys, metadata, fn key, metadata ->
      metadata
      |> Map.delete(key)
      |> Map.delete(Atom.to_string(key))
    end)
  end

  defp maybe_put_reason_class(metadata, nil), do: metadata

  defp maybe_put_reason_class(metadata, reason),
    do: Map.put(metadata, :reason_class, reason_class(reason))

  defp allowed_metadata(metadata, metadata_keys) do
    metadata_keys
    |> Enum.reduce([], fn key, acc ->
      value = allowed_metadata_value(key, metadata_value(metadata, key))

      if is_nil(value) do
        acc
      else
        [{key, value} | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp allowed_metadata_value(:reconnect_disposition, value),
    do: DiagnosticTaxonomy.reconnect_disposition(value)

  defp allowed_metadata_value(:handoff_outcome, value),
    do: DiagnosticTaxonomy.handoff_outcome(value)

  defp allowed_metadata_value(:rejection_stage, value),
    do: fixed_vocabulary(value, @replay_rejection_stages)

  defp allowed_metadata_value(:skip_reason, value),
    do: fixed_vocabulary(value, @upstream_close_skip_reasons)

  defp allowed_metadata_value(:forwarding, value),
    do: fixed_vocabulary(value, @upstream_close_forwarding)

  defp allowed_metadata_value(:owner, value),
    do: fixed_vocabulary(value, @owner_exit_owners)

  defp allowed_metadata_value(:generation, value) when is_integer(value) and value > 0, do: value
  defp allowed_metadata_value(:generation, _value), do: nil

  defp allowed_metadata_value(_key, value), do: value

  defp metadata_value(metadata, key) do
    Map.get(metadata, key) || Map.get(metadata, Atom.to_string(key))
  end

  defp failure_log_metadata(metadata) do
    metadata
    |> replace_failure_correlator(:request_id)
    |> replace_failure_code(:error_code)
  end

  defp replace_failure_correlator(metadata, key) do
    value = metadata_value(metadata, key)
    metadata = Map.delete(metadata, Atom.to_string(key))
    Map.put(metadata, key, DiagnosticTaxonomy.safe_correlator(value))
  end

  defp replace_failure_code(metadata, key) do
    value = metadata_value(metadata, key)
    metadata = Map.delete(metadata, Atom.to_string(key))

    case DiagnosticTaxonomy.identifier(value) do
      nil -> Map.delete(metadata, key)
      identifier -> Map.put(metadata, key, identifier)
    end
  end

  defp put_native_reason_code(metadata, reason) do
    metadata =
      metadata
      |> Map.delete(:reason_code)
      |> Map.delete("reason_code")

    case DiagnosticTaxonomy.reason_code(reason) do
      nil -> metadata
      reason_code -> Map.put(metadata, :reason_code, reason_code)
    end
  end

  defp fixed_vocabulary(value, vocabulary) when is_atom(value) do
    value
    |> Atom.to_string()
    |> fixed_vocabulary(vocabulary)
  end

  defp fixed_vocabulary(value, vocabulary) when is_binary(value) do
    if value in vocabulary, do: value
  end

  defp fixed_vocabulary(_value, _vocabulary), do: nil

  defp safe_log_value(key, value) when key in [:error_code, :reason_code, :reason_class, :public_code],
    do: DiagnosticTaxonomy.identifier(value) || "unknown"

  defp safe_log_value(_key, value), do: safe_log_value(value)

  defp safe_log_value(value) when is_atom(value),
    do: value |> Atom.to_string() |> DiagnosticTaxonomy.safe_correlator()

  defp safe_log_value(value) when is_integer(value), do: Integer.to_string(value)

  defp safe_log_value(value) when is_binary(value) do
    DiagnosticTaxonomy.safe_correlator(value)
  end

  defp safe_log_value(_value), do: "unknown"
end
