defmodule CodexPooler.Gateway.Websocket.NativeCompactionRefusalLog do
  @moduledoc false

  # One warning for every refusal of a native compaction whose admission was
  # unavailable, whichever route decided it, and operators count refusals by
  # it. The socket decides on arrival (nothing tracked), at dequeue behind a
  # tracked response task, and on the active-turn reconnect route, which
  # decides from the cause the frame met on arrival (findings#206 row 206-394).
  # The runtime decides at the reservation's accounting start, when the
  # upstream connection the reservation was bound to closed in between
  # (findings#284); that refusal used to leave only the generic failed-turn
  # line (findings#270 row 270-246). `decided_at` names the route and
  # `reservation_phase` the reservation (`final` is the turn that continues on
  # a compacted history, whose metadata names no compaction phase).

  require Logger

  alias CodexPooler.Gateway.Payloads.NativeCodexTurnMetadata
  alias CodexPooler.Gateway.Transports.Websocket.DiagnosticTaxonomy

  @routes [:arrival, :dequeue, :reconnect, :accounting_start]

  @type refusal :: %{
          required(:refusal) => %{required(:code) => String.t(), required(:status) => pos_integer(), optional(atom()) => term()},
          required(:metadata) => NativeCodexTurnMetadata.t() | nil,
          required(:reservation_phase) => atom(),
          required(:cause) => term(),
          required(:decided_at) => :arrival | :dequeue | :reconnect | :accounting_start,
          required(:topology) => :direct | :forwarded,
          required(:codex_session_id) => String.t()
        }

  @spec warn(refusal()) :: :ok
  def warn(%{decided_at: decided_at, topology: topology} = fields) when decided_at in @routes and topology in [:direct, :forwarded] do
    Logger.warning(fn ->
      "native compaction refused before dispatch " <>
        "reason=admission_unavailable " <>
        "cause=#{DiagnosticTaxonomy.identifier(fields.cause) || "unknown"} " <>
        "code=#{fields.refusal.code} " <>
        "status=#{fields.refusal.status} " <>
        "compaction_phase=#{compaction_phase(fields.metadata)} " <>
        "topology=#{topology} " <>
        "decided_at=#{decided_at} " <>
        "reservation_phase=#{fields.reservation_phase} " <>
        "codex_session_id=#{fields.codex_session_id}"
    end)
  end

  defp compaction_phase(%NativeCodexTurnMetadata{compaction: %NativeCodexTurnMetadata.Compaction{phase: phase}}), do: phase
  defp compaction_phase(_metadata), do: "none"
end
