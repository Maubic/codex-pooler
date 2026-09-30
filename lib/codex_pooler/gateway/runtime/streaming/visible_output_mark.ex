defmodule CodexPooler.Gateway.Runtime.Streaming.VisibleOutputMark do
  @moduledoc """
  Stamps a Codex turn visible (`SessionContinuity.mark_codex_turn_visible/2`)
  before the relay hands the client the turn's first visible output.

  The stamp is not bookkeeping. `first_visible_output_at` is the authority the
  resend and replay fences read to decide whether the client may already hold
  output of the turn, and the same transaction refuses output from a replay
  generation that another generation superseded. So a transient database
  failure does not skip it: the mark runs again inside
  `Finalization.SettlementRetry`'s window while the relay holds the output
  back (findings#294), instead of letting the exception end the connection
  mid-stream.

  When the window closes, the mark fails closed with
  `{:error, :visible_output_unavailable}`. No output may be released without
  the durable visibility and replay-generation fence. The relay handles this
  through its ordinary stream-failure finalization instead of letting a
  database exception terminate the connection process.
  """

  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry

  @type result :: :ok | {:error, :stale_generation | :visible_output_unavailable}

  @spec mark(map(), map() | nil) :: result()
  def mark(request, attempt) do
    :visible_output
    |> SettlementRetry.run(request, attempt, fn -> SessionContinuity.mark_codex_turn_visible(request, attempt) end, subject: "visible output mark", fallback: "withheld_output", exhaustion: :return)
    |> case do
      {:error, :settlement_retry_exhausted} -> {:error, :visible_output_unavailable}
      result -> result
    end
  end
end
