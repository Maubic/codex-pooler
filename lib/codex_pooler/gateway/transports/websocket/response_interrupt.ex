defmodule CodexPooler.Gateway.Transports.Websocket.ResponseInterrupt do
  @moduledoc false

  # The released Codex client (0.159.0) stops a running turn served under Lite
  # by sending `{"type": "response.interrupt", "response_id": ..., "mode":
  # "discard_partial_items"}` on the websocket that carries the turn. The
  # provider answers on that connection only: `response.interrupt.accepted`,
  # `response.output_item.interrupted` for an item it had open, then the
  # terminal `response.incomplete` with `incomplete_details.reason` =
  # `interrupted` and the usage generated so far. The client reads that
  # terminal as a turn ended without end-of-turn and sends its follow-up
  # anchored on the interrupted response (findings#270 row 270-272).
  #
  # The frame is not a request: it takes no claim, no reservation and no row.
  # It goes to the upstream session that carries the turn, which writes it only
  # while the response it names is in flight there (`UpstreamWebsocketSession.
  # interrupt/2`). Every other interrupt is dropped without an answer, because
  # an error frame would end the client's turn, and the provider itself answers
  # an interrupt it cannot apply with a non-terminal `response.interrupt.failed`
  # the client ignores. Each interrupt leaves one line naming where it ended,
  # from a fixed vocabulary, and never the response id.

  require Logger

  @type t :: %{response_id: String.t(), mode: String.t()}

  @type outcome ::
          :written
          | :malformed
          | :no_running_turn
          | :session_unavailable
          | :session_idle
          | :not_relay
          | :response_mismatch
          | :terminal_seen
          | :owner_unavailable
          | :owner_not_downstream
          | :owner_turn_not_relay
          | :owner_protocol_unsupported

  @outcomes [
    :written,
    :malformed,
    :no_running_turn,
    :session_unavailable,
    :session_idle,
    :not_relay,
    :response_mismatch,
    :terminal_seen,
    :owner_unavailable,
    :owner_not_downstream,
    :owner_turn_not_relay,
    :owner_protocol_unsupported
  ]

  @topologies [:direct, :owner]

  # A provider response id, bounded like every id the gateway compares.
  @response_id ~r/\Aresp_[A-Za-z0-9_-]{1,1020}\z/
  @mode ~r/\A[a-z][a-z0-9_]{0,63}\z/

  @doc """
  The interrupt a decoded native frame carries: `{:ok, t()}`, `:malformed` for
  a `response.interrupt` frame whose `response_id` is not a provider response id
  or whose `mode` is not a bounded identifier, `:not_interrupt` otherwise.
  """
  @spec parse(term()) :: {:ok, t()} | :malformed | :not_interrupt
  def parse(%{"type" => "response.interrupt"} = frame) do
    with response_id when is_binary(response_id) <- Map.get(frame, "response_id"),
         true <- Regex.match?(@response_id, response_id),
         mode when is_binary(mode) <- Map.get(frame, "mode"),
         true <- Regex.match?(@mode, mode) do
      {:ok, %{response_id: response_id, mode: mode}}
    else
      _malformed -> :malformed
    end
  end

  def parse(_frame), do: :not_interrupt

  @doc "The frame the upstream session writes: the interrupt's own fields and nothing else of what the client sent."
  @spec frame(t()) :: binary()
  def frame(%{response_id: response_id, mode: mode}),
    do: CodexPooler.JSON.encode!(%{"type" => "response.interrupt", "response_id" => response_id, "mode" => mode})

  @doc "Every outcome `log/2` names."
  @spec outcomes() :: [outcome()]
  def outcomes, do: @outcomes

  @doc """
  The one line an interrupt leaves: `outcome` is where it ended (`:written`
  when the upstream session wrote it), `topology` whose upstream session
  carries the turn: the socket's own (`:direct`) or its owner's (`:owner`).
  """
  @spec log(outcome(), :direct | :owner) :: :ok
  def log(outcome, topology) when outcome in @outcomes and topology in @topologies do
    Logger.info("native websocket response interrupt outcome=#{outcome} topology=#{topology}")
  end
end
