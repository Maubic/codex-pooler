defmodule CodexPooler.Gateway.Transports.Websocket.ResponseSteer do
  @moduledoc false

  require Logger

  @type t :: %{previous_response_id: String.t(), input: list(), frame: map()}
  @type topology :: :direct | :owner
  @type outcome :: :written | :malformed | :session_unavailable | :session_idle | :not_relay | :owner_unavailable | :owner_not_downstream | :owner_turn_not_relay | :owner_protocol_unsupported | :accepted | :failed | :native_lane_error

  @response_id ~r/\Aresp_[A-Za-z0-9_-]{1,1020}\z/
  @outcomes [:written, :malformed, :session_unavailable, :session_idle, :not_relay, :owner_unavailable, :owner_not_downstream, :owner_turn_not_relay, :owner_protocol_unsupported, :accepted, :failed, :native_lane_error]

  @spec parse(term()) :: {:ok, t()} | :malformed | :not_steer
  def parse(%{"type" => "response.steer"} = frame) do
    with id when is_binary(id) <- Map.get(frame, "previous_response_id"),
         true <- Regex.match?(@response_id, id),
         input when is_list(input) <- Map.get(frame, "input") do
      {:ok, %{previous_response_id: id, input: input, frame: frame}}
    else
      _invalid -> :malformed
    end
  end

  def parse(_frame), do: :not_steer

  # Extra fields remain on the wire. The provider's invalid_input answer must
  # not become an acceptance because a proxy silently removed those fields.
  @spec frame(t()) :: binary()
  def frame(%{frame: frame}), do: CodexPooler.JSON.encode!(frame)

  @spec outcomes() :: [outcome()]
  def outcomes, do: @outcomes

  @spec native_lane_error?(term()) :: boolean()
  def native_lane_error?(%{"type" => "error", "error" => %{"code" => "unsupported_native_inflight_message"}}), do: true
  def native_lane_error?(_frame), do: false

  @spec fingerprint(term()) :: String.t()
  def fingerprint(id) when is_binary(id), do: id |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower) |> binary_part(0, 12)
  def fingerprint(_id), do: "none"

  @spec log(outcome(), topology(), String.t(), term()) :: :ok
  def log(outcome, topology, frame_type \\ "response.steer", steer_id \\ nil)
      when outcome in @outcomes and topology in [:direct, :owner] and frame_type in ["response.steer", "response.steer.accepted", "response.steer.failed", "error"] do
    Logger.info("native websocket response steer frame_type=#{frame_type} outcome=#{outcome} topology=#{topology} steer_id_fingerprint=#{fingerprint(steer_id)}")
  end
end
