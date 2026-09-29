defmodule CodexPooler.Gateway.Payloads.ContinuityPayload do
  @moduledoc false

  alias CodexPooler.Gateway.Payloads.NativeCodexTurnMetadata
  alias CodexPooler.Gateway.Payloads.RequestOptions

  @http_transports ["http_json", "http_sse", "http_compact_json"]
  @backend_codex_agent_path ~r/\A(?:\/morpheus|\/root(?:\/[a-z0-9_]+)*)\z/

  @spec put_previous_response_id(RequestOptions.t(), map()) :: RequestOptions.t()
  def put_previous_response_id(%RequestOptions{} = request_options, payload)
      when is_map(payload) do
    case blank_to_nil(request_options.continuity.previous_response_id) do
      nil ->
        RequestOptions.put_continuity(request_options,
          previous_response_id: previous_response_id(payload)
        )

      _response_id ->
        request_options
    end
  end

  @spec previous_response_id(map()) :: String.t() | nil
  def previous_response_id(payload) when is_map(payload) do
    payload
    |> Map.get("previous_response_id")
    |> Kernel.||(Map.get(payload, :previous_response_id))
    |> blank_to_nil()
  end

  @doc """
  The session header a native HTTP request falls back to when its own window
  has no live session: the previous window of its thread (findings#289).

  The released Codex client moves `x-codex-window-id` to the next window of its
  thread after every compaction it completes and keeps `session-id`,
  `thread-id` and the turn ids, so over HTTP the request that resumes after a
  compaction names a window no session knows yet. On the websocket the socket's
  session carries the thread across windows (findings#206, P115); over HTTP
  only the previous window's alias does. Only a native HTTP request keyed by
  its window header qualifies, and only while no client turn state or
  response anchor names a session instead; a `/v1` request, a websocket
  upgrade and anything else answer `nil`.
  """
  @spec previous_window_session_header(RequestOptions.t()) :: String.t() | nil
  def previous_window_session_header(%RequestOptions{
        transport: %{transport: transport},
        openai_compatibility: %{source_endpoint: nil},
        continuity: %{session_header_source: "x-codex-window-id", session_header: window} = continuity
      })
      when transport in @http_transports and is_binary(window) do
    with nil <- blank_to_nil(continuity.accepted_turn_state),
         nil <- blank_to_nil(continuity.previous_response_id),
         true <- continuity.authenticated_owner_attach != true,
         window when is_binary(window) <- blank_to_nil(window),
         {:ok, previous} <- NativeCodexTurnMetadata.previous_window(window) do
      previous
    else
      _other -> nil
    end
  end

  def previous_window_session_header(%RequestOptions{}), do: nil

  @spec current_encrypted_reasoning?(term()) :: boolean()
  def current_encrypted_reasoning?(%{
        "type" => "reasoning",
        "content" => nil,
        "encrypted_content" => encrypted_content
      })
      when is_binary(encrypted_content),
      do: String.trim(encrypted_content) != ""

  def current_encrypted_reasoning?(_item), do: false

  @spec v2_encrypted_handoff?(term()) :: boolean()
  def v2_encrypted_handoff?(%{
        "type" => "agent_message",
        "author" => author,
        "recipient" => recipient,
        "content" => [
          %{"type" => "input_text", "text" => text},
          %{"type" => "encrypted_content", "encrypted_content" => encrypted_content}
        ]
      })
      when is_binary(author) and is_binary(recipient) and is_binary(text) and
             is_binary(encrypted_content) do
    backend_codex_agent_path?(author) and backend_codex_agent_path?(recipient) and
      String.trim(encrypted_content) != "" and
      text in [
        "Message Type: NEW_TASK\nTask name: #{recipient}\nSender: #{author}\nPayload:\n",
        "Message Type: MESSAGE\nTask name: #{recipient}\nSender: #{author}\nPayload:\n"
      ]
  end

  def v2_encrypted_handoff?(_item), do: false

  defp backend_codex_agent_path?(path), do: Regex.match?(@backend_codex_agent_path, path)

  defp blank_to_nil(value) when is_binary(value) do
    value = String.trim(value)
    if value == "", do: nil, else: value
  end

  defp blank_to_nil(_value), do: nil
end
