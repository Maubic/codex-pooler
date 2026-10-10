defmodule CodexPooler.Gateway.Runtime.Finalization.ProviderRefusalMessage do
  @moduledoc """
  Reads the facts a provider validation refusal states in its message text.

  The Codex backend answers a request it refuses at validation over HTTP with
  a coded error object (`code`, `param`, `message`) or, for its older checks,
  with a `{"detail": ...}` body. On its websocket the same refusal arrives as a
  wrapped error frame whose error object has the text and no code and no param
  (direct probe 2026-10-07, findings#336, `gpt-6-luna`, Full and Lite, three
  samples per request; the text is the HTTP message):

    * `Unsupported parameter: <path>` and `Unsupported tool type: <type>`,
      the HTTP `detail` texts (findings#333);
    * `Invalid response.create payload: <message>`, after which the provider
      keeps the connection open;
    * `[<Schema>] [<path>] [<kind>] <message>`, after which the provider sends
      a Close frame. The provider words the same fault in either of the last two
      ways, chosen per connection.

  Reading the text gives the relay and the attempt record what the HTTP answer
  of the same fault carries: the code, and the field path where the text names
  it. Only the templates measured against an HTTP pair are read, and what is
  read is bounded: the code comes from a fixed vocabulary, the param passes the
  same field-path grammar as a provider param (`UpstreamErrorParam`), and the
  one value kept (an unsupported tool type) goes through the diagnostic
  taxonomy: a bounded ASCII identifier stays readable, anything else becomes
  its fingerprint. Provider message text itself is never kept or relayed.

  An `Invalid value: '<value>'. Supported values are: ...` message names no
  field, so its reading has the code and a `nil` param: the HTTP answer's param
  is not in the frame, and a Pooler-side guess would be wrong whenever two
  fields held the same value. The bracket wording of the same fault names it.
  """

  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol.UpstreamErrorParam
  alias CodexPooler.Gateway.Transports.Websocket.DiagnosticTaxonomy

  @payload_prefix "Invalid response.create payload: "
  @unsupported_parameter_prefix "Unsupported parameter: "
  @unsupported_tool_type_prefix "Unsupported tool type: "
  @unsupported_tool_type_class "unsupported_tool_type"

  # `[<Schema>] [<path>] [<kind>] <message>`: the path nests brackets (`include[0]`), so it is read lazily up to the
  # first `] [<kind>] `. The kind is the provider's own validation vocabulary; each one below was seen with the HTTP
  # code it maps to on the same fault (`invalid_enum_value` is the HTTP `invalid_value`).
  @bracket ~r/\A\[[A-Za-z_][A-Za-z0-9_]{0,63}\] \[(.{1,200}?)\] \[([a-z_]{1,64})\] /s
  @bracket_codes %{
    "invalid_enum_value" => "invalid_value",
    "invalid_type" => "invalid_type",
    "unknown_parameter" => "unknown_parameter",
    "missing_required_parameter" => "missing_required_parameter",
    "string_above_max_length" => "string_above_max_length"
  }

  # The HTTP messages of the same codes, as the payload wording repeats them. `Invalid value: '` names no field.
  @invalid_value_prefix "Invalid value: '"
  @inner_templates [
    {~r/\AInvalid type for '([^']{1,200})': /, "invalid_type"},
    {~r/\AMissing required parameter: '([^']{1,200})'\.?\z/, "missing_required_parameter"},
    {~r/\AUnknown parameter: '([^']{1,200})'\.?\z/, "unknown_parameter"},
    {~r/\AInvalid '([^']{1,200})': string too long\. /, "string_above_max_length"}
  ]

  @typedoc """
  What a message states. `class` is the fixed label recorded on the attempt, the code when the text reads as one;
  `code` is the HTTP code of the same fault, or `nil` when the text has none (an unsupported tool type); `param` is
  the bounded field path the text names, or `nil`; `value` is the bounded identifier a fixed template names.
  """
  @type reading :: %{
          required(:class) => String.t(),
          required(:code) => String.t() | nil,
          required(:param) => String.t() | nil,
          required(:value) => String.t() | nil
        }

  @doc """
  Reads the message of an HTTP `{"detail": ...}` body or of a websocket refusal frame: the detail texts, the payload
  wording and the bracket wording.
  """
  @spec read(term()) :: {:ok, reading()} | :error
  def read(message) when is_binary(message) do
    case message do
      @payload_prefix <> inner -> read_payload(inner)
      "[" <> _bracket -> read_bracket(message)
      _detail -> read_detail(message)
    end
  end

  def read(_message), do: :error

  @doc """
  Reads only the detail texts, the shapes an HTTP `{"detail": ...}` body has. The wrapped websocket wordings are not
  detail bodies, so an HTTP body in that wording stays unread.
  """
  @spec read_detail(term()) :: {:ok, reading()} | :error
  def read_detail(@unsupported_parameter_prefix <> path) do
    case UpstreamErrorParam.sanitize(path) do
      nil -> :error
      param -> {:ok, reading("unsupported_parameter", param)}
    end
  end

  def read_detail(@unsupported_tool_type_prefix <> type) when type != "" do
    {:ok, %{class: @unsupported_tool_type_class, code: nil, param: nil, value: DiagnosticTaxonomy.identifier(type)}}
  end

  def read_detail(_message), do: :error

  defp read_payload(@invalid_value_prefix <> _rest), do: {:ok, reading("invalid_value", nil)}

  defp read_payload(inner) do
    Enum.find_value(@inner_templates, :error, fn {template, code} ->
      case Regex.run(template, inner) do
        [_match, path] -> {:ok, reading(code, UpstreamErrorParam.sanitize(path))}
        nil -> nil
      end
    end)
  end

  defp read_bracket(message) do
    with [_match, path, kind] <- Regex.run(@bracket, message),
         {:ok, code} <- Map.fetch(@bracket_codes, kind) do
      {:ok, reading(code, UpstreamErrorParam.sanitize(path))}
    else
      _unread -> :error
    end
  end

  defp reading(code, param), do: %{class: code, code: code, param: param, value: nil}
end
