defmodule CodexPooler.Gateway.Runtime.Finalization.ProviderRefusalMessageTest do
  # provenance: observed findings#336 direct websocket and HTTP probes (2026-10-07, gpt-6-luna, Full and Lite, three
  # samples per request): every message below is the text of a captured wrapped error frame, next to the HTTP error
  # of the same fault (the supported-value lists of the include, item and format rows are shortened); the request
  # values in them are synthetic. The near-miss controls are synthetic.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Runtime.Finalization.ProviderRefusalMessage
  alias CodexPooler.Gateway.Runtime.Finalization.ValidationRejection

  @effort_message "Invalid value: 'zz_probe_effort'. Supported values are: 'none', 'minimal', 'low', 'medium', 'high', 'xhigh', and 'max'."

  # {websocket message, HTTP code, HTTP param, the param the text names}
  @captured [
    {"Invalid response.create payload: " <> @effort_message, "invalid_value", "reasoning.effort", nil},
    {"[ReasoningEffortParam] [reasoning.effort] [invalid_enum_value] " <> @effort_message, "invalid_value", "reasoning.effort", "reasoning.effort"},
    {"[ReasoningSummaryParam] [reasoning.summary] [invalid_enum_value] Invalid value: 'zz_probe_summary'. Supported values are: 'concise', 'detailed', and 'auto'.", "invalid_value", "reasoning.summary", "reasoning.summary"},
    {"[EnumParam] [include[0]] [invalid_enum_value] Invalid value: 'zz_probe_include'. Supported values are: 'file_search_call.results', and 'message.output_text.logprobs'.", "invalid_value", "include[0]", "include[0]"},
    {"[ItemParam] [input[0]] [invalid_enum_value] Invalid value: 'zz_probe_item'. Supported values are: 'additional_tools', 'message', and 'web_search_call'.", "invalid_value", "input[0]", "input[0]"},
    {"[FormatParam] [text.format.type] [invalid_enum_value] Invalid value: 'zz_probe_format'. Supported values are: 'json_object', 'text', and 'json_schema'.", "invalid_value", "text.format.type", "text.format.type"},
    {"Invalid response.create payload: Invalid type for 'parallel_tool_calls': expected a boolean, but got a string instead.", "invalid_type", "parallel_tool_calls", "parallel_tool_calls"},
    {"[StringParam] [instructions] [invalid_type] Invalid type for 'instructions': expected a string, but got an integer instead.", "invalid_type", "instructions", "instructions"},
    {"[_PromptCacheBreakpointObjectParam] [input[0].content[0].prompt_cache_breakpoint] [invalid_type] Invalid type for 'input[0].content[0].prompt_cache_breakpoint': expected an object, but got a boolean instead.", "invalid_type", "input[0].content[0].prompt_cache_breakpoint", "input[0].content[0].prompt_cache_breakpoint"},
    {"[ObjectParam] [tools[0].zz_probe_unknown_key] [unknown_parameter] Unknown parameter: 'tools[0].zz_probe_unknown_key'.", "unknown_parameter", "tools[0].zz_probe_unknown_key", "tools[0].zz_probe_unknown_key"},
    {"Invalid response.create payload: Unknown parameter: 'input[0].tools[0].zz_probe_unknown_key'.", "unknown_parameter", "input[0].tools[0].zz_probe_unknown_key", "input[0].tools[0].zz_probe_unknown_key"},
    {"[ObjectParam] [tools[0].name] [missing_required_parameter] Missing required parameter: 'tools[0].name'.", "missing_required_parameter", "tools[0].name", "tools[0].name"},
    {"Invalid response.create payload: Missing required parameter: 'tools[0].name'.", "missing_required_parameter", "tools[0].name", "tools[0].name"},
    {"[StringParam] [tools[0].name] [string_above_max_length] Invalid 'tools[0].name': string too long. Expected a string with maximum length 128, but got a string with length 200 instead.", "string_above_max_length", "tools[0].name", "tools[0].name"},
    {"Invalid response.create payload: Invalid 'input[0].tools[0].name': string too long. Expected a string with maximum length 128, but got a string with length 200 instead.", "string_above_max_length", "input[0].tools[0].name", "input[0].tools[0].name"},
    {"Unsupported parameter: metadata", "unsupported_parameter", "metadata", "metadata"}
  ]

  for {message, code, _http_param, param} <- @captured do
    test "reads #{String.slice(message, 0, 70)}" do
      assert ProviderRefusalMessage.read(unquote(message)) == {:ok, %{class: unquote(code), code: unquote(code), param: unquote(param), value: nil}}
    end
  end

  test "every code it reads is a code the validation relay accepts" do
    codes = for {message, _code, _http_param, _param} <- @captured, {:ok, %{code: code}} = ProviderRefusalMessage.read(message), uniq: true, do: code

    assert Enum.sort(codes) == Enum.sort(~w(invalid_value invalid_type unknown_parameter missing_required_parameter string_above_max_length unsupported_parameter))
    assert Enum.all?(codes, &(&1 in ValidationRejection.relayable_codes()))
  end

  test "an `Invalid value` message names no field, so no param is read for it" do
    assert {:ok, %{code: "invalid_value", param: nil}} = ProviderRefusalMessage.read("Invalid response.create payload: " <> @effort_message)
  end

  describe "the detail texts" do
    test "an unsupported tool type is a class with its bounded type, never a code" do
      assert ProviderRefusalMessage.read("Unsupported tool type: web_search_preview") == {:ok, %{class: "unsupported_tool_type", code: nil, param: nil, value: "web_search_preview"}}
      assert ProviderRefusalMessage.read_detail("Unsupported tool type: zz_probe_tool") == {:ok, %{class: "unsupported_tool_type", code: nil, param: nil, value: "zz_probe_tool"}}
    end

    test "a tool type outside the bounded identifier alphabet is kept as its fingerprint" do
      for type <- ["zz probe tool", "zz_probe_tool\nprivate-sentinel", String.duplicate("t", 81), "tool/with/slash", "outil-é"] do
        assert {:ok, %{class: "unsupported_tool_type", value: "sha256_" <> fingerprint}} = ProviderRefusalMessage.read("Unsupported tool type: " <> type), type
        assert fingerprint =~ ~r/\A[0-9a-f]{12}\z/
      end

      refute inspect(ProviderRefusalMessage.read("Unsupported tool type: zz probe tool private-sentinel")) =~ "private-sentinel"
    end

    test "an empty tool type, or an unbounded unsupported parameter, is not read" do
      assert ProviderRefusalMessage.read("Unsupported tool type: ") == :error
      assert ProviderRefusalMessage.read("Unsupported parameter: metadata synthetic prompt sentinel") == :error
      assert ProviderRefusalMessage.read("Unsupported parameter: ") == :error
    end

    test "only the detail texts are read as an HTTP detail body" do
      assert {:ok, %{class: "unsupported_parameter"}} = ProviderRefusalMessage.read_detail("Unsupported parameter: metadata")

      for message <- [
            "Invalid response.create payload: " <> @effort_message,
            "[ReasoningEffortParam] [reasoning.effort] [invalid_enum_value] " <> @effort_message
          ] do
        assert ProviderRefusalMessage.read_detail(message) == :error
      end
    end
  end

  describe "near misses stay unread" do
    test "an unrecognised message in either wrapped wording" do
      for message <- [
            "Invalid response.create payload: Something else entirely.",
            "Invalid response.create payload: ",
            "Invalid response.create payload:Invalid value: 'x'. Supported values are: 'a'.",
            "invalid response.create payload: Invalid value: 'x'. Supported values are: 'a'.",
            "[ObjectParam] [tools[0].x] [zz_new_kind] Something.",
            "[ObjectParam] [tools[0].x] [Invalid_Type] Something.",
            "[ObjectParam] [tools[0].x] Something without a kind.",
            "[ObjectParam] [] [invalid_type] Empty path.",
            "[Bad Schema] [tools[0].x] [invalid_type] Spaces in the schema.",
            "[ObjectParam][tools[0].x][invalid_type] No separators."
          ] do
        assert ProviderRefusalMessage.read(message) == :error, message
      end
    end

    test "the wordings of provider texts with another shape, and a bare payload message" do
      for message <- [
            # The registry refusal keeps its provider param and has no code over HTTP either.
            "Invalid Value: 'tools'. invalid tool registry: unsupported loaded tool entry type \"web_search_preview\"",
            "Tool choice 'zz_probe_missing' must be specified with 'tools' parameter.",
            # The payload wording is read only with its prefix.
            @effort_message,
            "Missing required parameter: 'tools[0].name'.",
            "Unsupported value: 'minimal' is not supported with the 'gpt-6-luna' model. Supported values are: 'none', 'low'.",
            "Unsupported service_tier: zz_probe_tier",
            "Input must be a list"
          ] do
        assert ProviderRefusalMessage.read(message) == :error, message
      end
    end

    test "a template followed by anything beyond what it measured is unread" do
      assert ProviderRefusalMessage.read("Invalid response.create payload: Missing required parameter: 'tools[0].name'. And more text.") == :error
      assert ProviderRefusalMessage.read("Invalid response.create payload: Unknown parameter: 'tools[0].x' trailing") == :error
    end

    test "anything that is not a binary" do
      for message <- [nil, 400, %{"message" => "x"}, ["Unsupported parameter: metadata"]] do
        assert ProviderRefusalMessage.read(message) == :error
        assert ProviderRefusalMessage.read_detail(message) == :error
      end
    end
  end

  describe "the field path stays bounded" do
    test "a path outside the field-path grammar is dropped and the code is kept" do
      unbounded = String.duplicate("a", 161)

      for path <- ["tools[0].zz probe key", "tools[0].key\nvalue", unbounded, "tools[0].key[x]"] do
        assert ProviderRefusalMessage.read("[ObjectParam] [#{path}] [unknown_parameter] Unknown parameter: '#{path}'.") ==
                 {:ok, %{class: "unknown_parameter", code: "unknown_parameter", param: nil, value: nil}},
               path

        assert ProviderRefusalMessage.read("Invalid response.create payload: Missing required parameter: '#{path}'.") ==
                 {:ok, %{class: "missing_required_parameter", code: "missing_required_parameter", param: nil, value: nil}},
               path
      end
    end

    test "a path in the grammar at its bound is kept" do
      path = "a" <> String.duplicate("b", 159)
      assert {:ok, %{param: ^path}} = ProviderRefusalMessage.read("[ObjectParam] [#{path}] [invalid_type] Invalid type for '#{path}': expected an object.")
    end
  end

  test "reading the text never returns the text" do
    sentinel = "private-sentinel-prompt-text"

    for message <- [
          "Invalid response.create payload: Invalid value: '#{sentinel}'. Supported values are: 'a'.",
          "[StringParam] [instructions] [invalid_type] Invalid type for 'instructions': #{sentinel}",
          "Unsupported tool type: #{sentinel} with spaces"
        ] do
      {:ok, reading} = ProviderRefusalMessage.read(message)
      refute inspect(reading) =~ sentinel
    end
  end
end
