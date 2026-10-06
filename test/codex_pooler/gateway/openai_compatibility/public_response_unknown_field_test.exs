defmodule CodexPooler.Gateway.OpenAICompatibility.PublicResponseUnknownFieldTest do
  # A `/v1/responses` websocket client meets a provider refusal as the OpenAI websocket-mode `error` event carrying the
  # error object the HTTP answer to the same refusal carries. For `unknown_parameter` and `invalid_parameter` (the
  # provider's refusal of a request key it does not know, and of a field the model does not support) that object is the
  # Pooler-authored `upstream rejected parameter <param> (<code>)`, not the generic redacted `upstream request failed`
  # an SDK could not act on. The provider message never travels, and the socket holds no per-turn input index map, so
  # an `input[N]` position loses its index.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.OpenAICompatibility.PublicResponse

  @provider_sentinel "private-provider-unknown-field-sentinel"

  for {code, param, public_param} <- [
        {"unknown_parameter", "tools[0].zz_probe_unknown_key", "tools[0].zz_probe_unknown_key"},
        {"unknown_parameter", "input[0].tools[0].index_gated_web_access", "input[].tools[0].index_gated_web_access"},
        {"invalid_parameter", "prompt_cache_breakpoint", "prompt_cache_breakpoint"}
      ] do
    test "the public websocket error event relays #{code} on #{param}" do
      error = %{"type" => "invalid_request_error", "code" => unquote(code), "param" => unquote(param), "message" => "Unknown parameter: '#{unquote(param)}'. #{@provider_sentinel}"}

      event = PublicResponse.provider_rejection_websocket_event(400, error)

      assert event == %{
               "type" => "error",
               "status" => 400,
               "error" => %{
                 "type" => "invalid_request_error",
                 "code" => unquote(code),
                 "param" => unquote(public_param),
                 "message" => "upstream rejected parameter #{unquote(public_param)} (#{unquote(code)})"
               }
             }

      refute inspect(event) =~ @provider_sentinel
    end
  end

  test "a refusal outside the relayed codes keeps the generic redacted public error" do
    error = %{"type" => "invalid_request_error", "code" => "invalid_encrypted_content", "param" => "input[2].encrypted_content", "message" => "synthetic #{@provider_sentinel}"}

    assert %{"type" => "error", "status" => 400, "error" => %{"code" => "upstream_status", "message" => "upstream request failed"} = public} =
             PublicResponse.provider_rejection_websocket_event(400, error)

    refute inspect(public) =~ @provider_sentinel
  end
end
