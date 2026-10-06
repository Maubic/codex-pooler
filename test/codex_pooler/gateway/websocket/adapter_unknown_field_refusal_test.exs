defmodule CodexPooler.Gateway.Websocket.AdapterUnknownFieldRefusalTest do
  # A native websocket turn whose HTTP predecessor was refused by a relayable parameter-validation code is answered
  # with the recorded refusal instead of being served again, because the provider repeats that refusal for the same
  # body (findings#254 row 254-141). `unknown_parameter` and `invalid_parameter` are such codes: the provider names
  # the request key it does not know, or the field the model does not support, and answers the same on every resend.
  # Every other recorded 4xx of an HTTP predecessor keeps the claim's step-over.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Websocket.Adapter

  for {code, param, client_param} <- [
        {"unknown_parameter", "tools[0].zz_probe_unknown_key", "tools[0].zz_probe_unknown_key"},
        {"unknown_parameter", "input[0].tools[0].zz_probe_unknown_key", "input[].tools[0].zz_probe_unknown_key"},
        {"invalid_parameter", "prompt_cache_breakpoint", "prompt_cache_breakpoint"}
      ] do
    test "an HTTP predecessor refused with #{code} on #{param} is answered with the recorded refusal" do
      metadata = %{
        "rejection_predecessor_transport" => "http",
        "rejection_upstream_status" => 400,
        "rejection_error_type" => "invalid_request_error",
        "rejection_error_code" => unquote(code),
        "rejection_error_param" => unquote(param)
      }

      assert Adapter.recorded_final_refusal_error(metadata) ==
               {:ok,
                %{
                  "type" => "invalid_request_error",
                  "code" => unquote(code),
                  "param" => unquote(client_param),
                  "message" => "upstream rejected parameter #{unquote(client_param)} (#{unquote(code)})"
                }}
    end

    test "the same #{code} refusal on #{param} recorded on the websocket itself rebuilds the wrapped 400 error" do
      metadata = %{
        "rejection_upstream_status" => 400,
        "rejection_error_type" => "invalid_request_error",
        "rejection_error_code" => unquote(code),
        "rejection_error_param" => unquote(param)
      }

      assert {:ok, %{"code" => unquote(code), "param" => unquote(client_param)}} = Adapter.recorded_final_refusal_error(metadata)
    end
  end

  test "an HTTP predecessor refused with a code outside the relayed set is served again" do
    for code <- ~w(invalid_encrypted_content unknown_parameters) do
      metadata = %{
        "rejection_predecessor_transport" => "http",
        "rejection_upstream_status" => 400,
        "rejection_error_type" => "invalid_request_error",
        "rejection_error_code" => code,
        "rejection_error_param" => "tools[0].key"
      }

      assert Adapter.recorded_final_refusal_error(metadata) == :none, code
    end
  end

  test "a predecessor refused with a relayed code under another type or status is not replayed" do
    for metadata <- [
          %{"rejection_predecessor_transport" => "http", "rejection_upstream_status" => 400, "rejection_error_type" => "server_error", "rejection_error_code" => "unknown_parameter"},
          %{"rejection_predecessor_transport" => "http", "rejection_upstream_status" => 422, "rejection_error_type" => "invalid_request_error", "rejection_error_code" => "unknown_parameter"}
        ] do
      assert Adapter.recorded_final_refusal_error(metadata) == :none, inspect(metadata)
    end
  end
end
