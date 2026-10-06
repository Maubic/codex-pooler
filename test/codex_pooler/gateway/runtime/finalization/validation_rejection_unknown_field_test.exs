defmodule CodexPooler.Gateway.Runtime.Finalization.ValidationRejectionUnknownFieldTest do
  # The provider refuses a request key it does not know with `400 invalid_request_error`, code `unknown_parameter`
  # and the offending path in `param` (`tools[0].<key>`, `input[0].tools[0].<key>` inside a Lite manifest, a nested
  # `tools[0].user_location.<key>`), and a field the model does not support with code `invalid_parameter`
  # (`prompt_cache_breakpoint`; direct probe, 2026-10-06). Both are the client's parameter errors like the codes
  # relayed before them, so a `/v1` client reads the Pooler-authored `upstream rejected parameter <param> (<code>)`
  # instead of the generic `upstream request failed`. The provider message never travels.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Finalization.ValidationRejection

  @provider_sentinel "private-provider-unknown-field-sentinel"
  @new_codes ~w(unknown_parameter invalid_parameter)
  @observed [
    {"unknown_parameter", "tools[0].zz_probe_unknown_key"},
    {"unknown_parameter", "input[0].tools[0].index_gated_web_access"},
    {"unknown_parameter", "tools[0].user_location.zz_probe_unknown_key"},
    {"invalid_parameter", "prompt_cache_breakpoint"}
  ]

  describe "relayable codes" do
    test "include the unknown-field pair and keep the value-list pair unchanged" do
      assert ValidationRejection.relayable_codes() == ~w(
               unsupported_value
               invalid_value
               unsupported_parameter
               missing_required_parameter
               invalid_type
               string_above_max_length
               unknown_parameter
               invalid_parameter
             )

      assert ValidationRejection.supported_values_codes() == ~w(unsupported_value invalid_value)
    end
  end

  describe "fetch/2 and fetch_ordinary_route/1" do
    for {code, param} <- @observed, endpoint <- ["/backend-api/codex/responses", "/v1/responses"] do
      test "admit #{code} on #{param} for #{endpoint} with no value list and no persisted state" do
        options = request_options(unquote(endpoint))
        response = rejection(400, unquote(code), unquote(param))

        expected = %{code: unquote(code), param: unquote(param), supported_values: nil, supported_values_state: nil}

        assert ValidationRejection.fetch(response, options) == expected
        assert ValidationRejection.fetch_ordinary_route(response) == expected
        assert ValidationRejection.attempt_metadata(expected) == %{}
      end
    end

    for {code, param} <- @observed do
      test "render #{code} on #{param} as the Pooler-authored error and never the provider message" do
        rejection = ValidationRejection.fetch_ordinary_route(rejection(400, unquote(code), unquote(param)))

        assert ValidationRejection.error(rejection) == %{
                 "type" => "invalid_request_error",
                 "code" => unquote(code),
                 "param" => unquote(param),
                 "message" => "upstream rejected parameter #{unquote(param)} (#{unquote(code)})"
               }

        refute inspect(ValidationRejection.error(rejection)) =~ @provider_sentinel
      end
    end

    test "name the request without a param when the provider path is not a bounded identifier path" do
      for param <- ["tools[0].has-hyphen", "tools[0]." <> String.duplicate("a", 200), "tools[01].key", "input[0].tools[12345].key"] do
        for code <- @new_codes do
          rejection = ValidationRejection.fetch_ordinary_route(rejection(400, code, param))

          assert ValidationRejection.error(rejection)["param"] == nil, param
          assert ValidationRejection.error(rejection)["message"] == "upstream rejected the request (#{code})"
        end
      end
    end

    test "do not admit them for another type, another status, another spelling or a compact route" do
      for code <- @new_codes do
        assert ValidationRejection.fetch(rejection(400, code, "tools[0].key", "server_error"), request_options("/backend-api/codex/responses")) == nil
        assert ValidationRejection.fetch(rejection(400, code, "tools[0].key", nil), request_options("/backend-api/codex/responses")) == nil
        assert ValidationRejection.fetch(rejection(422, code, "tools[0].key"), request_options("/backend-api/codex/responses")) == nil
        assert ValidationRejection.fetch(rejection(400, String.upcase(code), "tools[0].key"), request_options("/backend-api/codex/responses")) == nil
        assert ValidationRejection.fetch(rejection(400, code, "tools[0].key"), request_options("/backend-api/codex/responses/compact")) == nil
        assert ValidationRejection.fetch_ordinary_route(rejection(429, code, "tools[0].key")) == nil
      end
    end

    test "leave the codes outside the allowlist as they were" do
      for code <- ~w(invalid_encrypted_content invalid_prompt context_length_exceeded unknown_parameters invalid_parameter_value) do
        assert ValidationRejection.fetch_ordinary_route(rejection(400, code, "tools[0].key")) == nil
      end
    end
  end

  describe "for_client/2" do
    test "names a refused Lite manifest field without a position the client never sent" do
      rejection = ValidationRejection.fetch_ordinary_route(rejection(400, "unknown_parameter", "input[0].tools[0].zz_probe_unknown_key"))

      assert ValidationRejection.for_client(rejection, {:shift, 0, 1}).param == "input[].tools[0].zz_probe_unknown_key"
      assert ValidationRejection.for_client(rejection, :unknown).param == "input[].tools[0].zz_probe_unknown_key"
      assert ValidationRejection.for_client(rejection, :identity).param == "input[0].tools[0].zz_probe_unknown_key"
    end

    test "renders a client-sent manifest item in the client's own position" do
      rejection = ValidationRejection.fetch_ordinary_route(rejection(400, "unknown_parameter", "input[3].tools[0].zz_probe_unknown_key"))

      assert ValidationRejection.for_client(rejection, {:shift, 0, 2}).param == "input[1].tools[0].zz_probe_unknown_key"
    end
  end

  defp request_options(endpoint), do: RequestOptions.build(%{}, endpoint, %{"model" => "example-model"})

  defp rejection(status, code, param, type \\ "invalid_request_error") do
    error = %{"code" => code, "message" => "Unknown parameter: '#{param}'. " <> @provider_sentinel, "param" => param}
    error = if type, do: Map.put(error, "type", type), else: error

    %Req.Response{status: status, body: CodexPooler.JSON.encode!(%{"error" => error})}
  end
end
