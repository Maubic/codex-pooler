defmodule CodexPoolerWeb.V1.ResponsesUnknownFieldRejectionTest do
  # The provider refuses a request key it does not know with `400 invalid_request_error`, code `unknown_parameter` and
  # the offending path in `param`, and a field the model does not support with code `invalid_parameter` (direct probe,
  # 2026-10-06). An Auto or Lite Pool used to answer both with the generic redacted error (`upstream request failed`,
  # code `upstream_status`, no param), so a client could not tell which of its keys the provider refused, for example
  # a key inside a client-sent `additional_tools` manifest item that the adapter does not key-validate (an explicit
  # Full override already relays the sanitized code and path of every non-429 4xx). Both codes now join the
  # parameter-validation codes `/v1` relays on every Pool: the client reads the Pooler-authored `upstream rejected
  # parameter <param> (<code>)` in its own input positions, streaming or not. The provider message never travels,
  # and the attempt keeps the provider's own sanitized code, type and path.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport,
    only: [model_serving_scope: 0, set_model_serving_mode!: 3]

  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  @moduletag capture_log: true
  @sentinel "private-provider-unknown-field-sentinel"
  @hosted_tool %{"type" => "web_search", "external_web_access" => false}
  @manifest_key "zz_probe_unknown_key"

  # {label, code, provider path under Full, provider path under Lite, path the client reads under Full, under Lite}
  @refusals [
    {"an unknown hosted tool key", "unknown_parameter", "tools[0].#{@manifest_key}", "input[0].tools[0].#{@manifest_key}", "tools[0].#{@manifest_key}", "input[].tools[0].#{@manifest_key}"},
    {"a field the model does not support", "invalid_parameter", "prompt_cache_breakpoint", "prompt_cache_breakpoint", "prompt_cache_breakpoint", "prompt_cache_breakpoint"}
  ]

  # `auto` leaves the catalog's serving mode in charge (Full for the fixture model, source `catalog`); `lite` and `full`
  # are explicit overrides. Only the explicit Full override relayed these refusals before.
  for mode <- ["auto", "lite", "full"], stream? <- [true, false], {label, code, full_path, lite_path, full_client, lite_client} <- @refusals do
    test "#{mode} serving, stream #{stream?}: #{label} is relayed as #{code}", %{conn: conn} do
      mode = unquote(mode)
      provider_path = if mode == "lite", do: unquote(lite_path), else: unquote(full_path)
      client_path = if mode == "lite", do: unquote(lite_client), else: unquote(full_client)
      {upstream, setup} = refusing_setup(unquote(code), provider_path, mode)

      response = post_responses(conn, setup, %{"model" => setup.model.exposed_model_id, "input" => "synthetic input", "stream" => unquote(stream?), "tools" => [@hosted_tool]})

      assert json_response(response, 400) == %{
               "error" => %{
                 "type" => "invalid_request_error",
                 "code" => unquote(code),
                 "param" => client_path,
                 "message" => "upstream rejected parameter #{client_path} (#{unquote(code)})"
               }
             }

      refute response.resp_body =~ @sentinel
      assert :ok = FakeUpstream.verify!(upstream)
      assert_attempt_keeps_the_provider_facts!(setup, unquote(code), provider_path)
    end
  end

  # R-V1: the adapter does not key-validate a `web_search` carried by a client-sent `additional_tools` item (only `mcp`
  # and `tool_search` are refused there), so a bad key in it reaches the provider. The relayed path names the client's
  # own position of that item.
  for mode <- ["auto", "lite", "full"] do
    test "#{mode} serving: a bad key inside a client-sent additional_tools item names the client's position", %{conn: conn} do
      provider_path = "input[1].tools[0].#{@manifest_key}"
      {upstream, setup} = refusing_setup("unknown_parameter", provider_path, unquote(mode))

      manifest = %{"type" => "additional_tools", "role" => "developer", "tools" => [Map.put(@hosted_tool, @manifest_key, true)]}
      input = [%{"type" => "message", "role" => "user", "content" => [%{"type" => "input_text", "text" => "synthetic input"}]}, manifest]

      response = post_responses(conn, setup, %{"model" => setup.model.exposed_model_id, "input" => input, "stream" => true})

      assert json_response(response, 400)["error"] == %{
               "type" => "invalid_request_error",
               "code" => "unknown_parameter",
               "param" => provider_path,
               "message" => "upstream rejected parameter #{provider_path} (unknown_parameter)"
             }

      assert :ok = FakeUpstream.verify!(upstream)
      assert [captured] = FakeUpstream.requests(upstream)
      assert %{"type" => "additional_tools"} = Enum.at(captured.json["input"], 1)
      assert_attempt_keeps_the_provider_facts!(setup, "unknown_parameter", provider_path)
    end
  end

  test "an Auto Pool still answers a provider refusal outside the relayed codes with the generic public error", %{conn: conn} do
    {upstream, setup} = refusing_setup("invalid_encrypted_content", "input[0].encrypted_content", "auto")

    response = post_responses(conn, setup, %{"model" => setup.model.exposed_model_id, "input" => "synthetic input", "stream" => true})

    assert json_response(response, 400)["error"] == %{"type" => "invalid_request_error", "code" => "upstream_status", "message" => "upstream request failed"}
    refute response.resp_body =~ @sentinel
    assert :ok = FakeUpstream.verify!(upstream)
  end

  defp refusing_setup(code, provider_path, mode) do
    error = %{"type" => "invalid_request_error", "code" => code, "message" => "Unknown parameter: '#{provider_path}'. #{@sentinel}", "param" => provider_path}

    upstream =
      start_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(method: "POST", path: "/backend-api/codex/responses", respond: {:json_error, 400, %{"error" => error}})
        ])
      )

    setup = gateway_setup(upstream)
    if mode != "auto", do: set_model_serving_mode!(model_serving_scope(), setup, mode)
    {upstream, setup}
  end

  defp post_responses(conn, setup, payload), do: conn |> auth(setup) |> post("/v1/responses", payload)

  defp assert_attempt_keeps_the_provider_facts!(setup, code, provider_path) do
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "failed"
    assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
    assert request.last_error_code == "upstream_status"

    assert Map.take(attempt.response_metadata, ["rejection_error_code", "rejection_error_type", "rejection_error_param"]) == %{
             "rejection_error_code" => code,
             "rejection_error_type" => "invalid_request_error",
             "rejection_error_param" => provider_path
           }

    # A code that carries no value list leaves the supported-values facts out entirely.
    refute Map.has_key?(attempt.response_metadata, "rejection_supported_values")
    refute Map.has_key?(attempt.response_metadata, "rejection_supported_values_state")
    refute inspect({request, attempt}, limit: :infinity, printable_limit: :infinity) =~ @sentinel
  end
end
