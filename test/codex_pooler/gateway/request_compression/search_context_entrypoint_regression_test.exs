defmodule CodexPooler.Gateway.RequestCompression.SearchContextEntrypointRegressionTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.RequestCompression

  test "search compression retains every C3 context row through the request entrypoint" do
    output =
      Enum.map_join(1..8, "\n--\n", fn file ->
        Enum.map_join(1..7, "\n", fn line ->
          separator = if line == 4, do: ":", else: "-"
          kind = if line == 4, do: "needle", else: "context"
          "lib/synthetic/search/context/example_#{file}.ex#{separator}#{line}#{separator} #{kind} row #{file}-#{line}"
        end)
      end)

    payload = %{
      "model" => "gpt-4o",
      "input" => [
        %{"type" => "function_call", "name" => "exec_command", "call_id" => "synthetic_search", "arguments" => CodexPooler.JSON.encode!(%{"cmd" => "rg -n -C3 needle lib"})},
        %{"type" => "function_call_output", "call_id" => "synthetic_search", "output" => output}
      ]
    }

    endpoint = "/backend-api/codex/responses"

    options =
      RequestOptions.build(%{transport: "http_json", upstream_endpoint: endpoint}, endpoint, payload)
      |> RequestOptions.put_transport(route_class: "proxy_http", upstream_endpoint: endpoint)

    context = %{endpoint: endpoint, model: %{exposed_model_id: "gpt-4o", upstream_model_id: "gpt-4o"}, route_state: %{routing_settings: %{request_compression_enabled: true}}, route_class: "proxy_http"}
    body = CodexPooler.JSON.encode!(payload)

    {rewritten, result} = RequestCompression.maybe_compress(body, context, options)

    assert rewritten != body
    assert result.runtime.payload_compression["compressed_count"] == 1
    assert {:ok, %{"input" => [_call, %{"output" => compressed}]}} = CodexPooler.JSON.decode(rewritten)
    assert compressed =~ "8/8 matches, 8/8 files"

    for file <- 1..8, line <- [1, 2, 3, 5, 6, 7] do
      assert compressed =~ "  #{line}- context row #{file}-#{line}"
    end

    refute compressed =~ "omitted"
  end
end
