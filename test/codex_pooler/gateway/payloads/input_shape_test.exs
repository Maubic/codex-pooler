defmodule CodexPooler.Gateway.Payloads.InputShapeTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.OpenAICompatibility.Responses
  alias CodexPooler.Gateway.Payloads.InputShape
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Transports.Streaming.WebsocketCodec

  test "native websocket preparation rejects SVG tool images in Full and Lite before sealing" do
    for mode <- ~w(full lite), output_type <- ~w(function_call_output custom_tool_call_output) do
      payload = output_payload([svg_part()], output_type) |> Map.put("type", "response.create")
      result = WebsocketCodec.prepare_frame(CodexPooler.JSON.encode!(payload), websocket_options(payload, mode), fn _ -> :ok end)
      assert elem(result, 0) == :error
      assert elem(result, 1).code == "unsupported_input_image_format"
    end
  end

  test "native websocket preparation preserves raster tool images in Full and Lite" do
    for mode <- ~w(full lite), mime <- ~w(image/png image/jpeg), output_type <- ~w(function_call_output custom_tool_call_output) do
      url = "data:#{mime};base64," <> Base.encode64("synthetic image")
      payload = output_payload([%{"type" => "input_image", "image_url" => url}], output_type) |> Map.put("type", "response.create")
      result = WebsocketCodec.prepare_frame(CodexPooler.JSON.encode!(payload), websocket_options(payload, mode), fn _ -> :ok end)
      assert elem(result, 0) == :ok
      prepared = elem(result, 1)
      output = List.last(prepared.payload["input"])["output"]
      assert length(output) == 1
      assert :crypto.hash(:sha256, hd(output)["image_url"]) == :crypto.hash(:sha256, url)
    end
  end

  test "direct SVG image parts in function and custom outputs are refused" do
    for output_type <- ~w(function_call_output custom_tool_call_output) do
      assert {:error, %{status: 400, code: "unsupported_input_image_format"}} =
               InputShape.validate(output_payload([svg_part()], output_type))
    end
  end

  test "image-like arbitrary tool result objects and nested values remain opaque" do
    image = svg_part()

    for output_type <- ~w(function_call_output custom_tool_call_output),
        output <- [image, %{"content" => [image]}, [%{"value" => image}], [[image]], CodexPooler.JSON.encode!(image)] do
      assert :ok = InputShape.validate(output_payload(output, output_type))
    end
  end

  test "existing sediment references, remote SVG URLs and raster-labelled bytes remain unchanged" do
    for output_type <- ~w(function_call_output custom_tool_call_output),
        url <- ["sediment://file_fixture", "https://example.com/sample.svg", "data:image/png;base64," <> Base.encode64("<svg/>")] do
      payload = output_payload([%{"type" => "input_image", "image_url" => url}], output_type)
      assert :ok = InputShape.validate(payload)
      assert {:ok, result} = Responses.coerce(payload)
      output = List.last(result.payload["input"])["output"]
      assert :crypto.hash(:sha256, hd(output)["image_url"]) == :crypto.hash(:sha256, url)
    end
  end

  test "unknown tool output variants remain opaque" do
    payload = %{"input" => [%{"type" => "sample_tool_output", "call_id" => "call_fixture", "result" => [svg_part()]}]}
    assert :ok = InputShape.validate(payload)
  end

  test "SVG source supplied as text stays text" do
    output = [%{"type" => "input_text", "text" => "<svg/>"}]
    payload = output_payload(output)
    assert :ok = InputShape.validate(payload)
    assert {:ok, result} = Responses.coerce(payload)
    preserved = List.last(result.payload["input"])["output"]
    assert :crypto.hash(:sha256, CodexPooler.JSON.encode!(preserved)) == :crypto.hash(:sha256, CodexPooler.JSON.encode!(output))
  end

  test "Responses tool role image_url parts use the same SVG refusal" do
    payload = %{
      "model" => "gpt-test-model",
      "input" => [
        %{"type" => "function_call", "call_id" => "call_fixture", "name" => "inspect_fixture", "arguments" => "{}"},
        %{"role" => "tool", "tool_call_id" => "call_fixture", "content" => [%{"type" => "image_url", "image_url" => %{"url" => svg_part()["image_url"]}}]}
      ]
    }

    assert {:error, %{status: 400, code: "unsupported_input_image_format"}} = Responses.coerce(payload)
  end

  defp svg_part, do: %{"type" => "input_image", "image_url" => "data:image/svg+xml;base64," <> Base.encode64("<svg/>")}

  defp websocket_options(payload, mode) do
    %{transport: "websocket", upstream_websocket_session: self(), codex_session: %{id: Ecto.UUID.generate()}}
    |> RequestOptions.build("/backend-api/codex/responses", payload)
    |> RequestOptions.put_model_serving_mode(%{configured_mode: mode, effective_mode: mode, source: "override"})
  end

  defp output_payload(output, type \\ "function_call_output")

  defp output_payload(output, "function_call_output"),
    do: %{"model" => "gpt-test-model", "input" => [%{"type" => "function_call", "call_id" => "call_fixture", "name" => "inspect_fixture", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "call_fixture", "output" => output}]}

  defp output_payload(output, "custom_tool_call_output"),
    do: %{"model" => "gpt-test-model", "input" => [%{"type" => "custom_tool_call", "call_id" => "call_fixture", "name" => "inspect_fixture", "input" => "{}"}, %{"type" => "custom_tool_call_output", "call_id" => "call_fixture", "output" => output}]}
end
