defmodule CodexPoolerWeb.Runtime.ImageInputContractTest do
  use CodexPoolerWeb.ConnCase, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [auth: 2, gateway_setup: 2, public_websocket_connect!: 4, public_websocket_receive_text!: 3, public_websocket_send_text!: 4, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Pools.ModelServingOverride
  alias CodexPooler.Repo

  for endpoint <- ["/backend-api/codex/responses", "/v1/responses", "/v1/chat/completions"],
      mode <- ~w(full lite) do
    test("#{endpoint} rejects SVG data URLs before dispatch in #{mode}", %{conn: conn}, do: assert_svg_rejected(conn, unquote(endpoint), unquote(mode)))

    test("#{endpoint} preserves raster data URLs in #{mode}", %{conn: conn}, do: assert_raster_preserved(conn, unquote(endpoint), unquote(mode)))

    test("#{endpoint} rejects SVG image tool output before dispatch in #{mode}", %{conn: conn}, do: assert_svg_tool_output_rejected(conn, unquote(endpoint), unquote(mode)))

    test("#{endpoint} preserves raster image tool output in #{mode}", %{conn: conn}, do: assert_raster_tool_output_preserved(conn, unquote(endpoint), unquote(mode)))
  end

  for endpoint <- ["/backend-api/codex/responses", "/v1/responses"], mode <- ~w(full lite) do
    test("#{endpoint} rejects SVG custom tool image output in #{mode}", %{conn: conn}, do: assert_svg_tool_output_rejected(conn, unquote(endpoint), unquote(mode), "custom_tool_call_output"))

    test("#{endpoint} preserves raster custom tool image output in #{mode}", %{conn: conn}, do: assert_raster_tool_output_preserved(conn, unquote(endpoint), unquote(mode), "custom_tool_call_output"))
  end

  for endpoint <- ["/backend-api/codex/responses", "/v1/responses"], mode <- ~w(full lite), output_type <- ~w(function_call_output custom_tool_call_output) do
    test("#{endpoint} websocket rejects SVG #{output_type} in #{mode} before upstream dispatch", do: assert_websocket_svg_rejected(unquote(endpoint), unquote(mode), unquote(output_type)))
  end

  defp assert_websocket_svg_rejected(endpoint, mode, output_type) do
    {upstream, setup} = setup_gateway(mode)
    port = start_public_endpoint!()
    thread_id = Ecto.UUID.generate()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, thread_id, endpoint)

    try do
      url = "data:image/svg+xml;base64," <> Base.encode64("<svg/>")

      payload =
        tool_payload(endpoint, setup.model.exposed_model_id, url, output_type)
        |> Map.merge(%{"type" => "response.create", "stream" => true, "generate" => true, "client_metadata" => %{"x-codex-turn-metadata" => CodexPooler.JSON.encode!(%{"thread_id" => thread_id, "turn_id" => Ecto.UUID.generate()})}})

      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, CodexPooler.JSON.encode!(payload))
      {_conn, _websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)
      response = CodexPooler.JSON.decode!(frame)
      assert response["type"] == "error"
      assert get_in(response, ["error", "code"]) == "unsupported_input_image_format"
      assert get_in(response, ["error", "param"]) == "input"
      refute String.contains?(frame, url)
      assert FakeUpstream.count(upstream) == 0
      assert FakeUpstream.physical_counts(upstream).http_generation == 0
      assert FakeUpstream.physical_counts(upstream).websocket_generation == 0
      assert Repo.aggregate(Attempt, :count) == 0
      assert Repo.aggregate(LedgerEntry, :count) == 0
    after
      Mint.HTTP.close(conn)
    end
  end

  defp assert_svg_rejected(conn, endpoint, mode) do
    {upstream, setup} = setup_gateway(mode)

    for url <- svg_urls() do
      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post(endpoint, payload(endpoint, setup.model.exposed_model_id, url))

      assert %{"error" => %{"code" => "unsupported_input_image_format", "param" => "input", "message" => message}} = json_response(response, 400)
      assert message =~ "supported image data URLs"
      refute String.contains?(response.resp_body, url)
    end

    assert FakeUpstream.count(upstream) == 0
    assert Repo.aggregate(Attempt, :count) == 0
    assert Repo.aggregate(LedgerEntry, :count) == 0
  end

  defp assert_raster_preserved(conn, endpoint, mode) do
    {upstream, setup} = setup_gateway(mode)

    for mime <- ~w(image/png image/jpeg) do
      url = "data:#{mime};base64," <> Base.encode64("synthetic raster fixture")

      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post(endpoint, payload(endpoint, setup.model.exposed_model_id, url))

      assert response.status == 200
      captured = List.last(FakeUpstream.requests(upstream))
      assert Enum.any?(captured.json["input"], &(&1["type"] == "additional_tools")) == (mode == "lite")
      messages = Enum.filter(captured.json["input"], &(&1["role"] == "user"))
      assert length(messages) == 1
      content = hd(messages)["content"]
      assert length(content) == 1
      assert hd(content)["type"] == "input_image"
      captured_url = hd(content)["image_url"]
      assert :crypto.hash(:sha256, captured_url) == :crypto.hash(:sha256, url)
    end

    assert FakeUpstream.count(upstream) == 2
    assert Repo.aggregate(Attempt, :count) == 2
  end

  defp assert_svg_tool_output_rejected(conn, endpoint, mode, output_type \\ "function_call_output") do
    {upstream, setup} = setup_gateway(mode)

    for url <- svg_urls() do
      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post(endpoint, tool_payload(endpoint, setup.model.exposed_model_id, url, output_type))

      assert response.status == 400
      assert get_in(json_response(response, 400), ["error", "code"]) == "unsupported_input_image_format"
      refute String.contains?(response.resp_body, url)
    end

    assert FakeUpstream.count(upstream) == 0
    assert Repo.aggregate(Attempt, :count) == 0
    assert Repo.aggregate(LedgerEntry, :count) == 0
  end

  defp assert_raster_tool_output_preserved(conn, endpoint, mode, output_type \\ "function_call_output") do
    {upstream, setup} = setup_gateway(mode)

    for mime <- ~w(image/png image/jpeg) do
      url = "data:#{mime};base64," <> Base.encode64("synthetic raster fixture")

      response =
        conn
        |> recycle()
        |> auth(setup)
        |> post(endpoint, tool_payload(endpoint, setup.model.exposed_model_id, url, output_type))

      assert response.status == 200
      captured = List.last(FakeUpstream.requests(upstream))
      assert Enum.any?(captured.json["input"], &(&1["type"] == "additional_tools")) == (mode == "lite")
      output = Enum.find(captured.json["input"], &(&1["type"] == output_type))["output"]
      assert length(output) == 1
      assert hd(output)["type"] == "input_image"
      assert :crypto.hash(:sha256, hd(output)["image_url"]) == :crypto.hash(:sha256, url)
    end

    assert FakeUpstream.count(upstream) == 2
    assert Repo.aggregate(Attempt, :count) == 2
  end

  defp svg_urls do
    [
      "data:image/svg+xml;base64," <> Base.encode64("<svg/>"),
      "data:IMAGE/SVG+XML;BASE64," <> Base.encode64("<svg/>"),
      "data:image/svg+xml,%3Csvg%2F%3E",
      "data:image/svg+xml;charset=utf-8,%3Csvg%2F%3E",
      "data:image/svg+xml,%E0%A4%A",
      "data:image/svg+xml;base64,not-valid-base64!",
      "data:image/svg+xml;base64,",
      "data:image/svg+xml;base64",
      "data:image/svg+xml;base64," <> String.duplicate("A", 1_048_576)
    ]
  end

  defp setup_gateway(mode) do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_image_contract", "object" => "response", "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 5, "output_tokens" => 2, "total_tokens" => 7}}))
    setup = gateway_setup(upstream, model_metadata: %{"supported_input_modalities" => ["text", "image"]})
    timestamp = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%ModelServingOverride{pool_id: setup.pool.id, exposed_model_id: setup.model.exposed_model_id, mode: mode, created_at: timestamp, updated_at: timestamp})
    {upstream, setup}
  end

  defp payload("/v1/chat/completions", model, url),
    do: %{"model" => model, "messages" => [%{"role" => "user", "content" => [%{"type" => "image_url", "image_url" => %{"url" => url}}]}]}

  defp payload(_endpoint, model, url),
    do: %{"model" => model, "input" => [%{"role" => "user", "content" => [%{"type" => "input_image", "image_url" => url}]}]}

  defp tool_payload("/v1/chat/completions", model, url, "function_call_output"),
    do: %{"model" => model, "messages" => [%{"role" => "assistant", "content" => nil, "tool_calls" => [%{"id" => "call_fixture", "type" => "function", "function" => %{"name" => "inspect_fixture", "arguments" => "{}"}}]}, %{"role" => "tool", "tool_call_id" => "call_fixture", "content" => [%{"type" => "image_url", "image_url" => %{"url" => url}}]}]}

  defp tool_payload(_endpoint, model, url, "function_call_output"),
    do: %{"model" => model, "input" => [%{"type" => "function_call", "call_id" => "call_fixture", "name" => "inspect_fixture", "arguments" => "{}"}, %{"type" => "function_call_output", "call_id" => "call_fixture", "output" => [%{"type" => "input_image", "image_url" => url}]}]}

  defp tool_payload(_endpoint, model, url, "custom_tool_call_output"),
    do: %{"model" => model, "input" => [%{"type" => "custom_tool_call", "call_id" => "call_fixture", "name" => "inspect_fixture", "input" => "{}"}, %{"type" => "custom_tool_call_output", "call_id" => "call_fixture", "output" => [%{"type" => "input_image", "image_url" => url}]}]}
end
