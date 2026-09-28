defmodule CodexPoolerWeb.Admin.RequestLogRouteTest do
  use ExUnit.Case, async: true

  import Phoenix.LiveViewTest

  alias CodexPoolerWeb.Admin.RequestLogsPresentation

  @source "/v1/responses"
  @destination "/backend-api/codex/responses"

  test "translated HTTP paths share the first line and protocol and client share the second" do
    document = render_route(@destination, origin_metadata(@source, @destination))

    assert_pair(document, @source, @destination)
    assert text(document, "[data-role='route-context-line'] [data-role='protocol-badge']") == "HTTP SSE"
    assert text(document, "[data-role='route-context-line'] [data-role='user-agent-text']") == "OpenAI Python"
    assert Enum.empty?(LazyHTML.query(document, "[data-role='route-context-line'] [data-role='route-origin']"))
    assert Enum.empty?(LazyHTML.query(document, "[data-role='route-paths-line'] [data-role='user-agent']"))
  end

  test "public WebSocket accounting uses the recorded translated path rather than duplicating its source" do
    document = render_route(@source, origin_metadata(@source, @destination), "websocket")

    assert_pair(document, @source, @destination)
    assert text(document, "[data-role='protocol-badge']") == "WebSocket"
  end

  test "compact preserves its distinct accounting route" do
    compact = "/backend-api/codex/responses/compact"
    document = render_route(compact, origin_metadata(@source, @destination))

    assert_pair(document, @source, compact)
    assert LazyHTML.query(document, "[data-role='route']") |> LazyHTML.attribute("title") == [compact]
  end

  test "native routes and identical recorded pairs render only one path" do
    for {endpoint, metadata} <- [
          {@destination, %{}},
          {@destination, nil},
          {@source, origin_metadata(@source, @source)},
          {@source, origin_metadata(@source, nil)},
          {@destination, origin_metadata("", @destination)}
        ] do
      document = render_route(endpoint, metadata)

      assert text(document, "[data-role='route-paths-line']") == endpoint
      assert Enum.empty?(LazyHTML.query(document, "[data-role='route-origin'], [data-role='route-translation']"))
    end
  end

  test "historical and incomplete metadata only use recorded available paths" do
    assert_pair(render_route(@destination, origin_metadata(@source, nil)), @source, @destination)
    assert_pair(render_route(nil, origin_metadata(@source, @destination)), @source, @destination)

    for {endpoint, metadata, expected} <- [
          {nil, origin_metadata(@source, nil), @source},
          {nil, origin_metadata(nil, @destination), @destination},
          {@source, %{"openai_compatibility" => "not collected"}, @source},
          {nil, %{}, "unknown endpoint"}
        ] do
      document = render_route(endpoint, metadata)

      assert text(document, "[data-role='route-paths-line']") == expected
      assert Enum.empty?(LazyHTML.query(document, "[data-role='route-translation']"))
    end
  end

  defp render_route(endpoint, metadata, transport \\ "http_sse") do
    render_component(&RequestLogsPresentation.request_log_route_cell/1,
      request_log: %{id: "sample-request", endpoint: endpoint, metadata: metadata, transport: transport, user_agent: "OpenAI/Python 1.2.3"},
      prefix: "request-log"
    )
    |> LazyHTML.from_fragment()
  end

  defp origin_metadata(source, destination),
    do: %{"openai_compatibility" => %{"source_endpoint" => source, "translated_endpoint" => destination}}

  defp assert_pair(document, source, destination) do
    assert text(document, "[data-role='route-origin']") == source
    assert text(document, "[data-role='route']") == destination
    assert LazyHTML.query(document, "[data-role='route-paths-line'] > [data-role]") |> LazyHTML.attribute("data-role") == ["route-origin", "route-translation", "route"]
    assert text(document, "[data-role='route-translation']") == "translated to"
    assert LazyHTML.query(document, "[data-role='route-paths-line']") |> LazyHTML.attribute("title") == ["Translated from #{source} to #{destination}"]
  end

  defp text(document, selector), do: document |> LazyHTML.query(selector) |> LazyHTML.text() |> String.trim()
end
