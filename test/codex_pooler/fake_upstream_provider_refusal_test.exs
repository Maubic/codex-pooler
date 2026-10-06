defmodule CodexPooler.FakeUpstreamProviderRefusalTest do
  # The Codex backend refuses a top-level `metadata` (an empty object and null
  # included) and a top-level `tools` entry of type `programmatic_tool_calling`
  # or `web_search_preview` before it generates anything: over HTTP with
  # `400 {"detail": ...}`, on the websocket with the codeless wrapped error
  # frame of the same text, after which it answers nothing more on that
  # connection and drops it without a Close frame (findings#333, direct probe
  # 2026-10-06). FakeUpstream answers the same way, so no gateway test can
  # certify a request the provider refuses.
  use ExUnit.Case, async: true

  alias CodexPooler.FakeUpstream

  @completed ~s({"type":"response.completed","response":{"id":"resp_fake_upstream_scripted","status":"completed","output":[]}})

  test "an HTTP responses request carrying a refused field gets the provider's refusal without consuming a scripted response" do
    upstream = start!(FakeUpstream.strict_sequence([FakeUpstream.json_response(%{"id" => "resp_fake_upstream_scripted"})]))

    for {body, detail} <- [
          {%{"metadata" => %{"label" => "synthetic"}}, "Unsupported parameter: metadata"},
          {%{"metadata" => nil}, "Unsupported parameter: metadata"},
          {%{"tools" => [%{"type" => "web_search"}, %{"type" => "programmatic_tool_calling"}]}, "Unsupported tool type: programmatic_tool_calling"},
          {%{"tools" => [%{"type" => "web_search_preview"}]}, "Unsupported tool type: web_search_preview"}
        ] do
      response = post!(upstream, "/backend-api/codex/responses", Map.put(body, "input", []))
      assert response.status == 400
      assert CodexPooler.JSON.decode!(response.body) == %{"detail" => detail}
    end

    # A Lite manifest is not the top-level declaration the provider refuses on Full.
    manifest = %{"input" => [%{"type" => "additional_tools", "role" => "developer", "tools" => [%{"type" => "programmatic_tool_calling"}]}]}
    assert post!(upstream, "/backend-api/codex/responses", manifest).status == 200
    assert length(FakeUpstream.requests(upstream)) == 5
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "a scripted provider refusal answers HTTP with the detail body" do
    upstream = start!(FakeUpstream.strict_sequence([FakeUpstream.provider_refusal("Unsupported parameter: zz_synthetic")]))

    response = post!(upstream, "/backend-api/codex/responses", %{"input" => []})
    assert response.status == 400
    assert CodexPooler.JSON.decode!(response.body) == %{"detail" => "Unsupported parameter: zz_synthetic"}
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "a websocket frame carrying a refused field gets the codeless refusal frame, and the connection then drops on the next frame" do
    upstream = start!(FakeUpstream.repeat_last([FakeUpstream.websocket_text_frames([@completed])]))
    {conn, websocket, ref} = connect!(upstream)

    try do
      {conn, websocket} = send!(conn, websocket, ref, %{"type" => "response.create", "input" => [], "metadata" => %{}})
      {conn, websocket, [frame]} = receive_frames!(conn, websocket, ref)
      assert CodexPooler.JSON.decode!(frame) == CodexPooler.JSON.decode!(FakeUpstream.provider_refusal_frame("Unsupported parameter: metadata"))

      assert %{"type" => "error", "status" => 400, "error" => %{"type" => "invalid_request_error", "code" => nil, "param" => nil, "message" => "Unsupported parameter: metadata"}} =
               CodexPooler.JSON.decode!(frame)

      # The next request on the refused connection is recorded, never answered, and the connection drops without a Close frame.
      {conn, _websocket} = send!(conn, websocket, ref, %{"type" => "response.create", "input" => []})
      assert_dropped!(conn)
      assert [%{json: %{"metadata" => %{}}}, %{json: second}] = FakeUpstream.requests(upstream)
      refute Map.has_key?(second, "metadata")
    after
      Mint.HTTP.close(conn)
    end
  end

  defp start!(mode) do
    {:ok, upstream} = FakeUpstream.start_link(mode)
    on_exit(fn -> FakeUpstream.stop(upstream) end)
    upstream
  end

  defp post!(upstream, path, body) do
    Req.post!(FakeUpstream.url(upstream) <> path, body: CodexPooler.JSON.encode!(body), headers: [{"content-type", "application/json"}], decode_body: false, retry: false)
  end

  defp connect!(upstream) do
    %URI{port: port} = URI.parse(FakeUpstream.url(upstream))
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])
    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/backend-api/codex/responses", [])
    {conn, %{status: status, headers: headers}} = await_upgrade!(conn, ref, %{})
    {:ok, conn, websocket} = Mint.WebSocket.new(conn, ref, status, headers)
    {conn, websocket, ref}
  end

  defp await_upgrade!(conn, ref, acc) do
    receive do
      message ->
        {:ok, conn, parts} = Mint.WebSocket.stream(conn, message)

        acc =
          Enum.reduce(parts, acc, fn
            {:status, ^ref, status}, acc -> Map.put(acc, :status, status)
            {:headers, ^ref, headers}, acc -> Map.put(acc, :headers, headers)
            {:done, ^ref}, acc -> Map.put(acc, :done, true)
            _part, acc -> acc
          end)

        if acc[:done], do: {conn, acc}, else: await_upgrade!(conn, ref, acc)
    after
      5_000 -> flunk("websocket upgrade timed out")
    end
  end

  defp send!(conn, websocket, ref, payload) do
    {:ok, websocket, data} = Mint.WebSocket.encode(websocket, {:text, CodexPooler.JSON.encode!(payload)})
    {:ok, conn} = Mint.WebSocket.stream_request_body(conn, ref, data)
    {conn, websocket}
  end

  defp receive_frames!(conn, websocket, ref) do
    receive do
      message ->
        {:ok, conn, parts} = Mint.WebSocket.stream(conn, message)

        {websocket, texts} =
          for {:data, ^ref, data} <- parts, reduce: {websocket, []} do
            {websocket, texts} ->
              {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)
              {websocket, texts ++ for({:text, text} <- frames, do: text)}
          end

        if texts == [], do: receive_frames!(conn, websocket, ref), else: {conn, websocket, texts}
    after
      5_000 -> flunk("no frame from the fake upstream")
    end
  end

  # A drop is a transport close, never a Close frame.
  defp assert_dropped!(conn) do
    receive do
      message ->
        case Mint.WebSocket.stream(conn, message) do
          {:error, _conn, %Mint.TransportError{reason: :closed}, _parts} -> :ok
          {:ok, conn, parts} -> if Enum.any?(parts, &match?({:data, _ref, _data}, &1)), do: flunk("the refused connection answered"), else: assert_dropped!(conn)
          other -> flunk("unexpected stream result #{inspect(other)}")
        end
    after
      5_000 -> flunk("the refused connection was not dropped")
    end
  end
end
