defmodule CodexPooler.Gateway.Transports.UpstreamConnectionProbeTest do
  # Whether an upstream request opened a connection or reused a pooled one is the difference between a provider that
  # held a stream and a stale socket that had to be replaced. Finch reports it only through telemetry emitted in the
  # process that runs the request, so the probe hands the answer back to the requesting process, which drains it
  # right after the response headers arrived. The requests are real: a `FakeUpstream` HTTP server and the
  # production `OutboundHTTP` Finch pool selection.
  use ExUnit.Case, async: false

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [start_upstream: 1]

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Transports.UpstreamConnectionProbe
  alias CodexPooler.Platform.OutboundHTTP

  defp sse_upstream do
    start_upstream(FakeUpstream.sse_stream([{"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_probe"}}}]))
  end

  defp stream_post(upstream) do
    UpstreamConnectionProbe.observe(fn probe_options ->
      {:ok, response} = OutboundHTTP.post(FakeUpstream.url(upstream) <> "/probe", [body: "{}", headers: [{"content-type", "application/json"}], into: :self, decode_body: false, retry: false] ++ probe_options)
      response
    end)
  end

  # Reading the whole body hands the connection back to the pool, like a relayed stream that reached its end.
  defp drain!(response), do: Enum.each(response.body, fn _chunk -> :ok end)

  defp probe_messages do
    {:messages, messages} = Process.info(self(), :messages)
    Enum.filter(messages, &(is_tuple(&1) and tuple_size(&1) > 0 and elem(&1, 0) == :codex_pooler_upstream_connection))
  end

  test "the first request to an origin opens a connection and the next one reuses it" do
    upstream = sse_upstream()

    {first, first_class} = stream_post(upstream)
    assert first.status == 200
    assert first_class == "fresh"
    drain!(first)

    {second, second_class} = stream_post(upstream)
    assert second_class == "reused"
    drain!(second)

    assert probe_messages() == []
  end

  test "a connection that cannot be opened is still a fresh attempt and leaves nothing in the mailbox" do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)

    {result, class} =
      UpstreamConnectionProbe.observe(fn probe_options ->
        OutboundHTTP.post("http://127.0.0.1:#{port}/probe", [body: "{}", into: :self, retry: false, receive_timeout: 1_000] ++ probe_options)
      end)

    assert {:error, _reason} = result
    assert class == "fresh"
    assert probe_messages() == []
  end

  test "a request without the probe sends no message and an exception inside observe leaves no message behind" do
    upstream = sse_upstream()

    {:ok, response} = OutboundHTTP.post(FakeUpstream.url(upstream) <> "/probe", body: "{}", into: :self, decode_body: false, retry: false)
    drain!(response)
    assert probe_messages() == []

    assert_raise RuntimeError, "synthetic failure", fn ->
      UpstreamConnectionProbe.observe(fn probe_options ->
        {:ok, response} = OutboundHTTP.post(FakeUpstream.url(upstream) <> "/probe", [body: "{}", into: :self, decode_body: false, retry: false] ++ probe_options)
        drain!(response)
        raise "synthetic failure"
      end)
    end

    assert probe_messages() == []
  end

  test "the class travels on the response and only a known class is stored" do
    response = %Req.Response{status: 200}

    for class <- ["fresh", "reused"] do
      assert response |> UpstreamConnectionProbe.put_connection(class) |> UpstreamConnectionProbe.connection() == class
    end

    for junk <- [nil, "Authorization: Bearer sk-secret", :fresh, 1] do
      assert UpstreamConnectionProbe.put_connection(response, junk) == response
    end

    assert UpstreamConnectionProbe.connection(response) == nil
    assert UpstreamConnectionProbe.connections() == ["fresh", "reused"]

    # A request result passes through: only an `{:ok, response}` carries the class.
    assert {:ok, tagged} = UpstreamConnectionProbe.put_connection({:ok, response}, "reused")
    assert UpstreamConnectionProbe.connection(tagged) == "reused"
    assert UpstreamConnectionProbe.put_connection({:error, :closed}, "fresh") == {:error, :closed}
  end
end
