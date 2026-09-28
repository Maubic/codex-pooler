defmodule CodexPooler.Gateway.RequestCompression.NestedJsonBoundsTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.RequestCompression.ContentDetector
  alias CodexPooler.Gateway.RequestCompression.ResponsesLiveZone

  test "dense inner outputs and irrelevant arguments finish under a bounded child heap" do
    dense = "[" <> Enum.join(List.duplicate(~s({"v":0}), 300_000), ",") <> "]"

    for item <- [
          %{"type" => "local_shell_call_output", "output" => dense},
          %{"type" => "function_call", "call_id" => "synthetic", "name" => "run", "arguments" => ~s({"rows":#{dense}})}
        ] do
      body = CodexPooler.JSON.encode!(%{"input" => [item]})
      {result, reductions, memory} = bounded_child(fn -> ResponsesLiveZone.plan(body, max_candidates: 50, max_json_values: 262_144) end)
      assert {:ok, plan} = result
      assert Enum.all?(plan.candidates, &(not &1.compressible))
      assert reductions < 20_000_000
      assert memory < 33_554_432
    end
  end

  test "deep and dense JSON including embedded and concatenated containers never becomes lossy" do
    deep = String.duplicate("[", 513) <> "0" <> String.duplicate("]", 513)
    dense = "[" <> String.duplicate("0,", 262_144) <> "0]"

    for json <- [deep, dense], content <- [json, "Build failed\n" <> json, ~s({"rows":#{json}}\n{"ok":true})] do
      {decision, reductions, memory} = bounded_child(fn -> ContentDetector.detect(content) end)
      refute decision.compressible
      assert reductions < 10_000_000
      assert memory < 33_554_432
    end
  end

  test "uninspectable arguments preserve matched command outputs" do
    deep = String.duplicate("[", 513) <> "0" <> String.duplicate("]", 513)
    arguments = ~s({"cmd":"cat sample.json","padding":#{deep}})
    output = String.duplicate("Build failed: synthetic error\n", 100)

    body =
      CodexPooler.JSON.encode!(%{
        "input" => [
          %{"type" => "function_call", "call_id" => "synthetic", "name" => "run", "arguments" => arguments},
          %{"type" => "function_call_output", "call_id" => "synthetic", "output" => output}
        ]
      })

    assert {:ok, %{candidates: [], protected_tool_output_skipped_count: 1}} = ResponsesLiveZone.plan(body)
  end

  test "ordinary JSON retains the public planner classification" do
    output = "{\n  \"rows\": [" <> Enum.map_join(1..100, ",\n", &~s({"value": #{&1}})) <> "]\n}"
    body = CodexPooler.JSON.encode!(%{"input" => [%{"type" => "local_shell_call_output", "output" => output}]})
    assert {:ok, %{candidates: [candidate]}} = ResponsesLiveZone.plan(body)
    assert candidate.strategy == :json_document_lossless
    assert candidate.compressible
  end

  test "a newly appended output cannot consume argument safety work ahead of the prefix" do
    output = "{\n  \"rows\": [" <> Enum.map_join(1..100, ",\n", &~s({"value": #{&1}})) <> "]\n}"
    old_call = %{"type" => "function_call", "call_id" => "old", "name" => "run", "arguments" => CodexPooler.JSON.encode!(%{"padding" => String.duplicate("x", 1_048_576)})}
    prefix = [old_call, %{"type" => "function_call", "call_id" => "prefix", "name" => "run", "arguments" => ~s({"cmd":"build"})}, %{"type" => "function_call_output", "call_id" => "prefix", "output" => output}]
    suffix = %{"type" => "function_call_output", "call_id" => "old", "output" => output}
    assert {:ok, short} = ResponsesLiveZone.plan(CodexPooler.JSON.encode!(%{"input" => prefix}))
    assert {:ok, long} = ResponsesLiveZone.plan(CodexPooler.JSON.encode!(%{"input" => prefix ++ [suffix]}))
    assert short.candidates == long.candidates
    assert long.protected_tool_output_skipped_count == 1
  end

  test "large native argv and structured command arguments are inspected only for matched outputs" do
    for call <- [
          %{"type" => "local_shell_call", "id" => "native", "action" => %{"type" => "exec", "command" => ["cat", String.duplicate("x", 3_000_000)]}},
          %{"type" => "function_call", "call_id" => "native", "name" => "run", "arguments" => %{"cmd" => "cat " <> String.duplicate("x", 3_000_000)}}
        ],
        matched? <- [false, true] do
      output = %{"type" => "function_call_output", "call_id" => "native", "output" => String.duplicate("Build failed\n", 100)}
      items = if matched?, do: [call, output], else: [call]
      body = CodexPooler.JSON.encode!(%{"input" => items})
      {result, reductions, memory} = bounded_child(fn -> ResponsesLiveZone.plan(body) end)
      assert {:ok, %{candidates: [], protected_tool_output_skipped_count: count}} = result
      assert count == if(matched?, do: 1, else: 0)
      assert reductions < 20_000_000
      assert memory < 33_554_432
    end
  end

  test "ordinary compressed prefix survives oversized output suffix at dispatch" do
    alias CodexPooler.Gateway.Payloads.RequestOptions
    alias CodexPooler.Gateway.RequestCompression
    output = "{\n  \"rows\": [" <> Enum.map_join(1..100, ",\n", &~s({"value": #{&1}})) <> "]\n}"
    item = %{"type" => "local_shell_call_output", "output" => output}
    suffix = %{item | "output" => String.duplicate("[", 513) <> "0" <> String.duplicate("]", 513)}
    endpoint = "/backend-api/codex/responses"
    options = %{transport: "http_json", upstream_endpoint: endpoint} |> RequestOptions.build(endpoint, %{}) |> RequestOptions.put_transport(route_class: "proxy_http", upstream_endpoint: endpoint)
    context = %{endpoint: endpoint, route_class: "proxy_http", model: %{upstream_model_id: "gpt-4o"}, route_state: %{routing_settings: %{request_compression_enabled: true}}}
    delayed = %{"type" => "function_call", "call_id" => "delayed", "name" => "run", "arguments" => CodexPooler.JSON.encode!(%{"padding" => String.duplicate("x", 1_048_576)})}
    function = %{"type" => "function_call", "call_id" => "prefix", "name" => "run", "arguments" => ~s({"cmd":"build"})}
    function_output = %{"type" => "function_call_output", "call_id" => "prefix", "output" => output}
    delayed_output = %{function_output | "call_id" => "delayed"}

    for {prefix, appended} <- [
          {[item], suffix},
          {[item], %{item | "output" => String.duplicate("x", 1_048_577)}},
          {[delayed, function, function_output], delayed_output}
        ] do
      short_body = CodexPooler.JSON.encode!(%{"input" => prefix})
      long_body = CodexPooler.JSON.encode!(%{"input" => prefix ++ [appended]})
      {short, short_opts} = RequestCompression.maybe_compress(short_body, context, options)
      {long, long_opts} = RequestCompression.maybe_compress(long_body, context, options)
      assert short_opts.runtime.payload_compression["compressed_count"] == 1
      assert long_opts.runtime.payload_compression["compressed_count"] == 1
      short_input = CodexPooler.JSON.decode!(short)["input"]
      long_input = CodexPooler.JSON.decode!(long)["input"]
      assert short_input == Enum.take(long_input, length(prefix))
      assert List.last(long_input) == appended
    end
  end

  defp bounded_child(fun) do
    parent = self()

    {pid, monitor} =
      spawn_monitor(fn ->
        Process.flag(:max_heap_size, %{size: 4_194_304, kill: true, error_logger: false})
        {:reductions, before} = Process.info(self(), :reductions)
        result = fun.()
        {:reductions, after_run} = Process.info(self(), :reductions)
        {:memory, memory} = Process.info(self(), :memory)
        send(parent, {:bounded_result, self(), result, after_run - before, memory})
      end)

    receive do
      {:bounded_result, ^pid, result, reductions, memory} ->
        assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
        {result, reductions, memory}

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        flunk("bounded child exited before result: #{inspect(reason)}")
    after
      10_000 ->
        Process.exit(pid, :kill)
        flunk("bounded child did not finish")
    end
  end
end
