defmodule CodexPoolerWeb.V1.ResponsesSSEToolIntegrityTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [auth: 2, gateway_setup: 1, start_upstream: 1]

  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Persistence.BridgeSessionAlias
  alias CodexPooler.Repo

  for transport <- [:http, :bridge], kind <- ["function_call", "custom_tool_call"], scenario <- [:missing_done, :wrong_index, :wrong_id, :missing_index, :string_index, :orphan_done, :parallel_pending] do
    @tag transport: transport, kind: kind, scenario: scenario
    test "#{transport} #{kind} #{scenario} fails instead of completing", %{transport: transport, kind: kind, scenario: scenario} do
      if transport == :bridge do
        CodexPooler.TestAppEnv.restore_on_exit(:websocket_owner_forwarding_enabled)
        Application.put_env(:codex_pooler, :websocket_owner_forwarding_enabled, true)
      end

      item = %{"id" => "tool_fixture", "call_id" => "call_fixture", "type" => kind, "name" => "lookup"}
      field = if kind == "function_call", do: "arguments", else: "input"
      delta_type = if kind == "function_call", do: "response.function_call_arguments.delta", else: "response.custom_tool_call_input.delta"

      added = %{"type" => "response.output_item.added", "output_index" => 0, "item" => Map.put(item, field, "")}
      done = %{"type" => "response.output_item.done", "output_index" => 0, "item" => Map.put(item, field, "{}")}
      delta = {delta_type, %{"type" => delta_type, "output_index" => 0, "item_id" => "tool_fixture", "delta" => "{}"}}

      events =
        case scenario do
          :missing_done -> [{"response.output_item.added", added}, delta]
          :wrong_index -> [{"response.output_item.added", added}, delta, {"response.output_item.done", %{done | "output_index" => 1}}]
          :wrong_id -> [{"response.output_item.added", added}, delta, {"response.output_item.done", %{done | "item" => %{item | "id" => "tool_wrong"}}}]
          :missing_index -> [{"response.output_item.added", Map.delete(added, "output_index")}, delta]
          :string_index -> [{"response.output_item.added", %{added | "output_index" => "0"}}, delta]
          :orphan_done -> [{"response.output_item.done", done}, delta]
          :parallel_pending -> [{"response.output_item.added", added}, delta, {"response.output_item.added", %{added | "output_index" => 1, "item" => %{item | "id" => "tool_other", "call_id" => "call_other"}}}, {"response.output_item.done", done}]
        end

      events = events ++ [completed([Map.put(item, field, "{}")])]

      upstream = start_upstream(FakeUpstream.sse_stream(events, done: false))
      setup = gateway_setup(upstream)
      owner = self()
      handler = "sse-integrity-#{System.unique_integer([:positive])}"
      :ok = :telemetry.attach(handler, [:codex_pooler, :gateway, :stream, :outcome], fn _event, _measurements, metadata, target -> if self() == target, do: send(target, {:settled_stream, metadata}) end, owner)
      on_exit(fn -> :telemetry.detach(handler) end)

      conn =
        build_conn()
        |> auth(setup)
        |> maybe_session(transport)
        |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic integrity request", "stream" => true})

      assert conn.status == 200
      types = event_types(conn.resp_body)
      assert [captured] = FakeUpstream.requests(upstream)
      assert captured.method == if(transport == :http, do: "POST", else: "WEBSOCKET")
      assert Enum.count(types, &(&1 == "error")) == 1
      refute "response.completed" in types
      assert Enum.count(types, &(&1 == "response.output_item.done")) == Enum.count(events, fn {type, _} -> type == "response.output_item.done" end)
      assert delta_type in types
      refute conn.resp_body =~ "incomplete_tool_item"

      assert_receive {:settled_stream, %{outcome: "failed"}}, 5_000
      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert request.status == "failed"
      assert request.last_error_code == "upstream_stream_error"
      assert request.usage_status == "usage_known"
      assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))
      assert attempt.status == "failed"
      assert attempt.network_error_code == "upstream_stream_error"
      assert [settlement] = Repo.all(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "settlement"))
      assert settlement.input_tokens == 5457
      assert settlement.output_tokens == 3
      assert settlement.cache_write_tokens == 5454
      assert settlement.amount_status == "recorded"
      assert Repo.aggregate(from(a in BridgeSessionAlias, where: a.pool_id == ^setup.pool.id and a.alias_kind == "previous_response_id"), :count) == 0
      assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id and l.entry_kind == "release"), :count) == 1

      FakeUpstream.set_mode(upstream, FakeUpstream.sse_stream([completed([])]))
      healthy = build_conn() |> auth(setup) |> maybe_session(transport) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic next request", "stream" => true})
      assert List.last(event_types(healthy.resp_body)) == "response.completed"
      assert FakeUpstream.count(upstream) == 2
    end
  end

  test "Full parallel completed calls and Lite tool-free next requests retain healthy usage" do
    item = %{"id" => "tool_parallel", "call_id" => "call_parallel", "type" => "function_call", "name" => "lookup", "arguments" => "{}"}

    events = [
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 0, "item" => item}},
      {"response.output_item.added", %{"type" => "response.output_item.added", "output_index" => 1, "item" => %{item | "id" => "tool_other", "call_id" => "call_other"}}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 1, "item" => %{item | "id" => "tool_other", "call_id" => "call_other"}}},
      {"response.output_item.done", %{"type" => "response.output_item.done", "output_index" => 0, "item" => item}},
      completed([item])
    ]

    upstream = start_upstream(FakeUpstream.sse_stream(events))
    setup = gateway_setup(upstream)
    conn = build_conn() |> auth(setup) |> post("/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic valid request", "stream" => true})
    assert List.last(event_types(conn.resp_body)) == "response.completed"
    assert Enum.count(event_types(conn.resp_body), &(&1 == "response.output_item.done")) == 2
    refute "error" in event_types(conn.resp_body)
    assert conn.resp_body =~ ~s("cache_write_tokens":5454)

    lite_upstream = start_upstream(FakeUpstream.sse_stream([completed([])]))
    lite = gateway_setup(lite_upstream)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    Repo.insert!(%CodexPooler.Pools.ModelServingOverride{pool_id: lite.pool.id, exposed_model_id: lite.model.exposed_model_id, mode: "lite", created_at: now, updated_at: now})
    response = build_conn() |> auth(lite) |> post("/v1/responses", %{"model" => lite.model.exposed_model_id, "input" => "synthetic lite control", "stream" => true})
    assert List.last(event_types(response.resp_body)) == "response.completed"
    assert [captured] = FakeUpstream.requests(lite_upstream)
    assert Map.new(captured.headers)["x-openai-internal-codex-responses-lite"] == "true"
    refute Map.has_key?(captured.json, "tools")
  end

  defp maybe_session(conn, :http), do: conn
  defp maybe_session(conn, :bridge), do: put_req_header(conn, "x-session-id", "integrity-#{System.unique_integer([:positive])}")

  defp event_types(body) do
    body |> String.split("\n") |> Enum.filter(&String.starts_with?(&1, "event: ")) |> Enum.map(&String.replace_prefix(&1, "event: ", ""))
  end

  defp completed(output) do
    {"response.completed", %{"type" => "response.completed", "response" => %{"id" => "resp_integrity_fixture", "status" => "completed", "output" => output, "usage" => %{"input_tokens" => 5457, "output_tokens" => 3, "total_tokens" => 5460, "input_tokens_details" => %{"cached_tokens" => 2, "cache_write_tokens" => 5454}}}}}
  end
end
