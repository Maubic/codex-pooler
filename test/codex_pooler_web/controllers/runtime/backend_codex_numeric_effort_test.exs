defmodule CodexPoolerWeb.Runtime.BackendCodexNumericEffortTest do
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport
  import CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Events
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo

  for mode <- ["full", "lite"], transport <- [:http, :websocket] do
    @mode mode
    @transport transport
    test "#{mode} #{transport} forwards numeric budgets and records exact accounting metadata", %{conn: conn} do
      for effort <- [0, 64, 18_446_744_073_709_551_615] do
        setup = numeric_setup(@mode)
        payload = payload(setup, effort)
        assert :ok = Events.subscribe_pool(setup.pool)
        assert_success(conn, setup, payload, @transport)

        assert [captured] = FakeUpstream.requests(setup.numeric_upstream)
        assert captured.method == if(@transport == :websocket, do: "WEBSOCKET", else: "POST")
        assert get_in(captured.json, ["reasoning", "effort"]) === effort

        if @transport == :websocket do
          assert_receive {Events, %{reason: "request_finalized", payload: %{"status" => "succeeded"}}}, 15_000
        end

        assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
        assert request.reasoning_effort == Integer.to_string(effort)
        assert [attempt] = Repo.all(from(a in Attempt, where: a.request_id == ^request.id))

        for key <- ~w(requested_effort applied_effort effective_effort) do
          assert get_in(attempt.response_metadata, ["reasoning", key]) === effort
        end

        assert get_in(attempt.response_metadata, ["reasoning", "source"]) == "client"
        assert %{items: [log]} = Accounting.list_request_logs(setup.pool)
        assert log.applied_reasoning_effort == Integer.to_string(effort)
        assert log.effective_reasoning_effort == Integer.to_string(effort)
      end
    end

    test "#{mode} #{transport} enforces named policy over a numeric budget", %{conn: conn} do
      setup = numeric_setup(@mode, enforced_reasoning_effort: "high")
      assert_success(conn, setup, payload(setup, 64), @transport)
      assert [captured] = FakeUpstream.requests(setup.numeric_upstream)
      assert get_in(captured.json, ["reasoning", "effort"]) == "high"
    end

    test "#{mode} #{transport} rejects numeric budgets under named maximum before reservation", %{conn: conn} do
      setup = numeric_setup(@mode, maximum_reasoning_effort: "medium")
      assert_error(conn, setup, payload(setup, 64), @transport, "reasoning_effort_not_allowed")
      assert_no_dispatch(setup)
      assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
      assert get_in(request.request_metadata, ["gateway_denial", "reasoning_policy", "requested_effort"]) === 64
    end

    test "#{mode} #{transport} rejects invalid numeric budget shapes before reservation", %{conn: conn} do
      for effort <- [-1, 18_446_744_073_709_551_616, 64.0, true] do
        setup = numeric_setup(@mode, maximum_reasoning_effort: "medium")
        assert_error(conn, setup, payload(setup, effort), @transport, "invalid_request")
        assert_no_dispatch(setup)
      end
    end
  end

  defp numeric_setup(mode, policy \\ []) do
    upstream = start_upstream(FakeUpstream.json_response(%{"id" => "resp_numeric_effort", "object" => "response", "usage" => %{"input_tokens" => 4, "output_tokens" => 3, "total_tokens" => 7}}))
    setup = gateway_setup(upstream)
    setup.api_key |> Ecto.Changeset.change(policy) |> Repo.update!()
    set_model_serving_mode!(model_serving_scope(), setup, mode)
    Map.put(setup, :numeric_upstream, upstream)
  end

  defp payload(setup, effort) do
    %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic"), "reasoning" => %{"effort" => effort}}
  end

  defp assert_success(conn, setup, payload, :http) do
    response = conn |> recycle() |> auth(setup) |> post("/backend-api/codex/responses", payload)
    assert %{"id" => "resp_numeric_effort"} = json_response(response, 200)
  end

  defp assert_success(_conn, setup, payload, :websocket) do
    assert %{"id" => "resp_numeric_effort"} = websocket_response(setup, payload)
  end

  defp assert_error(conn, setup, payload, :http, code) do
    response = conn |> recycle() |> auth(setup) |> post("/backend-api/codex/responses", payload)
    assert %{"error" => %{"code" => ^code, "param" => "reasoning.effort"}} = json_response(response, 400)
  end

  defp assert_error(_conn, setup, payload, :websocket, code) do
    {response, logs} = capture_native_turn_warning(fn -> websocket_response(setup, payload) end)
    assert %{"type" => "error", "status" => 400, "error" => %{"code" => ^code, "param" => "reasoning.effort"}} = response
    assert_native_turn_warnings(logs, 1)
  end

  defp websocket_response(setup, payload) do
    port = start_public_endpoint!()
    {conn, websocket, ref} = public_websocket_connect!(port, setup, "numeric-#{System.unique_integer([:positive])}")

    try do
      frame = payload |> Map.merge(%{"type" => "response.create", "stream" => true, "generate" => true}) |> CodexPooler.JSON.encode!()
      {conn, websocket} = public_websocket_send_text!(conn, websocket, ref, frame)
      {_conn, _websocket, response} = public_websocket_receive_text!(conn, websocket, ref)
      CodexPooler.JSON.decode!(response)
    after
      Mint.HTTP.close(conn)
    end
  end

  defp assert_no_dispatch(setup) do
    assert FakeUpstream.count(setup.numeric_upstream) == 0
    assert [request] = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    assert request.status == "rejected"
    assert Repo.aggregate(from(a in Attempt, where: a.request_id == ^request.id), :count) == 0
    assert Repo.aggregate(from(l in LedgerEntry, where: l.request_id == ^request.id), :count) == 0
  end
end
