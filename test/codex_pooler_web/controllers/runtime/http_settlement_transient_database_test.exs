defmodule CodexPoolerWeb.Runtime.HTTPSettlementTransientDatabaseTest do
  # A served HTTP response whose settlement meets a transient database
  # failure (findings#291). In production a database stall kept the settlement
  # COMMIT waiting past the Repo's 15 s query timeout; DBConnection cancelled
  # it, PostgreSQL answered `57014 query_canceled`, the exception ended the
  # connection process after the client had read every SSE event, the chunked
  # body was never terminated, and the unsettled request was recovered minutes
  # later as `failed 499 dead_execution_recovered` with its usage lost.
  #
  # The failure is real and happens at the same place: a deferred constraint
  # trigger on this test's own Pool makes the settlement COMMIT wait on an
  # advisory lock the test holds, and the test cancels or terminates the
  # waiting backend. A sequence the trigger advances before it waits counts
  # the settlement's COMMITs without being rolled back with them.
  #
  # Topology: one node, committed rows, the real listener, Mint as the client,
  # FakeUpstream over HTTP SSE, the Pool's default serving mode (Full), owner
  # forwarding irrelevant (HTTP).
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, register_unboxed_pool_cleanup!: 1, start_public_endpoint!: 0, start_upstream: 1]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.Accounting.RequestLifecycle
  alias CodexPooler.ExecutionProofSupport
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Repo
  alias CodexPooler.UnboxedFixture
  alias Ecto.Adapters.SQL.Sandbox

  @detection_timeout_ms 15_000
  @response_id "resp_settlement_transient_database"

  setup do
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    CodexPooler.TestAppEnv.restore_on_exit(SettlementRetry)
    Application.put_env(:codex_pooler, SettlementRetry, initial_backoff_ms: 10, max_backoff_ms: 50)
    :ok
  end

  for {surface, path} <- [v1: "/v1/responses", native: "/backend-api/codex/responses"] do
    @path path

    test "#{surface}: a settlement COMMIT cancelled by the database is retried, the stream ends cleanly and the request settles succeeded" do
      {setup, observer, gate, holder} = streaming_fixture!()
      client = post!(setup, @path, stream_payload(setup, @path))

      {client, logs} =
        with_info_log(fn ->
          waiter = await_gate_waiter!(observer, gate, holder, 1)
          client = read_until!(client, &String.contains?(&1, "response.completed"))
          assert cancel_backend!(observer, waiter)

          _retried = await_gate_waiter!(observer, gate, holder, 2)

          # While the retry is pending its executor is alive, so neither
          # execution recovery authority can settle the request.
          assert [%Attempt{status: "in_progress"} = pending] = pool_attempts(setup)
          assert ExecutionIdentity.status(pending) == :alive
          assert RequestLifecycle.execution_recovery_authority(pending) == nil

          release_gate!(holder)
          read_to_end(client)
        end)

      assert client.status == 200
      assert client.outcome == :done
      assert "response.completed" in event_types(client.body)

      request = settled_request!(setup)
      assert {request.status, request.last_error_code, request.usage_status} == {"succeeded", nil, "usage_known"}
      assert [%Attempt{status: "succeeded"}] = attempts(request)
      assert recorded_settlements(request) == [{"usage_known", 5}]
      assert gate_passes(observer, gate) == 2

      assert logs =~ "gateway settlement met a transient database failure; retrying stage=finalize_success request_id=#{request.id}"
      assert logs =~ "reason_class=postgres_query_canceled"
      assert logs =~ "gateway settlement completed after a transient database failure stage=finalize_success request_id=#{request.id}"
      assert logs =~ "settlement_tries=2"

      # The retried settlement leaves execution recovery nothing to do.
      [attempt] = attempts(request)
      assert ExecutionIdentity.status(attempt) == :dead
      :ok = ExecutionProofSupport.publish_committed_terminal!(attempt)
      assert {:ok, %{dead_execution_attempts_recovered: 0}} = Accounting.recover_dead_execution_attempts(DateTime.add(DateTime.utc_now(), 121, :second))
      assert Repo.reload!(request).status == "succeeded"
    end
  end

  test "v1: a settlement whose database connection is terminated at COMMIT is retried on another connection" do
    {setup, observer, gate, holder} = streaming_fixture!()
    client = post!(setup, "/v1/responses", stream_payload(setup, "/v1/responses"))

    {client, logs} =
      with_info_log(fn ->
        waiter = await_gate_waiter!(observer, gate, holder, 1)
        client = read_until!(client, &String.contains?(&1, "response.completed"))
        assert terminate_backend!(observer, waiter)

        retried = await_gate_waiter!(observer, gate, holder, 2)
        assert retried != waiter
        release_gate!(holder)
        read_to_end(client)
      end)

    assert {client.status, client.outcome} == {200, :done}
    request = settled_request!(setup)
    assert {request.status, request.usage_status} == {"succeeded", "usage_known"}
    assert recorded_settlements(request) == [{"usage_known", 5}]
    assert logs =~ "gateway settlement met a transient database failure; retrying stage=finalize_success request_id=#{request.id}"
  end

  test "v1: a non-streaming response whose settlement COMMIT is cancelled is retried and answered" do
    {setup, observer, gate, holder} = streaming_fixture!()
    client = post!(setup, "/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic settlement retry json", "stream" => false})

    waiter = await_gate_waiter!(observer, gate, holder, 1)
    assert cancel_backend!(observer, waiter)
    _retried = await_gate_waiter!(observer, gate, holder, 2)
    release_gate!(holder)
    client = read_to_end(client)

    assert {client.status, client.outcome} == {200, :done}
    assert %{"id" => @response_id, "status" => "completed"} = CodexPooler.JSON.decode!(client.body)
    request = settled_request!(setup)
    assert {request.status, request.usage_status} == {"succeeded", "usage_known"}
    assert recorded_settlements(request) == [{"usage_known", 5}]
  end

  test "v1: when the retry window closes a non-streaming response answers the sanitized accounting error" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)
    {setup, observer, gate, holder} = streaming_fixture!()
    client = post!(setup, "/v1/responses", %{"model" => setup.model.exposed_model_id, "input" => "synthetic settlement retry json", "stream" => false})

    {client, logs} =
      with_log([level: :warning], fn ->
        waiter = await_gate_waiter!(observer, gate, holder, 1)
        assert cancel_backend!(observer, waiter)
        read_to_end(client)
      end)

    release_gate!(holder)
    assert {client.status, client.outcome} == {500, :done}
    # `/v1` keeps the gateway's code and redacts the message of its 500.
    assert %{"error" => %{"code" => "gateway_accounting_failed", "type" => "server_error"}} = CodexPooler.JSON.decode!(client.body)
    assert [%Request{status: "in_progress"} = request] = pool_requests(setup)
    assert logs =~ "gateway settlement abandoned after transient database failures stage=finalize_success request_id=#{request.id}"
  end

  test "v1: when the retry window closes the stream still ends cleanly, one warning names the request and the stage, and execution recovery settles the request" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)
    {setup, observer, gate, holder} = streaming_fixture!()
    client = post!(setup, "/v1/responses", stream_payload(setup, "/v1/responses"))

    {client, logs} =
      with_log([level: :warning], fn ->
        waiter = await_gate_waiter!(observer, gate, holder, 1)
        client = read_until!(client, &String.contains?(&1, "response.completed"))
        assert cancel_backend!(observer, waiter)
        read_to_end(client)
      end)

    assert {client.status, client.outcome} == {200, :done}
    assert "response.completed" in event_types(client.body)
    assert gate_passes(observer, gate) == 1
    release_gate!(holder)

    [request] = pool_requests(setup)
    assert request.status == "in_progress"

    assert [line] = Regex.scan(~r/gateway settlement abandoned after transient database failures[^\n]*/, logs) |> List.flatten()
    assert line =~ "stage=finalize_success request_id=#{request.id}"
    assert line =~ "settlement_tries=1"
    assert line =~ "reason_class=postgres_query_canceled fallback=execution_recovery"

    # The existing cleanup: the connection completed its execution, the proof
    # is published, and dead-execution recovery settles the request.
    [attempt] = attempts(request)
    assert ExecutionIdentity.status(attempt) == :dead
    :ok = ExecutionProofSupport.publish_committed_terminal!(attempt)
    assert {:ok, %{dead_execution_attempts_recovered: 1}} = Accounting.recover_dead_execution_attempts(DateTime.add(DateTime.utc_now(), 121, :second))
    assert %Request{status: "failed", response_status_code: 499, last_error_code: "dead_execution_recovered"} = Repo.reload!(request)
  end

  test "v1: a constraint violation at the settlement COMMIT is not retried and still surfaces" do
    {setup, observer, gate, _holder} = streaming_fixture!(:reject)
    client = post!(setup, "/v1/responses", stream_payload(setup, "/v1/responses"))

    {client, logs} = with_log([level: :info], fn -> read_to_end(client) end)

    assert gate_passes(observer, gate) == 1
    assert {:error, _reason} = client.outcome
    refute logs =~ "gateway settlement met a transient database failure"
    assert [%Request{status: "in_progress"}] = pool_requests(setup)
  end

  # --- fixture -------------------------------------------------------------

  defp streaming_fixture!(gate_mode \\ :hold) do
    upstream =
      start_upstream(
        # provenance: synthetic_adversarial (one ordinary Responses stream with usage; the Pooler's settlement is what fails)
        FakeUpstream.sse_stream([created_event(), delta_event(), completed_event()], done: false, headers: [{"content-type", "text/event-stream"}])
      )

    setup = gateway_setup(upstream)
    register_unboxed_pool_cleanup!(setup)
    observer = observer!()
    gate = install_commit_gate!(setup.pool.id, gate_mode)
    holder = if gate_mode == :hold, do: hold_gate!(gate), else: nil
    {setup, observer, gate, holder}
  end

  # A deferred constraint trigger runs at COMMIT, the step the production
  # settlement was cut in. It counts the COMMIT on a sequence (sequences are
  # not transactional, so the count survives the rollback), then waits on the
  # test's advisory lock or rejects the settlement as a check violation.
  defp install_commit_gate!(pool_id, mode) do
    suffix = System.unique_integer([:positive])
    name = "settlement_commit_gate_#{suffix}"
    key = 291_000_000 + suffix

    UnboxedFixture.register_unboxed_cleanup!(fn ->
      Repo.query!("DROP TRIGGER IF EXISTS #{name} ON requests")
      Repo.query!("DROP FUNCTION IF EXISTS #{name}()")
      Repo.query!("DROP SEQUENCE IF EXISTS #{name}_passes")
    end)

    body =
      case mode do
        :hold -> "PERFORM pg_advisory_xact_lock(#{key});"
        :reject -> "RAISE EXCEPTION 'synthetic settlement rejection' USING ERRCODE = 'check_violation';"
      end

    UnboxedFixture.run_unboxed(fn ->
      Repo.query!("CREATE SEQUENCE #{name}_passes")
      Repo.query!("CREATE FUNCTION #{name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM nextval('#{name}_passes'); #{body} RETURN NULL; END $$")

      Repo.query!(
        "CREATE CONSTRAINT TRIGGER #{name} AFTER UPDATE ON requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW " <>
          "WHEN (NEW.pool_id = '#{pool_id}'::uuid AND NEW.status = 'succeeded' AND OLD.status IS DISTINCT FROM NEW.status) EXECUTE FUNCTION #{name}()"
      )
    end)

    %{name: name, key: key}
  end

  defp hold_gate!(%{key: key}) do
    holder = start_supervised!({Postgrex, connection_options()}, id: {:settlement_gate_holder, key})
    %{rows: [[backend]]} = Postgrex.query!(holder, "SELECT pg_backend_pid()", [])
    %{rows: [[_void]]} = Postgrex.query!(holder, "SELECT pg_advisory_lock($1)", [key])
    %{conn: holder, backend: backend, key: key}
  end

  defp release_gate!(%{conn: holder, key: key}) do
    assert %{rows: [[true]]} = Postgrex.query!(holder, "SELECT pg_advisory_unlock($1)", [key])
    :ok
  end

  # A PostgreSQL connection outside the Repo pool the listener draws from
  # (findings#206 row 206-501).
  defp observer!, do: start_supervised!({Postgrex, connection_options()}, id: {:settlement_gate_observer, System.unique_integer([:positive])})

  defp connection_options, do: Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database, :socket_dir])

  # The settlement has reached its `passes`-th COMMIT and waits on the gate.
  # `pg_stat_activity` pairs a live wait with the backend's status snapshot,
  # so both facts are sampled again until they agree (findings#206 row 206-182).
  defp await_gate_waiter!(observer, gate, holder, passes) do
    await_gate_waiter!(observer, gate, holder, passes, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_gate_waiter!(observer, gate, holder, passes, deadline) do
    %{rows: waiters} = Postgrex.query!(observer, "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [holder.backend])
    seen = gate_passes(observer, gate)

    cond do
      seen == passes and match?([[_pid]], waiters) ->
        [[pid]] = waiters
        pid

      seen > passes ->
        flunk("the settlement passed its COMMIT gate #{seen} times, expected #{passes}")

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("the settlement never waited at COMMIT pass #{passes} (passes #{seen}, waiters #{inspect(waiters)})")

      true ->
        Process.sleep(10)
        await_gate_waiter!(observer, gate, holder, passes, deadline)
    end
  end

  defp gate_passes(observer, %{name: name}) do
    %{rows: [[value, called?]]} = Postgrex.query!(observer, "SELECT last_value, is_called FROM #{name}_passes", [])
    if called?, do: value, else: 0
  end

  defp cancel_backend!(observer, backend) do
    %{rows: [[cancelled?]]} = Postgrex.query!(observer, "SELECT pg_cancel_backend($1)", [backend])
    cancelled?
  end

  defp terminate_backend!(observer, backend) do
    %{rows: [[terminated?]]} = Postgrex.query!(observer, "SELECT pg_terminate_backend($1)", [backend])
    terminated?
  end

  # --- client --------------------------------------------------------------

  defp post!(setup, path, payload) do
    port = start_public_endpoint!()
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, mode: :passive, protocols: [:http1])
    headers = [{"authorization", setup.authorization}, {"content-type", "application/json"}, {"accept", "text/event-stream"}]
    {:ok, conn, ref} = Mint.HTTP.request(conn, "POST", path, headers, CodexPooler.JSON.encode!(payload))
    on_exit(fn -> Mint.HTTP.close(conn) end)
    %{conn: conn, ref: ref, status: nil, body: "", outcome: nil}
  end

  # Reads what already arrived until the body satisfies `fun`, without
  # waiting for the end of the response.
  defp read_until!(client, fun) do
    cond do
      fun.(client.body) -> client
      client.outcome != nil -> flunk("the response ended before the expected bytes: #{inspect(client.outcome)}")
      true -> client |> recv() |> read_until!(fun)
    end
  end

  defp read_to_end(%{outcome: nil} = client), do: client |> recv() |> read_to_end()
  defp read_to_end(client), do: client

  defp recv(%{conn: conn, ref: ref} = client) do
    case Mint.HTTP.recv(conn, 0, @detection_timeout_ms) do
      {:ok, conn, responses} ->
        Enum.reduce(responses, %{client | conn: conn}, fn
          {:status, ^ref, status}, client -> %{client | status: status}
          {:headers, ^ref, _headers}, client -> client
          {:data, ^ref, data}, client -> %{client | body: client.body <> data}
          {:done, ^ref}, client -> %{client | outcome: :done}
          _other, client -> client
        end)

      {:error, conn, reason, responses} ->
        client =
          Enum.reduce(responses, %{client | conn: conn}, fn
            {:data, ^ref, data}, client -> %{client | body: client.body <> data}
            _other, client -> client
          end)

        %{client | outcome: {:error, reason}}
    end
  end

  defp with_info_log(fun) do
    previous = Logger.level()
    on_exit(fn -> Logger.configure(level: previous) end)
    Logger.configure(level: :info)

    try do
      with_log([level: :info], fun)
    after
      Logger.configure(level: previous)
    end
  end

  defp event_types(body) do
    body
    |> String.split("\n", trim: true)
    |> Enum.flat_map(&data_event_type/1)
  end

  defp data_event_type("data: " <> json) do
    case CodexPooler.JSON.decode(json) do
      {:ok, %{"type" => type}} -> [type]
      _other -> []
    end
  end

  defp data_event_type(_line), do: []

  # --- rows ----------------------------------------------------------------

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))

  defp settled_request!(setup), do: await_settled!(setup, System.monotonic_time(:millisecond) + @detection_timeout_ms)

  defp await_settled!(setup, deadline) do
    case pool_requests(setup) do
      [%Request{status: status} = request] when status not in ["accepted", "in_progress"] ->
        request

      requests ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: flunk("the request never settled: #{inspect(Enum.map(requests, & &1.status))}"),
          else: Process.sleep(10) && await_settled!(setup, deadline)
    end
  end

  defp pool_attempts(setup), do: Repo.all(from(a in Attempt, join: r in Request, on: r.id == a.request_id, where: r.pool_id == ^setup.pool.id))

  defp attempts(request), do: Repo.all(from(a in Attempt, where: a.request_id == ^request.id, order_by: a.attempt_number))

  defp recorded_settlements(request) do
    Repo.all(
      from(entry in LedgerEntry,
        where: entry.request_id == ^request.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded",
        select: {entry.usage_status, entry.total_tokens}
      )
    )
  end

  # --- payloads ------------------------------------------------------------

  defp stream_payload(setup, "/v1/responses"), do: %{"model" => setup.model.exposed_model_id, "input" => "synthetic settlement retry stream", "stream" => true}
  defp stream_payload(setup, _native), do: %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic settlement retry stream"), "stream" => true}

  defp created_event, do: {"response.created", %{"type" => "response.created", "response" => %{"id" => @response_id, "status" => "in_progress"}}}

  defp delta_event,
    do: {"response.output_text.delta", %{"type" => "response.output_text.delta", "response_id" => @response_id, "output_index" => 0, "content_index" => 0, "delta" => "synthetic settled text"}}

  defp completed_event do
    {"response.completed",
     %{
       "type" => "response.completed",
       "response" => %{"id" => @response_id, "status" => "completed", "output" => [], "usage" => %{"input_tokens" => 2, "output_tokens" => 3, "total_tokens" => 5}}
     }}
  end
end
