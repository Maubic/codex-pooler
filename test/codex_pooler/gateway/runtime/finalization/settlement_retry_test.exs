defmodule CodexPooler.Gateway.Runtime.Finalization.SettlementRetryTest do
  # The retry boundary around one settlement transaction (findings#291),
  # driven with genuine PostgreSQL errors: a statement the server cancels on
  # its own `statement_timeout` (`57014 query_canceled`, the class the
  # production settlement COMMIT met) and a division by zero (`22012`, a
  # failure no retry can fix). The controller-level coverage of the served
  # response lives in `http_settlement_transient_database_test.exs`.
  use CodexPooler.DataCase, async: false

  import ExUnit.CaptureLog

  alias CodexPooler.Accounting.Request
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry
  alias CodexPooler.Repo

  @http_request %Request{id: "00000000-0000-4000-8000-000000000291", transport: "http_sse"}
  @websocket_request %Request{id: "00000000-0000-4000-8000-000000000292", transport: "websocket"}

  setup do
    CodexPooler.TestAppEnv.restore_on_exit(SettlementRetry)
    Application.put_env(:codex_pooler, SettlementRetry, initial_backoff_ms: 1, max_backoff_ms: 2)
    :ok
  end

  test "a settlement cancelled by the database runs again and returns what its second try returns" do
    tries = :counters.new(1, [])

    {result, logs} =
      with_info_log(fn ->
        SettlementRetry.run(:finalize_success, @http_request, nil, fn ->
          :counters.add(tries, 1, 1)
          if :counters.get(tries, 1) == 1, do: cancelled_by_statement_timeout!(), else: {:ok, :settled}
        end)
      end)

    assert result == {:ok, :settled}
    assert :counters.get(tries, 1) == 2
    assert logs =~ "gateway settlement met a transient database failure; retrying stage=finalize_success request_id=#{@http_request.id} attempt_id=unknown settlement_try=1 reason_class=postgres_query_canceled"
    assert logs =~ "gateway settlement completed after a transient database failure stage=finalize_success request_id=#{@http_request.id}"
  end

  test "a failure no retry can fix raises at once" do
    tries = :counters.new(1, [])

    {error, logs} =
      with_info_log(fn ->
        assert_raise Postgrex.Error, fn ->
          SettlementRetry.run(:finalize_success, @http_request, nil, fn ->
            :counters.add(tries, 1, 1)
            Repo.query!("SELECT 1 / 0")
          end)
        end
      end)

    assert %Postgrex.Error{postgres: %{code: :division_by_zero}} = error
    assert :counters.get(tries, 1) == 1
    refute logs =~ "gateway settlement"
  end

  test "inside a transaction the caller owns, a transient failure raises at once and the caller's rollback decides" do
    tries = :counters.new(1, [])

    assert_raise Postgrex.Error, ~r/57014/, fn ->
      Repo.transaction(fn ->
        SettlementRetry.run(:finalize_success, @http_request, nil, fn ->
          :counters.add(tries, 1, 1)
          cancelled_by_statement_timeout!()
        end)
      end)
    end

    assert :counters.get(tries, 1) == 1
  end

  test "when the window has closed, an HTTP settlement answers the exhausted error after one warning naming the request and the stage" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)
    tries = :counters.new(1, [])

    {result, logs} =
      with_log([level: :warning], fn ->
        SettlementRetry.run(:finalize_partial_stream_failure, @http_request, %{id: "attempt-291"}, fn ->
          :counters.add(tries, 1, 1)
          cancelled_by_statement_timeout!()
        end)
      end)

    assert result == {:error, :settlement_retry_exhausted}
    assert :counters.get(tries, 1) == 1

    assert [line] = Regex.scan(~r/gateway settlement abandoned[^\n]*/, logs) |> List.flatten()

    assert line =~
             "gateway settlement abandoned after transient database failures stage=finalize_partial_stream_failure " <>
               "request_id=#{@http_request.id} attempt_id=attempt-291 settlement_tries=1 elapsed_ms="

    assert line =~ ~r/ reason_class=postgres_query_canceled fallback=execution_recovery$/
  end

  test "when the window has closed, a websocket settlement raises its last failure for the task's own exception finalization" do
    Application.put_env(:codex_pooler, SettlementRetry, window_ms: 0)

    {error, logs} =
      with_log([level: :warning], fn ->
        assert_raise Postgrex.Error, fn ->
          SettlementRetry.run(:finalize_success, @websocket_request, nil, fn -> cancelled_by_statement_timeout!() end)
        end
      end)

    assert %Postgrex.Error{postgres: %{code: :query_canceled}} = error
    assert logs =~ "request_id=#{@websocket_request.id}"
    assert logs =~ "fallback=task_exception"
  end

  # PostgreSQL cancels the statement itself when it outlives the transaction's
  # 1 ms `statement_timeout`; the transaction rolls back like the settlement's.
  defp cancelled_by_statement_timeout! do
    Repo.transaction(fn ->
      Repo.query!("SET LOCAL statement_timeout = 1")
      Repo.query!("SELECT pg_sleep(1)")
    end)
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
end
