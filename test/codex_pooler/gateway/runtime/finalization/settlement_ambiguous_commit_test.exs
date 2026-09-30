defmodule CodexPooler.Gateway.Runtime.Finalization.SettlementAmbiguousCommitTest do
  # A COMMIT the client saw fail can still have committed on the server: the
  # connection drops after PostgreSQL committed and before its reply arrived.
  # `SettlementRetry` then runs the same settlement again, and the retried try
  # meets the first try's own writes (findings#291, findings#294).
  #
  # The ambiguity is produced for real: a loopback proxy in front of the test
  # PostgreSQL forwards the settlement's `COMMIT`, waits until the server
  # answers `CommandComplete COMMIT`, swallows that answer and closes the
  # client's socket. The Repo behind the proxy is a separate pool
  # (`Repo.put_dynamic_repo/1`), so only the call under test goes through it.
  #
  # Topology: one node, committed rows, no listener (the settlement boundary is
  # called directly as the HTTP relay calls it), HTTP SSE request rows.
  use CodexPoolerWeb.ConnCase, async: false

  import Ecto.Query
  import ExUnit.CaptureLog

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport,
    only: [gateway_setup: 1, native_text_input: 1, register_unboxed_pool_cleanup!: 1, start_upstream: 1]

  alias CodexPooler.Access
  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request}
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  # The simple-query COMMIT Postgrex sends and the server's CommandComplete
  # answer to it: type byte, int32 length (4 + "COMMIT\0"), tag.
  @commit_query <<?Q, 0, 0, 0, 11, "COMMIT", 0>>
  @commit_complete <<?C, 0, 0, 0, 11, "COMMIT", 0>>

  setup do
    Sandbox.mode(Repo, :auto)
    on_exit(fn -> Sandbox.mode(Repo, :manual) end)
    CodexPooler.TestAppEnv.restore_on_exit(SettlementRetry)
    Application.put_env(:codex_pooler, SettlementRetry, initial_backoff_ms: 10, max_backoff_ms: 50)
    :ok
  end

  test "record_retryable_failure: a retry that meets its own committed retryable failure fails over instead of answering attempt_already_finalized" do
    {setup, request, attempt} = attempt_fixture!()
    proxy = start_commit_cut_proxy!()
    proxied = start_proxied_repo!(proxy)

    {result, logs} =
      with_info_log(fn ->
        through(proxied, fn ->
          arm_commit_cut!(proxy)
          AttemptSettlement.record_retryable_failure(request, attempt, %{response_status_code: 502, last_error_code: "upstream_5xx"})
        end)
      end)

    assert commit_cuts(proxy) == 1
    assert {:ok, %Attempt{id: attempt_id, status: "retryable_failed"}} = result
    assert attempt_id == attempt.id
    assert %Attempt{status: "retryable_failed", retryable: true, network_error_code: "upstream_5xx"} = Repo.get!(Attempt, attempt.id)
    assert [%Request{status: "in_progress"}] = pool_requests(setup)
    # Consumer-visible failover: a second attempt is still admitted and the
    # eventual success creates exactly one settlement, not one per retry.
    assert {:ok, next} = Accounting.create_attempt(request, setup.assignment)
    assert next.id != attempt.id
    assert {:ok, _finalized} = AttemptSettlement.finalize_success(request, next, %{status: "usage_known", input_tokens: 2, output_tokens: 3, total_tokens: 5}, %{response_status_code: 200})
    assert recorded_settlements(request) == [{"usage_known", 5}]

    assert logs =~ "gateway settlement met a transient database failure; retrying stage=record_retryable_failure request_id=#{request.id} attempt_id=#{attempt.id} settlement_try=1 reason_class=DBConnection.ConnectionError"
    assert logs =~ "gateway settlement completed after a transient database failure stage=record_retryable_failure request_id=#{request.id}"
  end

  test "record_retryable_failure: an attempt already finalized before the call still answers attempt_already_finalized" do
    {_setup, request, attempt} = attempt_fixture!()
    attrs = %{response_status_code: 502, last_error_code: "upstream_5xx"}

    assert {:ok, %Attempt{status: "retryable_failed"}} = AttemptSettlement.record_retryable_failure(request, attempt, attrs)
    assert {:error, %{status: 499, code: "attempt_already_finalized"}} = AttemptSettlement.record_retryable_failure(request, attempt, attrs)
  end

  test "record_retryable_failure: another committed failure is not reconciled as this caller's ambiguous commit" do
    {_setup, request, attempt} = attempt_fixture!()
    at = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    assert {:ok, _recorded} = AttemptSettlement.record_retryable_failure(request, attempt, %{now: at, response_status_code: 502, last_error_code: "upstream_5xx", error_message: "synthetic competing failure", latency_ms: 17, attempt_metadata: %{synthetic_competitor: true}})
    original = Repo.get!(Attempt, attempt.id)
    opts = Repo.config() |> Keyword.take([:hostname, :port, :username, :password, :database])
    holder = start_supervised!({Postgrex, opts}, id: :competing_failure_lock)
    observer = start_supervised!({Postgrex, opts}, id: :competing_failure_observer)
    Postgrex.query!(holder, "BEGIN", [])
    %{rows: [[holding_pid]]} = Postgrex.query!(holder, "SELECT pg_backend_pid()", [])
    Postgrex.query!(holder, "SELECT id FROM requests WHERE id=$1 FOR UPDATE", [Ecto.UUID.dump!(request.id)])
    logs = with_info_log(fn ->
      task = Task.async(fn -> AttemptSettlement.record_retryable_failure(request, attempt, %{now: at, response_status_code: 502, last_error_code: "upstream_5xx", error_message: "synthetic own failure", latency_ms: 23}) end)
      backend = await_competing_waiter!(observer, holding_pid, System.monotonic_time(:millisecond) + 15_000)
      assert %{rows: [[true]]} = Postgrex.query!(observer, "SELECT pg_cancel_backend($1)", [backend])
      Postgrex.query!(holder, "COMMIT", [])
      assert {:error, %{code: "attempt_already_finalized"}} = Task.await(task, 15_000)
    end) |> elem(1)
    assert logs =~ "retrying stage=record_retryable_failure"
    assert Repo.get!(Attempt, attempt.id) == original
  end

  defp await_competing_waiter!(observer, holder, deadline) do
    case Postgrex.query!(observer, "SELECT pid FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))", [holder]).rows do
      [[backend]] -> backend
      [] ->
        assert System.monotonic_time(:millisecond) < deadline
        Process.sleep(5)
        await_competing_waiter!(observer, holder, deadline)
    end
  end

  test "finalize_success: a retry that meets its own committed settlement reuses it and records one settlement" do
    {setup, request, attempt} = attempt_fixture!()
    proxy = start_commit_cut_proxy!()
    proxied = start_proxied_repo!(proxy)
    usage = %{status: "usage_known", input_tokens: 2, output_tokens: 3, total_tokens: 5}

    {result, logs} =
      with_info_log(fn ->
        through(proxied, fn ->
          arm_commit_cut!(proxy)
          AttemptSettlement.finalize_success(request, attempt, usage, %{response_status_code: 200})
        end)
      end)

    assert commit_cuts(proxy) == 1
    assert {:ok, %{finalization_disposition: :reused, request: %Request{status: "succeeded"}}} = result
    assert [%Request{status: "succeeded", usage_status: "usage_known"} = settled] = pool_requests(setup)
    assert recorded_settlements(settled) == [{"usage_known", 5}]
    assert logs =~ "gateway settlement met a transient database failure; retrying stage=finalize_success request_id=#{request.id}"
  end

  # --- rows ----------------------------------------------------------------

  defp attempt_fixture! do
    setup = gateway_setup(start_upstream(FakeUpstream.sse_stream([])))
    register_unboxed_pool_cleanup!(setup)
    {:ok, auth} = Access.authenticate_authorization_header(setup.authorization)
    payload = %{"model" => setup.model.exposed_model_id, "input" => native_text_input("synthetic ambiguous commit"), "stream" => true}

    {:ok, reserved} =
      Accounting.reserve(auth, setup.model, payload, %{
        endpoint: "/v1/responses",
        transport: "http_sse",
        correlation_id: "ambiguous-commit-#{System.unique_integer([:positive])}",
        request_metadata: %{}
      })

    {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    {setup, reserved.request, attempt}
  end

  defp pool_requests(setup), do: Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))

  defp recorded_settlements(request) do
    Repo.all(
      from(entry in LedgerEntry,
        where: entry.request_id == ^request.id and entry.entry_kind == "settlement" and entry.amount_status == "recorded",
        select: {entry.usage_status, entry.total_tokens}
      )
    )
  end

  # --- proxy ---------------------------------------------------------------

  defp start_commit_cut_proxy! do
    config = Repo.config()
    target = {String.to_charlist(config[:hostname]), config[:port]}
    # slot 1: 1 while the next COMMIT answer is to be cut; slot 2: answers cut
    flags = :atomics.new(2, [])
    caller = self()
    ref = make_ref()

    _acceptor =
      start_supervised!(
        {Task,
         fn ->
           {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])
           {:ok, port} = :inet.port(listen)
           send(caller, {ref, port})
           accept_loop(listen, target, flags)
         end},
        id: {:commit_cut_proxy, ref}
      )

    assert_receive {^ref, port}, 15_000
    %{port: port, flags: flags}
  end

  defp arm_commit_cut!(%{flags: flags}), do: :atomics.put(flags, 1, 1)
  defp commit_cuts(%{flags: flags}), do: :atomics.get(flags, 2)

  defp accept_loop(listen, target, flags) do
    case :gen_tcp.accept(listen) do
      {:ok, client} ->
        start_relays(client, target, flags)
        accept_loop(listen, target, flags)

      {:error, _closed} ->
        :ok
    end
  end

  defp start_relays(client, {host, port}, flags) do
    case :gen_tcp.connect(host, port, [:binary, active: false]) do
      {:ok, server} ->
        cut = :atomics.new(1, [])
        spawn_link(fn -> client_to_server(client, server, flags, cut) end)
        spawn_link(fn -> server_to_client(server, client, flags, cut) end)

      {:error, _reason} ->
        :gen_tcp.close(client)
    end
  end

  # The connection that carries the armed COMMIT is marked before the COMMIT
  # is forwarded, so its answer cannot overtake the mark.
  defp client_to_server(client, server, flags, cut) do
    case :gen_tcp.recv(client, 0) do
      {:ok, data} ->
        if :binary.match(data, @commit_query) != :nomatch and :atomics.compare_exchange(flags, 1, 1, 0) == :ok,
          do: :atomics.put(cut, 1, 1)

        _sent = :gen_tcp.send(server, data)
        client_to_server(client, server, flags, cut)

      {:error, _closed} ->
        :gen_tcp.close(server)
    end
  end

  # The server committed; the client never hears it and loses the connection.
  defp server_to_client(server, client, flags, cut) do
    case :gen_tcp.recv(server, 0) do
      {:ok, data} ->
        if :atomics.get(cut, 1) == 1 and :binary.match(data, @commit_complete) != :nomatch do
          :atomics.add(flags, 2, 1)
          :gen_tcp.close(client)
          :gen_tcp.close(server)
        else
          _sent = :gen_tcp.send(client, data)
          server_to_client(server, client, flags, cut)
        end

      {:error, _closed} ->
        :gen_tcp.close(client)
    end
  end

  # A production-style pool (no sandbox) whose connections go through the proxy.
  defp start_proxied_repo!(%{port: port}) do
    repo =
      start_supervised!({Repo, name: nil, pool: DBConnection.ConnectionPool, pool_size: 2, hostname: "127.0.0.1", port: port},
        id: {:commit_cut_repo, port}
      )

    through(repo, fn -> Repo.query!("SELECT 1") end)
    repo
  end

  defp through(repo, fun) do
    previous = Repo.put_dynamic_repo(repo)

    try do
      fun.()
    after
      Repo.put_dynamic_repo(previous)
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
end
