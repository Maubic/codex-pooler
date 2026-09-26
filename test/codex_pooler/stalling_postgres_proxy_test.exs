defmodule CodexPooler.StallingPostgresProxyTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.StallingPostgresProxy

  @detection_timeout_ms 15_000

  test "teardown stops the frozen proxy and its PostgreSQL backend without an in-body stop" do
    # This holder deliberately outlives the test and its supervised clients.
    {:ok, holder} = Agent.start(fn -> nil end)

    on_exit(fn ->
      try do
        assert_proxy_stopped!(Agent.get(holder, & &1))
      after
        Agent.stop(holder)
      end
    end)

    proxy = StallingPostgresProxy.start!()
    connection = start_supervised!({Postgrex, Keyword.merge(postgres_options(), hostname: "127.0.0.1", port: proxy.port)})
    %{rows: [[backend]]} = Postgrex.query!(connection, "SELECT pg_backend_pid()", [])
    Agent.update(holder, fn _ -> {proxy, backend, []} end)
    :ok = StallingPostgresProxy.stall!(proxy)
    relays = await_frozen_relays!(proxy.acceptor, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    Agent.update(holder, fn _ -> {proxy, backend, relays} end)
  end

  defp assert_proxy_stopped!(nil), do: :ok

  defp assert_proxy_stopped!({proxy, backend, relays}) do
    assert_down!(proxy.acceptor)
    Enum.each(relays, &assert_down!/1)
    assert_backend_absent!(backend)
    # The freed port may already belong to another partition's listener.
    assert {:error, :einval} = :inet.sockname(proxy.listen)
  after
    StallingPostgresProxy.stop!(proxy)
    Enum.each(relays, &Process.exit(&1, :kill))
  end

  defp await_frozen_relays!(acceptor, deadline) do
    {:links, links} = Process.info(acceptor, :links)
    frozen = Enum.filter(links, &(is_pid(&1) and Process.info(&1, :current_function) == {:current_function, {Process, :sleep, 1}}))

    case frozen do
      [_, _] ->
        frozen

      _ ->
        assert System.monotonic_time(:millisecond) < deadline, "both proxy relays did not freeze"

        receive do
        after
          10 -> await_frozen_relays!(acceptor, deadline)
        end
    end
  end

  defp assert_down!(pid) do
    ref = Process.monitor(pid)
    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, @detection_timeout_ms
  end

  defp assert_backend_absent!(backend) do
    connection = start_unboxed_connection!()

    try do
      await_backend_absent!(connection, backend, System.monotonic_time(:millisecond) + @detection_timeout_ms)
    after
      GenServer.stop(connection)
    end
  end

  defp postgres_options, do: Keyword.take(Repo.config(), [:hostname, :port, :database, :username, :password])

  defp start_unboxed_connection! do
    {:ok, connection} = Postgrex.start_link(postgres_options())
    connection
  end

  defp await_backend_absent!(connection, backend, deadline) do
    %{rows: [[present]]} = Postgrex.query!(connection, "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)", [backend])

    if present do
      assert System.monotonic_time(:millisecond) < deadline, "the proxy PostgreSQL backend survived teardown"

      receive do
      after
        10 -> await_backend_absent!(connection, backend, deadline)
      end
    end
  end
end
