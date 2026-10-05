defmodule CodexPoolerWeb.WebsocketDownstreamWriteWatchTest do
  # The failed-write signal behind the websocket delivery receipt (findings#232
  # row 232-256): ThousandIsland reports a failed write as
  # `[:thousand_island, :connection, :send_error]` in the connection process,
  # and the application-attached handler keeps the first failure of a watched
  # process only. Each case runs in its own process, as a connection does.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPoolerWeb.WebsocketDownstreamWriteWatch

  @event [:thousand_island, :connection, :send_error]
  @sent_event [:thousand_island, :connection, :send]
  @port_exit_budget_ms 15_000

  test "the application attaches the handler" do
    assert Enum.any?(:telemetry.list_handlers(@event), &(&1.id == {WebsocketDownstreamWriteWatch, :send_error}))
  end

  test "the application attaches the send handler that observes the driver queue" do
    assert Enum.any?(:telemetry.list_handlers(@sent_event), &(&1.id == {WebsocketDownstreamWriteWatch, :send}))
  end

  test "a process that is not watched records nothing" do
    assert in_process(fn ->
             send_error(:timeout)
             WebsocketDownstreamWriteWatch.failure()
           end) == nil
  end

  test "a watched connection keeps the class of its first failed write" do
    assert in_process(fn ->
             :ok = WebsocketDownstreamWriteWatch.watch()
             send_error(:timeout)
             send_error(:closed)
             WebsocketDownstreamWriteWatch.failure()
           end) == "timeout"

    assert in_process(fn ->
             :ok = WebsocketDownstreamWriteWatch.watch()
             send_error(:closed)
             WebsocketDownstreamWriteWatch.failure()
           end) == "closed"

    assert in_process(fn ->
             :ok = WebsocketDownstreamWriteWatch.watch()
             send_error(:econnreset)
             WebsocketDownstreamWriteWatch.failure()
           end) == "other"
  end

  # The client-retry window of a turn cut by the failure starts when it was
  # reported (findings#232 row 232-261), so a later failure must not move it.
  test "a watched connection keeps when its first failed write was reported" do
    {before_failure, failed_at, after_second} =
      in_process(fn ->
        :ok = WebsocketDownstreamWriteWatch.watch()
        nil = WebsocketDownstreamWriteWatch.failed_at()
        before_failure = DateTime.utc_now()
        send_error(:timeout)
        failed_at = WebsocketDownstreamWriteWatch.failed_at()
        Process.sleep(2)
        send_error(:closed)
        {before_failure, failed_at, WebsocketDownstreamWriteWatch.failed_at()}
      end)

    assert %DateTime{} = failed_at
    assert DateTime.compare(failed_at, before_failure) in [:eq, :gt]
    assert after_second == failed_at

    assert in_process(fn ->
             send_error(:timeout)
             WebsocketDownstreamWriteWatch.failed_at()
           end) == nil
  end

  test "every class it records is in the receipt's write_failure vocabulary" do
    assert WebsocketDownstreamWriteWatch.failures() == DeliveryReceipt.write_failures()

    for error <- [:timeout, :closed, :enotconn, :epipe] do
      assert in_process(fn ->
               :ok = WebsocketDownstreamWriteWatch.watch()
               send_error(error)
               WebsocketDownstreamWriteWatch.failure()
             end) in DeliveryReceipt.write_failures()
    end
  end

  test "confirmed evidence stops moving at the first failed write" do
    assert in_process(fn ->
             :ok = WebsocketDownstreamWriteWatch.watch()
             :ok = WebsocketDownstreamWriteWatch.confirm(:before)
             send_error(:timeout)
             :ok = WebsocketDownstreamWriteWatch.confirm(:after)
             WebsocketDownstreamWriteWatch.confirmed()
           end) == :before
  end

  test "a failed write's frame data never reaches the recorded failure" do
    assert in_process(fn ->
             :ok = WebsocketDownstreamWriteWatch.watch()
             :telemetry.execute(@event, %{data: "synthetic frame bytes", error: {:synthetic, "synthetic frame bytes"}}, %{})
             WebsocketDownstreamWriteWatch.failure()
           end) == "other"
  end

  # The released client closes right after reading a content-filter terminal and the connection's port exits with that close (`exit_on_close`), where its driver queue can no longer be read: a socket confirms what it pushed at its next callback or at `terminate/2`, and either can run after the close (findings#303 row 303-4). What the latest write left in the queue is what the watch remembers for it.
  describe "a connection whose port exited with the client's close" do
    test "confirms evidence when the latest write left the driver queue empty" do
      assert with_connection(fn server, client ->
               :ok = :gen_tcp.send(server, "synthetic frame bytes")
               sent()
               close_client_and_await_port_exit!(server, client)
               :ok = WebsocketDownstreamWriteWatch.confirm(:written)
               WebsocketDownstreamWriteWatch.confirmed()
             end) == :written
    end

    test "confirms nothing when no write was ever observed" do
      assert with_connection(fn server, client ->
               close_client_and_await_port_exit!(server, client)
               :ok = WebsocketDownstreamWriteWatch.confirm(:written)
               WebsocketDownstreamWriteWatch.confirmed()
             end) == nil
    end

    test "confirms nothing when the latest write left the driver queue non-empty" do
      assert with_connection([sndbuf: 4_096, high_watermark: 64 * 1024 * 1024], fn server, client ->
               :ok = :gen_tcp.send(server, :binary.copy("x", 16 * 1024 * 1024))
               assert {:queue_size, queued} = :erlang.port_info(server, :queue_size)
               assert queued > 0
               sent()
               close_client_and_await_port_exit!(server, client)
               :ok = WebsocketDownstreamWriteWatch.confirm(:written)
               WebsocketDownstreamWriteWatch.confirmed()
             end) == nil
    end

    test "confirms nothing after a failed write, whatever the queue held" do
      assert with_connection(fn server, client ->
               :ok = :gen_tcp.send(server, "synthetic frame bytes")
               sent()
               :ok = WebsocketDownstreamWriteWatch.confirm(:before)
               send_error(:timeout)
               close_client_and_await_port_exit!(server, client)
               :ok = WebsocketDownstreamWriteWatch.confirm(:after)
               WebsocketDownstreamWriteWatch.confirmed()
             end) == :before
    end

    # The `send` event runs after the write returned, so the port can exit before it reads the queue: the latest frame is then not counted, whatever an earlier write left.
    test "confirms nothing when the port exited before the latest write's queue was read" do
      assert with_connection(fn server, client ->
               :ok = :gen_tcp.send(server, "synthetic frame one")
               sent()
               :ok = :gen_tcp.send(server, "synthetic frame two")
               close_client_and_await_port_exit!(server, client)
               sent()
               :ok = WebsocketDownstreamWriteWatch.confirm(:written)
               WebsocketDownstreamWriteWatch.confirmed()
             end) == nil
    end

    test "confirms nothing when the latest write was still queued as the port exited, after an earlier write reached the kernel" do
      assert with_connection([sndbuf: 4_096, high_watermark: 64 * 1024 * 1024], fn server, client ->
               :ok = :gen_tcp.send(server, "synthetic frame one")
               sent()
               :ok = :gen_tcp.send(server, :binary.copy("x", 16 * 1024 * 1024))
               assert {:queue_size, queued} = :erlang.port_info(server, :queue_size)
               assert queued > 0
               close_client_and_await_port_exit!(server, client)
               sent()
               :ok = WebsocketDownstreamWriteWatch.confirm(:written)
               WebsocketDownstreamWriteWatch.confirmed()
             end) == nil
    end
  end

  # Bandit writes the upgrade's 101 in the same process before `CodexResponsesSocket.init/1` watches it.
  test "a write before the process is watched leaves nothing a closed port could confirm" do
    assert with_connection([watch?: false], fn server, client ->
             :ok = :gen_tcp.send(server, "synthetic upgrade response")
             sent()
             :ok = WebsocketDownstreamWriteWatch.watch()
             close_client_and_await_port_exit!(server, client)
             :ok = WebsocketDownstreamWriteWatch.confirm(:written)
             WebsocketDownstreamWriteWatch.confirmed()
           end) == nil
  end

  defp send_error(reason), do: :telemetry.execute(@event, %{data: "synthetic frame bytes", error: reason, monotonic_time: 0}, %{})

  defp in_process(fun) do
    task = Task.async(fun)
    Task.await(task, 2 * @port_exit_budget_ms)
  end

  # ThousandIsland emits this in the connection process after every successful write.
  defp sent, do: :telemetry.execute(@sent_event, %{data: "synthetic frame bytes"}, %{})

  # The watched process owns exactly one port, the accepted one, as a Bandit connection does (the watch reads the first `tcp_inet` port among its links); the listener and the client belong to the test process.
  defp with_connection(opts \\ [], fun) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, active: false] ++ Keyword.take(opts, [:sndbuf, :high_watermark]))
    {:ok, listen_port} = :inet.port(listener)
    {:ok, client} = :gen_tcp.connect({127, 0, 0, 1}, listen_port, [:binary, active: false])

    try do
      in_process(fn ->
        {:ok, server} = :gen_tcp.accept(listener, @port_exit_budget_ms)
        # A closed peer is noticed, and the port exits with it, only while the port is armed.
        :ok = :inet.setopts(server, active: :once)
        if Keyword.get(opts, :watch?, true), do: :ok = WebsocketDownstreamWriteWatch.watch()
        fun.(server, client)
      end)
    after
      :gen_tcp.close(client)
      :gen_tcp.close(listener)
    end
  end

  defp close_client_and_await_port_exit!(server, client) do
    monitor = Port.monitor(server)
    :ok = :gen_tcp.close(client)

    receive do
      {:DOWN, ^monitor, :port, ^server, _reason} -> :ok
    after
      @port_exit_budget_ms -> flunk("the server port did not exit with the client's close")
    end
  end
end
