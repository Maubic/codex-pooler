defmodule CodexPooler.UnboxedFixtureContractTest do
  use CodexPooler.DataCase, async: false
  import ExUnit.CaptureLog
  alias CodexPooler.Pools.Pool
  alias CodexPooler.UnboxedFixture
  alias CodexPoolerWeb.WebsocketControlPath

  test "failed cleanup barrier retains committed rows until their owned writer has stopped" do
    id = Ecto.UUID.generate()
    UnboxedFixture.register_unboxed_cleanup!(fn -> Repo.delete_all(from p in Pool, where: p.id == ^id) end)
    pool = UnboxedFixture.run_unboxed(fn -> Repo.insert!(%Pool{id: id, name: "Owned cleanup sample", slug: "owned-#{id}", status: "archived", created_at: DateTime.utc_now(), updated_at: DateTime.utc_now()}) end)
    parent = self()

    log =
      capture_log(fn ->
        WebsocketControlPath.cleanup(fn ->
          send(parent, {:held_writer, self()})

          receive do
            :release -> :ok
          end
        end)
      end)

    assert log =~ "cleanup_deferred"
    assert_receive {:held_writer, writer}
    monitor = Process.monitor(writer)
    on_exit(fn -> if Process.alive?(writer), do: send(writer, :release) end)
    delete = fn -> Repo.delete!(pool) end

    assert_raise ExUnit.AssertionError, ~r/cleanup.*still running/, fn ->
      UnboxedFixture.cleanup_unboxed!(delete, 1_000, cleanup_wait: 0)
    end

    assert Process.alive?(writer)
    assert UnboxedFixture.run_unboxed(fn -> Repo.get(Pool, id) end)
    send(writer, :release)
    assert_receive {:DOWN, ^monitor, :process, ^writer, :normal}
    assert %Pool{id: ^id} = UnboxedFixture.cleanup_unboxed!(delete, 1_000, cleanup_wait: 100)
    refute UnboxedFixture.run_unboxed(fn -> Repo.get(Pool, id) end)
  end
end
