defmodule CodexPooler.Upstreams.PermanentDeletionAssignmentRaceTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import CodexPooler.PoolerFixtures
  import CodexPooler.UnboxedFixture, only: [register_unboxed_cleanup!: 1, run_unboxed: 1]
  import Ecto.Query

  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Lifecycle.IdentitySlotLock
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias Ecto.Adapters.SQL.Sandbox

  @budget 10_000

  for operation <- [:create, :activate] do
    test "#{operation} waits for deletion identity lock and rejects the committed marker" do
      suffix = Ecto.UUID.generate()
      slug = "assignment-race-#{suffix}"
      label = "Synthetic assignment race #{suffix}"

      register_unboxed_cleanup!(fn ->
        Repo.delete_all(from row in UpstreamIdentity, where: row.account_label == ^label)
        Repo.delete_all(from row in Pool, where: row.slug == ^slug)
      end)

      fixture =
        run_unboxed(fn ->
          pool = pool_fixture(%{slug: slug})
          fixture = upstream_assignment_fixture(pool, %{account_label: label, assignment_status: "deleted"})
          Map.put(fixture, :pool, pool)
        end)

      parent = self()
      barrier = make_ref()

      holder =
        Task.async(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            Repo.transaction(fn ->
              IdentitySlotLock.lock_identity_rows!([fixture.identity.id])
              fixture.identity |> Ecto.Changeset.change(status: "deleted", metadata: %{"permanent_deletion_requested_at" => DateTime.to_iso8601(DateTime.utc_now())}) |> Repo.update!()
              send(parent, {barrier, :holder, backend_pid!()})

              receive do
                {^barrier, :release} -> :ok
              after
                @budget -> raise "identity lock release missing"
              end
            end)
          end)
        end)

      holder_monitor = Process.monitor(holder.pid)
      stop_task_on_exit(holder.pid)
      assert_receive {^barrier, :holder, holder_pid}, @budget

      waiter =
        Task.async(fn ->
          Sandbox.unboxed_run(Repo, fn ->
            Repo.transact(fn ->
              send(parent, {barrier, :waiter, backend_pid!()})

              case unquote(operation) do
                :create -> PoolAssignments.create_pool_assignment(fixture.pool, fixture.identity)
                :activate -> PoolAssignments.activate_pool_assignment(fixture.assignment)
              end
            end)
          end)
        end)

      waiter_monitor = Process.monitor(waiter.pid)
      stop_task_on_exit(waiter.pid)
      assert_receive {^barrier, :waiter, waiter_pid}, @budget
      assert waiter_pid != holder_pid
      assert_waiting!(waiter_pid, holder_pid, System.monotonic_time(:millisecond) + @budget)
      send(holder.pid, {barrier, :release})
      assert {:ok, :ok} = Task.await(holder, @budget)
      assert {:error, %{code: :upstream_account_deleting}} = Task.await(waiter, @budget)
      assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @budget
      assert_receive {:DOWN, ^waiter_monitor, :process, _, :normal}, @budget

      run_unboxed(fn ->
        assert Repo.reload!(fixture.assignment) == fixture.assignment
        assert Repo.aggregate(from(row in PoolUpstreamAssignment, where: row.upstream_identity_id == ^fixture.identity.id), :count) == 1
      end)
    end
  end

  defp backend_pid! do
    %{rows: [[pid]]} = Repo.query!("SELECT pg_backend_pid()")
    pid
  end

  defp stop_task_on_exit(pid) do
    on_exit(fn ->
      monitor = Process.monitor(pid)
      if Process.alive?(pid), do: Process.exit(pid, :kill)
      assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, @budget
    end)
  end

  defp assert_waiting!(waiter, holder, deadline) do
    blocked? =
      run_unboxed(fn ->
        %{rows: [[blocked?]]} = Repo.query!("SELECT $1 = ANY(pg_blocking_pids($2))", [holder, waiter])
        blocked?
      end)

    cond do
      blocked? ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          1 -> assert_waiting!(waiter, holder, deadline)
        end

      true ->
        flunk("assignment writer did not wait for deletion identity lock")
    end
  end
end
