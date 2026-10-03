defmodule CodexPooler.Accounting.AttemptFinalizationDeliveryReceiptTest do
  # A websocket socket merges its delivery receipt into the attempt row with its
  # own statement, normally after the gateway finalized the attempt. A socket
  # that closes while its turn is still settling records it first, and the
  # finalization, holding the attempt it loaded before, replaced the whole
  # metadata map: the receipt was gone and the resend admission that reads it
  # refused the released client's identical resend (findings#232, one
  # forwarding-on released-client run of five). Both orders must end with the
  # receipt on the row.
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.Gateway.Websocket.DeliveryReceipt
  alias CodexPooler.UnboxedFixture
  alias Ecto.Adapters.SQL.Sandbox

  import CodexPoolerWeb.Runtime.BackendCodexTestSupport, only: [cleanup_unboxed_pool!: 1]

  test "a receipt recorded before the attempt's finalization survives it" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    receipt = aborted_partial_receipt()

    assert :ok = DeliveryReceipt.persist(attempt.id, receipt)

    # `attempt` is the struct loaded before the receipt was merged, as the
    # settling task holds it.
    assert {:ok, _finalized} = Accounting.finalize_request(request, attempt, interrupted_attrs())

    assert %Attempt{status: "failed", response_metadata: metadata} = Repo.get!(Attempt, attempt.id)
    assert metadata["downstream_delivery"] == receipt
    assert metadata["attempt_marker"] == "finalization"
  end

  test "a receipt recorded after the attempt's finalization is merged beside its metadata" do
    %{attempt: attempt, request: request} = websocket_attempt!()
    receipt = aborted_partial_receipt()

    assert {:ok, _finalized} = Accounting.finalize_request(request, attempt, interrupted_attrs())
    assert :ok = DeliveryReceipt.persist(attempt.id, receipt)

    assert %Attempt{response_metadata: metadata} = Repo.get!(Attempt, attempt.id)
    assert metadata["downstream_delivery"] == receipt
    assert metadata["attempt_marker"] == "finalization"
  end

  for first <- [:receipt, :finalizer] do
    @tag first: first
    test "independent PostgreSQL #{first} before the other writer preserves the receipt and final metadata", %{first: first} do
      holder = String.to_atom("receipt-order-fixture-#{System.unique_integer([:positive])}")
      # The unlinked holder survives an assertion failure long enough for the
      # pre-registered exact committed-graph cleanup; it is stopped afterward.
      on_exit(fn ->
        if pid = Process.whereis(holder) do
          try do
            if fixture = Agent.get(pid, & &1) do
              UnboxedFixture.cleanup_unboxed!(fn -> cleanup_unboxed_pool!(fixture.setup) end)
            end
          after
            Agent.stop(pid)
          end
        end
      end)

      {:ok, _holder} = Agent.start(fn -> nil end, name: holder)

      fixture =
        UnboxedFixture.run_unboxed(fn ->
          {:ok, fixture} =
            Repo.transaction(fn ->
              fixture = websocket_attempt!()
              Agent.update(holder, fn _ -> fixture end)
              fixture
            end)

          fixture
        end)

      supervisor = start_supervised!(Task.Supervisor)
      parent = self()
      release = make_ref()
      receipt = aborted_partial_receipt()
      first_task = start_ordered_writer(supervisor, fixture, receipt, first, parent, release, true)
      first_monitor = Process.monitor(first_task.pid)
      assert_receive {:ordered_writer_ready, ^first, first_backend, first_pid}, 15_000
      second = if first == :receipt, do: :finalizer, else: :receipt
      second_task = start_ordered_writer(supervisor, fixture, receipt, second, parent, release, false)
      second_monitor = Process.monitor(second_task.pid)
      assert_receive {:ordered_writer_ready, ^second, second_backend, _second_pid}, 15_000
      assert first_backend != second_backend
      await_backend_blocked!(second_backend, first_backend, System.monotonic_time(:millisecond) + 15_000)
      send(first_pid, {:release_ordered_writer, release})
      assert {:ok, _result} = Task.await(first_task, 15_000)
      assert {:ok, _result} = Task.await(second_task, 15_000)
      assert_receive {:DOWN, ^first_monitor, :process, _pid, :normal}, 15_000
      assert_receive {:DOWN, ^second_monitor, :process, _pid, :normal}, 15_000
      stored = UnboxedFixture.run_unboxed(fn -> Repo.get!(Attempt, fixture.attempt.id) end)
      assert stored.status == "failed"
      assert stored.response_metadata["downstream_delivery"] == receipt
      assert stored.response_metadata["attempt_marker"] == "finalization"
    end
  end

  defp start_ordered_writer(supervisor, fixture, receipt, kind, parent, release, hold?) do
    context = %{fixture: fixture, receipt: receipt, kind: kind, parent: parent, release: release, hold?: hold?}

    Task.Supervisor.async_nolink(supervisor, fn ->
      Sandbox.unboxed_run(Repo, fn -> ordered_transaction(context) end)
    end)
  end

  defp ordered_transaction(context), do: Repo.transaction(fn -> run_ordered_statement(context) end)

  defp run_ordered_statement(context) do
    [[backend]] = Repo.query!("SELECT pg_backend_pid()").rows
    if not context.hold?, do: send(context.parent, {:ordered_writer_ready, context.kind, backend, self()})

    result =
      case context.kind do
        :receipt -> assert :ok = DeliveryReceipt.persist(context.fixture.attempt.id, context.receipt)
        :finalizer -> assert {:ok, _finalized} = Accounting.finalize_request(context.fixture.request, context.fixture.attempt, interrupted_attrs())
      end

    if context.hold? do
      send(context.parent, {:ordered_writer_ready, context.kind, backend, self()})
      await_ordered_release(context.release)
    end

    result
  end

  defp await_ordered_release(release) do
    receive do
      {:release_ordered_writer, ^release} -> :ok
    after
      15_000 -> Repo.rollback(:ordered_writer_release_missing)
    end
  end

  defp await_backend_blocked!(waiter, blocker, deadline) do
    blockers =
      UnboxedFixture.run_unboxed(fn ->
        [[blocked_by]] = Repo.query!("SELECT pg_blocking_pids($1)", [waiter]).rows
        blocked_by
      end)

    if blocker not in blockers do
      assert System.monotonic_time(:millisecond) < deadline, "independent finalizer/receipt statement never reached its actual row lock"

      receive do
      after
        5 -> await_backend_blocked!(waiter, blocker, deadline)
      end
    end
  end

  defp websocket_attempt! do
    setup = accounting_setup()

    assert {:ok, reserved} =
             Accounting.reserve(setup.auth, setup.model, %{"model" => setup.model.exposed_model_id, "input" => []}, %{
               endpoint: "/backend-api/codex/responses",
               transport: "websocket",
               correlation_id: Ecto.UUID.generate()
             })

    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment)
    %{attempt: attempt, request: reserved.request, setup: setup}
  end

  defp aborted_partial_receipt do
    DeliveryReceipt.build(%{outcome: "aborted", terminal_class: nil, pushed_at: nil, frames_after_visible: 4, transport: "websocket", highest_frame_class: "part_added"})
  end

  defp interrupted_attrs do
    %{
      status: "failed",
      response_status_code: 499,
      last_error_code: "client_disconnected",
      attempt_metadata: %{"attempt_marker" => "finalization"},
      usage: %{status: "usage_unknown", source: "unavailable"}
    }
  end
end
