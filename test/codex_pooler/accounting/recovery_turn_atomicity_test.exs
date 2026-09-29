defmodule CodexPooler.Accounting.RecoveryTurnAtomicityTest do
  # The recovery writers that settle a request carrying a codex turn outside
  # the gateway's settlement complete that turn inside the request's own
  # settlement transaction (findings#288): the six-hour stale-reservation sweep,
  # for a request that never dispatched (its reservation released) and for one
  # that did (settled), and absent-instance recovery of an attempt whose owner
  # recorded no execution identity. Each used to commit the request first and
  # interrupt its turn in a statement of its own, so a reader between the two
  # met a terminal request behind an open turn.
  #
  # The recovering process is held right after the commit that wrote the
  # request's terminal status: its turn is already interrupted, in that same
  # commit. Rows committed in the shared sandbox; the recovery runs in its own
  # process so the test can read while it is held.
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.Request
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.SettlementTransactionHold

  @detection_timeout_ms 15_000

  setup do
    boot_id = Identity.boot_id()
    on_exit(fn -> :persistent_term.put({Identity, :boot_id}, boot_id) end)
    :ok
  end

  describe "the stale-reservation sweep" do
    test "releases an undispatched request and interrupts its turn in one commit" do
      setup = accounting_setup()
      now = now()
      stale = DateTime.add(now, -7, :hour)
      %{request: request, turn: turn} = open_turn!(setup, stale, dispatch?: false)

      held = held_recovery!(request, fn -> Accounting.recover_stale_reservations(now) end)

      assert held.recovery == {:ok, %{stale_reservations_released: 1, stale_reservations_settled: 0, stale_turn_claims_recovered: 0, stale_terminal_attempts_recovered: 0}}
      assert held.at_hold == {"failed", "interrupted"}
      assert held.turn_in_commit? == true
      assert %CodexTurn{status: "interrupted", error_code: "stale_reservation_recovered"} = Repo.reload!(turn)
    end

    test "settles a dispatched request and interrupts its turn in one commit" do
      setup = accounting_setup()
      now = now()
      stale = DateTime.add(now, -7, :hour)
      %{request: request, attempt: attempt, turn: turn} = open_turn!(setup, stale, dispatch?: true)

      held = held_recovery!(request, fn -> Accounting.recover_stale_reservations(now) end)

      assert held.recovery == {:ok, %{stale_reservations_released: 0, stale_reservations_settled: 1, stale_turn_claims_recovered: 0, stale_terminal_attempts_recovered: 0}}
      assert held.at_hold == {"failed", "interrupted"}
      assert held.turn_in_commit? == true
      assert %CodexTurn{status: "interrupted", error_code: "stale_reservation_recovered", final_attempt_id: final_attempt_id} = Repo.reload!(turn)
      assert final_attempt_id == attempt.id
    end
  end

  describe "absent-instance recovery" do
    test "recovers an attempt with no execution identity and interrupts its turn in one commit" do
      setup = accounting_setup()
      now = now()
      stale = DateTime.add(now, -180, :second)
      owner = Identity.new("sample-owner@remote", Ecto.UUID.generate())
      {:ok, _} = InstancePresence.record_heartbeat(owner, stale)
      {:ok, _} = InstancePresence.record_heartbeat()

      %{request: request, turn: turn} =
        open_turn!(setup, stale,
          dispatch?: true,
          attempt: %{owner_instance_id: owner.node_name, owner_instance_boot_id: owner.boot_id, owner_process_id: nil, owner_execution_id: nil}
        )

      held = held_recovery!(request, fn -> Accounting.recover_absent_instance_attempts(now) end)

      assert held.recovery == {:ok, %{absent_instance_attempts_recovered: 1}}
      assert held.at_hold == {"failed", "interrupted"}
      assert held.turn_in_commit? == true
      assert %CodexTurn{status: "interrupted", error_code: "absent_instance_recovered"} = Repo.reload!(turn)
    end
  end

  # Runs `recover` in its own process, holds it right after the commit that
  # settled `request`, and reads the request and its turn there.
  defp held_recovery!(%Request{id: request_id}, recover) do
    hold = SettlementTransactionHold.after_commit!(request_id)
    task = Task.async(recover)
    {settler, %{turn_in_commit?: turn_in_commit?}} = SettlementTransactionHold.await_held!(hold)
    at_hold = {Repo.get!(Request, request_id).status, Repo.get_by!(CodexTurn, request_id: request_id).status}
    :ok = SettlementTransactionHold.release(hold, settler)

    %{recovery: Task.await(task, @detection_timeout_ms), at_hold: at_hold, turn_in_commit?: turn_in_commit?}
  end

  # A reserved request with a turn on a session that carries no owner lease,
  # so the recovery passes may reach it; dispatched, it has one attempt.
  defp open_turn!(setup, at, opts) do
    {:ok, reserved} =
      Accounting.reserve(
        setup.auth,
        setup.model,
        %{"model" => setup.model.exposed_model_id, "stream" => true, "max_output_tokens" => 10},
        %{correlation_id: "corr-recovery-turn-#{System.unique_integer([:positive])}", now: at, transport: "http_sse"}
      )

    attempt =
      if Keyword.fetch!(opts, :dispatch?) do
        {:ok, attempt} = Accounting.create_attempt(reserved.request, setup.assignment, Map.put(Keyword.get(opts, :attempt, %{}), :now, at))
        attempt
      end

    session =
      %CodexSession{
        pool_id: setup.pool.id,
        api_key_id: setup.api_key.id,
        session_key: "session-#{System.unique_integer([:positive])}",
        pool_upstream_assignment_id: setup.assignment.id,
        status: "active",
        created_at: at,
        updated_at: at
      }
      |> Repo.insert!()

    turn =
      %CodexTurn{
        codex_session_id: session.id,
        request_id: reserved.request.id,
        turn_sequence: 1,
        transport_kind: "http_sse",
        status: "in_progress",
        started_at: at,
        created_at: at,
        updated_at: at
      }
      |> Repo.insert!()

    %{request: reserved.request, attempt: attempt, turn: turn}
  end

  defp now, do: DateTime.utc_now() |> DateTime.truncate(:microsecond)
end
