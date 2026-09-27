defmodule CodexPooler.Accounting.PermanentUpstreamDeletionTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountingTestSupport
  import CodexPooler.RequestReplayFixtures, only: [replay_fixture: 1, arm_input: 1, consume_input: 3]

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestReplay, RequestReplayEntitlement}
  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Repo

  test "a stale candidate cannot insert an attempt after permanent deletion is requested" do
    fixture = accounting_setup()
    reserved = reserve!(fixture)
    mark_deleting!(fixture.identity)

    assert {:error, %{code: :upstream_account_deleting}} =
             Accounting.create_attempt(reserved.request, fixture.assignment)

    refute Repo.exists?(from row in Attempt, where: row.request_id == ^reserved.request.id)
    assert Repo.get!(Request, reserved.request.id).status == reserved.request.status
  end

  test "an already admitted attempt can settle after the permanent deletion fence" do
    fixture = accounting_setup()
    reserved = reserve!(fixture)
    assert {:ok, attempt} = Accounting.create_attempt(reserved.request, fixture.assignment)
    mark_deleting!(fixture.identity)

    assert {:ok, _} =
             Accounting.finalize_success(reserved.request, attempt, %{status: "usage_known", input_tokens: 1, output_tokens: 1, total_tokens: 2}, %{response_status_code: 200})

    assert Repo.get!(Attempt, attempt.id).status == "succeeded"
    assert Repo.exists?(from row in LedgerEntry, where: row.request_id == ^reserved.request.id and row.entry_kind == "settlement")
  end

  test "deleting an assignment retains attempts, sessions, turns, replay and ledger history" do
    fixture = replay_fixture(reservation?: true)
    assert {:ok, _} = RequestReplay.arm(arm_input(fixture))
    fixture.session |> Ecto.Changeset.change(pool_upstream_assignment_id: fixture.assignment.id) |> Repo.update!()
    entitlement = Repo.one!(from row in RequestReplayEntitlement, where: row.request_id == ^fixture.request.id)
    ledger_ids = Repo.all(from row in LedgerEntry, where: row.request_id == ^fixture.request.id, select: row.id)
    assert ledger_ids != []

    Repo.delete!(fixture.assignment)

    assert Repo.get!(Attempt, fixture.attempt.id).pool_upstream_assignment_id == nil
    assert Repo.get!(CodexSession, fixture.session.id).pool_upstream_assignment_id == nil
    assert Repo.get!(CodexTurn, fixture.turn.id).request_id == fixture.request.id
    assert Repo.get!(RequestReplayEntitlement, entitlement.id).request_id == fixture.request.id
    assert Repo.get!(Request, fixture.request.id).id == fixture.request.id
    assert Repo.all(from row in LedgerEntry, where: row.request_id == ^fixture.request.id, select: row.id) == ledger_ids
  end

  test "a reserved replay cannot insert its next generation after permanent deletion is requested" do
    fixture = replay_fixture(reservation?: true)
    assert {:ok, armed} = RequestReplay.arm(arm_input(fixture))
    input = consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
    mark_deleting!(fixture.identity)
    request = Repo.reload!(fixture.request)
    attempt = Repo.reload!(fixture.attempt)
    entitlement = Repo.one!(from row in RequestReplayEntitlement, where: row.request_id == ^fixture.request.id)

    assert {:error, :upstream_account_deleting} = RequestReplay.consume(input)

    assert Repo.reload!(request) == request
    assert Repo.reload!(attempt) == attempt
    assert Repo.reload!(entitlement) == entitlement
    assert Repo.aggregate(from(row in Attempt, where: row.request_id == ^request.id), :count) == 1
  end

  defp reserve!(fixture) do
    {:ok, reserved} = Accounting.reserve(fixture.auth, fixture.model, %{"model" => fixture.model.exposed_model_id}, %{correlation_id: Ecto.UUID.generate()})
    reserved
  end

  defp mark_deleting!(identity) do
    identity
    |> Ecto.Changeset.change(status: "deleted", metadata: Map.put(identity.metadata || %{}, "permanent_deletion_requested_at", DateTime.to_iso8601(DateTime.utc_now())))
    |> Repo.update!()
  end
end
