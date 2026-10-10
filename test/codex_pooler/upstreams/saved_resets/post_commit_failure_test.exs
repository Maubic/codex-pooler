defmodule CodexPooler.Upstreams.SavedResets.PostCommitFailureTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.FakeUpstream
  alias CodexPooler.SavedResetPostCommitFailureSupport, as: Support
  alias CodexPooler.Upstreams.SavedResetRedemption

  @moduletag capture_log: true

  setup context, do: %{support: Support.start!(context)}

  for mode <- [:none, :publication] do
    test "committed applied result survives #{mode} publication", %{support: support} do
      fixture = Support.fixture!(support)
      :ok = Support.arm!(fixture, unquote(mode))
      result = SavedResetRedemption.redeem(fixture.assignment)
      Support.restore!(fixture)
      receipt = Support.receipt!(fixture)
      Support.emit(fixture, "publication_#{unquote(mode)}")
      assert receipt.phase == "consumed_pending_probe"
      assert FakeUpstream.physical_counts(fixture.fake).consume == 1
      assert {:ok, %{applied?: true, phase: "consumed_pending_probe"}} = result
    end
  end

  test "failed finalization before commit remains ambiguous", %{support: support} do
    fixture = Support.fixture!(support)
    :ok = Support.arm!(fixture, :precommit)
    result = SavedResetRedemption.redeem(fixture.assignment)
    Support.restore!(fixture)
    Support.assert_no_applied_witness!(fixture)
    assert {:error, :saved_reset_consume_outcome_ambiguous} = result
    redemption = Repo.reload!(fixture.identity).metadata["saved_reset_redemption"]
    assert redemption["phase"] == "consuming"
    assert redemption["result"] == nil
    assert redemption["provider_replay"]["provider_dispatches"] == 1
    assert redemption["provider_replay"]["last_code"] == "persistence_failed"
    assert FakeUpstream.physical_counts(fixture.fake).consume == 1
  end

  test "publication failure inside the caller transaction cannot claim a durable applied commit", %{support: support} do
    fixture = Support.fixture!(support)
    :ok = Support.arm!(fixture, :nested_publication)

    result =
      Repo.transaction(fn ->
        outcome = SavedResetRedemption.redeem(fixture.assignment)
        Repo.rollback(outcome)
      end)

    Support.restore!(fixture)
    Support.assert_no_applied_witness!(fixture)
    assert {:error, {:error, :saved_reset_consume_outcome_ambiguous}} = result
    assert Repo.reload!(fixture.identity).metadata["saved_reset_redemption"] == nil
    assert FakeUpstream.physical_counts(fixture.fake).consume == 1
  end

  for code <- ["no_credit", "nothing_to_reset"] do
    test "provider #{code} stays not applied", %{support: support} do
      fixture = Support.fixture!(support, response_code: unquote(code))
      :ok = Support.arm!(fixture, :none)
      assert {:ok, %{applied?: false, code: code}} = SavedResetRedemption.redeem(fixture.assignment)
      Support.restore!(fixture)
      Support.assert_no_applied_witness!(fixture)
      assert code == unquote(code)
      assert FakeUpstream.physical_counts(fixture.fake).consume == 1
    end
  end

  test "an unknown provider response stays ambiguous without an applied witness", %{support: support} do
    fixture = Support.fixture!(support, response_code: "unrecognized_result")
    :ok = Support.arm!(fixture, :none)
    assert {:error, :saved_reset_consume_outcome_ambiguous} = SavedResetRedemption.redeem(fixture.assignment)
    Support.restore!(fixture)
    Support.assert_no_applied_witness!(fixture)
    redemption = Repo.reload!(fixture.identity).metadata["saved_reset_redemption"]
    assert redemption["phase"] == "consuming"
    assert redemption["result"] == nil
    assert redemption["provider_replay"]["provider_dispatches"] == 1
    assert FakeUpstream.physical_counts(fixture.fake).consume == 1
  end
end
