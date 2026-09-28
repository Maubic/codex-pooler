defmodule CodexPooler.Gateway.Runtime.ReplayPreparationRetirementTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.RequestReplayFixtures

  alias CodexPooler.Accounting.{RequestReplay, RequestReplayEntitlement}
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Dispatch.ReplayPreparation
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Repo
  alias Ecto.Adapters.SQL.Sandbox

  test "only canonical false v1 preparation is eligible; sanitization retains historical true" do
    metadata = replay_preparation_metadata()
    assert ReplayPreparation.replay_eligible?(metadata)
    historical = put_in(metadata, ["native_replay_preparation", "request_compression_enabled"], true)
    assert ReplayPreparation.sanitize(historical["native_replay_preparation"])["request_compression_enabled"] == true

    for invalid <- invalid_preparations() ++ [nil, [], "invalid"] do
      refute ReplayPreparation.replay_eligible?(invalid)
      if is_map(invalid), do: assert({:error, :invalid_replay_preparation} = ReplayPreparation.restore(RequestOptions.for_websocket(%{}), invalid))
    end
  end

  test "invalid original preparations refuse preflight and direct consume before owner redemption and settle once on expiry" do
    for invalid <- invalid_preparations() do
      set_replay_db_now!(DateTime.utc_now())
      fixture = replay_fixture(reservation?: true)
      assert {:ok, armed} = RequestReplay.arm(arm_input(fixture))
      input = consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
      {:ok, owner} = WebsocketOwnerSession.lookup(fixture.session.id)
      set_preparation(fixture.attempt, invalid)
      before_counts = counts()
      assert {:error, :invalid_replay_preparation} = RequestReplay.preflight_snapshot(fixture.preflight)
      assert {:error, :invalid_replay_preparation} = RequestReplay.consume(input)
      assert counts() == before_counts
      refute :sys.get_state(owner).suspended_replay.reserve_receipt_used?
      entitlement = Repo.get_by!(RequestReplayEntitlement, request_id: fixture.request.id)
      assert entitlement.status == "armed"
      assert is_nil(entitlement.replay_attempt_id)
      assert terminal_ledger_count(fixture.request.id, "reservation") == 1
      assert terminal_ledger_count(fixture.request.id, "settlement") == 0
      assert terminal_ledger_count(fixture.request.id, "release") == 0
      set_replay_db_now!(DateTime.add(entitlement.expires_at, 1, :second))
      assert {:ok, %{replay_entitlements_closed: 1}} = RequestReplay.cleanup_due()
      assert {:ok, %{replay_entitlements_closed: 0}} = RequestReplay.cleanup_due()
      assert terminal_ledger_count(fixture.request.id, "settlement") == 1
      assert terminal_ledger_count(fixture.request.id, "release") == 1
      stop_replay_owner(fixture.session.id)
    end
  end

  test "direct consume binds preparation to the original request and does not read a later generation" do
    fixture = replay_fixture()
    other = replay_fixture()
    assert {:ok, armed} = RequestReplay.arm(arm_input(fixture))
    input = consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
    assert {:error, :invalid_replay_preparation} = RequestReplay.consume(%{input | eligible_attempt_id: other.attempt.id})
    assert {:ok, consumed} = RequestReplay.consume(input)
    assert consumed.attempt.replay_generation == 1
    assert {:error, reason} = RequestReplay.consume(input)
    refute reason == :invalid_replay_preparation
  end

  test "preparation invalidated after reserve redemption is rejected under the attempt lock and releases the hold" do
    fixture = replay_fixture(reservation?: true)
    assert {:ok, armed} = RequestReplay.arm(arm_input(fixture))
    input = consume_input(fixture, armed, :crypto.strong_rand_bytes(32))
    {:ok, owner} = WebsocketOwnerSession.lookup(fixture.session.id)
    barrier = make_ref()
    CodexPooler.TestAppEnv.restore_on_exit(:request_replay_consume_test_barrier)
    Application.put_env(:codex_pooler, :request_replay_consume_test_barrier, {self(), barrier})
    parent = self()

    task =
      Task.async(fn ->
        Sandbox.allow(Repo, parent, self())
        RequestReplay.consume(input)
      end)

    assert_receive {:request_replay_owner_reserve_redeemed, consumer, ^barrier}
    assert :sys.get_state(owner).suspended_replay.reserve_receipt_used?
    set_preparation(fixture.attempt, %{})
    send(consumer, {:release_request_replay_consume, barrier})
    assert {:error, :invalid_replay_preparation} = Task.await(task, 15_000)
    assert request_attempt_count(fixture.request.id) == 1
    assert Repo.get_by!(RequestReplayEntitlement, request_id: fixture.request.id).status == "armed"
    assert terminal_ledger_count(fixture.request.id, "settlement") == 0
    assert :sys.get_state(owner).suspended_replay.consume_fence == nil
    assert :sys.get_state(owner).suspended_replay.consume_pid == nil
  end

  defp invalid_preparations do
    metadata = replay_preparation_metadata()

    [
      %{},
      %{"native_replay_preparation" => nil},
      put_in(metadata, ["native_replay_preparation", "request_compression_enabled"], true),
      put_in(metadata, ["native_replay_preparation", "request_compression_enabled"], "false"),
      put_in(metadata, ["native_replay_preparation", "version"], 2),
      update_in(metadata, ["native_replay_preparation"], &Map.delete(&1, "request_compression_enabled"))
    ]
  end

  defp set_preparation(attempt, metadata) do
    attempt |> Ecto.Changeset.change(response_metadata: metadata) |> Repo.update!()
  end
end
