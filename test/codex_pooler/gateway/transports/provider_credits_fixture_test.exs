defmodule CodexPooler.Gateway.Transports.ProviderCreditsFixtureTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import Ecto.Query

  alias CodexPooler.{FakeUpstream, PeerRegistry, ProviderCreditsFixtures, Repo, UnboxedFixture}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Quotas.CapacityFacts
  alias CodexPooler.Quotas.Evidence.CodexParsers
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity

  @budget 15_000
  @moduletag capture_log: true

  @tag slow: "boots an owned peer with real PostgreSQL and exercises physical Req and Mint sockets"
  test "physical generation receipts distinguish one HTTP send and one websocket frame from consume and usage" do
    completed = CodexPooler.JSON.encode!(%{"type" => "response.completed", "response" => %{"id" => "resp_synthetic_credit_fixture", "status" => "completed"}})
    # provenance: synthetic_adversarial
    mode =
      FakeUpstream.strict_sequence([
        FakeUpstream.expect_request(respond: {:json, 200, %{"code" => "reset"}}, method: "POST", path: "/api/codex/rate-limit-reset-credits/consume"),
        FakeUpstream.expect_request(respond: {:json, 200, ProviderCreditsFixtures.usage_payload(:included)}, method: "GET", path: "/backend-api/wham/usage"),
        FakeUpstream.expect_request(respond: {:json, 200, %{"id" => "resp_synthetic_http"}}, method: "POST", path: "/backend-api/codex/responses"),
        FakeUpstream.expect_request(respond: FakeUpstream.websocket_text_frames([completed]), method: "WEBSOCKET", websocket_connection_ordinal: 1)
      ])

    fixture = ProviderCreditsFixtures.open!(mode: mode)
    refute fixture.peer.node == node()
    assert fixture.assignments[{:a, :weekly_credit_only}].upstream_identity_id == fixture.assignments[{:b, :weekly_credit_only}].upstream_identity_id
    assert %{allow_provider_credits: false, capacity_facts: %{included_permission: :available}} = ProviderCreditsFixtures.snapshot!(fixture, :included)
    assert %{allow_provider_credits: false, capacity_facts: %{included_permission: :exhausted, credit_permission: :available}} = ProviderCreditsFixtures.snapshot!(fixture, :weekly_credit_only)
    assert %{raw_windows: [], capacity_facts: %{included_permission: :available}} = ProviderCreditsFixtures.snapshot!(fixture, :windowless_included)
    assert %{raw_windows: [], capacity_facts: %{included_permission: :exhausted, credit_permission: :available}} = ProviderCreditsFixtures.snapshot!(fixture, :windowless_credit_only)
    assert %{raw_windows: [], capacity_facts: %{included_permission: :unknown, credit_permission: :unknown}} = ProviderCreditsFixtures.snapshot!(fixture, :windowless_unknown)

    url = FakeUpstream.url(fixture.upstream)
    assert %{status: 200} = Req.post!(url <> "/api/codex/rate-limit-reset-credits/consume", json: %{"credit_id" => "synthetic_reset_1"}, retry: false)
    assert %{status: 200} = Req.get!(url <> "/backend-api/wham/usage", retry: false)
    assert 200 == :erpc.call(fixture.peer.node, ProviderCreditsFixtures, :http_generation!, [url], @budget)
    assert "response.completed" == :erpc.call(fixture.peer.node, ProviderCreditsFixtures, :websocket_generation!, [url], @budget)
    assert %{http_generation: 1, websocket_generation: 1, usage: 1, consume: 1, other: 0} == FakeUpstream.physical_counts(fixture.upstream)

    assert Enum.map(FakeUpstream.physical_receipts(fixture.upstream), &{&1.ordinal, &1.kind, &1.transport, &1.connection_id}) == [
             {1, :consume, :http, nil},
             {2, :usage, :http, nil},
             {3, :generation, :http, nil},
             {4, :generation, :websocket, 1}
           ]

    assert :ok == FakeUpstream.verify!(fixture.upstream)
    assert :ok == ProviderCreditsFixtures.close!(fixture)
    assert_closed!(fixture)
  end

  @tag slow: "boots a real-Repo peer and holds its exact coherent read on a PostgreSQL relation fence"
  test "commit before the held final read is visible to the reader and leaves the physical socket unsent" do
    fixture = ProviderCreditsFixtures.open!(allow_provider_credits: true)
    identity_id = fixture.identities.weekly_credit_only.id
    test_pid = self()
    start_ref = make_ref()
    url = FakeUpstream.url(fixture.upstream)
    worker = ProviderCreditsFixtures.start_peer_work!(fixture, {ProviderCreditsFixtures, :read_then_generation, [identity_id, url, [wait_before_read: {test_pid, start_ref}]]})
    assert_receive {:fixture_reader_ready, ^start_ref, reader, backend_pid}, @budget
    barrier = ProviderCreditsFixtures.before_read_barrier!(fixture, identity_id, backend_pid: backend_pid)
    send(reader, {:read, start_ref})
    assert %{backend_pid: ^backend_pid, phase: :before_final_read} = ProviderCreditsFixtures.await_before_read!(barrier)
    assert %DateTime{} = ProviderCreditsFixtures.commit_policy_and_release!(barrier, false)
    work_ref = worker.ref
    assert_receive {:provider_credits_work, ^work_ref, {:unsent, false}}, @budget
    assert_worker_down!(worker)
    assert %{http_generation: 0, websocket_generation: 0} = FakeUpstream.physical_counts(fixture.upstream)
    assert :ok == ProviderCreditsFixtures.close!(fixture)
    assert_closed!(fixture)
  end

  @tag slow: "boots a real-Repo peer and synchronously holds the final read before real HTTP generation"
  test "commit after the held read leaves that read admitted, while the next read sees opt-out" do
    fixture = ProviderCreditsFixtures.open!(allow_provider_credits: true, mode: {:json, 200, %{"id" => "resp_synthetic_admitted"}})
    identity_id = fixture.identities.weekly_credit_only.id
    barrier = ProviderCreditsFixtures.after_read_barrier!(fixture, identity_id, node: fixture.peer.node)
    url = FakeUpstream.url(fixture.upstream)
    worker = ProviderCreditsFixtures.start_peer_work!(fixture, {ProviderCreditsFixtures, :read_then_generation, [identity_id, url, []]})
    barrier_ref = barrier.ref
    assert_receive {:provider_credits_barrier, ^barrier_ref, :after_final_read, emitter}, @budget
    assert emitter == worker.pid
    assert %{http_generation: 0} = FakeUpstream.physical_counts(fixture.upstream)
    ProviderCreditsFixtures.commit_policy!(fixture, :weekly_credit_only, false)
    ProviderCreditsFixtures.release_barrier!(barrier)
    assert_receive {:provider_credits_barrier_released, ^barrier_ref, ^emitter}, @budget
    work_ref = worker.ref
    assert_receive {:provider_credits_work, ^work_ref, {:sent, 200}}, @budget
    assert_worker_down!(worker)
    assert %{allow_provider_credits: false} = ProviderCreditsFixtures.snapshot!(fixture, :weekly_credit_only)
    assert %{http_generation: 1, websocket_generation: 0} = FakeUpstream.physical_counts(fixture.upstream)
    assert :ok == ProviderCreditsFixtures.close!(fixture)
    assert_closed!(fixture)
  end

  @tag credits_negative: true
  @tag slow: "boots an owned real-Repo peer and injects a failure while its final-read barrier is held"
  test "a deliberate held-barrier failure cancels its writer and cleans up listener, node and committed rows" do
    fixture = ProviderCreditsFixtures.open!(allow_provider_credits: true)
    identity_id = fixture.identities.weekly_credit_only.id
    barrier = ProviderCreditsFixtures.after_read_barrier!(fixture, identity_id, node: fixture.peer.node)
    url = FakeUpstream.url(fixture.upstream)
    worker = ProviderCreditsFixtures.start_peer_work!(fixture, {ProviderCreditsFixtures, :read_then_generation, [identity_id, url, [ignore_policy: true]]})
    barrier_ref = barrier.ref
    assert_receive {:provider_credits_barrier, ^barrier_ref, :after_final_read, _emitter}, @budget

    assert_raise RuntimeError, "synthetic admission barrier failure", fn ->
      try do
        raise "synthetic admission barrier failure"
      after
        ProviderCreditsFixtures.close!(fixture)
      end
    end

    assert_worker_down!(worker)
    assert_closed!(fixture)
  end

  test "fractional and unlimited synthetic observations preserve authority without rounding or invented windows" do
    now = DateTime.utc_now()
    {:ok, fractional} = CodexParsers.parse_codex_usage_result(ProviderCreditsFixtures.usage_payload(:windowless_credit_only, now: now, credits: :fractional), now)
    assert %{balance: "0.024", account_windows: [], credit_permission: :available} = fractional.capacity_facts
    assert CapacityFacts.positive_balance?(fractional.capacity_facts)
    {:ok, unlimited} = CodexParsers.parse_codex_usage_result(ProviderCreditsFixtures.usage_payload(:windowless_credit_only, now: now, credits: :unlimited), now)
    assert %{balance: nil, unlimited: true, account_windows: [], credit_permission: :available} = unlimited.capacity_facts
    assert [] == unlimited.windows
  end

  defp assert_worker_down!(worker) do
    pid = worker.pid
    monitor = worker.monitor
    assert_receive {:DOWN, ^monitor, :process, ^pid, _reason}, @budget
  end

  defp assert_closed!(fixture) do
    refute Process.alive?(fixture.keeper)
    refute Process.alive?(fixture.upstream.server)
    refute Process.alive?(fixture.upstream.supervisor)
    PeerRegistry.assert_peer_absent!(fixture.peer.name, peer_node: fixture.peer.node, budget_ms: @budget)
    uri = URI.parse(FakeUpstream.url(fixture.upstream))

    case :gen_tcp.connect({127, 0, 0, 1}, uri.port, [:binary, active: false], @budget) do
      {:error, :econnrefused} ->
        :ok

      {:ok, socket} ->
        :gen_tcp.close(socket)
        flunk("owned fake listener remains open")

      {:error, reason} ->
        flunk("could not establish owned listener absence: #{inspect(reason)}")
    end

    UnboxedFixture.run_unboxed(fn ->
      assert Repo.aggregate(from(pool in Pool, where: pool.id in ^fixture.pool_ids), :count) == 0
      assert Repo.aggregate(from(identity in UpstreamIdentity, where: identity.id in ^fixture.identity_ids), :count) == 0
    end)
  end
end
