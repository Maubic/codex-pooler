defmodule CodexPooler.Upstreams.Reconciliation.QuotaConvergenceBoundaryTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Quotas.Evidence
  alias CodexPooler.Upstreams.Quota.Windows
  alias CodexPooler.Upstreams.Reconciliation.{PoolReconciliation, QuotaConvergenceVerifier}

  test "discovers persisted selector and converges through real HTTP probes and reconciliation" do
    %{identity: identity, fake: fake} = fixture()

    assert {:ok, report} =
             QuotaConvergenceVerifier.run(samples: 2, interval_ms: 1, mode: "converged")

    assert report.scope == "account"
    assert report.sample_count == 2
    assert Enum.all?(report.samples, & &1.converged)
    assert Enum.all?(report.samples, &(&1.provider.source_class == "provider_usage"))
    assert Enum.all?(report.samples, &(&1.persisted.freshness == "fresh"))
    assert Enum.map(report.samples, & &1.index) == [1, 2]
    assert length(FakeUpstream.requests(fake)) >= 6
    assert [_] = Windows.list_evidence(identity)
  end

  test "does not accept a provider descriptor without persisted evidence" do
    fixture(persist: false)
    assert {:error, %{code: "no_accepted_selector"}} = QuotaConvergenceVerifier.run()
  end

  test "stable mode accepts repeated real observations despite provider and database precision" do
    fixture()

    assert {:ok, %{mode: "stable"}} =
             QuotaConvergenceVerifier.run(samples: 2, interval_ms: 0)
  end

  test "sampling rejects unusable provider evidence after discovery" do
    %{fake: fake} = fixture = fixture()
    candidate = candidate(fixture)

    FakeUpstream.set_mode(
      fake,
      {:json, 200, %{"rate_limit" => %{}, "additional_rate_limits" => []}}
    )

    assert {:error, %{code: "observation_failed", status: "failed"}} =
             QuotaConvergenceVerifier.run(
               candidate_source: [candidate],
               samples: 2,
               interval_ms: 0
             )
  end

  test "sampling sanitizes an unavailable provider without treating cached rows as convergence" do
    %{fake: fake} = fixture = fixture()
    candidate = candidate(fixture)
    FakeUpstream.set_mode(fake, {:json_error, 503, %{}})

    assert {:error, %{code: "observation_failed", status: "failed"}} =
             QuotaConvergenceVerifier.run(
               candidate_source: [candidate],
               samples: 2,
               interval_ms: 0
             )
  end

  test "unavailable provider discovery produces no accepted selector" do
    %{fake: fake} = fixture()
    FakeUpstream.set_mode(fake, {:json_error, 503, %{}})
    assert {:error, %{code: "no_accepted_selector"}} = QuotaConvergenceVerifier.run()
  end

  test "invalid options are rejected before contacting the provider" do
    %{fake: fake} = fixture(persist: false)

    for options <- [
          [mode: "unsupported"],
          [samples: 1],
          [samples: 61],
          [samples: 2.0],
          [interval_ms: -1],
          [interval_ms: 60_001]
        ] do
      assert {:error, %{code: "invalid_options"}} = QuotaConvergenceVerifier.run(options)
    end

    assert FakeUpstream.requests(fake) == []
  end

  defp fixture(opts \\ []) do
    reset_at = DateTime.utc_now() |> DateTime.add(3_600, :second) |> DateTime.to_unix()

    payload = %{
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 12,
          "limit_window_seconds" => 18_000,
          "reset_at" => reset_at
        }
      }
    }

    {:ok, fake} =
      FakeUpstream.start_link(
        {:path_json,
         %{
           "/backend-api/wham/usage" => {200, payload},
           "/backend-api/codex/usage" => {200, payload}
         }}
      )

    on_exit(fn -> FakeUpstream.stop(fake) end)

    %{identity: identity, assignment: assignment} =
      active_upstream_assignment_fixture(pool_fixture(), %{
        metadata: %{"usage_base_url" => FakeUpstream.url(fake)}
      })

    if Keyword.get(opts, :persist, true) do
      assert {:ok, _identity} = PoolReconciliation.refresh_quota_from_usage(identity, assignment)
    end

    %{identity: identity, assignment: assignment, fake: fake}
  end

  defp candidate(%{identity: identity, assignment: assignment}) do
    [window] = Windows.list_evidence(identity)

    %{
      identity: identity,
      assignment: assignment,
      selector: {identity.id, Evidence.descriptor_key(window)},
      descriptor_count: 1,
      row_count: 1,
      provider_window: window,
      persisted_row: window
    }
  end
end
