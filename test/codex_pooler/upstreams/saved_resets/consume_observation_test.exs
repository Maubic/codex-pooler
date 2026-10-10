defmodule CodexPooler.Upstreams.SavedResets.ConsumeObservationTest do
  # Whether a consume also resets an exhausted 5-hour window is not observed
  # yet (findings#310 follow-up). The automatic spend fence assumes it does
  # not; the released client's backend fixtures suggest it does. These pins
  # cover what a consume and the bank now record so the next real consume, a
  # manual one or an automatic one on an install that redeems, settles it:
  # the provider's `windows_reset` count, the account's Usage API 5-hour
  # reading before the consume's quota refresh and the first one after it,
  # and the kinds of the banked credits (machine `reset_type` and a title
  # fingerprint, never the title). Only fake upstreams are involved.
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows
  alias CodexPooler.Upstreams.SavedResetRedemption
  alias CodexPooler.Upstreams.SavedResets
  alias CodexPooler.Upstreams.SavedResets.RedemptionLifecycle

  @list_path "/backend-api/wham/rate-limit-reset-credits"
  @consume_path "/backend-api/wham/rate-limit-reset-credits/consume"
  @usage_path "/backend-api/wham/usage"
  @title "Full reset (Weekly + 5 hr)"

  describe "a consume" do
    test "records the provider's windows_reset and the 5-hour readings around its quota refresh" do
      %{identity: identity, assignment: assignment} = two_window_account!(consume_response(%{"code" => "reset", "windows_reset" => 2}))

      assert {:ok, %{status: :succeeded, applied?: true, code: "reset"}} = SavedResetRedemption.redeem(assignment)

      persisted = Repo.reload!(identity)
      redemption = persisted.metadata["saved_reset_redemption"]
      result = redemption["result"]

      assert result["windows_reset"] == 2
      assert %{"used_percent" => "100.0", "reset_at" => before_reset, "observed_at" => _observed} = result["five_hour_before"]
      assert map_size(result["five_hour_before"]) == 3
      assert {:ok, _reset_at, 0} = DateTime.from_iso8601(before_reset)

      # The first post-consume reading is the one the provider reported. The
      # stored 100% row was observed moments before, so the evidence store
      # keeps it against this lone 0% reading and the marker says so.
      assert %{"used_percent" => "0.0", "reset_at" => _after_reset, "observed_at" => after_observed, "accepted" => false} = result["five_hour_after"]
      assert {:ok, after_observed_at, 0} = DateTime.from_iso8601(after_observed)
      assert {:ok, consumed_at, 0} = DateTime.from_iso8601(redemption["consumed_at"])
      assert DateTime.compare(after_observed_at, consumed_at) != :lt
      assert Decimal.equal?(stored_five_hour(persisted).used_percent, 100)

      # The bank read after the consume (the fake's credit list still lists
      # both credits) keeps their kinds; the title appears nowhere.
      assert [%{"reset_type" => "codex_rate_limits", "title_fingerprint" => fingerprint, "count" => 2}] = persisted.metadata["saved_resets"]["available_credit_kinds"]
      assert fingerprint == "sha256_" <> (:crypto.hash(:sha256, @title) |> Base.encode16(case: :lower) |> String.slice(0, 12))
      refute CodexPooler.JSON.encode!(persisted.metadata) =~ "Weekly + 5 hr"
    end

    test "marks the post-consume 5-hour reading accepted when the stored window took it" do
      # A stored reading older than the freshness TTL no longer outweighs a
      # forward-cycle 0% reading, so this time the row takes it.
      %{identity: identity, assignment: assignment} = two_window_account!(consume_response(%{"code" => "reset", "windows_reset" => 2}), 16 * 60)

      assert {:ok, %{applied?: true}} = SavedResetRedemption.redeem(assignment)

      persisted = Repo.reload!(identity)
      result = persisted.metadata["saved_reset_redemption"]["result"]

      assert %{"used_percent" => "100.0"} = result["five_hour_before"]
      assert %{"used_percent" => "0.0", "observed_at" => observed_at, "accepted" => true} = result["five_hour_after"]
      stored = stored_five_hour(persisted)
      assert Decimal.equal?(stored.used_percent, 0)
      assert DateTime.to_iso8601(stored.observed_at) == observed_at
    end

    for {label, body} <- [{"out of range", %{"code" => "reset", "windows_reset" => 99}}, {"negative", %{"code" => "reset", "windows_reset" => -1}}, {"absent", %{"code" => "reset"}}] do
      test "keeps no windows_reset when the provider's count is #{label}" do
        %{identity: identity, assignment: assignment} = two_window_account!(consume_response(unquote(Macro.escape(body))))

        assert {:ok, %{applied?: true}} = SavedResetRedemption.redeem(assignment)

        result = Repo.reload!(identity).metadata["saved_reset_redemption"]["result"]
        refute Map.has_key?(result, "windows_reset")
        assert result["code"] == "reset"
      end
    end
  end

  describe "the bank snapshot" do
    test "keeps a reset_type within the identifier rule in cleartext and fingerprints any other" do
      odd = "codex rate limits (beta)"
      long = String.duplicate("a", 81)

      kinds = kinds_of([credit("codex_rate_limits", @title), credit(odd, nil), credit(long, nil)])

      assert Enum.map(kinds, & &1["reset_type"]) |> Enum.sort() == Enum.sort(["codex_rate_limits", fingerprint(odd), fingerprint(long)])
      refute CodexPooler.JSON.encode!(kinds) =~ odd
    end

    test "counts identical credits as one kind and records nothing past eight kinds" do
      assert [%{"reset_type" => "codex_rate_limits", "count" => 3}] = kinds_of(List.duplicate(credit("codex_rate_limits", @title), 3))

      many = for index <- 1..9, do: credit("kind_#{index}", nil)
      refute Map.has_key?(snapshot_of(many), "available_credit_kinds")
    end

    test "an authoritative zero records no kinds, and a refresh without an authoritative detail keeps the previous ones" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      assert SavedResets.usage_snapshot(usage_with(0, []), now, "https://example.invalid#{@usage_path}", nil)["available_credit_kinds"] == []

      previous = snapshot_of([credit("codex_rate_limits", @title)])
      incomplete = SavedResets.usage_snapshot(%{"rate_limit_reset_credits" => %{"available_count" => 1}}, now, "https://example.invalid#{@usage_path}", %{"saved_resets" => previous})
      assert incomplete["available_credit_kinds"] == previous["available_credit_kinds"]

      malformed = Map.put(previous, "available_credit_kinds", [%{"reset_type" => "x", "count" => "1"}])
      refute Map.has_key?(SavedResets.usage_snapshot(%{"rate_limit_reset_credits" => %{"available_count" => 1}}, now, "https://example.invalid#{@usage_path}", %{"saved_resets" => malformed}), "available_credit_kinds")
    end
  end

  # A node of the previous release reads both records with readers this
  # change leaves untouched (the redemption lifecycle fences and the bank
  # snapshot projection), so their decisions with and without the new keys
  # are what such a node decides. A node of the previous release that
  # rewrites either record writes it without the new keys; the readers here
  # treat their absence as nothing observed.
  describe "rolling deploy" do
    test "the lifecycle fences decide the same with and without the observation keys" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      consumed_at = DateTime.add(now, -5, :minute)

      for phase <- ["consumed_pending_probe", "confirmed_by_quota"] do
        bare = redemption_record(phase, consumed_at, %{"code" => "reset", "applied" => true, "available_count_before" => 2, "available_count_after" => 1, "http_status" => 200})
        observed = put_in(bare, ["result"], Map.merge(bare["result"], %{"windows_reset" => 2, "five_hour_before" => reading("100.0", false), "five_hour_after" => reading("0.0", true)}))

        assert RedemptionLifecycle.gateway_auto_latch(observed, now) == RedemptionLifecycle.gateway_auto_latch(bare, now)
        assert RedemptionLifecycle.gateway_auto_sibling_fence(observed, now) == RedemptionLifecycle.gateway_auto_sibling_fence(bare, now)
        assert RedemptionLifecycle.gateway_auto_latch(observed, now) != :clear
      end
    end

    test "the bank snapshot projects the same with and without the credit kinds" do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
      with_kinds = snapshot_of([credit("codex_rate_limits", @title)])
      without_kinds = Map.delete(with_kinds, "available_credit_kinds")

      assert Map.has_key?(with_kinds, "available_credit_kinds")
      assert SavedResets.snapshot(%{"saved_resets" => with_kinds}, now) == SavedResets.snapshot(%{"saved_resets" => without_kinds}, now)
      refute Map.has_key?(SavedResets.usage_snapshot(%{"rate_limit_reset_credits" => %{"available_count" => 1}}, now, "https://example.invalid#{@usage_path}", %{"saved_resets" => without_kinds}), "available_credit_kinds")
    end
  end

  defp two_window_account!(consume, five_hour_observed_ago \\ 0) do
    expires_at = DateTime.utc_now() |> DateTime.add(20, :day) |> DateTime.truncate(:second) |> DateTime.to_iso8601()
    list = %{"credits" => [credit("codex_rate_limits", @title, expires_at, "credit_1"), credit("codex_rate_limits", @title, expires_at, "credit_2")], "available_count" => 2}

    {:ok, fake} = FakeUpstream.start_link({:path_json, %{@list_path => {200, list}, @consume_path => consume, @usage_path => {200, post_consume_usage()}}})
    on_exit(fn -> FakeUpstream.stop(fake) end)

    unique = Ecto.UUID.generate()
    observed_at = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    saved_resets = %{"status" => "reported", "available_count" => 2, "source" => "codex_usage_api", "path_style" => "chatgpt_api", "observed_at" => DateTime.to_iso8601(observed_at), "usage_path" => @usage_path, "reason" => nil}

    fixture =
      active_upstream_assignment_fixture(pool_fixture(), %{
        chatgpt_account_id: "acct_#{unique}",
        account_label: "Consume observation #{unique}",
        metadata: %{"usage_base_url" => FakeUpstream.url(fake), "saved_resets" => saved_resets}
      })

    five_hour_observed_at = DateTime.add(observed_at, -five_hour_observed_ago, :second)
    assert {:ok, [_primary, _secondary]} = QuotaWindows.upsert_quota_windows(fixture.identity, [window("primary", 300, 3 * 3_600, five_hour_observed_at), window("secondary", 10_080, 3 * 86_400, observed_at)])
    fixture
  end

  defp consume_response(body), do: {200, body}

  # Both windows spent before the consume.
  defp window(kind, minutes, reset_in, observed_at) do
    %{
      quota_key: "account",
      window_kind: kind,
      window_minutes: minutes,
      used_percent: Decimal.new("100"),
      reset_at: DateTime.add(observed_at, reset_in, :second),
      observed_at: observed_at,
      last_sync_at: observed_at,
      source: "codex_usage_api",
      source_precision: "observed",
      quota_scope: "account",
      quota_family: "account",
      freshness_state: "fresh"
    }
  end

  # The provider's Usage API right after the consume: both windows in a new,
  # idle cycle at 0%, one credit left.
  defp post_consume_usage do
    now = DateTime.utc_now() |> DateTime.to_unix()

    %{
      "plan_type" => "pro",
      "credits" => %{"balance" => "0", "has_credits" => false, "unlimited" => false},
      "rate_limit_reset_credits" => %{"available_count" => 1},
      "rate_limit" => %{
        "allowed" => true,
        "limit_reached" => false,
        "primary_window" => %{"used_percent" => 0, "limit_window_seconds" => 18_000, "reset_after_seconds" => 18_000, "reset_at" => now + 18_000},
        "secondary_window" => %{"used_percent" => 0, "limit_window_seconds" => 604_800, "reset_after_seconds" => 604_800, "reset_at" => now + 604_800}
      }
    }
  end

  defp stored_five_hour(identity) do
    identity
    |> QuotaWindows.list_evidence()
    |> Enum.find(&(&1.quota_key == "account" and &1.window_kind == "primary" and &1.window_minutes == 300 and &1.source == "codex_usage_api"))
  end

  defp credit(reset_type, title, expires_at \\ nil, id \\ nil) do
    expires_at = expires_at || DateTime.utc_now() |> DateTime.add(20, :day) |> DateTime.truncate(:second) |> DateTime.to_iso8601()

    %{"id" => id || "credit_#{System.unique_integer([:positive])}", "status" => "available", "expires_at" => expires_at, "reset_type" => reset_type}
    |> then(&if title, do: Map.put(&1, "title", title), else: &1)
  end

  defp usage_with(count, credits), do: %{"rate_limit_reset_credits" => %{"available_count" => count, "credits" => credits}}

  defp snapshot_of(credits) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    SavedResets.usage_snapshot(usage_with(length(credits), credits), now, "https://example.invalid#{@usage_path}", nil)
  end

  defp kinds_of(credits), do: snapshot_of(credits)["available_credit_kinds"]

  defp fingerprint(value), do: "sha256_" <> (:crypto.hash(:sha256, value) |> Base.encode16(case: :lower) |> String.slice(0, 12))

  defp redemption_record(phase, consumed_at, result) do
    %{
      "phase" => phase,
      "status" => if(phase == "consumed_pending_probe", do: "redeeming", else: "succeeded"),
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 1,
      "trigger_kind" => "gateway_auto",
      "started_at" => DateTime.to_iso8601(consumed_at),
      "consumed_at" => DateTime.to_iso8601(consumed_at),
      "deadline_at" => DateTime.to_iso8601(RedemptionLifecycle.deadline_at(consumed_at)),
      "result" => result
    }
  end

  defp reading(used_percent, accepted) do
    now = DateTime.utc_now() |> DateTime.truncate(:second)
    %{"used_percent" => used_percent, "reset_at" => DateTime.to_iso8601(DateTime.add(now, 3_600)), "observed_at" => DateTime.to_iso8601(now), "accepted" => accepted}
  end
end
