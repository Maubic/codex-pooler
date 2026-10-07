defmodule CodexPooler.Upstreams.Quota.CapacityFactsStoreTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas.{CapacityFacts, Evidence}
  alias CodexPooler.Upstreams.Quota.CapacityFactsStore

  @now ~U[2026-10-01 12:00:00Z]

  test "exact canonical decimal authority is fenced and fresh only within its own receipt" do
    metadata = CapacityFactsStore.transition(%{"unrelated" => true}, credit(), 2)
    assert metadata["unrelated"]
    assert {:ok, facts} = CapacityFactsStore.load(metadata)
    assert facts.balance == "0.125"
    assert CapacityFactsStore.fresh?(facts, 2, @now)
    assert CapacityFactsStore.fresh?(facts, 2, DateTime.add(@now, Evidence.freshness_ttl_seconds()))
    refute CapacityFactsStore.fresh?(facts, 2, DateTime.add(@now, Evidence.freshness_ttl_seconds() + 1))
    refute CapacityFactsStore.fresh?(facts, 3, @now)
    refute CapacityFactsStore.fresh?(facts, 2, DateTime.add(@now, -1))
  end

  @tag credits_negative: true
  test "newer unknown full observation revokes credit authority and an old receipt cannot resurrect it" do
    grant = credit()
    newer = CapacityFacts.revoke(%{grant | observed_at: DateTime.add(@now, 1)})
    metadata = %{} |> CapacityFactsStore.transition(grant, 1) |> CapacityFactsStore.transition(newer, 1)
    assert {:ok, facts} = CapacityFactsStore.load(metadata)
    assert facts.credit_permission == :unknown
    assert metadata == CapacityFactsStore.transition(metadata, grant, 1)
  end

  @tag credits_negative: true
  test "same-instant conflicting grant cannot override a blocker; blockers do not expire into grants" do
    grant = credit()
    denial = %{grant | credit_permission: :unavailable, denial_category: :workspace_limit}
    metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(grant, 1)
    assert {:ok, facts} = CapacityFactsStore.load(metadata)
    assert CapacityFactsStore.hard_denial?(facts, 1, DateTime.add(@now, 100_000))
    refute CapacityFactsStore.hard_denial?(facts, 2, @now)
  end

  @tag credits_negative: true
  test "a previous-credential receipt cannot be rebound into the replacement epoch" do
    metadata = CapacityFactsStore.transition(%{}, %{credit() | credential_epoch: 2}, 2)
    old = %{credit() | credential_epoch: 1, observed_at: DateTime.add(@now, 1)}
    assert CapacityFactsStore.transition(metadata, old, 2) == metadata
    assert {:ok, facts} = CapacityFactsStore.load(metadata)
    assert facts.credential_epoch == 2
    assert facts.observed_at == @now
  end

  @tag credits_negative: true
  test "newer unknown cannot erase a same-epoch blocker and another source cannot clear it" do
    denial = %{credit() | credit_permission: :unavailable, denial_category: :workspace_limit, source_kind: :wham_usage}
    unknown = CapacityFacts.revoke(%{credit() | observed_at: DateTime.add(@now, 1), source_kind: :wham_usage})
    metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(unknown, 1)
    assert {:ok, %{credit_permission: :unknown}} = CapacityFactsStore.load(metadata)
    assert {:ok, %{observations: [blocker], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    assert blocker.denial_category == :workspace_limit
    other_source = %{credit() | observed_at: DateTime.add(@now, 2), source_kind: :codex_usage}
    metadata = CapacityFactsStore.transition(metadata, other_source, 1)
    assert {:ok, %{observations: [^blocker], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    matching = %{credit() | observed_at: DateTime.add(@now, 3), source_kind: :wham_usage}
    assert CapacityFactsStore.load_blockers(CapacityFactsStore.transition(metadata, matching, 1)) == :error
  end

  @tag credits_negative: true
  test "new included permission with absent spend evidence cannot clear credit spend denial" do
    denial = %{credit() | credit_permission: :unavailable, denial_category: :spend_limit, source_kind: :wham_usage}
    included = %{credit() | observed_at: DateTime.add(@now, 1), included_permission: :available, credit_permission: :unknown, denial_category: :none, source_kind: :wham_usage}
    metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(included, 1)
    assert {:ok, %{included_permission: :available}} = CapacityFactsStore.load(metadata)
    assert {:ok, %{observations: [%{denial_category: :spend_limit}]}} = CapacityFactsStore.load_blockers(metadata)
  end

  for {hard_category, replacement_category} <- [
        {:workspace_limit, :spend_limit},
        {:workspace_limit, :malformed},
        {:workspace_limit, :model_limit},
        {:model_limit, :spend_limit},
        {:model_limit, :malformed}
      ] do
    @tag credits_negative: true
    test "a newer #{replacement_category} receipt cannot weaken a retained #{hard_category} blocker" do
      original = %{credit() | credit_permission: :unavailable, denial_category: unquote(hard_category), source_kind: :codex_usage, account_windows: [descriptor("secondary", 10_080)]}
      replacement = %{original | observed_at: DateTime.add(@now, 1), denial_category: unquote(replacement_category), included_permission: :unknown, credit_permission: :unknown}
      metadata = %{} |> CapacityFactsStore.transition(original, 1) |> CapacityFactsStore.transition(replacement, 1)

      assert {:ok, persisted} = CapacityFactsStore.load(metadata)
      assert persisted.denial_category == unquote(replacement_category)
      assert {:ok, %{observations: [blocker | _]}} = CapacityFactsStore.load_blockers(metadata)
      assert blocker.denial_category == unquote(hard_category)
      assert blocker.observed_at == @now
      assert blocker.source_kind == :codex_usage
      assert blocker.account_windows == original.account_windows
      assert CapacityFactsStore.hard_denial?(blocker, 1, DateTime.add(@now, 2))
    end
  end

  for hard_category <- [:workspace_limit, :model_limit],
      incompatible <- [:source, :window] do
    @tag credits_negative: true
    test "a repeated #{hard_category} blocker with an incompatible #{incompatible} cannot launder later clearance" do
      original = %{credit() | credit_permission: :unavailable, denial_category: unquote(hard_category), source_kind: :codex_usage, account_windows: [descriptor("secondary", 10_080)]}
      repeated = %{original | observed_at: DateTime.add(@now, 1)}

      repeated =
        case unquote(incompatible) do
          :source -> %{repeated | source_kind: :wham_usage}
          :window -> %{repeated | account_windows: [descriptor("primary", 300)]}
        end

      metadata = %{} |> CapacityFactsStore.transition(original, 1) |> CapacityFactsStore.transition(repeated, 1)
      assert {:ok, %{observations: [blocker, _repeated]}} = CapacityFactsStore.load_blockers(metadata)
      assert blocker.observed_at == @now
      assert blocker.source_kind == original.source_kind
      assert blocker.account_windows == original.account_windows

      unrelated_clear = %{repeated | observed_at: DateTime.add(@now, 2), included_permission: :available, credit_permission: :available, denial_category: :none}
      metadata = CapacityFactsStore.transition(metadata, unrelated_clear, 1)
      assert {:ok, %{observations: [^blocker]}} = CapacityFactsStore.load_blockers(metadata)
      assert CapacityFactsStore.hard_denial?(blocker, 1, DateTime.add(@now, 2))

      compatible_clear = %{unrelated_clear | observed_at: DateTime.add(@now, 3), source_kind: original.source_kind, account_windows: original.account_windows}
      assert CapacityFactsStore.load_blockers(CapacityFactsStore.transition(metadata, compatible_clear, 1)) == :error
    end
  end

  @tag credits_negative: true
  test "compatible hard and credit denials retain their independent clearance requirements" do
    spend = %{credit() | credit_permission: :unavailable, denial_category: :spend_limit}
    hard = %{spend | observed_at: DateTime.add(@now, 1), denial_category: :workspace_limit}
    metadata = %{} |> CapacityFactsStore.transition(spend, 1) |> CapacityFactsStore.transition(hard, 1)
    assert {:ok, %{observations: [%{denial_category: :spend_limit}, %{denial_category: :workspace_limit}]}} = CapacityFactsStore.load_blockers(metadata)

    included_only = %{credit() | observed_at: DateTime.add(@now, 2), included_permission: :available, credit_permission: :unknown, denial_category: :none}
    metadata = CapacityFactsStore.transition(metadata, included_only, 1)
    assert {:ok, %{observations: [%{denial_category: :spend_limit}], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    complete = %{included_only | observed_at: DateTime.add(@now, 3), credit_permission: :available}
    assert CapacityFactsStore.load_blockers(CapacityFactsStore.transition(metadata, complete, 1)) == :error
  end

  for credit_category <- [:spend_limit, :malformed], first_clear <- [:credit, :hard] do
    @tag credits_negative: true
    test "incompatible #{credit_category} and workspace fences survive #{first_clear}-first clearance and unknown receipts" do
      original = %{credit() | source_kind: :codex_usage, credit_permission: :unknown, denial_category: unquote(credit_category), account_windows: [descriptor("secondary", 10_080)]}
      hard = %{original | observed_at: DateTime.add(@now, 1), source_kind: :wham_usage, denial_category: :workspace_limit, credit_permission: :unavailable, account_windows: [descriptor("primary", 300)]}
      metadata = %{} |> CapacityFactsStore.transition(original, 1) |> CapacityFactsStore.transition(hard, 1)
      assert {:ok, %{observations: [retained_credit, retained_hard], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
      assert retained_credit.denial_category == unquote(credit_category)
      assert CapacityFactsStore.hard_denial?(retained_hard, 1, DateTime.add(@now, 1))

      {first, remaining} = if unquote(first_clear) == :credit, do: {retained_credit, retained_hard}, else: {retained_hard, retained_credit}
      clear = %{first | observed_at: DateTime.add(@now, 2), included_permission: :available, credit_permission: :available, denial_category: :none}
      metadata = CapacityFactsStore.transition(metadata, clear, 1)
      assert {:ok, %{observations: [^remaining], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
      unknown = CapacityFacts.revoke(%{clear | observed_at: DateTime.add(@now, 3)})
      metadata = CapacityFactsStore.transition(metadata, unknown, 1)
      assert {:ok, %{observations: [^remaining], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
      clear = %{remaining | observed_at: DateTime.add(@now, 4), included_permission: :available, credit_permission: :available, denial_category: :none}
      assert CapacityFactsStore.load_blockers(CapacityFactsStore.transition(metadata, clear, 1)) == :error
    end
  end

  @tag credits_negative: true
  test "repeated resource bindings retain their first observation and original reset instant" do
    original = %{credit() | source_kind: :codex_usage, credit_permission: :unavailable, denial_category: :workspace_limit, account_windows: [descriptor("secondary", 10_080)]}
    repeated = %{original | observed_at: DateTime.add(@now, 1), account_windows: [%{hd(original.account_windows) | reset_at: DateTime.add(@now, 7_200), used_percent: "50"}]}
    metadata = %{} |> CapacityFactsStore.transition(original, 1) |> CapacityFactsStore.transition(repeated, 1)
    assert {:ok, %{observations: [blocker], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    assert blocker.observed_at == @now
    assert blocker.account_windows == original.account_windows
  end

  @tag credits_negative: true
  test "a newer stronger compatible hard fence cannot hide an earlier time-visible model denial" do
    model = %{credit() | credit_permission: :unavailable, denial_category: :model_limit, source_kind: :codex_usage, account_windows: [descriptor("secondary", 10_080)]}
    workspace = %{model | observed_at: DateTime.add(@now, 1), denial_category: :workspace_limit}
    metadata = %{} |> CapacityFactsStore.transition(model, 1) |> CapacityFactsStore.transition(workspace, 1)
    assert {:ok, %{observations: [earlier, later]}} = CapacityFactsStore.load_blockers(metadata)
    assert earlier.denial_category == :model_limit
    assert later.denial_category == :workspace_limit
    assert CapacityFactsStore.hard_denial?(earlier, 1, @now)
    refute CapacityFactsStore.current?(later, 1, @now)
  end

  @tag credits_negative: true
  test "equal-instant independent blockers are retained without replacing current authority" do
    first = %{credit() | credit_permission: :unavailable, denial_category: :spend_limit, source_kind: :codex_usage}
    second = %{first | denial_category: :workspace_limit, source_kind: :wham_usage}
    metadata = %{} |> CapacityFactsStore.transition(first, 1) |> CapacityFactsStore.transition(second, 1)
    assert {:ok, %{denial_category: :spend_limit}} = CapacityFactsStore.load(metadata)
    assert {:ok, %{observations: [%{denial_category: :spend_limit}, %{denial_category: :workspace_limit}]}} = CapacityFactsStore.load_blockers(metadata)
  end

  @tag credits_negative: true
  test "sixteen independent fences remain individually clearable without overflow" do
    {metadata, original} = fill_blockers(16)
    assert {:ok, %{observations: retained, overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    assert Enum.map(retained, & &1.account_windows) == Enum.map(original, & &1.account_windows)

    cleared = clear_blockers(metadata, original, 1, 20)
    assert CapacityFactsStore.load_blockers(cleared) == :error
  end

  @tag credits_negative: true
  test "overflow retains all sixteen witnesses and cannot clear until credential epoch cutover" do
    {metadata, original} = fill_blockers(16)
    extra = %{credit() | observed_at: DateTime.add(@now, 17), credit_permission: :unavailable, denial_category: :workspace_limit, source_kind: :codex_usage, account_windows: [descriptor("primary", 17)]}
    metadata = CapacityFactsStore.transition(metadata, extra, 1)
    assert {:ok, %{observations: retained, overflowed?: true}} = CapacityFactsStore.load_blockers(metadata)
    assert Enum.map(retained, & &1.account_windows) == Enum.map(original, & &1.account_windows)

    cleared = clear_blockers(metadata, original, 1, 20)
    assert {:ok, %{observations: [], overflowed?: true, credential_epoch: 1}} = CapacityFactsStore.load_blockers(cleared)
    new_epoch = CapacityFactsStore.transition(cleared, %{credit() | observed_at: DateTime.add(@now, 40)}, 2)
    assert CapacityFactsStore.load_blockers(new_epoch) == :error
    assert {:ok, %{credential_epoch: 2, credit_permission: :available}} = CapacityFactsStore.load(new_epoch)
    assert new_epoch == CapacityFactsStore.transition(new_epoch, %{extra | observed_at: DateTime.add(@now, 41), credential_epoch: 1}, 2)
  end

  @tag credits_negative: true
  test "a valid future-epoch retained envelope is not treated as an earlier credential cutover" do
    future = %{credit() | credit_permission: :unavailable, denial_category: :workspace_limit}
    metadata = CapacityFactsStore.transition(%{}, future, 2)
    rewritten = CapacityFactsStore.transition(metadata, %{credit() | observed_at: DateTime.add(@now, 1)}, 1)
    assert {:ok, %{observations: [], overflowed?: true, credential_epoch: 1}} = CapacityFactsStore.load_blockers(rewritten)
  end

  @tag credits_negative: true
  test "a valid empty non-overflowed envelope disappears on the next observation" do
    metadata = %{"quota_capacity_blocker" => %{"version" => 1, "credential_epoch" => 1, "overflowed" => false, "observations" => []}}
    assert {:ok, %{observations: [], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    assert CapacityFactsStore.load_blockers(CapacityFactsStore.transition(metadata, credit(), 1)) == :error
  end

  @tag credits_negative: true
  test "malformed retained envelopes cannot be laundered by a newer complete receipt" do
    denied = %{credit() | credit_permission: :unavailable, denial_category: :workspace_limit}
    metadata = CapacityFactsStore.transition(%{}, denied, 1)
    envelope = metadata["quota_capacity_blocker"]

    for malformed <- [nil, "invalid", CapacityFactsStore.encode!(denied, 1), Map.put(envelope, "payload", %{}), Map.put(envelope, "version", 2), Map.put(envelope, "overflowed", "false"), Map.put(envelope, "observations", %{}), Map.put(envelope, "observations", List.duplicate(hd(envelope["observations"]), 17)), Map.put(envelope, "observations", [CapacityFactsStore.encode!(denied, 2)]), Map.put(envelope, "observations", [CapacityFactsStore.encode!(credit(), 1)])] do
      malformed_metadata = Map.put(metadata, "quota_capacity_blocker", malformed)
      assert CapacityFactsStore.load_blockers(malformed_metadata) == :error
      rewritten = CapacityFactsStore.transition(malformed_metadata, %{credit() | observed_at: DateTime.add(@now, 1)}, 1)
      assert {:ok, %{observations: [], overflowed?: true, credential_epoch: 1}} = CapacityFactsStore.load_blockers(rewritten)
    end
  end

  # A Team member's workspace denial recorded while its 5-hour window was
  # exhausted, as the reconciliation persists it.
  @tag credits_negative: true
  test "a newer receipt with each exhausted window in a later cycle releases a retained workspace denial whatever it leaves unknown" do
    denial = workspace_denial()
    later_cycle = [window("primary", 300, 14 * 3_600, "0"), window("secondary", 10_080, 7 * 86_400 + 9 * 3_600, "0")]
    released = CapacityFacts.revoke(%{denial | observed_at: DateTime.add(@now, 9 * 3_600), account_windows: later_cycle}, :malformed)
    metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(released, 1)
    assert {:ok, %{denial_category: :malformed}} = CapacityFactsStore.load(metadata)
    assert {:ok, %{observations: [%{denial_category: :malformed}], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
  end

  for {name, primary} <- [{"the same cycle", {3_600, "100"}}, {"a later cycle exhausted again", {6 * 3_600, "100"}}, {"a cycle within reset rounding", {3_605, "0"}}] do
    @tag credits_negative: true
    test "a newer unknown receipt with the exhausted window in #{name} keeps the workspace denial" do
      denial = workspace_denial()
      {reset_in, used} = unquote(primary)
      newer = CapacityFacts.revoke(%{denial | observed_at: DateTime.add(@now, 60), account_windows: [window("primary", 300, reset_in, used), window("secondary", 10_080, 6 * 86_400, "16")]}, :malformed)
      metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(newer, 1)
      assert {:ok, %{observations: [^denial | _]}} = CapacityFactsStore.load_blockers(metadata)
    end
  end

  @tag credits_negative: true
  test "a newer receipt that still reports the workspace denial in a later cycle keeps denying" do
    denial = workspace_denial()
    repeated = %{denial | observed_at: DateTime.add(@now, 2 * 3_600), account_windows: [window("primary", 300, 6 * 3_600, "100"), window("secondary", 10_080, 6 * 86_400, "30")]}
    metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(repeated, 1)
    assert {:ok, %{observations: [^denial, ^repeated]}} = CapacityFactsStore.load_blockers(metadata)
    assert {:ok, ^repeated} = CapacityFactsStore.load(metadata)
    refute CapacityFactsStore.hard_denial_lapsed?(repeated, DateTime.add(@now, 6 * 3_600 - 1))
    assert CapacityFactsStore.hard_denial_lapsed?(repeated, DateTime.add(@now, 6 * 3_600))
  end

  # A newer denial of a retained binding keeps its own end: a reading that
  # releases only the earlier witness's exhausted window cannot release it.
  for {name, primary} <- [{"an exhausted window in a later cycle", "100"}, {"no exhausted window", "0"}] do
    @tag credits_negative: true
    test "a newer workspace denial with #{name} survives a reading that releases the earlier witness" do
      denial = workspace_denial()
      newer = %{denial | observed_at: DateTime.add(@now, 1_800), account_windows: [window("primary", 300, 6 * 3_600, unquote(primary)), window("secondary", 10_080, 6 * 86_400, "16")]}
      released = CapacityFacts.revoke(%{denial | observed_at: DateTime.add(@now, 2 * 3_600), account_windows: [window("primary", 300, 6 * 3_600, "20"), window("secondary", 10_080, 6 * 86_400, "20")]}, :malformed)
      metadata = Enum.reduce([denial, newer, released], %{}, &CapacityFactsStore.transition(&2, &1, 1))
      assert {:ok, %{observations: [^newer, %{denial_category: :malformed}], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
      refute CapacityFactsStore.hard_denial_lapsed?(newer, DateTime.add(@now, 6 * 3_600 - 1))
      assert CapacityFactsStore.hard_denial_lapsed?(newer, DateTime.add(@now, 6 * 3_600)) == (unquote(primary) == "100")
    end
  end

  @tag credits_negative: true
  test "a newer model denial keeps its own reset beside a stronger workspace witness" do
    denial = workspace_denial()
    model = %{denial | observed_at: DateTime.add(@now, 1_800), denial_category: :model_limit, account_windows: [window("primary", 300, 6 * 3_600, "100"), window("secondary", 10_080, 6 * 86_400, "16")]}
    metadata = %{} |> CapacityFactsStore.transition(denial, 1) |> CapacityFactsStore.transition(model, 1)
    assert {:ok, %{observations: [^denial, ^model], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
  end

  @tag credits_negative: true
  test "a denial repeated through many cycles keeps its first and newest witness, and a same-cycle repeat only a later reset" do
    denial = workspace_denial()
    repeats = for cycle <- 1..20, do: %{denial | observed_at: DateTime.add(@now, cycle * 18_000), account_windows: [window("primary", 300, cycle * 18_000 + 3_600, "100"), window("secondary", 10_080, 6 * 86_400, "16")]}
    metadata = Enum.reduce([denial | repeats], %{}, &CapacityFactsStore.transition(&2, &1, 1))
    newest = List.last(repeats)
    assert {:ok, %{observations: [^denial, ^newest], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)

    repeat = fn {offset, seconds} -> %{denial | observed_at: DateTime.add(@now, offset), account_windows: [window("primary", 300, 3_600 + seconds, "100"), window("secondary", 10_080, 6 * 86_400, "16")]} end
    metadata = Enum.reduce(Enum.map([{0, 0}, {60, -2}, {120, 0}, {180, -1}], repeat), %{}, &CapacityFactsStore.transition(&2, &1, 1))
    assert {:ok, %{observations: [^denial], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    latest = repeat.({360, 5})
    metadata = Enum.reduce(Enum.map([{240, 1}, {300, 3}], repeat) ++ [latest], metadata, &CapacityFactsStore.transition(&2, &1, 1))
    assert {:ok, %{observations: [^denial, ^latest], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
  end

  # The witness is released 5 seconds after its own reset and lapses at it, so it
  # covers no denial resetting later, however close.
  @tag credits_negative: true
  test "a newer denial resetting seconds after the witness keeps its own release and lapse" do
    denial = workspace_denial()
    newer = %{denial | observed_at: DateTime.add(@now, 1_800), account_windows: [window("primary", 300, 3_605, "100"), window("secondary", 10_080, 6 * 86_400, "16")]}
    released = CapacityFacts.revoke(%{denial | observed_at: DateTime.add(@now, 2_400), account_windows: [window("primary", 300, 3_608, "20"), window("secondary", 10_080, 6 * 86_400, "20")]}, :malformed)
    metadata = Enum.reduce([denial, newer, released], %{}, &CapacityFactsStore.transition(&2, &1, 1))
    assert {:ok, %{observations: [^newer, %{denial_category: :malformed}], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)

    same_cycle = %{released | account_windows: [window("primary", 300, 3_602, "100"), window("secondary", 10_080, 6 * 86_400, "16")]}
    metadata = Enum.reduce([denial, newer, same_cycle], %{}, &CapacityFactsStore.transition(&2, &1, 1))
    assert {:ok, %{observations: [^denial, ^newer, %{denial_category: :malformed}], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
    assert CapacityFactsStore.hard_denial_lapsed?(denial, DateTime.add(@now, 3_604))
    refute CapacityFactsStore.hard_denial_lapsed?(newer, DateTime.add(@now, 3_604))
  end

  # An earlier release left a newer denial of a retained binding out of the
  # witnesses and kept it only as the current reading.
  @tag credits_negative: true
  test "a denial an earlier release kept only as the current reading is retained before a newer reading replaces it" do
    denial = workspace_denial()
    newer = %{denial | observed_at: DateTime.add(@now, 2 * 3_600), account_windows: [window("primary", 300, 6 * 3_600, "100"), window("secondary", 10_080, 6 * 86_400, "16")]}
    released = CapacityFacts.revoke(%{denial | observed_at: DateTime.add(@now, 3 * 3_600), account_windows: [window("primary", 300, 6 * 3_600, "20"), window("secondary", 10_080, 6 * 86_400, "20")]}, :malformed)
    stored = %{"quota_capacity_facts" => CapacityFactsStore.encode!(newer, 1), "quota_capacity_blocker" => %{"version" => 1, "credential_epoch" => 1, "overflowed" => false, "observations" => [CapacityFactsStore.encode!(denial, 1)]}}
    metadata = CapacityFactsStore.transition(stored, released, 1)
    assert {:ok, %{observations: [^newer, %{denial_category: :malformed}], overflowed?: false}} = CapacityFactsStore.load_blockers(metadata)
  end

  test "a hard denial lapses only once every exhausted window it recorded has reset" do
    denial = workspace_denial()
    refute CapacityFactsStore.hard_denial_lapsed?(denial, DateTime.add(@now, 3_599))
    assert CapacityFactsStore.hard_denial_lapsed?(denial, DateTime.add(@now, 3_600))

    both = %{denial | account_windows: [window("primary", 300, 3_600, "100"), window("secondary", 10_080, 6 * 86_400, "100")]}
    refute CapacityFactsStore.hard_denial_lapsed?(both, DateTime.add(@now, 6 * 86_400 - 1))
    assert CapacityFactsStore.hard_denial_lapsed?(both, DateTime.add(@now, 6 * 86_400))

    unexhausted = %{denial | account_windows: [window("primary", 300, 3_600, "0"), window("secondary", 10_080, 6 * 86_400, "16")]}
    refute CapacityFactsStore.hard_denial_lapsed?(unexhausted, DateTime.add(@now, 30 * 86_400))
    refute CapacityFactsStore.hard_denial_lapsed?(%{denial | account_windows: []}, DateTime.add(@now, 30 * 86_400))
    refute CapacityFactsStore.hard_denial_lapsed?(%{denial | denial_category: :spend_limit}, DateTime.add(@now, 30 * 86_400))
  end

  defp workspace_denial do
    %{credit() | credential_epoch: 1, included_permission: :unknown, credit_permission: :unavailable, denial_category: :workspace_limit, source_kind: :wham_usage, balance: nil, has_credits: false, account_windows: [window("primary", 300, 3_600, "100"), window("secondary", 10_080, 6 * 86_400, "16")]}
  end

  defp window(kind, minutes, reset_in, used), do: %{window_kind: kind, window_minutes: minutes, reset_at: DateTime.add(@now, reset_in), used_percent: used}

  defp fill_blockers(count) do
    observations = for index <- 1..count, do: %{credit() | observed_at: DateTime.add(@now, index), credit_permission: :unavailable, denial_category: :workspace_limit, source_kind: :codex_usage, account_windows: [descriptor("primary", index)]}
    {Enum.reduce(observations, %{}, &CapacityFactsStore.transition(&2, &1, 1)), observations}
  end

  defp clear_blockers(metadata, observations, epoch, offset) do
    observations
    |> Enum.with_index(offset)
    |> Enum.reduce(metadata, fn {observation, index}, metadata ->
      clear = %{observation | observed_at: DateTime.add(@now, index), included_permission: :available, credit_permission: :available, denial_category: :none}
      CapacityFactsStore.transition(metadata, clear, epoch)
    end)
  end

  defp descriptor(kind, minutes), do: %{window_kind: kind, window_minutes: minutes, reset_at: DateTime.add(@now, 3_600), used_percent: "25"}

  @tag credits_negative: true
  test "strict decoder rejects unexpected keys, malformed flags and noncanonical amounts" do
    encoded = CapacityFactsStore.encode!(credit(), 1)

    for malformed <- [Map.put(encoded, "payload", %{}), Map.put(encoded, "version", 2), Map.put(encoded, "balance", "0.1250"), Map.put(encoded, "credential_epoch", 0), Map.put(encoded, "has_credits", "true"), Map.put(encoded, "source_kind", "arbitrary-provider-string")] do
      assert CapacityFactsStore.decode(malformed) == :error
    end
  end

  defp credit do
    %CapacityFacts{observed_at: @now, included_permission: :exhausted, credit_permission: :available, denial_category: :included_limit, balance: "0.125", has_credits: true, unlimited: false}
  end
end
