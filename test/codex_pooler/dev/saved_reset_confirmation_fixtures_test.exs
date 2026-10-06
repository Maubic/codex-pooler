defmodule CodexPooler.Dev.SavedResetConfirmationFixturesTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.PoolerFixtures
  import Phoenix.LiveViewTest

  alias CodexPooler.Accounts
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Accounts.User
  alias CodexPooler.Dev.SavedResetConfirmationFixtures, as: Fixtures
  alias CodexPooler.Pools
  alias CodexPooler.Pools.{Membership, OperatorPoolAssignment}
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Reconciliation.UsagePollCooldown
  alias CodexPooler.Upstreams.Schemas.{EncryptedSecret, PoolUpstreamAssignment, UpstreamIdentity}
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel
  alias CodexPoolerWeb.Admin.UpstreamAccountsReadModel.QuotaProjection
  alias CodexPoolerWeb.Admin.UpstreamCockpitReadModel
  alias CodexPoolerWeb.Admin.UpstreamPageComponents.AccountCard.SavedResetMeter

  test "workflow fixtures compose trusted requests and truthful list/cockpit operation facts" do
    root = temp_journal_root!()
    assert {:ok, receipt} = Fixtures.seed("all", fixture_opts(root, browser_auth: true))

    journal = Fixtures.read_journal!(receipt.journal_path)

    for {scenario, request, outcome, verification} <- [
          {"queued", :queued, :not_recorded, :not_started},
          {"processing", :processing, :unknown, :not_started},
          {"unknown", :none, :unknown, :not_started},
          {"applied_pending", :none, :applied, :pending},
          {"candidate_progression", :none, :applied, :candidate},
          {"confirmed", :none, :applied, :quota_confirmed},
          {"provisional", :none, :applied, :request_verified},
          {"no_credit", :none, :not_applied, :not_started},
          {"nothing_to_reset", :none, :not_applied, :not_started},
          {"reblocked", :none, :applied, :reblocked},
          {"expired", :none, :applied, :expired}
        ] do
      account = scenario_account!(receipt, scenario)
      assert account.saved_reset_operation.request.state == request, scenario
      assert account.saved_reset_operation.provider_outcome == outcome, scenario
      assert account.saved_reset_operation.verification == verification, scenario
      journal = Fixtures.read_journal!(receipt.journal_path)
      scope = User |> Repo.get!(hd(journal["actor_user_ids"])) |> Scope.for_user()
      assert {:ok, cockpit} = UpstreamCockpitReadModel.load_visible_without_request_metrics(scope, account.identity.id)
      assert Map.drop(cockpit.saved_reset_operation, [:last_checked_at]) == Map.drop(account.saved_reset_operation, [:last_checked_at])
      CodexPooler.TestDiagnostics.puts(Jason.encode!(%{scenario: scenario, request: request, outcome: outcome, verification: verification, list_cockpit_equivalent: true}))
    end

    available = scenario_account!(receipt, "exhausted")
    assert available.saved_reset_redemption_action.available?
    assert available.secret_status == :present
    assert available.refresh_status == "imported"
    candidate = scenario_account!(receipt, "candidate_progression")
    weekly = Enum.find(candidate.quota_limits, &(&1.key == :weekly))
    assert Decimal.equal?(weekly.percent, 0)
    assert weekly.saved_reset_context.candidate?
    confirmed = scenario_account!(receipt, "confirmed")
    assert Decimal.equal?(Enum.find(confirmed.quota_limits, &(&1.key == :weekly)).percent, 80)
    assert length(weekly.observations) == 2
    assert Repo.aggregate(from(secret in EncryptedSecret, where: secret.upstream_identity_id in ^journal["identity_ids"] and secret.secret_kind == "refresh_token"), :count) == 0
    assert {:ok, _} = Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
    assert Repo.aggregate(from(secret in EncryptedSecret, where: secret.upstream_identity_id in ^journal["identity_ids"]), :count) == 0
  end

  test "normal transitions retain quota source ids and owned removal/visibility controls restore safely" do
    root = temp_journal_root!()
    sentinel = active_upstream_assignment_fixture(pool_fixture())
    assert {:ok, receipt} = Fixtures.seed("applied_pending", fixture_opts(root, browser_auth: true))
    journal = Fixtures.read_journal!(receipt.journal_path)
    [identity_id] = journal["identity_ids"]
    original = window_ids(identity_id)
    assert map_size(original) >= 2

    for scenario <- ~w(candidate_progression confirmed provisional no_credit unknown applied_pending) do
      assert {:ok, _} = Fixtures.transition(receipt.journal_path, identity_id, scenario, fixture_opts(root))
      assert window_ids(identity_id) == original, scenario
    end

    assert {:ok, _} = Fixtures.transition(receipt.journal_path, identity_id, "source_removed", fixture_opts(root))
    assert map_size(window_ids(identity_id)) == map_size(original) - 1
    removed = scenario_account!(receipt, "applied_pending")
    assert length(Enum.find(removed.quota_limits, &(&1.key == :weekly)).observations) == 1
    assert {:ok, _} = Fixtures.transition(receipt.journal_path, identity_id, "source_restore", fixture_opts(root))
    restored = window_ids(identity_id)
    assert map_size(restored) == map_size(original)
    assert restored["codex_usage_api"] == original["codex_usage_api"]
    assert {:ok, _} = Fixtures.transition(receipt.journal_path, identity_id, "visibility_loss", fixture_opts(root))
    scope = User |> Repo.get!(hd(journal["actor_user_ids"])) |> Scope.for_user()
    assert UpstreamAccountsReadModel.list_visible_accounts(scope, Pools.list_visible_pools(scope), %{identity_id: identity_id}) == []
    assert :error = UpstreamCockpitReadModel.load_visible_without_request_metrics(scope, identity_id)
    assert {:ok, _} = Fixtures.transition(receipt.journal_path, identity_id, "visibility_restore", fixture_opts(root))
    assert scenario_account!(receipt, "applied_pending").identity.id == identity_id
    assert Repo.get!(UpstreamIdentity, sentinel.identity.id)
    assert {:error, _} = Fixtures.transition(receipt.journal_path, sentinel.identity.id, "source_removed", fixture_opts(root))
    assert {:ok, _} = Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
  end

  defp window_ids(identity_id) do
    Repo.all(from window in AccountQuotaWindow, where: window.upstream_identity_id == ^identity_id, select: {window.source, window.id}) |> Map.new()
  end

  test "owned transitions preserve ids, replace lifecycle and refuse foreign targets" do
    root = temp_journal_root!()
    assert {:ok, receipt} = Fixtures.seed("confirmed", fixture_opts(root))
    journal = Fixtures.read_journal!(receipt.journal_path)
    [identity_id] = journal["identity_ids"]
    original = Repo.get!(UpstreamIdentity, identity_id)

    for scenario <- ~w(queued processing unknown applied_pending provisional confirmed no_credit nothing_to_reset reblocked expired poll_paused) do
      assert {:ok, %{scenario: ^scenario}} = Fixtures.transition(receipt.journal_path, identity_id, scenario, fixture_opts(root))
      identity = Repo.get!(UpstreamIdentity, identity_id)
      assert identity.id == original.id
      assert identity.created_at == original.created_at
      assert identity.metadata["fixture_scenario"] == scenario
      assert Fixtures.read_journal!(receipt.journal_path)["identity_ids"] == [identity_id]
    end

    before = Repo.get!(UpstreamIdentity, identity_id)
    assert {:error, _} = Fixtures.transition(receipt.journal_path, Ecto.UUID.generate(), "confirmed", fixture_opts(root))
    assert {:error, _} = Fixtures.transition(receipt.journal_path, identity_id, "foreign", fixture_opts(root))
    assert {:error, _} = Fixtures.transition(receipt.journal_path, identity_id, "confirmed", fixture_opts(root, expected_run_fingerprint: "stale"))
    assert Repo.get!(UpstreamIdentity, identity_id) == before
    assert {:ok, _} = Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
  end

  test "synthetic operation states retain truthful result and request facts" do
    root = temp_journal_root!()
    assert {:ok, receipt} = Fixtures.seed("all", fixture_opts(root))
    journal = Fixtures.read_journal!(receipt.journal_path)
    scenarios = journal["scenario"] |> String.split(",") |> Enum.zip(journal["identity_ids"]) |> Map.new()

    for scenario <- ~w(applied_pending provisional confirmed reblocked expired) do
      identity = Repo.get!(UpstreamIdentity, scenarios[scenario])
      assert identity.metadata["saved_reset_redemption"]["result"] == %{"applied" => true, "code" => "reset"}
    end

    for scenario <- ~w(no_credit nothing_to_reset) do
      identity = Repo.get!(UpstreamIdentity, scenarios[scenario])
      assert identity.metadata["saved_reset_redemption"]["result"] == %{"applied" => false, "code" => scenario}
      refute identity.metadata["saved_reset_redemption"]["consumed_at"]
      refute identity.metadata["saved_reset_redemption"]["deadline_at"]
    end

    assert Repo.get!(UpstreamIdentity, scenarios["no_credit"]).metadata["saved_resets"]["available_count"] == 0
    assert Repo.get!(UpstreamIdentity, scenarios["provisional"]).metadata["saved_reset_redemption"]["phase"] == "confirmed_by_upstream"
    paused = Repo.get!(UpstreamIdentity, scenarios["poll_paused"])
    assert [_pause] = UsagePollCooldown.active_pauses(paused.metadata, UsagePollCooldown.current_scope(paused), DateTime.utc_now())
    assert Repo.aggregate(from(job in Oban.Job, where: job.args["manual_request_target"]["upstream_identity_id"] in ^journal["identity_ids"]), :count) == 2
    assert {:ok, _} = Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
    assert Repo.aggregate(from(job in Oban.Job, where: job.args["manual_request_target"]["upstream_identity_id"] in ^journal["identity_ids"]), :count) == 0
  end

  test "serializes concurrent fixture holders with a PostgreSQL advisory lock" do
    parent = self()

    first =
      Task.async(fn ->
        Fixtures.with_advisory_lock(Repo.config(), fn ->
          send(parent, :fixture_lock_held)

          receive do
            :release_fixture_lock -> :ok
          end
        end)
      end)

    assert_receive :fixture_lock_held

    assert {:error, "another saved-reset confirmation fixture run is active"} =
             Fixtures.with_advisory_lock(Repo.config(), fn -> :unexpected end)

    send(first.pid, :release_fixture_lock)
    assert {:ok, :ok} = Task.await(first)
  end

  # Pointing the lock at a dead port is the scenario, so Postgrex's connect
  # retries are expected output rather than a symptom worth printing.
  @tag :capture_log
  test "fails closed when the advisory lock database is unavailable" do
    unavailable_config =
      Repo.config() |> Keyword.put(:hostname, "127.0.0.1") |> Keyword.put(:port, 1)

    assert {:error, "saved-reset confirmation fixture lock could not connect"} =
             Fixtures.with_advisory_lock(unavailable_config, fn -> :unexpected end)
  end

  test "a browser-auth crash retains a metadata-only journal that resumes exact cleanup" do
    root = temp_journal_root!()

    assert {:error, _reason} =
             Fixtures.seed(
               "confirmed",
               fixture_opts(root, browser_auth: true, crash_after: :browser_auth_actor)
             )

    [journal_path] =
      root
      |> Path.join("*.json")
      |> Path.wildcard()
      |> Enum.reject(&String.ends_with?(&1, ".browser-auth.json"))

    journal = Fixtures.read_journal!(journal_path)

    assert Map.keys(journal) |> Enum.sort() ==
             ~w(actor_membership_ids actor_operator_pool_assignment_ids actor_user_ids assignment_ids browser_auth_path identity_ids pool_ids run_fingerprint scenario status)

    assert journal["status"] == "seeding"
    assert [_user_id] = journal["actor_user_ids"]
    assert [_membership_id] = journal["actor_membership_ids"]
    assert [_operator_assignment_id] = journal["actor_operator_pool_assignment_ids"]
    assert [auth_path] = Path.wildcard(Path.join(root, "*.browser-auth.json"))
    assert {:ok, %File.Stat{mode: mode}} = File.stat(auth_path)
    assert Bitwise.band(mode, 0o777) == 0o600

    assert {:ok, %{cleanup: "exact_owned_rows_removed"}} =
             Fixtures.cleanup(journal_path, fixture_opts(root))

    refute File.exists?(journal_path)
    refute File.exists?(auth_path)
  end

  test "cleanup deletes only journaled rows and retains unrelated sentinels" do
    sentinel = active_upstream_assignment_fixture(pool_fixture())
    root = temp_journal_root!()

    assert {:ok, receipt} = Fixtures.seed("all", fixture_opts(root))
    assert receipt.status == "ready"
    assert receipt.scenario_count > 1

    journal = Fixtures.read_journal!(receipt.journal_path)
    assert Enum.all?(journal["pool_ids"], &Repo.get(Pool, &1))
    assert Enum.all?(journal["identity_ids"], &Repo.get(UpstreamIdentity, &1))
    assert Enum.all?(journal["assignment_ids"], &Repo.get(PoolUpstreamAssignment, &1))

    assert {:ok, %{cleanup: "exact_owned_rows_removed"}} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))

    assert Repo.get(Pool, sentinel.assignment.pool_id)
    assert Repo.get(UpstreamIdentity, sentinel.identity.id)
    assert Repo.get(PoolUpstreamAssignment, sentinel.assignment.id)
  end

  test "browser-auth seed journals a disposable instance admin and its private auth file" do
    sentinel = active_upstream_assignment_fixture(pool_fixture())
    root = temp_journal_root!()

    assert {:ok, receipt} = Fixtures.seed("confirmed", fixture_opts(root, browser_auth: true))

    journal = Fixtures.read_journal!(receipt.journal_path)

    assert Path.basename(receipt.browser_auth_path) == journal["browser_auth_path"]
    assert [user_id] = journal["actor_user_ids"]
    assert [membership_id] = journal["actor_membership_ids"]
    assert [operator_assignment_id] = journal["actor_operator_pool_assignment_ids"]
    assert %User{password_change_required: false, status: "active"} = Repo.get(User, user_id)

    assert %Membership{user_id: ^user_id, role: "instance_admin", status: "active"} =
             Repo.get(Membership, membership_id)

    assert %OperatorPoolAssignment{user_id: ^user_id, pool_id: pool_id, status: "active"} =
             Repo.get(OperatorPoolAssignment, operator_assignment_id)

    assert pool_id in journal["pool_ids"]
    assert File.exists?(receipt.browser_auth_path)

    assert {:ok, %File.Stat{mode: mode}} = File.stat(receipt.browser_auth_path)
    assert Bitwise.band(mode, 0o777) == 0o600

    assert {:ok, %{"email" => email, "password" => password}} =
             receipt.browser_auth_path
             |> File.read()
             |> then(fn result ->
               with {:ok, auth} <- result, do: CodexPooler.JSON.decode(auth)
             end)

    assert {:ok, %{user: %User{id: ^user_id}}} =
             Accounts.login_user(%{"email" => email, "password" => password})

    assert {:ok, %{cleanup: "exact_owned_rows_removed"}} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))

    refute File.exists?(receipt.browser_auth_path)
    refute File.exists?(receipt.journal_path)
    refute Repo.get(User, user_id)
    refute Repo.get(Membership, membership_id)
    refute Repo.get(OperatorPoolAssignment, operator_assignment_id)
    assert Repo.get(Pool, sentinel.assignment.pool_id)
    assert Repo.get(UpstreamIdentity, sentinel.identity.id)
    assert Repo.get(PoolUpstreamAssignment, sentinel.assignment.id)

    assert {:error, "saved-reset confirmation fixture journal does not exist"} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
  end

  test "browser-auth cleanup fails closed for missing or foreign auth paths" do
    root = temp_journal_root!()

    assert {:ok, receipt} = Fixtures.seed("confirmed", fixture_opts(root, browser_auth: true))
    journal = Fixtures.read_journal!(receipt.journal_path)
    [user_id] = journal["actor_user_ids"]

    File.write!(
      receipt.journal_path,
      CodexPooler.JSON.encode!(Map.delete(journal, "browser_auth_path"))
    )

    assert {:error, "invalid saved-reset confirmation fixture journal"} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))

    assert Repo.get(User, user_id)

    File.write!(
      receipt.journal_path,
      CodexPooler.JSON.encode!(Map.put(journal, "browser_auth_path", "foreign.browser-auth.json"))
    )

    assert {:error, "invalid saved-reset confirmation fixture browser auth file"} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))

    assert Repo.get(User, user_id)

    File.write!(receipt.journal_path, CodexPooler.JSON.encode!(journal))

    assert {:ok, %{cleanup: "exact_owned_rows_removed"}} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
  end

  test "browser-auth cleanup retains owned rows when the private auth file is missing" do
    root = temp_journal_root!()

    assert {:ok, receipt} = Fixtures.seed("confirmed", fixture_opts(root, browser_auth: true))
    journal = Fixtures.read_journal!(receipt.journal_path)
    [user_id] = journal["actor_user_ids"]

    File.rm!(receipt.browser_auth_path)

    assert {:error, "invalid saved-reset confirmation fixture browser auth file"} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))

    assert Repo.get(User, user_id)
    assert File.exists?(receipt.journal_path)
  end

  test "fixture scenarios render actionable confirmation states and hide healthy completion" do
    root = temp_journal_root!()

    assert {:ok, receipt} = Fixtures.seed("all", fixture_opts(root, browser_auth: true))

    assert_scenario_confirmation!(receipt, "candidate_progression", :awaiting_confirmation,
      state_label: "Awaiting confirmation",
      routing_label: "Routing paused",
      blocker_state: :none,
      blocker_label: "None"
    )

    assert_confirmed_scenario_hidden!(receipt)

    assert_scenario_confirmation!(receipt, "expired", :confirmation_expired,
      state_label: "Confirmation expired",
      routing_label: "Routing pause released",
      blocker_state: :none,
      blocker_label: "None"
    )

    assert_scenario_confirmation!(receipt, "not_applied", :not_applied,
      state_label: "Not applied",
      routing_label: "Routing pause released",
      blocker_state: :none,
      blocker_label: "None"
    )

    for scenario <- ["blocker_sibling", "blocker_circuit"] do
      assert_scenario_confirmation!(receipt, scenario, :awaiting_confirmation,
        state_label: "Awaiting confirmation",
        routing_label: "Routing paused",
        blocker_state: :exhausted,
        blocker_label: "Exhausted"
      )
    end

    assert {:ok, %{cleanup: "exact_owned_rows_removed"}} =
             Fixtures.cleanup(receipt.journal_path, fixture_opts(root))
  end

  test "fixture task rejects unknown scenarios before it creates a journal or rows" do
    root = temp_journal_root!()
    sentinel = active_upstream_assignment_fixture(pool_fixture())

    assert {:error, "unknown saved-reset confirmation fixture scenario"} =
             Fixtures.seed("malformed-scenario", fixture_opts(root, browser_auth: true))

    assert Path.wildcard(Path.join(root, "*")) == []
    assert Repo.get(Pool, sentinel.assignment.pool_id)
    assert Repo.get(UpstreamIdentity, sentinel.identity.id)
    assert Repo.get(PoolUpstreamAssignment, sentinel.assignment.id)
  end

  test "the real confirmation projection omits a noncanonical phase" do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    assert QuotaProjection.saved_reset_confirmation(
             %{"phase" => "not_applied"},
             [],
             [],
             now
           ) == nil
  end

  defp fixture_opts(root, extra \\ []) do
    [environment: :test, allow_test_database: true, journal_root: root] ++ extra
  end

  defp assert_scenario_confirmation!(receipt, scenario, expected_state, expected) do
    account = scenario_account!(receipt, scenario)

    assert %{confirmation_state: ^expected_state} = account.saved_reset_confirmation

    html =
      render_component(&SavedResetMeter.saved_reset_meter/1,
        id: "fixture-#{scenario}",
        saved_resets: account.saved_resets,
        saved_reset_policy: account.saved_reset_policy,
        saved_reset_confirmation: account.saved_reset_confirmation
      )

    document = LazyHTML.from_fragment(html)
    state = Atom.to_string(expected_state)
    blocker_state = expected |> Keyword.fetch!(:blocker_state) |> Atom.to_string()

    assert LazyHTML.query(
             document,
             "[data-role='upstream-saved-reset-confirmation-state'][data-confirmation-state='#{state}']"
           )
           |> LazyHTML.text()
           |> String.trim() == Keyword.fetch!(expected, :state_label)

    assert LazyHTML.query(
             document,
             "[data-role='upstream-saved-reset-additional-blocker'][data-blocker-state='#{blocker_state}']"
           )
           |> LazyHTML.text()
           |> String.trim() == Keyword.fetch!(expected, :blocker_label)

    assert LazyHTML.query(
             document,
             "[data-role='upstream-saved-reset-routing-pause'][data-routing-paused='#{expected_state == :awaiting_confirmation}']"
           )
           |> LazyHTML.text()
           |> String.trim() == Keyword.fetch!(expected, :routing_label)

    assert LazyHTML.query(document, "[data-role='upstream-saved-reset-confirmation-summary']")
           |> LazyHTML.text()
           |> String.trim() != ""
  end

  defp assert_confirmed_scenario_hidden!(receipt) do
    account = scenario_account!(receipt, "confirmed")

    assert %{confirmation_state: :confirmed} = account.saved_reset_confirmation

    html =
      render_component(&SavedResetMeter.saved_reset_meter/1,
        id: "fixture-confirmed",
        saved_resets: account.saved_resets,
        saved_reset_policy: account.saved_reset_policy,
        saved_reset_confirmation: account.saved_reset_confirmation
      )

    document = LazyHTML.from_fragment(html)

    refute LazyHTML.query(document, "[data-role='upstream-saved-reset-confirmation']")
           |> Enum.any?()

    for index <- 1..5 do
      segment = LazyHTML.query(document, "#fixture-confirmed-segment-#{index}")

      assert LazyHTML.attribute(segment, "data-confirmation-state") == []
      assert LazyHTML.attribute(segment, "title") == []
    end
  end

  defp scenario_account!(receipt, scenario) do
    journal = Fixtures.read_journal!(receipt.journal_path)
    [user_id] = journal["actor_user_ids"]
    scope = User |> Repo.get!(user_id) |> Scope.for_user()
    pools = Pools.list_visible_pools(scope)

    identity_id =
      journal["scenario"]
      |> String.split(",")
      |> Enum.zip(journal["identity_ids"])
      |> Map.new()
      |> Map.fetch!(scenario)

    assert [account] =
             UpstreamAccountsReadModel.list_visible_accounts(scope, pools, %{
               identity_id: identity_id
             })

    account
  end

  defp temp_journal_root! do
    root =
      Path.join(System.tmp_dir!(), "saved-reset-fixtures-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf!(root) end)
    root
  end
end
