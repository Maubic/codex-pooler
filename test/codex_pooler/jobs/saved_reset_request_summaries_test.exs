defmodule CodexPooler.Jobs.SavedResetRequestSummariesTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Jobs
  alias CodexPooler.Jobs.{RuntimeStateCleanupWorker, SavedResetRedemptionWorker}
  alias CodexPooler.Pools.{Membership, OperatorPoolAssignment}
  alias CodexPooler.Upstreams.Assignments.PoolAssignments
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures

  @time ~U[2026-05-04 10:00:00.000000Z]

  setup do
    %{user: owner} = bootstrap_owner_fixture(%{"email" => unique_user_email()})
    %{owner: owner, scope: Scope.for_user(owner)}
  end

  test "scoped_batch_manual_requests selects at most two rows per identity with one job query", %{scope: scope} do
    pool = pool_fixture()

    identities =
      for _index <- 1..20 do
        %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool)

        for offset <- 1..5 do
          insert_request(assignment, state: "available", inserted_at: DateTime.add(@time, offset, :second))
          insert_request(assignment, state: "completed", inserted_at: DateTime.add(@time, offset, :second))
        end

        identity
      end

    {single, single_queries} = capture_job_queries(fn -> Jobs.saved_reset_request_summaries(scope, [hd(identities).id]) end)
    {batch, batch_queries} = capture_job_queries(fn -> Jobs.saved_reset_request_summaries(scope, Enum.map(identities, & &1.id)) end)

    assert map_size(single) == 1
    assert map_size(batch) == 20
    assert [%{selected_rows: 2}] = single_queries
    assert [%{selected_rows: 40}] = batch_queries

    CodexPooler.TestDiagnostics.puts(Jason.encode!(%{scenario: "scoped_batch_manual_requests", single_identity_count: map_size(single), batch_identity_count: map_size(batch), single_job_select_count: length(single_queries), batch_job_select_count: length(batch_queries), single_selected_rows: hd(single_queries).selected_rows, batch_selected_rows: hd(batch_queries).selected_rows}))

    for identity <- identities do
      assert batch[identity.id] == %{
               open: %{state: :queued, requested_at: DateTime.add(@time, 5, :second), scheduled_at: @time},
               latest_terminal: %{state: :completed, requested_at: DateTime.add(@time, 5, :second), scheduled_at: @time}
             }
    end
  end

  test "inserted time and private id break ties independently of scheduled time", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()
    insert_request(assignment, inserted_at: @time, state: "scheduled", scheduled_at: DateTime.add(@time, 3600, :second))
    newer = DateTime.add(@time, 1, :second)
    insert_request(assignment, inserted_at: newer, state: "available")
    insert_request(assignment, inserted_at: newer, state: "executing", scheduled_at: DateTime.add(@time, -3600, :second))

    assert %{open: %{state: :processing, requested_at: ^newer, scheduled_at: scheduled}, latest_terminal: nil} =
             Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id]

    assert scheduled == DateTime.add(@time, -3600, :second)
  end

  test "hidden_and_stale_job_context authorizes exact pool assignment despite a shared identity", %{owner: owner} do
    owner_scope = Scope.for_user(owner)
    %{user: operator} = operator_fixture(owner_scope)
    visible_pool = pool_fixture()
    hidden_pool = pool_fixture()
    operator_pool_assignment_fixture(operator, visible_pool)
    operator_scope = Scope.for_user(operator)
    %{identity: identity, assignment: visible_assignment} = upstream_assignment_fixture(visible_pool)
    hidden_assignment = second_assignment(identity, hidden_pool)
    %{identity: hidden_identity, assignment: hidden_only} = upstream_assignment_fixture(hidden_pool)
    insert_request(visible_assignment, state: "available", inserted_at: @time)
    insert_request(hidden_assignment, state: "executing", inserted_at: DateTime.add(@time, 60, :second))
    insert_request(hidden_only, state: "completed", inserted_at: @time)

    summary = Jobs.saved_reset_request_summaries(operator_scope, [identity.id, hidden_identity.id])
    assert Map.keys(summary) == [identity.id]
    assert summary[identity.id].open.state == :queued
    assert Jobs.saved_reset_request_summaries(operator_scope, [identity.id], pool_ids: [hidden_pool.id]) == %{}
    assert Jobs.list_latest_jobs(operator_scope) == []
    assert Jobs.saved_reset_request_summaries(owner_scope, [identity.id])[identity.id].open.state == :processing

    from(row in OperatorPoolAssignment, where: row.user_id == ^operator.id)
    |> Repo.update_all(set: [status: "revoked"])

    assert Jobs.saved_reset_request_summaries(operator_scope, [identity.id]) == %{}
  end

  test "revoked role and deleted assignment invalidate stale scopes", %{scope: scope} do
    %{user: operator} = operator_fixture(scope)
    pool = pool_fixture()
    operator_pool_assignment_fixture(operator, pool)
    operator_scope = Scope.for_user(operator)
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool)
    insert_request(assignment)
    assert Jobs.saved_reset_request_summaries(operator_scope, [identity.id])[identity.id].open

    from(row in Membership, where: row.user_id == ^operator.id)
    |> Repo.update_all(set: [status: "revoked"])

    assert Jobs.saved_reset_request_summaries(operator_scope, [identity.id]) == %{}

    from(row in PoolUpstreamAssignment, where: row.id == ^assignment.id)
    |> Repo.update_all(set: [status: "deleted"])

    assert Jobs.saved_reset_request_summaries(scope, [identity.id]) == %{}
  end

  test "deleted identities and inactive pools omit retained manual request context", %{scope: scope} do
    pool = pool_fixture()
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool)
    insert_request(assignment)
    assert Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id].open

    from(row in UpstreamIdentity, where: row.id == ^identity.id)
    |> Repo.update_all(set: [status: "deleted"])

    assert Jobs.saved_reset_request_summaries(scope, [identity.id]) == %{}

    from(row in UpstreamIdentity, where: row.id == ^identity.id)
    |> Repo.update_all(set: [status: "active"])

    from(row in CodexPooler.Pools.Pool, where: row.id == ^pool.id)
    |> Repo.update_all(set: [status: "disabled"])

    assert Jobs.saved_reset_request_summaries(scope, [identity.id]) == %{}
  end

  test "new open manual requests retain older terminal context independently", %{scope: scope} do
    pool = pool_fixture()
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(pool)
    another_assignment = second_assignment(identity, pool_fixture())
    old_time = DateTime.add(@time, -60, :second)
    insert_request(assignment, state: "completed", inserted_at: old_time)
    insert_request(another_assignment, state: "available", inserted_at: @time)
    insert_request(assignment, state: "executing", inserted_at: DateTime.add(@time, -120, :second), args: %{"pool_upstream_assignment_id" => assignment.id, "trigger_kind" => "stale_consuming_recovery", "recovery_kind" => "stale_consuming"})

    summary = Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id]
    assert summary.open == %{state: :queued, requested_at: @time, scheduled_at: @time}
    assert summary.latest_terminal == %{state: :completed, requested_at: old_time, scheduled_at: @time}
    assert Jobs.saved_reset_request_summaries(scope, [identity.id], pool_ids: [pool.id])[identity.id].open == nil
  end

  test "empty and malformed scope targets fail closed without any job read", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()
    insert_request(assignment)

    for {target_scope, ids, opts} <- [
          {nil, [identity.id], []},
          {:system, [identity.id], []},
          {%Scope{}, [identity.id], []},
          {%Scope{user: %{id: "malformed"}}, [identity.id], []},
          {scope, [], []},
          {scope, [nil, "malformed", %{}, 12], []},
          {scope, identity.id, []},
          {scope, [identity.id | :malformed], []},
          {scope, [identity.id], [pool_ids: []]},
          {scope, [identity.id], [pool_ids: nil]},
          {scope, [identity.id], [pool_ids: [identity.id | :malformed]]},
          {scope, [identity.id], [pool_ids: [nil, "malformed"]]},
          {scope, [identity.id], [pool_ids: "malformed"]},
          {scope, [identity.id], %{}},
          {scope, [identity.id], [:malformed]},
          {scope, [identity.id], [{:pool_ids, []} | :malformed]}
        ] do
      {result, queries} = capture_job_queries(fn -> Jobs.saved_reset_request_summaries(target_scope, ids, opts) end)
      assert result == %{}
      assert queries == []
    end

    assert Map.keys(Jobs.saved_reset_request_summaries(scope, [identity.id, identity.id, nil])) == [identity.id]
  end

  test "scheduled recovery malformed and unrelated jobs cannot masquerade as manual requests", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()

    bound_args = request_args(assignment)

    for args <- [
          Map.put(bound_args, "trigger_kind", "scheduled_expiry_rescue"),
          Map.put(bound_args, "recovery_kind", "stale_consuming"),
          Map.put(bound_args, "pool_upstream_assignment_id", "malformed"),
          Map.put(bound_args, "pool_upstream_assignment_id", %{"id" => assignment.id}),
          Map.delete(bound_args, "trigger_kind"),
          Map.update!(bound_args, "manual_request_target", &Map.put(&1, "upstream_identity_id", Ecto.UUID.generate())),
          Map.update!(bound_args, "manual_request_target", &Map.put(&1, "pool_id", Ecto.UUID.generate()))
        ] do
      insert_request(assignment, args: args)
    end

    insert_request(assignment, worker: RuntimeStateCleanupWorker)
    assert Jobs.saved_reset_request_summaries(scope, [identity.id]) == %{identity.id => %{open: nil, latest_terminal: nil}}

    insert_request(assignment, state: "retryable", inserted_at: @time)
    assert Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id].open.state == :queued
  end

  test "terminal and pruned jobs are safe request context without provider or actor claims", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()

    # The terminal kind is how the job ended; it never states the provider outcome.
    for {state, kind} <- [{"completed", :completed}, {"cancelled", :cancelled}, {"discarded", :discarded}] do
      job = insert_request(assignment, state: state, args: Map.put(request_args(assignment), "synthetic_private_field", "private-job-context"))
      from(row in Oban.Job, where: row.id == ^job.id) |> Repo.update_all(set: [errors: [%{"error" => "synthetic-private-error"}]])
      result = Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id]
      assert result.open == nil
      assert result.latest_terminal == %{state: kind, requested_at: @time, scheduled_at: @time}
      assert Map.keys(result.latest_terminal) |> Enum.sort() == [:requested_at, :scheduled_at, :state]
      refute inspect(result) =~ "private-job-context"
      refute inspect(result) =~ "synthetic-private-error"
      refute inspect(result) =~ assignment.id
      refute Map.has_key?(result.latest_terminal, :provider_outcome)
      refute Map.has_key?(result.latest_terminal, :actor)
      Repo.delete!(job)
      assert Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id] == %{open: nil, latest_terminal: nil}
    end
  end

  test "a suspended job is held before it runs and stays an open request", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()
    insert_request(assignment, state: "completed", inserted_at: DateTime.add(@time, -60, :second))
    insert_request(assignment, state: "suspended", inserted_at: @time)

    # The worker's uniqueness treats a suspended job as incomplete, so a new submission would join it rather than start.
    assert Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id] == %{
             open: %{state: :queued, requested_at: @time, scheduled_at: @time},
             latest_terminal: %{state: :completed, requested_at: DateTime.add(@time, -60, :second), scheduled_at: @time}
           }
  end

  test "real manual enqueue is visible only through its trusted persisted target", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()
    assert {:ok, job} = Jobs.enqueue_saved_reset_redemption(assignment)
    job = Repo.reload!(job)

    summary = Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id]
    assert summary == %{open: %{state: :queued, requested_at: job.inserted_at, scheduled_at: job.scheduled_at}, latest_terminal: nil}
  end

  test "legacy manual requests without both target bindings are excluded", %{scope: scope} do
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture()
    bound_args = request_args(assignment)

    for args <- [
          Map.delete(bound_args, "manual_request_target"),
          bound_args |> Map.delete("manual_request_target") |> Map.put("pool_id", assignment.pool_id) |> Map.put("upstream_identity_id", assignment.upstream_identity_id),
          Map.put(bound_args, "manual_request_target", nil),
          Map.put(bound_args, "manual_request_target", "malformed"),
          Map.put(bound_args, "manual_request_target", []),
          Map.update!(bound_args, "manual_request_target", &Map.delete(&1, "pool_id")),
          Map.update!(bound_args, "manual_request_target", &Map.delete(&1, "upstream_identity_id")),
          Map.update!(bound_args, "manual_request_target", &Map.put(&1, "pool_id", nil)),
          Map.update!(bound_args, "manual_request_target", &Map.put(&1, "upstream_identity_id", nil))
        ] do
      insert_request(assignment, args: args)
    end

    assert Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id] == %{open: nil, latest_terminal: nil}
    insert_request(assignment)
    assert Jobs.saved_reset_request_summaries(scope, [identity.id])[identity.id].open.state == :queued
  end

  test "moving an assignment from a hidden pool cannot expose its originally bound request", %{scope: scope} do
    %{user: operator} = operator_fixture(scope)
    hidden_pool = pool_fixture()
    visible_pool = pool_fixture()
    operator_pool_assignment_fixture(operator, visible_pool)
    operator_scope = Scope.for_user(operator)
    %{identity: identity, assignment: assignment} = upstream_assignment_fixture(hidden_pool)
    insert_request(assignment)
    assert Jobs.saved_reset_request_summaries(operator_scope, [identity.id]) == %{}

    assert {:ok, _assignment} = PoolAssignments.update_pool_assignment(assignment, %{pool_id: visible_pool.id})
    assert Jobs.saved_reset_request_summaries(operator_scope, [identity.id])[identity.id] == %{open: nil, latest_terminal: nil}
  end

  test "retargeting an assignment cannot attribute a retained request to its new identity", %{scope: scope} do
    %{identity: original_identity, assignment: assignment} = upstream_assignment_fixture()
    new_identity = upstream_identity_fixture(%{status: "active"})
    insert_request(assignment, state: "completed")

    assert {:ok, _assignment} = PoolAssignments.update_pool_assignment(assignment, %{upstream_identity_id: new_identity.id})
    assert Jobs.saved_reset_request_summaries(scope, [original_identity.id]) == %{}
    assert Jobs.saved_reset_request_summaries(scope, [new_identity.id])[new_identity.id] == %{open: nil, latest_terminal: nil}
  end

  defp request_args(assignment) do
    %{
      "pool_upstream_assignment_id" => assignment.id,
      "trigger_kind" => "admin_manual",
      "manual_request_target" => %{
        "upstream_identity_id" => assignment.upstream_identity_id,
        "pool_id" => assignment.pool_id
      }
    }
  end

  defp insert_request(assignment, opts \\ []) do
    args = Keyword.get(opts, :args, request_args(assignment))
    worker = Keyword.get(opts, :worker, SavedResetRedemptionWorker)
    assert {:ok, job} = args |> worker.new(unique: false) |> Oban.insert()
    updates = [state: Keyword.get(opts, :state, "available"), inserted_at: Keyword.get(opts, :inserted_at, @time), scheduled_at: Keyword.get(opts, :scheduled_at, @time)]
    from(row in Oban.Job, where: row.id == ^job.id) |> Repo.update_all(set: updates)
    Repo.reload!(job)
  end

  defp second_assignment(identity, pool) do
    %{assignment: assignment} = upstream_assignment_fixture(pool)
    assignment |> Ecto.Changeset.change(upstream_identity_id: identity.id) |> Repo.update!()
  end

  defp capture_job_queries(fun) do
    test_pid = self()
    handler_id = {__MODULE__, test_pid, System.unique_integer([:positive])}
    on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :repo, :query],
        fn _event, _measurements, metadata, _config ->
          if self() == test_pid and metadata[:repo] == Repo and is_binary(metadata[:query]) and String.starts_with?(metadata.query, "SELECT") and String.contains?(metadata.query, "\"oban_jobs\"") do
            {:ok, %{num_rows: selected_rows}} = metadata.result
            send(test_pid, {handler_id, %{selected_rows: selected_rows}})
          end
        end,
        nil
      )

    try do
      result = fun.()
      {result, drain_query_events(handler_id, [])}
    after
      :telemetry.detach(handler_id)
      refute Enum.any?(:telemetry.list_handlers([:codex_pooler, :repo, :query]), &(&1.id == handler_id))
    end
  end

  defp drain_query_events(handler_id, acc) do
    receive do
      {^handler_id, event} -> drain_query_events(handler_id, [event | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
