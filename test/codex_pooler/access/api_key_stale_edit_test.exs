defmodule CodexPooler.Access.APIKeyStaleEditTest do
  use CodexPooler.DataCase, async: false

  import CodexPooler.AccountsFixtures

  alias CodexPooler.Access
  alias CodexPooler.Access.{APIKey, APIKeyPolicyBinding}
  alias CodexPooler.Access.APIKeys.{RuntimeAuthorization, TouchDebounce}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.Pools
  alias CodexPoolerWeb.Admin.ApiKeyPolicyForm

  setup do
    %{user: owner} = bootstrap_owner_fixture()
    scope = Scope.for_user(owner)
    {:ok, pool} = Pools.create_pool(scope, %{slug: "edit-#{System.unique_integer([:positive])}", name: "Edit Pool"})
    {:ok, %{api_key: key, raw_key: raw_key}} = Access.create_api_key(scope, pool, %{display_name: "Editable key", model_mode: "selected_models", allowed_model_identifiers: ["gpt-alpha", "gpt-beta"], default_policy: %{max_requests_per_minute: 30}, metadata: %{"operator_notes" => "Original"}})
    %{scope: scope, pool: pool, key: key, raw_key: raw_key}
  end

  test "stale full form cannot undo a pause or restore runtime admission", %{scope: scope, key: key} do
    attrs = form_attrs(scope, key.id)
    {:ok, paused} = Access.pause_api_key(scope, key.id)
    outcome = update_outcome(Access.update_api_key_with_policy(scope, key.id, attrs))
    current = Repo.get!(APIKey, key.id)
    admitted? = runtime_admitted?(key.id)
    assert %{outcome: outcome, status: current.status, admitted?: admitted?, epoch: current.runtime_revocation_epoch} == %{outcome: :api_key_edit_conflict, status: "paused", admitted?: false, epoch: paused.runtime_revocation_epoch}
  end

  test "stale full form cannot undo an allow-list narrowing", %{scope: scope, key: key} do
    attrs = form_attrs(scope, key.id)
    {:ok, _} = Access.update_api_key_with_policy(scope, key.id, %{model_mode: "selected_models", allowed_model_identifiers: ["gpt-alpha"]})
    outcome = update_outcome(Access.update_api_key_with_policy(scope, key.id, attrs))
    current = Repo.get!(APIKey, key.id)
    denied? = match?({:error, _}, Access.authorize_api_key_policy(current, %{model: "gpt-beta"}))
    assert %{outcome: outcome, allowed: current.allowed_model_identifiers, denied?: denied?} == %{outcome: :api_key_edit_conflict, allowed: ["gpt-alpha"], denied?: true}
  end

  for writer <- [:bindings, :model_bindings, :direct_row, :pool_move, :expiry, :metadata] do
    test "#{writer} changes invalidate a whole-form edit without a second write", %{scope: scope, key: key} do
      attrs = form_attrs(scope, key.id)

      case unquote(writer) do
        :bindings ->
          {:ok, _} = Access.update_api_key_with_policy(scope, key.id, %{default_policy: %{max_requests_per_minute: 5}})

        :model_bindings ->
          {:ok, _} = Access.update_api_key_with_policy(scope, key.id, %{model_policies: [%{model_identifier: "gpt-alpha", max_tokens_per_day: 50}]})

        :direct_row ->
          {:ok, _} = Access.update_api_key(scope, key.id, %{display_name: "Other writer"})

        :pool_move ->
          {:ok, destination} = Pools.create_pool(scope, %{slug: "move-#{System.unique_integer([:positive])}", name: "Destination"})
          :ok = Access.assign_api_keys_to_pool(scope, destination, [key.id])

        :expiry ->
          {:ok, _} = Access.update_api_key(scope, key.id, %{expires_at: DateTime.add(DateTime.utc_now(), 3600)})

        :metadata ->
          {:ok, _} = Access.update_api_key(scope, key.id, %{metadata: %{"operator_notes" => "Other notes"}})
      end

      before_rows = snapshot_rows(key.id)
      assert update_outcome(Access.update_api_key_with_policy(scope, key.id, attrs)) == :api_key_edit_conflict
      unchanged? = before_rows == snapshot_rows(key.id)
      assert unchanged?
    end
  end

  test "reopened form permits authorized status edits and intentional clears", %{scope: scope, key: key} do
    {:ok, _} = Access.pause_api_key(scope, key.id)
    {:ok, %{api_key: paused, policy_bindings: bindings}} = Access.get_api_key_with_policy(scope, key.id)
    attrs = paused |> ApiKeyPolicyForm.params_for(bindings) |> ApiKeyPolicyForm.merge_params(%{"status" => "active", "model_mode" => "all_models", "operator_notes" => "", "default_max_requests_per_minute" => ""}) |> ApiKeyPolicyForm.attrs()
    assert update_outcome(Access.update_api_key_with_policy(scope, key.id, attrs)) == :updated
    current = Repo.get!(APIKey, key.id)
    assert current.status == "active"
    assert is_nil(current.allowed_model_identifiers)
    assert is_nil(current.metadata["operator_notes"])
    assert runtime_admitted?(key.id)
    assert is_nil(Repo.one!(from b in APIKeyPolicyBinding, where: b.api_key_id == ^key.id).max_requests_per_minute)
  end

  test "usage, rotation and semantically unchanged binding replacement keep a form current", %{scope: scope, key: key, raw_key: raw_key} do
    attrs = form_attrs(scope, key.id)
    assert {:ok, _} = Access.authenticate_api_key(raw_key)
    assert :ok = TouchDebounce.flush()
    refute is_nil(Repo.get!(APIKey, key.id).last_used_at)
    {:ok, _} = Access.rotate_api_key(scope, key.id)
    {:ok, _} = Access.update_api_key_with_policy(scope, key.id, %{default_policy: %{max_requests_per_minute: 30}})
    assert update_outcome(Access.update_api_key_with_policy(scope, key.id, attrs)) == :updated
  end

  test "form precondition cannot be replaced or omitted by a client edit", %{scope: scope, key: key} do
    {:ok, %{api_key: current, policy_bindings: bindings}} = Access.get_api_key_with_policy(scope, key.id)
    params = ApiKeyPolicyForm.params_for(current, bindings)
    merged = ApiKeyPolicyForm.merge_params(params, %{"stored_edit_revision" => nil, "expected_edit_revision" => "forged"})
    assert ApiKeyPolicyForm.attrs(merged).expected_edit_revision == ApiKeyPolicyForm.attrs(params).expected_edit_revision
    missing = params |> Map.delete("stored_edit_revision") |> ApiKeyPolicyForm.attrs()
    assert update_outcome(Access.update_api_key_with_policy(scope, key.id, missing)) == :api_key_edit_conflict
    %{user: admin} = operator_fixture(scope, %{"password_change_required" => "false"})
    assert update_outcome(Access.update_api_key_with_policy(Scope.for_user(admin), key.id, missing)) == :api_key_not_found
    assert update_outcome(Access.update_api_key_with_policy(scope, "not-a-uuid", missing)) == :api_key_not_found
  end

  defp snapshot_rows(id) do
    {Repo.get!(APIKey, id), Repo.all(from b in APIKeyPolicyBinding, where: b.api_key_id == ^id, order_by: b.id), Repo.all(from a in AuditEvent, where: a.target_id == ^id, order_by: a.id)}
  end

  defp form_attrs(scope, id) do
    {:ok, %{api_key: key, policy_bindings: bindings}} = Access.get_api_key_with_policy(scope, id)
    key |> ApiKeyPolicyForm.params_for(bindings) |> ApiKeyPolicyForm.attrs()
  end

  defp update_outcome({:ok, _}), do: :updated
  defp update_outcome({:error, %{code: code}}), do: code
  defp update_outcome({:error, _}), do: :other_error

  defp runtime_admitted?(id) do
    {:ok, admitted?} =
      Repo.transaction(fn ->
        with {:ok, epoch} <- RuntimeAuthorization.capture(id),
             {:ok, _} <- RuntimeAuthorization.authorize_turn(id, epoch) do
          true
        else
          {:error, _} -> false
        end
      end)

    admitted?
  end
end

defmodule CodexPooler.Access.APIKeyStaleEditConcurrencyTest do
  use ExUnit.Case, async: false
  use CodexPooler.CommittedWriteGuard

  import CodexPooler.AccountsFixtures
  import CodexPooler.UnboxedFixture, only: [run_unboxed: 1, register_unboxed_cleanup!: 1]
  import Ecto.Query

  alias CodexPooler.Access
  alias CodexPooler.Access.{APIKey, APIKeyPolicyBinding}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Pools
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPoolerWeb.Admin.ApiKeyPolicyForm
  alias Ecto.Adapters.SQL.Sandbox

  @budget 10_000

  test "save waits for the canonical key and binding transaction" do
    %{user: owner} = committed_bootstrap_owner_fixture!()
    slug = "stale-edit-#{Ecto.UUID.generate()}"

    register_unboxed_cleanup!(fn ->
      ids = Repo.all(from p in Pool, where: p.slug == ^slug, select: p.id)
      CodexPooler.PoolerFixtures.delete_committed_pools!(ids)
    end)

    {scope, key, attrs} =
      run_unboxed(fn ->
        scope = Scope.for_user(owner)
        {:ok, pool} = Pools.create_pool(scope, %{slug: slug, name: "Concurrent edit"})
        {:ok, %{api_key: key}} = Access.create_api_key(scope, pool, %{display_name: "Concurrent key", default_policy: %{max_requests_per_minute: 30}})
        {:ok, %{api_key: current, policy_bindings: bindings}} = Access.get_api_key_with_policy(scope, key.id)
        {scope, key, current |> ApiKeyPolicyForm.params_for(bindings) |> ApiKeyPolicyForm.attrs()}
      end)

    parent = self()
    barrier = make_ref()
    handler = {__MODULE__, barrier}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.observe_lock/4, barrier)
    supervisor = start_supervised!({Task.Supervisor, []})

    holder =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.transaction(fn ->
            {:ok, _} = Access.update_api_key_with_policy(scope, key.id, %{default_policy: %{max_requests_per_minute: 5}})
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            send(parent, {barrier, :holder, backend})

            receive do
              {^barrier, :release} -> :changed
            after
              @budget -> raise "edit writer release missing"
            end
          end)
        end)
      end)

    holder_monitor = Process.monitor(holder.pid)
    assert_receive {^barrier, :holder, holder_backend}, @budget
    assert run_unboxed(fn -> Repo.one!(from b in APIKeyPolicyBinding, where: b.api_key_id == ^key.id, select: b.max_requests_per_minute) end) == 30

    waiter =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Repo.checkout(fn ->
            %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
            Process.put({__MODULE__, :probe}, barrier)

            call = fn -> Access.update_api_key_with_policy(scope, key.id, attrs) end

            proof =
              try do
                Repo.transaction(fn ->
                  Repo.query!("SET LOCAL lock_timeout = '200ms'")
                  call.()
                end)

                :did_not_wait
              rescue
                error in Postgrex.Error -> {error.postgres.code, Process.get({__MODULE__, :lock}, :unknown)}
              end

            send(parent, {barrier, :waiter, backend, proof})

            receive do
              {^barrier, :retry} ->
                case call.() do
                  {:ok, _} -> :updated
                  {:error, %{code: code}} -> code
                end
            after
              @budget -> raise "edit waiter release missing"
            end
          end)
        end)
      end)

    waiter_monitor = Process.monitor(waiter.pid)
    assert_receive {^barrier, :waiter, waiter_backend, proof}, @budget
    send(holder.pid, {barrier, :release})
    assert {:ok, :changed} = Task.await(holder, @budget)
    send(waiter.pid, {barrier, :retry})
    outcome = Task.await(waiter, @budget)
    assert_receive {:DOWN, ^holder_monitor, :process, _, :normal}, @budget
    assert_receive {:DOWN, ^waiter_monitor, :process, _, :normal}, @budget
    assert waiter_backend != holder_backend

    assert proof == {:lock_not_available, :write}
    assert outcome == :api_key_edit_conflict

    assert run_unboxed(fn -> Repo.one!(from b in APIKeyPolicyBinding, where: b.api_key_id == ^key.id, select: b.max_requests_per_minute) end) == 5
    assert run_unboxed(fn -> Repo.get!(APIKey, key.id).status end) == "active"
  end

  test "the edit snapshot cannot mix an old key with newly committed bindings" do
    %{user: owner} = committed_bootstrap_owner_fixture!()
    slug = "snapshot-#{Ecto.UUID.generate()}"

    register_unboxed_cleanup!(fn ->
      ids = Repo.all(from p in Pool, where: p.slug == ^slug, select: p.id)
      CodexPooler.PoolerFixtures.delete_committed_pools!(ids)
    end)

    {scope, key} =
      run_unboxed(fn ->
        scope = Scope.for_user(owner)
        {:ok, pool} = Pools.create_pool(scope, %{slug: slug, name: "Snapshot Pool"})
        {:ok, %{api_key: key}} = Access.create_api_key(scope, pool, %{display_name: "Before", default_policy: %{max_requests_per_minute: 30}})
        {scope, key}
      end)

    parent = self()
    barrier = make_ref()
    handler = {__MODULE__, barrier}
    on_exit(fn -> :telemetry.detach(handler) end)
    :ok = :telemetry.attach(handler, [:codex_pooler, :repo, :query], &__MODULE__.hold_snapshot/4, {parent, barrier})
    supervisor = start_supervised!({Task.Supervisor, []})

    reader =
      Task.Supervisor.async_nolink(supervisor, fn ->
        Sandbox.unboxed_run(Repo, fn ->
          Process.put({__MODULE__, :snapshot}, barrier)
          {:ok, %{api_key: snapshot, policy_bindings: [binding]}} = Access.get_api_key_with_policy(scope, key.id)
          {snapshot.display_name, binding.max_requests_per_minute}
        end)
      end)

    monitor = Process.monitor(reader.pid)
    assert_receive {^barrier, :snapshot_read}, @budget
    {:ok, _} = run_unboxed(fn -> Access.update_api_key_with_policy(scope, key.id, %{display_name: "After", default_policy: %{max_requests_per_minute: 5}}) end)
    send(reader.pid, {barrier, :continue})
    assert Task.await(reader, @budget) == {"Before", 30}
    assert_receive {:DOWN, ^monitor, :process, _, :normal}, @budget
    {:ok, current} = run_unboxed(fn -> Access.get_api_key_with_policy(scope, key.id) end)
    assert current.api_key.display_name == "After"
    assert hd(current.policy_bindings).max_requests_per_minute == 5
  end

  @doc false
  def hold_snapshot(_event, _measurements, metadata, {parent, barrier}) do
    if Process.get({__MODULE__, :snapshot}) == barrier and String.contains?(metadata.query, ~s(FROM "api_keys")) do
      Process.delete({__MODULE__, :snapshot})
      send(parent, {barrier, :snapshot_read})

      receive do
        {^barrier, :continue} -> :ok
      after
        @budget -> raise "snapshot reader release missing"
      end
    end
  end

  @doc false
  def observe_lock(_event, _measurements, metadata, barrier) do
    if Process.get({__MODULE__, :probe}) == barrier and match?({:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}}, metadata.result) do
      query = metadata.query

      lock = if String.contains?(query, ~s(FROM "api_keys")) and String.ends_with?(query, "FOR UPDATE"), do: :write, else: :unknown

      Process.put({__MODULE__, :lock}, lock)
    end
  end
end
