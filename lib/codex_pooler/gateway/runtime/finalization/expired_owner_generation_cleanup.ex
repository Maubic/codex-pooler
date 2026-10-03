defmodule CodexPooler.Gateway.Runtime.Finalization.ExpiredOwnerGenerationCleanup do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Access
  alias CodexPooler.Accounting.{Attempt, Metadata, Request}
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, CodexTurn}
  alias CodexPooler.Platform.ExecutionIdentity
  alias CodexPooler.Platform.InstancePresence
  alias CodexPooler.Platform.InstancePresence.Identity
  alias CodexPooler.Repo

  @key "expired_owner_stop"
  @transient :expired_owner_stop_disposition
  @uuid_fields ~w(session_id pool_id api_key_id model_id request_id attempt_id lease_id turn_id stop_decision_id)
  @identity_fields ~w(owner_instance_id owner_instance_boot_id owner_process_id owner_execution_id)
  @scope_fields @uuid_fields ++ ~w(replay_generation principal_epoch lease_identity_digest session_deadline lease_deadline owner_instance_id owner_instance_boot_id process_generation task_digest task_address downstream_epoch executor producer authorized_at)
  @ended_fields ~w(observed_end_at end_kind)
  @fields @scope_fields ++ @ended_fields ++ ~w(version phase)
  @max_bytes 4096
  @deadline_key {__MODULE__, :deadline}
  @signal_key {CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession, :expired_slot_signal}
  @phase_parent {__MODULE__, :phase_parent}
  @backstop_key {__MODULE__, :stale_backstop_authority}
  @decision_key {__MODULE__, :stop_decision_id}

  @type witness :: %{required(String.t()) => term()}
  @type result :: {:ok, witness()} | :none | {:error, :invalid_expired_owner_stop}

  @spec within_deadline(integer(), (-> term())) :: term()
  def within_deadline(deadline, operation) do
    previous = Process.get(@deadline_key)
    previous_decision = Process.get(@decision_key)
    Process.put(@deadline_key, deadline)
    Process.put(@decision_key, Ecto.UUID.generate())

    try do
      if remaining_ms() > 0, do: operation.(), else: {:error, :owner_unavailable}
    after
      if previous, do: Process.put(@deadline_key, previous), else: Process.delete(@deadline_key)
      if previous_decision, do: Process.put(@decision_key, previous_decision), else: Process.delete(@decision_key)
    end
  end

  @spec remaining_ms() :: non_neg_integer()
  def remaining_ms do
    case Process.get(@deadline_key) do
      nil -> 5_000
      deadline -> max(deadline - System.monotonic_time(:millisecond), 0)
    end
  end

  @spec signal_issued() :: :ok
  def signal_issued do
    Process.put(@signal_key, true)
    if parent = Process.get(@phase_parent), do: send(elem(parent, 0), {:expired_stop_signal, elem(parent, 1)})
    :ok
  end

  @spec sql_phase((-> term()), keyword()) :: {:ok, term()} | {:error, term()}
  def sql_phase(operation, options \\ []) do
    case Process.get(@deadline_key) do
      nil -> Repo.transaction(operation)
      deadline -> bounded_sql_phase(operation, deadline, options)
    end
  end

  defp bounded_sql_phase(operation, deadline, options) do
    budget = remaining_ms()

    if budget > 0 do
      parent = self()
      ref = make_ref()
      signal_phase? = Keyword.get(options, :signal_phase, false)
      inherited_signal = Process.get(@signal_key)
      decision = Process.get(@decision_key)
      if signal_phase?, do: Process.put(@signal_key, :unknown)

      task = start_sql_phase(operation, %{parent: parent, ref: ref, deadline: deadline, decision: decision, signal: if(signal_phase?, do: false, else: inherited_signal), budget: budget})
      finish_sql_phase(task, budget, ref, signal_phase?)
    else
      {:error, :owner_unavailable}
    end
  end

  defp start_sql_phase(operation, context) do
    Task.Supervisor.async_nolink(CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession.TaskSupervisor, fn ->
      Process.put(@deadline_key, context.deadline)
      Process.put(@decision_key, context.decision)
      Process.put(@phase_parent, {context.parent, context.ref})
      Process.put(@signal_key, context.signal)
      {run_sql_operation(operation, context), Process.get(@signal_key)}
    end)
  end

  defp run_sql_operation(operation, context) do
    Repo.transaction(operation, timeout: context.budget, deadline: context.deadline, checkout_retries: 0)
  rescue
    _exception -> {:error, :owner_unavailable}
  catch
    :exit, _reason -> {:error, :owner_unavailable}
  end

  defp finish_sql_phase(task, budget, ref, signal_phase?) do
    case Task.yield(task, budget) do
      {:ok, {result, signal}} ->
        if signal_phase?, do: Process.put(@signal_key, signal)
        flush_signal_fact(ref)
        result

      _timeout_or_exit ->
        Task.shutdown(task, :brutal_kill)
        flush_signal_fact(ref)
        {:error, :owner_unavailable}
    end
  end

  defp flush_signal_fact(ref) do
    receive do
      {:expired_stop_signal, ^ref} -> Process.put(@signal_key, true)
    after
      0 -> :ok
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec checkpoint(atom(), witness()) :: :ok
    def checkpoint(stage, witness) do
      case Application.get_env(:codex_pooler, :expired_owner_generation_checkpoint) do
        {observer, ref, ^stage} when is_pid(observer) and is_reference(ref) ->
          send(observer, {:expired_owner_generation_checkpoint, self(), ref, stage, witness})

          receive do
            {:release_expired_owner_generation_checkpoint, ^ref} -> :ok
          after
            max(remaining_ms() - 250, 0) -> exit(:sample_expired_checkpoint_not_released)
          end

        _unconfigured ->
          :ok
      end
    end

    defp maybe_rollback(stage) do
      if Application.get_env(:codex_pooler, :expired_owner_generation_rollback) == stage, do: Repo.rollback(:sample_expired_owner_rollback)
    end
  else
    @doc false
    @spec checkpoint(atom(), witness()) :: :ok
    def checkpoint(_stage, _witness), do: :ok
    defp maybe_rollback(_stage), do: :ok
  end

  @spec observe_pending(Attempt.t()) :: {:ok, Attempt.t()} | {:error, term()}
  def observe_pending(attempt) do
    case read(attempt) do
      {:ok, %{"phase" => "authorized", "producer" => producer} = witness} ->
        observe_authorized(attempt, producer, witness)

      _ordinary_or_ended ->
        {:ok, attempt}
    end
  end

  defp observe_authorized(attempt, producer, witness) do
    authority = if Repo.in_transaction?(), do: :unknown, else: producer_end_authority(producer)
    corroborate_end(attempt, witness, authority)
  end

  defp corroborate_end(attempt, witness, authority) when authority in ["producer_process_dead", "producer_vm_superseded"] do
    case record_end(witness, authority) do
      {:ok, _ended} -> {:ok, Repo.get!(Attempt, attempt.id)}
      {:error, _reason} = error -> error
    end
  end

  defp corroborate_end(attempt, _witness, _unknown), do: {:ok, attempt}

  defp producer_end_authority(producer) do
    identity = Map.new(producer, fn {key, value} -> {String.to_existing_atom(key), value} end)

    case producer_status(identity) do
      :dead ->
        "producer_process_dead"

      :alive ->
        :unknown

      :unknown ->
        owner = Identity.owner(identity.owner_instance_id, identity.owner_instance_boot_id)
        if InstancePresence.superseded?(owner), do: "producer_vm_superseded", else: :unknown
    end
  end

  @spec authorize(map(), map()) :: {:ok, witness()} | {:error, term()}
  def authorize(state, candidate) do
    guarded_actor_operation(state, fn -> sql_phase(fn -> authorize_locked(state, candidate) end) end)
  end

  defp guarded_actor_operation(state, operation) do
    cond do
      Repo.in_transaction?() -> {:error, :caller_transaction}
      producer_status(Map.get(state, :producer_identity)) == :alive -> operation.()
      true -> {:error, :owner_unavailable}
    end
  end

  defp authorize_locked(state, candidate) do
    rows = lock_current_slot!(state, candidate)
    witness = build_witness(state, rows)

    case read(rows.attempt) do
      :none ->
        witness = put_witness!(rows.attempt, witness)
        maybe_rollback(:authorization)
        witness

      {:ok, existing} ->
        if same_authorization?(existing, witness), do: existing, else: Repo.rollback(:stale_owner)

      {:error, reason} ->
        Repo.rollback(reason)
    end
  end

  @spec signal_authorized(map(), map(), witness(), (-> :ok)) :: {:ok, :ok} | {:error, term()}
  def signal_authorized(state, candidate, witness, signal) when is_function(signal, 0) do
    guarded_actor_operation(state, fn -> sql_phase(fn -> signal_locked(state, candidate, witness, signal) end, signal_phase: true) end)
  end

  defp signal_locked(state, candidate, witness, signal) do
    rows = lock_current_slot!(state, candidate)
    ensure!(read(rows.attempt) == {:ok, witness})
    ensure!(same_authorization?(build_witness(state, rows), witness))
    ensure!(Process.alive?(state.active_turn.task_pid), :task_after_locks)
    maybe_rollback(:second_guard)
    signal.()
  end

  @spec record_end(witness(), String.t()) :: {:ok, witness()} | {:error, term()}
  def record_end(witness, kind) when kind in ["serialized_connection_closed", "producer_process_dead", "producer_vm_superseded"] do
    if Repo.in_transaction?() do
      {:error, :caller_transaction}
    else
      sql_phase(fn -> record_end_locked(witness, kind) end)
    end
  end

  defp record_end_locked(witness, kind) do
    {request, attempt} = lock_witness_tuple!(witness)
    ensure!(matches_request?(witness, request))

    case read(attempt) do
      {:ok, %{"phase" => "ended"} = existing} ->
        ensure!(scope(existing) == scope(witness))
        existing

      {:ok, ^witness} ->
        ended = Map.merge(witness, %{"phase" => "ended", "end_kind" => kind, "observed_end_at" => DateTime.to_iso8601(database_now())})
        put_witness!(attempt, ended)

      _conflict ->
        Repo.rollback(:stale_owner)
    end
  end

  @spec clear_unsignalled_authorization(map(), witness()) :: {:ok, :cleared} | {:error, term()}
  def clear_unsignalled_authorization(state, %{"phase" => "authorized"} = witness) do
    if Repo.in_transaction?() do
      {:error, :caller_transaction}
    else
      sql_phase(fn ->
        ensure!(Process.get({CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession, :expired_slot_signal}) == false)
        ensure!(witness["stop_decision_id"] == Process.get(@decision_key), :decision_origin)
        ensure!(unsignalled_slot_matches?(state, witness))
        {request, attempt} = lock_witness_tuple!(witness)
        ensure!(matches_request?(witness, request) and read(attempt) == {:ok, witness})
        Repo.update!(Ecto.Changeset.change(attempt, response_metadata: Map.delete(attempt.response_metadata || %{}, @key)))
        :cleared
      end)
    end
  end

  def clear_unsignalled_authorization(_state, _witness), do: {:error, :stale_owner}

  defp unsignalled_slot_matches?(%{active_turn: %{cleanup_witness: cleanup, task_pid: task, task_ref: ref}} = state, witness) when is_map(cleanup) do
    state.process_generation == witness["process_generation"] and cleanup.request_id == witness["request_id"] and cleanup.attempt_id == witness["attempt_id"] and
      cleanup.replay_generation == witness["replay_generation"] and digest(ref) == witness["task_digest"] and
      List.to_string(:erlang.pid_to_list(task)) == witness["task_address"] and witness["owner_instance_id"] == Identity.local_node_name() and witness["owner_instance_boot_id"] == Identity.boot_id()
  end

  defp unsignalled_slot_matches?(_state, _witness), do: false

  @spec lock_witness_tuple!(witness()) :: {Request.t(), Attempt.t()}
  def lock_witness_tuple!(witness) do
    Repo.one!(from s in CodexSession, where: s.id == ^witness["session_id"], lock: "FOR UPDATE")
    Access.lock_api_key_for_read(witness["api_key_id"])
    request = Repo.one!(from r in Request, where: r.id == ^witness["request_id"], lock: "FOR UPDATE")
    turn = Repo.one!(from t in CodexTurn, where: t.id == ^witness["turn_id"] and t.request_id == ^request.id, lock: "FOR UPDATE")
    attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id, order_by: [desc: a.attempt_number], limit: 1, lock: "FOR UPDATE")
    ensure!(turn.codex_session_id == witness["session_id"] and matches_attempt?(witness, attempt))
    {request, attempt}
  end

  defp lock_current_slot!(state, candidate) do
    Enum.each(runtime_slot_checks(state, candidate), fn {clause, matched} -> ensure!(matched, clause) end)
    active = state.active_turn
    cleanup = active.cleanup_witness
    session = Repo.one!(from s in CodexSession, where: s.id == ^candidate.session_id, lock: "FOR UPDATE")
    lease = Repo.one!(from l in BridgeOwnerLease, where: l.codex_session_id == ^session.id and l.status == "active", lock: "FOR UPDATE")
    key = Access.lock_api_key_for_read(session.api_key_id)
    request = Repo.one!(from r in Request, where: r.id == ^cleanup.request_id, lock: "FOR UPDATE")
    turn = Repo.one!(from t in CodexTurn, where: t.request_id == ^request.id, lock: "FOR UPDATE")
    attempt = Repo.one!(from a in Attempt, where: a.request_id == ^request.id, order_by: [desc: a.attempt_number], limit: 1, lock: "FOR UPDATE")
    clock = database_now()
    validate_owner_identity!(session, lease, candidate)
    validate_owner_deadlines!(session, lease, candidate, clock)
    validate_principal_scope!(session, lease, key, request)
    validate_turn_scope!(session, turn, request, attempt)
    validate_attempt_scope!(request, attempt, cleanup, key)
    %{session: session, lease: lease, request: request, turn: turn, attempt: attempt, clock: clock}
  end

  defp validate_owner_identity!(session, lease, candidate) do
    ensure!(session.owner_instance_id == candidate.owner_instance_id and session.owner_instance_id == Identity.local_node_name(), :owner_node)
    ensure!(session.owner_instance_boot_id == Identity.boot_id() and lease.owner_instance_boot_id == Identity.boot_id(), :owner_boot)
    ensure!(lease.owner_instance_id == session.owner_instance_id and session.owner_lease_token == candidate.owner_lease_token and lease.lease_token == candidate.owner_lease_token)
  end

  defp validate_owner_deadlines!(session, lease, candidate, clock) do
    ensure!(session.owner_lease_expires_at == candidate.owner_lease_expires_at and lease.expires_at == session.owner_lease_expires_at)
    ensure!(DateTime.compare(session.owner_lease_expires_at, clock) != :gt)
  end

  defp validate_principal_scope!(session, lease, key, request) do
    ensure!(request.pool_id == session.pool_id and request.api_key_id == session.api_key_id and lease.pool_id == session.pool_id and lease.api_key_id == session.api_key_id)
    ensure!(key.pool_id == session.pool_id, :key_pool)
  end

  defp validate_turn_scope!(session, turn, request, attempt) do
    ensure!(turn.codex_session_id == session.id and turn.final_attempt_id in [nil, attempt.id])
    ensure!(not Repo.exists?(from t in CodexTurn, where: t.codex_session_id == ^session.id and (t.turn_sequence > ^turn.turn_sequence or (t.status == "in_progress" and t.id != ^turn.id))))
    ensure!(request.status == "in_progress" and turn.status == "in_progress")
  end

  defp validate_attempt_scope!(request, attempt, cleanup, key) do
    ensure!(attempt.id == cleanup.attempt_id and attempt.replay_generation == 0 and cleanup.replay_generation == 0 and attempt.model_id == request.model_id)
    ensure!(attempt.transport == "websocket" and attempt.status == "in_progress")
    ensure!(is_nil(request.native_client_retry_auth_epoch) or request.native_client_retry_auth_epoch == key.runtime_revocation_epoch)
  end

  defp runtime_slot_checks(%{active_turn: %{cleanup_witness: cleanup, task_pid: task, task_ref: ref, pending_result: nil} = active} = state, candidate) when is_map(cleanup) do
    [
      task: is_pid(task) and is_reference(ref) and Process.alive?(task),
      session: state.codex_session_id == candidate.session_id and state.owner_lease_token == candidate.owner_lease_token,
      witness: cleanup_scope_matches?(cleanup, candidate),
      admission: cleanup.request_id == active.admission_request_id and cleanup.attempt_id == active.admission_attempt_id,
      downstream: is_map(active.downstream) and cleanup.downstream_epoch == active.downstream.epoch,
      nonterminal: active.terminal_forwarded? == false and active.task_settled? == false,
      replay: is_nil(cleanup.native_replay_binding),
      new_work: no_other_owner_work?(state),
      producer: producer_matches?(state)
    ]
  end

  defp runtime_slot_checks(_state, _candidate), do: [runtime_slot: false]

  defp cleanup_scope_matches?(cleanup, candidate), do: cleanup.session_id == candidate.session_id and cleanup.owner_instance_id == candidate.owner_instance_id and cleanup.owner_lease_token == candidate.owner_lease_token
  defp no_other_owner_work?(state), do: is_nil(state.pending_handoff) and is_nil(state.suspended_replay) and map_size(state.pending_admissions) == 0

  defp producer_matches?(%{producer_identity: producer, upstream_pid: pid}) when is_map(producer) do
    producer.owner_instance_id == Identity.local_node_name() and producer.owner_instance_boot_id == Identity.boot_id() and
      producer.owner_process_id == List.to_string(:erlang.pid_to_list(pid)) and Process.alive?(pid)
  end

  defp producer_matches?(_state), do: false

  defp build_witness(state, rows) do
    %{session: session, lease: lease, request: request, turn: turn, attempt: attempt} = rows

    %{
      "version" => 1,
      "stop_decision_id" => Process.get(@decision_key),
      "phase" => "authorized",
      "session_id" => session.id,
      "pool_id" => request.pool_id,
      "api_key_id" => request.api_key_id,
      "model_id" => request.model_id,
      "request_id" => request.id,
      "attempt_id" => attempt.id,
      "turn_id" => turn.id,
      "lease_id" => lease.id,
      "replay_generation" => attempt.replay_generation,
      "principal_epoch" => request.native_client_retry_auth_epoch,
      "lease_identity_digest" => digest(lease.lease_token),
      "session_deadline" => DateTime.to_iso8601(session.owner_lease_expires_at),
      "lease_deadline" => DateTime.to_iso8601(lease.expires_at),
      "owner_instance_id" => lease.owner_instance_id,
      "owner_instance_boot_id" => lease.owner_instance_boot_id,
      "process_generation" => state.process_generation,
      "task_digest" => digest(state.active_turn.task_ref),
      "task_address" => List.to_string(:erlang.pid_to_list(state.active_turn.task_pid)),
      "downstream_epoch" => state.active_turn.cleanup_witness.downstream_epoch,
      "executor" => identity(attempt),
      "producer" => identity(state.producer_identity),
      "authorized_at" => DateTime.to_iso8601(rows.clock)
    }
  end

  defp same_authorization?(left, right), do: Map.drop(scope(left), ["authorized_at", "stop_decision_id"]) == Map.drop(scope(right), ["authorized_at", "stop_decision_id"])

  defp put_witness!(attempt, witness) do
    ensure!(match?({:ok, _}, validate(witness, attempt)))
    Repo.update!(Ecto.Changeset.change(attempt, response_metadata: Map.put(attempt.response_metadata || %{}, @key, witness)))
    witness
  end

  defp ensure!(condition, clause \\ :locked_tuple)
  defp ensure!(true, _clause), do: :ok
  defp ensure!(_false, clause), do: Repo.rollback({:stale_owner, clause})
  defp database_now, do: InstancePresence.database_now()

  defp producer_status(identity) do
    if remaining_ms() >= 1_000, do: ExecutionIdentity.status(identity), else: :unknown
  end

  @spec read(Attempt.t()) :: result()
  def read(%Attempt{response_metadata: metadata} = attempt) do
    case Map.fetch(metadata || %{}, @key) do
      :error -> :none
      {:ok, value} -> validate(value, attempt)
    end
  end

  @spec validate(term(), Attempt.t()) :: result()
  def validate(value, %Attempt{} = attempt) when is_map(value) do
    if valid_shape?(value) and matches_attempt?(value, attempt) and Metadata.sanitize_metadata(value) == value do
      {:ok, value}
    else
      {:error, :invalid_expired_owner_stop}
    end
  rescue
    _error -> {:error, :invalid_expired_owner_stop}
  end

  def validate(_value, _attempt), do: {:error, :invalid_expired_owner_stop}

  @spec preserve(map(), Attempt.t()) :: map()
  def preserve(metadata, %Attempt{} = attempt) do
    metadata = metadata |> Map.delete(@key) |> Map.delete(:expired_owner_stop)

    case read(attempt) do
      {:ok, witness} -> Map.put(metadata, @key, witness)
      _untrusted -> metadata
    end
  end

  @spec finalization(map(), Request.t(), Attempt.t()) :: map()
  def finalization(finalization, request, attempt) do
    case read(attempt) do
      {:ok, witness} ->
        guarded_finalization(finalization, request, witness)

      :none ->
        finalization

      {:error, _malformed} ->
        if stale_backstop_authorized?(request, attempt.id, finalization),
          do: finalization,
          else: Repo.rollback(Metadata.accounting_error(:owner_unavailable, "websocket owner is unavailable"))
    end
  end

  @doc false
  @spec with_stale_backstop_authority(Request.t(), Attempt.t(), (-> term())) :: term()
  def with_stale_backstop_authority(request, attempt, operation) do
    previous = Process.get(@backstop_key)
    Process.put(@backstop_key, {make_ref(), request.id, attempt.id})

    try do
      operation.()
    after
      if previous, do: Process.put(@backstop_key, previous), else: Process.delete(@backstop_key)
    end
  end

  defp guarded_finalization(%{request_status: "succeeded"} = finalization, request, witness) do
    if matches_request?(witness, request), do: finalization, else: Repo.rollback(Metadata.accounting_error(:owner_unavailable, "websocket owner is unavailable"))
  end

  defp guarded_finalization(finalization, request, witness) do
    cond do
      matches_request?(witness, request) and witness["phase"] == "ended" ->
        %{finalization | request_status: "failed", attempt_status: "failed", response_status_code: 499, last_error_code: "owner_unavailable"}

      matches_request?(witness, request) and stale_backstop_authorized?(request, witness["attempt_id"], finalization) ->
        finalization

      true ->
        Repo.rollback(Metadata.accounting_error(:owner_unavailable, "websocket owner is unavailable"))
    end
  end

  defp stale_backstop_authorized?(request, selected_attempt_id, %{last_error_code: "stale_reservation_recovered"}) do
    case Process.get(@backstop_key) do
      {ref, request_id, attempt_id} when is_reference(ref) and request_id == request.id ->
        clock = database_now()
        latest = Repo.one(from a in Attempt, where: a.request_id == ^request.id, order_by: [desc: a.attempt_number], limit: 1, select: a.id)

        latest == attempt_id and attempt_id == selected_attempt_id and
          DateTime.compare(request.admitted_at, DateTime.add(clock, -6 * 60 * 60, :second)) != :gt and not live_request_lease?(request.id, clock)

      _untrusted ->
        false
    end
  end

  defp stale_backstop_authorized?(_request, _witness, _finalization), do: false

  defp live_request_lease?(request_id, clock) do
    Repo.exists?(from t in CodexTurn, join: l in BridgeOwnerLease, on: l.codex_session_id == t.codex_session_id, where: t.request_id == ^request_id and t.status == "in_progress" and l.status == "active" and l.expires_at > ^clock)
  end

  @spec marked?(Attempt.t()) :: boolean()
  def marked?(attempt), do: read(attempt) != :none

  @spec scope(witness()) :: map()
  def scope(witness), do: Map.take(witness, @scope_fields)

  @spec digest(term()) :: String.t()
  def digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value)) |> Base.encode16(case: :lower)

  @spec matches_request?(witness(), Request.t()) :: boolean()
  def matches_request?(witness, request) do
    witness["request_id"] == request.id and witness["pool_id"] == request.pool_id and
      witness["api_key_id"] == request.api_key_id and witness["model_id"] == request.model_id and witness["principal_epoch"] == request.native_client_retry_auth_epoch
  end

  @spec disposition(witness()) :: map()
  def disposition(witness), do: %{version: 1, scope: scope(witness), stop_issued: true}

  @spec stopped_caller?(map(), Request.t(), Attempt.t()) :: boolean()
  def stopped_caller?(error, %Request{} = request, %Attempt{} = attempt) do
    case Map.get(error, @transient) do
      %{version: 1, scope: scope, stop_issued: true} when is_map(scope) ->
        authorized = Map.merge(scope, %{"version" => 1, "phase" => "authorized"})
        match?({:ok, _}, validate(authorized, attempt)) and matches_request?(scope, request)

      _missing ->
        false
    end
  end

  def stopped_caller?(_error, _request, _attempt), do: false

  @spec strip(map()) :: map()
  def strip(error), do: Map.delete(error, @transient)

  @spec receipt_completed?(witness()) :: boolean()
  def receipt_completed?(witness) do
    request = Repo.get(Request, witness["request_id"])
    attempt = Repo.get(Attempt, witness["attempt_id"])

    match?(%Request{status: status} when status in ["failed", "succeeded"], request) and
      match?(%Attempt{status: status} when status in ["failed", "succeeded"], attempt) and
      matches_request?(witness, request) and matches_attempt?(witness, attempt)
  end

  @spec retry_blocked?(map(), Request.t(), Attempt.t()) :: boolean()
  def retry_blocked?(error, %Request{} = request, %Attempt{} = attempt) do
    stopped_caller?(error, request, attempt) or stored_retry_blocked?(request, attempt)
  end

  def retry_blocked?(_error, _request, _attempt), do: false

  defp stored_retry_blocked?(request, attempt) do
    case Repo.get(Attempt, attempt.id) do
      %Attempt{} = stored ->
        case read(stored) do
          {:ok, witness} -> matches_request?(witness, request) and matches_attempt?(witness, attempt)
          _unmarked -> committed_owner_failure?(request, attempt, stored)
        end

      _missing ->
        false
    end
  end

  defp committed_owner_failure?(request, attempt, stored) do
    actual = Repo.get(Request, request.id)
    latest = Repo.one(from a in Attempt, where: a.request_id == ^request.id, order_by: [desc: a.attempt_number], limit: 1, select: a.id)

    match?(%Request{status: "failed", response_status_code: 499, last_error_code: "owner_unavailable"}, actual) and
      stored.status == "failed" and stored.network_error_code == "owner_unavailable" and latest == attempt.id and
      stored.replay_generation == attempt.replay_generation and identity(stored) == identity(attempt) and
      Map.take(actual, [:pool_id, :api_key_id, :model_id, :native_client_retry_auth_epoch]) == Map.take(request, [:pool_id, :api_key_id, :model_id, :native_client_retry_auth_epoch])
  end

  defp matches_attempt?(witness, attempt) do
    witness["attempt_id"] == attempt.id and witness["request_id"] == attempt.request_id and
      witness["model_id"] == attempt.model_id and witness["replay_generation"] == attempt.replay_generation and attempt.transport == "websocket" and
      witness["executor"] == identity(attempt)
  end

  @spec identity(map()) :: map()
  def identity(value), do: Map.new(@identity_fields, &{&1, Map.get(value, String.to_existing_atom(&1))})

  defp valid_shape?(value) do
    Map.keys(value) |> Enum.all?(&(&1 in @fields)) and
      value["version"] == 1 and value["phase"] in ["authorized", "ended"] and
      byte_size(CodexPooler.JSON.encode!(value)) <= @max_bytes and
      Enum.all?(@uuid_fields, &uuid?(value[&1])) and
      valid_runtime_scope?(value) and valid_end?(value)
  end

  defp valid_runtime_scope?(value) do
    valid_generation?(value) and valid_runtime_identities?(value) and Enum.all?(~w(session_deadline lease_deadline authorized_at), &timestamp?(value[&1]))
  end

  defp valid_generation?(value) do
    value["replay_generation"] == 0 and is_integer(value["process_generation"]) and value["process_generation"] > 0 and
      is_integer(value["downstream_epoch"]) and value["downstream_epoch"] > 0 and
      valid_principal_epoch?(value["principal_epoch"])
  end

  defp valid_principal_epoch?(nil), do: true
  defp valid_principal_epoch?(value), do: is_integer(value) and value >= 0

  defp valid_runtime_identities?(value) do
    digest?(value["lease_identity_digest"]) and digest?(value["task_digest"]) and process_address?(value["task_address"]) and
      bounded_identifier?(value["owner_instance_id"]) and bounded_identifier?(value["owner_instance_boot_id"]) and
      identity?(value["executor"]) and identity?(value["producer"])
  end

  defp valid_end?(%{"phase" => "authorized"} = value), do: Enum.all?(@ended_fields, &(not Map.has_key?(value, &1)))
  defp valid_end?(%{"phase" => "ended"} = value), do: value["end_kind"] in ["serialized_connection_closed", "producer_process_dead", "producer_vm_superseded"] and timestamp?(value["observed_end_at"])

  defp identity?(value) when is_map(value) do
    Enum.sort(Map.keys(value)) == Enum.sort(@identity_fields) and
      bounded_identifier?(value["owner_instance_id"]) and bounded_identifier?(value["owner_instance_boot_id"]) and
      process_address?(value["owner_process_id"]) and uuid?(value["owner_execution_id"])
  end

  defp identity?(_value), do: false
  defp uuid?(value) when is_binary(value), do: match?({:ok, ^value}, Ecto.UUID.cast(value))
  defp uuid?(_value), do: false
  defp timestamp?(value) when is_binary(value), do: match?({:ok, _, 0}, DateTime.from_iso8601(value))
  defp timestamp?(_value), do: false
  defp digest?(value) when is_binary(value), do: byte_size(value) == 64 and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp digest?(_value), do: false
  defp process_address?(value) when is_binary(value), do: byte_size(value) <= 64 and Regex.match?(~r/\A<0\.[0-9]+\.[0-9]+>\z/, value)
  defp process_address?(_value), do: false
  defp bounded_identifier?(value), do: is_binary(value) and byte_size(value) in 1..255 and String.valid?(value)
end
