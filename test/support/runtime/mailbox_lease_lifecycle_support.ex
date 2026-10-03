defmodule CodexPoolerWeb.Runtime.MailboxLeaseLifecycleSupport do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Repo
  @budget 15_000

  @spec suppress_owned_idle_renewal!(pid()) :: :ok
  def suppress_owned_idle_renewal!(owner) do
    original = :sys.get_state(owner)

    on_exit(fn -> restore_owned_schedule(owner, original) end)

    state = :sys.replace_state(owner, &__MODULE__.disable_idle_schedule/1)
    assert state.active_turn == nil
    assert state.owner_renewal_ms == 0
    assert state.owner_renewal_ref == nil
    :ok
  end

  defp restore_owned_schedule(owner, original) do
    if (node(owner) == node() or node(owner) in Node.list()) and :erpc.call(node(owner), Process, :alive?, [owner]) do
      :sys.replace_state(owner, fn state -> %{state | owner_renewal_ms: original.owner_renewal_ms, owner_renewal_delay: original.owner_renewal_delay} end)
      send(owner, :renew_owner_lease)
    end
  end

  @spec stop_owned_after_expiry!(pid()) :: :ok
  def stop_owned_after_expiry!(owner) do
    monitor = Process.monitor(owner)

    try do
      GenServer.stop(owner, :normal, @budget)
    catch
      :exit, _stopped_concurrently -> :ok
    end

    assert_receive {:DOWN, ^monitor, :process, ^owner, reason}, @budget
    assert reason in [:normal, :noproc, {:shutdown, :stale_owner}]
    :ok
  end

  @spec disable_idle_schedule(map()) :: map()
  def disable_idle_schedule(state) do
    assert state.active_turn == nil
    if is_reference(state.owner_renewal_ref), do: Process.cancel_timer(state.owner_renewal_ref)
    drain_periodic_tick()
    %{state | owner_renewal_ms: 0, owner_renewal_ref: nil}
  end

  defp drain_periodic_tick do
    receive do
      :renew_owner_lease -> drain_periodic_tick()
    after
      0 -> :ok
    end
  end

  @spec renew_and_observe_expiry!(map()) :: map()
  def renew_and_observe_expiry!(session) do
    options = RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: 1})
    assert {:ok, renewed} = SessionContinuity.renew_owner_token(session, session.owner_lease_token, options)
    initial = observe!(session.id)
    assert initial.session_deadline == renewed.owner_lease_expires_at
    assert initial.session_deadline == initial.lease_deadline
    assert DateTime.compare(initial.clock, initial.session_deadline) == :lt
    final = await_crossing!(session.id, initial.session_deadline, System.monotonic_time(:millisecond) + @budget)
    %{session_deadline: initial.session_deadline, lease_deadline: initial.lease_deadline, observed_before: initial.clock, observed_after: final.clock, unchanged_deadlines: true, genuine_expiry: true}
  end

  @spec observe!(Ecto.UUID.t()) :: map()
  def observe!(session_id) do
    %{rows: [[session_deadline, lease_deadline, clock]]} = Repo.query!("SELECT s.owner_lease_expires_at, l.expires_at, clock_timestamp() FROM codex_sessions s JOIN bridge_owner_leases l ON l.codex_session_id = s.id AND l.status = 'active' AND l.lease_token = s.owner_lease_token WHERE s.id = $1", [Ecto.UUID.dump!(session_id)])
    %{session_deadline: utc(session_deadline), lease_deadline: utc(lease_deadline), clock: utc(clock)}
  end

  defp utc(%NaiveDateTime{} = value), do: DateTime.from_naive!(value, "Etc/UTC")
  defp utc(%DateTime{} = value), do: value

  defp await_crossing!(id, expected, deadline) do
    observation = observe!(id)
    assert observation.session_deadline == expected
    assert observation.lease_deadline == expected

    cond do
      DateTime.compare(observation.clock, expected) != :lt ->
        observation

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("owned unchanged lease did not reach PostgreSQL expiry")

      true ->
        receive do
        after
          10 -> await_crossing!(id, expected, deadline)
        end
    end
  end
end
