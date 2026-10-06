defmodule CodexPoolerWeb.Runtime.MailboxLeaseLifecycleSupport do
  @moduledoc false
  import ExUnit.Assertions
  import ExUnit.Callbacks
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{CodexSession, SessionContinuity}
  alias CodexPooler.Repo
  @budget 15_000

  @spec suppress_owned_idle_renewal!(pid()) :: :ok
  def suppress_owned_idle_renewal!(owner) do
    original = :sys.get_state(owner, @budget)

    on_exit(fn -> restore_owned_schedule(owner, original) end)

    state = :sys.replace_state(owner, &__MODULE__.disable_idle_schedule/1, @budget)
    assert state.active_turn == nil
    assert state.owner_renewal_ms == 0
    assert state.owner_renewal_ref == nil
    :ok
  end

  # An owner whose lease expired retires on its own, so finding it alive does not promise that it still is when its schedule is put
  # back: one that is gone has nothing to restore (findings#303 rows 303-10 and 303-11).
  defp restore_owned_schedule(owner, original) do
    if (node(owner) == node() or node(owner) in Node.list()) and :erpc.call(node(owner), Process, :alive?, [owner]) do
      try do
        :sys.replace_state(owner, fn state -> %{state | owner_renewal_ms: original.owner_renewal_ms, owner_renewal_delay: original.owner_renewal_delay} end, @budget)
        send(owner, :renew_owner_lease)
      catch
        :exit, _gone_or_retiring -> :ok
      end
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
    initial = shorten_lease!(session.id, 1)
    final = await_crossing!(session.id, initial.session_deadline, System.monotonic_time(:millisecond) + @budget)
    %{session_deadline: initial.session_deadline, lease_deadline: initial.lease_deadline, observed_before: initial.clock, observed_after: final.clock, unchanged_deadlines: true, genuine_expiry: true}
  end

  # Starts the ttl a test is about. A scenario opens its lease with a ttl that outlives any scheduling delay, so the renewing owner
  # or heartbeat is never what a loaded machine breaks; this writes the claim's ttl with one renewal on the product's own path
  # (`SessionContinuity.renew_owner_token/3`), and the deadline it writes is the baseline the boundary observes.
  #
  # With the renewing actor's periodic schedule off (`suppressed?: true`) nothing else writes the lease, so the database's
  # deadlines must be the ones this renewal wrote. They are not asserted to still lie ahead of the clock: a stall between this
  # renewal and the observation does not change what the claims are about (an unrenewed lease that crosses its deadline), and the
  # crossing is awaited on the database clock either way. With the schedule running, a tick may renew right after this call, so
  # the deadline this renewal wrote is returned as it is, with the clock read before it.
  @spec shorten_lease!(Ecto.UUID.t(), pos_integer(), keyword()) :: map()
  def shorten_lease!(session_id, ttl_seconds, opts \\ []) do
    stable = observe!(session_id)
    assert DateTime.compare(stable.clock, stable.lease_deadline) == :lt, "the lease had already expired when the test took it over"
    assert DateTime.compare(stable.clock, stable.session_deadline) == :lt, "the session had already expired when the test took it over"
    session = Repo.get!(CodexSession, session_id)
    options = RequestOptions.for_websocket(%{bridge_owner_lease_ttl_seconds: ttl_seconds})
    assert {:ok, renewed} = SessionContinuity.renew_owner_token(session, session.owner_lease_token, options)

    if Keyword.get(opts, :suppressed?, true) do
      observation = observe!(session_id)
      assert observation.session_deadline == renewed.owner_lease_expires_at
      assert observation.session_deadline == observation.lease_deadline
      observation
    else
      %{session_deadline: renewed.owner_lease_expires_at, lease_deadline: renewed.owner_lease_expires_at, clock: stable.clock}
    end
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
