defmodule CodexPooler.Gateway.Persistence.SessionContinuity.ExpiredSessions do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Gateway.Persistence.{
    BridgeOwnerLease,
    BridgeSessionAlias,
    CodexSession
  }

  alias CodexPooler.Gateway.Persistence.StatusVocabulary.OwnerLease, as: OwnerLeaseStatus
  alias CodexPooler.Gateway.Persistence.StatusVocabulary.Session, as: SessionStatus

  alias CodexPooler.Gateway.Persistence.StatusVocabulary.SessionAlias,
    as: SessionAliasStatus

  alias CodexPooler.Repo

  @alias_active SessionAliasStatus.active_status()
  @alias_expired SessionAliasStatus.expired_status()
  @lease_active OwnerLeaseStatus.active_status()
  @lease_expired OwnerLeaseStatus.expired_status()
  @session_closed SessionStatus.closed_status()
  @session_reconnectable_statuses SessionStatus.reconnectable_statuses()

  @type session_snapshot :: %{
          id: Ecto.UUID.t(),
          api_key_id: Ecto.UUID.t() | nil,
          pool_upstream_assignment_id: Ecto.UUID.t() | nil,
          last_heartbeat_at: DateTime.t() | nil,
          updated_at: DateTime.t() | nil,
          created_at: DateTime.t() | nil
        }

  @type result :: %{
          closed_count: non_neg_integer(),
          preferred_assignment_id: Ecto.UUID.t() | nil
        }

  @doc """
  Closes this API key's sessions whose owner lease has expired for this key.

  Another key's expired session on the same key is left alone: it does not
  block this key's replacement under `codex_sessions_pool_api_key_session_key_uq`,
  and closing it would expire that key's aliases, including the
  `previous_response_id` aliases that outlive an expired lease.

  Returns how many sessions were closed plus the assignment the caller should
  softly prefer when it inserts the replacement session. The preference is the
  just-closed session's `pool_upstream_assignment_id`; it is a routing hint
  only, and a caller that cannot use it must fall through to ordinary ordering.
  """
  @spec close_for_key!(Ecto.UUID.t(), Ecto.UUID.t(), String.t(), DateTime.t()) :: result()
  def close_for_key!(pool_id, api_key_id, session_key, %DateTime{} = now),
    do: close_for_key_and_aliases!(pool_id, api_key_id, session_key, [], now)

  @doc """
  `close_for_key!/4` over the sessions the request reaches through a
  `session_header` alias of one of `session_header_values` as well: the
  request's own window and, on native HTTP, the previous window of its thread.

  A window linked to another window's session (the websocket frame alias of
  findings#206 P115, or the previous-window fallback of findings#289) keeps
  the alias, not the key, so a start on it after the owner lease expired found
  nothing to close by key: the replacement softly preferred no assignment and
  the expired session stayed `active` until the runtime sweep (findings#270
  row 270-282). Both sets are locked in one statement, in session id order,
  closed together, and the preference is picked over both. Only this API key's
  sessions, reached through this key's aliases, and only sessions whose lease
  has expired are ever closed.
  """
  @spec close_for_key_and_aliases!(Ecto.UUID.t(), Ecto.UUID.t(), String.t(), [String.t()], DateTime.t()) :: result()
  def close_for_key_and_aliases!(pool_id, api_key_id, session_key, session_header_values, %DateTime{} = now)
      when is_list(session_header_values) do
    alias_hashes = Enum.map(session_header_values, &:crypto.hash(:sha256, &1))
    expiring_sessions = lock_expired_sessions(pool_id, api_key_id, session_key, alias_hashes, now)
    session_ids = Enum.map(expiring_sessions, & &1.id)

    lock_active_leases!(session_ids)
    lock_active_aliases!(session_ids)

    expire_leases!(session_ids, now)
    expire_aliases!(session_ids, now)
    {closed_count, _returning} = close_sessions!(session_ids, now)

    %{
      closed_count: closed_count,
      preferred_assignment_id: preferred_assignment_id(expiring_sessions, api_key_id)
    }
  end

  @doc """
  Picks the assignment a replacement session should softly prefer.

  Scope: only a session belonging to the same API key can donate its
  assignment, so the preference never escapes the `(pool_id, api_key_id,
  session_key)` tuple that already scopes session continuity. A session with no
  bound assignment has nothing to donate.

  Tie-break: most recently active first. `last_heartbeat_at` is the last moment
  an owner proved liveness on that assignment, so it names the session whose
  upstream affinity is freshest and therefore the likeliest to still line up
  with warm provider-side context; that is the whole point of the preference.
  `updated_at` and `created_at` only break exact heartbeat ties, and `id` is the
  final total-order fallback so identical input always yields the same choice
  rather than whatever physical order the scan returned. Missing timestamps
  rank last instead of raising.

  By key alone more than one candidate is not reachable: the partial unique
  index `codex_sessions_pool_api_key_session_key_uq` admits at most one session
  per `(pool_id, api_key_id, lower(session_key))` while its status is
  reconnectable, and only reconnectable sessions are closed here. The sessions
  a request reaches through its window aliases as well
  (`close_for_key_and_aliases!/5`, findings#270 row 270-282) can add a second
  one, and the ordering picks the freshest; it also keeps a legacy row
  resolving deterministically, and it is covered as a pure function.
  """
  @spec preferred_assignment_id([session_snapshot()], Ecto.UUID.t() | nil) ::
          Ecto.UUID.t() | nil
  def preferred_assignment_id(expiring_sessions, api_key_id) do
    expiring_sessions
    |> Enum.filter(fn session ->
      not is_nil(api_key_id) and session.api_key_id == api_key_id and
        is_binary(session.pool_upstream_assignment_id)
    end)
    |> case do
      [] ->
        nil

      candidates ->
        candidates
        |> Enum.max_by(&preference_rank/1)
        |> Map.fetch!(:pool_upstream_assignment_id)
    end
  end

  defp preference_rank(session) do
    {
      recency(session.last_heartbeat_at),
      recency(session.updated_at),
      recency(session.created_at),
      session.id
    }
  end

  # DateTime structs must not be compared with Erlang term order: that compares
  # map keys alphabetically, so `day` would outrank `year`. The leading flag
  # ranks a missing timestamp below every present one; an atom sentinel would
  # do the opposite, because atoms sort above integers.
  defp recency(%DateTime{} = at), do: {1, DateTime.to_unix(at, :microsecond)}
  defp recency(_missing), do: {0, 0}

  defp lock_expired_sessions(pool_id, api_key_id, session_key, alias_hashes, now) do
    Repo.all(
      from session in CodexSession,
        where:
          session.pool_id == ^pool_id and session.api_key_id == ^api_key_id and
            session.status in ^@session_reconnectable_statuses and
            not is_nil(session.owner_lease_expires_at) and
            session.owner_lease_expires_at <= ^now,
        where: ^reached_by_key_or_alias(pool_id, api_key_id, session_key, alias_hashes, now),
        order_by: [asc: session.id],
        select: %{
          id: session.id,
          api_key_id: session.api_key_id,
          pool_upstream_assignment_id: session.pool_upstream_assignment_id,
          last_heartbeat_at: session.last_heartbeat_at,
          updated_at: session.updated_at,
          created_at: session.created_at
        },
        lock: "FOR UPDATE"
    )
  end

  defp reached_by_key_or_alias(_pool_id, _api_key_id, session_key, [], _now),
    do: dynamic([session], fragment("lower(?)", session.session_key) == ^String.downcase(session_key))

  defp reached_by_key_or_alias(pool_id, api_key_id, session_key, alias_hashes, now) do
    aliased =
      from alias_record in BridgeSessionAlias,
        where:
          alias_record.pool_id == ^pool_id and alias_record.api_key_id == ^api_key_id and
            alias_record.alias_kind == "session_header" and alias_record.alias_hash in ^alias_hashes and
            alias_record.status == ^@alias_active and alias_record.expires_at > ^now,
        select: alias_record.codex_session_id

    dynamic(
      [session],
      fragment("lower(?)", session.session_key) == ^String.downcase(session_key) or
        session.id in subquery(aliased)
    )
  end

  defp lock_active_leases!([]), do: []

  defp lock_active_leases!(session_ids) do
    Repo.all(
      from lease in BridgeOwnerLease,
        where: lease.codex_session_id in ^session_ids and lease.status == ^@lease_active,
        order_by: [asc: lease.id],
        select: lease.id,
        lock: "FOR UPDATE"
    )
  end

  defp lock_active_aliases!([]), do: []

  defp lock_active_aliases!(session_ids) do
    Repo.all(
      from alias_record in BridgeSessionAlias,
        where:
          alias_record.codex_session_id in ^session_ids and
            alias_record.status == ^@alias_active,
        order_by: [asc: alias_record.id],
        select: alias_record.id,
        lock: "FOR UPDATE"
    )
  end

  defp expire_leases!([], _now), do: {0, nil}

  defp expire_leases!(session_ids, now) do
    BridgeOwnerLease
    |> where(
      [lease],
      lease.codex_session_id in ^session_ids and lease.status == ^@lease_active
    )
    |> Repo.update_all(set: [status: @lease_expired, released_at: now, updated_at: now])
  end

  defp expire_aliases!([], _now), do: {0, nil}

  defp expire_aliases!(session_ids, now) do
    BridgeSessionAlias
    |> where(
      [alias_record],
      alias_record.codex_session_id in ^session_ids and
        alias_record.status == ^@alias_active
    )
    |> Repo.update_all(set: [status: @alias_expired, updated_at: now])
  end

  defp close_sessions!([], _now), do: {0, nil}

  defp close_sessions!(session_ids, now) do
    CodexSession
    |> where([session], session.id in ^session_ids)
    |> Repo.update_all(set: [status: @session_closed, closed_at: now, updated_at: now])
  end
end
