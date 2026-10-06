defmodule CodexPooler.Gateway.Persistence.SessionContinuity.AssignmentClaim do
  @moduledoc false

  # The account pin of a session that has none yet, taken from the attempt
  # that is serving the client output (findings#324).
  #
  # `pool_upstream_assignment_id` is otherwise written only when a turn of the
  # session succeeds, inside its settlement transaction
  # (`TurnLifecycle.update_session_assignment/3`). A request of the same
  # session that attached before that commit read the session unpinned and was
  # routed on its own, and so did every request after a first turn that never
  # succeeded: the released Codex Desktop cuts a stream for pending input and
  # sends its next request in the same session, and the cut turn settles
  # `client_disconnected`. Measured in production, the second request then
  # moved to another account, which accepts the reasoning items the client
  # replays and drops them (findings#318).
  #
  # The claim is the served account, not the planned one: it runs once the
  # relay has marked the turn visible and is about to hand the client that
  # account's first output (b0f31295f moved the pin off dispatch for exactly
  # that reason). It writes only while the session has no pin, never over one,
  # and only under the request's own owner lease (same token, reconnectable
  # session, unexpired lease on the database clock), so a superseded request
  # writes nothing. It is one statement on the session row alone, run after the
  # visibility transaction committed, so it holds no other row lock and cannot
  # invert the session-then-turn lock order. Completion still writes the
  # outcome over it, as before.

  import Ecto.Query

  alias CodexPooler.Gateway.Persistence.CodexSession
  alias CodexPooler.Gateway.Persistence.StatusVocabulary.Session, as: SessionStatus
  alias CodexPooler.Repo

  @reconnectable_statuses SessionStatus.reconnectable_statuses()

  @type result :: :claimed | :unclaimed

  @doc """
  Pins the session to `assignment_id` when it has no pin and `lease_token` is
  still its live owner token; `:unclaimed` when it already had a pin or the
  token no longer owns it.
  """
  @spec claim(Ecto.UUID.t(), Ecto.UUID.t(), Ecto.UUID.t()) :: result()
  def claim(session_id, assignment_id, lease_token)
      when is_binary(session_id) and is_binary(assignment_id) and is_binary(lease_token) do
    query =
      from session in CodexSession,
        where:
          session.id == ^session_id and is_nil(session.pool_upstream_assignment_id) and
            session.owner_lease_token == ^lease_token and session.status in ^@reconnectable_statuses and
            session.owner_lease_expires_at > fragment("clock_timestamp()"),
        update: [set: [pool_upstream_assignment_id: ^assignment_id, updated_at: fragment("clock_timestamp()")]]

    case Repo.update_all(query, []) do
      {1, _rows} -> :claimed
      {0, _rows} -> :unclaimed
    end
  end
end
