defmodule CodexPooler.Gateway.Persistence.SessionContinuity.MailboxSessionAuthority do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Gateway.Persistence.{CodexSession, CodexTurn}
  alias CodexPooler.Gateway.Persistence.SessionContinuity.MailboxAdmissionLocks
  alias CodexPooler.Gateway.Persistence.StatusVocabulary.Session
  alias CodexPooler.Repo

  @type scope :: %{required(:pool_id) => Ecto.UUID.t(), required(:api_key_id) => Ecto.UUID.t()}
  @type verdict :: :same_session | :expired_replacement | :rejected

  @spec verdict(Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, scope()) :: verdict()
  def verdict(id, id, _scope) when is_binary(id), do: :same_session

  def verdict(previous_id, current_id, scope) when is_binary(previous_id) and is_binary(current_id) do
    rows = Repo.all(from s in CodexSession, where: s.id in ^[previous_id, current_id] and s.pool_id == ^scope.pool_id and s.api_key_id == ^scope.api_key_id)
    previous = Enum.find(rows, &(&1.id == previous_id))
    current = Enum.find(rows, &(&1.id == current_id))

    if previous && current && MailboxAdmissionLocks.coordinated?() do
      MailboxAdmissionLocks.require_sessions!([previous_id, current_id])
      if expired_replacement?(previous, current), do: :expired_replacement, else: :rejected
    else
      :rejected
    end
  end

  def verdict(_previous_id, _current_id, _scope), do: :rejected

  @spec edge_verdict(Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, Ecto.UUID.t() | nil, scope()) :: verdict()
  def edge_verdict(previous_id, successor_id, current_id, scope) do
    final_verdict = verdict(previous_id, current_id, scope)

    cond do
      final_verdict == :rejected -> :rejected
      successor_id == current_id or successor_id == previous_id -> final_verdict
      not is_binary(successor_id) -> :rejected
      verdict(successor_id, current_id, scope) != :expired_replacement -> :rejected
      true -> historical_edge_verdict(previous_id, successor_id, scope)
    end
  end

  defp historical_edge_verdict(previous_id, successor_id, scope) do
    previous = Repo.get!(CodexSession, previous_id)
    successor = Repo.get!(CodexSession, successor_id)
    MailboxAdmissionLocks.require_sessions!([previous_id, successor_id])

    if previous.pool_id == scope.pool_id and previous.api_key_id == scope.api_key_id and
         successor.pool_id == scope.pool_id and successor.api_key_id == scope.api_key_id and
         certified_close?(previous) and certified_close?(successor) and
         DateTime.compare(successor.created_at, previous.closed_at) != :lt and
         same_canonical_key?(previous_id, successor_id), do: :expired_replacement, else: :rejected
  end

  defp certified_close?(%CodexSession{status: "closed", close_reason: "owner_lease_expired", closed_at: %DateTime{} = closed, owner_lease_expires_at: %DateTime{} = expires}), do: DateTime.compare(expires, closed) != :gt
  defp certified_close?(_session), do: false

  defp expired_replacement?(%CodexSession{status: "closed", close_reason: "owner_lease_expired", closed_at: %DateTime{} = closed_at, owner_lease_expires_at: %DateTime{} = expired_at} = previous, %CodexSession{created_at: %DateTime{} = created_at} = current) do
    current.status in Session.reconnectable_statuses() and
      DateTime.compare(expired_at, closed_at) != :gt and
      DateTime.compare(created_at, closed_at) != :lt and
      same_canonical_key?(previous.id, current.id) and
      not Repo.exists?(from turn in CodexTurn, where: turn.codex_session_id == ^current.id and turn.status == "in_progress")
  end

  defp expired_replacement?(_previous, _current), do: false

  defp same_canonical_key?(previous_id, current_id),
    do: Repo.exists?(from previous in CodexSession, join: current in CodexSession, on: current.id == ^current_id, where: previous.id == ^previous_id and fragment("lower(?) = lower(?)", previous.session_key, current.session_key))
end
