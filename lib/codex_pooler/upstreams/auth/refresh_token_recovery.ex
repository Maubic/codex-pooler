defmodule CodexPooler.Upstreams.Auth.RefreshTokenRecovery do
  @moduledoc false
  import Ecto.Query
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPooler.Upstreams.Secrets

  @type result :: {:ok, :retained | :ignored, UpstreamIdentity.t() | nil} | {:error, map()}

  # The caller owns the transaction. Reacquire the identity row before reading
  # provenance, so an old struct cannot authorize a secret write. This is a
  # row-only leaf: never acquire the identity advisory mutex from here.
  @spec retain(UpstreamIdentity.t() | nil, map(), map(), String.t()) :: result()
  def retain(identity, attempt, attrs, reason) do
    if Repo.in_transaction?() do
      locked = if identity, do: Repo.one(from i in UpstreamIdentity, where: i.id == ^identity.id, lock: "FOR UPDATE")
      retain_locked(locked, attempt, attrs, reason)
    else
      {:error, %{code: :refresh_recovery_transaction_required, message: "refresh recovery requires a transaction"}}
    end
  end

  defp retain_locked(%UpstreamIdentity{} = identity, attempt, attrs, reason) do
    if eligible?(identity, attempt) and reason in ~w(expired_access_token superseded_attempt) do
      identity = CredentialFencing.lock_credential_replacement_after_identity(identity)

      with token when is_binary(token) and byte_size(token) > 0 <- Map.get(attrs, :refresh_token),
           {:ok, previous, source_id} <- Secrets.decrypt_active_secret_with_id(identity, "refresh_token"),
           true <- source_id == attempt.source_secret_id and token != previous,
           {:ok, secret} <- Secrets.store_encrypted_secret(identity, %{secret_kind: "refresh_token", plaintext: token}) do
        receipt = %{
          "version" => 1,
          "credential_epoch" => attempt.credential_epoch,
          "attempt_id" => attempt.attempt_id,
          "generation" => attempt.generation,
          "source_secret_id" => source_id,
          "replacement_secret_id" => secret.id,
          "retained_at" => DateTime.to_iso8601(DateTime.utc_now()),
          "reason" => reason
        }

        updated = Repo.update!(Ecto.Changeset.change(identity, metadata: Map.put(identity.metadata, "refresh_token_recovery", receipt)))
        {:ok, :retained, updated}
      else
        {:error, error} -> {:error, error}
        _ineligible -> {:ok, :ignored, identity}
      end
    else
      {:ok, :ignored, identity}
    end
  end

  defp retain_locked(nil, _attempt, _attrs, _reason), do: {:ok, :ignored, nil}

  defp eligible?(identity, attempt) do
    metadata = identity.metadata || %{}
    current = metadata["token_refresh"]

    with true <- identity.status in ~w(active refresh_due refresh_failed refreshing reauth_required),
         false <- Map.has_key?(metadata, "permanent_deletion_requested_at"),
         {:ok, epoch} <- CredentialFencing.validate_current_credential_epoch(identity),
         ^epoch <- Map.get(attempt, :credential_epoch),
         true <- CredentialFencing.same_credential_since?(identity, epoch),
         {:ok, _} <- Ecto.UUID.cast(Map.get(attempt, :attempt_id)),
         {:ok, _} <- Ecto.UUID.cast(Map.get(attempt, :source_secret_id)),
         generation when is_integer(generation) and generation > 0 <- Map.get(attempt, :generation),
         %{"credential_epoch" => ^epoch, "source_secret_id" => source, "generation" => current_generation, "status" => status} <- current,
         true <- source == attempt.source_secret_id,
         true <- is_integer(current_generation) and current_generation >= generation,
         true <- current_generation > generation or current["attempt_id"] == attempt.attempt_id,
         true <- status in ~w(refreshing failed reauth_required) do
      true
    else
      _untrusted -> false
    end
  end
end
