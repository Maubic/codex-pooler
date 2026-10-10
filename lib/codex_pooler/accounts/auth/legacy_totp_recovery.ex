defmodule CodexPooler.Accounts.LegacyTOTPRecovery do
  @moduledoc false

  alias CodexPooler.Repo

  @timeout 60_000
  @select "SELECT id, secret_ciphertext, secret_key_version FROM totp_settings ORDER BY id"

  @spec reencrypt(term(), term(), term()) :: {:ok, map()} | {:error, atom()}
  def reencrypt(configured_key, version, acknowledgement) do
    with :ok <- acknowledge(acknowledgement),
         {:ok, key} <- destination_key(configured_key),
         :ok <- validate_version(version) do
      transaction(&reencrypt_all!(key, version, &1))
    end
  end

  defp reencrypt_all!(key, version, deadline) do
    query!("LOCK TABLE totp_settings, recovery_codes IN ACCESS EXCLUSIVE MODE NOWAIT", [], deadline)
    originals = read_secrets!(legacy_key(), deadline)
    Enum.each(originals, &replace_ciphertext!(&1, key, version, deadline))
    if read_secrets!(key, deadline) != originals, do: Repo.rollback(:verification_failed)
    %{rows: versions} = query!("SELECT DISTINCT secret_key_version FROM totp_settings", [], deadline)
    if versions not in [[], [[version]]], do: Repo.rollback(:verification_failed)
    %{rows: length(originals)}
  end

  defp replace_ciphertext!({id, secret}, key, version, deadline) do
    check_deadline!(deadline)
    encrypted = encrypt(key, secret)
    if decrypt(encrypted, key) != {:ok, secret}, do: Repo.rollback(:verification_failed)

    case query!("UPDATE totp_settings SET secret_ciphertext = $1, secret_key_version = $2 WHERE id = $3", [encrypted, version, id], deadline) do
      %{num_rows: 1} -> :ok
      _other -> Repo.rollback(:verification_failed)
    end
  end

  @spec verify(term()) :: {:ok, map()} | {:error, atom()}
  def verify(configured_key) do
    with {:ok, key} <- destination_key(configured_key) do
      transaction(fn deadline ->
        query!("LOCK TABLE totp_settings IN SHARE MODE NOWAIT", [], deadline)
        %{rows: length(read_secrets!(key, deadline))}
      end)
    end
  end

  defp transaction(fun) do
    deadline = System.monotonic_time(:millisecond) + @timeout

    Repo.transaction(
      fn ->
        result = fun.(deadline)
        check_deadline!(deadline)
        result
      end,
      deadline: deadline,
      log: false
    )
  rescue
    _error in [Postgrex.Error, DBConnection.ConnectionError] -> {:error, :database_failed}
  end

  defp query!(sql, params, deadline) do
    check_deadline!(deadline)

    case Repo.query(sql, params, log: false, deadline: deadline) do
      {:ok, result} -> result
      {:error, %Postgrex.Error{postgres: %{code: :lock_not_available}}} -> Repo.rollback(:writers_active)
      {:error, _error} -> Repo.rollback(:database_failed)
    end
  end

  defp check_deadline!(deadline) do
    if System.monotonic_time(:millisecond) >= deadline, do: Repo.rollback(:deadline_exceeded)
  end

  defp read_secrets!(key, deadline) do
    %{rows: rows} = query!(@select, [], deadline)

    Enum.map(rows, fn [id, encrypted, _version] ->
      check_deadline!(deadline)

      case decrypt(encrypted, key) do
        {:ok, secret} -> {id, secret}
        :error -> Repo.rollback(:ciphertext_not_recoverable)
      end
    end)
  end

  defp decrypt(<<nonce::binary-size(12), tag::binary-size(16), encrypted::binary-size(32)>>, key) do
    case :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, encrypted, "totp", tag, false) do
      secret when is_binary(secret) ->
        case Base.decode32(secret, case: :upper, padding: false) do
          {:ok, decoded} when byte_size(decoded) == 20 -> {:ok, secret}
          _invalid -> :error
        end

      :error ->
        :error
    end
  end

  defp decrypt(_invalid, _key), do: :error

  defp encrypt(key, secret) do
    nonce = :crypto.strong_rand_bytes(12)
    {encrypted, tag} = :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, secret, "totp", true)
    nonce <> tag <> encrypted
  end

  defp acknowledge("all-writers-stopped"), do: :ok
  defp acknowledge(_other), do: {:error, :offline_acknowledgement_required}

  defp validate_version(version) when is_binary(version) do
    if String.trim(version) == "", do: {:error, :invalid_key_version}, else: :ok
  end

  defp validate_version(_other), do: {:error, :invalid_key_version}

  defp destination_key(configured) when is_binary(configured) and byte_size(configured) == 32,
    do: private_destination(configured)

  defp destination_key(configured) when is_binary(configured) do
    case Base.decode64(configured) do
      {:ok, key} when byte_size(key) == 32 -> private_destination(key)
      _invalid -> {:error, :invalid_destination_key}
    end
  end

  defp destination_key(_other), do: {:error, :invalid_destination_key}

  defp private_destination(key) do
    if key == legacy_key(), do: {:error, :public_destination_forbidden}, else: {:ok, key}
  end

  # Only this explicit offline operation may authenticate legacy fallback rows.
  # Never publish this value into serving configuration or diagnostic output.
  defp legacy_key, do: :crypto.hash(:sha256, "codex-pooler-local-totp-key")
end
