# Evaluated by an already-running release. Never halt, reconfigure, or write
# from this remote process: the local shell maps the report to its exit status.
report =
  try do
    config = Application.get_env(:codex_pooler, CodexPooler.Accounts, [])
    configured = Keyword.get(config, :totp_encryption_key)
    legacy_key = :crypto.hash(:sha256, "codex-pooler-local-totp-key")

    {key_source, decoded} =
      cond do
        is_binary(configured) and byte_size(configured) == 32 -> {"configured_raw", {:ok, configured}}
        is_binary(configured) -> {"configured_base64", Base.decode64(configured)}
        true -> {"legacy_fallback", {:ok, legacy_key}}
      end

    key =
      case decoded do
        {:ok, value} when byte_size(value) == 32 -> value
        _ -> nil
      end

    key_report = %{
      key_source: key_source,
      configured_bytes: if(is_binary(configured), do: byte_size(configured), else: nil),
      decoded_bytes:
        case decoded do
          {:ok, value} -> byte_size(value)
          _ -> nil
        end,
      valid_key_shape: is_binary(key),
      known_legacy_key: key == legacy_key
    }

    {:ok, inventory} =
      CodexPooler.Repo.transaction(
        fn ->
          CodexPooler.Repo.query!("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
          CodexPooler.Repo.query!("SET LOCAL statement_timeout = '5s'")
          %Postgrex.Result{rows: [[total]]} = CodexPooler.Repo.query!("SELECT count(*) FROM totp_settings")

          if total > 10_000 do
            %{disposition: "inventory_limit_exceeded", total: total, checked: 0, inventory_complete: false}
          else
            # Do not transfer an unbounded malformed bytea into the release process.
            %Postgrex.Result{rows: rows} = CodexPooler.Repo.query!("SELECT status, secret_key_version, CASE WHEN octet_length(secret_ciphertext) = 60 THEN secret_ciphertext ELSE NULL END FROM totp_settings")
            version = Keyword.get(config, :totp_key_version, "v1")
            initial = %{total: total, checked: 0, decrypt_ok: 0, decrypt_failed: 0, invalid_plaintext: 0, malformed: 0, statuses: %{"pending" => 0, "active" => 0, "disabled" => 0, "other" => 0}, versions: %{"current" => 0, "other" => 0, "missing" => 0}}

            counts =
              Enum.reduce(rows, initial, fn [status, row_version, ciphertext], acc ->
                status_group = if status in ["pending", "active", "disabled"], do: status, else: "other"

                version_group =
                  cond do
                    row_version in [nil, ""] -> "missing"
                    row_version == version -> "current"
                    true -> "other"
                  end

                acc = acc |> update_in([:statuses, status_group], &(&1 + 1)) |> update_in([:versions, version_group], &(&1 + 1)) |> Map.update!(:checked, &(&1 + 1))

                case ciphertext do
                  <<nonce::binary-size(12), tag::binary-size(16), body::binary>> when is_binary(key) ->
                    result =
                      try do
                        case :crypto.crypto_one_time_aead(:aes_256_gcm, key, nonce, body, "totp", tag, false) do
                          plaintext when is_binary(plaintext) ->
                            case Base.decode32(plaintext, padding: false) do
                              {:ok, secret} when byte_size(secret) == 20 -> :decrypt_ok
                              _ -> :invalid_plaintext
                            end

                          _ ->
                            :decrypt_failed
                        end
                      rescue
                        _ -> :decrypt_failed
                      catch
                        _, _ -> :decrypt_failed
                      end

                    Map.update!(acc, result, &(&1 + 1))

                  <<_::binary-size(60)>> ->
                    acc

                  _ ->
                    Map.update!(acc, :malformed, &(&1 + 1))
                end
              end)

            disposition =
              cond do
                counts.checked != total -> "preflight_failed"
                counts.malformed > 0 or counts.decrypt_failed > 0 or counts.invalid_plaintext > 0 or counts.statuses["other"] > 0 or counts.versions["missing"] > 0 -> "restore_key_or_investigate"
                total == 0 and (is_nil(key) or key_report.known_legacy_key) -> "replace_totp_key"
                is_nil(key) -> "restore_key_or_investigate"
                key_report.known_legacy_key -> "offline_reencryption_required"
                counts.decrypt_ok == total -> "ready"
                true -> "restore_key_or_investigate"
              end

            Map.merge(counts, %{disposition: disposition, inventory_complete: counts.checked == total})
          end
        end,
        timeout: 15_000
      )

    Map.merge(key_report, inventory)
  rescue
    _ -> %{disposition: "preflight_failed", inventory_complete: false}
  catch
    _, _ -> %{disposition: "preflight_failed", inventory_complete: false}
  end

IO.puts("TOTP_UPGRADE_PREFLIGHT_V1 " <> Jason.encode!(report))
