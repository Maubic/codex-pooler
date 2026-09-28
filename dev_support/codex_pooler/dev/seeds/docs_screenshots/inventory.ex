defmodule CodexPooler.Dev.Seeds.DocsScreenshots.Inventory do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Catalog.Model
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Auth.{AccessTokenExpiry, TokenRefreshMetadata}
  alias CodexPooler.Upstreams.Quota.AccountQuotaWindow
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}

  @extra_pools [
    {"dev-docs-automation", "Example Automation"},
    {"dev-docs-research", "Example Research"},
    {"dev-docs-staging", "Example Staging"}
  ]
  @extra_accounts ["Example Build Agents", "Example Code Review", "Example Research Pro", "Example Staging Pro"]
  @bank_counts [3, 2, 0, 1, 0, 0, 4, 2, 3, 5, 2, 1]
  @access_expiry_minutes [4620, 1620, 540, 25, -120, 1680, 135, 1110, 8640, 45, 5400, 3060]
  @seed_key "codex_pooler_dev_seed"

  @spec reset!() :: :ok
  def reset! do
    slugs = Enum.map(@extra_pools, &elem(&1, 0))
    pools = Repo.all(from pool in Pool, where: pool.slug in ^slugs)

    Enum.each(pools, fn pool ->
      if pool.id != pool_id(pool.slug), do: raise("Pool #{pool.slug} is not owned by the screenshot seed")
    end)

    Enum.each(pools, &Repo.delete!/1)
    :ok
  end

  @spec expand!(map()) :: map()
  def expand!(result) do
    timestamp = DateTime.utc_now()
    pools = result.pools ++ Enum.map(@extra_pools, &insert_pool!(&1, result.owner, timestamp))
    extra_identities = insert_identities!(hd(result.upstream_identities), timestamp)

    identities =
      (result.upstream_identities ++ extra_identities)
      |> add_reset_banks!(timestamp)
      |> add_access_expiries!(timestamp)

    assignments = result.assignments ++ insert_assignments!(pools, identities, result.owner, timestamp)
    api_keys = result.api_keys ++ insert_api_keys!(tl(pools), result.owner, timestamp)
    models = result.models ++ insert_models!(pools, assignments, result.models, timestamp)

    Map.merge(result, %{pools: pools, upstream_identities: identities, assignments: assignments, api_keys: api_keys, models: models})
  end

  defp pool_id(slug), do: :crypto.hash(:sha256, "codex_pooler_docs_screenshots:#{slug}") |> binary_part(0, 16) |> Ecto.UUID.load!()

  defp insert_pool!({slug, name}, owner, timestamp) do
    %Pool{id: pool_id(slug)}
    |> Pool.changeset(%{slug: slug, name: name, status: "active", created_by_user_id: owner.id, created_at: timestamp, updated_at: timestamp})
    |> Repo.insert!()
  end

  defp insert_identities!(template, timestamp) do
    @extra_accounts
    |> Enum.with_index(9)
    |> Enum.map(fn {label, index} ->
      identity =
        %UpstreamIdentity{}
        |> UpstreamIdentity.changeset(%{
          chatgpt_account_id: "sample-account-#{index |> Integer.to_string() |> String.pad_leading(2, "0")}",
          account_label: label,
          onboarding_method: "import",
          status: "active",
          plan_family: "pro",
          plan_label: "Pro",
          headers_profile_version: 1,
          auth_fresh_at: timestamp,
          auth_verified_at: timestamp,
          last_successful_sync_at: timestamp,
          created_by_user_id: template.created_by_user_id,
          created_at: timestamp,
          updated_at: timestamp,
          metadata: %{"dev_seed" => @seed_key, "base_url" => Map.fetch!(template.metadata, "base_url")}
        })
        |> Repo.insert!()

      insert_quota_windows!(identity, index, timestamp)
      identity
    end)
  end

  defp insert_quota_windows!(identity, index, timestamp) do
    for {kind, minutes, remaining} <- [{"primary", 300, 92 - rem(index * 7, 37)}, {"secondary", 10_080, 88 - rem(index * 11, 53)}] do
      %AccountQuotaWindow{}
      |> AccountQuotaWindow.changeset(%{
        upstream_identity_id: identity.id,
        quota_key: "account",
        window_kind: kind,
        window_minutes: minutes,
        active_limit: 1000,
        credits: remaining * 10,
        used_percent: Decimal.new(100 - remaining),
        reset_at: DateTime.add(timestamp, minutes - index * 3, :minute),
        source: "dev_seed",
        source_precision: "observed",
        quota_scope: "account",
        quota_family: "account",
        freshness_state: "fresh",
        last_sync_at: timestamp,
        observed_at: timestamp,
        merge_precedence: 50,
        metadata: %{"dev_seed" => @seed_key},
        created_at: timestamp,
        updated_at: timestamp
      })
      |> Repo.insert!()
    end
  end

  defp add_reset_banks!(identities, timestamp) do
    identities
    |> Enum.zip(@bank_counts)
    |> Enum.with_index()
    |> Enum.map(fn {{identity, count}, index} ->
      expirations = reset_expirations(count, index, timestamp)
      dates = Enum.map(expirations, & &1["expires_at"])

      snapshot = %{
        "status" => "reported",
        "available_count" => count,
        "source" => "codex_usage_api",
        "path_style" => "codex_api",
        "usage_path" => "/api/codex/usage",
        "observed_at" => DateTime.to_iso8601(timestamp),
        "expires_observed_at" => DateTime.to_iso8601(timestamp),
        "available_expires_at" => dates,
        "available_expirations" => expirations,
        "next_expires_at" => List.first(dates)
      }

      identity
      |> UpstreamIdentity.changeset(%{
        metadata: Map.put(identity.metadata, "saved_resets", snapshot),
        saved_reset_auto_redeem_enabled: count > 0 and rem(index, 3) != 1,
        saved_reset_auto_redeem_keep_credits: if(count > 2, do: 1, else: 0)
      })
      |> Repo.update!()
    end)
  end

  defp add_access_expiries!(identities, timestamp) do
    identities
    |> Enum.with_index()
    |> Enum.map(fn {identity, index} ->
      deadline = DateTime.add(timestamp, Enum.fetch!(@access_expiry_minutes, index), :minute)
      resolution = AccessTokenExpiry.known(deadline, :explicit)
      epoch = Map.get(identity.metadata, "credential_epoch", 1)
      metadata = TokenRefreshMetadata.build_imported(identity.metadata, resolution, epoch, "docs_screenshot", timestamp)

      identity
      |> UpstreamIdentity.changeset(%{metadata: metadata})
      |> Repo.update!()
    end)
  end

  defp reset_expirations(0, _index, _timestamp), do: []

  defp reset_expirations(count, index, timestamp) do
    Enum.map(1..count, fn slot ->
      expires_at = DateTime.add(timestamp, 3 + rem(index, 6) + slot * 3, :day)
      granted_at = DateTime.add(expires_at, -30, :day)
      %{"expires_at" => DateTime.to_iso8601(expires_at), "granted_at" => DateTime.to_iso8601(granted_at), "first_seen_at" => DateTime.to_iso8601(granted_at)}
    end)
  end

  defp insert_assignments!(pools, identities, owner, timestamp) do
    # Shared upstreams make the Pool inventory match the multi-Pool product topology.
    memberships = [{0, [8, 9]}, {1, [1, 8]}, {2, [6, 7]}, {3, [0, 8, 9]}, {4, [1, 10]}, {5, [7, 11]}]

    for {pool_index, identity_indices} <- memberships, identity_index <- identity_indices do
      pool = Enum.at(pools, pool_index)
      identity = Enum.at(identities, identity_index)

      %PoolUpstreamAssignment{}
      |> PoolUpstreamAssignment.changeset(%{
        pool_id: pool.id,
        upstream_identity_id: identity.id,
        assignment_label: "#{pool.name} #{identity.chatgpt_account_id}",
        status: "active",
        health_status: "active",
        eligibility_status: "eligible",
        last_healthcheck_at: timestamp,
        last_successful_sync_at: timestamp,
        created_by_user_id: owner.id,
        created_at: timestamp,
        updated_at: timestamp,
        metadata: %{"dev_seed" => @seed_key, "quota_priming" => %{"status" => "known"}}
      })
      |> Repo.insert!()
    end
  end

  defp insert_api_keys!(pools, owner, timestamp) do
    pools
    |> Enum.with_index(5)
    |> Enum.map(fn {pool, index} ->
      %APIKey{}
      |> APIKey.changeset(%{
        pool_id: pool.id,
        display_name: "#{pool.name} client",
        key_prefix: "sk-cxp-docs#{index |> Integer.to_string() |> String.pad_leading(8, "0")}",
        key_hash: :crypto.hash(:sha256, "docs-only-nonissued-key:#{pool.id}"),
        status: "active",
        dashboard_access: false,
        created_by_user_id: owner.id,
        created_at: timestamp,
        metadata: %{"operator_notes" => "Generated for public documentation screenshots"}
      })
      |> Repo.insert!()
    end)
  end

  defp insert_models!(pools, assignments, existing, timestamp) do
    templates = existing |> Enum.filter(&(&1.status == "active")) |> Enum.uniq_by(& &1.exposed_model_id)

    for pool <- pools, template <- templates, not Enum.any?(existing, &(&1.pool_id == pool.id and &1.exposed_model_id == template.exposed_model_id)) do
      source_ids = assignments |> Enum.filter(&(&1.pool_id == pool.id)) |> Enum.map(& &1.id)
      source = template.metadata["source_assignment_models"] |> Map.values() |> hd() |> Map.put("use_responses_lite", false)

      attrs =
        template
        |> Map.from_struct()
        |> Map.drop([:__meta__, :id, :pool_id])
        |> Map.merge(%{
          pool_id: pool.id,
          source_assignment_count: length(source_ids),
          first_seen_at: timestamp,
          last_seen_at: timestamp,
          metadata: %{"dev_seed" => @seed_key, "source_assignment_ids" => source_ids, "source_assignment_models" => Map.new(source_ids, &{&1, source})}
        })

      %Model{} |> Model.changeset(attrs) |> Repo.insert!()
    end
  end
end
