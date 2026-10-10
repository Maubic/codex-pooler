defmodule CodexPooler.Dev.Seeds.ObservatoryPreview do
  @moduledoc """
  Adds sixty synthetic, settled request logs to one existing local Observatory key.

  Deterministic request ids make reruns additive and idempotent. Existing rows,
  credentials, model configuration and upstream identities remain untouched.
  """

  import Ecto.Query

  alias CodexPooler.Access.APIKey
  alias CodexPooler.Accounting.{Attempt, LedgerEntry, Request, RequestLogFacts}
  alias CodexPooler.Catalog.Model
  alias CodexPooler.Dev.Seeds.DocsScreenshots.Traffic
  alias CodexPooler.Pools.Pool
  alias CodexPooler.Repo

  @marker "dev-observatory-preview"
  @count 60

  @spec seed!(String.t()) :: %{inserted: non_neg_integer(), existing: non_neg_integer(), total: pos_integer()}
  def seed!(key_prefix) when is_binary(key_prefix) do
    unless Application.get_env(:codex_pooler, :dev_seeds_enabled, false), do: raise("development seeds are disabled for this environment")

    {:ok, result} =
      Repo.transact(fn ->
        key = load_key!(key_prefix)
        pool = Repo.get!(Pool, key.pool_id)
        models = Repo.all(from model in Model, where: model.pool_id == ^pool.id and model.status == "active", order_by: model.exposed_model_id)
        if pool.status != "active" or models == [], do: raise("Observatory preview requires an active Pool with active models")

        now = DateTime.utc_now()
        inserted = Enum.count(1..@count, &insert_missing!(pool, key, models, &1, now))
        {:ok, %{inserted: inserted, existing: @count - inserted, total: @count}}
      end)

    result
  end

  # Serialize simultaneous seeds for the same key across VMs through Postgres.
  defp load_key!(prefix) do
    now = DateTime.utc_now()

    case Repo.one(from key in APIKey, where: key.key_prefix == ^prefix and key.status == "active" and key.dashboard_access == true and (is_nil(key.expires_at) or key.expires_at > ^now), lock: "FOR UPDATE") do
      %APIKey{} = key -> key
      nil -> raise "Observatory preview requires an existing active dashboard-enabled API-key prefix"
    end
  end

  defp insert_missing!(pool, key, models, slot, now) do
    id = :crypto.hash(:sha256, "#{@marker}:#{key.id}:#{slot}") |> binary_part(0, 16) |> Ecto.UUID.load!()

    case Repo.get(Request, id) do
      nil ->
        insert_row!(pool, key, models, slot, now, id)
        true

      %Request{api_key_id: key_id, pool_id: pool_id, request_metadata: %{"dev_seed" => @marker}} when key_id == key.id and pool_id == pool.id ->
        false

      %Request{} ->
        raise "Observatory preview request id is occupied by a row outside its seed namespace"
    end
  end

  defp insert_row!(pool, key, models, slot, now, id) do
    model = Enum.at(models, rem(slot - 1, length(models)))
    tier = Traffic.service_tier(model.exposed_model_id, div(slot - 1, length(models)))
    client = Enum.at(Traffic.clients(), rem(slot - 1, length(Traffic.clients())))
    occurred_at = DateTime.add(now, -slot * 30, :second)
    identity = %{id: nil, account_label: "Example Preview", plan_family: "pro", plan_label: "Pro"}
    row = Traffic.traffic_row({pool, key, model, %{id: nil}, identity, {0, rem(slot, 24), slot, occurred_at}}, client, tier)
    metadata = %{"dev_seed" => @marker, "synthetic" => true}
    request_metadata = row.request.request_metadata |> Map.drop(["dev_seed", "docs_screenshot"]) |> Map.merge(metadata)

    request = Repo.insert!(struct!(Request, Map.merge(row.request, %{id: id, correlation_id: "#{@marker}-#{slot}", request_metadata: request_metadata})))
    :ok = RequestLogFacts.record_request_created!(request)

    attempt = Repo.insert!(struct!(Attempt, Map.merge(row.attempt, %{request_id: id, response_metadata: metadata})))
    :ok = RequestLogFacts.record_attempt_written!(attempt)

    details = row.entry.details |> Map.drop(["dev_seed", "docs_screenshot"]) |> Map.merge(metadata)
    entry = Repo.insert!(struct!(LedgerEntry, Map.merge(row.entry, %{request_id: id, details: details})))
    :ok = RequestLogFacts.record_settlement_written!(entry)
  end
end
