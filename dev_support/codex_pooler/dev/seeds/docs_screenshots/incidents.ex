defmodule CodexPooler.Dev.Seeds.DocsScreenshots.Incidents do
  @moduledoc false

  # Provider status rows for the incident documentation images. They are not part of the scenario: an active incident puts
  # a banner above the page header of every admin page, so the standard captures run without them. The capture applies them
  # for the incident images only and clears them again.
  #
  # Titles and summaries are generic, the links are synthetic ids on the status host (the page only accepts that host),
  # and every row carries the marker prefix so a rerun replaces exactly its own.

  import Ecto.Query

  alias CodexPooler.OpenAIStatus
  alias CodexPooler.Repo
  alias CodexPooler.Status.Schemas.{FeedState, Incident}

  @prefix "docs-screenshot-incident-"

  # {id, status, minutes since the provider's last update, minutes it was first seen before that, title, component, summary, retired minutes ago}
  @incidents [
    {"01", "Investigating", 38, 22, "Elevated error rates for Responses API requests", "Responses API (Partial outage) Codex (Degraded performance)", "Some Responses API requests return errors. We are investigating the cause and will share an update shortly.", nil},
    {"02", "Monitoring", 118, 95, "Slower task completion in Codex", "Codex (Degraded performance)", "A fix has been applied and task completion times are recovering. We are monitoring the results.", nil},
    {"03", "Resolved", 310, 180, "Intermittent timeouts when starting Codex sessions", "Codex (Operational) Responses API (Operational)", "Session start timeouts are resolved. All affected systems are operating normally.", nil},
    {"04", "Resolved", 1_600, 420, "Delayed usage reporting for Codex", "Codex (Operational)", "Usage figures were delayed for some accounts. The backlog has been processed.", nil},
    {"05", "Resolved", 4_480, 600, "Increased latency for Chat Completions requests", "Chat Completions (Operational)", "Latency has returned to normal levels for all Chat Completions requests.", nil},
    {"06", "Investigating", 5_900, 90, "Intermittent errors on file uploads", "Files (Degraded performance)", "Some file uploads fail. We are investigating.", 4_300},
    {"07", "Resolved", 7_300, 240, "Sign-in failures for some Codex CLI users", "Codex (Operational)", "Sign-in works again for all users. We apologize for the disruption.", nil}
  ]

  @spec apply!() :: :ok
  def apply! do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)
    clear!()

    Enum.each(@incidents, fn {id, status, updated_minutes, seen_minutes, title, component, summary, retired_minutes} ->
      published_at = DateTime.add(now, -updated_minutes * 60, :second)

      {:ok, incident} =
        OpenAIStatus.upsert_incident(
          %{
            guid: @prefix <> id,
            title: title,
            status: status,
            summary: "Status: #{status} #{summary} Affected components #{component}",
            component: component,
            link: "https://status.openai.com/incidents/docs-sample-" <> id,
            published_at: published_at,
            content_hash: "docs-screenshot-" <> id
          },
          now
        )

      first_seen_at = DateTime.add(published_at, -seen_minutes * 60, :second)
      retired_at = retired_minutes && DateTime.add(now, -retired_minutes * 60, :second)
      Repo.update_all(from(i in Incident, where: i.id == ^incident.id), set: [first_seen_at: first_seen_at, last_seen_at: published_at, retired_at: retired_at])
    end)

    active = Enum.count(@incidents, fn {_id, status, _u, _s, _t, _c, _m, retired} -> status != "Resolved" and is_nil(retired) end)

    {:ok, _state} =
      OpenAIStatus.upsert_feed_state(%{
        last_success_at: DateTime.add(now, -120, :second),
        last_attempt_at: DateTime.add(now, -120, :second),
        last_error_code: nil,
        last_error_at: nil,
        active_count: active,
        aggregate_revision: DateTime.to_unix(now),
        updated_at: now
      })

    :ok
  end

  @doc "Removes the incident rows and the feed state, which returns the database to the state the scenario seeds."
  @spec clear!() :: :ok
  def clear! do
    Repo.delete_all(from(i in Incident, where: like(i.guid, ^(@prefix <> "%"))))
    Repo.delete_all(FeedState)
    :ok
  end
end
