defmodule CodexPooler.Gateway.Transports.TransportFailureReasonEventTaxonomyTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Transports.TransportFailureReason
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.EventTaxonomy

  test "keeps finite known unknown and legacy response event buckets" do
    cases = [
      {"response.output_text", "response_event"},
      {"response.reasoning", "response_event"},
      {"response.mcp_call", "response_event"},
      {"response.metadata", "response_event"},
      {"response.compaction", "response_event"},
      {"response.unknown", "response_unknown_event"},
      {"response.other", "response_event"}
    ]

    for {event_type, event_class} <- cases do
      metadata =
        TransportFailureReason.sanitize_transport_failure_metadata(%{
          "last_upstream_event_type" => event_type,
          "last_upstream_event_class" => event_class
        })

      assert metadata == %{
               "last_upstream_event_type" => event_type,
               "last_upstream_event_class" => event_class
             }
    end
  end

  test "drops raw response subtypes even when their family is known" do
    metadata =
      TransportFailureReason.sanitize_transport_failure_metadata(%{
        "last_upstream_event_type" => "response.output_text.delta",
        "last_upstream_event_class" => "response_event"
      })

    assert metadata == %{"last_upstream_event_class" => "response_event"}
  end

  # The provider's compaction stream carries `response.compaction.compacting`
  # between the announced item and the closed one (measured, gpt-6-luna), so it
  # has a family of its own; a sibling the taxonomy does not list still
  # surfaces as unknown.
  test "classifies the compaction phase event in its own family and keeps unlisted siblings unknown" do
    assert EventTaxonomy.classify("response.compaction.compacting") == {"response.compaction", "response_event"}
    assert EventTaxonomy.allowed_event_type?("response.compaction")
    assert EventTaxonomy.classify("response.compaction.summarizing") == {"response.unknown", "response_unknown_event"}
  end
end
