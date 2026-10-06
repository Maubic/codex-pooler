defmodule CodexPooler.Gateway.OpenAICompatibility.ResponsesWebSearchCallTest do
  # A Full-served model that ran the hosted `web_search` tool returns a `web_search_call` output item (`id`, `status`,
  # an `action` naming the search, the opened page or the looked-for pattern). `/v1` returns it, so a client that sends
  # `response.output` back as the history of its next stateless request meets it, and it used to be a `400
  # invalid_request` on `input` ("input item shape is not translatable"). The provider reads the item in a stateless
  # request in the Full and the Lite request shape (direct probe, 2026-10-06) and validates it strictly, so the
  # adapter mirrors that validation and forwards an admitted item untouched; a null is never forwarded because the
  # provider fails the whole request for a null `queries`.
  use ExUnit.Case, async: true

  alias CodexPooler.CompatibilityMatrix
  alias CodexPooler.Gateway.OpenAICompatibility.Responses

  @refusal %{status: 400, code: "invalid_request", param: "input", message: "input item shape is not translatable"}
  @search %{"id" => "ws_synthetic_0001", "type" => "web_search_call", "status" => "completed", "action" => %{"type" => "search", "query" => "synthetic release notes", "queries" => ["synthetic release notes"]}}

  @accepted [
    {"the item as the provider emits it", :as_emitted},
    {"a search with its sources", :sources},
    {"a search with a query only", :query_only},
    {"a search with queries only", :queries_only},
    {"a search with an empty query list", :empty_queries},
    {"an opened page", :open_page},
    {"an opened page without its url", :open_page_bare},
    {"a find in page", :find_in_page},
    {"a find in page without its pattern", :find_in_page_url_only},
    {"an item with an id only", :id_only},
    {"an item without id, status and action", :bare},
    {"an item without an id", :no_id},
    {"an item without a status", :no_status},
    {"an interrupted search status", :in_progress},
    {"a passthrough map", :passthrough_map},
    {"a null passthrough", :passthrough_null}
  ]

  @refused [
    {"an unknown item key", :unknown_item_key},
    {"a blank id", :blank_id},
    {"a null id", :null_id},
    {"a non-string id", :integer_id},
    {"a blank status", :blank_status},
    {"a null status", :null_status},
    {"a non-string status", :integer_status},
    {"a null action", :null_action},
    {"an action that is not an object", :string_action},
    {"an action without a type", :action_without_type},
    {"an action of an unknown type", :unknown_action_type},
    {"an unknown action key", :unknown_action_key},
    {"a search carrying the open_page url", :search_with_url},
    {"an opened page carrying a query", :open_page_with_query},
    {"a find in page carrying a query", :find_in_page_with_query},
    {"a null query", :null_query},
    {"a null query list", :null_queries},
    {"a null url", :null_url},
    {"a null pattern", :null_pattern},
    {"null sources", :null_sources},
    {"a non-string query", :integer_query},
    {"a query list that is not a list", :string_queries},
    {"a query list with a non-string entry", :mixed_queries},
    {"sources that are not a list", :map_sources},
    {"a source of another type", :source_other_type},
    {"a source without its url", :source_without_url},
    {"a source with an extra key", :source_extra_key},
    {"a source with a non-string url", :source_integer_url},
    {"a non-map passthrough", :passthrough_string}
  ]

  describe "admitted web_search_call items" do
    for {label, variant} <- @accepted do
      test "forwards #{label} untouched, in the position the client sent it" do
        item = mutate(unquote(variant), @search)

        assert {:ok, %{payload: %{"input" => input}}} = coerce(history(item))
        assert Enum.map(input, &(&1["type"] || "message")) == ["message", "web_search_call", "message", "message"]
        assert Enum.at(input, 1) == item
      end
    end

    test "keeps the assistant message that follows the item as the client sent it" do
      answer = assistant("synthetic answer")

      assert {:ok, %{payload: %{"input" => [_user, _call, forwarded, _follow_up]}}} = coerce([user("synthetic task"), @search, answer, user("synthetic follow-up")])
      assert forwarded["role"] == "assistant"
      assert forwarded["content"] == answer["content"]
    end
  end

  describe "refused web_search_call items" do
    for {label, variant} <- @refused do
      test "refuses #{label} before dispatch" do
        assert {:error, @refusal} = coerce(history(mutate(unquote(variant), @search)))
      end
    end
  end

  describe "the id the public stream invents for an item the upstream sent without one" do
    for id <- ["web_search_call", "web_search_call_0", "web_search_call_12"] do
      test "#{id} is dropped so the upstream receives the item as it produced it" do
        item = Map.put(@search, "id", unquote(id))

        assert {:ok, %{payload: %{"input" => input}}} = coerce(history(item))
        assert Enum.at(input, 1) == Map.delete(item, "id")
      end
    end

    for id <- ["ws_synthetic_0001", "web_search_call_first", "ws_call_0", "web_search_calls_1", "message_1"] do
      test "#{id} is not a fallback id and reaches the upstream unchanged" do
        item = Map.put(@search, "id", unquote(id))

        assert {:ok, %{payload: %{"input" => input}}} = coerce(history(item))
        assert Enum.at(input, 1) == item
      end
    end
  end

  describe "compatibility matrix" do
    test "states the contract this adapter implements" do
      contract = CompatibilityMatrix.fixture!(:responses_chat).web_search_call_history

      assert contract.accepted_items == ["web_search_call"]
      assert contract.action.types == ["search", "open_page", "find_in_page"]
      assert contract.nulls == "refused_except_passthrough"
      assert contract.serving_modes == ["full", "lite"]
      assert Map.delete(contract.refusal, :upstream_dispatch) == Map.take(@refusal, [:status, :code, :param])
      assert contract.refusal.upstream_dispatch == false
    end
  end

  defp coerce(input), do: Responses.coerce(%{"model" => "gpt-fixture-text", "input" => input})

  defp history(item), do: [user("synthetic task"), item, assistant("synthetic answer"), user("synthetic follow-up")]

  defp user(text), do: %{"role" => "user", "content" => [%{"type" => "input_text", "text" => text}]}

  defp assistant(text), do: %{"role" => "assistant", "content" => [%{"type" => "output_text", "text" => text}]}

  defp mutate(:as_emitted, item), do: item
  defp mutate(:sources, item), do: put_in(item, ["action", "sources"], [%{"type" => "url", "url" => "https://example.com/a"}, %{"type" => "url", "url" => "https://example.com/b"}])
  defp mutate(:query_only, item), do: update_in(item, ["action"], &Map.delete(&1, "queries"))
  defp mutate(:queries_only, item), do: update_in(item, ["action"], &Map.delete(&1, "query"))
  defp mutate(:empty_queries, item), do: put_in(item, ["action", "queries"], [])
  defp mutate(:open_page, item), do: Map.put(item, "action", %{"type" => "open_page", "url" => "https://example.com/"})
  defp mutate(:open_page_bare, item), do: Map.put(item, "action", %{"type" => "open_page"})
  defp mutate(:find_in_page, item), do: Map.put(item, "action", %{"type" => "find_in_page", "url" => "https://example.com/", "pattern" => "synthetic"})
  defp mutate(:find_in_page_url_only, item), do: Map.put(item, "action", %{"type" => "find_in_page", "url" => "https://example.com/"})
  defp mutate(:id_only, item), do: Map.take(item, ["type", "id"])
  defp mutate(:bare, item), do: Map.take(item, ["type"])
  defp mutate(:no_id, item), do: Map.delete(item, "id")
  defp mutate(:no_status, item), do: Map.delete(item, "status")
  defp mutate(:in_progress, item), do: Map.put(item, "status", "in_progress")
  defp mutate(:passthrough_map, item), do: Map.put(item, "internal_chat_message_metadata_passthrough", %{"turn_id" => "turn-synthetic-1"})
  defp mutate(:passthrough_null, item), do: Map.put(item, "internal_chat_message_metadata_passthrough", nil)
  defp mutate(:unknown_item_key, item), do: Map.put(item, "zz_probe_unknown_key", true)
  defp mutate(:blank_id, item), do: Map.put(item, "id", " ")
  defp mutate(:null_id, item), do: Map.put(item, "id", nil)
  defp mutate(:integer_id, item), do: Map.put(item, "id", 7)
  defp mutate(:blank_status, item), do: Map.put(item, "status", "")
  defp mutate(:null_status, item), do: Map.put(item, "status", nil)
  defp mutate(:integer_status, item), do: Map.put(item, "status", 1)
  defp mutate(:null_action, item), do: Map.put(item, "action", nil)
  defp mutate(:string_action, item), do: Map.put(item, "action", "search")
  defp mutate(:action_without_type, item), do: Map.put(item, "action", %{"query" => "synthetic"})
  defp mutate(:unknown_action_type, item), do: put_in(item, ["action", "type"], "bogus_action")
  defp mutate(:unknown_action_key, item), do: put_in(item, ["action", "zz_probe_unknown_key"], true)
  defp mutate(:search_with_url, item), do: put_in(item, ["action", "url"], "https://example.com/")
  defp mutate(:open_page_with_query, item), do: Map.put(item, "action", %{"type" => "open_page", "url" => "https://example.com/", "query" => "synthetic"})
  defp mutate(:find_in_page_with_query, item), do: Map.put(item, "action", %{"type" => "find_in_page", "url" => "https://example.com/", "query" => "synthetic"})
  defp mutate(:null_query, item), do: put_in(item, ["action", "query"], nil)
  defp mutate(:null_queries, item), do: put_in(item, ["action", "queries"], nil)
  defp mutate(:null_url, item), do: Map.put(item, "action", %{"type" => "open_page", "url" => nil})
  defp mutate(:null_pattern, item), do: Map.put(item, "action", %{"type" => "find_in_page", "url" => "https://example.com/", "pattern" => nil})
  defp mutate(:null_sources, item), do: put_in(item, ["action", "sources"], nil)
  defp mutate(:integer_query, item), do: put_in(item, ["action", "query"], 5)
  defp mutate(:string_queries, item), do: put_in(item, ["action", "queries"], "synthetic")
  defp mutate(:mixed_queries, item), do: put_in(item, ["action", "queries"], ["synthetic", 5])
  defp mutate(:map_sources, item), do: put_in(item, ["action", "sources"], %{"type" => "url", "url" => "https://example.com/"})
  defp mutate(:source_other_type, item), do: put_in(item, ["action", "sources"], [%{"type" => "file", "url" => "https://example.com/"}])
  defp mutate(:source_without_url, item), do: put_in(item, ["action", "sources"], [%{"type" => "url"}])
  defp mutate(:source_extra_key, item), do: put_in(item, ["action", "sources"], [%{"type" => "url", "url" => "https://example.com/", "title" => "synthetic"}])
  defp mutate(:source_integer_url, item), do: put_in(item, ["action", "sources"], [%{"type" => "url", "url" => 5}])
  defp mutate(:passthrough_string, item), do: Map.put(item, "internal_chat_message_metadata_passthrough", "turn-synthetic-1")
end
