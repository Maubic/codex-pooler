defmodule CodexPooler.Gateway.OpenAICompatibility.ResponsesWebSearchToolTest do
  # The hosted `web_search` tool keys `/v1/responses` admits are the keys the provider accepts. The provider validates
  # them strictly, in the Full request and in the Lite manifest alike (direct probe, 2026-10-06): it accepts
  # `external_web_access`, `indexed_web_access`, `filters`, `user_location`, `search_context_size` and
  # `search_content_types`, and answers `400 unknown_parameter` to every other key, the pre-0.144.0 Codex spelling
  # `index_gated_web_access` included. Released Codex serializes exactly the accepted keys since 0.144.0 (a bundled Full
  # catalog entry adds `search_content_types`, an indexed search adds `indexed_web_access`), so the adapter forwards
  # them untouched and refuses everything else before dispatch.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.OpenAICompatibility.Responses

  @refusal %{status: 400, code: "invalid_request", param: "tools", message: "tool shape is not translatable"}
  @cached %{"type" => "web_search", "external_web_access" => false}
  @indexed %{"type" => "web_search", "external_web_access" => true, "indexed_web_access" => true}
  @location %{"type" => "approximate", "country" => "US", "region" => "California", "city" => "San Francisco", "timezone" => "America/Los_Angeles"}

  describe "accepted web_search shapes" do
    for {label, tool} <- [
          {"cached search, Codex's default", @cached},
          {"live search", %{"type" => "web_search", "external_web_access" => true}},
          {"type only", %{"type" => "web_search"}},
          {"a bundled Full catalog entry's text and image search", Map.put(@cached, "search_content_types", ["text", "image"])},
          {"indexed search as released Codex builds it", @indexed},
          {"a text-only content type list", Map.put(@cached, "search_content_types", ["text"])},
          {"an image-only content type list", Map.put(@cached, "search_content_types", ["image"])},
          {"an allowed domain list", Map.put(@cached, "filters", %{"allowed_domains" => ["example.com"]})},
          {"a blocked domain list", Map.put(@cached, "filters", %{"blocked_domains" => ["example.org"]})},
          {"a full approximate user location", Map.put(@cached, "user_location", @location)},
          {"an approximate user location with a country only", Map.put(@cached, "user_location", %{"type" => "approximate", "country" => "US"})},
          {"an approximate user location of type alone", Map.put(@cached, "user_location", %{"type" => "approximate"})},
          {"search context size low", Map.put(@cached, "search_context_size", "low")},
          {"search context size medium", Map.put(@cached, "search_context_size", "medium")},
          {"search context size high", Map.put(@cached, "search_context_size", "high")},
          {"every key released Codex serializes at once",
           %{
             "type" => "web_search",
             "external_web_access" => false,
             "filters" => %{"allowed_domains" => ["example.com"]},
             "user_location" => @location,
             "search_context_size" => "low",
             "search_content_types" => ["text", "image"]
           }}
        ] do
      test "forwards #{label} unchanged" do
        tool = unquote(Macro.escape(tool))

        assert {:ok, %{payload: payload}} = coerce([tool])
        assert payload["tools"] == [tool]
      end
    end

    test "keeps accepting a declaration-backed allowed_tools choice for a web_search tool that carries the new keys" do
      tool = Map.merge(@cached, %{"search_context_size" => "low", "search_content_types" => ["text", "image"]})
      choice = %{"type" => "allowed_tools", "mode" => "auto", "tools" => [%{"type" => "web_search"}]}

      assert {:ok, %{payload: payload}} = Responses.coerce(%{"model" => "gpt-fixture-text", "input" => "synthetic input", "tools" => [tool], "tool_choice" => choice})
      assert payload["tools"] == [tool]
      assert payload["tool_choice"] == choice
    end
  end

  describe "refused web_search shapes" do
    for {label, tool} <- [
          {"the pre-0.144.0 spelling index_gated_web_access with live search", %{"type" => "web_search", "external_web_access" => true, "index_gated_web_access" => true}},
          {"index_gated_web_access false", %{"type" => "web_search", "external_web_access" => true, "index_gated_web_access" => false}},
          {"index_gated_web_access beside cached search", Map.put(@cached, "index_gated_web_access", true)},
          {"index_gated_web_access alone", %{"type" => "web_search", "index_gated_web_access" => true}},
          {"a key the provider does not know", Map.put(@cached, "zz_probe_unknown_key", true)},
          {"return_token_budget, which only the provider's response echoes", Map.put(@cached, "return_token_budget", "default")},
          {"indexed_web_access false", Map.put(@indexed, "indexed_web_access", false)},
          {"indexed_web_access beside cached search", %{@indexed | "external_web_access" => false}},
          {"indexed_web_access without external_web_access", %{"type" => "web_search", "indexed_web_access" => true}},
          {"a non-boolean indexed_web_access", %{@indexed | "indexed_web_access" => "true"}},
          {"a non-boolean external_web_access", %{"type" => "web_search", "external_web_access" => "true"}},
          {"a user location that is not an object", Map.put(@cached, "user_location", "US")},
          {"a null user location", Map.put(@cached, "user_location", nil)},
          {"a user location sub-key the provider does not know", Map.put(@cached, "user_location", %{"type" => "approximate", "zz_probe_unknown_key" => "x"})},
          {"a user location without the type the provider requires", Map.put(@cached, "user_location", %{"country" => "US"})},
          {"an empty user location", Map.put(@cached, "user_location", %{})},
          {"a user location type other than approximate", Map.put(@cached, "user_location", %{"type" => "exact", "country" => "US"})},
          {"a null user location field", Map.put(@cached, "user_location", %{"type" => "approximate", "city" => nil})},
          {"a non-string user location field", Map.put(@cached, "user_location", %{"type" => "approximate", "country" => 1})},
          {"a blank user location field", Map.put(@cached, "user_location", %{"type" => "approximate", "timezone" => " "})},
          {"a search context size outside the provider vocabulary", Map.put(@cached, "search_context_size", "huge")},
          {"a blank search context size", Map.put(@cached, "search_context_size", "")},
          {"a numeric search context size", Map.put(@cached, "search_context_size", 1)},
          {"a null search context size", Map.put(@cached, "search_context_size", nil)},
          {"a content type outside the provider vocabulary", Map.put(@cached, "search_content_types", ["video"])},
          {"one bad content type beside a good one", Map.put(@cached, "search_content_types", ["text", "video"])},
          {"an empty content type list", Map.put(@cached, "search_content_types", [])},
          {"a content type string instead of a list", Map.put(@cached, "search_content_types", "text")},
          {"a non-string content type", Map.put(@cached, "search_content_types", ["text", 1])},
          {"a null content type list", Map.put(@cached, "search_content_types", nil)}
        ] do
      test "refuses #{label} before dispatch" do
        assert {:error, @refusal} = coerce([unquote(Macro.escape(tool))])
      end
    end

    # The provider refuses the tool type itself (findings#333): every shape is refused, the type-only one included, and
    # none is rewritten to `web_search`.
    test "refuses web_search_preview in every shape" do
      refusal = %{@refusal | message: "web_search_preview tools are not supported; declare web_search"}

      for option <- [%{}, %{"search_context_size" => "low"}, %{"user_location" => @location}, %{"search_content_types" => ["text"]}] do
        assert {:error, ^refusal} = coerce([Map.merge(%{"type" => "web_search_preview"}, option)])
      end
    end
  end

  defp coerce(tools), do: Responses.coerce(%{"model" => "gpt-fixture-text", "input" => "synthetic input", "tools" => tools})
end
