defmodule CodexPooler.Gateway.RequestCompression.MetadataTest do
  use ExUnit.Case, async: true
  alias CodexPooler.Gateway.RequestCompression.Metadata

  test "runtime metadata omits missing measurements and invalid strategy names" do
    assert Metadata.request_envelope(nil) == %{}

    assert Metadata.request_envelope(%{attempted: true}) == %{
             "payload_compression" => %{"attempted" => true}
           }

    assert Metadata.runtime_metadata(%{attempted: false}) == nil

    assert Metadata.runtime_metadata(%{
             attempted: true,
             original_bytes: 10,
             original_tokens: 20,
             strategies: [:unknown]
           }) ==
             %{"attempted" => true, "original_bytes" => 10, "original_tokens" => 20}

    assert Metadata.runtime_metadata(%{
             "attempted" => true,
             "enabled" => false,
             "original_bytes" => 0,
             "compressed_bytes" => 0
           }) ==
             %{
               "attempted" => true,
               "enabled" => false,
               "original_bytes" => 0,
               "compressed_bytes" => 0,
               "saved_bytes" => 0
             }
  end

  test "runtime metadata keeps work-budget skips" do
    assert %{"reason" => "work_budget_exhausted", "work_budget_skipped_count" => 3} =
             Metadata.runtime_metadata(%{attempted: true, reason: :work_budget_exhausted, work_budget_skipped_count: 3})
  end

  test "exact savings are derived from counts and bounded counts never claim exact savings" do
    base = %{
      attempted: true,
      original_bytes: 100,
      compressed_bytes: 25,
      saved_bytes: 999,
      original_tokens: 10,
      compressed_tokens: 5,
      original_tokens_lower_bound: 8,
      strategies: List.duplicate(:diff, 15)
    }

    metadata = Metadata.runtime_metadata(base)
    assert metadata["saved_bytes"] == 75
    assert metadata["byte_savings_percent"] == 75.0
    assert metadata["compression_ratio"] == 0.25
    assert metadata["saved_tokens"] == 5
    assert length(metadata["strategies"]) == 12
    bounded = Metadata.runtime_metadata(Map.put(base, :token_count_mode, :bounded_original))
    assert bounded["original_tokens_lower_bound"] == 8
    refute Map.has_key?(bounded, "original_tokens")
    refute Map.has_key?(bounded, "saved_tokens")
  end

  test "sanitizer validates each metadata type and redacts unknown fields" do
    for {key, valid} <- [
          {:attempted, true},
          {:candidate_count, 2},
          {:compression_ratio, 1},
          {:compression_ratio, 0.5},
          {:route_class, "proxy_http"},
          {:status, "compressed"},
          {:reason, "rewritten"},
          {:reason, "work_budget_exhausted"},
          {:work_budget_skipped_count, 3},
          {:token_count_mode, "exact"}
        ] do
      assert Metadata.sanitize_map(%{key => valid}) == %{key => valid}
      assert Metadata.sanitize_map(%{key => nil}) == %{key => nil}
      assert Metadata.sanitize_map(%{key => []}) == %{key => "[REDACTED]"}
    end

    assert Metadata.sanitize_map(%{nil => "private", :unknown => "private"}) == %{
             nil => "[REDACTED]",
             :unknown => "[REDACTED]"
           }

    assert Metadata.sanitize_map(%{strategies: nil}) == %{strategies: nil}
    assert Metadata.sanitize_map(%{strategies: "diff"}) == %{strategies: "[REDACTED]"}
    assert Metadata.sanitize_map(%{strategies: [:diff, :unknown, nil]}) == %{strategies: ["diff"]}
  end

  test "identifiers reject unsafe values and credential-shaped strings" do
    for value <- [
          [],
          " ",
          "bad/name",
          "Bearer synthetic",
          "sk-cxp-0123456789ab-synthetic",
          "sk-" <> String.duplicate("x", 24)
        ] do
      assert Metadata.safe_identifier(value) == nil
    end

    assert Metadata.safe_identifier(:proxy_http) == "proxy_http"
    assert Metadata.sanitize_map(%{status: "unknown"}) == %{status: "[REDACTED]"}
    assert Metadata.sanitize_map(%{status: " "}) == %{status: nil}

    assert Metadata.put_writer_fields(%{}, :compressed, :rewritten, 0) == %{
             attempted: true,
             status: "compressed",
             reason: "rewritten",
             elapsed_ms: 0
           }
  end
end
