defmodule CodexPooler.QuotasTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Quotas
  alias CodexPooler.Quotas.Evidence

  @observed_at ~U[2026-09-07 10:00:00Z]
  @reset_at ~U[2026-09-07 11:00:00Z]

  for {label, balance, expected} <- [
        {"integer overflow", 9_223_372_036_854_775_808, nil},
        {"string overflow", "9223372036854775808", nil},
        {"float overflow", 1.0e20, nil},
        {"exponent overflow", "1e20", nil},
        {"integer maximum", 9_223_372_036_854_775_807, 9_223_372_036_854_775_807},
        {"string maximum", "9223372036854775807", 9_223_372_036_854_775_807},
        {"zero", 0, 0},
        {"float rounding", 12.5, 13},
        {"string rounding", "12.4", 12}
      ] do
    test "usage credit #{label} preserves windows with a storable balance" do
      payload = Map.put(usage_payload(), "credits", %{"balance" => unquote(balance)})
      assert {:ok, [evidence]} = Quotas.parse_codex_usage_payload(payload, @observed_at)
      assert evidence.active_limit == unquote(expected)
      assert evidence.credits == unquote(expected)
      assert evidence.reset_at == @reset_at
      assert Decimal.equal?(evidence.used_percent, 40)
    end
  end

  for field <- [:credits, :active_limit] do
    test "direct evidence rejects #{field} above the bigint boundary" do
      assert {:ok, [evidence]} = Quotas.parse_codex_usage_payload(usage_payload(), @observed_at)
      oversized = Map.put(evidence, unquote(field), 9_223_372_036_854_775_808)
      assert {:error, errors} = Evidence.validate(oversized)
      assert Map.has_key?(errors, unquote(field))
      assert {:error, errors} = Evidence.new(Map.from_struct(oversized), @observed_at)
      assert Map.has_key?(errors, unquote(field))
    end
  end

  test "normalizes string attributes and validates the resulting account evidence" do
    attrs = %{
      "window_kind" => " PRIMARY ",
      "window_minutes" => "300",
      "used_percent" => "42.5",
      "source" => "codex_usage_api",
      "reset_at" => DateTime.to_iso8601(@reset_at)
    }

    assert {:ok, evidence} = Quotas.normalize_evidence(attrs, @observed_at)
    assert evidence.quota_key == "account"
    assert evidence.window_kind == "primary"
    assert evidence.window_minutes == 300
    assert Decimal.equal?(evidence.used_percent, "42.5")
    assert evidence.observed_at == @observed_at
    assert evidence.reset_at == @reset_at
    assert :ok = Quotas.validate_evidence(attrs)
    assert :ok = Quotas.validate_evidence(evidence)

    assert {:error, %{window_minutes: [_]}} =
             Quotas.validate_evidence(%{attrs | "window_minutes" => "0"})

    assert {:error, %{attrs: ["must be a map"]}} = Quotas.validate_evidence(nil)
  end

  test "default observation clocks bound evidence and relative reset times" do
    before = DateTime.utc_now()

    assert {:ok, normalized} =
             Quotas.normalize_evidence(%{
               window_kind: "primary",
               window_minutes: 300,
               source: "codex_usage_api"
             })

    assert {:ok, [usage]} = Quotas.parse_codex_usage_payload(usage_payload())
    assert [headers] = Quotas.parse_codex_headers(headers())
    assert [event] = Quotas.parse_codex_rate_limit_event(event())
    assert [error] = Quotas.parse_rate_limit_error(error_payload())
    after_call = DateTime.utc_now()

    for evidence <- [normalized, usage, headers, event, error] do
      assert DateTime.compare(evidence.observed_at, before) in [:eq, :gt]
      assert DateTime.compare(evidence.observed_at, after_call) in [:eq, :lt]
      assert evidence.last_sync_at == evidence.observed_at
    end

    for evidence <- [usage, event, error] do
      assert DateTime.diff(evidence.reset_at, evidence.observed_at) == 3600
    end
  end

  test "usage parsing keeps valid windows and rejects unusable payloads" do
    assert {:ok, [evidence]} = Quotas.parse_codex_usage_payload(usage_payload(), @observed_at)
    assert evidence.source == "codex_usage_api"
    assert evidence.reset_at == @reset_at
    assert Decimal.equal?(evidence.used_percent, 40)

    for payload <- [nil, [], %{}, %{"rate_limit" => %{"primary_window" => false}}] do
      assert {:error, %{code: :upstream_quota_unusable}} =
               Quotas.parse_codex_usage_payload(payload, @observed_at)
    end
  end

  test "headers use case-insensitive names and normalize weekly primary slots" do
    assert [evidence] = Quotas.parse_codex_headers(headers(), @observed_at)
    assert evidence.window_kind == "secondary"
    assert evidence.window_minutes == 10_080
    assert evidence.reset_at == @reset_at
    assert evidence.source == "codex_response_headers"
    assert evidence.metadata == %{"header_limit_id" => "codex"}
    assert [] = Quotas.parse_codex_headers(nil, @observed_at)
  end

  test "events retain valid primary windows when the secondary window is malformed" do
    payload = put_in(event(), ["rate_limits", "secondary"], false)
    assert [evidence] = Quotas.parse_codex_rate_limit_event(payload, @observed_at)
    assert evidence.source == "codex_rate_limit_event"
    assert evidence.reset_at == @reset_at
    assert Evidence.current_freshness_state(evidence, @observed_at) == "fresh"

    assert [] =
             Quotas.parse_codex_rate_limit_event(%{"type" => "response.completed"}, @observed_at)
  end

  test "malformed rate_limits containers do not crash quota observation" do
    for limits <- [nil, false, true, 42, "invalid", [], [%{}]] do
      assert [] =
               Quotas.parse_codex_rate_limit_event(
                 %{"type" => "codex.rate_limits", "rate_limits" => limits},
                 @observed_at
               )
    end
  end

  test "rate limit errors require both duration and reset proof" do
    assert [evidence] = Quotas.parse_rate_limit_error(error_payload(), @observed_at)
    assert evidence.source == "codex_rate_limit_error"
    assert evidence.reset_at == @reset_at
    assert Decimal.equal?(evidence.used_percent, 100)

    for key <- ["window_minutes", "reset_after_seconds"] do
      assert [] = Quotas.parse_rate_limit_error(Map.delete(error_payload(), key), @observed_at)
    end

    assert [] = Quotas.parse_rate_limit_error(nil, @observed_at)
  end

  defp usage_payload do
    %{
      "rate_limit" => %{
        "primary_window" => %{
          "used_percent" => 40,
          "limit_window_seconds" => 18_000,
          "reset_after_seconds" => 3600
        }
      }
    }
  end

  defp headers do
    [
      {"X-Codex-Primary-Used-Percent", "40"},
      {"X-Codex-Primary-Window-Minutes", "10080"},
      {"X-Codex-Primary-Reset-At", DateTime.to_iso8601(@reset_at)}
    ]
  end

  defp event do
    %{
      "type" => "codex.rate_limits",
      "rate_limits" => %{
        "primary" => %{
          "used_percent" => 40,
          "window_minutes" => 300,
          "reset_after_seconds" => 3600
        }
      }
    }
  end

  defp error_payload do
    %{"window_minutes" => 300, "used_percent" => 100, "reset_after_seconds" => 3600}
  end
end
