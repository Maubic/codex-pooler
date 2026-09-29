defmodule CodexPooler.Gateway.ContractsTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Contracts

  @anchor_headers [
    "x-codex-previous-response-id",
    "x-codex-turn-state",
    "x-codex-window-id",
    "x-codex-session-id",
    "session-id",
    "x-session-id",
    "x-session-affinity",
    "session_id",
    "x-codex-conversation-id"
  ]

  test "hard-pinned continuation recovery contracts carry restart guidance" do
    errors = [
      Contracts.pinned_continuation_reauth_required_error(),
      Contracts.pinned_continuation_unavailable_error(%{
        "pin_mode" => "hard",
        "pin_reason" => "previous_response_id",
        "internal_reason" => "quota_exhausted"
      })
    ]

    for error <- errors do
      assert error.status == 503
      assert error.retryable == false
      assert error.requires_new_upstream_session == true

      assert error.code in [
               "pinned_continuation_reauth_required",
               "pinned_continuation_unavailable"
             ]

      assert Contracts.recovery_response_headers(error) == [
               {"x-codex-recovery-kind", "restart_with_full_context"}
             ]

      assert %{
               "retryable" => false,
               "requires_new_upstream_session" => true,
               "recovery_kind" => "restart_with_full_context",
               "recovery" => recovery
             } = Contracts.recovery_error_fields(error)

      assert recovery["kind"] == "restart_with_full_context"

      assert recovery["guidance"] ==
               "Restart with full visible context and no continuation anchors."

      assert recovery["anchor_removal"]["body"] == ["previous_response_id"]
      assert recovery["anchor_removal"]["headers"] == @anchor_headers

      assert "Full visible context means client-visible conversation state and tool results." in recovery[
               "notes"
             ]

      assert "Do not replay stored prompts or hidden server state." in recovery["notes"]
      assert Contracts.hard_pinned_continuation_recovery?(error)
    end
  end

  test "recovery fields are limited to hard-pinned continuation recovery errors" do
    for error <- [
          %{status: 503, code: "session_assignment_unavailable", message: "session unavailable"},
          %{status: 400, code: "unsupported_model_capability", message: "model unsupported"},
          %{status: 400, code: "invalid_request", message: "request invalid"}
        ] do
      assert Contracts.recovery_response_headers(error) == []
      assert Contracts.recovery_error_fields(error) == %{}
      refute Contracts.pinned_continuation_reauth_required?(error)
      refute Contracts.hard_pinned_continuation_recovery?(error)
    end
  end

  # findings#279 point 2: the Codex Desktop app shows the message of a `400
  # invalid_prompt` as sent, and never the reset of a `429
  # usage_limit_reached`.
  describe "native_usage_limit_answer/2" do
    test "answers Codex Desktop 400 invalid_prompt with the reset, rounded up to the minute, in the message" do
      for {resets_at, seconds, wait} <- [
            {1_790_904_430, 5_008, "at 01:28 UTC (in about 1 h 24 min)"},
            {1_790_904_420, 840, "at 01:27 UTC (in about 14 min)"},
            {1_790_904_420, 1, "at 01:27 UTC (in about 1 min)"},
            {1_790_904_420, 7_200, "at 01:27 UTC (in about 2 h)"},
            {1_790_904_420, 86_400, "on 2026-10-02 at 01:27 UTC (in about 1 day)"},
            {1_791_071_999, 90_000, "on 2026-10-04 at 00:00 UTC (in about 1 day 1 h)"},
            {1_791_180_030, 273_900, "on 2026-10-05 at 06:01 UTC (in about 3 days 5 h)"}
          ] do
        refusal = pool_refusal(resets_at, seconds)
        answer = Contracts.native_usage_limit_answer(refusal, "Codex Desktop")

        assert Map.take(answer, [:status, :code, :message, :param, :usage_limit]) == %{
                 status: 400,
                 code: "invalid_prompt",
                 message: "The Pool's usage limit is reached. Try again #{wait}.",
                 param: nil,
                 usage_limit: refusal.usage_limit
               }

        assert Contracts.usage_limit_error_fields(answer) == %{"resets_at" => resets_at, "resets_in_seconds" => seconds}
        assert Contracts.usage_limit_record(answer) == Contracts.usage_limit_record(refusal)
        assert Contracts.usage_limit_response_headers(answer) == [{"x-should-retry", "false"}]
      end
    end

    test "leaves every other originator and every other refusal unchanged" do
      refusal = pool_refusal(1_790_904_430, 5_008)

      for originator <- ["codex-tui", "codex_exec", "codex_vscode", "codex desktop", "Codex Desktop/0.158.0", nil] do
        assert Contracts.native_usage_limit_answer(refusal, originator) == refusal
      end

      for error <- [
            Map.merge(Map.delete(refusal, :usage_limit), %{status: 503}),
            %{status: 429, code: "api_key_policy_limit_exceeded", message: "policy", pooler_policy: true, retry_after_seconds: 30},
            %{status: 400, code: "invalid_prompt", message: "provider refusal"}
          ] do
        assert Contracts.native_usage_limit_answer(error, "Codex Desktop") == error
        assert Contracts.usage_limit_error_fields(error) == %{}
        assert Contracts.usage_limit_response_headers(error) == []
      end
    end
  end

  defp pool_refusal(resets_at, seconds),
    do: %{status: 429, code: "quota_exhausted", message: "upstream quota is exhausted until its reset time", param: "model", usage_limit: %{resets_at: resets_at, resets_in_seconds: seconds}}
end
