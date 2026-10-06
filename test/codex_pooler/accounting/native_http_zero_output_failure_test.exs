defmodule CodexPooler.Accounting.NativeHttpZeroOutputFailureTest do
  # `ClientRetry.verified_native_http_zero_output_failure?/3` decides which failed
  # native HTTP chain node the client's exact retry may follow (findings#314 row
  # 314-1). It reads the persisted rows only; these are the shapes the HTTP
  # finalizers write, built as structs.
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting.{Attempt, ClientRetry, Request}
  alias CodexPooler.Gateway.Persistence.CodexTurn

  @request_id "8f2b1a32-4b5c-4c7d-9e0f-102132435465"
  @attempt_id "1e2d3c4b-5a69-4788-97a6-b5c4d3e2f101"
  @now ~U[2026-10-06 12:00:00.000000Z]

  describe "admits" do
    for arm <- ["opening", "steered_continuation", "tool_continuation"], transport <- ["http_sse", "http_json"] do
      test "a first-event server_error of the #{arm} arm over #{transport}" do
        assert ClientRetry.verified_native_http_zero_output_failure?(turn(transport: unquote(transport)), request(arm: unquote(arm), transport: unquote(transport)), attempt(transport: unquote(transport)))
      end
    end

    test "a failure decided before the relay began, whatever the turn showed" do
      for code <- ["rate_limit_exceeded", "upstream_status", "overloaded_error"] do
        assert ClientRetry.verified_native_http_zero_output_failure?(turn(code: code), request(code: code), attempt(code: code)), code
      end
    end

    test "a cut code other than upstream_stream_error while the turn showed nothing" do
      for code <- ["stream_idle_timeout", "owner_task_exception"] do
        assert ClientRetry.verified_native_http_zero_output_failure?(turn(code: code, visible_at: nil, status: "interrupted"), request(code: code), attempt(code: code)), code
      end
    end

    test "a relay that counted no completed item" do
      metadata = %{"native_http_resume_progress" => %{"version" => 1, "output_item_done_count" => 0, "digest" => String.duplicate("A", 43)}}
      assert ClientRetry.verified_native_http_zero_output_failure?(turn(), request(), attempt(metadata: metadata))
    end
  end

  describe "refuses" do
    test "upstream_stream_error, which keeps its own proofs, even before visible output" do
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(code: "upstream_stream_error", visible_at: nil), request(code: "upstream_stream_error"), attempt(code: "upstream_stream_error"))
    end

    test "a cut after the turn showed the client output" do
      for code <- ["stream_idle_timeout", "client_disconnected", "owner_task_exception"] do
        refute ClientRetry.verified_native_http_zero_output_failure?(turn(code: code), request(code: code), attempt(code: code)), code
      end
    end

    test "a relay that counted a completed item" do
      metadata = %{"native_http_resume_progress" => %{"version" => 1, "output_item_done_count" => 1, "digest" => String.duplicate("A", 43)}}
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), request(), attempt(metadata: metadata))
    end

    test "a websocket node, another generation, another arm or another route" do
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(transport: "websocket"), request(transport: "websocket"), attempt(transport: "websocket"))
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), request(), %{attempt() | replay_generation: 1})

      for arm <- ["post_compaction_resume", "compaction", "prewarm", "memory"] do
        refute ClientRetry.verified_native_http_zero_output_failure?(turn(), request(arm: arm), attempt()), arm
      end

      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), %{request() | endpoint: "/backend-api/codex/responses/compact"}, attempt())
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), %{request() | request_metadata: %{}}, attempt())
    end

    test "rows that do not agree, a served request or an unfinished one" do
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(code: "rate_limit_exceeded"), request(), attempt())
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), request(), %{attempt() | id: "2e2d3c4b-5a69-4788-97a6-b5c4d3e2f101"})
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), %{request() | status: "succeeded"}, attempt())
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), %{request() | completed_at: nil}, attempt())
      refute ClientRetry.verified_native_http_zero_output_failure?(%{turn() | completed_at: nil}, request(), attempt())
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), request(), %{attempt() | status: "in_progress"})
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(status: "succeeded"), request(), attempt())
      refute ClientRetry.verified_native_http_zero_output_failure?(turn(), request(), nil)
    end
  end

  defp turn(opts \\ []) do
    %CodexTurn{
      request_id: @request_id,
      status: Keyword.get(opts, :status, "failed"),
      error_code: Keyword.get(opts, :code, "server_error"),
      final_attempt_id: @attempt_id,
      transport_kind: Keyword.get(opts, :transport, "http_sse"),
      first_visible_output_at: Keyword.get(opts, :visible_at, @now),
      completed_at: @now
    }
  end

  defp request(opts \\ []) do
    %Request{
      id: @request_id,
      status: "failed",
      last_error_code: Keyword.get(opts, :code, "server_error"),
      transport: Keyword.get(opts, :transport, "http_sse"),
      endpoint: "/backend-api/codex/responses",
      completed_at: @now,
      request_metadata: %{"native_http_claim_arm" => Keyword.get(opts, :arm, "opening")}
    }
  end

  defp attempt(opts \\ []) do
    code = Keyword.get(opts, :code, "server_error")

    %Attempt{
      id: @attempt_id,
      request_id: @request_id,
      status: "failed",
      network_error_code: code,
      transport: Keyword.get(opts, :transport, "http_sse"),
      replay_generation: 0,
      completed_at: @now,
      response_metadata: Keyword.get(opts, :metadata, %{"stream_failure_stage" => "first_event", "stream_error_code" => code})
    }
  end
end
