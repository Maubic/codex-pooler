defmodule CodexPooler.Accounting.NativeReplayClaimTest do
  # findings#323: the replay claim an anchored native websocket request runs
  # under, recorded on its request row. The recorded shape is exact: anything
  # else is dropped whole by the metadata sanitizer and is not read back.
  use ExUnit.Case, async: true

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{NativeReplayClaim, Request}

  @claim :crypto.hash(:sha256, "synthetic anchored replay claim")

  test "the recorded shape survives the metadata sanitizer and reads back as the claim" do
    recorded = NativeReplayClaim.metadata(@claim)

    assert recorded == %{"version" => 1, "digest" => Base.url_encode64(@claim, padding: false)}
    assert Accounting.sanitize_metadata(%{"native_replay_claim" => recorded}) == %{"native_replay_claim" => recorded}
    assert NativeReplayClaim.recorded(%Request{transport: "websocket", request_metadata: %{"native_replay_claim" => recorded}}) == @claim
  end

  test "any other shape is dropped whole and reads back as nothing" do
    recorded = NativeReplayClaim.metadata(@claim)
    short = Base.url_encode64(binary_part(@claim, 0, 31), padding: false)
    padded = Base.url_encode64(@claim)

    for invalid <- [nil, "synthetic", [], %{}, Map.put(recorded, "version", 2), Map.put(recorded, "extra", true), Map.delete(recorded, "version"), Map.put(recorded, "digest", short), Map.put(recorded, "digest", padded), Map.put(recorded, "digest", String.duplicate("!", 43))] do
      assert Accounting.sanitize_metadata(%{"native_replay_claim" => invalid}) == %{"native_replay_claim" => %{}}
      assert NativeReplayClaim.recorded(%Request{transport: "websocket", request_metadata: %{"native_replay_claim" => invalid}}) == nil
    end
  end

  test "only a websocket request row carries a claim to read" do
    recorded = NativeReplayClaim.metadata(@claim)

    assert NativeReplayClaim.recorded(%Request{transport: "http_sse", request_metadata: %{"native_replay_claim" => recorded}}) == nil
    assert NativeReplayClaim.recorded(%Request{transport: "websocket", request_metadata: %{}}) == nil
    assert NativeReplayClaim.recorded(%Request{transport: "websocket", request_metadata: nil}) == nil
  end
end
