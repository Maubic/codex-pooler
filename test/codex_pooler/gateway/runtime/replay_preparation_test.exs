defmodule CodexPooler.Gateway.Runtime.ReplayPreparationTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Access.APIKeys.ReasoningEffortPolicy.Decision
  alias CodexPooler.Accounting
  alias CodexPooler.Catalog.Model
  alias CodexPooler.Gateway.Payloads.PayloadNormalizer
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Runtime.Dispatch.ReplayPreparation
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext
  alias CodexPooler.Pools.RoutingSettings

  test "numeric budget preparations survive durable sanitization and restore without permitting numeric policy configuration" do
    original = original_context()
    base = ReplayPreparation.attempt_metadata(original)["native_replay_preparation"]

    for effort <- [0, 64, 18_446_744_073_709_551_615] do
      snapshot = Map.merge(base, %{"reasoning_mode" => "unrestricted", "configured_effort" => nil, "requested_effort" => effort, "applied_effort" => effort})
      metadata = Accounting.sanitize_metadata(%{"native_replay_preparation" => snapshot})
      assert ReplayPreparation.replay_eligible?(metadata)
      assert {:ok, restored, _settings} = ReplayPreparation.restore(RequestOptions.for_websocket(%{}), metadata)
      assert restored.routing.reasoning_effort_decision.requested_effort === effort
      assert restored.routing.reasoning_effort_decision.applied_effort === effort
      assert ReplayPreparation.sanitize(Map.put(snapshot, "configured_effort", effort)) == %{}
    end

    for effort <- [-1, 18_446_744_073_709_551_616, 64.0] do
      assert ReplayPreparation.sanitize(Map.put(base, "requested_effort", effort)) == %{}
      assert ReplayPreparation.sanitize(Map.put(base, "applied_effort", effort)) == %{}
    end
  end

  test "original native preparation survives metadata sanitization and replaces replay defaults" do
    original = original_context()
    metadata = original |> ReplayPreparation.attempt_metadata() |> Accounting.sanitize_metadata()

    assert {:ok, restored, settings} =
             ReplayPreparation.restore(RequestOptions.for_websocket(%{}, %{}), metadata)

    assert RequestOptions.model_serving_mode_snapshot(restored) ==
             RequestOptions.model_serving_mode_snapshot(original.request_options)

    assert restored.routing.reasoning_effort_decision ==
             original.request_options.routing.reasoning_effort_decision

    assert restored.routing.supports_reasoning_summary_parameter? == false
    assert %RoutingSettings{} = settings
    assert metadata["native_replay_preparation"]["request_compression_enabled"] == false
    assert restored.continuity.request_claim_key == nil
    assert restored.transport.websocket_owner.enabled? == false
  end

  test "snapshot rejects malformed policy and drops arbitrary fields" do
    metadata = ReplayPreparation.attempt_metadata(original_context())
    snapshot = metadata["native_replay_preparation"]

    sanitized =
      Accounting.sanitize_metadata(%{
        "native_replay_preparation" => Map.merge(snapshot, %{"unknown" => "synthetic", "instructions" => "synthetic"})
      })

    assert sanitized == metadata

    for malformed <- ["synthetic", ["synthetic"], 1, nil] do
      assert Accounting.sanitize_metadata(%{"native_replay_preparation" => malformed}) ==
               %{"native_replay_preparation" => %{}}
    end

    for {key, value} <- [
          {"version", 2},
          {"effective_mode", "invalid"},
          {"reasoning_mode", "invalid"},
          {"applied_effort", "invalid"},
          {"supports_reasoning_summary", "true"},
          {"request_compression_enabled", "true"}
        ] do
      invalid = %{"native_replay_preparation" => Map.put(snapshot, key, value)}

      assert Accounting.sanitize_metadata(invalid) == %{"native_replay_preparation" => %{}}

      assert {:error, :invalid_replay_preparation} =
               ReplayPreparation.restore(RequestOptions.for_websocket(%{}, %{}), invalid)
    end
  end

  test "preparation preserves only a well-formed turn models ETag for the replay" do
    models_etag = ~s(W/"cp-models-v1-#{String.duplicate("0f", 32)}")
    original = original_context()

    original = %{
      original
      | route_state: RouteState.put_codex_models_etag(original.route_state, models_etag)
    }

    metadata = original |> ReplayPreparation.attempt_metadata() |> Accounting.sanitize_metadata()
    assert metadata["native_replay_preparation"]["models_etag"] == models_etag
    assert ReplayPreparation.models_etag(metadata) == models_etag

    assert {:ok, _restored, _settings} =
             ReplayPreparation.restore(RequestOptions.for_websocket(%{}, %{}), metadata)

    legacy = ReplayPreparation.attempt_metadata(original_context())
    refute Map.has_key?(legacy["native_replay_preparation"], "models_etag")
    assert ReplayPreparation.models_etag(legacy) == nil

    snapshot = metadata["native_replay_preparation"]

    for invalid <- [
          "hostile-provider-etag-sentinel",
          ~s(W/"cp-models-v1-short"),
          ~s(W/"cp-models-v1-#{String.duplicate("0F", 32)}"),
          models_etag <> "\n",
          1,
          nil
        ] do
      sanitized =
        Accounting.sanitize_metadata(%{
          "native_replay_preparation" => Map.put(snapshot, "models_etag", invalid)
        })

      assert sanitized["native_replay_preparation"] == Map.delete(snapshot, "models_etag")
      assert ReplayPreparation.models_etag(sanitized) == nil
    end

    for malformed <- [nil, "metadata", %{"native_replay_preparation" => "synthetic"}] do
      assert ReplayPreparation.models_etag(malformed) == nil
    end
  end

  test "ordinary and public translated requests do not receive native replay preparation" do
    original = original_context()

    ordinary = RequestOptions.for_websocket(%{websocket_owner_forwarding_enabled?: true}, %{})

    assert ReplayPreparation.attempt_metadata(%{original | request_options: ordinary}) == %{}

    translated =
      RequestOptions.mark_openai_compatibility_origin(
        original.request_options,
        "/v1/responses",
        "/backend-api/codex/responses"
      )

    assert ReplayPreparation.attempt_metadata(%{original | request_options: translated}) == %{}
  end

  test "restored preparation retains real normalization and exact tool output" do
    payload = %{
      "model" => "gpt-4o",
      "instructions" => "synthetic instructions",
      "reasoning" => %{"summary" => "auto"},
      "input" => [
        %{
          "type" => "function_call",
          "name" => "sample_tool",
          "call_id" => "call_sample",
          "arguments" => "{}"
        },
        %{
          "type" => "function_call_output",
          "call_id" => "call_sample",
          "output" => CodexPooler.JSON.encode!(%{"rows" => Enum.to_list(1..160)}, pretty: true)
        }
      ],
      "stream" => true
    }

    original = original_context()
    original = %{original | payload: payload}
    metadata = ReplayPreparation.attempt_metadata(original)

    assert {:ok, restored, settings} =
             ReplayPreparation.restore(RequestOptions.for_websocket(%{}, payload), metadata)

    replay = %{
      original
      | request_options: restored,
        route_state: %{original.route_state | routing_settings: settings}
    }

    {original_bytes, _original_opts} = prepared_payload(original)
    {replay_bytes, _replay_opts} = prepared_payload(replay)
    assert original_bytes == replay_bytes
    decoded = CodexPooler.JSON.decode!(replay_bytes)
    assert List.last(decoded["input"])["output"] == List.last(payload["input"])["output"]
  end

  defp prepared_payload(context) do
    assert {:ok, bytes, options} =
             PayloadNormalizer.prepare_upstream_payload(context.payload, context.model, context.endpoint, context.request_options)

    {bytes, options}
  end

  defp original_context do
    options =
      RequestOptions.for_websocket(
        %{
          websocket_owner_forwarding_enabled?: true,
          request_claim_key: "codex-request:" <> Base.url_encode64(<<0::256>>, padding: false),
          replay_claim_digest: <<0::256>>
        },
        %{}
      )
      |> RequestOptions.put_model_serving_mode(%{
        configured_mode: "lite",
        effective_mode: "lite",
        source: "override"
      })
      |> RequestOptions.put_routing(
        reasoning_effort_decision: %Decision{
          mode: :allow_up_to,
          configured_effort: "medium",
          requested_effort: nil,
          applied_effort: "medium"
        },
        supports_reasoning_summary_parameter?: false
      )

    %SelectedCandidateContext{
      model: %Model{exposed_model_id: "gpt-4o", upstream_model_id: "gpt-4o"},
      endpoint: "/backend-api/codex/responses",
      route_class: "proxy_websocket",
      request_options: options,
      route_state:
        RouteState.new(%{
          visible_model: %Model{},
          candidates: [],
          routing_settings: Map.put(%RoutingSettings{}, :request_compression_enabled, true)
        })
    }
  end
end
