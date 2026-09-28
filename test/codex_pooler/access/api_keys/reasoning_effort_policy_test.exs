defmodule CodexPooler.Access.APIKeys.ReasoningEffortPolicyTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Access
  alias CodexPooler.Access.APIKey
  alias CodexPooler.Access.APIKeys.ReasoningEffortPolicy.{Decision, MetadataProjection}

  @known ~w(none minimal low medium high xhigh max ultra)
  @fallback ~w(low medium high xhigh)

  describe "resolve_reasoning_effort/4" do
    test "unrestricted preserves omission and explicit current or custom values without model membership" do
      key = %APIKey{}

      assert {:ok, %Decision{mode: :unrestricted, applied_effort: nil}} =
               Access.resolve_reasoning_effort(key, nil, [], nil)

      for requested <- ["high", "Focused", 0, 64, 18_446_744_073_709_551_615] do
        assert {:ok,
                %Decision{
                  mode: :unrestricted,
                  configured_effort: nil,
                  requested_effort: ^requested,
                  applied_effort: ^requested
                }} = Access.resolve_reasoning_effort(key, requested, [], nil)
      end
    end

    test "allow-up-to admits every configured bound at that bound when advertised" do
      for bound <- @known do
        key = %APIKey{maximum_reasoning_effort: bound}

        assert {:ok,
                %Decision{
                  mode: :allow_up_to,
                  configured_effort: ^bound,
                  requested_effort: ^bound,
                  applied_effort: ^bound
                }} = Access.resolve_reasoning_effort(key, bound, @known, "medium")
      end
    end

    test "allow-up-to normalizes known input and rejects unknown or above-bound input" do
      key = %APIKey{maximum_reasoning_effort: "medium"}

      assert {:ok, %Decision{requested_effort: " Medium ", applied_effort: "medium"}} =
               Access.resolve_reasoning_effort(key, " Medium ", @known, "low")

      for requested <- ["high", "focused", 0, 64, 18_446_744_073_709_551_615] do
        assert {:error, :reasoning_effort_not_allowed} =
                 Access.resolve_reasoning_effort(key, requested, @known, "low")
      end
    end

    test "allow-up-to omission chooses permitted default, then highest permitted model level" do
      key = %APIKey{maximum_reasoning_effort: "high"}

      for {default, expected} <- [{"low", "low"}, {"xhigh", "high"}, {nil, "high"}] do
        assert {:ok, %Decision{applied_effort: ^expected}} =
                 Access.resolve_reasoning_effort(key, nil, ["high", "low", "xhigh"], default)
      end
    end

    test "allow-up-to uses catalog fallback only when model levels are unavailable" do
      key = %APIKey{maximum_reasoning_effort: "medium"}

      assert {:ok, %Decision{applied_effort: "medium"}} =
               Access.resolve_reasoning_effort(key, nil, nil, nil)

      assert {:error, :reasoning_effort_not_allowed} =
               Access.resolve_reasoning_effort(key, nil, [], "medium")
    end

    test "none is permitted only when model-effective" do
      key = %APIKey{maximum_reasoning_effort: "none"}

      assert {:ok, %Decision{applied_effort: "none"}} =
               Access.resolve_reasoning_effort(key, nil, ["none", "low"], "low")

      assert {:error, :reasoning_effort_not_allowed} =
               Access.resolve_reasoning_effort(key, nil, @fallback, "low")
    end

    test "always-use applies its configured effort regardless of request or model levels" do
      key = %APIKey{enforced_reasoning_effort: "ultra"}

      for requested <- [nil, "low", "custom", 0, 64, 18_446_744_073_709_551_615] do
        assert {:ok,
                %Decision{
                  mode: :always_use,
                  configured_effort: "ultra",
                  requested_effort: ^requested,
                  applied_effort: "ultra"
                }} = Access.resolve_reasoning_effort(key, requested, [], nil)
      end
    end
  end

  describe "project_reasoning_effort_metadata/4" do
    test "unrestricted preserves model levels, descriptions, order, and default" do
      levels = [level("xhigh", "deep"), level("low", "quick"), level("custom", "custom")]

      assert %MetadataProjection{levels: ^levels, default_effort: "custom"} =
               Access.project_reasoning_effort_metadata(%APIKey{}, levels, "custom")
    end

    test "allow-up-to emits known permitted levels in stable model order and a permitted default" do
      levels = [level("high"), level("none"), level("xhigh"), level("low"), level("custom")]
      key = %APIKey{maximum_reasoning_effort: "high"}

      assert %MetadataProjection{
               levels: [%{"effort" => "high"}, %{"effort" => "none"}, %{"effort" => "low"}],
               default_effort: "high"
             } = Access.project_reasoning_effort_metadata(key, levels, "xhigh")
    end

    test "allow-up-to projects fallback metadata and all bounds consistently" do
      for bound <- @known do
        key = %APIKey{maximum_reasoning_effort: bound}
        projection = Access.project_reasoning_effort_metadata(key, nil, nil)
        expected = Enum.filter(@fallback, &(rank(&1) <= rank(bound)))

        assert Enum.map(projection.levels, & &1["effort"]) == expected
        assert projection.default_effort == List.last(expected)
      end
    end

    test "allow-up-to empty intersection remains an empty projection" do
      key = %APIKey{maximum_reasoning_effort: "minimal"}

      assert %MetadataProjection{levels: [], default_effort: nil} =
               Access.project_reasoning_effort_metadata(key, [level("medium")], "medium")
    end

    test "always-use projects a singleton only when model-effective" do
      key = %APIKey{enforced_reasoning_effort: "high"}
      levels = [level("low"), level("high", "model high")]

      assert %MetadataProjection{
               levels: [%{"effort" => "high", "description" => "model high"}],
               default_effort: "high"
             } = Access.project_reasoning_effort_metadata(key, levels, "low")

      assert %MetadataProjection{levels: [], default_effort: nil} =
               Access.project_reasoning_effort_metadata(key, [level("low")], "low")
    end

    test "always-use normalizes known advertised effort while preserving its original map" do
      key = %APIKey{enforced_reasoning_effort: "high"}
      advertised = %{"effort" => " HIGH ", "description" => "Model high"}

      assert %MetadataProjection{levels: [^advertised], default_effort: "high"} =
               Access.project_reasoning_effort_metadata(key, [advertised], "low")
    end

    test "always-use excludes malformed advertised levels without crashing" do
      key = %APIKey{enforced_reasoning_effort: "high"}

      for malformed <- [%{}, %{"effort" => nil}, %{"effort" => 42}, %{"effort" => "focused"}] do
        assert %MetadataProjection{levels: [], default_effort: nil} =
                 Access.project_reasoning_effort_metadata(key, [malformed], "high")
      end
    end
  end

  describe "project_reasoning_effort_denial_metadata/2" do
    test "derives the safe denial snapshot for every policy mode" do
      for {key, requested_effort, expected} <- [
            {%APIKey{}, "custom",
             %{
               policy_mode: "unrestricted",
               configured_effort: nil,
               requested_effort: "custom",
               applied_effort: nil
             }},
            {%APIKey{maximum_reasoning_effort: "medium"}, "high",
             %{
               policy_mode: "allow_up_to",
               configured_effort: "medium",
               requested_effort: "high",
               applied_effort: nil
             }},
            {%APIKey{enforced_reasoning_effort: "high"}, nil,
             %{
               policy_mode: "always_use",
               configured_effort: "high",
               requested_effort: nil,
               applied_effort: nil
             }}
          ] do
        assert Access.project_reasoning_effort_denial_metadata(key, requested_effort) == expected
      end
    end
  end

  defp level(effort, description \\ nil) do
    if description,
      do: %{"effort" => effort, "description" => description},
      else: %{"effort" => effort}
  end

  defp rank(effort), do: Enum.find_index(@known, &(&1 == effort))
end
