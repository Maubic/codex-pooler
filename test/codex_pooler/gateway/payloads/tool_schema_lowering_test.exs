defmodule CodexPooler.Gateway.Payloads.ToolSchemaLoweringTest do
  # Public /v1 lowering keeps the `encrypted: true` parameter markers of namespace functions. The provider reserves
  # some namespaced functions (the Codex client's `collaboration` multi-agent tools) and refuses a declaration that
  # differs from the schema it configured for them, marker included (`400 invalid_request_error`, param `tools`).
  # Ordinary top-level functions still lose the marker, as on the backend routes.
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.Payloads.ToolSchemaLowering

  describe "lower_non_strict_function_tools/1" do
    test "keeps a Codex collaboration namespace exactly as declared" do
      namespace = collaboration_namespace()

      assert %{"tools" => [^namespace]} = ToolSchemaLowering.lower_non_strict_function_tools(%{"tools" => [namespace]})
    end

    test "keeps a namespace function's marker at every schema position, and only as boolean true" do
      parameters = %{
        "type" => "object",
        "properties" => %{
          "message" => %{"type" => "string", "encrypted" => true, "title" => "dropped"},
          "notes" => %{"type" => "array", "items" => %{"type" => "string", "encrypted" => true}},
          "choice" => %{"anyOf" => [%{"type" => "string", "encrypted" => true}, %{"type" => "null"}]},
          "named" => %{"$ref" => "#/$defs/secret"},
          "loose" => %{"type" => "string", "encrypted" => "yes"},
          "off" => %{"type" => "string", "encrypted" => false},
          "encrypted" => %{"type" => "boolean"}
        },
        "$defs" => %{"secret" => %{"type" => "string", "encrypted" => true}},
        "additionalProperties" => %{"type" => "string", "encrypted" => true},
        "required" => ["message"]
      }

      %{"tools" => [%{"tools" => [lowered]}]} = ToolSchemaLowering.lower_non_strict_function_tools(%{"tools" => [namespace_with(function_tool("note_add", parameters))]})

      assert lowered["parameters"] == %{
               "type" => "object",
               "properties" => %{
                 "message" => %{"type" => "string", "encrypted" => true},
                 "notes" => %{"type" => "array", "items" => %{"type" => "string", "encrypted" => true}},
                 "choice" => %{"anyOf" => [%{"type" => "string", "encrypted" => true}, %{"type" => "null"}]},
                 "named" => %{"$ref" => "#/$defs/secret"},
                 "loose" => %{"type" => "string"},
                 "off" => %{"type" => "string"},
                 "encrypted" => %{"type" => "boolean"}
               },
               "$defs" => %{"secret" => %{"type" => "string", "encrypted" => true}},
               "additionalProperties" => %{"type" => "string", "encrypted" => true},
               "required" => ["message"]
             }
    end

    test "drops the marker from a top-level function and leaves a strict namespace function untouched" do
      strict = Map.put(function_tool("strict_note", message_parameters()), "strict", true)
      flat = function_tool("send_message", message_parameters())

      %{"tools" => [%{"tools" => [lowered_strict]}, lowered_flat]} = ToolSchemaLowering.lower_non_strict_function_tools(%{"tools" => [namespace_with(strict), flat]})

      assert lowered_strict == strict
      assert get_in(lowered_flat, ["parameters", "properties", "message"]) == %{"type" => "string", "description" => "Message text to queue on the target agent."}
    end
  end

  describe "lower_backend_non_strict_function_tools/1" do
    test "still drops the marker from a top-level function and forwards a namespace term untouched" do
      namespace = collaboration_namespace()
      flat = function_tool("send_message", message_parameters())

      assert %{"tools" => [^namespace, lowered_flat]} = ToolSchemaLowering.lower_backend_non_strict_function_tools(%{"tools" => [namespace, flat]})
      refute Map.has_key?(get_in(lowered_flat, ["parameters", "properties", "message"]), "encrypted")
    end
  end

  # The released Codex client's multi-agent v2 declarations, trimmed to their shape: string, boolean, array and object
  # schemas with descriptions, `required`, `additionalProperties: false`, and `encrypted: true` on each message.
  defp collaboration_namespace do
    spawn_parameters = %{
      "type" => "object",
      "properties" => %{
        "message" => %{"type" => "string", "description" => "Initial plain-text task for the new agent.", "encrypted" => true},
        "items" => %{
          "type" => "array",
          "description" => "Structured input items.",
          "items" => %{"type" => "object", "properties" => %{"type" => %{"type" => "string", "description" => "Input item type."}, "text" => %{"type" => "string", "description" => "Text content."}}, "additionalProperties" => false}
        },
        "fork_context" => %{"type" => "boolean", "description" => "Fork the parent's context."}
      },
      "required" => ["message"],
      "additionalProperties" => false
    }

    %{
      "type" => "namespace",
      "name" => "collaboration",
      "description" => "Tools for spawning and managing sub-agents.",
      "tools" => [
        function_tool("spawn_agent", spawn_parameters),
        function_tool("send_message", message_parameters()),
        function_tool("followup_task", message_parameters()),
        function_tool("list_agents", %{"type" => "object", "properties" => %{}, "additionalProperties" => false})
      ]
    }
  end

  defp message_parameters do
    %{
      "type" => "object",
      "properties" => %{
        "target" => %{"type" => "string", "description" => "Relative or canonical task name to message."},
        "message" => %{"type" => "string", "description" => "Message text to queue on the target agent.", "encrypted" => true}
      },
      "required" => ["target", "message"],
      "additionalProperties" => false
    }
  end

  defp namespace_with(tool), do: %{"type" => "namespace", "name" => "fixture_namespace", "description" => "Synthetic namespace tools", "tools" => [tool]}

  defp function_tool(name, parameters), do: %{"type" => "function", "name" => name, "description" => "Synthetic #{name}.", "strict" => false, "parameters" => parameters}
end
