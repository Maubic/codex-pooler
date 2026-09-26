defmodule CodexPooler.MCP.ToolSchemaRegressionTest do
  use CodexPooler.DataCase, async: false

  alias CodexPooler.MCP.ToolDispatch

  @object %{
    "type" => "object",
    "required" => ["count"],
    "additionalProperties" => false,
    "properties" => %{"count" => %{"type" => "integer"}}
  }

  for {label, schema, value} <- [
        {"union required field", Map.put(@object, "type", ["object", "null"]), %{}},
        {"union extra field", Map.put(@object, "type", ["object", "null"]), %{"count" => 1, "extra" => true}},
        {"union typed field", Map.put(@object, "type", ["object", "null"]), %{"count" => "one"}},
        {"array object field", %{"type" => "array", "items" => @object}, [%{}]},
        {"array scalar item", %{"type" => "array", "items" => %{"type" => "integer"}}, ["one"]},
        {"nullable array", %{"type" => ["array", "null"], "items" => @object}, [%{"count" => "one"}]},
        {"nested array", %{"type" => "array", "items" => %{"type" => "array", "items" => @object}}, [[%{}]]}
      ] do
    test "dispatch rejects invalid #{label}" do
      tool = tool(unquote(Macro.escape(schema)))
      assert {:ok, result} = ToolDispatch.call(tool, %{}, %{value: unquote(Macro.escape(value))})
      assert result["isError"] == true
      refute Map.has_key?(result, "structuredContent")
      assert hd(result["content"])["text"] =~ "invalid_tool_output:"
    end
  end

  test "nullable objects and arrays retain their valid values" do
    for {schema, values} <- [
          {Map.put(@object, "type", ["object", "null"]), [nil, %{"count" => 1}]},
          {%{"type" => ["array", "null"], "items" => @object}, [nil, [], [%{"count" => 1}]]}
        ],
        value <- values do
      assert {:ok, result} = ToolDispatch.call(tool(schema), %{}, %{value: value})
      assert result["isError"] == false
      assert result["structuredContent"] == %{"value" => value}
    end
  end

  def handler(_args, %{value: value}), do: {:ok, %{"value" => value}, "schema fixture"}

  defp tool(schema) do
    %{
      name: "sample_schema_tool",
      input_schema: %{"type" => "object", "properties" => %{}, "additionalProperties" => false},
      output_schema: %{"type" => "object", "required" => ["value"], "properties" => %{"value" => schema}, "additionalProperties" => false},
      handler: {__MODULE__, :handler}
    }
  end
end
