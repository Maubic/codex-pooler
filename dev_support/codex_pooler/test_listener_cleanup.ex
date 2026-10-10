defmodule CodexPooler.TestListenerCleanup do
  @moduledoc """
  Checks that listener cleanup is not proved by reconnecting to a reusable TCP port.

  The bounded source check covers direct assertions and connection results assigned
  in the same lexical block. It does not infer the behavior of arbitrary wrappers.
  """

  @type finding :: %{path: String.t(), line: pos_integer()}

  @spec check_file!(String.t()) :: [finding()]
  def check_file!(path) do
    path
    |> File.read!()
    |> Code.string_to_quoted!(file: path)
    |> findings(path)
  end

  @spec findings(Macro.t(), String.t()) :: [finding()]
  def findings(ast, path) do
    ast |> walk(%{}, path) |> Enum.uniq() |> Enum.sort_by(& &1.line)
  end

  @spec message(finding()) :: String.t()
  def message(%{path: path, line: line}) do
    "#{path}:#{line}: TCP connection refusal cannot prove owned listener cleanup: the numeric port can be reused. Use capture_public_endpoint_identity!/1 before stopping it and assert_public_endpoint_identity_released!/1 afterward, or the corresponding owned process/socket monitors."
  end

  defp walk({:__block__, _, expressions}, bindings, path) do
    {findings, _bindings} =
      Enum.map_reduce(expressions, bindings, fn expression, current ->
        {walk(expression, current, path), bind(expression, current)}
      end)

    List.flatten(findings)
  end

  defp walk({:assert, metadata, [expression | _]}, bindings, path) do
    if refused_assertion?(expression, bindings), do: [%{path: path, line: Keyword.get(metadata, :line, 1)}], else: []
  end

  defp walk({definition, _, arguments}, _bindings, path) when definition in [:def, :defp, :defmacro, :defmacrop, :defmodule], do: walk(arguments, %{}, path)
  defp walk({:->, _, [patterns, body]}, bindings, path), do: walk(body, Map.drop(bindings, pattern_variables(patterns)), path)
  defp walk({scope, _, arguments}, _bindings, path) when scope in [:for, :with], do: walk(arguments, %{}, path)

  # Quoted fixture source describes code; it does not execute that assertion.
  defp walk({:quote, _, _}, _bindings, _path), do: []
  defp walk({_name, _metadata, arguments}, bindings, path) when is_list(arguments), do: walk(arguments, bindings, path)
  defp walk({left, right}, bindings, path), do: walk(left, bindings, path) ++ walk(right, bindings, path)
  defp walk(expressions, bindings, path) when is_list(expressions), do: Enum.flat_map(expressions, &walk(&1, bindings, path))
  defp walk(_literal, _bindings, _path), do: []

  defp bind({:=, _, [{name, _, context}, value]}, bindings) when is_atom(name) and is_atom(context) do
    Map.put(bindings, {name, context}, tcp_connect?(value, bindings))
  end

  defp bind({:=, _, [pattern, _value]}, bindings), do: Map.drop(bindings, pattern_variables(pattern))
  defp bind(_expression, bindings), do: bindings

  defp pattern_variables({:^, _, _}), do: []
  defp pattern_variables({:when, _, arguments}), do: arguments |> Enum.drop(-1) |> pattern_variables()
  defp pattern_variables({name, _, context}) when is_atom(name) and is_atom(context), do: [{name, context}]
  defp pattern_variables({_name, _metadata, arguments}) when is_list(arguments), do: pattern_variables(arguments)
  defp pattern_variables({left, right}), do: pattern_variables(left) ++ pattern_variables(right)
  defp pattern_variables(patterns) when is_list(patterns), do: Enum.flat_map(patterns, &pattern_variables/1)
  defp pattern_variables(_literal), do: []

  defp refused_assertion?({operator, _, [left, right]}, bindings) when operator in [:=, :==, :===, :match?] do
    (left == {:error, :econnrefused} and tcp_connect?(right, bindings)) or
      (right == {:error, :econnrefused} and tcp_connect?(left, bindings))
  end

  defp refused_assertion?(_expression, _bindings), do: false

  # Unix sockets have path ownership rather than a recycled numeric TCP port.
  defp tcp_connect?({{:., _, [:gen_tcp, :connect]}, _, [{:local, _path} | _]}, _bindings), do: false
  defp tcp_connect?({{:., _, [:gen_tcp, :connect]}, _, arguments}, _bindings) when length(arguments) in [3, 4], do: true
  defp tcp_connect?({name, _, context}, bindings) when is_atom(name) and is_atom(context), do: Map.get(bindings, {name, context}, false)
  defp tcp_connect?(_expression, _bindings), do: false
end
