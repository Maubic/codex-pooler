defmodule CodexPooler.Gateway.RequestCompression.DirectReadCommand do
  @moduledoc false

  alias CodexPooler.Gateway.RequestCompression.ShellCommand

  @spec read?(term()) :: boolean()
  def read?(%{"type" => "local_shell_call"} = item) do
    with %{"action" => %{"type" => "exec", "command" => command}} <- item,
         {:ok, tokens} <- ShellCommand.native_argv(command) do
      direct_read?(tokens)
    else
      _not_read -> false
    end
  end

  def read?(arguments) when is_binary(arguments) do
    case CodexPooler.JSON.decode(arguments) do
      {:ok, decoded} when is_map(decoded) -> read?(decoded)
      _invalid -> false
    end
  end

  def read?(arguments) when is_map(arguments) do
    with {:ok, command} when is_binary(command) and command != "" <- ShellCommand.command_argument(arguments),
         {:ok, tokens} <- ShellCommand.lex(command) do
      scalar_tokens_read?(tokens)
    else
      _not_read -> false
    end
  end

  def read?(_arguments), do: false

  defp scalar_tokens_read?(tokens) do
    case Enum.split_while(tokens, &(&1 != :pipe)) do
      {direct, []} -> direct_read?(direct)
      {left, [:pipe | right]} -> nl_read?(left) and pipeline_sed_read?(right)
    end
  end

  defp direct_read?([{:unquoted, "cat"} | args]), do: file_command_read?(args, [])
  defp direct_read?([{:native, "cat"} | args]), do: file_command_read?(args, [])
  defp direct_read?([{:unquoted, "nl"} | args]), do: nl_args_read?(args)
  defp direct_read?([{:native, "nl"} | args]), do: nl_args_read?(args)
  defp direct_read?([{:unquoted, "head"} | args]), do: head_tail_args_read?(args)
  defp direct_read?([{:native, "head"} | args]), do: head_tail_args_read?(args)
  defp direct_read?([{:unquoted, "tail"} | args]), do: head_tail_args_read?(args)
  defp direct_read?([{:native, "tail"} | args]), do: head_tail_args_read?(args)
  defp direct_read?([{:unquoted, "sed"} | args]), do: sed_args_read?(args, true)
  defp direct_read?([{:native, "sed"} | args]), do: sed_args_read?(args, true)
  defp direct_read?(_tokens), do: false

  defp nl_read?([{:unquoted, "nl"} | args]), do: nl_args_read?(args)
  defp nl_read?(_tokens), do: false

  defp nl_args_read?([{:unquoted, "-ba"} | args]), do: file_command_read?(args, [])
  defp nl_args_read?([{:native, "-ba"} | args]), do: file_command_read?(args, [])
  defp nl_args_read?(args), do: file_command_read?(args, [])

  defp head_tail_args_read?([{:unquoted, "-n"}, {:unquoted, count} | args]) do
    decimal?(count) and file_command_read?(args, [])
  end

  defp head_tail_args_read?([{:native, "-n"}, {:native, count} | args]) do
    decimal?(count) and file_command_read?(args, [])
  end

  defp head_tail_args_read?([{quote, "--"} | _args] = args)
       when quote in [:unquoted, :native],
       do: file_command_read?(args, [])

  defp head_tail_args_read?([{quote, <<"-", count::binary>>} | args])
       when quote in [:unquoted, :native] do
    decimal?(count) and file_command_read?(args, [])
  end

  defp head_tail_args_read?(args), do: file_command_read?(args, [])

  defp sed_args_read?([{quote, "-n"}, script | args], require_files?)
       when quote in [:unquoted, :native] do
    print_script?(script) and
      if(require_files?, do: file_command_read?(args, []), else: args == [])
  end

  defp sed_args_read?(_args, _require_files?), do: false

  defp pipeline_sed_read?([{:unquoted, "sed"} | args]), do: sed_args_read?(args, false)
  defp pipeline_sed_read?(_tokens), do: false

  defp print_script?({quote, script}) do
    valid_print_script?(script) and
      (not String.contains?(script, "$") or quote in [:single, :native])
  end

  defp valid_print_script?(script) do
    case Regex.run(~r/\A(?:[0-9]+|\$)(?:,(?:[0-9]+|\$))?p\z/, script) do
      [_match] -> true
      _no_match -> false
    end
  end

  defp file_command_read?(tokens, files) do
    case tokens do
      [] ->
        files != []

      [{quote, "--"} | rest] when quote in [:unquoted, :native] and files == [] ->
        files_after_separator?(rest)

      [{_quote, file} | rest] ->
        if file_operand?(file, false) do
          file_command_read?(rest, [file | files])
        else
          false
        end
    end
  end

  defp files_after_separator?([]), do: false

  defp files_after_separator?(tokens) do
    Enum.all?(tokens, fn {_quote, file} -> file != "--" and file_operand?(file, true) end)
  end

  defp file_operand?(file, after_separator?) do
    String.trim(file) != "" and file != "-" and
      (after_separator? or not String.starts_with?(file, "-"))
  end

  defp decimal?(value) when value != "" do
    value
    |> :binary.bin_to_list()
    |> Enum.all?(&(&1 in ?0..?9))
  end

  defp decimal?(_value), do: false
end
