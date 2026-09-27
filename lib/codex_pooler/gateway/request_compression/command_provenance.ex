defmodule CodexPooler.Gateway.RequestCompression.CommandProvenance do
  @moduledoc false

  # Classifies the command that produced a tool output, when the call carries
  # one the bounded shell reader understands. `:search` means some word of the
  # command names a search engine; `:other` means a readable command that runs
  # none; `:unknown` covers calls without a readable command. Content detection
  # uses `:other` to refuse the lossy search strategy for grep-shaped output that
  # a compiler, linter, or any other non-search command printed.

  alias CodexPooler.Gateway.RequestCompression.ShellCommand

  @search_programs MapSet.new(~w(rg ripgrep grep egrep fgrep zgrep ag ack ugrep))
  @shells MapSet.new(~w(sh bash zsh dash))
  @shell_script_flag ~r/\A-[a-z]*c\z/

  @type t :: :search | :other | :unknown

  @spec classify(term()) :: t()
  def classify(%{"type" => "local_shell_call"} = item) do
    case item do
      %{"action" => %{"type" => "exec", "command" => command}} -> classify_argv(command)
      _item -> :unknown
    end
  end

  def classify(arguments) when is_binary(arguments) do
    case CodexPooler.JSON.decode(arguments) do
      {:ok, decoded} when is_map(decoded) -> classify(decoded)
      _invalid -> :unknown
    end
  end

  def classify(arguments) when is_map(arguments) do
    case ShellCommand.command_argument(arguments) do
      {:ok, command} when is_binary(command) -> classify_script(command)
      {:ok, command} when is_list(command) -> classify_argv(command)
      _no_command -> :unknown
    end
  end

  def classify(_arguments), do: :unknown

  defp classify_argv(argv) do
    case ShellCommand.native_argv(argv) do
      {:ok, tokens} -> classify_tokens(tokens)
      :error -> :unknown
    end
  end

  defp classify_script(script) do
    case ShellCommand.lex(script) do
      {:ok, tokens} -> classify_tokens(tokens)
      :error -> :unknown
    end
  end

  # `bash -lc '<script>'` runs its script, so classify the script instead.
  defp classify_tokens([{_shell_quote, shell}, {_flag_quote, flag}, {_script_quote, script}] = tokens) do
    if MapSet.member?(@shells, Path.basename(shell)) and Regex.match?(@shell_script_flag, flag) do
      classify_script(script)
    else
      classify_words(tokens)
    end
  end

  defp classify_tokens(tokens), do: classify_words(tokens)

  defp classify_words(tokens) do
    if Enum.any?(tokens, &search_program?/1), do: :search, else: :other
  end

  defp search_program?({_quote, word}), do: MapSet.member?(@search_programs, Path.basename(word))
  defp search_program?(:pipe), do: false
end
