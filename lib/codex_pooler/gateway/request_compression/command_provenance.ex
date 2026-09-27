defmodule CodexPooler.Gateway.RequestCompression.CommandProvenance do
  @moduledoc false

  # Classifies the command that produced a tool output, when the call carries
  # one the bounded shell reader understands. Only executed program positions
  # count: the first word of each pipeline stage after leading `NAME=value`
  # assignments, the program behind a supported wrapper, the script of a shell
  # `-c` invocation, and the `git grep` subcommand. Operands never count:
  # `staticcheck ./grep` runs staticcheck. `:search` means some stage runs a
  # search engine; `:other` means every stage runs a program that is not one;
  # `:unknown` covers calls without a readable command, a stage with no program
  # and wrapper options this grammar cannot place. Content detection uses
  # `:other` to refuse the lossy search strategy for grep-shaped output of
  # compilers, linters, and any other non-search command.

  alias CodexPooler.Gateway.RequestCompression.ShellCommand

  @search_programs MapSet.new(~w(rg ripgrep grep egrep fgrep zgrep ag ack ugrep))
  @shells MapSet.new(~w(sh bash zsh dash))
  @assignment ~r/\A[A-Za-z_][A-Za-z0-9_]*=/

  # Wrappers that run the rest of their words as a command. `flags` take no
  # value, `valued` options take the next word or an attached value (`-n1`,
  # `--signal=KILL`), `patterns` are whole-word option forms, and `operands`
  # counts the positional words (timeout's duration) before the command.
  @wrappers %{
    "command" => %{flags: ~w(-p -v -V), valued: [], patterns: [], operands: 0},
    "env" => %{flags: ~w(-i -0 --ignore-environment --null), valued: ~w(-u -C --unset --chdir), patterns: [], operands: 0},
    "exec" => %{flags: ~w(-c -l -cl -lc), valued: ~w(-a), patterns: [], operands: 0},
    "nice" => %{flags: [], valued: ~w(-n --adjustment), patterns: [~r/\A-\d+\z/], operands: 0},
    "nohup" => %{flags: [], valued: [], patterns: [], operands: 0},
    "stdbuf" => %{flags: [], valued: ~w(-i -o -e --input --output --error), patterns: [], operands: 0},
    "time" => %{flags: ~w(-p -v -a -q --portability --verbose --append --quiet), valued: ~w(-f -o --format --output), patterns: [], operands: 0},
    "timeout" => %{flags: ~w(-v --verbose --foreground --preserve-status), valued: ~w(-k -s --kill-after --signal), patterns: [], operands: 1},
    "xargs" => %{
      flags: ~w(-0 -r -t -p -x -o --null --no-run-if-empty --verbose --interactive --exit --open-tty),
      valued: ~w(-I -L -n -P -s -d -E -a --arg-file --delimiter --max-args --max-procs --max-chars --process-slot-var),
      patterns: [~r/\A-[ile].*\z/, ~r/\A--(?:replace|max-lines|eof)(?:=.*)?\z/],
      operands: 0
    }
  }

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
      {:ok, words} -> classify_stages([words])
      :error -> :unknown
    end
  end

  defp classify_script(script) do
    case ShellCommand.lex(script) do
      {:ok, tokens} -> tokens |> Enum.chunk_by(&(&1 == :pipe)) |> Enum.reject(&(&1 == [:pipe])) |> classify_stages()
      :error -> :unknown
    end
  end

  defp classify_stages(stages) do
    classes = Enum.map(stages, &classify_stage/1)

    cond do
      :search in classes -> :search
      :unknown in classes -> :unknown
      true -> :other
    end
  end

  defp classify_stage(words) do
    case executed_program(words) do
      {:ok, program, args} -> classify_program(Path.basename(program), args)
      :error -> :unknown
    end
  end

  defp executed_program([{_quote, word} | rest]) do
    cond do
      Regex.match?(@assignment, word) -> executed_program(rest)
      Map.has_key?(@wrappers, Path.basename(word)) -> unwrap(Map.fetch!(@wrappers, Path.basename(word)), rest)
      true -> {:ok, word, rest}
    end
  end

  defp executed_program([]), do: :error

  defp unwrap(spec, words) do
    with {:ok, rest} <- skip_options(words, spec),
         {:ok, rest} <- skip_operands(rest, spec.operands) do
      executed_program(rest)
    end
  end

  defp skip_options([{_quote, "--"} | rest], _spec), do: {:ok, rest}

  defp skip_options([{_quote, "-" <> _suffix = option} | rest], spec) do
    cond do
      option in spec.flags or Enum.any?(spec.patterns, &Regex.match?(&1, option)) -> skip_options(rest, spec)
      option in spec.valued -> skip_valued_option(rest, spec)
      attached_value?(option, spec.valued) -> skip_options(rest, spec)
      true -> :error
    end
  end

  defp skip_options(words, _spec), do: {:ok, words}

  defp skip_valued_option([_value | rest], spec), do: skip_options(rest, spec)
  defp skip_valued_option([], _spec), do: :error

  # `-n1` or `--signal=KILL`: the value is part of the option word.
  defp attached_value?("--" <> _long = option, valued) do
    case String.split(option, "=", parts: 2) do
      [name, _value] -> name in valued
      [_name] -> false
    end
  end

  defp attached_value?(<<?-, letter, _value::binary-size(1), _rest::binary>>, valued), do: <<?-, letter>> in valued
  defp attached_value?(_option, _valued), do: false

  defp skip_operands(words, 0), do: {:ok, words}
  defp skip_operands([_operand | rest], count), do: skip_operands(rest, count - 1)
  defp skip_operands([], _count), do: :error

  defp classify_program(program, args) do
    cond do
      MapSet.member?(@search_programs, program) -> :search
      program == "git" -> classify_git(args)
      MapSet.member?(@shells, program) -> classify_shell(args, false)
      true -> :other
    end
  end

  # Git's global options come before the subcommand; only `-C` and `-c` take a
  # separate value, every long option carries its value after `=`.
  defp classify_git([{_quote, option}, _value | rest]) when option in ["-C", "-c"], do: classify_git(rest)
  defp classify_git([{_quote, "-" <> _option} | rest]), do: classify_git(rest)
  defp classify_git([{_quote, "grep"} | _rest]), do: :search
  defp classify_git(_args), do: :other

  # `sh -c 'script'` (with any other shell options around `-c`) runs its
  # script, so classify the script; without `-c` the shell runs a script file.
  defp classify_shell([{_quote, <<sign, flags::binary>>}, _option_name | rest], command?)
       when sign in [?-, ?+] and flags != "" and binary_part(flags, byte_size(flags) - 1, 1) in ["o", "O"] do
    classify_shell(rest, command? or String.contains?(flags, "c"))
  end

  defp classify_shell([{_quote, "--" <> _long} | rest], command?), do: classify_shell(rest, command?)

  defp classify_shell([{_quote, <<sign, flags::binary>>} | rest], command?) when sign in [?-, ?+] and flags != "" do
    classify_shell(rest, command? or String.contains?(flags, "c"))
  end

  defp classify_shell([{_quote, script} | _positional], true), do: classify_script(script)
  defp classify_shell([], true), do: :unknown
  defp classify_shell(_args, false), do: :other
end
