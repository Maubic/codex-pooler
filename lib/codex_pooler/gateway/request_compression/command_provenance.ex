defmodule CodexPooler.Gateway.RequestCompression.CommandProvenance do
  @moduledoc false

  # Classifies the command that produced a tool output, when the call carries
  # one the bounded shell reader understands. What matters is the program that
  # produces the final output: the last pipeline stage, unless that stage is a
  # filter that only passes upstream lines through (`head`, `tail`, `sort`,
  # `uniq`, `cat` without file operands, `tee`, or a search engine reading its
  # input), in which case the stage before it decides. Within a stage only
  # executed program positions count: the first word after `NAME=value`
  # assignments, the program behind a supported wrapper, the script of a shell
  # `-c` invocation, and a git subcommand read after git's own options.
  # Operands never count (`staticcheck ./grep` runs staticcheck), and a program
  # started by `xargs` prints its own output for the files it is given (`rg
  # --files | xargs cat` prints file contents, not search results).
  #
  # `:search` means the final output comes from a search engine; `:other` means
  # it comes from a readable program that is not one; `:unknown` covers calls
  # without a readable command, empty stages, and options outside the grammar
  # below. Content detection uses `:other` to refuse the lossy search strategy.

  alias CodexPooler.Gateway.RequestCompression.ShellCommand

  @search_programs MapSet.new(~w(rg ripgrep grep egrep fgrep zgrep ag ack ugrep))
  @shells MapSet.new(~w(sh bash zsh dash))
  @assignment ~r/\A[A-Za-z_][A-Za-z0-9_]*=/

  # Option grammars. `flags` take no value, `valued` options take the next word
  # or an attached value (`-n1`, `--signal=KILL`), `patterns` are whole-word
  # option forms, and `operands` counts positional words (timeout's duration)
  # before the wrapped command.
  @wrappers %{
    "command" => %{flags: ~w(-p), valued: [], patterns: [], operands: 0},
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

  # Filters that print lines of their input unchanged when no file operand is
  # given; with file operands they print those files instead.
  @filters %{
    "cat" => %{flags: [], valued: [], patterns: [~r/\A-[benstuvAET]+\z/, ~r/\A--(?:number|number-nonblank|squeeze-blank|show-all|show-ends|show-tabs|show-nonprinting)\z/], operands: 0},
    "head" => %{flags: ~w(-q -v -z --quiet --silent --verbose --zero-terminated), valued: ~w(-n -c --lines --bytes), patterns: [~r/\A-\d+\z/], operands: 0},
    "tail" => %{
      flags: ~w(-f -F -q -r -v -z --follow --retry --quiet --silent --verbose --zero-terminated),
      valued: ~w(-n -c -b -s --lines --bytes --sleep-interval --pid --max-unchanged-stats),
      patterns: [~r/\A[-+]\d+[bcl]?\z/],
      operands: 0
    },
    "sort" => %{
      flags: [],
      valued: ~w(-k -t -o -T -S --key --field-separator --output --temporary-directory --buffer-size --parallel --batch-size --compress-program --files0-from --random-source --sort),
      patterns: [
        ~r/\A-[bcCdfghimMnrRsuVz]+\z/,
        ~r/\A--(?:ignore-leading-blanks|check|dictionary-order|ignore-case|general-numeric-sort|human-numeric-sort|ignore-nonprinting|merge|month-sort|numeric-sort|reverse|random-sort|stable|unique|version-sort|zero-terminated)\z/
      ],
      operands: 0
    },
    "uniq" => %{
      flags: [],
      valued: ~w(-f -s -w --skip-fields --skip-chars --check-chars),
      patterns: [~r/\A-[cdDiuz]+\z/, ~r/\A--(?:count|repeated|ignore-case|unique|zero-terminated)\z/, ~r/\A--(?:all-repeated|group)(?:=.*)?\z/],
      operands: 0
    }
  }

  # Git's global options before the subcommand. The value-taking ones accept
  # a separate word as well as `=value`, so `git --git-dir grep show` runs show.
  @git_options %{
    flags: ~w(-p -P -v -h --paginate --no-pager --no-replace-objects --no-lazy-fetch --no-optional-locks --no-advice --bare --literal-pathspecs --glob-pathspecs --noglob-pathspecs --icase-pathspecs --html-path --man-path --info-path --exec-path --version --help),
    valued: ~w(-C -c --git-dir --work-tree --namespace --config-env --super-prefix --attr-source),
    patterns: [~r/\A--(?:exec-path|list-cmds)=/],
    operands: 0
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
      {:ok, words} -> classify_producer([words])
      :error -> :unknown
    end
  end

  defp classify_script(script) do
    case ShellCommand.lex(script) do
      {:ok, tokens} -> tokens |> pipeline_stages() |> Enum.reverse() |> classify_producer()
      :error -> :unknown
    end
  end

  # Split on pipes, keeping the empty stage a dangling pipe leaves behind.
  defp pipeline_stages(tokens) do
    Enum.chunk_while(
      tokens,
      [],
      fn
        :pipe, stage -> {:cont, Enum.reverse(stage), []}
        word, stage -> {:cont, [word | stage]}
      end,
      fn stage -> {:cont, Enum.reverse(stage), []} end
    )
  end

  # Stages last first: the last stage produces the output unless it filters
  # the output of the stage before it.
  defp classify_producer([stage | upstream]) do
    case stage_role(stage) do
      {:produces, class} -> class
      :search_filter when upstream == [] -> :search
      _filter when upstream == [] -> :unknown
      _filter -> classify_producer(upstream)
    end
  end

  defp stage_role([]), do: {:produces, :unknown}

  defp stage_role(words) do
    case executed_program(words, :direct) do
      {:ok, program, args, started_by} -> program_role(Path.basename(program), args, started_by)
      :describes -> {:produces, :other}
      :error -> {:produces, :unknown}
    end
  end

  defp executed_program([{_quote, word} | rest], started_by) do
    name = Path.basename(word)

    cond do
      Regex.match?(@assignment, word) -> executed_program(rest, started_by)
      name == "command" and describes_program?(rest) -> :describes
      Map.has_key?(@wrappers, name) -> unwrap(Map.fetch!(@wrappers, name), rest, if(name == "xargs", do: :xargs, else: started_by))
      true -> {:ok, word, rest, started_by}
    end
  end

  defp executed_program([], _started_by), do: :error

  defp unwrap(spec, words, started_by) do
    with {:ok, rest} <- skip_options(words, spec),
         {:ok, rest} <- skip_operands(rest, spec.operands) do
      executed_program(rest, started_by)
    end
  end

  # `command -v NAME` and `command -V NAME` print what NAME is; they run nothing.
  defp describes_program?(words) do
    words
    |> Enum.take_while(fn {_quote, word} -> String.starts_with?(word, "-") and word != "--" end)
    |> Enum.any?(fn {_quote, word} -> String.contains?(word, ["v", "V"]) end)
  end

  defp program_role(program, args, started_by) do
    cond do
      MapSet.member?(@search_programs, program) -> search_role(started_by)
      program == "tee" or Map.has_key?(@filters, program) -> filter_program_role(program, args, started_by)
      program == "git" -> {:produces, classify_git(args)}
      MapSet.member?(@shells, program) -> {:produces, classify_shell(args, false)}
      true -> {:produces, :other}
    end
  end

  # Started by xargs, a search engine searches the files it is given;
  # otherwise, after another stage, it filters that stage's lines.
  defp search_role(:xargs), do: {:produces, :search}
  defp search_role(:direct), do: :search_filter

  defp filter_program_role(_program, _args, :xargs), do: {:produces, :other}
  defp filter_program_role("tee", _args, :direct), do: :filter
  defp filter_program_role(program, args, :direct), do: filter_role(Map.fetch!(@filters, program), args)

  defp filter_role(spec, args) do
    case skip_options(args, spec) do
      {:ok, []} -> :filter
      {:ok, [{_quote, "-"}]} -> :filter
      {:ok, _files} -> {:produces, :other}
      :error -> {:produces, :unknown}
    end
  end

  defp classify_git(args) do
    case skip_options(args, @git_options) do
      {:ok, [{_quote, "grep"} | _args]} -> :search
      {:ok, _subcommand} -> :other
      :error -> :unknown
    end
  end

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
end
