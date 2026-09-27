defmodule CodexPooler.Gateway.RequestCompression.ShellCommand do
  @moduledoc false

  # Bounded shell-word reading shared by the command recognizers. Plain words,
  # whole single- or double-quoted words, and at most one ` | ` pipe are
  # accepted; expansion, escapes, redirection, sequencing, grouping, and
  # comments are rejected because they could change what actually runs.

  @type quote :: :unquoted | :single | :double | :native
  @type token :: {quote(), String.t()}

  @doc "The single `cmd` or `command` argument of a function call, as sent."
  @spec command_argument(map()) :: {:ok, term()} | :error
  def command_argument(arguments) when is_map(arguments) do
    case Enum.filter(["cmd", "command"], &Map.has_key?(arguments, &1)) do
      [key] -> {:ok, Map.fetch!(arguments, key)}
      _keys -> :error
    end
  end

  @doc "A native argv vector as tokens; every word must be printable."
  @spec native_argv(term()) :: {:ok, [token()]} | :error
  def native_argv(command) when is_list(command) and command != [] do
    if Enum.all?(command, &valid_native_token?/1) do
      {:ok, Enum.map(command, &{:native, &1})}
    else
      :error
    end
  end

  def native_argv(_command), do: :error

  @spec lex(String.t()) :: {:ok, [token() | :pipe]} | :error
  def lex(command) when is_binary(command) do
    lex(command, :between, [], [], false)
  end

  defp valid_native_token?(token) when is_binary(token) and token != "" do
    token
    |> :binary.bin_to_list()
    |> Enum.all?(&(&1 >= 32 and &1 != 127))
  end

  defp valid_native_token?(_token), do: false

  defp lex(<<>>, :between, [], [], _pipe?), do: :error
  defp lex(<<>>, :between, [], tokens, _pipe?), do: {:ok, Enum.reverse(tokens)}

  defp lex(<<>>, :unquoted, current, tokens, _pipe?) do
    with {:ok, next_tokens} <- finish_token(:unquoted, current, tokens) do
      {:ok, Enum.reverse(next_tokens)}
    end
  end

  defp lex(<<>>, quote, _current, _tokens, _pipe?) when quote in [:single, :double], do: :error

  defp lex(<<char, rest::binary>>, state, current, tokens, pipe?) when char in [32, 9] do
    case state do
      :between ->
        lex(rest, :between, [], tokens, pipe?)

      :unquoted ->
        with {:ok, next_tokens} <- finish_token(:unquoted, current, tokens) do
          lex(rest, :between, [], next_tokens, pipe?)
        end

      quoted when quoted in [:single, :double] ->
        lex(rest, state, [char | current], tokens, pipe?)
    end
  end

  defp lex(<<char, _rest::binary>>, _state, _current, _tokens, _pipe?)
       when char < 32 or char == 127,
       do: :error

  defp lex(<<quote, rest::binary>>, :between, [], tokens, pipe?) when quote in [?', ?"] do
    lex(rest, if(quote == ?', do: :single, else: :double), [], tokens, pipe?)
  end

  defp lex(<<quote, rest::binary>>, kind, current, tokens, pipe?)
       when (quote == ?' and kind == :single) or (quote == ?" and kind == :double) do
    case rest do
      <<>> ->
        with {:ok, next_tokens} <- finish_token(kind, current, tokens) do
          {:ok, Enum.reverse(next_tokens)}
        end

      <<next, _tail::binary>> when next in [32, 9] ->
        with {:ok, next_tokens} <- finish_token(kind, current, tokens) do
          lex(rest, :between, [], next_tokens, pipe?)
        end

      _mixed ->
        :error
    end
  end

  defp lex(<<char, _rest::binary>>, :unquoted, _current, _tokens, _pipe?)
       when char in [?', ?"],
       do: :error

  defp lex(<<char, _rest::binary>>, :between, _current, _tokens, _pipe?)
       when char in [?;, ?&, ?<, ?>, ?(, ?), ?{, ?}, ?!, ?#],
       do: :error

  defp lex(<<char, _rest::binary>>, :unquoted, _current, _tokens, _pipe?)
       when char in [?;, ?&, ?<, ?>, ?(, ?), ?{, ?}, ?!],
       do: :error

  defp lex(<<?|, rest::binary>>, :between, [], tokens, false) when tokens != [] do
    case rest do
      <<next, _tail::binary>> when next in [32, 9] ->
        lex(rest, :between, [], [:pipe | tokens], true)

      _adjacent ->
        :error
    end
  end

  defp lex(<<?|, _rest::binary>>, state, _current, _tokens, _pipe?)
       when state in [:between, :unquoted],
       do: :error

  defp lex(<<92, _rest::binary>>, quoted, _current, _tokens, _pipe?)
       when quoted in [:single, :double],
       do: :error

  defp lex(<<96, _rest::binary>>, quoted, _current, _tokens, _pipe?)
       when quoted in [:single, :double],
       do: :error

  defp lex(<<36, _rest::binary>>, :double, _current, _tokens, _pipe?),
    do: :error

  defp lex(<<char, _rest::binary>>, :between, _current, _tokens, _pipe?)
       when char == 36 or char == 92 or char == 96,
       do: :error

  defp lex(<<char, rest::binary>>, :unquoted, current, tokens, pipe?) do
    if char in [36, 92, 96] do
      :error
    else
      lex(rest, :unquoted, [char | current], tokens, pipe?)
    end
  end

  defp lex(<<char, rest::binary>>, :between, [], tokens, pipe?) do
    lex(rest, :unquoted, [char], tokens, pipe?)
  end

  defp lex(<<char, rest::binary>>, kind, current, tokens, pipe?)
       when kind in [:single, :double] do
    lex(rest, kind, [char | current], tokens, pipe?)
  end

  defp finish_token(_quote, [], _tokens), do: :error

  defp finish_token(quote, reversed, tokens) do
    {:ok, [{quote, reversed |> Enum.reverse() |> :erlang.list_to_binary()} | tokens]}
  end
end
