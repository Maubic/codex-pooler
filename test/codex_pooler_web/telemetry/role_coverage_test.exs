defmodule CodexPoolerWeb.Telemetry.RoleCoverageTest do
  # The Prometheus reporter is switched off for OBAN_MODE=worker and scheduler,
  # and the chart's ServiceMonitor scrapes only the app pods, so a telemetry
  # event emitted from a job never becomes a series. Everything an author can
  # check still passes: the event fires, in-process handlers see it, the metric
  # is declared, a panel renders. The graph is empty forever and reads as "this
  # never happens".
  #
  # This file derives, from the compiled application, the set of declared
  # metrics with an emission an Oban job can reach, and requires it to equal
  # what CodexPoolerWeb.Telemetry.RoleCoverage declares. A metric added on a job
  # path therefore fails here until someone declares it and writes the caveat
  # into the metric and its dashboard panels. That the guard notices a new one
  # is itself covered, against a synthetic worker compiled inside the test.
  #
  # Touching OBAN_MODE is process-global, so this module is serial. Reading the
  # whole application's debug info costs a second or two of setup_all, which is
  # the property under test: the claim is about every compiled module, not a
  # sample.
  use ExUnit.Case, async: false

  alias CodexPooler.Telemetry.RelayRuntime
  alias CodexPoolerWeb.Telemetry
  alias CodexPoolerWeb.Telemetry.RoleCoverage
  alias Elixir.Telemetry.Metrics.Distribution, as: DistributionMetric

  @dashboard Path.expand(
               "../../../docs-site/public/operators/monitoring/codex-pooler-runtime-triage.json",
               __DIR__
             )

  # TelemetryMetricsPrometheus.Core exports a distribution as three series.
  @distribution_suffixes ["_bucket", "_sum", "_count"]

  # The selector a shadowed family's panels pin, as a label and a value rather
  # than as text: what the guard has to decide is whether the charted series
  # carries this label matcher, which is a question about PromQL structure and
  # not about which characters appear near each other.
  @shadow_label "via"
  @shadow_value "in_process"
  @shadow_filter ~s(#{@shadow_label}="#{@shadow_value}")

  @repo_root Path.expand("../../..", __DIR__)

  # This file, and a test in it, used as real promotion evidence below. Renaming
  # that test reds the resolver, which is the property being demonstrated.
  @guard_file "test/codex_pooler_web/telemetry/role_coverage_test.exs"
  @probe_dir "tmp/role_coverage_guard"

  defmodule CallGraph do
    @moduledoc false
    # An intra-process call graph read out of compiled BEAM debug info, plus the
    # telemetry events each function can emit. Remote calls, local calls,
    # function captures and inline closures are edges; a message to another
    # process is not, because the receiving process runs wherever it was started
    # and is usually not the job's role.

    defstruct graph: %{}, emitters: %{}, literals: %{}, workers: []

    @type t :: %__MODULE__{}

    # `elixirc_paths` puts test support and development helpers in the same ebin
    # as the application, and one of those helpers is an Oban worker. Rooting the
    # derivation at a test harness would let test code claim a production
    # emission, so the graph is built from modules compiled out of `lib/` only.
    @application_source ~r{(^|/)lib/}

    @spec scan([Path.t()]) :: t()
    def scan(ebin_dirs) do
      ebin_dirs
      |> Enum.flat_map(&Path.wildcard(Path.join(&1, "*.beam")))
      |> Enum.reduce(%__MODULE__{}, &scan_beam/2)
    end

    defp scan_beam(path, acc) do
      case :beam_lib.chunks(String.to_charlist(path), [:abstract_code]) do
        {:ok, {module, [abstract_code: {:raw_abstract_v1, forms}]}} ->
          if application_module?(forms), do: add_module(acc, module, forms), else: acc

        _no_debug_info ->
          acc
      end
    end

    defp application_module?(forms) do
      Enum.any?(forms, fn
        {:attribute, _line, :file, {source, _l}} ->
          Regex.match?(@application_source, List.to_string(source))

        _other ->
          false
      end)
    end

    defp add_module(acc, module, forms) do
      behaviours = for {:attribute, _line, :behaviour, behaviour} <- forms, do: behaviour

      {graph, emitters, literals} =
        for {:function, _line, name, arity, clauses} <- forms,
            reduce: {acc.graph, acc.emitters, MapSet.new()} do
          {graph, emitters, literals} ->
            {calls, emits?, found} = walk(clauses, module, {[], false, []})
            mfa = {module, name, arity}
            found = MapSet.new(found)

            {
              Map.put(graph, mfa, Enum.uniq(calls)),
              if(emits?, do: Map.put(emitters, mfa, found), else: emitters),
              MapSet.union(literals, found)
            }
        end

      %{
        acc
        | graph: graph,
          emitters: emitters,
          literals: Map.put(acc.literals, module, literals),
          workers: if(Oban.Worker in behaviours, do: [module | acc.workers], else: acc.workers)
      }
    end

    # Returns {calls, emits_telemetry?, atom-list literals seen}.
    defp walk({:call, _line, {:remote, _l, {:atom, _lm, m}, {:atom, _lf, f}}, args}, mod, acc) do
      {calls, emits?, literals} = acc
      emits? = emits? or (m == :telemetry and f == :execute)
      walk(args, mod, {[{m, f, length(args)} | calls], emits?, literals})
    end

    defp walk({:call, _line, {:atom, _l, f}, args}, mod, {calls, emits?, literals}) do
      walk(args, mod, {[{mod, f, length(args)} | calls], emits?, literals})
    end

    defp walk({:fun, _l, {:function, {:atom, _, m}, {:atom, _, f}, {:integer, _, a}}}, _mod, acc) do
      {calls, emits?, literals} = acc
      {[{m, f, a} | calls], emits?, literals}
    end

    defp walk({:fun, _l, {:function, f, a}}, mod, {calls, emits?, literals})
         when is_atom(f) and is_integer(a) do
      {[{mod, f, a} | calls], emits?, literals}
    end

    defp walk({:cons, _l, {:atom, _la, _head}, _tail} = node, mod, acc) do
      {calls, emits?, literals} = walk(Tuple.to_list(node), mod, acc)

      case atom_list(node) do
        nil -> {calls, emits?, literals}
        list -> {calls, emits?, [list | literals]}
      end
    end

    defp walk(list, mod, acc) when is_list(list),
      do: Enum.reduce(list, acc, &walk(&1, mod, &2))

    defp walk(tuple, mod, acc) when is_tuple(tuple),
      do: walk(Tuple.to_list(tuple), mod, acc)

    defp walk(_other, _mod, acc), do: acc

    defp atom_list({nil, _line}), do: []

    defp atom_list({:cons, _line, {:atom, _l, head}, tail}) do
      case atom_list(tail) do
        nil -> nil
        rest -> [head | rest]
      end
    end

    defp atom_list(_other), do: nil

    @doc """
    Emission sites per event: functions calling `:telemetry.execute/3` whose own
    literals name the event. A function naming no candidate event takes the name
    from its module's literals instead, which is how an emitter receiving the
    event as an argument — a module attribute, or a prefix plus a suffix — is
    still attributed. Falling back at module level unconditionally would give
    every emitter in a module all of that module's events, which is how the
    first run of this guard confused stream finalization with stream outcome.
    """
    @spec emission_sites(t(), [[atom()]]) :: %{[atom()] => [mfa()]}
    def emission_sites(%__MODULE__{} = graph, events) do
      for {{module, _f, _a} = mfa, own} <- graph.emitters,
          reduce: Map.new(events, &{&1, []}) do
        acc ->
          named =
            case named_events(own, events) do
              [] -> named_events(Map.get(graph.literals, module, MapSet.new()), events)
              named -> named
            end

          Enum.reduce(named, acc, fn event, inner -> Map.update!(inner, event, &[mfa | &1]) end)
      end
    end

    defp named_events(literals, events),
      do: for(event <- events, Enum.any?(literals, &names_event?(&1, event)), do: event)

    defp names_event?(literal, event),
      do: literal == event or (literal != [] and List.starts_with?(event, literal))

    @doc "One entrypoint per Oban worker module that defines `perform/1`."
    @spec worker_entrypoints(t()) :: [mfa()]
    def worker_entrypoints(%__MODULE__{} = graph) do
      graph.workers
      |> Enum.map(&{&1, :perform, 1})
      |> Enum.filter(&Map.has_key?(graph.graph, &1))
      |> Enum.sort()
    end

    @doc "Every function reachable from `roots` without crossing a process boundary."
    @spec reachable(t(), [mfa()]) :: MapSet.t(mfa())
    def reachable(%__MODULE__{} = graph, roots),
      do: graph |> parents(roots) |> Map.keys() |> MapSet.new()

    @doc "Shortest call path from one of `roots` to `target`, or nil when unreachable."
    @spec witness(t(), [mfa()], mfa()) :: [mfa()] | nil
    def witness(%__MODULE__{} = graph, roots, target) do
      parents = parents(graph, roots)

      if Map.has_key?(parents, target) do
        target
        |> Stream.iterate(&Map.get(parents, &1))
        |> Enum.take_while(&(&1 != nil))
        |> Enum.reverse()
      end
    end

    defp parents(%__MODULE__{graph: graph}, roots) do
      {parents, _frontier} =
        {%{}, Enum.map(roots, &{&1, nil})}
        |> Stream.iterate(fn {parents, frontier} ->
          fresh = Enum.reject(frontier, fn {mfa, _parent} -> Map.has_key?(parents, mfa) end)
          parents = Enum.reduce(fresh, parents, fn {mfa, p}, acc -> Map.put_new(acc, mfa, p) end)

          next =
            Enum.flat_map(fresh, fn {mfa, _parent} ->
              graph |> Map.get(mfa, []) |> Enum.map(&{&1, mfa})
            end)

          {parents, next}
        end)
        |> Enum.find(fn {_parents, frontier} -> frontier == [] end)

      parents
    end
  end

  defmodule PromQL do
    @moduledoc false
    # Label matchers, and series selection, read out of a PromQL expression.
    #
    # Six rounds of review defeated a scan over the expression's *text*, each time
    # with a literal that is not what the scan took it for: a pin supplied by a
    # second series, a label whose name merely ends in `via`, a `#` comment left
    # where a pin used to be, a single-quoted or backtick label value quoting the
    # pin, and — one step out from all of those — a panel that selects the family
    # through `{__name__=~"…"}` and so never writes its name at all, which made
    # the guard skip the panel entirely rather than judge it. PromQL has three
    # string forms, permits comments, and lets a series be named indirectly, so no
    # amount of patching a scanner answers the question; a matcher that has to be
    # patched once per review round is the defect.
    #
    # This tokenizes, and answers both halves from the same tokens: which declared
    # series an expression selects (`charted_series/2`) and what each occurrence's
    # label matchers are (`occurrences/2`). A literal that is not a matcher cannot
    # become one, and a selection that does not spell the name is still found.
    #
    # It is not a PromQL parser: it knows strings, comments, braces, matcher
    # operators and identifiers, and treats everything else as opaque. An
    # expression Prometheus would reject — an unterminated string, an unbalanced
    # brace, an escape that is not an escape — is `:invalid`, and every caller
    # resolves that against itself: an invalid expression charts every candidate,
    # is not pinned, and does pin somewhere, so a broken panel reds both
    # directions instead of slipping through one. An earlier version instead let
    # an unbalanced brace run to the next `}`, which let a family's selector
    # inherit a pin from an unrelated series — the first defeat, reachable again.

    @type matcher :: {String.t(), String.t(), String.t()}

    @doc """
    The label matchers on each occurrence of `series` in `expr`, or `:invalid`.

    An occurrence is the series named bare (`foo`, `foo{…}`), or a selector that
    reaches it through `__name__` — by equality, by an anchored regex, or by not
    bounding the name at all, which is what `{job="x"}` does. A bare occurrence
    with no braces has no matchers, which is what makes an unpinned one visible.
    """
    @spec occurrences(String.t(), String.t()) :: [[matcher()]] | :invalid
    def occurrences(expr, series) when is_binary(expr) and is_binary(series) do
      case tokens(expr) do
        :invalid -> :invalid
        tokens -> scan(tokens, series, [])
      end
    end

    @doc """
    Which of `candidates` `expr` selects.

    This is the step the sixth defeat attacked. Deciding it by scanning the text
    for `codex_pooler_[a-z0-9_]+` means a panel that selects a declared family
    without spelling its name is associated with no family at all, so every rule
    that would have judged it silently does not run. An invalid expression charts
    everything, so it is judged rather than skipped.
    """
    @spec charted_series(String.t(), [String.t()]) :: [String.t()]
    def charted_series(expr, candidates) when is_binary(expr) and is_list(candidates) do
      case tokens(expr) do
        :invalid ->
          candidates

        tokens ->
          Enum.filter(candidates, &selected?(scan(tokens, &1, [])))
      end
    end

    defp selected?(:invalid), do: true
    defp selected?(occurrences), do: occurrences != []

    @doc """
    Whether `matchers` selects exactly `label` = `value`, with nothing contradicting it.

    `=~` on the literal is the same selection: PromQL anchors a matcher regex, so
    `via=~"in_process"` picks the same series `via="in_process"` does. An equivalent
    spelling such as `via=~"^in_process$"` reads as not selecting it — the safe
    direction, since the consequence is a guard failure rather than a family
    silently escaping one. Every matcher on the label must accept the value, so a
    regex negation that matches it contradicts the pin just like exact inequality.
    """
    @spec selects?([matcher()], String.t(), String.t()) :: boolean()
    def selects?(matchers, label, value) do
      on_label = for {name, operator, matched} <- matchers, name == label, do: {operator, matched}
      positive = Enum.filter(on_label, fn {operator, _matched} -> operator in ~w(= =~) end)

      positive != [] and
        Enum.all?(positive, &exact_positive?(&1, value)) and
        Enum.all?(on_label, &accepts?(&1, value))
    end

    defp scan([], _series, acc), do: Enum.reverse(acc)

    # A brace group written after a name belongs to that name, whatever is inside
    # it: `up{__name__="x"}` is not an occurrence of `x`.
    defp scan([{:ident, name}, :lbrace | rest], series, acc) do
      case matchers(rest, []) do
        :invalid -> :invalid
        {found, rest} -> scan(rest, series, if(name == series, do: [found | acc], else: acc))
      end
    end

    defp scan([{:ident, series} | rest], series, acc), do: scan(rest, series, [[] | acc])

    defp scan([:lbrace | rest], series, acc) do
      case matchers(rest, []) do
        :invalid ->
          :invalid

        {found, rest} ->
          scan(rest, series, if(names?(found, series), do: [found | acc], else: acc))
      end
    end

    # Every `}` in a valid expression closes a `{` this walk already consumed.
    defp scan([:rbrace | _rest], _series, _acc), do: :invalid

    defp scan([_token | rest], series, acc), do: scan(rest, series, acc)

    defp matchers([], _acc), do: :invalid
    defp matchers([:lbrace | _rest], _acc), do: :invalid
    defp matchers([:rbrace | rest], acc), do: {Enum.reverse(acc), rest}

    defp matchers([{:ident, name}, {:op, operator}, {:string, value} | rest], acc) do
      if operator in ~w(=~ !~) and not re2_compatible?(value) do
        :invalid
      else
        matchers(rest, [{name, operator, value} | acc])
      end
    end

    defp matchers([_token | rest], acc), do: matchers(rest, acc)

    # Whether a selector reaches `series` through `__name__`. With no `__name__`
    # matcher the selection is not bounded by name at all — `{job="x"}` selects
    # every metric that job exports, this family included. Otherwise every matcher
    # must accept the candidate, because PromQL combines them by conjunction.
    defp names?(matchers, series) do
      on_name = for {"__name__", operator, value} <- matchers, do: {operator, value}

      on_name == [] or Enum.all?(on_name, &accepts?(&1, series))
    end

    defp exact_positive?({"=", matched}, value), do: matched == value

    defp exact_positive?({"=~", pattern}, value),
      do: pattern == value and anchored?(pattern, value)

    defp accepts?({"=", matched}, value), do: matched == value
    defp accepts?({"!=", matched}, value), do: matched != value
    defp accepts?({"=~", pattern}, value), do: anchored?(pattern, value)
    defp accepts?({"!~", pattern}, value), do: not anchored?(pattern, value)

    # Prometheus anchors a label-matcher regex at both ends. A pattern that will
    # not compile is invalid PromQL, and assuming it reaches the family keeps a
    # broken panel inside the rule rather than outside it.
    defp anchored?(pattern, series) do
      case Regex.compile("\\A(?:" <> pattern <> ")\\z") do
        {:ok, regex} -> Regex.match?(regex, series)
        {:error, _reason} -> true
      end
    end

    # Prometheus uses RE2, whose accepted syntax and match semantics differ from
    # both local engines. This deliberately accepts a conservative shared subset:
    # advanced group prefixes, backreferences, possessive quantifiers, engine-specific
    # escape families and POSIX character classes fail closed before local matching.
    defp re2_compatible?(pattern) do
      not re2_unsupported?(pattern) and
        not oversized_repetition?(pattern) and
        match?({:ok, _regex}, Regex.compile("\\A(?:" <> pattern <> ")\\z"))
    end

    defp oversized_repetition?(pattern) do
      ~r/\{(\d+)(?:,(\d*))?\}/
      |> Regex.scan(pattern, capture: :all_but_first)
      |> Enum.any?(fn captures ->
        [lower | rest] = captures
        upper = List.first(rest)

        String.to_integer(lower) > 1_000 or
          (upper not in [nil, ""] and String.to_integer(upper) > 1_000)
      end)
    end

    defp re2_unsupported?(pattern), do: re2_unsupported_scan?(pattern, :outside)

    defp re2_unsupported_scan?(<<>>, _class_state), do: false
    defp re2_unsupported_scan?(<<"\\">>, _class_state), do: true

    defp re2_unsupported_scan?(<<"\\", digit, _rest::binary>>, _class_state)
         when digit in ?1..?9,
         do: true

    defp re2_unsupported_scan?(<<"\\", escape, rest::binary>>, class_state)
         when escape in ?A..?Z or escape in ?a..?z do
      if escape in ~c"AbBdDsSwWafnrtvx",
        do: re2_unsupported_scan?(rest, after_class_member(class_state)),
        else: true
    end

    defp re2_unsupported_scan?(<<"\\", _escaped::utf8, rest::binary>>, class_state),
      do: re2_unsupported_scan?(rest, after_class_member(class_state))

    defp re2_unsupported_scan?(<<"[", rest::binary>>, :outside),
      do: re2_unsupported_scan?(rest, :class_open)

    defp re2_unsupported_scan?(<<"^", rest::binary>>, :class_open),
      do: re2_unsupported_scan?(rest, :class_first)

    defp re2_unsupported_scan?(pattern, :class_open),
      do: re2_unsupported_scan?(pattern, :class_first)

    defp re2_unsupported_scan?(<<"[:", _rest::binary>>, state)
         when state in [:class_first, :class_body],
         do: true

    defp re2_unsupported_scan?(<<"]", rest::binary>>, :class_first),
      do: re2_unsupported_scan?(rest, :class_body)

    defp re2_unsupported_scan?(<<"]", rest::binary>>, :class_body),
      do: re2_unsupported_scan?(rest, :outside)

    defp re2_unsupported_scan?(<<"(?", _rest::binary>>, :outside), do: true
    defp re2_unsupported_scan?(<<"(*", _rest::binary>>, :outside), do: true

    defp re2_unsupported_scan?(<<quantifier, "+", _rest::binary>>, :outside)
         when quantifier in ~c"*+?}",
         do: true

    defp re2_unsupported_scan?(<<_char::utf8, rest::binary>>, class_state),
      do: re2_unsupported_scan?(rest, after_class_member(class_state))

    defp after_class_member(class_state) when class_state in [:class_open, :class_first],
      do: :class_body

    defp after_class_member(class_state), do: class_state

    defp tokens(expr), do: tokens(expr, [])

    defp tokens(<<>>, acc), do: Enum.reverse(acc)
    defp tokens(<<"#", rest::binary>>, acc), do: tokens(comment(rest), acc)
    defp tokens(<<c, rest::binary>>, acc) when c in ~c" \t\n\r", do: tokens(rest, acc)

    defp tokens(<<q, rest::binary>>, acc) when q in ~c"\"'" do
      case quoted(rest, q, []) do
        :invalid -> :invalid
        {value, rest} -> tokens(rest, [{:string, value} | acc])
      end
    end

    defp tokens(<<"`", rest::binary>>, acc) do
      # A backquoted string is raw: Prometheus does not read escapes inside one.
      case backquoted(rest, []) do
        :invalid -> :invalid
        {value, rest} -> tokens(rest, [{:string, value} | acc])
      end
    end

    defp tokens(<<"{", rest::binary>>, acc), do: tokens(rest, [:lbrace | acc])
    defp tokens(<<"}", rest::binary>>, acc), do: tokens(rest, [:rbrace | acc])
    defp tokens(<<"=~", rest::binary>>, acc), do: tokens(rest, [{:op, "=~"} | acc])
    defp tokens(<<"!~", rest::binary>>, acc), do: tokens(rest, [{:op, "!~"} | acc])
    defp tokens(<<"!=", rest::binary>>, acc), do: tokens(rest, [{:op, "!="} | acc])
    defp tokens(<<"==", rest::binary>>, acc), do: tokens(rest, [:other | acc])
    defp tokens(<<"=", rest::binary>>, acc), do: tokens(rest, [{:op, "="} | acc])

    defp tokens(<<c, _::binary>> = expr, acc) when c in ?a..?z or c in ?A..?Z or c in ~c"_:" do
      {name, rest} = ident(expr, [])
      tokens(rest, [{:ident, name} | acc])
    end

    defp tokens(<<_c, rest::binary>>, acc), do: tokens(rest, [:other | acc])

    defp ident(<<c, rest::binary>>, acc)
         when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c in ~c"_:",
         do: ident(rest, [c | acc])

    defp ident(rest, acc), do: {acc |> Enum.reverse() |> List.to_string(), rest}

    # Codepoints, not bytes: accumulating bytes and finishing with
    # `List.to_string/1` re-encodes a UTF-8 label value.
    defp quoted(<<>>, _q, _acc), do: :invalid

    defp quoted(<<"\\", rest::binary>>, q, acc) do
      case escape(rest) do
        :invalid -> :invalid
        {decoded, rest} -> quoted(rest, q, [decoded | acc])
      end
    end

    defp quoted(<<q, rest::binary>>, q, acc), do: {collect(acc), rest}
    defp quoted(<<c::utf8, rest::binary>>, q, acc), do: quoted(rest, q, [<<c::utf8>> | acc])
    defp quoted(<<c, rest::binary>>, q, acc), do: quoted(rest, q, [<<c>> | acc])

    defp backquoted(<<>>, _acc), do: :invalid
    defp backquoted(<<"`", rest::binary>>, acc), do: {collect(acc), rest}
    defp backquoted(<<c::utf8, rest::binary>>, acc), do: backquoted(rest, [<<c::utf8>> | acc])
    defp backquoted(<<c, rest::binary>>, acc), do: backquoted(rest, [<<c>> | acc])

    # Prometheus decodes these; stripping the backslash and keeping the next
    # character instead answers a question about a string Prometheus never reads
    # (`via="i\n_process"` selects `i<LF>_process` and charts nothing, while
    # `via="in\x5fprocess"` selects `in_process` and is a pin). An escape
    # Prometheus does not accept makes the whole expression invalid.
    defp escape(<<"\\", rest::binary>>), do: {"\\", rest}
    defp escape(<<"\"", rest::binary>>), do: {"\"", rest}
    defp escape(<<"'", rest::binary>>), do: {"'", rest}
    defp escape(<<"`", rest::binary>>), do: {"`", rest}
    defp escape(<<"a", rest::binary>>), do: {<<7>>, rest}
    defp escape(<<"b", rest::binary>>), do: {<<8>>, rest}
    defp escape(<<"f", rest::binary>>), do: {<<12>>, rest}
    defp escape(<<"n", rest::binary>>), do: {<<10>>, rest}
    defp escape(<<"r", rest::binary>>), do: {<<13>>, rest}
    defp escape(<<"t", rest::binary>>), do: {<<9>>, rest}
    defp escape(<<"v", rest::binary>>), do: {<<11>>, rest}
    defp escape(<<"x", rest::binary>>), do: codepoint(rest, 2, 16)
    defp escape(<<"u", rest::binary>>), do: codepoint(rest, 4, 16)
    defp escape(<<"U", rest::binary>>), do: codepoint(rest, 8, 16)
    defp escape(<<c, _::binary>> = rest) when c in ?0..?7, do: codepoint(rest, 3, 8)
    defp escape(_rest), do: :invalid

    defp codepoint(binary, length, base) do
      with <<digits::binary-size(^length), rest::binary>> <- binary,
           {value, ""} <- Integer.parse(digits, base),
           true <- value in 0..0x10FFFF and value not in 0xD800..0xDFFF do
        {<<value::utf8>>, rest}
      else
        _unparsable -> :invalid
      end
    end

    defp collect(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

    defp comment(<<>>), do: <<>>
    defp comment(<<"\n", rest::binary>>), do: rest
    defp comment(<<_c, rest::binary>>), do: comment(rest)
  end

  setup_all do
    graph = CallGraph.scan([Mix.Project.compile_path()])
    roots = CallGraph.worker_entrypoints(graph)

    # A derivation that found nothing would satisfy "no new member" without
    # proving anything, so bind its scale.
    assert map_size(graph.graph) > 5_000,
           "the call graph is empty or tiny; compiled debug info is probably missing"

    assert length(roots) >= 10, "no Oban workers were found to root the derivation from"

    {:ok, graph: graph, roots: roots, reachable: CallGraph.reachable(graph, roots), sites: CallGraph.emission_sites(graph, declared_metric_events())}
  end

  describe "the premise" do
    test "the reporter is disabled for exactly the OBAN_MODE values RoleCoverage declares" do
      original = System.fetch_env("OBAN_MODE")

      on_exit(fn ->
        case original do
          {:ok, value} -> System.put_env("OBAN_MODE", value)
          :error -> System.delete_env("OBAN_MODE")
        end
      end)

      for mode <- RoleCoverage.unscraped_oban_modes() do
        System.put_env("OBAN_MODE", mode)

        refute Telemetry.prometheus_reporter_enabled?(),
               "RoleCoverage calls OBAN_MODE=#{mode} unscraped, but the reporter starts there"
      end

      for mode <- ~w(web all) do
        System.put_env("OBAN_MODE", mode)

        assert Telemetry.prometheus_reporter_enabled?(),
               "OBAN_MODE=#{mode} no longer runs the reporter; RoleCoverage needs to say so"
      end

      System.delete_env("OBAN_MODE")
      assert Telemetry.prometheus_reporter_enabled?()
    end
  end

  describe "derived role coverage" do
    test "supervised heartbeat emission has manual partial coverage", context do
      event = [:codex_pooler, :instance_presence, :heartbeat]

      assert %{entrypoints: [], coverage: :partial, fallback: "instance_presences.last_seen_at"} =
               Map.fetch!(RoleCoverage.unscraped_emissions(), event)

      assert {CodexPooler.Platform.InstanceHeartbeat, :log_failure, 0} in Map.fetch!(
               context.sites,
               event
             )

      assert derived_entrypoints(context, event) == []
    end

    test "every metric with a job-reachable emission is declared", context do
      undeclared =
        context
        |> derive_unscraped_events()
        |> Enum.reject(fn {event, _sites} -> RoleCoverage.declared?(event) end)

      assert undeclared == [],
             """
             These telemetry events have a declared metric and an emission an Oban job can reach,
             so their series lose everything the job emits on any deployment that is not
             OBAN_MODE=all. Declare each one in CodexPoolerWeb.Telemetry.RoleCoverage, name the
             durable rows an operator should read instead, and put the caveat in the metric
             description and in every dashboard panel that charts it.

             """ <>
               Enum.map_join(undeclared, "\n\n", &describe_derivation(context, &1))
    end

    test "no declaration outlives the emission it was written for", context do
      derived = derived_events(context)

      stale =
        Enum.reject(RoleCoverage.unscraped_emissions(), fn {event, declaration} ->
          if declaration.entrypoints == [],
            do: Map.get(context.sites, event, []) != [] and event not in derived,
            else: event in derived
        end)

      assert stale == [],
             "no Oban job reaches an emission of #{inspect(stale)} any more; drop the " <>
               "RoleCoverage declaration and the caveats it forced onto the metric and panels"
    end

    test "each declaration names the jobs that actually reach it", context do
      mismatched =
        for {event, declaration} <- RoleCoverage.unscraped_emissions(),
            declared = Enum.sort(declaration.entrypoints),
            derived = derived_entrypoints(context, event),
            declared != derived,
            do: {event, declared, derived}

      assert mismatched == [],
             "RoleCoverage entrypoints disagree with the call graph: " <>
               Enum.map_join(mismatched, "; ", fn {event, declared, derived} ->
                 "#{inspect(event)} declares #{inspect(declared)} but #{inspect(derived)} reach it"
               end)
    end
  end

  describe "the caveat an operator has to see" do
    test "saved-reset convergence points at lifecycle metadata rather than a nonexistent audit trail" do
      declaration =
        RoleCoverage.unscraped_emissions()
        |> Map.fetch!([:codex_pooler, :saved_reset, :convergence])

      assert declaration.fallback ==
               "the upstream identity saved_reset_redemption lifecycle metadata"

      descriptions =
        Telemetry.prometheus_metrics()
        |> Enum.filter(&(&1.event_name == [:codex_pooler, :saved_reset, :convergence]))
        |> Enum.map(& &1.description)

      assert length(descriptions) == 4

      assert Enum.all?(
               descriptions,
               &String.contains?(
                 &1,
                 "upstream identity saved_reset_redemption lifecycle metadata"
               )
             )

      refute Enum.any?(descriptions, &String.contains?(&1, "audit trail"))
    end

    test "every declared event's metrics say so in their description" do
      silent =
        for metric <- Telemetry.prometheus_metrics(),
            coverage = RoleCoverage.coverage_for(metric.event_name),
            coverage != nil,
            not RoleCoverage.markers_correct?(metric.description, coverage),
            do: {Enum.join(metric.name, "."), RoleCoverage.required_marker(coverage)}

      assert silent == [],
             "these metrics are declared in RoleCoverage and their description does not carry " <>
               "the marker their coverage state owes, or still carries the one it retires: " <>
               Enum.map_join(silent, ", ", fn {name, marker} -> "#{name} (#{marker})" end)
    end

    test "promoting a family forces its metric descriptions to be rewritten" do
      # A :partial family's description has to name the relay, to say where the
      # job share arrives once something drains it, so requiring the relay
      # marker on promotion demands nothing new of it. The false
      # "OBAN_MODE=worker or scheduler ... run no reporter" sentence would
      # survive on a graph that had started carrying that share. What catches
      # it is the marker a promoted description may NOT carry.
      assert RoleCoverage.caveat_marker() in RoleCoverage.forbidden_markers(:relayed)
      assert RoleCoverage.forbidden_markers(:partial) == []

      # One literal token is not the claim. A caveat reworded to drop OBAN_MODE
      # says the same false thing to an operator, so the roles it has to name are
      # forbidden too — and the promoted description has nothing true left to say
      # about them, because the relay now carries exactly the share they withhold.
      reworded =
        "The promoted total combines in_process and job_relay. When the pooler runs as " <>
          "worker or scheduler, no reporter runs, so this graph is empty."

      refute String.contains?(reworded, RoleCoverage.caveat_marker())
      assert RoleCoverage.marker_present?(reworded, :relayed)
      refute RoleCoverage.markers_correct?(reworded, :relayed)

      # The same sentence in the plural is the more natural English of the two,
      # and a snake_case spelling is what a code comment reaches for. Both used
      # to pass: a bare word boundary rejects a following `s`, and treating `_`
      # as a word character rejects a preceding one.
      for escape <- [
            "in_process plus job_relay form the total. When the pooler runs as workers or " <>
              "schedulers, no reporter runs, so this graph is empty.",
            "in_process plus job_relay carry it; the oban_worker and oban_scheduler roles " <>
              "run no reporter",
            "in_process plus job_relay; SCHEDULERS export nothing"
          ] do
        assert RoleCoverage.marker_present?(escape, :relayed)

        refute RoleCoverage.markers_correct?(escape, :relayed),
               "a promoted description may not name the unscraped roles: #{escape}"
      end

      # Case and word boundaries: the token itself is caught however it is cased,
      # and a longer word that merely contains a role name is not a caveat.
      refute RoleCoverage.markers_correct?(
               "in_process plus job_relay, exported only under oban_mode=all",
               :relayed
             )

      assert RoleCoverage.markers_correct?(
               "in_process plus job_relay from coworkers and reschedulers",
               :relayed
             )

      # The residual no word list closes: a caveat that never names a role. It is
      # left to review, and pinned here so the limit is not mistaken for a gap.
      assert RoleCoverage.markers_correct?(
               "in_process plus job_relay; on a split-role deployment the job pods run no " <>
                 "reporter, so this graph is empty",
               :relayed
             )

      declared =
        for metric <- Telemetry.prometheus_metrics(),
            RoleCoverage.coverage_for(metric.event_name) != nil,
            do: {Enum.join(metric.name, "."), metric.description}

      assert declared != [], "no declared family has a metric; this check would pass vacuously"

      presence_only =
        for {name, description} <- declared,
            RoleCoverage.marker_present?(description, :relayed),
            do: name

      assert presence_only != [],
             "no shipped description names the relay, so requiring the relay marker would " <>
               "already be discriminating and this test is measuring the wrong thing"

      survives =
        for {name, description} <- declared,
            RoleCoverage.markers_correct?(description, :relayed),
            do: name

      assert survives == [],
             "these declared metrics would pass the relayed check unchanged, so promoting " <>
               "their family would leave a false OBAN_MODE caveat on a graph that now carries " <>
               "the job share: " <> Enum.join(survives, ", ")
    end

    test "the marker a declaration owes follows its coverage state, not a fixed constant" do
      # A promoted family's graph carries the job share, so the OBAN_MODE
      # sentence would be false on it; an unpromoted one's does not, so the
      # relay marker alone would be. Each coverage state is checked against a
      # description carrying only the other state's marker.
      partial_only = "exported only under OBAN_MODE=all"
      relayed_only = "in_process plus job_relay form the promoted total"
      relay_name_only = "the job share arrives as via=\"job_relay\""

      assert RoleCoverage.required_marker(:partial) == RoleCoverage.caveat_marker()
      assert RoleCoverage.required_marker(:unscraped_only) == RoleCoverage.caveat_marker()
      assert RoleCoverage.required_marker(:relayed) == RoleCoverage.relay_marker()
      refute RoleCoverage.relay_marker() == RoleCoverage.caveat_marker()

      assert RoleCoverage.marker_present?(partial_only, :partial)
      refute RoleCoverage.marker_present?(partial_only, :relayed)
      assert RoleCoverage.marker_present?(relayed_only, :relayed)
      refute RoleCoverage.marker_present?(relay_name_only, :relayed)
      refute RoleCoverage.marker_present?(relayed_only, :partial)
      refute RoleCoverage.marker_present?(nil, :relayed)

      refute RoleCoverage.marker_present?("exported only under OBAN_MODEX=all", :partial)
      refute RoleCoverage.marker_present?("exported only under OBAN_MODE_worker", :partial)

      refute RoleCoverage.marker_present?(
               "in_process_total plus job_relay_total form the promoted total",
               :relayed
             )

      refute RoleCoverage.marker_present?(
               "in_process plus job_relay_total form the promoted total",
               :relayed
             )

      # A description carrying both is what every shipped family has, and it is
      # exactly the case presence alone cannot decide.
      both = partial_only <> " " <> relayed_only

      assert RoleCoverage.marker_present?(both, :partial)
      assert RoleCoverage.marker_present?(both, :relayed)
      assert RoleCoverage.markers_correct?(both, :partial)
      refute RoleCoverage.markers_correct?(both, :relayed)
      assert RoleCoverage.markers_correct?(relayed_only, :relayed)
      refute RoleCoverage.markers_correct?(relay_name_only, :relayed)
      refute RoleCoverage.markers_correct?("in_process_total job_relay_total", :relayed)
      refute RoleCoverage.markers_correct?(nil, :relayed)
    end

    test "every coverage state is classified, marked and checked, and an unknown one raises" do
      # `:unscraped_only` was declared in the type, in the marker map and in a
      # passing marker test, so it read as a supported state — and matched no
      # guard clause anywhere. A family moved to it could have its panels
      # unpinned, which is the substantive act of promotion, while owing no
      # evidence and keeping a now-false OBAN_MODE caveat. Nothing here may
      # depend on the guards happening to know today's three atoms.
      states = RoleCoverage.coverage_states()

      assert Enum.sort(states) == Enum.sort([:partial, :unscraped_only, :relayed])

      # `coverage_states/0` derives from the classification map, while the marker
      # functions guard on a second one. Pinning them equal keeps "one place a
      # guard asks" true in both directions rather than only downwards.
      assert Enum.sort(RoleCoverage.marked_coverage_states()) == Enum.sort(states)

      for state <- states do
        assert RoleCoverage.coverage_class(state) in [:shadowed, :promoted],
               "#{state} is not classified, so no panel rule applies to it"

        assert RoleCoverage.shadowed?(state) != RoleCoverage.promoted?(state),
               "#{state} is both shadowed and promoted, or neither"

        assert is_binary(RoleCoverage.required_marker(state)),
               "#{state} owes no marker, so its descriptions are unchecked"

        expected = if RoleCoverage.promoted?(state), do: RoleCoverage.promotion_gates(), else: []

        assert RoleCoverage.unmet_promotion_gates(%{coverage: state}) == expected,
               "#{state} does not demand the promotion evidence its class owes"
      end

      # Every shipped declaration is one of them, so no family sits outside the
      # classification the guards below run on.
      undeclared_states =
        for {event, %{coverage: coverage}} <- RoleCoverage.unscraped_emissions(),
            coverage not in states,
            do: {event, coverage}

      assert undeclared_states == [],
             "these families declare a coverage state nothing classifies: " <>
               inspect(undeclared_states)

      # A state invented tomorrow is refused rather than skipped.
      assert_raise FunctionClauseError, fn -> RoleCoverage.coverage_class(:invented_state) end
      assert_raise FunctionClauseError, fn -> RoleCoverage.shadowed?(:invented_state) end

      assert_raise FunctionClauseError, fn ->
        RoleCoverage.unmet_promotion_gates(%{coverage: :invented_state})
      end
    end

    test "every promoted family carries evidence for each of its four gates" do
      # The gate this replaces failed on any :relayed declaration at all. It
      # said nothing about which family had which evidence, and it was one line
      # for a promoting author to delete. This one asks each promoted family for
      # its own four by name: the three test gates each name the test that
      # proves them for that family, and the live comparison names the record of
      # a measurement no test can take.
      unmet =
        for {event, declaration} <- RoleCoverage.unscraped_emissions(),
            gate <- RoleCoverage.unmet_promotion_gates(declaration),
            do: "#{inspect(event)}: #{gate}"

      assert unmet == [],
             "these promoted families have not named evidence for a promotion gate: " <>
               Enum.join(unmet, ", ")

      unresolved =
        for {event, declaration} <- RoleCoverage.unscraped_emissions(),
            {gate, reason} <- unresolved_promotion_gates(declaration),
            do: "#{inspect(event)}: #{gate} #{inspect(reason)}"

      assert unresolved == [],
             "these promoted families name evidence that does not resolve against this " <>
               "repository: " <> Enum.join(unresolved, ", ")

      # Both loops above are empty today, because no shipped family is promoted:
      # they would stay green whatever the promoted branch did. So run the same
      # machinery over a shipped declaration moved into that state, and execute
      # the branch a promotion will actually take against this repository.
      {event, shipped} = RoleCoverage.unscraped_emissions() |> Enum.sort() |> hd()
      promoting = Map.put(shipped, :coverage, :relayed)

      assert RoleCoverage.unmet_promotion_gates(promoting) == RoleCoverage.promotion_gates(),
             "promoting #{inspect(event)} unchanged owes every gate"

      {path, names, _suffix} = write_probe_test_file()
      evidenced = Map.put(promoting, :promotion_evidence, evidence_from(path, names))

      assert RoleCoverage.unmet_promotion_gates(evidenced) == []
      assert unresolved_promotion_gates(evidenced) == []

      fabricated =
        Map.put(promoting, :promotion_evidence, evidence_from(path, "a test nobody wrote"))

      assert RoleCoverage.unmet_promotion_gates(fabricated) == [],
             "the shape half cannot tell a fabricated test name from a real one; that is what " <>
               "the resolver is for"

      assert Enum.map(unresolved_promotion_gates(fabricated), &elem(&1, 0)) ==
               RoleCoverage.test_promotion_gates()
    end

    test "a promotion gate naming a test nobody wrote does not resolve" do
      exclusions = ExUnit.configuration()[:exclude]
      on_exit(fn -> ExUnit.configure(exclude: exclusions) end)
      ExUnit.configure(exclude: [role_coverage_excluded_probe: true] ++ exclusions)

      # The shape check passes any pair of non-empty strings, so a declaration
      # could name a test that does not exist, in a file that does not exist, and
      # ship green. Resolving the name binds it to the repository: the file is
      # loaded and the names come from the ExUnit.Test structs the compiled module
      # registers. That is runtime introspection of a compiled artifact, the same
      # class of evidence CallGraph above reads out of .beam chunks, and not a
      # scan of source text — renaming the test reds this, reformatting the file
      # does not. What it binds is that the named test exists and can run: no
      # check here can say it proves its gate, and review still has to.
      promoted = fn evidence ->
        %{
          coverage: :relayed,
          entrypoints: [],
          fallback: "rows",
          note: "note",
          promotion_evidence: evidence
        }
      end

      {path, names, suffix} = write_probe_test_file()
      gates = RoleCoverage.test_promotion_gates()

      # The probe file is not part of this run, so the first resolution compiles
      # it. Every later one reads the same registrations back off the code
      # server, which is the branch a promotion naming an already-loaded file
      # takes.
      refute Path.expand(path, @repo_root) in Code.required_files()
      assert unresolved_promotion_gates(promoted.(evidence_from(path, names))) == []
      assert Path.expand(path, @repo_root) in Code.required_files()
      assert unresolved_promotion_gates(promoted.(evidence_from(path, names))) == []

      # ExUnit registers a test under its describe block, so either spelling of
      # the name resolves.
      qualified = Map.new(names, fn {gate, name} -> {gate, "the probe #{name}"} end)
      assert unresolved_promotion_gates(promoted.(evidence_from(path, qualified))) == []

      for {label, evidence, expected} <- [
            {"a test nobody wrote in a real test file", evidence_from(path, "nobody wrote this"), {:no_such_test, path, "nobody wrote this"}},
            {"a test file nobody wrote", evidence_from("test/does_not_exist_xyz_test.exs", "a test nobody wrote"), {:no_such_file, "test/does_not_exist_xyz_test.exs"}},
            {"a path that is not a test file", evidence_from(".", "."), {:not_a_test_file, "."}},
            {"the repository's own mix file", evidence_from("mix.exs", "anything"), {:not_a_test_file, "mix.exs"}},
            {"a path that escapes the repository", evidence_from("test/../../elsewhere_test.exs", "anything"), {:not_a_test_file, "test/../../elsewhere_test.exs"}},
            {"an absolute path", evidence_from(Path.expand(path, @repo_root), "anything"), {:not_a_test_file, Path.expand(path, @repo_root)}},
            {"this guard's own test", evidence_from(@guard_file, "anything"), {:names_the_guard_itself, @guard_file}},
            {"a test ExUnit will never run", evidence_from(path, "the probe skipped test #{suffix}"), {:skipped_test, path, "the probe skipped test #{suffix}"}},
            {"a test this suite excludes", evidence_from(path, "the probe excluded test #{suffix}"), {:excluded_test, path, "the probe excluded test #{suffix}"}}
          ] do
        declaration = promoted.(Map.put(evidence, :live_comparison, "TODO"))

        assert RoleCoverage.unmet_promotion_gates(declaration) == [],
               "#{label} passes the shape check, which is why the resolver exists"

        assert unresolved_promotion_gates(declaration) == Enum.map(gates, &{&1, expected}),
               "#{label} should not resolve"
      end

      # Three gates asking three different questions cannot be answered by one
      # test. The cheapest declaration that satisfied the resolver named one test
      # three times, and nothing said so.
      one_test = evidence_from(path, names[:double_emission])

      assert unresolved_promotion_gates(promoted.(one_test)) ==
               Enum.map(
                 gates,
                 &{&1, {:evidence_shared_with_another_gate, {path, names[:double_emission]}}}
               )

      two_of_three =
        Map.put(evidence_from(path, names), :relay_round_trip, {path, names[:double_emission]})

      assert Enum.map(unresolved_promotion_gates(promoted.(two_of_three)), &elem(&1, 0)) ==
               [:double_emission, :relay_round_trip]

      # An unpromoted declaration is not resolved at all, so an unpromoted family
      # is never asked for evidence it does not owe.
      assert unresolved_promotion_gates(%{
               promoted.(evidence_from(path, "nobody"))
               | coverage: :partial
             }) ==
               []
    end

    test "the promotion gate reads each family's own evidence" do
      # Run the checker against synthetic declarations, because no shipped
      # family is promoted: a gate that only ever sees :partial declarations
      # proves nothing about what it does to a promoted one.
      assert RoleCoverage.promotion_gates() == [
               :real_worker_emission,
               :double_emission,
               :relay_round_trip,
               :live_comparison
             ]

      complete = %{
        coverage: :relayed,
        entrypoints: [],
        fallback: "rows",
        note: "note",
        promotion_evidence: %{
          real_worker_emission: {"test/some_test.exs", "the real worker emits"},
          double_emission: {"test/some_test.exs", "one sample per emission"},
          relay_round_trip: {"test/some_test.exs", "the row is drained and scraped"},
          live_comparison: "runbook manual telemetry-relay, 2026-09-15 comparison"
        }
      }

      assert RoleCoverage.unmet_promotion_gates(complete) == []

      for gate <- RoleCoverage.promotion_gates() do
        without = update_in(complete.promotion_evidence, &Map.delete(&1, gate))
        assert RoleCoverage.unmet_promotion_gates(without) == [gate]

        blanked =
          put_in(
            complete.promotion_evidence[gate],
            if(gate == :live_comparison, do: "  ", else: {"", ""})
          )

        assert RoleCoverage.unmet_promotion_gates(blanked) == [gate]
      end

      assert RoleCoverage.unmet_promotion_gates(%{complete | promotion_evidence: %{}}) ==
               RoleCoverage.promotion_gates()

      assert RoleCoverage.unmet_promotion_gates(Map.delete(complete, :promotion_evidence)) ==
               RoleCoverage.promotion_gates()

      # An unpromoted family owes none of them.
      assert RoleCoverage.unmet_promotion_gates(%{complete | coverage: :partial}) == []
    end

    test "no shipped family is promoted while its live comparison is outstanding" do
      # Phase 1 is shadowed on purpose: the relay carries the job share, the
      # panels still select via="in_process", and the caveats stay true. This
      # states today's position; the gate above is what a promotion has to pass.
      promoted =
        for {event, %{coverage: coverage}} <- RoleCoverage.unscraped_emissions(),
            RoleCoverage.promoted?(coverage),
            do: event

      assert promoted == [],
             "#{inspect(promoted)} is declared :relayed. Promotion needs that family's " <>
               "real-worker emission test, its double-emission pins, its relay round trip and " <>
               "its live comparison, plus panels that stop filtering via=\"in_process\""
    end

    test "every job-reachable declaration is on the relay allowlist" do
      relayed = MapSet.new(RelayRuntime.relayed_events())

      unrelayed =
        for {event, declaration} <- RoleCoverage.unscraped_emissions(),
            declaration.entrypoints != [],
            not MapSet.member?(relayed, event),
            do: event

      assert unrelayed == [],
             "#{inspect(unrelayed)} is declared with an Oban entrypoint but is not on the " <>
               "relay allowlist, so its job share has no transport to a reporter at all"

      # The converse keeps the allowlist honest: a relayed event nothing
      # declares would carry a job share the guard never checks.
      undeclared = Enum.reject(relayed, &RoleCoverage.declared?/1)

      assert undeclared == [],
             "#{inspect(undeclared)} is relayed but undeclared in RoleCoverage"
    end

    test "declared dashboard series follow the real metric type" do
      by_series = declared_series_by_event()

      assert Map.has_key?(by_series, "codex_pooler_saved_reset_convergence_count")
      refute Map.has_key?(by_series, "codex_pooler_saved_reset_convergence_count_bucket")

      assert Map.has_key?(
               by_series,
               "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds_bucket"
             )

      refute Map.has_key?(
               by_series,
               "codex_pooler_saved_reset_convergence_applied_to_canonical_seconds"
             )
    end

    test "every operator dashboard panel charting one says so in its description" do
      by_series = declared_series_by_event()
      panels = dashboard_panels()

      candidates = Map.keys(by_series)

      charting =
        for panel <- panels,
            events =
              panel
              |> panel_series(candidates)
              |> Enum.flat_map(&Map.get(by_series, &1, []))
              |> Enum.uniq(),
            events != [],
            do: {panel, events}

      # A dashboard that stopped charting these would pass vacuously.
      assert length(charting) >= 4,
             "the operator dashboard no longer charts the metrics RoleCoverage declares; " <>
               "either the panels were dropped or this test stopped finding them"

      silent =
        for {panel, events} <- charting,
            event <- events,
            coverage = RoleCoverage.coverage_for(event),
            not RoleCoverage.markers_correct?(Map.get(panel, "description"), coverage),
            do: {Map.get(panel, "title", "<untitled>"), RoleCoverage.required_marker(coverage)}

      assert silent == [],
             "these operator dashboard panels chart a declared metric and their description " <>
               "does not carry the marker its coverage state owes, so the graph misdescribes " <>
               "itself: " <>
               Enum.map_join(silent, ", ", fn {title, marker} -> "#{title} (#{marker})" end)
    end

    test "a shadowed family's panels select only the in-process share" do
      # Phase 1 relays the job share but keeps it out of the operator totals:
      # every panel charting a still-:partial relayed family pins
      # via="in_process", so the caveat that panel carries stays true. A
      # promoted family owes the opposite — its panels must stop pinning one
      # share — and both directions are checked from the coverage state.
      relayed = MapSet.new(RelayRuntime.relayed_events())
      by_series = declared_series_by_event()
      shadow_filter = @shadow_filter

      candidates = Map.keys(by_series)

      charted =
        for panel <- dashboard_panels(),
            target <- targets(panel),
            expr = target_expr(target),
            series <- panel_series(%{"targets" => [target]}, candidates),
            event <- Map.get(by_series, series, []),
            MapSet.member?(relayed, event),
            do: {Map.get(panel, "title", "<untitled>"), series, expr, RoleCoverage.coverage_for(event)}

      assert charted != [],
             "no operator dashboard panel charts a relayed family any more; this check would " <>
               "pass vacuously"

      # Classified through RoleCoverage rather than matched against the atoms
      # this file happens to know: a family in a state neither arm named used to
      # match neither and be skipped by both.
      unshadowed =
        for {title, series, expr, coverage} <- charted,
            RoleCoverage.shadowed?(coverage),
            not series_pinned?(expr, series),
            do: "#{title} (#{series})"

      assert unshadowed == [],
             "these panels chart a family whose relay is still shadowed but do not pin " <>
               "#{shadow_filter}, so the relayed share silently changes an operator total: " <>
               Enum.join(unshadowed, ", ")

      still_pinned =
        for {title, series, expr, coverage} <- charted,
            RoleCoverage.promoted?(coverage),
            series_pins_anywhere?(expr, series),
            do: "#{title} (#{series})"

      assert still_pinned == [],
             "these panels chart a promoted family but still pin #{shadow_filter}, so the " <>
               "relayed share it was promoted for stays invisible: " <>
               Enum.join(still_pinned, ", ")
    end

    test "the shadow-selector check reads the charted family's own selector" do
      # Three ways a panel used to satisfy the pin without pinning the family it
      # charts. Each expression contains the literal `via="in_process"`, which is
      # all a substring test over the whole expression ever asked for.
      series = "codex_pooler_quota_cycle_decision_count"
      other = "codex_pooler_gateway_stream_outcome_count"

      # A second series in the same expression carries the pin.
      mixed = ~s|sum(rate(#{series}[5m])) + sum(rate(#{other}{via="in_process"}[5m]))|
      assert String.contains?(mixed, @shadow_filter)
      refute series_pinned?(mixed, series)
      assert series_pinned?(mixed, other)

      # A different label whose name merely ends in `via` selects nothing of the kind.
      renamed = ~s|sum(rate(#{series}{relay_via="in_process"}[5m]))|
      assert String.contains?(renamed, @shadow_filter)
      refute series_pinned?(renamed, series)

      # One occurrence pinned and another bare is not a pinned family either.
      half = ~s|sum(rate(#{series}{via="in_process"}[5m])) + sum(rate(#{series}[5m]))|
      assert String.contains?(half, @shadow_filter)
      refute series_pinned?(half, series)

      refute series_pinned?(~s|sum(rate(#{series}[5m]))|, series)
      assert series_pinned?(~s|sum(rate(#{series}{via="in_process"}[5m]))|, series)

      # A Grafana variable puts a `}` inside a quoted label value, so a selector
      # cannot be taken to end at the first closing brace.
      assert series_pinned?(
               ~s|sum(rate(#{series}{pod=~"${pod:regex}", via="in_process"}[5m]))|,
               series
             )

      # PromQL has three string forms and permits comments, so the literal can
      # sit inside the charted series' own braces while the series carries no
      # `via` matcher at all. All three used to read as pinned.
      commented =
        ~s|sum(rate(#{series}{namespace="$ns", job="app"\n| <>
          ~S|  # was via="in_process" before promotion review| <>
          ~s|\n}[5m]))|

      single_quoted = ~s|sum(rate(#{series}{note='via="in_process"'}[5m]))|
      backticked = ~s|sum(rate(#{series}{note=`via="in_process"`}[5m]))|

      for {form, expr} <- [
            {"a # comment where the pin was", commented},
            {"a single-quoted label value", single_quoted},
            {"a backtick label value", backticked}
          ] do
        assert String.contains?(expr, @shadow_filter), "#{form} should contain the literal"
        refute series_pinned?(expr, series), "#{form} is not a label matcher"
        refute series_pins_anywhere?(expr, series), "#{form} is not a label matcher"
      end

      # And the pin is still read when it is a matcher next to any of them.
      assert series_pinned?(
               ~s|sum(rate(#{series}{note='x', via="in_process"} # pinned\n[5m]))|,
               series
             )

      # A series selected by __name__ rather than written bare is the same
      # occurrence. Hardening the matcher had made this correctly-pinned form
      # read as unpinned.
      assert series_pinned?(~s|sum(rate({__name__="#{series}", via="in_process"}[5m]))|, series)
      refute series_pinned?(~s|sum(rate({__name__="#{series}", job="app"}[5m]))|, series)

      assert series_pins_anywhere?(
               ~s|sum(rate({__name__="#{series}", via="in_process"}))|,
               series
             )

      refute series_pinned?(~s|sum(rate({__name__="#{other}", via="in_process"}[5m]))|, series)

      # `via=~"in_process"` selects exactly what `via="in_process"` does, and the
      # negated operators select its complement.
      assert series_pinned?(~s|sum(rate(#{series}{via=~"in_process"}[5m]))|, series)
      refute series_pinned?(~s|sum(rate(#{series}{via!="in_process"}[5m]))|, series)
      refute series_pinned?(~s|sum(rate(#{series}{via="job_relay"}[5m]))|, series)

      refute series_pinned?(
               ~s|sum(rate(#{series}{via="in_process", via!~"in_.*"}[5m]))|,
               series
             )

      assert series_pinned?(
               ~s|sum(rate(#{series}{via="in_process", via!~"job_.*"}[5m]))|,
               series
             )

      for pattern <- ["[a[:alpha:]_]+", "[^[:digit:]]+", "[^][:digit:]]+"] do
        invalid = ~s|sum(rate(#{series}{via="in_process", via!~"#{pattern}"}[5m]))|
        assert PromQL.occurrences(invalid, series) == :invalid
        refute series_pinned?(invalid, series)
        assert series_pins_anywhere?(invalid, series)
      end

      # A `via` written as a grouping label or as a string argument is not a
      # matcher on the series either.
      refute series_pinned?(~s|sum by (via) (rate(#{series}[5m]))|, series)

      refute series_pinned?(
               ~s|label_replace(sum(rate(#{series}[5m])), "via", "in_process", "", "")|,
               series
             )

      # An expression Prometheus would reject does not get to answer either
      # question. Letting an unbalanced brace run to the next `}` let the charted
      # family inherit a pin from an unrelated series — the first defeat again.
      swallowed = ~s|sum(rate(#{series}{namespace="x"[5m])) + sum(rate(up{via="in_process"}[5m]))|

      assert PromQL.occurrences(swallowed, series) == :invalid
      refute series_pinned?(swallowed, series)
      assert series_pins_anywhere?(swallowed, series)
      assert PromQL.charted_series(swallowed, [series]) == [series]

      for broken <- [
            ~s|sum(rate(#{series}{via="in_process"[5m]))|,
            ~s|sum(rate(#{series}{a="1"{, via="in_process"}[5m]))|,
            ~s|sum(rate(#{series}{job="app}[5m]))|
          ] do
        assert PromQL.occurrences(broken, series) == :invalid
        refute series_pinned?(broken, series)
      end

      # Escapes are decoded, not stripped: `\n` is a newline Prometheus selects on
      # and `\x5f` is the underscore it selects on, so one of these charts nothing
      # and the other is a real pin. An escape Prometheus rejects is invalid, and a
      # backquoted string is raw, so the same text there is not an escape at all.
      escaped = fn value -> ~s|sum(rate(#{series}{via=#{value}}[5m]))| end

      refute series_pinned?(escaped.(~S|"i\n_process"|), series)
      assert series_pinned?(escaped.(~S|"in\x5fprocess"|), series)
      assert series_pinned?(escaped.(~S|"in\u005fprocess"|), series)
      assert series_pinned?(escaped.(~S|"in\137process"|), series)
      assert PromQL.occurrences(escaped.(~S|"in\_process"|), series) == :invalid
      refute series_pinned?(escaped.(~S|`in\x5fprocess`|), series)
      assert series_pinned?(escaped.(~S|`in_process`|), series)

      # Label values survive as they were written, not re-encoded byte by byte.
      assert [[{"note", "=", "café"}, {"via", "=", "in_process"}]] =
               PromQL.occurrences(~s|#{series}{note="café", via="in_process"}|, series)

      # The promoted direction fires on a pin anywhere on the charted family.
      assert series_pins_anywhere?(half, series)
      refute series_pins_anywhere?(~s|sum(rate(#{series}[5m]))|, series)
      refute series_pins_anywhere?(mixed, series)

      # Two matchers on one label that contradict each other select nothing, so
      # the family is not pinned by either.
      refute series_pinned?(
               ~s|sum(rate(#{series}{via="in_process", via="job_relay"}[5m]))|,
               series
             )

      refute series_pinned?(
               ~s|sum(rate(#{series}{via="job_relay", via="in_process"}[5m]))|,
               series
             )

      refute series_pinned?(
               ~s|sum(rate(#{series}{via="in_process", via!="in_process"}[5m]))|,
               series
             )

      assert series_pinned?(
               ~s|sum(rate(#{series}{via="in_process", via!="job_relay"}[5m]))|,
               series
             )
    end

    test "the charted-family check reads what an expression selects, not what it spells" do
      series = "codex_pooler_quota_cycle_decision_count"
      other = "codex_pooler_gateway_stream_outcome_count"
      candidates = [series, other]

      # The sixth defeat. Prometheus resolves this to exactly `series`, relayed
      # samples included, and the expression contains no `codex_pooler_` token at
      # all, so a text scan associated the panel with no family and every rule
      # below it silently did not run.
      regex_named =
        ~s|sum by (scope) (rate({__name__=~".+_quota_cycle_decision_count", job="app"}[5m]))|

      refute Regex.match?(~r/\bcodex_pooler_[a-z0-9_]+\b/, regex_named),
             "this expression must not spell the series, or it proves nothing"

      assert PromQL.charted_series(regex_named, candidates) == [series]
      refute series_pinned?(regex_named, series)

      assert series_pinned?(
               String.replace(regex_named, ~s|job="app"|, ~s|via="in_process"|),
               series
             )

      # The ordinary spellings still resolve, and only to their own family.
      assert PromQL.charted_series(~s|sum(rate(#{series}[5m]))|, candidates) == [series]

      assert PromQL.charted_series(~s|sum(rate({__name__="#{other}"}[5m]))|, candidates) == [
               other
             ]

      assert PromQL.charted_series(~s|sum(rate(#{series}[5m])) + #{other}|, candidates) ==
               candidates

      assert PromQL.charted_series(~s|sum(rate(up[5m]))|, candidates) == []
      assert PromQL.charted_series(~s|sum(rate(up{__name__="#{series}"}[5m]))|, candidates) == []

      # A selector that does not bound the name at all selects every metric the
      # job exports, this family included.
      assert PromQL.charted_series(~s|sum(rate({job="codex-pooler-app"}[5m]))|, candidates) ==
               candidates

      assert PromQL.charted_series(~s|sum(rate({__name__!="#{series}"}[5m]))|, candidates) == [
               other
             ]

      # A regex that reaches neither is neither, and one that reaches both is both.
      assert PromQL.charted_series(~s|sum(rate({__name__=~"unrelated_.*"}[5m]))|, candidates) ==
               []

      assert PromQL.charted_series(~s|sum(rate({__name__=~"codex_pooler_.*"}[5m]))|, candidates) ==
               candidates

      assert PromQL.charted_series(
               ~s|sum(rate({__name__=~"codex_pooler_[]a-z_]+"}[5m]))|,
               candidates
             ) == candidates

      assert PromQL.charted_series(
               ~s|sum(rate({__name__=~"codex_pooler_[^]0-9]+"}[5m]))|,
               candidates
             ) == candidates

      # Prometheus anchors the pattern, so a fragment that would match unanchored
      # does not name the series.
      assert PromQL.charted_series(~s|sum(rate({__name__=~"quota_cycle"}[5m]))|, candidates) == []

      # A pattern that will not compile is invalid PromQL; the panel stays inside
      # the rule rather than escaping it.
      assert PromQL.charted_series(~s|sum(rate({__name__=~"["}[5m]))|, candidates) == candidates
      refute series_pinned?(~s|sum(rate({__name__=~"["}[5m]))|, series)

      # Unsupported patterns fail closed before either local engine evaluates the
      # selector. The POSIX-class forms are valid RE2, but Python does not implement
      # their semantics, so the shared conservative subset rejects them too.
      for pattern <- [
            "(?=codex_pooler_.*)codex_pooler_.*",
            ~S|(codex_pooler_.*)\1|,
            "codex_pooler_.{1001}",
            ~S|codex_pooler_\u0061|,
            "codex_pooler_[[:alpha:]_]+",
            "codex_pooler_[a[:alpha:]_]+",
            "codex_pooler_[^[:digit:]]+",
            "codex_pooler_[][:alpha:]_]+"
          ] do
        encoded = String.replace(pattern, "\\", "\\\\")
        invalid = ~s|sum(rate({__name__=~"#{encoded}", via="in_process"}[5m]))|
        assert PromQL.occurrences(invalid, series) == :invalid
        assert PromQL.charted_series(invalid, candidates) == candidates
        refute series_pinned?(invalid, series)
        assert series_pins_anywhere?(invalid, series)
      end
    end

    test "a panel nested two rows deep is still a panel this guard reads" do
      # Grafana rows nest. Flattening one level left a panel two levels down
      # charting whatever it liked, seen by no check here.
      buried = %{"title" => "buried", "targets" => [%{"expr" => "codex_pooler_x"}]}
      inner = %{"title" => "inner row", "panels" => [buried]}
      outer = %{"title" => "outer row", "panels" => [inner]}

      assert Enum.map(flatten_panel(outer), &Map.get(&1, "title")) == [
               "outer row",
               "inner row",
               "buried"
             ]
    end

    test "the panel check would catch a promoted family whose panels still say OBAN_MODE" do
      # The shipped panels all carry OBAN_MODE because every family is
      # :partial. Re-running the same selection against :relayed proves the
      # check is reading the coverage state rather than passing on a constant
      # that happens to be present everywhere.
      by_series = declared_series_by_event()

      charting =
        for panel <- dashboard_panels(),
            panel_series(panel, Map.keys(by_series)) != [],
            do: panel

      assert charting != []

      still_caveated =
        for panel <- charting,
            not RoleCoverage.marker_present?(Map.get(panel, "description"), :relayed),
            do: Map.get(panel, "title", "<untitled>")

      assert still_caveated != [],
             "no shipped panel would fail the relayed-marker check, so promoting a family " <>
               "would not force its panels to be rewritten"
    end
  end

  describe "the guard itself" do
    @tag slow: "compiles a synthetic worker and scans the actual whole-application BEAM call graph for unscripted job metrics"
    test "a metric added on a new job path is derived even though nothing declares it", context do
      {dir, modules} = compile_synthetic_worker()

      on_exit(fn ->
        Enum.each(modules, fn module ->
          :code.purge(module)
          :code.delete(module)
        end)

        File.rm_rf!(dir)
      end)

      graph = CallGraph.scan([Mix.Project.compile_path(), dir])
      roots = CallGraph.worker_entrypoints(graph)
      event = [:codex_pooler, :role_coverage_guard, :probe]

      probe = %{
        graph: graph,
        roots: roots,
        reachable: CallGraph.reachable(graph, roots),
        sites: CallGraph.emission_sites(graph, [event | declared_metric_events()])
      }

      # The synthetic worker stands in for the next metric someone adds on a job
      # path: a declared metric whose only emitter is reached from a perform/1.
      assert length(roots) == length(context.roots) + 1
      refute RoleCoverage.declared?(event)

      derived = derive_unscraped_events(probe)

      assert List.keymember?(derived, event, 0),
             "the guard did not derive a synthetic job-only emission, so it would not catch " <>
               "the next metric added on a job path"

      # The same scan still sees the real application, so the probe is not
      # passing by scanning nothing but itself.
      assert derived |> Enum.map(&elem(&1, 0)) |> Enum.sort() ==
               Enum.sort([event | job_declared_events()])
    end
  end

  defp derive_unscraped_events(%{sites: sites, reachable: reachable}) do
    for {event, event_sites} <- Enum.sort(sites),
        reached = Enum.filter(event_sites, &MapSet.member?(reachable, &1)),
        reached != [],
        do: {event, Enum.sort(reached)}
  end

  defp derived_events(context),
    do: context |> derive_unscraped_events() |> Enum.map(&elem(&1, 0))

  defp job_declared_events do
    for {event, declaration} <- RoleCoverage.unscraped_emissions(),
        declaration.entrypoints != [],
        do: event
  end

  defp derived_entrypoints(%{graph: graph, roots: roots, sites: sites}, event) do
    sites = Map.fetch!(sites, event)

    roots
    |> Enum.filter(fn root ->
      from_root = CallGraph.reachable(graph, [root])
      Enum.any?(sites, &MapSet.member?(from_root, &1))
    end)
    |> Enum.map(fn {module, _f, _a} -> module end)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp describe_derivation(%{graph: graph, roots: roots}, {event, sites}) do
    witness =
      sites
      |> Enum.find_value(&CallGraph.witness(graph, roots, &1))
      |> Enum.map_join("\n    ", &inspect/1)

    "#{inspect(event)} reached by:\n    #{witness}"
  end

  defp declared_metric_events,
    do: Telemetry.prometheus_metrics() |> Enum.map(& &1.event_name) |> Enum.uniq()

  defp declared_series_by_event do
    for metric <- Telemetry.prometheus_metrics(),
        RoleCoverage.declared?(metric.event_name),
        name <- exported_metric_series(metric),
        reduce: %{} do
      acc -> Map.update(acc, name, [metric.event_name], &Enum.uniq([metric.event_name | &1]))
    end
  end

  defp exported_metric_series(metric) do
    base = Enum.map_join(metric.name, "_", &Atom.to_string/1)

    case metric do
      %DistributionMetric{} ->
        Enum.map(@distribution_suffixes, &(base <> &1))

      _metric ->
        [base]
    end
  end

  # Rows nest, and a row inside a row was invisible to a single flatten: a panel
  # two levels down charted whatever it liked and no guard here saw it.
  defp dashboard_panels do
    @dashboard
    |> File.read!()
    |> Jason.decode!()
    |> Map.fetch!("panels")
    |> Enum.flat_map(&flatten_panel/1)
  end

  defp flatten_panel(panel),
    do: [panel | Enum.flat_map(Map.get(panel, "panels") || [], &flatten_panel/1)]

  # Which declared series a panel charts. Scanning the expression's text for
  # `codex_pooler_…` answered a different question — which names it spells — and a
  # panel that selects a declared family through `{__name__=~"…"}` spells none, so
  # it was associated with no family and every rule below silently skipped it. The
  # selection is read from the same tokens the pin is.
  defp panel_series(panel, candidates) do
    panel
    |> targets()
    |> Enum.flat_map(&PromQL.charted_series(target_expr(&1), candidates))
    |> Enum.uniq()
  end

  # Grafana serialises a panel with no queries as `"targets": null`, which
  # `Map.get/3` hands back as `nil` rather than as its default.
  defp targets(panel), do: Map.get(panel, "targets") || []

  defp target_expr(target), do: Map.get(target, "expr") || ""

  # Whether `series` carries the shadow pin as a label matcher of its OWN, at
  # every place the expression selects it. A test over the expression's text is
  # satisfied by any other series in the same expression that happens to pin it,
  # by a label whose name merely ends in `via`, by a `#` comment where a pin used
  # to be, and by the pin quoted inside some other label's single-quoted or
  # backtick value. None of those says anything about the family being charted,
  # and none of them is a label matcher, so PromQL.occurrences/2 never sees one.
  defp series_pinned?(expr, series) do
    case PromQL.occurrences(expr, series) do
      :invalid -> false
      [] -> false
      occurrences -> Enum.all?(occurrences, &shadow_pin?/1)
    end
  end

  # The promoted direction: the family is charted with the shadow pin still on
  # it anywhere, so the share the promotion was for stays invisible.
  # An expression Prometheus would reject is treated as pinning, so a promoted
  # family's broken panel reds here too rather than passing the one direction
  # `series_pinned?/2`'s `false` does not cover.
  defp series_pins_anywhere?(expr, series) do
    case PromQL.occurrences(expr, series) do
      :invalid -> true
      occurrences -> Enum.any?(occurrences, &shadow_pin?/1)
    end
  end

  defp shadow_pin?(matchers), do: PromQL.selects?(matchers, @shadow_label, @shadow_value)

  # Promotion evidence built from a probe file: `{path, name}` per test gate,
  # plus a live comparison record the resolver does not touch. A single name is
  # expanded to all three gates, which is what a fabricated declaration looks
  # like and is exactly what the distinctness rule refuses.
  defp evidence_from(path, name) when is_binary(name),
    do: evidence_from(path, Map.new(RoleCoverage.test_promotion_gates(), &{&1, name}))

  defp evidence_from(path, names) when is_map(names) do
    names
    |> Map.new(fn {gate, name} -> {gate, {path, name}} end)
    |> Map.put(:live_comparison, "runbook manual telemetry-relay, 2026-09-15 comparison")
  end

  # The test gates whose declared `{file, test name}` names nothing this
  # repository has, or names it in a way that is not evidence, with the reason.
  # `RoleCoverage.unmet_promotion_gates/1` can only check the shape of a
  # declaration; binding the name to reality needs the repository, which is the
  # guard's to read.
  defp unresolved_promotion_gates(%{coverage: coverage} = declaration) do
    if RoleCoverage.promoted?(coverage),
      do: gate_reasons(Map.get(declaration, :promotion_evidence) || %{}),
      else: []
  end

  defp gate_reasons(evidence) do
    gates = RoleCoverage.test_promotion_gates()
    shared = repeated_evidence(evidence, gates)

    gates
    |> Enum.map(&{&1, gate_reason(Map.get(evidence, &1), &1 in shared)})
    |> Enum.reject(&match?({_gate, nil}, &1))
  end

  defp gate_reason(reference, shared?) do
    unresolved_reason(reference) ||
      if shared?, do: {:evidence_shared_with_another_gate, reference}
  end

  # Three gates asking three different questions cannot be answered by one test.
  # Nothing said so, and the cheapest declaration that satisfied the resolver was
  # to name one test three times.
  defp repeated_evidence(evidence, gates) do
    evidence
    |> Map.take(gates)
    |> Enum.group_by(&elem(&1, 1), &elem(&1, 0))
    |> Enum.flat_map(fn {_reference, sharing} ->
      if length(sharing) > 1, do: sharing, else: []
    end)
  end

  # A gate may name only a test file, by a repository-relative path. Without that
  # the resolver hands whatever the declaration points at to `Code.require_file/1`
  # and runs it: measured, `mix.exs` redefines the Mix project and raises,
  # `README.md` raises a `SyntaxError`, and an ordinary `.exs` executes its
  # top-level code inside the test run. The declaration is in-repo and reviewed,
  # so that is a sharp edge rather than an injection channel, and it has no
  # upside. Naming this guard is refused separately: a guard cannot be its own
  # evidence.
  defp unresolved_reason({path, name}) when is_binary(path) and is_binary(name) do
    cond do
      not test_file_path?(path) -> {:not_a_test_file, path}
      path == @guard_file -> {:names_the_guard_itself, path}
      not File.regular?(Path.expand(path, @repo_root)) -> {:no_such_file, path}
      true -> registration_reason(path, name)
    end
  end

  defp unresolved_reason(evidence), do: {:not_a_test_reference, evidence}

  defp test_file_path?(path) do
    Path.type(path) == :relative and String.ends_with?(path, "_test.exs") and
      ".." not in Path.split(path)
  end

  defp registration_reason(path, name) do
    case find_registered_test(Path.expand(path, @repo_root), name) do
      nil -> {:no_such_test, path, name}
      test -> runnable_reason(test, path, name)
    end
  end

  # A registered test is not necessarily a test that runs. `@tag :skip` and a tag
  # the suite excludes each leave the registration in place while ExUnit refuses
  # to execute it, so evidence naming one names nothing that ever asserts
  # anything. The decision is ExUnit's own, not a second implementation of it.
  defp runnable_reason(test, path, name) do
    config = ExUnit.configuration()

    case ExUnit.Filters.eval(config[:include] || [], config[:exclude] || [], test.tags, []) do
      :ok -> if test.tags[:skip], do: {:skipped_test, path, name}
      {:excluded, _why} -> {:excluded_test, path, name}
      {:skipped, _why} -> {:skipped_test, path, name}
    end
  end

  # Loading the file compiles it; the compiled module exports `__ex_unit__/0`,
  # whose `%ExUnit.Test{}` structs carry the registered name, its tags and the
  # file it was defined in. No test is run, and no source text is read. A file
  # this run has already loaded is not required again, so its modules come from
  # the code server instead.
  defp find_registered_test(file, name) do
    modules =
      case Code.require_file(file) do
        nil -> for {module, _beam} <- :code.all_loaded(), do: module
        compiled -> Enum.map(compiled, &elem(&1, 0))
      end

    Enum.find_value(modules, fn module ->
      function_exported?(module, :__ex_unit__, 0) and
        Enum.find(module.__ex_unit__().tests, &names_test?(&1, file, name))
    end)
  end

  # ExUnit registers a test as `:"test <describe> <name>"`, so a gate may name it
  # either as it is written or with its describe block prefixed.
  defp names_test?(%ExUnit.Test{} = test, file, name) do
    test.tags.file == file and
      Atom.to_string(test.name) in ["test #{name}", "test #{test.tags[:describe]} #{name}"]
  end

  # An ExUnit file this run has not loaded, with one test per test gate plus a
  # skipped and an excluded one, so a declaration built from it resolves, is
  # distinct per gate, and can demonstrate what does not resolve. It is written
  # into the repository because a gate may only name a repository-relative path;
  # `tmp/` is gitignored and outside `test_paths`, so `mix test` never collects
  # it, and `compile_synthetic_worker/0` can use `System.tmp_dir!()` only because
  # nothing there has to be namable by a declaration. Every test in it raises, so
  # if loading one ever enqueued it with ExUnit this run would carry an extra
  # failure rather than quietly running someone else's test.
  defp write_probe_test_file do
    suffix = System.unique_integer([:positive])
    gates = RoleCoverage.test_promotion_gates()
    names = Map.new(gates, &{&1, "the probe #{&1} gate resolves #{suffix}"})
    path = Path.join(@probe_dir, "role_coverage_gate_probe_#{suffix}_test.exs")
    full = Path.expand(path, @repo_root)
    File.mkdir_p!(Path.dirname(full))
    sweep_stale_probes(Path.dirname(full))

    on_exit(fn ->
      File.rm_rf!(full)
      File.rmdir(Path.dirname(full))
    end)

    body =
      Enum.map_join(names, "\n", fn {_gate, name} -> probe_test(name, "") end) <>
        probe_test("the probe skipped test #{suffix}", "@tag :skip\n    ") <>
        probe_test("the probe excluded test #{suffix}", "@tag :role_coverage_excluded_probe\n    ")

    File.write!(full, """
    defmodule RoleCoverageGateProbe#{suffix}Test do
      use ExUnit.Case, async: false

      describe "the probe" do
    #{body}  end
    end
    """)

    {path, names, suffix}
  end

  defp probe_test(name, tag) do
    """
        #{tag}test "#{name}" do
          raise "loading a named evidence file must not run its tests"
        end
    """
  end

  # An abnormal exit leaves one behind in a directory six agents share; a run an
  # hour later is not the one that wrote it.
  defp sweep_stale_probes(dir) do
    cutoff = System.os_time(:second) - 3600

    for stale <- Path.wildcard(Path.join(dir, "role_coverage_gate_probe_*_test.exs")),
        match?(
          {:ok, %File.Stat{mtime: mtime}} when mtime < cutoff,
          File.stat(stale, time: :posix)
        ),
        do: File.rm(stale)
  end

  defp compile_synthetic_worker do
    dir =
      Path.join(System.tmp_dir!(), "role_coverage_guard_#{System.unique_integer([:positive])}")

    # The probe is compiled from a lib/ path because the guard builds its graph
    # from application sources only.
    source_dir = Path.join(dir, "lib")
    File.mkdir_p!(source_dir)
    suffix = System.unique_integer([:positive])

    source = """
    defmodule RoleCoverageGuardProbe.Emitter#{suffix} do
      @event [:codex_pooler, :role_coverage_guard, :probe]

      def record(value) do
        :telemetry.execute(@event, %{count: 1}, %{value: value})
      end
    end

    defmodule RoleCoverageGuardProbe.Worker#{suffix} do
      @behaviour Oban.Worker

      alias RoleCoverageGuardProbe.Emitter#{suffix}, as: Emitter

      @impl Oban.Worker
      def perform(%{args: args}), do: handle(args)

      defp handle(args), do: Emitter.record(args)
    end
    """

    # The guard reads abstract code, so the probe has to carry it whatever the
    # ambient compiler options are.
    debug_info = Code.get_compiler_option(:debug_info)
    Code.put_compiler_option(:debug_info, true)

    source_path = Path.join(source_dir, "role_coverage_guard_probe.ex")
    File.write!(source_path, source)

    {compiled, _stderr} =
      try do
        ExUnit.CaptureIO.with_io(:stderr, fn -> Code.compile_file(source_path) end)
      after
        Code.put_compiler_option(:debug_info, debug_info)
      end

    for {module, binary} <- compiled do
      path = Path.join(dir, "#{module}.beam")
      File.write!(path, binary)

      assert {:ok, {^module, [abstract_code: {:raw_abstract_v1, _forms}]}} =
               :beam_lib.chunks(String.to_charlist(path), [:abstract_code]),
             "the probe module compiled without debug info, so it proves nothing"
    end

    {dir, Enum.map(compiled, &elem(&1, 0))}
  end
end
