defmodule CodexPooler.DialyzerIgnoreTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Dialyxir.{FilterMap, Formatter, Project}

  @source "lib/codex_pooler/catalog/sync/persistence.ex"

  test "only the confirmed public Multi.run opacity warning is excluded" do
    filter_map = Project.filter_map([])
    assert {true, [_filter]} = Project.filter_warning?(multi_warning(), filter_map)

    controls = [
      multi_warning(file: "lib/codex_pooler/catalog/sync.ex"),
      multi_warning(file: "other/" <> @source),
      multi_warning(line: 41),
      multi_warning(column: 12),
      multi_warning(function: :update),
      multi_warning(function: :run_after),
      {:warn_return_no_exit, {String.to_charlist(@source), {40, 11}}, {:no_return, [:only_normal, :persist_catalog, 5]}}
    ]

    for warning <- controls do
      assert {false, []} = Project.filter_warning?(warning, filter_map)
    end
  end

  test "the stock formatter retains other warnings and reports exactly one skipped warning" do
    remaining = multi_warning(line: 41)

    output =
      capture_io(fn ->
        assert {:ok, [formatted], :no_unused_filters} =
                 Formatter.format_and_filter([multi_warning(), remaining], Project, [], [Formatter.Raw])

        assert formatted == Formatter.Raw.format(remaining)
      end)

    assert output =~ "Total errors: 2, Skipped: 1, Unnecessary Skips: 0"
  end

  test "a new real warning category at the same call site remains active" do
    warning = {:warn_return_no_exit, {String.to_charlist(@source), {40, 11}}, {:no_return, [:only_normal, :persist_catalog, 5]}}

    capture_io(fn ->
      assert {:ok, [formatted], :no_unused_filters} =
               Formatter.format_and_filter([multi_warning(), warning], Project, [], [Formatter.Raw])

      assert formatted == Formatter.Raw.format(warning)
    end)
  end

  test "the configured unused filter check fails when the upstream warning disappears" do
    map = Project.filter_map([])
    assert map.list_unused_filters?
    assert map.unused_filters_as_errors?
    assert [_filter] = FilterMap.unused_filters(map)

    output =
      capture_io(fn ->
        assert {:error, [], {:unused_filters_present, description}} =
                 Formatter.format_and_filter([], Project, [], [Formatter.Raw])

        assert description =~ "Unused filters:"
      end)

    assert output =~ "Total errors: 0, Skipped: 0, Unnecessary Skips: 1"
  end

  test "an explicit false list-unused option keeps stock caller precedence" do
    refute Project.filter_map(list_unused_filters: false).unused_filters_as_errors?
  end

  # Observed OTP 29 warning envelope and call site; reduced synthetic type payload.
  # The real filter API formats this tuple, not a prebuilt description string.
  defp multi_warning(opts \\ []) do
    file = opts |> Keyword.get(:file, @source) |> String.to_charlist()
    location = {Keyword.get(opts, :line, 40), Keyword.get(opts, :column, 11)}
    args = ~C"(#{'__struct__':='Elixir.Ecto.Multi'},'owned_run',fun((_,_) -> {'ok','ok'}))"
    conflicts = [{1, :erl_types.t_any(), ~c"'Elixir.Ecto.Multi':t()"}]

    {:warn_opaque, {file, location}, {:call_without_opaque, [Ecto.Multi, Keyword.get(opts, :function, :run), args, conflicts, []]}}
  end
end
