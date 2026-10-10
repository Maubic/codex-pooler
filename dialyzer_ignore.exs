[
  # Stock Ecto.Multi.new/0 -> run/3 infers conflicting MapSet/sets opacity.
  # https://github.com/elixir-lang/elixir/issues/15391
  # https://github.com/elixir-lang/elixir/issues/15673
  # Exact call site and short diagnostic; unused filters fail the quality gate.
  {"lib/codex_pooler/catalog/sync/persistence.ex:40:11:call_without_opaque Type mismatch in call without opaque term in run.", :call_without_opaque, 40}
]
