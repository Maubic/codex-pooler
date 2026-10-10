Code.require_file("codex_pooler/test_listener_cleanup.ex", __DIR__)

paths =
  case System.argv() do
    [] -> Path.wildcard("test/**/*.{ex,exs}")
    paths -> paths
  end

if paths == [], do: raise("listener cleanup: no test source files found")

findings = Enum.flat_map(paths, &CodexPooler.TestListenerCleanup.check_file!/1)

if findings == [] do
  IO.puts("listener cleanup: checked #{length(paths)} files")
else
  Enum.each(findings, &IO.puts(:stderr, CodexPooler.TestListenerCleanup.message(&1)))
  System.halt(1)
end
