defmodule CodexPooler.Gateway.RequestCompression.CommandProvenanceTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.RequestCompression.CommandProvenance

  test "classifies function-call commands by the programs they execute" do
    for {command, expected} <- [
          {"rg -n 'needle' lib", :search},
          {"/usr/bin/grep -rn needle .", :search},
          {"git grep -n needle", :search},
          {"git -C lib -c core.quotepath=off grep -n needle", :search},
          {"find lib -name '*.ex' | xargs grep -n needle", :search},
          {"stdbuf -oL rg needle lib | head -n 20", :search},
          {"make lint", :other},
          {"staticcheck ./...", :other},
          {"git log --grep needle", :other}
        ] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == expected, command
    end
  end

  test "never takes an operand for the executed program" do
    for command <- [
          "staticcheck ./grep",
          "python sample.py grep",
          "go test ./grep/...",
          "make rg",
          "echo grep rg ag",
          "xargs echo grep",
          "git log --grep grep",
          "bash -lc 'python sample.py grep'",
          "timeout 5 staticcheck ./grep"
        ] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == :other, command
    end

    assert CommandProvenance.classify(%{"command" => ["staticcheck", "./grep"]}) == :other
    assert CommandProvenance.classify(%{"command" => ["bash", "-lc", "python sample.py grep"]}) == :other
  end

  test "reads git global options with their values before the subcommand" do
    for {command, expected} <- [
          {"git --git-dir grep show HEAD:diagnostics.txt", :other},
          {"git --git-dir=grep show HEAD:diagnostics.txt", :other},
          {"git -C grep log", :other},
          {"git --work-tree lib --no-pager grep -n needle", :search},
          {"git --namespace grep --config-env color.ui=GIT_COLOR grep -n needle", :search},
          {"git --frobnicate grep -n needle", :unknown},
          {"git --version", :other}
        ] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == expected, command
    end
  end

  test "classifies the stage producing the final output of a pipeline" do
    for {command, expected} <- [
          {"rg --files -g diagnostics.txt | xargs cat", :other},
          {"rg --files -g '*.go' | xargs staticcheck", :other},
          {"rg -n needle lib | head -n 20", :search},
          {"rg -n needle lib | uniq -c", :search},
          # The bounded lexer reads one pipe; longer pipelines stay unknown.
          {"rg -n needle lib | sort | uniq -c", :unknown},
          {"git grep -n needle | env LC_ALL=C sort -k1,1", :search},
          {"rg -n needle lib | tee matches.txt", :search},
          {"rg -n needle lib | sort -k1,1", :search},
          # --files0-from makes sort read the files a list names, not its input.
          {"rg --files -g '*.txt' | sort --files0-from=paths.nul", :other},
          {"rg --files -g '*.txt' | sort --files0-from paths.nul", :other},
          {"rg --files -g '*.txt' | sort -u --files0-from=-", :other},
          {"make lint | grep error", :other},
          {"cargo build | tee build.log", :other},
          {"cat notes.txt | grep -n needle", :other},
          {"rg -n needle lib | head notes.txt", :other},
          {"rg -n needle lib | timeout --bogus 5 cat", :unknown},
          {"rg -n needle lib |", :unknown},
          {"rg -n needle lib | ", :unknown},
          {"head -n 5", :unknown}
        ] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == expected, command
    end
  end

  test "treats command lookups as describing, not running, the program" do
    for command <- ["command -v grep", "command -V rg", "command -pv rg"] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == :other, command
    end

    assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => "command rg needle lib"})) == :search
  end

  test "looks through assignments and supported wrappers to the executed program" do
    for command <- [
          "LC_ALL=C rg needle lib",
          "env LC_ALL=C grep -rn needle .",
          "env -i PATH=/usr/bin grep -rn needle .",
          "time rg needle lib",
          "nice -n 5 rg needle lib",
          "nice -10 rg needle lib",
          "timeout 10 rg needle lib",
          "timeout -s KILL --kill-after=2 5s grep -rn needle .",
          "xargs -0 -n1 -P4 grep -n needle",
          "command rg needle lib",
          "nohup grep -rn needle .",
          "bash -o pipefail -c 'rg -n needle lib'",
          "zsh -lc 'env LC_ALL=C grep -rn needle lib'"
        ] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == :search, command
    end
  end

  test "reads argv vectors, unwrapping shell script runners" do
    assert CommandProvenance.classify(%{"command" => ["bash", "-lc", "rg -n needle lib"]}) == :search
    assert CommandProvenance.classify(%{"command" => ["cargo", "build"]}) == :other

    local_shell = %{"type" => "local_shell_call", "action" => %{"type" => "exec", "command" => ["/bin/zsh", "-c", "grep -n needle lib"]}}
    assert CommandProvenance.classify(local_shell) == :search
  end

  test "reports unknown provenance when the executed program cannot be read" do
    for arguments <- [
          "{}",
          "not json",
          CodexPooler.JSON.encode!(%{"cmd" => "cd lib && rg needle"}),
          CodexPooler.JSON.encode!(%{"cmd" => "rg needle 2>/dev/null"}),
          CodexPooler.JSON.encode!(%{"cmd" => "rg a", "command" => "rg b"}),
          CodexPooler.JSON.encode!(%{"cmd" => "env -S 'rg needle'"}),
          CodexPooler.JSON.encode!(%{"cmd" => "timeout --bogus 5 rg needle"}),
          CodexPooler.JSON.encode!(%{"cmd" => "LC_ALL=C"}),
          CodexPooler.JSON.encode!(%{"cmd" => "make lint | xargs --bogus grep needle"}),
          %{"command" => ["bash", "-lc", "cd lib && rg needle"]},
          %{"type" => "local_shell_call", "action" => %{"type" => "exec", "command" => []}},
          nil
        ] do
      assert CommandProvenance.classify(arguments) == :unknown, inspect(arguments)
    end
  end
end
