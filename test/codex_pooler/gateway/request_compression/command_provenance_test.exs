defmodule CodexPooler.Gateway.RequestCompression.CommandProvenanceTest do
  use ExUnit.Case, async: true

  alias CodexPooler.Gateway.RequestCompression.CommandProvenance

  test "classifies function-call commands by whether any word names a search engine" do
    for {command, expected} <- [
          {"rg -n 'needle' lib", :search},
          {"/usr/bin/grep -rn needle .", :search},
          {"git grep -n needle", :search},
          {"find lib -name '*.ex' | xargs grep -n needle", :search},
          {"make lint", :other},
          {"staticcheck ./...", :other},
          {"bash -lc 'rg -n needle lib'", :search},
          {"bash -lc 'go vet ./...'", :other}
        ] do
      assert CommandProvenance.classify(CodexPooler.JSON.encode!(%{"cmd" => command})) == expected
    end
  end

  test "reads argv vectors, unwrapping shell script runners" do
    assert CommandProvenance.classify(%{"command" => ["bash", "-lc", "rg -n needle lib"]}) == :search
    assert CommandProvenance.classify(%{"command" => ["cargo", "build"]}) == :other

    local_shell = %{"type" => "local_shell_call", "action" => %{"type" => "exec", "command" => ["/bin/zsh", "-c", "grep -n needle lib"]}}
    assert CommandProvenance.classify(local_shell) == :search
  end

  test "reports unknown provenance for calls without a readable command" do
    for arguments <- [
          "{}",
          "not json",
          CodexPooler.JSON.encode!(%{"cmd" => "cd lib && rg needle"}),
          CodexPooler.JSON.encode!(%{"cmd" => "rg needle 2>/dev/null"}),
          CodexPooler.JSON.encode!(%{"cmd" => "rg a", "command" => "rg b"}),
          %{"command" => ["bash", "-lc", "cd lib && rg needle"]},
          %{"type" => "local_shell_call", "action" => %{"type" => "exec", "command" => []}},
          nil
        ] do
      assert CommandProvenance.classify(arguments) == :unknown
    end
  end
end
