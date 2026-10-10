defmodule CodexPooler.TestListenerCleanupTest do
  use ExUnit.Case, async: true

  alias CodexPooler.TestListenerCleanup

  @script Path.expand("../../../dev_support/check_test_listener_cleanup.exs", __DIR__)
  @root Path.expand("../../..", __DIR__)

  for expression <- [
        "assert {:error, :econnrefused} = :gen_tcp.connect({127, 0, 0, 1}, port, [], 1000)",
        "assert :gen_tcp.connect(~c\"127.0.0.1\", port, []) == {:error, :econnrefused}",
        "assert {:error, :econnrefused} === :gen_tcp.connect(host, port, [], 1000)",
        "assert match?({:error, :econnrefused}, :gen_tcp.connect(host, port, []))",
        "result = :gen_tcp.connect(host, port, []); assert {:error, :econnrefused} = result",
        "result = :gen_tcp.connect(host, port, []); assert result == {:error, :econnrefused}"
      ] do
    test "rejects #{expression}" do
      source = "defmodule Example do\n  def check do\n    #{unquote(expression)}\n  end\nend"
      assert [%{path: "fixture.exs", line: 3}] = TestListenerCleanup.findings(Code.string_to_quoted!(source), "fixture.exs")
    end
  end

  test "keeps comments, strings, quoted fixtures, error handling and owned Unix socket assertions" do
    source = ~S'''
    # assert {:error, :econnrefused} = :gen_tcp.connect(host, port, [])
    text = "assert {:error, :econnrefused} = :gen_tcp.connect(host, port, [])"
    quote do: assert({:error, :econnrefused} = :gen_tcp.connect(host, port, []))
    case :gen_tcp.connect(host, port, []) do
      {:error, :econnrefused} -> :unavailable
      {:ok, socket} -> :gen_tcp.close(socket)
    end
    assert {:error, :econnrefused} = :gen_tcp.connect({:local, socket_path}, 0, [])
    assert {:error, :econnrefused} = expected_error()
    assert {:ok, _socket} = :gen_tcp.connect(host, port, [])
    '''

    assert [] == TestListenerCleanup.findings(Code.string_to_quoted!(source), "fixture.exs")
  end

  test "does not carry a connection result across lexical blocks or variable reassignment" do
    source = ~S'''
    def first do
      result = :gen_tcp.connect(host, port, [])
      result = expected_error()
      assert {:error, :econnrefused} = result
    end
    def second do
      assert {:error, :econnrefused} = result
    end
    '''

    assert [] == TestListenerCleanup.findings(Code.string_to_quoted!(source), "fixture.exs")
  end

  for scope <- [
        "fn result -> assert {:error, :econnrefused} = result end",
        "fn _unused, result when is_tuple(result) -> assert {:error, :econnrefused} = result end",
        "fn %{result: result} when is_tuple(result) -> assert {:error, :econnrefused} = result end",
        "def check(result), do: assert({:error, :econnrefused} = result)",
        "case expected_error() do result -> assert {:error, :econnrefused} = result end",
        "{result, other} = expected_pair(); assert {:error, :econnrefused} = result",
        "for result <- results, do: assert({:error, :econnrefused} = result)",
        "with {:ok, result} <- expected_error(), do: assert({:error, :econnrefused} = result)"
      ] do
    test "respects shadowing in #{scope}" do
      source = "result = :gen_tcp.connect(host, port, []); #{unquote(scope)}"
      assert [] == TestListenerCleanup.findings(Code.string_to_quoted!(source), "fixture.exs")
    end
  end

  test "still rejects a captured connection result and a pinned clause variable" do
    for scope <- ["fn _other -> assert {:error, :econnrefused} = result end", "fn first, second when is_tuple(result) -> assert {:error, :econnrefused} = result end", "case value do ^result -> assert {:error, :econnrefused} = result end"] do
      source = "result = :gen_tcp.connect(host, port, []); #{scope}"
      assert [%{line: 1}] = TestListenerCleanup.findings(Code.string_to_quoted!(source), "fixture.exs")
    end
  end

  @tag :tmp_dir
  test "actual Mix alias runs from a cold source-only copy without dependencies or a build directory", %{tmp_dir: tmp_dir} do
    files = ["mix.exs", "dev_support/check_test_listener_cleanup.exs", "dev_support/codex_pooler/test_listener_cleanup.ex"]

    for file <- files do
      target = Path.join(tmp_dir, file)
      File.mkdir_p!(Path.dirname(target))
      File.cp!(Path.join(@root, file), target)
    end

    File.mkdir_p!(Path.join(tmp_dir, "test"))
    fixture = Path.join(tmp_dir, "test/listener_test.exs")
    File.write!(fixture, "assert :erlang.port_info(owned_socket) == :undefined\n")
    refute File.exists?(Path.join(tmp_dir, "deps"))
    refute File.exists?(Path.join(tmp_dir, "_build"))
    env = [{"ERL_FLAGS", "+S 2:2"}, {"MIX_ENV", "test"}, {"MIX_BUILD_PATH", nil}, {"MIX_DEPS_PATH", nil}]
    options = [cd: tmp_dir, stderr_to_stdout: true, env: env]

    assert {"listener cleanup: checked 1 files\n", 0} = System.cmd(System.find_executable("mix"), ["quality.test_cleanup"], options)
    File.write!(fixture, "assert {:error, :econnrefused} = :gen_tcp.connect(host, port, [])\n")
    {output, exit_code} = System.cmd(System.find_executable("mix"), ["quality.test_cleanup"], options)
    assert exit_code == 1
    assert output =~ "TCP connection refusal cannot prove owned listener cleanup"
    refute File.exists?(Path.join(tmp_dir, "deps"))
    refute File.exists?(Path.join(tmp_dir, "_build"))
  end

  @tag :tmp_dir
  test "standalone gate fails before application boot, names the offending line, and accepts a corrected fixture", %{tmp_dir: tmp_dir} do
    path = Path.join(tmp_dir, "listener_test.exs")
    File.write!(path, "assert {:error, :econnrefused} = :gen_tcp.connect(host, port, [])\n")
    {output, exit_code} = run_gate(path)
    assert exit_code == 1
    assert output =~ "#{path}:1: TCP connection refusal cannot prove owned listener cleanup"

    File.write!(path, "assert :erlang.port_info(owned_socket) == :undefined\n")
    assert {"listener cleanup: checked 1 files\n", 0} = run_gate(path)

    File.write!(path, "def broken(\n")
    {output, exit_code} = run_gate(path)
    assert exit_code != 0
    assert output =~ "TokenMissingError"

    {output, exit_code} = System.cmd(System.find_executable("elixir"), [@script], cd: tmp_dir, stderr_to_stdout: true, env: [{"ERL_FLAGS", "+S 2:2"}])
    assert exit_code != 0
    assert output =~ "no test source files found"
  end

  defp run_gate(path) do
    System.cmd(System.find_executable("elixir"), [@script, path], stderr_to_stdout: true, env: [{"ERL_FLAGS", "+S 2:2"}])
  end
end
