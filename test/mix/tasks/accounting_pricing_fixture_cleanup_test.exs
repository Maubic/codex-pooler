defmodule CodexPooler.AccountingPricingFixtureCleanupTest do
  use ExUnit.Case, async: false

  @probe ~S"""
  ExUnit.start(autorun: false, max_cases: 2, seed: 478)
  :ets.new(:pricing_fixture_probe, [:named_table, :public])
  coordinator = spawn_link(fn ->
    receive do
      {:ready, first} ->
        receive do
          {:ready, second} ->
            send(first, :both_ready)
            send(second, :both_ready)
        after
          5_000 -> exit(:second_fixture_not_ready)
        end
    after
      5_000 -> exit(:first_fixture_not_ready)
    end
  end)
  Process.register(coordinator, :pricing_fixture_barrier)

  defmodule PricingFixturePassingCase do
    use ExUnit.Case, async: true
    test "keeps its fixture while the other case fails" do
      generated_at = ~U[2026-01-01 00:00:00Z]
      path = CodexPooler.AccountingTestSupport.write_tmp_pricing_json!(generated_at, "sample-model", %{"input" => Decimal.new("0.0125")})
      document = path |> File.read!() |> CodexPooler.JSON.decode!()
      assert document["generated_at"] == DateTime.to_iso8601(generated_at)
      assert document["models"]["sample-model"]["prices"]["standard"]["default"]["input"] == 0.0125
      :ets.insert(:pricing_fixture_probe, {:passing, self(), path})
      send(:pricing_fixture_barrier, {:ready, self()})
      assert_receive :both_ready, 5_000
      assert_receive {:failed_cleanup, isolated}, 5_000
      assert isolated
      assert File.regular?(path)
      assert File.read!(System.fetch_env!("PRICING_SENTINEL")) == "unrelated"
    end
  end

  defmodule PricingFixtureFailingCase do
    use ExUnit.Case, async: true
    test "removes both fixtures even after an assertion failure" do
      # Registered first so the helper's later callbacks run before observation.
      on_exit(fn ->
        [{:failing, paths}] = :ets.lookup(:pricing_fixture_probe, :failing)
        [{:passing, pid, path}] = :ets.lookup(:pricing_fixture_probe, :passing)
        isolated = Enum.all?(paths, &(not File.exists?(&1))) and File.regular?(path)
        send(pid, {:failed_cleanup, isolated})
      end)
      paths = for _ <- 1..2, do: CodexPooler.AccountingTestSupport.write_tmp_pricing_json!(~U[2026-01-01 00:00:00Z], "sample-model", %{"input" => 10})
      assert length(Enum.uniq(paths)) == 2
      assert Enum.all?(paths, &File.regular?/1)
      :ets.insert(:pricing_fixture_probe, {:failing, paths})
      send(:pricing_fixture_barrier, {:ready, self()})
      assert_receive :both_ready, 5_000
      flunk("intentional lifecycle failure")
    end
  end

  result = ExUnit.run()
  [{:failing, failed_paths}] = :ets.lookup(:pricing_fixture_probe, :failing)
  [{:passing, _, passed_path}] = :ets.lookup(:pricing_fixture_probe, :passing)
  receipt = %{
    total: result.total,
    failures: result.failures,
    failed_removed: Enum.all?(failed_paths, &(not File.exists?(&1))),
    passed_removed: not File.exists?(passed_path),
    sentinel_retained: File.read!(System.fetch_env!("PRICING_SENTINEL")) == "unrelated"
  }
  File.write!(System.fetch_env!("PRICING_RECEIPT"), CodexPooler.JSON.encode!(receipt))
  """

  @tag :tmp_dir
  @tag slow: "runs two concurrent real ExUnit cases in an isolated child VM, including an intentional assertion failure"
  test "pricing fixture teardown is exact and runs after passing and failing cases", %{tmp_dir: root} do
    on_exit(fn ->
      File.rm_rf!(root)
      refute File.exists?(root)
    end)

    script = Path.join(root, "probe.exs")
    receipt_path = Path.join(root, "receipt.json")
    sentinel = Path.join(root, "unrelated.json")
    File.write!(script, @probe)
    File.write!(sentinel, "unrelated")
    code_paths = :code.get_path() |> Enum.flat_map(&["-pa", List.to_string(&1)])

    assert {_output, 0} = System.cmd("elixir", ["--erl", "+S 2:2"] ++ code_paths ++ [script], env: [{"TMPDIR", Path.expand(root)}, {"PRICING_SENTINEL", Path.expand(sentinel)}, {"PRICING_RECEIPT", Path.expand(receipt_path)}], stderr_to_stdout: true)

    receipt = receipt_path |> File.read!() |> CodexPooler.JSON.decode!()
    assert receipt["failed_removed"]
    assert receipt["passed_removed"]
    assert receipt["sentinel_retained"]
    assert receipt["total"] == 2
    assert receipt["failures"] == 1
    assert Enum.sort(File.ls!(root)) == ["probe.exs", "receipt.json", "unrelated.json"]
    CodexPooler.TestDiagnostics.puts("pricing fixture child cases=2 expected_failures=1 failed_removed=true passed_removed=true concurrent_owner_preserved=true sentinel_retained=true")
  end
end
