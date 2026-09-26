defmodule CodexPooler.TestAppEnvTest do
  use ExUnit.Case, async: false

  alias CodexPooler.TestAppEnv

  @flag :websocket_owner_forwarding_enabled
  @budget_ms 5_000

  for outcome <- [:normal, :synthetic_failure] do
    @tag outcome: outcome
    test "preserves absent, nil and boolean flags after #{outcome} test exit", %{outcome: outcome} do
      TestAppEnv.restore_on_exit(@flag)

      for initial <- [:error, {:ok, nil}, {:ok, false}, {:ok, true}] do
        case initial do
          :error -> Application.delete_env(:codex_pooler, @flag)
          {:ok, value} -> Application.put_env(:codex_pooler, @flag, value)
        end

        # Exercise the same on-exit handler ExUnit invokes after its test
        # process dies; the callback must not depend on that process surviving.
        {pid, monitor} =
          spawn_monitor(fn ->
            ExUnit.OnExitHandler.register(self())
            TestAppEnv.restore_on_exit(@flag)
            Application.put_env(:codex_pooler, @flag, :changed)
            exit(outcome)
          end)

        assert_receive {:DOWN, ^monitor, :process, ^pid, ^outcome}, @budget_ms
        assert :ok = ExUnit.OnExitHandler.run(pid, @budget_ms)
        assert Application.fetch_env(:codex_pooler, @flag) === initial
      end
    end
  end
end
