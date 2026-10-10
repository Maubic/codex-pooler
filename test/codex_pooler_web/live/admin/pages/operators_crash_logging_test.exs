defmodule CodexPoolerWeb.Admin.OperatorsCrashLoggingTest do
  use CodexPoolerWeb.ConnCase, async: false

  import ExUnit.CaptureLog
  import Phoenix.LiveViewTest

  setup :register_and_log_in_user

  # An outer failure-log capture would itself print the private report on the red run.
  @tag capture_log: false
  test "an unmatched authenticated event still crashes with useful credential-free diagnostics", %{conn: conn, user: user} do
    Process.flag(:trap_exit, true)
    {:ok, view, _html} = live(conn, ~p"/admin/operators")
    summary = capture_termination(view, user.password_hash)

    assert summary.process_failed?
    assert summary.termination_report?
    assert summary.function_clause?
    assert summary.operator_handler?
    refute summary.hash_visible?
    refute summary.password_visible?
  end

  defp capture_termination(view, hash) do
    monitor = Process.monitor(view.pid)

    {failed?, log} =
      with_log(fn ->
        try do
          render_hook(view, "synthetic_unmatched_event", %{})
        rescue
          _error -> :raised
        catch
          :exit, _reason -> :exited
        end

        receive do
          {:DOWN, ^monitor, :process, _pid, reason} -> match?({:function_clause, _}, reason)
        after
          5_000 -> false
        end
      end)

    %{
      process_failed?: failed?,
      termination_report?: String.contains?(log, "terminating"),
      function_clause?: String.contains?(log, "FunctionClauseError"),
      operator_handler?: String.contains?(log, "CodexPoolerWeb.Admin.OperatorsLive.handle_event/3"),
      hash_visible?: String.contains?(log, hash),
      password_visible?: String.contains?(log, CodexPooler.AccountsFixtures.valid_user_password())
    }
  end
end
