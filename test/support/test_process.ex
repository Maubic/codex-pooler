defmodule CodexPooler.TestProcess do
  @moduledoc """
  Helpers for tests that watch another process end.

  `Process.monitor/1` only queues its request. The VM parks the request taken by a process on a normal scheduler and
  sends it when that process is scheduled out, sends a signal to the same pid, or takes another monitor; a signal sent
  to a different process does not send it. A test that monitors X and then ends X through a signal to another process
  can therefore read `DOWN ... :noproc` instead of X's exit reason when its scheduler thread stalls between the two
  calls and X dies in the meantime (findings#303 row 303-8, Drone 1809; the same race is the unexplained `:noproc` of
  findings#221 row 221-75).
  """

  @doc """
  Monitors `pid` and returns the monitor reference once the VM has sent the monitor request to `pid`, so a process that
  dies right afterwards still reports its own exit reason.

  Prefer a protocol-level ordering wherever `pid` can answer: monitor it, then call it (`:sys.get_state/1` for a
  gen_server, gen_statem or any process that answers system messages, or a call the test already makes, moved after the
  monitor). A message to the monitored process is ordered behind the monitor request by the pairwise order of signals
  and its reply proves the request was handled, on any OTP. A trigger that is itself a signal to `pid` needs nothing,
  and neither does a monitor taken at spawn (`spawn_monitor/1`, a Task's own `ref`).

  Use this function only for a plain process that cannot answer. It relies on behaviour measured on OTP 29.1.1 (erts
  17.1), not on a documented guarantee (findings#303 row 303-8): the caller's pending monitor request is sent when the
  caller is scheduled out, and `:erlang.yield/0` schedules it out. `Process.alive?/1`, `Process.info/2` and monitoring
  `self()` do not send it. With the test thread blocked right after the trigger (12 rounds, on the default scheduler
  count and on 4), a bare `Process.monitor/1` read `:noproc` in every round and this function never did. Measure again
  after an OTP upgrade.
  """
  @spec monitor_flushed(pid()) :: reference()
  def monitor_flushed(pid) when is_pid(pid) do
    reference = Process.monitor(pid)
    :erlang.yield()
    reference
  end
end
