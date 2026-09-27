defmodule CodexPooler.UnboxedFixture do
  @moduledoc """
  Helpers for fixtures that must commit rows outside the Ecto sandbox.

  `run_unboxed/2` runs the block in a linked `Task` so the caller's sandbox
  checkout is never replaced. That link is also why cleanup must be *registered*
  rather than *scoped*: an assertion failing inside the block raises in the task,
  the exit signal kills the untrapped test process immediately, and a `try/after`
  wrapped around the call never runs. The committed rows then survive into every
  later test of the same `mix test` invocation, where they surface as unique index
  violations in unrelated files instead of as the one real failure.

  `register_unboxed_cleanup!/2` puts the cleanup in ExUnit's own teardown, which
  runs however the test process died.
  """

  alias CodexPooler.Repo
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence
  alias Ecto.Adapters.SQL.Sandbox

  # Failure-detection budget for one unboxed block, not a behaviour timer.
  @default_timeout 15_000

  @doc """
  Runs `fun` against an unboxed connection and returns its value.
  """
  @spec run_unboxed((-> result), timeout()) :: result when result: term()
  def run_unboxed(fun, timeout \\ @default_timeout) when is_function(fun, 0) do
    Task.async(fn -> Sandbox.unboxed_run(Repo, fun) end)
    |> Task.await(timeout)
  end

  @doc """
  Registers `fun` as unboxed teardown for the calling test.

  Must be called from the test process. The callback runs in ExUnit's on-exit
  handler after the test process is gone, including when it died from an
  assertion that failed inside an earlier `run_unboxed/2` block. It first
  waits for every websocket session cleanup still running, since one can
  still write rows of the fixture (findings#206 row 206-405).

  A timed-out barrier fails teardown without deleting the rows: a live writer
  can still commit references to them. This is deliberate fail-closed behavior,
  not successful cleanup. The generic fixture has no socket/task ownership
  proof with which to cancel those writers; the owning scenario must release
  its barriers and stop its processes before this callback runs.
  """
  @spec register_unboxed_cleanup!((-> term()), timeout()) :: :ok
  def register_unboxed_cleanup!(fun, timeout \\ @default_timeout) when is_function(fun, 0) do
    ExUnit.Callbacks.on_exit(fn -> cleanup_unboxed!(fun, timeout) end)
  end

  @doc false
  @spec cleanup_unboxed!((-> term()), timeout(), keyword()) :: term()
  def cleanup_unboxed!(fun, timeout \\ @default_timeout, opts \\ []) when is_function(fun, 0) do
    :ok = WebsocketCleanupFence.await_session_cleanups!(Keyword.get(opts, :cleanup_wait, @default_timeout))
    run_unboxed(fun, timeout)
  end
end
