defmodule CodexPoolerWeb.Runtime.CleanupProofRace do
  @moduledoc false

  # A closing socket stops its direct task, then interrupts the task's request
  # with its own reason (`DirectCleanup.terminate_admission/2`). The stopped
  # task's end is an execution end like any other, so the production proof
  # publisher publishes its terminal proof: about 100 ms after the exit, or at
  # once when an earlier early publication or the tick is already under way.
  # When the proof lands between the stop and the interrupt (a few
  # milliseconds), the interrupt used to take it for a lost executor and settle
  # the request `dead_execution_recovered` (findings#270 row 270-353).
  #
  # `arm!/1` makes that order deterministic. It starts the production
  # publisher and holds the activity registry, with a `:sys` debug hook, right
  # before it answers the closing socket's direct-cleanup await: the task is
  # stopped by then and its request not interrupted yet. A prover process then
  # waits until the publisher proved the stopped task's end and releases the
  # registry. The hold is outside any transaction, so the publisher's write
  # does not queue behind the interrupt on the shared sandbox connection.

  import Ecto.Query
  import ExUnit.Assertions

  alias CodexPooler.Accounting.Attempt
  alias CodexPooler.ExecutionProofSupport
  alias CodexPooler.Gateway.Transports.Websocket.ActivityRegistry
  alias CodexPooler.Repo

  @detection_timeout_ms 15_000

  @type t :: %{ref: reference(), prover: pid()}

  @doc """
  Runs `cut` (the client's close and the wait for its cleanup) and, with
  `proof_before_cleanup: true` in `opts`, proves the stopped task's end before
  the cleanup interrupts its request. Returns the cut's result and the proven
  attempt's id (nil without the option).
  """
  @spec around_cut(keyword(), Ecto.UUID.t(), (-> result)) :: {result, Ecto.UUID.t() | nil} when result: term()
  def around_cut(opts, request_id, cut) when is_function(cut, 0) do
    if Keyword.get(opts, :proof_before_cleanup, false) do
      race = arm!(request_id)
      result = cut.()
      {result, assert_proven!(race)}
    else
      {cut.(), nil}
    end
  end

  @doc "Arms the hold for the cut of `request_id`, from the test process, before the cut."
  @spec arm!(Ecto.UUID.t()) :: t()
  def arm!(request_id) do
    publisher = CodexPooler.ExecutionProofSupport.start_publisher!(name: :cleanup_proof_race_publisher, interval_ms: 60_000)
    test = self()
    ref = make_ref()
    prover = spawn_link(fn -> prove(ref, request_id, publisher, test) end)
    # The hook's state is a map: `:sys` drops a two-tuple state silently.
    :ok = :sys.install(ActivityRegistry, {&__MODULE__.hook/3, %{ref: ref, prover: prover}})
    %{ref: ref, prover: prover}
  end

  @doc "Asserts that the cleanup met the proof of its stopped task's end."
  @spec assert_proven!(t()) :: Ecto.UUID.t()
  def assert_proven!(%{ref: ref}) do
    assert_receive {^ref, :proven, attempt_id}, @detection_timeout_ms
    attempt_id
  end

  @doc """
  The owner's side of the same race (findings#270 row 270-362): the owner
  interrupts its own active turn through its persistence right after the
  drain's cut made the socket stop the turn's executor. Wraps the owner's
  interruption callback, in the owner's own state, so that it waits until the
  production publisher proved the executor's end, then interrupts as before.
  Call it from the test process once the owner runs the turn and before the
  cut; `assert_owner_interrupt_proven!/1` then asserts the order held.
  """
  @spec hold_owner_interrupt!(pid(), Ecto.UUID.t()) :: reference()
  def hold_owner_interrupt!(owner, request_id) do
    publisher = CodexPooler.ExecutionProofSupport.start_publisher!(name: :cleanup_proof_race_owner_publisher, interval_ms: 60_000)
    test = self()
    ref = make_ref()

    _state =
      :sys.replace_state(owner, fn state ->
        interrupt = state.persistence.interrupt_codex_session

        proven_interrupt = fn session_id, opts ->
          attempt = latest_attempt!(request_id)
          :ok = ExecutionProofSupport.await_terminal!(attempt, publisher)
          send(test, {ref, :proven, attempt.id})
          interrupt.(session_id, opts)
        end

        %{state | persistence: %{state.persistence | interrupt_codex_session: proven_interrupt}}
      end)

    ref
  end

  @spec assert_owner_interrupt_proven!(reference()) :: Ecto.UUID.t()
  def assert_owner_interrupt_proven!(ref) do
    assert_receive {^ref, :proven, attempt_id}, @detection_timeout_ms
    attempt_id
  end

  @doc """
  The window a resend can be claimed in (findings#270 row 270-364): holds the
  closing socket's cleanup at the first transaction it begins once `task` is
  dead, which is its interrupt of the stopped task's request, right after that
  transaction's `begin`, before it read or locked anything. Rows must be
  committed (the test runs the Sandbox in `:auto` mode, each process on its own
  connection), so a resend claimed on a new connection meanwhile reads what the
  cleanup committed before its interrupt, with the request still in progress.
  Only statements of processes started by `socket` are matched (the cleanup
  runs in a task the socket started, `$callers`). `await_interrupt_held!/1`
  answers the held process; it waits for `release_interrupt/2`, or releases
  itself after the detection budget.
  """
  @spec hold_interrupt_after_stop!(pid(), pid()) :: reference()
  def hold_interrupt_after_stop!(socket, task) do
    ref = make_ref()
    handler_id = {__MODULE__, ref}
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{ref: ref, test: self(), socket: socket, task: task, claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :repo, :query], &__MODULE__.handle_query/4, config)
    ref
  end

  @spec await_interrupt_held!(reference()) :: pid()
  def await_interrupt_held!(ref) do
    receive do
      {^ref, :held, cleanup} ->
        ExUnit.Callbacks.on_exit(fn -> send(cleanup, {ref, :release}) end)
        cleanup
    after
      @detection_timeout_ms -> flunk("the closing socket's cleanup never began its interrupt after the stop")
    end
  end

  @spec release_interrupt(reference(), pid()) :: :ok
  def release_interrupt(ref, cleanup) do
    send(cleanup, {ref, :release})
    :ok
  end

  @doc false
  def handle_query(_event, _measurements, %{query: "begin"}, %{ref: ref, test: test, socket: socket, task: task, claimed: claimed}) do
    if socket in Process.get(:"$callers", []) and not Process.alive?(task) and :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {ref, :held, self()})

      receive do
        {^ref, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  def handle_query(_event, _measurements, _metadata, _config), do: :ok

  @doc false
  def hook(%{ref: ref, prover: prover}, {:in, {:"$gen_call", _from, {:direct_await, _context}}}, _name) do
    send(prover, {ref, :held, self()})

    receive do
      {^ref, :release} -> :ok
    after
      @detection_timeout_ms -> :ok
    end

    :done
  end

  def hook(hold, _event, _name), do: hold

  defp prove(ref, request_id, publisher, test) do
    receive do
      {^ref, :held, registry} ->
        attempt = latest_attempt!(request_id)
        :ok = ExecutionProofSupport.await_terminal!(attempt, publisher)
        send(registry, {ref, :release})
        send(test, {ref, :proven, attempt.id})
    after
      @detection_timeout_ms -> flunk("the closing socket never awaited its stopped direct task's cleanup")
    end
  end

  defp latest_attempt!(request_id), do: Repo.one!(from(a in Attempt, where: a.request_id == ^request_id, order_by: [desc: a.attempt_number], limit: 1))
end
