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
  alias CodexPooler.Platform.ExecutionProofPublisher
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
    publisher = ExUnit.Callbacks.start_supervised!({ExecutionProofPublisher, enabled: true, name: :cleanup_proof_race_publisher, interval_ms: 60_000})
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
        attempt = Repo.one!(from(a in Attempt, where: a.request_id == ^request_id, order_by: [desc: a.attempt_number], limit: 1))
        :ok = ExecutionProofSupport.await_terminal!(attempt, publisher)
        send(registry, {ref, :release})
        send(test, {ref, :proven, attempt.id})
    after
      @detection_timeout_ms -> flunk("the closing socket never awaited its stopped direct task's cleanup")
    end
  end
end
