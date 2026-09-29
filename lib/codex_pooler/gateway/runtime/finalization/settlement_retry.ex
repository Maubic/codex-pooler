defmodule CodexPooler.Gateway.Runtime.Finalization.SettlementRetry do
  @moduledoc """
  Runs one settlement transaction again after a transient database failure,
  inside a bounded window, instead of letting the exception end the process
  that settles the request.

  A settlement is one PostgreSQL transaction (`RequestLifecycle`): the
  request, attempt, ledger, rollup and turn writes and the route-health and
  quota side effects of `before_finalize` commit or roll back together. When
  PostgreSQL cancels, drops or refuses that transaction for a reason that says
  nothing about the work (`CodexPooler.Platform.TransientDatabaseError`), none
  of it is left behind and the same call can run again. A COMMIT the client
  saw fail may still have committed on the server; the settlement locks the
  request's rows first and reuses a recorded settlement it finds there, so the
  repeated call writes nothing twice.

  Any other exception, and any failure inside a transaction the caller owns
  (whose rollback belongs to the caller), still raises at once.

  The window covers the production database stalls that cut settlements
  (about 30 to 75 seconds of queued and cancelled statements, the longest
  needing about 55 seconds after the first failure): a new try starts at most
  `@window_ms` after the first failure, each try keeps the Repo's own query
  timeout, and the tries are spaced by a capped exponential backoff. The HTTP
  body of a stream ends after its settlement, so the client waits through the
  retries; the OpenAI SDKs read for 600 seconds and the released Codex client
  stops reading at `response.completed`.

  When the window closes, one warning names the request and the stage. An
  HTTP request then answers the sanitized accounting failure so its
  connection still ends cleanly, and its row is left to execution recovery,
  as before; a websocket response task raises the last failure, so its own
  exception finalization settles the turn as before.
  """

  require Logger

  alias CodexPooler.Platform.TransientDatabaseError
  alias CodexPooler.Repo

  @window_ms 60_000
  @initial_backoff_ms 250
  @max_backoff_ms 5_000

  @type stage :: atom()

  @doc """
  Runs `settle`, retrying it after a transient database failure within the
  window. Returns what `settle` returns, or `{:error, :settlement_retry_exhausted}`
  for an HTTP request whose window closed.
  """
  @spec run(stage(), map() | nil, map() | nil, (-> result)) :: result | {:error, :settlement_retry_exhausted}
        when result: term()
  def run(stage, request, attempt, settle) when is_atom(stage) and is_function(settle, 0) do
    if Repo.in_transaction?() do
      settle.()
    else
      settle(%{stage: stage, request: request, attempt: attempt, settle: settle, config: config(), try_number: 1, first_failure_ms: nil, started_ms: now_ms()})
    end
  end

  defp settle(%{settle: settle} = run) do
    result = settle.()
    if run.try_number > 1, do: log_settled_after_retry(run)
    result
  rescue
    error in [DBConnection.ConnectionError, Postgrex.Error] ->
      if TransientDatabaseError.transient?(error),
        do: retry_or_give_up(run, error, __STACKTRACE__),
        else: reraise(error, __STACKTRACE__)
  end

  defp retry_or_give_up(run, error, stacktrace) do
    now = now_ms()
    first_failure_ms = run.first_failure_ms || now
    remaining_ms = run.config.window_ms - (now - first_failure_ms)

    if remaining_ms > 0 do
      delay_ms = min(backoff_ms(run.config, run.try_number), remaining_ms)
      log_retry(run, error, delay_ms)
      Process.sleep(delay_ms)
      settle(%{run | try_number: run.try_number + 1, first_failure_ms: first_failure_ms})
    else
      log_exhausted(run, error, now)
      exhausted(run, error, stacktrace)
    end
  end

  # A websocket response task keeps its exception finalization; every other
  # transport settles in the HTTP connection process, whose response must end.
  defp exhausted(%{request: %{transport: "websocket"}}, error, stacktrace), do: reraise(error, stacktrace)
  defp exhausted(_run, _error, _stacktrace), do: {:error, :settlement_retry_exhausted}

  # Equal jitter: half the doubled step plus a random share of the other half,
  # so the settlements a stall cut do not all try again in the same instant.
  defp backoff_ms(%{initial_backoff_ms: initial_ms, max_backoff_ms: cap_ms}, try_number) do
    step = min(initial_ms * Integer.pow(2, try_number - 1), cap_ms)
    half = div(step, 2)
    half + :rand.uniform(max(step - half, 1)) - 1
  end

  defp log_retry(run, error, delay_ms) do
    Logger.info(
      "gateway settlement met a transient database failure; retrying " <>
        "stage=#{run.stage} request_id=#{record_id(run.request)} attempt_id=#{record_id(run.attempt)} " <>
        "settlement_try=#{run.try_number} reason_class=#{TransientDatabaseError.reason_class(error)} retry_in_ms=#{delay_ms}"
    )
  end

  defp log_settled_after_retry(run) do
    Logger.info(
      "gateway settlement completed after a transient database failure " <>
        "stage=#{run.stage} request_id=#{record_id(run.request)} attempt_id=#{record_id(run.attempt)} " <>
        "settlement_tries=#{run.try_number} elapsed_ms=#{now_ms() - run.started_ms}"
    )
  end

  defp log_exhausted(run, error, now) do
    Logger.warning(
      "gateway settlement abandoned after transient database failures " <>
        "stage=#{run.stage} request_id=#{record_id(run.request)} attempt_id=#{record_id(run.attempt)} " <>
        "settlement_tries=#{run.try_number} elapsed_ms=#{now - run.started_ms} " <>
        "reason_class=#{TransientDatabaseError.reason_class(error)} fallback=#{fallback(run.request)}"
    )
  end

  defp fallback(%{transport: "websocket"}), do: "task_exception"
  defp fallback(_request), do: "execution_recovery"

  defp record_id(%{id: id}) when is_binary(id), do: id
  defp record_id(_record), do: "unknown"

  defp now_ms, do: System.monotonic_time(:millisecond)

  defp config do
    overrides = config_overrides()

    %{
      window_ms: Keyword.get(overrides, :window_ms, @window_ms),
      initial_backoff_ms: Keyword.get(overrides, :initial_backoff_ms, @initial_backoff_ms),
      max_backoff_ms: Keyword.get(overrides, :max_backoff_ms, @max_backoff_ms)
    }
  end

  # The window and backoff are fixed in every release; only the test
  # environment reads a shorter window or backoff, so no operator setting or
  # environment variable can change them.
  if Mix.env() == :test do
    defp config_overrides, do: Application.get_env(:codex_pooler, __MODULE__, [])
  else
    defp config_overrides, do: []
  end
end
