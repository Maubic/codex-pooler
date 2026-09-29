defmodule CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement do
  @moduledoc """
  Final accounting boundary for routed gateway attempts.

  Runtime gateway dispatch decides transport flow and route health side effects; this
  module owns the terminal attempt/reservation accounting calls and their
  sanitized failure logging.
  """

  alias CodexPooler.Accounting
  alias CodexPooler.Accounting.{Attempt, Request}
  alias CodexPooler.Accounting.FailureResponse
  alias CodexPooler.Gateway.Contracts
  alias CodexPooler.Gateway.Persistence.CodexTurn
  alias CodexPooler.Gateway.Persistence.SessionContinuity
  alias CodexPooler.Gateway.Persistence.SessionContinuity.OwnerWitness
  alias CodexPooler.Gateway.Runtime.Finalization.SettlementRetry

  @type attrs :: %{optional(atom()) => term()}
  @type usage :: %{optional(atom()) => term()} | %{optional(String.t()) => term()}
  @type gateway_error :: Contracts.gateway_error()
  @type finalization_result ::
          {:ok, Accounting.internal_request_result_row()}
          | {:stale_generation, Accounting.internal_request_result_row()}
          | {:error, gateway_error()}
  @type settlement_result ::
          finalization_result()
          | {:ok, Accounting.request_result_row() | Attempt.t()}

  @spec stale_generation?(term()) :: boolean()
  def stale_generation?({:stale_generation, _result}), do: true
  def stale_generation?(_result), do: false

  @doc false
  @spec first_settlement?(
          Accounting.internal_request_result_row()
          | Accounting.finalization_disposition()
        ) :: boolean()
  def first_settlement?(%{finalization_disposition: disposition}),
    do: first_settlement?(disposition)

  def first_settlement?(:inserted), do: true
  def first_settlement?(disposition) when disposition in [:replaced, :reused], do: false

  @spec finalize_success(Request.t(), Attempt.t(), usage(), attrs()) :: finalization_result()
  def finalize_success(request, attempt, usage, attrs),
    do: finalize_success(request, attempt, usage, attrs, nil)

  @spec finalize_success(Request.t(), Attempt.t(), usage(), attrs(), OwnerWitness.t() | nil) ::
          finalization_result()
  def finalize_success(request, attempt, usage, attrs, owner_witness) do
    attrs =
      attrs
      |> Map.new()
      |> complete_turn_before_commit(CodexTurn.succeeded_status(), nil, attempt, owner_witness)

    :finalize_success
    |> SettlementRetry.run(request, attempt, fn -> Accounting.finalize_success_with_disposition(request, attempt, usage, attrs) end)
    |> accounting_result(:finalize_success, request, attempt)
  end

  @spec finalize_failure(Request.t(), Attempt.t(), attrs()) :: finalization_result()
  def finalize_failure(request, attempt, attrs),
    do: finalize_failure(request, attempt, attrs, nil)

  @spec finalize_failure(Request.t(), Attempt.t(), attrs(), OwnerWitness.t() | nil) ::
          finalization_result()
  def finalize_failure(request, attempt, attrs, owner_witness) do
    attrs = Map.new(attrs)

    attrs =
      complete_turn_before_commit(
        attrs,
        CodexTurn.failed_status(),
        Map.get(attrs, :last_error_code),
        attempt,
        owner_witness
      )

    :finalize_failure
    |> SettlementRetry.run(request, attempt, fn -> Accounting.finalize_failure_with_disposition(request, attempt, attrs) end)
    |> accounting_result(:finalize_failure, request, attempt)
  end

  @spec finalize_partial_stream_failure(Request.t(), Attempt.t(), usage(), attrs()) ::
          finalization_result()
  def finalize_partial_stream_failure(request, attempt, usage, attrs),
    do: finalize_partial_stream_failure(request, attempt, usage, attrs, nil)

  @spec finalize_partial_stream_failure(
          Request.t(),
          Attempt.t(),
          usage(),
          attrs(),
          OwnerWitness.t() | nil
        ) :: finalization_result()
  def finalize_partial_stream_failure(request, attempt, usage, attrs, owner_witness) do
    attrs = Map.new(attrs)
    error_code = Map.get(attrs, :last_error_code)

    attrs =
      complete_turn_before_commit(
        attrs,
        partial_stream_turn_status(error_code),
        error_code,
        attempt,
        owner_witness
      )

    :finalize_partial_stream_failure
    |> SettlementRetry.run(request, attempt, fn -> Accounting.finalize_partial_stream_failure_with_disposition(request, attempt, usage, attrs) end)
    |> accounting_result(:finalize_partial_stream_failure, request, attempt)
  end

  @spec record_retryable_failure(Request.t(), Attempt.t(), attrs()) :: settlement_result()
  def record_retryable_failure(request, attempt, attrs) do
    :record_retryable_failure
    |> SettlementRetry.run(request, attempt, fn -> Accounting.record_retryable_attempt_failure(attempt, attrs) end)
    |> accounting_result(:record_retryable_failure, request, attempt)
  end

  @spec finalize_reservation_failure(Request.t(), attrs()) :: settlement_result()
  def finalize_reservation_failure(request, attrs) do
    attrs = Map.new(attrs)
    error_code = Map.get(attrs, :last_error_code)

    attrs =
      Map.put(attrs, :before_commit, fn result ->
        SessionContinuity.complete_codex_turn({:ok, result}, CodexTurn.failed_status(), error_code)
        :ok
      end)

    :finalize_reservation_failure
    |> SettlementRetry.run(request, nil, fn -> Accounting.finalize_reservation_failure(request, attrs) end)
    |> accounting_result(:finalize_reservation_failure, request)
  end

  # The request's codex turn completes inside the settlement transaction, as
  # its last write, so no resend can observe the request terminal while its
  # turn is still in progress (findings#288). Accounting runs the callback
  # only for the current generation: a stale generation writes nothing, and
  # its turn belongs to the generation that replaced it.
  defp complete_turn_before_commit(attrs, status, error_code, attempt, owner_witness) do
    Map.put(attrs, :before_commit, fn result ->
      SessionContinuity.complete_codex_turn({:ok, result}, status, error_code, attempt, owner_witness)
      :ok
    end)
  end

  defp accounting_result(result, operation, request, attempt \\ nil)

  defp accounting_result(
         {:ok, %{stale_generation?: true} = value},
         _operation,
         _request,
         _attempt
       ),
       do: {:stale_generation, value}

  defp accounting_result({:ok, value}, _operation, _request, _attempt), do: {:ok, value}

  # `SettlementRetry` already logged the one warning that names the request
  # and the stage; the request is left to execution recovery.
  defp accounting_result({:error, :settlement_retry_exhausted}, _operation, _request, _attempt),
    do: {:error, %{status: 500, code: "gateway_accounting_failed", message: "gateway accounting finalization failed"}}

  defp accounting_result(
         {:error, %{code: code}},
         _operation,
         _request,
         _attempt
       )
       when code in [:request_already_finalized, :attempt_already_finalized] do
    {:error,
     %{
       status: 499,
       code: Atom.to_string(code),
       message: "request lifecycle already completed"
     }}
  end

  defp accounting_result({:error, reason}, operation, request, attempt) do
    FailureResponse.accounting_failure(operation, request, attempt, reason)
  end

  defp partial_stream_turn_status("client_disconnected"), do: CodexTurn.interrupted_status()
  defp partial_stream_turn_status(:client_disconnected), do: CodexTurn.interrupted_status()
  # A rollout drain ends the turn the same way owner-side drain finalization
  # does (`Finalization.Interruption.terminal_turn_status/1`): interrupted, not
  # failed. The upstream did nothing wrong.
  defp partial_stream_turn_status("owner_drained"), do: CodexTurn.interrupted_status()
  defp partial_stream_turn_status(:owner_drained), do: CodexTurn.interrupted_status()
  defp partial_stream_turn_status(_error_code), do: CodexTurn.failed_status()
end
