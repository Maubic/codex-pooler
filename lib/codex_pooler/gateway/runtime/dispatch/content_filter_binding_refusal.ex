defmodule CodexPooler.Gateway.Runtime.Dispatch.ContentFilterBindingRefusal do
  @moduledoc false

  # The successor of a content-filter terminal dispatches only under the binding
  # its guided retry took at admission: the account, credential epoch, serving
  # mode and models of the content-filtered attempt, with that attempt still the
  # predecessor's final one (`NativeContentFilterRetry.dispatch_allowed?/2`).
  # Admission refuses every other resend of such a turn, so attempt creation
  # refuses only a binding the candidate can no longer honour. That refusal used
  # to answer 500 `gateway_accounting_failed` and leave the request `in_progress`
  # until the stale-reservation sweep (findings#316). It is finalized instead,
  # its reservation released in full, and answered as a refused resend. Before
  # any attempt the release is a pre-attempt `routing_rejected` one; after an
  # attempt whose retry never started it carries that attempt and no phase
  # (findings#221).

  alias CodexPooler.Accounting.{Attempt, PreAttemptRelease}
  alias CodexPooler.Gateway.Routing.RouteLifecycle
  alias CodexPooler.Gateway.Runtime.Dispatch.SelectedCandidateContext
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Runtime.Routing.DispatchLifecycle

  @code :invalid_content_filter_retry_binding

  @doc "The accounting error code attempt creation refuses a content-filter retry binding with."
  @spec code() :: atom()
  def code, do: @code

  @doc """
  Finalizes a request whose first attempt was refused by its binding; the caller
  has already released the candidate's circuit probe.
  """
  @spec finalize_unstarted(SelectedCandidateContext.t()) :: {:error, map()}
  def finalize_unstarted(%SelectedCandidateContext{} = context) do
    finalize(context, %{usage_status: "not_applicable", pre_attempt_phase: PreAttemptRelease.routing_rejected()})
  end

  @doc """
  Releases the candidate's circuit probe and finalizes a request whose retry
  attempt on the same assignment was refused by its binding, after the attempt
  it retries was recorded as a retryable failure.
  """
  @spec finalize_retry(SelectedCandidateContext.t()) :: {:error, map()}
  def finalize_retry(%SelectedCandidateContext{attempt: %Attempt{} = attempt} = context) do
    RouteLifecycle.log_optional_result(
      "release_refused_content_filter_retry_circuit_probe",
      [request_id: context.reserved.request.id],
      DispatchLifecycle.neutral_completion(context)
    )

    finalize(context, %{usage_status: "usage_unknown", released_after_attempt: attempt})
  end

  defp finalize(context, release) do
    attrs = Map.merge(%{response_status_code: 409, last_error_code: Atom.to_string(@code)}, release)

    case AttemptSettlement.finalize_reservation_failure(context.reserved.request, attrs) do
      {:ok, _finalized} -> {:error, refused_resend_error()}
      {:error, gateway_error} -> {:error, gateway_error}
    end
  end

  defp refused_resend_error,
    do: %{status: 409, code: "duplicate_turn", message: "a request with the same turn identity already exists", param: "request"}
end
