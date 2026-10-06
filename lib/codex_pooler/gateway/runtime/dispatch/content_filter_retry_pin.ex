defmodule CodexPooler.Gateway.Runtime.Dispatch.ContentFilterRetryPin do
  @moduledoc false

  # A verified guided content-filter retry dispatches only on the account that
  # served the content-filtered attempt: reservation records that source as the
  # retry's binding and attempt creation refuses any other assignment
  # (`NativeContentFilterRetry.dispatch_allowed?/2`). Routing used to ignore the
  # binding, so a session affinity moved to another account between the
  # terminal and the retry sent the retry there, where dispatch refused it
  # although the bound account could serve it (findings#318).
  #
  # The retry has to stay on that account because the provider reads a
  # reasoning item's `encrypted_content` only on the account that produced it.
  # Replayed on another account the item is accepted and dropped: the request
  # is answered and billed exactly as if the item were absent, the model
  # reasons again, and nothing reports the loss (a tampered ciphertext is
  # refused `invalid_encrypted_content`, so the item is verified, not ignored).
  # The guided retry carries the reasoning its predecessor completed before the
  # filter.
  #
  # The pin narrows the candidates route filtering admitted to the bound one and
  # never adds it back. A bound account route filtering excluded (an open
  # circuit, spent quota) refuses the retry with the retryable `503
  # no_eligible_backend` before any attempt: its reservation is released and it
  # gives up its claim and link (`NativeContentFilterRetry.release_refused_retry/1`),
  # so the client's next retry, sent as the released client sends one after a
  # 503 (Codex rust-v0.160.1 `retry_delay`, `Retry-After` honoured), chains onto
  # the content-filtered request again and reaches the account once it is
  # eligible. No failure moves a bound retry to another candidate (`Dispatch`)
  # or partition (`PartitionFallback.available?/1`). A request without a valid
  # binding routes as before.

  alias CodexPooler.Accounting.{NativeContentFilterRetry, PreAttemptRelease, Request}
  alias CodexPooler.Gateway.Routing.{BridgeRing, CircuitRetryAfter}
  alias CodexPooler.Gateway.Runtime.Dispatch.RouteState
  alias CodexPooler.Gateway.Runtime.Finalization.AttemptSettlement
  alias CodexPooler.Gateway.Transports.Websocket.DiagnosticTaxonomy

  require Logger

  @doc "True for a reserved request a valid content-filter retry binding pins to one account."
  @spec bound?(Request.t() | term()) :: boolean()
  def bound?(request), do: not is_nil(NativeContentFilterRetry.bound_assignment(request))

  @doc """
  The candidates a reserved request is routed over: the bound candidate alone
  when the request carries a valid binding and route filtering admitted that
  candidate, every candidate when it carries none, and `:unavailable` when the
  bound candidate is not among them.
  """
  @spec candidates(Request.t() | term(), [BridgeRing.candidate()]) :: {:ok, [BridgeRing.candidate()]} | :unavailable
  def candidates(request, candidates) when is_list(candidates) do
    case NativeContentFilterRetry.bound_assignment(request) do
      nil ->
        {:ok, candidates}

      bound ->
        case Enum.filter(candidates, &bound_candidate?(&1, bound)) do
          [] -> :unavailable
          pinned -> {:ok, pinned}
        end
    end
  end

  @doc """
  Finalizes a bound request whose account route filtering excluded, before any
  attempt, and answers the retryable `503 no_eligible_backend`, with the bound
  account's circuit `Retry-After` when an open circuit excluded it.
  """
  @spec refuse(map()) :: {:error, map()}
  def refuse(%{auth: auth, model: model, reserved: %{request: %Request{} = request}, request_options: request_options, route_state: route_state}) do
    attrs = %{response_status_code: 503, last_error_code: "no_eligible_backend", usage_status: "not_applicable", pre_attempt_phase: PreAttemptRelease.routing_rejected()}

    case AttemptSettlement.finalize_reservation_failure(request, attrs) do
      {:ok, _finalized} ->
        release_claim(request)
        bound = NativeContentFilterRetry.bound_assignment(request)
        excluded = route_state |> RouteState.route_filter_candidates() |> Enum.filter(&bound_candidate?(&1, bound)) |> Enum.take(1)

        {:error, %{status: 503, code: "no_eligible_backend", message: "no healthy eligible backend is currently available", param: "model"}}
        |> CircuitRetryAfter.put_current(auth, model, excluded, request_options.transport.route_class)

      {:error, gateway_error} ->
        {:error, gateway_error}
    end
  end

  # A release that fails leaves the refused row a terminal predecessor: the
  # client's next retry meets `409 duplicate_turn`, as before the release.
  defp release_claim(request) do
    case NativeContentFilterRetry.release_refused_retry(request) do
      :ok -> :ok
      {:error, reason} -> Logger.warning("content-filter retry claim release failed request_id=#{DiagnosticTaxonomy.safe_correlator(request.id)} reason_code=#{DiagnosticTaxonomy.reason_code(reason) || "unknown"}")
    end
  end

  defp bound_candidate?({assignment, identity}, {assignment_id, identity_id}), do: assignment.id == assignment_id and identity.id == identity_id
end
