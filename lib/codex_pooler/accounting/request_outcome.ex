defmodule CodexPooler.Accounting.RequestOutcome do
  @moduledoc """
  The outcome class operator surfaces read from a request row, derived from
  `requests.status`, `requests.last_error_code` and bounded interruption metadata
  without changing the recorded status or code.

  A request that failed with `client_disconnected` is presented as a client
  cancellation unless its bounded `downstream_interruption` metadata identifies
  the server's idle timeout. That timeout retains the disconnect code for
  retry and settlement compatibility. Its origin does not establish provider
  fault: the client may have stopped answering pings, or delivery may have
  stalled. The client-activity receipt distinguishes an observed pong from
  unknown responsiveness without storing frames.

  Other Pooler-side cuts retain their existing codes (`owner_drained`,
  `owner_unavailable`, `owner_crashed`, `dead_execution_recovered`,
  `absent_instance_recovered`, `stale_reservation_recovered`). The class never
  reads `response_status_code`: those share the `499`, and a client disconnect
  after an HTTP stream already answered keeps its `200`.

  Only a `failed` row qualifies. A request whose late answer corrected an
  interrupt to `succeeded` can keep the code, and it is a success.

  `websocket_replay_expired` stays out of the class. It closes a replay that no
  resend claimed, but the replay is armed on any loss of a forwarded socket
  before visible output, including the loss of the node that carried the
  socket, and nothing durable records which loss it was: the attempt reads
  `client_disconnected` either way.

  The class changes presentation and health counting only. The row stays
  `failed`, and accounting, settlement, retries and the wire are unchanged.
  Request logs (row status, filter, detail), the upstream cockpit (request
  health, recent activity), the stats page, the MCP request-log tool and the
  API Key Observatory (the holder's counts, success rate and recent outcomes)
  all read the class from here.
  """

  import Ecto.Query

  @failed "failed"
  @client_cancelled "client_cancelled"
  @client_cancellation_error_codes ~w(client_disconnected)

  @doc "The value surfaces show and filter by for a client cancellation."
  @spec client_cancelled() :: String.t()
  def client_cancelled, do: @client_cancelled

  @doc "The `last_error_code` values that make a failed request a client cancellation."
  @spec client_cancellation_error_codes() :: [String.t()]
  def client_cancellation_error_codes, do: @client_cancellation_error_codes

  @doc "Whether a request row (or a map with its `status` and `last_error_code`) is a client cancellation."
  @spec client_cancelled?(map()) :: boolean()
  def client_cancelled?(%{status: @failed, last_error_code: code, client_cancelled?: cancelled?}) when code in @client_cancellation_error_codes and is_boolean(cancelled?), do: cancelled?

  def client_cancelled?(%{status: status, last_error_code: code} = request),
    do: client_cancelled?(status, code) and not server_idle_timeout?(Map.get(request, :request_metadata))

  def client_cancelled?(_request), do: false

  @spec client_cancelled?(term(), term()) :: boolean()
  def client_cancelled?(@failed, code) when code in @client_cancellation_error_codes, do: true
  def client_cancelled?(_status, _code), do: false

  defp server_idle_timeout?(%{"downstream_interruption" => %{"origin" => "server", "cause" => "idle_timeout"}}), do: true
  defp server_idle_timeout?(_metadata), do: false

  @spec display_status(map()) :: term()
  def display_status(%{status: status} = request), do: if(client_cancelled?(request), do: @client_cancelled, else: status)

  @doc """
  The status a surface shows for a request: `"client_cancelled"` for a client
  cancellation, the recorded status otherwise.
  """
  @spec display_status(term(), term()) :: term()
  def display_status(status, code), do: if(client_cancelled?(status, code), do: @client_cancelled, else: status)

  @doc """
  The query condition of `client_cancelled?/1` on the request bound as
  `binding`. It is never NULL (a NULL `last_error_code` reads as no code), so
  its negation keeps every other row, failed rows without a code included.
  """
  @spec client_cancelled_condition(atom()) :: Ecto.Query.dynamic_expr()
  def client_cancelled_condition(binding \\ :request) when is_atom(binding) do
    dynamic([{^binding, request}], request.status == "failed" and coalesce(request.last_error_code, "") in @client_cancellation_error_codes and not (fragment("COALESCE(?->'downstream_interruption'->>'origin', '')", request.request_metadata) == "server" and fragment("COALESCE(?->'downstream_interruption'->>'cause', '')", request.request_metadata) == "idle_timeout"))
  end

  @doc """
  Every row `client_cancelled_condition/1` leaves out, failed rows without a
  code included. A dynamic interpolates only at the top of a `where`, so a
  caller that excludes the class takes this one whole.
  """
  @spec not_client_cancelled_condition(atom()) :: Ecto.Query.dynamic_expr()
  def not_client_cancelled_condition(binding \\ :request) when is_atom(binding),
    do: dynamic(not (^client_cancelled_condition(binding)))
end
