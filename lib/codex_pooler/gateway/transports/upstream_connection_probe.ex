defmodule CodexPooler.Gateway.Transports.UpstreamConnectionProbe do
  @moduledoc false

  # Whether an upstream HTTP request opened a connection (`fresh`) or took a pooled one (`reused`): the difference
  # between a provider that held a stream and a stale socket that had to be replaced. Finch reports it only through
  # telemetry, emitted in the process that runs the request (an `into: :self` request runs in a process Finch spawns),
  # where the owner's request cannot be seen. So the owner puts a probe `{pid, ref}` in the Finch request's private
  # data, the handler that sees the request leaves it in that process's dictionary for the connect or reuse event that
  # follows in the same process, and the answer is sent back to the owner. Messages from one process arrive in the
  # order they were sent, and the answer is sent before Finch sends the response, so it is in the owner's mailbox when
  # the response headers arrive. `observe/1` drains it right after the request returned and again on every exit, so
  # nothing is left in a long-lived connection process's mailbox.
  #
  # A request that carries no probe costs one map lookup per Finch queue event; no handler reads a request body, a
  # header or a URL, and the only value that leaves a request is one of two words.

  @queue_event [:finch, :queue, :stop]
  @connect_event [:finch, :connect, :start]
  @reused_event [:finch, :reused_connection]
  @events [@queue_event, @connect_event, @reused_event]
  @handler_id {__MODULE__, :connection}
  @finch_private_key :codex_pooler_upstream_connection_probe
  @pending_key {__MODULE__, :pending}
  @message :codex_pooler_upstream_connection
  @response_private_key :codex_pooler_upstream_connection
  @connections ~w(fresh reused)

  @type connection :: String.t()

  @doc "Every value the probe can answer."
  @spec connections() :: [connection()]
  def connections, do: @connections

  @spec attach() :: :ok
  def attach do
    case :telemetry.attach_many(@handler_id, @events, &__MODULE__.handle_event/4, :ok) do
      :ok -> :ok
      {:error, :already_exists} -> :ok
    end
  end

  @doc """
  Runs `fun` with the `Req` option that carries a probe for the request it makes, and answers its result together with
  the connection class, or `nil` when the request never reached a connection or the pool is not an HTTP/1 one.
  """
  @spec observe((keyword() -> result)) :: {result, connection() | nil} when result: term()
  def observe(fun) when is_function(fun, 1) do
    ref = make_ref()

    try do
      result = fun.(finch_private: %{@finch_private_key => {self(), ref}})
      {result, take(ref)}
    after
      _flushed = take(ref)
    end
  end

  @doc "Stores the class on a request result's response; anything outside the vocabulary and any error passes through."
  @spec put_connection(term(), term()) :: term()
  def put_connection({:ok, %Req.Response{} = response}, class), do: {:ok, put_connection(response, class)}

  def put_connection(%Req.Response{} = response, class) when class in @connections,
    do: Req.Response.put_private(response, @response_private_key, class)

  def put_connection(result, _class), do: result

  @doc "The class stored on a response by `put_connection/2`, or `nil`."
  @spec connection(Req.Response.t()) :: connection() | nil
  def connection(%Req.Response{} = response) do
    case Req.Response.get_private(response, @response_private_key) do
      class when class in @connections -> class
      _none -> nil
    end
  end

  @doc false
  @spec handle_event([atom()], map(), map(), term()) :: :ok
  def handle_event(@queue_event, _measurements, %{request: %{private: %{@finch_private_key => {pid, ref}}}}, _config)
      when is_pid(pid) and is_reference(ref) do
    _previous = Process.put(@pending_key, {pid, ref})
    :ok
  end

  def handle_event(@connect_event, _measurements, _metadata, _config), do: answer("fresh")
  def handle_event(@reused_event, _measurements, _metadata, _config), do: answer("reused")
  def handle_event(_event, _measurements, _metadata, _config), do: :ok

  defp answer(class) do
    case Process.delete(@pending_key) do
      {pid, ref} -> send(pid, {@message, ref, class})
      nil -> :ok
    end

    :ok
  end

  # Every answer of this probe, oldest first; the last one belongs to the response that was returned.
  defp take(ref, class \\ nil) do
    receive do
      {@message, ^ref, answered} -> take(ref, answered)
    after
      0 -> class
    end
  end
end
