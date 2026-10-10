defmodule CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.ConnectionUpgrade do
  @moduledoc false

  alias CodexPooler.Platform.OutboundHTTP

  @type request_caller :: {pid(), reference()} | nil
  # `data` holds the bytes the peer wrote behind a `101` in the same read as the response head, which are the
  # websocket's first bytes (see `finish_connection/6`). The body of any other answer is never kept.
  @type upgrade_response :: %{status: non_neg_integer() | nil, headers: Mint.Types.headers(), data: binary()}
  @connect_ready_tag :upstream_websocket_connect_ready
  @connect_result_tag :upstream_websocket_connect_result
  @connect_task_shutdown_timeout_ms 1_000

  @spec connect_state(map(), term(), binary(), Mint.Types.headers(), map(), request_caller()) ::
          {:ok, map()} | {:error, term(), map()}
  def connect_state(state, key, url, headers, timeouts, request_caller) do
    if request_caller_down?(request_caller) do
      {:error, :client_disconnected, state}
    else
      do_connect_state(state, key, url, headers, timeouts, request_caller)
    end
  end

  defp do_connect_state(state, key, url, headers, timeouts, request_caller) do
    with {:ok, target} <- websocket_target(url),
         {:ok, conn} <- connect_websocket(target, timeouts, request_caller),
         {:ok, conn, ref} <- upgrade_websocket(conn, target, headers, request_caller),
         {:ok, conn, response_headers, upgrade_data} <- await_upgrade(conn, ref, timeouts, request_caller) do
      finish_connection(state, key, conn, ref, response_headers, upgrade_data)
    else
      {:error, conn, :client_disconnected} ->
        {:ok, _conn} = Mint.HTTP.close(conn)
        {:error, :client_disconnected, state}

      {:error, reason} ->
        {:error, reason, state}

      {:error, conn, reason} ->
        {:error, reason, Map.put(state, :conn, conn)}
    end
  end

  defp connect_websocket(%{connect_scheme: :http} = target, timeouts, request_caller) do
    connect_in_task(
      target,
      timeouts,
      request_caller,
      OutboundHTTP.proxy_options_for_url(target.uri,
        transport_opts: [timeout: timeouts.connect_timeout_ms]
      )
    )
  end

  defp connect_websocket(%{connect_scheme: :https} = target, timeouts, request_caller) do
    proxy_options =
      OutboundHTTP.proxy_options_for_url(target.uri,
        transport_opts: [timeout: timeouts.connect_timeout_ms]
      )

    if proxy_options == [] do
      raw_target = %{target | connect_scheme: :http}

      with {:ok, raw_conn} <- connect_in_task(raw_target, timeouts, request_caller, []) do
        upgrade_tls_connection(raw_conn, target, timeouts, request_caller)
      end
    else
      connect_in_task(target, timeouts, request_caller, proxy_options)
    end
  end

  defp connect_in_task(target, timeouts, request_caller, proxy_options) do
    parent = self()

    {:ok, connect_pid} =
      Task.start(fn ->
        parent_monitor = Process.monitor(parent)

        connect_options =
          [
            protocols: [:http1],
            transport_opts: websocket_transport_opts(target, timeouts)
          ] ++ proxy_options

        result =
          Mint.HTTP.connect(target.connect_scheme, target.host, target.port, connect_options)

        send(parent, {@connect_ready_tag, self(), result})

        receive do
          {:accept_upstream_websocket_connection, ^parent} ->
            Process.demonitor(parent_monitor, [:flush])
            result = transfer_connection(result, parent)
            send(parent, {@connect_result_tag, self(), result})

          {:reject_upstream_websocket_connection, ^parent} ->
            Process.demonitor(parent_monitor, [:flush])
            close_connection(result)

          {:DOWN, ^parent_monitor, :process, ^parent, _reason} ->
            close_connection(result)
        end
      end)

    connect_monitor = Process.monitor(connect_pid)
    await_connect(connect_pid, connect_monitor, timeouts, request_caller)
  end

  defp upgrade_tls_connection(raw_conn, target, timeouts, request_caller) do
    watcher = start_tls_upgrade_caller_watcher(raw_conn, request_caller)

    result =
      Mint.HTTP.upgrade(
        :http,
        Mint.HTTP.get_socket(raw_conn),
        :https,
        target.host,
        target.port,
        protocols: [:http1],
        transport_opts: websocket_transport_opts(target, timeouts)
      )

    stop_tls_upgrade_caller_watcher(watcher)
    caller_down? = request_caller_down?(request_caller)

    case result do
      {:ok, conn} when caller_down? ->
        {:ok, _conn} = Mint.HTTP.close(conn)
        {:error, :client_disconnected}

      {:ok, conn} ->
        {:ok, conn}

      {:error, reason} ->
        {:ok, _conn} = Mint.HTTP.close(raw_conn)
        if caller_down?, do: {:error, :client_disconnected}, else: {:error, reason}
    end
  end

  defp start_tls_upgrade_caller_watcher(
         raw_conn,
         {request_caller_pid, _request_caller_monitor}
       )
       when is_pid(request_caller_pid) do
    parent = self()

    spawn(fn ->
      caller_monitor = Process.monitor(request_caller_pid)
      parent_monitor = Process.monitor(parent)

      receive do
        :upstream_websocket_tls_upgrade_complete ->
          Process.demonitor(caller_monitor, [:flush])
          Process.demonitor(parent_monitor, [:flush])

        {:DOWN, ^caller_monitor, :process, ^request_caller_pid, _reason} ->
          Mint.HTTP.close(raw_conn)

        {:DOWN, ^parent_monitor, :process, ^parent, _reason} ->
          Mint.HTTP.close(raw_conn)
      end
    end)
  end

  defp start_tls_upgrade_caller_watcher(_raw_conn, _request_caller), do: nil

  defp stop_tls_upgrade_caller_watcher(watcher) when is_pid(watcher) do
    send(watcher, :upstream_websocket_tls_upgrade_complete)
    :ok
  end

  defp stop_tls_upgrade_caller_watcher(_watcher), do: :ok

  defp transfer_connection({:ok, conn}, parent) do
    case Mint.HTTP.controlling_process(conn, parent) do
      {:ok, conn} ->
        {:ok, conn}

      {:error, reason} ->
        {:ok, _conn} = Mint.HTTP.close(conn)
        {:error, reason}
    end
  end

  defp transfer_connection({:error, reason}, _parent), do: {:error, reason}

  defp close_connection({:ok, conn}) do
    {:ok, _conn} = Mint.HTTP.close(conn)
    :ok
  end

  defp close_connection({:error, _reason}), do: :ok

  defp await_connect(connect_pid, connect_monitor, timeouts, request_caller) do
    request_caller_pid = request_caller_pid(request_caller)
    request_caller_monitor = request_caller_monitor(request_caller)

    receive do
      {:DOWN, ^request_caller_monitor, :process, ^request_caller_pid, _reason}
      when is_reference(request_caller_monitor) and is_pid(request_caller_pid) ->
        stop_connect_task(connect_pid, connect_monitor)
        {:error, :client_disconnected}

      {@connect_ready_tag, ^connect_pid, {:ok, _conn}} ->
        if request_caller_down?(request_caller) do
          stop_connect_task(connect_pid, connect_monitor)
          {:error, :client_disconnected}
        else
          send(connect_pid, {:accept_upstream_websocket_connection, self()})
          await_transferred_connection(connect_pid, connect_monitor, request_caller)
        end

      {@connect_ready_tag, ^connect_pid, {:error, reason}} ->
        send(connect_pid, {:reject_upstream_websocket_connection, self()})
        Process.demonitor(connect_monitor, [:flush])
        {:error, reason}

      {:DOWN, ^connect_monitor, :process, ^connect_pid, _reason} ->
        {:error, :upstream_websocket_connect_task_exited}
    after
      timeouts.connect_timeout_ms ->
        stop_connect_task(connect_pid, connect_monitor)
        {:error, :upstream_websocket_connect_timeout}
    end
  end

  defp await_transferred_connection(connect_pid, connect_monitor, request_caller) do
    request_caller_pid = request_caller_pid(request_caller)
    request_caller_monitor = request_caller_monitor(request_caller)

    receive do
      {:DOWN, ^request_caller_monitor, :process, ^request_caller_pid, _reason}
      when is_reference(request_caller_monitor) and is_pid(request_caller_pid) ->
        close_transferred_connection(connect_pid, connect_monitor)
        {:error, :client_disconnected}

      {@connect_result_tag, ^connect_pid, {:ok, conn}} ->
        Process.demonitor(connect_monitor, [:flush])

        if request_caller_down?(request_caller) do
          {:ok, _conn} = Mint.HTTP.close(conn)
          {:error, :client_disconnected}
        else
          {:ok, conn}
        end

      {@connect_result_tag, ^connect_pid, {:error, reason}} ->
        Process.demonitor(connect_monitor, [:flush])

        if request_caller_down?(request_caller) do
          {:error, :client_disconnected}
        else
          {:error, reason}
        end

      {:DOWN, ^connect_monitor, :process, ^connect_pid, _reason} ->
        {:error, :upstream_websocket_connect_task_exited}
    end
  end

  defp close_transferred_connection(connect_pid, connect_monitor) do
    receive do
      {@connect_result_tag, ^connect_pid, {:ok, conn}} ->
        {:ok, _conn} = Mint.HTTP.close(conn)
        Process.demonitor(connect_monitor, [:flush])

      {@connect_result_tag, ^connect_pid, {:error, _reason}} ->
        Process.demonitor(connect_monitor, [:flush])

      {:DOWN, ^connect_monitor, :process, ^connect_pid, _reason} ->
        :ok
    end
  end

  defp stop_connect_task(connect_pid, connect_monitor) do
    if Process.alive?(connect_pid), do: Process.exit(connect_pid, :kill)

    receive do
      {:DOWN, ^connect_monitor, :process, ^connect_pid, _reason} -> :ok
    after
      @connect_task_shutdown_timeout_ms -> Process.demonitor(connect_monitor, [:flush])
    end
  end

  defp upgrade_websocket(conn, target, headers, request_caller) do
    if request_caller_down?(request_caller) do
      {:error, conn, :client_disconnected}
    else
      case Mint.WebSocket.upgrade(target.ws_scheme, conn, target.path, headers) do
        {:ok, conn, ref} -> {:ok, conn, ref}
        {:error, conn, reason} -> {:error, conn, reason}
      end
    end
  end

  @spec new_websocket(
          Mint.HTTP.t(),
          Mint.Types.request_ref(),
          Mint.Types.headers()
        ) ::
          {:ok, Mint.HTTP.t(), Mint.WebSocket.t()} | {:error, Mint.HTTP.t(), term()}
  # Dialyzer reads `Mint.WebSocket.new/4` as always failing: Mint's contract returns the opaque `Mint.WebSocket.t()`,
  # which does not meet the plain struct its success typing builds, so only the error tuple survives. The success
  # clause is the one every established upstream websocket takes, and Mint does refuse a mismatched nonce or
  # extension, so both clauses stay and the no-match warning is silenced.
  @dialyzer {:no_match, new_websocket: 3}
  defp new_websocket(conn, ref, response_headers) do
    case Mint.WebSocket.new(conn, ref, 101, response_headers) do
      {:ok, conn, websocket} -> {:ok, conn, websocket}
      {:error, conn, reason} -> {:error, conn, reason}
    end
  end

  # Dialyzer inherits new_websocket/3's error-only reading, so it takes the success clause below, and the two helpers
  # only that clause calls, for unreachable code; every established upstream websocket runs them.
  #
  # The state carries the frames decoded from `upgrade_data` under `:upgrade_frames` (absent when there are none);
  # the session takes them out right after the connection is established and settles them before it writes the
  # request (findings#304).
  @dialyzer {:no_match, finish_connection: 6}
  @dialyzer {:no_unused, [decode_upgrade_data: 2, put_upgrade_frames: 2]}
  defp finish_connection(state, key, conn, ref, response_headers, upgrade_data) do
    case new_websocket(conn, ref, response_headers) do
      {:ok, conn, websocket} ->
        {websocket, upgrade_frames} = decode_upgrade_data(websocket, upgrade_data)

        connection_state = %{
          key: key,
          conn: conn,
          ref: ref,
          websocket: websocket,
          headers: response_headers,
          connection_started_at_monotonic_ms: System.monotonic_time(:millisecond),
          connection_request_count: 0,
          last_request_completed_at_monotonic_ms: nil
        }

        state =
          state
          |> Map.merge(connection_state)
          |> put_upgrade_frames(upgrade_frames)
          |> Map.update!(:generation, &(&1 + 1))
          |> Map.delete(:reconnect_pending?)

        {:ok, state}

      {:error, conn, reason} ->
        {:error, reason, Map.put(state, :conn, conn)}
    end
  end

  # Mint reports the bytes that follow the `101` in the same read as a data part of the upgrade response, and
  # `Mint.WebSocket` hands data after the upgrade response to the websocket. The new websocket decodes them here,
  # in order, so the decoder state carries over: a frame that read cut completes with the next read. A decoder
  # failure stays in-band, the way Mint reports a frame error, so the session has one place that answers it.
  defp decode_upgrade_data(websocket, <<>>), do: {websocket, []}

  defp decode_upgrade_data(websocket, data) when is_binary(data) do
    case Mint.WebSocket.decode(websocket, data) do
      {:ok, websocket, frames} -> {websocket, frames}
      {:error, websocket, reason} -> {websocket, [{:error, reason}]}
    end
  end

  defp put_upgrade_frames(state, []), do: Map.delete(state, :upgrade_frames)
  defp put_upgrade_frames(state, frames), do: Map.put(state, :upgrade_frames, frames)

  defp websocket_target(url) do
    uri = URI.parse(url)

    with scheme when scheme in ["http", "https"] <- uri.scheme,
         host when is_binary(host) and host != "" <- uri.host do
      connect_scheme = if scheme == "https", do: :https, else: :http
      ws_scheme = if scheme == "https", do: :wss, else: :ws
      port = uri.port || if(scheme == "https", do: 443, else: 80)
      path = websocket_path(uri)

      {:ok,
       %{
         connect_scheme: connect_scheme,
         ws_scheme: ws_scheme,
         host: host,
         port: port,
         path: path,
         uri: uri
       }}
    else
      _invalid -> {:error, :invalid_upstream_websocket_url}
    end
  end

  defp websocket_path(uri) do
    path = uri.path || "/"

    case uri.query do
      nil -> path
      query -> path <> "?" <> query
    end
  end

  defp websocket_transport_opts(%{connect_scheme: :https, host: host}, timeouts) do
    [timeout: timeouts.connect_timeout_ms, server_name_indication: String.to_charlist(host)]
  end

  defp websocket_transport_opts(_target, timeouts), do: [timeout: timeouts.connect_timeout_ms]

  # `timeouts` may carry a test-only `:upgrade_clock`, a zero-arity function
  # returning monotonic milliseconds, read wherever the upgrade deadline is set
  # or checked; runtime code never passes it and the monotonic clock is used
  # (findings#206 row 206-320).
  defp await_upgrade(conn, ref, timeouts, request_caller) do
    clock = Map.get(timeouts, :upgrade_clock, &monotonic_ms/0)
    deadline = {clock.() + timeouts.connect_timeout_ms, clock}
    await_upgrade(conn, ref, deadline, request_caller, %{status: nil, headers: [], data: <<>>})
  end

  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  defp await_upgrade(conn, ref, {deadline_ms, clock} = deadline, request_caller, response) do
    socket = mint_socket(conn)
    request_caller_pid = request_caller_pid(request_caller)
    request_caller_monitor = request_caller_monitor(request_caller)

    receive do
      {:DOWN, ^request_caller_monitor, :process, ^request_caller_pid, _reason}
      when is_reference(request_caller_monitor) and is_pid(request_caller_pid) ->
        {:error, conn, :client_disconnected}

      {:tcp, ^socket, _data} = message ->
        handle_upgrade_message(conn, ref, deadline, request_caller, response, message)

      {:ssl, ^socket, _data} = message ->
        handle_upgrade_message(conn, ref, deadline, request_caller, response, message)

      {:tcp_closed, ^socket} = message ->
        handle_upgrade_message(conn, ref, deadline, request_caller, response, message)

      {:ssl_closed, ^socket} = message ->
        handle_upgrade_message(conn, ref, deadline, request_caller, response, message)

      {:tcp_error, ^socket, _reason} = message ->
        handle_upgrade_message(conn, ref, deadline, request_caller, response, message)

      {:ssl_error, ^socket, _reason} = message ->
        handle_upgrade_message(conn, ref, deadline, request_caller, response, message)
    after
      max(deadline_ms - clock.(), 0) ->
        {:error, conn, :upstream_websocket_upgrade_timeout}
    end
  end

  defp handle_upgrade_message(conn, ref, deadline, request_caller, response, message) do
    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        upgrade_response(conn, ref, responses, deadline, request_caller, response)

      {:error, conn, reason, _responses} ->
        {:error, conn, reason}

      :unknown ->
        await_upgrade(conn, ref, deadline, request_caller, response)
    end
  end

  defp upgrade_response(conn, ref, responses, deadline, request_caller, response) do
    case fold_upgrade_responses(responses, ref, response) do
      {:done, %{status: 101, headers: headers, data: data}} ->
        {:ok, conn, headers, data}

      {:done, %{status: status, headers: headers}} when is_integer(status) ->
        {:error, conn, {:websocket_upgrade_failed, status, headers}}

      {:done, _response} ->
        {:error, conn, :invalid_upstream_websocket_upgrade}

      {:continue, response} ->
        await_upgrade(conn, ref, deadline, request_caller, response)
    end
  end

  @spec fold_upgrade_responses(
          [Mint.Types.response()],
          Mint.Types.request_ref(),
          upgrade_response()
        ) ::
          {:continue, upgrade_response()} | {:done, upgrade_response()}
  defp fold_upgrade_responses(responses, ref, response) do
    {response, completed_response, open?} =
      Enum.reduce(responses, {response, nil, is_integer(response.status)}, fn
        {:status, ^ref, status}, {_response, completed_response, _open?}
        when is_integer(status) and status >= 0 ->
          {%{status: status, headers: [], data: <<>>}, completed_response, true}

        {:headers, ^ref, headers}, {response, completed_response, true}
        when is_list(headers) ->
          headers =
            Enum.filter(headers, fn
              {name, value} when is_binary(name) and is_binary(value) -> true
              _header -> false
            end)

          {%{response | headers: response.headers ++ headers}, completed_response, true}

        # Mint returns the bytes behind a `101` as one data part of the response, ahead of its `:done`:
        # `Mint.WebSocket` documents data after the upgrade response as belonging to the websocket, so they are
        # kept for it (findings#304). The body of any other answer is dropped as before, so a refusal can never
        # grow this buffer.
        {:data, ^ref, data}, {%{status: 101} = response, completed_response, true}
        when is_binary(data) ->
          {%{response | data: response.data <> data}, completed_response, true}

        {:done, ^ref}, {response, _completed_response, true} ->
          {response, response, false}

        _part, accumulator ->
          accumulator
      end)

    cond do
      open? -> {:continue, response}
      completed_response -> {:done, completed_response}
      true -> {:continue, response}
    end
  end

  defp request_caller_down?({request_caller_pid, request_caller_monitor})
       when is_pid(request_caller_pid) and is_reference(request_caller_monitor) do
    receive do
      {:DOWN, ^request_caller_monitor, :process, ^request_caller_pid, _reason} -> true
    after
      0 -> false
    end
  end

  defp request_caller_down?(_request_caller), do: false

  defp request_caller_pid({request_caller_pid, _request_caller_monitor}), do: request_caller_pid
  defp request_caller_pid(_request_caller), do: nil

  defp request_caller_monitor({_request_caller_pid, request_caller_monitor}),
    do: request_caller_monitor

  defp request_caller_monitor(_request_caller), do: nil

  defp mint_socket(conn), do: Mint.HTTP.get_socket(conn)
end
