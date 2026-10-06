defmodule CodexPoolerWeb.Runtime.BackendCodexWebsocketSupport do
  @moduledoc false

  # Helpers shared by more than one family under
  # test/codex_pooler_web/controllers/runtime/backend_codex_websocket/.

  import Ecto.Query
  import ExUnit.Assertions
  import ExUnit.CaptureLog
  import CodexPoolerWeb.Runtime.BackendCodexTestSupport

  alias CodexPooler.Accounting.{Attempt, Request, RequestLogs}
  alias CodexPooler.Accounts.Scope
  alias CodexPooler.Audit.AuditEvent
  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway, as: RuntimeGateway
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Persistence.{BridgeDemotion, CodexSession, CodexTurn, RoutingCircuitState}
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Websocket.Adapter
  alias CodexPooler.Pools
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.IdentityLifecycle
  alias CodexPooler.Upstreams.Quota.Windows, as: QuotaWindows
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.WebsocketCleanupFence

  # Detection budget for a server-side connection teardown the test only
  # observes, never a scenario timeout.
  @connection_shutdown_timeout_ms 15_000
  # Failure-detection budget for the polling helpers below: each returns as
  # soon as the awaited row or state exists, so only a missing one spends it.
  @detection_timeout_ms 15_000

  def strict_native_request(connection_ordinal, respond) do
    FakeUpstream.expect_request(
      method: "WEBSOCKET",
      path: "/backend-api/codex/responses",
      websocket_connection_ordinal: connection_ordinal,
      json: [valid: true, equals: %{"type" => "response.create"}],
      respond: respond
    )
  end

  def strict_native_response(response_id, connection_ordinal, input_tokens, output_tokens) do
    FakeUpstream.expect_request(
      method: "WEBSOCKET",
      path: "/backend-api/codex/responses",
      websocket_connection_ordinal: connection_ordinal,
      json: [valid: true, equals: %{"type" => "response.create"}],
      respond:
        FakeUpstream.websocket_text_frames([
          CodexPooler.JSON.encode!(%{
            "id" => response_id,
            "object" => "response",
            "usage" => %{
              "input_tokens" => input_tokens,
              "output_tokens" => output_tokens,
              "total_tokens" => input_tokens + output_tokens
            }
          })
        ])
    )
  end

  def with_info_log(fun) do
    previous_logger_level = Logger.level()
    # Also on_exit: a linked crash or the ExUnit timeout kills the test before `after` runs.
    ExUnit.Callbacks.on_exit(fn -> Logger.configure(level: previous_logger_level) end)
    Logger.configure(level: :info)

    try do
      with_log([level: :info], fun)
    after
      Logger.configure(level: previous_logger_level)
    end
  end

  def capture_native_turn_warning(fun) when is_function(fun, 0) do
    ExUnit.CaptureLog.with_log([level: :warning], fun)
  end

  def assert_native_turn_warnings(logs, expected_count) do
    assert length(Regex.scan(~r/websocket native turn failed/, logs)) == expected_count
  end

  def websocket_auth_refresh_payload(setup, marker) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.create",
      "model" => setup.model.exposed_model_id,
      "input" => native_text_input("websocket auth refresh fixture #{marker}"),
      "stream" => true,
      "generate" => true
    })
  end

  # A websocket handshake the provider answers 401 with its authorization
  # error header, held before the headers until the test sends
  # `{:fake_upstream_release_timeout, release_ref}`, so the identity can change
  # after routing and before the refresh reads it (findings#325).
  def held_websocket_handshake_401(notify, release_ref) do
    FakeUpstream.expect_request(
      method: "GET",
      respond:
        FakeUpstream.websocket_upgrade_error(
          %{"error" => %{"code" => "invalid_api_key"}},
          status: 401,
          headers: [{"x-openai-authorization-error", "invalid_api_key"}],
          notify: notify,
          release_ref: release_ref
        )
    )
  end

  # The setup's model served by two candidates in the given serving mode. The
  # second carries an ordering demotion, so the first stays preferred while the
  # second remains eligible for failover (findings#325).
  def websocket_failover_candidates!(first_upstream, second_upstream, mode) do
    setup = gateway_setup(first_upstream)
    second = gateway_upstream(setup.pool, second_upstream, "upstream-token-ws-failover-second", [])
    prime_routing_quota!(second.identity)
    setup = %{setup | model: put_model_source_assignments!(setup.model, [setup.assignment, second.assignment])}
    _revision = set_model_serving_mode!(model_serving_scope(), setup, mode)
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    Repo.insert!(%BridgeDemotion{
      pool_id: setup.pool.id,
      api_key_id: setup.api_key.id,
      model_identifier: setup.model.exposed_model_id,
      pool_upstream_assignment_id: second.assignment.id,
      upstream_identity_id: second.identity.id,
      reason_code: "upstream_5xx",
      status: "active",
      demoted_until: DateTime.add(now, 600, :second),
      attempt_count: 1,
      metadata: %{},
      created_at: now,
      updated_at: now
    })

    {setup, second}
  end

  # The assignment's recorded route-circuit failures, `{reason_code, failure_count}`.
  def route_circuit_failures(assignment_id) do
    Repo.all(
      from(c in RoutingCircuitState,
        where: c.pool_upstream_assignment_id == ^assignment_id and c.failure_count > 0,
        select: {c.reason_code, c.failure_count}
      )
    )
  end

  # Reads a public websocket connection's frames up to the turn's terminal
  # (`response.completed`, `response.failed` or `error`): the connection, the
  # types seen before it in order, and the decoded terminal.
  def receive_public_websocket_until_terminal(conn, websocket, ref, seen_types) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal
      when type in ["response.completed", "response.failed", "error"] ->
        {conn, websocket, Enum.reverse(seen_types), terminal}

      %{"type" => type} ->
        receive_public_websocket_until_terminal(conn, websocket, ref, [type | seen_types])
    end
  end

  # The provider refresh a scenario expects after a 401, if the refresh reaches
  # the provider at all: a `{:provider, status, body}` answer, or none when the
  # identity state decides the refresh (`put_refresh_state!/2`).
  def provider_refresh({:provider, status, body}),
    do: [FakeUpstream.expect_request(method: "POST", path: "/oauth/token", respond: FakeUpstream.json_response(body, status))]

  def provider_refresh(_identity_state), do: []

  # The refresh reads the identity as it is when the 401 lands: paused, it is
  # a no-op; marked refreshing by another caller, this request is a follower.
  def put_refresh_state!(_identity, {:provider, _status, _body}), do: :ok

  def put_refresh_state!(identity, :identity_paused) do
    assert {:ok, _identity} = IdentityLifecycle.update_upstream_identity(identity, %{status: "paused"})
    :ok
  end

  def put_refresh_state!(identity, :identity_refreshing) do
    metadata = Map.put(identity.metadata || %{}, "token_refresh", active_token_refresh_metadata())
    assert {:ok, _identity} = IdentityLifecycle.update_upstream_identity(identity, %{status: "refreshing", metadata: metadata})
    :ok
  end

  # Another caller's refresh in flight on the identity, as `TokenRefresh`
  # records it.
  def active_token_refresh_metadata(opts \\ []) do
    %{
      "status" => "refreshing",
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => Keyword.get(opts, :generation, 1),
      "started_at" => DateTime.utc_now() |> DateTime.truncate(:microsecond) |> DateTime.to_iso8601(),
      "trigger_kind" => "test",
      "receive_timeout_ms" => Keyword.get(opts, :receive_timeout_ms, 30_000),
      "stale_after_ms" => Keyword.get(opts, :stale_after_ms, 60_000)
    }
  end

  # The account reconciliation jobs enqueued for an upstream identity: exhausted
  # upstream auth queues one so the operator sees the account's auth state.
  def account_reconciliation_jobs(identity_id) do
    [repo: Repo, worker: CodexPooler.Jobs.AccountReconciliationWorker]
    |> Oban.Testing.all_enqueued()
    |> Enum.filter(&(&1.args["upstream_identity_id"] == identity_id))
  end

  # A half-open `proxy_websocket` circuit on the assignment with its probe slot
  # free, so the next websocket turn routed to it claims the probe.
  def half_open_websocket_circuit!(setup, assignment) do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    %RoutingCircuitState{
      pool_id: setup.pool.id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: assignment.upstream_identity_id,
      model_identifier: setup.model.exposed_model_id,
      route_class: "proxy_websocket",
      status: "half_open",
      reason_code: "test_probe",
      failure_count: 1,
      success_count: 0,
      opened_at: DateTime.add(now, -120, :second),
      half_opened_at: now,
      metadata: %{"probe_in_flight_count" => 0},
      created_at: now,
      updated_at: now
    }
    |> Repo.insert!()
  end

  # A port nothing listens on: bound once to learn its number, closed again,
  # then probed to prove the kernel refuses it before the test relies on that.
  # Binding and closing alone has a TOCTOU window in which a concurrent
  # partition can take the freed port, which would turn a refused-connect test
  # into a hang or an unrelated failure (findings#208). The probe does not
  # close the window, it bounds it: a port that no longer refuses is discarded
  # and a fresh one is drawn, and exhausting the attempts fails loudly with the
  # real cause instead of leaving a mystery timeout.
  @closed_port_attempts 10
  @closed_port_probe_timeout_ms 200

  def reserve_closed_port!(attempts \\ @closed_port_attempts) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, ip: {127, 0, 0, 1}, reuseaddr: true])
    {:ok, port} = :inet.port(listener)
    :ok = :gen_tcp.close(listener)

    case :gen_tcp.connect(
           {127, 0, 0, 1},
           port,
           [:binary, active: false],
           @closed_port_probe_timeout_ms
         ) do
      {:error, :econnrefused} ->
        port

      {:ok, socket} ->
        :ok = :gen_tcp.close(socket)
        retry_closed_port!(attempts, port, :accepted)

      {:error, reason} ->
        retry_closed_port!(attempts, port, reason)
    end
  end

  defp retry_closed_port!(attempts, _port, _reason) when attempts > 1,
    do: reserve_closed_port!(attempts - 1)

  defp retry_closed_port!(_attempts, port, reason) do
    flunk(
      "no refusing loopback port after #{@closed_port_attempts} attempts; " <>
        "last port #{port} answered #{inspect(reason)}"
    )
  end

  def synthetic_access_token(residency) do
    header = Base.url_encode64(CodexPooler.JSON.encode!(%{"alg" => "none"}), padding: false)

    payload =
      Base.url_encode64(
        CodexPooler.JSON.encode!(%{
          "https://api.openai.com/auth" => %{
            "chatgpt_compute_residency" => residency
          }
        }),
        padding: false
      )

    "#{header}.#{payload}.signature"
  end

  def header_values(headers, target_name) do
    for {name, value} <- headers, String.downcase(name) == target_name, do: value
  end

  def assert_websocket_values_not_persisted!(setup, forbidden_values, logs) do
    requests = Repo.all(from(r in Request, where: r.pool_id == ^setup.pool.id))
    request_ids = Enum.map(requests, & &1.id)
    attempts = Repo.all(from(a in Attempt, where: a.request_id in ^request_ids))
    sessions = Repo.all(from(s in CodexSession, where: s.pool_id == ^setup.pool.id))
    session_ids = Enum.map(sessions, & &1.id)
    turns = Repo.all(from(t in CodexTurn, where: t.codex_session_id in ^session_ids))
    audit_events = Repo.all(from(e in AuditEvent))
    request_logs = RequestLogs.list(setup.pool.id, limit: 10)

    durable_text =
      inspect({requests, attempts, sessions, turns, audit_events, request_logs.items})

    for value <- forbidden_values do
      refute durable_text =~ value
      refute logs =~ value
    end
  end

  def pin_session_to_assignment!(session, assignment) do
    session
    |> Ecto.Changeset.change(%{pool_upstream_assignment_id: assignment.id})
    |> Repo.update!()
  end

  def codex_rate_limits_payload(used_percent, reset_at) do
    %{
      "type" => "codex.rate_limits",
      "rate_limits" => %{
        "primary" => %{
          "used_percent" => used_percent,
          "window_minutes" => 300,
          "reset_at" => DateTime.to_unix(reset_at)
        }
      }
    }
  end

  def wait_for_rate_limit_event_window(identity, window_kind, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    identity
    |> QuotaWindows.list_evidence()
    |> Enum.find(&(&1.source == "codex_rate_limit_event" and &1.window_kind == window_kind))
    |> case do
      nil ->
        if System.monotonic_time(:millisecond) < deadline do
          receive do
          after
            10 -> wait_for_rate_limit_event_window(identity, window_kind, deadline)
          end
        else
          flunk("expected codex.rate_limits quota window for #{window_kind}")
        end

      window ->
        window
    end
  end

  def wait_for_rate_limit_event_tasks(deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    case Task.Supervisor.children(CodexPooler.RateLimitEventSupervisor) do
      [] ->
        :ok

      _children ->
        if System.monotonic_time(:millisecond) < deadline do
          receive do
          after
            10 -> wait_for_rate_limit_event_tasks(deadline)
          end
        else
          flunk("expected codex.rate_limits persistence tasks to finish")
        end
    end
  end

  def put_setup_model_source_metadata!(setup, source_metadata) when is_map(source_metadata) do
    source_metadata = Map.put_new(source_metadata, "slug", setup.model.exposed_model_id)

    metadata =
      setup.model.metadata
      |> Map.put("source_assignment_models", %{setup.assignment.id => source_metadata})

    model =
      setup.model
      |> Ecto.Changeset.change(%{metadata: metadata})
      |> Repo.update!()

    %{setup | model: model}
  end

  # An instance owner changes a Pool's serving modes. Inside the sandbox the
  # fixture's owner goes with the test's transaction. A test that commits its
  # rows (`Sandbox.mode(Repo, :auto)`, `Sandbox.unboxed_run/2`) gets its owner
  # from `committed_bootstrap_owner_fixture!/1`, which registers the removal of
  # everything that owner commits: the sandbox fixture committed one there that
  # nothing removed (findings#270 row 270-295).
  def model_serving_scope do
    %{user: owner} =
      if inside_sandbox_transaction?(),
        do: CodexPooler.AccountsFixtures.bootstrap_owner_fixture(),
        else: CodexPooler.AccountsFixtures.committed_bootstrap_owner_fixture!()

    Scope.for_user(owner, ["instance_owner"])
  end

  # The sandbox runs a test's statements inside the transaction it opened for
  # the test, where a savepoint is allowed (and goes with the sandbox's own
  # per-statement savepoint); a committing connection has no transaction
  # block, and PostgreSQL refuses the savepoint there.
  defp inside_sandbox_transaction? do
    case Repo.query("SAVEPOINT codex_pooler_sandbox_probe") do
      {:ok, _result} -> true
      {:error, %Postgrex.Error{postgres: %{code: :no_active_sql_transaction}}} -> false
    end
  end

  def set_model_serving_mode!(scope, setup, mode, expected_revision \\ nil) do
    expected_revision =
      expected_revision ||
        case Pools.model_serving_modes_snapshot(scope, setup.pool) do
          {:ok, snapshot} -> snapshot.revision
          {:error, error} -> flunk("failed to read model serving modes: #{inspect(error)}")
        end

    assert {:ok, result} =
             Pools.update_model_serving_modes(
               scope,
               setup.pool,
               [%{exposed_model_id: setup.model.exposed_model_id, mode: mode}],
               expected_revision
             )

    result.revision
  end

  def await_succeeded_pool_requests!(pool_id, expected_count, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    requests =
      Repo.all(
        from(request in Request,
          where: request.pool_id == ^pool_id,
          order_by: [asc: request.admitted_at]
        )
      )

    if length(requests) == expected_count and
         Enum.all?(requests, &(&1.status == "succeeded")) do
      requests
    else
      if System.monotonic_time(:millisecond) < deadline do
        receive do
        after
          5 -> await_succeeded_pool_requests!(pool_id, expected_count, deadline)
        end
      else
        flunk("expected #{expected_count} succeeded websocket requests, got #{inspect(Enum.map(requests, & &1.status))}")
      end
    end
  end

  def execute_websocket_response(auth, raw_payload, opts, push_frame) do
    request_options = RequestOptions.for_websocket(opts)

    capture_metadata_control? = Map.get(opts, :capture_metadata_control?, false)

    RuntimeGateway.execute_websocket_response(auth, raw_payload, request_options, fn frame ->
      if capture_metadata_control? || not metadata_control_frame?(frame) do
        push_frame.(frame)
      end
    end)
  end

  def stop_websocket_owner_session(codex_session_id) do
    case WebsocketOwnerSession.lookup(codex_session_id) do
      {:ok, owner_pid} ->
        monitor = Process.monitor(owner_pid)

        try do
          GenServer.stop(owner_pid, :shutdown, @connection_shutdown_timeout_ms)
        catch
          :exit, {:noproc, _details} -> :ok
        end

        assert_receive {:DOWN, ^monitor, :process, ^owner_pid, _reason},
                       @connection_shutdown_timeout_ms

      {:error, :owner_unavailable} ->
        :ok
    end

    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(codex_session_id)
  end

  def metadata_control_frame?(%{"type" => "codex.response.metadata"}), do: true

  def metadata_control_frame?({:text, frame}) when is_binary(frame),
    do: metadata_control_frame?(frame)

  def metadata_control_frame?(frame) when is_binary(frame) do
    match?({:ok, %{"type" => "codex.response.metadata"}}, CodexPooler.JSON.decode(frame))
  end

  def metadata_control_frame?(_frame), do: false

  def capture_stream_outcome_telemetry(fun) do
    handler_id = "native-stream-outcome-#{System.unique_integer([:positive])}"
    parent = self()

    # Also on_exit: a linked crash or the ExUnit timeout kills the test before `after` runs.
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)

    :ok =
      :telemetry.attach(
        handler_id,
        [:codex_pooler, :gateway, :stream, :outcome],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:stream_outcome, metadata})
        end,
        nil
      )

    try do
      fun.()
    after
      :telemetry.detach(handler_id)
    end
  end

  @doc """
  A `response.create` shaped like the released client's: its turn metadata
  names the thread and the turn, so a resend of the same turn carries the same
  turn id. Returns `fn input, turn_id, extra -> encoded frame end`.
  """
  def released_client_frame(setup, thread) do
    fn input, turn_id, extra ->
      metadata = %{"session_id" => thread, "thread_id" => thread, "turn_id" => turn_id}

      %{
        "type" => "response.create",
        "model" => setup.model.exposed_model_id,
        "input" => input,
        "stream" => true,
        "generate" => true,
        "client_metadata" => Map.put(metadata, "x-codex-turn-metadata", CodexPooler.JSON.encode!(Map.put(metadata, "request_kind", "turn")))
      }
      |> Map.merge(extra)
      |> CodexPooler.JSON.encode!()
    end
  end

  @doc "A native turn's frames: each output item done, then `response.completed` with usage."
  def completed_response_frames(response_id, output, input_tokens, output_tokens) do
    FakeUpstream.websocket_text_frames(completed_response_events(response_id, output, input_tokens, output_tokens) |> Enum.map(&CodexPooler.JSON.encode!/1))
  end

  @doc "The events of `completed_response_frames/4`, for FakeUpstream modes that take events."
  def completed_response_events(response_id, output, input_tokens, output_tokens) do
    Enum.map(output, &%{"type" => "response.output_item.done", "item" => &1}) ++
      [
        %{
          "type" => "response.completed",
          "response" => %{"id" => response_id, "status" => "completed", "output" => output, "usage" => %{"input_tokens" => input_tokens, "output_tokens" => output_tokens, "total_tokens" => input_tokens + output_tokens}}
        }
      ]
  end

  def native_previous_response_retry_event do
    %{
      "type" => "error",
      "status" => 400,
      "error" => %{
        "type" => "invalid_request_error",
        "code" => "previous_response_not_found",
        "message" => "Previous response was not found. Retrying the full request."
      }
    }
  end

  def receive_socket_push(state, timeout_ms) do
    receive do
      {:codex_response_chunk, task_pid, frame} ->
        result = CodexResponsesSocket.handle_info({:codex_response_chunk, task_pid, frame}, state)

        if StreamProtocol.internal_control_event?(frame) do
          receive_socket_push(state, timeout_ms)
        else
          result
        end
    after
      timeout_ms -> flunk("expected websocket response chunk")
    end
  end

  @native_turn_failure_shapes [
    :lifecycle_cut,
    :provider_error_event,
    :response_failed,
    :pre_visible_close,
    :upgrade_rejected
  ]
  @native_turn_terminal_types ~w(response.completed response.failed response.incomplete error)

  def native_turn_failure_shapes, do: @native_turn_failure_shapes

  # One native turn whose single upstream send fails in the named shape. Every
  # shape is one strict entry, so a hidden retry or replay fails `verify!/1`.
  def strict_native_turn_failure(:lifecycle_cut) do
    # provenance: observed findings issue 124 (lifecycle frames then a transport close; ids synthetic)
    FakeUpstream.strict_sequence([
      strict_native_request(
        1,
        FakeUpstream.websocket_text_frames_then_abrupt_close([
          CodexPooler.JSON.encode!(%{
            "type" => "response.created",
            "response" => %{"id" => "resp_single_terminal_cut", "status" => "in_progress"}
          }),
          CodexPooler.JSON.encode!(%{
            "type" => "response.in_progress",
            "response" => %{"id" => "resp_single_terminal_cut", "status" => "in_progress"}
          })
        ])
      )
    ])
  end

  def strict_native_turn_failure(:provider_error_event) do
    # provenance: synthetic_adversarial (lifecycle frame then a provider type:error event)
    FakeUpstream.strict_sequence([
      strict_native_request(
        1,
        FakeUpstream.websocket_text_frames([
          CodexPooler.JSON.encode!(%{
            "type" => "response.created",
            "response" => %{"id" => "resp_single_terminal_error", "status" => "in_progress"}
          }),
          CodexPooler.JSON.encode!(%{
            "type" => "error",
            "status" => 500,
            "error" => %{
              "type" => "server_error",
              "code" => "server_error",
              "message" => "synthetic"
            }
          })
        ])
      )
    ])
  end

  def strict_native_turn_failure(:response_failed) do
    # provenance: synthetic_adversarial (invented response.failed terminal)
    FakeUpstream.strict_sequence([
      strict_native_request(1, FakeUpstream.websocket_terminal_failure("server_error"))
    ])
  end

  def strict_native_turn_failure(:pre_visible_close) do
    # provenance: synthetic_adversarial (peer close before any frame)
    FakeUpstream.strict_sequence([
      strict_native_request(1, FakeUpstream.websocket_close(code: 1011))
    ])
  end

  def strict_native_turn_failure(:upgrade_rejected) do
    # provenance: synthetic_adversarial (handshake rejected before any frame)
    FakeUpstream.strict_sequence([
      FakeUpstream.expect_request(
        method: "GET",
        path: "/backend-api/codex/responses",
        respond:
          FakeUpstream.websocket_upgrade_error(
            %{"error" => %{"code" => "upgrade_rejected"}},
            status: 403
          )
      )
    ])
  end

  # A provider `type:error` event reaches the native client normalized as its
  # single `response.failed` terminal, never as a relayed error frame.
  def native_turn_failure_terminal_type(:provider_error_event), do: "response.failed"
  def native_turn_failure_terminal_type(:response_failed), do: "response.failed"
  def native_turn_failure_terminal_type(_shape), do: "error"

  @doc """
  Drives one native websocket turn through the socket callbacks and returns
  every client-visible frame the socket pushed for it, decoded, in push order.

  The turn is settled once the response task reported done, the socket tracks
  no task, and, for an owner-forwarded socket, the owner's `:complete` arrived.
  Every producer of a frame for the turn has fired by then (the owner relays
  before it replies, the task reports after the reply), so the closing mailbox
  check proves nothing else is pending without waiting on a timer.
  """
  def collect_native_turn_frames!(state, timeout_ms \\ @connection_shutdown_timeout_ms) do
    seen = %{done?: false, complete?: not Adapter.owner?(state)}
    collect_native_turn_frames(state, [], seen, timeout_ms)
  end

  defp collect_native_turn_frames(state, frames, seen, timeout_ms) do
    if seen.done? and seen.complete? and MapSet.size(state.tasks) == 0 do
      refute_received {:codex_response_chunk, _task_pid, _frame}
      refute_received {:websocket_owner_frame, _correlation_id, _epoch, _owner_turn_id, _payload}
      refute_received {:websocket_owner_frame, _correlation_id, _epoch, _payload}
      refute_received {:codex_response_done, _task_pid, _result}
      {state, Enum.reverse(frames)}
    else
      message = receive_native_turn_message(timeout_ms)
      seen = mark_native_turn_message(seen, message)

      case CodexResponsesSocket.handle_info(message, state) do
        {:push, {:text, frame}, state} ->
          collect_native_turn_frames(
            state,
            [CodexPooler.JSON.decode!(frame) | frames],
            seen,
            timeout_ms
          )

        {:ok, state} ->
          collect_native_turn_frames(state, frames, seen, timeout_ms)

        {:stop, _reason, close_detail, _state} ->
          flunk("native turn closed the socket with #{inspect(close_detail)}")
      end
    end
  end

  defp receive_native_turn_message(timeout_ms) do
    receive do
      {:codex_response_chunk, _task_pid, _frame} = message -> message
      {:websocket_owner_frame, _, _, _, _} = message -> message
      {:websocket_owner_frame, _, _, _} = message -> message
      {:websocket_owner_output_commit_probe, _, _, _, _, _, _} = message -> message
      {:websocket_owner_cleanup_witness, _, _, _, _} = message -> message
      {:websocket_response_activity, _task_pid, _token} = message -> message
      {:codex_response_done, _task_pid, _result} = message -> message
      {:websocket_response_delivery_complete, _task_pid, _token} = message -> message
    after
      timeout_ms -> flunk("expected native websocket turn settlement")
    end
  end

  defp mark_native_turn_message(seen, {:codex_response_done, _task_pid, _result}),
    do: %{seen | done?: true}

  defp mark_native_turn_message(seen, {:websocket_owner_frame, _, _, _, :complete}),
    do: %{seen | complete?: true}

  defp mark_native_turn_message(seen, {:websocket_owner_frame, _, _, :complete}),
    do: %{seen | complete?: true}

  defp mark_native_turn_message(seen, _message), do: seen

  @doc """
  Asserts the turn delivered exactly one client-visible terminal frame, of
  `expected_type`, as its last frame. Failure messages carry only frame types,
  statuses, and error codes.
  """
  def assert_single_native_turn_terminal!(frames, expected_type) do
    summary = Enum.map(frames, &{&1["type"], &1["status"], get_in(&1, ["error", "code"])})
    terminals = Enum.filter(frames, &(&1["type"] in @native_turn_terminal_types))

    assert Enum.map(terminals, & &1["type"]) == [expected_type],
           "expected exactly one #{expected_type} terminal for the turn, pushed: #{inspect(summary)}"

    assert List.last(frames)["type"] == expected_type
    List.last(frames)
  end

  @released_client_terminal_types ["response.completed", "response.failed", "response.incomplete", "error"]

  @doc """
  Opens a public websocket the way the released Codex client (observed
  on the wire for findings#255) opens one: `session-id`, `thread-id`
  and `x-client-request-id` carry the thread, `x-codex-window-id` carries
  `<thread>:<window number>`, and no `x-codex-turn-state` is ever sent on the
  upgrade. The connection is owned by the calling process; open one at a time
  per process so no receive can consume another connection's messages.
  """
  def released_client_connect!(port, authorization, thread, window, extra_headers \\ []) do
    {:ok, conn} = Mint.HTTP.connect(:http, "127.0.0.1", port, protocols: [:http1])

    headers =
      [
        {"authorization", authorization},
        {"session-id", thread},
        {"thread-id", thread},
        {"x-client-request-id", thread},
        {"x-codex-window-id", window}
      ] ++ extra_headers

    {:ok, conn, ref} = Mint.WebSocket.upgrade(:ws, conn, "/backend-api/codex/responses", headers)
    {:ok, conn, status, response_headers} = await_public_websocket_upgrade(conn, ref)
    {conn, websocket} = mint_websocket_new!(conn, ref, status, response_headers)
    %{conn: conn, websocket: websocket, ref: ref, thread: thread, window: window}
  end

  @doc """
  Sends one released-client `response.create` turn and returns
  `{:terminal, client, type}` with the first terminal frame type, or
  `{:closed, client, code, reason}` when the Pooler closes the connection
  instead (an owner refusal closes right after the 101).
  """
  def released_client_turn(client, model_id) do
    turn_id = Ecto.UUID.generate()

    payload =
      CodexPooler.JSON.encode!(%{
        "type" => "response.create",
        "model" => model_id,
        "prompt_cache_key" => client.thread,
        "input" => native_text_input("synthetic released client turn"),
        "stream" => true,
        "client_metadata" => %{
          "session_id" => client.thread,
          "thread_id" => client.thread,
          "turn_id" => turn_id,
          "x-codex-window-id" => client.window,
          "x-codex-turn-metadata" =>
            CodexPooler.JSON.encode!(%{
              "session_id" => client.thread,
              "thread_id" => client.thread,
              "turn_id" => turn_id,
              "window_id" => client.window,
              "request_kind" => "turn"
            })
        }
      })

    {:ok, websocket, data} = Mint.WebSocket.encode(client.websocket, {:text, payload})
    client = %{client | websocket: websocket}

    case Mint.WebSocket.stream_request_body(client.conn, client.ref, data) do
      {:ok, conn} -> receive_released_client_outcome(%{client | conn: conn}, released_client_deadline())
      {:error, conn, _reason} -> receive_released_client_outcome(%{client | conn: conn}, released_client_deadline())
    end
  end

  @doc "The outcome of `released_client_turn/2` without the connection state."
  def released_client_outcome_summary({:terminal, _client, type}), do: {:terminal, type}
  def released_client_outcome_summary({:closed, _client, code, reason}), do: {:closed, code, reason}

  def released_client_close(client) do
    Mint.HTTP.close(client.conn)
    :ok
  end

  defp released_client_deadline, do: System.monotonic_time(:millisecond) + @connection_shutdown_timeout_ms

  defp receive_released_client_outcome(client, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    message = receive_mint_socket_message!(client.conn, remaining, "timed out waiting for a released-client websocket outcome")

    case Mint.WebSocket.stream(client.conn, message) do
      {:ok, conn, responses} ->
        released_client_responses(%{client | conn: conn}, responses, deadline)

      {:error, conn, _reason, responses} ->
        case released_client_responses(%{client | conn: conn}, responses, :no_wait) do
          :continue -> {:closed, %{client | conn: conn}, nil, "transport_closed"}
          outcome -> outcome
        end

      :unknown ->
        receive_released_client_outcome(client, deadline)
    end
  end

  defp released_client_responses(client, responses, deadline) do
    Enum.reduce_while(responses, {:cont, client}, fn
      {:data, ref, data}, {:cont, client} when ref == client.ref ->
        {:ok, websocket, frames} = Mint.WebSocket.decode(client.websocket, data)
        client = %{client | websocket: websocket}

        case released_client_frames_outcome(frames) do
          {:terminal, type} -> {:halt, {:terminal, client, type}}
          {:closed, code, reason} -> {:halt, {:closed, client, code, reason}}
          :continue -> {:cont, {:cont, client}}
        end

      {:done, ref}, {:cont, client} when ref == client.ref ->
        {:halt, {:closed, client, nil, "done"}}

      _part, acc ->
        {:cont, acc}
    end)
    |> case do
      {:cont, _client} when deadline == :no_wait -> :continue
      {:cont, client} -> receive_released_client_outcome(client, deadline)
      outcome -> outcome
    end
  end

  defp released_client_frames_outcome(frames) do
    Enum.find_value(frames, :continue, fn
      {:close, code, reason} ->
        {:closed, code, reason}

      {:text, text} ->
        case CodexPooler.JSON.decode(text) do
          {:ok, %{"type" => type}} when type in @released_client_terminal_types -> {:terminal, type}
          _other -> nil
        end

      _frame ->
        nil
    end)
  end

  @doc """
  The `CodexResponsesSocket` state of one listener connection, read from its
  ThousandIsland handler process (the pid `WebsocketCleanupFence` registers).
  """
  def socket_connection_state!(connection_pid) when is_pid(connection_pid) do
    {_socket, handler_state} = :sys.get_state(connection_pid)
    handler_state.connection.websock_state
  end

  @doc "Polls `socket_connection_state!/1` until `predicate` holds, within the detection budget."
  def await_socket_connection_state!(connection_pid, predicate, deadline \\ nil) when is_function(predicate, 1) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms
    state = socket_connection_state!(connection_pid)

    cond do
      predicate.(state) ->
        state

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          5 -> await_socket_connection_state!(connection_pid, predicate, deadline)
        end

      true ->
        flunk("websocket connection state did not reach the expected condition")
    end
  end

  @doc """
  Pings the Pooler and waits for the pong: the socket handled every frame the
  client sent before the ping and is still open. A Close or a text frame
  other than the Pooler's metadata control frame fails the barrier.
  """
  def socket_transport_barrier!(conn, websocket, ref) do
    payload = "transport-barrier-#{System.unique_integer([:positive])}"
    {:ok, websocket, data} = Mint.WebSocket.encode(websocket, {:ping, payload})
    {:ok, conn} = Mint.WebSocket.stream_request_body(conn, ref, data)
    await_socket_pong!(conn, websocket, ref, payload, System.monotonic_time(:millisecond) + @detection_timeout_ms)
  end

  defp await_socket_pong!(conn, websocket, ref, payload, deadline) do
    message = receive_mint_socket_message!(conn, max(deadline - System.monotonic_time(:millisecond), 0), "timed out waiting for the websocket transport barrier")

    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        {websocket, pong?} =
          Enum.reduce(responses, {websocket, false}, fn
            {:data, ^ref, data}, {websocket, pong?} ->
              assert {:ok, websocket, frames} = Mint.WebSocket.decode(websocket, data)
              assert Enum.reject(frames, &(&1 == {:pong, payload} or metadata_control_frame?(&1))) == []
              {websocket, pong? or {:pong, payload} in frames}

            _response, acc ->
              acc
          end)

        if pong?, do: {conn, websocket}, else: await_socket_pong!(conn, websocket, ref, payload, deadline)

      {:error, _conn, reason, _responses} ->
        flunk("websocket transport barrier failed: #{inspect(reason)}")

      :unknown ->
        await_socket_pong!(conn, websocket, ref, payload, deadline)
    end
  end

  @doc """
  Collects the frames the Pooler sends until its Close frame, without the
  metadata control frames, and returns `{conn, websocket, frames}` with the
  `{:close, code, reason}` frame last. The client does not answer the Close,
  as the released Codex client does not.
  """
  def receive_frames_until_close!(conn, websocket, ref, frames \\ []) do
    message = receive_mint_socket_message!(conn, @connection_shutdown_timeout_ms, "timed out waiting for the websocket close")

    case Mint.WebSocket.stream(conn, message) do
      {:ok, conn, responses} ->
        {websocket, frames} =
          Enum.reduce(responses, {websocket, frames}, fn
            {:data, ^ref, data}, {websocket, frames} ->
              assert {:ok, websocket, decoded} = Mint.WebSocket.decode(websocket, data)
              {websocket, frames ++ Enum.reject(decoded, &metadata_control_frame?/1)}

            _response, acc ->
              acc
          end)

        if Enum.any?(frames, &match?({:close, _code, _reason}, &1)),
          do: {conn, websocket, frames},
          else: receive_frames_until_close!(conn, websocket, ref, frames)

      {:error, _conn, reason, _responses} ->
        flunk("websocket connection ended before a close frame: #{inspect(reason)}")

      :unknown ->
        receive_frames_until_close!(conn, websocket, ref, frames)
    end
  end

  @doc "Receives text frames until a native terminal and returns `{conn, websocket, decoded_terminal}`."
  def receive_native_terminal!(conn, websocket, ref) do
    {conn, websocket, frame} = public_websocket_receive_text!(conn, websocket, ref)

    case CodexPooler.JSON.decode!(frame) do
      %{"type" => type} = terminal when type in @native_turn_terminal_types -> {conn, websocket, terminal}
      _progress -> receive_native_terminal!(conn, websocket, ref)
    end
  end

  @doc """
  Holds the first response task that settles a websocket turn from now on,
  right after its settlement and outside any transaction, so its socket still
  tracks the turn until `release_settled_websocket_turn/2`. The held task
  reports `{hold, :held, task_pid}`; one it is never released from releases
  itself after the detection budget.
  """
  def hold_settled_websocket_turn! do
    hold = make_ref()
    handler_id = {__MODULE__, :settled_websocket_turn_hold, hold}
    ExUnit.Callbacks.on_exit(fn -> :telemetry.detach(handler_id) end)
    config = %{hold: hold, test: self(), claimed: :atomics.new(1, [])}
    :ok = :telemetry.attach(handler_id, [:codex_pooler, :gateway, :stream, :outcome], &__MODULE__.hold_settled_websocket_turn_task/4, config)
    hold
  end

  @doc false
  def hold_settled_websocket_turn_task(_event, _measurements, %{outcome: "succeeded", downstream_transport: "websocket"}, %{hold: hold, test: test, claimed: claimed}) do
    if not Repo.in_transaction?() and :atomics.add_get(claimed, 1, 1) == 1 do
      send(test, {hold, :held, self()})

      receive do
        {^hold, :release} -> :ok
      after
        @detection_timeout_ms -> :ok
      end
    end

    :ok
  end

  def hold_settled_websocket_turn_task(_event, _measurements, _metadata, _config), do: :ok

  def release_settled_websocket_turn(hold, task_pid) when is_reference(hold) and is_pid(task_pid) do
    send(task_pid, {hold, :release})
    :ok
  end

  # Upstream connection closes between two requests (findings#270), shared
  # by the forwarding-off and forwarding-on families.

  @doc "A native turn request that must carry no `previous_response_id`, on `connection_ordinal` (nil: any)."
  def anchorless_request(connection_ordinal, respond) do
    [method: "WEBSOCKET", path: "/backend-api/codex/responses", json: [valid: true, equals: %{"type" => "response.create"}, forbidden: ["previous_response_id"]], respond: respond]
    |> then(&if(connection_ordinal, do: Keyword.put(&1, :websocket_connection_ordinal, connection_ordinal), else: &1))
    |> FakeUpstream.expect_request()
  end

  @doc """
  Waits until the upstream websocket session holds no connection. The session
  drops its connection before it sends its close signal, so the signal is
  then in its subscriber's mailbox or handled.
  """
  def await_session_disconnected!(session, deadline \\ nil) do
    deadline = deadline || System.monotonic_time(:millisecond) + @detection_timeout_ms

    cond do
      not Map.has_key?(:sys.get_state(session), :conn) ->
        :ok

      System.monotonic_time(:millisecond) < deadline ->
        receive do
        after
          5 -> await_session_disconnected!(session, deadline)
        end

      true ->
        flunk("the upstream session kept its connection")
    end
  end

  @doc """
  Traces every `handle_in/2` call of one listener connection with its return,
  until the test ends (the pattern goes on exit; the flag goes with the
  process).
  """
  def trace_socket_frames!(socket) do
    handle_in = {CodexResponsesSocket, :handle_in, 2}
    ExUnit.Callbacks.on_exit(fn -> :erlang.trace_pattern(handle_in, false, []) end)
    1 = :erlang.trace_pattern(handle_in, [{:_, [], [{:return_trace}]}], [])
    1 = :erlang.trace(socket, true, [:call, {:tracer, self()}])
    :ok
  end

  @doc "Splits frames into the decoded terminals before the first non-text frame and the frames from there on."
  def split_turn_frames(frames) do
    {texts, rest} = Enum.split_while(frames, &match?({:text, _text}, &1))
    {Enum.map(texts, fn {:text, text} -> CodexPooler.JSON.decode!(text) end) |> Enum.filter(&(&1["type"] in ["response.completed", "response.failed", "error"])), rest}
  end

  def downstream_closed_line(cause, lifecycle_id, generation, forwarding \\ "off"),
    do: "websocket downstream closed after upstream connection close reason_code=#{cause} lifecycle_id=#{lifecycle_id} generation=#{generation} forwarding=#{forwarding} codex_session_id="

  def kept_open_line(cause, skip_reason, lifecycle_id, generation, forwarding \\ "off"),
    do: "websocket downstream kept open after upstream connection close reason_code=#{cause} skip_reason=#{skip_reason} lifecycle_id=#{lifecycle_id} generation=#{generation} forwarding=#{forwarding} codex_session_id="

  @doc """
  The socket (and, with owner forwarding on, the owner) logs one line per
  decision: exactly the expected upstream-close lines, in order.
  """
  def assert_upstream_close_lines!(log, expected) do
    lines = log |> String.split("\n") |> Enum.filter(&(&1 =~ "after upstream connection close"))
    assert length(lines) == length(expected), "upstream close lines: #{inspect(lines)}"
    Enum.zip_with(lines, expected, fn line, text -> assert line =~ text end)
  end

  @doc """
  Closing the socket and the client's dropped connection leave no warning or
  error line (the `cleanup_deferred` warning depends only on scheduling).
  """
  def assert_quiet_close!(log) do
    quiet = WebsocketCleanupFence.without_deferred_cleanup(log)
    refute quiet =~ "[warning]"
    refute quiet =~ "[error]"
  end
end
