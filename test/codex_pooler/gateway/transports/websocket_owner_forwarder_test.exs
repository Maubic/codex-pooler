defmodule CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarderTest do
  use CodexPooler.DataCase, async: false

  @moduletag capture_log: true

  import CodexPooler.AccountsFixtures
  import CodexPooler.PoolerFixtures
  import ExUnit.CaptureLog

  alias CodexPooler.FakeUpstream
  alias CodexPooler.Gateway.OperationalSettings
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.{ResetProbe, TimeoutConfig}
  alias CodexPooler.Gateway.Persistence.{BridgeOwnerLease, CodexSession, SessionContinuity}
  alias CodexPooler.Gateway.Transports.Streaming.RuntimeAdmissionProof
  alias CodexPooler.Gateway.Transports.Streaming.StreamProtocol
  alias CodexPooler.Gateway.Transports.Websocket.AbandonedSubmissions
  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionAdmission
  alias CodexPooler.Gateway.Transports.Websocket.NativeReplayAdmission
  alias CodexPooler.Gateway.Transports.Websocket.OwnerDefaults
  alias CodexPooler.Gateway.Transports.Websocket.RemoteReconnectControlV2
  alias CodexPooler.Gateway.Transports.Websocket.RolloutDrain
  alias CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerAdmissionControlV1
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerContract
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequest
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV2
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV3
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV4
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV5
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV8
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness
  alias CodexPooler.Gateway.Transports.WebsocketOwnerPreviousReleaseCaller
  alias CodexPooler.Gateway.Transports.WebsocketRolloutDrainSupport
  alias CodexPooler.Gateway.Websocket, as: Gateway
  alias CodexPooler.Gateway.Websocket.ResponseTask
  alias CodexPooler.InstancePresencePeer
  alias CodexPooler.PeerRegistry
  alias CodexPooler.UnboxedFixture
  alias CodexPooler.Upstreams.SavedResets.{ProbeLease, RedemptionLifecycle}
  alias CodexPooler.Upstreams.Schemas.UpstreamIdentity
  alias CodexPoolerWeb.CodexResponsesSocket
  alias CodexPoolerWeb.Runtime.BackendCodexWebsocketOwnerForwardingSupport

  @frame "synthetic-frame"
  @peer_detection_timeout_ms 10_000
  @stalled_owner_node_ms 1_200
  @timeouts %{connect_timeout_ms: 1_000, receive_timeout_ms: 1_000}

  defmodule CurrentFailureKindNodeClient do
    @moduledoc false

    def connected_app_nodes, do: Process.get({__MODULE__, :nodes}, [])
    def app_node?(_node), do: true

    def call_owner(node, _module, function, args, _timeout) do
      send(self(), {:current_failure_kind_call, node, function, length(args)})

      case Process.get({__MODULE__, :action}) do
        {:return, value} -> value
        {:error, reason} -> :erlang.error(reason)
        {:exit, reason} -> exit(reason)
        {:throw, reason} -> throw(reason)
      end
    end

    def configure(nodes, action) do
      Process.put({__MODULE__, :nodes}, nodes)
      Process.put({__MODULE__, :action}, action)
    end

    def reset do
      Process.delete({__MODULE__, :nodes})
      Process.delete({__MODULE__, :action})
    end
  end

  defmodule ReconnectTimeoutNodeClient do
    @moduledoc false

    @remote_node :"codex_pooler@reconnect-timeout-owner-app.example"

    def connected_app_nodes, do: [@remote_node]
    def app_node?(@remote_node), do: true

    def call_owner(
          _node,
          module,
          :remote_reconnect_control_v1,
          [%{action: :preflight} = control],
          _timeout
        ) do
      result = module.remote_reconnect_control_v1(control)
      send(control.downstream.pid, {:remote_reconnect_preflight_processed, result})
      {:error, :timeout}
    end

    def call_owner(
          _node,
          module,
          :remote_reconnect_control_v1,
          [%{action: :cancel} = control],
          _timeout
        ) do
      result = module.remote_reconnect_control_v1(control)
      send(control.downstream.pid, {:remote_reconnect_cancel_processed, result})
      result
    end
  end

  defmodule ReplayTimeoutNodeClient do
    @moduledoc false

    def connected_app_nodes, do: Process.get({__MODULE__, :nodes}, [])
    def app_node?(_node), do: true

    def call_owner(
          _node,
          _module,
          :remote_submit_request_v8,
          [_session_id, _downstream, %WebsocketOwnerRequestV8{request: %WebsocketOwnerRequestV4{} = request}],
          _
        ) do
      send(self(), {:replay_v4_submit, request.native_replay_binding.owner_process_generation})
      {:error, :owner_forward_timeout}
    end

    def call_owner(_node, _module, :remote_reconnect_control_v2, [control], _) do
      send(self(), {:replay_v4_control, control.action, control.provisional_token})

      case {Process.get({__MODULE__, :status}), control.action} do
        {status, :provisional_query} -> {:ok, status}
        {_status, :provisional_cancel} -> {:ok, :cancelled}
      end
    end

    def call_owner(_node, _module, function, _args, _) do
      send(self(), {:unexpected_replay_v4_call, function})
      {:error, :owner_unavailable}
    end

    def configure(nodes, status) do
      Process.put({__MODULE__, :nodes}, nodes)
      Process.put({__MODULE__, :status}, status)
    end

    def reset do
      Process.delete({__MODULE__, :nodes})
      Process.delete({__MODULE__, :status})
    end
  end

  defmodule ReplayCallerDeathNodeClient do
    @moduledoc false

    def connected_app_nodes, do: [:"codex_pooler@replay-caller-death.example"]
    def app_node?(_node), do: true

    def call_owner(
          _node,
          _module,
          :remote_submit_request_v8,
          [_session_id, _downstream, %WebsocketOwnerRequestV8{request: %WebsocketOwnerRequestV4{} = request}],
          _
        ) do
      notify({:replay_caller_death_submit, self()})

      receive do
        :never_release_replay_submit ->
          {:ok, request.native_replay_binding.owner_process_generation}
      end
    end

    def call_owner(_node, _module, :remote_reconnect_control_v2, [control], _) do
      notify({:replay_caller_death_control, control.action, control.provisional_token})

      case {control.provisional_token, control.action} do
        {<<1::256>>, :provisional_query} -> {:ok, :started}
        {<<2::256>>, :provisional_query} -> {:ok, :consume_reserved}
        {<<2::256>>, :provisional_cancel} -> {:ok, :cancelled}
      end
    end

    def call_owner(_node, _module, function, _args, _) do
      notify({:unexpected_replay_caller_death_call, function})
      {:error, :owner_unavailable}
    end

    defp notify(message) do
      if pid = Process.whereis(:replay_caller_death_test), do: send(pid, message)
    end
  end

  defmodule ReplayCallerDeathSlowOwnerNodeClient do
    @moduledoc false
    # The watcher's controls go through the production erpc client against the
    # local node, so the caller's timeout is enforced as it is between two
    # nodes; the owner answers the query only after `@owner_query_ms`, the
    # binding read a loaded owner node can take.
    alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder.ERPCNodeClient

    @owner_query_ms 1_200

    def connected_app_nodes, do: [:"codex_pooler@replay-caller-death-slow.example"]
    def app_node?(_node), do: true

    # A caller that set `:submit_times_out` reads the submission's own
    # `owner_forward_timeout`; any other caller blocks until it is killed.
    def call_owner(_node, _module, :remote_submit_request_v8, [_session_id, _downstream, %WebsocketOwnerRequestV8{request: %WebsocketOwnerRequestV4{} = request}], _timeout) do
      notify({:slow_owner_submit, self()})

      if Process.get({__MODULE__, :submit_times_out}) do
        {:error, :owner_forward_timeout}
      else
        receive do
          :never_release_replay_submit -> {:ok, request.native_replay_binding.owner_process_generation}
        end
      end
    end

    def call_owner(_node, _module, :remote_reconnect_control_v2, [control], timeout) do
      notify({:slow_owner_control_sent, control.action, timeout})
      ERPCNodeClient.call_owner(node(), __MODULE__, :owner_control, [control], timeout)
    end

    def call_owner(_node, _module, function, _args, _timeout) do
      notify({:unexpected_slow_owner_call, function})
      {:error, :owner_unavailable}
    end

    def owner_control(%{action: :provisional_query}) do
      Process.sleep(@owner_query_ms)
      {:ok, :consume_reserved}
    end

    def owner_control(%{action: :provisional_cancel, provisional_token: token}) do
      notify({:slow_owner_cancelled, token})
      {:ok, :cancelled}
    end

    defp notify(message) do
      if pid = Process.whereis(:replay_caller_death_slow_owner_test), do: send(pid, message)
    end
  end

  defmodule LateEarlyDetachNodeClient do
    @moduledoc false
    # The closing socket's early detach goes through the production erpc
    # client against the local node, so its one-second budget is enforced as
    # between two nodes; the owner receives the call only after
    # `@owner_busy_ms`, as behind a replay arm of another turn. Every other call
    # runs in process.
    alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder
    alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder.ERPCNodeClient

    @owner_busy_ms 1_200

    def connected_app_nodes, do: [:"codex_pooler@late-early-detach.example"]
    def app_node?(_node), do: true

    def call_owner(_node, _module, :remote_detach_previsible_downstream_v1, args, timeout),
      do: ERPCNodeClient.call_owner(node(), __MODULE__, :late_detach_previsible, [args], timeout)

    def call_owner(_node, module, function, args, _timeout), do: apply(module, function, args)

    def late_detach_previsible(args) do
      Process.sleep(@owner_busy_ms)
      result = apply(WebsocketOwnerForwarder, :remote_detach_previsible_downstream_v1, args)

      if pid = Process.whereis(:late_early_detach_test), do: send(pid, {:late_early_detach_answer, result})

      result
    end
  end

  setup_all do
    ensure_epmd_started!()
    :ok
  end

  setup do
    reset_bootstrap_state_fixture!()
    auth = auth_fixture()
    BackendCodexWebsocketOwnerForwardingSupport.stop_pool_owners_on_exit(auth.pool)
    identity = active_upstream_identity_fixture()
    Process.put({__MODULE__, :upstream_identity}, identity)
    context = CodexPooler.ProviderCreditsDispatchSupport.context!(identity, pool: auth.pool)
    Process.put({__MODULE__, :provider_credits_context}, context)

    on_exit(fn ->
      CurrentFailureKindNodeClient.reset()
      ReplayTimeoutNodeClient.reset()
    end)

    {:ok, auth: auth}
  end

  @tag :replay_protocol_v2
  @tag :replay_topology
  test "an abandon recorded before owner registration refuses the late collected turn without upstream dispatch" do
    %{session: session} = owner_session_fixture(auth_fixture(), Atom.to_string(node()))
    target = %{pid: self(), epoch: 1, correlation_id: "late-collect", owner_turn_id: self()}
    key = AbandonedSubmissions.key(session.id, target)
    on_exit(fn -> AbandonedSubmissions.consume(key) end)
    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.remote_abandon_turn_v1(session.id, target)
    assert AbandonedSubmissions.recorded?(key)
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), return_request_result?: true)
    assert {:ok, owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}
    assert {:ok, attached} = WebsocketOwnerSession.attach_downstream(owner, target)
    assert attached.epoch == target.epoch
    envelope = owner_request_v2(request("late-collect"))
    assert {:error, :stale_downstream} = WebsocketOwnerForwarder.remote_submit_request_v8(session.id, target, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(envelope))
    assert WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid) == []
    refute AbandonedSubmissions.recorded?(key)
    assert :sys.get_state(owner).active_turn == nil
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
  end

  test "collected and replay submissions consume a node-level abandon before owner lookup" do
    for envelope <- [owner_request_v2(request("abandoned-collect")), abandoned_v3_request(), owner_request_v4(replay_binding(1)), abandoned_v5_request()] do
      session_id = Ecto.UUID.generate()
      target = %{pid: self(), epoch: 1, correlation_id: "abandoned", owner_turn_id: self()}
      key = AbandonedSubmissions.key(session_id, target)
      on_exit(fn -> AbandonedSubmissions.consume(key) end)
      assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.remote_abandon_turn_v1(session_id, target)
      assert AbandonedSubmissions.recorded?(key)
      assert {:error, :stale_downstream} = WebsocketOwnerForwarder.remote_submit_request_v8(session_id, target, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(envelope))
      refute AbandonedSubmissions.recorded?(key)
      assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session_id)
    end
  end

  # A socket whose attach to a remote owner timed out abandons the attach on
  # the owner's node before it gives up, while the attach still waits in the
  # stalled owner's mailbox (findings#270 row 270-248). The record comes before
  # the owner is looked up, and the owner that takes the attach afterwards
  # refuses it once: the live socket stays its downstream. An owner node of an
  # earlier release has no entrypoint for the abandon, and its failure is
  # ignored.
  test "an abandoned attach is refused once by the owner that takes it after the abandon", %{auth: auth} do
    %{session: session} = owner_session_fixture(auth, Atom.to_string(node()), "abandoned-attach")
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: [])
    {:ok, owner} = start_owner(session, upstream)
    live = attach_downstream(session.id, "live-attach")
    abandoned = %{pid: idle_socket!(), correlation_id: "abandoned-attach"}
    key = AbandonedSubmissions.attach_key(session.id, abandoned)
    on_exit(fn -> AbandonedSubmissions.consume(key) end)

    assert :ok = WebsocketOwnerForwarder.remote_abandon_attach_v1(session.id, abandoned)
    assert AbandonedSubmissions.recorded?(key)
    assert {:error, :stale_downstream} = WebsocketOwnerSession.attach_downstream(owner, abandoned)
    refute AbandonedSubmissions.recorded?(key)
    assert :sys.get_state(owner).downstream == live
    assert {:ok, %{correlation_id: "abandoned-attach"}} = WebsocketOwnerSession.attach_downstream(owner, abandoned)

    earlier_release_node = :"codex_pooler@earlier-release-owner.example"
    undef = {:error, {:exception, :undef, [{WebsocketOwnerForwarder, :remote_abandon_attach_v1, [session.id, abandoned], []}]}}
    opts = WebsocketOwnerNodeHarness.node_client_opts([earlier_release_node], calls: %{earlier_release_node => {:return, undef}})
    assert :ok = WebsocketOwnerForwarder.abandon_remote_attach(earlier_release_node, session.id, abandoned, opts)
    assert_receive {:websocket_owner_harness_node_call, %{node: ^earlier_release_node, function: :remote_abandon_attach_v1, arity: 2, timeout: 1_000}}
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
  end

  # The owner took the attach before the abandon recorded it: the abandon
  # detaches the socket that gave up, and the socket it replaced is not
  # restored. That socket meets `stale_owner` on its next request and attaches
  # again on its retry (findings#270 row 270-248).
  test "an abandon that arrives after its owner took the attach detaches that socket and restores nothing", %{auth: auth} do
    %{session: session} = owner_session_fixture(auth, Atom.to_string(node()), "late-abandon")
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: [])
    {:ok, owner} = start_owner(session, upstream)
    _replaced = attach_downstream(session.id, "replaced-attach")
    abandoned = %{pid: idle_socket!(), correlation_id: "late-abandoned-attach"}
    assert {:ok, %{correlation_id: "late-abandoned-attach"}} = WebsocketOwnerSession.attach_downstream(owner, abandoned)
    key = AbandonedSubmissions.attach_key(session.id, abandoned)
    on_exit(fn -> AbandonedSubmissions.consume(key) end)

    assert :ok = WebsocketOwnerForwarder.remote_abandon_attach_v1(session.id, abandoned)
    assert %{downstream: nil, downstream_monitor: nil} = :sys.get_state(owner)
    refute AbandonedSubmissions.recorded?(key)
    assert Process.alive?(abandoned.pid)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
  end

  # A replay-path call an owner on the caller's node does not answer within
  # the owner call budget answers as the same call to a remote owner does,
  # instead of exiting into its caller (findings#270 row 270-284): the next
  # turn's replay descriptor answers the timeout, a replay reserve receipt call
  # `owner_unavailable`.
  test "replay-path calls to a local owner that does not answer within its budget answer as a remote owner's do", %{auth: auth} do
    %{session: session, token: token} = owner_session_fixture(auth, Atom.to_string(node()), "slow-replay-owner")
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: [])
    {:ok, owner} = start_owner(session, upstream)
    downstream = attach_downstream(session.id, "slow-replay-owner")
    original = CodexPooler.TestAppEnv.restore_on_exit(OwnerDefaults)
    Application.put_env(:codex_pooler, OwnerDefaults, Keyword.merge(original, owner_call_timeout_ms: 200))
    :ok = :sys.suspend(owner)
    proof = %{owner_lease_token: token}

    assert {:error, :owner_forward_timeout} = WebsocketOwnerForwarder.prepare_next_replay_descriptor(session, token, downstream, %{kind: :native}, [])
    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.consume_replay_reserve(session, token, proof, [])
    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.validate_replay_reserve(session, token, proof, make_ref(), [])
    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.release_replay_reserve(session, token, proof, make_ref(), [])

    :ok = :sys.resume(owner)
    assert :ok = WebsocketOwnerSession.drain_owner(owner)
  end

  test "a client retry submission without a recoverable owner refuses instead of raising" do
    downstream = %{pid: self(), epoch: 1, correlation_id: "missing-retry-owner", owner_turn_id: self()}
    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.remote_submit_request_v8(Ecto.UUID.generate(), downstream, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(abandoned_v5_request()))
  end

  test "dedicated V2 control has local and simulated remote parity", %{auth: auth} do
    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(node()), "v2-local")

    assert {:ok, _owner} =
             start_owner(session, WebsocketOwnerNodeHarness.fake_upstream_boundary(self()))

    assert_receive {:websocket_owner_harness_upstream_started, _}
    control = v2_control(auth, session, token)

    assert {:ok, :fresh_dispatch, _downstream} =
             WebsocketOwnerForwarder.reconnect_control_v2(session, token, control)

    remote = :"codex_pooler@v2-owner-app.example"
    remote_session = %{session | owner_instance_id: Atom.to_string(remote)}
    opts = WebsocketOwnerNodeHarness.node_client_opts([remote], calls: %{remote => :success})

    assert {:ok, :fresh_dispatch, _downstream} =
             WebsocketOwnerForwarder.reconnect_control_v2(remote_session, token, control, opts)

    assert_receive {:websocket_owner_harness_node_call, %{function: :remote_reconnect_control_v2}}
  end

  @tag :replay_protocol_v2
  @tag :replay_topology
  @tag :replay_race
  test "remote V4 timeout preserves a started replay after factual owner query", %{auth: auth} do
    remote = :"codex_pooler@replay-v4-timeout.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote), "replay-v4-started")

    binding = replay_binding(37)
    request = owner_request_v4(binding)
    ReplayTimeoutNodeClient.configure([remote], :started)
    opts = [node_client: ReplayTimeoutNodeClient, timeout: 25]

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.submit_request(session, token, downstream("replay-v4-started"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), opts)

    assert_receive {:replay_v4_submit, 37}
    assert_receive {:replay_v4_control, :provisional_query, provisional_token}
    assert provisional_token == request.provisional_token
    refute_received {:replay_v4_control, :provisional_cancel, _token}
    refute_received {:unexpected_replay_v4_call, _function}
  end

  @tag :replay_protocol_v2
  @tag :replay_topology
  @tag :replay_race
  test "remote V4 timeout cancels only an owner-proven unconsumed reservation", %{auth: auth} do
    remote = :"codex_pooler@replay-v4-unconsumed.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote), "replay-v4-unconsumed")

    request = owner_request_v4(replay_binding(41))
    ReplayTimeoutNodeClient.configure([remote], :consume_reserved)
    opts = [node_client: ReplayTimeoutNodeClient, timeout: 25]

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.submit_request(session, token, downstream("replay-v4-unconsumed"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), opts)

    assert_receive {:replay_v4_control, :provisional_query, provisional_token}
    assert_receive {:replay_v4_control, :provisional_cancel, ^provisional_token}
    refute_received {:unexpected_replay_v4_call, _function}
  end

  @tag :replay_protocol_v2
  @tag :replay_topology
  @tag :replay_cleanup
  test "remote V4 caller death preserves started replay after durable query", %{auth: auth} do
    Process.register(self(), :replay_caller_death_test)

    on_exit(fn ->
      if Process.whereis(:replay_caller_death_test),
        do: Process.unregister(:replay_caller_death_test)
    end)

    remote = :"codex_pooler@replay-caller-death.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote), "replay-caller-death-started")

    request = owner_request_v4(replay_binding(51), <<1::256>>)
    envelope = CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request)

    caller =
      spawn(fn ->
        WebsocketOwnerForwarder.submit_request(session, token, downstream("replay-caller-death-started"), envelope, node_client: ReplayCallerDeathNodeClient)
      end)

    assert_receive {:replay_caller_death_submit, ^caller}
    caller_monitor = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^caller_monitor, :process, ^caller, :killed}
    assert_receive {:replay_caller_death_control, :provisional_query, <<1::256>>}
    refute_received {:replay_caller_death_control, :provisional_cancel, _token}
    refute_received {:unexpected_replay_caller_death_call, _function}
  end

  @tag :replay_protocol_v2
  @tag :replay_topology
  @tag :replay_cleanup
  test "remote V4 caller death cancels only a durable unconsumed reservation", %{auth: auth} do
    Process.register(self(), :replay_caller_death_test)

    on_exit(fn ->
      if Process.whereis(:replay_caller_death_test),
        do: Process.unregister(:replay_caller_death_test)
    end)

    remote = :"codex_pooler@replay-caller-death.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote), "replay-caller-death-unconsumed")

    request = owner_request_v4(replay_binding(53), <<2::256>>)
    envelope = CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request)

    caller =
      spawn(fn ->
        WebsocketOwnerForwarder.submit_request(session, token, downstream("replay-caller-death-unconsumed"), envelope, node_client: ReplayCallerDeathNodeClient)
      end)

    assert_receive {:replay_caller_death_submit, ^caller}
    caller_monitor = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^caller_monitor, :process, ^caller, :killed}
    assert_receive {:replay_caller_death_control, :provisional_query, <<2::256>>}
    assert_receive {:replay_caller_death_control, :provisional_cancel, <<2::256>>}
    refute_received {:unexpected_replay_caller_death_call, _function}
  end

  # findings#206 row 206-241: the watcher of a dead V4 submitter used the one
  # second downstream budget, so an owner that answered the query after a
  # slower binding read had its answer dropped and the unconsumed reservation
  # was never cancelled.
  @tag :replay_topology
  @tag :replay_cleanup
  test "remote V4 caller death waits the owner call budget for the query before cancelling", %{auth: auth} do
    Process.register(self(), :replay_caller_death_slow_owner_test)

    on_exit(fn ->
      if Process.whereis(:replay_caller_death_slow_owner_test),
        do: Process.unregister(:replay_caller_death_slow_owner_test)
    end)

    remote = :"codex_pooler@replay-caller-death-slow.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote), "replay-caller-death-slow-owner")

    request = owner_request_v4(replay_binding(57), <<3::256>>)
    envelope = CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request)

    caller =
      spawn(fn ->
        WebsocketOwnerForwarder.submit_request(session, token, downstream("replay-caller-death-slow-owner"), envelope, node_client: ReplayCallerDeathSlowOwnerNodeClient)
      end)

    assert_receive {:slow_owner_submit, ^caller}
    caller_monitor = Process.monitor(caller)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^caller_monitor, :process, ^caller, :killed}

    owner_budget = WebsocketOwnerContract.default_owner_call_timeout_ms()
    assert_receive {:slow_owner_control_sent, :provisional_query, query_timeout}
    # The owner reported the reservation unconsumed after the slow read, so the
    # dead submitter's reservation is cancelled.
    assert_receive {:slow_owner_cancelled, <<3::256>>}, owner_budget
    assert query_timeout == owner_budget
    assert_received {:slow_owner_control_sent, :provisional_cancel, ^owner_budget}
    refute_received {:unexpected_slow_owner_call, _function}
  end

  @tag :replay_topology
  @tag :replay_cleanup
  test "remote V4 timeout waits the owner call budget for the query before cancelling", %{auth: auth} do
    Process.register(self(), :replay_caller_death_slow_owner_test)
    Process.put({ReplayCallerDeathSlowOwnerNodeClient, :submit_times_out}, true)

    on_exit(fn ->
      if Process.whereis(:replay_caller_death_slow_owner_test),
        do: Process.unregister(:replay_caller_death_slow_owner_test)
    end)

    remote = :"codex_pooler@replay-caller-death-slow.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote), "replay-timeout-slow-owner")

    request = owner_request_v4(replay_binding(59), <<4::256>>)

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.submit_request(session, token, downstream("replay-timeout-slow-owner"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), node_client: ReplayCallerDeathSlowOwnerNodeClient)

    owner_budget = WebsocketOwnerContract.default_owner_call_timeout_ms()
    assert_received {:slow_owner_control_sent, :provisional_query, query_timeout}
    assert_received {:slow_owner_cancelled, <<4::256>>}
    assert query_timeout == owner_budget
    assert_received {:slow_owner_control_sent, :provisional_cancel, ^owner_budget}
    refute_received {:unexpected_slow_owner_call, _function}
  end

  defp v2_control(auth, session, token) do
    {:ok, control} =
      RemoteReconnectControlV2.new(%{
        version: 2,
        action: :preflight,
        intent: :fresh,
        codex_session_id: session.id,
        downstream: %{pid: self(), epoch: 1, correlation_id: "v2-control"},
        semantic_turn_digest: <<1::256>>,
        replay_claim_digest: <<2::256>>,
        provisional_token: nil,
        replay_generation: nil,
        owner_lease_token: token,
        control_ref: make_ref(),
        authorization_binding: %{
          api_key_id: auth.api_key.id,
          api_key_runtime_epoch: auth.api_key.runtime_revocation_epoch,
          pool_id: auth.pool.id,
          codex_session_id: session.id,
          model_identifier: "gpt-test"
        },
        consume_binding: nil
      })

    control
  end

  test "local owner resolution submits to local WebsocketOwnerSession", %{auth: auth} do
    %{session: session, token: token} = owner_session_fixture(auth, Atom.to_string(node()))
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: ["local-delta"])
    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    downstream = attach_downstream(session.id, "corr-local")

    assert :ok = WebsocketOwnerForwarder.submit_frame(session, token, downstream, @frame)

    assert WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid) == [@frame]
    assert_receive {:websocket_owner_frame, "corr-local", 1, {:data, "local-delta"}}
    assert_receive {:websocket_owner_frame, "corr-local", 1, :complete}
  end

  test "local missing-owner recovery receives the caller options", %{auth: auth} do
    local_node_string = Atom.to_string(node())
    %{session: session, token: token} = owner_session_fixture(auth, local_node_string)

    terminal_frame =
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => "resp_local_recovery_options"}
      })

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame],
        return_request_result?: true
      )

    assert {:ok, %{body: body, terminal: "response.completed", status: 200}} =
             WebsocketOwnerForwarder.submit_request(session, token, downstream("corr-local-recovery-options"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request("local-recovery-options")),
               upstream: upstream,
               request_id: "local-recovery-options"
             )

    assert body =~ "resp_local_recovery_options"
    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}
  end

  # Findings #119 follow-up: the recovery start path forwarded only the
  # upstream boundary into `start_owner/1`, so a recovered owner silently kept
  # the default handoff timeouts that the caller options carried.
  test "local missing-owner recovery starts the owner with the caller handoff timeouts", %{
    auth: auth
  } do
    local_node_string = Atom.to_string(node())
    handoff_soft_timeout_ms = 25
    handoff_absolute_timeout_ms = 2_000

    %{session: session, token: token} =
      owner_session_fixture(auth, local_node_string, "recovery-handoff")

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame("resp_local_recovery_handoff")],
        return_request_result?: true
      )

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             WebsocketOwnerForwarder.submit_request(session, token, downstream("corr-local-recovery-handoff"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request("local-recovery-handoff")),
               upstream: upstream,
               request_id: "local-recovery-handoff",
               handoff_soft_timeout_ms: handoff_soft_timeout_ms,
               handoff_absolute_timeout_ms: handoff_absolute_timeout_ms
             )

    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}
    assert {:ok, recovered_owner} = WebsocketOwnerSession.lookup(session.id)

    assert %{
             handoff_soft_timeout_ms: ^handoff_soft_timeout_ms,
             handoff_absolute_timeout_ms: ^handoff_absolute_timeout_ms
           } = :sys.get_state(recovered_owner)

    %{session: default_session, token: default_token} =
      owner_session_fixture(auth, local_node_string, "recovery-handoff-default")

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             WebsocketOwnerForwarder.submit_request(default_session, default_token, downstream("corr-local-recovery-handoff-default"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request("local-recovery-handoff-default")),
               upstream: upstream,
               request_id: "local-recovery-handoff-default",
               handoff_soft_timeout_ms: "25",
               handoff_absolute_timeout_ms: 0
             )

    assert_receive {:websocket_owner_harness_upstream_started, _default_upstream_pid}
    assert {:ok, default_owner} = WebsocketOwnerSession.lookup(default_session.id)
    default_state = :sys.get_state(default_owner)
    refute default_state.handoff_soft_timeout_ms == handoff_soft_timeout_ms
    refute default_state.handoff_absolute_timeout_ms == handoff_absolute_timeout_ms
    assert is_integer(default_state.handoff_soft_timeout_ms)
    assert is_integer(default_state.handoff_absolute_timeout_ms)
  end

  @tag :rollout_drain_t3
  test "T3 marker refuses target-side missing-owner resurrection", %{auth: auth} do
    local_node_string = Atom.to_string(node())
    %{session: session, token: token} = owner_session_fixture(auth, local_node_string)
    original_lease = Repo.get_by!(BridgeOwnerLease, lease_token: token)

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame("resp_marker_must_not_run")],
        return_request_result?: true
      )

    _marker_path = WebsocketRolloutDrainSupport.configure_drain_marker!()

    assert {:error, :owner_drained} =
             WebsocketOwnerForwarder.submit_request(session, token, downstream("corr-marker-resurrection"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request("marker-resurrection")),
               upstream: upstream,
               request_id: "marker-resurrection"
             )

    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session.id)
    refute_received {:websocket_owner_harness_upstream_started, _upstream_pid}
    assert Repo.get!(CodexSession, session.id).owner_lease_token == token
    assert Repo.get!(BridgeOwnerLease, original_lease.id).status == "active"
  end

  @tag :rollout_drain_t3
  test "T3 marker refuses crashed-owner takeover before visible output", %{auth: auth} do
    local_node_string = Atom.to_string(node())
    %{session: session, token: token} = owner_session_fixture(auth, local_node_string)
    original_lease = Repo.get_by!(BridgeOwnerLease, lease_token: token)
    parent = self()
    release_ref = make_ref()

    first_upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, _request, _writer ->
        send(parent, {:marker_takeover_submit_started, self(), release_ref})

        receive do
          {:release_marker_takeover_submit, ^release_ref} -> :ok
        end
      end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    {:ok, first_owner} = start_owner(session, first_upstream)

    assert {:ok, stable_downstream} =
             WebsocketOwnerSession.attach_downstream(
               first_owner,
               downstream("corr-marker-takeover")
             )

    recovery_upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame("resp_marker_takeover_must_not_run")],
        return_request_result?: true
      )

    owner_request = request("marker-takeover")

    submitter =
      Task.async(fn ->
        WebsocketOwnerForwarder.submit_request(session, token, stable_downstream, owner_request,
          upstream: recovery_upstream,
          local_node_string: local_node_string,
          request_id: "marker-takeover"
        )
      end)

    assert_receive {:marker_takeover_submit_started, first_worker, ^release_ref}
    _marker_path = WebsocketRolloutDrainSupport.configure_drain_marker!()

    first_owner_ref = Process.monitor(first_owner)
    Process.exit(first_owner, :kill)
    assert_receive {:DOWN, ^first_owner_ref, :process, ^first_owner, :killed}
    send(first_worker, {:release_marker_takeover_submit, release_ref})

    assert Task.await(submitter, 2_000) == {:error, :owner_drained}
    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session.id)
    refute_received {:websocket_owner_runtime_recovered, _, _, _}
    refute_received {:websocket_owner_harness_upstream_started, _recovery_upstream_pid}
    assert Repo.get!(CodexSession, session.id).owner_lease_token == token
    assert Repo.get!(BridgeOwnerLease, original_lease.id).status == "active"

    assert Repo.aggregate(
             from(lease in BridgeOwnerLease,
               where: lease.codex_session_id == ^session.id and lease.status == "active"
             ),
             :count
           ) == 1
  end

  test "bound reset probe does not recover a missing local owner", %{auth: auth} do
    local_node_string = Atom.to_string(node())
    %{session: session} = owner_session_fixture(auth, local_node_string)

    _upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame("resp_bound_missing_owner")],
        return_request_result?: true
      )

    envelope = owner_request(%{request("bound-missing-owner") | reset_probe: bound_reset_probe()})
    envelope = CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(envelope)

    assert {:error, :owner_unavailable} =
             WebsocketOwnerForwarder.remote_submit_request_v8(
               session.id,
               downstream("corr-bound-missing-owner"),
               envelope
             )

    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session.id)
    refute_received {:websocket_owner_harness_upstream_started, _upstream_pid}
  end

  @tag :owner_forwarding_regression
  test "RED-R05 owner-unavailable response option recovery preserves per-call owner turn id", %{
    auth: auth
  } do
    %{session: session, token: token} = owner_session_fixture(auth, Atom.to_string(node()))
    owner_turn_id = self()

    downstream =
      downstream("corr-red-response-options")
      |> Map.put(:active_turn_reconnect?, false)
      |> Map.put(:owner_turn_id, owner_turn_id)

    opts =
      %{}
      |> RequestOptions.for_websocket()
      |> RequestOptions.put_openai_compatibility(public_openai_responses_stream: true)
      |> Gateway.websocket_owner_response_options(session, token, downstream)

    assert {:ok, recovered_opts} = Gateway.recover_websocket_owner_response_options(opts)

    recovered_downstream = recovered_opts.transport.websocket_owner.downstream
    assert Map.get(recovered_downstream, :owner_turn_id) == owner_turn_id

    assert MapSet.new(Map.keys(recovered_downstream)) ==
             MapSet.new([
               :pid,
               :epoch,
               :correlation_id,
               :active_turn_reconnect?,
               :owner_turn_id
             ])
  end

  test "public response option recovery fails closed for missing mutated or extra turn identity",
       %{
         auth: auth
       } do
    %{session: session, token: token} = owner_session_fixture(auth, Atom.to_string(node()))

    stable_downstream =
      downstream("corr-invalid-response-options")
      |> Map.put(:active_turn_reconnect?, false)

    mutated_owner_turn_id = spawn(fn -> :ok end)

    invalid_downstreams = [
      stable_downstream,
      Map.put(stable_downstream, :owner_turn_id, mutated_owner_turn_id),
      stable_downstream
      |> Map.put(:owner_turn_id, self())
      |> Map.put(:unexpected, true)
    ]

    for invalid_downstream <- invalid_downstreams do
      opts =
        %{}
        |> RequestOptions.for_websocket()
        |> RequestOptions.put_openai_compatibility(public_openai_responses_stream: true)
        |> Gateway.websocket_owner_response_options(session, token, invalid_downstream)

      assert Gateway.recover_websocket_owner_response_options(opts) ==
               {:error, :owner_unavailable}
    end
  end

  @tag :owner_forwarding_regression
  test "RED-R06 direct recovery keeps per-call owner turn id out of stable restore state", %{
    auth: auth
  } do
    local_node_string = Atom.to_string(node())
    %{session: session, token: token} = owner_session_fixture(auth, local_node_string)
    owner_turn_id = self()
    frame = terminal_frame("resp_red_stable_restore")

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [frame],
        return_request_result?: true
      )

    per_call_downstream =
      downstream("corr-red-stable-restore")
      |> Map.put(:active_turn_reconnect?, false)
      |> Map.put(:owner_turn_id, owner_turn_id)

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             WebsocketOwnerForwarder.submit_request(session, token, per_call_downstream, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request("red-stable-restore")),
               upstream: upstream,
               local_node_string: local_node_string,
               request_id: "red-stable-restore"
             )

    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}

    assert_receive {:websocket_owner_frame, "corr-red-stable-restore", 1, ^owner_turn_id, {:data, ^frame}}

    assert_receive {:websocket_owner_frame, "corr-red-stable-restore", 1, ^owner_turn_id, :complete}

    refute_received {:websocket_owner_frame, "corr-red-stable-restore", 1, _legacy_payload}
    assert {:ok, owner} = WebsocketOwnerSession.lookup(session.id)
    stable_downstream = :sys.get_state(owner).downstream

    assert MapSet.new(Map.keys(stable_downstream)) ==
             MapSet.new([:pid, :epoch, :correlation_id, :active_turn_reconnect?])

    assert is_boolean(stable_downstream.active_turn_reconnect?)
  end

  test "exit-after-first-submit recovery retries with the original public owner turn id", %{
    auth: auth
  } do
    local_node_string = Atom.to_string(node())
    %{session: session, token: token} = owner_session_fixture(auth, local_node_string)
    parent = self()
    release_ref = make_ref()

    first_upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, _request, _writer ->
        send(parent, {:first_public_owner_submit_started, self(), release_ref})

        receive do
          {:release_first_public_owner_submit, ^release_ref} -> :ok
        end
      end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    {:ok, first_owner} = start_owner(session, first_upstream)

    assert {:ok, stable_downstream} =
             WebsocketOwnerSession.attach_downstream(
               first_owner,
               downstream("corr-exit-after-submit")
             )

    terminal_frame = terminal_frame("resp_exit_after_submit")

    recovery_upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame],
        return_request_result?: true
      )

    owner_request = request("exit-after-submit")

    submitter =
      spawn(fn ->
        receive do
          {:submit_public_owner_request, downstream} ->
            result =
              WebsocketOwnerForwarder.submit_request(session, token, downstream, owner_request,
                upstream: recovery_upstream,
                local_node_string: local_node_string,
                request_id: "exit-after-submit"
              )

            send(parent, {:public_owner_retry_result, self(), result})
        end
      end)

    per_call_downstream = Map.put(stable_downstream, :owner_turn_id, submitter)
    send(submitter, {:submit_public_owner_request, per_call_downstream})

    assert_receive {:first_public_owner_submit_started, first_worker, ^release_ref}

    first_owner_ref = Process.monitor(first_owner)
    Process.exit(first_owner, :kill)
    assert_receive {:DOWN, ^first_owner_ref, :process, ^first_owner, :killed}
    send(first_worker, {:release_first_public_owner_submit, release_ref})

    assert_receive {:websocket_owner_runtime_recovered, "corr-exit-after-submit", 1, %{websocket_owner_downstream: recovered_stable}}

    assert MapSet.new(Map.keys(recovered_stable)) ==
             MapSet.new([:pid, :epoch, :correlation_id, :active_turn_reconnect?])

    refute Map.has_key?(recovered_stable, :owner_turn_id)

    assert_receive {:websocket_owner_harness_upstream_started, _recovery_upstream_pid}

    assert_receive {:websocket_owner_frame, "corr-exit-after-submit", 1, ^submitter, {:data, ^terminal_frame}}

    assert_receive {:websocket_owner_frame, "corr-exit-after-submit", 1, ^submitter, :complete}

    assert_receive {:public_owner_retry_result, ^submitter, {:ok, %{terminal: "response.completed", status: 200}}}

    assert {:ok, recovered_owner} = WebsocketOwnerSession.lookup(session.id)
    recovered_owner_state = :sys.get_state(recovered_owner)
    assert recovered_owner_state.downstream == recovered_stable
    refute Map.has_key?(recovered_owner_state.downstream, :owner_turn_id)
  end

  test "bound reset probe does not replace a crashed pre-visible owner", %{auth: auth} do
    local_node_string = Atom.to_string(node())
    %{session: session, token: token} = owner_session_fixture(auth, local_node_string)
    parent = self()
    release_ref = make_ref()

    first_upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, _request, _writer ->
        send(parent, {:bound_owner_submit_started, self(), release_ref})

        receive do
          {:release_bound_owner_submit, ^release_ref} -> :ok
        end
      end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    {:ok, first_owner} = start_owner(session, first_upstream)

    assert {:ok, stable_downstream} =
             WebsocketOwnerSession.attach_downstream(
               first_owner,
               downstream("corr-bound-owner-crash")
             )

    recovery_upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame("resp_bound_owner_crash")],
        return_request_result?: true
      )

    bound_request =
      owner_request(%{request("bound-owner-crash") | reset_probe: bound_reset_probe()})
      |> CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!()

    submitter =
      Task.async(fn ->
        WebsocketOwnerForwarder.submit_request(session, token, stable_downstream, bound_request,
          upstream: recovery_upstream,
          local_node_string: local_node_string,
          request_id: "bound-owner-crash"
        )
      end)

    assert_receive {:bound_owner_submit_started, first_worker, ^release_ref}

    first_owner_ref = Process.monitor(first_owner)
    Process.exit(first_owner, :kill)
    assert_receive {:DOWN, ^first_owner_ref, :process, ^first_owner, :killed}
    send(first_worker, {:release_bound_owner_submit, release_ref})

    assert Task.await(submitter, 2_000) == {:error, :owner_crashed}
    assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session.id)
    refute_received {:websocket_owner_runtime_recovered, _, _, _}
    refute_received {:websocket_owner_harness_upstream_started, _upstream_pid}
  end

  test "remote success reaches simulated owner and returns owner result", %{auth: auth} do
    remote_node = :"codex_pooler@owner-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string)

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: ["remote-delta"])

    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    downstream = attach_downstream(session.id, "corr-remote")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    assert :ok =
             WebsocketOwnerForwarder.submit_frame(session, token, downstream, @frame, opts)

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_submit_frame}}

    assert WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid) == [@frame]
    assert_receive {:websocket_owner_frame, "corr-remote", 1, {:data, "remote-delta"}}
    assert_receive {:websocket_owner_frame, "corr-remote", 1, :complete}
  end

  test "remote request submits to the owner node with owner-local settings", %{auth: auth} do
    previous_operational_settings = Application.get_env(:codex_pooler, OperationalSettings)
    idle_shutdown_ms = 120

    on_exit(fn -> restore_operational_settings(previous_operational_settings) end)
    put_owner_idle_timeout(idle_shutdown_ms)

    remote_node = :"codex_pooler@recover-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string)

    terminal_frame =
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{
          "id" => "resp_recovered_owner",
          "usage" => %{"input_tokens" => 7, "output_tokens" => 5, "total_tokens" => 12}
        }
      })

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame],
        return_request_result?: true
      )

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )
      |> Keyword.put(:local_node_string, remote_node_string)
      |> Keyword.put(:upstream, upstream)

    request =
      %UpstreamWebsocketSession.Request{
        url: "https://example.com/backend-api/codex/responses",
        headers: [],
        payload: "request-frame",
        timeouts: %{}
      }
      |> owner_request()

    {:ok, recovered_owner} =
      start_owner(session, upstream, idle_shutdown_ms: idle_shutdown_ms)

    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    {:ok, recovered_downstream} =
      WebsocketOwnerSession.attach_downstream(
        recovered_owner,
        downstream("corr-recovered-owner")
      )

    assert {:websocket_owner_submission_accepted, {:ok, %{body: body, terminal: "response.completed", status: 200}}} =
             WebsocketOwnerForwarder.submit_request(session, token, recovered_downstream, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), opts)

    assert body =~ "resp_recovered_owner"

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_submit_request_v8, arity: 3}}

    assert [%UpstreamWebsocketSession.Request{payload: "request-frame"}] =
             WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid)

    assert_receive {:websocket_owner_frame, "corr-recovered-owner", 1, {:data, ^terminal_frame}}

    assert_receive {:websocket_owner_frame, "corr-recovered-owner", 1, :complete}

    assert %{idle_shutdown_ms: ^idle_shutdown_ms} = :sys.get_state(recovered_owner)

    new_idle_shutdown_ms = 240
    put_owner_idle_timeout(new_idle_shutdown_ms)

    assert {:ok, existing_owner} = WebsocketOwnerSession.lookup(session.id)
    assert existing_owner == recovered_owner
    assert %{idle_shutdown_ms: ^idle_shutdown_ms} = :sys.get_state(existing_owner)

    %{session: new_session, token: new_token} =
      owner_session_fixture(auth, remote_node_string, "recover-owner-new")

    {:ok, new_owner} =
      start_owner(new_session, upstream, idle_shutdown_ms: new_idle_shutdown_ms)

    assert_receive {:websocket_owner_harness_upstream_started, new_upstream_pid}

    {:ok, new_downstream} =
      WebsocketOwnerSession.attach_downstream(
        new_owner,
        downstream("corr-recovered-owner-new")
      )

    assert {:websocket_owner_submission_accepted, {:ok, %{body: new_body, terminal: "response.completed", status: 200}}} =
             WebsocketOwnerForwarder.submit_request(new_session, new_token, new_downstream, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), opts)

    assert new_body =~ "resp_recovered_owner"

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_submit_request_v8, arity: 3}}

    assert [%UpstreamWebsocketSession.Request{payload: "request-frame"}] =
             WebsocketOwnerNodeHarness.fake_upstream_frames(new_upstream_pid)

    assert new_owner != recovered_owner
    assert %{idle_shutdown_ms: ^new_idle_shutdown_ms} = :sys.get_state(new_owner)
  end

  test "guarded bridge attach and request traverse the remote owner boundary", %{auth: auth} do
    remote_node = :"codex_pooler@bridge-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string)

    terminal_frame =
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => "resp_remote_bridge", "status" => "completed"}
      })

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame],
        return_request_result?: true
      )

    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    attach_args =
      WebsocketOwnerForwarder.remote_attach_args(
        session.id,
        downstream("corr-remote-bridge"),
        reject_if_busy: true
      )

    assert {:ok, %{correlation_id: "corr-remote-bridge", epoch: epoch} = attached} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               attach_args,
               opts
             )

    assert is_integer(epoch)

    reset_probe = bound_reset_probe()

    request =
      %UpstreamWebsocketSession.Request{
        url: "https://example.com/backend-api/codex/responses",
        headers: [],
        payload: "bridge-request-frame",
        timeouts: %{},
        reset_probe: reset_probe
      }
      |> owner_request()

    assert {:websocket_owner_submission_accepted, {:ok, %{terminal: "response.completed", status: 200}}} =
             WebsocketOwnerForwarder.submit_request(session, token, attached, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), opts)

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_attach_downstream, arity: 3}}

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_submit_request_v8, arity: 3}}

    assert [
             %UpstreamWebsocketSession.Request{
               payload: "bridge-request-frame",
               reset_probe: ^reset_probe
             }
           ] =
             WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid)

    assert_receive {:websocket_owner_frame, "corr-remote-bridge", ^epoch, {:data, ^terminal_frame}}

    assert_receive {:websocket_owner_frame, "corr-remote-bridge", ^epoch, :complete}
  end

  test "remote owner acceptance remains causally attached to the RPC result", %{auth: auth} do
    remote_node = :"codex_pooler@acceptance-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string)
    terminal_frame = terminal_frame("resp_remote_acceptance")

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame],
        return_request_result?: true
      )

    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}
    attached = attach_downstream(session.id, "corr-remote-acceptance")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    request = owner_request(request("remote-acceptance"), submission_notification?: true)

    assert {:websocket_owner_submission_accepted, {:ok, %{terminal: "response.completed", status: 200}}} =
             WebsocketOwnerForwarder.submit_request(session, token, attached, CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request), opts)

    refute_received {:remote_submission_observer_ran, _observer_pid}

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_submit_request_v8, arity: 3}}
  end

  test "exact missing v8 RPC fails closed for returned errors and every caught failure kind", %{
    auth: auth
  } do
    remote_node = :"codex_pooler@old-collect-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)

    %{session: session, token: token} =
      owner_session_fixture(auth, remote_node_string, "collect-protocol-mismatch")

    owner_request = owner_request_v2(request("collect-protocol-mismatch")) |> CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!()
    downstream = downstream("corr-collect-protocol-mismatch")
    args = [session.id, downstream, owner_request]

    exact_undef =
      {:exception, :undef, [{WebsocketOwnerForwarder, :remote_submit_request_v8, args, []}]}

    original_session = Repo.get!(CodexSession, session.id)
    original_lease = Repo.get_by!(BridgeOwnerLease, lease_token: token)

    for action <- [
          {:return, {:error, exact_undef}},
          {:error, exact_undef},
          {:exit, exact_undef},
          {:throw, exact_undef}
        ] do
      CurrentFailureKindNodeClient.configure([remote_node], action)

      assert {:error, :owner_unavailable} =
               WebsocketOwnerForwarder.submit_request(session, token, downstream, owner_request,
                 node_client: CurrentFailureKindNodeClient,
                 app_node_names: [remote_node_string]
               )

      assert_receive {:current_failure_kind_call, ^remote_node, :remote_submit_request_v8, 3}

      for obsolete <- [:remote_submit_request, :remote_submit_request_v1, :remote_submit_request_v2, :remote_submit_request_v3, :remote_submit_request_v4, :remote_submit_request_v5, :remote_submit_request_v6, :remote_submit_request_v7] do
        refute_received {:current_failure_kind_call, ^remote_node, ^obsolete, _arity}
      end

      assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session.id)

      assert Repo.get!(CodexSession, session.id).owner_lease_token ==
               original_session.owner_lease_token

      assert Repo.get!(BridgeOwnerLease, original_lease.id).status == "active"
    end

    for unrelated <- [
          {:exception, :undef,
           [
             {WebsocketOwnerRequestV2, :nested_missing, [], []},
             {WebsocketOwnerForwarder, :remote_submit_request_v8, args, []}
           ]},
          {:exception, :undef,
           [
             {WebsocketOwnerForwarder, :remote_submit_request_v8, [session.id, downstream, owner_request.request], []}
           ]}
        ] do
      CurrentFailureKindNodeClient.configure([remote_node], {:exit, unrelated})

      assert {:error, :owner_crashed} =
               WebsocketOwnerForwarder.submit_request(session, token, downstream, owner_request,
                 node_client: CurrentFailureKindNodeClient,
                 app_node_names: [remote_node_string]
               )

      assert_receive {:current_failure_kind_call, ^remote_node, :remote_submit_request_v8, 3}
      refute_received {:current_failure_kind_call, ^remote_node, :remote_submit_request_v2, _arity}
      assert {:error, :owner_unavailable} = WebsocketOwnerSession.lookup(session.id)

      assert Repo.get!(CodexSession, session.id).owner_lease_token ==
               original_session.owner_lease_token

      assert Repo.get!(BridgeOwnerLease, original_lease.id).status == "active"
    end
  end

  test "current remote entrypoint rejects unguarded inner requests before owner submission", %{auth: auth} do
    local_node_string = Atom.to_string(node())
    %{session: session} = owner_session_fixture(auth, local_node_string, "legacy-reject")
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self())
    {:ok, owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    inner = owner_request(request("legacy-reject"))
    envelope = CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(inner)
    assert :ok = WebsocketOwnerRequestV8.validate(envelope)

    for invalid <- [
          request("legacy-reject"),
          inner,
          Map.from_struct(inner),
          Map.from_struct(envelope),
          %{envelope | version: 7},
          %{envelope | request: Map.from_struct(inner)},
          %{envelope | provider_credits_context: nil},
          %{envelope | provider_credits_context: Map.from_struct(envelope.provider_credits_context)},
          %{envelope | provider_credits_context: %{envelope.provider_credits_context | request_id: Ecto.UUID.generate()}},
          %{envelope | provider_credits_context: %{envelope.provider_credits_context | attempt_id: Ecto.UUID.generate()}},
          %{envelope | provider_credits_context: %{envelope.provider_credits_context | serving_mode: :lite}},
          %{envelope | provider_credits_context: %{envelope.provider_credits_context | upstream_identity_id: Ecto.UUID.generate()}}
        ] do
      assert {:error, :owner_unavailable} =
               WebsocketOwnerForwarder.remote_submit_request_v8(
                 session.id,
                 downstream("corr-legacy-reject"),
                 invalid
               )
    end

    assert WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid) == []
    assert :ok = WebsocketOwnerForwarder.remote_submit_request_v8(session.id, attach_downstream(session.id, "corr-guarded-control"), envelope)
    assert [%UpstreamWebsocketSession.Request{payload: "legacy-reject"}] = WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid)

    assert Process.alive?(owner)
  end

  test "only an exact top-frame missing v8 entrypoint maps to owner unavailable", %{auth: auth} do
    remote_node = :"codex_pooler@protocol-mismatch-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)

    %{session: session, token: token} =
      owner_session_fixture(auth, remote_node_string, "protocol-mismatch")

    owner_request = owner_request(request("protocol-mismatch")) |> CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!()
    downstream = downstream("corr-protocol-mismatch")
    args = [session.id, downstream, owner_request]

    exact_undef =
      {:exception, :undef, [{WebsocketOwnerForwarder, :remote_submit_request_v8, args, []}]}

    exact_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => {:return, {:error, exact_undef}}}
      )

    log =
      capture_log(fn ->
        assert {:error, :owner_unavailable} =
                 WebsocketOwnerForwarder.submit_request(session, token, downstream, owner_request, exact_opts)
      end)

    assert log =~ "event=owner_protocol_incompatible"

    inner_undef =
      {:exception, :undef,
       [
         {WebsocketOwnerRequest, :nested_missing_function, [], []},
         {WebsocketOwnerForwarder, :remote_submit_request_v8, args, []}
       ]}

    inner_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => {:return, {:error, inner_undef}}}
      )

    inner_log =
      capture_log(fn ->
        assert {:error, :owner_crashed} =
                 WebsocketOwnerForwarder.submit_request(session, token, downstream, owner_request, inner_opts)
      end)

    refute inner_log =~ "event=owner_protocol_incompatible"
  end

  test "reconnect control maps only exact outer undef to old-owner unavailable", %{auth: auth} do
    remote_node = :"codex_pooler@control-mismatch-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string, "control")
    downstream = downstream("corr-control")
    semantic_turn_key = :crypto.hash(:sha256, "opaque-control-key")
    control_ref = make_ref()

    control =
      WebsocketOwnerForwarder.reconnect_control(
        :preflight,
        session.id,
        downstream,
        semantic_turn_key,
        control_ref
      )

    args = [control]

    exact_undef =
      {:exception, :undef, [{WebsocketOwnerForwarder, :remote_reconnect_control_v1, args, []}]}

    exact_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => {:return, {:error, exact_undef}}}
      )

    assert {:error, :owner_unavailable} =
             WebsocketOwnerForwarder.preflight_reconnect(
               session,
               token,
               downstream,
               semantic_turn_key,
               control_ref,
               exact_opts
             )

    inner_undef =
      {:exception, :undef,
       [
         {WebsocketOwnerSession, :nested_missing_function, [], []},
         {WebsocketOwnerForwarder, :remote_reconnect_control_v1, args, []}
       ]}

    inner_opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => {:return, {:error, inner_undef}}}
      )

    assert {:error, :owner_crashed} =
             WebsocketOwnerForwarder.preflight_reconnect(
               session,
               token,
               downstream,
               semantic_turn_key,
               control_ref,
               inner_opts
             )
  end

  test "local and simulated remote reconnect controls return the same idle decision", %{
    auth: auth
  } do
    local_node = Atom.to_string(node())

    %{session: local_session, token: local_token} =
      owner_session_fixture(auth, local_node, "local-control-parity")

    assert {:ok, _local_owner} =
             start_owner(local_session, WebsocketOwnerNodeHarness.fake_upstream_boundary(self()))

    assert_receive {:websocket_owner_harness_upstream_started, _local_upstream_pid}
    local_downstream = attach_downstream(local_session.id, "local-control-parity")
    local_key = :crypto.hash(:sha256, "local-control-parity")

    assert {:ok, :dispatch} =
             WebsocketOwnerForwarder.preflight_reconnect(
               local_session,
               local_token,
               local_downstream,
               local_key,
               make_ref()
             )

    remote_node = :"codex_pooler@remote-control-parity-owner-app.example"

    %{session: remote_session, token: remote_token} =
      owner_session_fixture(auth, Atom.to_string(remote_node), "remote-control-parity")

    assert {:ok, _remote_owner} =
             start_owner(remote_session, WebsocketOwnerNodeHarness.fake_upstream_boundary(self()))

    assert_receive {:websocket_owner_harness_upstream_started, _remote_upstream_pid}
    remote_downstream = attach_downstream(remote_session.id, "remote-control-parity")
    remote_key = :crypto.hash(:sha256, "remote-control-parity")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    assert {:ok, :dispatch} =
             WebsocketOwnerForwarder.preflight_reconnect(
               remote_session,
               remote_token,
               remote_downstream,
               remote_key,
               make_ref(),
               opts
             )
  end

  test "remote reconnect timeout cancels the exact pending owner handoff", %{auth: auth} do
    remote_node = :"codex_pooler@reconnect-timeout-owner-app.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote_node), "reconnect-timeout")

    parent = self()
    release_ref = make_ref()

    upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, _request, _writer ->
        Process.flag(:trap_exit, true)
        send(parent, {:reconnect_timeout_turn_started, self()})

        await_reconnect_timeout_release(release_ref)
      end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    assert {:ok, owner_pid} = start_owner(session, upstream)

    {:ok, first_downstream} =
      WebsocketOwnerSession.attach_downstream(owner_pid, downstream("reconnect-timeout-a"))

    payload =
      CodexPooler.JSON.encode!(%{
        "type" => "response.create",
        "model" => "gpt-example",
        "input" => [],
        "turn_id" => "turn-a"
      })

    native_request = %{
      request(payload)
      | message_mapper: &StreamProtocol.canonicalize_native_codex_responses_json_message/1
    }

    submitter =
      spawn(fn ->
        result = WebsocketOwnerSession.submit_request(owner_pid, first_downstream, native_request)

        send(parent, {:reconnect_timeout_old_result, result})

        receive do
          :release_reconnect_timeout_submitter -> :ok
        end
      end)

    assert_receive {:reconnect_timeout_turn_started, old_turn_pid}
    assert :ok = WebsocketOwnerSession.detach_downstream(owner_pid, first_downstream)

    assert %{
             active_turn: %{
               canceled_result: {:error, :client_disconnected},
               descriptor: %{kind: :native, semantic_turn_key: active_turn_key}
             }
           } = :sys.get_state(owner_pid)

    assert {:ok, replacement_downstream} =
             WebsocketOwnerSession.attach_downstream(
               owner_pid,
               downstream("reconnect-timeout-b")
             )

    semantic_turn_key =
      :crypto.hash(:sha256, session.id <> <<0>> <> "turn-b")

    refute semantic_turn_key == active_turn_key

    control_ref = make_ref()

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.preflight_reconnect(
               session,
               token,
               replacement_downstream,
               semantic_turn_key,
               control_ref,
               node_client: ReconnectTimeoutNodeClient,
               app_node_names: [Atom.to_string(remote_node)],
               timeout: 25
             )

    assert_receive {:remote_reconnect_preflight_processed, {:ok, :replacement_handoff, ^control_ref}}

    assert_receive {:remote_reconnect_cancel_processed, :ok}
    assert_receive {:reconnect_timeout_old_result, {:error, :client_disconnected}}
    assert %{pending_handoff: nil, active_turn: nil} = :sys.get_state(owner_pid)
    refute_received {:websocket_owner_handoff_ready, _, _, _, _, _}
    refute_received {:websocket_owner_handoff_failed, _, _, _, _, _, _}

    send(submitter, :release_reconnect_timeout_submitter)
    send(old_turn_pid, {:release_reconnect_timeout_turn, release_ref})
  end

  test "simulated remote reconnect becomes ready only after predecessor caller exits and consumes once",
       %{auth: auth} do
    remote_node = :"codex_pooler@remote-control-parity-owner-app.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote_node), "reconnect-ready")

    parent = self()
    counter = :counters.new(1, [:atomics])

    upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, _request, _writer ->
        :counters.add(counter, 1, 1)
        count = :counters.get(counter, 1)
        Process.flag(:trap_exit, true)
        send(parent, {:remote_reconnect_send, count, self()})

        if count == 1 do
          receive do
            {:EXIT, _from, :shutdown} ->
              receive do
                :never_release -> :ok
              end
          end
        end

        :ok
      end,
      invalidate: fn _upstream_pid -> send(parent, :remote_reconnect_invalidated) end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    assert {:ok, owner_pid} =
             start_owner(session, upstream,
               handoff_soft_timeout_ms: 25,
               handoff_absolute_timeout_ms: 2_000
             )

    {:ok, first_downstream} =
      WebsocketOwnerSession.attach_downstream(owner_pid, downstream("reconnect-ready-a"))

    first_request = native_request("turn-a")

    submitter =
      spawn(fn ->
        result = WebsocketOwnerSession.submit_request(owner_pid, first_downstream, first_request)
        send(parent, {:remote_reconnect_predecessor_result, result})

        receive do
          :release_remote_reconnect_submitter -> :ok
        end
      end)

    assert_receive {:remote_reconnect_send, 1, _old_turn_pid}
    assert :ok = WebsocketOwnerSession.detach_downstream(owner_pid, first_downstream)

    assert {:ok, replacement_downstream} =
             WebsocketOwnerSession.attach_downstream(
               owner_pid,
               downstream("reconnect-ready-b")
             )

    control_ref = make_ref()
    replacement_key = :crypto.hash(:sha256, session.id <> <<0>> <> "turn-b")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    assert {:ok, :replacement_handoff, ^control_ref} =
             WebsocketOwnerForwarder.preflight_reconnect(
               session,
               token,
               replacement_downstream,
               replacement_key,
               control_ref,
               opts
             )

    assert_receive :remote_reconnect_invalidated
    assert_receive {:remote_reconnect_predecessor_result, {:error, :client_disconnected}}
    refute_received {:websocket_owner_handoff_ready, _, _, _, _, ^control_ref}

    send(submitter, :release_remote_reconnect_submitter)

    assert_receive {:websocket_owner_handoff_ready, "reconnect-ready-b", 2, owner_turn_id, downstream_pid, ^control_ref}

    assert is_pid(owner_turn_id)
    assert downstream_pid == self()

    assert :ok =
             WebsocketOwnerSession.submit_request(
               owner_pid,
               replacement_downstream,
               native_request("turn-b")
             )

    assert_receive {:remote_reconnect_send, 2, _replacement_turn_pid}
    assert_receive {:websocket_owner_frame, "reconnect-ready-b", 2, :complete}
    assert %{pending_handoff: nil, active_turn: nil} = :sys.get_state(owner_pid)
    refute_received {:remote_reconnect_send, 3, _unexpected_turn_pid}
  end

  test "accepted malformed remote owner results retain submission causality", %{auth: auth} do
    remote_node = :"codex_pooler@malformed-acceptance-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)

    for {suffix, malformed_result} <-
          [unknown: :unexpected, unknown_error: {:error, :unexpected_owner_error}] do
      %{session: session, token: token} =
        owner_session_fixture(auth, remote_node_string, "malformed-acceptance-#{suffix}")

      opts =
        WebsocketOwnerNodeHarness.node_client_opts([remote_node],
          calls: %{
            remote_node => {:return, {:websocket_owner_submission_accepted, malformed_result}}
          }
        )

      assert {:websocket_owner_submission_accepted, {:error, :owner_crashed}} =
               WebsocketOwnerForwarder.submit_request(session, token, downstream("corr-malformed-acceptance-#{suffix}"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(owner_request(request("malformed-acceptance-#{suffix}"))), opts)
    end
  end

  test "remote timeout maps to owner_forward_timeout within configured timeout", %{auth: auth} do
    remote_node = :"codex_pooler@timeout-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string)
    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self())
    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}

    downstream = attach_downstream(session.id, "corr-timeout")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :timeout}
      )

    started = System.monotonic_time(:millisecond)

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.submit_frame(
               session,
               token,
               downstream,
               @frame,
               Keyword.put(opts, :timeout, 25)
             )

    elapsed = System.monotonic_time(:millisecond) - started
    assert elapsed < 250
    refute_receive {:websocket_owner_frame, "corr-timeout", 1, _payload}
  end

  test "nodedown and crash map to safe owner errors", %{auth: auth} do
    nodedown_node = :"codex_pooler@nodedown-app.example"
    crash_node = :"codex_pooler@crash-app.example"

    %{session: nodedown_session, token: nodedown_token} =
      owner_session_fixture(auth, Atom.to_string(nodedown_node), "nodedown")

    %{session: crash_session, token: crash_token} =
      owner_session_fixture(auth, Atom.to_string(crash_node), "crash")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([nodedown_node, crash_node],
        calls: %{nodedown_node => :nodedown, crash_node => :crash}
      )

    assert {:error, :owner_unavailable} =
             WebsocketOwnerForwarder.submit_frame(
               nodedown_session,
               nodedown_token,
               downstream("corr-nodedown"),
               @frame,
               opts
             )

    assert {:error, :owner_crashed} =
             WebsocketOwnerForwarder.submit_frame(
               crash_session,
               crash_token,
               downstream("corr-crash"),
               @frame,
               opts
             )
  end

  test "call_remote normalizes raw erpc timeout and connection failures", %{auth: auth} do
    timeout_node = :"codex_pooler@raw-timeout.example"
    noconnection_node = :"codex_pooler@raw-noconnection.example"
    noproc_node = :"codex_pooler@raw-noproc.example"
    nodedown_node = :"codex_pooler@raw-nodedown.example"

    %{session: session} = owner_session_fixture(auth, Atom.to_string(timeout_node), "raw")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts(
        [timeout_node, noconnection_node, noproc_node, nodedown_node],
        calls: %{
          timeout_node => :raw_timeout,
          noconnection_node => :raw_noconnection,
          noproc_node => :raw_noproc,
          nodedown_node => :raw_nodedown
        }
      )

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.call_remote(
               timeout_node,
               :remote_attach_downstream,
               [session.id, downstream("corr-raw-timeout")],
               Keyword.put(opts, :timeout, 25)
             )

    for {node, correlation_id} <- [
          {noconnection_node, "corr-raw-noconnection"},
          {noproc_node, "corr-raw-noproc"},
          {nodedown_node, "corr-raw-nodedown"}
        ] do
      assert {:error, :owner_unavailable} =
               WebsocketOwnerForwarder.call_remote(
                 node,
                 :remote_attach_downstream,
                 [session.id, downstream(correlation_id)],
                 opts
               )
    end
  end

  # An owner call that timed out, or found its owner gone, inside the remote
  # worker reaches the caller as the erpc exit `{:exception, exit_reason}`: it
  # keeps its own cause instead of the crash default (findings#270 row 270-250).
  test "normalize_remote_failure keeps an owner call's timeout and an owner gone inside the remote worker" do
    call = {GenServer, :call, [self(), :any_owner_call, 1_000]}

    for {reason, expected} <- [
          {{:exception, {:timeout, call}}, :owner_forward_timeout},
          {{:exception, {:noproc, call}}, :owner_unavailable},
          {{:exception, {:normal, call}}, :owner_unavailable},
          {{:exception, {:shutdown, call}}, :owner_unavailable},
          {{:exception, {{:shutdown, :stale_owner}, call}}, :owner_unavailable},
          {{:exception, {:killed, call}}, :owner_crashed},
          {{:exception, :unrelated_exit}, :owner_crashed}
        ] do
      assert WebsocketOwnerForwarder.normalize_remote_failure(:exit, reason, WebsocketOwnerForwarder, :remote_reconnect_control_v1, [%{}]) == expected,
             inspect(reason)
    end
  end

  # A local owner's admission control answer reaches the socket unchanged; a
  # remote one crosses `call_remote`, whose generic normalization folds every
  # reason outside the owner vocabulary into `owner_crashed`. Every refusal of
  # `NativeCompactionAdmission` must come back as the local owner gives it
  # (a remote `compaction_item_mismatch` read as a crash answered 503 instead
  # of 409), and nothing else may widen the pass-through (findings#206 rows
  # 206-397/206-400).
  test "a remote owner's native compaction admission refusals come back with their own reason", %{auth: auth} do
    remote_node = :"codex_pooler@admission-refusal-owner-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string, "admission-refusal")
    downstream = downstream("corr-admission-refusal")
    opts = [node_client: CurrentFailureKindNodeClient, app_node_names: [remote_node_string]]

    assert {:ok, control} =
             WebsocketOwnerAdmissionControlV1.new(%{
               version: 1,
               action: :snapshot,
               downstream: downstream,
               binding: nil,
               phase: nil,
               control_ref: nil,
               capability: nil,
               disposition: nil,
               success?: nil,
               compaction_item_digest: nil,
               confirmation: nil,
               first_compact_collection: nil,
               expires_at_ms: nil,
               now_ms: nil
             })

    refusals = NativeCompactionAdmission.refusal_reasons()
    assert :compaction_item_mismatch in refusals
    assert refusals -- WebsocketOwnerContract.owner_errors() == refusals

    # The owner session's own refusals outside the admission (a stale or
    # drained owner) keep the owner vocabulary.
    for reason <- refusals ++ [:stale_downstream, :owner_drained, :owner_unavailable] do
      CurrentFailureKindNodeClient.configure([remote_node], {:return, {:error, reason}})
      assert {:error, ^reason} = WebsocketOwnerForwarder.admission_control(session, token, control, opts)
      assert_received {:current_failure_kind_call, ^remote_node, :remote_admission_control_v1, 2}
    end

    # Controls: an unknown reason from the admission call, and an admission
    # refusal answered by another remote call, still read `owner_crashed`.
    CurrentFailureKindNodeClient.configure([remote_node], {:return, {:error, :not_an_admission_refusal}})
    assert {:error, :owner_crashed} = WebsocketOwnerForwarder.admission_control(session, token, control, opts)

    for reason <- refusals do
      CurrentFailureKindNodeClient.configure([remote_node], {:return, {:error, reason}})

      assert {:error, :owner_crashed} =
               WebsocketOwnerForwarder.call_remote(remote_node, :remote_attach_downstream, [session.id, downstream], opts)
    end
  end

  # An owner answer outside both vocabularies is folded at the owner's node,
  # before it crosses: a local owner used to hand it to the caller raw and a
  # remote one as `owner_crashed`, so the same refusal meant two different
  # things by topology. Both now read the admission's `invalid_transition`
  # and log the unlisted reason (findings#206 row 206-402).
  test "an owner refusal outside the admission and owner vocabularies reads invalid_transition from a local and a remote owner alike",
       %{auth: auth} do
    %{session: session, token: token} = owner_session_fixture(auth, Atom.to_string(node()), "admission-unlisted")
    test_pid = self()

    {:ok, owner} =
      Task.start(fn ->
        {:ok, _registration} = Registry.register(WebsocketOwnerSession.Registry, session.id, nil)
        send(test_pid, :unlisted_owner_registered)
        unlisted_owner_loop()
      end)

    on_exit(fn -> Process.exit(owner, :kill) end)
    assert_receive :unlisted_owner_registered

    downstream = downstream("corr-admission-unlisted")

    assert {:ok, control} =
             WebsocketOwnerAdmissionControlV1.new(%{
               version: 1,
               action: :snapshot,
               downstream: downstream,
               binding: nil,
               phase: nil,
               control_ref: nil,
               capability: nil,
               disposition: nil,
               success?: nil,
               compaction_item_digest: nil,
               confirmation: nil,
               first_compact_collection: nil,
               expires_at_ms: nil,
               now_ms: nil
             })

    refute NativeCompactionAdmission.refusal_reason?(:synthetic_unlisted_refusal)
    refute WebsocketOwnerContract.owner_error?(:synthetic_unlisted_refusal)

    local_log =
      capture_log(fn ->
        assert {:error, :invalid_transition} = WebsocketOwnerForwarder.admission_control(session, token, control)
      end)

    assert_received {:unlisted_owner_answered, :snapshot}

    # The remote path: the calling node reaches the owner's node through the
    # node client, which runs `remote_admission_control_v1/2` there.
    remote = :"codex_pooler@admission-unlisted-owner-app.example"
    remote_session = %{session | owner_instance_id: Atom.to_string(remote)}
    opts = WebsocketOwnerNodeHarness.node_client_opts([remote], calls: %{remote => :success})

    remote_log =
      capture_log(fn ->
        assert {:error, :invalid_transition} = WebsocketOwnerForwarder.admission_control(remote_session, token, control, opts)
      end)

    assert_received {:unlisted_owner_answered, :snapshot}
    assert_receive {:websocket_owner_harness_node_call, %{function: :remote_admission_control_v1}}

    for log <- [local_log, remote_log] do
      assert log =~ "native compaction admission refusal outside vocabulary"
      assert log =~ "action=snapshot reason_code=synthetic_unlisted_refusal answered=invalid_transition"
    end

    # A listed refusal from the same owner is still passed through unchanged.
    send(owner, {:answer_next, :compaction_item_mismatch})
    assert {:error, :compaction_item_mismatch} = WebsocketOwnerForwarder.admission_control(remote_session, token, control, opts)
  end

  defp unlisted_owner_loop(answer \\ :synthetic_unlisted_refusal) do
    receive do
      {:answer_next, next} ->
        unlisted_owner_loop(next)

      {:"$gen_call", from, {:admission_control_v1, control}} ->
        send(from |> elem(0), {:unlisted_owner_answered, control.action})
        GenServer.reply(from, {:error, answer})
        unlisted_owner_loop()
    end
  end

  test "gateway remote detach treats stale downstream as caller safe", %{auth: auth} do
    remote_node = :"codex_pooler@detach-stale-app.example"
    remote_node_string = Atom.to_string(remote_node)

    %{session: session, token: token} =
      owner_session_fixture(auth, remote_node_string, "detach-stale")

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: ["after-stale-detach"])

    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    assert {:ok, stale_downstream} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               [session.id, downstream("corr-stale-detach")],
               opts
             )

    assert {:ok, current_downstream} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               [session.id, downstream("corr-current-detach")],
               opts
             )

    gateway_opts = %{websocket_owner_forwarder_opts: opts}

    assert :detached_stale_downstream =
             Gateway.detach_websocket_owner_downstream(
               session,
               token,
               stale_downstream,
               gateway_opts
             )

    assert :detached_stale_downstream =
             Gateway.detach_websocket_owner_downstream(
               session,
               token,
               stale_downstream,
               gateway_opts
             )

    assert :ok =
             WebsocketOwnerForwarder.submit_frame(
               session,
               token,
               current_downstream,
               @frame,
               opts
             )

    assert_receive {:websocket_owner_frame, "corr-current-detach", 2, {:data, "after-stale-detach"}}
  end

  # findings#206 row 206-245: the closing socket's early detach keeps its
  # one-second budget because an answer lost to it converges. The owner still
  # fences the downstream once it gets to the call, and the ordinary detach the
  # socket then sends after its drain reads that fence as a stale downstream:
  # no owner-lost recovery and no turn interrupt, as when the answer arrives.
  test "a remote early detach answered after its budget leaves the fence to the ordinary detach", %{auth: auth} do
    Process.register(self(), :late_early_detach_test)
    remote_node = :"codex_pooler@late-early-detach.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote_node), "late-early-detach")

    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: [])
    {:ok, owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}

    opts = [node_client: LateEarlyDetachNodeClient]

    assert {:ok, closing} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               [session.id, downstream("corr-late-early-detach")],
               opts
             )

    gateway_opts = %{websocket_owner_forwarder_opts: opts}

    assert :not_previsible =
             Gateway.detach_previsible_websocket_owner_downstream(session, token, closing, gateway_opts)

    assert_receive {:late_early_detach_answer, :detached}, @peer_detection_timeout_ms
    fenced = Map.take(closing, [:pid, :epoch, :correlation_id])
    assert %{downstream: nil, closed_downstream: ^fenced} = :sys.get_state(owner)

    assert :detached_stale_downstream =
             Gateway.detach_websocket_owner_downstream(session, token, closing, gateway_opts)

    assert %{downstream: nil, closed_downstream: ^fenced, active_turn: nil} = :sys.get_state(owner)
  end

  test "unknown malicious owner_instance_id does not create atoms", %{auth: auth} do
    malicious_owner =
      "malicious-owner-#{System.unique_integer([:positive])}@not-connected.example"

    %{session: session, token: token} = owner_session_fixture(auth, malicious_owner)
    refute existing_atom?(malicious_owner)

    opts = WebsocketOwnerNodeHarness.node_client_opts([])

    assert {:error, :owner_unavailable} =
             WebsocketOwnerForwarder.submit_frame(
               session,
               token,
               downstream("corr-malicious"),
               @frame,
               opts
             )

    refute existing_atom?(malicious_owner)
  end

  test "worker scheduler and migration node strings are rejected even when connected", %{
    auth: auth
  } do
    role_nodes = [
      :"codex_pooler@sample-worker-0.cluster.local",
      :"codex_pooler@sample-scheduler-0.cluster.local",
      :"codex_pooler@sample-migration-0.cluster.local"
    ]

    opts = WebsocketOwnerNodeHarness.node_client_opts(role_nodes)

    for {node, suffix} <- Enum.with_index(role_nodes) do
      %{session: session, token: token} =
        owner_session_fixture(auth, Atom.to_string(node), "role-#{suffix}")

      assert {:error, :owner_unavailable} =
               WebsocketOwnerForwarder.submit_frame(
                 session,
                 token,
                 downstream("corr-role-#{suffix}"),
                 @frame,
                 opts
               )
    end

    refute_received {:websocket_owner_harness_node_call, _call}
  end

  @tag :continuation_generation_boundary
  test "real peer owner guards a replacement generation before proxy settlement" do
    peer_node = start_current_peer!("continuation_guard_owner")
    terminal = native_retry_terminal()
    release_ref = make_ref()

    # Strict finite scenario: the warmup rides the first physical connection;
    # the guarded continuation must send nothing, and the retry must land on
    # the replacement connection opened after invalidation.
    upstream =
      start_fake_upstream(
        FakeUpstream.strict_sequence([
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            websocket_connection_ordinal: 1,
            respond: websocket_success_without_id()
          ),
          FakeUpstream.expect_request(
            method: "WEBSOCKET",
            websocket_connection_ordinal: 2,
            respond: websocket_success_without_id()
          )
        ])
      )

    persistence =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    downstream_sender =
      :erpc.call(
        peer_node,
        WebsocketOwnerNodeHarness,
        :terminal_barrier_downstream_sender,
        [self(), terminal, release_ref]
      )

    session_id = "real-peer-continuation-guard"

    assert {:ok, owner_pid} =
             :erpc.call(peer_node, WebsocketOwnerSession, :start_owner, [
               [
                 codex_session_id: session_id,
                 owner_lease_token: "synthetic-continuation-owner-token",
                 owner_instance_id: Atom.to_string(peer_node),
                 owner_renewal_ms: 60_000,
                 downstream_sender: downstream_sender,
                 persistence: persistence
               ]
             ])

    assert node(owner_pid) == peer_node

    client = WebsocketOwnerForwarder.ERPCNodeClient

    assert {:ok, attached} =
             client.call_owner(
               peer_node,
               WebsocketOwnerForwarder,
               :remote_attach_downstream,
               [session_id, downstream("corr-peer-continuation-guard")],
               @peer_detection_timeout_ms
             )

    warmup = request("warmup-request", FakeUpstream.url(upstream))

    assert {:ok, %{terminal: "response.completed", status: 200}} =
             client.call_owner(
               peer_node,
               WebsocketOwnerSession,
               :submit_request,
               [owner_pid, attached, warmup],
               @peer_detection_timeout_ms
             )

    assert_receive {:websocket_owner_frame, "corr-peer-continuation-guard", 1, {:data, _warmup_terminal}}

    assert_receive {:websocket_owner_frame, "corr-peer-continuation-guard", 1, :complete}

    upstream_pid =
      :erpc.call(peer_node, :erlang, :map_get, [:upstream_pid, :sys.get_state(owner_pid)])

    assert :ok =
             :erpc.call(peer_node, UpstreamWebsocketSession, :invalidate_connection, [
               upstream_pid
             ])

    guarded_request = %{warmup | connection_bound_continuation?: true}

    guarded_submit =
      Task.async(fn ->
        client.call_owner(
          peer_node,
          WebsocketOwnerSession,
          :submit_request,
          [owner_pid, attached, guarded_request],
          @peer_detection_timeout_ms
        )
      end)

    assert_receive {:websocket_owner_harness_terminal_delivery_barrier, barrier_pid, ^release_ref}
    assert Task.yield(guarded_submit, 0) == nil

    refute_received {:websocket_owner_frame, "corr-peer-continuation-guard", 1, {:data, ^terminal}}

    assert length(FakeUpstream.requests(upstream)) == 1

    send(barrier_pid, {:websocket_owner_harness_release_terminal_delivery, release_ref})

    assert_receive {:websocket_owner_frame, "corr-peer-continuation-guard", 1, {:data, ^terminal}}

    assert_receive {:websocket_owner_harness_terminal_delivered, ^release_ref}

    assert {:ok, guarded_result} = Task.await(guarded_submit, @peer_detection_timeout_ms)
    assert guarded_result.terminal == "error"
    assert guarded_result.upstream_error_code == "previous_response_not_found"
    assert guarded_result.upstream_websocket_connection.reconnected
    assert_receive {:websocket_owner_frame, "corr-peer-continuation-guard", 1, :complete}

    refute_received {:websocket_owner_frame, "corr-peer-continuation-guard", 1, {:data, ^terminal}}

    refute_received {:websocket_owner_frame, "corr-peer-continuation-guard", 1, :complete}
    assert length(FakeUpstream.requests(upstream)) == 1

    assert {:ok, retry_result} =
             client.call_owner(
               peer_node,
               WebsocketOwnerSession,
               :submit_request,
               [owner_pid, attached, warmup],
               @peer_detection_timeout_ms
             )

    assert retry_result.terminal == "response.completed"
    assert retry_result.upstream_websocket_connection.reused
    assert_receive {:websocket_owner_frame, "corr-peer-continuation-guard", 1, {:data, _retry}}
    assert_receive {:websocket_owner_frame, "corr-peer-continuation-guard", 1, :complete}

    assert [warmup_request, retry_request] = FakeUpstream.requests(upstream)
    assert warmup_request.websocket_connection_id != retry_request.websocket_connection_id
    assert FakeUpstream.websocket_connection_count(upstream) == 2
    assert :ok = FakeUpstream.verify!(upstream)
  end

  test "remote owner cancels an active turn when the local forwarding proxy dies", %{
    auth: auth
  } do
    remote_node = :"codex_pooler@cancel-owner-app.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(remote_node), "remote-cancel")

    parent = self()
    release_ref = make_ref()

    upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, _request, _writer ->
        send(parent, {:remote_cancel_turn_started, self(), release_ref})

        receive do
          {:release_remote_cancel_turn, ^release_ref} -> :ok
        end
      end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    assert {:ok, owner_pid} =
             start_owner(session, upstream)

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :success}
      )

    assert {:ok, attached} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               [session.id, downstream("corr-real-peer-cancel")],
               opts
             )

    remote_cancel_request =
      request("remote-cancel") |> owner_request()

    remote_cancel_request = CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(remote_cancel_request)

    submitter =
      Task.async(fn ->
        WebsocketOwnerNodeHarness.with_node_client(
          [remote_node],
          [calls: %{remote_node => :success}],
          fn task_opts ->
            WebsocketOwnerForwarder.submit_request(session, token, attached, remote_cancel_request, task_opts)
          end
        )
      end)

    assert_receive {:remote_cancel_turn_started, remote_turn_task, ^release_ref}

    assert %{active_turn: %{task_pid: ^remote_turn_task}} = :sys.get_state(owner_pid)

    remote_turn_monitor = Process.monitor(remote_turn_task)

    Task.shutdown(submitter, :brutal_kill)

    assert_receive {:DOWN, ^remote_turn_monitor, :process, ^remote_turn_task, :shutdown},
                   @peer_detection_timeout_ms

    await_owner_cancellation!(owner_pid)
  end

  test "owner_drained remote cancellation stops only the matching turn and preserves owner reuse",
       %{
         auth: auth
       } do
    remote_node = :"codex_pooler@drain-cancel-owner-app.example"

    %{session: session} =
      owner_session_fixture(auth, Atom.to_string(remote_node), "remote-drain-cancel")

    parent = self()
    release_ref = make_ref()

    upstream = %{
      start: fn -> Agent.start_link(fn -> :ready end) end,
      send: fn _upstream_pid, request, _writer ->
        send(parent, {:remote_drain_turn_started, self(), request})

        receive do
          {:release_remote_drain_turn, ^release_ref} -> :ok
        end
      end,
      close: fn upstream_pid ->
        if Process.alive?(upstream_pid), do: Agent.stop(upstream_pid)
      end
    }

    assert {:ok, owner_pid} = start_owner(session, upstream)

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node], calls: %{remote_node => :success})

    assert {:ok, stable_downstream} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               [session.id, downstream("corr-drain-cancel")],
               opts
             )

    owner_turn_id = spawn(fn -> receive do: (:stop -> :ok) end)
    downstream = Map.put(stable_downstream, :owner_turn_id, owner_turn_id)
    owner_request = request("remote-drain-cancel")

    submitter =
      Task.async(fn ->
        WebsocketOwnerSession.submit_request(
          owner_pid,
          downstream,
          owner_request,
          true
        )
      end)

    assert_receive {:remote_drain_turn_started, remote_turn_task, _request}

    wrong_turn_id = spawn(fn -> receive do: (:stop -> :ok) end)

    assert {:error, :stale_downstream} =
             WebsocketOwnerSession.cancel_downstream(
               owner_pid,
               Map.put(downstream, :owner_turn_id, wrong_turn_id),
               :owner_drained
             )

    assert %{active_turn: %{task_pid: ^remote_turn_task}, downstream: ^stable_downstream} =
             :sys.get_state(owner_pid)

    send(wrong_turn_id, :stop)

    assert :ok =
             WebsocketOwnerForwarder.cancel_remote_downstream(
               remote_node,
               session.id,
               downstream,
               :owner_drained,
               opts
             )

    assert {:websocket_owner_submission_accepted, {:error, :owner_drained}} =
             Task.await(submitter, @peer_detection_timeout_ms)

    assert Process.alive?(owner_pid)
    assert %{active_turn: nil, draining?: false} = :sys.get_state(owner_pid)

    assert {:ok, replacement_downstream} =
             WebsocketOwnerSession.attach_downstream(
               owner_pid,
               downstream("corr-drain-cancel-reuse")
             )

    retry_request = request("retry")

    retry =
      Task.async(fn ->
        WebsocketOwnerSession.submit_request(owner_pid, replacement_downstream, retry_request)
      end)

    assert_receive {:remote_drain_turn_started, retry_task, _request}
    send(retry_task, {:release_remote_drain_turn, release_ref})
    assert :ok = Task.await(retry, @peer_detection_timeout_ms)
    assert Process.alive?(owner_pid)

    send(owner_turn_id, :stop)
    refute_received {:websocket_owner_frame, "corr-drain-cancel", 1, :complete}
    refute Process.alive?(remote_turn_task)
  end

  @tag :rollout_drain_peer_qa
  test "real peer owner stays reusable while rollout drain waits for its proxy-local task" do
    peer_node = start_current_peer!("rollout_proxy_owner")
    parent = self()
    release_ref = make_ref()
    session_id = "real-peer-rollout-proxy"

    terminal = terminal_frame("resp_real_peer_rollout_proxy")

    upstream =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_upstream_boundary, [
        parent,
        [block_ref: release_ref, messages: [terminal], return_request_result?: true]
      ])

    persistence = :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    assert {:ok, owner_pid} =
             :erpc.call(peer_node, WebsocketOwnerSession, :start_owner, [
               [
                 codex_session_id: session_id,
                 owner_lease_token: "synthetic-rollout-proxy-token",
                 owner_instance_id: Atom.to_string(peer_node),
                 owner_renewal_ms: 60_000,
                 upstream: upstream,
                 persistence: persistence
               ]
             ])

    client = WebsocketOwnerForwarder.ERPCNodeClient

    assert {:ok, stable_downstream} =
             client.call_owner(
               peer_node,
               WebsocketOwnerForwarder,
               :remote_attach_downstream,
               [session_id, downstream("corr-real-peer-rollout-proxy")],
               @peer_detection_timeout_ms
             )

    harness = WebsocketRolloutDrainSupport.start_rollout_drain_harness(self())
    response_task_parent = self()
    owner_request = request("real-peer-rollout-proxy")

    {:ok, response_task} =
      ResponseTask.start(
        response_task_parent,
        :proxy,
        fn task_pid ->
          send(parent, {:real_peer_proxy_dispatch_identity, self(), task_pid})
          per_call_downstream = Map.put(stable_downstream, :owner_turn_id, task_pid)

          client.call_owner(
            peer_node,
            WebsocketOwnerSession,
            :submit_request,
            [owner_pid, per_call_downstream, owner_request],
            @peer_detection_timeout_ms
          )
        end,
        fn task_pid, :owner_drained ->
          per_call_downstream = Map.put(stable_downstream, :owner_turn_id, task_pid)

          assert :ok =
                   client.call_owner(
                     peer_node,
                     WebsocketOwnerForwarder,
                     :remote_cancel_downstream_v1,
                     [session_id, per_call_downstream, :owner_drained],
                     @peer_detection_timeout_ms
                   )

          :await_worker
        end,
        activity_registry: harness.activity_registry
      )

    assert_receive {:real_peer_proxy_dispatch_identity, ^response_task, ^response_task}
    assert_receive {:websocket_owner_harness_barrier, peer_turn_task, ^release_ref}

    assert_receive {:websocket_owner_frame, "corr-real-peer-rollout-proxy", 1, ^response_task, {:data, ^terminal}} = terminal_message

    drain_task =
      Task.async(fn ->
        RolloutDrain.start_drain(
          [name: harness.name, timeout_ms: 500] ++
            WebsocketRolloutDrainSupport.deadline_options(harness.deadline)
        )
      end)

    assert_receive {:rollout_drain_deadline_wait, _deadline, _wait_ms}
    refute_received {:codex_response_done, ^response_task, _result}
    send(peer_turn_task, {:websocket_owner_harness_release, release_ref})

    assert_receive {:websocket_response_activity, ^response_task, activity_token}

    assert_receive {:codex_response_done, ^response_task, {:ok, %{terminal: "response.completed"}}} = done_message

    assert_receive {:websocket_owner_frame, "corr-real-peer-rollout-proxy", 1, ^response_task, :complete} = complete_message

    task_monitor = Process.monitor(response_task)

    socket_state = %{
      opts: RequestOptions.for_websocket(%{}),
      tasks: MapSet.new([response_task]),
      task_monitors: %{response_task => task_monitor},
      queued_response_payloads: :queue.new(),
      websocket_owner_downstream: stable_downstream,
      native_turn_output_task_pids: MapSet.new()
    }

    assert {:ok, activity_state} =
             CodexResponsesSocket.handle_info(
               {:websocket_response_activity, response_task, activity_token},
               socket_state
             )

    assert {:ok, result_state} = CodexResponsesSocket.handle_info(done_message, activity_state)

    assert Process.alive?(drain_task.pid)

    assert {:push, {:text, ^terminal}, terminal_state} =
             CodexResponsesSocket.handle_info(terminal_message, result_state)

    assert Process.alive?(drain_task.pid)

    assert {:ok, completed_state} =
             CodexResponsesSocket.handle_info(complete_message, terminal_state)

    assert_receive {:websocket_response_delivery_complete, ^response_task, ^activity_token} =
                     delivery_ack

    assert {:ok, _final_state} = CodexResponsesSocket.handle_info(delivery_ack, completed_state)

    assert %{proxy_turns_seen: 1, proxy_turns_completed: 1, proxy_turns_aborted: 0} =
             Task.await(drain_task, @peer_detection_timeout_ms)

    assert :erpc.call(peer_node, Process, :alive?, [owner_pid])

    assert %{active_turn: nil, draining?: false} =
             :erpc.call(peer_node, :sys, :get_state, [owner_pid])
  end

  test "versioned remote cancellation falls back to the legacy /2 entrypoint" do
    remote_node = :"codex_pooler@legacy-cancel-owner-app.example"
    session_id = "legacy-cancel-session"
    stop_synthetic_owner_on_exit(session_id)

    assert {:ok, _owner_pid} =
             WebsocketOwnerSession.start_owner(
               codex_session_id: session_id,
               owner_lease_token: "synthetic-legacy-cancel-token",
               owner_instance_id: Atom.to_string(node()),
               owner_renewal_ms: 60_000,
               upstream: WebsocketOwnerNodeHarness.fake_upstream_boundary(self()),
               persistence: WebsocketOwnerNodeHarness.fake_persistence_boundary()
             )

    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}
    downstream = attach_downstream(session_id, "corr-legacy-cancel")

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :old_release}
      )

    assert :ok =
             WebsocketOwnerForwarder.cancel_remote_downstream(
               remote_node,
               session_id,
               downstream,
               :client_disconnected,
               opts
             )
  end

  @tag :continuation_generation_boundary
  test "real peer owner defaults a missing forwarded continuation marker to false" do
    peer_node = start_current_peer!("legacy_continuation_owner")
    upstream = start_fake_upstream(websocket_success_without_id())

    persistence =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    session_id = "real-peer-legacy-continuation"

    assert {:ok, owner_pid} =
             :erpc.call(peer_node, WebsocketOwnerSession, :start_owner, [
               [
                 codex_session_id: session_id,
                 owner_lease_token: "synthetic-legacy-owner-token",
                 owner_instance_id: Atom.to_string(peer_node),
                 owner_renewal_ms: 60_000,
                 persistence: persistence
               ]
             ])

    client = WebsocketOwnerForwarder.ERPCNodeClient

    assert {:ok, attached} =
             client.call_owner(
               peer_node,
               WebsocketOwnerForwarder,
               :remote_attach_downstream,
               [session_id, downstream("corr-peer-legacy-continuation")],
               @peer_detection_timeout_ms
             )

    private_marker = "synthetic-private-previous-response"

    legacy_request =
      CodexPooler.JSON.encode!(%{"previous_response_id" => private_marker})
      |> request(FakeUpstream.url(upstream))
      |> Map.delete(:connection_bound_continuation?)

    assert is_struct(legacy_request, UpstreamWebsocketSession.Request)
    refute Map.has_key?(legacy_request, :connection_bound_continuation?)

    assert {:ok, result} =
             client.call_owner(
               peer_node,
               WebsocketOwnerSession,
               :submit_request,
               [owner_pid, attached, legacy_request],
               @peer_detection_timeout_ms
             )

    assert result.terminal == "response.completed"
    refute Map.has_key?(result, :transport_failure)
    refute inspect(result) =~ private_marker

    assert [%{json: %{"previous_response_id" => ^private_marker}}] =
             FakeUpstream.requests(upstream)

    assert_receive {:websocket_owner_frame, "corr-peer-legacy-continuation", 1, {:data, _terminal}}

    assert_receive {:websocket_owner_frame, "corr-peer-legacy-continuation", 1, :complete}
  end

  test "real peer owner preserves a structured response identity" do
    peer_node = start_current_peer!("response_identity_owner")
    response_id = "resp_real_peer_identity"

    upstream =
      start_fake_upstream(FakeUpstream.websocket_text_frames([terminal_frame(response_id)]))

    persistence =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    session_id = "real-peer-response-identity"

    assert {:ok, owner_pid} =
             :erpc.call(peer_node, WebsocketOwnerSession, :start_owner, [
               [
                 codex_session_id: session_id,
                 owner_lease_token: "synthetic-response-identity-owner-token",
                 owner_instance_id: Atom.to_string(peer_node),
                 owner_renewal_ms: 60_000,
                 persistence: persistence
               ]
             ])

    assert node(owner_pid) == peer_node

    client = WebsocketOwnerForwarder.ERPCNodeClient

    assert {:ok, attached} =
             client.call_owner(
               peer_node,
               WebsocketOwnerForwarder,
               :remote_attach_downstream,
               [session_id, downstream("corr-real-peer-response-identity")],
               @peer_detection_timeout_ms
             )

    assert {:ok, %{response_id: ^response_id} = result} =
             client.call_owner(
               peer_node,
               WebsocketOwnerSession,
               :submit_request,
               [
                 owner_pid,
                 attached,
                 request("real-peer-response-identity", FakeUpstream.url(upstream))
               ],
               @peer_detection_timeout_ms
             )

    assert %{terminal: "response.completed", status: 200} = result

    assert_receive {:websocket_owner_frame, "corr-real-peer-response-identity", 1, {:data, _terminal}}

    assert_receive {:websocket_owner_frame, "corr-real-peer-response-identity", 1, :complete}
  end

  # findings#206 row 206-245: resolving a remote owner asks the owner node for
  # its role. Under the one-second probe budget an owner node that stalled for
  # longer (a stop-the-world pause, a congested distribution link) read as
  # unavailable, and a socket's detach that reads it runs its owner-lost
  # recovery against an owner still serving the turn. The peer's OS process is stopped
  # for 1.2 s, longer than that budget and well inside the owner call budget.
  test "a stalled real peer owner node still resolves as the owner" do
    {_peer_pid, peer_node} = start_current_peer_process!("stalled_role_probe_owner")
    os_pid = peer_node |> :erpc.call(:os, :getpid, [], @peer_detection_timeout_ms) |> List.to_string()
    on_exit(fn -> System.cmd("kill", ["-CONT", os_pid], stderr_to_stdout: true) end)
    session = %CodexSession{owner_instance_id: Atom.to_string(peer_node)}

    assert {_output, 0} = System.cmd("kill", ["-STOP", os_pid], stderr_to_stdout: true)

    resume =
      Task.async(fn ->
        Process.sleep(@stalled_owner_node_ms)
        System.cmd("kill", ["-CONT", os_pid], stderr_to_stdout: true)
      end)

    assert {:ok, {:remote, ^peer_node, _owner_instance_id}} = WebsocketOwnerForwarder.resolve_owner(session)
    assert {_output, 0} = Task.await(resume, @peer_detection_timeout_ms)
  end

  test "real peer owner captures its node-local timeout and recovery captures the recovering node timeout",
       %{auth: auth} do
    previous_operational_settings = Application.get_env(:codex_pooler, OperationalSettings)
    on_exit(fn -> restore_operational_settings(previous_operational_settings) end)

    peer_node = start_current_peer!("settings_owner")
    owner_timeout = 180_001
    proxy_timeout = 240_002
    changed_owner_timeout = 300_003
    recovery_timeout = 360_004

    assert :ok =
             :erpc.call(
               peer_node,
               WebsocketOwnerNodeHarness,
               :put_owner_idle_timeout,
               [owner_timeout]
             )

    put_owner_idle_timeout(proxy_timeout)
    assert OperationalSettings.current().websocket_owner_idle_timeout_ms == proxy_timeout

    upstream =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_upstream_boundary, [
        self(),
        []
      ])

    persistence =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    session_id = "real-peer-node-local-owner"

    assert {:ok, owner_pid} =
             :erpc.call(
               peer_node,
               WebsocketOwnerNodeHarness,
               :start_owner_with_local_idle_timeout,
               [
                 [
                   codex_session_id: session_id,
                   owner_lease_token: "synthetic-owner-token",
                   owner_instance_id: Atom.to_string(peer_node),
                   owner_renewal_ms: 60_000,
                   upstream: upstream,
                   persistence: persistence
                 ]
               ]
             )

    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}
    assert node(owner_pid) == peer_node

    assert owner_timeout ==
             :erpc.call(
               peer_node,
               WebsocketOwnerNodeHarness,
               :owner_idle_timeout,
               [owner_pid]
             )

    client = WebsocketOwnerForwarder.ERPCNodeClient

    assert {:ok, attached} =
             client.call_owner(
               peer_node,
               WebsocketOwnerForwarder,
               :remote_attach_downstream,
               [session_id, downstream("corr-node-local-owner")],
               2_000
             )

    proxy_args = [session_id, attached, @frame, []]
    assert [^session_id, ^attached, @frame, []] = proxy_args

    assert :ok =
             client.call_owner(
               peer_node,
               WebsocketOwnerForwarder,
               :remote_submit_frame,
               proxy_args,
               2_000
             )

    assert_receive {:websocket_owner_harness_upstream_sent, ^upstream_pid}

    assert :ok =
             :erpc.call(
               peer_node,
               WebsocketOwnerNodeHarness,
               :put_owner_idle_timeout,
               [changed_owner_timeout]
             )

    put_owner_idle_timeout(recovery_timeout)

    assert %{websocket_owner_idle_timeout_ms: ^changed_owner_timeout} =
             :erpc.call(peer_node, OperationalSettings, :current, [])

    assert OperationalSettings.current().websocket_owner_idle_timeout_ms == recovery_timeout

    assert owner_timeout ==
             :erpc.call(
               peer_node,
               WebsocketOwnerNodeHarness,
               :owner_idle_timeout,
               [owner_pid]
             )

    %{session: recovered_session} =
      owner_session_fixture(auth, Atom.to_string(node()), "real-peer-recovery")

    recovery_frame =
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"id" => "resp_recovery_setting"}
      })

    recovery_upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [recovery_frame],
        return_request_result?: true
      )

    request = request("real-peer-recovery-request")

    assert {:ok, %{status: 200, terminal: "response.completed"}} =
             WebsocketOwnerForwarder.submit_request(recovered_session, recovered_session.owner_lease_token, downstream("corr-real-peer-recovery"), CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!(request),
               upstream: recovery_upstream,
               local_node_string: Atom.to_string(node())
             )

    assert {:ok, recovered_owner} = WebsocketOwnerSession.lookup(recovered_session.id)
    assert %{idle_shutdown_ms: ^recovery_timeout} = :sys.get_state(recovered_owner)
  end

  test "current proxy rejects a frozen previous-release owner without sending a generation" do
    peer_node = start_current_peer!("previous_owner")
    terminal_frame = terminal_frame("resp_previous_owner")

    upstream =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_upstream_boundary, [
        self(),
        [messages: [terminal_frame], return_request_result?: true]
      ])

    persistence =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    session_id = "real-peer-previous-owner"

    assert {:ok, owner_pid} =
             :erpc.call(peer_node, WebsocketOwnerSession, :start_owner, [
               [
                 codex_session_id: session_id,
                 owner_lease_token: "synthetic-previous-owner-token",
                 owner_instance_id: Atom.to_string(peer_node),
                 owner_renewal_ms: 60_000,
                 upstream: upstream,
                 persistence: persistence
               ]
             ])

    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    assert 300_000 ==
             :erpc.call(
               peer_node,
               WebsocketOwnerNodeHarness,
               :owner_idle_timeout,
               [owner_pid]
             )

    module = WebsocketOwnerForwarder
    {:ok, ^module, beam} = previous_release_forwarder_beam(module)

    assert {:module, ^module} =
             :erpc.call(peer_node, :code, :load_binary, [module, ~c"previous_release.ex", beam])

    assert :erpc.call(peer_node, :erlang, :function_exported, [
             module,
             :remote_attach_downstream,
             2
           ])

    refute :erpc.call(peer_node, :erlang, :function_exported, [
             module,
             :remote_attach_downstream,
             3
           ])

    opts = [node_client: WebsocketOwnerForwarder.ERPCNodeClient]

    assert {:ok, attached} =
             WebsocketOwnerForwarder.call_remote(
               peer_node,
               :remote_attach_downstream,
               [session_id, downstream("corr-previous-owner")],
               opts
             )

    envelope =
      request("previous-owner-request")
      |> owner_request()
      |> CodexPooler.ProviderCreditsDispatchSupport.owner_envelope!()

    assert {:error, :owner_unavailable} =
             WebsocketOwnerForwarder.call_remote(
               peer_node,
               :remote_submit_request_v8,
               [session_id, attached, envelope],
               opts
             )

    assert :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_upstream_frames, [upstream_pid]) == []
    assert :erpc.call(peer_node, Process, :alive?, [owner_pid])
    refute_received {:websocket_owner_harness_upstream_sent, ^upstream_pid}
    refute_received {:websocket_owner_frame, "corr-previous-owner", _, _}

    bridge_args =
      WebsocketOwnerForwarder.remote_attach_args(
        session_id,
        downstream("corr-previous-owner-bridge"),
        reject_if_busy: true
      )

    assert [_session_id, _downstream, [reject_if_busy: true]] = bridge_args

    assert {:error, :owner_crashed} =
             WebsocketOwnerForwarder.call_remote(
               peer_node,
               :remote_attach_downstream,
               bridge_args,
               opts
             )
  end

  test "real previous-release peer caller fails closed before current owner submission" do
    caller_node = start_current_peer!("previous_caller")

    connection = %{
      lifecycle_id: "11111111-1111-4111-8111-111111111111",
      generation: 2,
      reused: true,
      reconnected: false
    }

    terminal_frame = terminal_frame("resp_previous_caller")

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: [terminal_frame],
        return_request_result?: true,
        upstream_websocket_connection: connection
      )

    session_id = "real-peer-current-owner"
    stop_synthetic_owner_on_exit(session_id)

    assert {:ok, _owner_pid} =
             WebsocketOwnerSession.start_owner(
               codex_session_id: session_id,
               owner_lease_token: "synthetic-current-owner-token",
               owner_instance_id: Atom.to_string(node()),
               owner_renewal_ms: 60_000,
               upstream: upstream,
               persistence: WebsocketOwnerNodeHarness.fake_persistence_boundary()
             )

    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}

    error =
      assert_raise ErlangError, fn ->
        :erpc.call(
          caller_node,
          WebsocketOwnerPreviousReleaseCaller,
          :attach_and_submit,
          [
            node(),
            session_id,
            downstream("corr-previous-caller"),
            request("previous-caller-request")
          ]
        )
      end

    assert {:exception, {:exception, :undef, [{WebsocketOwnerForwarder, :remote_submit_request, _args, _location} | _inner_stack]}, _outer_stack} = error.original
    assert WebsocketOwnerNodeHarness.fake_upstream_frames(upstream_pid) == []

    refute_received {:websocket_owner_previous_release_caller, ^caller_node, _keys}
    refute_received {:websocket_owner_harness_upstream_sent, ^upstream_pid}
  end

  test "current proxy relays exact private-safe connection metadata from a real current peer" do
    peer_node = start_current_peer!("current_owner")

    connection = %{
      lifecycle_id: "22222222-2222-4222-8222-222222222222",
      generation: 3,
      reused: false,
      reconnected: true
    }

    terminal_frame = terminal_frame("resp_current_peer")

    upstream =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_upstream_boundary, [
        self(),
        [
          messages: [terminal_frame],
          return_request_result?: true,
          upstream_websocket_connection: connection
        ]
      ])

    persistence =
      :erpc.call(peer_node, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    session_id = "real-peer-current-relay"

    assert {:ok, owner_pid} =
             :erpc.call(peer_node, WebsocketOwnerSession, :start_owner, [
               [
                 codex_session_id: session_id,
                 owner_lease_token: "synthetic-current-relay-token",
                 owner_instance_id: Atom.to_string(peer_node),
                 owner_renewal_ms: 60_000,
                 upstream: upstream,
                 persistence: persistence
               ]
             ])

    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}
    opts = [node_client: WebsocketOwnerForwarder.ERPCNodeClient]

    assert {:ok, attached} =
             WebsocketOwnerForwarder.call_remote(
               peer_node,
               :remote_attach_downstream,
               [
                 session_id,
                 downstream("corr-current-relay"),
                 [reject_if_busy: true]
               ],
               opts
             )

    assert {:ok, %{upstream_websocket_connection: relayed_connection}} =
             WebsocketOwnerForwarder.ERPCNodeClient.call_owner(
               peer_node,
               WebsocketOwnerSession,
               :submit_request,
               [owner_pid, attached, request("current-relay-request")],
               @peer_detection_timeout_ms
             )

    assert relayed_connection == connection

    assert Map.keys(relayed_connection) |> Enum.sort() == [
             :generation,
             :lifecycle_id,
             :reconnected,
             :reused
           ]

    for forbidden <- [:pid, :node, :socket, :header, :headers, :payload] do
      refute Map.has_key?(relayed_connection, forbidden)
    end

    assert_receive {:websocket_owner_harness_upstream_sent, ^upstream_pid}
  end

  @tag slow: "boots two real BEAM peers, terminates the first owner and proves one recovered lease and owner on the survivor"
  test "real current peers replace a nodedown owner and converge on one recovered owner lease",
       %{auth: auth} do
    {peer_a_pid, peer_a} = start_current_peer_process!("replacement_owner_a")
    {peer_b_pid, peer_b} = start_current_peer_process!("replacement_owner_b")
    owner_a_timeout = 300_101
    owner_b_timeout = 420_202

    assert :ok =
             :erpc.call(peer_a, WebsocketOwnerNodeHarness, :put_owner_idle_timeout, [
               owner_a_timeout
             ])

    assert :ok =
             :erpc.call(peer_b, WebsocketOwnerNodeHarness, :put_owner_idle_timeout, [
               owner_b_timeout
             ])

    %{session: session} =
      owner_session_fixture(auth, Atom.to_string(peer_a), "real-peer-replacement")

    lifecycle_a = Ecto.UUID.generate()

    connection_a = %{
      lifecycle_id: lifecycle_a,
      generation: 1,
      reused: false,
      reconnected: false
    }

    upstream_a =
      :erpc.call(peer_a, WebsocketOwnerNodeHarness, :fake_upstream_boundary, [
        self(),
        [
          messages: [terminal_frame("resp_replacement_a")],
          return_request_result?: true,
          upstream_websocket_connection: connection_a
        ]
      ])

    persistence_a =
      :erpc.call(peer_a, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    assert {:ok, owner_a} =
             :erpc.call(
               peer_a,
               WebsocketOwnerNodeHarness,
               :start_owner_with_local_idle_timeout,
               [
                 [
                   codex_session_id: session.id,
                   owner_lease_token: session.owner_lease_token,
                   owner_instance_id: Atom.to_string(peer_a),
                   owner_renewal_ms: 60_000,
                   upstream: upstream_a,
                   persistence: persistence_a
                 ]
               ]
             )

    assert_receive {:websocket_owner_harness_upstream_started, upstream_a_pid}

    assert owner_a_timeout ==
             :erpc.call(peer_a, WebsocketOwnerNodeHarness, :owner_idle_timeout, [owner_a])

    opts = [node_client: WebsocketOwnerForwarder.ERPCNodeClient]

    assert {:ok, %{epoch: epoch_a} = attached_a} =
             WebsocketOwnerForwarder.call_remote(
               peer_a,
               :remote_attach_downstream,
               [session.id, downstream("corr-replacement-a")],
               opts
             )

    assert {:ok, %{upstream_websocket_connection: ^connection_a}} =
             WebsocketOwnerForwarder.ERPCNodeClient.call_owner(
               peer_a,
               WebsocketOwnerSession,
               :submit_request,
               [owner_a, attached_a, request("replacement-a-request")],
               @peer_detection_timeout_ms
             )

    assert_receive {:websocket_owner_harness_upstream_sent, ^upstream_a_pid}
    assert_receive {:websocket_owner_frame, "corr-replacement-a", ^epoch_a, :complete}

    peer_a_monitor = Process.monitor(peer_a_pid)
    assert :ok = :peer.stop(peer_a_pid)
    assert_receive {:DOWN, ^peer_a_monitor, :process, ^peer_a_pid, _reason}, 2_000

    assert {:error, :owner_unavailable} =
             :erpc.call(
               peer_b,
               WebsocketOwnerForwarder.ERPCNodeClient,
               :call_owner,
               [
                 peer_a,
                 WebsocketOwnerForwarder,
                 :remote_attach_downstream,
                 [session.id, downstream("corr-replacement-detect")],
                 2_000
               ]
             )

    replacement_options =
      RequestOptions.for_websocket(%{owner_instance_id: Atom.to_string(peer_b)})

    assert {:ok, replacement_session} =
             SessionContinuity.replace_unavailable_owner_lease(
               session,
               replacement_options
             )

    refute replacement_session.owner_lease_token == session.owner_lease_token

    lifecycle_b = Ecto.UUID.generate()

    connection_b = %{
      lifecycle_id: lifecycle_b,
      generation: 1,
      reused: false,
      reconnected: false
    }

    upstream_b =
      :erpc.call(peer_b, WebsocketOwnerNodeHarness, :fake_upstream_boundary, [
        self(),
        [
          messages: [terminal_frame("resp_replacement_b")],
          return_request_result?: true,
          upstream_websocket_connection: connection_b
        ]
      ])

    persistence_b =
      :erpc.call(peer_b, WebsocketOwnerNodeHarness, :fake_persistence_boundary, [])

    assert {:ok, owner_b} =
             :erpc.call(
               peer_b,
               WebsocketOwnerNodeHarness,
               :start_owner_with_local_idle_timeout,
               [
                 [
                   codex_session_id: replacement_session.id,
                   owner_lease_token: replacement_session.owner_lease_token,
                   owner_instance_id: Atom.to_string(peer_b),
                   owner_renewal_ms: 60_000,
                   upstream: upstream_b,
                   persistence: persistence_b
                 ]
               ]
             )

    assert_receive {:websocket_owner_harness_upstream_started, upstream_b_pid}

    assert owner_b_timeout ==
             :erpc.call(peer_b, WebsocketOwnerNodeHarness, :owner_idle_timeout, [owner_b])

    assert {:ok, %{epoch: epoch_b} = attached_b} =
             WebsocketOwnerForwarder.call_remote(
               peer_b,
               :remote_attach_downstream,
               [replacement_session.id, downstream("corr-replacement-b")],
               opts
             )

    assert {:ok, %{upstream_websocket_connection: recovered_connection}} =
             WebsocketOwnerForwarder.ERPCNodeClient.call_owner(
               peer_b,
               WebsocketOwnerSession,
               :submit_request,
               [owner_b, attached_b, request("replacement-b-request")],
               @peer_detection_timeout_ms
             )

    assert recovered_connection == connection_b
    assert lifecycle_b != lifecycle_a
    assert {:ok, ^lifecycle_a} = Ecto.UUID.cast(lifecycle_a)
    assert {:ok, ^lifecycle_b} = Ecto.UUID.cast(lifecycle_b)

    assert Map.keys(recovered_connection) |> Enum.sort() == [
             :generation,
             :lifecycle_id,
             :reconnected,
             :reused
           ]

    assert_receive {:websocket_owner_harness_upstream_sent, ^upstream_b_pid}
    assert_receive {:websocket_owner_frame, "corr-replacement-b", ^epoch_b, :complete}

    assert 1 ==
             :erpc.call(peer_b, WebsocketOwnerNodeHarness, :owner_count, [
               replacement_session.id
             ])

    active_status = BridgeOwnerLease.active_status()

    active_lease_query =
      from lease in BridgeOwnerLease,
        where:
          lease.codex_session_id == ^replacement_session.id and
            lease.status == ^active_status

    assert Repo.aggregate(active_lease_query, :count, :id) == 1

    assert Repo.exists?(
             from lease in active_lease_query,
               where:
                 lease.owner_instance_id == ^Atom.to_string(peer_b) and
                   lease.lease_token == ^replacement_session.owner_lease_token
           )

    peer_b_monitor = Process.monitor(peer_b_pid)
    assert :ok = :peer.stop(peer_b_pid)
    assert_receive {:DOWN, ^peer_b_monitor, :process, ^peer_b_pid, _reason}, 2_000
  end

  test "attaching through an old-release owner node fails closed for the bridge and still serves native attaches",
       %{auth: auth} do
    remote_node = :"codex_pooler@old-release-owner.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session} = owner_session_fixture(auth, remote_node_string)

    upstream = WebsocketOwnerNodeHarness.fake_upstream_boundary(self(), messages: [])
    {:ok, _owner} = start_owner(session, upstream)

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => :old_release}
      )

    # This is verbatim the proxy-side remote attach: websocket.ex calls
    # call_remote(:remote_attach_downstream, remote_attach_args(...)). The
    # option-carrying bridge attach hits the missing /3 on the old node and
    # must map the :erpc undef to a fail-closed owner error so the bridge
    # falls back to HTTP instead of committing.
    bridge_args =
      WebsocketOwnerForwarder.remote_attach_args(
        session.id,
        downstream("corr-old-release-bridge"),
        reject_if_busy: true
      )

    assert {:error, :owner_crashed} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               bridge_args,
               opts
             )

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_attach_downstream, arity: 3}}

    # The option-less native attach keeps the two-argument shape the old
    # release exports and reaches the real owner process end to end.
    native_args =
      WebsocketOwnerForwarder.remote_attach_args(
        session.id,
        downstream("corr-old-release-native"),
        []
      )

    assert {:ok, %{correlation_id: "corr-old-release-native", epoch: epoch}} =
             WebsocketOwnerForwarder.call_remote(
               remote_node,
               :remote_attach_downstream,
               native_args,
               opts
             )

    assert is_integer(epoch)

    assert_receive {:websocket_owner_harness_node_call, %{node: ^remote_node, function: :remote_attach_downstream, arity: 2}}
  end

  test "disconnected remote owner string maps to owner_unavailable", %{auth: auth} do
    remote_node = :"codex_pooler@known-app.example"
    disconnected_node = :"codex_pooler@disconnected-app.example"

    %{session: session, token: token} =
      owner_session_fixture(auth, Atom.to_string(disconnected_node))

    opts = WebsocketOwnerNodeHarness.node_client_opts([remote_node])

    assert {:error, :owner_unavailable} =
             WebsocketOwnerForwarder.submit_frame(
               session,
               token,
               downstream("corr-disconnected"),
               @frame,
               opts
             )
  end

  test "delayed owner completion after proxy timeout produces no late downstream frames", %{
    auth: auth
  } do
    remote_node = :"codex_pooler@delayed-app.example"
    remote_node_string = Atom.to_string(remote_node)
    %{session: session, token: token} = owner_session_fixture(auth, remote_node_string)
    release_ref = make_ref()

    upstream =
      WebsocketOwnerNodeHarness.fake_upstream_boundary(self(),
        messages: ["late-delta"]
      )

    {:ok, _owner} = start_owner(session, upstream)
    assert_receive {:websocket_owner_harness_upstream_started, _upstream_pid}

    opts =
      WebsocketOwnerNodeHarness.node_client_opts([remote_node],
        calls: %{remote_node => {:delayed_success, self(), release_ref}}
      )

    assert {:error, :owner_forward_timeout} =
             WebsocketOwnerForwarder.submit_frame(
               session,
               token,
               downstream("corr-late"),
               @frame,
               Keyword.put(opts, :timeout, 25)
             )

    assert_receive {:websocket_owner_harness_delayed_started, delayed_pid, ^release_ref}
    send(delayed_pid, {:websocket_owner_harness_release_delayed, release_ref})

    assert_receive {:websocket_owner_harness_delayed_result, ^release_ref, {:error, :stale_downstream}}

    refute_receive {:websocket_owner_frame, "corr-late", 1, _payload}
  end

  defp auth_fixture do
    %{user: owner} = bootstrap_owner_fixture()
    pool = pool_fixture(%{created_by_user_id: owner.id})
    %{api_key: api_key} = active_api_key_fixture(pool, %{created_by_user_id: owner.id})
    %{pool: pool, api_key: api_key}
  end

  defp owner_session_fixture(auth, owner_instance_id, suffix \\ "owner") do
    assert {:ok, %CodexSession{} = session} =
             Gateway.start_codex_session(auth, %{
               accepted_turn_state: "forwarder-#{suffix}-#{System.unique_integer([:positive])}",
               owner_instance_id: owner_instance_id
             })

    session = Repo.get!(CodexSession, session.id)
    %{session: session, token: session.owner_lease_token}
  end

  defp await_reconnect_timeout_release(release_ref) do
    receive do
      {:EXIT, _from, :shutdown} -> await_reconnect_timeout_release(release_ref)
      {:release_reconnect_timeout_turn, ^release_ref} -> :ok
    end
  end

  defp native_request(turn_id) do
    %{
      request(CodexPooler.JSON.encode!(%{"type" => "response.create", "turn_id" => turn_id}))
      | message_mapper: &StreamProtocol.canonicalize_native_codex_responses_json_message/1
    }
  end

  defp start_owner(session, upstream, opts \\ []) do
    owner_opts =
      [
        codex_session_id: session.id,
        owner_lease_token: session.owner_lease_token,
        owner_instance_id: session.owner_instance_id,
        upstream: upstream
      ]
      |> Keyword.merge(opts)

    WebsocketOwnerSession.start_owner(owner_opts)
  end

  # A synthetic session id belongs to no Pool, so no Pool cleanup stops the owner started under
  # it; left running it stayed in the application registry for the rest of the partition
  # (findings#206 row 206-387). Registered before the owner starts.
  defp stop_synthetic_owner_on_exit(codex_session_id) do
    on_exit(fn -> capture_log(fn -> BackendCodexWebsocketOwnerForwardingSupport.await_owner_cleanup!(codex_session_id) end) end)
  end

  defp attach_downstream(codex_session_id, correlation_id) do
    {:ok, owner} = WebsocketOwnerSession.lookup(codex_session_id)
    {:ok, downstream} = WebsocketOwnerSession.attach_downstream(owner, downstream(correlation_id))
    downstream
  end

  defp downstream(correlation_id), do: %{pid: self(), epoch: 1, correlation_id: correlation_id}

  # A socket process that stays alive until the test ends.
  defp idle_socket! do
    pid = spawn(fn -> receive do: (:stop -> :ok) end)
    on_exit(fn -> send(pid, :stop) end)
    pid
  end

  defp request(payload) do
    %UpstreamWebsocketSession.Request{
      provider_credits_context: request_context!(),
      effective_serving_mode: "full",
      url: "https://example.com/backend-api/codex/responses",
      headers: [],
      payload: payload,
      timeouts: %{}
    }
  end

  defp request(payload, base_url) do
    %UpstreamWebsocketSession.Request{
      provider_credits_context: request_context!(),
      effective_serving_mode: "full",
      url: base_url <> "/backend-api/codex/responses",
      headers: [],
      payload: payload,
      timeouts: @timeouts
    }
  end

  defp request_context! do
    context = Process.get({__MODULE__, :peer_provider_credits_context}) || Process.get({__MODULE__, :provider_credits_context})
    assert is_struct(context, CodexPooler.Gateway.Transports.ProviderCreditsAdmission.Context)
    context
  end

  defp owner_request(%UpstreamWebsocketSession.Request{} = request, opts \\ []) do
    identity = Process.get({__MODULE__, :upstream_identity})

    {:ok, owner_request} =
      WebsocketOwnerRequest.new(%{
        version: 1,
        url: request.url,
        headers: request.headers,
        payload: request.payload,
        timeouts: normalize_timeouts(request.timeouts),
        mapper: Keyword.get(opts, :mapper, :codex_responses),
        upstream_identity_id: identity.id,
        observation: %{
          request_id: nil,
          client_request_id: nil,
          attempt_id: nil,
          mode: "full"
        },
        reset_probe: request.reset_probe,
        native_codex_response_control: request.native_codex_response_control,
        assignment_advertised?: request.assignment_advertised?,
        connection_bound_continuation?: request.connection_bound_continuation?,
        forward_error_body?: request.forward_error_body?,
        submission_notification?: Keyword.get(opts, :submission_notification?, false)
      })

    owner_request
  end

  defp owner_request_v2(%UpstreamWebsocketSession.Request{} = request) do
    attrs = owner_request(request) |> Map.from_struct()

    {:ok, owner_request} =
      WebsocketOwnerRequestV2.new(
        attrs
        |> Map.put(:version, 2)
        |> Map.put(:websocket_delivery_mode, :collect_compaction)
        |> Map.put(:effective_serving_mode, :full)
      )

    owner_request
  end

  defp abandoned_v5_request do
    authority = %CodexPooler.Accounting.ClientRetry.DispatchAuthority{
      version: 1,
      predecessor_request_id: Ecto.UUID.generate(),
      successor_request_id: Ecto.UUID.generate(),
      link_id: Ecto.UUID.generate(),
      successor_claim: "client-retry-v1:synthetic"
    }

    attrs = owner_request(request("abandoned-retry")) |> Map.from_struct()
    {:ok, envelope} = WebsocketOwnerRequestV5.new(Map.merge(attrs, %{version: 5, client_retry_dispatch_authority: authority}))
    envelope
  end

  defp abandoned_v3_request do
    attrs = owner_request_v2(request("abandoned-collect")) |> Map.from_struct()
    # The admission object is opaque to this boundary; no owner exists or
    # materialization runs once the exact submission was abandoned.
    capability = struct(NativeCompactionAdmission.Capability, %{})
    {:ok, envelope} = WebsocketOwnerRequestV3.new(Map.merge(attrs, %{version: 3, owner_admission_capability: capability, first_compact_collection: nil}))
    envelope
  end

  defp owner_request_v4(%NativeReplayAdmission.Binding{} = binding, token \\ <<9::256>>) do
    attrs = owner_request(request("replay-v4")) |> Map.from_struct()
    {:ok, binding_digest} = NativeReplayAdmission.binding_digest(binding)

    proof =
      RuntimeAdmissionProof.new(self(), make_ref(), make_ref(), binding_digest, :native_replay)

    {:ok, owner_request} =
      WebsocketOwnerRequestV4.new(
        attrs
        |> Map.put(:version, 4)
        |> Map.put(:websocket_delivery_mode, :relay)
        |> Map.put(:effective_serving_mode, :full)
        |> Map.put(:native_replay_binding, binding)
        |> Map.put(:native_replay_proof, proof)
        |> Map.put(:provisional_token, token)
      )

    owner_request
  end

  defp replay_binding(owner_process_generation) do
    %NativeReplayAdmission.Binding{
      request_id: Ecto.UUID.generate(),
      codex_turn_id: Ecto.UUID.generate(),
      eligible_attempt_id: Ecto.UUID.generate(),
      replay_attempt_id: Ecto.UUID.generate(),
      replay_generation: 1,
      semantic_turn_digest: <<1::256>>,
      replay_claim_digest: <<2::256>>,
      provisional_binding_digest: <<3::256>>,
      owner_lease_digest: <<4::256>>,
      downstream_epoch: 1,
      owner_process_generation: owner_process_generation
    }
  end

  defp normalize_timeouts(%TimeoutConfig{} = timeouts), do: timeouts
  defp normalize_timeouts(timeouts), do: TimeoutConfig.build(timeouts)

  defp start_fake_upstream(mode) do
    {:ok, upstream} = FakeUpstream.start_link(mode)
    on_exit(fn -> FakeUpstream.stop(upstream) end)
    upstream
  end

  defp websocket_success_without_id do
    FakeUpstream.websocket_text_frames([
      CodexPooler.JSON.encode!(%{
        "type" => "response.completed",
        "response" => %{"status" => "completed"}
      })
    ])
  end

  defp native_retry_terminal do
    CodexPooler.JSON.encode!(%{
      "type" => "error",
      "status" => 400,
      "error" => %{
        "type" => "invalid_request_error",
        "code" => "previous_response_not_found",
        "message" => "Previous response was not found. Retrying the full request."
      }
    })
  end

  defp bound_reset_probe do
    identity = Process.get({__MODULE__, :upstream_identity})
    context = CodexPooler.ProviderCreditsDispatchSupport.context!(identity, model: "gpt-reset-probe-owner")
    now = DateTime.utc_now()
    identity = CodexPooler.ProviderCreditsFixtures.persist_usage!(Repo.reload!(identity), CodexPooler.ProviderCreditsFixtures.usage_payload(:weekly_credit_only, now: now, credits: :none), now)

    redemption = %{
      "phase" => "consumed_pending_probe",
      "status" => "redeeming",
      "attempt_id" => Ecto.UUID.generate(),
      "generation" => 1,
      "started_at" => DateTime.to_iso8601(now),
      "consumed_at" => DateTime.to_iso8601(now),
      "deadline_at" => DateTime.to_iso8601(RedemptionLifecycle.deadline_at(now)),
      "result" => %{"applied" => true}
    }

    identity = Repo.update!(Ecto.Changeset.change(identity, metadata: Map.put(identity.metadata, "saved_reset_redemption", redemption)))
    assert {:ok, probe} = ResetProbe.bind(ResetProbe.new(), context.pool_upstream_assignment_id, identity.id, context.model, context.route_class)
    assert {:ok, :claimed} = ProbeLease.claim(identity, redemption["generation"], redemption["attempt_id"], probe)
    Process.put({__MODULE__, :upstream_identity}, Repo.reload!(identity))
    probe
  end

  defp terminal_frame(response_id) do
    CodexPooler.JSON.encode!(%{
      "type" => "response.completed",
      "response" => %{"id" => response_id, "status" => "completed"}
    })
  end

  defp put_owner_idle_timeout(timeout) do
    settings = OperationalSettings.current()

    Application.put_env(:codex_pooler, OperationalSettings, settings: %{settings | websocket_owner_idle_timeout_ms: timeout})
  end

  defp restore_operational_settings(nil),
    do: Application.delete_env(:codex_pooler, OperationalSettings)

  defp restore_operational_settings(previous_settings),
    do: Application.put_env(:codex_pooler, OperationalSettings, previous_settings)

  defp existing_atom?(value) do
    _atom = :erlang.binary_to_existing_atom(value)
    true
  rescue
    ArgumentError -> false
  end

  defp ensure_test_distribution_started!, do: start_test_distribution!(node())

  defp start_test_distribution!(:nonode@nohost) do
    # This throwaway single-host mesh does not use global names; global's
    # overlapping-partition guard only adds nondeterministic teardown
    # disconnects that print WARNING REPORTs into the suite output on slow
    # CI machines. Disable it on every node involved (peers get the same
    # flag at boot) and restore the previous setting afterwards.
    previous_partition_guard =
      Application.fetch_env(:kernel, :prevent_overlapping_partitions)

    Application.put_env(:kernel, :prevent_overlapping_partitions, false)

    node_name = String.to_atom("codex_pooler_test_#{System.unique_integer([:positive])}")
    assert {:ok, _pid} = :net_kernel.start([node_name, :shortnames])

    on_exit(fn ->
      assert :ok = :net_kernel.stop()

      case previous_partition_guard do
        {:ok, value} -> Application.put_env(:kernel, :prevent_overlapping_partitions, value)
        :error -> Application.delete_env(:kernel, :prevent_overlapping_partitions)
      end
    end)
  end

  defp start_test_distribution!(_distributed_node), do: :ok

  defp start_current_peer!(prefix) do
    {_peer_pid, peer_node} = start_current_peer_process!(prefix)
    peer_node
  end

  defp start_current_peer_process!(prefix) do
    ensure_test_distribution_started!()
    ensure_peer_request_scope!()

    peer_name = String.to_atom("#{prefix}_#{System.unique_integer([:positive])}")
    boot_id = Ecto.UUID.generate()
    on_exit(fn -> PeerRegistry.assert_peer_absent!(peer_name) end)

    assert {:ok, peer_pid, peer_node} =
             :peer.start_link(%{
               name: peer_name,
               args: [~c"-kernel", ~c"prevent_overlapping_partitions", ~c"false"]
             })

    Process.unlink(peer_pid)

    on_exit(fn ->
      stop_peer(peer_pid)
      PeerRegistry.assert_peer_absent!(peer_name, peer_node: peer_node)
      UnboxedFixture.run_unboxed(fn -> InstancePresencePeer.assert_peer_connections_absent!(boot_id, @peer_detection_timeout_ms) end)
    end)

    assert :ok = :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])
    os_pid = :erpc.call(peer_node, System, :pid, [])
    os_identity = InstancePresencePeer.capture_os_process_identity!(os_pid)

    on_exit(fn ->
      stop_peer(peer_pid)
      InstancePresencePeer.assert_os_process_stopped!(os_identity, budget_ms: @peer_detection_timeout_ms)
    end)

    repo_config = Repo.config() |> Keyword.merge(pool: DBConnection.ConnectionPool, pool_size: 2, parameters: [application_name: InstancePresencePeer.peer_application_name(boot_id)])
    assert :ok = :erpc.call(peer_node, CodexPooler.ProviderCreditsFixtures, :start_peer_runtime!, [repo_config])
    {peer_pid, peer_node}
  end

  defp ensure_peer_request_scope! do
    unless Process.get({__MODULE__, :peer_provider_credits_context}) do
      pool_id = Ecto.UUID.generate()
      identity_id = Ecto.UUID.generate()

      UnboxedFixture.register_unboxed_cleanup!(fn ->
        CodexPooler.PoolerFixtures.delete_committed_pools!([pool_id])
        Repo.delete_all(from identity in UpstreamIdentity, where: identity.id == ^identity_id)
      end)

      context =
        UnboxedFixture.run_unboxed(fn ->
          now = DateTime.utc_now()
          pool = Repo.insert!(%CodexPooler.Pools.Pool{id: pool_id, slug: "owner-scope-#{pool_id}", name: "Synthetic owner scope", status: "active", created_at: now, updated_at: now})
          identity = %UpstreamIdentity{id: identity_id, chatgpt_account_id: "synthetic-#{identity_id}"}
          CodexPooler.ProviderCreditsDispatchSupport.context!(identity, pool: pool)
        end)

      Process.put({__MODULE__, :peer_provider_credits_context}, context)
    end

    :ok
  end

  defp ensure_epmd_started! do
    case :erl_epmd.names() do
      {:ok, _names} ->
        false

      {:error, _reason} ->
        assert {_output, 0} = System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)

        PeerRegistry.assert_epmd_ready!()

        true
    end
  end

  # The owner ends the turn when the submitter's exit reaches it, but drops the
  # downstream only when the forwarding side's cancellation watcher, a process
  # of its own that saw the same exit, detaches it, and nothing tells the test
  # when it has. So the owner's state is polled against one monotonic deadline
  # under the peer detection budget, not a count of scheduler yields, which a
  # watcher delayed on a loaded host outlasted (findings#270 row 270-148).
  defp await_owner_cancellation!(owner_pid),
    do: await_owner_cancellation!(owner_pid, System.monotonic_time(:millisecond) + @peer_detection_timeout_ms)

  defp await_owner_cancellation!(owner_pid, deadline) do
    case :sys.get_state(owner_pid) do
      %{active_turn: nil, downstream: nil} ->
        :ok

      %{active_turn: active_turn, downstream: downstream} ->
        if System.monotonic_time(:millisecond) >= deadline,
          do: flunk("the owner still held turn? #{active_turn != nil} downstream #{inspect(downstream)} #{@peer_detection_timeout_ms}ms after the forwarding proxy died")

        receive do
        after
          1 -> await_owner_cancellation!(owner_pid, deadline)
        end
    end
  end

  defp previous_release_forwarder_beam(module) do
    harness = WebsocketOwnerNodeHarness

    forms = [
      {:attribute, 1, :module, module},
      {:attribute, 1, :export, [remote_attach_downstream: 2, remote_submit_request: 4]},
      {:function, 1, :remote_attach_downstream, 2,
       [
         {:clause, 1, [{:var, 1, :Session}, {:var, 1, :Downstream}], [],
          [
            {:call, 1, {:remote, 1, {:atom, 1, harness}, {:atom, 1, :previous_release_attach}}, [{:var, 1, :Session}, {:var, 1, :Downstream}]}
          ]}
       ]},
      {:function, 1, :remote_submit_request, 4,
       [
         {:clause, 1,
          [
            {:var, 1, :Session},
            {:var, 1, :Downstream},
            {:var, 1, :Request},
            {:var, 1, :Opts}
          ], [],
          [
            {:call, 1, {:remote, 1, {:atom, 1, harness}, {:atom, 1, :previous_release_submit_request}},
             [
               {:var, 1, :Session},
               {:var, 1, :Downstream},
               {:var, 1, :Request},
               {:var, 1, :Opts}
             ]}
          ]}
       ]}
    ]

    :compile.forms(forms, [:binary])
  end

  defp stop_peer(peer_pid) do
    if Process.alive?(peer_pid), do: :peer.stop(peer_pid)
    :ok
  catch
    :exit, _reason -> :ok
  end
end
