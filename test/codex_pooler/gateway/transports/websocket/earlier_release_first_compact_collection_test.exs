defmodule CodexPooler.Gateway.Transports.Websocket.EarlierReleaseFirstCompactCollectionTest do
  use CodexPooler.DataCase, async: false

  @moduletag capture_log: true

  import CodexPooler.PoolerFixtures

  alias CodexPooler.Gateway.Payloads.RequestOptions.TimeoutConfig
  alias CodexPooler.Gateway.Transports.OrdinarySuccessTestSeed
  alias CodexPooler.Gateway.Transports.Websocket.NativeCompactionAdmission
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerAdmissionControlV1
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerForwarder
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV3
  alias CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerSession
  alias CodexPooler.Gateway.Transports.WebsocketOwnerNodeHarness

  # Releases v0.6.13 to v0.6.15 authorized a socket's first full-history
  # native compaction before sending it: the proxy read the owner's admission
  # (`:snapshot`), asked the owner for a provenance
  # (`:authorize_first_compact_collection` with only a binding and a control
  # reference) and sent the compaction as a V3 owner request carrying that
  # provenance. v0.7.0 authorizes the collection after the exchange instead,
  # from the upstream session's own result receipt, and the owner refuses the
  # earlier control; the owner's send-time path for a provenance was removed as
  # unreachable (findings#270 row 270-204). These are the messages such a proxy
  # sends, delivered to a real current owner through the owner-node entry
  # points a remote proxy's `:erpc` call runs. A running earlier node is not
  # involved: the term shapes are those of tag codex-pooler-v0.6.15 (commit
  # 2ff1954dd98301234c8536e7afddd5caad15351d), sent by
  # `authorize_first_full_history_compact/3` in codex_responses_socket.ex, with
  # the field lists below copied from websocket_owner_admission_control_v1.ex,
  # websocket_owner_request_v3.ex and native_compaction_admission.ex there (the
  # binding has none of the keys added since).
  @binding_fields [:semantic_turn_key, :window_digest, :context_digest, :window_number, :compaction_item_digest, :previous_response_digest, :serving_mode, :topology, :lifecycle_id, :generation]
  @control_fields [:version, :action, :downstream, :binding, :phase, :control_ref, :capability, :disposition, :success?, :compaction_item_digest, :confirmation, :first_compact_collection, :expires_at_ms, :now_ms]
  @request_v3_fields [
    :version,
    :url,
    :headers,
    :payload,
    :timeouts,
    :mapper,
    :upstream_identity_id,
    :observation,
    :reset_probe,
    :native_codex_response_control,
    :assignment_advertised?,
    :connection_bound_continuation?,
    :forward_error_body?,
    :submission_notification?,
    :websocket_delivery_mode,
    :effective_serving_mode,
    :owner_admission_capability,
    :first_compact_collection
  ]

  # The control carried a binding and a control reference only: since v0.7.0
  # the owner authorizes a first collection from the session's result receipt,
  # so it refuses this one before acting, and the earlier proxy answers its
  # client the retryable `503 owner_unavailable` it gives any owner refusal.
  test "an earlier release's first-compact authorization reaches a current owner as owner_unavailable", context do
    armed = armed_owner!(context)
    binding = earlier_binding!(armed)
    admission = :sys.get_state(armed.owner).native_compaction_admission

    control = earlier_control(:authorize_first_compact_collection, armed.downstream, binding: binding, control_ref: make_ref())

    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.remote_admission_control_v1(armed.codex_session_id, control)

    # Refused before the owner acted on it: the admission is untouched and
    # nothing went upstream.
    assert :sys.get_state(armed.owner).native_compaction_admission == admission
    assert WebsocketOwnerNodeHarness.fake_upstream_frames(armed.upstream_pid) == []
  end

  # A provenance only an earlier owner issued, from before the session moved
  # to a current owner (the same HMAC over the earlier binding). The V3
  # submission is refused as a malformed owner request, before the owner is
  # looked up, the request materialized or anything sent.
  test "an earlier release's compaction carrying a provenance reaches a current owner as owner_unavailable before any work", context do
    armed = armed_owner!(context)
    provenance = NativeCompactionAdmission.FirstCompactCollection.issue(earlier_binding!(armed), make_ref())
    request = earlier_request_v3(active_upstream_identity_fixture().id, provenance)
    admission = :sys.get_state(armed.owner).native_compaction_admission

    assert {:error, :owner_unavailable} = WebsocketOwnerForwarder.remote_submit_request_v8(armed.codex_session_id, armed.downstream, request)

    assert %{active_turn: nil, native_compaction_admission: ^admission} = :sys.get_state(armed.owner)
    assert WebsocketOwnerNodeHarness.fake_upstream_frames(armed.upstream_pid) == []
    refute_received {:websocket_owner_harness_upstream_sent, _upstream_pid}
  end

  # A real owner armed by one ordinary success, as a first compaction finds
  # it, with its upstream boundary recording whatever it is asked to send.
  defp armed_owner!(context) do
    codex_session_id = "codex-session-#{System.unique_integer([:positive])}"
    lease_token = "owner-token-#{System.unique_integer([:positive])}"
    instance_id = Atom.to_string(node())
    on_exit(fn -> cleanup_owner_session(codex_session_id) end)

    {boundary, seed_url} = OrdinarySuccessTestSeed.boundary(WebsocketOwnerNodeHarness.fake_upstream_boundary(self()))

    assert {:ok, owner} =
             WebsocketOwnerSession.start_owner(
               codex_session_id: codex_session_id,
               owner_lease_token: lease_token,
               owner_instance_id: instance_id,
               upstream: boundary
             )

    assert_receive {:websocket_owner_harness_upstream_started, upstream_pid}
    assert {:ok, downstream} = WebsocketOwnerSession.attach_downstream(owner, %{pid: self(), correlation_id: "earlier-release-#{context.test}"})

    topology = WebsocketOwnerAdmissionControlV1.forwarded_topology(instance_id, lease_token, downstream.epoch)

    seed = %NativeCompactionAdmission.Binding{
      semantic_turn_key: <<1::256>>,
      window_digest: <<2::256>>,
      context_digest: <<3::256>>,
      window_number: 1,
      previous_response_digest: nil,
      serving_mode: :full,
      topology: topology,
      lifecycle_id: Ecto.UUID.generate(),
      generation: 1
    }

    {binding, receipt} = OrdinarySuccessTestSeed.request(owner, downstream, seed, seed_url)

    {:ok, record} =
      WebsocketOwnerAdmissionControlV1.new(%{
        version: 1,
        action: :record_ordinary_success,
        downstream: Map.take(downstream, [:pid, :epoch, :correlation_id]),
        binding: binding,
        phase: nil,
        control_ref: nil,
        capability: nil,
        disposition: nil,
        success?: nil,
        compaction_item_digest: nil,
        confirmation: nil,
        first_compact_collection: receipt,
        expires_at_ms: System.system_time(:millisecond) + 30_000,
        now_ms: nil
      })

    assert {:ok, _pending} = WebsocketOwnerForwarder.remote_admission_control_v1(codex_session_id, record)

    %{owner: owner, codex_session_id: codex_session_id, downstream: Map.take(downstream, [:pid, :epoch, :correlation_id]), upstream_pid: upstream_pid}
  end

  # The earlier proxy read the owner's admission first, with a control whose
  # shape is unchanged and still answered, and built the compaction's binding
  # from it: no compaction item and no response anchor for a full-history
  # compaction.
  defp earlier_binding!(armed) do
    assert {:ok, %NativeCompactionAdmission{binding: previous}} =
             WebsocketOwnerForwarder.remote_admission_control_v1(armed.codex_session_id, earlier_control(:snapshot, armed.downstream, []))

    previous
    |> Map.take(@binding_fields)
    |> Map.merge(%{compaction_item_digest: nil, previous_response_digest: nil, serving_mode: :full})
    |> Map.put(:__struct__, NativeCompactionAdmission.Binding)
  end

  defp earlier_control(action, downstream, attrs) do
    @control_fields
    |> Map.new(&{&1, nil})
    |> Map.merge(%{version: 1, action: action, downstream: downstream})
    |> Map.merge(Map.new(attrs))
    |> Map.put(:__struct__, WebsocketOwnerAdmissionControlV1)
  end

  defp earlier_request_v3(upstream_identity_id, provenance) do
    request = %{
      version: 3,
      url: "https://upstream.example.com/backend-api/codex/responses",
      headers: [],
      payload: CodexPooler.JSON.encode!(%{"type" => "response.create", "model" => "sample-model", "input" => [%{"role" => "user", "content" => "sample"}, %{"type" => "compaction_trigger"}]}),
      timeouts: %TimeoutConfig{connect_timeout_ms: 1_000, pool_timeout_ms: 1_000, receive_timeout_ms: 30_000},
      mapper: :native_codex_responses,
      upstream_identity_id: upstream_identity_id,
      observation: %{request_id: Ecto.UUID.generate(), client_request_id: nil, attempt_id: Ecto.UUID.generate(), mode: "full"},
      reset_probe: nil,
      native_codex_response_control: nil,
      assignment_advertised?: false,
      connection_bound_continuation?: false,
      forward_error_body?: false,
      submission_notification?: false,
      websocket_delivery_mode: :collect_compaction,
      effective_serving_mode: :full,
      owner_admission_capability: nil,
      first_compact_collection: provenance
    }

    assert Enum.sort(Map.keys(request)) == Enum.sort(@request_v3_fields)
    Map.put(request, :__struct__, WebsocketOwnerRequestV3)
  end

  defp cleanup_owner_session(codex_session_id) do
    case WebsocketOwnerSession.lookup(codex_session_id) do
      {:ok, owner} ->
        owner_ref = Process.monitor(owner)
        _result = GenServer.stop(owner, :normal, 15_000)

        receive do
          {:DOWN, ^owner_ref, :process, ^owner, _reason} -> :ok
        after
          15_000 -> flunk("websocket owner did not terminate during test cleanup")
        end

      {:error, :owner_unavailable} ->
        :ok
    end
  catch
    :exit, _reason -> :ok
  end
end
