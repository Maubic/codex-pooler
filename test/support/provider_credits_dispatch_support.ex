defmodule CodexPooler.ProviderCreditsDispatchSupport do
  @moduledoc false

  import Ecto.Query

  alias CodexPooler.Access
  alias CodexPooler.Gateway.Payloads.RequestOptions
  alias CodexPooler.Gateway.Payloads.RequestOptions.ResetProbe
  alias CodexPooler.Gateway.Runtime.Service
  alias CodexPooler.Gateway.Transports.ProviderCreditsAdmission
  alias CodexPooler.Gateway.Transports.UpstreamDispatch
  alias CodexPooler.Gateway.Transports.Websocket.{UpstreamWebsocketSession, WebsocketOwnerRequestV8, WebsocketOwnerSession}
  alias CodexPooler.PoolerFixtures
  alias CodexPooler.ProviderCreditsFixtures
  alias CodexPooler.Repo
  alias CodexPooler.Upstreams.Lifecycle.CredentialFencing
  alias CodexPooler.Upstreams.Schemas.{PoolUpstreamAssignment, UpstreamIdentity}
  alias Ecto.Adapters.SQL.Sandbox

  @doc "Creates real, independently non-credit capacity for low-level transport scenarios. No production exception is qualified."
  @spec context!(UpstreamIdentity.t() | nil, keyword()) :: ProviderCreditsAdmission.Context.t()
  def context!(identity \\ nil, opts \\ []) do
    key = {__MODULE__, identity && identity.id}

    {assignment, identity} =
      case Process.get(key) do
        nil ->
          rows = create_scope!(identity, opts)
          Process.put(key, rows)
          rows

        rows ->
          rows
      end

    %ProviderCreditsAdmission.Context{
      version: 1,
      pool_id: assignment.pool_id,
      pool_upstream_assignment_id: assignment.id,
      upstream_identity_id: identity.id,
      credential_epoch: CredentialFencing.credential_epoch(identity),
      model: Keyword.get(opts, :model, "example-model"),
      upstream_model: Keyword.get(opts, :upstream_model, "example-model"),
      serving_mode: Keyword.get(opts, :serving_mode, :full),
      transport: Keyword.get(opts, :transport, :native_websocket),
      route_class: Keyword.get(opts, :route_class, "proxy_websocket"),
      request_id: Keyword.get(opts, :request_id),
      attempt_id: Keyword.get(opts, :attempt_id),
      reset_probe: nil,
      redemption_generation: nil,
      redemption_attempt_id: nil
    }
  end

  @doc "Binds a physical wire fixture to real persisted scope and its current accounting identities."
  @spec wire_request!(UpstreamWebsocketSession.Request.t(), keyword()) :: UpstreamWebsocketSession.Request.t()
  def wire_request!(%UpstreamWebsocketSession.Request{} = request, opts \\ []) do
    model =
      case CodexPooler.JSON.decode(request.payload) do
        {:ok, %{"model" => model}} when is_binary(model) and byte_size(model) > 0 -> model
        _synthetic_control -> "example-model"
      end

    mode = if request.effective_serving_mode in [:lite, "lite"], do: :lite, else: :full
    context_opts = Keyword.merge([model: model, upstream_model: model, serving_mode: mode, request_id: request.request_id, attempt_id: request.attempt_id], Keyword.delete(opts, :identity))
    context = context!(Keyword.get(opts, :identity), context_opts)
    context = bind_probe_context(context, request.reset_probe, Repo.get!(UpstreamIdentity, context.upstream_identity_id))
    %{request | provider_credits_context: context}
  end

  @spec attach!(UpstreamDispatch.Request.t()) :: UpstreamDispatch.Request.t()
  def attach!(%UpstreamDispatch.Request{request_options: %RequestOptions{} = options} = request) do
    options = if is_nil(options.routing.model_serving_mode), do: RequestOptions.put_model_serving_mode(options, %{configured_mode: "full", effective_mode: "full", source: "override"}), else: options
    context = context!(request.identity, model: options.routing.effective_model || "example-model", upstream_model: options.routing.effective_model || "example-model", serving_mode: if(RequestOptions.model_serving_mode(options) == "lite", do: :lite, else: :full), transport: if(options.transport.transport == "websocket", do: :native_websocket, else: :http_json), route_class: options.transport.route_class || "proxy_websocket", request_id: if(request.accounting_request, do: request.accounting_request.id, else: options.request_metadata.request_id), attempt_id: request.accounting_attempt && request.accounting_attempt.id)
    context = bind_probe_context(context, options.routing.reset_probe, Repo.get!(UpstreamIdentity, context.upstream_identity_id))
    %{request | provider_credits_context: context, request_options: options}
  end

  @spec ignore_frame(binary()) :: :ok
  def ignore_frame(_frame), do: :ok
  @doc "Wraps an existing exact transport codec with current persisted test-owned admission scope."
  @spec owner_envelope!(struct()) :: CodexPooler.Gateway.Transports.Websocket.WebsocketOwnerRequestV8.t() | CodexPooler.Gateway.Transports.Websocket.UpstreamWebsocketSession.Request.t()
  def owner_envelope!(%WebsocketOwnerRequestV8{} = envelope), do: envelope
  def owner_envelope!(%UpstreamWebsocketSession.Request{} = request), do: request

  def owner_envelope!(inner) do
    identity = Repo.get(UpstreamIdentity, inner.upstream_identity_id) || %UpstreamIdentity{id: inner.upstream_identity_id}

    mode =
      case Map.get(inner, :effective_serving_mode) || inner.observation.mode do
        mode when mode in [:lite, "lite"] -> :lite
        _full -> :full
      end

    probe = inner.reset_probe
    scope = if match?(%ResetProbe{}, probe) and ResetProbe.bound?(probe), do: [model: probe.effective_model, upstream_model: probe.effective_model, route_class: probe.route_class], else: []
    context = context!(identity, scope ++ [serving_mode: mode, request_id: inner.observation.request_id, attempt_id: inner.observation.attempt_id])
    context = bind_probe_context(context, inner.reset_probe, identity)
    {:ok, envelope} = WebsocketOwnerRequestV8.new(%{version: 8, request: inner, provider_credits_context: context})
    envelope
  end

  defp bind_probe_context(context, %ResetProbe{} = probe, identity) do
    if ResetProbe.bound?(probe) do
      redemption = (identity.metadata || %{})["saved_reset_redemption"] || %{}
      %{context | reset_probe: probe, redemption_generation: redemption["generation"], redemption_attempt_id: redemption["attempt_id"]}
    else
      context
    end
  end

  defp bind_probe_context(context, nil, _identity), do: context

  @doc false
  @spec start_session() :: {:ok, pid()}
  def start_session do
    {:ok, pid} = UpstreamWebsocketSession.start_link([])
    Process.unlink(pid)
    {:ok, pid}
  end

  @doc false
  @spec held_http(UpstreamDispatch.Request.t(), pid(), reference()) :: term()
  def held_http(request, notify, reference) do
    Repo.checkout(fn ->
      %{rows: [[backend]]} = Repo.query!("SELECT pg_backend_pid()")
      send(notify, {:dispatch_reader_ready, reference, self(), backend})

      receive do
        {:dispatch_read, ^reference} -> UpstreamDispatch.http_request(request)
      after
        60_000 -> raise "dispatch read release missing"
      end
    end)
  end

  @doc false
  @spec transcribe(String.t(), Plug.Upload.t()) :: term()
  def transcribe(authorization, upload) do
    {:ok, auth} = Access.authenticate_authorization_header(authorization)
    options = RequestOptions.build(%{}, "/backend-api/transcribe", %{})
    Service.execute_multipart(auth, "/backend-api/transcribe", %{"file" => upload}, options)
  end

  @doc false
  @spec hold_attempt_insert(term(), map(), map(), map()) :: :ok
  def hold_attempt_insert(_event, _measurements, metadata, config) do
    key = {__MODULE__, :accounted_attempt, config.reference}
    capture_owned_attempt(metadata, config.identity_id, key)
    hold_committed_attempt(metadata.query, key, config)
    :ok
  end

  defp capture_owned_attempt(%{query: query, params: params}, identity_id, key) do
    params = params || []

    if String.starts_with?(query, ~s(INSERT INTO "attempts")) and (identity_id in params or Ecto.UUID.dump!(identity_id) in params),
      do: Process.put(key, inserted_attempt_id(query, params))
  end

  defp inserted_attempt_id(query, params) do
    fields = query |> String.split("(", parts: 2) |> List.last() |> String.split(")", parts: 2) |> hd() |> String.split(",") |> Enum.map(&(String.trim(&1) |> String.trim(~s("))))
    attrs = Map.new(Enum.zip(fields, params))

    case Ecto.UUID.cast(attrs["id"]) do
      {:ok, id} -> id
      :error -> Ecto.UUID.load!(attrs["id"])
    end
  end

  defp hold_committed_attempt(query, key, config) when query in ["commit", "COMMIT"], do: hold_accounted_attempt(Process.delete(key), config)
  defp hold_committed_attempt(_query, _key, _config), do: :ok
  defp hold_accounted_attempt(nil, _config), do: :ok

  defp hold_accounted_attempt(attempt_id, config) do
    caller = self()
    Agent.update(config.keeper, fn _ -> caller end)
    send(config.notify, {:provider_credits_accounted_attempt, config.reference, caller, attempt_id})

    receive do
      {:provider_credits_attempt_release, reference} when reference == config.reference -> :ok
    after
      60_000 -> raise "owned multipart attempt hold release missing"
    end
  end

  @doc false
  @spec execute(UpstreamDispatch.Request.t(), pid() | nil) :: term()
  def execute(request, session) do
    run = fn -> execute_transport(request, session) end

    if Repo.config()[:pool] == Ecto.Adapters.SQL.Sandbox, do: Sandbox.unboxed_run(Repo, run), else: run.()
  end

  @spec execute_terminal(UpstreamDispatch.Request.t(), pid() | nil) :: term()
  def execute_terminal(request, session) do
    case execute(request, session) do
      {:ok, %Req.Response{body: %Req.Response.Async{}} = response} ->
        {:ok, %{provider_credits_admission: Req.Response.get_private(response, :provider_credits_admission), terminal_completed?: peer_sse_terminal?(response)}}

      result ->
        result
    end
  end

  defp peer_sse_terminal?(response, chunks \\ []) do
    reference = response.body.ref

    receive do
      {^reference, {:data, data}} -> peer_sse_terminal?(response, [data | chunks])
      {^reference, :done} -> chunks |> Enum.reverse() |> IO.iodata_to_binary() |> String.contains?("response.completed")
      {^reference, {:error, _reason}} -> false
    after
      15_000 -> raise "owned peer SSE terminal did not arrive"
    end
  end

  defp execute_transport(%{provider_credits_context: %{transport: transport}} = request, session)
       when transport in [:native_websocket, :bridged_websocket] do
    options = if is_pid(session), do: RequestOptions.put_transport(request.request_options, upstream_websocket_session: session), else: request.request_options
    UpstreamDispatch.websocket_request(%{request | request_options: options})
  end

  defp execute_transport(request, _session), do: UpstreamDispatch.http_request(request)

  @doc "Pauses only an armed real owner handoff, then executes the unchanged request on its Mint session."
  @spec start_gated_owner(CodexPooler.Gateway.Persistence.CodexSession.t(), pid()) :: {:ok, map()}
  def start_gated_owner(session, notify) do
    {:ok, gate} = Agent.start(fn -> %{armed: nil, held: nil} end, name: owner_gate_name(session.id))

    upstream = %{
      start: fn -> UpstreamWebsocketSession.start_link(connection_close_subscriber: self(), admission_topology: :forwarded) end,
      send: fn upstream_session, request, writer -> gated_owner_send(gate, notify, upstream_session, request, writer) end,
      close: &UpstreamWebsocketSession.close/1,
      live_connection: &UpstreamWebsocketSession.live_connection/1
    }

    {:ok, owner} = WebsocketOwnerSession.start_owner(codex_session_id: session.id, owner_lease_token: session.owner_lease_token, owner_instance_id: session.owner_instance_id, owner_renewal_ms: 60_000, upstream: upstream)
    state = :sys.get_state(owner)
    {:ok, %{owner: owner, upstream_session: state.upstream_pid, gate: gate, codex_session_id: session.id}}
  end

  @spec arm_owner_gate(pid(), reference()) :: :ok
  def arm_owner_gate(gate, reference), do: Agent.update(gate, fn %{armed: nil, held: nil} -> %{armed: reference, held: nil} end)

  @spec release_owner_gate(Ecto.UUID.t()) :: :ok
  def release_owner_gate(session_id) do
    if gate = Process.whereis(owner_gate_name(session_id)) do
      case Agent.get(gate, & &1.held) do
        {pid, reference} -> send(pid, {:provider_credits_owner_release, reference})
        nil -> :ok
      end
    end

    :ok
  end

  defp owner_gate_name(session_id), do: String.to_atom("provider_credits_owner_gate_#{session_id}")

  defp gated_owner_send(_gate, _notify, upstream_session, payload, _writer) when is_binary(payload) do
    case UpstreamWebsocketSession.send_request_frame(upstream_session, payload) do
      {:ok, :sent} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp gated_owner_send(gate, notify, upstream_session, request, writer) do
    reference = take_owner_gate(gate)
    hold_owner_handoff(reference, gate, notify, request)
    upstream_session |> UpstreamWebsocketSession.request(%{request | writer: writer}) |> normalize_owner_result()
  end

  defp take_owner_gate(gate) do
    caller = self()

    Agent.get_and_update(gate, fn
      %{armed: nil} = state -> {nil, state}
      %{armed: reference} -> {reference, %{armed: nil, held: {caller, reference}}}
    end)
  end

  defp hold_owner_handoff(nil, _gate, _notify, _request), do: :ok

  defp hold_owner_handoff(reference, gate, notify, request) do
    send(notify, {:provider_credits_owner_handoff, reference, self(), owner_handoff_summary(request)})

    receive do
      {:provider_credits_owner_release, ^reference} -> :ok
    after
      60_000 -> raise "owned physical handoff release missing"
    end

    Agent.update(gate, &%{&1 | held: nil})
  end

  defp owner_handoff_summary(request) do
    capability = request.forwarded_owner_capability
    %{request_id: request.request_id, attempt_id: request.attempt_id, capability_phase: capability && capability.phase, native_replay?: not is_nil(request.native_replay_binding) and not is_nil(request.native_replay_proof), connection_bound?: request.connection_bound_continuation?, serving_mode: request.effective_serving_mode, delivery_mode: request.websocket_delivery_mode}
  end

  defp normalize_owner_result({:error, %{reason: :provider_credits_policy_denied}} = result), do: result
  defp normalize_owner_result({:error, %{transport_failure: failure}} = result) when is_map(failure), do: result
  defp normalize_owner_result({:error, %{upstream_websocket_connection: connection}} = result) when is_map(connection), do: result
  defp normalize_owner_result({:error, %{reason: reason}}) when is_atom(reason), do: {:error, reason}
  defp normalize_owner_result(result), do: result

  defp create_scope!(nil, opts) do
    pool = Keyword.get_lazy(opts, :pool, &PoolerFixtures.pool_fixture/0)
    %{identity: identity, assignment: assignment} = PoolerFixtures.upstream_assignment_fixture(pool)
    {assignment, included_identity!(identity)}
  end

  defp create_scope!(%UpstreamIdentity{} = selected, opts) do
    identity =
      Repo.get(UpstreamIdentity, selected.id) ||
        Repo.insert!(%UpstreamIdentity{id: selected.id, chatgpt_account_id: selected.chatgpt_account_id, account_label: "Synthetic transport", onboarding_method: "import", status: "active", headers_profile_version: 1, created_at: DateTime.utc_now(), updated_at: DateTime.utc_now(), metadata: CredentialFencing.initialize_metadata(%{})})

    assignment = Repo.one(from a in PoolUpstreamAssignment, where: a.upstream_identity_id == ^identity.id, limit: 1)
    assignment = assignment || Repo.insert!(%PoolUpstreamAssignment{pool_id: Keyword.get_lazy(opts, :pool, &PoolerFixtures.pool_fixture/0).id, upstream_identity_id: identity.id, assignment_label: "Synthetic transport", status: "active", health_status: "active", eligibility_status: "eligible", created_at: DateTime.utc_now(), updated_at: DateTime.utc_now(), metadata: %{}})
    {assignment, included_identity!(identity)}
  end

  defp included_identity!(identity) do
    if Map.has_key?(identity.metadata || %{}, "quota_capacity_facts") or Map.has_key?(identity.metadata || %{}, "saved_reset_redemption"),
      do: identity,
      else: ProviderCreditsFixtures.persist_usage!(identity, ProviderCreditsFixtures.usage_payload(:windowless_included, credits: :none), DateTime.utc_now())
  end
end
